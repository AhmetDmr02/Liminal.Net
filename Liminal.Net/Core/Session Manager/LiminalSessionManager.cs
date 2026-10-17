using Liminal.Net.Interfaces;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public enum DeliveryMethod : byte
    {
        Unreliable = 0,
        Reliable = 1
    }

    public class LiminalSessionManager : IDisposable, ISessionTelemetryProvider
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();
        private LiminalSession[] _activeSessions = Array.Empty<LiminalSession>();

        private readonly ConcurrentQueue<(ushort SenderId, InboundPacket Packet)> _loopbackQueue = new();

        //Written under _lifecycleLock together with the session map so connect and disconnect land in the order the transport raised them
        private readonly ConcurrentQueue<LiminalLifecycleEntry> _lifecycleQueue = new();
        private bool _lifecycleClosed;
        private Action<LiminalLifecycleEntry> _lifecycleSink;

        internal Action<LiminalLifecycleEntry> LifecycleSink
        {
            get => Volatile.Read(ref _lifecycleSink);
            set => Volatile.Write(ref _lifecycleSink, value);
        }

        //A local disconnect this closed manager can no longer queue, the game is still told its own connection ended
        private Action<ushort> _orphanLocalClientDisconnected;
        internal Action<ushort> OrphanLocalClientDisconnected
        {
            get => Volatile.Read(ref _orphanLocalClientDisconnected);
            set => Volatile.Write(ref _orphanLocalClientDisconnected, value);
        }

        //Raised on the transport thread once a local connect or disconnect made it into this run's queue, the manager moves its state right away
        internal Action<ushort> LocalClientConnectedQueued;
        internal Action<ushort> LocalClientDisconnectedQueued;

        //0 none, 1 the transport shut down during this run, 2 and the game was told
        private int _transportShutdown;
        internal bool TransportShutDown => Volatile.Read(ref _transportShutdown) != 0;
        internal bool TryTakeTransportShutdownEvent() => Interlocked.CompareExchange(ref _transportShutdown, 2, 1) == 1;

        private readonly ILiminalTransport _transport;
        private readonly LiminalNetworkConfig _config;
        private readonly LiminalPacketFramerPipeline _pipeline;
        private readonly ArrayPool<byte> _privatePool;
        private readonly ArrayPool<byte> _recoveryPacketPool;

        private readonly LiminalHiccupController _hiccup;

        private volatile bool _sessionManagerDisposed;
        private int _isPolling;
        private int _loopbackCount;

        private long _totalPacketsInbound;
        private long _totalPacketsOutbound;
        private volatile LiminalTelemetryConfig _telemetryConfig;

        private readonly LiminalPacketInterpreter _interpreter;
        private readonly LiminalPacketFragmentor _fragmentor;

        public event Action<ServerStarvationEvent> OnServerStarvation;

        private readonly object _lifecycleLock = new();

        //Kicked connections whose own disconnect has not come yet, only touched under _lifecycleLock and gone with this run
        private readonly HashSet<LiminalConnection> _kickedConnections = new();

        public LiminalSessionManager(ILiminalTransport transport, LiminalPacketInterpreter interpreter, LiminalNetworkConfig config, LiminalPacketFramerPipeline pipeline)
        {
            _transport = transport;
            _config = config;
            _pipeline = pipeline;
            _interpreter = interpreter;

            _config.Validate();

            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);
            _recoveryPacketPool = ArrayPool<byte>.Create(config.Hiccup.GetRecoverySize(config.MaxPacketSizePerBatch), 50);

            _hiccup = new LiminalHiccupController(_transport, _config, _sessions);
            _hiccup.OnServerStarvation += eventData => OnServerStarvation?.Invoke(eventData);

            _fragmentor = new LiminalPacketFragmentor(_transport, _config);
            _fragmentor.OnMessageReassembled += HandleReliableMessage;

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;

            //Subscribed per run, an event raised late for this run lands here and the closed queue drops it instead of a newer run taking it
            _transport.OnClientConnected += OnTransportClientConnected;
            _transport.OnClientDisconnected += OnTransportClientDisconnected;
            _transport.OnClientKicked += OnTransportClientKicked;
            _transport.OnLocalClientConnected += OnTransportLocalClientConnected;
            _transport.OnLocalClientDisconnected += OnTransportLocalClientDisconnected;
            _transport.OnShutdown += OnTransportShutdown;

            _interpreter.OnSendRequest += BufferPacket;
        }

        //Next run or final shutdown, until then this closed manager keeps dropping its run's late transport events
        internal void DetachTransport()
        {
            _transport.OnClientConnected -= OnTransportClientConnected;
            _transport.OnClientDisconnected -= OnTransportClientDisconnected;
            _transport.OnClientKicked -= OnTransportClientKicked;
            _transport.OnLocalClientConnected -= OnTransportLocalClientConnected;
            _transport.OnLocalClientDisconnected -= OnTransportLocalClientDisconnected;
            _transport.OnShutdown -= OnTransportShutdown;
        }

        #region Receive Path

        private void HandleReliableMessage(ReadOnlySpan<byte> data, LiminalConnection from) => ProcessIncoming(from, data, DeliveryMethod.Reliable);

        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, LiminalConnection from) => ProcessIncoming(from, data, DeliveryMethod.Unreliable);

        private void ProcessIncoming(LiminalConnection from, ReadOnlySpan<byte> transportData, DeliveryMethod deliveryMethod)
        {
            if (_sessionManagerDisposed)
                return;

            ushort ownerId = from.ClientId;

            //Transports raise connect before their reader starts, no session here means the client is gone
            //a session of another connection means these bytes are an old socket's leftovers, they must not count against the new one
            if (!_sessions.TryGetValue(ownerId, out var session) || session.Connection != from)
                return;

            TelemetryFlags activeFlags = _telemetryConfig?.Flags ?? TelemetryFlags.None;
            bool countPackets = (activeFlags & TelemetryFlags.PacketCounting) != 0;
            int inboundBatchCount = 0;

            bool shouldKick = false;
            string kickReason = null;
            ServerStarvationEvent? starvation = null;

            try
            {
                lock (session.ReceiveLock)
                {
                    if (session.IsDisposed())
                        return;

                    if (!_sessions.TryGetValue(ownerId, out var current) || !ReferenceEquals(current, session))
                        return;

                    if (deliveryMethod == DeliveryMethod.Unreliable)
                    {
                        if (transportData.Length > _config.MaxPacketSizePerBatch || session.InboundPacketCount >= _config.MaxPacketCount)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Dropped inbound unreliable batch from client {ownerId}. Batch size or packet queue limit reached.");
                            return;
                        }

                        var processedBatch = _pipeline.ExecuteInboundBatch(session, transportData);

                        if (processedBatch.IsEmpty)
                            return;

                        if (processedBatch.Length > _config.MaxPacketSizePerBatch)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Dropped transformed unreliable batch ({processedBatch.Length}b) from client {ownerId}. Exceeded batch size.");
                            return;
                        }

                        int offset = 0;

                        while (offset + 4 <= processedBatch.Length)
                        {
                            int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedBatch.Slice(offset, 4));

                            //totalLen comes from the wire and offset + 4 + totalLen can overflow so check it with subtraction
                            if (totalLen < 2 || totalLen > processedBatch.Length - offset - 4)
                                break;

                            if (session.InboundPacketCount >= _config.MaxPacketCount || !session.TryReserveInboundPacketUnreliable(_config.MaxPacketCount))
                            {
                                LiminalLogger.LogWarning($"[SessionManager] Inbound queue capacity reached for client {ownerId}. Dropping remaining unreliable packets.");
                                break;
                            }

                            ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(processedBatch.Slice(offset + 4, 2));
                            int payloadLen = totalLen - 2;

                            byte[] rentedBuffer = null;

                            try
                            {
                                rentedBuffer = _privatePool.Rent(payloadLen);
                                processedBatch.Slice(offset + 6, payloadLen).CopyTo(rentedBuffer);

                                session.InboundQueueUnreliable.Enqueue(
                                    new InboundPacket(packetId, rentedBuffer, payloadLen, usesRecoveryPool: false));

                                rentedBuffer = null;
                            }
                            catch
                            {
                                session.ReleaseInboundPacketUnreliable();
                                throw;
                            }
                            finally
                            {
                                if (rentedBuffer != null)
                                    _privatePool.Return(rentedBuffer);
                            }

                            if (countPackets)
                                inboundBatchCount++;

                            offset += 4 + totalLen;
                        }

                        return;
                    }

                    bool exceedsNormalThreshold =
                        transportData.Length > _config.MaxPacketSizePerBatch ||
                        session.InboundPacketCount >= _config.MaxPacketCount;

                    if (exceedsNormalThreshold)
                    {
                        bool canAvoidRecovery =
                            !session.InboundRecoveryActive &&
                            session.InboundPacketCountUnreliable > 0 &&
                            transportData.Length <= _config.MaxPacketSizePerBatch &&
                            session.InboundPacketCountReliable < _config.MaxPacketCount;

                        if (canAvoidRecovery)
                        {
                            int dropped = session.DropQueuedUnreliableInbound(ReturnPacketBuffer);

                            LiminalLogger.LogWarning(
                                $"[SessionManager] Dropped {dropped} queued unreliable inbound packets for client {ownerId} to prevent Hiccup recovery.");
                        }

                        if (transportData.Length > _config.MaxPacketSizePerBatch ||
                            session.InboundPacketCount >= _config.MaxPacketCount)
                        {
                            if (!EnsureInboundRecoveryLocked(session, ownerId, transportData.Length, Stopwatch.GetTimestamp(), out string failureReason, ref starvation))
                            {
                                shouldKick = true;
                                kickReason = failureReason;
                                return;
                            }
                        }
                    }

                    if (transportData.Length > GetInboundStageCapacity(session))
                    {
                        shouldKick = true;
                        kickReason = "Inbound payload exceeded the session recovery ceiling.";
                        return;
                    }

                    var processedReliableBatch = _pipeline.ExecuteInboundBatch(session, transportData);

                    if (processedReliableBatch.IsEmpty)
                        return;

                    if (processedReliableBatch.Length > GetInboundStageCapacity(session))
                    {
                        shouldKick = true;
                        kickReason = "Inbound transformed batch exceeded the session recovery ceiling.";
                        return;
                    }

                    int relOffset = 0;

                    while (relOffset + 4 <= processedReliableBatch.Length)
                    {
                        int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedReliableBatch.Slice(relOffset, 4));

                        if (totalLen < 2 || totalLen > processedReliableBatch.Length - relOffset - 4)
                            break;

                        if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                        {
                            if (!session.InboundRecoveryActive &&
                                session.InboundPacketCountUnreliable > 0 &&
                                session.InboundPacketCountReliable < _config.MaxPacketCount)
                            {
                                int dropped = session.DropQueuedUnreliableInbound(ReturnPacketBuffer);

                                LiminalLogger.LogWarning(
                                    $"[SessionManager] Dropped {dropped} queued unreliable inbound packets for client {ownerId} to fit reliable packet.");
                            }

                            if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                            {
                                if (!EnsureInboundRecoveryLocked(session, ownerId, processedReliableBatch.Length, Stopwatch.GetTimestamp(), out string failureReason, ref starvation))
                                {
                                    shouldKick = true;
                                    kickReason = failureReason;
                                    return;
                                }

                                if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                                {
                                    shouldKick = true;
                                    kickReason = "Inbound packet-count recovery ceiling exceeded.";
                                    return;
                                }
                            }
                        }

                        if (!session.TryReserveInboundPacketReliable(GetInboundPacketLimit(session)))
                        {
                            shouldKick = true;
                            kickReason = "Inbound packet-count reservation failed at the active ceiling.";
                            return;
                        }

                        ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(processedReliableBatch.Slice(relOffset + 4, 2));
                        int payloadLen = totalLen - 2;

                        ArrayPool<byte> payloadPool =
                            payloadLen > _config.MaxPacketSizePerBatch
                                ? _recoveryPacketPool
                                : _privatePool;

                        byte[] rentedBuffer = null;

                        try
                        {
                            rentedBuffer = payloadPool.Rent(payloadLen);

                            processedReliableBatch
                                .Slice(relOffset + 6, payloadLen)
                                .CopyTo(rentedBuffer);

                            session.InboundQueueReliable.Enqueue(
                                new InboundPacket(
                                    packetId,
                                    rentedBuffer,
                                    payloadLen,
                                    payloadPool == _recoveryPacketPool));

                            rentedBuffer = null;
                        }
                        catch
                        {
                            session.ReleaseInboundPacketReliable();
                            throw;
                        }
                        finally
                        {
                            if (rentedBuffer != null)
                                payloadPool.Return(rentedBuffer);
                        }

                        if (session.InboundRecoveryActive)
                            session.Recovery.InboundLimiter.LastActivityTimestamp = Stopwatch.GetTimestamp();

                        if (countPackets)
                            inboundBatchCount++;

                        relOffset += 4 + totalLen;
                    }
                }
            }
            catch (Exception e)
            {
                LiminalLogger.LogError($"[SessionManager] Error processing inbound packet: {e}");
                shouldKick = true;
                kickReason = "Inbound processing failure.";
            }
            finally
            {
                if (inboundBatchCount > 0)
                    Interlocked.Add(ref _totalPacketsInbound, inboundBatchCount);

                if (starvation.HasValue)
                    EnqueueLifecycle(LiminalLifecycleEntry.Starved(starvation.Value));

                if (shouldKick)
                {
                    _hiccup.Kick(session, LiminalRecoveryDirection.Inbound, kickReason);
                }
            }
        }

        #endregion

        #region Write Path

        //Size only, the frame lives in a native send buffer that gets reused and freed once the lock is released
        public event Action<ushort, int> OnPacketBuffered;

        public void BufferPacket(ushort senderId, ushort targetId, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable)
        {
            if (_sessionManagerDisposed)
                return;

            TelemetryFlags activeFlags = _telemetryConfig?.Flags ?? TelemetryFlags.None;
            bool countPackets = (activeFlags & TelemetryFlags.PacketCounting) != 0;

            bool isHostMode = _transport.IsServer && _transport.IsClient;
            ushort localClientId = _transport.LocalClientId;

            bool isLocalDestination =
                targetId == localClientId ||
                (isHostMode && targetId == ILiminalTransport.SERVER_ID);

            if (isLocalDestination)
            {
                //Reserve first, a count check then enqueue lets every concurrent sender past the limit
                if (Interlocked.Increment(ref _loopbackCount) > _config.MaxPacketCount)
                {
                    Interlocked.Decrement(ref _loopbackCount);
                    LiminalLogger.LogError("[SessionManager] Loopback queue overflow. Dropping packet.");
                    return;
                }

                byte[] rentedBuffer = _privatePool.Rent(payload.Length);

                try
                {
                    payload.CopyTo(rentedBuffer);

                    var packet = new InboundPacket(packetId, rentedBuffer, payload.Length);

                    if (countPackets)
                        Interlocked.Increment(ref _totalPacketsOutbound);

                    _loopbackQueue.Enqueue((senderId, packet));

                    rentedBuffer = null;
                    return;
                }
                finally
                {
                    if (rentedBuffer != null)
                    {
                        _privatePool.Return(rentedBuffer);
                        Interlocked.Decrement(ref _loopbackCount);
                    }
                }
            }

            if (!_sessions.TryGetValue(targetId, out var session))
            {
                LiminalLogger.LogWarning($"[SessionManager] Cannot route packet. Target {targetId} does not exist.");
                return;
            }

            int frameSize = 4 + 2 + payload.Length;

            bool shouldKick = false;
            string kickReason = null;
            ServerStarvationEvent? starvation = null;

            int bufferedSize = 0;
            bool raiseBufferedEvent = false;

            try
            {
                lock (session.SendLock)
                {
                    if (session.IsDisposed())
                        return;

                    if (!_sessions.TryGetValue(targetId, out var current) || !ReferenceEquals(current, session))
                        return;

                    if (deliveryMethod == DeliveryMethod.Unreliable)
                    {
                        int unreliableCapacity = session.RawSendBufferUnreliable.GetSpan().Length;

                        int totalCombined =
                            session.RawSendCursorReliable +
                            session.RawSendCursorUnreliable +
                            frameSize;

                        if (session.RawSendCursorUnreliable + frameSize > unreliableCapacity ||
                            totalCombined > _config.MaxPacketSizePerBatch)
                        {
                            LiminalLogger.LogWarning(
                                $"[SessionManager] Dropped unreliable packet {packetId} ({frameSize}b) for client {targetId}. Outbound capacity limit reached.");

                            return;
                        }

                        Span<byte> dest =
                            session.RawSendBufferUnreliable
                                .GetSpan()
                                .Slice(session.RawSendCursorUnreliable);

                        BinaryPrimitives.WriteInt32LittleEndian(
                            dest.Slice(0, 4),
                            payload.Length + 2);

                        BinaryPrimitives.WriteUInt16LittleEndian(
                            dest.Slice(4, 2),
                            packetId);

                        payload.CopyTo(dest.Slice(6));

                        session.RawSendCursorUnreliable += frameSize;

                        if (_telemetryConfig?.EnablePacketCounting == true)
                        {
                            bufferedSize = frameSize;

                            raiseBufferedEvent = true;
                        }

                        if (countPackets)
                            Interlocked.Increment(ref _totalPacketsOutbound);
                    }
                    else
                    {
                        int activeCapacity = GetOutboundBufferCapacity(session);

                        int totalBuffered =
                            session.RawSendCursorReliable +
                            session.RawSendCursorUnreliable;

                        int reliableRequired =
                            session.RawSendCursorReliable +
                            frameSize;

                        bool exceedsThreshold =
                            frameSize > activeCapacity ||
                            (totalBuffered + frameSize) > activeCapacity ||
                            totalBuffered >= _config.MaxPacketSizePerBatch;

                        if (exceedsThreshold)
                        {
                            bool canAvoidRecoveryByDroppingUnreliable =
                                !session.OutboundRecoveryActive &&
                                session.RawSendCursorUnreliable > 0 &&
                                reliableRequired <= _config.MaxPacketSizePerBatch;

                            if (canAvoidRecoveryByDroppingUnreliable)
                            {
                                int droppedBytes = session.RawSendCursorUnreliable;

                                session.RawSendCursorUnreliable = 0;

                                LiminalLogger.LogWarning(
                                    $"[SessionManager] Dropped {droppedBytes}b of queued unreliable packets for client {targetId} to fit reliable packet {packetId} without triggering Hiccup recovery.");
                            }
                            else
                            {
                                int required = Math.Max(frameSize, reliableRequired);

                                if (!EnsureOutboundRecoveryLocked(
                                        session,
                                        targetId,
                                        required,
                                        Stopwatch.GetTimestamp(),
                                        out string failureReason,
                                        ref starvation))
                                {
                                    session.RawSendCursorReliable = 0;
                                    session.RawSendCursorUnreliable = 0;

                                    shouldKick = true;
                                    kickReason = failureReason;
                                    return;
                                }

                                activeCapacity = GetOutboundBufferCapacity(session);

                                if (frameSize > activeCapacity || reliableRequired > activeCapacity)
                                {
                                    session.RawSendCursorReliable = 0;
                                    session.RawSendCursorUnreliable = 0;

                                    shouldKick = true;
                                    kickReason = "Outbound recovery ceiling exceeded.";
                                    return;
                                }
                            }
                        }

                        Span<byte> relDest =
                            session.ActiveRawSendBufferReliable
                                .GetSpan()
                                .Slice(session.RawSendCursorReliable);

                        BinaryPrimitives.WriteInt32LittleEndian(
                            relDest.Slice(0, 4),
                            payload.Length + 2);

                        BinaryPrimitives.WriteUInt16LittleEndian(
                            relDest.Slice(4, 2),
                            packetId);

                        payload.CopyTo(relDest.Slice(6));

                        session.RawSendCursorReliable += frameSize;

                        if (_telemetryConfig?.EnablePacketCounting == true)
                        {
                            bufferedSize = frameSize;

                            raiseBufferedEvent = true;
                        }

                        if (session.OutboundRecoveryActive)
                        {
                            session.Recovery.OutboundLimiter.LastActivityTimestamp =
                                Stopwatch.GetTimestamp();
                        }

                        if (countPackets)
                            Interlocked.Increment(ref _totalPacketsOutbound);
                    }
                }

                if (raiseBufferedEvent)
                    OnPacketBuffered?.Invoke(packetId, bufferedSize);
            }
            finally
            {
                if (starvation.HasValue)
                    EnqueueLifecycle(LiminalLifecycleEntry.Starved(starvation.Value));

                if (shouldKick)
                {
                    _hiccup.Kick(
                        session,
                        LiminalRecoveryDirection.Outbound,
                        kickReason);
                }
            }
        }

        public void Flush()
        {
            if (_sessionManagerDisposed)
            {
                RaiseShutdown();
                return;
            }

            long now = Stopwatch.GetTimestamp();

            LiminalSession[] activeSessions = Volatile.Read(ref _activeSessions);
            for (int i = 0; i < activeSessions.Length; i++)
            {
                var session = activeSessions[i];

                try
                {
                    FlushSession(session);
                    _hiccup.ReleaseOutboundWhenSafe(session, now);
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError(
                        $"[SessionManager] Error flushing session {session.Id}: {ex}");

                    _hiccup.Kick(
                        session,
                        LiminalRecoveryDirection.Outbound,
                        "Flush pipeline failure.");
                }
            }
        }

        private void FlushSession(LiminalSession session)
        {
            byte[] unreliableBatch = null;
            int unreliableLength = 0;

            byte[] reliableBatch = null;
            int reliableLength = 0;

            bool shouldKick = false;
            string kickReason = null;

            try
            {
                lock (session.SendLock)
                {
                    if (session.IsDisposed())
                        return;

                    if (!_sessions.TryGetValue(session.Id, out var current) || !ReferenceEquals(current, session))
                        return;

                    if (session.RawSendCursorUnreliable > 0)
                    {
                        int rawLength = session.RawSendCursorUnreliable;

                        int transformedLength =
                            _pipeline.ExecuteOutboundBatch(
                                session,
                                rawLength,
                                session.RawSendBufferUnreliable);

                        if (transformedLength > session.RawSendBufferUnreliable.GetSpan().Length)
                        {
                            LiminalLogger.LogWarning(
                                $"[SessionManager] Transformed unreliable batch exceeded normal capacity for {session.Id}. Dropping.");

                            transformedLength = 0;
                        }

                        if (transformedLength > 0)
                        {
                            unreliableBatch =
                                ArrayPool<byte>.Shared.Rent(transformedLength);

                            session.ActiveSendBuffer
                                .Memory
                                .Slice(0, transformedLength)
                                .CopyTo(unreliableBatch);

                            unreliableLength = transformedLength;
                        }

                        session.RawSendCursorUnreliable = 0;
                    }

                    if (session.RawSendCursorReliable > 0)
                    {
                        int rawLength = session.RawSendCursorReliable;

                        int transformedLength =
                            _pipeline.ExecuteOutboundBatch(
                                session,
                                rawLength,
                                session.ActiveRawSendBufferReliable);

                        int outboundCapacity = GetOutboundBufferCapacity(session);

                        if (transformedLength > outboundCapacity)
                        {
                            shouldKick = true;
                            kickReason =
                                "Outbound transformed reliable batch exceeded the recovery ceiling.";

                            session.RawSendCursorReliable = 0;
                            return;
                        }

                        if (transformedLength > 0)
                        {
                            reliableBatch =
                                ArrayPool<byte>.Shared.Rent(transformedLength);

                            session.ActiveSendBuffer
                                .Memory
                                .Slice(0, transformedLength)
                                .CopyTo(reliableBatch);

                            reliableLength = transformedLength;
                        }

                        session.RawSendCursorReliable = 0;
                    }
                }

                if (unreliableLength > 0)
                {
                    _fragmentor.Send(
                        unreliableBatch.AsSpan(0, unreliableLength),
                        session.Connection,
                        TransportFlags.Unreliable);
                }

                if (reliableLength > 0)
                {
                    _fragmentor.Send(
                        reliableBatch.AsSpan(0, reliableLength),
                        session.Connection,
                        TransportFlags.Reliable);
                }
            }
            finally
            {
                if (unreliableBatch != null)
                    ArrayPool<byte>.Shared.Return(unreliableBatch);

                if (reliableBatch != null)
                    ArrayPool<byte>.Shared.Return(reliableBatch);

                if (shouldKick)
                {
                    _hiccup.Kick(
                        session,
                        LiminalRecoveryDirection.Outbound,
                        kickReason);
                }
            }
        }

        #endregion

        #region Polling Logic

        public void Poll()
        {
            if (_sessionManagerDisposed)
            {
                RaiseShutdown();
                return;
            }

            if (Interlocked.CompareExchange(ref _isPolling, 1, 0) != 0)
            {
                return;
            }

            try
            {
                _fragmentor.PruneStaleAssemblies();

                //Before any packet so a session's connect event is always raised ahead of its first packet
                ProcessLifecycleEvents();

                while (!_loopbackQueue.IsEmpty &&
                       _loopbackQueue.TryDequeue(out var loopbackItem))
                {
                    Interlocked.Decrement(ref _loopbackCount);
                    var senderId = loopbackItem.SenderId;
                    var packet = loopbackItem.Packet;

                    try
                    {
                        _interpreter.Dispatch(
                            packet.PacketId,
                            senderId,
                            packet.AsMemory());
                    }
                    finally
                    {
                        ReturnPacketBuffer(packet);
                    }
                }

                long now = Stopwatch.GetTimestamp();

                LiminalSession[] activeSessions = Volatile.Read(ref _activeSessions);
                for (int i = 0; i < activeSessions.Length; i++)
                {
                    var session = activeSessions[i];
                    if (session.IsDisposed() || !session.Announced)
                        continue;

                    if (!_sessions.TryGetValue(session.Id, out var current) || !ReferenceEquals(current, session))
                        continue;

                    DrainInbound(session, dispatch: true);

                    _hiccup.ReleaseInboundWhenSafe(session, now);
                }
            }
            finally
            {
                Volatile.Write(ref _isPolling, 0);
            }
        }

        private void DrainInbound(LiminalSession session, bool dispatch)
        {
            while (session.InboundQueueUnreliable.TryDequeue(out var packet))
            {
                session.ReleaseInboundPacketUnreliable();
                DispatchAndReturn(session.Id, packet, dispatch);
            }

            while (session.InboundQueueReliable.TryDequeue(out var packet))
            {
                session.ReleaseInboundPacketReliable();
                DispatchAndReturn(session.Id, packet, dispatch);
            }
        }

        private void DispatchAndReturn(ushort sessionId, InboundPacket packet, bool dispatch)
        {
            try
            {
                if (dispatch)
                {
                    _interpreter.Dispatch(
                        packet.PacketId,
                        sessionId,
                        packet.AsMemory());
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError(
                    $"[SessionManager] Error in handler for ID {packet.PacketId}: {ex}");
            }
            finally
            {
                ReturnPacketBuffer(packet);
            }
        }

        #endregion

        #region Lifecycle

        public bool HasSession(ushort sessionId)
        {
            return !_sessionManagerDisposed && _sessions.ContainsKey(sessionId);
        }

        public bool TryGetSession(ushort sessionId, out LiminalSession session)
        {
            if (!_sessionManagerDisposed && _sessions.TryGetValue(sessionId, out session))
                return true;
            session = null;
            return false;
        }

        private void OnTransportClientConnected(LiminalConnection client) => HandleClientConnected(client);

        private void OnTransportClientDisconnected(LiminalConnection client) => HandleClientDisconnected(client);

        private void OnTransportClientKicked(LiminalConnection client) => HandleClientDisconnected(client, LiminalLifecycleKind.ClientKicked);

        private void OnTransportLocalClientDisconnected(ushort localClientId, LiminalConnection server)
        {
            switch (HandleLocalClientDisconnected(localClientId, server))
            {
                case LifecycleAccept.Queued:
                    LocalClientDisconnectedQueued?.Invoke(localClientId);
                    break;
                case LifecycleAccept.Closed:
                    OrphanLocalClientDisconnected?.Invoke(localClientId);
                    break;
            }
        }

        //Acted on by the manager's next drain, not here on the transport thread
        private void OnTransportShutdown()
        {
            lock (_lifecycleLock)
            {
                if (_lifecycleClosed)
                    return;

                Interlocked.CompareExchange(ref _transportShutdown, 1, 0);
            }
        }

        //Server session is created now so the reader that starts after this finds it
        private void OnTransportLocalClientConnected(ushort localClientId, LiminalConnection server)
        {
            if (QueueLocalConnection(localClientId, server))
                LocalClientConnectedQueued?.Invoke(localClientId);
        }

        private bool QueueLocalConnection(ushort localClientId, LiminalConnection server)
        {
            lock (_lifecycleLock)
            {
                if (_sessionManagerDisposed || _lifecycleClosed)
                    return false;

                //That connection already ended before its connect got here, its disconnect finds no session either so the game hears neither
                if (_transport != null && _transport.GetConnection(ILiminalTransport.SERVER_ID) != server)
                    return false;

                if (_sessions.TryRemove(ILiminalTransport.SERVER_ID, out var existingServerSession))
                {
                    EndSession(existingServerSession, dispatch: false);
                }

                var session = new LiminalSession(
                    server,
                    _config.MaxPacketSizePerBatch);

                _sessions[ILiminalTransport.SERVER_ID] = session;

                UpdateActiveSessionsArrayLocked();

                LiminalLogger.Log(
                    $"[SessionManager] Created session for Server (ID: {ILiminalTransport.SERVER_ID})");

                _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(LiminalLifecycleKind.LocalClientConnected, localClientId, session));
                return true;
            }
        }

        internal enum LifecycleAccept : byte
        {
            Queued,
            //Belongs to a connection this run no longer has, dropped
            Ignored,
            //This run's lifecycle is closed
            Closed
        }

        //Every check and map change below is under _lifecycleLock, the only lock the session map changes under
        internal LifecycleAccept HandleClientConnected(LiminalConnection client)
        {
            lock (_lifecycleLock)
            {
                if (_sessionManagerDisposed || _lifecycleClosed)
                    return LifecycleAccept.Closed;

                //That connection already ended or was replaced before its connect got here, its disconnect finds no session either so the game hears neither
                if (_transport != null && _transport.GetConnection(client.ClientId) != client)
                    return LifecycleAccept.Ignored;

                //Replaced without a disconnect, its leftovers belong to a connection that no longer exists
                if (_sessions.TryRemove(client.ClientId, out var existingSession))
                {
                    EndSession(existingSession, dispatch: false);
                }

                var session = new LiminalSession(
                    client,
                    _config.MaxPacketSizePerBatch);

                _sessions[client.ClientId] = session;

                UpdateActiveSessionsArrayLocked();

                _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(LiminalLifecycleKind.ClientConnected, client.ClientId, session));
                return LifecycleAccept.Queued;
            }
        }

        //Only the session of that same connection leaves, a late disconnect of an old connection must not take a reconnect's session
        //what it already has is dispatched before the event
        internal LifecycleAccept HandleClientDisconnected(LiminalConnection client, LiminalLifecycleKind kind = LiminalLifecycleKind.ClientDisconnected)
        {
            lock (_lifecycleLock)
            {
                if (_sessionManagerDisposed || _lifecycleClosed)
                    return LifecycleAccept.Closed;

                if (!_sessions.TryGetValue(client.ClientId, out var session) || session.Connection != client)
                {
                    //A kick is followed by that same connection's disconnect, the kick already took the session but the game still gets both
                    if (kind == LiminalLifecycleKind.ClientDisconnected && _kickedConnections.Remove(client))
                    {
                        _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(kind, client.ClientId, null));
                        return LifecycleAccept.Queued;
                    }

                    return LifecycleAccept.Ignored;
                }

                _sessions.TryRemove(client.ClientId, out _);
                UpdateActiveSessionsArrayLocked();

                if (kind == LiminalLifecycleKind.ClientKicked)
                    _kickedConnections.Add(client);

                _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(kind, client.ClientId, session));
                return LifecycleAccept.Queued;
            }
        }

        //server is default when the connect failed before there was a connection, the game is still told it ended
        internal LifecycleAccept HandleLocalClientDisconnected(ushort localClientId, LiminalConnection server)
        {
            lock (_lifecycleLock)
            {
                if (_sessionManagerDisposed || _lifecycleClosed)
                    return LifecycleAccept.Closed;

                _sessions.TryGetValue(ILiminalTransport.SERVER_ID, out var serverSession);
                if (server.IsValid && (serverSession == null || serverSession.Connection != server))
                    return LifecycleAccept.Ignored;

                bool changed = false;

                if (localClientId != ILiminalTransport.SERVER_ID && _sessions.TryRemove(localClientId, out var ownSession))
                {
                    changed = true;
                    _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(LiminalLifecycleKind.SessionClosed, localClientId, ownSession));
                }

                if (serverSession != null && _sessions.TryRemove(ILiminalTransport.SERVER_ID, out _))
                {
                    changed = true;
                }

                if (changed)
                {
                    UpdateActiveSessionsArrayLocked();
                }

                _lifecycleQueue.Enqueue(new LiminalLifecycleEntry(LiminalLifecycleKind.LocalClientDisconnected, localClientId, serverSession));
                return LifecycleAccept.Queued;
            }
        }

        internal bool EnqueueLifecycle(LiminalLifecycleEntry entry)
        {
            lock (_lifecycleLock)
            {
                if (_lifecycleClosed)
                    return false;

                _lifecycleQueue.Enqueue(entry);
                return true;
            }
        }

        private void UpdateActiveSessionsArrayLocked()
        {
            var array = new LiminalSession[_sessions.Count];
            int index = 0;
            foreach (var kvp in _sessions)
            {
                array[index++] = kvp.Value;
            }
            Volatile.Write(ref _activeSessions, array);
        }

        //Tick thread, or teardown once the tick is stopped
        internal void ProcessLifecycleEvents() => ProcessLifecycleEvents(dispatchPackets: true);

        private void ProcessLifecycleEvents(bool dispatchPackets)
        {
            while (_lifecycleQueue.TryDequeue(out var entry))
            {
                var session = entry.Session;
                if (session != null)
                {
                    if (entry.Kind == LiminalLifecycleKind.ClientConnected || entry.Kind == LiminalLifecycleKind.LocalClientConnected)
                    {
                        session.Announced = true;
                    }
                    else
                    {
                        EndSession(session, dispatchPackets);
                    }
                }

                try
                {
                    if (entry.Kind == LiminalLifecycleKind.Starvation)
                    {
                        _hiccup.RaiseStarvation(entry.Starvation);
                    }
                    else if (entry.Kind != LiminalLifecycleKind.SessionClosed)
                    {
                        LifecycleSink?.Invoke(entry);
                    }
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[SessionManager] Error raising {entry.Kind} for {entry.ClientId}: {ex}");
                }
            }
        }

        private void EndSession(LiminalSession session, bool dispatch)
        {
            //Disposed first, it takes the session locks so a receive still running finishes before the drain and none can queue or attach recovery after it
            session.Dispose();
            ReleaseRecoveryForSession(session);
            DrainInbound(session, dispatch);
        }

        private void ReleaseRecoveryForSession(LiminalSession session) =>
            _hiccup.ReleaseForSession(session);

        private void RaiseShutdown()
        {
            //Flush, Poll and Dispose can all get here at once from different threads
            if (Interlocked.Exchange(ref _shutdownRaised, 1) != 0)
                return;

            _fragmentor.OnMessageReassembled -= HandleReliableMessage;
            _fragmentor.Dispose();

            _transport.OnMessageReceivedReliable -= HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable -= HandleUnreliableMessage;

            _interpreter.OnSendRequest -= BufferPacket;

            while (_loopbackQueue.TryDequeue(out var loopbackItem))
            {
                Interlocked.Decrement(ref _loopbackCount);
                ReturnPacketBuffer(loopbackItem.Packet);
            }

            LiminalSession[] remaining;
            lock (_lifecycleLock)
            {
                //Closed under the same lock sessions are created under, nothing new appears after the snapshot below
                _lifecycleClosed = true;
                remaining = new LiminalSession[_sessions.Count];
                _sessions.Values.CopyTo(remaining, 0);
                _sessions.Clear();
                Volatile.Write(ref _activeSessions, Array.Empty<LiminalSession>());
            }

            //Events still queued are raised once, packets of the ended sessions are dropped
            ProcessLifecycleEvents(dispatchPackets: false);

            for (int i = 0; i < remaining.Length; i++)
            {
                EndSession(remaining[i], dispatch: false);
            }

            _hiccup.Dispose();
        }

        private int _shutdownRaised;

        public void Dispose()
        {
            _sessionManagerDisposed = true;
            RaiseShutdown();
        }

        #endregion

        #region Helpers

        private void ReturnPacketBuffer(InboundPacket packet)
        {
            if (packet.BackingBuffer == null)
                return;

            if (packet.UsesRecoveryPool)
                _recoveryPacketPool.Return(packet.BackingBuffer);
            else
                _privatePool.Return(packet.BackingBuffer);
        }

        public int GetActiveSessionCount()
        {
            var arr = Volatile.Read(ref _activeSessions);
            return Math.Max(_sessions.Count, arr != null ? arr.Length : 0);
        }

        public int GetSessionIds(Span<ushort> destination)
        {
            int written = 0;
            int maxCapacity = destination.Length;

            LiminalSession[] activeSessions = Volatile.Read(ref _activeSessions);
            for (int i = 0; i < activeSessions.Length; i++)
            {
                if (written >= maxCapacity)
                {
                    LiminalLogger.LogWarning(
                        $"[SessionManager] Buffer capacity ({maxCapacity}) reached. Truncating session ID copy.");

                    break;
                }

                destination[written++] = activeSessions[i].Id;
            }

            return written;
        }

        #endregion

        #region Hiccup Recovery

        private int GetInboundStageCapacity(LiminalSession session) =>
            session.InboundRecoveryActive
                ? _hiccup.RecoverySize
                : _config.MaxPacketSizePerBatch;

        private int GetInboundPacketLimit(LiminalSession session) =>
            session.InboundRecoveryActive
                ? _hiccup.RecoveryPacketCount
                : _config.MaxPacketCount;

        private int GetOutboundBufferCapacity(LiminalSession session) =>
            session.OutboundRecoveryActive
                ? _hiccup.RecoverySize
                : _config.MaxPacketSizePerBatch;

        private bool EnsureInboundRecoveryLocked(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason, ref ServerStarvationEvent? starvation)
        {
            var result = _hiccup.TryEnableInbound(session, clientId, requiredBytes, now, out failureReason, out var captured);
            if (captured.HasValue) starvation = captured;
            return result == HiccupEnableResult.Success;
        }

        private bool EnsureOutboundRecoveryLocked(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason, ref ServerStarvationEvent? starvation)
        {
            var result = _hiccup.TryEnableOutbound(session, clientId, requiredBytes, now, out failureReason, out var captured);
            if (captured.HasValue) starvation = captured;
            return result == HiccupEnableResult.Success;
        }

        #endregion

        #region ISessionTelemetryProvider

        public GlobalSessionTelemetrySnapshot GetGlobalSnapshot()
        {
            return new GlobalSessionTelemetrySnapshot(
                activeSessionCount: _sessions.Count,
                loopbackQueueCount: _loopbackQueue.Count,
                totalPacketsInbound: Volatile.Read(ref _totalPacketsInbound),
                totalPacketsOutbound: Volatile.Read(ref _totalPacketsOutbound));
        }

        public void InitializeConfig(LiminalTelemetryConfig config)
        {
            _telemetryConfig = config;
        }

        #endregion
    }
}