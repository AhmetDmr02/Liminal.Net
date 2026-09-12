using Liminal.Net.Interfaces;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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

        private readonly ConcurrentQueue<(ushort SenderId, InboundPacket Packet)> _loopbackQueue = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly LiminalPacketFramerPipeline _pipeline;
        private readonly ArrayPool<byte> _privatePool;
        private readonly ArrayPool<byte> _recoveryPacketPool;

        private readonly LiminalHiccupController _hiccup;

        private volatile bool _sessionManagerDisposed;

        private long _totalPacketsInbound;
        private long _totalPacketsOutbound;
        private volatile LiminalTelemetryConfig _telemetryConfig;

        private readonly LiminalPacketInterpreter _interpreter;

        private readonly LiminalPacketFragmentor _fragmentor;

        public event Action<ServerStarvationEvent> OnServerStarvation;

        public LiminalSessionManager(ILiminalTransport transport, LiminalPacketInterpreter interpreter, LiminalTransportConfig config, LiminalPacketFramerPipeline pipeline)
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

            _loopbackQueue = new ConcurrentQueue<(ushort SenderId, InboundPacket Packet)>();

            _fragmentor = new LiminalPacketFragmentor(_transport, _config);

            _fragmentor.OnMessageReassembled += HandleReliableMessage;

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;

            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnLocalClientConnected += HandleLocalConnection;
            _transport.OnClientKicked += HandleClientDisconnected;
            _interpreter.OnSendRequest += BufferPacket;
        }

        #region Receive Path (Background Threads)

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data, DeliveryMethod.Reliable);
        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data, DeliveryMethod.Unreliable);

        private void ProcessIncoming(ushort ownerId, ReadOnlySpan<byte> transportData, DeliveryMethod deliveryMethod)
        {
            if (_sessionManagerDisposed || !_sessions.TryGetValue(ownerId, out var session)) return;

            TelemetryFlags activeFlags = _telemetryConfig?.Flags ?? TelemetryFlags.None;
            bool countPackets = (activeFlags & TelemetryFlags.PacketCounting) != 0;
            int inboundBatchCount = 0;

            bool shouldKick = false;
            string kickReason = null;

            try
            {
                lock (session.ReceiveLock)
                {
                    if (session.IsDisposed()) return;

                    if (deliveryMethod == DeliveryMethod.Unreliable)
                    {
                        if (transportData.Length > _config.MaxPacketSizePerBatch || session.InboundPacketCount >= _config.MaxPacketCount)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Dropped inbound unreliable batch from client {ownerId}. Batch size or packet queue limit reached.");
                            return;
                        }

                        var processedBatch = _pipeline.ExecuteInboundBatch(session, transportData);
                        if (processedBatch.IsEmpty) return;

                        if (processedBatch.Length > _config.MaxPacketSizePerBatch)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Dropped transformed unreliable batch ({processedBatch.Length}b) from client {ownerId}. Exceeded batch size.");
                            return;
                        }

                        int offset = 0;
                        while (offset + 4 <= processedBatch.Length)
                        {
                            int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedBatch.Slice(offset, 4));
                            if (totalLen < 2 || offset + 4 + totalLen > processedBatch.Length) break;

                            if (session.InboundPacketCount >= _config.MaxPacketCount ||
                                !session.TryReserveInboundPacketUnreliable(_config.MaxPacketCount))
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
                                session.InboundQueueUnreliable.Enqueue(new InboundPacket(packetId, rentedBuffer, payloadLen, usesRecoveryPool: false));
                                rentedBuffer = null;
                            }
                            catch
                            {
                                session.ReleaseInboundPacketUnreliable();
                                throw;
                            }
                            finally
                            {
                                if (rentedBuffer != null) _privatePool.Return(rentedBuffer);
                            }

                            if (countPackets) inboundBatchCount++;
                            offset += 4 + totalLen;
                        }

                        return;
                    }

                    //RELIABLE RECEIVE PATH
                    bool exceedsNormalThreshold = transportData.Length > _config.MaxPacketSizePerBatch || session.InboundPacketCount >= _config.MaxPacketCount;

                    if (exceedsNormalThreshold)
                    {
                        // Drop queued unreliable packets only if it prevents entering Hiccup
                        bool canAvoidRecovery = !session.InboundRecoveryActive &&
                                                session.InboundPacketCountUnreliable > 0 &&
                                                transportData.Length <= _config.MaxPacketSizePerBatch &&
                                                session.InboundPacketCountReliable < _config.MaxPacketCount;

                        if (canAvoidRecovery)
                        {
                            int dropped = session.DropQueuedUnreliableInbound(ReturnPacketBuffer);
                            LiminalLogger.LogWarning($"[SessionManager] Dropped {dropped} queued unreliable inbound packets for client {ownerId} to prevent Hiccup recovery.");
                        }

                        if (transportData.Length > _config.MaxPacketSizePerBatch || session.InboundPacketCount >= _config.MaxPacketCount)
                        {
                            if (!EnsureInboundRecoveryLocked(session, ownerId, transportData.Length, Stopwatch.GetTimestamp(), out string failureReason))
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
                    if (processedReliableBatch.IsEmpty) return;

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
                        if (totalLen < 2 || relOffset + 4 + totalLen > processedReliableBatch.Length) break;

                        if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                        {
                            // Try dropping queued unreliable before expanding recovery ceiling
                            if (!session.InboundRecoveryActive && session.InboundPacketCountUnreliable > 0 &&
                                session.InboundPacketCountReliable < _config.MaxPacketCount)
                            {
                                int dropped = session.DropQueuedUnreliableInbound(ReturnPacketBuffer);
                                LiminalLogger.LogWarning($"[SessionManager] Dropped {dropped} queued unreliable inbound packets for client {ownerId} to fit reliable packet.");
                            }

                            if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                            {
                                if (!EnsureInboundRecoveryLocked(session, ownerId, processedReliableBatch.Length, Stopwatch.GetTimestamp(), out string failureReason))
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

                        ArrayPool<byte> payloadPool = payloadLen > _config.MaxPacketSizePerBatch
                            ? _recoveryPacketPool
                            : _privatePool;

                        byte[] rentedBuffer = null;
                        try
                        {
                            rentedBuffer = payloadPool.Rent(payloadLen);
                            processedReliableBatch.Slice(relOffset + 6, payloadLen).CopyTo(rentedBuffer);
                            session.InboundQueueReliable.Enqueue(new InboundPacket(packetId, rentedBuffer, payloadLen, payloadPool == _recoveryPacketPool));
                            rentedBuffer = null;
                        }
                        catch
                        {
                            session.ReleaseInboundPacketReliable();
                            throw;
                        }
                        finally
                        {
                            if (rentedBuffer != null) payloadPool.Return(rentedBuffer);
                        }

                        if (session.InboundRecoveryActive)
                            session.Recovery.InboundLimiter.LastActivityTimestamp = Stopwatch.GetTimestamp();

                        if (countPackets) inboundBatchCount++;
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

                if (shouldKick)
                {
                    _hiccup.Kick(ownerId, LiminalRecoveryDirection.Inbound, kickReason);
                }
            }
        }

        #endregion

        #region Write Path (Game Thread)

        public event Action<ushort, Memory<byte>> OnPacketBuffered;

        public void BufferPacket(ushort senderId, ushort targetId, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable)
        {
            if (_sessionManagerDisposed) return;

            TelemetryFlags activeFlags = _telemetryConfig?.Flags ?? TelemetryFlags.None;
            bool countPackets = (activeFlags & TelemetryFlags.PacketCounting) != 0;

            bool isHostMode = _transport.IsServer && _transport.IsClient;
            ushort localClientId = _transport.LocalClientId;
            bool isLocalDestination = (targetId == localClientId) || (isHostMode && targetId == ILiminalTransport.SERVER_ID);

            if (isLocalDestination)
            {
                if (_loopbackQueue.Count >= _config.MaxPacketCount)
                {
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
                        _privatePool.Return(rentedBuffer);
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

            try
            {
                Memory<byte> bufferedMemory = default;
                bool raiseBufferedEvent = false;


                lock (session.SendLock)
                {
                    if (session.IsDisposed()) return;

                    if (deliveryMethod == DeliveryMethod.Unreliable)
                    {
                        int unreliableCapacity = session.RawSendBufferUnreliable.GetSpan().Length;
                        int totalCombined = session.RawSendCursorReliable + session.RawSendCursorUnreliable + frameSize;

                        // Drop if unreliable buffer is individually exceeded or if it pushes the combined batch over normal capacity
                        if (session.RawSendCursorUnreliable + frameSize > unreliableCapacity || totalCombined > _config.MaxPacketSizePerBatch)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Dropped unreliable packet {packetId} ({frameSize}b) for client {targetId}. Outbound capacity limit reached.");
                            return;
                        }

                        Span<byte> dest = session.RawSendBufferUnreliable.GetSpan().Slice(session.RawSendCursorUnreliable);
                        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), payload.Length + 2);
                        BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(4, 2), packetId);
                        payload.CopyTo(dest.Slice(6));

                        session.RawSendCursorUnreliable += frameSize;

                        if (_telemetryConfig != null && _telemetryConfig.EnablePacketCounting)
                        {
                            bufferedMemory = session.RawSendBufferUnreliable.Memory.Slice(session.RawSendCursorUnreliable - frameSize, frameSize);

                            raiseBufferedEvent = true;
                        }

                        if (countPackets)
                            Interlocked.Increment(ref _totalPacketsOutbound);

                        return;
                    }

                    //RELIABLE DELIVERY PATH
                    int activeCapacity = GetOutboundBufferCapacity(session);
                    int totalBuffered = session.RawSendCursorReliable + session.RawSendCursorUnreliable;
                    int reliableRequired = session.RawSendCursorReliable + frameSize;

                    bool exceedsThreshold = frameSize > activeCapacity ||
                                            (totalBuffered + frameSize) > activeCapacity ||
                                            totalBuffered >= _config.MaxPacketSizePerBatch;

                    if (exceedsThreshold)
                    {
                        //Only drop unreliable if recovery is NOT already active
                        //AND dropping it actually keeps the reliable batch within normal capacity.
                        bool canAvoidRecoveryByDroppingUnreliable = !session.OutboundRecoveryActive && session.RawSendCursorUnreliable > 0 && reliableRequired <= _config.MaxPacketSizePerBatch;

                        if (canAvoidRecoveryByDroppingUnreliable)
                        {
                            int droppedBytes = session.RawSendCursorUnreliable;
                            session.RawSendCursorUnreliable = 0;

                            LiminalLogger.LogWarning(
                                $"[SessionManager] Dropped {droppedBytes}b of queued unreliable packets for client {targetId} to fit reliable packet {packetId} without triggering Hiccup recovery.");
                        }
                        else
                        {
                            //Dropping unreliable won't prevent Hiccup recovery, so keep the unreliable queue intact.
                            int required = Math.Max(frameSize, reliableRequired);
                            if (!EnsureOutboundRecoveryLocked(session, targetId, required, Stopwatch.GetTimestamp(), out string failureReason))
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

                    Span<byte> relDest = session.ActiveRawSendBufferReliable.GetSpan().Slice(session.RawSendCursorReliable);
                    BinaryPrimitives.WriteInt32LittleEndian(relDest.Slice(0, 4), payload.Length + 2);
                    BinaryPrimitives.WriteUInt16LittleEndian(relDest.Slice(4, 2), packetId);
                    payload.CopyTo(relDest.Slice(6));

                    session.RawSendCursorReliable += frameSize;

                    if (_telemetryConfig != null && _telemetryConfig.EnablePacketCounting)
                    {
                        bufferedMemory = session.ActiveRawSendBufferReliable.Memory.Slice(session.RawSendCursorReliable - frameSize, frameSize);
                        raiseBufferedEvent = true;
                    }

                    if (session.OutboundRecoveryActive)
                        session.Recovery.OutboundLimiter.LastActivityTimestamp = Stopwatch.GetTimestamp();

                    if (countPackets)
                        Interlocked.Increment(ref _totalPacketsOutbound);
                }


                if (raiseBufferedEvent)
                {
                    OnPacketBuffered?.Invoke(packetId, bufferedMemory);
                }
            }
            finally
            {
                if (shouldKick)
                {
                    _hiccup.Kick(targetId, LiminalRecoveryDirection.Outbound, kickReason);
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

            foreach (var session in _sessions.Values)
            {
                try
                {
                    FlushSession(session);
                    _hiccup.ReleaseOutboundWhenSafe(session, now);
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[SessionManager] Error flushing session {session.Id}: {ex}");
                    _hiccup.Kick(session.Id, LiminalRecoveryDirection.Outbound, "Flush pipeline failure.");
                }
            }
        }

        private void FlushSession(LiminalSession session)
        {
            bool shouldKick = false;
            string kickReason = null;

            try
            {
                lock (session.SendLock)
                {
                    if ((session.RawSendCursorReliable == 0 && session.RawSendCursorUnreliable == 0) || session.IsDisposed())
                        return;

                    if (session.RawSendCursorUnreliable > 0)
                    {
                        int unBytesToSend = _pipeline.ExecuteOutboundBatch(session, session.RawSendCursorUnreliable, session.RawSendBufferUnreliable);
                        session.RawSendCursorUnreliable = 0;

                        if (unBytesToSend > session.RawSendBufferUnreliable.GetSpan().Length)
                        {
                            LiminalLogger.LogWarning($"[SessionManager] Transformed unreliable batch exceeded normal capacity for {session.Id}. Dropping.");
                            unBytesToSend = 0;
                        }

                        if (unBytesToSend > 0)
                        {
                            _fragmentor.Send(session.ActiveSendBuffer.GetSpan().Slice(0, unBytesToSend), session.Id, TransportFlags.Unreliable);
                        }
                    }

                    if (session.RawSendCursorReliable > 0)
                    {
                        int relBytesToSend = _pipeline.ExecuteOutboundBatch(session, session.RawSendCursorReliable,session.ActiveRawSendBufferReliable);
                        session.RawSendCursorReliable = 0;

                        if (relBytesToSend > GetOutboundBufferCapacity(session))
                        {
                            shouldKick = true;
                            kickReason = "Outbound transformed reliable batch exceeded the recovery ceiling.";
                            return;
                        }

                        if (relBytesToSend > 0)
                        {
                            _fragmentor.Send(session.ActiveSendBuffer.GetSpan().Slice(0, relBytesToSend), session.Id, TransportFlags.Reliable);
                        }
                    }
                }
            }
            finally
            {
                if (shouldKick)
                {
                    _hiccup.Kick(session.Id, LiminalRecoveryDirection.Outbound, kickReason);
                }
            }
        }

        #endregion

        #region Polling Logic (Game Thread)
        public void Poll()
        {
            if (_sessionManagerDisposed)
            {
                RaiseShutdown();
                return;
            }

            while (!_loopbackQueue.IsEmpty && _loopbackQueue.TryDequeue(out var loopbackItem))
            {
                var senderId = loopbackItem.SenderId;
                var packet = loopbackItem.Packet;

                try
                {
                    _interpreter.Dispatch(packet.PacketId, senderId, packet.AsMemory());
                }
                finally
                {
                    ReturnPacketBuffer(packet);
                }
            }

            long now = Stopwatch.GetTimestamp();

            foreach (var session in _sessions.Values)
            {
                if (session.IsDisposed()) continue;

                while (session.InboundQueueUnreliable.TryDequeue(out var packet))
                {
                    session.ReleaseInboundPacketUnreliable();
                    try
                    {
                        _interpreter.Dispatch(packet.PacketId, session.Id, packet.AsMemory());
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[SessionManager] Error in handler for ID {packet.PacketId}: {ex}");
                    }
                    finally
                    {
                        ReturnPacketBuffer(packet);
                    }
                }

                while (session.InboundQueueReliable.TryDequeue(out var packet))
                {
                    session.ReleaseInboundPacketReliable();
                    try
                    {
                        _interpreter.Dispatch(packet.PacketId, session.Id, packet.AsMemory());
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[SessionManager] Error in handler for ID {packet.PacketId}: {ex}");
                    }
                    finally
                    {
                        ReturnPacketBuffer(packet);
                    }
                }

                _hiccup.ReleaseInboundWhenSafe(session, now);
            }

            ProcessPendingDisconnects();
        }

        #endregion

        #region Lifecycle Handlers (Standard)
        private void HandleLocalConnection(ushort clientId)
        {
            if (!_sessionManagerDisposed)
            {
                _sessions.TryAdd(ILiminalTransport.SERVER_ID, new LiminalSession(ILiminalTransport.SERVER_ID, _config.MaxPacketSizePerBatch));
                LiminalLogger.Log($"[SessionManager] Created session for Server (ID: {ILiminalTransport.SERVER_ID})");
            }
        }

        private void HandleClientConnected(ushort id)
        {
            if (!_sessionManagerDisposed)
                _sessions.TryAdd(id, new LiminalSession(id, _config.MaxPacketSizePerBatch));
        }

        private readonly ConcurrentQueue<ushort> _pendingDisconnects = new();
        private void HandleClientDisconnected(ushort id) => _pendingDisconnects.Enqueue(id);

        private void ReleaseRecoveryForSession(LiminalSession session) =>
            _hiccup.ReleaseForSession(session);

        private void ProcessPendingDisconnects()
        {
            while (_pendingDisconnects.TryDequeue(out var id))
            {
                if (_sessions.TryRemove(id, out var session))
                {
                    ReleaseRecoveryForSession(session);
                    session.Dispose();

                    while (session.InboundQueueUnreliable.TryDequeue(out var packet))
                    {
                        session.ReleaseInboundPacketUnreliable();
                        ReturnPacketBuffer(packet);
                    }

                    while (session.InboundQueueReliable.TryDequeue(out var packet))
                    {
                        session.ReleaseInboundPacketReliable();
                        ReturnPacketBuffer(packet);
                    }
                }
            }
        }

        private void RaiseShutdown()
        {
            if (disposed) return;
            disposed = true;

            _fragmentor.OnMessageReassembled -= HandleReliableMessage;
            _fragmentor.Dispose();

            _transport.OnMessageReceivedReliable -= HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable -= HandleUnreliableMessage;
            _transport.OnClientConnected -= HandleClientConnected;
            _transport.OnClientDisconnected -= HandleClientDisconnected;
            _transport.OnLocalClientConnected -= HandleLocalConnection;
            _transport.OnClientKicked -= HandleClientDisconnected;
            _interpreter.OnSendRequest -= BufferPacket;

            while (_loopbackQueue.TryDequeue(out var loopbackItem))
                ReturnPacketBuffer(loopbackItem.Packet);

            foreach (var session in _sessions.Values)
            {
                ReleaseRecoveryForSession(session);
                session.Dispose();

                while (session.InboundQueueUnreliable.TryDequeue(out var packet))
                {
                    session.ReleaseInboundPacketUnreliable();
                    ReturnPacketBuffer(packet);
                }

                while (session.InboundQueueReliable.TryDequeue(out var packet))
                {
                    session.ReleaseInboundPacketReliable();
                    ReturnPacketBuffer(packet);
                }
            }

            _sessions.Clear();
            _pendingDisconnects.Clear();

            _hiccup.Dispose();
        }

        private bool disposed = false;
        public void Dispose()
        {
            if (_sessionManagerDisposed) return;
            _sessionManagerDisposed = true;
        }
        #endregion

        #region Helpers
        private void ReturnPacketBuffer(InboundPacket packet)
        {
            if (packet.BackingBuffer == null) return;

            if (packet.UsesRecoveryPool)
                _recoveryPacketPool.Return(packet.BackingBuffer);
            else
                _privatePool.Return(packet.BackingBuffer);
        }

        public int GetActiveSessionCount() => _sessions.Count;

        public int GetSessionIds(Span<ushort> destination)
        {
            int written = 0;
            int maxCapacity = destination.Length;

            foreach (var session in _sessions.Values)
            {
                if (written >= maxCapacity)
                {
                    LiminalLogger.LogWarning(
                        $"[SessionManager] Buffer capacity ({maxCapacity}) reached. Truncating session ID copy.");
                    break;
                }

                destination[written++] = session.Id;
            }

            return written;
        }
        #endregion

        #region Hiccup Recovery

        private int GetInboundStageCapacity(LiminalSession session) =>
            session.InboundRecoveryActive ? _hiccup.RecoverySize : _config.MaxPacketSizePerBatch;

        private int GetInboundPacketLimit(LiminalSession session) =>
            session.InboundRecoveryActive ? _hiccup.RecoveryPacketCount : _config.MaxPacketCount;

        private int GetOutboundBufferCapacity(LiminalSession session) =>
            session.OutboundRecoveryActive ? _hiccup.RecoverySize : _config.MaxPacketSizePerBatch;

        private bool EnsureInboundRecoveryLocked(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason) =>
            _hiccup.TryEnableInbound(session, clientId, requiredBytes, now, out failureReason) == HiccupEnableResult.Success;

        private bool EnsureOutboundRecoveryLocked(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason) =>
            _hiccup.TryEnableOutbound(session, clientId, requiredBytes, now, out failureReason) == HiccupEnableResult.Success;

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
