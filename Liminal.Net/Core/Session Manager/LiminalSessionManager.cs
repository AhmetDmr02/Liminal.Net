using Liminal.Net.Interfaces;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
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

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;
            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnLocalClientConnected += HandleLocalConnection;
            _transport.OnClientKicked += HandleClientDisconnected;
            _interpreter.OnSendRequest += BufferPacket;
        }

        #region Receive Path (Background Threads)

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);
        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);

        private void ProcessIncoming(ushort ownerId, ReadOnlySpan<byte> transportData)
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

                    if (transportData.Length > _config.MaxPacketSizePerBatch || session.InboundPacketCount >= _config.MaxPacketCount)
                    {
                        if (!EnsureInboundRecoveryLocked(session, ownerId, transportData.Length, Stopwatch.GetTimestamp(), out string failureReason))
                        {
                            shouldKick = true;
                            kickReason = failureReason;
                            return;
                        }
                    }

                    if (transportData.Length > GetInboundStageCapacity(session))
                    {
                        shouldKick = true;
                        kickReason = "Inbound payload exceeded the session recovery ceiling.";
                        return;
                    }

                    var processedBatch = _pipeline.ExecuteInboundBatch(session, transportData);
                    if (processedBatch.IsEmpty) return;

                    if (processedBatch.Length > GetInboundStageCapacity(session))
                    {
                        shouldKick = true;
                        kickReason = "Inbound transformed batch exceeded the session recovery ceiling.";
                        return;
                    }

                    int offset = 0;
                    while (offset + 4 <= processedBatch.Length)
                    {
                        int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedBatch.Slice(offset, 4));
                        if (totalLen < 2 || offset + 4 + totalLen > processedBatch.Length)
                            break;

                        if (session.InboundPacketCount >= GetInboundPacketLimit(session))
                        {
                            if (!EnsureInboundRecoveryLocked(session, ownerId, processedBatch.Length, Stopwatch.GetTimestamp(), out string failureReason))
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

                        if (!session.TryReserveInboundPacket(GetInboundPacketLimit(session)))
                        {
                            shouldKick = true;
                            kickReason = "Inbound packet-count reservation failed at the active ceiling.";
                            return;
                        }

                        ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(processedBatch.Slice(offset + 4, 2));
                        int payloadLen = totalLen - 2;

                        ArrayPool<byte> payloadPool = payloadLen > _config.MaxPacketSizePerBatch
                            ? _recoveryPacketPool
                            : _privatePool;

                        byte[] rentedBuffer = null;
                        try
                        {
                            rentedBuffer = payloadPool.Rent(payloadLen);
                            processedBatch.Slice(offset + 6, payloadLen).CopyTo(rentedBuffer);
                            session.InboundQueue.Enqueue(new InboundPacket(packetId, rentedBuffer, payloadLen, payloadPool == _recoveryPacketPool));
                            rentedBuffer = null;
                        }
                        catch
                        {
                            session.ReleaseInboundPacket();
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

                        offset += 4 + totalLen;
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

        public event Action<ushort,Memory<byte>> OnPacketBuffered;
        public void BufferPacket(ushort senderId, ushort targetId, ushort packetId, ReadOnlySpan<byte> payload)
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
                    {
                        _privatePool.Return(rentedBuffer);
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

            try
            {
                lock (session.SendLock)
                {
                    if (session.IsDisposed()) return;

                    int activeCapacity = GetOutboundBufferCapacity(session);
                    if (frameSize > activeCapacity || session.RawSendCursor + frameSize > activeCapacity)
                    {
                        int required = Math.Max(frameSize, session.RawSendCursor + frameSize);
                        if (!EnsureOutboundRecoveryLocked(session, targetId, required, Stopwatch.GetTimestamp(), out string failureReason))
                        {
                            session.RawSendCursor = 0;
                            shouldKick = true;
                            kickReason = failureReason;
                            return;
                        }

                        activeCapacity = GetOutboundBufferCapacity(session);
                        if (frameSize > activeCapacity || session.RawSendCursor + frameSize > activeCapacity)
                        {
                            session.RawSendCursor = 0;
                            shouldKick = true;
                            kickReason = "Outbound recovery ceiling exceeded.";
                            return;
                        }
                    }

                    Span<byte> dest = session.ActiveRawSendBuffer.GetSpan().Slice(session.RawSendCursor);
                    BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), payload.Length + 2);
                    BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(4, 2), packetId);
                    payload.CopyTo(dest.Slice(6));

                    session.RawSendCursor += frameSize;

                    if (_telemetryConfig.EnablePacketCounting)
                    {
                        Memory<byte> framedMemory = session.ActiveRawSendBuffer.Memory.Slice(session.RawSendCursor - frameSize, frameSize);
                        OnPacketBuffered.Invoke(packetId, framedMemory);
                    }

                    if (session.OutboundRecoveryActive)
                        session.Recovery.OutboundLimiter.LastActivityTimestamp = Stopwatch.GetTimestamp();

                    if (countPackets)
                        Interlocked.Increment(ref _totalPacketsOutbound);
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
            int bytesToSend = 0;

            try
            {
                lock (session.SendLock)
                {
                    if (session.RawSendCursor == 0 || session.IsDisposed()) return;

                    int activeCapacity = GetOutboundBufferCapacity(session);
                    if (session.RawSendCursor > activeCapacity)
                    {
                        if (!EnsureOutboundRecoveryLocked(session, session.Id, session.RawSendCursor, Stopwatch.GetTimestamp(), out string failureReason))
                        {
                            session.RawSendCursor = 0;
                            shouldKick = true;
                            kickReason = failureReason;
                            return;
                        }

                        activeCapacity = GetOutboundBufferCapacity(session);
                        if (session.RawSendCursor > activeCapacity)
                        {
                            session.RawSendCursor = 0;
                            shouldKick = true;
                            kickReason = "Outbound raw buffer exceeded the recovery ceiling.";
                            return;
                        }
                    }

                    bytesToSend = _pipeline.ExecuteOutboundBatch(session, session.RawSendCursor);
                    session.RawSendCursor = 0;

                    if (bytesToSend > GetOutboundBufferCapacity(session))
                    {
                        shouldKick = true;
                        kickReason = "Outbound transformed batch exceeded the recovery ceiling.";
                        bytesToSend = 0;
                        return;
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

            if (bytesToSend > 0)
            {
                _transport.SendReliable(session.ActiveSendBuffer.GetSpan().Slice(0, bytesToSend), session.Id);
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
                if (!session.InboundQueue.IsEmpty)
                {
                    while (session.InboundQueue.TryDequeue(out var packet))
                    {
                        session.ReleaseInboundPacket();
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

                    while (session.InboundQueue.TryDequeue(out var packet))
                    {
                        session.ReleaseInboundPacket();
                        ReturnPacketBuffer(packet);
                    }
                }
            }
        }

        private void RaiseShutdown()
        {
            if (disposed) return;
            disposed = true;

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

                while (session.InboundQueue.TryDequeue(out var packet))
                {
                    session.ReleaseInboundPacket();
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
