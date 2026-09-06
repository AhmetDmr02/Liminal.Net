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
    public class LiminalSessionManager : IDisposable, ISessionTelemetryProvider
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();

        private readonly ConcurrentQueue<(ushort SenderId, InboundPacket Packet)> _loopbackQueue = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly LiminalPacketFramerPipeline _pipeline;
        private readonly ArrayPool<byte> _privatePool;
        private volatile bool _sessionManagerDisposed;

        private long _totalPacketsInbound;
        private long _totalPacketsOutbound;

        private volatile LiminalTelemetryConfig _telemetryConfig;

        private readonly LiminalPacketInterpreter _interpreter;

        private const bool DisconnectClientOnInboundOverflow = true;

        public LiminalSessionManager(ILiminalTransport transport, LiminalPacketInterpreter interpreter, LiminalTransportConfig config, LiminalPacketFramerPipeline pipeline)
        {
            _transport = transport;
            _config = config;
            _pipeline = pipeline;
            _interpreter = interpreter;

            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);
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

            try
            {
                lock (session.ReceiveLock)
                {
                    if (session.IsDisposed()) return;

                    var processedBatch = _pipeline.ExecuteInboundBatch(session, transportData);
                    if (processedBatch.IsEmpty) return;

                    int offset = 0;
                    while (offset + 4 <= processedBatch.Length)
                    {
                        if (session.InboundQueue.Count >= _config.MaxPacketCount)
                        {
                            LiminalLogger.LogWarning($"[Security] Client {ownerId} exceeded InboundQueue capacity. Dropping connection.");

                            if (_transport.IsServer)
                            {
                                _transport.Kick(ownerId);
                            }
                            else
                            {
                                if (DisconnectClientOnInboundOverflow)
                                    _transport.Disconnect();
                            }
                            return;
                        }

                        int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedBatch.Slice(offset, 4));
                        if (totalLen <= 0 || offset + 4 + totalLen > processedBatch.Length) break;

                        ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(processedBatch.Slice(offset + 4, 2));
                        int payloadLen = totalLen - 2;

                        byte[] rentedBuffer = _privatePool.Rent(payloadLen);
                        processedBatch.Slice(offset + 6, payloadLen).CopyTo(rentedBuffer);

                        session.InboundQueue.Enqueue(new InboundPacket(packetId, rentedBuffer, payloadLen));

                        if (countPackets)
                        {
                            inboundBatchCount++;
                        }

                        offset += 4 + totalLen;
                    }
                }
            }
            catch (Exception e)
            {
                LiminalLogger.LogError($"[SessionManager] Error processing inbound packet: {e}");

                if (_transport.IsServer)
                {
                    _transport.Kick(ownerId);
                }
                else
                {
                    _transport.Disconnect();
                }
            }
            finally
            {
                if (inboundBatchCount > 0)
                {
                    Interlocked.Add(ref _totalPacketsInbound, inboundBatchCount);
                }
            }
        }

        #endregion

        #region Write Path (Game Thread)

        public void BufferPacket(ushort targetId, ushort packetId, ReadOnlySpan<byte> payload)
        {
            if (_sessionManagerDisposed) return;

            TelemetryFlags activeFlags = _telemetryConfig?.Flags ?? TelemetryFlags.None;
            bool countPackets = (activeFlags & TelemetryFlags.PacketCounting) != 0;

            bool isHostMode = _transport.IsServer && _transport.IsClient;
            ushort localClientId = _transport.LocalClientId;

            bool isSelfSend = !isHostMode && (targetId == localClientId);
            bool isHostClientToServer = isHostMode && (targetId == ILiminalTransport.SERVER_ID);
            bool isHostServerToClient = isHostMode && (targetId == localClientId);

            if (isSelfSend || isHostClientToServer || isHostServerToClient)
            {
                if (_loopbackQueue.Count >= _config.MaxPacketCount)
                {
                    LiminalLogger.LogError("[SessionManager] Loopback queue overflow. Dropping packet.");
                    return;
                }

                byte[] rentedBuffer = _privatePool.Rent(payload.Length);
                payload.CopyTo(rentedBuffer);

                var packet = new InboundPacket(packetId, rentedBuffer, payload.Length);

                if (countPackets)
                {
                    Interlocked.Increment(ref _totalPacketsOutbound);
                }

                ushort senderId;
                if (isHostServerToClient)
                {
                    senderId = ILiminalTransport.SERVER_ID;
                }
                else if (isHostClientToServer)
                {
                    senderId = localClientId;
                }
                else
                {
                    senderId = localClientId;
                }

                _loopbackQueue.Enqueue((senderId, packet));
                return;
            }

            if (!_sessions.TryGetValue(targetId, out var session))
            {
                LiminalLogger.LogWarning($"[SessionManager] Cannot route packet. Target {targetId} does not exist.");
                return;
            }

            int frameSize = 4 + 2 + payload.Length;

            lock (session.SendLock)
            {
                if (session.IsDisposed()) return;

                if (session.RawSendCursor + frameSize > session.RawSendBuffer.Memory.Length)
                {
                    LiminalLogger.LogError(
                        $"[Manager] FATAL: Outbound send buffer exceeded for Session {targetId} " +
                        $"({session.RawSendCursor + frameSize}b > {session.RawSendBuffer.Memory.Length}b). " +
                        $"Kicking client to prevent state desync. Increase MaxPacketSizePerBatch or throttle outgoing packets.");

                    session.RawSendCursor = 0;

                    if (_transport.IsServer)
                    {
                        _transport.Kick(targetId);
                    }
                    else
                    {
                        _transport.Disconnect();
                    }

                    return;
                }

                // [Len][ID][Payload]
                Span<byte> dest = session.RawSendBuffer.GetSpan().Slice(session.RawSendCursor);
                BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), payload.Length + 2);
                BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(4, 2), packetId);
                payload.CopyTo(dest.Slice(6));

                session.RawSendCursor += frameSize;

                if (countPackets)
                {
                    Interlocked.Increment(ref _totalPacketsOutbound);
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

            foreach (var session in _sessions.Values)
            {
                try
                {
                    FlushSession(session);
                }
                catch
                {
                    LiminalLogger.LogError($"[SessionManager] Error flushing session {session.Id}");
                    _transport.Kick(session.Id);

                    continue;
                }
            }
        }

        private void FlushSession(LiminalSession session)
        {
            lock (session.SendLock)
            {
                if (session.RawSendCursor == 0) return;
                if (session.IsDisposed()) return;

                // Pipeline handles the messy transform logic
                int bytesToSend = _pipeline.ExecuteOutboundBatch(session, session.RawSendCursor);
                session.RawSendCursor = 0;

                if (bytesToSend > session.SendBuffer.Memory.Length)
                {
                    LiminalLogger.LogError($"[SessionManager] Pipeline output ({bytesToSend}b) exceeds buffer size! Dropping packet.");
                    return;
                }

                if (bytesToSend > 0)
                {
                    _transport.SendReliable(session.SendBuffer.GetSpan().Slice(0, bytesToSend), session.Id);
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

            //VIRTUAL LOOPBACK Self Sends
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
                    _privatePool.Return(packet.BackingBuffer);
                }
            }

            foreach (var session in _sessions.Values)
            {
                if (session.IsDisposed()) continue;
                if (session.InboundQueue.IsEmpty) continue;

                while (session.InboundQueue.TryDequeue(out var packet))
                {
                    try
                    {
                        _interpreter.Dispatch(packet.PacketId, session.Id, packet.AsMemory());
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[SessionManager] Error in handler for ID {packet.PacketId}: {ex}");
                        continue;
                    }
                    finally
                    {
                        _privatePool.Return(packet.BackingBuffer);
                    }
                }
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
            if (!_sessionManagerDisposed) _sessions.TryAdd(id, new LiminalSession(id, _config.MaxPacketSizePerBatch));
        }

        private readonly ConcurrentQueue<ushort> _pendingDisconnects = new();
        private void HandleClientDisconnected(ushort id)
        {
            _pendingDisconnects.Enqueue(id);
        }

        private void ProcessPendingDisconnects()
        {
            while (_pendingDisconnects.TryDequeue(out var id))
            {
                if (_sessions.TryRemove(id, out var session))
                {
                    session.Dispose();

                    while (session.InboundQueue.TryDequeue(out var packet))
                    {
                        _privatePool.Return(packet.BackingBuffer);
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
            {
                _privatePool.Return(loopbackItem.Packet.BackingBuffer);
            }

            foreach (var session in _sessions.Values)
            {
                session.Dispose();

                while (session.InboundQueue.TryDequeue(out var packet))
                {
                    _privatePool.Return(packet.BackingBuffer);
                }
            }

            _sessions.Clear();
            _pendingDisconnects.Clear();
        }

        private bool disposed = false;
        public void Dispose()
        {
            if (_sessionManagerDisposed) return;
            _sessionManagerDisposed = true;
        }
        #endregion

        #region Helpers
        public int GetActiveSessionCount() => _sessions.Count;


        /// <summary>
        /// Copies active session IDs into the destination span up to its capacity.
        /// </summary>
        /// <param name="destination">Span to receive session IDs.</param>
        /// <returns>The actual number of Ids written into destination.</returns>
        public int GetSessionIds(Span<ushort> destination)
        {
            int written = 0;
            int maxCapacity = destination.Length;

            foreach (var session in _sessions.Values)
            {
                if (written >= maxCapacity)
                {
                    // Destination buffer was too small to hold all concurrent sessions
                    LiminalLogger.LogWarning(
                        $"[SessionManager] Buffer capacity ({maxCapacity}) reached. Truncating session ID copy.");
                    break;
                }

                destination[written++] = session.Id;
            }

            return written;
        }
        #endregion

        #region ISessionTelemetryProvider

        public GlobalSessionTelemetrySnapshot GetGlobalSnapshot()
        {
            return new GlobalSessionTelemetrySnapshot(
                activeSessionCount: _sessions.Count,
                loopbackQueueCount: _loopbackQueue.Count,
                totalPacketsInbound: Volatile.Read(ref _totalPacketsInbound),
                totalPacketsOutbound: Volatile.Read(ref _totalPacketsOutbound)
            );
        }

        public void InitializeConfig(LiminalTelemetryConfig config)
        {
            _telemetryConfig = config;
        }
        #endregion
    }
}