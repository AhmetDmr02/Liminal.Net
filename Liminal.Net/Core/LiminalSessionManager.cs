using Liminal.Net.Interfaces;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Liminal.Net.Core
{
    public class LiminalSessionManager : IDisposable
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();
        private readonly ConcurrentQueue<(ushort SenderId, InboundPacket Packet)> _loopbackQueue = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly LiminalPacketFramerPipeline _pipeline;
        private readonly ArrayPool<byte> _privatePool;
        private volatile bool _sessionManagerDisposed;

        private readonly LiminalPacketInterpreter _interpreter;

        public LiminalSessionManager(ILiminalTransport transport, LiminalPacketInterpreter interpreter, LiminalTransportConfig config, LiminalPacketFramerPipeline pipeline)
        {
            _transport = transport;
            _config = config;
            _pipeline = pipeline;
            _interpreter = interpreter;
            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;
            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnLocalClientConnected += HandleLocalConnection;
            _transport.OnClientKicked += HandleClientDisconnected;
            _interpreter.OnSendRequest += BufferPacket;
            _transport.OnShutdown += Dispose;
        }

        #region Receive Path (Background Threads)

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);
        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);

        private void ProcessIncoming(ushort ownerId, ReadOnlySpan<byte> transportData)
        {
            if (_sessionManagerDisposed || !_sessions.TryGetValue(ownerId, out var session)) return;

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
                        int totalLen = BinaryPrimitives.ReadInt32LittleEndian(processedBatch.Slice(offset, 4));
                        if (totalLen <= 0 || offset + 4 + totalLen > processedBatch.Length) break;

                        ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(processedBatch.Slice(offset + 4, 2));
                        int payloadLen = totalLen - 2;

                        byte[] rentedBuffer = _privatePool.Rent(payloadLen);
                        processedBatch.Slice(offset + 6, payloadLen).CopyTo(rentedBuffer);

                        session.InboundQueue.Enqueue(new InboundPacket(packetId, rentedBuffer, payloadLen));

                        offset += 4 + totalLen;
                    }
                }
            }
            catch (Exception e)
            {
                LiminalLogger.LogError($"[SessionManager] Error processing inbound packet: {e}");
                _transport.Kick(ownerId);
            }
        }

        #endregion

        #region Write Path (Game Thread)

        public void BufferPacket(ushort targetId, ushort packetId, ReadOnlySpan<byte> payload)
        {
            if (_sessionManagerDisposed) return;

            if (!_sessions.TryGetValue(targetId, out var session))
            {
                if (targetId == _transport.LocalClientId)
                {
                    byte[] rentedBuffer = _privatePool.Rent(payload.Length);
                    payload.CopyTo(rentedBuffer);

                    var packet = new InboundPacket(packetId, rentedBuffer, payload.Length);

                    _loopbackQueue.Enqueue((targetId, packet));
                    return;
                }

                LiminalLogger.LogWarning($"[SessionManager] Cannot route packet. Target {targetId} does not exist.");
                return;
            }

            int frameSize = 4 + 2 + payload.Length;

            lock (session.SendLock)
            {
                if (session.IsDisposed()) return;

                if (session.RawSendCursor + frameSize > session.RawSendBuffer.Memory.Length)
                {
                    LiminalLogger.LogError($"[Manager] Packet dropped for {targetId}. Send Buffer Full.");
                    return;
                }

                // [Len][ID][Payload]
                Span<byte> dest = session.RawSendBuffer.GetSpan().Slice(session.RawSendCursor);
                BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), payload.Length + 2);
                BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(4, 2), packetId);
                payload.CopyTo(dest.Slice(6));

                session.RawSendCursor += frameSize;
            }
        }

        public void Flush()
        {
            foreach (var session in _sessions.Values) FlushSession(session);
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
            if (_sessionManagerDisposed) return;

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
                    finally
                    {
                        _privatePool.Return(packet.BackingBuffer);
                    }
                }
            }
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

        private void HandleClientDisconnected(ushort id)
        {
            if (_sessions.TryRemove(id, out var session)) session.Dispose();
        }

        public void Dispose()
        {
            if (_sessionManagerDisposed) return;
            _sessionManagerDisposed = true;

            _transport.OnMessageReceivedReliable -= HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable -= HandleUnreliableMessage;
            _transport.OnClientConnected -= HandleClientConnected;
            _transport.OnClientDisconnected -= HandleClientDisconnected;
            _transport.OnLocalClientConnected -= HandleLocalConnection;
            _transport.OnClientKicked -= HandleClientDisconnected;
            _interpreter.OnSendRequest -= BufferPacket;
            _transport.OnShutdown -= Dispose;

            while (_loopbackQueue.TryDequeue(out var loopbackItem))
            {
                _privatePool.Return(loopbackItem.Packet.BackingBuffer);
            }

            foreach (var session in _sessions.Values)
            {
                while (session.InboundQueue.TryDequeue(out var packet))
                    _privatePool.Return(packet.BackingBuffer);
            }

            foreach (var s in _sessions.Values) s.Dispose();
            _sessions.Clear();
        }
        #endregion

        #region Test Helpers
        public int GetActiveSessionCount() => _sessions.Count;
        #endregion
    }
}