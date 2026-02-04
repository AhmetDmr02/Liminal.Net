using System;
using System.Collections.Concurrent;
using System.Buffers;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;

namespace Liminal.Net.Core
{
    public class LiminalSessionManager : IDisposable
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();
        private readonly ConcurrentQueue<IncomingMessage> _incomingMessages = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly ArrayPool<byte> _privatePool;

        // stateless processors
        // private readonly LiminalPipeline _pipeline; 

        private volatile bool _disposed;

        public LiminalSessionManager(ILiminalTransport transport, LiminalTransportConfig config)
        {
            _transport = transport;
            _config = config;

            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;
            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnShutdown += Dispose;
        }

        #region Receive Path (Background Threads)

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id)
            => ProcessIncoming(id, data, true);

        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, ushort id)
            => ProcessIncoming(id, data, false);

        private void ProcessIncoming(ushort id, ReadOnlySpan<byte> data, bool reliable)
        {
            if (_disposed) return;
            if (!_sessions.TryGetValue(id, out var session)) return;

            unsafe
            {
                Span<byte> stagingA = session.StagingBufferA.GetSpan();
                data.CopyTo(stagingA);

                //encryptors/Decompressors in Ping-Pong mode.
                int processedLength = data.Length;

                EnqueueToGameThread(id, stagingA.Slice(0, processedLength), reliable);
            }
        }

        private void EnqueueToGameThread(ushort id, Span<byte> finalData, bool reliable)
        {
            byte[] managedBuffer = _privatePool.Rent(finalData.Length);

            try
            {
                finalData.CopyTo(managedBuffer);

                if (!_disposed)
                {
                    _incomingMessages.Enqueue(new IncomingMessage(id, managedBuffer, finalData.Length, reliable));
                }
                else
                {
                    _privatePool.Return(managedBuffer);
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[SessionManager] Enqueue failed for {id}: {ex.Message}");
                _privatePool.Return(managedBuffer);
            }
        }

        #endregion

        #region Write Path (Game Thread)

        public void Send(ushort clientId, ReadOnlySpan<byte> data, bool reliable)
        {
            if (_disposed || !_sessions.TryGetValue(clientId, out var session)) return;

            // Move to native SendBuffer for potential transformation
            Span<byte> sendSpan = session.SendBuffer.GetSpan();
            data.CopyTo(sendSpan);

            // Add encryption/compression here later
            int finalLength = data.Length;

            // Hand-off to Transport
            if (reliable)
                _transport.SendReliable(sendSpan.Slice(0, finalLength), clientId);
            else
                _transport.SendUnreliable(sendSpan.Slice(0, finalLength), clientId);
        }

        #endregion

        #region Polling Logic (Main Thread / Unity Update)

        public void Poll()
        {
            if (_disposed) return;

            while (_incomingMessages.TryDequeue(out var msg))
            {
                try
                {
                    if (!_sessions.TryGetValue(msg.ClientId, out var session)) continue;

                    // GAME LOGIC ENTRY POINT
                    // Hand off msg.Buffer.AsSpan(0, msg.Length) to your packet handlers.
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[SessionManager] Poll error on client {msg.ClientId}: {ex.Message}");
                }
                finally
                {
                    // Always return the bridge buffer to the pool
                    _privatePool.Return(msg.Buffer);
                }
            }
        }

        #endregion

        #region Lifecycle Handlers

        private void HandleClientConnected(ushort id)
        {
            if (_disposed) return;
            var session = new LiminalSession(id, _config.MaxPacketSizePerBatch);
            _sessions.TryAdd(id, session);
            LiminalLogger.Log($"[SessionManager] Session {id} initialized.");
        }

        private void HandleClientDisconnected(ushort id)
        {
            if (_sessions.TryRemove(id, out var session))
            {
                session.Dispose();
                LiminalLogger.Log($"[SessionManager] Session {id} cleaned up.");
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _transport.OnMessageReceivedReliable -= HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable -= HandleUnreliableMessage;
            _transport.OnClientConnected -= HandleClientConnected;
            _transport.OnClientDisconnected -= HandleClientDisconnected;
            _transport.OnShutdown -= Dispose;

            // Cleanup all sessions (Native Memory Free)
            foreach (var session in _sessions.Values)
            {
                session.Dispose();
            }
            _sessions.Clear();

            // Flush the queue and return buffers
            while (_incomingMessages.TryDequeue(out var msg))
            {
                _privatePool.Return(msg.Buffer);
            }
        }

        #endregion

        private readonly struct IncomingMessage
        {
            public readonly ushort ClientId;
            public readonly byte[] Buffer;
            public readonly int Length;
            public readonly bool Reliable;

            public IncomingMessage(ushort id, byte[] buffer, int length, bool reliable)
            {
                ClientId = id;
                Buffer = buffer;
                Length = length;
                Reliable = reliable;
            }
        }
    }
}