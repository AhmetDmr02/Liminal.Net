using System.Collections.Concurrent;
using System.Buffers;
using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    public class LiminalSessionManager : IDisposable
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly ArrayPool<byte> _privatePool;
        private readonly ConcurrentQueue<IncomingMessage> _incomingMessages = new();
        private volatile bool _disposed;

        public LiminalSessionManager(ILiminalTransport transport, LiminalTransportConfig config)
        {
            _transport = transport;
            _config = config;

            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);

            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnShutdown += Dispose;
        }

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id)
            => EnqueueMessage(id, data, true);

        private void EnqueueMessage(ushort id, ReadOnlySpan<byte> data, bool reliable)
        {
            if (_disposed) return;

            byte[] buffer = _privatePool.Rent(data.Length);

            try
            {
                data.CopyTo(buffer);

                // Double-check disposal before enqueueing
                // (prevents buffer leak if Dispose() called mid-execution)
                if (!_disposed)
                {
                    _incomingMessages.Enqueue(new IncomingMessage(id, buffer, data.Length));
                }
                else
                {
                    // Dispose happened while we were working - return immediately
                    _privatePool.Return(buffer);
                }
            }
            catch
            {
                // If anything fails, return the buffer before re-throwing
                _privatePool.Return(buffer);
                throw;
            }
        }

        public void Poll()
        {
            while (_incomingMessages.TryDequeue(out var msg))
            {
                try
                {
                    if (_disposed)
                    {
                        continue;
                    }

                    ReadOnlySpan<byte> packetData = msg.Buffer.AsSpan(0, msg.Length);

                    if (_sessions.TryGetValue(msg.ClientId, out var session))
                    {
                        // TODO: Your game logic here
                        // ProcessGamePacket(session, packetData);

                        // Game logic MUST NOT store packetData or msg.Buffer!
                        // If you need the data later, do: byte[] copy = packetData.ToArray();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message from client {msg.ClientId}: {ex}");
                }
                finally
                {
                    _privatePool.Return(msg.Buffer);
                }
            }
        }

        private void HandleClientConnected(ushort id)
        {
            if (_disposed) return;

            var session = new LiminalSession(id, _config.MaxPacketSizePerBatch);
            _sessions.TryAdd(id, session);
        }

        private void HandleClientDisconnected(ushort id)
        {
            if (_sessions.TryRemove(id, out var session))
            {
                session.Dispose();
            }
        }

        private readonly struct IncomingMessage
        {
            public readonly ushort ClientId;
            public readonly byte[] Buffer;
            public readonly int Length;

            public IncomingMessage(ushort id, byte[] buffer, int length)
            {
                ClientId = id;
                Buffer = buffer;
                Length = length;
            }
        }

        public void Dispose()
        {
            _disposed = true;

            _transport.OnClientConnected -= HandleClientConnected;
            _transport.OnClientDisconnected -= HandleClientDisconnected;
            _transport.OnMessageReceivedReliable -= HandleReliableMessage;
            _transport.OnShutdown -= Dispose;

            foreach (var session in _sessions.Values)
            {
                session.Dispose();
            }
            _sessions.Clear();

            while (_incomingMessages.TryDequeue(out var msg))
            {
                _privatePool.Return(msg.Buffer);
            }
        }

        public bool TryGetSession(ushort id, out LiminalSession session)
            => _sessions.TryGetValue(id, out session);

        public int QueuedMessageCount => _incomingMessages.Count;
    }
}