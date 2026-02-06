using System.Collections.Concurrent;
using System.Buffers;
using Liminal.Net.Interfaces;
using System.Buffers.Binary;

namespace Liminal.Net.Core
{
    public class LiminalSessionManager : IDisposable
    {
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions = new();
        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly LiminalPacketFramerPipeline _pipeline;
        private readonly ArrayPool<byte> _privatePool;

        private volatile bool _disposed;

        public LiminalSessionManager(ILiminalTransport transport, LiminalTransportConfig config, LiminalPacketFramerPipeline pipeline)
        {
            _transport = transport;
            _config = config;
            _pipeline = pipeline;

            _privatePool = ArrayPool<byte>.Create(config.MaxPacketSizePerBatch, 50);

            _transport.OnMessageReceivedReliable += HandleReliableMessage;
            _transport.OnMessageReceivedUnreliable += HandleUnreliableMessage;
            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnShutdown += Dispose;
        }

        #region Receive Path (Background Threads)

        private void HandleReliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);
        private void HandleUnreliableMessage(ReadOnlySpan<byte> data, ushort id) => ProcessIncoming(id, data);

        private void ProcessIncoming(ushort ownerId, ReadOnlySpan<byte> data)
        {
            if (_disposed || !_sessions.TryGetValue(ownerId, out var session)) return;

            unsafe
            {
                lock (session)
                {
                    // It contains [4-byte len][Packet A][4-byte len][Packet B]...
                    data.CopyTo(session.IngestBuffer.GetSpan());

                    // It will loop through StagingA, Decrypt into StagingB, 
                    // and append to ReceiveBuffer in the end.
                    _pipeline.ExecuteInbound(session, data.Length);
                }
            }
        }

        #endregion

        #region Write Path (Game Thread)

        /// <summary>
        /// Sends a packet to a client.
        /// </summary>
        /// <param name="targetId">The client to send the packet to</param>
        /// <param name="data">raw packet data</param>
        public void Send(ushort targetId, ReadOnlySpan<byte> data, bool reliable)
        {
            if (_disposed || !_sessions.TryGetValue(targetId, out var session)) return;

            lock (session)
            {
                data.CopyTo(session.StagingBufferA.GetSpan());

                // Transform & Append to SendBuffer batch
                _pipeline.ExecuteOutbound(session, data.Length);
            }
        }

        /// <summary>
        /// Flushes all batched outbound data to the transport.
        /// </summary>
        public void Flush()
        {
            foreach (var session in _sessions.Values)
            {
                lock (session)
                {
                    if (session.SendCursor == 0) continue;

                    _transport.SendReliable(session.SendBuffer.GetSpan().Slice(0, session.SendCursor), session.Id);
                    session.SendCursor = 0;
                }
            }
        }

        #endregion

        #region Polling Logic (Game Thread)

        public void Poll()
        {
            if (_disposed) return;

            foreach (var session in _sessions.Values)
            {
                lock (session)
                {
                    if (session.ReceiveCursor == 0) continue;

                    // THE BUFFER WALKER: Slicing the batch back into packets
                    ProcessReceiveBatch(session);

                    // Reset batch for next frame
                    session.ReceiveCursor = 0;
                }
            }
        }

        private void ProcessReceiveBatch(LiminalSession session)
        {
            Span<byte> batch = session.ReceiveBuffer.GetSpan().Slice(0, session.ReceiveCursor);
            int offset = 0;

            while (offset + 4 <= batch.Length)
            {
                int payloadSize = BinaryPrimitives.ReadInt32LittleEndian(batch.Slice(offset, 4));

                if (offset + 4 + payloadSize > batch.Length) break;

                ReadOnlySpan<byte> packetData = batch.Slice(offset + 4, payloadSize);

                byte[] managedCopy = _privatePool.Rent(payloadSize);
                packetData.CopyTo(managedCopy);

                try
                {
                    // LiminalEventBus.Publish(session.Id, managedCopy, payloadSize);
                    // This is where high-level systems gets the data.
                }
                finally
                {
                    _privatePool.Return(managedCopy);
                }

                offset += 4 + payloadSize;
            }
        }

        #endregion

        #region Lifecycle Handlers
        private void HandleClientConnected(ushort id)
        {
            if (_disposed) return;
            _sessions.TryAdd(id, new LiminalSession(id, _config.MaxPacketSizePerBatch));
        }

        private void HandleClientDisconnected(ushort id)
        {
            if (_sessions.TryRemove(id, out var session)) session.Dispose();
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

            foreach (var session in _sessions.Values) session.Dispose();
            _sessions.Clear();
        }
        #endregion
    }
}