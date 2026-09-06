using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Threading;

namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        private int _disposed = 0;

        internal readonly object SendLock = new();
        internal readonly object ReceiveLock = new();

        internal readonly LiminalNativeBuffer RawSendBuffer;
        internal int RawSendCursor = 0;

        internal readonly LiminalNativeBuffer SendBuffer;

        internal readonly ConcurrentQueue<InboundPacket> InboundQueue = new();

        // Staging buffers for the Ping-Pong transformation pipeline
        internal readonly LiminalNativeBuffer InboundStagingA;
        internal readonly LiminalNativeBuffer InboundStagingB;

        internal readonly LiminalNativeBuffer OutboundStagingA;
        internal readonly LiminalNativeBuffer OutboundStagingB;

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;
            RawSendBuffer = new LiminalNativeBuffer(bufferSize);
            SendBuffer = new LiminalNativeBuffer(bufferSize);

            InboundStagingA = new LiminalNativeBuffer(bufferSize);
            InboundStagingB = new LiminalNativeBuffer(bufferSize);

            OutboundStagingA = new LiminalNativeBuffer(bufferSize);
            OutboundStagingB = new LiminalNativeBuffer(bufferSize);
        }

        public bool IsDisposed()
        {
            return Volatile.Read(ref _disposed) == 1;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

            lock (SendLock)
            {
                RawSendBuffer.ManualDispose();
                SendBuffer.ManualDispose();
                OutboundStagingA.ManualDispose();
                OutboundStagingB.ManualDispose();
            }

            lock (ReceiveLock)
            {
                InboundStagingA.ManualDispose();
                InboundStagingB.ManualDispose();
            }
        }
    }
}

public readonly struct InboundPacket
{
    public readonly ushort PacketId;
    public readonly byte[] BackingBuffer;
    public readonly int Length;

    public InboundPacket(ushort packetId, byte[] backingBuffer, int length)
    {
        PacketId = packetId;
        BackingBuffer = backingBuffer;
        Length = length;
    }

    public ReadOnlyMemory<byte> AsMemory() => new ReadOnlyMemory<byte>(BackingBuffer, 0, Length);
}