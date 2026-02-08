using System.Collections.Concurrent;

namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        internal readonly object SendLock = new();
        internal readonly LiminalNativeBuffer RawSendBuffer;
        internal int RawSendCursor = 0;
        internal readonly LiminalNativeBuffer SendBuffer;

        internal readonly ConcurrentQueue<InboundPacket> InboundQueue = new();

        // Staging buffers for the Ping-Pong transformation pipeline
        internal readonly LiminalNativeBuffer StagingBufferA;
        internal readonly LiminalNativeBuffer StagingBufferB;

        public bool IsActive { get; private set; }

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;
            RawSendBuffer = new LiminalNativeBuffer(bufferSize);
            SendBuffer = new LiminalNativeBuffer(bufferSize);

            StagingBufferA = new LiminalNativeBuffer(bufferSize);
            StagingBufferB = new LiminalNativeBuffer(bufferSize);
            IsActive = true;
        }

        public void Dispose()
        {
            if (!IsActive) return;
            IsActive = false;

            RawSendBuffer.ManualDispose();
            SendBuffer.ManualDispose();
            StagingBufferA.ManualDispose();
            StagingBufferB.ManualDispose();
        }
    }

    public struct InboundPacket
    {
        public byte[] Buffer;
        public int Length;
        public ushort PacketId;

        public void Init(byte[] buffer, int length, ushort packetId)
        {
            Buffer = buffer;
            Length = length;
            PacketId = packetId;
        }
        public void Reset()
        {
            Buffer = null;
            Length = 0;
            PacketId = 0;
        }
    }
}