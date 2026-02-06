namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        internal readonly LiminalNativeBuffer SendBuffer;
        internal int SendCursor = 0;

        internal readonly LiminalNativeBuffer ReceiveBuffer;
        internal int ReceiveCursor = 0;

        // The "Blob" Buffer
        internal readonly LiminalNativeBuffer IngestBuffer;

        // Staging buffers for the Ping-Pong transformation pipeline
        internal readonly LiminalNativeBuffer StagingBufferA;
        internal readonly LiminalNativeBuffer StagingBufferB;

        public bool IsActive { get; private set; }

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;
            SendBuffer = new LiminalNativeBuffer(bufferSize);
            ReceiveBuffer = new LiminalNativeBuffer(bufferSize);
            IngestBuffer = new LiminalNativeBuffer(bufferSize);
            StagingBufferA = new LiminalNativeBuffer(bufferSize);
            StagingBufferB = new LiminalNativeBuffer(bufferSize);
            IsActive = true;
        }

        public void Dispose()
        {
            if (!IsActive) return;
            IsActive = false;

            SendBuffer.ManualDispose();
            ReceiveBuffer.ManualDispose();
            IngestBuffer.ManualDispose();
            StagingBufferA.ManualDispose();
            StagingBufferB.ManualDispose();
        }
    }
}