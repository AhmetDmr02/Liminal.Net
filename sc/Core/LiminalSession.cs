namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        internal LiminalNativeBuffer SendBuffer { get; }
        internal LiminalNativeBuffer ReceiveBuffer { get; }

        public bool IsActive { get; private set; }

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;
            SendBuffer = new LiminalNativeBuffer(bufferSize);
            ReceiveBuffer = new LiminalNativeBuffer(bufferSize);
            IsActive = true;
        }

        public void Dispose()
        {
            if (!IsActive) return;
            IsActive = false;

            SendBuffer.ManualDispose();
            ReceiveBuffer.ManualDispose();
        }
    }
}