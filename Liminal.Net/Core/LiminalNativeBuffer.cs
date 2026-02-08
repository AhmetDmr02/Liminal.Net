using System.Buffers;
using System.Runtime.InteropServices;

namespace Liminal.Net.Core
{
    internal sealed unsafe class LiminalNativeBuffer : MemoryManager<byte>, IMemoryOwner<byte>
    {
        private byte* _ptr;
        private readonly int _length;
        private bool _disposed;

        internal bool IsDisposed => _disposed;

        public LiminalNativeBuffer(int length)
        {
            _length = length;
            _ptr = (byte*)NativeMemory.Alloc((nuint)length);
        }

        ~LiminalNativeBuffer()
        {
            Dispose(false);
        }

        public Memory<byte> Memory => CreateMemory(_length);

        public override Span<byte> GetSpan() => new Span<byte>(_ptr, _length);

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if ((uint)elementIndex >= (uint)_length) throw new ArgumentOutOfRangeException();
            return new MemoryHandle(_ptr + elementIndex);
        }

        public override void Unpin() { }

        internal void ManualDispose() => Dispose(true);

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (_ptr != null)
                {
                    NativeMemory.Free(_ptr);
                    _ptr = null;
                }
                _disposed = true;
            }
        }
    }
}