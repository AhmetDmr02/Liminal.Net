using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;

namespace Liminal.Net.Core
{
    internal sealed unsafe class LiminalNativeBuffer : MemoryManager<byte>, IMemoryOwner<byte>
    {
        private byte* _ptr;
        private readonly int _length;
        private int _disposed = 0;

        internal bool IsDisposed => _disposed != 0;

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

        public override Span<byte> GetSpan()
        {
            if (Volatile.Read(ref _disposed) != 0)
                throw new ObjectDisposedException("LiminalNativeBuffer");

            return new Span<byte>(_ptr, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if ((uint)elementIndex >= (uint)_length) throw new ArgumentOutOfRangeException();
            return new MemoryHandle(_ptr + elementIndex);
        }

        public override void Unpin() { }

        internal void ManualDispose() => Dispose(true);

        protected override void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                if (_ptr != null)
                {
                    NativeMemory.Free(_ptr);
                    _ptr = null;
                }

                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }
            }
        }
    }
}