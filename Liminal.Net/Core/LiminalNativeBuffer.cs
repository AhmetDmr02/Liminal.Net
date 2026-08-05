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

        private int _state;
        private const int DisposedFlag = 1 << 31;

        public bool IsDisposed => (Volatile.Read(ref _state) & DisposedFlag) != 0;

        public LiminalNativeBuffer(int length)
        {
            if (length <= 0)
                throw new ArgumentOutOfRangeException(nameof(length), "Length must be greater than zero.");

            _length = length;
            _ptr = (byte*)NativeMemory.Alloc((nuint)length);
        }

        ~LiminalNativeBuffer()
        {
            Dispose(false);
        }

        public override Span<byte> GetSpan()
        {
            if (IsDisposed)
                throw new ObjectDisposedException(nameof(LiminalNativeBuffer));

            return new Span<byte>(_ptr, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if ((uint)elementIndex > (uint)_length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));

            while (true)
            {
                int state = Volatile.Read(ref _state);

                if ((state & DisposedFlag) != 0)
                    throw new ObjectDisposedException(nameof(LiminalNativeBuffer));

                int newState = state + 1;
                if (Interlocked.CompareExchange(ref _state, newState, state) == state)
                    break;
            }

            return new MemoryHandle(_ptr + elementIndex, pinnable: this);
        }

        public override void Unpin()
        {
            while (true)
            {
                int state = Volatile.Read(ref _state);

                if ((state & ~DisposedFlag) == 0)
                    throw new InvalidOperationException("Unmatched Unpin. The buffer is not currently pinned.");

                int newState = state - 1; 
                if (Interlocked.CompareExchange(ref _state, newState, state) == state)
                {
                    if (newState == DisposedFlag)
                    {
                        FreeMemory();
                    }
                    break;
                }
            }
        }

        internal void ManualDispose() => ((IDisposable)this).Dispose();

        protected override void Dispose(bool disposing)
        {
            while (true)
            {
                int state = Volatile.Read(ref _state);

                if ((state & DisposedFlag) != 0)
                    return; 

                int newState = state | DisposedFlag;
                if (Interlocked.CompareExchange(ref _state, newState, state) == state)
                {
                    if (newState == DisposedFlag)
                    {
                        FreeMemory();
                    }

                    if (disposing)
                    {
                        GC.SuppressFinalize(this);
                    }
                    break;
                }
            }
        }

        private void FreeMemory()
        {
            byte* ptr = _ptr;
            _ptr = null;

            if (ptr != null)
            {
                NativeMemory.Free(ptr);
            }
        }
    }
}