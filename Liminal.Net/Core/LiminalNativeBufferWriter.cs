using System.Buffers;

namespace Liminal.Net.Core
{
    internal sealed class LiminalNativeBufferWriter : IBufferWriter<byte>
    {
        private readonly LiminalNativeBuffer _buffer;
        private int _position;

        public LiminalNativeBufferWriter(int size)
        {
            _buffer = new LiminalNativeBuffer(size);
        }

        public void Advance(int count)
        {
            _position += count;
            if (_position > _buffer.Memory.Length)
            {
                throw new InvalidOperationException("BufferWriter advanced beyond capacity.");
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.Memory.Slice(_position);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.GetSpan().Slice(_position);
        }

        public ReadOnlySpan<byte> WrittenSpan => _buffer.GetSpan().Slice(0, _position);

        public void Clear() => _position = 0;

        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint == 0) sizeHint = 1;

            if (_position + sizeHint > _buffer.Memory.Length)
            {
                throw new OutOfMemoryException($"[Liminal] Serialization exceeded fixed native buffer size of {_buffer.Memory.Length}.");
            }
        }
    }
}