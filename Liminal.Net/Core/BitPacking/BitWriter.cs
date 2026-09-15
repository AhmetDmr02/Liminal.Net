using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Liminal.Net.Core
{
    /// <summary>
    /// High-performance, zero-allocation bitstream writer.
    /// Operates as a stack-allocated ref struct capable of writing variable-length bitfields,
    /// primitives, quantized floats, and raw byte spans into an <see cref="IBufferWriter{T}"/> or destination span.
    /// </summary>
    public ref struct BitWriter
    {
        private readonly IBufferWriter<byte> _bufferWriter;
        private Span<byte> _currentSpan;
        private int _spanIndex;
        private ulong _bitAccumulator;
        private int _bitCount;
        private int _totalBitsWritten;

        /// <summary>
        /// Total number of bits written (including padding introduced by alignment).
        /// </summary>
        public int BitsWritten => _totalBitsWritten;

        /// <summary>
        /// Total number of full or partial bytes written so far.
        /// </summary>
        public int BytesWritten => (_totalBitsWritten + 7) / 8;

        /// <summary>
        /// Creates a bit writer targeting an <see cref="IBufferWriter{T}"/> (e.g. LiminalNativeBufferWriter).
        /// </summary>
        public BitWriter(IBufferWriter<byte> bufferWriter)
        {
            _bufferWriter = bufferWriter ?? throw new ArgumentNullException(nameof(bufferWriter));
            _currentSpan = _bufferWriter.GetSpan(256);
            _spanIndex = 0;
            _bitAccumulator = 0;
            _bitCount = 0;
            _totalBitsWritten = 0;
        }

        /// <summary>
        /// Creates a bit writer targeting a fixed destination span.
        /// </summary>
        public BitWriter(Span<byte> destination)
        {
            _bufferWriter = null;
            _currentSpan = destination;
            _spanIndex = 0;
            _bitAccumulator = 0;
            _bitCount = 0;
            _totalBitsWritten = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureSpan(int count)
        {
            if (_spanIndex + count <= _currentSpan.Length)
                return;

            if (_bufferWriter != null)
            {
                _bufferWriter.Advance(_spanIndex);
                int request = Math.Max(count, 256);
                _currentSpan = _bufferWriter.GetSpan(request);
                _spanIndex = 0;
            }
            else
            {
                throw new InvalidOperationException("BitWriter destination span capacity exceeded.");
            }
        }

        /// <summary>
        /// Writes an unsigned integer with specified bit count (1-32 bits, LSB-first).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBits(uint value, int bitCount)
        {
            if ((uint)(bitCount - 1) >= 32)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 32.");

            uint mask = (bitCount == 32) ? uint.MaxValue : ((1u << bitCount) - 1);
            value &= mask;

            _bitAccumulator |= ((ulong)value) << _bitCount;
            _bitCount += bitCount;
            _totalBitsWritten += bitCount;

            while (_bitCount >= 8)
            {
                EnsureSpan(1);
                _currentSpan[_spanIndex++] = (byte)(_bitAccumulator & 0xFF);
                _bitAccumulator >>= 8;
                _bitCount -= 8;
            }
        }

        /// <summary>
        /// Writes an unsigned 64-bit integer with specified bit count (1-64 bits, LSB-first).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteULongBits(ulong value, int bitCount)
        {
            if ((uint)(bitCount - 1) >= 64)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 64.");

            if (bitCount <= 32)
            {
                WriteBits((uint)value, bitCount);
            }
            else
            {
                WriteBits((uint)value, 32);
                WriteBits((uint)(value >> 32), bitCount - 32);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBool(bool value) => WriteBits(value ? 1u : 0u, 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte value) => WriteBits(value, 8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUShort(ushort value, int bitCount = 16) => WriteBits(value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteShort(short value, int bitCount = 16) => WriteBits((uint)value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUInt(uint value, int bitCount = 32) => WriteBits(value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt(int value, int bitCount = 32) => WriteBits((uint)value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteULong(ulong value, int bitCount = 64) => WriteULongBits(value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteLong(long value, int bitCount = 64) => WriteULongBits((ulong)value, bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteFloat(float value) => WriteUInt(Unsafe.As<float, uint>(ref value), 32);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDouble(double value) => WriteULong(Unsafe.As<double, ulong>(ref value), 64);

        /// <summary>
        /// Quantizes a float value between min and max into an integer of specified bit resolution (1-32 bits).
        /// </summary>
        public void WriteQuantizedFloat(float value, float min, float max, int bitCount)
        {
            if (bitCount <= 0 || bitCount > 32)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 32.");

            if (max <= min)
                throw new ArgumentException("Max must be strictly greater than min.", nameof(max));

            float clamped = Math.Clamp(value, min, max);
            float normalized = (clamped - min) / (max - min);
            uint maxVal = (bitCount == 32) ? uint.MaxValue : ((1u << bitCount) - 1);
            uint raw = (uint)MathF.Round(normalized * maxVal);

            WriteBits(raw, bitCount);
        }

        /// <summary>
        /// Writes raw bytes into the stream. Automatically aligns the stream to byte boundary first.
        /// </summary>
        public void WriteBytes(ReadOnlySpan<byte> bytes)
        {
            if (bytes.IsEmpty)
                return;

            Align();

            if (_bufferWriter != null)
            {
                if (_spanIndex > 0)
                {
                    _bufferWriter.Advance(_spanIndex);
                    _currentSpan = default;
                    _spanIndex = 0;
                }

                var dest = _bufferWriter.GetSpan(bytes.Length);
                bytes.CopyTo(dest);
                _bufferWriter.Advance(bytes.Length);

                _currentSpan = _bufferWriter.GetSpan(256);
                _spanIndex = 0;
            }
            else
            {
                EnsureSpan(bytes.Length);
                bytes.CopyTo(_currentSpan.Slice(_spanIndex));
                _spanIndex += bytes.Length;
            }

            _totalBitsWritten += bytes.Length * 8;
        }

        /// <summary>
        /// Pads the stream with zero-bits until the bit position is aligned to a byte boundary.
        /// </summary>
        public void Align()
        {
            if (_bitCount > 0)
            {
                EnsureSpan(1);
                _currentSpan[_spanIndex++] = (byte)(_bitAccumulator & 0xFF);
                int remainder = 8 - _bitCount;
                _totalBitsWritten += remainder;
                _bitAccumulator = 0;
                _bitCount = 0;
            }
        }

        /// <summary>
        /// Flushes any pending bits and commits written bytes to the underlying buffer writer.
        /// </summary>
        public void Flush()
        {
            Align();

            if (_bufferWriter != null && _spanIndex > 0)
            {
                _bufferWriter.Advance(_spanIndex);
                _currentSpan = default;
                _spanIndex = 0;
            }
        }
    }
}
