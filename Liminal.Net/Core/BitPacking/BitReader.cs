using System;
using System.Runtime.CompilerServices;

namespace Liminal.Net.Core
{
    public ref struct BitReader
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _bytePosition;
        private ulong _bitAccumulator;
        private int _bitCount;
        private int _totalBitsRead;

        /// <summary>
        /// Total capacity of the underlying buffer in bits.
        /// </summary>
        public int TotalBits => _data.Length * 8;

        /// <summary>
        /// Number of unread bits remaining in the stream.
        /// </summary>
        public int BitsRemaining => Math.Max(0, (_data.Length * 8) - _totalBitsRead);

        /// <summary>
        /// Total number of bits consumed so far (including alignment padding).
        /// </summary>
        public int BitsRead => _totalBitsRead;

        /// <summary>
        /// Total number of full or partial bytes consumed so far.
        /// </summary>
        public int BytesRead => (_totalBitsRead + 7) / 8;

        /// <summary>
        /// Total length of the underlying payload in bytes.
        /// </summary>
        public int LengthInBytes => _data.Length;

        /// <summary>
        /// True if there are more bits available to read.
        /// </summary>
        public bool HasMore => BitsRemaining > 0;

        /// <summary>
        /// Initializes a BitReader over a source byte span.
        /// </summary>
        public BitReader(ReadOnlySpan<byte> data)
        {
            _data = data;
            _bytePosition = 0;
            _bitAccumulator = 0;
            _bitCount = 0;
            _totalBitsRead = 0;
        }

        /// <summary>
        /// Reads an unsigned integer of specified bit count (1-32 bits, LSB-first).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadBits(int bitCount)
        {
            if ((uint)(bitCount - 1) >= 32)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 32.");

            while (_bitCount < bitCount && _bytePosition < _data.Length && _bitCount <= 56)
            {
                _bitAccumulator |= ((ulong)_data[_bytePosition++]) << _bitCount;
                _bitCount += 8;
            }

            if (_bitCount < bitCount)
                throw new InvalidOperationException($"BitReader underflow: Requested {bitCount} bits, but only {_bitCount} bits remain.");

            uint mask = (bitCount == 32) ? uint.MaxValue : ((1u << bitCount) - 1);
            uint result = (uint)(_bitAccumulator & mask);

            _bitAccumulator >>= bitCount;
            _bitCount -= bitCount;
            _totalBitsRead += bitCount;

            return result;
        }

        /// <summary>
        /// Reads an unsigned 64-bit integer of specified bit count (1-64 bits, LSB-first).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadULongBits(int bitCount)
        {
            if ((uint)(bitCount - 1) >= 64)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 64.");

            if (bitCount <= 32)
            {
                return ReadBits(bitCount);
            }
            else
            {
                ulong low = ReadBits(32);
                ulong high = ReadBits(bitCount - 32);
                return low | (high << 32);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ReadBool() => ReadBits(1) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte() => (byte)ReadBits(8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUShort(int bitCount = 16) => (ushort)ReadBits(bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadShort(int bitCount = 16)
        {
            uint raw = ReadBits(bitCount);
            if (bitCount < 16 && (raw & (1u << (bitCount - 1))) != 0)
                raw |= (~0u << bitCount);
            return (short)raw;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt(int bitCount = 32) => ReadBits(bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt(int bitCount = 32)
        {
            uint raw = ReadBits(bitCount);
            if (bitCount < 32 && (raw & (1u << (bitCount - 1))) != 0)
                raw |= (~0u << bitCount);
            return (int)raw;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadULong(int bitCount = 64) => ReadULongBits(bitCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadLong(int bitCount = 64)
        {
            ulong raw = ReadULongBits(bitCount);
            if (bitCount < 64 && (raw & (1ul << (bitCount - 1))) != 0)
                raw |= (~0ul << bitCount);
            return (long)raw;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadFloat()
        {
            uint raw = ReadUInt(32);
            return Unsafe.As<uint, float>(ref raw);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ReadDouble()
        {
            ulong raw = ReadULong(64);
            return Unsafe.As<ulong, double>(ref raw);
        }

        /// <summary>
        /// Reads an integer of specified bit resolution and un-quantizes it into a float between min and max.
        /// </summary>
        public float ReadQuantizedFloat(float min, float max, int bitCount)
        {
            if (bitCount <= 0 || bitCount > 32)
                throw new ArgumentOutOfRangeException(nameof(bitCount), "Bit count must be between 1 and 32.");

            if (max <= min)
                throw new ArgumentException("Max must be strictly greater than min.", nameof(max));

            uint raw = ReadBits(bitCount);
            uint maxVal = (bitCount == 32) ? uint.MaxValue : ((1u << bitCount) - 1);
            float normalized = (float)raw / maxVal;

            return min + normalized * (max - min);
        }

        /// <summary>
        /// Reads raw bytes into the destination span. Automatically aligns to the next byte boundary first.
        /// </summary>
        public void ReadBytes(Span<byte> destination)
        {
            if (destination.IsEmpty)
                return;

            Align();

            int destIndex = 0;
            while (_bitCount >= 8 && destIndex < destination.Length)
            {
                destination[destIndex++] = (byte)(_bitAccumulator & 0xFF);
                _bitAccumulator >>= 8;
                _bitCount -= 8;
            }

            if (destIndex < destination.Length)
            {
                int remaining = destination.Length - destIndex;
                if (_bytePosition + remaining > _data.Length)
                    throw new InvalidOperationException($"BitReader underflow: Destination requested {destination.Length} bytes, but stream ran out.");

                _data.Slice(_bytePosition, remaining).CopyTo(destination.Slice(destIndex));
                _bytePosition += remaining;
            }

            _totalBitsRead += destination.Length * 8;
        }

        /// <summary>
        /// Discards any unread bits of the current byte to align reading to the next byte boundary.
        /// </summary>
        public void Align()
        {
            int unalignedBits = _totalBitsRead % 8;
            if (unalignedBits != 0)
            {
                int bitsToDiscard = 8 - unalignedBits;

                if (_bitCount >= bitsToDiscard)
                {
                    _bitAccumulator >>= bitsToDiscard;
                    _bitCount -= bitsToDiscard;
                }
                else
                {
                    while (_bitCount < bitsToDiscard && _bytePosition < _data.Length)
                    {
                        _bitAccumulator |= ((ulong)_data[_bytePosition++]) << _bitCount;
                        _bitCount += 8;
                    }

                    if (_bitCount >= bitsToDiscard)
                    {
                        _bitAccumulator >>= bitsToDiscard;
                        _bitCount -= bitsToDiscard;
                    }
                    else
                    {
                        _bitAccumulator = 0;
                        _bitCount = 0;
                    }
                }

                _totalBitsRead += bitsToDiscard;
            }
        }

        /// <summary>
        /// Returns the remaining byte-aligned payload slice. Aligns the stream to byte boundary first.
        /// </summary>
        public ReadOnlySpan<byte> RemainingSpan
        {
            get
            {
                Align();
                int bufferedBytes = _bitCount / 8;
                int actualBytePos = _bytePosition - bufferedBytes;
                return actualBytePos < _data.Length ? _data.Slice(actualBytePos) : ReadOnlySpan<byte>.Empty;
            }
        }
    }
}
