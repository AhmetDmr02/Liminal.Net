using System.Runtime.CompilerServices;

namespace Liminal.Net.Core
{
    public static class BitOperationsCompat
    {
#if NETCOREAPP3_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(ulong value) => System.Numerics.BitOperations.TrailingZeroCount(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(long value) => System.Numerics.BitOperations.TrailingZeroCount(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(uint value) => System.Numerics.BitOperations.TrailingZeroCount(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(int value) => System.Numerics.BitOperations.TrailingZeroCount(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int PopCount(ulong value) => System.Numerics.BitOperations.PopCount(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int PopCount(uint value) => System.Numerics.BitOperations.PopCount(value);
#else
        // 64-bit De Bruijn lookup table matching .NET Core runtime implementation
        private static readonly byte[] TrailingZeroCountDeBruijn = new byte[64]
        {
            0,  1,  2, 53,  3,  7, 54, 27,
            4, 38, 41,  8, 34, 55, 48, 28,
            62,  5, 39, 46, 44, 42, 22,  9,
            24, 35, 59, 56, 49, 18, 29, 11,
            63, 52,  6, 26, 37, 40, 33, 47,
            61, 45, 43, 21, 23, 58, 17, 10,
            51, 25, 36, 32, 60, 20, 57, 16,
            50, 31, 19, 15, 30, 14, 13, 12
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(ulong value)
        {
            if (value == 0) return 64;
            return TrailingZeroCountDeBruijn[((value & (0ul - value)) * 0x022fdd63cc95386dUL) >> 58];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(long value) => TrailingZeroCount((ulong)value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(uint value)
        {
            if (value == 0) return 32;
            return TrailingZeroCount((ulong)value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroCount(int value) => TrailingZeroCount((uint)value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int PopCount(ulong value)
        {
            const ulong c1 = 0x5555555555555555UL;
            const ulong c2 = 0x3333333333333333UL;
            const ulong c4 = 0x0F0F0F0F0F0F0F0FUL;
            const ulong c5 = 0x0101010101010101UL;
            value -= (value >> 1) & c1;
            value = (value & c2) + ((value >> 2) & c2);
            value = (value + (value >> 4)) & c4;
            return (int)((value * c5) >> 56);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int PopCount(uint value) => PopCount((ulong)value);
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint RoundUpToPowerOf2(uint value)
        {
            if (value <= 1) return 1;
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            return value + 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Log2(uint value)
        {
            if (value == 0) return 0;
            int log = 0;
            if (value >= 1u << 16) { value >>= 16; log += 16; }
            if (value >= 1u << 8)  { value >>= 8;  log += 8; }
            if (value >= 1u << 4)  { value >>= 4;  log += 4; }
            if (value >= 1u << 2)  { value >>= 2;  log += 2; }
            if (value >= 1u << 1)  { log += 1; }
            return log;
        }
    }
}
