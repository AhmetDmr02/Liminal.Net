using Liminal.Net.Core;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    public sealed class LockFreeSlotBitset
    {
        private readonly long[] _allocatedMasks;
        private readonly int _capacity;

        public int Capacity => _capacity;
        public int BucketCount => _allocatedMasks.Length;

        public LockFreeSlotBitset(int capacity = 4096)
        {
            if (capacity <= 0) capacity = 4096;
            _capacity = capacity;
            int buckets = (capacity + 63) / 64;
            _allocatedMasks = new long[buckets];
        }

        public bool TryAllocate(out ushort slotId)
        {
            int buckets = _allocatedMasks.Length;
            for (int b = 0; b < buckets; b++)
            {
                while (true)
                {
                    long current = Volatile.Read(ref _allocatedMasks[b]);
                    if (current == -1L)
                    {
                        break;
                    }

                    ulong inverted = (ulong)~current;
                    int bit = BitOperationsCompat.TrailingZeroCount(inverted);
                    int candidate = (b << 6) + bit;
                    if (candidate >= _capacity)
                    {
                        slotId = ushort.MaxValue;
                        return false;
                    }

                    long mask = 1L << bit;
                    long updated = current | mask;
                    long prior = Interlocked.CompareExchange(ref _allocatedMasks[b], updated, current);
                    if (prior == current)
                    {
                        slotId = (ushort)candidate;
                        return true;
                    }
                }
            }

            slotId = ushort.MaxValue;
            return false;
        }

        public void Free(ushort slotId)
        {
            if (slotId >= _capacity) return;
            int b = slotId >> 6;
            long bit = 1L << (slotId & 63);

#if NET5_0_OR_GREATER
            Interlocked.And(ref _allocatedMasks[b], ~bit);
#else
            long current = Volatile.Read(ref _allocatedMasks[b]);
            while ((current & bit) != 0)
            {
                long updated = current & ~bit;
                long prior = Interlocked.CompareExchange(ref _allocatedMasks[b], updated, current);
                if (prior == current)
                    break;
                current = prior;
            }
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsAllocated(ushort slotId)
        {
            if (slotId >= _capacity) return false;
            int b = slotId >> 6;
            long bit = 1L << (slotId & 63);
            return (Volatile.Read(ref _allocatedMasks[b]) & bit) != 0;
        }

        public void Clear()
        {
            for (int i = 0; i < _allocatedMasks.Length; i++)
            {
                Volatile.Write(ref _allocatedMasks[i], 0);
            }
        }

        public int CountAllocated()
        {
            int count = 0;
            for (int i = 0; i < _allocatedMasks.Length; i++)
            {
                ulong u = (ulong)Volatile.Read(ref _allocatedMasks[i]);
                count += BitOperationsCompat.PopCount(u);
            }
            return count;
        }
    }
}
