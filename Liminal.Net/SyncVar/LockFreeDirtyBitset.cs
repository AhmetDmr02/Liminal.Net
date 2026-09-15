using System.Threading;

namespace Liminal.Net.SyncVar
{
    public sealed class LockFreeDirtyBitset
    {
        private readonly long[] _masks = new long[64];

        public void SetDirty(ushort id)
        {
            int bucket = id >> 6;
            long bit = 1L << (id & 63);

#if NET5_0_OR_GREATER
            Interlocked.Or(ref _masks[bucket], bit);
#else
            long current = Volatile.Read(ref _masks[bucket]);
            while ((current & bit) == 0)
            {
                long updated = current | bit;
                long prior = Interlocked.CompareExchange(ref _masks[bucket], updated, current);
                if (prior == current)
                    break;
                current = prior;
            }
#endif
        }

        public bool TryConsumeBucket(int bucketIndex, out long dirtyMask)
        {
            dirtyMask = Interlocked.Exchange(ref _masks[bucketIndex], 0);
            return dirtyMask != 0;
        }

        public void Clear()
        {
            for (int i = 0; i < _masks.Length; i++)
            {
                Volatile.Write(ref _masks[i], 0);
            }
        }

        public bool IsDirty(ushort id)
        {
            int bucket = id >> 6;
            long bit = 1L << (id & 63);
            return (Volatile.Read(ref _masks[bucket]) & bit) != 0;
        }
    }
}