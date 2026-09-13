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

            Interlocked.Or(ref _masks[bucket], bit);
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
    }
}