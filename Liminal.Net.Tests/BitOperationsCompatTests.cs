using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using NUnit.Framework;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class BitOperationsCompatTests
    {
        [Test]
        public void TrailingZeroCount_Zero_Returns64()
        {
            Assert.That(BitOperationsCompat.TrailingZeroCount(0UL), Is.EqualTo(64));
            Assert.That(BitOperationsCompat.TrailingZeroCount(0L), Is.EqualTo(64));
            Assert.That(BitOperationsCompat.TrailingZeroCount(0U), Is.EqualTo(32));
            Assert.That(BitOperationsCompat.TrailingZeroCount(0), Is.EqualTo(32));
        }

        [Test]
        public void TrailingZeroCount_AllSingleBits_ReturnsCorrectPosition()
        {
            for (int bit = 0; bit < 64; bit++)
            {
                ulong value = 1UL << bit;
                Assert.That(BitOperationsCompat.TrailingZeroCount(value), Is.EqualTo(bit), $"Failed for bit {bit}");
                Assert.That(BitOperationsCompat.TrailingZeroCount((long)value), Is.EqualTo(bit), $"Failed for (long) bit {bit}");
            }

            for (int bit = 0; bit < 32; bit++)
            {
                uint value = 1U << bit;
                Assert.That(BitOperationsCompat.TrailingZeroCount(value), Is.EqualTo(bit), $"Failed for (uint) bit {bit}");
                Assert.That(BitOperationsCompat.TrailingZeroCount((int)value), Is.EqualTo(bit), $"Failed for (int) bit {bit}");
            }
        }

        [Test]
        public void TrailingZeroCount_MultipleBits_ReturnsLowestBitPosition()
        {
            ulong val = (1UL << 7) | (1UL << 24) | (1UL << 63);
            Assert.That(BitOperationsCompat.TrailingZeroCount(val), Is.EqualTo(7));

            ulong val2 = 0x8000_0000_0000_0000UL;
            Assert.That(BitOperationsCompat.TrailingZeroCount(val2), Is.EqualTo(63));

            ulong val3 = 0xFFFF_FFFF_FFFF_FFFFUL;
            Assert.That(BitOperationsCompat.TrailingZeroCount(val3), Is.EqualTo(0));
        }

        [Test]
        public void LockFreeDirtyBitset_SetAndConsume_WorksAccurately()
        {
            var bitset = new LockFreeDirtyBitset();

            // Set specific IDs
            ushort id1 = 5;      // Bucket 0, bit 5
            ushort id2 = 63;     // Bucket 0, bit 63
            ushort id3 = 64;     // Bucket 1, bit 0
            ushort id4 = 130;    // Bucket 2, bit 2

            bitset.SetDirty(id1);
            bitset.SetDirty(id2);
            bitset.SetDirty(id3);
            bitset.SetDirty(id4);

            // Consume bucket 0
            Assert.That(bitset.TryConsumeBucket(0, out long mask0), Is.True);
            long expectedMask0 = (1L << 5) | (1L << 63);
            Assert.That(mask0, Is.EqualTo(expectedMask0));

            // Consuming bucket 0 again should return false (empty)
            Assert.That(bitset.TryConsumeBucket(0, out _), Is.False);

            // Consume bucket 1
            Assert.That(bitset.TryConsumeBucket(1, out long mask1), Is.True);
            long expectedMask1 = 1L << 0;
            Assert.That(mask1, Is.EqualTo(expectedMask1));

            // Consume bucket 2
            Assert.That(bitset.TryConsumeBucket(2, out long mask2), Is.True);
            long expectedMask2 = 1L << 2;
            Assert.That(mask2, Is.EqualTo(expectedMask2));

            // Consuming empty bucket 3 should return false
            Assert.That(bitset.TryConsumeBucket(3, out _), Is.False);
        }

        [Test]
        public void LockFreeDirtyBitset_Clear_ResetsAllBuckets()
        {
            var bitset = new LockFreeDirtyBitset();
            bitset.SetDirty(10);
            bitset.SetDirty(100);

            bitset.Clear();

            Assert.That(bitset.TryConsumeBucket(0, out _), Is.False);
            Assert.That(bitset.TryConsumeBucket(1, out _), Is.False);
        }

        [Test]
        public void LockFreeDirtyBitset_ClearDirty_ClearsSpecificBitOnly()
        {
            var bitset = new LockFreeDirtyBitset(128);
            bitset.SetDirty(5);
            bitset.SetDirty(6);

            Assert.That(bitset.IsDirty(5), Is.True);
            Assert.That(bitset.IsDirty(6), Is.True);
            Assert.That(bitset.CountDirtyBits(), Is.EqualTo(2));

            bitset.ClearDirty(5);

            Assert.That(bitset.IsDirty(5), Is.False);
            Assert.That(bitset.IsDirty(6), Is.True);
            Assert.That(bitset.CountDirtyBits(), Is.EqualTo(1));
        }

        [Test]
        public void LockFreeSlotBitset_AllocateAndFree_RecyclesAccurately()
        {
            var slotBitset = new LockFreeSlotBitset(64);

            Assert.That(slotBitset.TryAllocate(out ushort s0), Is.True);
            Assert.That(s0, Is.EqualTo(0));
            Assert.That(slotBitset.TryAllocate(out ushort s1), Is.True);
            Assert.That(s1, Is.EqualTo(1));
            Assert.That(slotBitset.CountAllocated(), Is.EqualTo(2));

            slotBitset.Free(s0);
            Assert.That(slotBitset.IsAllocated(s0), Is.False);
            Assert.That(slotBitset.IsAllocated(s1), Is.True);
            Assert.That(slotBitset.CountAllocated(), Is.EqualTo(1));

            Assert.That(slotBitset.TryAllocate(out ushort sReused), Is.True);
            Assert.That(sReused, Is.EqualTo(0));
        }

        [Test]
        public void LockFreeSlotBitset_CapacityExceeded_ReturnsFalse()
        {
            var slotBitset = new LockFreeSlotBitset(3);

            Assert.That(slotBitset.TryAllocate(out _), Is.True);
            Assert.That(slotBitset.TryAllocate(out _), Is.True);
            Assert.That(slotBitset.TryAllocate(out _), Is.True);

            Assert.That(slotBitset.TryAllocate(out ushort overflow), Is.False);
            Assert.That(overflow, Is.EqualTo(ushort.MaxValue));
        }

        [Test]
        public void BitOperationsCompat_PopCount_MatchesExpected()
        {
            Assert.That(BitOperationsCompat.PopCount(0UL), Is.EqualTo(0));
            Assert.That(BitOperationsCompat.PopCount(1UL), Is.EqualTo(1));
            Assert.That(BitOperationsCompat.PopCount(0b1011_0101UL), Is.EqualTo(5));
            Assert.That(BitOperationsCompat.PopCount(0xFFFF_FFFF_FFFF_FFFFUL), Is.EqualTo(64));
        }
    }
}
