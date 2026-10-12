using Liminal.Net.SyncVar;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class LockFreeDirtyBitsetConcurrencyCoyoteTests
    {
        [CoyoteTest]
        public static async Task Coyote_DirtyBitset_ConcurrentSetAndConsume_ZeroLostBits()
        {
            var bitset = new LockFreeDirtyBitset();
            const int workers = 3;
            const int opsPerWorker = 15;

            long expectedBucket0 = 0;
            long expectedBucket1 = 0;

            for (ushort id = 0; id < 20; id++)
            {
                if (id < 64) expectedBucket0 |= (1L << id);
            }
            for (ushort id = 10; id < 30; id++)
            {
                if (id < 64) expectedBucket0 |= (1L << id);
            }
            for (ushort id = 60; id < 80; id++)
            {
                if (id < 64) expectedBucket0 |= (1L << id);
                else expectedBucket1 |= (1L << (id - 64));
            }

            long accumulatedBucket0 = 0;
            long accumulatedBucket1 = 0;
            int flusherRunning = 1;

            var workerTasks = new Task[workers];

            workerTasks[0] = Task.Run(async () =>
            {
                for (int iter = 0; iter < opsPerWorker; iter++)
                {
                    for (ushort id = 0; id < 20; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            workerTasks[1] = Task.Run(async () =>
            {
                for (int iter = 0; iter < opsPerWorker; iter++)
                {
                    for (ushort id = 10; id < 30; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            workerTasks[2] = Task.Run(async () =>
            {
                for (int iter = 0; iter < opsPerWorker; iter++)
                {
                    for (ushort id = 60; id < 80; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            var flusherTask = Task.Run(async () =>
            {
                while (Volatile.Read(ref flusherRunning) == 1)
                {
                    if (bitset.TryConsumeBucket(0, out long mask0))
                    {
                        InterlockedHelperOr(ref accumulatedBucket0, mask0);
                    }

                    if (bitset.TryConsumeBucket(1, out long mask1))
                    {
                        InterlockedHelperOr(ref accumulatedBucket1, mask1);
                    }

                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerTasks);
            Volatile.Write(ref flusherRunning, 0);
            await flusherTask;

            // Drain any residual bits left in buckets
            if (bitset.TryConsumeBucket(0, out long remaining0))
            {
                InterlockedHelperOr(ref accumulatedBucket0, remaining0);
            }
            if (bitset.TryConsumeBucket(1, out long remaining1))
            {
                InterlockedHelperOr(ref accumulatedBucket1, remaining1);
            }

            Specification.Assert(accumulatedBucket0 == expectedBucket0,
                $"Lost bits in bucket 0! Expected: 0x{expectedBucket0:X16}, Got: 0x{accumulatedBucket0:X16}");
            Specification.Assert(accumulatedBucket1 == expectedBucket1,
                $"Lost bits in bucket 1! Expected: 0x{expectedBucket1:X16}, Got: 0x{accumulatedBucket1:X16}");
        }

        [CoyoteTest]
        public static async Task Coyote_DirtyBitset_MultipleThreadsSameBit_ZeroRaceOrDeadlock()
        {
            var bitset = new LockFreeDirtyBitset();
            const int workers = 4;
            const int opsPerWorker = 20;
            const ushort targetBit = 42;

            long totalTimesObserved = 0;
            int flusherRunning = 1;

            var tasks = new Task[workers];
            for (int w = 0; w < workers; w++)
            {
                tasks[w] = Task.Run(async () =>
                {
                    for (int i = 0; i < opsPerWorker; i++)
                    {
                        bitset.SetDirty(targetBit);
                        await Task.Yield();
                    }
                });
            }

            var flusher = Task.Run(async () =>
            {
                while (Volatile.Read(ref flusherRunning) == 1)
                {
                    if (bitset.TryConsumeBucket(0, out long mask))
                    {
                        if ((mask & (1L << targetBit)) != 0)
                        {
                            Interlocked.Increment(ref totalTimesObserved);
                        }
                    }
                    await Task.Yield();
                }
            });

            await Task.WhenAll(tasks);
            Volatile.Write(ref flusherRunning, 0);
            await flusher;

            if (bitset.TryConsumeBucket(0, out long remaining))
            {
                if ((remaining & (1L << targetBit)) != 0)
                {
                    Interlocked.Increment(ref totalTimesObserved);
                }
            }

            Specification.Assert(totalTimesObserved >= 1, "Target bit was never observed by flusher.");
            Specification.Assert(!bitset.TryConsumeBucket(0, out _), "Bucket 0 should be empty after final drain.");
        }

        [CoyoteTest]
        public static async Task Coyote_SlotBitset_ConcurrentAllocateAndFree_ZeroCollisionsOrLeaks()
        {
            const int capacity = 128;
            var slotBitset = new LockFreeSlotBitset(capacity);
            var occupied = new int[capacity];

            const int workers = 4;
            const int opsPerWorker = 20;
            var tasks = new Task[workers];

            for (int w = 0; w < workers; w++)
            {
                tasks[w] = Task.Run(async () =>
                {
                    for (int i = 0; i < opsPerWorker; i++)
                    {
                        if (slotBitset.TryAllocate(out ushort slot))
                        {
                            int count = Interlocked.Increment(ref occupied[slot]);
                            Specification.Assert(count == 1, $"Slot collision detected on slot {slot}!");

                            await Task.Yield();

                            int released = Interlocked.Decrement(ref occupied[slot]);
                            Specification.Assert(released == 0, $"Slot state corruption on release of slot {slot}!");
                            slotBitset.Free(slot);
                        }
                        await Task.Yield();
                    }
                });
            }

            await Task.WhenAll(tasks);

            Specification.Assert(slotBitset.CountAllocated() == 0, "Slot leak detected: count is not zero after all workers completed.");
            for (int i = 0; i < capacity; i++)
            {
                Specification.Assert(Volatile.Read(ref occupied[i]) == 0, $"Occupancy tracker leaked for slot {i}.");
            }
        }

        [CoyoteTest]
        public static async Task Coyote_DirtyBitset_HardwareScanAtFlush_AllBitsExtractedCorrectly()
        {
            var bitset = new LockFreeDirtyBitset(128);
            const int workers = 3;
            const int iterations = 15;

            long expectedBucket0 = 0;
            long expectedBucket1 = 0;

            for (ushort id = 0; id < 40; id++)
                expectedBucket0 |= (1L << id);
            for (ushort id = 64; id < 100; id++)
                expectedBucket1 |= (1L << (id - 64));

            long scannedBucket0 = 0;
            long scannedBucket1 = 0;
            int flusherRunning = 1;

            var workerTasks = new Task[workers];
            workerTasks[0] = Task.Run(async () =>
            {
                for (int iter = 0; iter < iterations; iter++)
                {
                    for (ushort id = 0; id < 25; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            workerTasks[1] = Task.Run(async () =>
            {
                for (int iter = 0; iter < iterations; iter++)
                {
                    for (ushort id = 20; id < 40; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            workerTasks[2] = Task.Run(async () =>
            {
                for (int iter = 0; iter < iterations; iter++)
                {
                    for (ushort id = 64; id < 100; id++)
                    {
                        bitset.SetDirty(id);
                        await Task.Yield();
                    }
                }
            });

            var flusher = Task.Run(async () =>
            {
                while (Volatile.Read(ref flusherRunning) == 1)
                {
                    for (int b = 0; b < 2; b++)
                    {
                        if (bitset.IsBucketEmpty(b)) continue;
                        if (!bitset.TryConsumeBucket(b, out long mask)) continue;

                        ulong umask = (ulong)mask;
                        while (umask != 0)
                        {
                            int bit = Liminal.Net.Core.BitOperationsCompat.TrailingZeroCount(umask);
                            if (b == 0) InterlockedHelperOr(ref scannedBucket0, 1L << bit);
                            else InterlockedHelperOr(ref scannedBucket1, 1L << bit);
                            umask &= (umask - 1);
                        }
                    }
                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerTasks);
            Volatile.Write(ref flusherRunning, 0);
            await flusher;

            // Final drain
            for (int b = 0; b < 2; b++)
            {
                if (bitset.TryConsumeBucket(b, out long mask))
                {
                    ulong umask = (ulong)mask;
                    while (umask != 0)
                    {
                        int bit = Liminal.Net.Core.BitOperationsCompat.TrailingZeroCount(umask);
                        if (b == 0) InterlockedHelperOr(ref scannedBucket0, 1L << bit);
                        else InterlockedHelperOr(ref scannedBucket1, 1L << bit);
                        umask &= (umask - 1);
                    }
                }
            }

            Specification.Assert(scannedBucket0 == expectedBucket0, $"Bit scan mismatch in bucket 0. Expected: 0x{expectedBucket0:X}, Got: 0x{scannedBucket0:X}");
            Specification.Assert(scannedBucket1 == expectedBucket1, $"Bit scan mismatch in bucket 1. Expected: 0x{expectedBucket1:X}, Got: 0x{scannedBucket1:X}");
        }

        private static void InterlockedHelperOr(ref long target, long mask)
        {
            long current = Volatile.Read(ref target);
            while (true)
            {
                long updated = current | mask;
                long prior = Interlocked.CompareExchange(ref target, updated, current);
                if (prior == current) break;
                current = prior;
            }
        }
    }
}
