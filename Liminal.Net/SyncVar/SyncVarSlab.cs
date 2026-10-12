using Liminal.Net.Core;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    internal sealed class BoundedSlotPool
    {
        private long[] _slots;
        private int _count;
        private readonly object _lock = new();

        public BoundedSlotPool(int capacity = 512)
        {
            _slots = new long[capacity];
        }

        public bool TryPop(out long slot)
        {
            lock (_lock)
            {
                if (_count > 0)
                {
                    slot = _slots[--_count];
                    return true;
                }
            }

            slot = 0;
            return false;
        }

        public bool TryPush(long slot)
        {
            lock (_lock)
            {
                if (_count >= _slots.Length)
                {
                    Array.Resize(ref _slots, _slots.Length * 2);
                }
                _slots[_count++] = slot;
                return true;
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _count = 0;
            }
        }
    }

    public sealed class SyncVarSlab : IDisposable
    {
        public const int MinSlotSize = 64;

        public readonly int PageSize;
        public readonly int MaxPages;

        private readonly byte[][] _pages;
        private int _allocatedBytes;

        private readonly BoundedSlotPool[] _tierPools;
        private readonly int _tierCount;

        public SyncVarSlab(LiminalNetworkConfig config)
        {
            int configuredPageSize = config?.SyncVarMaxPageSize ?? 65536;
            PageSize = Math.Max(MinSlotSize, (int)BitOperationsCompat.RoundUpToPowerOf2((uint)configuredPageSize));
            MaxPages = config?.SyncVarMaxPageCount ?? 256;

            _pages = new byte[MaxPages][];
            _pages[0] = new byte[PageSize];

            _tierCount = BitOperationsCompat.Log2((uint)PageSize) - BitOperationsCompat.Log2((uint)MinSlotSize) + 1;
            _tierPools = new BoundedSlotPool[_tierCount];
            for (int i = 0; i < _tierCount; i++)
            {
                _tierPools[i] = new BoundedSlotPool(512);
            }
        }

        public int GetTierCapacity(int size)
        {
            if (size <= MinSlotSize) return MinSlotSize;
            if (size > PageSize)
                throw new ArgumentException($"[SyncVarSlab] Slot size {size} exceeds PageSize {PageSize}.");

            return (int)BitOperationsCompat.RoundUpToPowerOf2((uint)size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetTierIndex(int tierCapacity)
        {
            return BitOperationsCompat.TrailingZeroCount(tierCapacity) - 6;
        }

        public (int pageIndex, int pageOffset) AllocateSlot(int size)
        {
            return AllocateSlot(size, out _);
        }

        public (int pageIndex, int pageOffset) AllocateSlot(int size, out int actualCapacity)
        {
            int capacity = GetTierCapacity(size);
            actualCapacity = capacity;
            int tierIndex = GetTierIndex(capacity);

            if (tierIndex >= 0 && tierIndex < _tierCount)
            {
                if (_tierPools[tierIndex].TryPop(out long packed))
                {
                    int pageIndex = (int)(packed >> 32);
                    int pageOffset = (int)(packed & 0xFFFFFFFF);
                    return (pageIndex, pageOffset);
                }
            }

            return AllocateFromBump(capacity);
        }

        public void FreeSlot(int pageIndex, int pageOffset, int capacity)
        {
            if (pageIndex < 0 || pageOffset < 0 || capacity < MinSlotSize || capacity > PageSize)
                return;

            int tierCapacity = GetTierCapacity(capacity);
            int tierIndex = GetTierIndex(tierCapacity);

            if (tierIndex >= 0 && tierIndex < _tierCount)
            {
                long packed = ((long)pageIndex << 32) | (uint)pageOffset;
                _tierPools[tierIndex].TryPush(packed);
            }
        }

        private (int pageIndex, int pageOffset) AllocateFromBump(int size)
        {
            while (true)
            {
                int currentAllocated = Volatile.Read(ref _allocatedBytes);
                int pageIndex = currentAllocated / PageSize;
                int pageOffset = currentAllocated % PageSize;

                if (pageIndex >= MaxPages)
                    throw new OutOfMemoryException("[SyncVarSlab] Exceeded maximum slab capacity.");

                if (pageOffset + size > PageSize)
                {
                    int newPageIndex = pageIndex + 1;
                    if (newPageIndex >= MaxPages)
                        throw new OutOfMemoryException("[SyncVarSlab] Exceeded maximum slab capacity.");

                    int nextAllocated = (newPageIndex * PageSize) + size;

                    if (Interlocked.CompareExchange(ref _allocatedBytes, nextAllocated, currentAllocated) == currentAllocated)
                    {
                        EnsurePageAllocated(newPageIndex);
                        return (newPageIndex, 0);
                    }
                }
                else
                {
                    int nextAllocated = currentAllocated + size;
                    if (Interlocked.CompareExchange(ref _allocatedBytes, nextAllocated, currentAllocated) == currentAllocated)
                    {
                        EnsurePageAllocated(pageIndex);
                        return (pageIndex, pageOffset);
                    }
                }
            }
        }

        private void EnsurePageAllocated(int pageIndex)
        {
            if (Volatile.Read(ref _pages[pageIndex]) == null)
            {
                var newPage = new byte[PageSize];
                Interlocked.CompareExchange(ref _pages[pageIndex], newPage, null);
            }
        }

        public byte[] GetPage(int pageIndex) => Volatile.Read(ref _pages[pageIndex]);

        public Span<byte> GetSpan(int pageIndex, int pageOffset, int length)
        {
            var page = Volatile.Read(ref _pages[pageIndex]);
            if (page == null)
            {
                EnsurePageAllocated(pageIndex);
                page = Volatile.Read(ref _pages[pageIndex]);
            }
            return page.AsSpan(pageOffset, length);
        }

        public byte[] ExtractSnapshot(int totalBytes)
        {
            int allocated = Volatile.Read(ref _allocatedBytes);
            int bytesToCopy = Math.Max(totalBytes, allocated);
            byte[] snapshot = new byte[bytesToCopy];
            int copied = 0;
            int pageIndex = 0;

            while (copied < bytesToCopy && pageIndex < MaxPages)
            {
                int toCopy = Math.Min(PageSize, bytesToCopy - copied);
                var page = Volatile.Read(ref _pages[pageIndex]);
                if (page != null)
                {
                    Buffer.BlockCopy(page, 0, snapshot, copied, toCopy);
                }
                copied += toCopy;
                pageIndex++;
            }

            return snapshot;
        }

        public void InitializeFromSnapshot(ReadOnlySpan<byte> snapshot)
        {
            int copied = 0;
            int pageIndex = 0;

            while (copied < snapshot.Length && pageIndex < MaxPages)
            {
                EnsurePageAllocated(pageIndex);
                int toCopy = Math.Min(PageSize, snapshot.Length - copied);
                snapshot.Slice(copied, toCopy).CopyTo(_pages[pageIndex].AsSpan(0, toCopy));
                copied += toCopy;
                pageIndex++;
            }

            for (int i = 0; i < _tierCount; i++)
            {
                _tierPools[i]?.Clear();
            }

            Volatile.Write(ref _allocatedBytes, snapshot.Length);
        }

        public void Reset()
        {
            for (int i = 1; i < MaxPages; i++)
            {
                _pages[i] = null;
            }
            if (_pages[0] == null)
            {
                _pages[0] = new byte[PageSize];
            }
            else
            {
                Array.Clear(_pages[0], 0, _pages[0].Length);
            }
            for (int i = 0; i < _tierCount; i++)
            {
                _tierPools[i]?.Clear();
            }
            Volatile.Write(ref _allocatedBytes, 0);
        }

        public void Dispose()
        {
            for (int i = 0; i < MaxPages; i++)
            {
                _pages[i] = null;
            }
            for (int i = 0; i < _tierCount; i++)
            {
                _tierPools[i]?.Clear();
            }
            _allocatedBytes = 0;
        }
    }

    internal sealed class FixedBufferWriter : IBufferWriter<byte>
    {
        [ThreadStatic]
        private static FixedBufferWriter _threadInstance;
        public static FixedBufferWriter ThreadInstance => _threadInstance ??= new FixedBufferWriter();

        private byte[] _buffer;
        private int _start;
        private int _capacity;
        private int _written;

        public int WrittenCount => _written;

        public void Reset(byte[] buffer, int start, int capacity)
        {
            _buffer = buffer;
            _start = start;
            _capacity = capacity;
            _written = 0;
        }

        public void Advance(int count)
        {
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            return _buffer.AsMemory(_start + _written, _capacity - _written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            return _buffer.AsSpan(_start + _written, _capacity - _written);
        }
    }
}