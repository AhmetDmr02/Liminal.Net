using Liminal.Net.Core;
using System;
using System.Buffers;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    public sealed class SyncVarSlab : IDisposable
    {
        public readonly int PageSize;
        public readonly int MaxPages;

        private readonly byte[][] _pages;
        private int _allocatedBytes;

        public SyncVarSlab(LiminalNetworkConfig config)
        {
            PageSize = config?.SyncVarMaxPageSize ?? 65536;
            MaxPages = config?.SyncVarMaxPageCount ?? 256;

            _pages = new byte[MaxPages][];
            _pages[0] = new byte[PageSize];
        }

        public (int pageIndex, int pageOffset) AllocateSlot(int size)
        {
            if (size > PageSize)
                throw new ArgumentException($"[SyncVarSlab] Slot size {size} exceeds PageSize {PageSize}.");

            while (true)
            {
                int currentAllocated = Volatile.Read(ref _allocatedBytes);
                int pageIndex = currentAllocated / PageSize;
                int pageOffset = currentAllocated % PageSize;

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
            return _pages[pageIndex].AsSpan(pageOffset, length);
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

            Volatile.Write(ref _allocatedBytes, snapshot.Length);
        }

        public void Dispose()
        {
            for (int i = 0; i < MaxPages; i++)
            {
                _pages[i] = null;
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