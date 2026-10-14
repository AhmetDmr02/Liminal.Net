using System;
using System.Threading;

namespace Liminal.Net.SyncVar.Internal
{
    internal enum WireWriteResult
    {
        Written,
        NoSlab,
        Stale,
        Disposed
    }

    internal sealed class SyncVarWireBuffer : IDisposable
    {
        private volatile SyncVarSlab _slab;
        private readonly object _writeLock = new();

        private readonly (int PageIndex, int PageOffset)[] _slots = new (int, int)[2];
        private readonly int[] _slotCapacities = new int[2];
        private readonly int[] _slotLengths = new int[2];
        private readonly uint[] _slotVersions = new uint[2];

        private int _frontIndex = -1;
        private int _isDisposed = 0;
        private int _isClearing = 0;
        private int _readers0 = 0;
        private int _readers1 = 0;

        public SyncVarWireBuffer(SyncVarSlab slab)
        {
            _slab = slab;
            _slots[0] = (-1, -1);
            _slots[1] = (-1, -1);
        }

        public void UpdateSlab(SyncVarSlab slab)
        {
            lock (_writeLock)
            {
                if (Volatile.Read(ref _isDisposed) != 0) return;

                Volatile.Write(ref _isClearing, 1);
                Thread.MemoryBarrier();

                var spinner = new SpinWait();
                while (Volatile.Read(ref _readers0) > 0 || Volatile.Read(ref _readers1) > 0)
                {
                    spinner.SpinOnce();
                }

                if (_slab != slab)
                {
                    if (_slab != null)
                    {
                        if (_slotCapacities[0] > 0 && _slots[0].PageIndex >= 0)
                        {
                            _slab.FreeSlot(_slots[0].PageIndex, _slots[0].PageOffset, _slotCapacities[0]);
                        }
                        if (_slotCapacities[1] > 0 && _slots[1].PageIndex >= 0)
                        {
                            _slab.FreeSlot(_slots[1].PageIndex, _slots[1].PageOffset, _slotCapacities[1]);
                        }
                    }

                    _slots[0] = (-1, -1);
                    _slots[1] = (-1, -1);
                    _slotCapacities[0] = 0;
                    _slotCapacities[1] = 0;
                    _slotLengths[0] = 0;
                    _slotLengths[1] = 0;
                    _slotVersions[0] = 0;
                    _slotVersions[1] = 0;
                    Volatile.Write(ref _frontIndex, -1);
                    _slab = slab;
                }

                Volatile.Write(ref _isClearing, 0);
            }
        }

        public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        public (int PageIndex, int PageOffset) ActiveSlotCoordinates
        {
            get
            {
                lock (_writeLock)
                {
                    int f = Volatile.Read(ref _frontIndex);
                    return (f >= 0 && f <= 1) ? _slots[f] : (-1, -1);
                }
            }
        }

        public int Length
        {
            get
            {
                int f = Volatile.Read(ref _frontIndex);
                return (f >= 0 && f <= 1) ? _slotLengths[f] : 0;
            }
        }

        public void AllocateInitialSlots(int size)
        {
            lock (_writeLock)
            {
                var slab = _slab;
                if (slab == null || Volatile.Read(ref _isDisposed) != 0) return;

                if (_slots[0].PageIndex >= 0)
                {
                    return;
                }

                _slots[0] = slab.AllocateSlot(size, out int actual0);
                _slotCapacities[0] = actual0;
                _slotLengths[0] = size;
                _slots[1] = slab.AllocateSlot(size, out int actual1);
                _slotCapacities[1] = actual1;
                _slotLengths[1] = size;
                Volatile.Write(ref _frontIndex, 0);
            }
        }

        public bool AllocateAndWriteInitial(ReadOnlySpan<byte> initialBytes)
        {
            lock (_writeLock)
            {
                var slab = _slab;
                if (slab == null || Volatile.Read(ref _isDisposed) != 0) return false;

                if (_slots[0].PageIndex >= 0)
                {
                    return true;
                }

                int size = initialBytes.Length;
                _slots[0] = slab.AllocateSlot(size, out int actual0);
                _slotCapacities[0] = actual0;
                _slotLengths[0] = size;
                _slots[1] = slab.AllocateSlot(size, out int actual1);
                _slotCapacities[1] = actual1;
                _slotLengths[1] = size;

                if (_slots[0].PageIndex < 0 || _slots[0].PageOffset < 0)
                {
                    return false;
                }

                initialBytes.CopyTo(slab.GetSpan(_slots[0].PageIndex, _slots[0].PageOffset, size));
                Volatile.Write(ref _frontIndex, 0);
                return true;
            }
        }

        public WireWriteResult WriteBytes(ReadOnlySpan<byte> bytes, uint version, bool force = false)
        {
            lock (_writeLock)
            {
                var slab = _slab;
                if (Volatile.Read(ref _isDisposed) != 0) return WireWriteResult.Disposed;
                if (slab == null) return WireWriteResult.NoSlab;

                int currentFront = Volatile.Read(ref _frontIndex);
                if (!force && currentFront >= 0 && version < _slotVersions[currentFront])
                {
                    return WireWriteResult.Stale;
                }

                int backIndex = (currentFront == -1 || currentFront == 1) ? 0 : 1;
                int length = bytes.Length;

                var spinner = new SpinWait();
                if (backIndex == 0)
                {
                    while (Volatile.Read(ref _readers0) > 0)
                    {
                        spinner.SpinOnce();
                    }
                }
                else
                {
                    while (Volatile.Read(ref _readers1) > 0)
                    {
                        spinner.SpinOnce();
                    }
                }

                if (length > _slotCapacities[backIndex])
                {
                    int oldCap = _slotCapacities[backIndex];
                    var oldSlot = _slots[backIndex];

                    _slots[backIndex] = slab.AllocateSlot(length, out int actualCapacity);
                    _slotCapacities[backIndex] = actualCapacity;

                    if (oldCap > 0 && oldSlot.PageIndex >= 0)
                    {
                        slab.FreeSlot(oldSlot.PageIndex, oldSlot.PageOffset, oldCap);
                    }
                }

                var (page, offset) = _slots[backIndex];
                bytes.CopyTo(slab.GetSpan(page, offset, length));

                _slotLengths[backIndex] = length;
                _slotVersions[backIndex] = version;
                Volatile.Write(ref _frontIndex, backIndex);
                return WireWriteResult.Written;
            }
        }

        public bool TryBeginRead(out int slotIndex, out int pageIndex, out int pageOffset, out int length, out uint version)
        {
            slotIndex = -1;
            pageIndex = -1;
            pageOffset = -1;
            length = 0;
            version = 0;

            if (_slab == null) return false;

            var spinner = new SpinWait();
            while (true)
            {
                if (Volatile.Read(ref _isDisposed) != 0 || Volatile.Read(ref _isClearing) != 0)
                {
                    return false;
                }

                int front = Volatile.Read(ref _frontIndex);
                if (front < 0 || front > 1)
                {
                    return false;
                }

                if (front == 0)
                    Interlocked.Increment(ref _readers0);
                else
                    Interlocked.Increment(ref _readers1);

                Thread.MemoryBarrier();

                if (Volatile.Read(ref _isDisposed) != 0 ||
                    Volatile.Read(ref _isClearing) != 0 ||
                    Volatile.Read(ref _frontIndex) != front ||
                    _slotCapacities[front] <= 0)
                {
                    if (front == 0)
                        Interlocked.Decrement(ref _readers0);
                    else
                        Interlocked.Decrement(ref _readers1);

                    if (Volatile.Read(ref _isDisposed) != 0 || Volatile.Read(ref _isClearing) != 0)
                    {
                        return false;
                    }

                    spinner.SpinOnce();
                    continue;
                }

                var slot = _slots[front];
                if (slot.PageIndex < 0)
                {
                    if (front == 0)
                        Interlocked.Decrement(ref _readers0);
                    else
                        Interlocked.Decrement(ref _readers1);
                    return false;
                }

                slotIndex = front;
                pageIndex = slot.PageIndex;
                pageOffset = slot.PageOffset;
                length = _slotLengths[front];
                version = _slotVersions[front];
                return true;
            }
        }

        public void EndRead(int slotIndex)
        {
            if (slotIndex == 0)
            {
                Interlocked.Decrement(ref _readers0);
            }
            else if (slotIndex == 1)
            {
                Interlocked.Decrement(ref _readers1);
            }
        }

        public ReadOnlySpan<byte> GetSpan(int pageIndex, int pageOffset, int length)
        {
            var slab = _slab;
            if (slab == null || pageIndex < 0 || pageOffset < 0 || length <= 0)
            {
                return ReadOnlySpan<byte>.Empty;
            }
            return slab.GetSpan(pageIndex, pageOffset, length);
        }

        public void Clear()
        {
            lock (_writeLock)
            {
                if (_slab == null || Volatile.Read(ref _isDisposed) != 0) return;

                Volatile.Write(ref _isClearing, 1);
                Thread.MemoryBarrier();

                var spinner = new SpinWait();
                while (Volatile.Read(ref _readers0) > 0 || Volatile.Read(ref _readers1) > 0)
                {
                    spinner.SpinOnce();
                }

                if (_slotCapacities[0] > 0 && _slots[0].PageIndex >= 0 && _slab != null)
                {
                    _slab.FreeSlot(_slots[0].PageIndex, _slots[0].PageOffset, _slotCapacities[0]);
                }
                if (_slotCapacities[1] > 0 && _slots[1].PageIndex >= 0 && _slab != null)
                {
                    _slab.FreeSlot(_slots[1].PageIndex, _slots[1].PageOffset, _slotCapacities[1]);
                }

                _slots[0] = (-1, -1);
                _slots[1] = (-1, -1);
                _slotCapacities[0] = 0;
                _slotCapacities[1] = 0;
                _slotLengths[0] = 0;
                _slotLengths[1] = 0;
                _slotVersions[0] = 0;
                _slotVersions[1] = 0;
                Volatile.Write(ref _frontIndex, -1);

                Volatile.Write(ref _isClearing, 0);
            }
        }

        public void Dispose()
        {
            lock (_writeLock)
            {
                if (Volatile.Read(ref _isDisposed) != 0) return;
                Volatile.Write(ref _isDisposed, 1);
                Thread.MemoryBarrier();

                var spinner = new SpinWait();
                while (Volatile.Read(ref _readers0) > 0 || Volatile.Read(ref _readers1) > 0)
                {
                    spinner.SpinOnce();
                }

                if (_slab != null)
                {
                    if (_slotCapacities[0] > 0 && _slots[0].PageIndex >= 0)
                    {
                        _slab.FreeSlot(_slots[0].PageIndex, _slots[0].PageOffset, _slotCapacities[0]);
                    }
                    if (_slotCapacities[1] > 0 && _slots[1].PageIndex >= 0)
                    {
                        _slab.FreeSlot(_slots[1].PageIndex, _slots[1].PageOffset, _slotCapacities[1]);
                    }
                }

                _slots[0] = (-1, -1);
                _slots[1] = (-1, -1);
                _slotCapacities[0] = 0;
                _slotCapacities[1] = 0;
                _slotLengths[0] = 0;
                _slotLengths[1] = 0;
                _slotVersions[0] = 0;
                _slotVersions[1] = 0;
                Volatile.Write(ref _frontIndex, -1);
            }
        }
    }
}
