using System;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.SyncVar.Internal
{
    internal sealed class SyncVarValueCell<T>
    {
        private readonly object _lock = new();
        private readonly T[] _values = new T[2];
        private int _frontIndex = 0;
        private int _valueSeq = 0;
        private uint _version;

        private T _pendingOldValue;
        private T _pendingNewValue;
        private bool _hasPendingValueChanged;

        public SyncVarValueCell(T initialValue)
        {
            _values[0] = initialValue;
            _values[1] = initialValue;
            _frontIndex = 0;
            _version = 0;
        }

        public uint Version => Volatile.Read(ref _version);

        public T Value
        {
            get
            {
                var spinner = new SpinWait();
                while (true)
                {
                    int seq1 = Volatile.Read(ref _valueSeq);
                    if ((seq1 & 1) != 0)
                    {
                        spinner.SpinOnce();
                        continue;
                    }

                    int front = Volatile.Read(ref _frontIndex);
                    T val = _values[front];

                    Thread.MemoryBarrier();

                    int seq2 = Volatile.Read(ref _valueSeq);
                    if (seq1 == seq2)
                    {
                        return val;
                    }

                    spinner.SpinOnce();
                }
            }
        }

        public (T Value, uint Version) GetSnapshot()
        {
            var spinner = new SpinWait();
            while (true)
            {
                int seq1 = Volatile.Read(ref _valueSeq);
                if ((seq1 & 1) != 0)
                {
                    spinner.SpinOnce();
                    continue;
                }

                int front = Volatile.Read(ref _frontIndex);
                T val = _values[front];
                uint ver = Volatile.Read(ref _version);

                Thread.MemoryBarrier();

                int seq2 = Volatile.Read(ref _valueSeq);
                if (seq1 == seq2)
                {
                    return (val, ver);
                }

                spinner.SpinOnce();
            }
        }

        public bool TryPrepareUpdate(T newValue, bool force, out uint nextVersion)
        {
            lock (_lock)
            {
                int currentFront = Volatile.Read(ref _frontIndex);
                if (!force && EqualityComparer<T>.Default.Equals(_values[currentFront], newValue))
                {
                    nextVersion = Volatile.Read(ref _version);
                    return false;
                }

                nextVersion = Volatile.Read(ref _version) + 1;
                return true;
            }
        }

        public void CommitUpdate(T newValue, uint newVersion, bool deferEvent, out T oldValue, out bool fireEvent)
        {
            lock (_lock)
            {
                int currentFront = Volatile.Read(ref _frontIndex);
                int back = 1 - currentFront;

                Interlocked.Increment(ref _valueSeq);
                _values[back] = newValue;

                if (_hasPendingValueChanged)
                {
                    oldValue = _pendingOldValue;
                }
                else
                {
                    oldValue = _values[currentFront];
                }

                if (deferEvent)
                {
                    _pendingOldValue = oldValue;
                    _pendingNewValue = newValue;
                    _hasPendingValueChanged = true;
                    fireEvent = false;
                }
                else
                {
                    _hasPendingValueChanged = false;
                    _pendingOldValue = default;
                    _pendingNewValue = default;
                    fireEvent = true;
                }

                Volatile.Write(ref _frontIndex, back);
                Volatile.Write(ref _version, newVersion);
                Interlocked.Increment(ref _valueSeq);
            }
        }

        public bool TryUpdate(T newValue, bool force, bool deferEvent, out T oldValue, out bool fireEvent, out uint newVersion)
        {
            lock (_lock)
            {
                if (!TryPrepareUpdate(newValue, force, out newVersion))
                {
                    oldValue = default;
                    fireEvent = false;
                    return false;
                }

                CommitUpdate(newValue, newVersion, deferEvent, out oldValue, out fireEvent);
                return true;
            }
        }

        public bool TryApplyRemote(T deserialized, uint newVersion, bool force, bool deferEvent, out T oldValue, out bool fireEvent)
        {
            lock (_lock)
            {
                uint currentVer = Volatile.Read(ref _version);
                if (!force && newVersion <= currentVer && currentVer != 0)
                {
                    oldValue = default;
                    fireEvent = false;
                    return false;
                }

                int currentFront = Volatile.Read(ref _frontIndex);
                int back = 1 - currentFront;

                Interlocked.Increment(ref _valueSeq);
                _values[back] = deserialized;

                if (_hasPendingValueChanged)
                {
                    oldValue = _pendingOldValue;
                }
                else
                {
                    oldValue = _values[currentFront];
                }

                if (deferEvent)
                {
                    _pendingOldValue = oldValue;
                    _pendingNewValue = deserialized;
                    _hasPendingValueChanged = true;
                    fireEvent = false;
                }
                else
                {
                    _hasPendingValueChanged = false;
                    _pendingOldValue = default;
                    _pendingNewValue = default;
                    fireEvent = true;
                }

                Volatile.Write(ref _frontIndex, back);
                Volatile.Write(ref _version, newVersion);
                Interlocked.Increment(ref _valueSeq);
                return true;
            }
        }

        public void Clear(out T oldValue, out bool fireEvent)
        {
            lock (_lock)
            {
                int front = Volatile.Read(ref _frontIndex);
                oldValue = _values[front];

                Interlocked.Increment(ref _valueSeq);
                _values[0] = default;
                _values[1] = default;
                _frontIndex = 0;
                Volatile.Write(ref _version, 0);
                Interlocked.Increment(ref _valueSeq);

                _hasPendingValueChanged = false;
                _pendingOldValue = default;
                _pendingNewValue = default;
                fireEvent = !EqualityComparer<T>.Default.Equals(oldValue, default);
            }
        }

        public bool TryFirePendingValue(out T oldValue, out T newValue)
        {
            lock (_lock)
            {
                if (!_hasPendingValueChanged)
                {
                    oldValue = default;
                    newValue = default;
                    return false;
                }

                oldValue = _pendingOldValue;
                newValue = _pendingNewValue;
                _hasPendingValueChanged = false;
                _pendingOldValue = default;
                _pendingNewValue = default;
                return true;
            }
        }

        public void ClearPendingValue()
        {
            lock (_lock)
            {
                _hasPendingValueChanged = false;
                _pendingOldValue = default;
                _pendingNewValue = default;
            }
        }
    }
}
