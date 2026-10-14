using System;
using System.Collections.Generic;

namespace Liminal.Net.SyncVar.Internal
{
    //Process wide weak list of every syncvar, managers walk it on creation and restart to adopt the ones with no live manager
    internal static class SyncVarDirectory
    {
        private const int MinCompactThreshold = 64;

        private static readonly object _lock = new();
        private static readonly List<WeakReference<ISyncVarInternal>> _entries = new();
        private static int _compactAt = MinCompactThreshold;

        public static void Add(ISyncVarInternal syncVar)
        {
            lock (_lock)
            {
                if (_entries.Count >= _compactAt)
                {
                    CompactLocked(null);
                    _compactAt = Math.Max(MinCompactThreshold, _entries.Count * 2);
                }
                _entries.Add(new WeakReference<ISyncVarInternal>(syncVar));
            }
        }

        //Live syncvars oldest first
        public static void Snapshot(List<ISyncVarInternal> destination)
        {
            destination.Clear();
            lock (_lock)
            {
                CompactLocked(destination);
            }
        }

        //Coyote runs many iterations in one process so leftovers from one iteration cant get adopted by the next
        internal static void ResetForTesting()
        {
            lock (_lock)
            {
                _entries.Clear();
                _compactAt = MinCompactThreshold;
            }
        }

        internal static int EntryCount
        {
            get
            {
                lock (_lock) return _entries.Count;
            }
        }

        private static void CompactLocked(List<ISyncVarInternal> live)
        {
            int write = 0;
            for (int i = 0; i < _entries.Count; i++)
            {
                var entry = _entries[i];
                if (entry.TryGetTarget(out var syncVar) && !syncVar.IsDisposed)
                {
                    _entries[write++] = entry;
                    live?.Add(syncVar);
                }
            }
            _entries.RemoveRange(write, _entries.Count - write);
        }
    }
}
