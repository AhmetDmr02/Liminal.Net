using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    public sealed class Utf8TokenRegistry
    {
        private struct Entry
        {
            public ulong Hash;
            public byte[] TokenBytes;
            public string Token;
            public ISyncVarInternal Value;
            public int Next;
        }

        private sealed class Table
        {
            public readonly int[] Buckets;
            public readonly Entry[] Entries;
            public readonly int Count;

            public Table(int[] buckets, Entry[] entries, int count)
            {
                Buckets = buckets;
                Entries = entries;
                Count = count;
            }
        }

        private volatile Table _table;
        private readonly object _writeLock = new();

        public int Count => _table.Count;

        public Utf8TokenRegistry(int initialCapacity = 64)
        {
            int size = GetPrime(initialCapacity);
            var buckets = new int[size];
            for (int i = 0; i < size; i++) buckets[i] = -1;
            var entries = new Entry[size];
            _table = new Table(buckets, entries, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ComputeHash(ReadOnlySpan<byte> bytes)
        {
            ulong hash = 14695981039346656037UL;
            for (int i = 0; i < bytes.Length; i++)
            {
                hash ^= bytes[i];
                hash *= 1099511628211UL;
            }
            return hash;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ComputeHash(string str)
        {
            if (str == null) return 0;
            int maxBytes = (str.Length + 1) * 3;
            if (maxBytes <= 256)
            {
                Span<byte> buffer = stackalloc byte[256];
                int written = Encoding.UTF8.GetBytes(str, buffer);
                return ComputeHash(buffer.Slice(0, written));
            }
            return ComputeHash(Encoding.UTF8.GetBytes(str));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(ReadOnlySpan<byte> tokenSpan, out ISyncVarInternal syncVar)
        {
            var table = _table;
            var buckets = table.Buckets;
            var entries = table.Entries;

            ulong hash = ComputeHash(tokenSpan);
            int bucket = (int)(hash % (ulong)buckets.Length);
            int i = Volatile.Read(ref buckets[bucket]);

            while (i >= 0 && i < entries.Length)
            {
                ref readonly var entry = ref entries[i];
                if (entry.Hash == hash && tokenSpan.SequenceEqual(entry.TokenBytes))
                {
                    syncVar = entry.Value;
                    return true;
                }
                i = entry.Next;
            }

            syncVar = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(string token, out ISyncVarInternal syncVar)
        {
            if (token == null)
            {
                syncVar = null;
                return false;
            }

            int maxBytes = (token.Length + 1) * 3;
            if (maxBytes <= 256)
            {
                Span<byte> buffer = stackalloc byte[256];
                int written = Encoding.UTF8.GetBytes(token, buffer);
                return TryGet(buffer.Slice(0, written), out syncVar);
            }

            return TryGet(Encoding.UTF8.GetBytes(token), out syncVar);
        }

        public bool TryAdd(ISyncVarInternal syncVar)
        {
            if (syncVar == null || syncVar.Token == null) return false;

            lock (_writeLock)
            {
                var currentTable = _table;
                byte[] tokenBytes = syncVar.TokenBytes ?? Encoding.UTF8.GetBytes(syncVar.Token);
                ulong hash = ComputeHash(tokenBytes);
                int bucket = (int)(hash % (ulong)currentTable.Buckets.Length);

                // Check if existing
                for (int i = currentTable.Buckets[bucket]; i >= 0; i = currentTable.Entries[i].Next)
                {
                    if (currentTable.Entries[i].Hash == hash && ((ReadOnlySpan<byte>)tokenBytes).SequenceEqual(currentTable.Entries[i].TokenBytes))
                    {
                        var updatedEntries = new Entry[currentTable.Entries.Length];
                        Array.Copy(currentTable.Entries, updatedEntries, currentTable.Entries.Length);
                        updatedEntries[i].Value = syncVar;
                        updatedEntries[i].TokenBytes = tokenBytes;
                        Volatile.Write(ref _table, new Table(currentTable.Buckets, updatedEntries, currentTable.Count));
                        return true;
                    }
                }

                int currentCount = currentTable.Count;
                int[] newBuckets;
                Entry[] newEntries;

                if (currentCount >= currentTable.Entries.Length)
                {
                    int newSize = GetPrime(currentCount * 2);
                    newBuckets = new int[newSize];
                    for (int i = 0; i < newSize; i++) newBuckets[i] = -1;
                    newEntries = new Entry[newSize];
                    Array.Copy(currentTable.Entries, newEntries, currentCount);

                    for (int i = 0; i < currentCount; i++)
                    {
                        int b = (int)(newEntries[i].Hash % (ulong)newSize);
                        newEntries[i].Next = newBuckets[b];
                        newBuckets[b] = i;
                    }
                    bucket = (int)(hash % (ulong)newSize);
                }
                else
                {
                    newBuckets = new int[currentTable.Buckets.Length];
                    Array.Copy(currentTable.Buckets, newBuckets, currentTable.Buckets.Length);
                    newEntries = new Entry[currentTable.Entries.Length];
                    Array.Copy(currentTable.Entries, newEntries, currentTable.Entries.Length);
                }

                int index = currentCount;
                newEntries[index].Hash = hash;
                newEntries[index].TokenBytes = tokenBytes;
                newEntries[index].Token = syncVar.Token;
                newEntries[index].Value = syncVar;
                newEntries[index].Next = newBuckets[bucket];
                newBuckets[bucket] = index;

                Volatile.Write(ref _table, new Table(newBuckets, newEntries, currentCount + 1));
                return true;
            }
        }

        public bool TryRemove(string token, out ISyncVarInternal removed)
        {
            removed = null;
            if (token == null) return false;

            lock (_writeLock)
            {
                var currentTable = _table;
                byte[] tokenBytes = Encoding.UTF8.GetBytes(token);
                ulong hash = ComputeHash(tokenBytes);
                int bucket = (int)(hash % (ulong)currentTable.Buckets.Length);

                int targetIndex = -1;
                for (int i = currentTable.Buckets[bucket]; i >= 0; i = currentTable.Entries[i].Next)
                {
                    if (currentTable.Entries[i].Hash == hash && ((ReadOnlySpan<byte>)tokenBytes).SequenceEqual(currentTable.Entries[i].TokenBytes))
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0) return false;

                removed = currentTable.Entries[targetIndex].Value;

                int[] newBuckets = new int[currentTable.Buckets.Length];
                for (int i = 0; i < newBuckets.Length; i++) newBuckets[i] = -1;
                Entry[] newEntries = new Entry[currentTable.Entries.Length];
                int newCount = 0;

                for (int i = 0; i < currentTable.Count; i++)
                {
                    if (i == targetIndex) continue;

                    int newIdx = newCount++;
                    newEntries[newIdx] = currentTable.Entries[i];
                    int b = (int)(newEntries[newIdx].Hash % (ulong)newBuckets.Length);
                    newEntries[newIdx].Next = newBuckets[b];
                    newBuckets[b] = newIdx;
                }

                Volatile.Write(ref _table, new Table(newBuckets, newEntries, newCount));
                return true;
            }
        }

        public void GetAll(List<ISyncVarInternal> destination)
        {
            if (destination == null) return;
            destination.Clear();

            var table = _table;
            for (int i = 0; i < table.Count; i++)
            {
                if (table.Entries[i].Value != null)
                {
                    destination.Add(table.Entries[i].Value);
                }
            }
        }

        public void Clear()
        {
            lock (_writeLock)
            {
                int size = GetPrime(64);
                var buckets = new int[size];
                for (int i = 0; i < size; i++) buckets[i] = -1;
                var entries = new Entry[size];
                Volatile.Write(ref _table, new Table(buckets, entries, 0));
            }
        }

        private static int GetPrime(int min)
        {
            int[] primes = { 67, 131, 257, 521, 1031, 2053, 4099, 8209, 16411, 32771, 65537 };
            foreach (int p in primes)
            {
                if (p >= min) return p;
            }
            return min | 1;
        }
    }
}
