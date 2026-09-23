using System;
using System.Collections.Generic;
using System.Text;

namespace Liminal.Net.Core.Telemetry
{
    public class TickPayloadSizeDiagnostics : IDisposable
    {
        private readonly object _bufferLock = new();
        private PacketPayloadSizeEntry[] _activeEntries = new PacketPayloadSizeEntry[32];
        private int _activeCount = 0;

        private PacketPayloadSizeEntry[] _drainEntries = new PacketPayloadSizeEntry[32];
        private int _drainCount = 0;

        private readonly object _snapshotLock = new();
        private LiminalSessionManager _sessionManager;
        private LiminalTicker _ticker;

        private int currentIndex = 0;
        private TickPayloadSizeSnapshot[] _snapshots;

        public TickPayloadSizeDiagnostics(uint avgTickWindow, LiminalTicker ticker, LiminalSessionManager sessionManager)
        {
            _sessionManager = sessionManager;
            _ticker = ticker;

            uint window = Math.Max(1u, avgTickWindow);
            _snapshots = new TickPayloadSizeSnapshot[window];
            for (int i = 0; i < _snapshots.Length; i++)
            {
                _snapshots[i] = new TickPayloadSizeSnapshot();
            }

            if (_ticker != null)
            {
                _ticker.OnTick += HandleTick;
            }

            if (_sessionManager != null)
            {
                _sessionManager.OnPacketBuffered += HandlePacketBuffered;
            }
        }

        public void Dispose()
        {
            if (_sessionManager != null)
            {
                _sessionManager.OnPacketBuffered -= HandlePacketBuffered;
            }

            if (_ticker != null)
            {
                _ticker.OnTick -= HandleTick;
            }
        }

        private void HandleTick()
        {
            PacketPayloadSizeEntry[] toDrain;
            int toDrainCount;

            lock (_bufferLock)
            {
                toDrain = _activeEntries;
                toDrainCount = _activeCount;

                _activeEntries = _drainEntries;
                _activeCount = 0;

                _drainEntries = toDrain;
                _drainCount = toDrainCount;
            }

            lock (_snapshotLock)
            {
                var currentSnapshot = _snapshots[currentIndex];
                currentSnapshot.CopyFrom(toDrain, toDrainCount);
                currentIndex = (currentIndex + 1) % _snapshots.Length;
            }

            lock (_bufferLock)
            {
                _drainCount = 0;
            }
        }

        private void HandlePacketBuffered(ushort packetId, Memory<byte> memory)
        {
            int length = memory.Length;
            lock (_bufferLock)
            {
                for (int i = 0; i < _activeCount; i++)
                {
                    if (_activeEntries[i].packetId == packetId)
                    {
                        _activeEntries[i].size += length;
                        return;
                    }
                }

                if (_activeCount == _activeEntries.Length)
                {
                    Array.Resize(ref _activeEntries, _activeEntries.Length * 2);
                }

                _activeEntries[_activeCount++] = new PacketPayloadSizeEntry(packetId, length);
            }
        }

        public TickPacketSizeTelemetrySnapshot GetLatestTickSnapshot()
        {
            lock (_snapshotLock)
            {
                int previousTickIndex = (currentIndex + _snapshots.Length - 1) % _snapshots.Length;
                return new TickPacketSizeTelemetrySnapshot(FetchSnapshotTelemetryString(_snapshots[previousTickIndex]));
            }
        }

        public TickPacketSizeTelemetrySnapshot GetAverageTickSnapshot()
        {
            lock (_snapshotLock)
            {
                TickPayloadSizeSnapshot snapshot = GetMergedSnapshot(_snapshots);
                return new TickPacketSizeTelemetrySnapshot(FetchSnapshotTelemetryString(snapshot));
            }
        }

        public string FetchSnapshotTelemetryString(TickPayloadSizeSnapshot snapshot)
        {
            var sb = new StringBuilder();

            if (snapshot == null) return sb.ToString();

            var snapshots = snapshot.GetSnapshots();
            if (snapshots.Length == 0) return sb.ToString();

            long totalSize = 0;
            for (int i = 0; i < snapshots.Length; i++)
            {
                totalSize += snapshots[i].size;
            }

            var items = new PacketPayloadSizeEntry[snapshots.Length];
            snapshots.CopyTo(items);
            Array.Sort(items, (a, b) => b.size.CompareTo(a.size));

            sb.AppendLine($"Total Bytes: {totalSize}");

            for (int i = 0; i < items.Length; i++)
            {
                var item = items[i];
                double percentage = totalSize > 0 ? (double)item.size / totalSize * 100.0 : 0.0;
                bool success = LiminalPacketLibrary.TryGetType(item.packetId, out Type packetType);

                if (!success)
                {
                    sb.AppendLine($"Unknown Packet ID {item.packetId} ({item.size} bytes): {percentage:F2}%");
                    continue;
                }

                sb.AppendLine($"Packet ID {item.packetId}, Packet Type {packetType.Name} ({item.size} bytes): {percentage:F2}%");
            }

            return sb.ToString();
        }

        public TickPayloadSizeSnapshot GetMergedSnapshot(TickPayloadSizeSnapshot[] snapshots)
        {
            var mergedSnapshot = new TickPayloadSizeSnapshot();
            if (snapshots == null || snapshots.Length == 0) return mergedSnapshot;

            var totals = new Dictionary<ushort, (long totalSize, int count)>();

            for (int s = 0; s < snapshots.Length; s++)
            {
                var snapshot = snapshots[s];
                if (snapshot == null) continue;
                var entries = snapshot.GetSnapshots();
                for (int i = 0; i < entries.Length; i++)
                {
                    var item = entries[i];
                    if (!totals.TryGetValue(item.packetId, out var current))
                    {
                        totals[item.packetId] = (item.size, 1);
                    }
                    else
                    {
                        totals[item.packetId] = (current.totalSize + item.size, current.count + 1);
                    }
                }
            }

            foreach (var kvp in totals)
            {
                int avgSize = (int)(kvp.Value.totalSize / kvp.Value.count);
                mergedSnapshot.AddSnapshot(kvp.Key, avgSize);
            }

            return mergedSnapshot;
        }
    }

    public struct PacketPayloadSizeEntry
    {
        public ushort packetId;
        public int size;

        public PacketPayloadSizeEntry(ushort packetId, int size)
        {
            this.packetId = packetId;
            this.size = size;
        }

        public void Deconstruct(out ushort packetId, out int size)
        {
            packetId = this.packetId;
            size = this.size;
        }

        public static implicit operator (ushort packetId, int size)(PacketPayloadSizeEntry entry) =>
            (entry.packetId, entry.size);

        public static implicit operator PacketPayloadSizeEntry((ushort packetId, int size) tuple) =>
            new PacketPayloadSizeEntry(tuple.packetId, tuple.size);
    }

    public class TickPayloadSizeSnapshot
    {
        private PacketPayloadSizeEntry[] _entries;
        private int _count;

        public TickPayloadSizeSnapshot(int initialCapacity = 32)
        {
            _entries = new PacketPayloadSizeEntry[initialCapacity];
            _count = 0;
        }

        public int Count => _count;

        public ReadOnlySpan<PacketPayloadSizeEntry> Entries => _entries.AsSpan(0, _count);

        public void CopyFrom(PacketPayloadSizeEntry[] source, int count)
        {
            if (_entries.Length < count)
            {
                Array.Resize(ref _entries, Math.Max(count, _entries.Length * 2));
            }

            Array.Copy(source, _entries, count);
            _count = count;
        }

        public void AddSnapshot((ushort packetId, int size) snapshot)
        {
            AddSnapshot(snapshot.packetId, snapshot.size);
        }

        public void AddSnapshot(ushort packetId, int size)
        {
            if (_count == _entries.Length)
            {
                Array.Resize(ref _entries, _entries.Length * 2);
            }
            _entries[_count++] = new PacketPayloadSizeEntry(packetId, size);
        }

        public void Clear()
        {
            _count = 0;
        }

        public ReadOnlySpan<PacketPayloadSizeEntry> GetSnapshots() => Entries;
    }

    public struct TickPacketSizeTelemetrySnapshot
    {
        public string TelemetryData;

        public TickPacketSizeTelemetrySnapshot(string telemetryData)
        {
            TelemetryData = telemetryData;
        }
    }
}
