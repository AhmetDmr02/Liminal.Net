using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Liminal.Net.Core.Telemetry
{
    public class TickPayloadSizeDiagnostics : IDisposable
    {
        private ConcurrentDictionary<ushort, int> _payloadSizeMap = new ConcurrentDictionary<ushort, int>();

        private LiminalSessionManager _sessionManager;
        private LiminalTicker _ticker;

        private int currentIndex = 0;
        private TickPayloadSizeSnapshot[] _snapshots;
        public TickPayloadSizeDiagnostics(uint avgTickWindow, LiminalTicker ticker, LiminalSessionManager sessionManager)
        {
            _sessionManager = sessionManager;

            _ticker = ticker;

            _snapshots = new TickPayloadSizeSnapshot[avgTickWindow];

            _ticker.OnTick += HandleTick;

            sessionManager.OnPacketBuffered += HandlePacketBuffered;
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
            while (_payloadSizeMap.Count > 0)
            {
                var key = _payloadSizeMap.Keys.First();
                _payloadSizeMap.TryRemove(key, out int size);

                if(_snapshots[currentIndex] == null) 
                    _snapshots[currentIndex] = new TickPayloadSizeSnapshot();

                _snapshots[currentIndex].AddSnapshot((key, size));
            }

            currentIndex = (currentIndex + 1) % _snapshots.Length;
        }

        private void HandlePacketBuffered(ushort packetId, Memory<byte> memory)
        {
             _payloadSizeMap.AddOrUpdate(packetId, memory.Length, (k, v) => memory.Length + v);
        }

        public TickPacketSizeTelemetrySnapshot GetLatestTickSnapshot()
        {
            int previousTickIndex = (currentIndex + _snapshots.Length - 1) % _snapshots.Length;

            return new TickPacketSizeTelemetrySnapshot(FetchSnapshotTelemetryString(_snapshots[previousTickIndex]));
        }

        public TickPacketSizeTelemetrySnapshot GetAverageTickSnapshot()
        {
            Span<int> indices = stackalloc int[_snapshots.Length];

            TickPayloadSizeSnapshot snapshot = GetMergedSnapshot(_snapshots);

            return new TickPacketSizeTelemetrySnapshot(FetchSnapshotTelemetryString(snapshot));
        }

        public string FetchSnapshotTelemetryString(TickPayloadSizeSnapshot snapshot)
        {
            var sb = new StringBuilder();

            if (snapshot == null) return sb.ToString();

            var snapshots = snapshot.GetSnapshots();
            if (snapshots.Count == 0) return sb.ToString();

            long totalSize = snapshots.Sum(x => x.size);
            var sortedItems = snapshots.OrderByDescending(x => x.size);

            sb.AppendLine($"Total Bytes: {totalSize}");

            foreach (var item in sortedItems)
            {
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

            foreach (var snapshot in snapshots)
            {
                if (snapshot == null) continue;
                foreach (var item in snapshot.GetSnapshots())
                {
                    if (!totals.ContainsKey(item.packetId))
                    {
                        totals[item.packetId] = (0, 0);
                    }
                    var current = totals[item.packetId];
                    totals[item.packetId] = (current.totalSize + item.size, current.count + 1);
                }
            }

            foreach (var kvp in totals)
            {
                int avgSize = (int)(kvp.Value.totalSize / kvp.Value.count);
                mergedSnapshot.AddSnapshot((kvp.Key, avgSize));
            }

            return mergedSnapshot;
        }
    }

    public class TickPayloadSizeSnapshot
    {
        private List<(ushort packetId, int size)> _snapshots = new();

        public void AddSnapshot((ushort packetId, int size) snapshot)
        {
            _snapshots.Add(snapshot);
        }

        public IReadOnlyList<(ushort packetId, int size)> GetSnapshots() => _snapshots;
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
