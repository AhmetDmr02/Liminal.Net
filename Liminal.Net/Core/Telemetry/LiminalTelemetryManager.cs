using Liminal.Net.BasePackets;
using Liminal.Net.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalTelemetryManager : IDisposable
    {
        private readonly LiminalNetworkManager _networkManager;
        private readonly LiminalTicker _ticker;
        private readonly LiminalTelemetryConfig _config;

        private readonly ITransportTelemetryProvider _transportTelemetry;
        private readonly ISessionTelemetryProvider _sessionTelemetry;

        private volatile bool _disposed;

        // Interval calculation in Stopwatch ticks
        private readonly long _sampleIntervalTicks;
        private long _lastSampleTimestamp;

        private double _currentRttMs;
        public double RTT => Volatile.Read(ref _currentRttMs);

        private uint _clientPingSequence;
        private long _clientPingInFlightTimestamp;

        // Server-side State (Per-Client)
        private readonly ConcurrentDictionary<ushort, double> _clientRttMap = new();
        private readonly ConcurrentDictionary<ushort, (uint SequenceId, long Timestamp)> _serverInFlightPings = new();
        private uint _serverSequenceGenerator;

        // Safety timeout to re-ping if a packet is lost or peer hangs
        private static readonly long PingTimeoutTicks = Stopwatch.Frequency * 3; // 3 seconds

        public GlobalTransportTelemetrySnapshot LatestTransportSnapshot { get; private set; }
        public GlobalSessionTelemetrySnapshot LatestSessionSnapshot { get; private set; }

        public event Action<GlobalSessionTelemetrySnapshot, GlobalTransportTelemetrySnapshot> OnTelemetryUpdated;
        public event Action<ServerStarvationEvent> OnServerStarvation;

        public LiminalTelemetryManager(LiminalNetworkManager manager, LiminalTicker ticker, LiminalTelemetryConfig config)
        {
            _networkManager = manager ?? throw new ArgumentNullException(nameof(manager));
            _ticker = ticker ?? throw new ArgumentNullException(nameof(ticker));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            float intervalSec = MathF.Max(_config.PollIntervalInSeconds, 0f);
            _sampleIntervalTicks = (long)(intervalSec * Stopwatch.Frequency);
            _lastSampleTimestamp = Stopwatch.GetTimestamp();

            _transportTelemetry = manager.Transport as ITransportTelemetryProvider;
            _sessionTelemetry = manager.SessionManager as ISessionTelemetryProvider;

            manager.SessionManager.OnServerStarvation += HandleServerStarvation;

            _transportTelemetry?.InitializeConfig(_config);
            _sessionTelemetry?.InitializeConfig(_config);

            _networkManager.Interpreter.Subscribe<PingPacket>(HandlePing, this);
            _networkManager.Interpreter.Subscribe<PongPacket>(HandlePong, this);

            // Match transport disconnect cleanup pattern
            _networkManager.Transport.OnClientDisconnected += HandleClientDisconnected;
            _networkManager.Transport.OnClientKicked += HandleClientDisconnected;
            _networkManager.Transport.OnLocalClientDisconnected += HandleLocalClientDisconnected;

            _ticker.OnTick += HandleTick;
        }

        public bool TryGetClientRTT(ushort clientId, out double rttMs)
        {
            if (clientId == ILiminalTransport.SERVER_ID)
            {
                rttMs = RTT;
                return rttMs > 0.0;
            }

            return _clientRttMap.TryGetValue(clientId, out rttMs);
        }

        private void HandleClientDisconnected(ushort clientId)
        {
            _serverInFlightPings.TryRemove(clientId, out _);
            _clientRttMap.TryRemove(clientId, out _);
        }

        private void HandleLocalClientDisconnected(ushort localId)
        {
            ResetAllState();
        }

        private void HandleServerStarvation(ServerStarvationEvent evt)
        {
            OnServerStarvation?.Invoke(evt);
        }

        private void ResetAllState()
        {
            Volatile.Write(ref _clientPingInFlightTimestamp, 0);
            Volatile.Write(ref _currentRttMs, 0);
            Volatile.Write(ref _clientPingSequence, 0);

            _serverInFlightPings.Clear();
            _clientRttMap.Clear();
        }

        private void HandleTick()
        {
            if (_disposed) return;

            long now = Stopwatch.GetTimestamp();
            if (now - _lastSampleTimestamp >= _sampleIntervalTicks)
            {
                _lastSampleTimestamp = now;
                Sample();
            }
        }

        public void Sample()
        {
            if (_disposed) return;

            if (_sessionTelemetry != null)
                LatestSessionSnapshot = _sessionTelemetry.GetGlobalSnapshot();

            if (_transportTelemetry != null)
                LatestTransportSnapshot = _transportTelemetry.GetGlobalTransportSnapshot();

            if (_config.HasFlag(TelemetryFlags.RTT))
            {
                SendPing();
            }

            OnTelemetryUpdated?.Invoke(LatestSessionSnapshot, LatestTransportSnapshot);
        }

        private void SendPing()
        {
            long now = Stopwatch.GetTimestamp();

            if (_networkManager.Role == NetworkRole.Client || _networkManager.Role == NetworkRole.Host)
            {
                long inFlightAt = Volatile.Read(ref _clientPingInFlightTimestamp);

                if (inFlightAt == 0 || (now - inFlightAt) >= PingTimeoutTicks)
                {
                    uint nextSeq = unchecked(++_clientPingSequence);
                    Volatile.Write(ref _clientPingInFlightTimestamp, now);

                    _networkManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new PingPacket
                    {
                        SequenceId = nextSeq,
                        TimestampTicks = now
                    });
                }
            }

            if (_networkManager.Role == NetworkRole.Server || _networkManager.Role == NetworkRole.Host)
            {
                Span<ushort> ids = stackalloc ushort[_networkManager.Transport.Config.MaxConnectionCount];
                int count = _networkManager.SessionManager.GetSessionIds(ids);

                for (int i = 0; i < count; i++)
                {
                    ushort targetId = ids[i];

                    // Never let the server ping ID 0 (itself) or its own local client
                    // that RTT is already captured by the client-side ping above.
                    if (targetId == ILiminalTransport.SERVER_ID) continue;
                    if (_networkManager.Role == NetworkRole.Host && targetId == _networkManager.localID) continue;

                    if (_serverInFlightPings.TryGetValue(targetId, out var state) && state.Timestamp != 0)
                    {
                        if ((now - state.Timestamp) < PingTimeoutTicks)
                        {
                            continue;
                        }
                    }

                    uint nextSeq = unchecked(++_serverSequenceGenerator);
                    _serverInFlightPings[targetId] = (nextSeq, now);

                    _networkManager.Interpreter.SendCommand(targetId, new PingPacket
                    {
                        SequenceId = nextSeq,
                        TimestampTicks = now
                    });
                }
            }
        }

        private void HandlePing(PingPacket packet, ushort sender)
        {
            _networkManager.Interpreter.SendCommand(sender, new PongPacket
            {
                SequenceId = packet.SequenceId,
                TimestampTicks = packet.TimestampTicks
            });
        }

        private void HandlePong(PongPacket packet, ushort sender)
        {
            long stop = Stopwatch.GetTimestamp();

            bool isServerOrSelf = sender == ILiminalTransport.SERVER_ID ||
                                  (_networkManager.Role == NetworkRole.Host && sender == _networkManager.localID);

            if (isServerOrSelf)
            {
                uint expectedSeq = Volatile.Read(ref _clientPingSequence);
                long inFlightAt = Volatile.Read(ref _clientPingInFlightTimestamp);

                if (packet.SequenceId != expectedSeq || inFlightAt == 0)
                {
                    return;
                }

                long elapsedTicks = Math.Max(0, stop - packet.TimestampTicks);
                double ms = (elapsedTicks * 1000.0) / Stopwatch.Frequency;

                Volatile.Write(ref _currentRttMs, ms);
                Volatile.Write(ref _clientPingInFlightTimestamp, 0);

                if (_networkManager.Role == NetworkRole.Host)
                {
                    _clientRttMap[_networkManager.localID] = ms;
                }

                return;
            }

            if (_serverInFlightPings.TryGetValue(sender, out var state))
            {
                if (packet.SequenceId != state.SequenceId || state.Timestamp == 0)
                {
                    return;
                }

                long elapsedTicks = Math.Max(0, stop - packet.TimestampTicks);
                double ms = (elapsedTicks * 1000.0) / Stopwatch.Frequency;

                _clientRttMap[sender] = ms;
                _serverInFlightPings[sender] = (state.SequenceId, 0);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_ticker != null)
            {
                _ticker.OnTick -= HandleTick;
            }

            if (_networkManager.Transport != null)
            {
                _networkManager.Transport.OnClientDisconnected -= HandleClientDisconnected;
                _networkManager.Transport.OnClientKicked -= HandleClientDisconnected;
                _networkManager.Transport.OnLocalClientDisconnected -= HandleLocalClientDisconnected;
            }

            _networkManager.SessionManager.OnServerStarvation -= HandleServerStarvation;
            _networkManager.Interpreter.UnsubscribeAll(this);
            ResetAllState();

            OnTelemetryUpdated = null;
            OnServerStarvation = null;
        }
    }

    public readonly struct GlobalSessionTelemetrySnapshot
    {
        public readonly int ActiveSessionCount;
        public readonly int LoopbackQueueCount;
        public readonly long TotalPacketsInbound;
        public readonly long TotalPacketsOutbound;

        public GlobalSessionTelemetrySnapshot(
            int activeSessionCount,
            int loopbackQueueCount,
            long totalPacketsInbound,
            long totalPacketsOutbound)
        {
            ActiveSessionCount = activeSessionCount;
            LoopbackQueueCount = loopbackQueueCount;
            TotalPacketsInbound = totalPacketsInbound;
            TotalPacketsOutbound = totalPacketsOutbound;
        }
    }

    public readonly struct GlobalTransportTelemetrySnapshot
    {
        public readonly long TotalBytesInbound;
        public readonly long TotalBytesOutbound;

        public double InboundKB => TotalBytesInbound / 1024.0;
        public double InboundMB => TotalBytesInbound / (1024.0 * 1024.0);
        public double InboundGB => TotalBytesInbound / (1024.0 * 1024.0 * 1024.0);

        public double OutboundKB => TotalBytesOutbound / 1024.0;
        public double OutboundMB => TotalBytesOutbound / (1024.0 * 1024.0);
        public double OutboundGB => TotalBytesOutbound / (1024.0 * 1024.0 * 1024.0);

        /// <summary>
        /// Packet loss ratio (0.0 to 1.0). Always 0 on reliable stream transports like TCP.
        /// </summary>
        public readonly float PacketLossRate;

        public GlobalTransportTelemetrySnapshot(long totalBytesInbound, long totalBytesOutbound, float packetLossRate = 0f)
        {
            TotalBytesInbound = totalBytesInbound;
            TotalBytesOutbound = totalBytesOutbound;
            PacketLossRate = packetLossRate;
        }
    }
}

