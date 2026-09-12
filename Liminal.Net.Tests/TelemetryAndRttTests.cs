using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TelemetryAndRttTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7940;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }
            _serverManager?.Shutdown();
        }

        private LiminalNetworkManager CreateAndStartClient(LiminalTelemetryConfig telemetryConfig = null)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };
            var client = new LiminalNetworkManager(new TcpTransport(), config, telemetryConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        #region Telemetry & RTT Integration Tests

        [Test]
        public void Test55_Telemetry_ByteAndPacketCounting_TracksThroughputAccurately()
        {
            var telemetryConfig = new LiminalTelemetryConfig { Flags = TelemetryFlags.All };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            int packetsReceived = 0;
            client.Interpreter.Subscribe<ChatPacket>((pkt, sender) => Interlocked.Increment(ref packetsReceived), this);

            const int packetCount = 20;
            for (int i = 0; i < packetCount; i++)
            {
                _serverManager.Interpreter.SendCommand(clientId, new ChatPacket { Message = $"Telemetry_{i}" });
            }
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => packetsReceived == packetCount, 2000), Is.True);

            _serverManager.TelemetryManager.Sample();
            client.TelemetryManager.Sample();

            var serverSessionSnap = _serverManager.TelemetryManager.LatestSessionSnapshot;
            var serverTransportSnap = _serverManager.TelemetryManager.LatestTransportSnapshot;
            var clientSessionSnap = client.TelemetryManager.LatestSessionSnapshot;
            var clientTransportSnap = client.TelemetryManager.LatestTransportSnapshot;

            Assert.Multiple(() =>
            {
                // Server Outbound
                Assert.That(serverSessionSnap.TotalPacketsOutbound, Is.GreaterThanOrEqualTo(packetCount));
                Assert.That(serverTransportSnap.TotalBytesOutbound, Is.GreaterThan(0));

                // Client Inbound
                Assert.That(clientSessionSnap.TotalPacketsInbound, Is.GreaterThanOrEqualTo(packetCount));
                Assert.That(clientTransportSnap.TotalBytesInbound, Is.GreaterThan(0));
            });
        }

        [Test]
        public void Test56_Telemetry_RTT_MeasuresNonZeroLatencyBetweenPeers()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            bool rttUpdated = SpinWait.SpinUntil(() =>
            {
                return client.TelemetryManager != null &&
                       client.TelemetryManager.End2EndRTT > 0.0 &&
                       _serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out double serverSeenRtt) &&
                       serverSeenRtt > 0.0;
            }, 3000);

            Assert.That(rttUpdated, Is.True, "RTT measurement failed to complete for client or server.");
            Assert.That(client.TelemetryManager.End2EndRTT, Is.LessThan(150.0), "Local loopback RTT unusually high.");
        }

        [Test]
        public void Test57_Telemetry_InFlightGating_PreventsSpammingWhenPongDelayed()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.End2EndRTT,
                PollIntervalInSeconds = 0f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            int pingCount = 0;
            _serverManager.Interpreter.UnsubscribeAll(_serverManager.TelemetryManager);
            _serverManager.Interpreter.Subscribe<PingPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref pingCount);
            }, this);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            Thread.Sleep(300);

            Assert.That(pingCount, Is.EqualTo(1), "In-flight gate failed; client spammed multiple pings without receiving pong.");
        }

        [Test]
        public void Test58_Telemetry_StalePongSequence_IsRejectedAndDoesNotPoisonRTT()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 10f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            client.TelemetryManager.Sample();
            Assert.That(SpinWait.SpinUntil(() => client.TelemetryManager.End2EndRTT > 0.0, 2000), Is.True);
            double baselineRtt = client.TelemetryManager.End2EndRTT;

            long ancientTimestamp = Stopwatch.GetTimestamp() - (Stopwatch.Frequency * 10);
            var stalePong = new PongPacket
            {
                SequenceId = 99999,
                TimestampTicks = ancientTimestamp
            };

            _serverManager.Interpreter.SendCommand(client.localID, stalePong);
            _serverManager.SessionManager.Flush();

            Thread.Sleep(100);

            Assert.That(client.TelemetryManager.End2EndRTT, Is.LessThan(1000.0),
                "Stale pong with unmatched SequenceId poisoned client RTT calculation.");
            Assert.That(client.TelemetryManager.End2EndRTT, Is.EqualTo(baselineRtt),
                "RTT changed unexpectedly despite no new pings being scheduled.");
        }

        [Test]
        public void Test59_Telemetry_ClientDisconnect_EvictsServerTrackingDictionaries()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            Assert.That(SpinWait.SpinUntil(() => _serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out _), 2000), Is.True);

            client.Disconnect();

            bool removed = SpinWait.SpinUntil(() =>
            {
                return !_serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out _);
            }, 2000);

            Assert.That(removed, Is.True, "Server failed to evict disconnected client ID from telemetry tracking dictionary.");
        }

        [Test]
        public void Test60_Telemetry_LocalClientDisconnect_ResetsInFlightGateAndMetrics()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => client.TelemetryManager.End2EndRTT > 0.0, 3000), Is.True);

            var clientTelemetry = client.TelemetryManager;

            client.Transport.Disconnect();

            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True);

            Assert.That(clientTelemetry.End2EndRTT, Is.EqualTo(0.0),
                "Client telemetry state was not zeroed upon local disconnect.");
        }

        [Test]
        public void Test61_Telemetry_PingTimeout_SetsBothClientAndServerRTTTo999()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.End2EndRTT,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            bool baselineEstablished = SpinWait.SpinUntil(() =>
            {
                return client.TelemetryManager != null &&
                       client.TelemetryManager.End2EndRTT > 0.0 &&
                       client.TelemetryManager.End2EndRTT < 150.0 &&
                       _serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out double serverRtt) &&
                       serverRtt > 0.0 &&
                       serverRtt < 150.0;
            }, 3000);

            Assert.That(baselineEstablished, Is.True, "Initial baseline RTT measurement failed to establish.");

            _serverManager.Interpreter.Unsubscribe<PingPacket>(_serverManager.TelemetryManager);
            client.Interpreter.Unsubscribe<PingPacket>(client.TelemetryManager);

            bool bothTimedOut = SpinWait.SpinUntil(() =>
            {
                bool serverSeesTimeout = _serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out double serverMs) &&
                                         Math.Abs(serverMs - 999.0) < 0.01;

                bool clientSeesTimeout = Math.Abs(client.TelemetryManager.End2EndRTT - 999.0) < 0.01;

                return serverSeesTimeout && clientSeesTimeout;
            }, 6000);

            Assert.Multiple(() =>
            {
                Assert.That(bothTimedOut, Is.True, "Either client or server RTT failed to reach 999 ms after the 3-second timeout window.");
                Assert.That(client.TelemetryManager.End2EndRTT, Is.EqualTo(999.0).Within(0.01), "Client RTT did not reflect 999 ms timeout value.");

                bool serverReadSuccess = _serverManager.TelemetryManager.TryGetClientEnd2EndRTT(clientId, out double serverSeenRtt);
                Assert.That(serverReadSuccess, Is.True, "Server failed to retrieve RTT entry for client.");
                Assert.That(serverSeenRtt, Is.EqualTo(999.0).Within(0.01), "Server-measured client RTT did not reflect 999 ms timeout value.");
            });
        }

        #endregion

        #region Wire RTT Integration Tests

        [Test]
        public void Test62_WireRTT_BaselineLoopback_MeasuresAccurateAndSubMillisecondLatency()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.WireRTT,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            bool wireRttResolved = SpinWait.SpinUntil(() =>
            {
                bool clientHasWire = client.TelemetryManager != null && client.TelemetryManager.WireRTT > 0.0;
                bool serverHasWire = _serverManager.TelemetryManager != null &&
                                     _serverManager.TelemetryManager.TryGetClientWireRTT(clientId, out double sWire) &&
                                     sWire > 0.0;

                return clientHasWire && serverHasWire;
            }, 3000);

            Assert.That(wireRttResolved, Is.True, "Wire RTT failed to resolve on either client or server.");

            Assert.Multiple(() =>
            {
                Assert.That(client.TelemetryManager.WireRTT, Is.GreaterThan(0.0).And.LessThan(50.0),
                    "Client Wire RTT outside expected loopback range.");

                bool serverSuccess = _serverManager.TelemetryManager.TryGetClientWireRTT(clientId, out double serverSeenWire);
                Assert.That(serverSuccess, Is.True);
                Assert.That(serverSeenWire, Is.GreaterThan(0.0).And.LessThan(50.0),
                    "Server Wire RTT outside expected loopback range.");
            });
        }

        [Test]
        public void Test63_WireRTT_LatencySimulator_AccuratelyTracksConfiguredDelay()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            const double oneWayDelayMs = 40.0;
            const double expectedRttMs = oneWayDelayMs * 2.0; // 80ms RTT

            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.WireRTT,
                PollIntervalInSeconds = 0.05f
            };

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var serverSimTransport = new LatencySimulatorTransport()
            {
                OneWayDelayMs = oneWayDelayMs,
                JitterMs = 0.0
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(serverSimTransport, serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var clientSimTransport = new LatencySimulatorTransport()
            {
                OneWayDelayMs = oneWayDelayMs,
                JitterMs = 0.0
            };

            var client = new LiminalNetworkManager(clientSimTransport, clientConfig, telemetryConfig);
            _clientManagers.Add(client);

            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 4000), Is.True, "Client failed to connect.");

            // Allow wire pings to travel and stabilize
            bool stabilized = SpinWait.SpinUntil(() =>
            {
                return client.TelemetryManager != null &&
                       client.TelemetryManager.WireRTT >= (expectedRttMs - 15.0);
            }, 5000);

            Assert.That(stabilized, Is.True,
                $"Wire RTT failed to reflect simulated latency window. Client WireRTT: {client.TelemetryManager?.WireRTT} ms");

            Assert.That(client.TelemetryManager.WireRTT, Is.EqualTo(expectedRttMs).Within(30.0),
                $"Client measured {client.TelemetryManager.WireRTT:F2} ms, expected ~{expectedRttMs} ms.");
        }

        [Test]
        public void Test64_WireRTT_ContinuesMeasuringWhenGameInterpreterIsBlocked()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.WireRTT | TelemetryFlags.End2EndRTT,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => client.TelemetryManager.WireRTT > 0.0, 2000), Is.True);

            _serverManager.Interpreter.UnsubscribeAll(_serverManager.TelemetryManager);
            client.Interpreter.UnsubscribeAll(client.TelemetryManager);

            client.TelemetryManager.Sample();
            _serverManager.TelemetryManager.Sample();

            Thread.Sleep(200);

            var clientTransport = (ITransportTelemetryProvider)client.Transport;
            clientTransport.SendWirePing(ILiminalTransport.SERVER_ID);

            bool wireStillActive = SpinWait.SpinUntil(() => client.TelemetryManager.WireRTT > 0.0, 1000);
            Assert.That(wireStillActive, Is.True, "Wire RTT stopped functioning when high-level packet handlers were disabled.");
            Assert.That(client.TelemetryManager.WireRTT, Is.LessThan(50.0), "Wire RTT was stalled by high-level layer detachment.");
        }

        [Test]
        public void Test65_WireRTT_ClientDisconnect_CleansUpServerWireTracking()
        {
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.WireRTT,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, telemetryConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient(telemetryConfig);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort clientId = client.localID;

            Assert.That(SpinWait.SpinUntil(() => _serverManager.TelemetryManager.TryGetClientWireRTT(clientId, out _), 2000), Is.True);

            client.Disconnect();

            bool removed = SpinWait.SpinUntil(() =>
            {
                return !_serverManager.Transport.IsClientConnected(clientId);
            }, 2000);

            Assert.That(removed, Is.True, "Server transport failed to clear disconnected client socket.");
        }

        [Test]
        public void Test66_WireRTT_MismatchedSequencePong_IsIgnoredAndDoesNotPoisonLatency()
        {
            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            var clientTransport = (TcpTransport)client.Transport;

            clientTransport.SendWirePing(ILiminalTransport.SERVER_ID);
            Assert.That(SpinWait.SpinUntil(() => clientTransport.TryGetWireRTT(ILiminalTransport.SERVER_ID, out _), 2000), Is.True);
            clientTransport.TryGetWireRTT(ILiminalTransport.SERVER_ID, out double validBaseline);

            Span<byte> maliciousPongPayload = stackalloc byte[12];
            BinaryPrimitives.WriteUInt32LittleEndian(maliciousPongPayload.Slice(0, 4), 0xBADF00D);
            BinaryPrimitives.WriteInt64LittleEndian(maliciousPongPayload.Slice(4, 8), Stopwatch.GetTimestamp() - (Stopwatch.Frequency * 10));

            var serverTransport = (TcpTransport)_serverManager.Transport;
            serverTransport.Send(maliciousPongPayload, client.localID,TransportFlags.Reliable);

            Thread.Sleep(100);

            clientTransport.TryGetWireRTT(ILiminalTransport.SERVER_ID, out double currentWireRtt);

            Assert.That(currentWireRtt, Is.EqualTo(validBaseline),
                "Mismatched sequence number was processed and poisoned the wire RTT state.");
            Assert.That(currentWireRtt, Is.LessThan(1000.0),
                "Spoofed ancient timestamp inflated the wire RTT metric.");
        }

        #endregion
    }
}