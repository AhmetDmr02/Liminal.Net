using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class PhaseAlignmentAndSlewingTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        private static int _portCounter = 8880;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new ConcurrentBag<LiminalNetworkManager>();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 20,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var serverTelemetry = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.05f
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig, serverTelemetry);
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

        private LiminalNetworkManager CreateAndStartClient(ILiminalTransport transport, uint tickRate = 20)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = tickRate,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };

            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.05f
            };

            var client = new LiminalNetworkManager(transport, config, telemetryConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        [Test]
        public void Test01_ProportionalCushion_ScalesWithTickRate()
        {
            var config20Hz = new LiminalTransportConfig { TickRate = 20 };
            var aligner20Hz = new LiminalPhaseAligner(config20Hz);

            var config30Hz = new LiminalTransportConfig { TickRate = 30 };
            var aligner30Hz = new LiminalPhaseAligner(config30Hz);

            var config60Hz = new LiminalTransportConfig { TickRate = 60 };
            var aligner60Hz = new LiminalPhaseAligner(config60Hz);

            Assert.That(aligner20Hz.GetProportionalCushionMs(), Is.EqualTo(20.0).Within(0.001));
            Assert.That(aligner30Hz.GetProportionalCushionMs(), Is.EqualTo(1000.0 / 30.0 * 0.4).Within(0.001));
            Assert.That(aligner60Hz.GetProportionalCushionMs(), Is.EqualTo(1000.0 / 60.0 * 0.4).Within(0.001));
        }

        [Test]
        public void Test02_PhaseAligner_DeadbandPreventsMicroAdjustments()
        {
            var config = new LiminalTransportConfig { TickRate = 20 };
            var aligner = new LiminalPhaseAligner(config);

            long adjustment = aligner.CalculateSlewAdjustment(
                wireRttMs: 20.0,
                serverCountdownMs: 30.2,
                clientCountdownMs: 0.0
            );

            Assert.That(adjustment, Is.EqualTo(0), "Phase aligner should ignore micro-errors within the 0.5ms deadband.");
        }

        [Test]
        public void Test03_PhaseAligner_ClampsToMaxSlewLimit()
        {
            var config = new LiminalTransportConfig { TickRate = 20 };
            var aligner = new LiminalPhaseAligner(config);

            long oneMsTicks = Stopwatch.Frequency / 1000;

            long adjustmentEarly = aligner.CalculateSlewAdjustment(
                wireRttMs: 20.0,
                serverCountdownMs: 5.0,
                clientCountdownMs: 0.0
            );

            long adjustmentLate = aligner.CalculateSlewAdjustment(
                wireRttMs: 20.0,
                serverCountdownMs: 45.0,
                clientCountdownMs: 0.0
            );

            Assert.That(Math.Abs(adjustmentEarly), Is.LessThanOrEqualTo(oneMsTicks + 2), "Slew was not clamped to <= 1ms.");
            Assert.That(Math.Abs(adjustmentLate), Is.LessThanOrEqualTo(oneMsTicks + 2), "Slew was not clamped to <= 1ms.");
        }

        [Test]
        public void Test04_Ticker_ApplySlew_StretchesAndCompressesInterval()
        {
            var config = new LiminalTransportConfig { TickRate = 20 };
            var ticker = new LiminalTicker(config);

            long oneMsTicks = Stopwatch.Frequency / 1000;

            ticker.Start();
            try
            {
                Assert.That(SpinWait.SpinUntil(() => ticker.NextTickTimestamp > 0, 1000), Is.True);

                long anchorBefore = ticker.NextTickTimestamp;
                ticker.ApplySlew(oneMsTicks);

                Assert.That(SpinWait.SpinUntil(() => ticker.NextTickTimestamp > anchorBefore, 1000), Is.True);

                long deltaTicks = ticker.NextTickTimestamp - anchorBefore;
                double deltaMs = (deltaTicks * 1000.0) / Stopwatch.Frequency;

                Assert.That(deltaMs, Is.GreaterThanOrEqualTo(50.5), "Ticker did not stretch tick duration when positive slew was applied.");
            }
            finally
            {
                ticker.Stop();
            }
        }

        [Test]
        public void Test05_WirePingPong_PopulatesCountdownAndRtt()
        {
            var serverTelemetry = new LiminalTelemetryConfig { Flags = TelemetryFlags.All, PollIntervalInSeconds = 0.05f };
            var serverTransport = new LatencySimulatorTransport { OneWayDelayMs = 15.0, JitterMs = 0.0 };
            _serverManager = new LiminalNetworkManager(serverTransport, _serverConfig, serverTelemetry);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientTransport = new LatencySimulatorTransport { OneWayDelayMs = 15.0, JitterMs = 0.0 };
            var client = CreateAndStartClient(clientTransport, tickRate: 20);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True, "Client failed to connect.");

            bool receivedWireTelemetry = SpinWait.SpinUntil(() => client.TelemetryManager.WireRTT > 0.0, 4000);

            Assert.That(receivedWireTelemetry, Is.True, "Client failed to measure WireRTT.");
            Assert.That(client.TelemetryManager.WireRTT, Is.EqualTo(30.0).Within(6.0), "WireRTT did not match simulated latency.");
            Assert.That(client.TelemetryManager.ServerCountdownMs, Is.InRange(0.0, 50.0), "Server countdown is outside the 50ms tick boundary.");
        }

        [Test]
        public void Test06_PhaseConvergence_ClientSlewsTowardsTargetCushion()
        {
            var serverTelemetry = new LiminalTelemetryConfig { Flags = TelemetryFlags.All, PollIntervalInSeconds = 0.02f };
            var serverTransport = new LatencySimulatorTransport { OneWayDelayMs = 10.0, JitterMs = 0.0 };
            _serverManager = new LiminalNetworkManager(serverTransport, _serverConfig, serverTelemetry);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientTransport = new LatencySimulatorTransport { OneWayDelayMs = 10.0, JitterMs = 0.0 };
            var client = CreateAndStartClient(clientTransport, tickRate: 20);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client.TelemetryManager.WireRTT > 0.0, 3000), Is.True);

            var aligner = new LiminalPhaseAligner(_serverConfig);

            bool converged = SpinWait.SpinUntil(() =>
            {
                double wireRtt = client.TelemetryManager.WireRTT;
                double serverCountdown = client.TelemetryManager.ServerCountdownMs;

                if (wireRtt <= 0.0) return false;

                long slew = aligner.CalculateSlewAdjustment(wireRtt, serverCountdown, clientCountdownMs: 0.0);
                return slew == 0;
            }, 6000);

            Assert.That(converged, Is.True, "Client ticker failed to converge phase into the target cushion deadband.");
        }

        [Test]
        public void Test07_HigherTickRate_AlignsWithProportionalCushion()
        {
            _serverConfig.TickRate = 60;
            var serverTelemetry = new LiminalTelemetryConfig { Flags = TelemetryFlags.All, PollIntervalInSeconds = 0.05f };
            var serverTransport = new LatencySimulatorTransport { OneWayDelayMs = 5.0, JitterMs = 0.0 };
            _serverManager = new LiminalNetworkManager(serverTransport, _serverConfig, serverTelemetry);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientTransport = new LatencySimulatorTransport { OneWayDelayMs = 5.0, JitterMs = 0.0 };
            var client = CreateAndStartClient(clientTransport, tickRate: 60);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client.TelemetryManager.WireRTT > 0.0, 3000), Is.True);
            Assert.That(client.TelemetryManager.WireRTT, Is.EqualTo(10.0).Within(4.0));

            var aligner = new LiminalPhaseAligner(_serverConfig);
            Assert.That(aligner.GetProportionalCushionMs(), Is.EqualTo(1000.0 / 60.0 * 0.4).Within(0.1));
        }

        [Test]
        public void Test08_JitterSpikes_AbsorbedByCushionWithoutDesync()
        {
            var serverTransport = new LatencySimulatorTransport { OneWayDelayMs = 15.0, JitterMs = 3.0 };
            _serverManager = new LiminalNetworkManager(serverTransport, _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientTransport = new LatencySimulatorTransport { OneWayDelayMs = 15.0, JitterMs = 3.0 };
            var client = CreateAndStartClient(clientTransport, tickRate: 20);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int packetsDelivered = 0;
            client.Interpreter.Subscribe<ChatPacket>((pkt, senderId) =>
            {
                Interlocked.Increment(ref packetsDelivered);
            }, this);

            for (int i = 0; i < 20; i++)
            {
                _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = $"JitterPacket_{i}" });
                Thread.Sleep(25);
            }

            Assert.That(SpinWait.SpinUntil(() => packetsDelivered == 20, 3000), Is.True,
                $"Jitter caused dropped or starved packets! Only received {packetsDelivered}/20.");
        }
    }
}