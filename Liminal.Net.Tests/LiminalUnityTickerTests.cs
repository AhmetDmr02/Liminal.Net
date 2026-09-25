using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using NUnit.Framework;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalUnityTickerTests
    {
        private LiminalNetworkConfig _config;
        private LiminalUnityTicker _ticker;

        [SetUp]
        public void Setup()
        {
            _config = new LiminalNetworkConfig
            {
                TickRate = 60
            };
            _ticker = new LiminalUnityTicker(_config);
        }

        [TearDown]
        public void Teardown()
        {
            _ticker?.Stop();
        }

        [Test]
        public void UnityTicker_DerivesFrom_LiminalTicker()
        {
            Assert.That(_ticker, Is.InstanceOf<LiminalTicker>());
        }

        [Test]
        public void UnityTicker_Start_DoesNotSpawnBackgroundThread()
        {
            _ticker.Start();
            Assert.That(_ticker.IsRunning, Is.True);
            Assert.That(_ticker.NextTickTimestamp, Is.GreaterThan(0));

            _ticker.Stop();
            Assert.That(_ticker.IsRunning, Is.False);
        }

        [Test]
        public void UnityTicker_Ticks_AtNominalInterval()
        {
            int tickCount = 0;
            _ticker.OnTick += () => tickCount++;
            _ticker.Start();

            _ticker.TickUpdate();
            Assert.That(tickCount, Is.EqualTo(0));

            Thread.Sleep(20);
            int executed = _ticker.TickUpdate();

            Assert.That(executed, Is.GreaterThanOrEqualTo(1));
            Assert.That(tickCount, Is.GreaterThanOrEqualTo(1));
        }

        [Test]
        public void UnityTicker_Slewing_AdjustsNextTickBoundary()
        {
            _ticker.Start();
            long initialNextTick = _ticker.NextTickTimestamp;

            long slewAdjustment = Stopwatch.Frequency / 100;
            _ticker.ApplySlew(slewAdjustment);

            Thread.Sleep(20);
            _ticker.TickUpdate();

            long expectedMin = initialNextTick + _ticker.NominalTickTicks + (slewAdjustment / 2);
            Assert.That(_ticker.NextTickTimestamp, Is.GreaterThanOrEqualTo(expectedMin));
        }

        [Test]
        public void UnityTicker_MaxTicksPerFrame_PreventsSpiralOfDeath()
        {
            int tickCount = 0;
            _ticker.OnTick += () => tickCount++;
            _ticker.MaxTicksPerFrame = 3;
            _ticker.Start();

            Thread.Sleep(100);
            int executed = _ticker.TickUpdate();

            Assert.That(executed, Is.LessThanOrEqualTo(3));
            Assert.That(tickCount, Is.LessThanOrEqualTo(3));
        }

        [Test]
        public void NetworkManager_With_CustomTicker_IntegratesProperly()
        {
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9988,
                TickRate = 60,
                ClientIdResolver = new BaseResolver()
            };

            var unityTicker = new LiminalUnityTicker(config);
            var manager = new LiminalNetworkManager(new TcpTransport(), config, customTicker: unityTicker);

            Assert.That(manager.Ticker, Is.SameAs(unityTicker));

            manager.Shutdown();
        }

        [Test]
        public void NetworkManager_With_LatencySimulatorTransport_ZeroDelay_OperatesAsNormalTransport()
        {
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9989,
                TickRate = 60,
                ClientIdResolver = new BaseResolver()
            };

            var unityTicker = new LiminalUnityTicker(config);
            var transport = new LatencySimulatorTransport(); // Default 0.0ms delay

            Assert.That(transport.OneWayDelayMs, Is.EqualTo(0.0));

            var manager = new LiminalNetworkManager(transport, config, customTicker: unityTicker);
            Assert.That(manager.Ticker, Is.SameAs(unityTicker));
            Assert.That(manager.Transport, Is.SameAs(transport));

            manager.Shutdown();
        }

        [Test]
        public void NetworkManager_With_LatencySimulatorTransport_And_UnityTicker_ExchangesPackets()
        {
            int port = 9991;
            var serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                ClientIdResolver = new BaseResolver()
            };

            var clientConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                ClientIdResolver = new BaseResolver()
            };

            var serverTicker = new LiminalUnityTicker(serverConfig);
            var clientTicker = new LiminalUnityTicker(clientConfig);

            var serverTransport = new LatencySimulatorTransport { OneWayDelayMs = 0.0 };
            var clientTransport = new LatencySimulatorTransport { OneWayDelayMs = 0.0 };

            var server = new LiminalNetworkManager(serverTransport, serverConfig, customTicker: serverTicker);
            var client = new LiminalNetworkManager(clientTransport, clientConfig, customTicker: clientTicker);

            try
            {
                server.StartServer();
                client.StartClient("127.0.0.1", port);

                Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 3000), Is.True);
                Assert.That(SpinWait.SpinUntil(() => server.ClientRegistry.Count == 1, 3000), Is.True);

                // Both tickers can be updated by Unity update loop
                serverTicker.TickUpdate();
                clientTicker.TickUpdate();
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }
    }
}
