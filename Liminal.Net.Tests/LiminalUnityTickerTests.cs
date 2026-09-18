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
        public void NetworkManager_With_UnityTickerFactory_IntegratesProperly()
        {
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9988,
                TickRate = 60,
                ClientIdResolver = new BaseResolver(),
                TickerFactory = cfg => new LiminalUnityTicker(cfg)
            };

            var manager = new LiminalNetworkManager(new TcpTransport(), config);

            Assert.That(manager.Ticker, Is.InstanceOf<LiminalUnityTicker>());

            manager.Shutdown();
        }
    }
}
