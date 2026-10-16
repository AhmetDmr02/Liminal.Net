using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using NUnit.Framework;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TickerTeardownStallTests
    {
        //Shutdown holds the teardown lock while it waits for the tick thread, a tick handler calling Disconnect
        //blocked on that lock until the 500ms join gave up, then the rest of the tick ran beside the teardown
        [Test]
        public void DisconnectFromTickHandler_DuringShutdownOnAnotherThread_DoesNotStallTeardown()
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config);

            using var inTick = new ManualResetEventSlim();
            int armed = 0;
            server.OnPrePoll += () =>
            {
                if (Interlocked.Exchange(ref armed, 1) != 0) return;
                inTick.Set();
                Thread.Sleep(100);
                server.Disconnect();
            };

            server.StartServer("127.0.0.1", 7777);
            Assert.That(inTick.Wait(2000), Is.True, "Tick never ran.");

            var stopwatch = Stopwatch.StartNew();
            server.Shutdown();
            stopwatch.Stop();

            Assert.That(stopwatch.ElapsedMilliseconds, Is.LessThan(400), $"Shutdown took {stopwatch.ElapsedMilliseconds}ms, it stalled on the tick join.");
            Assert.That(server.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopped));
            Assert.That(server.Role, Is.EqualTo(NetworkRole.None));
        }
    }
}
