using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Unity;
using NUnit.Framework;

namespace Liminal.Net.Tests
{
    //Shutdown is final, it must never be dropped because a teardown was already running or about to run
    [TestFixture]
    public class ShutdownDuringTeardownTests
    {
        private static LiminalNetworkManager StartMockClient(out MockTransport transport)
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            transport = new MockTransport { IsServer = false };
            var manager = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            manager.StartClient("127.0.0.1", 7777);
            transport.TriggerLocalClientConnected(transport.LocalClientId);
            Assert.That(manager.IsConnected, Is.True);
            return manager;
        }

        private static void AssertShutDown(LiminalNetworkManager manager)
        {
            Assert.That(manager.SyncVarManager.IsTornDown, Is.True, "Shutdown did not tear down the SyncVarManager.");
            Assert.That(manager.StartClient("127.0.0.1", 7777), Is.False, "The manager can still be started, Shutdown was dropped.");
            Assert.That(manager.Role, Is.EqualTo(NetworkRole.None));
            Assert.That(manager.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopped));
        }

        //Connection dropped, the transport has not finished tearing down yet, the game shuts down in that gap
        [Test]
        public void Shutdown_BetweenConnectionDropAndTransportTeardown_IsNotIgnored()
        {
            var manager = StartMockClient(out var transport);
            try
            {
                transport.TriggerLocalClientDisconnected(transport.LocalClientId);
                Assert.That(manager.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopping), "Precondition: the drop should leave the manager stopping.");

                manager.Shutdown();
                Assert.That(manager.SyncVarManager.IsTornDown, Is.True, "Shutdown waited for the transport instead of running right away.");
                Assert.That(manager.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopped));

                transport.Shutdown();
                AssertShutDown(manager);
            }
            finally { manager.Shutdown(); }
        }

        [Test]
        public void Shutdown_FromLocalDisconnectHandler_IsNotIgnored()
        {
            var manager = StartMockClient(out var transport);
            manager.OnLocalClientDisconnected += _ => manager.Shutdown();
            try
            {
                transport.TriggerLocalClientDisconnected(transport.LocalClientId);
                transport.Shutdown();

                AssertShutDown(manager);
            }
            finally { manager.Shutdown(); }
        }

        //Handler runs on the tearing down thread while it holds the lifecycle lock, the shutdown is finished once that teardown ends
        [TestCase("ConnectionLost")]
        [TestCase("Disconnect")]
        public void Shutdown_FromLifecycleHandlerDuringTeardown_IsCompleted(string howEnded)
        {
            var manager = StartMockClient(out var transport);
            var states = new System.Collections.Generic.List<NetworkLifecycleState>();
            manager.OnLifecycleStateChanged += state =>
            {
                states.Add(state);
                if (state == NetworkLifecycleState.Stopping) manager.Shutdown();
            };
            try
            {
                if (howEnded == "Disconnect") manager.Disconnect();
                else transport.Shutdown();

                AssertShutDown(manager);

                //Shutdown runs after the teardown, not nested inside it, so every Stopping is closed by a Stopped before the next one
                for (int i = 0; i < states.Count; i++)
                {
                    var expected = i % 2 == 0 ? NetworkLifecycleState.Stopping : NetworkLifecycleState.Stopped;
                    Assert.That(states[i], Is.EqualTo(expected), $"Lifecycle events out of order: {string.Join(", ", states)}");
                }
                Assert.That(states.Count % 2, Is.EqualTo(0), $"Lifecycle ended without Stopped: {string.Join(", ", states)}");
            }
            finally { manager.Shutdown(); }
        }
    }
}
