using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalEventHubTests
    {
        private LiminalNetworkConfig CreateConfig()
        {
            return new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9999,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 100,
                ClientIdResolver = new BaseResolver()
            };
        }

        [Test]
        public void EventHub_GuaranteesSessionManagerReceivesConnect_BeforeHighLevelSubscribers()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);

            ushort connectingClientId = 42;
            bool sessionExistedWhenHighLevelRan = false;
            bool canRoutePacketImmediately = false;

            manager.Events.OnClientConnected += id =>
            {
                Assert.That(id, Is.EqualTo(connectingClientId));

                Span<ushort> activeSessions = stackalloc ushort[10];
                int count = manager.SessionManager.GetSessionIds(activeSessions);
                sessionExistedWhenHighLevelRan = (count == 1 && activeSessions[0] == connectingClientId);

                manager.Interpreter.SendCommand(connectingClientId, new ChatPacket { Message = "Welcome" });
                canRoutePacketImmediately = true;
            };

            transport.TriggerClientConnected(connectingClientId);

            Assert.That(sessionExistedWhenHighLevelRan, Is.True, "SessionManager did not create the session before high-level events fired.");
            Assert.That(canRoutePacketImmediately, Is.True, "Failed to route packet in OnClientConnected handler.");
        }

        [Test]
        public void EventHub_GuaranteesSessionManagerReceivesLocalConnect_BeforeHighLevelSubscribers()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);

            bool sessionExistedForServer = false;

            manager.Events.OnLocalClientConnected += id =>
            {
                Span<ushort> activeSessions = stackalloc ushort[10];
                int count = manager.SessionManager.GetSessionIds(activeSessions);
                sessionExistedForServer = (count >= 1 && activeSessions[0] == ILiminalTransport.SERVER_ID);
            };

            transport.TriggerLocalClientConnected(1);

            Assert.That(sessionExistedForServer, Is.True, "Session for Server was not created before OnLocalClientConnected fired.");
        }

        [Test]
        public void EventHub_ClearOnShutdown_NullifiesAllRegisteredListeners()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);

            bool clientConnectedCalled = false;
            manager.Events.OnClientConnected += id => clientConnectedCalled = true;

            manager.Shutdown();

            transport.TriggerClientConnected(99);

            Assert.That(clientConnectedCalled, Is.False, "EventHub listener was still invoked after manager shutdown.");
        }

        [Test]
        public void EventHub_ConcurrentAddRemove_IsThreadSafe()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);

            const int iterations = 1000;
            var countdown = new CountdownEvent(iterations * 2);
            var exceptions = new ConcurrentBag<Exception>();

            for (int i = 0; i < iterations; i++)
            {
                Action<ushort> handler = id => { };

                Task.Run(() =>
                {
                    try
                    {
                        manager.Events.OnClientConnected += handler;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                    finally
                    {
                        countdown.Signal();
                    }
                });

                Task.Run(() =>
                {
                    try
                    {
                        manager.Events.OnClientConnected -= handler;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                    finally
                    {
                        countdown.Signal();
                    }
                });
            }

            Assert.That(countdown.Wait(5000), Is.True, "Timed out waiting for concurrent add/remove operations.");
            Assert.That(exceptions, Is.Empty, "Exceptions occurred during concurrent delegate add/remove.");
        }

        [Test]
        public void EventHub_RestartLifecycle_MaintainsCorrectDispatchOrder()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);

            for (int cycle = 0; cycle < 3; cycle++)
            {
                int orderCheck = 0;
                ushort clientForCycle = (ushort)(10 + cycle);

                manager.StartServer("127.0.0.1", 9999);

                manager.Events.OnClientConnected += id =>
                {
                    Span<ushort> sessions = stackalloc ushort[5];
                    if (manager.SessionManager.GetSessionIds(sessions) > 0 && sessions[0] == clientForCycle)
                    {
                        orderCheck = 1;
                    }
                };

                transport.TriggerClientConnected(clientForCycle);
                Assert.That(orderCheck, Is.EqualTo(1), $"Cycle {cycle}: Session did not exist when OnClientConnected fired.");

                manager.Shutdown();
            }
        }
    }
}
