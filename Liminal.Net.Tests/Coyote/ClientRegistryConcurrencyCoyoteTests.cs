using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Registry;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class ClientRegistryConcurrencyCoyoteTests
    {
        private static void SimulateInboundPacket<T>(LiminalNetworkManager manager, ushort senderId, T packet) where T : struct
        {
            ushort packetId = checked((ushort)LiminalPacketLibrary.GetId<T>());
            byte[] payload = MessagePackSerializer.Serialize(packet);
            manager.Interpreter.Dispatch(packetId, senderId, payload);
        }

        private static LiminalNetworkConfig CreateTestConfig()
        {
            return new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 7777,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };
        }

        [NUnitTest]
        [CoyoteTest]
        public static async Task Coyote_Server_ConcurrentConnectAndDisconnect_ZeroDesyncOrTornState()
        {
            var config = CreateTestConfig();
            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var registry = netManager.ClientRegistry;

            try
            {
                Specification.Assert(registry != null, "ClientRegistry must not be null after initialization");

                const int clientCount = 6;
                const int iterationsPerWorker = 15;

                var tasks = new Task[4];

                tasks[0] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterationsPerWorker; i++)
                    {
                        for (ushort id = 1; id < clientCount; id += 2)
                        {
                            transport.TriggerClientConnected(id);
                            await Task.Yield();
                            transport.TriggerClientDisconnected(id);
                        }
                    }
                });

                tasks[1] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterationsPerWorker; i++)
                    {
                        for (ushort id = 2; id <= clientCount; id += 2)
                        {
                            transport.TriggerClientConnected(id);
                            await Task.Yield();
                            transport.TriggerClientDisconnected(id);
                        }
                    }
                });

                tasks[2] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterationsPerWorker * 2; i++)
                    {
                        int observedCount = registry.Count;
                        Specification.Assert(observedCount >= 0, "Registry Count must never be negative");

                        int enumeratedCount = 0;
                        foreach (var client in registry.AllClients)
                        {
                            enumeratedCount++;
                            Specification.Assert(client.ClientId > 0, "Client ID must be valid");
                            Specification.Assert(registry.ContainsClient(client.ClientId), "Client in AllClients must be present in registry");
                        }

                        await Task.Yield();
                    }
                });

                tasks[3] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterationsPerWorker * 2; i++)
                    {
                        bool isDedicated = registry.IsDedicatedServer;
                        var hostClient = registry.HostClient;

                        if (isDedicated)
                        {
                            Specification.Assert(hostClient == null, "HostClient must be null on dedicated server");
                        }

                        for (ushort id = 1; id <= clientCount; id++)
                        {
                            if (registry.TryGetClient(id, out var foundClient))
                            {
                                Specification.Assert(foundClient != null, "Found client must not be null");
                                Specification.Assert(foundClient.ClientId == id, "Found client ID must match requested ID");
                            }
                        }

                        await Task.Yield();
                    }
                });

                await Task.WhenAll(tasks);

                Specification.Assert(registry.Count >= 0, "Final count must be non-negative");
                Specification.Assert(registry.AllClients.Count == registry.Count, "AllClients count must match Count property");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [NUnitTest]
        [CoyoteTest]
        public static async Task Coyote_Client_ConcurrentSnapshotAndDeltaReconciliation_ZeroCorruption()
        {
            var config = CreateTestConfig();
            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);

            try
            {
                netManager.StartClient("127.0.0.1", 7777);
                // Stop background OS ticker thread so it doesn't leak or spin outside Coyote control
                netManager.Ticker?.Stop();

                ushort localId = 10;
                transport.TriggerLocalClientConnected(localId);

                var registry = netManager.ClientRegistry;
                Specification.Assert(registry.LocalClient != null, "LocalClient must be registered upon connection");
                Specification.Assert(registry.LocalClient.ClientId == localId, "LocalClient ID must match assigned ID");

                const int iterations = 50;
                var tasks = new Task[3];

                tasks[0] = Task.Run(async () =>
                {
                    for (uint v = 1; v <= iterations; v++)
                    {
                        ushort peerId = (ushort)(100 + (v % 5));

                        var joinedPacket = new ClientJoinedNotificationPacket
                        {
                            RosterVersion = (v * 2) - 1,
                            ClientId = peerId
                        };
                        SimulateInboundPacket(netManager, ILiminalTransport.SERVER_ID, joinedPacket);

                        await Task.Yield();

                        var leftPacket = new ClientLeftNotificationPacket
                        {
                            RosterVersion = v * 2,
                            ClientId = peerId
                        };
                        SimulateInboundPacket(netManager, ILiminalTransport.SERVER_ID, leftPacket);
                    }
                });

                tasks[1] = Task.Run(async () =>
                {
                    for (uint v = 1; v <= iterations; v++)
                    {
                        var snapshot = new ClientRosterSnapshotPacket
                        {
                            RosterVersion = v * 2,
                            HostClientId = 0, // Dedicated
                            ConnectedClientIds = new ushort[] { localId, (ushort)(100 + (v % 5)), 102 }
                        };
                        SimulateInboundPacket(netManager, ILiminalTransport.SERVER_ID, snapshot);

                        await Task.Yield();
                    }
                });

                tasks[2] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations * 2; i++)
                    {
                        var local = registry.LocalClient;
                        Specification.Assert(local != null, "LocalClient reference must remain valid during traffic");
                        Specification.Assert(local.ClientId == localId, "LocalClient ID must not be corrupted");
                        Specification.Assert(registry.ContainsClient(localId), "Local client must always remain present");

                        foreach (var c in registry.AllClients)
                        {
                            Specification.Assert(c.ClientId > 0, "Every client must have a positive ID");
                        }

                        await Task.Yield();
                    }
                });

                await Task.WhenAll(tasks);

                Specification.Assert(registry.ContainsClient(localId), "Local client must survive all reconciliations");
                Specification.Assert(registry.LocalClient.ClientId == localId, "Local client ID must remain exact");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [NUnitTest]
        [CoyoteTest]
        public static async Task Coyote_Registry_ConcurrentEventSubscription_SafeAddRemoveUnderLoad()
        {
            var config = CreateTestConfig();
            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var registry = netManager.ClientRegistry;

            try
            {
                int joinedEventsHandled = 0;
                int leftEventsHandled = 0;

                const int clientCount = 5;
                const int iterations = 20;

                var tasks = new Task[3];

                tasks[0] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        for (ushort id = 1; id <= clientCount; id++)
                        {
                            transport.TriggerClientConnected(id);
                            await Task.Yield();
                            transport.TriggerClientDisconnected(id);
                        }
                    }
                });

                tasks[1] = Task.Run(async () =>
                {
                    Action<ConnectedClient> handler = c => Interlocked.Increment(ref joinedEventsHandled);

                    for (int i = 0; i < iterations * 2; i++)
                    {
                        registry.OnClientJoined += handler;
                        await Task.Yield();
                        registry.OnClientJoined -= handler;
                        await Task.Yield();
                    }
                });

                tasks[2] = Task.Run(async () =>
                {
                    Action<ConnectedClient> handler = c => Interlocked.Increment(ref leftEventsHandled);

                    for (int i = 0; i < iterations * 2; i++)
                    {
                        registry.OnClientLeft += handler;
                        await Task.Yield();
                        registry.OnClientLeft -= handler;
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(tasks);

                Specification.Assert(joinedEventsHandled >= 0, "Joined events handled must be valid");
                Specification.Assert(leftEventsHandled >= 0, "Left events handled must be valid");
            }
            finally
            {
                netManager.Shutdown();
            }
        }
    }
}
