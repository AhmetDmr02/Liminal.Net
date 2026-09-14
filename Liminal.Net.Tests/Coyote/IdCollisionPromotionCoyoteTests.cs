using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    public static class IdCollisionPromotionCoyoteTests
    {
        /// <summary>
        /// Models the internal socket and queue table of TcpTransport under ID collision.
        /// </summary>
        private class TransportSocketRegistry
        {
            public readonly struct ConnectionHandle
            {
                public readonly int InstanceGeneration;
                public readonly ushort ClientId;

                public ConnectionHandle(ushort id, int generation)
                {
                    ClientId = id;
                    InstanceGeneration = generation;
                }
            }

            private readonly ConcurrentDictionary<ushort, ConnectionHandle> _activeSockets = new();
            private readonly ConcurrentDictionary<ushort, object> _outboundQueues = new();

            public ConnectionHandle PromoteClient(ushort id, int generation, out ConnectionHandle? oldHandle)
            {
                var newHandle = new ConnectionHandle(id, generation);
                var newQueue = new object();

                ConnectionHandle? capturedOld = null;

                _activeSockets.AddOrUpdate(id,
                    _ => newHandle,
                    (_, existing) =>
                    {
                        capturedOld = existing;
                        return newHandle;
                    });

                oldHandle = capturedOld;
                _outboundQueues.AddOrUpdate(id, _ => newQueue, (_, _) => newQueue);

                return newHandle;
            }

            public void TeardownSocket_Buggy(ushort id)
            {
                _activeSockets.TryRemove(id, out _);
                _outboundQueues.TryRemove(id, out _);
            }

            public void TeardownSocket_Fixed(ConnectionHandle handle)
            {
                ((System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<ushort, ConnectionHandle>>)_activeSockets)
                    .Remove(new System.Collections.Generic.KeyValuePair<ushort, ConnectionHandle>(handle.ClientId, handle));

            }

            public int ActiveSocketCount => _activeSockets.Count;
            public bool HasOutboundQueue(ushort id) => _outboundQueues.ContainsKey(id);
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_Replicate_CollidingId_TeardownCannibalization()
        {
            var registry = new TransportSocketRegistry();
            const ushort targetId = 42;

            int generationCounter = 0;
            var startGate = new TaskCompletionSource<bool>();

            var client1Handle = registry.PromoteClient(targetId, Interlocked.Increment(ref generationCounter), out _);

            var promoteClient2Task = Task.Run(async () =>
            {
                await Task.Yield();
                registry.PromoteClient(targetId, Interlocked.Increment(ref generationCounter), out var evicted);

                if (evicted.HasValue)
                {
                    startGate.TrySetResult(true);
                }
            });

            var client1TeardownTask = Task.Run(async () =>
            {
                await startGate.Task;
                await Task.Yield();

                registry.TeardownSocket_Buggy(targetId);
            });

            await Task.WhenAll(promoteClient2Task, client1TeardownTask);

            Specification.Assert(registry.ActiveSocketCount == 1,
                $"RACE OCCURRED: Active socket count was {registry.ActiveSocketCount}, expected 1! Client 1's teardown deleted Client 2's socket.");

            Specification.Assert(registry.HasOutboundQueue(targetId),
                "RACE OCCURRED: Client 2 lost its outbound queue due to Client 1's concurrent eviction!");
        }

        private class TcpTransportEngine
        {
            public class ClientHandle
            {
                public int Generation { get; }
                public volatile bool IsClosed = false;
                public volatile bool LoopsRunning = false;
                public QueueHandle SendState { get; set; }

                public ClientHandle(int generation)
                {
                    Generation = generation;
                }
            }

            public class QueueHandle
            {
                public int Generation { get; }
                public volatile bool IsDisposed = false;

                public QueueHandle(int generation)
                {
                    Generation = generation;
                }
            }

            public readonly ConcurrentDictionary<ushort, ClientHandle> ActiveSockets = new();
            public readonly ConcurrentDictionary<ushort, QueueHandle> OutboundQueues = new();
            public readonly ConcurrentDictionary<ClientHandle, byte> FinalizedConnections = new();
            private readonly object _connectionLifecycleLock = new();
            public int TotalConnections = 0;

            public bool TryClaimDisconnect(ClientHandle handle) => FinalizedConnections.TryAdd(handle, 0);

            public bool PromoteClient_Unsynchronized(ushort clientId, ClientHandle client, out ClientHandle evictedClient, out QueueHandle evictedQueue)
            {
                var sendState = new QueueHandle(client.Generation);
                client.SendState = sendState;

                QueueHandle oldQueue = null;
                OutboundQueues.AddOrUpdate(clientId, sendState, (_, old) =>
                {
                    oldQueue = old;
                    old.IsDisposed = true;
                    return sendState;
                });

                ClientHandle oldClient = null;
                ActiveSockets.AddOrUpdate(clientId, client, (_, old) =>
                {
                    oldClient = old;
                    if (TryClaimDisconnect(old))
                    {
                        Interlocked.Decrement(ref TotalConnections);
                    }
                    old.IsClosed = true;
                    return client;
                });

                evictedClient = oldClient;
                evictedQueue = oldQueue;

                if (!IsCurrentConnection(clientId, client, sendState))
                {
                    return false;
                }

                client.LoopsRunning = true;
                return true;
            }

            public bool PromoteClient(ushort clientId, ClientHandle client, out ClientHandle evictedClient, out QueueHandle evictedQueue)
            {
                var sendState = new QueueHandle(client.Generation);
                client.SendState = sendState;

                QueueHandle oldQueue = null;
                ClientHandle oldClient = null;

                lock (_connectionLifecycleLock)
                {
                    OutboundQueues.AddOrUpdate(clientId, sendState, (_, old) =>
                    {
                        oldQueue = old;
                        return sendState;
                    });

                    ActiveSockets.AddOrUpdate(clientId, client, (_, old) =>
                    {
                        oldClient = old;
                        if (TryClaimDisconnect(old))
                        {
                            Interlocked.Decrement(ref TotalConnections);
                        }
                        return client;
                    });

                    client.LoopsRunning = true;
                }

                if (oldQueue != null)
                {
                    oldQueue.IsDisposed = true;
                }

                if (oldClient != null)
                {
                    oldClient.IsClosed = true;
                }

                evictedClient = oldClient;
                evictedQueue = oldQueue;

                return true;
            }

            public bool IsCurrentConnection(ushort clientId, ClientHandle client, QueueHandle sendState)
            {
                return ActiveSockets.TryGetValue(clientId, out var curClient) &&
                       ReferenceEquals(curClient, client) &&
                       OutboundQueues.TryGetValue(clientId, out var curQueue) &&
                       ReferenceEquals(curQueue, sendState);
            }

            public void TeardownSendQueue(ushort clientId, QueueHandle ownedState)
            {
                if (ownedState != null)
                {
                    if (!((System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<ushort, QueueHandle>>)OutboundQueues)
                        .Remove(new System.Collections.Generic.KeyValuePair<ushort, QueueHandle>(clientId, ownedState)))
                    {
                        return;
                    }
                }
                else if (!OutboundQueues.TryRemove(clientId, out _))
                {
                    return;
                }

                if (ownedState != null)
                {
                    ownedState.IsDisposed = true;
                }
            }

            public void TeardownClient(ushort clientId, ClientHandle client, QueueHandle ownedSendState)
            {
                client.IsClosed = true;
                client.LoopsRunning = false;

                TeardownSendQueue(clientId, ownedSendState);

                ((System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<ushort, ClientHandle>>)ActiveSockets)
                    .Remove(new System.Collections.Generic.KeyValuePair<ushort, ClientHandle>(clientId, client));

                if (TryClaimDisconnect(client))
                {
                    Interlocked.Decrement(ref TotalConnections);
                }

                FinalizedConnections.TryRemove(client, out _);
            }
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_Test21_SimultaneousFiveClientPromotion_DemonstrateUnsynchronizedBug()
        {
            var transport = new TcpTransportEngine();
            const ushort targetId = 42;
            const int clientCount = 5;

            var clients = new TcpTransportEngine.ClientHandle[clientCount];
            for (int i = 0; i < clientCount; i++)
            {
                clients[i] = new TcpTransportEngine.ClientHandle(i + 1);
            }

            var promotionTasks = new Task[clientCount];
            var evictionTasks = new ConcurrentBag<Task>();

            for (int i = 0; i < clientCount; i++)
            {
                int index = i;
                promotionTasks[index] = Task.Run(async () =>
                {
                    Interlocked.Increment(ref transport.TotalConnections);
                    await Task.Yield();

                    var client = clients[index];
                    bool promoted = transport.PromoteClient_Unsynchronized(targetId, client, out var evictedClient, out var evictedQueue);

                    if (evictedClient != null)
                    {
                        var evTask = Task.Run(async () =>
                        {
                            await Task.Yield();
                            transport.TeardownClient(targetId, evictedClient, evictedQueue);
                        });
                        evictionTasks.Add(evTask);
                    }
                });
            }

            await Task.WhenAll(promotionTasks);
            await Task.WhenAll(evictionTasks);
            await Task.Yield();

            Specification.Assert(transport.ActiveSockets.Count == 1,
                $"CRITICAL RACE: Active socket count was {transport.ActiveSockets.Count}, expected 1!");

            Specification.Assert(transport.OutboundQueues.ContainsKey(targetId),
                "CRITICAL RACE: Transport lost the outbound queue for ID 42 after concurrent promotion!");

            transport.ActiveSockets.TryGetValue(targetId, out var retainedSocket);
            transport.OutboundQueues.TryGetValue(targetId, out var retainedQueue);

            Specification.Assert(retainedSocket != null && retainedQueue != null,
                "CRITICAL RACE: Missing retained socket or queue!");

            Specification.Assert(retainedSocket.Generation == retainedQueue.Generation,
                $"CRITICAL RACE (SPLIT-BRAIN): Socket generation {retainedSocket.Generation} does not match send queue generation {retainedQueue.Generation}!");

            Specification.Assert(retainedSocket.LoopsRunning,
                $"CRITICAL RACE (ZOMBIE CONNECTION): Retained socket (gen {retainedSocket.Generation}) failed IsCurrentConnection and never started its processing loops!");

            Specification.Assert(!retainedSocket.IsClosed,
                $"CRITICAL RACE: Retained socket (gen {retainedSocket.Generation}) was marked closed by a racing eviction!");
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_Test21_SimultaneousFiveClientPromotion_FixedVerification()
        {
            var transport = new TcpTransportEngine();
            const ushort targetId = 42;
            const int clientCount = 5;

            var clients = new TcpTransportEngine.ClientHandle[clientCount];
            for (int i = 0; i < clientCount; i++)
            {
                clients[i] = new TcpTransportEngine.ClientHandle(i + 1);
            }

            var promotionTasks = new Task[clientCount];
            var evictionTasks = new ConcurrentBag<Task>();

            for (int i = 0; i < clientCount; i++)
            {
                int index = i;
                promotionTasks[index] = Task.Run(async () =>
                {
                    Interlocked.Increment(ref transport.TotalConnections);
                    await Task.Yield();

                    var client = clients[index];
                    bool promoted = transport.PromoteClient(targetId, client, out var evictedClient, out var evictedQueue);

                    if (evictedClient != null)
                    {
                        var evTask = Task.Run(async () =>
                        {
                            await Task.Yield();
                            transport.TeardownClient(targetId, evictedClient, evictedQueue);
                        });
                        evictionTasks.Add(evTask);
                    }
                });
            }

            await Task.WhenAll(promotionTasks);
            await Task.WhenAll(evictionTasks);
            await Task.Yield();

            Specification.Assert(transport.ActiveSockets.Count == 1,
                $"Active socket count was {transport.ActiveSockets.Count}, expected 1!");

            Specification.Assert(transport.OutboundQueues.ContainsKey(targetId),
                "Transport lost the outbound queue for ID 42 after concurrent promotion!");

            transport.ActiveSockets.TryGetValue(targetId, out var retainedSocket);
            transport.OutboundQueues.TryGetValue(targetId, out var retainedQueue);

            Specification.Assert(retainedSocket != null && retainedQueue != null,
                "Missing retained socket or queue!");

            Specification.Assert(retainedSocket.Generation == retainedQueue.Generation,
                $"Split-brain detected: Socket gen {retainedSocket.Generation} != Queue gen {retainedQueue.Generation}!");

            Specification.Assert(retainedSocket.LoopsRunning,
                $"Retained socket (gen {retainedSocket.Generation}) failed IsCurrentConnection and never started its processing loops!");

            Specification.Assert(!retainedSocket.IsClosed,
                $"Retained socket (gen {retainedSocket.Generation}) was marked closed by a racing eviction!");
        }

        /// <summary>
        /// Systematic concurrency test directly exercising the production TcpTransport class.
        /// Pre-connects 5 loopback socket pairs, then launches 5 concurrent tasks promoting
        /// to the exact same client ID (42) to systematically explore all thread interleavings
        /// in TcpTransport.PromoteClient, verifying atomic state updates, eviction teardown,
        /// and absence of split-brain or memory access races.
        /// </summary>
        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_RealTcpTransport_SimultaneousFiveClientPromotion()
        {
            const ushort targetId = 42;
            const int clientCount = 5;

            var config = new LiminalNetworkConfig
            {
                ClientIdResolver = new BaseResolver(),
                SendResponseTimeout = 1,
                MaxPacketCount = 100
            };

            var transport = new TcpTransport();
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var serverSockets = new TcpClient[clientCount];
            var clientSockets = new TcpClient[clientCount];

            for (int i = 0; i < clientCount; i++)
            {
                var client = new TcpClient();
                client.Connect(IPAddress.Loopback, port);
                serverSockets[i] = listener.AcceptTcpClient();
                clientSockets[i] = client;
            }
            listener.Stop();

            try
            {
                var promotionTasks = new Task[clientCount];
                for (int i = 0; i < clientCount; i++)
                {
                    int index = i;
                    promotionTasks[index] = Task.Run(async () =>
                    {
                        await Task.Yield();
                        transport.PromoteClient(targetId, serverSockets[index]);
                    });
                }

                await Task.WhenAll(promotionTasks);
                await Task.Yield();

                Specification.Assert(transport.ConnectedClientCount == 1,
                    $"Active socket count in real TcpTransport was {transport.ConnectedClientCount}, expected 1!");

                Specification.Assert(transport._sockets.ContainsKey(targetId),
                    $"Real TcpTransport missing active socket for client ID {targetId}!");

                Specification.Assert(transport._sendQueues.ContainsKey(targetId),
                    $"Real TcpTransport missing outbound send queue for client ID {targetId}!");

                transport._sockets.TryGetValue(targetId, out var retainedSocket);
                transport._sendQueues.TryGetValue(targetId, out var retainedQueue);

                Specification.Assert(retainedSocket != null && retainedQueue != null,
                    "Missing retained socket or send queue in real TcpTransport!");

                Specification.Assert(!retainedQueue.LifetimeCts.IsCancellationRequested,
                    "Retained send queue was canceled by racing eviction!");
            }
            finally
            {
                transport.Shutdown();

                for (int i = 0; i < clientCount; i++)
                {
                    try { clientSockets[i].Close(); } catch { }
                    try { serverSockets[i].Close(); } catch { }
                }
            }
        }
    }
}