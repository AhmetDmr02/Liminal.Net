using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TransportTimeoutAndCapacityTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7850;
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

        #region Timeout Tests

        [Test]
        public void Test28_ServerReceiveTimeout_SilentClient_GetsKicked()
        {
            var serverTimeoutConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15,
                ReceiveResponseTimeout = 1
            };

            var timeoutServer = new LiminalNetworkManager(new TcpTransport(), serverTimeoutConfig);
            timeoutServer.StartServer("127.0.0.1", _currentTestPort);

            bool serverSawDisconnect = false;
            timeoutServer.Transport.OnClientDisconnected += (id) => serverSawDisconnect = true;

            var client = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            Assert.That(timeoutServer.Transport.ConnectedClientCount, Is.EqualTo(1));

            bool timedOut = SpinWait.SpinUntil(() => serverSawDisconnect, 3000);

            Assert.That(timedOut, Is.True);
            Assert.That(timeoutServer.Transport.ConnectedClientCount, Is.EqualTo(0));

            timeoutServer.Shutdown();
        }

        [Test]
        public void Test29_ClientReceiveTimeout_SilentServer_DisconnectsLocalClient()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var clientTimeoutConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15,
                ReceiveResponseTimeout = 1
            };

            var client = new LiminalNetworkManager(new TcpTransport(), clientTimeoutConfig);
            _clientManagers.Add(client);

            bool clientSawDisconnect = false;
            client.Transport.OnLocalClientDisconnected += (id) => clientSawDisconnect = true;

            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool clientTimedOut = SpinWait.SpinUntil(() => clientSawDisconnect, 3000);

            Assert.That(clientTimedOut, Is.True);
            Assert.That(client.Transport.IsConnected, Is.False);
        }

        [Test]
        public void Test30_ServerSendTimeout_UnresponsiveClientBuffer_DropsConnection()
        {
            var serverSendConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 65535,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15,
                SendResponseTimeout = 1
            };

            var timeoutServer = new LiminalNetworkManager(new TcpTransport(), serverSendConfig);
            timeoutServer.StartServer("127.0.0.1", _currentTestPort);

            using var rawClient = new TcpClient();
            rawClient.NoDelay = true;
            rawClient.Connect("127.0.0.1", _currentTestPort);

            var handshakeResult = DefaultHandshakes.ClientTcpHandshake(rawClient, serverSendConfig).GetAwaiter().GetResult();
            Assert.That(handshakeResult.Success, Is.True);
            ushort assignedId = handshakeResult.ClientId;
            Assert.That(assignedId, Is.Not.EqualTo(0));
            Assert.That(SpinWait.SpinUntil(() => timeoutServer.Transport.ConnectedClientCount == 1, 2000), Is.True);

            // Restrict kernel receive window and omit reading to force TCP window saturation
            rawClient.Client.ReceiveBufferSize = 4096;

            byte[] largePayload = new byte[32768];
            Array.Fill(largePayload, (byte)0xEE);

            bool sendFailed = false;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < 5000)
            {
                if (!timeoutServer.Transport.IsClientConnected(assignedId))
                {
                    sendFailed = true;
                    break;
                }

                timeoutServer.Transport.Send(largePayload, assignedId,TransportFlags.Reliable);
                Thread.Sleep(5);
            }

            Assert.That(sendFailed, Is.True);
            Assert.That(timeoutServer.Transport.IsClientConnected(assignedId), Is.False);

            timeoutServer.Shutdown();
        }

        [Test]
        public void Test31_ClientSendTimeout_UnresponsiveServerBuffer_TriggersShutdown()
        {
            var rawListener = new TcpListener(IPAddress.Parse("127.0.0.1"), _currentTestPort);
            rawListener.Start();

            var clientSendConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 65535,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15,
                SendResponseTimeout = 1
            };

            var client = new LiminalNetworkManager(new TcpTransport(), clientSendConfig);
            _clientManagers.Add(client);

            Task.Run(async () =>
            {
                var acceptedSocket = await rawListener.AcceptTcpClientAsync();
                acceptedSocket.NoDelay = true;
                // Restrict kernel receive window and omit reading to force TCP window saturation
                acceptedSocket.Client.ReceiveBufferSize = 4096;
                // UPDATED: ServerTcpHandshake takes (socket, config, canAccept delegate)
                _ = await DefaultHandshakes.ServerTcpHandshake(acceptedSocket, clientSendConfig, () => true);
            });

            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            byte[] largePayload = new byte[32768];
            Array.Fill(largePayload, (byte)0xFF);

            bool clientShutdown = false;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < 5000)
            {
                if (!client.Transport.IsConnected)
                {
                    clientShutdown = true;
                    break;
                }

                client.Transport.Send(largePayload, ILiminalTransport.SERVER_ID,TransportFlags.Reliable);
                Thread.Sleep(5);
            }

            Assert.That(clientShutdown, Is.True);
            Assert.That(client.Transport.IsConnected, Is.False);

            rawListener.Stop();
        }

        #endregion

        [Test]
        public void Test32_MaxConnectionCount_EnforcesCapUnderTightConcurrency()
        {
            const int maxConnections = 3;
            const int totalAttemptingClients = 10;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxConnectionCount = maxConnections,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var customServer = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            customServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.IsConnected, 2000), Is.True);

            var clients = new ConcurrentBag<LiminalNetworkManager>();
            var connectedClients = new ConcurrentBag<LiminalNetworkManager>();
            var barrier = new Barrier(totalAttemptingClients);

            var tasks = new Task[totalAttemptingClients];
            for (int i = 0; i < totalAttemptingClients; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    var clientConfig = new LiminalTransportConfig
                    {
                        Default_Host = "127.0.0.1",
                        Default_Port = _currentTestPort,
                        TickRate = 60,
                        MaxPacketSizePerBatch = 4096,
                        ClientIdResolver = new BaseResolver(),
                        ConnectionTimeout = 2,
                        HandshakeTimeout = 2
                    };

                    var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
                    clients.Add(client);

                    barrier.SignalAndWait();

                    client.StartClient("127.0.0.1", _currentTestPort);

                    if (SpinWait.SpinUntil(() => client.Transport.IsConnected, 2500))
                    {
                        connectedClients.Add(client);
                    }
                });
            }

            Task.WaitAll(tasks);

            Assert.That(customServer.Transport.ConnectedClientCount, Is.EqualTo(maxConnections),
                "Transport connected count exceeded MaxConnectionCount.");
            Assert.That(connectedClients.Count, Is.EqualTo(maxConnections),
                "More clients established a connection than MaxConnectionCount permits.");

            if (connectedClients.TryTake(out var disconnectedClient))
            {
                disconnectedClient.Disconnect();
            }

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.ConnectedClientCount == maxConnections - 1, 2000), Is.True,
                "Transport failed to drop ConnectedClientCount after graceful disconnect.");

            var lateClientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 2,
                HandshakeTimeout = 2
            };

            var lateClient = new LiminalNetworkManager(new TcpTransport(), lateClientConfig);
            clients.Add(lateClient);
            lateClient.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => lateClient.Transport.IsConnected, 2000), Is.True,
                "New client failed to claim the released connection slot.");
            Assert.That(customServer.Transport.ConnectedClientCount, Is.EqualTo(maxConnections),
                "Server count did not return to max capacity after late client connected.");

            foreach (var c in clients)
            {
                try { c.Shutdown(); } catch { }
            }
            customServer.Shutdown();
        }

        [Test]
        public void Test33_ConcurrentCollisions_UnderHardCap_NeverLeaksSlots()
        {
            const int maxConnections = 2;
            const int collidingAttempts = 8;
            var collisionResolver = new ForceCollisionResolver(targetId: 42, duplicateCount: 100);
            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxConnectionCount = maxConnections,
                ClientIdResolver = collisionResolver,
                ConnectionTimeout = 2,
                HandshakeTimeout = 2
            };
            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            server.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => server.Transport.IsConnected, 2000), Is.True);

            var clients = new ConcurrentBag<LiminalNetworkManager>();
            var settled = new Task[collidingAttempts];

            var barrier = new Barrier(collidingAttempts);
            var tasks = new Task[collidingAttempts];

            for (int i = 0; i < collidingAttempts; i++)
            {
                var tcs = new TaskCompletionSource<bool>();
                settled[i] = tcs.Task;

                tasks[i] = Task.Run(() =>
                {
                    var cfg = new LiminalTransportConfig
                    {
                        Default_Host = "127.0.0.1",
                        Default_Port = _currentTestPort,
                        TickRate = 60,
                        MaxPacketSizePerBatch = 4096,
                        ClientIdResolver = new BaseResolver(),
                        ConnectionTimeout = 2,
                        HandshakeTimeout = 2
                    };
                    var c = new LiminalNetworkManager(new TcpTransport(), cfg);
                    clients.Add(c);

                    c.Transport.OnLocalClientConnected += _ => tcs.TrySetResult(true);
                    c.Transport.OnShutdown += () => tcs.TrySetResult(false);

                    barrier.SignalAndWait();
                    c.StartClient("127.0.0.1", _currentTestPort);
                });
            }

            Task.WaitAll(tasks);

            bool allSettled = Task.WaitAll(settled, TimeSpan.FromSeconds(5));
            Assert.That(allSettled, Is.True, "Storm clients never reached a terminal state in time.");

            Assert.That(SpinWait.SpinUntil(() => server.Transport.ConnectedClientCount == 1, 3000), Is.True,
                "Expected exactly 1 socket surviving in the dictionary for ID 42.");

            collisionResolver.SetTargetId(999);
            var testClient = new LiminalNetworkManager(new TcpTransport(), new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 2,
                HandshakeTimeout = 2
            });
            clients.Add(testClient);
            testClient.StartClient("127.0.0.1", _currentTestPort);
            bool admitted = SpinWait.SpinUntil(() => testClient.Transport.IsConnected, 2500);
            Assert.That(admitted, Is.True, "CAPACITY LEAK: Server locked up because replaced sockets never decremented _totalConnections.");
            Assert.That(server.Transport.ConnectedClientCount, Is.EqualTo(maxConnections));

            foreach (var c in clients) try { c.Shutdown(); } catch { }
            server.Shutdown();
        }

        private class ForceCollisionResolver : ILiminalClientIdResolver
        {
            private volatile ushort _targetId;
            private readonly int _duplicateCount;
            private int _callCount = 0;
            private int _confirmedCount = 0;

            public int ConfirmedCount => Volatile.Read(ref _confirmedCount);
            public void SetTargetId(ushort newTargetId) => _targetId = newTargetId;

            public ForceCollisionResolver(ushort targetId, int duplicateCount)
            {
                _targetId = targetId;
                _duplicateCount = duplicateCount;
            }

            public void Initialize(ILiminalTransport transport) { }

            public ushort GenerateClientId()
            {
                int count = Interlocked.Increment(ref _callCount);
                return count <= _duplicateCount ? _targetId : (ushort)0;
            }

            public void ConfirmRegistration(ushort targetId)
            {
                Interlocked.Increment(ref _confirmedCount);
            }

            public ushort ResolveId(Span<byte> payload) => 0;

            public void ResetResolver()
            {
                Interlocked.Exchange(ref _callCount, 0);
                Interlocked.Exchange(ref _confirmedCount, 0);
            }
        }

        [Test]
        public void Test34_Kick_ReclaimsConnectionSlotImmediately()
        {
            const int maxConnections = 1;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxConnectionCount = maxConnections,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            server.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => server.Transport.IsConnected, 2000), Is.True);

            var client1 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client1.Transport.IsConnected, 2000), Is.True);
            Assert.That(server.Transport.ConnectedClientCount, Is.EqualTo(1));

            ushort client1Id = client1.localID;
            Assert.That(client1Id, Is.Not.EqualTo(0));

            server.Transport.Kick(client1Id);

            Assert.That(SpinWait.SpinUntil(() => server.Transport.ConnectedClientCount == 0, 2000), Is.True,
                "Server failed to decrement ConnectedClientCount after Kick.");

            var client2 = CreateAndStartClient();
            bool client2Connected = SpinWait.SpinUntil(() => client2.Transport.IsConnected, 2000);

            Assert.That(client2Connected, Is.True,
                "Kick dropped the socket but leaked the capacity slot, blocking future clients.");
            Assert.That(server.Transport.ConnectedClientCount, Is.EqualTo(1));

            client2.Shutdown();
            server.Shutdown();
        }
    }
}