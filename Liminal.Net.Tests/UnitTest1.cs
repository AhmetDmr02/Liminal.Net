using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TransportIntegrationTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7770;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new ();

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

        private LiminalNetworkManager CreateAndStartClient()
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
            var client = new LiminalNetworkManager(new TcpTransport(), config);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        [Test]
        public void Test01_ServerStartAndStop_ClearsState()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            _serverManager.Shutdown();
            Assert.That(_serverManager.Transport.IsConnected, Is.False);
            Assert.That(_serverManager.Role, Is.EqualTo(NetworkRole.None));
        }

        [Test]
        public void Test02_ClientConnect_WithoutServer_FailsGracefully()
        {
            var client = CreateAndStartClient();

            bool connected = SpinWait.SpinUntil(() => client.Transport.IsConnected, 1000);
            Assert.That(connected, Is.False, "Client magically connected to a non-existent server.");

            bool roleReset = SpinWait.SpinUntil(() => client.Role == NetworkRole.None, 6000);

            Assert.That(roleReset, Is.True, "Client role did not reset to None after the connection timeout.");
        }

        [Test]
        public void Test03_ClientConnect_And_Disconnect_FiresEvents()
        {
            bool serverSawConnect = false;
            bool serverSawDisconnect = false;

            _serverManager.Transport.OnClientConnected += (id) => serverSawConnect = true;
            _serverManager.Transport.OnClientDisconnected += (id) => serverSawDisconnect = true;

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => serverSawConnect, 2000), Is.True, "Server missed connect event.");

            client.Disconnect();
            Assert.That(SpinWait.SpinUntil(() => serverSawDisconnect, 2000), Is.True, "Server missed disconnect event.");
        }

        [Test]
        public void Test04_SendReliable_SmallPayload_Delivered()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            bool received = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) => { received = (pkt.Message == "Ping"); }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "Ping" });
            Assert.That(SpinWait.SpinUntil(() => received, 2000), Is.True, "Packet not delivered.");
        }

        [Test]
        public void Test05_SendReliable_FilePayload_DeliveredIntact()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            byte[] sentData = new byte[1024]; // 1KB test file
            for (int i = 0; i < sentData.Length; i++) sentData[i] = (byte)(i % 255);

            bool fileMatched = false;
            client.Interpreter.Subscribe<FilePacket>((pkt, id) =>
            {
                if (pkt.FileName == "test.bin" && pkt.Data.Length == 1024 && pkt.Data[50] == sentData[50])
                    fileMatched = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "test.bin", Data = sentData });
            Assert.That(SpinWait.SpinUntil(() => fileMatched, 2000), Is.True, "File packet corrupted or dropped.");
        }

        [Test]
        public void Test06_SendReliable_ExceedsMaxPacketSize_DropsGracefully()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            byte[] oversizedData = new byte[5000];

            Assert.DoesNotThrow(() =>
            {
                _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "huge.bin", Data = oversizedData });
            });
        }

        [Test]
        public void Test07_ServerKicksClient_ClientReceivesDisconnect()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool clientSawDisconnect = false;
            client.Transport.OnLocalClientDisconnected += (id) => clientSawDisconnect = true;

            _serverManager.Transport.Kick(1);

            Assert.That(SpinWait.SpinUntil(() => clientSawDisconnect, 2000), Is.True, "Client did not detect being kicked.");
            Assert.That(client.Transport.IsConnected, Is.False);
        }

        [Test]
        public void Test08_ServerShutdown_DropsAllActiveClients()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => c1.Transport.IsConnected && c2.Transport.IsConnected, 2000), Is.True);

            _serverManager.Shutdown();

            Assert.That(SpinWait.SpinUntil(() => !c1.Transport.IsConnected, 2000), Is.True, "Client 1 stayed alive.");
            Assert.That(SpinWait.SpinUntil(() => !c2.Transport.IsConnected, 2000), Is.True, "Client 2 stayed alive.");
        }

        [Test]
        public void Test09_MultipleClients_ConnectAndReceiveDistinctIds()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clients = new List<LiminalNetworkManager>();
            var assignedIds = new ConcurrentBag<ushort>();

            for (int i = 0; i < 3; i++)
            {
                var config = new LiminalTransportConfig
                {
                    Default_Host = "127.0.0.1",
                    Default_Port = _currentTestPort,
                    TickRate = 60,
                    MaxPacketSizePerBatch = 4096,
                    ClientIdResolver = new BaseResolver()
                };
                var c = new LiminalNetworkManager(new TcpTransport(), config);
                _clientManagers.Add(c);
                clients.Add(c);

                c.Transport.OnLocalClientConnected += (id) => assignedIds.Add(id);

                c.StartClient("127.0.0.1", _currentTestPort);
            }

            Assert.That(SpinWait.SpinUntil(() => assignedIds.Count == 3, 2000), Is.True, "Failed to capture all 3 connection events.");
            CollectionAssert.AllItemsAreUnique(assignedIds, "Resolver handed out duplicate IDs.");
        }

        [Test]
        public void Test10_RapidSpam_DoesNotCorruptBuffer()
        {
            _serverConfig.MaxPacketCount = 150;
            _serverConfig.MaxPacketSizePerBatch = 65535;

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 65535,
                MaxPacketCount = 150,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            int receivedCount = 0;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref receivedCount), this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            for (int i = 0; i < 100; i++)
            {
                _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "Spam" });
            }

            Assert.That(SpinWait.SpinUntil(() => receivedCount == 100, 3000), Is.True, $"Only received {receivedCount}/100 packets.");
        }

        [Test]
        public void Test11_HostMode_InitializesAndConnectsLocalClient()
        {
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True, "Host failed to start or Local Client failed to connect to itself.");

            Assert.That(_serverManager.Role, Is.EqualTo(NetworkRole.Host));

            var tcpTransport = (TcpTransport)_serverManager.Transport;
            Assert.That(tcpTransport.IsServer, Is.True);
            Assert.That(tcpTransport.IsClient, Is.True);

            Assert.That(_serverManager.SessionManager.GetActiveSessionCount(), Is.GreaterThanOrEqualTo(1), "Host SessionManager failed to register the local client session.");
        }

        [Test]
        public void Test12_HostMode_RemoteClientCanConnectToHost()
        {
            ushort remoteClientId = 0;
            _serverManager.Transport.OnClientConnected += (id) =>
            {
                if (id != _serverManager.localID) remoteClientId = id;
            };

            _serverManager.StartHost();
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var remoteClient = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected, 2000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => remoteClientId != 0, 2000), Is.True, "Host did not detect the remote client connecting.");
        }

        [Test]
        public void Test13_HostMode_TwoWayCommunicationWithRemoteClient()
        {
            ushort remoteClientId = 0;
            _serverManager.Transport.OnClientConnected += (id) =>
            {
                if (id != _serverManager.localID) remoteClientId = id;
            };

            _serverManager.StartHost();
            var remoteClient = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => remoteClientId != 0, 2000), Is.True);

            bool hostReceived = false;
            bool remoteReceived = false;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "FromRemote" && id == remoteClientId) hostReceived = true;
            }, this);

            remoteClient.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "FromHost" && id == ILiminalTransport.SERVER_ID) remoteReceived = true;
            }, this);

            remoteClient.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "FromRemote" });

            _serverManager.Interpreter.SendCommand(remoteClientId, new ChatPacket { Message = "FromHost" });

            Assert.That(SpinWait.SpinUntil(() => hostReceived, 2000), Is.True, "Host failed to receive packet from Remote Client.");
            Assert.That(SpinWait.SpinUntil(() => remoteReceived, 2000), Is.True, "Remote Client failed to receive packet from Host.");
        }

        [Test]
        public void Test14_ServerOnly_VirtualLoopback_DeliversToSelf()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True, "Server failed to start.");

            bool receivedSelf = false;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, senderId) =>
            {
                if (pkt.Message == "ServerSelfLoop" && senderId == ILiminalTransport.SERVER_ID)
                {
                    receivedSelf = true;
                }
            }, this);

            _serverManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "ServerSelfLoop" });

            Assert.That(SpinWait.SpinUntil(() => receivedSelf, 2000), Is.True, "Server did not receive its own virtual loopback packet.");
        }

        [Test]
        public void Test15_ClientOnly_VirtualLoopback_DeliversToSelf()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True, "Client failed to connect.");

            ushort myId = client.localID;
            Assert.That(myId, Is.Not.EqualTo(0), "Client was not assigned a valid ID.");

            bool receivedSelf = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, senderId) =>
            {
                if (pkt.Message == "ClientSelfLoop" && senderId == myId)
                {
                    receivedSelf = true;
                }
            }, this);

            client.Interpreter.SendCommand(myId, new ChatPacket { Message = "ClientSelfLoop" });

            Assert.That(SpinWait.SpinUntil(() => receivedSelf, 2000), Is.True, "Client did not receive its own virtual loopback packet.");
        }

        [Test]
        public void Test16_VirtualLoopback_InvalidTarget_DropsSilently()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True, "Server failed to start.");

            Assert.DoesNotThrow(() =>
            {
                _serverManager.Interpreter.SendCommand(999, new ChatPacket { Message = "IntoTheVoid" });
            }, "Sending to an invalid target caused an exception instead of dropping silently.");

            Assert.DoesNotThrow(() =>
            {
                _serverManager.ManualPoll();
            }, "Polling after a dropped packet caused an exception.");
        }
        [Test]
        public void Test17_Framer_Concurrency_Stress()
        {
            var racer = new RaceTriggerProcessor();
            _serverConfig.OutboundPacketProcessors.Add(racer);
            _serverConfig.InboundPacketProcessors.Add(racer);

            _serverConfig.MaxPacketSizePerBatch = ushort.MaxValue;

            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientManager = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => clientManager.Transport.IsConnected, 5000), Is.True);

            ushort assignedId = clientManager.localID;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, this);
            clientManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, this);

            int failed = 0;
            long serverSent = 0;
            long clientSent = 0;

            int iterations = 1000;

            int flushBatchSize = 20;
            int workerCount = Math.Max(4, Environment.ProcessorCount);

            var startGate = new CountdownEvent(1);
            var doneGate = new CountdownEvent(workerCount * 2);

            Task[] tasks = new Task[workerCount * 2];

            for (int w = 0; w < workerCount; w++)
            {
                int workerIndex = w;

                tasks[w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 1; i <= iterations; i++)
                    {
                        try
                        {
                            _serverManager.Interpreter.SendCommand(assignedId, new ChatPacket { Message = $"S|{workerIndex}|{i}" });
                            Interlocked.Increment(ref serverSent);

                            if (i % flushBatchSize == 0)
                            {
                                _serverManager.SessionManager.Flush();
                            }
                        }
                        catch
                        {
                            Interlocked.Exchange(ref failed, 1);
                            break;
                        }
                    }
                    _serverManager.SessionManager.Flush();
                    doneGate.Signal();
                });

                tasks[workerCount + w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 1; i <= iterations; i++)
                    {
                        try
                        {
                            clientManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"C|{workerIndex}|{i}" });
                            Interlocked.Increment(ref clientSent);

                            if (i % flushBatchSize == 0)
                            {
                                clientManager.SessionManager.Flush();
                            }
                        }
                        catch
                        {
                            Interlocked.Exchange(ref failed, 1);
                            break;
                        }
                    }
                    clientManager.SessionManager.Flush();
                    doneGate.Signal();
                });
            }

            startGate.Signal();

            bool completed = doneGate.Wait(TimeSpan.FromSeconds(60));
            Assert.That(completed, Is.True, "Timeout waiting for worker tasks to finish.");

            Assert.That(Interlocked.CompareExchange(ref failed, 0, 0), Is.EqualTo(0), "Framer collision or transport failure detected.");
            Assert.That(serverSent, Is.EqualTo((long)iterations * workerCount));
            Assert.That(clientSent, Is.EqualTo((long)iterations * workerCount));

            _serverConfig.MaxPacketSizePerBatch = 4096;
        }

        private class RaceTriggerProcessor : ILiminalInboundTransformer, ILiminalOutboundTransformer
        {
            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                for (int i = 0; i < input.Length; i++) output[i] = (byte)(input[i] ^ 0xFF);
                Thread.SpinWait(100);
                return input.Length;
            }

            public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                for (int i = 0; i < input.Length; i++) output[i] = (byte)(input[i] ^ 0xFF);
                Thread.SpinWait(100);
                return input.Length;
            }
        }
        [Test]
        public void Test18_UngracefulSocketClose_TriggersFatalLog()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var clientManager = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => clientManager.Transport.IsConnected, 2000), Is.True);

            Thread.Sleep(100);

            ushort targetId = clientManager.localID;

            _serverManager.Transport.Kick(targetId);

            Thread.Sleep(200);

            Assert.That(_serverManager.SessionManager.GetActiveSessionCount(), Is.EqualTo(0), "Session should be removed.");
        }
        // Carried Over To The Coyote Tests
        //[Test]
        //public void Test19_ConcurrentClientConnections_NoDuplicateIds()
        //{
        //    ThreadPool.SetMinThreads(200, 200);
        //    _serverManager.StartServer("127.0.0.1", _currentTestPort);
        //    Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

        //    const int CLIENT_COUNT = 10;
        //    var connectedIds = new ConcurrentBag<ushort>();
        //    var connectionEvents = new CountdownEvent(CLIENT_COUNT);

        //    _serverManager.Transport.OnClientConnected += (clientId) =>
        //    {
        //        connectedIds.Add(clientId);
        //        connectionEvents.Signal();
        //    };

        //    var clientTasks = new List<Task>();

        //    var startGate = new TaskCompletionSource<bool>();

        //    for (int i = 0; i < CLIENT_COUNT; i++)
        //    {
        //        clientTasks.Add(Task.Run(async () =>
        //        {
        //            await startGate.Task;

        //            var client = CreateAndStartClient();
        //        }));
        //    }

        //    startGate.SetResult(true);

        //    bool allConnected = connectionEvents.Wait(TimeSpan.FromSeconds(15));

        //    Assert.That(allConnected, Is.True, $"Only {CLIENT_COUNT - connectionEvents.CurrentCount}/{CLIENT_COUNT} clients connected in time.");

        //    Thread.Sleep(500);

        //    var idList = connectedIds.ToList();
        //    var uniqueIds = idList.Distinct().ToList();

        //    Assert.That(idList.Count, Is.EqualTo(uniqueIds.Count),
        //        $"RACE CONDITION DETECTED: {idList.Count - uniqueIds.Count} duplicate ID(s) assigned. " +
        //        $"IDs: [{string.Join(", ", idList.OrderBy(x => x))}]");

        //    foreach (var clientId in idList)
        //    {
        //        Assert.That(_serverConfig.ClientIdResolver.IsConnectionActive(clientId), Is.True,
        //            $"Client {clientId} connected but not registered in resolver.");
        //    }

        //    LiminalLogger.Log($"[Test19] Successfully tested {CLIENT_COUNT} concurrent connections - all IDs unique.");
        //}

        [Test]
        public void Test20_RapidConnectDisconnect_NoResourceLeaks()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            const int ITERATIONS = 30;

            for (int i = 0; i < ITERATIONS; i++)
            {
                var client = CreateAndStartClient();

                bool connected = SpinWait.SpinUntil(() => client.Transport.IsConnected, 1000);

                if (connected)
                {
                    client.Disconnect();
                    Thread.Sleep(50);
                }
            }

            Thread.Sleep(500);

            int activeCount = _serverManager.Transport.ConnectedClientCount;

            Assert.That(activeCount, Is.EqualTo(0),
                $"Socket leak detected: {activeCount} clients still connected in transport after disconnect.");

            LiminalLogger.Log($"[Test20] Successfully completed {ITERATIONS} rapid connect/disconnect cycles.");
        }

        [Test]
        public void Test21_SimultaneousSameIdPromotion_OnlyOneSucceeds()
        {
            var maliciousResolver = new ForceCollisionResolver(targetId: 42, duplicateCount: 5);

            var testConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = maliciousResolver
            };

            var testServer = new LiminalNetworkManager(new TcpTransport(), testConfig);
            testServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => testServer.Transport.IsConnected, 2000), Is.True);

            var connectedIds = new ConcurrentBag<ushort>();
            testServer.Transport.OnClientConnected += (id) => connectedIds.Add(id);

            var clients = new List<LiminalNetworkManager>();
            var startGate = new ManualResetEventSlim(false);

            for (int i = 0; i < 5; i++)
            {
                Task.Run(() =>
                {
                    startGate.Wait();

                    var clientConfig = new LiminalTransportConfig
                    {
                        Default_Host = "127.0.0.1",
                        Default_Port = _currentTestPort,
                        TickRate = 60,
                        MaxPacketSizePerBatch = 4096,
                        ClientIdResolver = new BaseResolver()
                    };

                    var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
                    lock (clients) clients.Add(client);

                    client.StartClient("127.0.0.1", _currentTestPort);
                });
            }

            startGate.Set();
            Thread.Sleep(2000);

            int confirmedCount = maliciousResolver.ConfirmedCount;
            int activeSocketsInTransport = testServer.Transport.ConnectedClientCount;

            // Transport replaces colliding IDs cleanly (AddOrUpdate), so exactly 1 remains in memory
            Assert.That(activeSocketsInTransport, Is.EqualTo(1),
                $"Expected 1 active socket in transport for ID 42, but found {activeSocketsInTransport}.");

            testServer.Shutdown();
            lock (clients)
            {
                foreach (var c in clients)
                {
                    try { c.Shutdown(); } catch { }
                }
            }

            LiminalLogger.Log($"[Test21] Collision test complete. Resolver handed out ID 42 5x, transport retained exactly {activeSocketsInTransport} socket.");
        }

        private class ForceCollisionResolver : ILiminalClientIdResolver
        {
            private readonly ushort _targetId;
            private readonly int _duplicateCount;
            private int _callCount = 0;
            private int _confirmedCount = 0;

            public int ConfirmedCount => Volatile.Read(ref _confirmedCount);

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
        public void Test22_Chaos_Teardown_Under_Heavy_Load()
        {
            var chaosTransformer = new ChaosTransformer();
            _serverConfig.InboundPacketProcessors.Add(chaosTransformer);
            _serverConfig.OutboundPacketProcessors.Add(chaosTransformer);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) => { }, this);

            int clientCount = 10;
            var activeClients = new List<LiminalNetworkManager>();
            var startGate = new ManualResetEventSlim(false);

            int architecturalExceptions = 0;

            for (int i = 0; i < clientCount; i++)
            {
                var c = CreateAndStartClient();
                activeClients.Add(c);
            }

            Assert.That(SpinWait.SpinUntil(() => _serverManager.SessionManager.GetActiveSessionCount() == clientCount, 3000), Is.True);

            var spamTasks = new List<Task>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            foreach (var client in activeClients)
            {
                spamTasks.Add(Task.Run(() =>
                {
                    startGate.Wait();
                    try
                    {
                        while (!cts.IsCancellationRequested)
                        {
                            if (client.Transport.IsConnected)
                            {
                                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Chaos" });
                                client.SessionManager.Flush();
                            }

                            if (Random.Shared.Next(100) < 5) Thread.Yield();
                        }
                    }
                    catch (Exception ex)
                    {
                        if (!(ex is System.IO.IOException || ex is System.Net.Sockets.SocketException))
                        {
                            LiminalLogger.LogError($"[Chaos] Race Condition Caught: {ex.GetType().Name} - {ex.Message}");
                            Interlocked.Increment(ref architecturalExceptions);
                        }
                    }
                }));
            }

            startGate.Set();

            Thread.Sleep(500);

            var teardownTasks = new List<Task>
    {
        Task.Run(() => _serverManager.Shutdown())
    };

            for (int i = 0; i < clientCount; i++)
            {
                int index = i;
                teardownTasks.Add(Task.Run(() =>
                {
                    if (Random.Shared.Next(100) < 50) Thread.Sleep(Random.Shared.Next(1, 10));

                    if (activeClients[index] != null && activeClients[index].Transport.IsConnected)
                    {
                        activeClients[index].Disconnect();
                    }
                }));
            }

            Task.WaitAll(teardownTasks.ToArray());
            cts.Cancel();
            Task.WaitAll(spamTasks.ToArray());

            Assert.That(architecturalExceptions, Is.EqualTo(0), "RACE CONDITION DETECTED! An internal exception (like NullReference or ObjectDisposed) leaked during concurrent teardown.");
            Assert.That(_serverManager.Transport.IsConnected, Is.False, "Server failed to fully shut down.");
            Assert.That(_serverManager.SessionManager.GetActiveSessionCount(), Is.EqualTo(0), "Session leak detected after chaos shutdown.");
        }

        private class ChaosTransformer : ILiminalInboundTransformer, ILiminalOutboundTransformer
        {
            public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                input.CopyTo(output);
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                return input.Length;
            }

            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                input.CopyTo(output);
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                return input.Length;
            }
        }

        [Test]
        public void Test23_CustomFraming_ValidHeader_DeliveredAndParsedSuccessfully()
        {
            var serverFramer = new SecureFramingProvider();
            var clientFramer = new SecureFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = clientFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            bool received = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "FramedPing") received = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            // Send 10 packets from Server to Client
            for (int i = 0; i < 10; i++)
            {
                _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "FramedPing" });
                _serverManager.SessionManager.Flush(); 
            }

            Assert.That(SpinWait.SpinUntil(() => received, 2000), Is.True, "Framed packet failed delivery.");
            Assert.That(SpinWait.SpinUntil(() => clientFramer.ValidPacketsCount >= 10, 2000), Is.True,
                $"Custom framing reader was not invoked on client. Valid: {clientFramer.ValidPacketsCount}, Corrupted: {clientFramer.CorruptedPacketsCount}");
            Assert.That(clientFramer.CorruptedPacketsCount, Is.EqualTo(0), "Custom framing detected unexpected corruption.");
        }

        [Test]
        public void Test24_CustomFraming_TamperedMagicCookie_TriggersMalformedKick()
        {
            var serverFramer = new SecureFramingProvider();
            var maliciousFramer = new BadMagicFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = maliciousFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);

            bool serverKickedClient = false;
            _serverManager.Transport.OnClientKicked += (id) => serverKickedClient = true;

            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            // Client sends packet with bad magic cookie
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Exploit" });
            client.SessionManager.Flush();

            // Server should detect bad magic, increment corruption count, and kick
            Assert.That(SpinWait.SpinUntil(() => serverFramer.CorruptedPacketsCount > 0, 2000), Is.True, "Server framer missed bad magic cookie.");
            Assert.That(SpinWait.SpinUntil(() => serverKickedClient, 2000), Is.True, "Server failed to kick client with malformed header.");
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True, "Client remained connected after kick.");
        }

        [Test]
        public void Test25_CustomFraming_HighConcurrency_ZeroRaceConditionsOnHeaderContext()
        {
            var serverFramer = new SecureFramingProvider();
            var clientFramer = new SecureFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverConfig.MaxPacketSizePerBatch = ushort.MaxValue;
            _serverConfig.MaxPacketCount = ushort.MaxValue;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = ushort.MaxValue,
                MaxPacketCount = ushort.MaxValue,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = clientFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort assignedId = client.localID;

            int totalPackets = 1000;
            int clientReceived = 0;
            int serverReceived = 0;

            client.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref clientReceived), this);
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref serverReceived), this);

            int workerCount = 4;
            var startGate = new ManualResetEventSlim(false);
            Task[] workers = new Task[workerCount * 2];

            for (int w = 0; w < workerCount; w++)
            {
                workers[w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 0; i < totalPackets / workerCount; i++)
                    {
                        _serverManager.Interpreter.SendCommand(assignedId, new ChatPacket { Message = "S2C" });
                        if (i % 25 == 0) _serverManager.SessionManager.Flush();
                    }
                    _serverManager.SessionManager.Flush();
                });

                workers[workerCount + w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 0; i < totalPackets / workerCount; i++)
                    {
                        client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "C2S" });
                        if (i % 25 == 0) client.SessionManager.Flush();
                    }
                    client.SessionManager.Flush();
                });
            }

            startGate.Set();
            Task.WaitAll(workers);

            Assert.That(SpinWait.SpinUntil(() => clientReceived == totalPackets, 5000), Is.True, $"Client received {clientReceived}/{totalPackets}");
            Assert.That(SpinWait.SpinUntil(() => serverReceived == totalPackets, 5000), Is.True, $"Server received {serverReceived}/{totalPackets}");

            Assert.That(serverFramer.CorruptedPacketsCount, Is.EqualTo(0), "Server detected header corruption under high load.");
            Assert.That(clientFramer.CorruptedPacketsCount, Is.EqualTo(0), "Client detected header corruption under high load.");
        }

        #region Test Framing Providers

        public readonly struct SecureFramingContext
        {
            public readonly uint MagicCookie;
            public readonly ushort SequenceNumber;

            public SecureFramingContext(uint magicCookie, ushort sequenceNumber)
            {
                MagicCookie = magicCookie;
                SequenceNumber = sequenceNumber;
            }
        }

        /// <summary>
        /// Custom 6-byte header extension: [uint MagicCookie (4b)][ushort SequenceNumber (2b)].
        /// Validates magic cookie and tracks received sequence numbers atomically.
        /// </summary>
        public class SecureFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public const uint ExpectedMagic = 0xDEADBEEF;
            public int CustomHeaderSize => 6;

            public int CorruptedPacketsCount = 0;
            public int ValidPacketsCount = 0;
            public readonly ConcurrentBag<ushort> ReceivedSequences = new();

            private int _outboundSeq = 0;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                uint cookie = context.MagicCookie != 0 ? context.MagicCookie : ExpectedMagic;
                ushort seq = context.SequenceNumber != 0 ? context.SequenceNumber : (ushort)Interlocked.Increment(ref _outboundSeq);

                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), cookie);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), seq);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
                ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));

                if (cookie != ExpectedMagic)
                {
                    Interlocked.Increment(ref CorruptedPacketsCount);
                    context = default;
                    return false;
                }

                Interlocked.Increment(ref ValidPacketsCount);
                ReceivedSequences.Add(seq);
                context = new SecureFramingContext(cookie, seq);
                return true;
            }
        }

        public class BadMagicFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public int CustomHeaderSize => 6;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                // Deliberately write invalid magic cookie
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), 0xBAADC0DE);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), 999);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                context = default;
                return true;
            }
        }
        [Test]
        public void Test26_InboundQueue_WithinMaxPacketCount_ProcessesSuccessfully()
        {
            const int maxPacketCount = 10;
            const int sentPackets = 5;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = maxPacketCount,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var customServer = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            customServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.IsConnected, 2000), Is.True);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int serverReceivedCount = 0;
            customServer.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref serverReceivedCount), this);

            for (int i = 0; i < sentPackets; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"SafeBatch_{i}" });
            }
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => serverReceivedCount == sentPackets, 2000), Is.True,
                $"Server only processed {serverReceivedCount}/{sentPackets} packets within the threshold.");
            Assert.That(client.Transport.IsConnected, Is.True, "Client was kicked unexpectedly under the packet limit.");

            customServer.Shutdown();
        }

        [Test]
        public void Test27_InboundQueue_ExceedingMaxPacketCount_KicksOffendingClient()
        {
            const int maxPacketCount = 5;
            const int overflowPackets = 15;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = maxPacketCount,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var customServer = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            customServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.IsConnected, 2000), Is.True);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool serverKickedClient = false;
            customServer.Transport.OnClientKicked += (id) => serverKickedClient = true;

            // Send more packets in a single batch than the server's InboundQueue MaxPacketCount can tolerate
            for (int i = 0; i < overflowPackets; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"Flood_{i}" });
            }
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => serverKickedClient, 2000), Is.True,
                "Server failed to kick client after exceeding InboundQueue MaxPacketCount.");
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True,
                "Client remained connected after exceeding InboundQueue capacity limit.");

            customServer.Shutdown();
        }
        #endregion
    }
}