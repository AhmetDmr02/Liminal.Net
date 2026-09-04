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
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using Liminal.Net.BasePackets;
using MessagePack;

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

            byte[] sentData = new byte[1024];
            Random.Shared.NextBytes(sentData);

            byte[] expectedHash = SHA256.HashData(sentData);

            bool packetReceived = false;
            byte[] actualHash = null;
            bool byteSequenceMatched = false;

            client.Interpreter.Subscribe<FilePacket>((pkt, id) =>
            {
                if (pkt.FileName == "test.bin" && pkt.Data != null)
                {
                    packetReceived = true;
                    actualHash = SHA256.HashData(pkt.Data);
                    byteSequenceMatched = sentData.AsSpan().SequenceEqual(pkt.Data);
                }
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True, "Client failed to connect.");

            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "test.bin", Data = sentData });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => packetReceived, 2000), Is.True, "File packet was not delivered.");

            Assert.That(actualHash, Is.Not.Null);
            Assert.That(
                Convert.ToHexString(actualHash),
                Is.EqualTo(Convert.ToHexString(expectedHash)),
                "SHA-256 digest mismatch: payload was modified or corrupted in transit."
            );
            Assert.That(byteSequenceMatched, Is.True, "Direct byte sequence comparison failed.");
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

            ushort assignedId = DefaultHandshakes.ClientTcpHandshake(rawClient, serverSendConfig).GetAwaiter().GetResult();
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

                timeoutServer.Transport.SendReliable(largePayload, assignedId);
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
                _ = await DefaultHandshakes.ServerTcpHandshake(acceptedSocket, clientSendConfig);
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

                client.Transport.SendReliable(largePayload, ILiminalTransport.SERVER_ID);
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
        #region Dynamic Packet Library & Determinism Tests

        [Test]
        public void Test35_PacketLibrary_HandshakePackets_RegisteredDeterministicallyWithNonZeroIds()
        {
            LiminalPacketLibrary.Initialize();

            ushort clientPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();
            ushort serverPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
            ushort ackPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();

            Assert.Multiple(() =>
            {
                Assert.That(clientPacketId, Is.GreaterThan(0), "ConnectionHandshakePacketClient must have an ID > 0.");
                Assert.That(serverPacketId, Is.GreaterThan(0), "ConnectionHandshakePacketServer must have an ID > 0.");
                Assert.That(ackPacketId, Is.GreaterThan(0), "ConnectionHandshakeClientAck must have an ID > 0.");

                // Verify IDs are distinct
                Assert.That(clientPacketId, Is.Not.EqualTo(serverPacketId), "Client and Server handshake packets share an ID.");
                Assert.That(serverPacketId, Is.Not.EqualTo(ackPacketId), "Server and ACK handshake packets share an ID.");
                Assert.That(clientPacketId, Is.Not.EqualTo(ackPacketId), "Client and ACK handshake packets share an ID.");
            });
        }

        [Test]
        public void Test36_PacketLibrary_BidirectionalTypeResolution_MatchesExactTypes()
        {
            LiminalPacketLibrary.Initialize();

            ushort clientPktId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();
            ushort serverPktId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
            ushort ackPktId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();

            bool clientFound = LiminalPacketLibrary.TryGetType(clientPktId, out var clientType);
            bool serverFound = LiminalPacketLibrary.TryGetType(serverPktId, out var serverType);
            bool ackFound = LiminalPacketLibrary.TryGetType(ackPktId, out var ackType);

            Assert.Multiple(() =>
            {
                Assert.That(clientFound, Is.True);
                Assert.That(clientType, Is.EqualTo(typeof(ConnectionHandshakePacketClient)));

                Assert.That(serverFound, Is.True);
                Assert.That(serverType, Is.EqualTo(typeof(ConnectionHandshakePacketServer)));

                Assert.That(ackFound, Is.True);
                Assert.That(ackType, Is.EqualTo(typeof(ConnectionHandshakeClientAck)));

                // Unregistered ID boundary test
                Assert.That(LiminalPacketLibrary.TryGetType(ushort.MaxValue, out _), Is.False, "Unknown ID resolved unexpectedly.");
            });
        }

        [Test]
        public void Test37_PacketLibrary_RegistryHash_IsDeterministicAndStable()
        {
            LiminalPacketLibrary.Initialize();

            uint hashFirstAccess = LiminalPacketLibrary.RegistryHash;
            Assert.That(hashFirstAccess, Is.Not.Zero, "Registry hash should never be 0.");

            // Repeated calls must produce the identical hash
            LiminalPacketLibrary.Initialize();
            uint hashSecondAccess = LiminalPacketLibrary.RegistryHash;

            Assert.That(hashSecondAccess, Is.EqualTo(hashFirstAccess), "Registry hash mutated across calls; dynamic discovery must remain deterministic.");
        }

        #endregion

        #region Handshake Pipeline Security & Protocol Tests

        [Test]
        public void Test38_HandshakePipeline_RoguePacketRegistryHash_RejectsAndDrops()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var rogueClient = new TcpClient();
            rogueClient.Connect("127.0.0.1", _currentTestPort);
            var stream = rogueClient.GetStream();

            // Craft packet 1 with an invalid Registry Hash
            var maliciousHandshake = new ConnectionHandshakePacketClient
            {
                ClientName = "DesyncedClient",
                ClientVersion = 1,
                PacketRegistryHash = LiminalPacketLibrary.RegistryHash ^ 0xDEADBEEF // Tampered hash
            };

            byte[] body = MessagePackSerializer.Serialize(maliciousHandshake);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            // Server must drop client on hash mismatch (RST / socket shutdown)
            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                rogueClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server answered a client with a mismatched packet registry hash instead of dropping it.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test39_HandshakePipeline_VersionMismatch_RejectsAndDrops()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var outdatedClient = new TcpClient();
            outdatedClient.Connect("127.0.0.1", _currentTestPort);
            var stream = outdatedClient.GetStream();

            // Client sending version 999
            var outdatedHandshake = new ConnectionHandshakePacketClient
            {
                ClientName = "OutdatedClient",
                ClientVersion = 999,
                PacketRegistryHash = LiminalPacketLibrary.RegistryHash
            };

            byte[] body = MessagePackSerializer.Serialize(outdatedHandshake);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                outdatedClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server did not drop client on version mismatch.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test40_HandshakePipeline_OversizedPayload_TriggersSecurityViolation()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var attackerClient = new TcpClient();
            attackerClient.Connect("127.0.0.1", _currentTestPort);
            var stream = attackerClient.GetStream();

            // Handshake pipeline caps frames at maxHandshakeSize (256 bytes default)
            byte[] maliciousHeader = new byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(0, 4), 1024 * 1024); // Claiming 1 MB
            BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());

            stream.Write(maliciousHeader, 0, maliciousHeader.Length);

            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                attackerClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server should immediately drop client attempting an oversized handshake header.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test41_HandshakePipeline_WrongSequencePacketId_Dropped()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var rogueClient = new TcpClient();
            rogueClient.Connect("127.0.0.1", _currentTestPort);
            var stream = rogueClient.GetStream();

            // Client skips Step 1 and immediately transmits Step 3 (Ack)
            var ack = new ConnectionHandshakeClientAck { Ack = true, ClientID = 1 };
            byte[] body = MessagePackSerializer.Serialize(ack);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                rogueClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server accepted out-of-order handshake packet instead of disconnecting.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test42_MulticastSend_SinglePayloadFannedOutToAllTargets()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            var c3 = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() =>
                c1.Transport.IsConnected && c2.Transport.IsConnected && c3.Transport.IsConnected, 3000), Is.True);

            ushort id1 = c1.localID;
            ushort id2 = c2.localID;
            ushort id3 = c3.localID;

            Assert.That(id1, Is.Not.EqualTo(0));
            Assert.That(id2, Is.Not.EqualTo(0));
            Assert.That(id3, Is.Not.EqualTo(0));

            int c1ReceivedCount = 0;
            int c2ReceivedCount = 0;
            int c3ReceivedCount = 0;

            string c1Message = null;
            string c2Message = null;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c1ReceivedCount);
                c1Message = pkt.Message;
            }, this);

            c2.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c2ReceivedCount);
                c2Message = pkt.Message;
            }, this);

            c3.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c3ReceivedCount);
            }, this);

            Span<ushort> targets = stackalloc ushort[] { id1, id2 };
            _serverManager.Interpreter.SendCommand(targets, new ChatPacket { Message = "Multicast_Payload" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1ReceivedCount == 1, 2000), Is.True, "Client 1 missed multicast packet.");
            Assert.That(SpinWait.SpinUntil(() => c2ReceivedCount == 1, 2000), Is.True, "Client 2 missed multicast packet.");
            Assert.That(c1Message, Is.EqualTo("Multicast_Payload"));
            Assert.That(c2Message, Is.EqualTo("Multicast_Payload"));

            Thread.Sleep(200);
            Assert.That(c3ReceivedCount, Is.EqualTo(0), "Client 3 received a packet it was not targeted for.");
        }
        #endregion

        [Test]
        public void Test43_SessionManager_WhenShutDown_IsGarbageCollected()
        {
            WeakReference weakSessionManager = IsolateNetworkRun();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.That(weakSessionManager.IsAlive, Is.False, "FATAL: SessionManager was not garbage collected! Event memory leak detected.");
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        private WeakReference IsolateNetworkRun()
        {
            var config = new LiminalTransportConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var transport = new TcpTransport();
            var manager = new LiminalNetworkManager(transport, config);

            manager.StartClient("127.0.0.1", 7777);

            var weak = new WeakReference(manager.SessionManager);

            manager.Shutdown();

            manager.StartClient("127.0.0.1", 7777);

            LiminalNetworkManager.Instance = null;

            return weak;
        }

        #region DisconnectReasonCoordinator Scenarios & Teardown Stress Tests 

        private static bool PollUntil(Func<bool> condition, LiminalNetworkManager server, LiminalNetworkManager client, int timeoutMs = 3000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (server?.SessionManager != null)
                {
                    server.SessionManager.Poll();
                    server.SessionManager.Flush();
                }

                if (client?.SessionManager != null)
                {
                    client.SessionManager.Poll();
                    client.SessionManager.Flush();
                }

                if (condition()) return true;

                Thread.Sleep(5);
            }
            return false;
        }

        [Test]
        public void Test44_DisconnectReason_Scenario1_ClientInitiatedDisconnect_ResolvesGracefullyWithAck()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;
            string serverResolvedMessage = null;
            bool serverResolved = false;

            DisconnectReason clientResolvedReason = DisconnectReason.Unknown;
            string clientResolvedMessage = null;
            bool clientResolved = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverResolvedReason = reason;
                    serverResolvedMessage = msg;
                    serverResolved = true;
                }
            };

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientResolvedReason = reason;
                clientResolvedMessage = msg;
                clientResolved = true;
            };

            // Client enqueues disconnect notice
            client.DisconnectCoordinator.ClientDisconnectWithReason(DisconnectReason.ClientDisconnected, "PlayerQuitCleanly");

            bool resolved = PollUntil(() => serverResolved && clientResolved, _serverManager, client, 3000);
            Assert.That(resolved, Is.True, "Handshake ACK or termination timed out between client and server.");

            Assert.Multiple(() =>
            {
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ClientDisconnected));
                Assert.That(serverResolvedMessage, Is.EqualTo("PlayerQuitCleanly"));
                Assert.That(clientResolvedReason, Is.EqualTo(DisconnectReason.ClientDisconnected));
                Assert.That(clientResolvedMessage, Is.EqualTo("PlayerQuitCleanly"));
                Assert.That(client.Transport.IsConnected, Is.False);
            });
        }

        [Test]
        public void Test45_DisconnectReason_Scenario2_ServerKick_UnblocksEarlyOnAck()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason clientReceivedReason = DisconnectReason.Unknown;
            string clientReceivedMessage = null;
            bool clientResolved = false;

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientReceivedReason = reason;
                clientReceivedMessage = msg;
                clientResolved = true;
            };

            var sw = System.Diagnostics.Stopwatch.StartNew();

            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.Kicked, "RuleViolation_SpeedHack", graceSeconds: 10);

            bool kicked = PollUntil(() => clientResolved && !client.Transport.IsConnected, _serverManager, client, 3000);
            sw.Stop();

            Assert.That(kicked, Is.True, "Client was not kicked within early ACK timeframe.");
            Assert.Multiple(() =>
            {
                Assert.That(sw.ElapsedMilliseconds, Is.LessThan(4000), "Kick stalled for full grace duration instead of unblocking on ACK.");
                Assert.That(clientReceivedReason, Is.EqualTo(DisconnectReason.Kicked));
                Assert.That(clientReceivedMessage, Is.EqualTo("RuleViolation_SpeedHack"));
                Assert.That(_serverManager.Transport.IsClientConnected(clientId), Is.False);
            });
        }

        [Test]
        public void Test46_DisconnectReason_Scenario3_UnresponsiveClient_FallsBackToGraceTimeout()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            client.Interpreter.UnsubscribeAll(client.DisconnectCoordinator);

            bool serverResolved = false;
            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverResolvedReason = reason;
                    serverResolved = true;
                }
            };

            var sw = System.Diagnostics.Stopwatch.StartNew();

            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.ServerShuttingDown, "Rebooting", graceSeconds: 1);

            bool resolved = PollUntil(() => serverResolved, _serverManager, client, 4000);
            sw.Stop();

            Assert.That(resolved, Is.True, "Server failed to kick unresponsive client after timeout.");
            Assert.Multiple(() =>
            {
                Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(950), "Server did not wait for the configured grace timeout.");
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ServerShuttingDown));
                Assert.That(_serverManager.Transport.IsClientConnected(clientId), Is.False);
            });
        }

        [Test]
        public void Test47_DisconnectReason_Scenario4_PhysicalDrop_FallsBackToConnectionLost()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason serverDetectedReason = DisconnectReason.Unknown;
            bool serverFired = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverDetectedReason = reason;
                    serverFired = true;
                }
            };

            ((TcpTransport)client.Transport).Disconnect();

            bool fired = PollUntil(() => serverFired, _serverManager, client, 2000);

            Assert.That(fired, Is.True, "Server missed abrupt connection drop.");
            Assert.That(serverDetectedReason, Is.EqualTo(DisconnectReason.ConnectionLost),
                "Abrupt drop should fall back to ConnectionLost when no reason was registered.");
        }

        [Test]
        public void Test48_DisconnectReason_Scenario5_TransportDiagnostics_ResolvesLocalOnlyReason()
        {
            var serverFramer = new SecureFramingProvider();
            var badFramer = new BadMagicFramingProvider();

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
                TransportFramingProvider = badFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);

            client.StartClient("127.0.0.1", _currentTestPort);

            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;
            bool serverResolved = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                serverResolvedReason = reason;
                serverResolved = true;
            };

            DisconnectReason clientResolvedReason = DisconnectReason.Unknown;
            bool clientResolved = false;

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientResolvedReason = reason;
                clientResolved = true;
            };

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);

            // Send malformed payload from client to server to trip framer
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "ExploitPayload" });

            // Server detects bad magic cookie and terminates socket
            bool serverFired = PollUntil(() => serverResolved, _serverManager, client, 2000);
            Assert.That(serverFired, Is.True, "Server failed to resolve transport diagnostic reason.");

            // Client observes socket drop and resolves ConnectionLost
            bool clientFired = PollUntil(() => clientResolved, _serverManager, client, 2000);
            Assert.That(clientFired, Is.True, "Client did not resolve disconnect.");

            Assert.Multiple(() =>
            {
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ProtocolViolation));
                Assert.That(clientResolvedReason, Is.EqualTo(DisconnectReason.ConnectionLost));
            });
        }

        [Test]
        public void Test49_TeardownStress_CoordinatorDispose_WhileInAckWaitWindow_CleansUpNeatly()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            client.Interpreter.UnsubscribeAll(client.DisconnectCoordinator);

            // Kick with long timeout window
            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.Kicked, "GraceWindowTest", graceSeconds: 30);

            // Flush once so the server actually pushes the packet out
            _serverManager.SessionManager.Flush();
            Thread.Sleep(50);

            Assert.DoesNotThrow(() =>
            {
                _serverManager.DisconnectCoordinator.Dispose();
            }, "Disposing coordinator during an active ACK wait window threw an exception.");

            Assert.DoesNotThrow(() =>
            {
                _serverManager.Shutdown();
            });

            Assert.That(_serverManager.Transport.IsConnected, Is.False);
        }

        [Test]
        public void Test50_TeardownStress_TransportKilledConcurrently_DuringDrainDelay_CancelsCleanly()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);

            int unhandledExceptions = 0;

            client.DisconnectCoordinator.ClientDisconnectWithReason(DisconnectReason.ClientDisconnected, "QuickTear");

            client.SessionManager.Flush();
            _serverManager.SessionManager.Flush();

            var killTask = Task.Run(() =>
            {
                try
                {
                    Thread.Sleep(10); // Land in the tick drain delay
                    client.DisconnectCoordinator.Dispose();
                    client.Transport.Disconnect();
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Stress] Caught unexpected teardown leak: {ex}");
                    Interlocked.Increment(ref unhandledExceptions);
                }
            });

            Assert.That(killTask.Wait(TimeSpan.FromSeconds(2)), Is.True, "Concurrent kill task deadlocked.");
            Assert.That(unhandledExceptions, Is.EqualTo(0), "Architectural exception leaked during concurrent mid-drain disposal.");
            Assert.That(client.Transport.IsConnected, Is.False);
        }

        #endregion
    }
}