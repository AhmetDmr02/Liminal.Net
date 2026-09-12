using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class CoreTransportTests
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
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True, "Client transport remained marked as connected.");
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
        #region Dual-Channel & DeliveryMethod Tests

        [Test]
        public void Test17_SendUnreliable_SmallPayload_Delivered()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            bool received = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "UnreliablePing") received = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "UnreliablePing" }, DeliveryMethod.Unreliable);
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => received, 2000), Is.True, "Unreliable packet was not delivered.");
        }

        [Test]
        public void Test18_SendMixed_ReliableAndUnreliable_BothDeliveredSequentially()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            bool receivedRel = false;
            bool receivedUnrel = false;

            client.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "ReliableMsg") receivedRel = true;
                if (pkt.Message == "UnreliableMsg") receivedUnrel = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "UnreliableMsg" }, DeliveryMethod.Unreliable);
            _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "ReliableMsg" }, DeliveryMethod.Reliable);

            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => receivedRel && receivedUnrel, 2000), Is.True,
                $"Failed mixed delivery. Reliable: {receivedRel}, Unreliable: {receivedUnrel}");
        }

        [Test]
        public void Test19_Broadcaster_SendToClient_ForwardsDeliveryMethod()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            bool received = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "BroadcastUnreliable") received = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            LiminalNetworkManager.Instance = _serverManager;

            Broadcaster.SendToClient(1, new ChatPacket { Message = "BroadcastUnreliable" }, DeliveryMethod.Unreliable);
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => received, 2000), Is.True,
                "Packet sent via Broadcaster with DeliveryMethod.Unreliable failed to arrive.");
        }

        [Test]
        public void Test20_UnreliableOverflow_DropsSilently_ConnectionRemainsAlive()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            byte[] oversized = new byte[5000];
            Assert.DoesNotThrow(() =>
            {
                _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "drop.bin", Data = oversized }, DeliveryMethod.Unreliable);
            });

            _serverManager.SessionManager.Flush();

            Thread.Sleep(100);
            Assert.That(client.Transport.IsConnected, Is.True, "Client was kicked due to an oversized unreliable packet!");

            bool reliableDelivered = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) => reliableDelivered = true, this);

            _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "StillAlive" }, DeliveryMethod.Reliable);
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => reliableDelivered, 2000), Is.True,
                "Subsequent reliable packet was not delivered after unreliable drop.");
        }

        [Test]
        public void Test21_ReliablePacket_DropsQueuedUnreliable_ToAvoidHiccupRecovery()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool sawUnreliable = false;
            bool sawReliable = false;

            client.Interpreter.Subscribe<FilePacket>((pkt, id) =>
            {
                if (pkt.FileName == "unreliable.bin") sawUnreliable = true;
                if (pkt.FileName == "reliable.bin") sawReliable = true;
            }, this);

            byte[] unrelPayload = new byte[2500];
            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "unreliable.bin", Data = unrelPayload }, DeliveryMethod.Unreliable);

            byte[] relPayload = new byte[2000];
            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "reliable.bin", Data = relPayload }, DeliveryMethod.Reliable);

            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => sawReliable, 2000), Is.True, "Reliable packet was not delivered.");
            Assert.That(sawUnreliable, Is.False, "Queued unreliable packet should have been dropped to prevent Hiccup recovery.");
        }

        [Test]
        public void Test22_UnderCapacity_BothReliableAndUnreliable_NeitherDropped()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool sawUnreliable = false;
            bool sawReliable = false;

            client.Interpreter.Subscribe<FilePacket>((pkt, id) =>
            {
                if (pkt.FileName == "unrel.bin") sawUnreliable = true;
                if (pkt.FileName == "rel.bin") sawReliable = true;
            }, this);

            byte[] payload = new byte[1200];
            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "unrel.bin", Data = payload }, DeliveryMethod.Unreliable);
            _serverManager.Interpreter.SendCommand(1, new FilePacket { FileName = "rel.bin", Data = payload }, DeliveryMethod.Reliable);

            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => sawUnreliable && sawReliable, 2000), Is.True,
                "Both packets should have been preserved and delivered since total size was under capacity.");
        }

        [Test]
        public void Test23_Loopback_SupportsUnreliableDeliveryMethod()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            bool receivedLoopback = false;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, senderId) =>
            {
                if (pkt.Message == "UnreliableLoop" && senderId == ILiminalTransport.SERVER_ID)
                {
                    receivedLoopback = true;
                }
            }, this);

            _serverManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "UnreliableLoop" }, DeliveryMethod.Unreliable);

            Assert.That(SpinWait.SpinUntil(() => receivedLoopback, 2000), Is.True,
                "Virtual loopback failed to route an Unreliable packet to itself.");
        }

        #endregion

        #region Inbound Receive Path Tests

        [Test]
        public void Test24_Inbound_OversizedUnreliableBatch_DroppedSilently_PeerNotKicked()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool sawUnreliable = false;
            _serverManager.Interpreter.Subscribe<FilePacket>((pkt, id) =>
            {
                if (pkt.FileName == "huge_unrel.bin") sawUnreliable = true;
            }, this);

            byte[] oversizedPayload = new byte[5000];
            Random.Shared.NextBytes(oversizedPayload);

            Span<byte> framed = stackalloc byte[4 + 2 + oversizedPayload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(framed.Slice(0, 4), oversizedPayload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(framed.Slice(4, 2), (ushort)LiminalPacketLibrary.GetId<FilePacket>());
            oversizedPayload.CopyTo(framed.Slice(6));

            client.Transport.Send(framed, ILiminalTransport.SERVER_ID, TransportFlags.Unreliable);

            Thread.Sleep(150);

            Assert.That(sawUnreliable, Is.False, "Oversized inbound unreliable packet should have been dropped.");
            Assert.That(client.Transport.IsConnected, Is.True, "Client should NOT have been kicked for sending an oversized unreliable batch.");

            bool reliableReceived = false;
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "AfterDropPing") reliableReceived = true;
            }, this);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "AfterDropPing" }, DeliveryMethod.Reliable);
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => reliableReceived, 2000), Is.True, "Subsequent reliable message failed to arrive after unreliable drop.");
        }

        [Test]
        public void Test25_Inbound_PollingDrainsUnreliableBeforeReliable()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            var dispatchOrder = new List<string>();

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                lock (dispatchOrder)
                {
                    dispatchOrder.Add(pkt.Message);
                }
            }, this);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "UnreliableFirst" }, DeliveryMethod.Unreliable);
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "ReliableSecond" }, DeliveryMethod.Reliable);
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() =>
            {
                lock (dispatchOrder) return dispatchOrder.Count == 2;
            }, 2000), Is.True, "Both packets failed to arrive.");

            lock (dispatchOrder)
            {
                Assert.That(dispatchOrder[0], Is.EqualTo("UnreliableFirst"));
                Assert.That(dispatchOrder[1], Is.EqualTo("ReliableSecond"));
            }
        }

        [Test]
        public void Test26_Inbound_ReliableDropsQueuedUnreliable_ToAvoidHiccupRecovery()
        {
            _serverConfig.MaxPacketCount = 5;
            _serverConfig.Hiccup.Enabled = true;

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 5,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int serverReceivedUnreliable = 0;
            int serverReceivedReliable = 0;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message.StartsWith("UnrelQueue")) Interlocked.Increment(ref serverReceivedUnreliable);
                if (pkt.Message.StartsWith("RelPriority")) Interlocked.Increment(ref serverReceivedReliable);
            }, this);

            for (int i = 0; i < 4; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"UnrelQueue_{i}" }, DeliveryMethod.Unreliable);
            }
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "RelPriority_0" }, DeliveryMethod.Reliable);
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "RelPriority_1" }, DeliveryMethod.Reliable);

            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref serverReceivedReliable) == 2, 2000), Is.True,
                $"Expected 2 reliable packets, but got {serverReceivedReliable}");

            Assert.That(Volatile.Read(ref serverReceivedUnreliable), Is.EqualTo(0),
                "Queued unreliable packets should have been dropped to accommodate reliable traffic.");
        }

        [Test]
        public void Test27_Inbound_QueueCapExceeded_DropsIncomingUnreliableWithoutKicking()
        {
            _serverConfig.MaxPacketCount = 4;

            _serverManager?.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int receivedCount = 0;
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref receivedCount), this);

            for (int i = 0; i < 10; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"Spam_{i}" }, DeliveryMethod.Unreliable);
            }
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref receivedCount) > 0, 2000), Is.True);

            Thread.Sleep(100);

            Assert.That(client.Transport.IsConnected, Is.True, "Client was kicked when unreliable inbound queue overflowed.");

            Assert.That(Volatile.Read(ref receivedCount), Is.EqualTo(4),
                $"Inbound queue should have capped queued unreliable packets at 4, but received {receivedCount}.");
        }

        [Test]
        public void Test28_Inbound_DisconnectDrainsBothQueues_ZeroMemoryLeak()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Unrel" }, DeliveryMethod.Unreliable);
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Rel" }, DeliveryMethod.Reliable);
            client.SessionManager.Flush();

            Thread.Sleep(50);

            client.Disconnect();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.SessionManager.GetActiveSessionCount() == 0, 2000), Is.True,
                "Server failed to clean up session on disconnect.");

            Assert.DoesNotThrow(() => _serverManager.ManualPoll());
        }

        #endregion
    }
}