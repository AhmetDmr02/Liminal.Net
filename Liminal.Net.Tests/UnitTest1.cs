using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TransportIntegrationTests
    {
        private LiminalNetworkManager _serverManager;
        private List<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7770;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new List<LiminalNetworkManager>();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
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
            _clientManagers.Clear();
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
                ClientIdResolver = new BaseResolver()
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
                var c = CreateAndStartClient();
                c.Transport.OnLocalClientConnected += (id) => assignedIds.Add(id);
                clients.Add(c);
            }

            Assert.That(SpinWait.SpinUntil(() => assignedIds.Count == 3, 2000), Is.True);

            CollectionAssert.AllItemsAreUnique(assignedIds, "Resolver handed out duplicate IDs.");
        }

        [Test]
        public void Test10_RapidSpam_DoesNotCorruptBuffer()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

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
    }
}