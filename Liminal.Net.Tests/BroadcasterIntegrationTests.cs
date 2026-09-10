using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class BroadcasterIntegrationTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        private static int _portCounter = 8880;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new ConcurrentBag<LiminalNetworkManager>();

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

            LiminalPacketLibrary.Initialize();
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }

            _serverManager?.Shutdown();
            LiminalNetworkManager.Instance = null;
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

        #region Guard & Error Handling Tests

        [Test]
        public void Test01_Send_WhenManagerInstanceIsNull_LogsErrorAndDoesNotThrow()
        {
            LiminalNetworkManager.Instance = null;

            Assert.DoesNotThrow(() =>
            {
                Broadcaster.Send(SendTo.Me, new ChatPacket { Message = "Orphan" });
            });
        }

        [Test]
        public void Test02_Send_WhenNetworkNotActive_DropsSilentlyWithoutThrowing()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);

            Assert.DoesNotThrow(() =>
            {
                Broadcaster.Send(SendTo.Server, new ChatPacket { Message = "NotConnected" });
            });
        }

        [Test]
        public void Test03_Client_UsingRestrictedTargets_FailsGracefullyAndDrops()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            LiminalNetworkManager.Instance = client;

            int receivedCount = 0;
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) => Interlocked.Increment(ref receivedCount), this);

            Broadcaster.Send(SendTo.Everyone, new ChatPacket { Message = "IllegalMulticast" });
            Broadcaster.Send(SendTo.NotMe, new ChatPacket { Message = "IllegalNotMe" });
            Broadcaster.Send(SendTo.NotServer, new ChatPacket { Message = "IllegalNotServer" });
            Broadcaster.Send(SendTo.NotHost, new ChatPacket { Message = "IllegalNotHost" });

            client.SessionManager.Flush();
            Thread.Sleep(100);

            Assert.That(receivedCount, Is.EqualTo(0), "Server received unauthorized multicast packets from standalone client.");
        }

        #endregion

        #region Standalone Client Tests

        [Test]
        public void Test04_Client_SendToServer_DeliveredWithClientSenderId()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            ushort assignedClientId = client.localID;
            Assert.That(assignedClientId, Is.Not.Zero);

            ushort receivedSenderId = 0;
            string receivedMsg = null;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                receivedSenderId = sender;
                receivedMsg = pkt.Message;
            }, this);

            LiminalNetworkManager.Instance = client;
            Broadcaster.Send(SendTo.Server, new ChatPacket { Message = "HelloServer" });
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => receivedMsg != null, 2000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(receivedMsg, Is.EqualTo("HelloServer"));
                Assert.That(receivedSenderId, Is.EqualTo(assignedClientId), "Server saw incorrect sender ID for client.");
            });
        }

        [Test]
        public void Test05_Client_SendToMe_DeliveredViaLoopback()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            ushort clientLocalId = client.localID;
            bool receivedLoopback = false;
            ushort receivedSender = 0;

            LiminalNetworkManager.Instance = client;
            Broadcaster.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "ClientLoopback")
                {
                    receivedLoopback = true;
                    receivedSender = sender;
                }
            }, this);

            Broadcaster.Send(SendTo.Me, new ChatPacket { Message = "ClientLoopback" });

            Assert.That(SpinWait.SpinUntil(() => receivedLoopback, 2000), Is.True);
            Assert.That(receivedSender, Is.EqualTo(clientLocalId), "Client loopback packet had mismatched sender ID.");
        }

        #endregion

        #region Dedicated Server Multicast Tests

        [Test]
        public void Test06_DedicatedServer_SendToEveryone_DeliveredToAllClientsAsServerAuthority()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => c1.Transport.IsConnected && c2.Transport.IsConnected, 3000), Is.True);

            int c1Count = 0, c2Count = 0;
            ushort c1SeenSender = 999, c2SeenSender = 999;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c1Count);
                c1SeenSender = sender;
            }, this);

            c2.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c2Count);
                c2SeenSender = sender;
            }, this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.Everyone, new ChatPacket { Message = "ServerBroadcast" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1 && c2Count == 1, 2000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(c1SeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID), "Client 1 saw non-server sender authority.");
                Assert.That(c2SeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID), "Client 2 saw non-server sender authority.");
            });
        }

        [Test]
        public void Test07_DedicatedServer_SendToNotServer_DeliveredToAllClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => c1.Transport.IsConnected && c2.Transport.IsConnected, 3000), Is.True);

            bool serverReceived = false;
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, s) => serverReceived = true, this);

            int c1Count = 0, c2Count = 0;
            c1.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c1Count), this);
            c2.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c2Count), this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.NotServer, new ChatPacket { Message = "AllClientsOnly" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1 && c2Count == 1, 2000), Is.True);
            Thread.Sleep(100);

            Assert.That(serverReceived, Is.False, "Dedicated server received a packet explicitly targeted to NotServer.");
        }

        #endregion

        #region Host Mode Scenarios & Sender Identity Fidelity Tests

        [Test]
        public void Test08_Host_SendToMe_DeliversToLocalClient_WithLocalClientIdSender()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            ushort hostClientId = _serverManager.localID;
            bool localClientReceived = false;
            ushort seenSender = 0;

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "SelfHostMsg")
                {
                    localClientReceived = true;
                    seenSender = sender;
                }
            }, this);

            Broadcaster.Send(SendTo.Me, new ChatPacket { Message = "SelfHostMsg" });

            Assert.That(SpinWait.SpinUntil(() => localClientReceived, 2000), Is.True);
            Assert.That(seenSender, Is.EqualTo(hostClientId),
                "Host sending to .Me must dispatch as its local client ID, not SERVER_ID (0).");
        }

        [Test]
        public void Test09_Host_SendToNotHost_ReachesOnlyRemoteClients_ExcludesServerAndHostPlayer()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient1 = CreateAndStartClient();
            var remoteClient2 = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => remoteClient1.Transport.IsConnected && remoteClient2.Transport.IsConnected, 3000), Is.True);

            bool hostReceived = false;
            int r1Count = 0, r2Count = 0;
            ushort r1SeenSender = 999;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, s) => hostReceived = true, this);

            remoteClient1.Interpreter.Subscribe<ChatPacket>((pkt, s) =>
            {
                Interlocked.Increment(ref r1Count);
                r1SeenSender = s;
            }, this);

            remoteClient2.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref r2Count), this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.NotHost, new ChatPacket { Message = "RemoteOnly" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => r1Count == 1 && r2Count == 1, 2000), Is.True);
            Thread.Sleep(100);

            Assert.Multiple(() =>
            {
                Assert.That(hostReceived, Is.False, "Host received a packet dispatched with SendTo.NotHost.");
                Assert.That(r1SeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID), "NotHost broadcast must arrive with Server authority (0).");
            });
        }

        [Test]
        public void Test10_Host_SendToNotMe_DeliversToRemotesAndServerLoopback_ExcludesHostClient()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected, 2000), Is.True);

            int hostReceivedCount = 0;
            int remoteReceivedCount = 0;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, s) =>
            {
                if (pkt.Message == "ExcludeHostPlayer") Interlocked.Increment(ref hostReceivedCount);
            }, this);

            remoteClient.Interpreter.Subscribe<ChatPacket>((pkt, s) =>
            {
                if (pkt.Message == "ExcludeHostPlayer") Interlocked.Increment(ref remoteReceivedCount);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.NotMe, new ChatPacket { Message = "ExcludeHostPlayer" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => remoteReceivedCount == 1, 2000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => hostReceivedCount == 1, 2000), Is.True,
                "Server authority session on Host should still receive NotMe (it represents the server loopback, not host client player).");
        }

        [Test]
        public void Test11_Host_SendToNotServer_SendsAsLocalClientId_ReachesRemotesAndHostClient()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);
            ushort hostClientId = _serverManager.localID;

            var remoteClient = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected, 2000), Is.True);

            bool hostClientReceived = false;
            ushort hostSeenSender = 999;

            bool remoteReceived = false;
            ushort remoteSeenSender = 999;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "ClientPeersOnly")
                {
                    hostClientReceived = true;
                    hostSeenSender = sender;
                }
            }, this);

            remoteClient.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "ClientPeersOnly")
                {
                    remoteReceived = true;
                    remoteSeenSender = sender;
                }
            }, this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.NotServer, new ChatPacket { Message = "ClientPeersOnly" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => hostClientReceived && remoteReceived, 2000), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(hostSeenSender, Is.EqualTo(hostClientId),
                    "Host player client should receive its own client ID via loopback.");

                Assert.That(remoteSeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID),
                    "Remote clients must always see traffic originating from the Server authority (0).");
            });
        }

        [Test]
        public void Test12_Host_SendToEveryone_DeliversToAllSessionsWithServerAuthority()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected, 2000), Is.True);

            int hostReceiveCount = 0;
            int remoteReceiveCount = 0;
            ushort remoteSeenSender = 999;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, s) =>
            {
                if (pkt.Message == "OmniPresence") Interlocked.Increment(ref hostReceiveCount);
            }, this);

            remoteClient.Interpreter.Subscribe<ChatPacket>((pkt, s) =>
            {
                if (pkt.Message == "OmniPresence")
                {
                    Interlocked.Increment(ref remoteReceiveCount);
                    remoteSeenSender = s;
                }
            }, this);

            LiminalNetworkManager.Instance = _serverManager;
            Broadcaster.Send(SendTo.Everyone, new ChatPacket { Message = "OmniPresence" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => hostReceiveCount >= 1 && remoteReceiveCount == 1, 2000), Is.True);
            Assert.That(remoteSeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID),
                "Broadcasting to Everyone must carry authoritative SERVER_ID (0).");
        }

        #endregion

        #region Subscription Relay Tests

        [Test]
        public void Test13_Broadcaster_SubscribeAndUnsubscribe_RelaysCleanly()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int receivedCount = 0;
            object subscriberTag = new object();

            LiminalNetworkManager.Instance = client;

            Broadcaster.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref receivedCount), subscriberTag);

            _serverManager.Interpreter.SendCommand(client.localID, new ChatPacket { Message = "First" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => receivedCount == 1, 2000), Is.True);

            Broadcaster.Unsubscribe<ChatPacket>(subscriberTag);

            _serverManager.Interpreter.SendCommand(client.localID, new ChatPacket { Message = "Second" });
            _serverManager.SessionManager.Flush();

            Thread.Sleep(150);
            Assert.That(receivedCount, Is.EqualTo(1), "Callback was invoked after being unsubscribed via Broadcaster.");
        }

        #endregion
        #region SendToClient Specific Targeted Tests

        [Test]
        public void Test14_SendToClient_ServerToSpecificClient_DeliveredOnlyToTarget()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => c1.Transport.IsConnected && c2.Transport.IsConnected, 3000), Is.True);

            ushort c1Id = c1.localID;
            ushort c2Id = c2.localID;
            Assert.That(c1Id, Is.Not.Zero);
            Assert.That(c2Id, Is.Not.Zero);

            int c1ReceivedCount = 0;
            int c2ReceivedCount = 0;
            string c1Message = null;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c1ReceivedCount);
                c1Message = pkt.Message;
            }, this);

            c2.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c2ReceivedCount);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            Broadcaster.SendToClient(c1Id, new ChatPacket { Message = "TargetedForC1Only" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1ReceivedCount == 1, 2000), Is.True);
            Assert.That(c1Message, Is.EqualTo("TargetedForC1Only"));

            Thread.Sleep(100);
            Assert.That(c2ReceivedCount, Is.EqualTo(0), "Client 2 received a packet targeted strictly to Client 1.");
        }

        [Test]
        public void Test15_SendToClient_HostToRemoteClient_DeliveredWithServerAuthority()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected, 2000), Is.True);

            ushort remoteClientId = remoteClient.localID;
            Assert.That(remoteClientId, Is.Not.Zero);

            bool remoteReceived = false;
            ushort remoteSeenSender = 999;
            string remoteReceivedMsg = null;

            remoteClient.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                remoteReceived = true;
                remoteSeenSender = sender;
                remoteReceivedMsg = pkt.Message;
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            Broadcaster.SendToClient(remoteClientId, new ChatPacket { Message = "DirectFromHost" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => remoteReceived, 2000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(remoteReceivedMsg, Is.EqualTo("DirectFromHost"));
                Assert.That(remoteSeenSender, Is.EqualTo(ILiminalTransport.SERVER_ID),
                    "Remote clients must receive direct server-dispatched packets under SERVER_ID authority (0).");
            });
        }

        [Test]
        public void Test16_SendToClient_InvalidOrUnconnectedClientId_DropsSilentlyWithoutThrowing()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            LiminalNetworkManager.Instance = _serverManager;

            Assert.DoesNotThrow(() =>
            {
                Broadcaster.SendToClient(9999, new ChatPacket { Message = "DeadEnd" });
                _serverManager.SessionManager.Flush();
            });
        }

        [Test]
        public void Test17_SendToClient_InvokedByStandaloneClient_LogsErrorAndDrops()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client1 = CreateAndStartClient();
            var client2 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client1.Transport.IsConnected && client2.Transport.IsConnected, 2000), Is.True);

            int client2ReceivedCount = 0;
            client2.Interpreter.Subscribe<ChatPacket>((pkt, sender) => Interlocked.Increment(ref client2ReceivedCount), this);

            LiminalNetworkManager.Instance = client1;

            Assert.DoesNotThrow(() =>
            {
                Broadcaster.SendToClient(client2.localID, new ChatPacket { Message = "IllegalPeerSend" });
                client1.SessionManager.Flush();
            });

            Thread.Sleep(150);
            Assert.That(client2ReceivedCount, Is.EqualTo(0), "Standalone client was able to invoke SendToClient.");
        }

        #endregion
    }
}