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
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class BroadcasterIntegrationTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalNetworkConfig _serverConfig;

        private static int _portCounter = 8880;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new ConcurrentBag<LiminalNetworkManager>();

            _serverConfig = new LiminalNetworkConfig
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

        private LiminalNetworkManager CreateAndStartClient(bool waitForConnect = true)
        {
            var config = new LiminalNetworkConfig
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

            bool connected = false;
            if (waitForConnect)
            {
                client.Events.OnLocalClientConnected += _ => connected = true;
            }

            client.StartClient("127.0.0.1", _currentTestPort);

            if (waitForConnect)
            {
                Assert.That(SpinWait.SpinUntil(() => connected, 2000), Is.True, "Client failed to connect via OnLocalClientConnected.");
            }

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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);

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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient1 = CreateAndStartClient();
            var remoteClient2 = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() =>
                _serverManager.Transport.IsClientConnected(remoteClient1.localID) &&
                _serverManager.Transport.IsClientConnected(remoteClient2.localID), 3000), Is.True);

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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();

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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);
            ushort hostClientId = _serverManager.localID;

            var remoteClient = CreateAndStartClient();

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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();

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

        [Test]
        public void Test18_Broadcaster_DuplicateSubscription_SuppressedForSameSubscriber()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();

            int invocationCount = 0;
            object subscriber = new object();
            Action<ChatPacket, ushort> onChatReceived = (pkt, s) => Interlocked.Increment(ref invocationCount);

            LiminalNetworkManager.Instance = client;

            Broadcaster.Subscribe(onChatReceived, subscriber);
            Broadcaster.Subscribe(onChatReceived, subscriber);
            Broadcaster.Subscribe(onChatReceived, subscriber);

            _serverManager.Interpreter.SendCommand(client.localID, new ChatPacket { Message = "DeduplicationCheck" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => invocationCount == 1, 2000), Is.True);

            Thread.Sleep(150);

            Assert.That(invocationCount, Is.EqualTo(1),
                "Duplicate subscriptions for the same subscriber instance fired multiple times.");
        }

        [Test]
        public void Test19_Broadcaster_SeparateSubscribers_NotSuppressedAcrossDistinctInstances()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateAndStartClient();

            int subscriber1Invocations = 0;
            int subscriber2Invocations = 0;

            var sub1 = new object();
            var sub2 = new object();

            Action<ChatPacket, ushort> callback1 = (pkt, s) => Interlocked.Increment(ref subscriber1Invocations);
            Action<ChatPacket, ushort> callback2 = (pkt, s) => Interlocked.Increment(ref subscriber2Invocations);

            LiminalNetworkManager.Instance = client;

            Broadcaster.Subscribe(callback1, sub1);
            Broadcaster.Subscribe(callback2, sub2);

            Broadcaster.Subscribe(callback1, sub1);

            _serverManager.Interpreter.SendCommand(client.localID, new ChatPacket { Message = "DistinctSubscriberCheck" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => subscriber1Invocations == 1 && subscriber2Invocations == 1, 2000), Is.True);

            Thread.Sleep(150);

            Assert.Multiple(() =>
            {
                Assert.That(subscriber1Invocations, Is.EqualTo(1), "Subscriber 1 received duplicate invocations.");
                Assert.That(subscriber2Invocations, Is.EqualTo(1), "Subscriber 2 was incorrectly suppressed by Subscriber 1.");
            });
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
            bool hostLocalConnected = false;
            _serverManager.Events.OnLocalClientConnected += _ => hostLocalConnected = true;
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => hostLocalConnected && _serverManager.localID != 0, 2000), Is.True);

            var remoteClient = CreateAndStartClient();

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

        #region Targeted Span Multicast Tests

        [Test]
        public void Test20_Send_TargetedSpan_DeliveredOnlyToSpecifiedClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            var c3 = CreateAndStartClient();

            int c1Count = 0, c2Count = 0, c3Count = 0;
            string c1Msg = null, c3Msg = null;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, s) => { Interlocked.Increment(ref c1Count); c1Msg = pkt.Message; }, this);
            c2.Interpreter.Subscribe<ChatPacket>((pkt, s) => { Interlocked.Increment(ref c2Count); }, this);
            c3.Interpreter.Subscribe<ChatPacket>((pkt, s) => { Interlocked.Increment(ref c3Count); c3Msg = pkt.Message; }, this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID, c3.localID };
            Broadcaster.Send(targetSpan, new ChatPacket { Message = "Only1And3" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1 && c3Count == 1, 2000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(c1Msg, Is.EqualTo("Only1And3"));
                Assert.That(c3Msg, Is.EqualTo("Only1And3"));
            });

            Thread.Sleep(100);
            Assert.That(c2Count, Is.EqualTo(0), "Untargeted client 2 received the packet.");
        }

        [Test]
        public void Test21_Send_TargetedSpan_DeduplicatesSameClient()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();

            int c1Count = 0;
            c1.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c1Count), this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID, c1.localID, c1.localID };
            Broadcaster.Send(targetSpan, new ChatPacket { Message = "DedupTest" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
            Thread.Sleep(100);
            Assert.That(c1Count, Is.EqualTo(1), "Duplicate IDs in target span caused duplicate packet delivery.");
        }

        [Test]
        public void Test22_Send_TargetedSpan_PartiallyInvalidIds_DeliversToValidAndDropsInvalid()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();

            int c1Count = 0;
            c1.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c1Count), this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID, 9998, 9999 };
            Broadcaster.Send(targetSpan, new ChatPacket { Message = "PartialValid" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
        }

        [Test]
        public void Test23_Send_TargetedSpan_InvokedByStandaloneClient_LogsAndDrops()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client1 = CreateAndStartClient();
            var client2 = CreateAndStartClient();

            int client2Count = 0;
            client2.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref client2Count), this);

            LiminalNetworkManager.Instance = client1;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { client2.localID };
            Broadcaster.Send(targetSpan, new ChatPacket { Message = "ClientSpanSend" });
            client1.SessionManager.Flush();

            Thread.Sleep(150);
            Assert.That(client2Count, Is.EqualTo(0), "Standalone client was able to invoke Send with target client IDs.");
        }

        [Test]
        public unsafe void Test24_Send_TargetedSpan_SerializedOnceDispatchedToMany()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            var c3 = CreateAndStartClient();

            var payloadPointers = new List<IntPtr>();
            var lockObj = new object();

            _serverManager.Interpreter.OnSendRequest += (sender, target, pid, payload, method) =>
            {
                fixed (byte* ptr = payload)
                {
                    lock (lockObj)
                    {
                        payloadPointers.Add((IntPtr)ptr);
                    }
                }
            };

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID, c2.localID, c3.localID };
            Broadcaster.Send(targetSpan, new ChatPacket { Message = "SerializedOnceCheck" });
            _serverManager.SessionManager.Flush();

            Assert.That(payloadPointers.Count, Is.EqualTo(3), "Expected 3 OnSendRequest invocations.");
            Assert.Multiple(() =>
            {
                Assert.That(payloadPointers[0], Is.EqualTo(payloadPointers[1]), "Payload buffer pointer differed between target 1 and target 2.");
                Assert.That(payloadPointers[1], Is.EqualTo(payloadPointers[2]), "Payload buffer pointer differed between target 2 and target 3.");
            });
        }

        [Test]
        public void Test25_SendBitStream_TargetedSpan_DeliveredOnlyToSpecifiedClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();

            int c1Count = 0, c2Count = 0;
            uint c1ReceivedTick = 0;

            c1.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c1Count);
                c1ReceivedTick = meta.Tick;
            }, this);

            c2.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c2Count);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            byte[] bitstream = new byte[] { 0xAA, 0xBB };
            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID };
            Broadcaster.SendBitStream(targetSpan, new TestSnapshotMeta { Tick = 777, Count = 2 }, bitstream.AsSpan());
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
            Assert.That(c1ReceivedTick, Is.EqualTo(777));

            Thread.Sleep(100);
            Assert.That(c2Count, Is.EqualTo(0), "Client 2 received untargeted bitstream.");
        }

        [Test]
        public void Test26_SendBitStream_BitStreamAction_TargetedSpan_DeliveredOnlyToSpecifiedClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();

            int c1Count = 0, c2Count = 0;
            uint c1Value = 0;

            c1.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c1Count);
                c1Value = reader.ReadUInt(16);
            }, this);

            c2.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c2Count);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID };
            Broadcaster.SendBitStream(targetSpan, new TestSnapshotMeta { Tick = 1, Count = 1 }, (ref BitWriter writer) =>
            {
                writer.WriteUInt(12345, 16);
            });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
            Assert.That(c1Value, Is.EqualTo(12345));

            Thread.Sleep(100);
            Assert.That(c2Count, Is.EqualTo(0));
        }

        [Test]
        public void Test27_SendBitStream_StatefulAction_TargetedSpan_DeliveredOnlyToSpecifiedClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();

            int c1Count = 0;
            uint c1Value = 0;

            c1.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c1Count);
                c1Value = reader.ReadUInt(16);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID };
            int customState = 4321;
            Broadcaster.SendBitStream(targetSpan, new TestSnapshotMeta { Tick = 2, Count = 1 }, customState, (ref BitWriter writer, in int state) =>
            {
                writer.WriteUInt((uint)state, 16);
            });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
            Assert.That(c1Value, Is.EqualTo(4321));
        }

        [Test]
        public void Test28_BeginBitStream_TargetedSpan_DeliveredOnlyToSpecifiedClients()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();

            int c1Count = 0, c2Count = 0;
            uint c1Value = 0;

            c1.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c1Count);
                c1Value = reader.ReadUInt(16);
            }, this);

            c2.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref c2Count);
            }, this);

            LiminalNetworkManager.Instance = _serverManager;

            ReadOnlySpan<ushort> targetSpan = stackalloc ushort[] { c1.localID };
            var scope = Broadcaster.BeginBitStream(targetSpan, new TestSnapshotMeta { Tick = 3, Count = 1 });
            try
            {
                scope.Writer.WriteUInt(9876, 16);
            }
            finally
            {
                scope.Dispose();
            }
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1Count == 1, 2000), Is.True);
            Assert.That(c1Value, Is.EqualTo(9876));

            Thread.Sleep(100);
            Assert.That(c2Count, Is.EqualTo(0));
        }

        [Test]
        public void Test29_Send_TargetedSpan_ConcurrentSends_ZeroRaceConditions()
        {
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();

            int c1ReceivedCount = 0;
            int c2ReceivedCount = 0;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c1ReceivedCount), this);
            c2.Interpreter.Subscribe<ChatPacket>((pkt, s) => Interlocked.Increment(ref c2ReceivedCount), this);

            LiminalNetworkManager.Instance = _serverManager;

            const int messagesPerThread = 5;
            ushort c1Id = c1.localID;
            ushort c2Id = c2.localID;

            var t1 = Task.Run(() =>
            {
                ReadOnlySpan<ushort> targets = stackalloc ushort[] { c1Id, c2Id };
                for (int i = 0; i < messagesPerThread; i++)
                {
                    Broadcaster.Send(targets, new ChatPacket { Message = $"T1_{i}" });
                }
            });

            var t2 = Task.Run(() =>
            {
                ReadOnlySpan<ushort> targets = stackalloc ushort[] { c1Id, c2Id };
                for (int i = 0; i < messagesPerThread; i++)
                {
                    Broadcaster.Send(targets, new ChatPacket { Message = $"T2_{i}" });
                }
            });

            Task.WaitAll(t1, t2);

            _serverManager.SessionManager.Flush();

            const int expectedTotal = messagesPerThread * 2;
            Assert.That(SpinWait.SpinUntil(() => c1ReceivedCount == expectedTotal && c2ReceivedCount == expectedTotal, 5000), Is.True,
                $"Expected {expectedTotal} messages on each client. C1={c1ReceivedCount}, C2={c2ReceivedCount}");
        }

        #endregion
    }
}