using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Registry;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class ClientRegistryAndLifecycleTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalNetworkConfig _serverConfig;

        private static int _portCounter = 8800;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new();

            _serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 10,
                HandshakeTimeout = 10
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                try { client?.Shutdown(); } catch { }
            }
            try { _serverManager?.Shutdown(); } catch { }
        }

        private LiminalNetworkManager CreateClientManager()
        {
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 10,
                HandshakeTimeout = 10
            };
            var client = new LiminalNetworkManager(new TcpTransport(), config);
            _clientManagers.Add(client);
            return client;
        }

        #region Lifecycle & Spam Prevention Tests

        [Test]
        public void StartServer_FirstCallReturnsTrue_SpamReturnsFalse()
        {
            bool first = _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(first, Is.True, "First StartServer call should return true");

            bool spamServer = _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(spamServer, Is.False, "Immediate second StartServer call should return false");

            bool startClient = _serverManager.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(startClient, Is.False, "StartClient while Server is active should return false");

            bool startHost = _serverManager.StartHost();
            Assert.That(startHost, Is.False, "StartHost while Server is active should return false");
        }

        [Test]
        public void StartHost_FirstCallReturnsTrue_SpamReturnsFalse()
        {
            bool first = _serverManager.StartHost();
            Assert.That(first, Is.True, "First StartHost call should return true");

            bool spamHost = _serverManager.StartHost();
            Assert.That(spamHost, Is.False, "Immediate second StartHost call should return false");

            bool startServer = _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(startServer, Is.False, "StartServer while Host is active should return false");

            bool startClient = _serverManager.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(startClient, Is.False, "StartClient while Host is active should return false");
        }

        [Test]
        public void StartClient_FirstCallReturnsTrue_SpamWhileConnectingReturnsFalse()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateClientManager();

            bool first = client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(first, Is.True, "First StartClient call should return true");

            bool spamClient = client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(spamClient, Is.False, "Second StartClient call while Connecting should return false");

            bool startServer = client.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(startServer, Is.False, "StartServer while Client is connecting should return false");

            bool startHost = client.StartHost();
            Assert.That(startHost, Is.False, "StartHost while Client is connecting should return false");
        }

        [Test]
        public void Client_LifecycleState_TransitionsThroughCorrectPhases()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateClientManager();
            Assert.That(client.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopped));
            Assert.That(client.IsConnected, Is.False);
            Assert.That(client.CanSend, Is.False);
            Assert.That(client.IsConnecting, Is.False);

            bool connectedAndReadyFired = false;
            client.OnConnectedAndReady += () => connectedAndReadyFired = true;

            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(client.IsConnecting, Is.True);
            Assert.That(client.Role, Is.EqualTo(NetworkRole.Client));

            Assert.That(SpinWait.SpinUntil(() => client.LifecycleState == NetworkLifecycleState.Connected, 3000), Is.True);
            Assert.That(client.IsConnected, Is.True);
            Assert.That(client.CanSend, Is.True);
            Assert.That(client.IsConnecting, Is.False);
            Assert.That(connectedAndReadyFired, Is.True, "OnConnectedAndReady should fire upon connection");

            client.Disconnect();
            Assert.That(client.LifecycleState, Is.EqualTo(NetworkLifecycleState.Stopped));
            Assert.That(client.Role, Is.EqualTo(NetworkRole.None));
            Assert.That(client.IsConnected, Is.False);
            Assert.That(client.CanSend, Is.False);
        }

        [Test]
        public void FailedConnection_RevertsStateToStopped_AndAllowsRetry()
        {
            int deadPort = Interlocked.Increment(ref _portCounter);
            var client = CreateClientManager();

            bool started = client.StartClient("127.0.0.1", deadPort);
            Assert.That(started, Is.True);
            Assert.That(client.LifecycleState, Is.EqualTo(NetworkLifecycleState.Connecting));

            Assert.That(SpinWait.SpinUntil(() => client.LifecycleState == NetworkLifecycleState.Stopped, 5000), Is.True);
            Assert.That(client.Role, Is.EqualTo(NetworkRole.None));
            Assert.That(client.IsConnected, Is.False);
            Assert.That(client.CanSend, Is.False);

            bool retry = client.StartClient("127.0.0.1", deadPort);
            Assert.That(retry, Is.True, "Retry should be accepted after reverting to Stopped");
        }

        #endregion

        #region ClientRegistry Tests

        [Test]
        public void DedicatedServer_RegistryState_Correct()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(_serverManager.ClientRegistry, Is.Not.Null);
            Assert.That(_serverManager.ClientRegistry.Count, Is.EqualTo(0));
            Assert.That(_serverManager.ClientRegistry.IsDedicatedServer, Is.True);
            Assert.That(_serverManager.ClientRegistry.HostClientId, Is.Null);
            Assert.That(_serverManager.ClientRegistry.HostClient, Is.Null);
            Assert.That(_serverManager.ClientRegistry.LocalClient, Is.Null);
        }

        [Test]
        public void HostMode_RegistryState_IdentifiesHostClient()
        {
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.LifecycleState == NetworkLifecycleState.HostActive, 3000), Is.True);

            var registry = _serverManager.ClientRegistry;
            Assert.That(registry, Is.Not.Null);
            Assert.That(registry.Count, Is.EqualTo(1));
            Assert.That(registry.IsDedicatedServer, Is.False);
            Assert.That(registry.HostClientId, Is.Not.Null);

            var hostClient = registry.HostClient;
            Assert.That(hostClient, Is.Not.Null);
            Assert.That(hostClient.IsHost, Is.True);
            Assert.That(hostClient.IsLocal, Is.True);
            Assert.That(registry.LocalClient, Is.SameAs(hostClient));
        }

        [Test]
        public void ClientConnectsToDedicatedServer_PopulatesBothRegistries()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateClientManager();
            ConnectedClient joinedOnServer = null;
            ConnectedClient joinedOnClient = null;
            ConnectedClient localReadyClient = null;

            _serverManager.ClientRegistry.OnClientJoined += c => joinedOnServer = c;
            client.ClientRegistry.OnClientJoined += c => joinedOnClient = c;
            client.ClientRegistry.OnLocalClientReady += c => localReadyClient = c;

            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.ClientRegistry.Count == 1, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client.ClientRegistry.Count == 1, 3000), Is.True);

            Assert.That(joinedOnServer, Is.Not.Null);
            Assert.That(joinedOnServer.IsLocal, Is.False);
            Assert.That(joinedOnServer.IsHost, Is.False);

            Assert.That(joinedOnClient, Is.Not.Null);
            Assert.That(joinedOnClient.IsLocal, Is.True);
            Assert.That(joinedOnClient.IsHost, Is.False);
            Assert.That(joinedOnClient.ClientId, Is.EqualTo(client.localID));
            Assert.That(localReadyClient, Is.Not.Null);
            Assert.That(localReadyClient.ClientId, Is.EqualTo(client.localID));

            Assert.That(client.ClientRegistry.IsDedicatedServer, Is.True);
            Assert.That(client.ClientRegistry.HostClientId, Is.Null);
        }

        [Test]
        public void MultipleClients_JoinAndLeave_SynchronizedAcrossAllPeers()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client1 = CreateClientManager();
            var client2 = CreateClientManager();

            client1.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client1.IsConnected, 3000), Is.True);

            List<ConnectedClient> c1JoinedList = new();
            List<ConnectedClient> c1LeftList = new();
            client1.ClientRegistry.OnClientJoined += c => c1JoinedList.Add(c);
            client1.ClientRegistry.OnClientLeft += c => c1LeftList.Add(c);

            client2.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client2.IsConnected, 3000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => _serverManager.ClientRegistry.Count == 2, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client1.ClientRegistry.Count == 2, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client2.ClientRegistry.Count == 2, 3000), Is.True);

            Assert.That(c1JoinedList.Any(c => c.ClientId == client2.localID), Is.True);

            ushort c2Id = client2.localID;
            client2.Disconnect();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.ClientRegistry.Count == 1, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client1.ClientRegistry.Count == 1, 3000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => c1LeftList.Any(c => c.ClientId == c2Id), 3000), Is.True);
            Assert.That(client1.ClientRegistry.ContainsClient(c2Id), Is.False);
        }

        [Test]
        public void LateJoiner_ReceivesFullRosterSnapshot()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client1 = CreateClientManager();
            var client2 = CreateClientManager();

            client1.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client1.IsConnected, 3000), Is.True);

            client2.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client2.IsConnected, 3000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => _serverManager.ClientRegistry.Count == 2, 3000), Is.True);

            var client3 = CreateClientManager();
            client3.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client3.IsConnected, 3000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => client3.ClientRegistry.Count == 3, 3000), Is.True);
            Assert.That(client3.ClientRegistry.ContainsClient(client1.localID), Is.True);
            Assert.That(client3.ClientRegistry.ContainsClient(client2.localID), Is.True);
            Assert.That(client3.ClientRegistry.ContainsClient(client3.localID), Is.True);
        }

        [Test]
        public void RemoteClient_ConnectingToHost_CorrectlyIdentifiesHostPeer()
        {
            _serverManager.StartHost();
            Assert.That(SpinWait.SpinUntil(() => _serverManager.LifecycleState == NetworkLifecycleState.HostActive, 3000), Is.True);

            ushort hostClientId = _serverManager.localID;

            var remoteClient = CreateClientManager();
            remoteClient.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => remoteClient.IsConnected, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => remoteClient.ClientRegistry.Count == 2, 3000), Is.True);

            Assert.That(remoteClient.ClientRegistry.IsDedicatedServer, Is.False);
            Assert.That(remoteClient.ClientRegistry.HostClientId, Is.EqualTo(hostClientId));

            var hostClientOnRemote = remoteClient.ClientRegistry.HostClient;
            Assert.That(hostClientOnRemote, Is.Not.Null);
            Assert.That(hostClientOnRemote.ClientId, Is.EqualTo(hostClientId));
            Assert.That(hostClientOnRemote.IsHost, Is.True);
            Assert.That(hostClientOnRemote.IsLocal, Is.False);

            var remoteSelf = remoteClient.ClientRegistry.LocalClient;
            Assert.That(remoteSelf, Is.Not.Null);
            Assert.That(remoteSelf.IsLocal, Is.True);
            Assert.That(remoteSelf.IsHost, Is.False);
        }

        [Test]
        public void ClientReconciliation_OnRequest_RestoresSynchronizedRoster()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateClientManager();
            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 3000), Is.True);

            // Request reconciliation manually
            client.ClientRegistry.RequestRosterReconciliation();

            // Roster remains synchronized and version remains consistent
            Assert.That(SpinWait.SpinUntil(() => client.ClientRegistry.Count == 1, 3000), Is.True);
            Assert.That(client.ClientRegistry.ContainsClient(client.localID), Is.True);
        }

        [Test]
        public void Disconnect_ClearsClientRegistry()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var client = CreateClientManager();
            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 3000), Is.True);
            Assert.That(client.ClientRegistry.Count, Is.GreaterThanOrEqualTo(1));

            client.Disconnect();

            Assert.That(client.ClientRegistry, Is.Not.Null);
            Assert.That(client.ClientRegistry.Count, Is.EqualTo(0), "Disconnect should clear clients in registry");
            Assert.That(client.ClientRegistry.LocalClient, Is.Null);
            Assert.That(client.IsConnected, Is.False);
        }

        #endregion
    }
}
