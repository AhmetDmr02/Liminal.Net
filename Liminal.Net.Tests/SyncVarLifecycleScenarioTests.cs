using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SyncVarLifecycleScenarioTests
    {
        private static int _portCounter = 9200;
        private int _currentPort;
        private ConcurrentBag<LiminalNetworkManager> _managersToTeardown;

        [SetUp]
        public void Setup()
        {
            _currentPort = Interlocked.Increment(ref _portCounter);
            _managersToTeardown = new();
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var manager in _managersToTeardown)
            {
                try { manager?.Shutdown(); } catch { }
            }
        }

        private LiminalNetworkManager CreateManager(int port = 0)
        {
            int p = port > 0 ? port : _currentPort;
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = p,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };
            var netManager = new LiminalNetworkManager(new TcpTransport(), config);
            _managersToTeardown.Add(netManager);
            return netManager;
        }

        #region Scenario A: Persistent NetworkManager (DontDestroyOnLoad)

        [Test]
        public void ScenarioA_PathA_PersistentManager_Reconnect_CleanSlabAndAllocations()
        {
            var hostManager = CreateManager();
            string token = $"scenA_pathA_{Guid.NewGuid():N}";
            var playerHp = new SyncVar<int>(token, 100, hostManager.SyncVarManager);

            // Session 1: Host runs
            Assert.That(hostManager.StartHost(), Is.True);
            Assert.That(playerHp.LocalSlot, Is.EqualTo(0));
            playerHp.Value = 25;
            Assert.That(playerHp.Value, Is.EqualTo(25));
            Assert.That(playerHp.Version, Is.GreaterThanOrEqualTo(1));

            // Session 1 ends: Disconnect
            hostManager.Disconnect();

            Assert.That(hostManager.SyncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(0));
            Assert.That(playerHp.LocalSlot, Is.EqualTo(ushort.MaxValue));
            Assert.That(playerHp.Version, Is.EqualTo(0));

            // Session 2: Same persistent hostManager restarts on new port
            int newPort = Interlocked.Increment(ref _portCounter);
            Assert.That(hostManager.StartHost(newPort), Is.True);

            // Access and write on clean slab
            playerHp.Value = 200;
            Assert.That(playerHp.LocalSlot, Is.EqualTo(0), "SyncVar did not allocate slot 0 on fresh session slab.");
            Assert.That(playerHp.Value, Is.EqualTo(200));
            Assert.That(playerHp.Version, Is.GreaterThanOrEqualTo(1));
            Assert.That(hostManager.SyncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(1));
        }

        [Test]
        public void ScenarioA_PathB_PersistentManager_Reconnect_ClientReceivesRealtimePush()
        {
            string token = $"scenA_pathB_{Guid.NewGuid():N}";
            var server = CreateManager();
            var client = CreateManager();

            Assert.That(server.StartServer("127.0.0.1", _currentPort), Is.True);
            var serverGold = new SyncVar<int>(token, 50, server.SyncVarManager);
            var clientGold = client.SyncVarManager.Bind<int>(token, 0);

            bool connected = false;
            client.Events.OnLocalClientConnected += _ => connected = true;
            client.StartClient("127.0.0.1", _currentPort);
            Assert.That(SpinWait.SpinUntil(() => connected, 3000), Is.True);

            Assert.That(SpinWait.SpinUntil(() => clientGold.Value == 50, 3000), Is.True);

            // Session 1 disconnects
            client.Disconnect();
            server.Disconnect();

            // Session 2 on new port
            int nextPort = Interlocked.Increment(ref _portCounter);
            Assert.That(server.StartServer("127.0.0.1", nextPort), Is.True);

            connected = false;
            client.StartClient("127.0.0.1", nextPort);
            Assert.That(SpinWait.SpinUntil(() => connected, 3000), Is.True);

            int callbackCount = 0;
            int latestClientVal = 0;
            clientGold.OnValueChanged += (_, newVal) =>
            {
                Interlocked.Increment(ref callbackCount);
                Volatile.Write(ref latestClientVal, newVal);
            };

            // Real-time server push
            serverGold.Value = 999;

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref latestClientVal) == 999, 3000), Is.True,
                "Persistent client failed to receive real-time push after reconnecting.");
            Assert.That(Volatile.Read(ref callbackCount), Is.GreaterThanOrEqualTo(1));
        }

        [Test]
        public void ScenarioA_PathC_PersistentManager_Reconnect_ServerGeneratesSnapshotForNewJoiner()
        {
            string token = $"scenA_pathC_{Guid.NewGuid():N}";
            var server = CreateManager();
            var client1 = CreateManager();

            Assert.That(server.StartServer("127.0.0.1", _currentPort), Is.True);
            var serverScore = new SyncVar<int>(token, 100, server.SyncVarManager);

            client1.StartClient("127.0.0.1", _currentPort);
            var c1Score = client1.SyncVarManager.Bind<int>(token);
            Assert.That(SpinWait.SpinUntil(() => c1Score.Value == 100, 3000), Is.True);

            // Session 1 teardown
            client1.Shutdown();
            server.Disconnect();

            // Session 2 restarts
            int nextPort = Interlocked.Increment(ref _portCounter);
            Assert.That(server.StartServer("127.0.0.1", nextPort), Is.True);
            serverScore.Value = 777;

            // Brand new client joins session 2
            var client2 = CreateManager(nextPort);
            client2.StartClient("127.0.0.1", nextPort);
            var c2Score = client2.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => c2Score.Value == 777, 3000), Is.True,
                "New joining client did not receive initial snapshot from persistent server after reconnect.");
        }

        [Test]
        public void ScenarioA_MemoryVerification_DisconnectAndShutdown_WipesSlabAndVersions()
        {
            var server = CreateManager();
            Assert.That(server.StartServer("127.0.0.1", _currentPort), Is.True);

            var v1 = new SyncVar<int>($"mem_v1_{Guid.NewGuid():N}", 11, server.SyncVarManager);
            var v2 = new SyncVar<int>($"mem_v2_{Guid.NewGuid():N}", 22, server.SyncVarManager);
            var v3 = new SyncVar<int>($"mem_v3_{Guid.NewGuid():N}", 33, server.SyncVarManager);

            v1.Value = 111;
            v2.Value = 222;
            v3.Value = 333;

            var syncVarManager = server.SyncVarManager;
            var slab = syncVarManager.Slab;

            Assert.That(syncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(3));
            Assert.That(slab.AllocatedBytes, Is.GreaterThan(0));
            Assert.That(slab.GetPage(0).Any(b => b != 0), Is.True);

            // Verify Disconnect wipes slab
            server.Disconnect();

            Assert.That(slab.AllocatedBytes, Is.EqualTo(0), "Slab AllocatedBytes was not reset on Disconnect.");
            Assert.That(syncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(0), "SlotBitset was not cleared on Disconnect.");
            Assert.That(syncVarManager.DirtyQueueCount, Is.EqualTo(0));
            Assert.That(slab.GetPage(0).All(b => b == 0), Is.True, "Slab page 0 memory was not zeroed on Disconnect.");
            for (int p = 1; p < slab.MaxPages; p++)
            {
                Assert.That(slab.GetPage(p), Is.Null, $"Slab page {p} was not nulled on Disconnect.");
            }
            Assert.That(v1.LocalSlot, Is.EqualTo(ushort.MaxValue));
            Assert.That(v2.LocalSlot, Is.EqualTo(ushort.MaxValue));
            Assert.That(v3.LocalSlot, Is.EqualTo(ushort.MaxValue));
            Assert.That(v1.Version, Is.EqualTo(0));
            Assert.That(v2.Version, Is.EqualTo(0));
            Assert.That(v3.Version, Is.EqualTo(0));

            // Verify Shutdown wipes slab as well
            int nextPort = Interlocked.Increment(ref _portCounter);
            Assert.That(server.StartServer("127.0.0.1", nextPort), Is.True);

            v1.Value = 999;
            Assert.That(slab.AllocatedBytes, Is.GreaterThan(0));

            server.Shutdown();

            Assert.That(slab.AllocatedBytes, Is.EqualTo(0), "Slab AllocatedBytes was not reset on Shutdown.");
            Assert.That(syncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(0), "SlotBitset was not cleared on Shutdown.");
            Assert.That(slab.GetPage(0).All(b => b == 0), Is.True, "Slab page 0 memory was not zeroed on Shutdown.");
            Assert.That(v1.Version, Is.EqualTo(0));
        }

        #endregion

        #region Scenario B: Totally New NetworkManager Created Per Session

        [Test]
        public void ScenarioB_PathA_NewManager_PreExistingSyncVar_ValueAccess_AutoClaimsNewManager()
        {
            string token = $"scenB_pathA_{Guid.NewGuid():N}";
            var preExistingHealth = new SyncVar<int>(token, 100);

            // Manager 1 session
            var manager1 = CreateManager();
            Assert.That(manager1.StartServer("127.0.0.1", _currentPort), Is.True);
            preExistingHealth.Value = 80;
            Assert.That(preExistingHealth.Value, Is.EqualTo(80));

            // Manager 1 discarded completely
            manager1.Shutdown();

            // Manager 2 created fresh
            int port2 = Interlocked.Increment(ref _portCounter);
            var manager2 = CreateManager(port2);
            Assert.That(manager2.StartServer("127.0.0.1", port2), Is.True);

            // Access pre-existing variable on new manager
            preExistingHealth.Value = 350;

            Assert.That(preExistingHealth.LocalSlot, Is.EqualTo(0), "Pre-existing SyncVar failed to claim slot 0 in Manager 2.");
            Assert.That(preExistingHealth.Value, Is.EqualTo(350));
            Assert.That(manager2.SyncVarManager.SlotBitset.CountAllocated(), Is.EqualTo(1));

            // Client connects to Manager 2
            var client = CreateManager(port2);
            client.StartClient("127.0.0.1", port2);
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 350, 3000), Is.True,
                "Pre-existing SyncVar auto-claimed into Manager 2 failed to propagate to connecting client.");
        }

        [Test]
        public void ScenarioB_PathB_Lazy_NewManager_ServerPushBuffersIntoUnboundCache_DrainsOnValueAccess()
        {
            string token = $"scenB_lazy_{Guid.NewGuid():N}";
            var preExistingMana = new SyncVar<int>(token, 10);

            int sPort = Interlocked.Increment(ref _portCounter);
            var server = CreateManager(sPort);
            Assert.That(server.StartServer("127.0.0.1", sPort), Is.True);

            // Server owns mana and writes 500
            var serverMana = new SyncVar<int>(token, 10, server.SyncVarManager);
            serverMana.Value = 500;

            // Client manager created fresh
            var client = CreateManager(sPort);
            bool connected = false;
            client.Events.OnLocalClientConnected += _ => connected = true;
            client.StartClient("127.0.0.1", sPort);
            Assert.That(SpinWait.SpinUntil(() => connected, 3000), Is.True);

            // Wait for push to buffer into UnboundCache without reading preExistingMana.Value yet
            Assert.That(SpinWait.SpinUntil(() => client.SyncVarManager.UnboundCacheCount > 0, 3000), Is.True,
                "Server update was not buffered into client UnboundCache.");

            // Touch .Value to drain cache
            int drainedValue = preExistingMana.Value;

            Assert.That(drainedValue, Is.EqualTo(500), "Pre-existing SyncVar did not drain buffered server value on .Value access.");
            Assert.That(client.SyncVarManager.UnboundCacheCount, Is.EqualTo(0), "UnboundCache was not cleared after drain.");
            Assert.That(preExistingMana.LocalSlot, Is.EqualTo(0));
        }

        [Test]
        public void ScenarioB_PathB_Realtime_NewManager_BindReceivesServerPushInRealtime()
        {
            string token = $"scenB_realtime_{Guid.NewGuid():N}";

            int sPort = Interlocked.Increment(ref _portCounter);
            var server = CreateManager(sPort);
            Assert.That(server.StartServer("127.0.0.1", sPort), Is.True);

            var serverStamina = new SyncVar<int>(token, 100, server.SyncVarManager);

            // Client uses Bind<T> on new manager
            var client = CreateManager(sPort);
            var clientStamina = client.SyncVarManager.Bind<int>(token);

            int callbackCount = 0;
            int receivedVal = 0;
            clientStamina.OnValueChanged += (_, newVal) =>
            {
                Interlocked.Increment(ref callbackCount);
                Volatile.Write(ref receivedVal, newVal);
            };

            bool connected = false;
            client.Events.OnLocalClientConnected += _ => connected = true;
            client.StartClient("127.0.0.1", sPort);
            Assert.That(SpinWait.SpinUntil(() => connected, 3000), Is.True);

            // Real-time server push
            serverStamina.Value = 85;

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref receivedVal) == 85, 3000), Is.True,
                "Client with Bind<T> did not observe real-time value push.");
            Assert.That(Volatile.Read(ref callbackCount), Is.GreaterThanOrEqualTo(1));
        }

        [Test]
        public void ScenarioB_PathC_NewManager_DedicatedServerWithBind_SendsInitialSnapshotToConnectingClient()
        {
            string token = $"scenB_pathC_{Guid.NewGuid():N}";

            int sPort = Interlocked.Increment(ref _portCounter);
            var server = CreateManager(sPort);
            Assert.That(server.StartServer("127.0.0.1", sPort), Is.True);

            // Server registers variable using Bind<T>
            var serverScore = server.SyncVarManager.Bind<int>(token, 420);

            // Client connects
            var client = CreateManager(sPort);
            var clientScore = client.SyncVarManager.Bind<int>(token);

            client.StartClient("127.0.0.1", sPort);

            Assert.That(SpinWait.SpinUntil(() => clientScore.Value == 420, 3000), Is.True,
                "Client failed to receive initial snapshot registered via Bind<T> on new server.");
        }

        [Test]
        public void ScenarioB_SharedState_MultipleBindCallers_ReceiveIdenticalReference()
        {
            var manager = CreateManager();
            string token = $"shared_{Guid.NewGuid():N}";

            // Multiple scripts watch same token
            var ref1 = manager.SyncVarManager.Bind<int>(token, 50);
            var ref2 = manager.SyncVarManager.Bind<int>(token, 50);

            Assert.That(ReferenceEquals(ref1, ref2), Is.True, "Multiple Bind<T> calls for same token returned distinct instances.");

            // Mutating ref1 is immediately visible on ref2
            ref1.Value = 999;
            Assert.That(ref2.Value, Is.EqualTo(999));
        }

        #endregion
    }
}
