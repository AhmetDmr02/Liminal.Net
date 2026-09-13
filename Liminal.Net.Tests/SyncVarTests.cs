using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [MessagePackObject]
    public struct TestStatePayload : IEquatable<TestStatePayload>
    {
        [Key(0)] public int Score;
        [Key(1)] public float CoordX;
        [Key(2)] public float CoordY;
        [Key(3)] public float CoordZ;

        [IgnoreMember]
        public bool IsConsistent => CoordX.Equals(CoordY) && CoordY.Equals(CoordZ);

        public bool Equals(TestStatePayload other) =>
            Score == other.Score &&
            CoordX.Equals(other.CoordX) &&
            CoordY.Equals(other.CoordY) &&
            CoordZ.Equals(other.CoordZ);
    }

    [TestFixture]
    public class SyncVarTests
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
            _clientManagers = new();

            _serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
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
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
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
        public void Test01_ServerRegistration_And_ClientLateJoin_ReceivesInitialSnapshot()
        {
            string token = $"init_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 1337);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 1337, 2000), Is.True,
                "Client failed to receive the initial snapshot blit on connection.");
        }

        [Test]
        public void Test02_ServerMutation_FlushesDelta_FiresClientCallback()
        {
            string token = $"delta_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 10);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 10, 2000), Is.True);

            int callbackOld = 0;
            int callbackNew = 0;
            bool callbackFired = false;

            clientVar.OnValueChanged += (oldVal, newVal) =>
            {
                callbackOld = oldVal;
                callbackNew = newVal;
                callbackFired = true;
            };

            serverVar.Value = 99;

            Assert.That(SpinWait.SpinUntil(() => callbackFired && clientVar.Value == 99, 2000), Is.True);
            Assert.That(callbackOld, Is.EqualTo(10));
            Assert.That(callbackNew, Is.EqualTo(99));
        }

        [Test]
        public void Test03_ClientUnauthorizedWrite_BlockedLocally()
        {
            string token = $"unauth_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 50);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 50, 2000), Is.True);
            Assert.That(clientVar.HasAuthority, Is.False);

            bool callbackFired = false;
            clientVar.OnValueChanged += (_, _) => callbackFired = true;

            clientVar.Value = 999;

            Assert.That(clientVar.Value, Is.EqualTo(50), "Local assignment succeeded without authority.");
            Assert.That(callbackFired, Is.False);

            Thread.Sleep(100);
            Assert.That(serverVar.Value, Is.EqualTo(50), "Server accepted an unauthorized client delta.");
        }

        [Test]
        public void Test04_AuthorityGranted_AllowsClientWrite_EchoesToPeers()
        {
            string token = $"auth_grant_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 100);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client1 = CreateAndStartClient();
            var client2 = CreateAndStartClient();

            var c1Var = client1.SyncVarManager.Bind<int>(token);
            var c2Var = client2.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client1.Transport.IsConnected && client2.Transport.IsConnected, 2000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => c1Var.Value == 100 && c2Var.Value == 100, 2000), Is.True);

            bool c1AuthEventFired = false;
            c1Var.OnAuthorityChanged += (hasAuth) => c1AuthEventFired = hasAuth;

            serverVar.AddAuthority(client1.localID);

            Assert.That(SpinWait.SpinUntil(() => c1AuthEventFired && c1Var.HasAuthority, 2000), Is.True);

            c1Var.Value = 777;

            Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 777, 2000), Is.True, "Server rejected authorized client write.");
            Assert.That(SpinWait.SpinUntil(() => c2Var.Value == 777, 2000), Is.True, "Server failed to echo authorized client write to peer.");
        }

        [Test]
        public void Test05_AuthorityRevocation_StopsClientWrites()
        {
            string token = $"auth_revoke_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 10);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 10, 2000), Is.True);

            serverVar.AddAuthority(client.localID);
            Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority, 2000), Is.True);

            serverVar.RemoveAuthority(client.localID);
            Assert.That(SpinWait.SpinUntil(() => !clientVar.HasAuthority, 2000), Is.True);

            clientVar.Value = 9999;

            Assert.That(clientVar.Value, Is.EqualTo(10));
            Thread.Sleep(100);
            Assert.That(serverVar.Value, Is.EqualTo(10));
        }

        [Test]
        public void Test06_DuplicateAuthority_OverwrittenGracefully()
        {
            string token = $"dup_auth_{Guid.NewGuid():N}";
            var syncVar = new SyncVar<int>(token, 0);

            syncVar.AddAuthority(5);
            syncVar.AddAuthority(5);

            Assert.That(syncVar.AuthIds.Length, Is.EqualTo(1));
            Assert.That(syncVar.AuthIds[0], Is.EqualTo(5));

            syncVar.SetAuthIds(5, 5, 6, 6, 7);
            Assert.That(syncVar.AuthIds.Length, Is.EqualTo(3));
            CollectionAssert.AreEqual(new ushort[] { 5, 6, 7 }, syncVar.AuthIds);
        }

        [Test]
        public void Test07_MultipleSyncVars_BatchPackedInSingleTick()
        {
            string t1 = $"multi_1_{Guid.NewGuid():N}";
            string t2 = $"multi_2_{Guid.NewGuid():N}";
            string t3 = $"multi_3_{Guid.NewGuid():N}";

            var s1 = new SyncVar<int>(t1, 1);
            var s2 = new SyncVar<float>(t2, 1.5f);
            var s3 = new SyncVar<string>(t3, "Init");

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            var c1 = client.SyncVarManager.Bind<int>(t1);
            var c2 = client.SyncVarManager.Bind<float>(t2);
            var c3 = client.SyncVarManager.Bind<string>(t3);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && c3.Value == "Init", 2000), Is.True);

            s1.Value = 500;
            s2.Value = 99.9f;
            s3.Value = "Batched";

            Assert.That(SpinWait.SpinUntil(() =>
                c1.Value == 500 &&
                Math.Abs(c2.Value - 99.9f) < 0.001f &&
                c3.Value == "Batched", 2000), Is.True);
        }

        [Test]
        public void Test08_CrossThreadWorker_DoubleBuffer_PreventsTornReads()
        {
            string token = $"tearing_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<TestStatePayload>(token, new TestStatePayload { Score = 0, CoordX = 0, CoordY = 0, CoordZ = 0 });

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<TestStatePayload>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int observedTornSnapshots = 0;
            int receivedUpdates = 0;

            clientVar.OnValueChanged += (_, newVal) =>
            {
                Interlocked.Increment(ref receivedUpdates);
                // Invariant validation: CoordX == CoordY == CoordZ. Tearing violates this.
                if (!newVal.IsConsistent)
                {
                    Interlocked.Increment(ref observedTornSnapshots);
                }
            };

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

            // Background worker thread continuously mutating state across multiple variables
            var workerTask = Task.Run(() =>
            {
                float counter = 1.0f;
                while (!cts.IsCancellationRequested)
                {
                    serverVar.Value = new TestStatePayload
                    {
                        Score = (int)counter,
                        CoordX = counter,
                        CoordY = counter,
                        CoordZ = counter
                    };
                    counter += 1.0f;
                    Thread.SpinWait(50);
                }
            });

            workerTask.Wait();

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref receivedUpdates) > 10, 2000), Is.True);
            Assert.That(Volatile.Read(ref observedTornSnapshots), Is.EqualTo(0),
                "Spin-gated double buffer permitted a torn partial struct read across threads.");
        }

        [Test]
        public void Test09_RapidMutation_WithinSingleTick_CollapsesDeltasMonotonically()
        {
            string token = $"rapid_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 0);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            for (int i = 1; i <= 100; i++)
            {
                serverVar.Value = i;
            }

            Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 100, 2000), Is.True);
            Assert.That(clientVar.Version, Is.EqualTo(serverVar.Version));
        }

        [Test]
        public void Test10_HostMode_BidirectionalSync()
        {
            string token = $"host_sync_{Guid.NewGuid():N}";
            _serverManager.StartHost();

            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected && _serverManager.localID != 0, 2000), Is.True);

            var hostVar = new SyncVar<int>(token, 10);
            var remoteClient = CreateAndStartClient();
            var clientVar = remoteClient.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => remoteClient.Transport.IsConnected && clientVar.Value == 10, 2000), Is.True);

            hostVar.Value = 42;
            Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 42, 2000), Is.True);

            hostVar.AddAuthority(remoteClient.localID);
            Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority, 2000), Is.True);

            clientVar.Value = 88;
            Assert.That(SpinWait.SpinUntil(() => hostVar.Value == 88, 2000), Is.True);
        }

        [Test]
        public void Test11_DynamicSlabPaging_ExceedsPageZeroCapacity_RetainsAllData()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            // ServerConfig sets page size to 4096. 30 large arrays push memory past page 0.
            const int totalVars = 30;
            var serverVars = new SyncVar<byte[]>[totalVars];
            var clientVars = new SyncVar<byte[]>[totalVars];

            for (int i = 0; i < totalVars; i++)
            {
                string token = $"page_overflow_{i}_{Guid.NewGuid():N}";
                byte[] data = new byte[256];
                Array.Fill(data, (byte)(i + 1));

                serverVars[i] = new SyncVar<byte[]>(token, data);
                clientVars[i] = client.SyncVarManager.Bind<byte[]>(token);
            }

            Assert.That(SpinWait.SpinUntil(() =>
            {
                for (int i = 0; i < totalVars; i++)
                {
                    var val = clientVars[i].Value;
                    if (val == null || val.Length != 256 || val[0] != (byte)(i + 1))
                        return false;
                }
                return true;
            }, 3000), Is.True, "Failed to synchronize state across dynamically allocated slab pages.");
        }

        [Test]
        public void Test12_ManagerShutdown_ClearsAndReinitializesSlabState()
        {
            string token = $"shutdown_reset_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 10);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            _serverManager.Shutdown();
            Assert.That(_serverManager.Transport.IsConnected, Is.False);

            // Reinitialize network on a fresh port and ensure previous shutdown cleaned up properly
            int newPort = Interlocked.Increment(ref _portCounter);
            _serverManager.StartServer("127.0.0.1", newPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            serverVar.Value = 20;

            var clientConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = newPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };
            var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", newPort);

            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 20, 2000), Is.True);
        }
        #region SetDirty, Authority Guard & Exclusion List Tests

        [Test]
        public void Test13_SetDirty_WithAuthority_BumpsVersionAndFlushesDelta()
        {
            string token = $"setdirty_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 50);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 50, 2000), Is.True);

            uint initialVersion = serverVar.Version;
            bool clientFired = false;

            clientVar.OnValueChanged += (oldVal, newVal) =>
            {
                if (newVal == 50) clientFired = true;
            };

            // Call SetDirty even though the primitive value itself did not change
            serverVar.SetDirty();

            Assert.That(serverVar.Version, Is.GreaterThan(initialVersion), "SetDirty did not increment variable version.");
            Assert.That(SpinWait.SpinUntil(() => clientFired, 2000), Is.True, "SetDirty failed to flush delta across network.");
        }

        [Test]
        public void Test14_SetDirty_WithoutAuthority_BlockedLocally()
        {
            string token = $"unauth_setdirty_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 75);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 75, 2000), Is.True);
            Assert.That(clientVar.HasAuthority, Is.False);

            uint clientVersionBefore = clientVar.Version;
            uint serverVersionBefore = serverVar.Version;

            clientVar.SetDirty();

            Assert.That(clientVar.Version, Is.EqualTo(clientVersionBefore), "Unauthorized SetDirty bumped local version.");
            Thread.Sleep(150);
            Assert.That(serverVar.Version, Is.EqualTo(serverVersionBefore), "Unauthorized SetDirty leaked dirty delta to server.");
        }

        [Test]
        public void Test15_Client_CannotModifyAuthority_LogsErrorAndBlocks()
        {
            string token = $"client_auth_hack_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 10);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 10, 2000), Is.True);
            Assert.That(clientVar.HasAuthority, Is.False);

            ushort clientId = client.localID;

            // Client attempts to grant itself authority
            clientVar.AddAuthority(clientId);
            Assert.That(clientVar.HasAuthority, Is.False, "Client was able to grant itself authority via AddAuthority.");
            Assert.That(clientVar.AuthIds, Does.Not.Contain(clientId));

            // Client attempts SetAuthIds
            clientVar.SetAuthIds(clientId, 99);
            Assert.That(clientVar.HasAuthority, Is.False, "Client was able to set authority via SetAuthIds.");
            Assert.That(clientVar.AuthIds, Does.Not.Contain(clientId));

            // Client attempts RemoveAuthority
            serverVar.AddAuthority(clientId);
            Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority, 2000), Is.True);

            clientVar.RemoveAuthority(clientId);
            Assert.That(clientVar.HasAuthority, Is.True, "Client was able to revoke authority via RemoveAuthority.");
        }

        [Test]
        public void Test16_Server_ExclusionList_AddRemoveSetAndContains()
        {
            string token = $"exclusion_ops_{Guid.NewGuid():N}";
            var syncVar = new SyncVar<int>(token, 0);

            // Add with deduplication
            syncVar.AddExclusion(1);
            syncVar.AddExclusion(1);
            syncVar.AddExclusion(2);

            Assert.Multiple(() =>
            {
                Assert.That(syncVar.ExclusionIds.Length, Is.EqualTo(2));
                Assert.That(syncVar.ContainsExclusion(1), Is.True);
                Assert.That(syncVar.IsExcluded(2), Is.True);
                Assert.That(syncVar.ContainsExclusion(3), Is.False);
            });

            // Remove
            syncVar.RemoveExclusion(1);
            Assert.Multiple(() =>
            {
                Assert.That(syncVar.ContainsExclusion(1), Is.False);
                Assert.That(syncVar.ContainsExclusion(2), Is.True);
                Assert.That(syncVar.ExclusionIds.Length, Is.EqualTo(1));
            });

            // Set with deduplication
            syncVar.SetExclusionIds(10, 10, 20, 30, 20);
            Assert.Multiple(() =>
            {
                Assert.That(syncVar.ExclusionIds.Length, Is.EqualTo(3));
                CollectionAssert.AreEqual(new ushort[] { 10, 20, 30 }, syncVar.ExclusionIds);
            });
        }

        [Test]
        public void Test17_Client_CannotModifyExclusions_LogsErrorAndBlocks()
        {
            string token = $"client_excl_hack_{Guid.NewGuid():N}";
            var serverVar = new SyncVar<int>(token, 100);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();
            var clientVar = client.SyncVarManager.Bind<int>(token);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected && clientVar.Value == 100, 2000), Is.True);

            // Client attempts to modify exclusion lists
            clientVar.AddExclusion(5);
            Assert.That(clientVar.ContainsExclusion(5), Is.False, "Client was able to add an exclusion.");

            clientVar.SetExclusionIds(1, 2, 3);
            Assert.That(clientVar.ExclusionIds, Is.Empty, "Client was able to set exclusions.");

            serverVar.AddExclusion(5);
            clientVar.RemoveExclusion(5);
            Assert.That(serverVar.ContainsExclusion(5), Is.True, "Client was able to remove a server exclusion.");
        }

        #endregion
    }
}