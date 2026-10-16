using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SyncVarAdoptionTests
    {
        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static LiminalNetworkManager CreateManager(out MockTransport transport)
        {
            var config = CreateConfig();
            transport = new MockTransport();
            return new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
        }

        private static LiminalNetworkManager StartMockServer()
        {
            var manager = CreateManager(out _);
            manager.StartServer("127.0.0.1", 7777);
            return manager;
        }

        private static LiminalNetworkManager StartMockClient()
        {
            var manager = CreateManager(out var transport);
            ConnectAsMockClient(manager, transport);
            return manager;
        }

        private static void ConnectAsMockClient(LiminalNetworkManager manager, MockTransport transport)
        {
            transport.IsServer = false;
            manager.StartClient("127.0.0.1", 7777);
            transport.TriggerLocalClientConnected(transport.LocalClientId);
            Assert.That(manager.IsConnected, Is.True, "Mock client failed to reach the Connected state.");
        }

        //Earlier tests can leave a live manager that new syncvars would join, create and shutdown one so there is no default manager
        private static void ClearDefaultManager()
        {
            CreateManager(out _).Shutdown();
            Assert.That(SyncVarManager.GetManagerForRegistration(), Is.Null);
        }

        private static void PushFromServer(LiminalNetworkManager client, string token, uint version, int value)
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(buffer);
            writer.WriteArrayHeader(1);
            var tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
            writer.WriteUInt8((byte)tokenBytes.Length);
            writer.WriteRaw(tokenBytes);
            writer.WriteUInt32(version);
            var valueBuffer = new ArrayBufferWriter<byte>();
            MessagePackSerializer.Serialize(valueBuffer, value);
            writer.WriteInt32(valueBuffer.WrittenCount);
            writer.WriteRaw(valueBuffer.WrittenSpan);
            writer.Flush();

            client.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarSlabBatchPacket>(),
                ILiminalTransport.SERVER_ID,
                MessagePackSerializer.Serialize(new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(buffer.WrittenMemory))));
        }

        private static int DecodeWire(SyncVar<int> syncVar)
        {
            Assert.That(syncVar.TryExtractSnapshotData(out byte[] data, out _), Is.True, $"'{syncVar.Token}' has no wire data.");
            return MessagePackSerializer.Deserialize<int>(data);
        }

        private static bool IsRegisteredIn(LiminalNetworkManager manager, ISyncVarInternal syncVar)
            => manager.SyncVarManager.TryGetSyncVar(syncVar.Token, out var registered) && ReferenceEquals(registered, syncVar);

        [Test]
        public void SyncVarCreatedBeforeManager_ReceivesServerValue_WithoutBeingTouched()
        {
            ClearDefaultManager();
            var hp = new SyncVar<int>("adopt_before_manager", 1);
            int seen = 0;
            hp.OnValueChanged += (_, next) => seen = next;

            var client = StartMockClient();
            try
            {
                PushFromServer(client, "adopt_before_manager", 5, 50);
                Assert.That(seen, Is.EqualTo(50), "OnValueChanged did not fire for a SyncVar created before the manager.");
            }
            finally { client.Shutdown(); }
        }

        [Test]
        public void SyncVar_SurvivesManagerRecreation_WithoutBeingTouched()
        {
            var first = StartMockClient();
            var hp = new SyncVar<int>("adopt_recreated_manager", 1, first.SyncVarManager);
            first.Shutdown();

            int seen = 0;
            hp.OnValueChanged += (_, next) => seen = next;

            var second = StartMockClient();
            try
            {
                PushFromServer(second, "adopt_recreated_manager", 5, 60);
                Assert.That(seen, Is.EqualTo(60), "OnValueChanged did not fire after the manager was recreated.");
                Assert.That(IsRegisteredIn(second, hp), Is.True);
                Assert.That(first.SyncVarManager.TryGetSyncVar("adopt_recreated_manager", out _), Is.False,
                    "The old manager still lists the adopted SyncVar.");
            }
            finally { second.Shutdown(); }
        }

        [Test]
        public void SyncVar_SurvivesSameManagerRestart_WithoutBeingTouched()
        {
            var manager = CreateManager(out var transport);
            var hp = new SyncVar<int>("adopt_same_manager", 1, manager.SyncVarManager);
            ConnectAsMockClient(manager, transport);
            manager.Disconnect();

            int seen = 0;
            hp.OnValueChanged += (_, next) => seen = next;

            ConnectAsMockClient(manager, transport);
            try
            {
                PushFromServer(manager, "adopt_same_manager", 5, 70);
                Assert.That(seen, Is.EqualTo(70));
            }
            finally { manager.Shutdown(); }
        }

        [Test]
        public void ServerAdoptsSyncVarCreatedBeforeIt()
        {
            ClearDefaultManager();
            var hp = new SyncVar<int>("adopt_server_side", 33);

            var server = StartMockServer();
            try
            {
                Assert.That(IsRegisteredIn(server, hp), Is.True, "The server did not adopt a SyncVar created before it.");
                Assert.That(DecodeWire(hp), Is.EqualTo(33), "The adopted SyncVar has no snapshot data for connecting clients.");
            }
            finally { server.Shutdown(); }
        }

        [Test]
        public void Adoption_NeverTakesSyncVarFromAnotherLiveManager()
        {
            var owner = StartMockClient();
            var hp = new SyncVar<int>("adopt_never_steal", 1, owner.SyncVarManager);

            var other = StartMockServer();
            try
            {
                Assert.That(IsRegisteredIn(owner, hp), Is.True);
                Assert.That(other.SyncVarManager.TryGetSyncVar("adopt_never_steal", out _), Is.False,
                    "A new manager took a SyncVar from a live manager.");

                other.Shutdown();
                other.StartServer("127.0.0.1", 7777);
                Assert.That(other.SyncVarManager.TryGetSyncVar("adopt_never_steal", out _), Is.False,
                    "A restarted manager took a SyncVar from a live manager.");
                Assert.That(IsRegisteredIn(owner, hp), Is.True);
            }
            finally
            {
                other.Shutdown();
                owner.Shutdown();
            }
        }

        [Test]
        public void UnregisteredSyncVar_IsNotAdopted_UntilUsedAgain()
        {
            var first = StartMockServer();
            var hp = new SyncVar<int>("adopt_unregistered", 1, first.SyncVarManager);
            first.SyncVarManager.UnregisterSyncVar(hp);

            first.Shutdown();
            first.StartServer("127.0.0.1", 7777);
            Assert.That(first.SyncVarManager.TryGetSyncVar("adopt_unregistered", out _), Is.False,
                "A restart brought back a SyncVar the game unregistered.");
            first.Shutdown();

            var second = StartMockServer();
            try
            {
                Assert.That(second.SyncVarManager.TryGetSyncVar("adopt_unregistered", out _), Is.False,
                    "A new manager adopted a SyncVar the game unregistered.");

                hp.Value = 5;
                Assert.That(IsRegisteredIn(second, hp), Is.True, "Using the SyncVar again did not register it.");
                Assert.That(DecodeWire(hp), Is.EqualTo(5));
            }
            finally { second.Shutdown(); }
        }

        //Leftover from an earlier session gets adopted but a newly constructed one with the same token has to replace it
        [Test]
        public void NewSyncVar_ReplacesAdoptedLeftoverWithSameToken()
        {
            var first = StartMockServer();
            var leftover = new SyncVar<int>("adopt_leftover", 1, first.SyncVarManager);
            first.Shutdown();

            var second = StartMockServer();
            try
            {
                Assert.That(IsRegisteredIn(second, leftover), Is.True);

                var fresh = new SyncVar<int>("adopt_leftover", 2, second.SyncVarManager);
                Assert.That(IsRegisteredIn(second, fresh), Is.True, "The new SyncVar was rejected by an adopted leftover.");

                leftover.Value = 99;
                Assert.That(IsRegisteredIn(second, fresh), Is.True, "Touching the leftover took the token back.");
                Assert.That(fresh.Value, Is.EqualTo(2));
            }
            finally { second.Shutdown(); }
        }

        //Manager kept across sessions, the game destroys its objects and binds the same tokens again in the next one
        [Test]
        public void NewSyncVar_ReplacesLeftoverFromPreviousSession_OnSameManager()
        {
            var manager = CreateManager(out var transport);
            var leftover = new SyncVar<int>("rejoin_same_manager", 1, manager.SyncVarManager);
            ConnectAsMockClient(manager, transport);
            manager.Disconnect();
            ConnectAsMockClient(manager, transport);
            try
            {
                var fresh = new SyncVar<int>("rejoin_same_manager", 2, manager.SyncVarManager);
                Assert.That(IsRegisteredIn(manager, fresh), Is.True, "The new SyncVar was rejected by the previous session's leftover.");

                int seen = 0;
                fresh.OnValueChanged += (_, next) => seen = next;
                PushFromServer(manager, "rejoin_same_manager", 5, 80);
                Assert.That(seen, Is.EqualTo(80), "The server value did not reach the new SyncVar.");

                leftover.Value = 99;
                Assert.That(IsRegisteredIn(manager, fresh), Is.True, "Touching the leftover took the token back.");
                Assert.That(fresh.Value, Is.EqualTo(80));
            }
            finally { manager.Shutdown(); }
        }

        [Test]
        public void NewSyncVar_CreatedWhileDisconnected_ReplacesLeftover()
        {
            var manager = CreateManager(out var transport);
            var leftover = new SyncVar<int>("rejoin_while_disconnected", 1, manager.SyncVarManager);
            ConnectAsMockClient(manager, transport);
            manager.Disconnect();
            try
            {
                var fresh = new SyncVar<int>("rejoin_while_disconnected", 2, manager.SyncVarManager);
                ConnectAsMockClient(manager, transport);

                Assert.That(IsRegisteredIn(manager, fresh), Is.True, "The new SyncVar lost its token when the leftover came back on reconnect.");
                Assert.That(IsRegisteredIn(manager, leftover), Is.False);
            }
            finally { manager.Shutdown(); }
        }

        //What the replacement warning suggests, Bind hands back the same instance in the next session instead of replacing it
        [TestCase("SameManager")]
        [TestCase("NewManager")]
        public void Bind_InNextSession_ReturnsTheExistingSyncVar(string how)
        {
            var first = CreateManager(out var transport);
            var hp = first.SyncVarManager.Bind<int>("bind_next_session_" + how, 1);
            ConnectAsMockClient(first, transport);
            first.Disconnect();

            var next = first;
            if (how == "SameManager") ConnectAsMockClient(first, transport);
            else
            {
                first.Shutdown();
                next = StartMockClient();
            }

            var originalOut = Console.Out;
            var captured = new System.IO.StringWriter();
            Console.SetOut(captured);
            try
            {
                var again = next.SyncVarManager.Bind<int>(hp.Token, 2);
                Console.SetOut(originalOut);

                Assert.That(again, Is.SameAs(hp), "Bind created a second SyncVar instead of returning the existing one.");
                Assert.That(IsRegisteredIn(next, hp), Is.True);
                Assert.That(captured.ToString(), Does.Not.Contain("replaced an older one"));
            }
            finally
            {
                Console.SetOut(originalOut);
                next.Shutdown();
            }
        }

        [Test]
        public void SyncVarBoundInNewSession_StillRefusesDuplicates()
        {
            var manager = CreateManager(out var transport);
            _ = new SyncVar<int>("rejoin_duplicate", 1, manager.SyncVarManager);
            ConnectAsMockClient(manager, transport);
            manager.Disconnect();
            ConnectAsMockClient(manager, transport);
            try
            {
                var fresh = new SyncVar<int>("rejoin_duplicate", 2, manager.SyncVarManager);
                var duplicate = new SyncVar<int>("rejoin_duplicate", 3, manager.SyncVarManager);

                Assert.That(IsRegisteredIn(manager, fresh), Is.True);
                Assert.That(IsRegisteredIn(manager, duplicate), Is.False, "A duplicate from the same session replaced a live SyncVar.");
            }
            finally { manager.Shutdown(); }
        }

        [Test]
        public void ReplacingLeftover_WarnsThatTheOldOneStoppedSyncing()
        {
            var manager = CreateManager(out var transport);
            _ = new SyncVar<int>("rejoin_warning", 1, manager.SyncVarManager);
            ConnectAsMockClient(manager, transport);
            manager.Disconnect();
            ConnectAsMockClient(manager, transport);

            var originalOut = Console.Out;
            var captured = new System.IO.StringWriter();
            Console.SetOut(captured);
            try
            {
                _ = new SyncVar<int>("rejoin_warning", 2, manager.SyncVarManager);
            }
            finally
            {
                Console.SetOut(originalOut);
                manager.Shutdown();
            }

            string log = captured.ToString();
            Assert.That(log, Does.Contain("'rejoin_warning'").And.Contain("no longer syncs"), $"Log was: {log}");
        }

        //Disconnect detaches the syncvar manager from manager events, a later Shutdown still has to tear it down so a new manager can adopt
        [TestCase("Disconnect")]
        [TestCase("ConnectionLost")]
        public void ShutdownAfterSessionEnded_NewManagerAdoptsSyncVars(string howEnded)
        {
            var first = CreateManager(out var transport);
            var hp = new SyncVar<int>("adopt_after_" + howEnded, 1, first.SyncVarManager);
            ConnectAsMockClient(first, transport);
            if (howEnded == "Disconnect") first.Disconnect();
            else
            {
                transport.TriggerLocalClientDisconnected(transport.LocalClientId);
                transport.Shutdown();
            }
            first.Shutdown();

            Assert.That(first.SyncVarManager.IsTornDown, Is.True, "Shutdown left the SyncVarManager alive.");

            int seen = 0;
            hp.OnValueChanged += (_, next) => seen = next;
            var second = StartMockClient();
            try
            {
                Assert.That(IsRegisteredIn(second, hp), Is.True, "The new manager did not adopt the SyncVar.");
                PushFromServer(second, hp.Token, 5, 90);
                Assert.That(seen, Is.EqualTo(90));
            }
            finally { second.Shutdown(); }
        }

        [Test]
        public void SyncVarConstructedForLiveManager_KeepsItsTokenAgainstNewcomers()
        {
            var server = StartMockServer();
            try
            {
                var original = new SyncVar<int>("adopt_duplicate", 1, server.SyncVarManager);
                var duplicate = new SyncVar<int>("adopt_duplicate", 2, server.SyncVarManager);

                Assert.That(IsRegisteredIn(server, original), Is.True);
                Assert.That(IsRegisteredIn(server, duplicate), Is.False);
            }
            finally { server.Shutdown(); }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static WeakReference CreateUnreferencedSyncVar(string token) => new WeakReference(new SyncVar<int>(token, 1));

        [Test]
        public void UnreferencedOrphanSyncVar_CanBeCollected()
        {
            ClearDefaultManager();
            var weak = CreateUnreferencedSyncVar("adopt_collectable");

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.That(weak.IsAlive, Is.False, "The SyncVar directory keeps orphan SyncVars alive.");
        }
    }
}
