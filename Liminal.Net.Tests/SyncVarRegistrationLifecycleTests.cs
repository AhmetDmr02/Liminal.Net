using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using NUnit.Framework;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SyncVarRegistrationLifecycleTests
    {
        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static LiminalNetworkManager StartMockServer()
        {
            var config = CreateConfig();
            var manager = new LiminalNetworkManager(new MockTransport(), config, customTicker: new LiminalUnityTicker(config));
            manager.StartServer("127.0.0.1", 7777);
            return manager;
        }

        private static int DecodeWire(SyncVar<int> syncVar)
        {
            Assert.That(syncVar.TryExtractSnapshotData(out byte[] data, out _), Is.True, $"'{syncVar.Token}' has no wire data.");
            return MessagePackSerializer.Deserialize<int>(data);
        }

        [Test]
        public void UnregisteredSyncVar_AfterSessionReset_DoesNotWriteIntoAnotherSyncVarsMemory()
        {
            var manager = StartMockServer();
            var mgr = manager.SyncVarManager;
            try
            {
                var shield = new SyncVar<int>("lifecycle_shield", 111, mgr);
                mgr.UnregisterSyncVar(shield);

                mgr.ResetSessionState();

                var ammo = new SyncVar<int>("lifecycle_ammo", 222, mgr);
                shield.Value = 999;
                shield.Value = 998;

                Assert.That(DecodeWire(ammo), Is.EqualTo(222), "Unregistered SyncVar wrote into another SyncVar's slab memory.");
                Assert.That(DecodeWire(shield), Is.EqualTo(998));
                Assert.That(shield.ActiveSlotCoordinates, Is.Not.EqualTo(ammo.ActiveSlotCoordinates));
            }
            finally { manager.Shutdown(); }
        }

        [Test]
        public void UnregisteredSyncVar_ReRegistersOnUse_KeepingItsValue()
        {
            var manager = StartMockServer();
            var mgr = manager.SyncVarManager;
            try
            {
                var shield = new SyncVar<int>("lifecycle_reregister", 5, mgr);
                shield.Value = 40;
                mgr.UnregisterSyncVar(shield);

                Assert.That(mgr.TryGetSyncVar("lifecycle_reregister", out _), Is.False);
                Assert.That(shield.Value, Is.EqualTo(40), "Unregistering must not lose the value.");

                shield.Value = 41;

                Assert.That(mgr.TryGetSyncVar("lifecycle_reregister", out var registered), Is.True, "Using the SyncVar did not re-register it.");
                Assert.That(registered, Is.SameAs(shield));
                Assert.That(DecodeWire(shield), Is.EqualTo(41));
            }
            finally { manager.Shutdown(); }
        }

        //Fills the reset slab from the start so a stale slot held by an unregistered syncvar overlaps one of these
        private static SyncVar<int>[] CreateVictims(SyncVarManager mgr, string prefix)
        {
            var victims = new SyncVar<int>[6];
            for (int i = 0; i < victims.Length; i++) victims[i] = new SyncVar<int>($"{prefix}_{i}", 500 + i, mgr);
            return victims;
        }

        private static void AssertVictimsIntact(SyncVar<int>[] victims)
        {
            for (int i = 0; i < victims.Length; i++)
            {
                Assert.That(DecodeWire(victims[i]), Is.EqualTo(500 + i), $"{victims[i].Token} was overwritten by a SyncVar that is not registered.");
            }
        }

        [Test]
        public void RejectedDuplicate_AfterSessionReset_DoesNotWriteIntoAnotherSyncVarsMemory()
        {
            var manager = StartMockServer();
            var mgr = manager.SyncVarManager;
            try
            {
                var original = new SyncVar<int>("lifecycle_dup", 1, mgr);
                var duplicate = new SyncVar<int>("lifecycle_dup", 2, mgr);
                duplicate.Value = 999;

                mgr.ResetSessionState();

                var victims = CreateVictims(mgr, "lifecycle_dup_victim");
                duplicate.Value = 777;
                duplicate.Value = 778;

                AssertVictimsIntact(victims);
                Assert.That(duplicate.Value, Is.EqualTo(778), "The rejected duplicate must still keep its value locally.");
                Assert.That(mgr.TryGetSyncVar("lifecycle_dup", out var registered) && ReferenceEquals(registered, original), Is.True);
            }
            finally { manager.Shutdown(); }
        }

        //Its old manager is shut down and the new one already has the token, so joining fails after picking the new slab
        [Test]
        public void SyncVarFailingToJoinNewManager_DoesNotWriteIntoThatManagersMemory()
        {
            var oldManager = StartMockServer();
            var leftover = new SyncVar<int>("lifecycle_join_taken", 1, oldManager.SyncVarManager);
            var newManager = StartMockServer();
            var mgr = newManager.SyncVarManager;
            try
            {
                var owner = new SyncVar<int>("lifecycle_join_taken", 2, mgr);
                oldManager.Shutdown();

                leftover.Value = 999;
                mgr.ResetSessionState();

                var victims = CreateVictims(mgr, "lifecycle_join_victim");
                leftover.Value = 777;
                leftover.Value = 778;

                AssertVictimsIntact(victims);
                Assert.That(mgr.TryGetSyncVar("lifecycle_join_taken", out var registered) && ReferenceEquals(registered, owner), Is.True);
            }
            finally
            {
                oldManager.Shutdown();
                newManager.Shutdown();
            }
        }
    }
}
