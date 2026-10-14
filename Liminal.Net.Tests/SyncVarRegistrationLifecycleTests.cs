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
    }
}
