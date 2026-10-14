using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SyncVarRegistrationLifecycleCoyoteTests
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
            Specification.Assert(syncVar.TryExtractSnapshotData(out byte[] data, out _), $"'{syncVar.Token}' has no wire data.");
            return MessagePackSerializer.Deserialize<int>(data);
        }

        private static Task RunCapturing(Func<Task> body, Action<Exception> onError) => Task.Run(async () =>
        {
            try { await body(); }
            catch (Exception ex) { onError(ex); }
        });

        [Test]
        public static async Task Coyote_SyncVar_UnregisterVsWritesAndFlush_NoLostWriteNoSharedMemory()
        {
            var manager = StartMockServer();
            var mgr = manager.SyncVarManager;
            Exception failure = null;

            var shield = new SyncVar<int>("unreg_shield", 0, mgr);
            var victim = new SyncVar<int>("unreg_victim", 777, mgr);

            var writer = RunCapturing(async () =>
            {
                for (int i = 1; i <= 3; i++)
                {
                    shield.Value = i;
                    await Task.Yield();
                }
            }, ex => failure = ex);
            var unregister = RunCapturing(() => { mgr.UnregisterSyncVar(shield); return Task.CompletedTask; }, ex => failure = ex);
            var flusher = RunCapturing(async () =>
            {
                for (int i = 0; i < 2; i++)
                {
                    mgr.FlushDirty();
                    await Task.Yield();
                }
            }, ex => failure = ex);

            await Task.WhenAll(writer, unregister, flusher);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(shield.Value == 3, $"A write was lost: shield is {shield.Value}, expected 3.");

            mgr.ResetSessionState();
            var newcomer = new SyncVar<int>("unreg_newcomer", 555, mgr);
            shield.Value = 10;
            shield.Value = 11;

            Specification.Assert(DecodeWire(newcomer) == 555, "The unregistered SyncVar wrote into a newer SyncVar's slab memory.");
            Specification.Assert(DecodeWire(shield) == 11, $"shield's wire bytes decode to {DecodeWire(shield)}, expected 11.");
            Specification.Assert(DecodeWire(victim) == 777, "An unrelated SyncVar's wire bytes changed.");

            manager.Shutdown();
        }

        [Test]
        public static async Task Coyote_SyncVar_SessionResetVsWrites_NoLostWrite()
        {
            var manager = StartMockServer();
            var mgr = manager.SyncVarManager;
            Exception failure = null;

            var hp = new SyncVar<int>("reset_hp", 0, mgr);

            var writer = RunCapturing(async () =>
            {
                for (int i = 1; i <= 3; i++)
                {
                    hp.Value = i;
                    await Task.Yield();
                }
            }, ex => failure = ex);
            var reset = RunCapturing(() => { mgr.ResetSessionState(); return Task.CompletedTask; }, ex => failure = ex);

            await Task.WhenAll(writer, reset);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(hp.Value == 3, $"A write was lost during the session reset: hp is {hp.Value}, expected 3.");

            hp.Value = 4;
            Specification.Assert(DecodeWire(hp) == 4, "hp did not re-register with a working slab after the reset.");

            manager.Shutdown();
        }
    }
}
