using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SyncVarAdoptionCoyoteTests
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

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        private static int CountRegistrySlots(LiminalNetworkManager manager, ISyncVarInternal syncVar)
        {
            var registry = (ISyncVarInternal[])typeof(SyncVarManager)
                .GetField("_localRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(manager.SyncVarManager)!;
            int count = 0;
            for (int i = 0; i < registry.Length; i++)
            {
                if (ReferenceEquals(Volatile.Read(ref registry[i]), syncVar)) count++;
            }
            return count;
        }

        private static bool IsRegisteredIn(LiminalNetworkManager manager, ISyncVarInternal syncVar)
            => manager.SyncVarManager.TryGetSyncVar(syncVar.Token, out var registered) && ReferenceEquals(registered, syncVar);

        private static bool IsListedIn(LiminalNetworkManager manager, string token)
            => manager.SyncVarManager.TryGetSyncVar(token, out _);

        private static int DecodeWire(SyncVar<int> syncVar)
        {
            Specification.Assert(syncVar.TryExtractSnapshotData(out byte[] data, out _), $"'{syncVar.Token}' has no wire data.");
            return MessagePackSerializer.Deserialize<int>(data);
        }

        private static SyncVar<int> CreateOrphan(string token, int value)
        {
            var old = StartMockServer();
            var syncVar = new SyncVar<int>(token, value, old.SyncVarManager);
            old.Shutdown();
            return syncVar;
        }

        private static Task RunCapturing(Func<Task> body, Action<Exception> onError) => Task.Run(async () =>
        {
            try { await body(); }
            catch (Exception ex) { onError(ex); }
        });

        private static Task RunCapturing(Action body, Action<Exception> onError) => Task.Run(() =>
        {
            try { body(); }
            catch (Exception ex) { onError(ex); }
        });

        private static void AssertOwnedByExactlyOne(SyncVar<int> syncVar, LiminalNetworkManager a, LiminalNetworkManager b)
        {
            bool inA = IsRegisteredIn(a, syncVar);
            bool inB = IsRegisteredIn(b, syncVar);
            Specification.Assert(inA ^ inB, $"'{syncVar.Token}' registered in A={inA}, B={inB}; expected exactly one.");

            var owner = inA ? a : b;
            var other = inA ? b : a;
            int ownerSlots = CountRegistrySlots(owner, syncVar);
            int otherSlots = CountRegistrySlots(other, syncVar);
            Specification.Assert(ownerSlots == 1, $"Owner holds {ownerSlots} slots, expected 1.");
            Specification.Assert(otherSlots == 0, $"Non-owner holds {otherSlots} slots, expected 0.");
            Specification.Assert(!IsListedIn(other, syncVar.Token), "Non-owner still lists the token.");
        }

        [Test]
        public static async Task Coyote_Adoption_VsValueWrites_JoinsOnceAndKeepsLatestValue()
        {
            Exception failure = null;
            var hp = CreateOrphan(UniqueToken("adopt_writes"), 0);
            LiminalNetworkManager adopter = null;

            var create = RunCapturing(() => { adopter = StartMockServer(); }, ex => failure = ex);
            var writer = RunCapturing(async () =>
            {
                for (int i = 1; i <= 3; i++)
                {
                    hp.Value = i;
                    await Task.Yield();
                }
            }, ex => failure = ex);

            await Task.WhenAll(create, writer);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(hp.Value == 3, $"A write was lost: hp is {hp.Value}, expected 3.");
            Specification.Assert(IsRegisteredIn(adopter, hp), "The new manager did not adopt the orphan.");
            Specification.Assert(CountRegistrySlots(adopter, hp) == 1, $"Orphan holds {CountRegistrySlots(adopter, hp)} slots, expected 1.");
            Specification.Assert(DecodeWire(hp) == 3, $"Wire bytes decode to {DecodeWire(hp)}, expected 3.");

            adopter.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_TwoNewManagers_ExactlyOneOwner()
        {
            Exception failure = null;
            var hp = CreateOrphan(UniqueToken("adopt_two_new"), 7);
            LiminalNetworkManager a = null, b = null;

            var createA = RunCapturing(() => { a = StartMockServer(); }, ex => failure = ex);
            var createB = RunCapturing(() => { b = StartMockServer(); }, ex => failure = ex);
            await Task.WhenAll(createA, createB);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertOwnedByExactlyOne(hp, a, b);
            Specification.Assert(DecodeWire(hp) == 7, "Wire bytes corrupted during adoption.");

            a.Shutdown();
            b.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_ConcurrentAdoptionPasses_ExactlyOneOwner()
        {
            Exception failure = null;
            var a = StartMockServer();
            var b = StartMockServer();
            var hp = CreateOrphan(UniqueToken("adopt_concurrent"), 6);

            var adoptA = RunCapturing(() => a.SyncVarManager.AdoptOrphans(), ex => failure = ex);
            var adoptB = RunCapturing(() => b.SyncVarManager.AdoptOrphans(), ex => failure = ex);
            await Task.WhenAll(adoptA, adoptB);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertOwnedByExactlyOne(hp, a, b);
            Specification.Assert(DecodeWire(hp) == 6, $"Wire bytes decode to {DecodeWire(hp)}, expected 6.");

            hp.Value = 7;
            Specification.Assert(DecodeWire(hp) == 7, "The owner's slab does not accept writes after adoption.");

            a.Shutdown();
            b.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_VsOwnerShutdown_NoSharedSlabMemory()
        {
            Exception failure = null;
            var adopter = StartMockServer();
            var owner = StartMockServer();
            var hp = new SyncVar<int>(UniqueToken("adopt_owner_shutdown"), 4, owner.SyncVarManager);

            var shutdown = RunCapturing(() => owner.Shutdown(), ex => failure = ex);
            var adopt = RunCapturing(() => adopter.SyncVarManager.AdoptOrphans(), ex => failure = ex);
            await Task.WhenAll(shutdown, adopt);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");

            owner.StartServer("127.0.0.1", 7777);
            adopter.SyncVarManager.AdoptOrphans();
            AssertOwnedByExactlyOne(hp, owner, adopter);

            var probeOwner = new SyncVar<int>(UniqueToken("adopt_probe_owner"), 111, owner.SyncVarManager);
            var probeAdopter = new SyncVar<int>(UniqueToken("adopt_probe_adopter"), 222, adopter.SyncVarManager);
            hp.Value = 5;
            hp.Value = 6;

            Specification.Assert(DecodeWire(hp) == 6, $"hp wire bytes decode to {DecodeWire(hp)}, expected 6.");
            Specification.Assert(DecodeWire(probeOwner) == 111, "hp wrote into the restarted owner's slab memory.");
            Specification.Assert(DecodeWire(probeAdopter) == 222, "hp wrote into the adopter's slab memory.");

            owner.Shutdown();
            adopter.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_OldManagerRestartVsNewManager_ExactlyOneOwner()
        {
            Exception failure = null;
            var old = StartMockServer();
            var hp = new SyncVar<int>(UniqueToken("adopt_restart"), 8, old.SyncVarManager);
            old.Shutdown();
            LiminalNetworkManager fresh = null;

            var restart = RunCapturing(() => { old.StartServer("127.0.0.1", 7777); }, ex => failure = ex);
            var create = RunCapturing(() => { fresh = StartMockServer(); }, ex => failure = ex);
            await Task.WhenAll(restart, create);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertOwnedByExactlyOne(hp, old, fresh);
            Specification.Assert(DecodeWire(hp) == 8, "Wire bytes corrupted during adoption.");

            hp.Value = 9;
            Specification.Assert(DecodeWire(hp) == 9, "The owner's slab does not accept writes after adoption.");

            old.Shutdown();
            fresh.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_VsTeardown_DisposedSyncVarNeverRegistered()
        {
            Exception failure = null;
            var hp = CreateOrphan(UniqueToken("adopt_teardown"), 1);
            LiminalNetworkManager adopter = null;

            var create = RunCapturing(() => { adopter = StartMockServer(); }, ex => failure = ex);
            var teardown = RunCapturing(() => ((ISyncVarInternal)hp).InternalTeardown(), ex => failure = ex);
            await Task.WhenAll(create, teardown);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(hp.IsDisposed, "Teardown did not dispose the SyncVar.");
            Specification.Assert(!IsListedIn(adopter, hp.Token), "A torn-down SyncVar is still registered.");
            Specification.Assert(CountRegistrySlots(adopter, hp) == 0, "A torn-down SyncVar still holds a slot.");

            var probe = new SyncVar<int>(UniqueToken("adopt_teardown_probe"), 42, adopter.SyncVarManager);
            Specification.Assert(DecodeWire(probe) == 42, "Slab memory was corrupted by the torn-down SyncVar.");

            adopter.Shutdown();
        }

        [Test]
        public static async Task Coyote_Adoption_NewSyncVarVsLeftoverUse_NewcomerOwnsToken()
        {
            Exception failure = null;
            string token = UniqueToken("adopt_newcomer");
            var leftover = CreateOrphan(token, 1);
            var adopter = StartMockServer();
            adopter.SyncVarManager.ResetSessionState();
            SyncVar<int> fresh = null;

            var construct = RunCapturing(() => { fresh = new SyncVar<int>(token, 2, adopter.SyncVarManager); }, ex => failure = ex);
            var use = RunCapturing(() => { leftover.Value = 99; }, ex => failure = ex);
            await Task.WhenAll(construct, use);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(IsRegisteredIn(adopter, fresh), "The newly constructed SyncVar does not own its token.");
            Specification.Assert(CountRegistrySlots(adopter, fresh) == 1, "Newcomer slot count is not 1.");
            Specification.Assert(CountRegistrySlots(adopter, leftover) == 0, "The leftover still holds a slot.");
            Specification.Assert(DecodeWire(fresh) == 2, $"Newcomer wire bytes decode to {DecodeWire(fresh)}, expected 2.");
            Specification.Assert(leftover.Value == 99, "The leftover lost its local value.");

            adopter.Shutdown();
        }
    }
}
