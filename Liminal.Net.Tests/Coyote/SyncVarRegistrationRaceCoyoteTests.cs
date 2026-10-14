using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SyncVarRegistrationRaceCoyoteTests
    {
        private static LiminalNetworkManager CreateServer()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };
            var manager = new LiminalNetworkManager(new MockTransport(), config, customTicker: new Liminal.Net.Unity.LiminalUnityTicker(config));
            manager.StartServer("127.0.0.1", 7777);
            return manager;
        }

        private static void StoreUnboundData(SyncVarManager mgr, string token, uint version, int value)
        {
            typeof(SyncVarManager)
                .GetMethod("StoreUnboundData", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(mgr, new object[] { token, version, MessagePackSerializer.Serialize(value) });
        }

        private static int CountRegistrySlots(SyncVarManager mgr, ISyncVarInternal sv)
        {
            var registry = (ISyncVarInternal[])typeof(SyncVarManager)
                .GetField("_localRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(mgr)!;
            int count = 0;
            for (int i = 0; i < registry.Length; i++)
            {
                if (ReferenceEquals(Volatile.Read(ref registry[i]), sv)) count++;
            }
            return count;
        }

        [Test]
        public static void Coyote_SyncVar_CreatedAfterCachedData_OccupiesExactlyOneSlot()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            StoreUnboundData(mgr, "Reentry_Var", 5u, 42);
            var syncVar = new SyncVar<int>("Reentry_Var", 0, mgr);

            int slots = CountRegistrySlots(mgr, syncVar);
            Specification.Assert(slots == 1, $"SyncVar occupies {slots} registry slots, expected 1 (LocalSlot={syncVar.LocalSlot}).");

            ((ISyncVarInternal)syncVar).InternalTeardown();
            int leaked = CountRegistrySlots(mgr, syncVar);
            Specification.Assert(leaked == 0, $"Torn-down SyncVar still referenced by {leaked} registry slot(s).");

            manager.Shutdown();
        }

        [Test]
        public static async Task Coyote_SyncVar_RegisterWithCachedDataVsEnsureRegistered_NoLockInversionDeadlock()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            var syncVar = new SyncVar<int>("Inversion_Var", 0, mgr);
            mgr.UnregisterSyncVar(syncVar);
            StoreUnboundData(mgr, "Inversion_Var", 5u, 42);

            var registerTask = Task.Run(() => mgr.RegisterSyncVar(syncVar));
            var ensureTask = Task.Run(() => { _ = syncVar.Value; });

            await Task.WhenAll(registerTask, ensureTask);

            manager.Shutdown();
        }

        [Test]
        public static async Task Coyote_SyncVar_RegisterVsTeardown_NoDisposedVarLeftInRegistry()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            var syncVar = new SyncVar<int>("RegTeardown_Var", 0, mgr);
            mgr.UnregisterSyncVar(syncVar);

            var registerTask = Task.Run(() => mgr.RegisterSyncVar(syncVar));
            var teardownTask = Task.Run(() => ((ISyncVarInternal)syncVar).InternalTeardown());

            await Task.WhenAll(registerTask, teardownTask);

            int leaked = CountRegistrySlots(mgr, syncVar);
            Specification.Assert(leaked == 0, $"Disposed SyncVar still referenced by {leaked} registry slot(s).");

            manager.Shutdown();
        }

        // Each handler below blocks on another thread that needs the lock in question.
        // If the event is raised while that lock is held, Coyote reports a deadlock.

        [Test]
        public static async Task Coyote_SyncVar_VisibilityEventOnCachedRegistration_NotRaisedUnderUnboundLock()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            var hp = new SyncVar<int>("EventLock_Register", 0, mgr);
            mgr.UnregisterSyncVar(hp);
            hp.SetLocallyVisible(false);

            int raised = 0;
            hp.OnVisibilityScopeChanged += _ =>
            {
                Interlocked.Increment(ref raised);
                Task.Run(() => new SyncVar<int>("EventLock_Register_Probe", 0, mgr)).Wait();
            };

            StoreUnboundData(mgr, "EventLock_Register", 5u, 42);
            await Task.Run(() => mgr.RegisterSyncVar(hp));

            Specification.Assert(Volatile.Read(ref raised) == 1, $"Visibility event raised {raised} times, expected 1.");
            manager.Shutdown();
        }

        [Test]
        public static async Task Coyote_SyncVar_VisibilityEventOnTeardown_NotRaisedUnderWriteLock()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            var hp = new SyncVar<int>("EventLock_Teardown", 0, mgr);
            hp.SetLocallyVisible(true);

            int raised = 0;
            hp.OnVisibilityScopeChanged += _ =>
            {
                Interlocked.Increment(ref raised);
                Task.Run(() => hp.ApplyClearFromRemote()).Wait();
            };

            await Task.Run(() => ((ISyncVarInternal)hp).InternalTeardown());

            Specification.Assert(Volatile.Read(ref raised) == 1, $"Visibility event raised {raised} times, expected 1.");
            manager.Shutdown();
        }

        [Test]
        public static async Task Coyote_SyncVar_VisibilityEventOnDisconnect_NotRaisedUnderUnboundLock()
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;

            var hp = new SyncVar<int>("EventLock_Disconnect", 0, mgr);
            hp.SetLocallyVisible(true);

            int raised = 0;
            hp.OnVisibilityScopeChanged += _ =>
            {
                Interlocked.Increment(ref raised);
                Task.Run(() => new SyncVar<int>("EventLock_Disconnect_Probe", 0, mgr)).Wait();
            };

            await Task.Run(() => manager.Disconnect());

            Specification.Assert(Volatile.Read(ref raised) == 1, $"Visibility event raised {raised} times, expected 1.");
            manager.Shutdown();
        }

        [Test]
        public static Task Coyote_SyncVar_WriteDuringSingleDisconnectReconnect_StaysRegisteredAndSynced()
            => RunWriteDuringDisconnectReconnect(cycles: 1);

        [Test]
        public static Task Coyote_SyncVar_WriteDuringRepeatedDisconnectReconnect_StaysRegisteredAndSynced()
            => RunWriteDuringDisconnectReconnect(cycles: 2);

        private static async Task RunWriteDuringDisconnectReconnect(int cycles)
        {
            var manager = CreateServer();
            var mgr = manager.SyncVarManager;
            var hp = new SyncVar<int>("Disconnect_Hp", 0, mgr);

            Exception? gameException = null;
            Exception? networkException = null;

            var gameTask = Task.Run(() =>
            {
                try
                {
                    for (int i = 1; i <= 4; i++)
                    {
                        hp.Value = i;
                    }
                }
                catch (Exception ex)
                {
                    gameException = ex;
                }
            });

            var networkTask = Task.Run(() =>
            {
                try
                {
                    for (int c = 0; c < cycles; c++)
                    {
                        manager.Disconnect();
                        manager.StartServer("127.0.0.1", 7777);
                    }
                }
                catch (Exception ex)
                {
                    networkException = ex;
                }
            });

            await Task.WhenAll(gameTask, networkTask);

            Specification.Assert(gameException == null, $"Game thread threw during disconnect: {gameException}");
            Specification.Assert(networkException == null, $"Network thread threw during disconnect: {networkException}");
            Specification.Assert(ReferenceEquals(manager.SyncVarManager, mgr), "Reconnect must reuse the same SyncVarManager.");

            hp.Value = 100;

            ushort slot = hp.LocalSlot;
            Specification.Assert(slot != ushort.MaxValue, "hp must hold a slot after reconnect.");
            Specification.Assert(mgr.SlotBitset.IsAllocated(slot), $"hp believes it owns slot {slot}, but the manager has it marked free.");

            int owned = CountRegistrySlots(mgr, hp);
            Specification.Assert(owned == 1, $"hp is referenced by {owned} registry slot(s), expected exactly 1.");

            var (page, offset) = hp.ActiveSlotCoordinates;
            Specification.Assert(page >= 0 && offset >= 0, $"hp has no slab memory after reconnect ({page}, {offset}).");
            Specification.Assert(mgr.DirtyBitset.IsDirty(slot), $"hp write after reconnect did not mark slot {slot} dirty, so it would never be sent.");

            var other = new SyncVar<int>("Disconnect_Other", 7, mgr);
            Specification.Assert(other.LocalSlot != slot, $"A new SyncVar was handed hp's slot {slot}.");
            Specification.Assert(other.ActiveSlotCoordinates != hp.ActiveSlotCoordinates,
                $"A new SyncVar shares hp's slab memory at {hp.ActiveSlotCoordinates}.");

            manager.Shutdown();
        }
    }
}
