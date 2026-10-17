using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Collections.Concurrent;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //Shutdown racing the teardown of a session that is already ending, and the server's last grants racing that teardown
    public static class SessionEndCoyoteTests
    {
        private static LiminalNetworkManager CreateClient(out MockTransport transport)
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            transport = new MockTransport { IsServer = false };
            return new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
        }

        private static void Connect(LiminalNetworkManager manager, MockTransport transport)
        {
            manager.StartClient("127.0.0.1", 7777);
            transport.TriggerLocalClientConnected(transport.LocalClientId);
            Specification.Assert(manager.IsConnected, "Mock client did not connect.");
        }

        private static LiminalNetworkManager StartClient(out MockTransport transport)
        {
            var manager = CreateClient(out transport);
            Connect(manager, transport);
            return manager;
        }

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        private static Task RunCapturing(Action body, Action<Exception> onError) => Task.Run(() =>
        {
            try { body(); }
            catch (Exception ex) { onError(ex); }
        });

        private static ConcurrentQueue<NetworkLifecycleState> RecordStates(LiminalNetworkManager manager)
        {
            var states = new ConcurrentQueue<NetworkLifecycleState>();
            manager.OnLifecycleStateChanged += states.Enqueue;
            return states;
        }

        private static void AssertShutDown(LiminalNetworkManager manager, ConcurrentQueue<NetworkLifecycleState> states)
        {
            Specification.Assert(manager.SyncVarManager.IsTornDown, "Shutdown did not tear down the SyncVarManager.");
            Specification.Assert(manager.Role == NetworkRole.None, $"Role is {manager.Role} after Shutdown.");
            Specification.Assert(manager.LifecycleState == NetworkLifecycleState.Stopped, $"State is {manager.LifecycleState} after Shutdown.");
            Specification.Assert(!manager.StartClient("127.0.0.1", 7777), "The manager started again, Shutdown was dropped.");

            //Every teardown is Stopping then Stopped, one never runs nested inside another
            var seen = states.ToArray();
            bool stopping = false;
            for (int i = 0; i < seen.Length; i++)
            {
                if (seen[i] == NetworkLifecycleState.Stopping)
                {
                    Specification.Assert(!stopping, $"A teardown started inside another: {string.Join(", ", seen)}");
                    stopping = true;
                }
                else if (seen[i] == NetworkLifecycleState.Stopped)
                {
                    stopping = false;
                }
            }
            Specification.Assert(!stopping, $"Lifecycle ended without Stopped: {string.Join(", ", seen)}");
        }

        private static void DispatchGrant(LiminalNetworkManager client, string token, ushort clientId)
        {
            client.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                ILiminalTransport.SERVER_ID,
                MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket { Token = token, AuthIds = new[] { clientId } }));
        }

        //Server dropped us, the game shuts down while the transport is still tearing the session down
        [Test]
        public static async Task Coyote_Shutdown_VsTransportTeardownAfterDrop_ManagerEndsShutDown()
        {
            Exception failure = null;
            var manager = StartClient(out var transport);
            var states = RecordStates(manager);
            transport.TriggerLocalClientDisconnected(transport.LocalClientId);

            var teardown = RunCapturing(() => transport.Shutdown(), ex => failure = ex);
            var shutdown = RunCapturing(() => manager.Shutdown(), ex => failure = ex);
            await Task.WhenAll(teardown, shutdown);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertShutDown(manager, states);
        }

        [Test]
        public static async Task Coyote_ShutdownFromDisconnectHandler_VsTransportTeardown_ManagerEndsShutDown()
        {
            Exception failure = null;
            var manager = StartClient(out var transport);
            var states = RecordStates(manager);
            manager.OnLocalClientDisconnected += _ => manager.Shutdown();

            var drop = RunCapturing(() => transport.TriggerLocalClientDisconnected(transport.LocalClientId), ex => failure = ex);
            var teardown = RunCapturing(() => transport.Shutdown(), ex => failure = ex);
            await Task.WhenAll(drop, teardown);

            //A drop the transport reports itself is torn down by the next tick, the disconnect handler runs there
            manager.Ticker.TickOnce();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertShutDown(manager, states);
        }

        [Test]
        public static async Task Coyote_Shutdown_VsDisconnect_ManagerEndsShutDown()
        {
            Exception failure = null;
            var manager = StartClient(out _);
            var states = RecordStates(manager);

            var disconnect = RunCapturing(() => manager.Disconnect(), ex => failure = ex);
            var shutdown = RunCapturing(() => manager.Shutdown(), ex => failure = ex);
            await Task.WhenAll(disconnect, shutdown);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertShutDown(manager, states);
        }

        //Either the start loses and is refused, or it wins and the shutdown that follows stops it
        [Test]
        public static async Task Coyote_Shutdown_VsStart_ManagerEndsShutDown()
        {
            Exception failure = null;
            var manager = CreateClient(out var transport);
            var states = RecordStates(manager);

            var start = RunCapturing(() =>
            {
                if (manager.StartClient("127.0.0.1", 7777)) transport.TriggerLocalClientConnected(transport.LocalClientId);
            }, ex => failure = ex);
            var shutdown = RunCapturing(() => manager.Shutdown(), ex => failure = ex);
            await Task.WhenAll(start, shutdown);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            AssertShutDown(manager, states);
        }

        //Transport reports the connection while the game shuts down, a connect landing during or after the shutdown must not bring the state back
        [Test]
        public static async Task Coyote_ConnectedEvent_VsShutdown_StateNeverComesBack()
        {
            Exception failure = null;
            var manager = CreateClient(out var transport);
            var states = RecordStates(manager);
            Specification.Assert(manager.StartClient("127.0.0.1", 7777), "Precondition: the start should succeed.");

            var connected = RunCapturing(() => transport.TriggerLocalClientConnected(transport.LocalClientId), ex => failure = ex);
            var shutdown = RunCapturing(() => manager.Shutdown(), ex => failure = ex);
            await Task.WhenAll(connected, shutdown);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            var seen = states.ToArray();
            int lastStopped = Array.LastIndexOf(seen, NetworkLifecycleState.Stopped);
            Specification.Assert(lastStopped == seen.Length - 1, $"Something was raised after the shutdown finished: {string.Join(", ", seen)}");
            AssertShutDown(manager, states);
        }

        //A grant the tick thread is still dispatching from the ended session must not survive into the next one
        [Test]
        public static async Task Coyote_LateGrantVsSessionEnd_NoAuthorityInNextSession()
        {
            Exception failure = null;
            var manager = StartClient(out var transport);
            var hp = new SyncVar<int>(UniqueToken("late_grant"), 1, manager.SyncVarManager);

            var grant = RunCapturing(() => DispatchGrant(manager, hp.Token, transport.LocalClientId), ex => failure = ex);
            var disconnect = RunCapturing(() => manager.Disconnect(), ex => failure = ex);
            await Task.WhenAll(grant, disconnect);
            Connect(manager, transport);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!hp.HasAuthority, "A grant from the ended session gave authority in the next one.");

            DispatchGrant(manager, hp.Token, transport.LocalClientId);
            Specification.Assert(hp.HasAuthority, "This session's grant was not applied.");

            manager.Shutdown();
        }

        //A packet that already passed the connected check when the session reset ran, the apply itself has to notice the session ended
        [Test]
        public static async Task Coyote_GrantPastServerCheckVsSessionReset_NoAuthorityInNextSession()
        {
            Exception failure = null;
            var manager = StartClient(out var transport);
            var hp = new SyncVar<int>(UniqueToken("grant_past_check"), 1, manager.SyncVarManager);
            var applyRemoteAuth = typeof(SyncVarManager).GetMethod("ApplyRemoteAuth", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var syncVarManager = manager.SyncVarManager;

            var grant = RunCapturing(() => applyRemoteAuth.Invoke(syncVarManager, new object[] { hp.Token, new[] { transport.LocalClientId } }), ex => failure = ex);
            var detach = RunCapturing(() => syncVarManager.DetachFromManager(), ex => failure = ex);
            await Task.WhenAll(grant, detach);
            manager.Disconnect();
            Connect(manager, transport);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!hp.HasAuthority, "A grant applied after the session reset gave authority in the next session.");

            manager.Shutdown();
        }

        //Same race for a token nothing has bound yet, the grant would wait in the unbound cache for the next session's SyncVar
        [Test]
        public static async Task Coyote_LateUnboundGrantVsSessionEnd_NoAuthorityInNextSession()
        {
            Exception failure = null;
            var manager = StartClient(out var transport);
            string token = UniqueToken("late_unbound_grant");

            var grant = RunCapturing(() => DispatchGrant(manager, token, transport.LocalClientId), ex => failure = ex);
            var disconnect = RunCapturing(() => manager.Disconnect(), ex => failure = ex);
            await Task.WhenAll(grant, disconnect);
            Connect(manager, transport);

            var hp = new SyncVar<int>(token, 1, manager.SyncVarManager);
            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!hp.HasAuthority, "A cached grant from the ended session gave authority in the next one.");

            manager.Shutdown();
        }
    }
}
