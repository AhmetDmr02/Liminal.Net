using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using MessagePack;
using NUnit.Framework;

namespace Liminal.Net.Tests
{
    //Client authority is granted by the server per session, a reused SyncVar must not carry it into the next session
    [TestFixture]
    public class SyncVarClientAuthoritySessionTests
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
            Assert.That(manager.IsConnected, Is.True);
        }

        private static void GrantFromServer(LiminalNetworkManager client, string token, params ushort[] authIds)
        {
            client.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                ILiminalTransport.SERVER_ID,
                MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket { Token = token, AuthIds = authIds }));
        }

        //Same manager after Disconnect or a lost connection, or a brand new one after Shutdown, every way a session ends starts the next one clean
        [TestCase("Disconnect")]
        [TestCase("ConnectionLost")]
        [TestCase("NewManagerAfterDisconnect")]
        [TestCase("NewManagerAfterShutdown")]
        public void ClientAuthority_FromLastSession_IsGoneAfterReconnect(string howEnded)
        {
            var manager = CreateClient(out var transport);
            var hp = new SyncVar<int>("client_auth_session_" + howEnded, 1, manager.SyncVarManager);
            Connect(manager, transport);
            GrantFromServer(manager, hp.Token, transport.LocalClientId);
            Assert.That(hp.HasAuthority, Is.True, "Precondition: the grant should give the client authority.");

            var next = manager;
            try
            {
                switch (howEnded)
                {
                    case "Disconnect":
                        manager.Disconnect();
                        break;
                    case "ConnectionLost":
                        transport.TriggerLocalClientDisconnected(transport.LocalClientId);
                        transport.Shutdown();
                        break;
                    case "NewManagerAfterDisconnect":
                        manager.Disconnect();
                        manager.Shutdown();
                        next = CreateClient(out transport);
                        break;
                    case "NewManagerAfterShutdown":
                        manager.Shutdown();
                        next = CreateClient(out transport);
                        break;
                }
                Connect(next, transport);

                Assert.That(next.SyncVarManager.TryGetSyncVar(hp.Token, out var registered) && ReferenceEquals(registered, hp), Is.True,
                    "The SyncVar is not registered in the next session, the check below would prove nothing.");
                Assert.That(hp.AuthIds, Is.Empty, $"Last session's authority survived: [{string.Join(", ", hp.AuthIds)}].");
                Assert.That(hp.HasAuthority, Is.False, "The client kept last session's authority before this session's server granted anything.");
            }
            finally
            {
                next.Shutdown();
                manager.Shutdown();
            }
        }

        //Cleanup happens when a session ends, setup done after that and before the next start is for the next session and stays
        [Test]
        public void ServerPermissions_SetBetweenSessions_CarryIntoNextSession()
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var server = new LiminalNetworkManager(new MockTransport(), config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            var hp = new SyncVar<int>("server_perms_between_sessions", 1, server.SyncVarManager);
            hp.AddAuthority(2);
            try
            {
                server.Disconnect();
                Assert.That(hp.AuthIds, Is.Empty, "Disconnect did not clear the session's authority.");

                hp.AddAuthority(3);
                server.StartServer("127.0.0.1", 7777);
                Assert.That(hp.AuthIds, Is.EqualTo(new ushort[] { 3 }), "Authority set up for the next session was lost on start.");
            }
            finally { server.Shutdown(); }
        }

        [Test]
        public void GrantInNextSession_RaisesOnAuthorityChanged()
        {
            var manager = CreateClient(out var transport);
            var hp = new SyncVar<int>("client_auth_regrant", 1, manager.SyncVarManager);
            Connect(manager, transport);
            GrantFromServer(manager, hp.Token, transport.LocalClientId);
            manager.Disconnect();
            Connect(manager, transport);

            int granted = 0;
            hp.OnAuthorityChanged += has => { if (has) granted++; };
            try
            {
                GrantFromServer(manager, hp.Token, transport.LocalClientId);
                Assert.That(hp.HasAuthority, Is.True);
                Assert.That(granted, Is.EqualTo(1), "The new session's grant did not raise OnAuthorityChanged(true).");
            }
            finally { manager.Shutdown(); }
        }

        //Client ids start over every session so id based permissions from the last one would land on whoever gets that id next
        [TestCase("Disconnect")]
        [TestCase("NewManager")]
        [TestCase("NewManagerAfterShutdown")]
        public void ServerPermissions_AreClearedWhenSessionEnds_VisibilityModeStays(string how)
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var server = new LiminalNetworkManager(new MockTransport(), config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            var hp = new SyncVar<int>("server_perms_session_" + how, 1, SyncVarVisibility.ManualObservers, server.SyncVarManager);
            hp.SetAuthIds(5);
            hp.AddObserver(6);
            hp.AddExclusion(7);
            Assert.That(hp.AuthIds, Is.EqualTo(new ushort[] { 5 }), "Precondition: the grant should be set.");

            var next = server;
            try
            {
                if (how != "NewManagerAfterShutdown") server.Disconnect();
                if (how == "Disconnect") server.StartServer("127.0.0.1", 7777);
                else
                {
                    server.Shutdown();
                    next = new LiminalNetworkManager(new MockTransport(), config, customTicker: new LiminalUnityTicker(config));
                    next.StartServer("127.0.0.1", 7777);
                }

                var state = ((ISyncVarInternal)hp).VisibilityState;
                Assert.That(hp.AuthIds, Is.Empty, "Last session's authority grant survived into the next session.");
                Assert.That(state.Observers, Is.Empty, "Last session's observers survived into the next session.");
                Assert.That(state.Exclusions, Is.Empty, "Last session's exclusions survived into the next session.");
                Assert.That(hp.Visibility, Is.EqualTo(SyncVarVisibility.ManualObservers), "The visibility mode is a setting, not a client id, it has to stay.");
            }
            finally
            {
                next.Shutdown();
                server.Shutdown();
            }
        }
    }
}
