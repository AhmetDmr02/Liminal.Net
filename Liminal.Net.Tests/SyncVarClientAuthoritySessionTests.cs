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

        [TestCase("Disconnect")]
        [TestCase("ConnectionLost")]
        public void ClientAuthority_FromLastSession_IsGoneAfterReconnect(string howEnded)
        {
            var manager = CreateClient(out var transport);
            var hp = new SyncVar<int>("client_auth_session_" + howEnded, 1, manager.SyncVarManager);
            Connect(manager, transport);
            GrantFromServer(manager, hp.Token, transport.LocalClientId);
            Assert.That(hp.HasAuthority, Is.True, "Precondition: the grant should give the client authority.");

            try
            {
                if (howEnded == "Disconnect") manager.Disconnect();
                else
                {
                    transport.TriggerLocalClientDisconnected(transport.LocalClientId);
                    transport.Shutdown();
                }
                Connect(manager, transport);

                Assert.That(hp.HasAuthority, Is.False, "The client kept last session's authority before this session's server granted anything.");
            }
            finally { manager.Shutdown(); }
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

        //Server and host authority is the server's own decision and survives a restart
        [Test]
        public void ServerAuthority_SurvivesRestart()
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var server = new LiminalNetworkManager(new MockTransport(), config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            var hp = new SyncVar<int>("server_auth_restart", 1, server.SyncVarManager);
            hp.SetAuthIds(5);
            try
            {
                server.Disconnect();
                server.StartServer("127.0.0.1", 7777);
                Assert.That(((ISyncVarInternal)hp).IsAuthorized(5), Is.True, "A server restart dropped the server's own authority grant.");
            }
            finally { server.Shutdown(); }
        }
    }
}
