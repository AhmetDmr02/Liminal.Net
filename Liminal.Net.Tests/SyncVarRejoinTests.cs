using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.SyncVar;
using Liminal.Net.Transports;
using NUnit.Framework;
using System.Threading;

namespace Liminal.Net.Tests
{
    //Host ends the session, players rejoin without restarting the game and their objects bind the same tokens again
    [TestFixture]
    public class SyncVarRejoinTests
    {
        private static int _port = 28300;

        private static LiminalNetworkConfig Config(int port) => new LiminalNetworkConfig
        {
            Default_Host = "127.0.0.1",
            Default_Port = port,
            TickRate = 60,
            ClientIdResolver = new BaseResolver(),
            HandshakeTimeout = 15,
            ConnectionTimeout = 15
        };

        private static bool IsRegisteredIn(LiminalNetworkManager manager, SyncVar<int> syncVar)
            => manager.SyncVarManager.TryGetSyncVar(syncVar.Token, out var registered) && ReferenceEquals(registered, syncVar);

        private static void Connect(LiminalNetworkManager server, LiminalNetworkManager client, int port)
        {
            if (!server.IsServer) Assert.That(server.StartServer("127.0.0.1", port), Is.True);
            Assert.That(client.StartClient("127.0.0.1", port), Is.True);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected && server.SessionManager.HasSession(client.localID), 3000), Is.True);
        }

        private static void EndSession(LiminalNetworkManager server, LiminalNetworkManager client)
        {
            server.Disconnect();
            Assert.That(SpinWait.SpinUntil(() => !client.IsConnected, 3000), Is.True);
            client.Disconnect();
        }

        //Same as the game: server binds and grants the owner, owner binds its side and writes
        private static SyncVar<int> SpawnPlayer(LiminalNetworkManager server, LiminalNetworkManager client, string token, int write, out SyncVar<int> clientVar)
        {
            var serverVar = new SyncVar<int>(token, 0, server.SyncVarManager);
            serverVar.SetAuthIds(client.localID);
            clientVar = new SyncVar<int>(token, 0, client.SyncVarManager);

            var owned = clientVar;
            Assert.That(SpinWait.SpinUntil(() => owned.HasAuthority, 3000), Is.True, $"'{token}': the owner never got authority.");
            clientVar.Value = write;
            Assert.That(SpinWait.SpinUntil(() => serverVar.Value == write, 3000), Is.True, $"'{token}': the owner's write never reached the server.");
            return serverVar;
        }

        [Test]
        public void PlayerRebindsAfterHostEndsSession_SameManagers()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            try
            {
                SyncVar<int> previousServerVar = null, previousClientVar = null;
                for (int session = 1; session <= 3; session++)
                {
                    Connect(server, client, port);
                    var serverVar = SpawnPlayer(server, client, "rejoin_tcp_P", session * 10, out var clientVar);

                    if (previousServerVar != null)
                    {
                        Assert.That(IsRegisteredIn(server, previousServerVar), Is.False, $"Session {session}: the dead player's server SyncVar is still registered.");
                        Assert.That(IsRegisteredIn(client, previousClientVar), Is.False, $"Session {session}: the dead player's client SyncVar is still registered.");
                    }
                    previousServerVar = serverVar;
                    previousClientVar = clientVar;
                    EndSession(server, client);
                }
            }
            finally
            {
                client.Dispose();
                server.Dispose();
            }
        }

        //Game spawns the player after this session's server state already arrived, so that state first lands on last session's leftover
        [Test]
        public void PlayerBindsAfterServerStateArrived_GetsAuthorityAndValue()
        {
            int port = Interlocked.Increment(ref _port);
            const string token = "rejoin_tcp_late_bind";
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            try
            {
                Connect(server, client, port);
                SpawnPlayer(server, client, token, 11, out var leftover);
                EndSession(server, client);

                Connect(server, client, port);
                var serverVar = new SyncVar<int>(token, 77, server.SyncVarManager);
                serverVar.SetAuthIds(client.localID);
                Assert.That(SpinWait.SpinUntil(() => leftover.HasAuthority && leftover.Value == 77, 3000), Is.True,
                    "Precondition: this session's state should reach the client before the game binds.");

                var fresh = new SyncVar<int>(token, 0, client.SyncVarManager);
                Assert.That(SpinWait.SpinUntil(() => fresh.HasAuthority && fresh.Value == 77, 3000), Is.True,
                    $"The newly bound SyncVar never got this session's state: authority={fresh.HasAuthority} value={fresh.Value}.");

                fresh.Value = 88;
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 88, 3000), Is.True, "The new SyncVar's write never reached the server.");
            }
            finally
            {
                client.Dispose();
                server.Dispose();
            }
        }

        //Game reuses its SyncVars with Bind and grants again on every join, the server already lists the owner from last session
        [Test]
        public void BindReusedAcrossSessions_RegrantOnJoin_OwnerStillGetsAuthority()
        {
            int port = Interlocked.Increment(ref _port);
            const string token = "rejoin_tcp_bind_at";
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            try
            {
                for (int session = 1; session <= 3; session++)
                {
                    Connect(server, client, port);
                    var serverVar = server.SyncVarManager.Bind<int>(token, 0);
                    serverVar.AddAuthority(client.localID);
                    var clientVar = client.SyncVarManager.Bind<int>(token, 0);

                    Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority, 3000), Is.True, $"Session {session}: the owner never got authority.");
                    clientVar.Value = session * 10;
                    Assert.That(SpinWait.SpinUntil(() => serverVar.Value == session * 10, 3000), Is.True, $"Session {session}: the owner's write never reached the server.");
                    EndSession(server, client);
                }
            }
            finally
            {
                client.Dispose();
                server.Dispose();
            }
        }

        //created -> disconnect -> rejoin on the same manager and rebind -> disconnect -> brand new client manager
        [Test]
        public void ReboundSyncVar_IsAdoptedByBrandNewManager()
        {
            int port = Interlocked.Increment(ref _port);
            const string token = "rejoin_tcp_new_manager";
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            LiminalNetworkManager newClient = null;
            try
            {
                Connect(server, client, port);
                SpawnPlayer(server, client, token, 11, out _);
                EndSession(server, client);

                Connect(server, client, port);
                SpawnPlayer(server, client, token, 22, out var rebound);
                EndSession(server, client);
                client.Shutdown();
                newClient = new LiminalNetworkManager(new TcpTransport(), Config(port));
                Connect(server, newClient, port);
                Assert.That(IsRegisteredIn(newClient, rebound), Is.True, "The brand new manager did not adopt the rebound SyncVar.");

                //This session's grant carries the server value, the write has to build on it
                var serverVar = new SyncVar<int>(token, 7, server.SyncVarManager);
                serverVar.SetAuthIds(newClient.localID);
                Assert.That(SpinWait.SpinUntil(() => rebound.HasAuthority && rebound.Value == 7, 3000), Is.True,
                    $"The adopted SyncVar never got this session's grant: authority={rebound.HasAuthority} value={rebound.Value}.");
                rebound.Value = 33;
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 33, 3000), Is.True, "The adopted SyncVar's write never reached the server.");
            }
            finally
            {
                newClient?.Dispose();
                client.Dispose();
                server.Dispose();
            }
        }
    }
}
