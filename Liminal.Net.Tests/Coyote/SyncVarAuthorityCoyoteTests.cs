using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SyncVarAuthorityCoyoteTests
    {
        private const ushort ClientId = 2;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static void Relay(MockTransport to, byte[] data, ushort from, TransportFlags flags)
        {
            if ((flags & TransportFlags.Fragmented) != 0) to.TriggerFragmentReceived(data, from);
            else if ((flags & TransportFlags.Reliable) != 0) to.TriggerReliableReceived(data, from);
            else to.TriggerUnreliableReceived(data, from);
        }

        //Mock server and client wired together in memory, nothing moves until Pump ticks both
        private sealed class Pair
        {
            public LiminalNetworkManager Server;
            public LiminalNetworkManager Client;

            public static Pair Connect()
            {
                var serverConfig = CreateConfig();
                var clientConfig = CreateConfig();
                var serverTransport = new MockTransport { IsServer = true };
                var clientTransport = new MockTransport { IsServer = false, LocalClientId = ClientId };

                var pair = new Pair
                {
                    Server = new LiminalNetworkManager(serverTransport, serverConfig, customTicker: new LiminalUnityTicker(serverConfig)),
                    Client = new LiminalNetworkManager(clientTransport, clientConfig, customTicker: new LiminalUnityTicker(clientConfig))
                };

                serverTransport.OnSendHook = (data, target, flags) =>
                {
                    if (target == ClientId) Relay(clientTransport, data, ILiminalTransport.SERVER_ID, flags);
                };
                clientTransport.OnSendHook = (data, target, flags) => Relay(serverTransport, data, ClientId, flags);

                pair.Server.StartServer("127.0.0.1", 7777);
                pair.Client.StartClient("127.0.0.1", 7777);
                clientTransport.TriggerLocalClientConnected(ClientId);
                serverTransport.ConnectedClients.Add(ClientId);
                serverTransport.TriggerClientConnected(ClientId);
                pair.Pump();

                Specification.Assert(pair.Client.IsConnected, "Mock client is not connected.");
                Specification.Assert(pair.Server.SessionManager.HasSession(ClientId), "Mock server has no session for the client.");
                return pair;
            }

            public void Pump(int rounds = 4)
            {
                for (int i = 0; i < rounds; i++)
                {
                    Server.Ticker.TickOnce();
                    Client.Ticker.TickOnce();
                }
            }

            public void Shutdown()
            {
                Client.Shutdown();
                Server.Shutdown();
            }
        }

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        private static (SyncVar<int> serverVar, SyncVar<int> clientVar) CreateClientOwnedVar(Pair pair, string token, int initial)
        {
            var serverVar = new SyncVar<int>(token, initial, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            serverVar.AddAuthority(ClientId);
            pair.Pump();
            Specification.Assert(clientVar.HasAuthority && clientVar.Value == initial,
                $"Precondition: client authority={clientVar.HasAuthority} value={clientVar.Value}, expected authority and {initial}.");
            return (serverVar, clientVar);
        }

        private static bool IsRegistered(LiminalNetworkManager manager, ISyncVarInternal syncVar)
            => manager.SyncVarManager.TryGetSyncVar(syncVar.Token, out var registered) && ReferenceEquals(registered, syncVar);

        private static Task RunCapturing(Action body, Action<Exception> onError) => Task.Run(() =>
        {
            try { body(); }
            catch (Exception ex) { onError(ex); }
        });

        //A flush snapshots visibility before it sends, an exclusion and its eviction landing in between must not be undone
        //by that flush's batch reaching the client after the eviction
        [Test]
        public static async Task Coyote_Visibility_FlushVsExclusion_ExcludedClientStaysHidden()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            string token = UniqueToken("visibility_exclude");
            var serverVar = new SyncVar<int>(token, 1, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            pair.Pump();
            Specification.Assert(clientVar.IsVisible && clientVar.Value == 1, "Precondition: client never saw the SyncVar.");

            serverVar.Value = 2;
            var tick = RunCapturing(() => pair.Server.Ticker.TickOnce(), ex => failure = ex);
            var exclude = RunCapturing(() => serverVar.AddExclusion(ClientId), ex => failure = ex);
            await Task.WhenAll(tick, exclude);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!clientVar.IsVisible, "A batch sent after the eviction made the excluded client see the SyncVar again.");

            pair.Shutdown();
        }

        //A value the server writes after excluding a client must never reach that client, even from a flush already under way
        [Test]
        public static async Task Coyote_Visibility_ValueWrittenAfterExclusion_NeverReachesExcludedClient()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            string token = UniqueToken("visibility_secret");
            var serverVar = new SyncVar<int>(token, 1, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            pair.Pump();
            Specification.Assert(clientVar.Value == 1, "Precondition: client never saw the SyncVar.");

            bool sawSecret = false;
            clientVar.OnValueChanged += (_, next) => { if (next == 99) sawSecret = true; };

            serverVar.Value = 2;
            var tick = RunCapturing(() => pair.Server.Ticker.TickOnce(), ex => failure = ex);
            var excludeThenWrite = RunCapturing(() =>
            {
                serverVar.AddExclusion(ClientId);
                serverVar.Value = 99;
            }, ex => failure = ex);
            await Task.WhenAll(tick, excludeThenWrite);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!sawSecret && clientVar.Value != 99, "A value written after the exclusion reached the excluded client.");

            pair.Shutdown();
        }

        //Unregister revokes and using it again regrants, whatever the order the client has to end up matching the server
        [Test]
        public static async Task Coyote_Authority_UnregisterVsUse_ClientMatchesServer()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            var (serverVar, clientVar) = CreateClientOwnedVar(pair, UniqueToken("authority_unregister"), 3);

            var unregister = RunCapturing(() => pair.Server.SyncVarManager.UnregisterSyncVar(serverVar), ex => failure = ex);
            var use = RunCapturing(() => { serverVar.Value = 9; }, ex => failure = ex);
            await Task.WhenAll(unregister, use);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            bool registered = IsRegistered(pair.Server, serverVar);
            Specification.Assert(clientVar.HasAuthority == registered,
                $"Server registered={registered} but client authority={clientVar.HasAuthority}.");
            if (registered)
            {
                Specification.Assert(clientVar.Value == serverVar.Value, $"Client value {clientVar.Value}, server value {serverVar.Value}.");
            }

            pair.Shutdown();
        }

        //Client write racing the revoke either gets applied or answered with server state
        [Test]
        public static async Task Coyote_Authority_ClientWriteVsRevoke_ClientMatchesServer()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            var (serverVar, clientVar) = CreateClientOwnedVar(pair, UniqueToken("authority_revoke"), 3);

            var write = RunCapturing(() =>
            {
                clientVar.Value = 50;
                pair.Client.Ticker.TickOnce();
            }, ex => failure = ex);
            var revoke = RunCapturing(() => serverVar.RemoveAuthority(ClientId), ex => failure = ex);
            await Task.WhenAll(write, revoke);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!clientVar.HasAuthority, "Client kept authority after the server revoked it.");
            Specification.Assert(clientVar.Value == serverVar.Value,
                $"Client value {clientVar.Value}, server value {serverVar.Value}.");

            pair.Shutdown();
        }

        //Server versions client writes so an older write's echo can have the same version as a newer local write
        //if a server write landed in between, the echo cant replace the newer value
        [Test]
        public static async Task Coyote_Authority_EchoDoesNotRevertNewerClientWrite()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            var (serverVar, clientVar) = CreateClientOwnedVar(pair, UniqueToken("authority_echo"), 1);

            var client = RunCapturing(() =>
            {
                clientVar.Value = 10;
                pair.Client.Ticker.TickOnce();
                pair.Server.Ticker.TickOnce();
                clientVar.Value = 20;
                pair.Client.Ticker.TickOnce();
            }, ex => failure = ex);
            var server = RunCapturing(() => { serverVar.Value = 5; }, ex => failure = ex);
            await Task.WhenAll(client, server);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(serverVar.Value == 20 && clientVar.Value == 20,
                $"The client's last write was lost: client={clientVar.Value} server={serverVar.Value}, expected 20.");

            pair.Shutdown();
        }

        //Grant racing server writes gives the client server value and version, client continues from there and server takes its next write
        [Test]
        public static async Task Coyote_Authority_GrantVsServerWrites_ClientContinuesFromServerState()
        {
            Exception failure = null;
            var pair = Pair.Connect();
            string token = UniqueToken("authority_grant");
            var serverVar = new SyncVar<int>(token, 1, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            pair.Pump();

            var grant = RunCapturing(() => serverVar.AddAuthority(ClientId), ex => failure = ex);
            var writes = RunCapturing(() =>
            {
                for (int i = 2; i <= 3; i++) serverVar.Value = i;
            }, ex => failure = ex);
            await Task.WhenAll(grant, writes);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(clientVar.HasAuthority, "Client never received authority.");
            Specification.Assert(clientVar.Value == 3 && serverVar.Value == 3,
                $"After the grant client={clientVar.Value} server={serverVar.Value}, expected 3.");

            clientVar.Value = 100;
            pair.Pump();
            Specification.Assert(serverVar.Value == 100, $"Server value {serverVar.Value} after the client's write, expected 100.");
            Specification.Assert(clientVar.Value == 100, $"Client value {clientVar.Value} after the echo, expected 100.");

            pair.Shutdown();
        }
    }
}
