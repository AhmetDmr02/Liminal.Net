using Liminal.Net.SyncVar;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SyncVarResyncCoyoteTests
    {
        private const ushort ClientId = InMemoryServerClient.ClientId;

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        private static Task RunCapturing(Action body, Action<Exception> onError) => Task.Run(() =>
        {
            try { body(); }
            catch (Exception ex) { onError(ex); }
        });

        private static InMemoryServerClient Connect()
        {
            var pair = InMemoryServerClient.Connect();
            Specification.Assert(pair.IsConnected, "Mock pair did not connect.");
            return pair;
        }

        //The client tick dispatching this session's state races the game binding the new SyncVar, it has to end up with that state either way
        [Test]
        public static async Task Coyote_Resync_StateDispatchVsRebind_NewSyncVarGetsServerState()
        {
            Exception failure = null;
            var pair = Connect();
            string token = UniqueToken("resync_dispatch_vs_bind");
            var leftover = pair.CreateClientLeftover(token);

            var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
            serverVar.SetAuthIds(ClientId);
            pair.Server.Ticker.TickOnce();
            SyncVar<int> fresh = null;

            var dispatch = RunCapturing(() => pair.Client.Ticker.TickOnce(), ex => failure = ex);
            var bind = RunCapturing(() => { fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager); }, ex => failure = ex);
            await Task.WhenAll(dispatch, bind);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(InMemoryServerClient.IsRegistered(pair.Client, fresh), "The new SyncVar does not own its token.");
            Specification.Assert(fresh.HasAuthority, "The new SyncVar never got this session's authority.");
            Specification.Assert(fresh.Value == 77, $"The new SyncVar has {fresh.Value}, expected the server's 77.");

            pair.Shutdown();
        }

        //Owner already got the grant and writes while the client tick delivers the resync answer, the write must win either way
        [Test]
        public static async Task Coyote_Resync_AnswerVsOwnerWrite_OwnerWriteIsKept()
        {
            Exception failure = null;
            var pair = Connect();
            string token = UniqueToken("resync_answer_vs_write");
            pair.CreateClientLeftover(token);

            var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
            serverVar.SetAuthIds(ClientId);
            pair.Server.Ticker.TickOnce();
            var fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            pair.Client.Ticker.TickOnce();
            pair.Server.Ticker.TickOnce();
            Specification.Assert(fresh.HasAuthority, "Precondition: the grant should reach the new SyncVar first.");

            var write = RunCapturing(() => { fresh.Value = 88; }, ex => failure = ex);
            var answer = RunCapturing(() => pair.Client.Ticker.TickOnce(), ex => failure = ex);
            await Task.WhenAll(write, answer);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(fresh.Value == 88, $"The resync answer erased the owner's write, client has {fresh.Value}.");
            Specification.Assert(serverVar.Value == 88, $"The owner's write never reached the server, server has {serverVar.Value}.");

            pair.Shutdown();
        }

        //Game writes before its grant arrived, the write is either refused or lands after the grant, client and server agree either way
        [Test]
        public static async Task Coyote_Resync_WriteBeforeGrantVsGrantDelivery_ClientAndServerAgree()
        {
            Exception failure = null;
            var pair = Connect();
            string token = UniqueToken("resync_write_before_grant");
            pair.CreateClientLeftover(token);

            var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
            serverVar.SetAuthIds(ClientId);
            pair.Server.Ticker.TickOnce();
            var fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            Specification.Assert(!fresh.HasAuthority, "Precondition: the grant should not be delivered yet.");

            var write = RunCapturing(() => { fresh.Value = 50; }, ex => failure = ex);
            var grant = RunCapturing(() => pair.Client.Ticker.TickOnce(), ex => failure = ex);
            await Task.WhenAll(write, grant);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(fresh.HasAuthority, "The new SyncVar never got its grant.");
            Specification.Assert(fresh.Value == serverVar.Value, $"Client has {fresh.Value} but server has {serverVar.Value}.");
            Specification.Assert(fresh.Value == 77 || fresh.Value == 50, $"Unexpected value {fresh.Value}.");

            pair.Shutdown();
        }

        //A resend answered while the server excludes the client must never leak a value written after the exclusion
        [Test]
        public static async Task Coyote_Resync_RequestVsExclusion_ExcludedClientNeverSeesLaterValue()
        {
            Exception failure = null;
            var pair = Connect();
            string token = UniqueToken("resync_vs_exclusion");
            var serverVar = new SyncVar<int>(token, 1, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            pair.Pump();

            bool sawSecret = false;
            clientVar.OnValueChanged += (_, next) => { if (next == 99) sawSecret = true; };

            var request = RunCapturing(() => pair.RequestResyncFromClient(token), ex => failure = ex);
            var exclude = RunCapturing(() =>
            {
                serverVar.AddExclusion(ClientId);
                serverVar.Value = 99;
            }, ex => failure = ex);
            await Task.WhenAll(request, exclude);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!sawSecret && clientVar.Value != 99, "A resend leaked a value written after the exclusion.");
            Specification.Assert(!clientVar.IsVisible, "A resend made the excluded client see the SyncVar again.");

            pair.Shutdown();
        }

        //A client's request must never bring back a SyncVar the server's game unregistered, and the client has to agree with the server
        [Test]
        public static async Task Coyote_Resync_RequestVsUnregister_ServerSyncVarStaysUnregistered()
        {
            Exception failure = null;
            var pair = Connect();
            string token = UniqueToken("resync_vs_unregister");
            var serverVar = new SyncVar<int>(token, 3, pair.Server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
            serverVar.AddAuthority(ClientId);
            pair.Pump();
            Specification.Assert(clientVar.HasAuthority, "Precondition: the client should own the SyncVar.");

            var request = RunCapturing(() => pair.RequestResyncFromClient(token), ex => failure = ex);
            var unregister = RunCapturing(() => pair.Server.SyncVarManager.UnregisterSyncVar(serverVar), ex => failure = ex);
            await Task.WhenAll(request, unregister);
            pair.Pump();

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(!InMemoryServerClient.IsRegistered(pair.Server, serverVar), "A client's resync request registered a SyncVar the server unregistered.");
            Specification.Assert(!clientVar.HasAuthority, "The client kept authority over a SyncVar the server unregistered.");

            pair.Shutdown();
        }
    }
}
