using Liminal.Net.SyncVar;
using NUnit.Framework;
using System;

namespace Liminal.Net.Tests
{
    //A new SyncVar replacing last session's leftover asks the server again for what already landed on the leftover
    [TestFixture]
    public class SyncVarResyncTests
    {
        private const ushort ClientId = InMemoryServerClient.ClientId;

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        [Test]
        public void NewSyncVar_ReplacingLeftover_GetsStateThatLandedOnIt()
        {
            var pair = InMemoryServerClient.Connect();
            Assert.That(pair.IsConnected, Is.True);
            string token = UniqueToken("resync_state");
            try
            {
                var leftover = pair.CreateClientLeftover(token);
                var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
                serverVar.SetAuthIds(ClientId);
                pair.Pump();
                Assert.That(leftover.HasAuthority && leftover.Value == 77, Is.True, "Precondition: the server state should land on the leftover.");

                var fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
                pair.Pump();

                Assert.That(fresh.HasAuthority, Is.True, "The new SyncVar never got the authority that landed on the leftover.");
                Assert.That(fresh.Value, Is.EqualTo(77), "The new SyncVar never got the value that landed on the leftover.");
                Assert.That(fresh.IsVisible, Is.True);

                fresh.Value = 88;
                pair.Pump();
                Assert.That(serverVar.Value, Is.EqualTo(88));
            }
            finally { pair.Shutdown(); }
        }

        //Grant reached the new SyncVar first, the owner wrote, then the resync answer arrived, the answer must not erase that write
        [Test]
        public void ResyncAnswer_ArrivingAfterOwnerWrote_DoesNotEraseTheWrite()
        {
            var pair = InMemoryServerClient.Connect();
            string token = UniqueToken("resync_after_write");
            try
            {
                pair.CreateClientLeftover(token);
                var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
                serverVar.SetAuthIds(ClientId);
                pair.Server.Ticker.TickOnce();

                var fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
                pair.Client.Ticker.TickOnce();
                Assert.That(fresh.HasAuthority && fresh.Value == 77, Is.True, "Precondition: the grant should reach the new SyncVar before the answer.");

                fresh.Value = 88;
                pair.Server.Ticker.TickOnce();
                pair.Pump();

                Assert.That(fresh.Value, Is.EqualTo(88), "The resync answer overwrote the owner's newer write.");
                Assert.That(serverVar.Value, Is.EqualTo(88), "The owner's write never reached the server.");
            }
            finally { pair.Shutdown(); }
        }

        //Write before the grant is refused without touching the local value, the grant then brings the server value
        [Test]
        public void WriteBeforeGrant_IsRefused_ThenGrantBringsServerValue()
        {
            var pair = InMemoryServerClient.Connect();
            string token = UniqueToken("resync_early_write");
            try
            {
                pair.CreateClientLeftover(token);
                var fresh = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);

                fresh.Value = 50;
                Assert.That(fresh.Value, Is.EqualTo(0), "A refused write still changed the local value.");

                var serverVar = new SyncVar<int>(token, 77, pair.Server.SyncVarManager);
                serverVar.SetAuthIds(ClientId);
                pair.Pump();

                Assert.That(fresh.HasAuthority, Is.True);
                Assert.That(fresh.Value, Is.EqualTo(77));
                Assert.That(serverVar.Value, Is.EqualTo(77), "The refused write reached the server.");

                fresh.Value = 51;
                pair.Pump();
                Assert.That(serverVar.Value, Is.EqualTo(51), "A write after the grant did not reach the server.");
            }
            finally { pair.Shutdown(); }
        }

        [Test]
        public void ResyncRequest_ForSyncVarTheClientCantSee_SendsNothing()
        {
            var pair = InMemoryServerClient.Connect();
            string token = UniqueToken("resync_hidden");
            try
            {
                var serverVar = new SyncVar<int>(token, 5, pair.Server.SyncVarManager);
                serverVar.AddExclusion(ClientId);
                var clientVar = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
                pair.Pump();

                pair.RequestResyncFromClient(token);
                pair.Pump();

                Assert.That(clientVar.IsVisible, Is.False, "A resync request revealed a SyncVar the client is excluded from.");
                Assert.That(clientVar.Value, Is.EqualTo(0), "A resync request leaked the value of a hidden SyncVar.");
            }
            finally { pair.Shutdown(); }
        }

        [Test]
        public void ResyncRequest_NeverRegistersSyncVarTheServerUnregistered()
        {
            var pair = InMemoryServerClient.Connect();
            string token = UniqueToken("resync_unregistered");
            try
            {
                var serverVar = new SyncVar<int>(token, 5, pair.Server.SyncVarManager);
                pair.Pump();
                pair.Server.SyncVarManager.UnregisterSyncVar(serverVar);

                pair.RequestResyncFromClient(token);
                pair.Pump();

                Assert.That(InMemoryServerClient.IsRegistered(pair.Server, serverVar), Is.False, "A client's resync request registered a SyncVar the server unregistered.");
            }
            finally { pair.Shutdown(); }
        }

        //Normal construction with nothing to replace sends no request
        [Test]
        public void NewSyncVar_WithoutLeftover_SendsNoResync()
        {
            var pair = InMemoryServerClient.Connect();
            string token = UniqueToken("resync_none");
            int requests = 0;
            pair.Server.Interpreter.Subscribe<SyncVarResyncRequestPacket>((_, _) => requests++, this);
            try
            {
                _ = new SyncVar<int>(token, 0, pair.Client.SyncVarManager);
                pair.Pump();
                Assert.That(requests, Is.EqualTo(0));

                var leftoverToken = UniqueToken("resync_one");
                pair.CreateClientLeftover(leftoverToken);
                _ = new SyncVar<int>(leftoverToken, 0, pair.Client.SyncVarManager);
                pair.Pump();
                Assert.That(requests, Is.EqualTo(1), "Replacing a leftover should send exactly one resync request.");
            }
            finally { pair.Shutdown(); }
        }
    }
}
