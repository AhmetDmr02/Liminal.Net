using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SyncVarAuthorityConsistencyTests
    {
        private static int _port = 25300;
        private const int Timeout = 3000;

        private static LiminalNetworkConfig Config(int port) => new LiminalNetworkConfig
        {
            Default_Host = "127.0.0.1",
            Default_Port = port,
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver(),
            HandshakeTimeout = 15,
            ConnectionTimeout = 15
        };

        private static (LiminalNetworkManager server, LiminalNetworkManager client) Connect()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            client.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected && client.localID != 0 && server.SessionManager.HasSession(client.localID), Timeout), Is.True);
            return (server, client);
        }

        private static (SyncVar<int> serverVar, SyncVar<int> clientVar) CreateClientOwnedVar(
            LiminalNetworkManager server, LiminalNetworkManager client, string token, int initial)
        {
            var serverVar = new SyncVar<int>(token, initial, server.SyncVarManager);
            var clientVar = new SyncVar<int>(token, 0, client.SyncVarManager);
            serverVar.AddAuthority(client.localID);
            Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority && clientVar.Value == initial, Timeout), Is.True,
                "Precondition: client never received the value and authority.");
            return (serverVar, clientVar);
        }

        //Makes the client think it has authority like a stale grant would
        private static void InjectLocalGrant(LiminalNetworkManager client, string token)
        {
            client.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                ILiminalTransport.SERVER_ID,
                MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket { Token = token, AuthIds = new[] { client.localID } }));
        }

        private static void SendRawClientWrite(LiminalNetworkManager client, string token, uint version, int value)
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(buffer);
            writer.WriteArrayHeader(1);
            var tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
            writer.WriteUInt8((byte)tokenBytes.Length);
            writer.WriteRaw(tokenBytes);
            writer.WriteUInt32(version);
            var valueBuffer = new ArrayBufferWriter<byte>();
            MessagePackSerializer.Serialize(valueBuffer, value);
            writer.WriteInt32(valueBuffer.WrittenCount);
            writer.WriteRaw(valueBuffer.WrittenSpan);
            writer.Flush();
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarSlabClientRequestPacket(buffer.WrittenMemory.ToArray()));
        }

        [Test]
        public void ClientWriteToUnknownToken_ServerRevokesClientAuthority()
        {
            var (server, client) = Connect();
            try
            {
                var ghost = new SyncVar<int>($"auth_ghost_{client.localID}_{_port}", 0, client.SyncVarManager);
                InjectLocalGrant(client, ghost.Token);
                Assert.That(ghost.HasAuthority, Is.True, "Precondition: client must believe it has authority.");

                ghost.Value = 7;

                Assert.That(SpinWait.SpinUntil(() => !ghost.HasAuthority, Timeout), Is.True,
                    "Server dropped a write to an unknown SyncVar without telling the client.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        [Test]
        public void UnauthorizedClientWrite_ServerRestoresAuthorityAndValue()
        {
            var (server, client) = Connect();
            try
            {
                string token = $"auth_unauthorized_{_port}";
                var serverVar = new SyncVar<int>(token, 5, server.SyncVarManager);
                var clientVar = new SyncVar<int>(token, 0, client.SyncVarManager);
                Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 5, Timeout), Is.True);

                InjectLocalGrant(client, token);
                clientVar.Value = 777;

                Assert.That(SpinWait.SpinUntil(() => !clientVar.HasAuthority && clientVar.Value == 5, Timeout), Is.True,
                    $"Client kept authority={clientVar.HasAuthority} value={clientVar.Value} after the server rejected its write.");
                Assert.That(serverVar.Value, Is.EqualTo(5));
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        [Test]
        public void UnregisteringServerSyncVar_RevokesClientAuthority_AndUseRegrantsIt()
        {
            var (server, client) = Connect();
            try
            {
                var (serverVar, clientVar) = CreateClientOwnedVar(server, client, $"auth_unregister_{_port}", 3);

                server.SyncVarManager.UnregisterSyncVar(serverVar);
                Assert.That(SpinWait.SpinUntil(() => !clientVar.HasAuthority, Timeout), Is.True,
                    "Client kept authority over a SyncVar the server unregistered.");

                serverVar.Value = 9;
                Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority && clientVar.Value == 9, Timeout), Is.True,
                    $"After the server used the SyncVar again the client has authority={clientVar.HasAuthority} value={clientVar.Value}.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        //Server replaces the syncvar with a fresh one (version restarts), client's higher local version cant keep the old value or push it over the new one
        [Test]
        public void RecreatedServerSyncVar_RegrantOverwritesClientValueAndVersion()
        {
            var (server, client) = Connect();
            try
            {
                string token = $"auth_recreated_{_port}";
                var (oldServerVar, clientVar) = CreateClientOwnedVar(server, client, token, 1);
                for (int i = 2; i <= 20; i++) clientVar.Value = i;
                Assert.That(SpinWait.SpinUntil(() => oldServerVar.Value == 20, Timeout), Is.True);

                server.SyncVarManager.UnregisterSyncVar(oldServerVar);
                var freshServerVar = new SyncVar<int>(token, 1000, server.SyncVarManager);

                var valueWhenGranted = new ConcurrentQueue<int>();
                clientVar.OnAuthorityChanged += granted => { if (granted) valueWhenGranted.Enqueue(clientVar.Value); };
                freshServerVar.AddAuthority(client.localID);

                Assert.That(SpinWait.SpinUntil(() => clientVar.HasAuthority && clientVar.Value == 1000, Timeout), Is.True,
                    $"Client has authority={clientVar.HasAuthority} value={clientVar.Value}; expected the server's 1000.");
                Thread.Sleep(200);
                Assert.That(freshServerVar.Value, Is.EqualTo(1000), "The client pushed its stale value over the new server SyncVar.");
                Assert.That(valueWhenGranted, Does.Contain(1000), "OnAuthorityChanged(true) fired before the server value arrived.");

                clientVar.Value = 1001;
                Assert.That(SpinWait.SpinUntil(() => freshServerVar.Value == 1001, Timeout), Is.True,
                    "The client's write after the regrant did not reach the server.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        [Test]
        public void ClientSendingMaxVersion_DoesNotFreezeServerWrites()
        {
            var (server, client) = Connect();
            try
            {
                string token = $"auth_maxversion_{_port}";
                var (serverVar, clientVar) = CreateClientOwnedVar(server, client, token, 1);

                SendRawClientWrite(client, token, uint.MaxValue, 66);
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 66, Timeout), Is.True, "Precondition: authorized write not applied.");

                serverVar.Value = 42;
                Assert.That(serverVar.Value, Is.EqualTo(42), "A client-chosen version froze the server's own writes.");
                Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 42, Timeout), Is.True,
                    $"The server's write never reached the client (client value {clientVar.Value}).");

                clientVar.Value = 43;
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 43, Timeout), Is.True,
                    "The client's normal writes stopped being accepted.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }
    }
}
