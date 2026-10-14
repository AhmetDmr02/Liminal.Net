using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using MessagePack;
using NUnit.Framework;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SecurityAndOrderingRegressionTests
    {
        private static int _port = 24900;

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

        #region Clients cannot use server-to-client SyncVar packets against the server

        private static (LiminalNetworkManager server, LiminalNetworkManager client, SyncVar<int> serverVar, string token) ConnectWithServerOwnedVar()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            client.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected && client.localID != 0 && server.SessionManager.HasSession(client.localID), 3000), Is.True);

            string token = $"server_owned_{port}";
            var serverVar = new SyncVar<int>(token, 5, SyncVarVisibility.Public, server.SyncVarManager);
            var clientVar = client.SyncVarManager.Bind<int>(token);
            Assert.That(SpinWait.SpinUntil(() => clientVar.Value == 5, 3000), Is.True, "Client never synced the server value.");
            Assert.That(serverVar.IsAuthorized(client.localID), Is.False, "Precondition: client must not have authority.");
            return (server, client, serverVar, token);
        }

        [Test]
        public void Client_SendingAuthUpdate_CannotGrantItselfAuthority()
        {
            var (server, client, serverVar, token) = ConnectWithServerOwnedVar();
            try
            {
                ushort me = client.localID;
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarAuthUpdatePacket { Token = token, AuthIds = new[] { me } });
                Assert.That(SpinWait.SpinUntil(() => serverVar.IsAuthorized(me), 1000), Is.False,
                    $"Client {me} granted itself authority on server SyncVar '{token}'.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        [Test]
        public void Client_SendingClear_CannotClearServerValue()
        {
            var (server, client, serverVar, token) = ConnectWithServerOwnedVar();
            try
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarClearPacket(token));
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 0, 1000), Is.False,
                    $"Client cleared server SyncVar '{token}'.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        [Test]
        public void Client_SendingSnapshot_CannotOverwriteServerValue()
        {
            var (server, client, serverVar, token) = ConnectWithServerOwnedVar();
            try
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarSnapshotPacket
                {
                    Entries = new List<SyncVarSnapshotEntry>
                    {
                        new SyncVarSnapshotEntry { Token = token, Version = 999, AuthIds = new ushort[0], Data = MessagePackSerializer.Serialize(666) }
                    }
                });
                Assert.That(SpinWait.SpinUntil(() => serverVar.Value == 666, 1000), Is.False,
                    $"Client overwrote server SyncVar '{token}'.");
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        #endregion

        #region Inbound batch length field cannot overflow the bounds check

        //Two packets, a valid 4 byte one then a header with attacker controlled length
        private static byte[] CraftUnreliableBatch(int secondTotalLen)
        {
            var batch = new byte[8 + 20];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), 4);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), 1);
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(8, 4), secondTotalLen);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(12, 2), 1);
            return batch;
        }

        //Before the fix overflowing length passed the check, server tried renting >2GB and the failure kicked the sender
        //correct check just rejects the length without touching the client
        [TestCase(100_000, TestName = "CraftedLength_TooLargeWithoutOverflow_IsRejectedWithoutKick")]
        [TestCase(2147483640, TestName = "CraftedLength_OverflowingAddition_IsRejectedWithoutKick")]
        public void CraftedLength_IsRejectedWithoutKick(int secondTotalLen)
        {
            var config = Config(0);
            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            manager.StartServer("127.0.0.1", 7777);
            transport.TriggerClientConnected(5);
            try
            {
                transport.TriggerUnreliableReceived(CraftUnreliableBatch(secondTotalLen), 5);
                Assert.That(transport.KickedClients.Contains(5), Is.False,
                    "The crafted length reached the allocation path and the session failed.");
            }
            finally
            {
                manager.Shutdown();
            }
        }

        #endregion

        #region Reliable ordering across fragmented and non-fragmented batches

        [Test]
        public void ReliableOrdering_FragmentedBatchThenSmallBatch_ArrivesInOrderOverTcp()
        {
            int port = Interlocked.Increment(ref _port);
            var serverConfig = Config(port);
            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig, customTicker: new LiminalUnityTicker(serverConfig));
            server.StartServer("127.0.0.1", port);
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            var received = new ConcurrentQueue<int>();
            client.Interpreter.Subscribe<ChatPacket>((p, _) => received.Enqueue(int.Parse(p.Message.Substring(0, 6))), this);
            client.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected && client.localID != 0 && server.SessionManager.HasSession(client.localID), 3000), Is.True);
            ushort id = client.localID;

            const int rounds = 100;
            string padding = new string('x', 2000);
            try
            {
                //Server ticker never ticks so this thread is the only flusher, any reordering is on the receiver
                for (int i = 0; i < rounds; i++)
                {
                    server.Interpreter.SendCommand(id, new ChatPacket { Message = (i * 2).ToString("D6") + padding });
                    server.SessionManager.Flush();
                    server.Interpreter.SendCommand(id, new ChatPacket { Message = (i * 2 + 1).ToString("D6") });
                    server.SessionManager.Flush();
                    int expected = (i + 1) * 2;
                    Assert.That(SpinWait.SpinUntil(() => received.Count >= expected, 3000), Is.True, $"Round {i}: only {received.Count}/{expected} arrived.");
                }

                var order = received.ToArray();
                for (int i = 1; i < order.Length; i++)
                {
                    Assert.That(order[i], Is.GreaterThan(order[i - 1]), $"Out-of-order delivery at index {i}: {order[i - 1]} then {order[i]}.");
                }
            }
            finally { client.Shutdown(); server.Shutdown(); }
        }

        #endregion
    }
}
