using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SessionPromotionFailureCleanupTests
    {
        private LiminalNetworkConfig CreateConfig()
        {
            return new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9999,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 100,
                ClientIdResolver = new BaseResolver()
            };
        }

        private static byte[] CreateFramedChatBatch(string message)
        {
            ushort chatPacketId = (ushort)LiminalPacketLibrary.GetId<ChatPacket>();
            if (chatPacketId == 0)
            {
                LiminalPacketLibrary.Initialize();
                chatPacketId = (ushort)LiminalPacketLibrary.GetId<ChatPacket>();
            }

            byte[] payload = MessagePackSerializer.Serialize(new ChatPacket { Message = message });

            // SessionManager batch format: [4 bytes totalLen (payload + 2)] [2 bytes packetId] [payload]
            int batchLen = 4 + 2 + payload.Length;
            byte[] batch = new byte[batchLen];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), chatPacketId);
            payload.CopyTo(batch.AsSpan(6));

            return batch;
        }

        [Test]
        public void SessionManager_OptionC_AllocatesSessionOnDemand_WhenTransportReportsClientConnected()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            const ushort targetClientId = 55;

            // Transport confirms client 55 is connected at transport layer
            transport.IsClientConnectedFunc = id => id == targetClientId;

            var manager = new LiminalNetworkManager(transport, config);

            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.False,
                "Precondition: session should not exist before any interaction.");

            // Attempt to send a packet BEFORE OnClientConnected event fires
            manager.Interpreter.SendCommand(targetClientId, new ChatPacket { Message = "OnDemandOutbound" });

            // Option C: SessionManager must allocate session on-demand rather than dropping
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.True,
                "Option C failed: Session was not allocated on-demand when sending to a connected client.");

            manager.Shutdown();
        }

        [Test]
        public void SessionManager_OptionC_DoesNotAllocateSession_WhenTransportReportsClientNotConnected()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            const ushort targetClientId = 99;

            // Transport confirms client 99 is NOT connected
            transport.IsClientConnectedFunc = id => false;

            var manager = new LiminalNetworkManager(transport, config);

            // Attempt to send a packet to non-connected client
            manager.Interpreter.SendCommand(targetClientId, new ChatPacket { Message = "ShouldBeDropped" });

            // Session must NOT be created
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.False,
                "SessionManager must not allocate a session if transport reports client is not connected.");

            manager.Shutdown();
        }

        [Test]
        public void SessionManager_OptionC_ProcessIncoming_AllocatesSessionOnDemand_WhenTransportConnected()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            const ushort senderClientId = 77;

            transport.IsClientConnectedFunc = id => id == senderClientId;

            var manager = new LiminalNetworkManager(transport, config);

            bool packetReceived = false;
            manager.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "InboundOnDemand")
                    packetReceived = true;
            }, "TestTag");

            Assert.That(manager.SessionManager.HasSession(senderClientId), Is.False);

            // Inbound packet arrives from transport before OnClientConnected was triggered
            byte[] rawBatch = CreateFramedChatBatch("InboundOnDemand");
            transport.TriggerReliableReceived(rawBatch, senderClientId);

            // Option C must have allocated session on demand to ingest the packet
            Assert.That(manager.SessionManager.HasSession(senderClientId), Is.True,
                "Option C failed: Inbound packet did not trigger on-demand session creation.");

            // Poll to process queue
            manager.SessionManager.Poll();
            Assert.That(packetReceived, Is.True, "Packet should have been delivered through on-demand session.");

            manager.Shutdown();
        }

        [Test]
        public void SessionManager_PromotionFailure_DisconnectCleansUpSessionAndBuffers()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            const ushort targetClientId = 88;

            transport.IsClientConnectedFunc = id => id == targetClientId;

            var manager = new LiminalNetworkManager(transport, config);

            // Allocate session on demand
            manager.Interpreter.SendCommand(targetClientId, new ChatPacket { Message = "Test" });
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.True);

            // Simulate disconnect / promotion failure
            transport.TriggerClientDisconnected(targetClientId);

            // Session must immediately be removed from active sessions
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.False,
                "SessionManager.HasSession must be false immediately after disconnect.");

            // Drain pending disconnects
            manager.SessionManager.ProcessPendingDisconnects();

            Span<ushort> remainingSessions = stackalloc ushort[10];
            int count = manager.SessionManager.GetSessionIds(remainingSessions);
            Assert.That(count, Is.EqualTo(0), "No active sessions should remain after disconnect cleanup.");

            manager.Shutdown();
        }

        [Test]
        public void SessionManager_PromotionFailure_DisconnectingSessionCannotBeReallocatedOnDemand()
        {
            var config = CreateConfig();
            var transport = new MockTransport();
            const ushort targetClientId = 88;

            transport.IsClientConnectedFunc = id => id == targetClientId;

            var manager = new LiminalNetworkManager(transport, config);

            // Allocate session
            manager.Interpreter.SendCommand(targetClientId, new ChatPacket { Message = "Test" });
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.True);

            // Trigger disconnect -> enters _disconnectingSessions
            transport.TriggerClientDisconnected(targetClientId);
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.False);

            // Try to send while disconnect is pending
            manager.Interpreter.SendCommand(targetClientId, new ChatPacket { Message = "AfterDisconnect" });

            // Must NOT re-allocate
            Assert.That(manager.SessionManager.HasSession(targetClientId), Is.False,
                "SessionManager must not resurrect a session that is in disconnecting state.");

            manager.SessionManager.ProcessPendingDisconnects();
            manager.Shutdown();
        }

        [Test]
        public void TcpTransport_PromoteClient_ExceptionDuringOnClientConnected_KicksClientAndCleansUp()
        {
            var config = CreateConfig();
            var transport = new TcpTransport();
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            ushort kickedId = 0;
            transport.OnClientKicked += id => kickedId = id;

            // Make OnClientConnected throw to simulate promotion failure in high-level listener
            transport.OnClientConnected += id => throw new InvalidOperationException("Simulated promotion failure");

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var client = new TcpClient();
            client.Connect(IPAddress.Loopback, port);
            var serverAccepted = listener.AcceptTcpClient();
            listener.Stop();

            const ushort clientId = 42;

            try
            {
                // PromoteClient should catch exception, log error, and kick the client
                transport.PromoteClient(clientId, serverAccepted);

                Assert.That(kickedId, Is.EqualTo(clientId),
                    "PromoteClient must invoke Kick when an exception occurs during promotion.");
                Assert.That(transport.IsClientConnected(clientId), Is.False,
                    "Client socket must be removed from transport when promotion fails.");
            }
            finally
            {
                transport.Shutdown();
                try { client.Close(); } catch { }
                try { serverAccepted.Close(); } catch { }
            }
        }

        [Test]
        public void TcpTransport_PromoteClient_ReplacedSocket_CleansUpOldSocketAndSendState()
        {
            var config = CreateConfig();
            var transport = new TcpTransport();
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var client1 = new TcpClient();
            client1.Connect(IPAddress.Loopback, port);
            var serverAccepted1 = listener.AcceptTcpClient();

            var client2 = new TcpClient();
            client2.Connect(IPAddress.Loopback, port);
            var serverAccepted2 = listener.AcceptTcpClient();
            listener.Stop();

            const ushort clientId = 10;

            try
            {
                // Promote first connection
                transport.PromoteClient(clientId, serverAccepted1);
                Assert.That(transport.IsClientConnected(clientId), Is.True);

                // Promote second connection for same client ID
                transport.PromoteClient(clientId, serverAccepted2);
                Assert.That(transport.IsClientConnected(clientId), Is.True);

                // First connection should be closed/disconnected
                Assert.That(serverAccepted1.Connected, Is.False,
                    "Old server socket must be closed when replaced by new connection.");
            }
            finally
            {
                transport.Shutdown();
                try { client1.Close(); } catch { }
                try { client2.Close(); } catch { }
                try { serverAccepted1.Close(); } catch { }
                try { serverAccepted2.Close(); } catch { }
            }
        }
    }
}
