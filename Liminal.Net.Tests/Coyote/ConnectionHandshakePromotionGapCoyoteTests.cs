using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class ConnectionHandshakePromotionGapCoyoteTests
    {
        private static int _portCounter = 8900;

        private static LiminalNetworkConfig CreateConfig(int port)
        {
            return new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 100,
                MaxConnectionCount = 10,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
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

            int batchLen = 4 + 2 + payload.Length;
            byte[] batch = new byte[batchLen];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), chatPacketId);
            payload.CopyTo(batch.AsSpan(6));

            byte[] frame = new byte[LiminalTransportHeader.BaseHeaderSize + batchLen];
            frame[0] = (byte)TransportFlags.Reliable;
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(1, 4), batchLen);
            batch.CopyTo(frame.AsSpan(LiminalTransportHeader.BaseHeaderSize));

            return frame;
        }

        [CoyoteTest]
        public static async Task Coyote_PromoteClient_SessionMustExistBeforeReceiveLoopReadsInboundData()
        {
            int port = Interlocked.Increment(ref _portCounter);
            var config = CreateConfig(port);
            var transport = new TcpTransport();
            var server = new LiminalNetworkManager(transport, config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int dynamicPort = ((IPEndPoint)listener.LocalEndpoint).Port;

            var clientSocket = new TcpClient();
            clientSocket.Connect(IPAddress.Loopback, dynamicPort);
            var serverSocket = listener.AcceptTcpClient();
            listener.Stop();

            const ushort targetId = 42;
            bool packetDelivered = false;

            server.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                if (pkt.Message == "InboundPreBuffered")
                {
                    packetDelivered = true;
                }
            }, "TestTag");

            try
            {
                byte[] framedPacket = CreateFramedChatBatch("InboundPreBuffered");
                await clientSocket.GetStream().WriteAsync(framedPacket, 0, framedPacket.Length);
                await clientSocket.GetStream().FlushAsync();

                var promoteTask = Task.Run(async () =>
                {
                    await Task.Yield();
                    transport.PromoteClient(targetId, serverSocket);
                });

                var checkTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 50; i++)
                    {
                        server.SessionManager.Poll();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(promoteTask, checkTask);

                for (int i = 0; i < 200 && !packetDelivered; i++)
                {
                    server.SessionManager.Poll();
                    await Task.Delay(5);
                }

                Specification.Assert(server.SessionManager.HasSession(targetId),
                    $"SessionManager must contain active session for client {targetId} after PromoteClient.");

                Specification.Assert(packetDelivered,
                    "RACE CONDITION IN PROMOTECLIENT: Inbound packet was dropped by SessionManager.ProcessIncoming " +
                    "because ReceiveLoop started reading data BEFORE OnClientConnected added the session to SessionManager!");
            }
            finally
            {
                server.Shutdown();
                try { clientSocket.Close(); } catch { }
                try { serverSocket.Close(); } catch { }
            }
        }

        [CoyoteTest]
        public static async Task Coyote_HandshakeGap_ServerSessionMustExistWhenLocalClientConnectedFires()
        {
            int port = Interlocked.Increment(ref _portCounter);
            var serverConfig = CreateConfig(port);
            var clientConfig = CreateConfig(port);

            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);

            server.StartServer("127.0.0.1", port);

            ushort localAssignedId = 0;
            var clientConnectedGate = new TaskCompletionSource<ushort>();

            client.Events.OnLocalClientConnected += id =>
            {
                localAssignedId = id;
                clientConnectedGate.TrySetResult(id);
            };

            try
            {
                client.StartClient("127.0.0.1", port);

                ushort id = await clientConnectedGate.Task;

                bool serverHasSession = server.SessionManager.HasSession(id);

                Specification.Assert(serverHasSession,
                    $"HANDSHAKE GAP DETECTED: Client fired OnLocalClientConnected for assigned ID {id}, " +
                    $"but server.SessionManager does not have an active session for {id} (HasSession == false)! " +
                    $"Any packet sent from server to client right now will be dropped with 'Target {id} does not exist'.");
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        [CoyoteTest]
        public static async Task Coyote_HandshakeGap_ServerToClientPacketDroppedIfSentImmediatelyOnLocalConnect()
        {
            int port = Interlocked.Increment(ref _portCounter);
            var serverConfig = CreateConfig(port);
            var clientConfig = CreateConfig(port);

            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);

            server.StartServer("127.0.0.1", port);

            var clientReceivedPacket = new TaskCompletionSource<string>();

            client.Interpreter.Subscribe<ChatPacket>((pkt, _) =>
            {
                clientReceivedPacket.TrySetResult(pkt.Message);
            }, "ClientTag");

            var localConnectedGate = new TaskCompletionSource<ushort>();

            client.Events.OnLocalClientConnected += id =>
            {
                localConnectedGate.TrySetResult(id);
            };

            try
            {
                client.StartClient("127.0.0.1", port);

                ushort clientId = await localConnectedGate.Task;

                Specification.Assert(server.SessionManager.HasSession(clientId),
                    $"Server SessionManager must have session for {clientId} when OnLocalClientConnected fires.");

                server.Interpreter.SendCommand(clientId, new ChatPacket { Message = "ServerToClientImmediate" });
                server.SessionManager.Flush();

                for (int i = 0; i < 500 && !clientReceivedPacket.Task.IsCompleted; i++)
                {
                    client.SessionManager.Poll();
                    server.SessionManager.Poll();
                    await Task.Yield();
                }

                if (!clientReceivedPacket.Task.IsCompleted)
                {
                    var deadline = DateTime.UtcNow.AddSeconds(3);
                    while (!clientReceivedPacket.Task.IsCompleted && DateTime.UtcNow < deadline)
                    {
                        client.SessionManager.Poll();
                        server.SessionManager.Poll();
                        await Task.Delay(10);
                    }
                }

                Specification.Assert(clientReceivedPacket.Task.IsCompleted,
                    $"Packet 'ServerToClientImmediate' was never received by client {clientId} within timeout.");

                string receivedMsg = await clientReceivedPacket.Task;
                Specification.Assert(receivedMsg == "ServerToClientImmediate",
                    $"Expected 'ServerToClientImmediate' but received '{receivedMsg}'");
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }
    }
}