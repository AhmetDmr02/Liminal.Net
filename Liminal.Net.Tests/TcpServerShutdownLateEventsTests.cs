using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Handshakes;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TcpServerShutdownLateEventsTests
    {
        private static int _port = 25900;

        private static LiminalNetworkConfig Config(int port) => new LiminalNetworkConfig
        {
            Default_Host = "127.0.0.1",
            Default_Port = port,
            TickRate = 60,
            MaxConnectionCount = 16,
            ClientIdResolver = new BaseResolver(),
            HandshakeTimeout = 1,
            ConnectionTimeout = 15
        };

        //Readers of closed sockets wake up a few ms after Shutdown, more clients and rounds make that late wake up near certain
        [Test]
        public void ServerShutdown_WithConnectedClients_NoLateDisconnectsAndCounterBackToZero()
        {
            for (int round = 0; round < 8; round++)
            {
                int port = Interlocked.Increment(ref _port);
                var serverTransport = new TcpTransport();
                var server = new LiminalNetworkManager(serverTransport, Config(port));
                server.StartServer("127.0.0.1", port);

                var clients = new List<LiminalNetworkManager>();
                try
                {
                    for (int i = 0; i < 7; i++)
                    {
                        var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
                        client.StartClient("127.0.0.1", port);
                        clients.Add(client);
                    }
                    Assert.That(SpinWait.SpinUntil(() => serverTransport.TotalConnections == 7, 3000), Is.True, $"Round {round}: 7 clients never connected.");

                    server.Shutdown();

                    int lateDisconnects = 0;
                    serverTransport.OnClientDisconnected += _ => Interlocked.Increment(ref lateDisconnects);
                    Thread.Sleep(300);

                    Assert.That(serverTransport.TotalConnections, Is.EqualTo(0), $"Round {round}: connection counter drifted after shutdown.");
                    Assert.That(Volatile.Read(ref lateDisconnects), Is.EqualTo(0), $"Round {round}: old clients were reported disconnected after Shutdown returned.");
                }
                finally
                {
                    foreach (var client in clients) client.Shutdown();
                    server.Shutdown();
                }
            }
        }

        private static void WriteHandshakePacket<T>(NetworkStream stream, T packet)
        {
            byte[] body = MessagePackSerializer.Serialize(packet);
            byte[] frame = new byte[TcpHandshakePipeline.HeaderSize + body.Length];
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0, 4), TcpHandshakePipeline.HandshakeMagic);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(8, 4), LiminalPacketLibrary.GetId<T>());
            body.CopyTo(frame.AsSpan(TcpHandshakePipeline.HeaderSize));
            stream.Write(frame, 0, frame.Length);
        }

        //Sends a valid hello and never the final ack, the server reserves a slot and waits on this socket
        private static TcpClient ConnectWithoutAck(int port)
        {
            var socket = new TcpClient();
            socket.Connect("127.0.0.1", port);
            WriteHandshakePacket(socket.GetStream(), new ConnectionHandshakePacketClient
            {
                ClientVersion = new LiminalNetworkConfig().Version,
                PacketRegistryHash = LiminalPacketLibrary.RegistryHash
            });
            return socket;
        }

        private static ushort ReadAssignedId(NetworkStream stream)
        {
            byte[] header = new byte[TcpHandshakePipeline.HeaderSize];
            ReadExactly(stream, header);
            byte[] body = new byte[BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4))];
            ReadExactly(stream, body);
            return MessagePackSerializer.Deserialize<ConnectionHandshakePacketServer>(body).AssignedClientID;
        }

        private static void ReadExactly(NetworkStream stream, byte[] buffer)
        {
            int read = 0;
            while (read < buffer.Length)
            {
                int n = stream.Read(buffer, read, buffer.Length - read);
                if (n == 0) throw new System.IO.EndOfStreamException();
                read += n;
            }
        }

        //The ack arrives after Shutdown so registering the client is refused, its reserved slot still has to come back
        [Test]
        public void ServerShutdown_BeforeFinalAck_RefusedClientGivesItsSlotBack()
        {
            int port = Interlocked.Increment(ref _port);
            var serverTransport = new TcpTransport();
            var server = new LiminalNetworkManager(serverTransport, Config(port));
            server.StartServer("127.0.0.1", port);

            TcpClient lateAck = null;
            try
            {
                lateAck = ConnectWithoutAck(port);
                var stream = lateAck.GetStream();
                ushort assignedId = ReadAssignedId(stream);
                Assert.That(serverTransport.TotalConnections, Is.EqualTo(1), "Precondition: handshake slot not reserved.");

                server.Shutdown();
                WriteHandshakePacket(stream, new ConnectionHandshakeClientAck { Ack = true, ClientID = assignedId });

                Assert.That(SpinWait.SpinUntil(() => serverTransport.TotalConnections == 0, 3000), Is.True,
                    $"Client refused after Shutdown kept its slot, counter is {serverTransport.TotalConnections}.");
            }
            finally
            {
                lateAck?.Dispose();
                server.Shutdown();
            }
        }

        //Shutdown lands between the slot being reserved and the handshake giving it back
        [Test]
        public void ServerShutdown_DuringPendingHandshake_SlotReleasedWithoutGoingNegative()
        {
            int port = Interlocked.Increment(ref _port);
            var serverTransport = new TcpTransport();
            var server = new LiminalNetworkManager(serverTransport, Config(port));
            server.StartServer("127.0.0.1", port);

            TcpClient halfOpen = null;
            try
            {
                halfOpen = ConnectWithoutAck(port);
                Assert.That(SpinWait.SpinUntil(() => serverTransport.TotalConnections == 1, 3000), Is.True, "Precondition: handshake slot never reserved.");

                server.Shutdown();
                halfOpen.Close();

                Assert.That(SpinWait.SpinUntil(() => serverTransport.TotalConnections < 0, 3000), Is.False,
                    $"Connection counter went negative ({serverTransport.TotalConnections}) once the pending handshake gave its slot back.");
                Assert.That(SpinWait.SpinUntil(() => serverTransport.TotalConnections == 0, 3000), Is.True,
                    $"Pending handshake never gave its slot back, counter is {serverTransport.TotalConnections}.");
            }
            finally
            {
                halfOpen?.Dispose();
                server.Shutdown();
            }
        }

        //A client's own connection to the server is never counted so its shutdown must not count it down either
        [Test]
        public void ClientShutdown_LeavesItsConnectionCounterAtZero()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var clientTransport = new TcpTransport();
            var client = new LiminalNetworkManager(clientTransport, Config(port));
            try
            {
                client.StartClient("127.0.0.1", port);
                Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 3000), Is.True, "Precondition: client never connected.");

                client.Shutdown();
                Thread.Sleep(200);

                Assert.That(clientTransport.TotalConnections, Is.EqualTo(0));
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }
    }
}
