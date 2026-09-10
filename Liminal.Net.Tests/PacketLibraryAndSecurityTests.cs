using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class PacketLibraryAndSecurityTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7880;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }
            _serverManager?.Shutdown();
        }

        private LiminalNetworkManager CreateAndStartClient(LiminalTelemetryConfig telemetryConfig = null)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };
            var client = new LiminalNetworkManager(new TcpTransport(), config, telemetryConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        #region Dynamic Packet Library & Determinism Tests

        [Test]
        public void Test35_PacketLibrary_HandshakePackets_RegisteredDeterministicallyWithNonZeroIds()
        {
            LiminalPacketLibrary.Initialize();

            ushort clientPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();
            ushort serverPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
            ushort ackPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();

            Assert.Multiple(() =>
            {
                Assert.That(clientPacketId, Is.GreaterThan(0), "ConnectionHandshakePacketClient must have an ID > 0.");
                Assert.That(serverPacketId, Is.GreaterThan(0), "ConnectionHandshakePacketServer must have an ID > 0.");
                Assert.That(ackPacketId, Is.GreaterThan(0), "ConnectionHandshakeClientAck must have an ID > 0.");

                // Verify IDs are distinct
                Assert.That(clientPacketId, Is.Not.EqualTo(serverPacketId), "Client and Server handshake packets share an ID.");
                Assert.That(serverPacketId, Is.Not.EqualTo(ackPacketId), "Server and ACK handshake packets share an ID.");
                Assert.That(clientPacketId, Is.Not.EqualTo(ackPacketId), "Client and ACK handshake packets share an ID.");
            });
        }

        [Test]
        public void Test36_PacketLibrary_BidirectionalTypeResolution_MatchesExactTypes()
        {
            LiminalPacketLibrary.Initialize();

            ushort clientPktId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();
            ushort serverPktId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
            ushort ackPktId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();

            bool clientFound = LiminalPacketLibrary.TryGetType(clientPktId, out var clientType);
            bool serverFound = LiminalPacketLibrary.TryGetType(serverPktId, out var serverType);
            bool ackFound = LiminalPacketLibrary.TryGetType(ackPktId, out var ackType);

            Assert.Multiple(() =>
            {
                Assert.That(clientFound, Is.True);
                Assert.That(clientType, Is.EqualTo(typeof(ConnectionHandshakePacketClient)));

                Assert.That(serverFound, Is.True);
                Assert.That(serverType, Is.EqualTo(typeof(ConnectionHandshakePacketServer)));

                Assert.That(ackFound, Is.True);
                Assert.That(ackType, Is.EqualTo(typeof(ConnectionHandshakeClientAck)));

                // Unregistered ID boundary test
                Assert.That(LiminalPacketLibrary.TryGetType(ushort.MaxValue, out _), Is.False, "Unknown ID resolved unexpectedly.");
            });
        }

        [Test]
        public void Test37_PacketLibrary_RegistryHash_IsDeterministicAndStable()
        {
            LiminalPacketLibrary.Initialize();

            uint hashFirstAccess = LiminalPacketLibrary.RegistryHash;
            Assert.That(hashFirstAccess, Is.Not.Zero, "Registry hash should never be 0.");

            // Repeated calls must produce the identical hash
            LiminalPacketLibrary.Initialize();
            uint hashSecondAccess = LiminalPacketLibrary.RegistryHash;

            Assert.That(hashSecondAccess, Is.EqualTo(hashFirstAccess), "Registry hash mutated across calls; dynamic discovery must remain deterministic.");
        }

        #endregion

        #region Handshake Pipeline Security & Protocol Tests

        [Test]
        public void Test38_HandshakePipeline_RoguePacketRegistryHash_RejectsAndDrops()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var rogueClient = new TcpClient();
            rogueClient.Connect("127.0.0.1", _currentTestPort);
            var stream = rogueClient.GetStream();

            var maliciousHandshake = new ConnectionHandshakePacketClient
            {
                ClientName = "DesyncedClient",
                ClientVersion = 1,
                PacketRegistryHash = LiminalPacketLibrary.RegistryHash ^ 0xDEADBEEF // Tampered hash
            };

            byte[] body = MessagePackSerializer.Serialize(maliciousHandshake);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            byte[] header = new byte[8];
            rogueClient.ReceiveTimeout = 2000;
            stream.ReadExactly(header, 0, 8);
            int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));

            byte[] payload = new byte[length];
            stream.ReadExactly(payload, 0, length);
            var response = MessagePackSerializer.Deserialize<ConnectionHandshakePacketServer>(payload);

            Assert.Multiple(() =>
            {
                Assert.That(response.AssignedClientID, Is.EqualTo(0), "Server should not assign an ID to a registry-mismatched client.");
                Assert.That(response.RejectReason, Is.EqualTo(DisconnectReason.ProtocolViolation));
                Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
            });
        }

        [Test]
        public void Test39_HandshakePipeline_VersionMismatch_RejectsAndDrops()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var outdatedClient = new TcpClient();
            outdatedClient.Connect("127.0.0.1", _currentTestPort);
            var stream = outdatedClient.GetStream();

            var outdatedHandshake = new ConnectionHandshakePacketClient
            {
                ClientName = "OutdatedClient",
                ClientVersion = 999,
                PacketRegistryHash = LiminalPacketLibrary.RegistryHash
            };

            byte[] body = MessagePackSerializer.Serialize(outdatedHandshake);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            byte[] header = new byte[8];
            outdatedClient.ReceiveTimeout = 2000;
            stream.ReadExactly(header, 0, 8);
            int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));

            byte[] payload = new byte[length];
            stream.ReadExactly(payload, 0, length);
            var response = MessagePackSerializer.Deserialize<ConnectionHandshakePacketServer>(payload);

            Assert.Multiple(() =>
            {
                Assert.That(response.AssignedClientID, Is.EqualTo(0));
                Assert.That(response.RejectReason, Is.EqualTo(DisconnectReason.VersionMismatch));
                Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
            });
        }

        [Test]
        public void Test40_HandshakePipeline_OversizedPayload_TriggersSecurityViolation()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var attackerClient = new TcpClient();
            attackerClient.Connect("127.0.0.1", _currentTestPort);
            var stream = attackerClient.GetStream();

            // Handshake pipeline caps frames at maxHandshakeSize (256 bytes default)
            byte[] maliciousHeader = new byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(0, 4), 1024 * 1024); // Claiming 1 MB
            BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());

            stream.Write(maliciousHeader, 0, maliciousHeader.Length);

            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                attackerClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server should immediately drop client attempting an oversized handshake header.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test41_HandshakePipeline_WrongSequencePacketId_Dropped()
        {
            LiminalPacketLibrary.Initialize();
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            using var rogueClient = new TcpClient();
            rogueClient.Connect("127.0.0.1", _currentTestPort);
            var stream = rogueClient.GetStream();

            // Client skips Step 1 and immediately transmits Step 3 (Ack)
            var ack = new ConnectionHandshakeClientAck { Ack = true, ClientID = 1 };
            byte[] body = MessagePackSerializer.Serialize(ack);
            byte[] frame = new byte[8 + body.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>());
            body.CopyTo(frame.AsSpan(8));

            stream.Write(frame, 0, frame.Length);

            byte[] readBuffer = new byte[8];
            int bytesRead = 0;
            try
            {
                rogueClient.ReceiveTimeout = 2000;
                bytesRead = stream.Read(readBuffer, 0, 8);
            }
            catch { bytesRead = 0; }

            Assert.That(bytesRead, Is.EqualTo(0), "Server accepted out-of-order handshake packet instead of disconnecting.");
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        [Test]
        public void Test42_MulticastSend_SinglePayloadFannedOutToAllTargets()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var c1 = CreateAndStartClient();
            var c2 = CreateAndStartClient();
            var c3 = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() =>
                c1.Transport.IsConnected && c2.Transport.IsConnected && c3.Transport.IsConnected, 3000), Is.True);

            ushort id1 = c1.localID;
            ushort id2 = c2.localID;
            ushort id3 = c3.localID;

            Assert.That(id1, Is.Not.EqualTo(0));
            Assert.That(id2, Is.Not.EqualTo(0));
            Assert.That(id3, Is.Not.EqualTo(0));

            int c1ReceivedCount = 0;
            int c2ReceivedCount = 0;
            int c3ReceivedCount = 0;

            string c1Message = null;
            string c2Message = null;

            c1.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c1ReceivedCount);
                c1Message = pkt.Message;
            }, this);

            c2.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c2ReceivedCount);
                c2Message = pkt.Message;
            }, this);

            c3.Interpreter.Subscribe<ChatPacket>((pkt, sender) =>
            {
                Interlocked.Increment(ref c3ReceivedCount);
            }, this);

            Span<ushort> targets = stackalloc ushort[] { id1, id2 };
            _serverManager.Interpreter.SendCommand(targets, new ChatPacket { Message = "Multicast_Payload" });
            _serverManager.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => c1ReceivedCount == 1, 2000), Is.True, "Client 1 missed multicast packet.");
            Assert.That(SpinWait.SpinUntil(() => c2ReceivedCount == 1, 2000), Is.True, "Client 2 missed multicast packet.");
            Assert.That(c1Message, Is.EqualTo("Multicast_Payload"));
            Assert.That(c2Message, Is.EqualTo("Multicast_Payload"));

            Thread.Sleep(200);
            Assert.That(c3ReceivedCount, Is.EqualTo(0), "Client 3 received a packet it was not targeted for.");
        }

        #endregion
    }
}