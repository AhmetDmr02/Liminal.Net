using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class CustomFramingTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7830;
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

        [Test]
        public void Test23_CustomFraming_ValidHeader_DeliveredAndParsedSuccessfully()
        {
            var serverFramer = new SecureFramingProvider();
            var clientFramer = new SecureFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = clientFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            bool received = false;
            client.Interpreter.Subscribe<ChatPacket>((pkt, id) =>
            {
                if (pkt.Message == "FramedPing") received = true;
            }, this);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            // Send 10 packets from Server to Client
            for (int i = 0; i < 10; i++)
            {
                _serverManager.Interpreter.SendCommand(1, new ChatPacket { Message = "FramedPing" });
                _serverManager.SessionManager.Flush();
            }

            Assert.That(SpinWait.SpinUntil(() => received, 2000), Is.True, "Framed packet failed delivery.");
            Assert.That(SpinWait.SpinUntil(() => clientFramer.ValidPacketsCount >= 10, 2000), Is.True,
                $"Custom framing reader was not invoked on client. Valid: {clientFramer.ValidPacketsCount}, Corrupted: {clientFramer.CorruptedPacketsCount}");
            Assert.That(clientFramer.CorruptedPacketsCount, Is.EqualTo(0), "Custom framing detected unexpected corruption.");
        }

        [Test]
        public void Test24_CustomFraming_TamperedMagicCookie_TriggersMalformedKick()
        {
            var serverFramer = new SecureFramingProvider();
            var maliciousFramer = new BadMagicFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = maliciousFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);

            bool serverKickedClient = false;
            _serverManager.Transport.OnClientKicked += (id) => serverKickedClient = true;

            client.StartClient("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            // Client sends packet with bad magic cookie
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Exploit" });
            client.SessionManager.Flush();

            // Server should detect bad magic, increment corruption count, and kick
            Assert.That(SpinWait.SpinUntil(() => serverFramer.CorruptedPacketsCount > 0, 2000), Is.True, "Server framer missed bad magic cookie.");
            Assert.That(SpinWait.SpinUntil(() => serverKickedClient, 2000), Is.True, "Server failed to kick client with malformed header.");
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True, "Client remained connected after kick.");
        }

        [Test]
        public void Test25_CustomFraming_HighConcurrency_ZeroRaceConditionsOnHeaderContext()
        {
            var serverFramer = new SecureFramingProvider();
            var clientFramer = new SecureFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverConfig.MaxPacketSizePerBatch = ushort.MaxValue;
            _serverConfig.MaxPacketCount = ushort.MaxValue;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = ushort.MaxValue,
                MaxPacketCount = ushort.MaxValue,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = clientFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);
            ushort assignedId = client.localID;

            int totalPackets = 1000;
            int clientReceived = 0;
            int serverReceived = 0;

            client.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref clientReceived), this);
            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref serverReceived), this);

            int workerCount = 4;
            var startGate = new ManualResetEventSlim(false);
            Task[] workers = new Task[workerCount * 2];

            for (int w = 0; w < workerCount; w++)
            {
                workers[w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 0; i < totalPackets / workerCount; i++)
                    {
                        _serverManager.Interpreter.SendCommand(assignedId, new ChatPacket { Message = "S2C" });
                        if (i % 25 == 0) _serverManager.SessionManager.Flush();
                    }
                    _serverManager.SessionManager.Flush();
                });

                workers[workerCount + w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 0; i < totalPackets / workerCount; i++)
                    {
                        client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "C2S" });
                        if (i % 25 == 0) client.SessionManager.Flush();
                    }
                    client.SessionManager.Flush();
                });
            }

            startGate.Set();
            Task.WaitAll(workers);

            Assert.That(SpinWait.SpinUntil(() => clientReceived == totalPackets, 5000), Is.True, $"Client received {clientReceived}/{totalPackets}");
            Assert.That(SpinWait.SpinUntil(() => serverReceived == totalPackets, 5000), Is.True, $"Server received {serverReceived}/{totalPackets}");

            Assert.That(serverFramer.CorruptedPacketsCount, Is.EqualTo(0), "Server detected header corruption under high load.");
            Assert.That(clientFramer.CorruptedPacketsCount, Is.EqualTo(0), "Client detected header corruption under high load.");
        }

        #region Test Framing Providers

        public readonly struct SecureFramingContext
        {
            public readonly uint MagicCookie;
            public readonly ushort SequenceNumber;

            public SecureFramingContext(uint magicCookie, ushort sequenceNumber)
            {
                MagicCookie = magicCookie;
                SequenceNumber = sequenceNumber;
            }
        }

        /// <summary>
        /// Custom 6-byte header extension: [uint MagicCookie (4b)][ushort SequenceNumber (2b)].
        /// Validates magic cookie and tracks received sequence numbers atomically.
        /// </summary>
        public class SecureFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public const uint ExpectedMagic = 0xDEADBEEF;
            public int CustomHeaderSize => 6;

            public int CorruptedPacketsCount = 0;
            public int ValidPacketsCount = 0;
            public readonly ConcurrentBag<ushort> ReceivedSequences = new();

            private int _outboundSeq = 0;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                uint cookie = context.MagicCookie != 0 ? context.MagicCookie : ExpectedMagic;
                ushort seq = context.SequenceNumber != 0 ? context.SequenceNumber : (ushort)Interlocked.Increment(ref _outboundSeq);

                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), cookie);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), seq);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
                ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));

                if (cookie != ExpectedMagic)
                {
                    Interlocked.Increment(ref CorruptedPacketsCount);
                    context = default;
                    return false;
                }

                Interlocked.Increment(ref ValidPacketsCount);
                ReceivedSequences.Add(seq);
                context = new SecureFramingContext(cookie, seq);
                return true;
            }
        }

        public class BadMagicFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public int CustomHeaderSize => 6;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                // Deliberately write invalid magic cookie
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), 0xBAADC0DE);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), 999);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                context = default;
                return true;
            }
        }

        #endregion
    }
}