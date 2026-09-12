using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalPacketFragmentorTests
    {
        private LiminalTransportConfig _defaultConfig;

        [SetUp]
        public void Setup()
        {
            _defaultConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9050,
                TickRate = 60,
                MaxPacketSizePerBatch = 8192,
                MaxPacketCount = 256,
                MaxConnectionCount = 100,
                ReceiveResponseTimeout = 1.0f,
                ClientIdResolver = new BaseResolver()
            };
        }

        #region Header Sizing & Mathematical Bounds

        [Test]
        public void Test01_Constructor_WhenMtuLessThanOrEqualToCombinedHeaders_ThrowsArgumentException()
        {
            var mockTransport = new MockTransport();
            var customFramer = new SecureFramingProvider();
            _defaultConfig.TransportFramingProvider = customFramer;

            int minimumMtu = LiminalPacketFragmentor.FragmentHeaderSize +
                             LiminalTransportHeader.BaseHeaderSize +
                             customFramer.CustomHeaderSize;

            Assert.That(minimumMtu, Is.EqualTo(17));

            Assert.Throws<ArgumentException>(() => new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 17));
            Assert.Throws<ArgumentException>(() => new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 16));
            Assert.DoesNotThrow(() => new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 18));
        }

        [Test]
        public void Test02_HeaderSizing_WithCustomFraming_CalculatesExactEffectiveMtuAndPayloadChunks()
        {
            var mockTransport = new MockTransport();
            var customFramer = new SecureFramingProvider();
            _defaultConfig.TransportFramingProvider = customFramer;

            int mtu = 1200;
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu);

            int expectedTransportHeader = 11;
            int expectedEffectivePayloadMtu = mtu - expectedTransportHeader;
            int expectedMaxFragmentPayload = expectedEffectivePayloadMtu - LiminalPacketFragmentor.FragmentHeaderSize;

            Assert.Multiple(() =>
            {
                Assert.That(fragmentor.MTU, Is.EqualTo(1200));
                Assert.That(expectedEffectivePayloadMtu, Is.EqualTo(1189));
                Assert.That(expectedMaxFragmentPayload, Is.EqualTo(1183));
            });

            byte[] passthroughPayload = new byte[expectedEffectivePayloadMtu];
            fragmentor.Send(passthroughPayload, targetId: 1, TransportFlags.Reliable);

            Assert.That(mockTransport.SentPackets.Count, Is.EqualTo(1));
            Assert.That(mockTransport.SentPackets[0].Flags, Is.EqualTo(TransportFlags.Reliable));
            Assert.That(mockTransport.SentPackets[0].Data.Length, Is.EqualTo(expectedEffectivePayloadMtu));

            mockTransport.SentPackets.Clear();

            byte[] splitPayload = new byte[expectedEffectivePayloadMtu + 1];
            fragmentor.Send(splitPayload, targetId: 1, TransportFlags.Reliable);

            int expectedRemainingPayload = splitPayload.Length - expectedMaxFragmentPayload;

            Assert.That(mockTransport.SentPackets.Count, Is.EqualTo(2));
            Assert.Multiple(() =>
            {
                Assert.That(mockTransport.SentPackets[0].Data.Length, Is.EqualTo(expectedMaxFragmentPayload + LiminalPacketFragmentor.FragmentHeaderSize));
                Assert.That(mockTransport.SentPackets[0].Flags, Is.EqualTo(TransportFlags.Reliable | TransportFlags.Fragmented));

                Assert.That(mockTransport.SentPackets[1].Data.Length, Is.EqualTo(expectedRemainingPayload + LiminalPacketFragmentor.FragmentHeaderSize));
                Assert.That(mockTransport.SentPackets[1].Flags, Is.EqualTo(TransportFlags.Reliable | TransportFlags.Fragmented));
            });
        }

        #endregion

        #region Outbound Path & Flag Upgrade

        [Test]
        public void Test03_Send_UnreliableBelowMtu_RetainsUnreliableFlagWithoutFragmentation()
        {
            var mockTransport = new MockTransport();
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1200);

            byte[] data = new byte[500];
            fragmentor.Send(data, targetId: 2, TransportFlags.Unreliable);

            Assert.That(mockTransport.SentPackets.Count, Is.EqualTo(1));
            Assert.That(mockTransport.SentPackets[0].Flags, Is.EqualTo(TransportFlags.Unreliable));
            Assert.That(mockTransport.SentPackets[0].TargetId, Is.EqualTo(2));
        }

        [Test]
        public void Test04_Send_UnreliableAboveMtu_AutoUpgradesToReliableAndFragmented()
        {
            var mockTransport = new MockTransport();
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1200);

            byte[] largeData = new byte[2500];
            fragmentor.Send(largeData, targetId: 3, TransportFlags.Unreliable);

            Assert.That(mockTransport.SentPackets.Count, Is.GreaterThan(1));
            foreach (var packet in mockTransport.SentPackets)
            {
                Assert.That(packet.Flags, Is.EqualTo(TransportFlags.Reliable | TransportFlags.Fragmented));
            }
        }

        [Test]
        public void Test05_Send_GeneratesMonotonicSequencesAndAccurateFragmentHeaders()
        {
            var mockTransport = new MockTransport();
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] largeData = new byte[2000];
            fragmentor.Send(largeData, targetId: 5, TransportFlags.Reliable);

            Assert.That(mockTransport.SentPackets.Count, Is.EqualTo(3));

            for (ushort i = 0; i < 3; i++)
            {
                var frame = mockTransport.SentPackets[i].Data;
                ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(0, 2));
                ushort index = BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(2, 2));
                ushort total = BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(4, 2));

                Assert.Multiple(() =>
                {
                    Assert.That(seq, Is.EqualTo(1));
                    Assert.That(index, Is.EqualTo(i));
                    Assert.That(total, Is.EqualTo(3));
                });
            }
        }

        #endregion

        #region Inbound Path & Reassembly

        [Test]
        public void Test06_Ingest_InOrderFragments_ReassemblesExactPayload()
        {
            var mockTransport = new MockTransport();
            mockTransport.ConnectedClients.Add(10);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1200);

            byte[] originalPayload = CreateRandomBuffer(3500);
            byte[] reassembledResult = null;
            ushort seenSender = 0;

            fragmentor.OnMessageReassembled += (data, sender) =>
            {
                reassembledResult = data.ToArray();
                seenSender = sender;
            };

            fragmentor.Send(originalPayload, targetId: 10, TransportFlags.Reliable);
            var generatedPackets = new List<MockTransport.PacketRecord>(mockTransport.SentPackets);
            mockTransport.SentPackets.Clear();

            foreach (var packet in generatedPackets)
            {
                fragmentor.IngestFragment(packet.Data, senderId: 10);
            }

            Assert.That(SpinWait.SpinUntil(() => reassembledResult != null, 2000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(seenSender, Is.EqualTo(10));
                Assert.That(reassembledResult, Is.EqualTo(originalPayload));
            });
        }

        [Test]
        public void Test07_Ingest_OutOfOrderFragments_ReassemblesAccurately()
        {
            var mockTransport = new MockTransport();
            mockTransport.ConnectedClients.Add(15);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 800);

            byte[] originalPayload = CreateRandomBuffer(2500);
            byte[] reassembledResult = null;

            fragmentor.OnMessageReassembled += (data, _) => reassembledResult = data.ToArray();

            fragmentor.Send(originalPayload, targetId: 15, TransportFlags.Reliable);
            var chunks = new List<MockTransport.PacketRecord>(mockTransport.SentPackets);
            mockTransport.SentPackets.Clear();

            Assert.That(chunks.Count, Is.GreaterThan(2));

            var scrambled = new List<MockTransport.PacketRecord>
            {
                chunks[^1],
                chunks[0]
            };
            for (int i = 1; i < chunks.Count - 1; i++)
            {
                scrambled.Add(chunks[i]);
            }

            foreach (var chunk in scrambled)
            {
                fragmentor.IngestFragment(chunk.Data, senderId: 15);
            }

            Assert.That(SpinWait.SpinUntil(() => reassembledResult != null, 2000), Is.True);
            Assert.That(reassembledResult, Is.EqualTo(originalPayload));
        }

        [Test]
        public void Test08_Ingest_WithCustomFramingProvider_MaintainsPayloadAlignment()
        {
            var mockTransport = new MockTransport();
            mockTransport.ConnectedClients.Add(20);
            var customFramer = new SecureFramingProvider();
            _defaultConfig.TransportFramingProvider = customFramer;

            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] originalPayload = CreateRandomBuffer(3000);
            byte[] reassembledResult = null;

            fragmentor.OnMessageReassembled += (data, _) => reassembledResult = data.ToArray();

            fragmentor.Send(originalPayload, targetId: 20, TransportFlags.Reliable);
            var chunks = new List<MockTransport.PacketRecord>(mockTransport.SentPackets);
            mockTransport.SentPackets.Clear();

            foreach (var chunk in chunks)
            {
                fragmentor.IngestFragment(chunk.Data, senderId: 20);
            }

            Assert.That(SpinWait.SpinUntil(() => reassembledResult != null, 2000), Is.True);
            Assert.That(reassembledResult, Is.EqualTo(originalPayload));
        }

        #endregion

        #region Malformed Wire Frames & Fault Injection

        [Test]
        public void Test09_Ingest_FragmentTooSmallOrExceedingMtu_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(30);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] tinyFragment = new byte[5];
            fragmentor.IngestFragment(tinyFragment, senderId: 30);

            Assert.That(mockTransport.KickedClients.Contains(30), Is.True);

            mockTransport.KickedClients.Clear();
            mockTransport.ConnectedClients.Add(31);

            byte[] massiveFragment = new byte[1001];
            fragmentor.IngestFragment(massiveFragment, senderId: 31);

            Assert.That(mockTransport.KickedClients.Contains(31), Is.True);
        }

        [Test]
        public void Test10_Ingest_CorruptIndices_ZeroTotalFragments_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(32);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] malformed = new byte[50];
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(4, 2), 0);

            fragmentor.IngestFragment(malformed, senderId: 32);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(32), 2000), Is.True);
        }

        [Test]
        public void Test11_Ingest_CorruptIndices_IndexExceedsOrEqualToTotal_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(33);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] malformed = new byte[50];
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(2, 2), 3);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(4, 2), 3);

            fragmentor.IngestFragment(malformed, senderId: 33);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(33), 2000), Is.True);
        }

        [Test]
        public void Test12_Ingest_IntermediateFragment_InvalidPayloadLength_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(34);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] malformed = new byte[6 + 500];
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(malformed.AsSpan(4, 2), 2);

            fragmentor.IngestFragment(malformed, senderId: 34);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(34), 2000), Is.True);
        }

        [Test]
        public void Test13_Ingest_DuplicateFragmentIndex_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(35);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            byte[] data = new byte[2000];
            fragmentor.Send(data, targetId: 35, TransportFlags.Reliable);
            var chunks = new List<MockTransport.PacketRecord>(mockTransport.SentPackets);
            mockTransport.SentPackets.Clear();

            fragmentor.IngestFragment(chunks[0].Data, senderId: 35);
            fragmentor.IngestFragment(chunks[0].Data, senderId: 35);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(35), 2000), Is.True);
        }

        [Test]
        public void Test14_Ingest_TotalFragmentsMismatchForSequence_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(36);
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            int effectiveMaxPayload = 1000 - 5 - 6;

            byte[] chunk0 = new byte[6 + effectiveMaxPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(0, 2), 10);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(4, 2), 3);

            byte[] chunk1 = new byte[6 + effectiveMaxPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(chunk1.AsSpan(0, 2), 10);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk1.AsSpan(2, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk1.AsSpan(4, 2), 4);

            fragmentor.IngestFragment(chunk0, senderId: 36);
            fragmentor.IngestFragment(chunk1, senderId: 36);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(36), 2000), Is.True);
        }

        [Test]
        public void Test15_Ingest_AssemblySizeExceedingMaxAllowedBatch_KicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(37);
            _defaultConfig.MaxPacketSizePerBatch = 4096;

            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            int effectiveMaxPayload = 1000 - 5 - 6;
            byte[] chunk = new byte[6 + effectiveMaxPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(4, 2), 10);

            fragmentor.IngestFragment(chunk, senderId: 37);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(37), 2000), Is.True);
        }

        #endregion

        #region Assembly Timeout & Teardown Lifecycle

        [Test]
        public void Test16_Ingest_AssemblyTimesOut_PrunesAssemblyAndKicksClient()
        {
            var mockTransport = new MockTransport { IsServer = true };
            mockTransport.ConnectedClients.Add(40);
            _defaultConfig.ReceiveResponseTimeout = 0.2f;

            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            int effectiveMaxPayload = 1000 - 5 - 6;
            byte[] chunk0 = new byte[6 + effectiveMaxPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(0, 2), 99);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk0.AsSpan(4, 2), 2);

            fragmentor.IngestFragment(chunk0, senderId: 40);

            Assert.That(SpinWait.SpinUntil(() => mockTransport.KickedClients.Contains(40), 3000), Is.True);
        }

        [Test]
        public void Test17_ClientDisconnected_CleansUpPeerChannelsWithoutExceptions()
        {
            var mockTransport = new MockTransport();
            mockTransport.ConnectedClients.Add(50);
            var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            int effectiveMaxPayload = 1000 - 5 - 6;
            byte[] chunk = new byte[6 + effectiveMaxPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(chunk.AsSpan(4, 2), 2);

            fragmentor.IngestFragment(chunk, senderId: 50);

            Assert.DoesNotThrow(() =>
            {
                mockTransport.RaiseClientDisconnected(50);
                fragmentor.Dispose();
            });
        }

        #endregion

        #region High Concurrency & Interleaved Peers

        [Test]
        public void Test18_HighConcurrency_MultiClientInterleavedFragments_ZeroCrossTalk()
        {
            var mockTransport = new MockTransport();
            using var fragmentor = new LiminalPacketFragmentor(mockTransport, _defaultConfig, mtu: 1000);

            int clientCount = 8;
            int payloadSize = 3500;
            var expectedPayloads = new Dictionary<ushort, byte[]>();
            var receivedPayloads = new ConcurrentDictionary<ushort, byte[]>();

            for (ushort id = 1; id <= clientCount; id++)
            {
                mockTransport.ConnectedClients.Add(id);
                expectedPayloads[id] = CreateRandomBuffer(payloadSize, seed: id);
            }

            fragmentor.OnMessageReassembled += (data, sender) =>
            {
                receivedPayloads[sender] = data.ToArray();
            };

            var allChunks = new List<(ushort SenderId, byte[] Buffer)>();
            for (ushort id = 1; id <= clientCount; id++)
            {
                fragmentor.Send(expectedPayloads[id], targetId: id, TransportFlags.Reliable);
                foreach (var packet in mockTransport.SentPackets)
                {
                    allChunks.Add((id, packet.Data));
                }
                mockTransport.SentPackets.Clear();
            }

            Shuffle(allChunks);

            Parallel.ForEach(allChunks, chunk =>
            {
                fragmentor.IngestFragment(chunk.Buffer, chunk.SenderId);
            });

            Assert.That(SpinWait.SpinUntil(() => receivedPayloads.Count == clientCount, 5000), Is.True);

            for (ushort id = 1; id <= clientCount; id++)
            {
                Assert.That(receivedPayloads[id], Is.EqualTo(expectedPayloads[id]));
            }
        }

        #endregion

        #region End-to-End TCP Transport Integration

        [Test]
        public void Test19_EndToEnd_TcpTransport_CustomHeader_LargeFragmentedPacket_DeliveredSuccessfully()
        {
            int port = 9075;
            var serverFramer = new SecureFramingProvider();
            var clientFramer = new SecureFramingProvider();

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 16384,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = serverFramer,
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 16384,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = clientFramer,
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var serverTransport = new TcpTransport<SecureFramingContext>();
            var clientTransport = new TcpTransport<SecureFramingContext>();

            serverTransport.InitializeTransport(serverConfig);
            clientTransport.InitializeTransport(clientConfig);

            serverTransport.StartServer("127.0.0.1", port);
            clientTransport.StartClient("127.0.0.1", port);

            Assert.That(SpinWait.SpinUntil(() => clientTransport.IsConnected && clientTransport.LocalClientId != 0, 3000), Is.True);
            ushort assignedClientId = clientTransport.LocalClientId;

            using var serverFragmentor = new LiminalPacketFragmentor(serverTransport, serverConfig, mtu: 1200);
            using var clientFragmentor = new LiminalPacketFragmentor(clientTransport, clientConfig, mtu: 1200);

            byte[] originalPayload = CreateRandomBuffer(7500);
            byte[] serverReceivedPayload = null;
            ushort serverSeenSender = 999;

            serverFragmentor.OnMessageReassembled += (data, sender) =>
            {
                serverReceivedPayload = data.ToArray();
                serverSeenSender = sender;
            };

            clientFragmentor.Send(originalPayload, ILiminalTransport.SERVER_ID, TransportFlags.Reliable);

            Assert.That(SpinWait.SpinUntil(() => serverReceivedPayload != null, 4000), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(serverSeenSender, Is.EqualTo(assignedClientId));
                Assert.That(serverReceivedPayload, Is.EqualTo(originalPayload));
                Assert.That(serverFramer.ValidPacketsCount, Is.GreaterThanOrEqualTo(7));
                Assert.That(serverFramer.CorruptedPacketsCount, Is.EqualTo(0));
            });

            clientTransport.Shutdown();
            serverTransport.Shutdown();
        }

        #endregion

        #region Test Helpers & Mock Transport

        private static byte[] CreateRandomBuffer(int size, int seed = 42)
        {
            var buffer = new byte[size];
            new Random(seed).NextBytes(buffer);
            return buffer;
        }

        private static void Shuffle<T>(IList<T> list)
        {
            var rng = new Random(1337);
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = rng.Next(n + 1);
                (list[k], list[n]) = (list[n], list[k]);
            }
        }

        public class MockTransport : ILiminalTransport
        {
            public struct PacketRecord
            {
                public byte[] Data;
                public ushort TargetId;
                public TransportFlags Flags;
            }

            public readonly List<PacketRecord> SentPackets = new();
            public readonly HashSet<ushort> ConnectedClients = new();
            public readonly HashSet<ushort> KickedClients = new();

            public ushort LocalClientId => 1;
            public bool IsServer { get; set; } = false;
            public bool IsClient => !IsServer;
            public bool IsConnected => true;
            public LiminalTransportConfig Config => null;
            public int ConnectedClientCount => ConnectedClients.Count;

            public bool IsClientConnected(ushort clientId) => ConnectedClients.Contains(clientId);

            public void InitializeTransport(LiminalTransportConfig config) { }
            public void StartServer(string ip, int port) => IsServer = true;
            public void StartClient(string ip, int port) => IsServer = false;
            public void Disconnect() { }
            public void Shutdown() { }

            public void Kick(ushort clientId)
            {
                KickedClients.Add(clientId);
                ConnectedClients.Remove(clientId);
                OnClientKicked?.Invoke(clientId);
            }

            public void Send(Span<byte> data, ushort clientId, TransportFlags flags)
            {
                SentPackets.Add(new PacketRecord
                {
                    Data = data.ToArray(),
                    TargetId = clientId,
                    Flags = flags
                });
            }

            public void RaiseClientDisconnected(ushort clientId)
            {
                ConnectedClients.Remove(clientId);
                OnClientDisconnected?.Invoke(clientId);
            }

            public event DataReceivedHandler OnMessageReceivedReliable;
            public event DataReceivedHandler OnMessageReceivedUnreliable;
            public event DataReceivedHandler OnMessageReceivedFragmented;
            public event TransportEventHandler OnServerStarted;
            public event TransportEventHandler OnShutdown;
            public event TransportEventHandler OnHandshakeInitialized;
            public event ClientConnectionHandler OnLocalClientConnected;
            public event ClientConnectionHandler OnLocalClientDisconnected;
            public event ClientConnectionHandler OnClientConnected;
            public event ClientConnectionHandler OnClientDisconnected;
            public event ClientConnectionHandler OnClientKicked;
        }

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

        public class SecureFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public const uint ExpectedMagic = 0xDEADBEEF;
            public int CustomHeaderSize => 6;

            public int CorruptedPacketsCount;
            public int ValidPacketsCount;
            private int _outboundSeq;

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
                context = new SecureFramingContext(cookie, seq);
                return true;
            }
        }

        #endregion
    }
}