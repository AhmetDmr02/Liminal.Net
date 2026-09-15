using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class BitPackingTests
    {
        #region BitWriter & BitReader Unit Tests

        [Test]
        public void Test_BitWriter_BitReader_SingleBits_Roundtrip()
        {
            byte[] buffer = new byte[64];
            var writer = new BitWriter(buffer.AsSpan());

            writer.WriteBits(1u, 1);      // 1 bit
            writer.WriteBits(5u, 3);      // 3 bits (0b101)
            writer.WriteBits(19u, 5);     // 5 bits
            writer.WriteBits(255u, 8);    // 8 bits
            writer.WriteBits(2047u, 11);  // 11 bits
            writer.WriteBits(0xDEADBEEF, 32); // 32 bits
            writer.WriteULongBits(0x123456789ABCDEF0UL, 64); // 64 bits
            writer.Flush();

            var reader = new BitReader(buffer.AsSpan(0, writer.BytesWritten));

            Assert.That(reader.ReadBits(1), Is.EqualTo(1u));
            Assert.That(reader.ReadBits(3), Is.EqualTo(5u));
            Assert.That(reader.ReadBits(5), Is.EqualTo(19u));
            Assert.That(reader.ReadBits(8), Is.EqualTo(255u));
            Assert.That(reader.ReadBits(11), Is.EqualTo(2047u));
            Assert.That(reader.ReadBits(32), Is.EqualTo(0xDEADBEEF));
            Assert.That(reader.ReadULongBits(64), Is.EqualTo(0x123456789ABCDEF0UL));
        }

        [Test]
        public void Test_BitWriter_BitReader_Primitives_Roundtrip()
        {
            byte[] buffer = new byte[128];
            var writer = new BitWriter(buffer.AsSpan());

            writer.WriteBool(true);
            writer.WriteBool(false);
            writer.WriteByte(0x42);
            writer.WriteUShort(12345);
            writer.WriteShort(-543);
            writer.WriteUInt(987654321u);
            writer.WriteInt(-1234567);
            writer.WriteULong(0xFEDCBA9876543210UL);
            writer.WriteLong(-9876543210123L);
            writer.WriteFloat(3.14159f);
            writer.WriteDouble(2.718281828459);
            writer.Flush();

            var reader = new BitReader(buffer.AsSpan(0, writer.BytesWritten));

            Assert.That(reader.ReadBool(), Is.True);
            Assert.That(reader.ReadBool(), Is.False);
            Assert.That(reader.ReadByte(), Is.EqualTo(0x42));
            Assert.That(reader.ReadUShort(), Is.EqualTo((ushort)12345));
            Assert.That(reader.ReadShort(), Is.EqualTo((short)-543));
            Assert.That(reader.ReadUInt(), Is.EqualTo(987654321u));
            Assert.That(reader.ReadInt(), Is.EqualTo(-1234567));
            Assert.That(reader.ReadULong(), Is.EqualTo(0xFEDCBA9876543210UL));
            Assert.That(reader.ReadLong(), Is.EqualTo(-9876543210123L));
            Assert.That(reader.ReadFloat(), Is.EqualTo(3.14159f).Within(0.00001f));
            Assert.That(reader.ReadDouble(), Is.EqualTo(2.718281828459).Within(0.0000001));
        }

        [Test]
        public void Test_BitWriter_BitReader_QuantizedFloat_Roundtrip()
        {
            byte[] buffer = new byte[64];
            var writer = new BitWriter(buffer.AsSpan());

            float min = -50f;
            float max = 50f;

            writer.WriteQuantizedFloat(-50f, min, max, 16);
            writer.WriteQuantizedFloat(0f, min, max, 16);
            writer.WriteQuantizedFloat(25.5f, min, max, 16);
            writer.WriteQuantizedFloat(50f, min, max, 16);
            writer.Flush();

            var reader = new BitReader(buffer.AsSpan(0, writer.BytesWritten));

            Assert.That(reader.ReadQuantizedFloat(min, max, 16), Is.EqualTo(-50f).Within(0.01f));
            Assert.That(reader.ReadQuantizedFloat(min, max, 16), Is.EqualTo(0f).Within(0.01f));
            Assert.That(reader.ReadQuantizedFloat(min, max, 16), Is.EqualTo(25.5f).Within(0.01f));
            Assert.That(reader.ReadQuantizedFloat(min, max, 16), Is.EqualTo(50f).Within(0.01f));
        }

        [Test]
        public void Test_BitWriter_BitReader_Bytes_Alignment()
        {
            byte[] buffer = new byte[64];
            var writer = new BitWriter(buffer.AsSpan());

            writer.WriteBits(7u, 3); // 3 unaligned bits
            byte[] rawBytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
            writer.WriteBytes(rawBytes); // Should align, then write 4 bytes
            writer.WriteBits(3u, 2); // 2 more bits
            writer.Flush();

            var reader = new BitReader(buffer.AsSpan(0, writer.BytesWritten));

            Assert.That(reader.ReadBits(3), Is.EqualTo(7u));

            byte[] readBytes = new byte[4];
            reader.ReadBytes(readBytes);
            Assert.That(readBytes, Is.EqualTo(rawBytes));

            Assert.That(reader.ReadBits(2), Is.EqualTo(3u));
        }

        [Test]
        public void Test_BitReader_Underflow_Throws()
        {
            byte[] buffer = new byte[2] { 0xAA, 0xBB };
            var reader = new BitReader(buffer.AsSpan());

            reader.ReadBits(16);
            bool threw = false;
            try
            {
                reader.ReadBits(1);
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "BitReader should throw InvalidOperationException on underflow.");
        }

        [Test]
        public void Test_BitWriter_SpanCapacity_Throws()
        {
            byte[] buffer = new byte[2];
            var writer = new BitWriter(buffer.AsSpan());

            writer.WriteBits(0xFFFF, 16);
            bool threw = false;
            try
            {
                writer.WriteBits(1, 1);
                writer.Flush();
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "BitWriter should throw InvalidOperationException when span capacity exceeded.");
        }

        [Test]
        public void Test_BitReader_RemainingSpan()
        {
            byte[] buffer = new byte[] { 0x11, 0x22, 0x33, 0x44, 0x55 };
            var reader = new BitReader(buffer.AsSpan());

            reader.ReadByte(); // reads 0x11
            reader.ReadByte(); // reads 0x22

            var remaining = reader.RemainingSpan;
            Assert.That(remaining.Length, Is.EqualTo(3));
            Assert.That(remaining[0], Is.EqualTo(0x33));
            Assert.That(remaining[1], Is.EqualTo(0x44));
            Assert.That(remaining[2], Is.EqualTo(0x55));
        }

        #endregion

        #region Network Integration Tests

        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private static int _portCounter = 9200;

        [SetUp]
        public void Setup()
        {
            LiminalPacketLibrary.Initialize();
            _clientManagers = new ConcurrentBag<LiminalNetworkManager>();
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }

            _serverManager?.Shutdown();
            LiminalNetworkManager.Instance = null;
        }

        private (LiminalNetworkManager server, LiminalNetworkManager client) CreateConnectedPair()
        {
            int port = Interlocked.Increment(ref _portCounter);

            var serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 32768,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var clientConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 32768,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            _serverManager.StartServer("127.0.0.1", port);

            var clientManager = new LiminalNetworkManager(new TcpTransport(), clientConfig);
            _clientManagers.Add(clientManager);

            bool connected = false;
            clientManager.Events.OnLocalClientConnected += _ => connected = true;

            clientManager.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => connected, 4000), Is.True, "Client failed to connect to server.");

            return (_serverManager, clientManager);
        }

        [Test]
        public void Test_BitStream_SendAndReceive_Roundtrip()
        {
            var (server, client) = CreateConnectedPair();

            uint receivedTick = 0;
            ushort receivedCount = 0;
            int receivedEntityId = 0;
            float receivedX = 0f;
            ushort receivedSender = 0;
            using var receivedEvent = new ManualResetEventSlim(false);

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                receivedTick = meta.Tick;
                receivedCount = meta.Count;
                receivedSender = sender;

                receivedEntityId = reader.ReadInt(16);
                receivedX = reader.ReadFloat();

                receivedEvent.Set();
            }, this);

            byte[] bitBuffer = new byte[32];
            var writer = new BitWriter(bitBuffer.AsSpan());
            writer.WriteInt(1042, 16);
            writer.WriteFloat(123.456f);
            writer.Flush();

            var meta = new TestSnapshotMeta { Tick = 777, Count = 1 };
            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, bitBuffer.AsSpan(0, writer.BytesWritten));

            Assert.That(receivedEvent.Wait(TimeSpan.FromSeconds(5)), Is.True, "Server did not receive bitstream packet.");
            Assert.That(receivedTick, Is.EqualTo(777));
            Assert.That(receivedCount, Is.EqualTo(1));
            Assert.That(receivedEntityId, Is.EqualTo(1042));
            Assert.That(receivedX, Is.EqualTo(123.456f).Within(0.001f));
            Assert.That(receivedSender, Is.EqualTo(client.localID));
        }

        [Test]
        public void Test_BitStream_ActionSend_Roundtrip()
        {
            var (server, client) = CreateConnectedPair();

            LiminalNetworkManager.Instance = client; // Set broadcaster context to client

            uint receivedTick = 0;
            int item1 = 0;
            int item2 = 0;
            using var receivedEvent = new ManualResetEventSlim(false);

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                receivedTick = meta.Tick;
                item1 = reader.ReadInt(16);
                item2 = reader.ReadInt(16);
                receivedEvent.Set();
            }, this);

            var meta = new TestSnapshotMeta { Tick = 999, Count = 2 };

            Broadcaster.SendBitStream(SendTo.Server, in meta, (ref BitWriter writer) =>
            {
                writer.WriteInt(111, 16);
                writer.WriteInt(222, 16);
            });

            Assert.That(receivedEvent.Wait(TimeSpan.FromSeconds(5)), Is.True, "Server did not receive scoped bitstream.");
            Assert.That(receivedTick, Is.EqualTo(999));
            Assert.That(item1, Is.EqualTo(111));
            Assert.That(item2, Is.EqualTo(222));
        }

        [Test]
        public void Test_BitStream_ActionWithState_Roundtrip()
        {
            var (server, client) = CreateConnectedPair();

            LiminalNetworkManager.Instance = client;

            int receivedA = 0;
            int receivedB = 0;
            using var receivedEvent = new ManualResetEventSlim(false);

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                receivedA = reader.ReadInt(16);
                receivedB = reader.ReadInt(16);
                receivedEvent.Set();
            }, this);

            var meta = new TestSnapshotMeta { Tick = 888, Count = 2 };
            var state = (ValA: 333, ValB: 444);

            Broadcaster.SendBitStream(SendTo.Server, in meta, in state, (ref BitWriter writer, in (int ValA, int ValB) s) =>
            {
                writer.WriteInt(s.ValA, 16);
                writer.WriteInt(s.ValB, 16);
            });

            Assert.That(receivedEvent.Wait(TimeSpan.FromSeconds(5)), Is.True);
            Assert.That(receivedA, Is.EqualTo(333));
            Assert.That(receivedB, Is.EqualTo(444));
        }

        [Test]
        public void Test_BitStream_MultipleSubscribers_Isolation()
        {
            var (server, client) = CreateConnectedPair();

            int sub1Read = 0;
            int sub2Read1 = 0;
            int sub2Read2 = 0;
            using var sub1Event = new ManualResetEventSlim(false);
            using var sub2Event = new ManualResetEventSlim(false);

            object listener1 = new object();
            object listener2 = new object();

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                sub1Read = reader.ReadInt(16);
                sub1Event.Set();
            }, listener1);

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                sub2Read1 = reader.ReadInt(16);
                sub2Read2 = reader.ReadInt(16);
                sub2Event.Set();
            }, listener2);

            byte[] bitBuffer = new byte[16];
            var writer = new BitWriter(bitBuffer.AsSpan());
            writer.WriteInt(555, 16);
            writer.WriteInt(777, 16);
            writer.Flush();

            var meta = new TestSnapshotMeta { Tick = 1, Count = 2 };
            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, bitBuffer.AsSpan(0, writer.BytesWritten));

            Assert.That(sub1Event.Wait(TimeSpan.FromSeconds(5)), Is.True);
            Assert.That(sub2Event.Wait(TimeSpan.FromSeconds(5)), Is.True);

            Assert.That(sub1Read, Is.EqualTo(555));
            Assert.That(sub2Read1, Is.EqualTo(555));
            Assert.That(sub2Read2, Is.EqualTo(777));
        }

        [Test]
        public void Test_BitStream_TagPacket_Roundtrip()
        {
            var (server, client) = CreateConnectedPair();

            ulong receivedValue = 0;
            using var receivedEvent = new ManualResetEventSlim(false);

            server.Interpreter.SubscribeBitStream<TestTagBitPacket>((ref BitReader reader, ushort sender) =>
            {
                receivedValue = reader.ReadULong(64);
                receivedEvent.Set();
            }, this);

            byte[] bitBuffer = new byte[16];
            var writer = new BitWriter(bitBuffer.AsSpan());
            writer.WriteULong(0xCAFEBABEDEADBEEFUL, 64);
            writer.Flush();

            var tag = new TestTagBitPacket();
            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in tag, bitBuffer.AsSpan(0, writer.BytesWritten));

            Assert.That(receivedEvent.Wait(TimeSpan.FromSeconds(5)), Is.True, "Server did not receive tag bitstream packet.");
            Assert.That(receivedValue, Is.EqualTo(0xCAFEBABEDEADBEEFUL));
        }

        [Test]
        public void Test_BitStream_LargePayload_Fragmentation()
        {
            var (server, client) = CreateConnectedPair();

            byte[] receivedCheck = null;
            using var receivedEvent = new ManualResetEventSlim(false);

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                receivedCheck = new byte[meta.Count];
                reader.ReadBytes(receivedCheck);
                receivedEvent.Set();
            }, this);

            // Large payload (> MTU)
            byte[] largeData = new byte[4000];
            for (int i = 0; i < largeData.Length; i++)
                largeData[i] = (byte)(i % 251);

            byte[] sendBuffer = new byte[4500];
            var writer = new BitWriter(sendBuffer.AsSpan());
            writer.WriteBytes(largeData);
            writer.Flush();

            var meta = new TestSnapshotMeta { Tick = 100, Count = (ushort)largeData.Length };
            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, sendBuffer.AsSpan(0, writer.BytesWritten));

            Assert.That(receivedEvent.Wait(TimeSpan.FromSeconds(5)), Is.True, "Server did not receive large bitstream packet.");
            Assert.That(receivedCheck, Is.EqualTo(largeData));
        }

        [Test]
        public void Test_BitStream_Unsubscribe()
        {
            var (server, client) = CreateConnectedPair();

            int callCount = 0;
            object listener = new object();

            server.Interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
            {
                Interlocked.Increment(ref callCount);
            }, listener);

            byte[] bitBuffer = new byte[8];
            var writer = new BitWriter(bitBuffer.AsSpan());
            writer.WriteInt(1, 16);
            writer.Flush();

            var meta = new TestSnapshotMeta { Tick = 1, Count = 1 };
            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, bitBuffer.AsSpan(0, writer.BytesWritten));

            Thread.Sleep(200);
            Assert.That(callCount, Is.EqualTo(1));

            server.Interpreter.UnsubscribeBitStream<TestSnapshotMeta>(listener);

            client.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, bitBuffer.AsSpan(0, writer.BytesWritten));
            Thread.Sleep(200);

            Assert.That(callCount, Is.EqualTo(1), "Unsubscribed handler should not be called again.");
        }

        #endregion
    }
}
