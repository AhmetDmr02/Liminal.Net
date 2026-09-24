using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class StickyPacketTests
    {
        private LiminalNetworkManager _manager = null!;
        private LiminalPacketInterpreter _interpreter = null!;
        private MockTransport _transport = null!;

        [SetUp]
        public void SetUp()
        {
            LiminalPacketLibrary.Initialize();

            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };

            _transport = new MockTransport();
            _manager = new LiminalNetworkManager(_transport, config);
            _interpreter = _manager.Interpreter;
        }

        [TearDown]
        public void TearDown()
        {
            _manager.Shutdown();
        }

        [Test]
        public void StickyPacket_DispatchedBeforeSubscription_ReplaysImmediatelyToDeferredSubscriber()
        {
            ushort packetId = LiminalPacketLibrary.GetId<TestStickyStatePacket>();
            Assert.That(LiminalPacketLibrary.IsSticky(packetId), Is.True, "TestStickyStatePacket should be recognized as sticky.");

            var packet = new TestStickyStatePacket { StateId = 42, StateName = "DeferredScene" };
            byte[] bytes = MessagePackSerializer.Serialize(packet);

            // 1. Packet arrives before any subscriber exists (deferred scene)
            _interpreter.Dispatch(packetId, 1, bytes);

            // Verify it was cached in TryGetSticky
            bool found = _interpreter.TryGetSticky<TestStickyStatePacket>(out var cached, out ushort sender);
            Assert.That(found, Is.True);
            Assert.That(cached.StateId, Is.EqualTo(42));
            Assert.That(cached.StateName, Is.EqualTo("DeferredScene"));
            Assert.That(sender, Is.EqualTo(1));

            // 2. Later, component wakes up and subscribes
            TestStickyStatePacket receivedPacket = default;
            ushort receivedSender = 0;
            int callCount = 0;
            object subscriber = new object();

            _interpreter.Subscribe<TestStickyStatePacket>((p, s) =>
            {
                callCount++;
                receivedPacket = p;
                receivedSender = s;
            }, subscriber);

            // Replay happened synchronously during Subscribe
            Assert.That(callCount, Is.EqualTo(1));
            Assert.That(receivedPacket.StateId, Is.EqualTo(42));
            Assert.That(receivedPacket.StateName, Is.EqualTo("DeferredScene"));
            Assert.That(receivedSender, Is.EqualTo(1));
        }

        [Test]
        public void StickyPacket_MultipleDeferredSubscribers_AllReceiveCachedState()
        {
            ushort packetId = LiminalPacketLibrary.GetId<TestStickyStatePacket>();
            var packet = new TestStickyStatePacket { StateId = 99, StateName = "SharedState" };
            byte[] bytes = MessagePackSerializer.Serialize(packet);

            _interpreter.Dispatch(packetId, 2, bytes);

            int sub1Calls = 0;
            int sub2Calls = 0;

            object sub1 = new object();
            object sub2 = new object();

            _interpreter.Subscribe<TestStickyStatePacket>((p, s) =>
            {
                sub1Calls++;
                Assert.That(p.StateId, Is.EqualTo(99));
            }, sub1);

            _interpreter.Subscribe<TestStickyStatePacket>((p, s) =>
            {
                sub2Calls++;
                Assert.That(p.StateId, Is.EqualTo(99));
            }, sub2);

            Assert.That(sub1Calls, Is.EqualTo(1));
            Assert.That(sub2Calls, Is.EqualTo(1));
        }

        [Test]
        public void StickyPacket_UpdatedBeforeSubscription_SubscriberReceivesOnlyLatest()
        {
            ushort packetId = LiminalPacketLibrary.GetId<TestStickyCounterPacket>();

            // Dispatch 3 versions consecutively
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 1 }));
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 2 }));
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 3 }));

            var receivedCounters = new List<int>();
            object sub = new object();

            _interpreter.Subscribe<TestStickyCounterPacket>((p, s) =>
            {
                receivedCounters.Add(p.Counter);
            }, sub);

            // Only the latest version (3) should be delivered upon subscription
            Assert.That(receivedCounters.Count, Is.EqualTo(1));
            Assert.That(receivedCounters[0], Is.EqualTo(3));
        }

        [Test]
        public void StickyPacket_LiveUpdatesAfterSubscription_DispatchedNormally()
        {
            ushort packetId = LiminalPacketLibrary.GetId<TestStickyCounterPacket>();

            // Initial packet before subscribe
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 10 }));

            var receivedCounters = new List<int>();
            object sub = new object();

            _interpreter.Subscribe<TestStickyCounterPacket>((p, s) =>
            {
                receivedCounters.Add(p.Counter);
            }, sub);

            Assert.That(receivedCounters, Is.EqualTo(new[] { 10 }));

            // Subsequent live packets
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 20 }));
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 30 }));

            Assert.That(receivedCounters, Is.EqualTo(new[] { 10, 20, 30 }));
        }

        [Test]
        public void StickyPacket_ClearSticky_RemovesCachedState()
        {
            ushort packetId = LiminalPacketLibrary.GetId<TestStickyStatePacket>();
            var packet = new TestStickyStatePacket { StateId = 77, StateName = "Cleared" };
            _interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(packet));

            Assert.That(_interpreter.TryGetSticky<TestStickyStatePacket>(out _), Is.True);

            _interpreter.ClearSticky<TestStickyStatePacket>();

            Assert.That(_interpreter.TryGetSticky<TestStickyStatePacket>(out _), Is.False);

            int callCount = 0;
            object sub = new object();
            _interpreter.Subscribe<TestStickyStatePacket>((p, s) => callCount++, sub);

            // After clearing, late subscriber receives nothing
            Assert.That(callCount, Is.EqualTo(0));
        }

        [Test]
        public void StickyPacket_ClearAllSticky_ClearsAllStickyPackets()
        {
            ushort stateId = LiminalPacketLibrary.GetId<TestStickyStatePacket>();
            ushort counterId = LiminalPacketLibrary.GetId<TestStickyCounterPacket>();

            _interpreter.Dispatch(stateId, 1, MessagePackSerializer.Serialize(new TestStickyStatePacket { StateId = 1, StateName = "A" }));
            _interpreter.Dispatch(counterId, 1, MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 5 }));

            Assert.That(_interpreter.TryGetSticky<TestStickyStatePacket>(out _), Is.True);
            Assert.That(_interpreter.TryGetSticky<TestStickyCounterPacket>(out _), Is.True);

            _interpreter.ClearAllSticky();

            Assert.That(_interpreter.TryGetSticky<TestStickyStatePacket>(out _), Is.False);
            Assert.That(_interpreter.TryGetSticky<TestStickyCounterPacket>(out _), Is.False);
        }

        [Test]
        public void NonStickyPacket_DoesNotReplayToLateSubscribers()
        {
            ushort chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            Assert.That(LiminalPacketLibrary.IsSticky(chatPacketId), Is.False);

            _interpreter.Dispatch(chatPacketId, 1, MessagePackSerializer.Serialize(new ChatPacket { Message = "Hello" }));

            int callCount = 0;
            object sub = new object();
            _interpreter.Subscribe<ChatPacket>((p, s) => callCount++, sub);

            // Non-sticky packet was dropped because no one was listening when it arrived
            Assert.That(callCount, Is.EqualTo(0));
        }
    }
}
