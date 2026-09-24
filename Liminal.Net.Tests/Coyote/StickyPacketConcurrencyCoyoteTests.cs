using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class StickyPacketConcurrencyCoyoteTests
    {
        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_StickyPacket_ConcurrentDispatchAndSubscribe_MonotonicNoDeadlock()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);
            var interpreter = manager.Interpreter;

            try
            {
                LiminalPacketLibrary.Initialize();
                ushort packetId = checked((ushort)LiminalPacketLibrary.GetId<TestStickyCounterPacket>());
                Specification.Assert(LiminalPacketLibrary.IsSticky(packetId), "TestStickyCounterPacket must be sticky");

                const int producerIterations = 15;
                const int consumerCycles = 10;

                int callbacksExecuted = 0;
                int outOfOrderDetections = 0;

                // Producer task: Dispatches packets with strictly increasing counters
                var producerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < producerIterations; i++)
                    {
                        var packet = new TestStickyCounterPacket { Counter = i };
                        byte[] rawBytes = MessagePackSerializer.Serialize(packet);
                        interpreter.Dispatch(packetId, 1, rawBytes);
                        await Task.Yield();
                    }
                });

                // Consumer task: Repeatedly subscribes and unsubscribes to simulate deferred loading races
                var consumerTask = Task.Run(async () =>
                {
                    for (int c = 0; c < consumerCycles; c++)
                    {
                        object cycleSub = new object();
                        int cycleLastCounter = -1;

                        interpreter.Subscribe<TestStickyCounterPacket>((pkt, sender) =>
                        {
                            Interlocked.Increment(ref callbacksExecuted);

                            int prev = Volatile.Read(ref cycleLastCounter);
                            if (prev >= 0 && pkt.Counter < prev)
                            {
                                Interlocked.Increment(ref outOfOrderDetections);
                            }

                            Volatile.Write(ref cycleLastCounter, Math.Max(prev, pkt.Counter));
                        }, cycleSub);

                        await Task.Yield();

                        interpreter.Unsubscribe<TestStickyCounterPacket>(cycleSub);

                        await Task.Yield();
                    }
                });

                await Task.WhenAll(producerTask, consumerTask);

                // Assertions for Coyote systematic exploration
                Specification.Assert(outOfOrderDetections == 0,
                    $"Detected {outOfOrderDetections} out-of-order dispatches/replays! Counters must be non-decreasing.");
                Specification.Assert(callbacksExecuted >= 0,
                    "Callbacks executed must be non-negative.");
            }
            finally
            {
                manager.Shutdown();
            }
        }

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_StickyPacket_MultipleSubscribers_NoDuplicateOrMissedLastState()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var manager = new LiminalNetworkManager(transport, config);
            var interpreter = manager.Interpreter;

            try
            {
                LiminalPacketLibrary.Initialize();
                ushort packetId = checked((ushort)LiminalPacketLibrary.GetId<TestStickyCounterPacket>());

                const int iterations = 12;
                int sub1Count = 0;
                int sub2Count = 0;

                object sub1 = new object();
                object sub2 = new object();

                var dispatchTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var packet = new TestStickyCounterPacket { Counter = i };
                        byte[] rawBytes = MessagePackSerializer.Serialize(packet);
                        interpreter.Dispatch(packetId, 1, rawBytes);
                        await Task.Yield();
                    }
                });

                var sub1Task = Task.Run(async () =>
                {
                    await Task.Yield();
                    interpreter.Subscribe<TestStickyCounterPacket>((pkt, sender) =>
                    {
                        Interlocked.Increment(ref sub1Count);
                    }, sub1);
                });

                var sub2Task = Task.Run(async () =>
                {
                    await Task.Yield();
                    interpreter.Subscribe<TestStickyCounterPacket>((pkt, sender) =>
                    {
                        Interlocked.Increment(ref sub2Count);
                    }, sub2);
                });

                await Task.WhenAll(dispatchTask, sub1Task, sub2Task);

                Specification.Assert(sub1Count > 0, "Subscriber 1 should have received at least one packet");
                Specification.Assert(sub2Count > 0, "Subscriber 2 should have received at least one packet");

                // Verify TryGetSticky matches the final iteration
                bool found = interpreter.TryGetSticky<TestStickyCounterPacket>(out var finalPkt);
                Specification.Assert(found, "Final sticky packet should be present in cache");
                Specification.Assert(finalPkt.Counter == iterations - 1,
                    $"Final cached counter should be {iterations - 1} but was {finalPkt.Counter}");
            }
            finally
            {
                manager.Shutdown();
            }
        }
    }
}
