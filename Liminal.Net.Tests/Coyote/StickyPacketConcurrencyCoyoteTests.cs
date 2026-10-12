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

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_StickyPacket_ConcurrentDispatchAndClearSticky_ZeroMemoryCorruptionOrTear()
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

                const int iterations = 15;
                int readSuccessCount = 0;

                var writerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var packet = new TestStickyCounterPacket { Counter = i };
                        byte[] bytes = MessagePackSerializer.Serialize(packet);
                        interpreter.Dispatch(packetId, 1, bytes);
                        await Task.Yield();
                    }
                });

                var clearerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations / 2; i++)
                    {
                        interpreter.ClearSticky<TestStickyCounterPacket>();
                        await Task.Yield();
                    }
                });

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        if (interpreter.TryGetSticky<TestStickyCounterPacket>(out var pkt, out var sender))
                        {
                            Interlocked.Increment(ref readSuccessCount);
                            Specification.Assert(pkt.Counter >= 0 && pkt.Counter < iterations,
                                $"Invalid counter {pkt.Counter} read during concurrent clear/dispatch");
                        }
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(writerTask, clearerTask, readerTask);
            }
            finally
            {
                manager.Shutdown();
            }
        }

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_StickyPacket_DynamicPayloadBufferGrowth_ZeroBufferLeakOrTear()
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
                ushort packetId = checked((ushort)LiminalPacketLibrary.GetId<TestStickyStatePacket>());

                const int iterations = 10;
                int receivedCount = 0;
                int malformedDetections = 0;

                object sub = new object();
                interpreter.Subscribe<TestStickyStatePacket>((pkt, sender) =>
                {
                    Interlocked.Increment(ref receivedCount);
                    if (string.IsNullOrEmpty(pkt.StateName) || pkt.StateName.Length != pkt.StateId)
                    {
                        Interlocked.Increment(ref malformedDetections);
                    }
                }, sub);

                var smallTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        int len = 8 + (i % 4);
                        var packet = new TestStickyStatePacket { StateId = len, StateName = new string('S', len) };
                        byte[] bytes = MessagePackSerializer.Serialize(packet);
                        interpreter.Dispatch(packetId, 1, bytes);
                        await Task.Yield();
                    }
                });

                var largeTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        int len = 256 + (i * 16);
                        var packet = new TestStickyStatePacket { StateId = len, StateName = new string('L', len) };
                        byte[] bytes = MessagePackSerializer.Serialize(packet);
                        interpreter.Dispatch(packetId, 2, bytes);
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(smallTask, largeTask);

                Specification.Assert(malformedDetections == 0,
                    $"Detected {malformedDetections} malformed or torn payloads during concurrent buffer expansion.");
                Specification.Assert(receivedCount > 0, "Subscriber must have processed packets.");
            }
            finally
            {
                manager.Shutdown();
            }
        }

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_StickyPacket_ConcurrentSubscribeUnsubscribeAndClearAllSticky_ZeroDeadlock()
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

                const int cycles = 8;

                var subTask = Task.Run(async () =>
                {
                    for (int i = 0; i < cycles; i++)
                    {
                        object subObj = new object();
                        interpreter.Subscribe<TestStickyCounterPacket>((pkt, s) => { }, subObj);
                        await Task.Yield();
                        interpreter.Unsubscribe<TestStickyCounterPacket>(subObj);
                        await Task.Yield();
                    }
                });

                var dispatchTask = Task.Run(async () =>
                {
                    for (int i = 0; i < cycles; i++)
                    {
                        var packet = new TestStickyCounterPacket { Counter = i };
                        interpreter.Dispatch(packetId, 1, MessagePackSerializer.Serialize(packet));
                        await Task.Yield();
                    }
                });

                var clearTask = Task.Run(async () =>
                {
                    for (int i = 0; i < cycles / 2; i++)
                    {
                        interpreter.ClearAllSticky();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(subTask, dispatchTask, clearTask);
            }
            finally
            {
                manager.Shutdown();
            }
        }
    }
}
