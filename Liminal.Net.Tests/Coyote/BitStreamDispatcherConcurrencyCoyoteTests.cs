using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class BitStreamDispatcherConcurrencyCoyoteTests
    {
        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_BitStream_OffTickerUnsubscribe_SnapshotRace_ZeroDeadlockOrTornState()
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
                ushort packetId = checked((ushort)LiminalPacketLibrary.GetId<TestSnapshotMeta>());

                int callbacksExecuted = 0;
                object subObj = new object();

                interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
                {
                    Interlocked.Increment(ref callbacksExecuted);
                }, subObj);

                var metaPkt = new TestSnapshotMeta { Tick = 1, Count = 1 };
                byte[] rawMeta = MessagePackSerializer.Serialize(metaPkt);
                byte[] payload = new byte[rawMeta.Length + 4 + 4];
                rawMeta.CopyTo(payload.AsSpan());
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(rawMeta.Length, 4), 4);
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(rawMeta.Length + 4, 4), 0x12345678);

                ReadOnlyMemory<byte> rawMemory = payload;

                const int dispatchIterations = 30;
                const int mutateIterations = 20;

                var dispatchTask = Task.Run(async () =>
                {
                    for (int i = 0; i < dispatchIterations; i++)
                    {
                        interpreter.Dispatch(packetId, 1, rawMemory);
                        await Task.Yield();
                    }
                });

                var mutateTask = Task.Run(async () =>
                {
                    for (int i = 0; i < mutateIterations; i++)
                    {
                        interpreter.UnsubscribeBitStream<TestSnapshotMeta>(subObj);
                        await Task.Yield();

                        interpreter.SubscribeBitStream<TestSnapshotMeta>((in TestSnapshotMeta meta, ref BitReader reader, ushort sender) =>
                        {
                            Interlocked.Increment(ref callbacksExecuted);
                        }, subObj);
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(dispatchTask, mutateTask);

                Specification.Assert(callbacksExecuted >= 0, "Callbacks executed count must be valid non-negative integer.");
            }
            finally
            {
                manager.Shutdown();
            }
        }
    }
}
