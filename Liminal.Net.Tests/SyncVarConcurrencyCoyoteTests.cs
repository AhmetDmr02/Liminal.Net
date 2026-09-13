using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [MessagePackObject]
    public struct CoyoteStatePayload : IEquatable<CoyoteStatePayload>
    {
        [Key(0)] public int Iteration;
        [Key(1)] public long TimestampTicks;
        [Key(2)] public int Checksum;

        [IgnoreMember]
        public bool IsConsistent => Checksum == (Iteration ^ 0x5A5A5A5A);

        public bool Equals(CoyoteStatePayload other) =>
            Iteration == other.Iteration &&
            TimestampTicks == other.TimestampTicks &&
            Checksum == other.Checksum;
    }

    public static class SyncVarConcurrencyCoyoteTests
    {
        [Test]
        public static async Task Coyote_SyncVar_WorkerWritesAndTickerFlush_ZeroDeadlockOrTearing()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            const int workerCount = 3;
            const int mutationsPerWorker = 20;

            var vars = new SyncVar<CoyoteStatePayload>[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                vars[i] = new SyncVar<CoyoteStatePayload>(
                    $"token_worker_{i}",
                    new CoyoteStatePayload { Iteration = 0, TimestampTicks = 0, Checksum = 0 ^ 0x5A5A5A5A },
                    syncVarManager);
            }

            var tickerVar = new SyncVar<CoyoteStatePayload>(
                "token_ticker_local",
                new CoyoteStatePayload { Iteration = 0, TimestampTicks = 0, Checksum = 0 ^ 0x5A5A5A5A },
                syncVarManager);

            int observedTornStates = 0;
            int totalValueChanges = 0;

            for (int i = 0; i < workerCount; i++)
            {
                vars[i].OnValueChanged += (oldVal, newVal) =>
                {
                    Interlocked.Increment(ref totalValueChanges);
                    if (!newVal.IsConsistent)
                    {
                        Interlocked.Increment(ref observedTornStates);
                    }
                };
            }

            var workerTasks = new Task[workerCount];
            for (int w = 0; w < workerCount; w++)
            {
                int workerIndex = w;
                var workerVar = vars[workerIndex];

                workerTasks[workerIndex] = Task.Run(async () =>
                {
                    for (int i = 1; i <= mutationsPerWorker; i++)
                    {
                        workerVar.Value = new CoyoteStatePayload
                        {
                            Iteration = i,
                            TimestampTicks = i * 1000L,
                            Checksum = i ^ 0x5A5A5A5A
                        };

                        if (i % 5 == 0)
                        {
                            workerVar.SetDirty();
                        }

                        await Task.Yield();
                    }
                });
            }

            var tickerTask = Task.Run(async () =>
            {
                for (int t = 1; t <= mutationsPerWorker; t++)
                {
                    tickerVar.Value = new CoyoteStatePayload
                    {
                        Iteration = t,
                        TimestampTicks = t * 2000L,
                        Checksum = t ^ 0x5A5A5A5A
                    };

                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerTasks);
            await tickerTask;

            syncVarManager.FlushDirty();

            Specification.Assert(observedTornStates == 0,
                $"Double-buffer spin-gate permitted {observedTornStates} torn struct states.");
            Specification.Assert(totalValueChanges > 0,
                "OnValueChanged failed to fire during concurrent mutations.");

            for (int i = 0; i < workerCount; i++)
            {
                Specification.Assert(vars[i].Value.IsConsistent,
                    $"Final state on worker {i} had corrupted checksum.");
                Specification.Assert(vars[i].Value.Iteration == mutationsPerWorker,
                    $"Worker {i} final iteration mismatch. Expected {mutationsPerWorker}, got {vars[i].Value.Iteration}.");
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_AuthorityAndExclusion_ConcurrentRace()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            var syncVar = new SyncVar<int>("token_auth_stress", 0, syncVarManager);

            const ushort clientA = 10;
            const ushort clientB = 20;
            const ushort clientC = 30;
            const int iterations = 30;

            var authWorker1 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.AddAuthority(clientA);
                    await Task.Yield();
                    syncVar.RemoveAuthority(clientA);
                    await Task.Yield();
                }
            });

            var authWorker2 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.SetAuthIds(clientB, clientC);
                    await Task.Yield();
                    syncVar.SetAuthIds(clientA, clientB, clientC);
                    await Task.Yield();
                }
            });

            var exclWorker1 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.AddExclusion(clientB);
                    await Task.Yield();
                    syncVar.RemoveExclusion(clientB);
                    await Task.Yield();
                }
            });

            var exclWorker2 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.SetExclusionIds(clientA, clientC);
                    await Task.Yield();
                    syncVar.SetExclusions(clientC);
                    await Task.Yield();
                }
            });

            var writerWorker = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    if (syncVar.HasAuthority)
                    {
                        syncVar.Value = i;
                    }

                    _ = syncVar.IsAuthorized(clientA);
                    _ = syncVar.IsExcluded(clientC);
                    _ = syncVar.ContainsExclusion(clientB);
                    await Task.Yield();
                }
            });

            var flusherTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(authWorker1, authWorker2, exclWorker1, exclWorker2, writerWorker, flusherTask);

            syncVar.SetAuthIds(clientA, clientB);
            syncVar.SetExclusionIds(clientC);

            Specification.Assert(syncVar.AuthIds.Length == 2, "AuthIds length mismatch.");
            Specification.Assert(syncVar.IsAuthorized(clientA) && syncVar.IsAuthorized(clientB), "Expected clients A and B to remain authorized.");
            Specification.Assert(syncVar.IsExcluded(clientC), "Expected client C to remain excluded.");
        }

        [Test]
        public static async Task Coyote_SyncVar_ClientRequestAndBatchEcho_PipelineConvergence()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            const ushort authorizedClientId = 42;
            transport.TriggerClientConnected(authorizedClientId);

            var serverVar = new SyncVar<int>("token_pipeline", 100, syncVarManager);
            serverVar.AddAuthority(authorizedClientId);

            byte[] BuildRequestPayload(ushort varId, uint version, int value)
            {
                var buffer = new ArrayBufferWriter<byte>(128);
                var writer = new MessagePackWriter(buffer);
                writer.WriteArrayHeader(1);
                writer.WriteUInt16(varId);
                writer.WriteUInt32(version);

                var valBuffer = new ArrayBufferWriter<byte>(32);
                MessagePackSerializer.Serialize(valBuffer, value);
                writer.WriteInt32(valBuffer.WrittenCount);
                writer.WriteRaw(valBuffer.WrittenSpan);

                writer.Flush();
                return buffer.WrittenSpan.ToArray();
            }

            int lastSeenValue = 100;
            serverVar.OnValueChanged += (oldVal, newVal) =>
            {
                Volatile.Write(ref lastSeenValue, newVal);
            };

            const int iterations = 15;

            var clientWorker = Task.Run(async () =>
            {
                for (int i = 1; i <= iterations; i++)
                {
                    byte[] payload = BuildRequestPayload(serverVar.Id, (uint)i, 200 + i);
                    var packet = new SyncVarSlabClientRequestPacket(new ReadOnlySequence<byte>(payload));

                    netManager.Interpreter.Dispatch(
                        LiminalPacketLibrary.GetId<SyncVarSlabClientRequestPacket>(),
                        authorizedClientId,
                        MessagePackSerializer.Serialize(packet));

                    await Task.Yield();
                }
            });

            var serverTicker = Task.Run(async () =>
            {
                for (int i = 0; i < iterations * 2; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(clientWorker, serverTicker);
            syncVarManager.FlushDirty();

            Specification.Assert(Volatile.Read(ref lastSeenValue) == 200 + iterations,
                $"Pipeline mismatch. Expected {200 + iterations}, got {lastSeenValue}.");
            Specification.Assert(serverVar.Version >= (uint)iterations,
                $"SyncVar version mismatch. Version {serverVar.Version} < {iterations}.");
        }
    }
}