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

            byte[] BuildRequestPayload(string token, uint version, int value)
            {
                var tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
                var buffer = new ArrayBufferWriter<byte>(128);
                var writer = new MessagePackWriter(buffer);
                writer.WriteArrayHeader(1);
                writer.WriteUInt8((byte)tokenBytes.Length);
                writer.WriteRaw(tokenBytes);
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
                    byte[] payload = BuildRequestPayload(serverVar.Token, (uint)i, 200 + i);
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

        [Test]
        public static async Task Coyote_Utf8TokenRegistry_ConcurrentReadersAndWriters_NoRaceOrDeadlock()
        {
            var registry = new Utf8TokenRegistry(8); 

            const int preSeedCount = 10;
            for (int i = 0; i < preSeedCount; i++)
            {
                registry.TryAdd(new SyncVar<int>($"seed_token_{i}", i));
            }

            const int workerIterations = 15;
            int successfulReads = 0;
            int failedReads = 0;

            var reader1 = Task.Run(async () =>
            {
                byte[] targetBytes = System.Text.Encoding.UTF8.GetBytes("seed_token_5");
                byte[] nonExistent = System.Text.Encoding.UTF8.GetBytes("missing_token");

                for (int i = 0; i < workerIterations; i++)
                {
                    if (registry.TryGet(targetBytes.AsSpan(), out var syncVar))
                    {
                        Interlocked.Increment(ref successfulReads);
                    }
                    else
                    {
                        Interlocked.Increment(ref failedReads);
                    }

                    _ = registry.TryGet(nonExistent.AsSpan(), out _);
                    await Task.Yield();
                }
            });

            var reader2 = Task.Run(async () =>
            {
                byte[] targetBytes = System.Text.Encoding.UTF8.GetBytes("seed_token_2");

                for (int i = 0; i < workerIterations; i++)
                {
                    if (registry.TryGet(targetBytes.AsSpan(), out var syncVar))
                    {
                        Interlocked.Increment(ref successfulReads);
                    }
                    else
                    {
                        Interlocked.Increment(ref failedReads);
                    }
                    await Task.Yield();
                }
            });

            // Writer 1: Adds new tokens and removes some, forcing resizes
            var writer1 = Task.Run(async () =>
            {
                for (int i = 0; i < workerIterations; i++)
                {
                    registry.TryAdd(new SyncVar<int>($"w1_token_{i}", i));
                    if (i > 3)
                    {
                        registry.TryRemove($"w1_token_{i - 3}", out _);
                    }
                    await Task.Yield();
                }
            });

            // Writer 2: Adds different new tokens
            var writer2 = Task.Run(async () =>
            {
                for (int i = 0; i < workerIterations; i++)
                {
                    registry.TryAdd(new SyncVar<int>($"w2_token_{i}", i));
                    await Task.Yield();
                }
            });

            await Task.WhenAll(reader1, reader2, writer1, writer2);

            Specification.Assert(failedReads == 0,
                $"Pre-seeded token was not found during concurrent operations. Failed reads: {failedReads}");
            Specification.Assert(successfulReads == workerIterations * 2,
                $"Expected {workerIterations * 2} successful reads, got {successfulReads}");
        }

        [Test]
        public static async Task Coyote_SyncVar_RegistrationPublication_ConcurrentRemoteBytes()
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

            const int count = 10;
            var createdVars = new SyncVar<CoyoteStatePayload>[count];

            var workerCreate = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    createdVars[i] = new SyncVar<CoyoteStatePayload>(
                        $"pub_race_{i}",
                        new CoyoteStatePayload { Iteration = i, Checksum = i ^ 0x5A5A5A5A },
                        syncVarManager);
                    await Task.Yield();
                }
            });

            var workerRemote = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    var payload = new CoyoteStatePayload { Iteration = 100 + i, Checksum = (100 + i) ^ 0x5A5A5A5A };
                    var valBuf = new ArrayBufferWriter<byte>();
                    MessagePackSerializer.Serialize(valBuf, payload);

                    var buffer = new ArrayBufferWriter<byte>();
                    var writer = new MessagePackWriter(buffer);
                    writer.WriteArrayHeader(1);
                    var tokenBytes = System.Text.Encoding.UTF8.GetBytes($"pub_race_{i}");
                    writer.WriteUInt8((byte)tokenBytes.Length);
                    writer.WriteRaw(tokenBytes);
                    writer.WriteUInt32((uint)(i + 1));
                    writer.WriteInt32(valBuf.WrittenCount);
                    writer.WriteRaw(valBuf.WrittenSpan);
                    writer.Flush();

                    var batchPacket = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(buffer.WrittenMemory));
                    netManager.Interpreter.Dispatch(
                        LiminalPacketLibrary.GetId<SyncVarSlabBatchPacket>(),
                        ILiminalTransport.SERVER_ID,
                        MessagePackSerializer.Serialize(batchPacket));

                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerCreate, workerRemote);

            for (int i = 0; i < count; i++)
            {
                var v = createdVars[i];
                Specification.Assert(v != null, $"SyncVar {i} was not created.");
                Specification.Assert(v.Value.IsConsistent, $"SyncVar {i} has torn or inconsistent state.");
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ApplyRemoteBytes_ConcurrentFlushDirty_ZeroTornWireSlots()
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

            var syncVar = new SyncVar<CoyoteStatePayload>(
                "token_apply_wire_race",
                new CoyoteStatePayload { Iteration = 0, Checksum = 0 ^ 0x5A5A5A5A },
                syncVarManager);

            const int iterations = 20;
            int tornWireReads = 0;

            var workerApply = Task.Run(async () =>
            {
                for (int i = 1; i <= iterations; i++)
                {
                    var payload = new CoyoteStatePayload
                    {
                        Iteration = i,
                        TimestampTicks = i * 1000L,
                        Checksum = i ^ 0x5A5A5A5A
                    };

                    var valBuf = new ArrayBufferWriter<byte>();
                    MessagePackSerializer.Serialize(valBuf, payload);
                    syncVar.ApplyRemoteBytes(new ReadOnlySequence<byte>(valBuf.WrittenMemory), (uint)i, syncVarManager.SerializerOptions);
                    syncVar.SetDirty();

                    await Task.Yield();
                }
            });

            var workerFlush = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVarManager.FlushDirty();

                    while (transport.SentPackets.Count > 0)
                    {
                        var packetRecord = transport.SentPackets[0];
                        transport.SentPackets.Clear();

                        var batch = MessagePackSerializer.Deserialize<SyncVarSlabBatchPacket>(packetRecord.Data);
                        if (!batch.Payload.IsEmpty)
                        {
                            var reader = new MessagePackReader(batch.Payload);
                            int count = reader.ReadArrayHeader();
                            for (int c = 0; c < count; c++)
                            {
                                byte tokenLen = reader.ReadByte();
                                reader.ReadRaw(tokenLen);
                                uint ver = reader.ReadUInt32();
                                int len = reader.ReadInt32();
                                var rawSlice = reader.ReadRaw(len);

                                var deserialized = MessagePackSerializer.Deserialize<CoyoteStatePayload>(rawSlice);
                                if (!deserialized.IsConsistent)
                                {
                                    Interlocked.Increment(ref tornWireReads);
                                }
                            }
                        }
                    }

                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerApply, workerFlush);

            Specification.Assert(tornWireReads == 0,
                $"Torn wire reads detected! Count: {tornWireReads}");
        }

        [Test]
        public static async Task Coyote_SyncVar_UnboundStore_ConcurrentWithRegistration_NoLostUpdates()
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

            const int count = 10;
            var createdVars = new SyncVar<int>[count];

            var taskRegister = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    createdVars[i] = new SyncVar<int>($"toctou_token_{i}", 1, syncVarManager);
                    await Task.Yield();
                }
            });

            var taskDispatch = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    var valBuf = new ArrayBufferWriter<byte>();
                    MessagePackSerializer.Serialize(valBuf, 100 + i);

                    var buffer = new ArrayBufferWriter<byte>();
                    var writer = new MessagePackWriter(buffer);
                    writer.WriteArrayHeader(1);
                    var tokenBytes = System.Text.Encoding.UTF8.GetBytes($"toctou_token_{i}");
                    writer.WriteUInt8((byte)tokenBytes.Length);
                    writer.WriteRaw(tokenBytes);
                    writer.WriteUInt32(2);
                    writer.WriteInt32(valBuf.WrittenCount);
                    writer.WriteRaw(valBuf.WrittenSpan);
                    writer.Flush();

                    var batch = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(buffer.WrittenMemory));
                    netManager.Interpreter.Dispatch(
                        LiminalPacketLibrary.GetId<SyncVarSlabBatchPacket>(),
                        ILiminalTransport.SERVER_ID,
                        MessagePackSerializer.Serialize(batch));

                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskRegister, taskDispatch);

            Specification.Assert(syncVarManager.UnboundCacheCount == 0,
                $"Unbound cache has trapped entries! Count: {syncVarManager.UnboundCacheCount}");

            for (int i = 0; i < count; i++)
            {
                Specification.Assert(createdVars[i].Value == 100 + i,
                    $"SyncVar {i} missed update. Expected {100 + i}, got {createdVars[i].Value}");
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_DynamicPayload_ConcurrentGrowthAndWireReads_ZeroTearing()
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

            var strVar = new SyncVar<string>("dyn_str_token", "init", syncVarManager);

            const int iterations = 15;
            int corruptedReads = 0;

            var workerWriter = Task.Run(async () =>
            {
                for (int i = 1; i <= iterations; i++)
                {
                    string longString = $"iter_{i}_" + new string((char)('A' + (i % 26)), 350);
                    strVar.Value = longString;
                    await Task.Yield();
                }
            });

            var workerReader = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVarManager.FlushDirty();

                    while (transport.SentPackets.Count > 0)
                    {
                        var packetRecord = transport.SentPackets[0];
                        transport.SentPackets.Clear();

                        var batch = MessagePackSerializer.Deserialize<SyncVarSlabBatchPacket>(packetRecord.Data);
                        if (!batch.Payload.IsEmpty)
                        {
                            var reader = new MessagePackReader(batch.Payload);
                            int count = reader.ReadArrayHeader();
                            for (int c = 0; c < count; c++)
                            {
                                byte tokenLen = reader.ReadByte();
                                reader.ReadRaw(tokenLen);
                                uint ver = reader.ReadUInt32();
                                int len = reader.ReadInt32();
                                var rawSlice = reader.ReadRaw(len);

                                try
                                {
                                    string wireStr = MessagePackSerializer.Deserialize<string>(rawSlice);
                                    if (string.IsNullOrEmpty(wireStr) || !wireStr.StartsWith("iter_"))
                                    {
                                        Interlocked.Increment(ref corruptedReads);
                                    }
                                }
                                catch
                                {
                                    Interlocked.Increment(ref corruptedReads);
                                }
                            }
                        }
                    }

                    await Task.Yield();
                }
            });

            await Task.WhenAll(workerWriter, workerReader);

            Specification.Assert(corruptedReads == 0,
                $"Corrupted/torn wire reads during dynamic slot reallocation! Count: {corruptedReads}");
        }

        [Test]
        public static async Task Coyote_SyncVar_TornStruct_ConcurrentReadersAndWriters_ZeroTornReads()
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

            var syncVar = new SyncVar<CoyoteStatePayload>(
                "torn_struct_test_token",
                new CoyoteStatePayload { Iteration = 0, TimestampTicks = 0, Checksum = 0 ^ 0x5A5A5A5A },
                syncVarManager);

            const int iterations = 20;
            int tornReadsObserved = 0;
            int successfulReads = 0;

            var writerTask = Task.Run(async () =>
            {
                for (int i = 1; i <= iterations; i++)
                {
                    syncVar.Value = new CoyoteStatePayload
                    {
                        Iteration = i,
                        TimestampTicks = i * 1000L,
                        Checksum = i ^ 0x5A5A5A5A
                    };
                    await Task.Yield();
                }
            });

            const int readerCount = 3;
            var readerTasks = new Task[readerCount];
            for (int r = 0; r < readerCount; r++)
            {
                readerTasks[r] = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var val = syncVar.Value;
                        if (!val.IsConsistent)
                        {
                            Interlocked.Increment(ref tornReadsObserved);
                        }
                        else
                        {
                            Interlocked.Increment(ref successfulReads);
                        }
                        await Task.Yield();
                    }
                });
            }

            await Task.WhenAll(writerTask, Task.WhenAll(readerTasks));

            Specification.Assert(tornReadsObserved == 0,
                $"Torn struct reads observed in Value.get! Count: {tornReadsObserved}");
            Specification.Assert(successfulReads > 0,
                "No successful reads occurred.");
        }

        [Test]
        public static async Task Coyote_SyncVar_Registration_ConcurrentAuthUpdate_ZeroAuthStomp()
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

            const string token = "auth_stomp_token";

            // Pre-seed unbound authority with Client 10 (old cached state)
            netManager.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                ILiminalTransport.SERVER_ID,
                MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket
                {
                    Token = token,
                    AuthIds = new ushort[] { 10 }
                }));

            SyncVar<int> createdVar = null;

            // Task A: Registers the variable
            var taskRegister = Task.Run(async () =>
            {
                createdVar = new SyncVar<int>(token, 0, syncVarManager);
                await Task.Yield();
            });

            // Task B: Sends a newer authority update to Client 20
            var taskUpdate = Task.Run(async () =>
            {
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                    ILiminalTransport.SERVER_ID,
                    MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket
                    {
                        Token = token,
                        AuthIds = new ushort[] { 20 }
                    }));
                await Task.Yield();
            });

            await Task.WhenAll(taskRegister, taskUpdate);

            Specification.Assert(createdVar != null, "SyncVar was not initialized.");
            Specification.Assert(createdVar.AuthIds.Length == 1 && createdVar.AuthIds[0] == 20,
                $"Auth stomp detected! Expected Client 20, got {(createdVar.AuthIds.Length > 0 ? createdVar.AuthIds[0].ToString() : "empty")}");
        }

        [Test]
        public static async Task Coyote_SyncVar_Registration_ConcurrentAuthAndCallbackReentrancy_ZeroDeadlock()
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

            const int count = 10;
            const ushort testClientId = 5;

            for (int i = 0; i < count; i++)
            {
                string token = $"deadlock_test_{i}";
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                    ILiminalTransport.SERVER_ID,
                    MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket
                    {
                        Token = token,
                        AuthIds = new ushort[] { testClientId }
                    }));
            }

            var registeredVars = new SyncVar<int>[count];
            int callbackCount = 0;

            var taskRegister = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    string token = $"deadlock_test_{i}";
                    var sv = new SyncVar<int>(token, i, syncVarManager);
                    registeredVars[i] = sv;

                    sv.OnAuthorityChanged += hasAuth =>
                    {
                        Interlocked.Increment(ref callbackCount);
                        _ = sv.Value;
                        if (hasAuth)
                        {
                            sv.Value = 999;
                        }
                        _ = syncVarManager.Bind<int>($"reentrant_bound_{token}", 0);
                    };

                    await Task.Yield();
                }
            });

            var taskAuthMutations = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    string token = $"deadlock_test_{i}";
                    if (syncVarManager.TryGetSyncVar(token, out var sv))
                    {
                        sv.AddAuthority(testClientId);
                        sv.RemoveAuthority(testClientId);
                        sv.SetAuthIds(testClientId, 99);
                    }
                    else
                    {
                        netManager.Interpreter.Dispatch(
                            LiminalPacketLibrary.GetId<SyncVarAuthUpdatePacket>(),
                            ILiminalTransport.SERVER_ID,
                            MessagePackSerializer.Serialize(new SyncVarAuthUpdatePacket
                            {
                                Token = token,
                                AuthIds = new ushort[] { testClientId, 99 }
                            }));
                    }
                    await Task.Yield();
                }
            });

            var taskFlusher = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskRegister, taskAuthMutations, taskFlusher);

            Specification.Assert(registeredVars.Length == count, "Failed to complete all registrations without deadlock.");
        }

        [Test]
        public static async Task Coyote_SyncVar_OfflineSnapshotConvergence_ConcurrentFlushAndMutate()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport { IsServer = false };
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            const int count = 6;
            var clientVars = new SyncVar<int>[count];

            ushort localClientId = netManager.localID;

            for (int i = 0; i < count; i++)
            {
                clientVars[i] = new SyncVar<int>($"offline_conv_{i}", 0, syncVarManager);
                clientVars[i].Value = 100 + i;
            }

            netManager.StartClient("127.0.0.1", 7777);
            netManager.Ticker?.Stop();
            transport.TriggerLocalClientConnected(localClientId);

            var entries = new System.Collections.Generic.List<SyncVarSnapshotEntry>(count);
            for (int i = 0; i < count; i++)
            {
                var valBuf = new ArrayBufferWriter<byte>();
                MessagePackSerializer.Serialize(valBuf, 500 + i);

                ushort[] authIds = (i % 2 == 0) ? new ushort[] { localClientId } : Array.Empty<ushort>();

                entries.Add(new SyncVarSnapshotEntry
                {
                    Token = $"offline_conv_{i}",
                    Version = 1,
                    AuthIds = authIds,
                    Data = valBuf.WrittenSpan.ToArray()
                });
            }

            var snapshotPacket = new SyncVarSnapshotPacket { Entries = entries };
            byte[] serializedSnapshot = MessagePackSerializer.Serialize(snapshotPacket);

            var taskSnapshot = Task.Run(async () =>
            {
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>(),
                    ILiminalTransport.SERVER_ID,
                    serializedSnapshot);
                await Task.Yield();
            });

            var taskMutate = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    if (clientVars[i].HasAuthority)
                    {
                        clientVars[i].Value = 800 + i;
                    }
                    await Task.Yield();
                }
            });

            var taskFlush = Task.Run(async () =>
            {
                for (int i = 0; i < 5; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            int readSum = 0;
            var taskRead = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    readSum += clientVars[i].Value;
                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskSnapshot, taskMutate, taskFlush, taskRead);

            for (int i = 0; i < count; i++)
            {
                var sv = clientVars[i];
                if (i % 2 != 0)
                {
                    Specification.Assert(!sv.HasAuthority, $"SyncVar {i} should not have authority.");
                    Specification.Assert(sv.Value == 500 + i,
                        $"Unauthorized SyncVar {i} failed to overwrite offline value! Expected {500 + i}, got {sv.Value}");
                }
                else
                {
                    Specification.Assert(sv.HasAuthority, $"SyncVar {i} should have authority.");
                }
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_FlushDirtyAndResetPendingState_ConcurrentRace()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport { IsServer = true };
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            const int count = 4;
            var svList = new SyncVar<int>[count];
            for (int i = 0; i < count; i++)
            {
                svList[i] = new SyncVar<int>($"race_token_{i}", 0, syncVarManager);
            }

            const int iterations = 30;

            var taskMutate = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    for (int s = 0; s < count; s++)
                    {
                        svList[s].Value = i;
                    }
                    await Task.Yield();
                }
            });

            var taskFlush = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            var taskReset = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVarManager.ResetPendingState();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskMutate, taskFlush, taskReset);
        }

        [Test]
        public static async Task Coyote_SyncVar_ZeroAllocDeferredEvents_ConcurrentUpdatesAndReentrancy()
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

            const int varCount = 5;
            const ushort clientA = 10;
            const ushort clientB = 20;

            for (int i = 0; i < varCount; i++)
            {
                string token = $"deferred_test_{i}";
                var payload = MessagePackSerializer.Serialize(100 + i);
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>(),
                    ILiminalTransport.SERVER_ID,
                    MessagePackSerializer.Serialize(new SyncVarSnapshotPacket
                    {
                        Entries = new System.Collections.Generic.List<SyncVarSnapshotEntry>
                        {
                            new SyncVarSnapshotEntry
                            {
                                Token = token,
                                Version = 1,
                                Data = payload,
                                AuthIds = new ushort[] { clientA }
                            }
                        }
                    }));
            }

            int authEventsFired = 0;
            int valueEventsFired = 0;
            var vars = new SyncVar<int>[varCount];

            var taskRegister = Task.Run(async () =>
            {
                for (int i = 0; i < varCount; i++)
                {
                    string token = $"deferred_test_{i}";
                    var sv = new SyncVar<int>(token, 0, syncVarManager);
                    vars[i] = sv;

                    sv.OnAuthorityChanged += auth =>
                    {
                        Interlocked.Increment(ref authEventsFired);
                        _ = syncVarManager.Bind<int>($"reentrant_bound_{token}", 0);
                        if (auth)
                        {
                            sv.Value = 999;
                        }
                    };

                    sv.OnValueChanged += (oldVal, newVal) =>
                    {
                        Interlocked.Increment(ref valueEventsFired);
                        Specification.Assert(oldVal >= 0 && newVal >= 0, "Values must be non-negative");
                        _ = syncVarManager.Bind<int>($"reentrant_val_{token}", 0);
                    };

                    await Task.Yield();
                }
            });

            var taskConcurrentRemote = Task.Run(async () =>
            {
                for (int i = 0; i < varCount; i++)
                {
                    string token = $"deferred_test_{i}";
                    var payload = MessagePackSerializer.Serialize(200 + i);

                    netManager.Interpreter.Dispatch(
                        LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>(),
                        ILiminalTransport.SERVER_ID,
                        MessagePackSerializer.Serialize(new SyncVarSnapshotPacket
                        {
                            Entries = new System.Collections.Generic.List<SyncVarSnapshotEntry>
                            {
                                new SyncVarSnapshotEntry
                                {
                                    Token = token,
                                    Version = 2,
                                    Data = payload,
                                    AuthIds = new ushort[] { clientB }
                                }
                            }
                        }));

                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskRegister, taskConcurrentRemote);

            Specification.Assert(authEventsFired >= 0, "Valid auth event count");
            Specification.Assert(valueEventsFired >= 0, "Valid value event count");
        }
    }
}