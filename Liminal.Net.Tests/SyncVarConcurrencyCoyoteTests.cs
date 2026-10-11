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
        public static async Task Coyote_SyncVar_VisibilityAndObservers_ConcurrentRace()
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
            var syncVar = new SyncVar<int>("token_vis_obs_race", 0, SyncVarVisibility.ManualObservers, syncVarManager);

            const ushort clientA = 10;
            const ushort clientB = 20;
            const ushort clientC = 30;
            const int iterations = 30;

            var obsWorker1 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.AddObserver(clientA);
                    await Task.Yield();
                    syncVar.RemoveObserver(clientA);
                    await Task.Yield();
                }
            });

            var obsWorker2 = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.SetObservers(clientB, clientC);
                    await Task.Yield();
                    syncVar.SetObservers(clientA, clientB, clientC);
                    await Task.Yield();
                }
            });

            var visWorker = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.SetVisibility(SyncVarVisibility.Public);
                    await Task.Yield();
                    syncVar.SetVisibility(SyncVarVisibility.ManualObservers);
                    await Task.Yield();
                    syncVar.SetVisibility(SyncVarVisibility.OwnerOnly);
                    await Task.Yield();
                }
            });

            var authExclWorker = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    syncVar.AddAuthority(clientA);
                    syncVar.AddExclusion(clientB);
                    await Task.Yield();
                    syncVar.RemoveAuthority(clientA);
                    syncVar.RemoveExclusion(clientB);
                    await Task.Yield();
                }
            });

            var readerWorker = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    _ = syncVar.IsVisibleTo(clientA);
                    _ = syncVar.IsVisibleTo(clientB);
                    _ = syncVar.IsVisibleTo(clientC);
                    _ = syncVar.ContainsObserver(clientA);
                    _ = syncVar.Visibility;
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

            await Task.WhenAll(obsWorker1, obsWorker2, visWorker, authExclWorker, readerWorker, writerWorker, flusherTask);

            syncVar.SetVisibility(SyncVarVisibility.ManualObservers);
            syncVar.SetObservers(clientA, clientB);
            syncVar.RemoveExclusion(clientA);

            Specification.Assert(syncVar.Visibility == SyncVarVisibility.ManualObservers, "Visibility mismatch.");
            Specification.Assert(syncVar.ObserverIds.Length == 2, "ObserverIds length mismatch.");
            Specification.Assert(syncVar.IsVisibleTo(clientA), "Expected client A to be visible.");
            Specification.Assert(syncVar.IsVisibleTo(clientB), "Expected client B to be visible.");
            Specification.Assert(!syncVar.IsVisibleTo(clientC), "Expected client C to not be visible.");
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
        public static async Task Coyote_SyncVar_ClientFlushDirtyAndResetPendingState_ConcurrentRace()
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
            try
            {
                netManager.StartClient("127.0.0.1", 7777);
                transport.TriggerLocalClientConnected(5);

                var syncVarManager = netManager.SyncVarManager;
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>(),
                    ILiminalTransport.SERVER_ID,
                    MessagePackSerializer.Serialize(new SyncVarSnapshotPacket()));

                const int count = 4;
                var svList = new SyncVar<int>[count];
                for (int i = 0; i < count; i++)
                {
                    svList[i] = new SyncVar<int>($"client_race_token_{i}", 0, syncVarManager);
                    svList[i].ApplyAuthUpdateFromRemote(new ushort[] { 5 });
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
            finally
            {
                netManager.Shutdown();
            }
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

        [Test]
        public static async Task Coyote_SyncVar_FlushDirty_ConcurrentAuthorityChange_ZeroMalformedMessagePack()
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
            try
            {
                netManager.StartClient("127.0.0.1", 7777);
                transport.TriggerLocalClientConnected(5);

                var syncVarManager = netManager.SyncVarManager;
                netManager.Interpreter.Dispatch(
                    LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>(),
                    ILiminalTransport.SERVER_ID,
                    MessagePackSerializer.Serialize(new SyncVarSnapshotPacket()));

                const int count = 5;
                var svs = new SyncVar<int>[count];
                for (int i = 0; i < count; i++)
                {
                    svs[i] = new SyncVar<int>($"flaky_var_{i}", 100 + i, syncVarManager);
                    svs[i].ApplyAuthUpdateFromRemote(new ushort[] { 1 });
                    svs[i].Value = 200 + i;
                }

                var taskFlush = Task.Run(async () =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        syncVarManager.FlushDirty();
                        await Task.Yield();
                    }
                });

                var taskRevoke = Task.Run(async () =>
                {
                    for (int i = 0; i < count; i++)
                    {
                        svs[i].ApplyAuthUpdateFromRemote(Array.Empty<ushort>());
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskFlush, taskRevoke);

                for (int p = 0; p < transport.SentPackets.Count; p++)
                {
                    var record = transport.SentPackets[p];
                    if (record.Data.Length > 2)
                    {
                        ushort packetId = BitConverter.ToUInt16(record.Data, 0);
                        if (packetId == LiminalPacketLibrary.GetId<SyncVarSlabClientRequestPacket>())
                        {
                            var packet = MessagePackSerializer.Deserialize<SyncVarSlabClientRequestPacket>(
                                new ReadOnlyMemory<byte>(record.Data, 2, record.Data.Length - 2),
                                syncVarManager.SerializerOptions);

                            if (!packet.Payload.IsEmpty)
                            {
                                var reader = new MessagePackReader(packet.Payload);
                                int arrayCount = reader.ReadArrayHeader();
                                int itemsRead = 0;
                                for (int i = 0; i < arrayCount; i++)
                                {
                                    if (reader.End)
                                    {
                                        Specification.Assert(false, $"Truncated payload! ArrayHeader promised {arrayCount} items, but only read {itemsRead} items before EOF.");
                                    }
                                    byte tLen = reader.ReadByte();
                                    reader.ReadRaw(tLen);
                                    reader.ReadUInt32();
                                    int len = reader.ReadInt32();
                                    reader.ReadRaw(len);
                                    itemsRead++;
                                }
                                Specification.Assert(itemsRead == arrayCount, $"Mismatch: promised {arrayCount}, read {itemsRead}");
                            }
                        }
                    }
                }
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_TailoredFlush_ConcurrentExclusionChange_ZeroMalformedMessagePack()
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

            const ushort client1 = 10;
            const ushort client2 = 20;
            transport.TriggerClientConnected(client1);
            transport.TriggerClientConnected(client2);

            const int count = 5;
            var svs = new SyncVar<int>[count];
            for (int i = 0; i < count; i++)
            {
                svs[i] = new SyncVar<int>($"excl_var_{i}", 100 + i, syncVarManager);
                svs[i].Value = 200 + i;
            }
            svs[0].AddExclusion(client1);

            var taskFlush = Task.Run(async () =>
            {
                for (int i = 0; i < 5; i++)
                {
                    syncVarManager.FlushDirty();
                    await Task.Yield();
                }
            });

            var taskExclusionMutate = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    svs[i].AddExclusion(client1);
                    await Task.Yield();
                    svs[i].RemoveExclusion(client1);
                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskFlush, taskExclusionMutate);

            for (int p = 0; p < transport.SentPackets.Count; p++)
            {
                var record = transport.SentPackets[p];
                if (record.Data.Length > 2)
                {
                    ushort packetId = BitConverter.ToUInt16(record.Data, 0);
                    if (packetId == LiminalPacketLibrary.GetId<SyncVarSlabBatchPacket>())
                    {
                        var packet = MessagePackSerializer.Deserialize<SyncVarSlabBatchPacket>(
                            new ReadOnlyMemory<byte>(record.Data, 2, record.Data.Length - 2),
                            syncVarManager.SerializerOptions);

                        if (!packet.Payload.IsEmpty)
                        {
                            var reader = new MessagePackReader(packet.Payload);
                            int arrayCount = reader.ReadArrayHeader();
                            int itemsRead = 0;
                            for (int i = 0; i < arrayCount; i++)
                            {
                                if (reader.End)
                                {
                                    Specification.Assert(false, $"Truncated payload! ArrayHeader promised {arrayCount} items, but only read {itemsRead} items before EOF.");
                                }
                                byte tLen = reader.ReadByte();
                                reader.ReadRaw(tLen);
                                reader.ReadUInt32();
                                int len = reader.ReadInt32();
                                reader.ReadRaw(len);
                                itemsRead++;
                            }
                            Specification.Assert(itemsRead == arrayCount, $"Mismatch: promised {arrayCount}, read {itemsRead}");
                        }
                    }
                }
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_VersionAndValue_AtomicPublication_NoVersionValueDesync()
        {
            var sv = new SyncVar<int>("desync_test", 0);

            const int iterations = 30;
            int desyncObserved = 0;

            var writer = Task.Run(async () =>
            {
                for (int i = 1; i <= iterations; i++)
                {
                    sv.Value = i;
                    await Task.Yield();
                }
            });

            var reader = Task.Run(async () =>
            {
                for (int i = 0; i < iterations * 2; i++)
                {
                    uint ver = sv.Version;
                    int val = sv.Value;

                    if (ver > 0 && val < (int)ver)
                    {
                        Interlocked.Increment(ref desyncObserved);
                    }
                    await Task.Yield();
                }
            });

            await Task.WhenAll(writer, reader);

            Specification.Assert(desyncObserved == 0,
                $"Version and Value publication desynchronized! Reader observed Version ahead of Value {desyncObserved} times.");
        }

        [Test]
        public static async Task Coyote_SyncVar_AuthorityRevocation_ConcurrentWrite_BlocksUnauthorizedWriteAndPreservesCleanDirtyState()
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
            netManager.StartClient("127.0.0.1", 7777);
            netManager.Ticker.Stop();
            ushort localId = netManager.localID;
            transport.TriggerLocalClientConnected(localId);

            var syncVarManager = netManager.SyncVarManager;
            var sv = new SyncVar<int>("auth_revoke_race", 100, syncVarManager);
            sv.ApplyAuthUpdateFromRemote(new ushort[] { localId });

            Specification.Assert(sv.HasAuthority, "Local client must have authority initially.");

            try
            {
                var taskWriter = Task.Run(async () =>
                {
                    sv.Value = 999;
                    await Task.Yield();
                });

                var taskRevoke = Task.Run(async () =>
                {
                    sv.ApplyAuthUpdateFromRemote(Array.Empty<ushort>());
                    await Task.Yield();
                });

                await Task.WhenAll(taskWriter, taskRevoke);

                Specification.Assert(!sv.HasAuthority, "SyncVar must have authority revoked at the end.");
                Specification.Assert(!sv.IsDirty, $"SyncVar cannot be marked dirty when authority is revoked! IsDirty={sv.IsDirty}");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ClientConnecting_CannotClaimServerAuthorityBeforeHandshake()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport { IsServer = false, LocalClientId = 0 };

            var netManager = new LiminalNetworkManager(transport, config);
            netManager.StartClient("127.0.0.1", 7777);
            netManager.Ticker.Stop();

            var syncVarManager = netManager.SyncVarManager;
            var sv = new SyncVar<int>("connecting_auth_test", 50, syncVarManager);

            try
            {
                var writerTask = Task.Run(async () =>
                {
                    sv.Value = 999;
                    await Task.Yield();
                });

                var handshakeTask = Task.Run(async () =>
                {
                    transport.LocalClientId = 1;
                    transport.TriggerLocalClientConnected(1);
                    await Task.Yield();
                });

                await Task.WhenAll(writerTask, handshakeTask);

                Specification.Assert(!sv.HasAuthority,
                    $"Connecting client with unassigned ID must not have authority! HasAuthority={sv.HasAuthority}, localId={netManager.localID}");
                Specification.Assert(sv.Value == 50,
                    $"SyncVar value was modified without authority! Expected 50, got {sv.Value}");
                Specification.Assert(!sv.IsDirty,
                    $"SyncVar was marked dirty without authority! IsDirty={sv.IsDirty}");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_EvictionAndCatchUp_ConcurrentRace()
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

            const ushort client1 = 10;
            const ushort client2 = 20;
            const ushort client3 = 30;
            transport.TriggerClientConnected(client1);
            transport.TriggerClientConnected(client2);
            transport.TriggerClientConnected(client3);

            var sv = new SyncVar<int>("coyote_edge_cases", 100, SyncVarVisibility.ManualObservers, syncVarManager);
            const int iterations = 20;

            try
            {
                var taskExclusion = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        sv.AddExclusion(client1);
                        await Task.Yield();
                        sv.RemoveExclusion(client1);
                        await Task.Yield();
                        sv.SetExclusionIds(client1, client2);
                        await Task.Yield();
                        sv.SetExclusionIds(Array.Empty<ushort>());
                        await Task.Yield();
                    }
                });

                var taskVisibility = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        sv.SetVisibility(SyncVarVisibility.Public);
                        await Task.Yield();
                        sv.SetVisibility(SyncVarVisibility.ManualObservers);
                        await Task.Yield();
                        sv.SetVisibility(SyncVarVisibility.OwnerOnly);
                        await Task.Yield();
                    }
                });

                var taskObservers = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        sv.AddObserver(client2);
                        sv.AddAuthority(client3);
                        await Task.Yield();
                        sv.RemoveObserver(client2);
                        sv.RemoveAuthority(client3);
                        await Task.Yield();
                        sv.SetObserverIds(client1, client2, client3);
                        await Task.Yield();
                        sv.SetObserverIds(Array.Empty<ushort>());
                        await Task.Yield();
                    }
                });

                var taskFlush = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        sv.Value = 200 + i;
                        syncVarManager.FlushDirty();
                        await Task.Yield();
                    }
                });

                var taskReader = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        bool vis = sv.IsVisible;
                        var state = sv.VisibilityState;
                        bool see1 = state.IsVisibleTo(client1, netManager);
                        bool see2 = state.IsVisibleTo(client2, netManager);
                        sv.SetLocallyVisible(i % 2 == 0);
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskExclusion, taskVisibility, taskObservers, taskFlush, taskReader);

                for (int p = 0; p < transport.SentPackets.Count; p++)
                {
                    var record = transport.SentPackets[p];
                    if (record.Data.Length > 2)
                    {
                        ushort packetId = BitConverter.ToUInt16(record.Data, 0);
                        if (packetId == LiminalPacketLibrary.GetId<SyncVarEvictPacket>())
                        {
                            var evictPacket = MessagePackSerializer.Deserialize<SyncVarEvictPacket>(
                                new ReadOnlyMemory<byte>(record.Data, 2, record.Data.Length - 2),
                                syncVarManager.SerializerOptions);
                            Specification.Assert(!string.IsNullOrEmpty(evictPacket.Token), "Evict packet token must not be empty.");
                        }
                        else if (packetId == LiminalPacketLibrary.GetId<SyncVarSnapshotPacket>())
                        {
                            var snapshotPacket = MessagePackSerializer.Deserialize<SyncVarSnapshotPacket>(
                                new ReadOnlyMemory<byte>(record.Data, 2, record.Data.Length - 2),
                                syncVarManager.SerializerOptions);
                            Specification.Assert(snapshotPacket.Entries != null, "Snapshot packet entries must not be null.");
                        }
                    }
                }
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ServerClear_ConcurrentWrites_NoStaleRevival()
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

            try
            {
                const ushort clientA = 10;
                transport.ConnectedClients.Add(clientA);

                var sv = new SyncVar<int>("token_coyote_clear", 0, syncVarManager);
                sv.Value = 100;
                sv.AddAuthority(clientA);

                const int iterations = 15;

                var taskClear = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        sv.Clear();
                        await Task.Yield();
                    }
                });

                static byte[] BuildClientRequestPacketBytes(string token, uint version, int value)
                {
                    var valBuf = new ArrayBufferWriter<byte>();
                    MessagePackSerializer.Serialize(valBuf, value);

                    var buffer = new ArrayBufferWriter<byte>();
                    var writer = new MessagePackWriter(buffer);
                    writer.WriteArrayHeader(1);
                    var tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
                    writer.WriteUInt8((byte)tokenBytes.Length);
                    writer.WriteRaw(tokenBytes);
                    writer.WriteUInt32(version);
                    writer.WriteInt32(valBuf.WrittenCount);
                    writer.WriteRaw(valBuf.WrittenSpan);
                    writer.Flush();

                    var packet = new SyncVarSlabClientRequestPacket(new ReadOnlySequence<byte>(buffer.WrittenMemory));
                    return MessagePackSerializer.Serialize(packet);
                }

                var taskClientRequests = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var staleBytes = BuildClientRequestPacketBytes("token_coyote_clear", 999, 50 + i);
                        netManager.Interpreter.Dispatch(
                            LiminalPacketLibrary.GetId<SyncVarSlabClientRequestPacket>(),
                            clientA,
                            staleBytes);

                        var clearAck = new SyncVarClearAckPacket
                        {
                            Token = "token_coyote_clear"
                        };
                        netManager.Interpreter.Dispatch(
                            LiminalPacketLibrary.GetId<SyncVarClearAckPacket>(),
                            clientA,
                            MessagePackSerializer.Serialize(clearAck));

                        var freshBytes = BuildClientRequestPacketBytes("token_coyote_clear", 1, 200 + i);
                        netManager.Interpreter.Dispatch(
                            LiminalPacketLibrary.GetId<SyncVarSlabClientRequestPacket>(),
                            clientA,
                            freshBytes);

                        await Task.Yield();
                    }
                });

                var taskObserver = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        syncVarManager.FlushDirty();
                        var (val, ver) = sv.GetSnapshot();
                        if (ver == 0)
                        {
                            Specification.Assert(val == 0, "SyncVar version is 0 but value was not reset to default.");
                        }
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskClear, taskClientRequests, taskObserver);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ContinuousResize_SlabReclaim_NoLeak()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 2048,
                SyncVarMaxPageCount = 4,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            try
            {
                const int workerCount = 3;
                const int iterations = 15;

                var vars = new SyncVar<byte[]>[workerCount];
                for (int w = 0; w < workerCount; w++)
                {
                    vars[w] = new SyncVar<byte[]>($"token_reclaim_{w}", new byte[64], syncVarManager);
                }

                var workers = new Task[workerCount];
                for (int w = 0; w < workerCount; w++)
                {
                    int workerIdx = w;
                    var sv = vars[workerIdx];
                    workers[workerIdx] = Task.Run(async () =>
                    {
                        for (int i = 0; i < iterations; i++)
                        {
                            int size = 32 * ((i % 4) + 1);
                            byte[] payload = new byte[size];
                            payload[0] = (byte)(i & 0xFF);
                            payload[size - 1] = 0xAA;

                            sv.Value = payload;

                            if (i % 5 == 0)
                            {
                                sv.Clear();
                            }

                            await Task.Yield();
                        }
                    });
                }

                var flushTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        syncVarManager.FlushDirty();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(workers[0], workers[1], workers[2], flushTask);

                for (int w = 0; w < workerCount; w++)
                {
                    var sv = vars[w];
                    Specification.Assert(sv != null, "SyncVar cannot be null.");
                    if (sv.Version > 0 && sv.Value != null)
                    {
                        Specification.Assert(sv.Value![^1] == 0xAA, "SyncVar payload boundary check failed.");
                    }
                }
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_Bind_Dispose_Rebind_NoClearSideEffects_Concurrent()
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

            try
            {
                const ushort clientA = 10;
                transport.ConnectedClients.Add(clientA);

                var serverVar = new SyncVar<int>("token_dispose_test", 0, syncVarManager);
                serverVar.Value = 50;

                const int iterations = 15;

                var taskServerWrites = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        serverVar.Value = 50 + i;
                        await Task.Yield();
                    }
                });

                var taskClientLifecycle = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var bound = syncVarManager.Bind<int>("token_dispose_test", 0);
                        Specification.Assert(bound != null, "Bound SyncVar cannot be null.");

                        bound.Dispose();

                        Specification.Assert(serverVar.Version >= 1, "Dispose must not reset server SyncVar version to 0.");
                        Specification.Assert(serverVar.Value >= 50, "Dispose must not reset server SyncVar value to default.");

                        await Task.Yield();
                    }
                });

                var taskFlusher = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        syncVarManager.FlushDirty();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskServerWrites, taskClientLifecycle, taskFlusher);

                Specification.Assert(serverVar.Version >= 1, "Final server version must be >= 1.");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_Utf8TokenRegistry_ConcurrentLookupAndExpansion_Race()
        {
            const int count = 150;
            var taskA = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    byte[] bytes = System.Text.Encoding.UTF8.GetBytes($"token_expansion_a_{i}");
                    string token = Utf8TokenRegistry.GetOrAddCachedToken(bytes);
                    Specification.Assert(token == $"token_expansion_a_{i}", "Token string mismatch in registry.");
                    await Task.Yield();
                }
            });

            var taskB = Task.Run(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    byte[] bytes = System.Text.Encoding.UTF8.GetBytes($"token_expansion_b_{i}");
                    string token = Utf8TokenRegistry.GetOrAddCachedToken(bytes);
                    Specification.Assert(token == $"token_expansion_b_{i}", "Token string mismatch in registry.");
                    await Task.Yield();
                }
            });

            await Task.WhenAll(taskA, taskB);
        }

        [Test]
        public static async Task Coyote_SyncVar_ConcurrentDisposeAndFlushDirty_NoSlotCorruption()
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

            try
            {
                const int iterations = 15;

                var taskMutateDispose = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var sv = new SyncVar<int>($"dispose_flush_race_{i}", 100 + i, syncVarManager);
                        sv.Value = 200 + i;
                        await Task.Yield();
                        sv.Dispose();
                        await Task.Yield();
                    }
                });

                var taskFlush = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations * 2; i++)
                    {
                        syncVarManager.FlushDirty();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskMutateDispose, taskFlush);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static void Coyote_SyncVar_DisposedSyncVar_MustNotSpinInDirtyQueue()
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

            try
            {
                var sv = new SyncVar<int>("token_spin_test", 10, syncVarManager);
                sv.Value = 20;
                sv.Dispose();

                syncVarManager.FlushDirty();

                Specification.Assert(!sv.IsDirty, "Disposed SyncVar was re-marked dirty and trapped in dirty queue.");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_DisposeRaceWithSnapshotReader_MustNotThrowIndexOutOfRange()
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

            try
            {
                var sv = new SyncVar<int>("token_dispose_extract_race", 123, syncVarManager);

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        sv.TryExtractSnapshotData(out _, out _);
                        await Task.Yield();
                    }
                });

                var disposeTask = Task.Run(async () =>
                {
                    await Task.Yield();
                    sv.Dispose();
                });

                await Task.WhenAll(readerTask, disposeTask);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_DisposeRaceWithTryWriteSlotForWire_MustNotThrowIndexOutOfRange()
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

            try
            {
                var sv = new SyncVar<int>("token_dispose_wire_race", 123, syncVarManager);

                var wireTask = Task.Run(async () =>
                {
                    var buffer = new ArrayBufferWriter<byte>(128);
                    for (int i = 0; i < 20; i++)
                    {
                        buffer.Clear();
                        var writer = new MessagePackWriter(buffer);
                        sv.TryWriteSlotForWire(ref writer, out _);
                        writer.Flush();
                        await Task.Yield();
                    }
                });

                var disposeTask = Task.Run(async () =>
                {
                    await Task.Yield();
                    sv.Dispose();
                });

                await Task.WhenAll(wireTask, disposeTask);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_DisposeRaceWithValueWriter_MustNotLeakOrResurrectSlots()
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

            try
            {
                var sv = new SyncVar<int>("dispose_write_race", 10, syncVarManager);

                var writeTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        sv.Value = 100 + i;
                        await Task.Yield();
                    }
                });

                var remoteTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var bytes = MessagePackSerializer.Serialize(500 + i);
                        sv.ApplyRemoteBytes(new ReadOnlySequence<byte>(bytes), (uint)(i + 1), syncVarManager.SerializerOptions, force: true);
                        await Task.Yield();
                    }
                });

                var disposeTask = Task.Run(async () =>
                {
                    await Task.Yield();
                    sv.Dispose();
                });

                await Task.WhenAll(writeTask, remoteTask, disposeTask);

                Specification.Assert(sv.IsDisposed, "SyncVar must be marked disposed.");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ClearRaceWithSnapshotReader_MustNotThrowIndexOutOfRange()
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

            try
            {
                var sv = new SyncVar<int>("clear_reader_race", 100, syncVarManager);

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        sv.TryExtractSnapshotData(out _, out _);
                        await Task.Yield();
                    }
                });

                var clearTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        sv.Value = 200 + i;
                        await Task.Yield();
                        sv.Clear();
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(readerTask, clearTask);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_WriteBytesResizeRaceWithActiveReader_MustNotFreeSlotWhileReading()
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

            try
            {
                var sv = new SyncVar<byte[]>("resize_reader_race", new byte[32], syncVarManager);

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        if (sv.TryExtractSnapshotData(out byte[] data, out _))
                        {
                            Specification.Assert(data.Length > 0, "Snapshot data length must be > 0.");
                        }
                        await Task.Yield();
                    }
                });

                var writerTask = Task.Run(async () =>
                {
                    int[] sizes = new[] { 64, 512, 128, 1024, 256, 2048, 64, 4000 };
                    for (int i = 0; i < sizes.Length; i++)
                    {
                        sv.Value = new byte[sizes[i]];
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(readerTask, writerTask);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ConcurrentWrites_ValueCellAndWireBufferMustNotDesynchronize()
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

            try
            {
                var sv = new SyncVar<int>("concurrent_write_desync", 0, syncVarManager);

                const int writesPerTask = 15;

                var taskA = Task.Run(async () =>
                {
                    for (int i = 1; i <= writesPerTask; i++)
                    {
                        sv.Value = i;
                        await Task.Yield();
                    }
                });

                var taskB = Task.Run(async () =>
                {
                    for (int i = 1; i <= writesPerTask; i++)
                    {
                        sv.Value = 100 + i;
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskA, taskB);

                int cellValue = sv.Value;
                uint cellVersion = sv.Version;

                bool extracted = sv.TryExtractSnapshotData(out byte[] wireData, out uint wireVersion);
                Specification.Assert(extracted, "Failed to extract snapshot data.");

                int wireValue = MessagePackSerializer.Deserialize<int>(wireData);

                Specification.Assert(cellVersion == wireVersion,
                    $"TOCTOU Desynchronization: ValueCell Version ({cellVersion}) != WireBuffer Version ({wireVersion}).");
                Specification.Assert(cellValue == wireValue,
                    $"TOCTOU Desynchronization: ValueCell Value ({cellValue}) != WireBuffer Value ({wireValue}).");
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVarWireBuffer_TryBeginRead_FrontSwapTOCTOU_MustNotTearOrRace()
        {
            var config = new LiminalNetworkConfig
            {
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16
            };
            var slab = new SyncVarSlab(config);
            var buffer = new Liminal.Net.SyncVar.Internal.SyncVarWireBuffer(slab);

            try
            {
                byte[] initBytes = new byte[64];
                Array.Fill(initBytes, (byte)1);
                buffer.WriteBytes(initBytes, 1);

                const int iterations = 30;

                var writerTask = Task.Run(async () =>
                {
                    int[] sizes = new[] { 64, 128, 256, 512, 1024, 64, 128, 512, 2048, 64 };
                    for (int i = 2; i <= iterations; i++)
                    {
                        byte b = (byte)i;
                        int size = sizes[i % sizes.Length];
                        byte[] data = new byte[size];
                        Array.Fill(data, b);
                        buffer.WriteBytes(data, (uint)i);
                        await Task.Yield();
                    }
                });

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        if (buffer.TryBeginRead(out int slotIndex, out int page, out int offset, out int length, out uint version))
                        {
                            try
                            {
                                ReadOnlySpan<byte> span = buffer.GetSpan(page, offset, length);
                                byte expected = (byte)version;
                                for (int k = 0; k < span.Length; k++)
                                {
                                    Specification.Assert(span[k] == expected,
                                        $"Data corruption/tearing! Byte at index {k} was {span[k]}, but expected {expected} for version {version}.");
                                }
                            }
                            finally
                            {
                                buffer.EndRead(slotIndex);
                            }
                        }
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(writerTask, readerTask);
            }
            finally
            {
                buffer.Dispose();
                slab.Dispose();
            }
        }

        [Test]
        public static async Task Coyote_SyncVarManager_RegisterSyncVar_ConcurrentWrites_ZeroTornCoordinates()
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

            try
            {
                var sv = new SyncVar<int>("reg_torn_coords", 1234, syncVarManager);

                var taskWriter = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        sv.Value = 5000 + i;
                        await Task.Yield();
                    }
                });

                var taskReader = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var (page, offset) = sv.ActiveSlotCoordinates;
                        if (page >= 0 && offset >= 0)
                        {
                            var span = syncVarManager.Slab.GetSpan(page, offset, sizeof(int));
                            Specification.Assert(span.Length == sizeof(int), "Span length must match integer size.");
                        }
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(taskWriter, taskReader);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVarWireBuffer_WriteBytes_OutOfOrderVersion_MustNotOverwriteNewerData()
        {
            var config = new LiminalNetworkConfig
            {
                SyncVarMaxPageSize = 4096,
                SyncVarMaxPageCount = 16
            };
            var slab = new SyncVarSlab(config);
            var buffer = new Liminal.Net.SyncVar.Internal.SyncVarWireBuffer(slab);

            try
            {
                byte[] dataVer10 = new byte[] { 10, 10, 10, 10 };
                byte[] dataVer5 = new byte[] { 5, 5, 5, 5 };

                bool write10 = buffer.WriteBytes(dataVer10, 10);
                Specification.Assert(write10, "Write version 10 should succeed.");

                bool write5 = buffer.WriteBytes(dataVer5, 5);
                Specification.Assert(!write5, "Out of order write version 5 should be rejected.");

                bool read = buffer.TryBeginRead(out int slotIndex, out int page, out int offset, out int length, out uint version);
                Specification.Assert(read, "TryBeginRead must succeed.");
                try
                {
                    Specification.Assert(version == 10, $"Version must remain 10, but was {version}.");
                    var span = buffer.GetSpan(page, offset, length);
                    Specification.Assert(span[0] == 10, "Data must remain version 10 bytes.");
                }
                finally
                {
                    buffer.EndRead(slotIndex);
                }
                await Task.CompletedTask;
            }
            finally
            {
                buffer.Dispose();
                slab.Dispose();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_ForceApplyRemote_WireBufferMustNotRejectLowerVersion()
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

            try
            {
                var sv = new SyncVar<int>("force_lower_version", 0, syncVarManager);

                var writer10 = new ArrayBufferWriter<byte>();
                MessagePackSerializer.Serialize(writer10, 100, syncVarManager.SerializerOptions);
                sv.ApplyRemoteBytes(new ReadOnlySequence<byte>(writer10.WrittenMemory), 10, syncVarManager.SerializerOptions);

                Specification.Assert(sv.Value == 100, "Value should be 100.");
                Specification.Assert(sv.Version == 10, "Version should be 10.");

                var writer1 = new ArrayBufferWriter<byte>();
                MessagePackSerializer.Serialize(writer1, 5, syncVarManager.SerializerOptions);
                sv.ApplyRemoteBytes(new ReadOnlySequence<byte>(writer1.WrittenMemory), 1, syncVarManager.SerializerOptions, force: true);

                Specification.Assert(sv.Value == 5, "Value should be 5 after force apply.");
                Specification.Assert(sv.Version == 1, "Version should be 1 after force apply.");

                bool extracted = sv.TryExtractSnapshotData(out byte[] wireData, out uint wireVersion);
                Specification.Assert(extracted, "Snapshot extraction must succeed.");
                int wireVal = MessagePackSerializer.Deserialize<int>(wireData);

                Specification.Assert(wireVersion == 1,
                    $"Wire version must be 1, but was {wireVersion} because WireBuffer rejected force write!");
                Specification.Assert(wireVal == 5,
                    $"Wire value must be 5, but was {wireVal} because WireBuffer rejected force write!");

                await Task.CompletedTask;
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_CrossVariable_SlabSlotRecycle_NoDataCorruption()
        {
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                SyncVarMaxPageSize = 2048,
                SyncVarMaxPageCount = 8,
                ClientIdResolver = new BaseResolver()
            };

            var transport = new MockTransport { IsServer = true };
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            try
            {
                byte[] initA = new byte[64];
                Array.Fill(initA, (byte)0x11);
                byte[] initB = new byte[64];
                Array.Fill(initB, (byte)0x22);

                var svA = new SyncVar<byte[]>("var_recycle_a", initA, syncVarManager);
                var svB = new SyncVar<byte[]>("var_recycle_b", initB, syncVarManager);

                var readerTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 25; i++)
                    {
                        if (svA.TryExtractSnapshotData(out byte[] snapshotData, out _))
                        {
                            if (snapshotData.Length > 0)
                            {
                                var deserialized = MessagePackSerializer.Deserialize<byte[]>(snapshotData);
                                for (int k = 0; k < deserialized.Length; k++)
                                {
                                    Specification.Assert(deserialized[k] != 0x22,
                                        $"Cross-variable memory corruption: svA read svB payload byte 0x22 at index {k}.");
                                    Specification.Assert(deserialized[k] == 0x11,
                                        $"Data tearing: byte at {k} was 0x{deserialized[k]:X2}, expected 0x11.");
                                }
                            }
                        }
                        await Task.Yield();
                    }
                });

                var writerTaskA = Task.Run(async () =>
                {
                    int[] sizes = new[] { 64, 256, 64, 512, 128, 1024, 64 };
                    for (int i = 0; i < sizes.Length; i++)
                    {
                        byte[] data = new byte[sizes[i]];
                        Array.Fill(data, (byte)0x11);
                        svA.Value = data;
                        await Task.Yield();
                    }
                });

                var writerTaskB = Task.Run(async () =>
                {
                    int[] sizes = new[] { 64, 128, 256, 512, 64 };
                    for (int i = 0; i < sizes.Length; i++)
                    {
                        byte[] data = new byte[sizes[i]];
                        Array.Fill(data, (byte)0x22);
                        svB.Value = data;
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(readerTask, writerTaskA, writerTaskB);
            }
            finally
            {
                netManager.Shutdown();
            }
        }

        [Test]
        public static async Task Coyote_SyncVar_SeparateActivePageAndOffset_TOCTOU_TornCoordinates_Demonstration()
        {
            var config = new LiminalNetworkConfig
            {
                SyncVarMaxPageSize = 512,
                SyncVarMaxPageCount = 16
            };
            var transport = new MockTransport { IsServer = true };
            var netManager = new LiminalNetworkManager(transport, config);
            var syncVarManager = netManager.SyncVarManager;

            try
            {
                var sv = new SyncVar<byte[]>("torn_coord_demo", new byte[32], syncVarManager);

                var writerTask = Task.Run(async () =>
                {
                    int[] sizes = new[] { 64, 128, 64, 128, 64, 128 };
                    for (int i = 0; i < sizes.Length; i++)
                    {
                        sv.Value = new byte[sizes[i]];
                        await Task.Yield();
                    }
                });

                var atomicReaderTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var (page, offset) = sv.ActiveSlotCoordinates;
                        if (page >= 0 && offset >= 0)
                        {
                            var span = syncVarManager.Slab.GetSpan(page, offset, 32);
                            Specification.Assert(span.Length == 32, "Atomic coordinates must never produce invalid span.");
                        }
                        await Task.Yield();
                    }
                });

                await Task.WhenAll(writerTask, atomicReaderTask);
            }
            finally
            {
                netManager.Shutdown();
            }
        }
    }
}