using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Core.Telemetry;
using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using CoyoteTestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;
using NUnitTestAttribute = NUnit.Framework.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    [TestFixture]
    public class WireLockConcurrencyCoyoteTests
    {
        private static int _portCounter = 8100;

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_LiminalTelemetryManager_RttLock_ConcurrentTicksPongsAndLifecycle_ZeroDeadlockOrRace()
        {
            LiminalPacketLibrary.Initialize();
            ushort pongPacketId = checked((ushort)LiminalPacketLibrary.GetId<PongPacket>());

            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096
            };
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.001f
            };

            var mockTransport = new MockTransport();
            var manager = new LiminalNetworkManager(mockTransport, config, telemetryConfig);
            var telemetry = manager.TelemetryManager;

            const int clientCount = 4;
            for (ushort id = 1; id <= clientCount; id++)
            {
                mockTransport.TriggerClientConnected(id);
            }

            const int iterations = 15;
            int exceptionsCaught = 0;

            var tickTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    try
                    {
                        manager.Ticker.TickOnce();
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref exceptionsCaught);
                    }
                    await Task.Yield();
                }
            });

            var pongTask = Task.Run(async () =>
            {
                long now = Stopwatch.GetTimestamp();
                for (int i = 0; i < iterations; i++)
                {
                    for (ushort id = 1; id <= clientCount; id++)
                    {
                        try
                        {
                            var pong = new PongPacket
                            {
                                SequenceId = (uint)(i + 1),
                                TimestampTicks = now
                            };
                            byte[] rawBytes = MessagePackSerializer.Serialize(pong);
                            manager.Interpreter.Dispatch(pongPacketId, id, rawBytes);
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref exceptionsCaught);
                        }
                    }
                    await Task.Yield();
                }
            });

            var queryTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    for (ushort id = 1; id <= clientCount; id++)
                    {
                        try
                        {
                            if (telemetry.TryGetClientEnd2EndRTT(id, out double rtt))
                            {
                                Specification.Assert(rtt >= 0.0, "E2E RTT must be non-negative");
                            }
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref exceptionsCaught);
                        }
                    }
                    await Task.Yield();
                }
            });

            var lifecycleTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    ushort churnId = (ushort)((i % clientCount) + 1);
                    try
                    {
                        mockTransport.TriggerClientDisconnected(churnId);
                        mockTransport.TriggerClientConnected(churnId);
                        if (i % 5 == 0)
                        {
                            mockTransport.TriggerLocalClientDisconnected(0);
                        }
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref exceptionsCaught);
                    }
                    await Task.Yield();
                }
            });

            await Task.WhenAll(tickTask, pongTask, queryTask, lifecycleTask);

            Specification.Assert(exceptionsCaught == 0, $"Expected 0 exceptions under concurrency, caught {exceptionsCaught}");
            manager.Shutdown();
        }

        [CoyoteTest]
        [NUnitTest]
        public static async Task Coyote_TcpTransport_WireLock_ConcurrentPingsQueriesAndDisconnection_ZeroDeadlockOrRace()
        {
            var transport = new TcpTransport();
            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 100
            };
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            const int clientCount = 4;
            const int iterations = 15;
            int exceptionsCaught = 0;

            var pingTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    for (ushort id = 1; id <= clientCount; id++)
                    {
                        try
                        {
                            transport.SendWirePing(id);
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref exceptionsCaught);
                        }
                    }
                    await Task.Yield();
                }
            });

            var queryTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    for (ushort id = 1; id <= clientCount; id++)
                    {
                        try
                        {
                            if (transport.TryGetWireRTT(id, out double rtt))
                            {
                                Specification.Assert(rtt >= 0.0, "Wire RTT must be non-negative");
                            }
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref exceptionsCaught);
                        }
                    }
                    await Task.Yield();
                }
            });

            var disconnectTask = Task.Run(async () =>
            {
                for (int i = 0; i < iterations; i++)
                {
                    ushort churnId = (ushort)((i % clientCount) + 1);
                    try
                    {
                        transport.Kick(churnId);
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref exceptionsCaught);
                    }
                    await Task.Yield();
                }
            });

            await Task.WhenAll(pingTask, queryTask, disconnectTask);

            Specification.Assert(exceptionsCaught == 0, $"Expected 0 exceptions under concurrency, caught {exceptionsCaught}");
            transport.Shutdown();
        }

        [NUnitTest]
        public void LiminalTelemetryManager_RttLock_MultiThreaded_HighContentionStress_Hammer()
        {
            LiminalPacketLibrary.Initialize();
            ushort pongPacketId = checked((ushort)LiminalPacketLibrary.GetId<PongPacket>());

            var config = new LiminalNetworkConfig
            {
                TickRate = 60,
                MaxPacketSizePerBatch = 4096
            };
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 0.001f
            };

            var mockTransport = new MockTransport();
            var manager = new LiminalNetworkManager(mockTransport, config, telemetryConfig);
            var telemetry = manager.TelemetryManager;

            const int clientCount = 8;
            for (ushort id = 1; id <= clientCount; id++)
            {
                mockTransport.TriggerClientConnected(id);
            }

            const int workerCount = 16;
            const int opsPerWorker = 3000;
            int totalExceptions = 0;
            var barrier = new Barrier(workerCount);
            var tasks = new Task[workerCount];

            for (int w = 0; w < workerCount; w++)
            {
                int workerIndex = w;
                tasks[w] = Task.Run(() =>
                {
                    barrier.SignalAndWait();

                    for (int i = 0; i < opsPerWorker; i++)
                    {
                        ushort targetId = (ushort)((i % clientCount) + 1);

                        try
                        {
                            switch (workerIndex % 4)
                            {
                                case 0:
                                    manager.Ticker.TickOnce();
                                    break;

                                case 1:
                                    var pong = new PongPacket
                                    {
                                        SequenceId = (uint)i,
                                        TimestampTicks = Stopwatch.GetTimestamp()
                                    };
                                    byte[] raw = MessagePackSerializer.Serialize(pong);
                                    manager.Interpreter.Dispatch(pongPacketId, targetId, raw);
                                    break;

                                case 2:
                                    telemetry.TryGetClientEnd2EndRTT(targetId, out _);
                                    break;

                                case 3:
                                    if (i % 50 == 0)
                                    {
                                        mockTransport.TriggerClientDisconnected(targetId);
                                        mockTransport.TriggerClientConnected(targetId);
                                    }
                                    else
                                    {
                                        telemetry.TryGetClientEnd2EndRTT(targetId, out _);
                                    }
                                    break;
                            }
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref totalExceptions);
                        }
                    }
                });
            }

            bool completed = Task.WaitAll(tasks, TimeSpan.FromSeconds(10));
            Assert.That(completed, Is.True, "Telemetry stress hammer deadlocked or exceeded time threshold");
            Assert.That(totalExceptions, Is.EqualTo(0), "No exceptions must be thrown during concurrent telemetry access");
            manager.Shutdown();
        }

        [NUnitTest]
        public void TcpTransport_WireLock_MultiThreaded_HighContentionStress_Hammer()
        {
            int port = Interlocked.Increment(ref _portCounter);

            var serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };

            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.WireRTT,
                PollIntervalInSeconds = 0.005f
            };

            var serverTransport = new TcpTransport();
            var serverManager = new LiminalNetworkManager(serverTransport, serverConfig, telemetryConfig);
            serverManager.StartServer("127.0.0.1", port);

            var clientConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver()
            };

            var clientTransport = new TcpTransport();
            var clientManager = new LiminalNetworkManager(clientTransport, clientConfig, telemetryConfig);

            bool connected = false;
            clientManager.Events.OnLocalClientConnected += _ => connected = true;
            clientManager.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => connected, 3000), Is.True, "Client failed to connect");

            const int workerCount = 16;
            const int opsPerWorker = 3000;
            int totalExceptions = 0;
            var barrier = new Barrier(workerCount);
            var tasks = new Task[workerCount];

            for (int w = 0; w < workerCount; w++)
            {
                int workerIndex = w;
                tasks[w] = Task.Run(() =>
                {
                    barrier.SignalAndWait();

                    for (int i = 0; i < opsPerWorker; i++)
                    {
                        try
                        {
                            switch (workerIndex % 4)
                            {
                                case 0:
                                    clientTransport.SendWirePing(ILiminalTransport.SERVER_ID);
                                    break;

                                case 1:
                                    serverTransport.SendWirePing(clientManager.localID);
                                    break;

                                case 2:
                                    clientTransport.TryGetWireRTT(ILiminalTransport.SERVER_ID, out _);
                                    break;

                                case 3:
                                    serverTransport.TryGetWireRTT(clientManager.localID, out _);
                                    break;
                            }
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref totalExceptions);
                        }
                    }
                });
            }

            bool completed = Task.WaitAll(tasks, TimeSpan.FromSeconds(10));
            Assert.That(completed, Is.True, "Transport stress hammer deadlocked or exceeded time threshold");
            Assert.That(totalExceptions, Is.EqualTo(0), "No exceptions must be thrown during concurrent transport lock access");

            clientManager.Shutdown();
            serverManager.Shutdown();
        }
    }
}
