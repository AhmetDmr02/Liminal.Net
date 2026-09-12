using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests
{
    public static class AdvancedConcurrencyTests
    {
        [Test]
        public static async Task TestSessionManagerFullPipelineChaos()
        {
            var config = new LiminalTransportConfig
            {
                MaxPacketSizePerBatch = 64,
                MaxPacketCount = 10,
                MaxConnectionCount = 4
            };
            config.Hiccup.MaxRecoveryScale = 4;
            config.Hiccup.GraceCount = 500;
            config.Hiccup.RecoveryHoldSeconds = 0.0001f;

            var transport = new MockTransport();
            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);
            var manager = new LiminalSessionManager(transport, interpreter, config, pipeline);

            ushort clientId = 42;
            transport.TriggerClientConnected(clientId);

            byte[] rawPayload = new byte[20];
            byte[] framedPacket = new byte[4 + 2 + rawPayload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(framedPacket.AsSpan(0, 4), rawPayload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(framedPacket.AsSpan(4, 2), 1);
            rawPayload.CopyTo(framedPacket.AsSpan(6));

            int totalDispatched = 0;

            interpreter.OnSendRequest += (sender, target, pid, data, DeliveryMethod) =>
            {
                Interlocked.Increment(ref totalDispatched);
            };

            var workers = new Task[3];

            workers[0] = Task.Run(async () =>
            {
                for (int i = 0; i < 25; i++)
                {
                    transport.TriggerMessageReceived(framedPacket, clientId);
                    await Task.Yield();
                }
            });

            workers[1] = Task.Run(async () =>
            {
                for (int i = 0; i < 25; i++)
                {
                    manager.BufferPacket(transport.LocalClientId, clientId, 1, rawPayload);
                    await Task.Yield();
                }
            });

            workers[2] = Task.Run(async () =>
            {
                for (int i = 0; i < 3; i++)
                {
                    await Task.Yield();
                    transport.Kick(clientId);
                    await Task.Yield();
                    transport.TriggerClientConnected(clientId);
                }
            });

            var allWorkers = Task.WhenAll(workers);
            while (!allWorkers.IsCompleted)
            {
                manager.Poll();
                manager.Flush();
                await Task.Yield();
            }

            await allWorkers;

            for (int i = 0; i < 5; i++)
            {
                manager.Poll();
                manager.Flush();
            }

            manager.Dispose();
            manager.Flush();

            Specification.Assert(manager.GetActiveSessionCount() == 0,
                "Sessions were not fully cleaned up after chaos disconnect and shutdown.");
        }

        [Test]
        public static async Task TestInterpreterSubscriptionRace()
        {
            var config = new LiminalTransportConfig();
            var interpreter = new LiminalPacketInterpreter(config);

            ushort chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            if (chatPacketId == 0)
            {
                LiminalPacketLibrary.Initialize();
                chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            }

            byte[] serializedChat = MessagePackSerializer.Serialize(new ChatPacket { Message = "RaceTest" });

            object subscriberA = new object();
            object subscriberB = new object();
            int totalInvocations = 0;

            Action<ChatPacket, ushort> callback = (pkt, id) =>
            {
                Interlocked.Increment(ref totalInvocations);
            };

            var tasks = new Task[4];

            tasks[0] = Task.Run(async () =>
            {
                for (int i = 0; i < 30; i++)
                {
                    interpreter.Subscribe(callback, subscriberA);
                    interpreter.Subscribe(callback, subscriberB);
                    await Task.Yield();
                }
            });

            tasks[1] = Task.Run(async () =>
            {
                for (int i = 0; i < 30; i++)
                {
                    interpreter.Unsubscribe<ChatPacket>(subscriberA);
                    await Task.Yield();
                }
            });

            tasks[2] = Task.Run(async () =>
            {
                for (int i = 0; i < 30; i++)
                {
                    interpreter.UnsubscribeAll(subscriberB);
                    await Task.Yield();
                }
            });

            tasks[3] = Task.Run(async () =>
            {
                for (int i = 0; i < 50; i++)
                {
                    interpreter.Dispatch(chatPacketId, 1, serializedChat);
                    await Task.Yield();
                }
            });

            await Task.WhenAll(tasks);

            // Clean teardown check
            interpreter.ClearAllHandlers();
            int finalHitsBefore = Volatile.Read(ref totalInvocations);
            interpreter.Dispatch(chatPacketId, 1, serializedChat);

            Specification.Assert(Volatile.Read(ref totalInvocations) == finalHitsBefore,
                "Handlers still dispatched after ClearAllHandlers!");
        }

        [Test]
        public static async Task HuntGhostSubscriptionRace()
        {
            var config = new LiminalTransportConfig();
            var interpreter = new LiminalPacketInterpreter(config);

            ushort chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            if (chatPacketId == 0)
            {
                LiminalPacketLibrary.Initialize();
                chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            }

            byte[] serializedChat = MessagePackSerializer.Serialize(new ChatPacket { Message = "GhostHunter" });

            object target = new object();
            int postUnsubscribeHits = 0;
            int phaseComplete = 0;

            Action<ChatPacket, ushort> ghostDetector = (pkt, sender) =>
            {
                if (Volatile.Read(ref phaseComplete) == 1)
                {
                    Interlocked.Increment(ref postUnsubscribeHits);
                }
            };

            var t1 = Task.Run(async () =>
            {
                for (int i = 0; i < 40; i++)
                {
                    interpreter.Subscribe(ghostDetector, target);
                    await Task.Yield();
                }
            });

            var t2 = Task.Run(async () =>
            {
                for (int i = 0; i < 40; i++)
                {
                    interpreter.UnsubscribeAll(target);
                    await Task.Yield();
                }
            });

            var t3 = Task.Run(async () =>
            {
                for (int i = 0; i < 40; i++)
                {
                    interpreter.Dispatch(chatPacketId, 1, serializedChat);
                    await Task.Yield();
                }
            });

            await Task.WhenAll(t1, t2, t3);

            interpreter.UnsubscribeAll(target);
            Volatile.Write(ref phaseComplete, 1);

            for (int i = 0; i < 20; i++)
            {
                interpreter.Dispatch(chatPacketId, 1, serializedChat);
            }

            Specification.Assert(
                postUnsubscribeHits == 0,
                $"GHOST SUBSCRIPTION LEAK: Target received {postUnsubscribeHits} packets after terminal UnsubscribeAll!");
        }

        [Test]
        public static async Task TestHiccupSharedPoolContentionAcrossSessions()
        {
            var config = new LiminalTransportConfig
            {
                MaxPacketSizePerBatch = 64,
                MaxPacketCount = 10,
                MaxConnectionCount = 4
            };

            config.Hiccup.MaxRecoveryScale = 50;
            config.Hiccup.GraceCount = 500;
            config.Hiccup.GraceWindowSeconds = 10f;
            config.Hiccup.CooldownSeconds = 0f;
            config.Hiccup.RecoveryHoldSeconds = 0.0001f;
            config.Hiccup.WarmRecoverySessions = 1;

            var transport = new MockTransport();
            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);
            var manager = new LiminalSessionManager(transport, interpreter, config, pipeline);

            ushort[] clientIds = { 21, 22, 23, 24 };
            foreach (var id in clientIds)
            {
                transport.TriggerClientConnected(id);
            }

            byte[] normalPayload = new byte[20];
            byte[] burstPayload = new byte[70];

            byte[] BuildFramedPacket(byte[] payload)
            {
                byte[] frame = new byte[4 + 2 + payload.Length];
                BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), payload.Length + 2);
                BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(4, 2), 1);
                payload.CopyTo(frame.AsSpan(6));
                return frame;
            }

            byte[] normalWireFrame = BuildFramedPacket(normalPayload);
            byte[] burstWireFrame = BuildFramedPacket(burstPayload);

            var workers = new Task[clientIds.Length * 2];
            int idx = 0;

            for (int c = 0; c < clientIds.Length; c++)
            {
                var capturedId = clientIds[c];
                workers[idx++] = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        byte[] frame = (i % 2 == 0) ? burstWireFrame : normalWireFrame;
                        transport.TriggerMessageReceived(frame, capturedId);
                        await Task.Yield();
                    }
                });
            }

            for (int c = 0; c < clientIds.Length; c++)
            {
                var capturedId = clientIds[c];
                workers[idx++] = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        byte[] p = (i % 2 == 0) ? burstPayload : normalPayload;
                        manager.BufferPacket(transport.LocalClientId, capturedId, 1, p);
                        await Task.Yield();
                    }
                });
            }

            var allWorkers = Task.WhenAll(workers);
            while (!allWorkers.IsCompleted)
            {
                manager.Poll();
                manager.Flush();
                await Task.Yield();
            }

            await allWorkers;

            for (int i = 0; i < 5; i++)
            {
                manager.Poll();
                manager.Flush();
            }

            manager.Dispose();
            manager.Flush();

            Specification.Assert(manager.GetActiveSessionCount() == 0,
                "Sessions were not cleaned up upon Dispose + final Flush.");
        }

        [Test]
        public static async Task TestResolverConcurrentAllocation()
        {
            var transport = new MockTransport();
            var resolver = new BaseResolver();
            resolver.Initialize(transport);

            var t1 = Task.Run(() => resolver.GenerateClientId());
            var t2 = Task.Run(() => resolver.GenerateClientId());
            var t3 = Task.Run(() => resolver.ResetResolver());

            await Task.WhenAll(t1, t2, t3);
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static void Coyote_ExecuteFragmentorStress()
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 10,
                ClientIdResolver = new BaseResolver()
            };

            var mockTransport = new MockTransport();
            var fragmentor = new LiminalPacketFragmentor(mockTransport, config, mtu: 500);

            int sendCount = 0;
            mockTransport.OnSendHook = (data, clientId, flags) =>
            {
                if (System.Threading.Interlocked.Increment(ref sendCount) == 5)
                {
                    fragmentor.Dispose();
                }
            };

            var sendTask1 = Task.Run(() =>
            {
                byte[] largePayload = new byte[1200];
                for (int i = 0; i < 10; i++)
                {
                    fragmentor.Send(largePayload, targetId: 1, TransportFlags.Reliable);
                }
            });

            var sendTask2 = Task.Run(() =>
            {
                byte[] smallPayload = new byte[100];
                for (int i = 0; i < 10; i++)
                {
                    fragmentor.Send(smallPayload, targetId: 1, TransportFlags.Reliable);
                }
            });

            var disposeTask = Task.Run(() =>
            {
                fragmentor.Dispose();
            });

            Task.WaitAll(sendTask1, sendTask2, disposeTask);
        }
        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_SessionManager_DisposeRace_ConcurrentWithTraffic()
        {
            var config = new LiminalTransportConfig
            {
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 512,
                MaxConnectionCount = 8,
                TickRate = 20
            };

            config.Hiccup.MaxRecoveryScale = 4;
            config.Hiccup.GraceCount = 1000;
            config.Hiccup.GraceWindowSeconds = 10f;
            config.Hiccup.CooldownSeconds = 0f;
            config.Hiccup.RecoveryHoldSeconds = 0.0001f;
            config.Hiccup.WarmRecoverySessions = 1;

            var transport = new MockTransport();
            transport.InitializeTransport(config);

            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);

            var manager = new LiminalSessionManager(
                transport,
                interpreter,
                config,
                pipeline);

            const ushort clientId = 77;
            transport.TriggerClientConnected(clientId);

            byte[] payload = new byte[16];
            byte[] frame = new byte[4 + 2 + payload.Length];

            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(4, 2), 20);
            payload.CopyTo(frame.AsSpan(6));

            var ticker = new LiminalTicker(config);

            ticker.OnTick += () =>
            {
                manager.Poll();

                manager.BufferPacket(
                    transport.LocalClientId,
                    clientId,
                    100,
                    payload,
                    DeliveryMethod.Reliable);

                manager.Flush();
            };

            var tickerTask = Task.Run(async () =>
            {
                for (int i = 0; i < 60; i++)
                {
                    ticker.TickOnce();
                    await Task.Yield();
                }
            });

            var reliableWorker = Task.Run(async () =>
            {
                for (int i = 0; i < 20; i++)
                {
                    transport.TriggerReliableReceived(frame, clientId);
                    await Task.Yield();
                }
            });

            var unreliableWorker = Task.Run(async () =>
            {
                for (int i = 0; i < 20; i++)
                {
                    transport.TriggerUnreliableReceived(frame, clientId);
                    await Task.Yield();
                }
            });

            var disposeTask = Task.Run(async () =>
            {
                await Task.Yield();
                await Task.Yield();
                manager.Dispose();
            });

            await Task.WhenAll(tickerTask, reliableWorker, unreliableWorker, disposeTask);

            manager.Flush();
            manager.Poll();

            Specification.Assert(
                manager.GetActiveSessionCount() == 0,
                "Manager did not reach a fully torn-down state after Dispose raced with in-flight traffic.");
        }
        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_SessionManager_ConcurrentThroughputConservation()
        {
            var config = new LiminalTransportConfig
            {
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = 512,
                MaxConnectionCount = 8,
                TickRate = 20
            };

            config.Hiccup.MaxRecoveryScale = 4;
            config.Hiccup.GraceCount = 1000;
            config.Hiccup.GraceWindowSeconds = 10f;
            config.Hiccup.CooldownSeconds = 0f;
            config.Hiccup.RecoveryHoldSeconds = 0.0001f;
            config.Hiccup.WarmRecoverySessions = 1;

            var transport = new MockTransport();
            transport.InitializeTransport(config);

            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);

            var manager = new LiminalSessionManager(
                transport,
                interpreter,
                config,
                pipeline);

            manager.InitializeConfig(new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.PacketCounting,
                EnablePacketCounting = true
            });

            long bufferedCount = 0;
            manager.OnPacketBuffered += (_, _) => Interlocked.Increment(ref bufferedCount);

            const ushort clientId = 77;
            const ushort churnClientId = 88;

            transport.TriggerClientConnected(clientId);

            byte[] payload = new byte[16];
            byte[] frame = new byte[4 + 2 + payload.Length];

            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(4, 2), 20);
            payload.CopyTo(frame.AsSpan(6));

            var ticker = new LiminalTicker(config);
            int ticks = 0;

            ticker.OnTick += () =>
            {
                ticks++;
                manager.Poll();

                manager.BufferPacket(
                    transport.LocalClientId,
                    clientId,
                    100,
                    payload,
                    DeliveryMethod.Reliable);

                manager.Flush();
            };

            const int mainTicks = 100;
            const int drainTicks = 20;

            var tickerTask = Task.Run(async () =>
            {
                for (int i = 0; i < mainTicks + drainTicks; i++)
                {
                    ticker.TickOnce();
                    await Task.Yield();
                }
            });

            const int reliableInboundCount = 10;
            const int unreliableInboundCount = 10;

            var reliableWorker = Task.Run(async () =>
            {
                for (int i = 0; i < reliableInboundCount; i++)
                {
                    transport.TriggerReliableReceived(frame, clientId);
                    await Task.Yield();
                }
            });

            var unreliableWorker = Task.Run(async () =>
            {
                for (int i = 0; i < unreliableInboundCount; i++)
                {
                    transport.TriggerUnreliableReceived(frame, clientId);
                    await Task.Yield();
                }
            });

            const int extraReliableSends = 10;
            const int extraUnreliableSends = 10;

            var extraReliableSender = Task.Run(async () =>
            {
                for (int i = 0; i < extraReliableSends; i++)
                {
                    manager.BufferPacket(
                        transport.LocalClientId,
                        clientId,
                        101,
                        payload,
                        DeliveryMethod.Reliable);

                    await Task.Yield();
                }
            });

            var extraUnreliableSender = Task.Run(async () =>
            {
                for (int i = 0; i < extraUnreliableSends; i++)
                {
                    manager.BufferPacket(
                        transport.LocalClientId,
                        clientId,
                        101,
                        payload,
                        DeliveryMethod.Unreliable);

                    await Task.Yield();
                }
            });

            var churnWorker = Task.Run(async () =>
            {
                for (int i = 0; i < 5; i++)
                {
                    transport.TriggerClientConnected(churnClientId);
                    await Task.Yield();
                    transport.TriggerClientDisconnected(churnClientId);
                    await Task.Yield();
                }
            });

            await Task.WhenAll(
                tickerTask,
                reliableWorker,
                unreliableWorker,
                extraReliableSender,
                extraUnreliableSender,
                churnWorker);

            Specification.Assert(
                ticks == mainTicks + drainTicks,
                $"Ticker executed {ticks} ticks, expected exactly {mainTicks + drainTicks}.");

            Specification.Assert(
                manager.GetActiveSessionCount() == 1,
                "Expected only the primary client's session to remain active after churn settled.");

            var snapshot = manager.GetGlobalSnapshot();

            const long expectedInbound = reliableInboundCount + unreliableInboundCount;
            const long expectedOutbound =
                (mainTicks + drainTicks) /* ticker's own send per tick */
                + extraReliableSends
                + extraUnreliableSends;

            Specification.Assert(
                snapshot.TotalPacketsInbound == expectedInbound,
                $"Expected {expectedInbound} inbound packets accepted under ReceiveLock, got {snapshot.TotalPacketsInbound}. " +
                "A mismatch here means concurrent reliable/unreliable inbound processing lost or double-counted a packet.");

            Specification.Assert(
                snapshot.TotalPacketsOutbound == expectedOutbound,
                $"Expected {expectedOutbound} outbound packets accepted under SendLock, got {snapshot.TotalPacketsOutbound}. " +
                "A mismatch here means concurrent BufferPacket callers raced past SendLock incorrectly.");

            Specification.Assert(
                Interlocked.Read(ref bufferedCount) == expectedOutbound,
                $"OnPacketBuffered fired {bufferedCount} times, expected {expectedOutbound}. " +
                "This should always match TotalPacketsOutbound - if it doesn't, the two counters are being " +
                "updated non-atomically with respect to each other somewhere.");

            Specification.Assert(
                snapshot.LoopbackQueueCount == 0,
                "Loopback queue leaked packets even though this test never targets the local client ID.");

            manager.Dispose();
            manager.Flush();
            manager.Poll();

            Specification.Assert(
                manager.GetActiveSessionCount() == 0,
                "Session was not cleaned up after Dispose/Flush/Poll.");
        }
    }
}