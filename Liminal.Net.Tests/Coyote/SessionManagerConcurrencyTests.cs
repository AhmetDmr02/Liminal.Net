using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using System.Buffers.Binary;
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

            interpreter.OnSendRequest += (sender, target, pid, data) =>
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
    }
}