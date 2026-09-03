using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.SystematicTesting;
using System.Threading.Tasks;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests
{
    public static class AdvancedConcurrencyTests
    {
        [Test]
        public static async Task TestSessionManagerFullPipelineChaos()
        {
            var config = new LiminalTransportConfig { MaxPacketCount = 5 };
            var transport = new MockTransport();
            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);
            var manager = new LiminalSessionManager(transport, interpreter, config, pipeline);

            ushort clientId = 42;
            transport.TriggerClientConnected(clientId);
            byte[] payload = new byte[64];

            var t1 = Task.Run(() => {
                for (int i = 0; i < 15; i++) transport.TriggerMessageReceived(payload, clientId);
            });

            var t2 = Task.Run(() => {
                for (int i = 0; i < 15; i++)
                {
                    manager.BufferPacket(clientId, 1, payload);
                    manager.Flush();
                }
            });

            var t3 = Task.Run(() => {
                for (int i = 0; i < 15; i++) manager.Poll();
            });

            var t4 = Task.Run(() => transport.Kick(clientId));

            await Task.WhenAll(t1, t2, t3, t4);
        }

        [Test]
        public static async Task TestInterpreterSubscriptionRace()
        {
            var config = new LiminalTransportConfig();
            var interpreter = new LiminalPacketInterpreter(config);
            object subscriber = new object();
            byte[] dummyData = new byte[10];

            ushort packetId = LiminalPacketLibrary.GetId<ChatPacket>();
            var t1 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.Subscribe<ChatPacket>((pkt, id) => { }, subscriber);
            });

            var t2 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.UnsubscribeAll(subscriber);
            });

            var t3 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.Dispatch(packetId, 1, dummyData);
            });

            await Task.WhenAll(t1, t2, t3);
        }

        [Test]
        public static async Task TestInterpreterMulticastAndGhostSubscriptionRace()
        {
            var config = new LiminalTransportConfig { MaxPacketSizePerBatch = 4096 };
            var interpreter = new LiminalPacketInterpreter(config);

            ushort chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            if (chatPacketId == 0)
            {
                LiminalPacketLibrary.Initialize();
                chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            }

            byte[] serializedChat = MessagePackSerializer.Serialize(new ChatPacket { Message = "RaceSpam" });

            interpreter.OnSendRequest += (sessionId, pid, payload) =>
            {
                if (payload.Length > 0)
                {
                    byte _ = payload[0];
                }
            };

            object target = new object();
            int postUnsubscribeHits = 0;
            int phaseComplete = 0;

            var tSubscribe = Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    interpreter.Subscribe<ChatPacket>((pkt, sender) =>
                    {
                        if (Volatile.Read(ref phaseComplete) == 1)
                        {
                            Interlocked.Increment(ref postUnsubscribeHits);
                        }
                    }, target);
                }
            });

            var tUnsubscribe = Task.Run(() =>
            {
                for (int i = 0; i < 10; i++)
                {
                    interpreter.UnsubscribeAll(target);
                }
            });

            var tDispatch = Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    interpreter.Dispatch(chatPacketId, 1, serializedChat);
                }
            });

            var tSend = Task.Run(() =>
            {
                ushort[] targets = { 1, 2, 3 };
                for (int i = 0; i < 15; i++)
                {
                    interpreter.SendCommand<ChatPacket>(targets.AsSpan(), new ChatPacket { Message = "Multi" });
                    interpreter.SendCommand(1, new ChatPacket { Message = "Uni" });
                }
            });

            await Task.WhenAll(tSubscribe, tUnsubscribe, tDispatch, tSend);

            interpreter.UnsubscribeAll(target);
            Volatile.Write(ref phaseComplete, 1);

            for (int i = 0; i < 10; i++)
            {
                interpreter.Dispatch(chatPacketId, 1, serializedChat);
            }

            Microsoft.Coyote.Specifications.Specification.Assert(
                postUnsubscribeHits == 0,
                $"GHOST SUBSCRIPTION LEAK: Target received {postUnsubscribeHits} packets after terminal UnsubscribeAll!");
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

            byte[] serializedChat = MessagePackSerializer.Serialize(new ChatPacket { Message = "Boom" });

            object target = new object();
            int ghostHit = 0;
            int phaseComplete = 0;

            Action<ChatPacket, ushort> cb1 = (pkt, sender) => { };
            Action<ChatPacket, ushort> cb2 = (pkt, sender) =>
            {
                if (Volatile.Read(ref phaseComplete) == 1)
                {
                    Interlocked.Increment(ref ghostHit);
                }
            };

            interpreter.Subscribe(cb1, target);

            var t1 = Task.Run(() =>
            {
                interpreter.Subscribe(cb2, target);
            });

            var t2 = Task.Run(() =>
            {
                interpreter.UnsubscribeAll(target);
            });

            await Task.WhenAll(t1, t2);

            interpreter.UnsubscribeAll(target);
            Volatile.Write(ref phaseComplete, 1);

            interpreter.Dispatch(chatPacketId, 1, serializedChat);

            Microsoft.Coyote.Specifications.Specification.Assert(
                ghostHit == 0,
                $"GHOST LEAK CONFIRMED: Ghost callback cb2 survived UnsubscribeAll and was invoked!");
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