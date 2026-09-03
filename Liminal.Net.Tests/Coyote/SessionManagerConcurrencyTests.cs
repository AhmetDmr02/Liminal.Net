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
        public static async Task TestInterpreterMulticastAndBufferRace()
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

            object subscriberA = new object();
            object subscriberB = new object();

            ushort[] targetPool = new ushort[] { 1, 2, 3, 4, 5 };

            var t1 = Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    interpreter.SendCommand<ChatPacket>(targetPool.AsSpan(0, 3), new ChatPacket { Message = $"Multi_{i}" });
                }
            });

            var t2 = Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    interpreter.SendCommand(targetPool[i % targetPool.Length], new ChatPacket { Message = $"Uni_{i}" });
                }
            });

            var t3 = Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, subscriberA);
                    if (i % 3 == 0) interpreter.UnsubscribeAll(subscriberA);
                }
            });

            var t4 = Task.Run(() =>
            {
                interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, subscriberB);
                for (int i = 0; i < 20; i++)
                {
                    interpreter.Dispatch(chatPacketId, 1, serializedChat);
                }
                interpreter.UnsubscribeAll(subscriberB);
            });

            await Task.WhenAll(t1, t2, t3, t4);
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