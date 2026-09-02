using Liminal.Net.Core;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Test;
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
            var config = new LiminalTransportConfig { MaxPacketCount = 5 }; // Low threshold to force queue capacity limits
            var transport = new MockTransport();
            var interpreter = new LiminalPacketInterpreter(config);
            var pipeline = new LiminalPacketFramerPipeline(config);
            var manager = new LiminalSessionManager(transport, interpreter, config, pipeline);

            ushort clientId = 42;
            transport.TriggerClientConnected(clientId);
            byte[] payload = new byte[64];

            // Thread 1: Inbound Network Flood (Background Transport)
            var t1 = Task.Run(() => {
                for (int i = 0; i < 15; i++) transport.TriggerMessageReceived(payload, clientId);
            });

            // Thread 2: Outbound Game Logic (Buffer + Flush)
            var t2 = Task.Run(() => {
                for (int i = 0; i < 15; i++)
                {
                    manager.BufferPacket(clientId, 1, payload);
                    manager.Flush();
                }
            });

            // Thread 3: Inbound Game Logic (Poll)
            var t3 = Task.Run(() => {
                for (int i = 0; i < 15; i++) manager.Poll();
            });

            // Thread 4: Surprise Kick
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

            var t1 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.Subscribe<ChatPacket>((pkt, id) => { }, subscriber);
            });

            var t2 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.UnsubscribeAll(subscriber);
            });

            var t3 = Task.Run(() => {
                for (int i = 0; i < 10; i++) interpreter.Dispatch(5, 1, dummyData); // ID 5 maps to ChatPacket
            });

            await Task.WhenAll(t1, t2, t3);
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