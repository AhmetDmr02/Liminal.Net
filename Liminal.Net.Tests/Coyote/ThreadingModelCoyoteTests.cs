using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Test;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Buffers.Binary;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //Races that come from transport threads and the tick thread both touching session, registry and game state
    public static class ThreadingModelCoyoteTests
    {
        private const ushort ClientId = 5;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static ushort PacketId<T>()
        {
            int id = LiminalPacketLibrary.GetId<T>();
            if (id == 0)
            {
                LiminalPacketLibrary.Initialize();
                id = LiminalPacketLibrary.GetId<T>();
            }
            return (ushort)id;
        }

        private static byte[] CreateChatBatch(string message)
        {
            byte[] payload = MessagePackSerializer.Serialize(new ChatPacket { Message = message });
            byte[] batch = new byte[4 + 2 + payload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), PacketId<ChatPacket>());
            payload.CopyTo(batch.AsSpan(6));
            return batch;
        }

        private static LiminalNetworkManager StartMockServer(LiminalNetworkConfig config, out MockTransport transport)
        {
            transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            return server;
        }

        //Sticky subscribe racing another subscriber leaving the same packet, the leaver must not drop the dispatcher the joiner is being added to
        [Test]
        public static async Task Coyote_Interpreter_StickySubscribeVsOtherUnsubscribe_SubscriptionNeverLost()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out _);
            var interpreter = server.Interpreter;
            ushort packetId = PacketId<TestStickyCounterPacket>();

            object leaving = new object();
            object joining = new object();
            int received = 0;

            interpreter.Subscribe<TestStickyCounterPacket>((_, _) => { }, leaving);

            var subscribe = Task.Run(() => interpreter.Subscribe<TestStickyCounterPacket>((_, _) => Interlocked.Increment(ref received), joining));
            var unsubscribe = Task.Run(() => interpreter.UnsubscribeAll(leaving));
            await Task.WhenAll(subscribe, unsubscribe);

            byte[] payload = MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = 1 });
            interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, payload);

            Specification.Assert(received == 1,
                $"Subscriber that is still subscribed got {received} sticky packets, its dispatcher was dropped from _handlers by the other subscriber's UnsubscribeAll.");

            server.Shutdown();
        }

        //Disconnect lands on the transport thread mid poll, the game removes the player in OnClientDisconnected and must not get its packet after that
        [Test]
        public static async Task Coyote_SessionManager_DisconnectVsPoll_NoPacketHandledAfterDisconnectEvent()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out var transport);
            transport.TriggerClientConnected(ClientId);

            int playerRemoved = 0;
            int handledAfterRemoval = 0;
            object game = new object();

            server.OnClientDisconnected += id =>
            {
                if (id == ClientId) Volatile.Write(ref playerRemoved, 1);
            };
            server.Interpreter.Subscribe<ChatPacket>((_, sender) =>
            {
                if (sender == ClientId && Volatile.Read(ref playerRemoved) == 1)
                    Interlocked.Increment(ref handledAfterRemoval);
            }, game);

            transport.TriggerReliableReceived(CreateChatBatch("move"), ClientId);

            var tick = Task.Run(() => server.Ticker.TickOnce());
            var drop = Task.Run(() => transport.TriggerClientDisconnected(ClientId));
            await Task.WhenAll(tick, drop);

            Specification.Assert(handledAfterRemoval == 0,
                $"Packet from client {ClientId} was handed to the game after OnClientDisconnected({ClientId}) already ran.");

            server.Shutdown();
        }

        //Transport accepted a reliable packet and then the peer closed, the packet is still dispatched before the disconnect
        [Test]
        public static async Task Coyote_SessionManager_ReliablePacketReceivedBeforeDisconnect_IsDelivered()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out var transport);
            transport.TriggerClientConnected(ClientId);

            int delivered = 0;
            object game = new object();
            server.Interpreter.Subscribe<ChatPacket>((_, sender) =>
            {
                if (sender == ClientId) Interlocked.Increment(ref delivered);
            }, game);

            byte[] lastWords = CreateChatBatch("gg leaving");

            var receiveThread = Task.Run(() =>
            {
                transport.TriggerReliableReceived(lastWords, ClientId);
                transport.TriggerClientDisconnected(ClientId);
            });
            var tick = Task.Run(() => server.Ticker.TickOnce());
            await Task.WhenAll(receiveThread, tick);

            server.Ticker.TickOnce();

            Specification.Assert(delivered == 1,
                $"Reliable packet the transport delivered before the disconnect was dispatched {delivered} times.");

            server.Shutdown();
        }

        //Starvation handler excludes the starving client (takes the syncvar visibility lock) while the tick's flush holds that lock and sends to the same session
        //the event must not be raised under SendLock or the two wait on each other
        [Test]
        public static async Task Coyote_StarvationEventUnderSendLock_VsSyncVarFlush_NoDeadlock()
        {
            var config = CreateConfig();
            config.MaxPacketSizePerBatch = 512;
            var server = StartMockServer(config, out var transport);
            transport.TriggerClientConnected(ClientId);

            var health = new SyncVar<int>($"starve_{Guid.NewGuid():N}", 100, server.SyncVarManager);
            health.Value = 90;

            server.SessionManager.OnServerStarvation += e => health.AddExclusion(e.ClientId);

            //Over MaxPacketSizePerBatch so BufferPacket enters outbound recovery and raises starvation
            var gameSend = Task.Run(() =>
                server.Interpreter.SendCommandAsServer(ClientId, new ChatPacket { Message = new string('x', 600) }));
            var tick = Task.Run(() => server.Ticker.TickOnce());
            await Task.WhenAll(gameSend, tick);

            server.Shutdown();
        }
    }
}
