using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Buffers.Binary;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class ConcurrencyAuditCoyoteTests
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

        private static byte[] Counter(int value) =>
            MessagePackSerializer.Serialize(new TestStickyCounterPacket { Counter = value });

        private static LiminalNetworkManager StartMockServer(LiminalNetworkConfig config, out MockTransport transport)
        {
            transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            return server;
        }

        //ManualPoll is what LiminalUnityBridge.Update calls every frame, it never goes through the ticker so Stop doesnt wait for it
        //a teardown on the transport thread then raises OnLocalClientDisconnected while the main thread is still dispatching
        [Test]
        public static async Task Coyote_ManualPoll_VsTransportTeardown_NoPacketHandledAfterDisconnectEvent()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = false, LocalClientId = ClientId };
            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            client.StartClient("127.0.0.1", 7777);

            transport.TriggerLocalClientConnected(ClientId);
            client.ManualPoll();

            int disconnectRaised = 0;
            int handledAfterDisconnect = 0;
            object game = new object();

            client.OnLocalClientDisconnected += _ => Volatile.Write(ref disconnectRaised, 1);
            client.Interpreter.Subscribe<ChatPacket>((_, _) =>
            {
                if (Volatile.Read(ref disconnectRaised) == 1)
                    Interlocked.Increment(ref handledAfterDisconnect);
            }, game);

            transport.TriggerReliableReceived(CreateChatBatch("last words"), ILiminalTransport.SERVER_ID);

            var mainThread = Task.Run(() => client.ManualPoll());
            var transportThread = Task.Run(() =>
            {
                transport.TriggerLocalClientDisconnected(ClientId);
                transport.TriggerShutdown();
            });
            await Task.WhenAll(mainThread, transportThread);

            Specification.Assert(handledAfterDisconnect == 0,
                $"ManualPoll dispatched a server packet {handledAfterDisconnect} time(s) after teardown already raised OnLocalClientDisconnected.");

            client.Shutdown();
        }

        //ClearSticky on the game thread resets every sticky subscription, that also wipes the pending entry of a packet
        //that arrived right after the clear, the store then holds a value its subscribers were never told about
        [Test]
        public static async Task Coyote_ClearSticky_VsDispatch_SubscriberNeverMissesStoredValue()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out _);
            var interpreter = server.Interpreter;
            ushort packetId = PacketId<TestStickyCounterPacket>();

            int lastSeen = 0;
            object game = new object();
            interpreter.Subscribe<TestStickyCounterPacket>((p, _) => Volatile.Write(ref lastSeen, p.Counter), game);

            interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, Counter(1));

            var clear = Task.Run(() => interpreter.ClearSticky<TestStickyCounterPacket>());
            var dispatch = Task.Run(() => interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, Counter(2)));
            await Task.WhenAll(clear, dispatch);

            if (interpreter.TryGetSticky<TestStickyCounterPacket>(out var stored) && stored.Counter == 2)
            {
                Specification.Assert(Volatile.Read(ref lastSeen) == 2,
                    $"Sticky store holds counter 2 but the subscriber last saw {lastSeen}, the notification was dropped by ClearSticky's reset.");
            }

            server.Shutdown();
        }

        //A dispatch that finds its entry disposed removes whatever entry is under the id now, which can be a newer one
        //another dispatch already filled after the clear, the newest sticky state gets replaced by an older one
        [Test]
        public static async Task Coyote_StickyDispatch_RetryAfterClear_NeverRemovesNewerEntry()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out _);
            var interpreter = server.Interpreter;
            ushort packetId = PacketId<TestStickyCounterPacket>();

            int lastSeen = 0;
            object game = new object();
            interpreter.Subscribe<TestStickyCounterPacket>((p, _) => Volatile.Write(ref lastSeen, p.Counter), game);

            interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, Counter(1));

            var first = Task.Run(() => interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, Counter(2)));
            var clearThenSecond = Task.Run(() =>
            {
                interpreter.ClearSticky<TestStickyCounterPacket>();
                interpreter.Dispatch(packetId, ILiminalTransport.SERVER_ID, Counter(3));
            });
            await Task.WhenAll(first, clearThenSecond);

            //Subscriber ending on 3 means 2 was dropped as the lower version, so the store holding 2 went backwards
            bool found = interpreter.TryGetSticky<TestStickyCounterPacket>(out var stored);
            Specification.Assert(!(found && stored.Counter == 2 && Volatile.Read(ref lastSeen) == 3),
                "Sticky store went back to 2 after the subscriber already got the newer 3, the retry removed the live entry holding 3.");

            server.Shutdown();
        }

        //The kick task acts on the client id when it wakes up, not on the connection it was started for
        //if that client dropped during the grace period and the same id came back, the new connection gets kicked
        [Test]
        public static async Task Coyote_KickWithReason_StaleGraceTask_NeverKicksReconnectedClient()
        {
            var config = CreateConfig();
            config.WaitForKickGracePeriod = 1;
            var server = StartMockServer(config, out var transport);
            transport.TriggerClientConnected(ClientId);

            int reconnected = 0;
            int newConnectionKicked = 0;
            transport.OnClientKicked += client =>
            {
                if (client.PeerId == ClientId && Volatile.Read(ref reconnected) == 1)
                    Interlocked.Increment(ref newConnectionKicked);
            };

            server.DisconnectCoordinator.ServerKickWithReason(ClientId, DisconnectReason.Kicked, "afk");

            var transportThread = Task.Run(() =>
            {
                transport.TriggerClientDisconnected(ClientId);
                transport.TriggerClientConnected(ClientId);
                Volatile.Write(ref reconnected, 1);
            });
            await transportThread;

            for (int i = 0; i < 20 && !transport.KickedClients.Contains(ClientId); i++)
                await Task.Delay(10);

            Specification.Assert(newConnectionKicked == 0,
                $"Kick grace task for the old connection of client {ClientId} kicked the new connection that reused the id.");

            server.Shutdown();
        }
    }
}
