using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //Library work that runs on transport threads racing the tick/game thread
    public static class TransportThreadBleedCoyoteTests
    {
        private const ushort ClientId = 2;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static string UniqueToken(string name) => $"{name}_{Guid.NewGuid():N}";

        private static Task RunCapturing(Action body, Action<Exception> onError) => Task.Run(() =>
        {
            try { body(); }
            catch (Exception ex) { onError(ex); }
        });

        private static void Relay(MockTransport to, byte[] data, ushort from, TransportFlags flags)
        {
            if ((flags & TransportFlags.Fragmented) != 0) to.TriggerFragmentReceived(data, from);
            else if ((flags & TransportFlags.Reliable) != 0) to.TriggerReliableReceived(data, from);
            else to.TriggerUnreliableReceived(data, from);
        }

        //Server join snapshot is built on the accept thread (OnClientConnected) while the tick thread flushes dirty syncvars
        //the delta can reach the client before the older snapshot, and the snapshot is applied with force so it wins
        [Test]
        public static async Task Coyote_ClientJoin_SnapshotVsDirtyFlush_ClientEndsOnServerValue()
        {
            Exception failure = null;
            var serverConfig = CreateConfig();
            var clientConfig = CreateConfig();
            var serverTransport = new MockTransport { IsServer = true };
            var clientTransport = new MockTransport { IsServer = false, LocalClientId = ClientId };
            var server = new LiminalNetworkManager(serverTransport, serverConfig, customTicker: new LiminalUnityTicker(serverConfig));
            var client = new LiminalNetworkManager(clientTransport, clientConfig, customTicker: new LiminalUnityTicker(clientConfig));

            serverTransport.OnSendHook = (data, target, flags) =>
            {
                if (target == ClientId) Relay(clientTransport, data, ILiminalTransport.SERVER_ID, flags);
            };
            clientTransport.OnSendHook = (data, target, flags) => Relay(serverTransport, data, ClientId, flags);

            server.StartServer("127.0.0.1", 7777);
            client.StartClient("127.0.0.1", 7777);
            clientTransport.TriggerLocalClientConnected(ClientId);

            string token = UniqueToken("join_snapshot_vs_flush");
            var serverVar = new SyncVar<int>(token, 1, server.SyncVarManager);
            server.Ticker.TickOnce();
            var clientVar = new SyncVar<int>(token, 0, client.SyncVarManager);

            var arrivals = new ConcurrentQueue<string>();
            client.Interpreter.Subscribe<SyncVarSnapshotPacket>((_, _) => arrivals.Enqueue("snapshot"), arrivals);
            client.Interpreter.Subscribe<SyncVarSlabBatchPacket>((_, _) => arrivals.Enqueue("delta"), arrivals);

            var accept = RunCapturing(() =>
            {
                serverTransport.ConnectedClients.Add(ClientId);
                serverTransport.TriggerClientConnected(ClientId);
            }, ex => failure = ex);

            var game = RunCapturing(() =>
            {
                serverVar.Value = 2;
                server.Ticker.TickOnce();
            }, ex => failure = ex);

            await Task.WhenAll(accept, game);

            for (int i = 0; i < 4; i++)
            {
                server.Ticker.TickOnce();
                client.Ticker.TickOnce();
            }

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(serverVar.Value == 2, $"Server has {serverVar.Value}, expected 2.");
            Specification.Assert(clientVar.Value == 2,
                $"Client is stuck on {clientVar.Value} while the server has 2, client received: {string.Join(", ", arrivals)}");

            client.Shutdown();
            server.Shutdown();
        }

        //Kick lands on a transport thread while the game thread tears the session down
        //LiminalEventHub checks _sessionManager for null and then reads it again to use it
        [Test]
        public static async Task Coyote_EventHub_KickVsTeardown_KickNeverThrows()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            transport.TriggerClientConnected(5);

            Exception kickFailure = null;
            Exception teardownFailure = null;

            var kick = RunCapturing(() => transport.Kick(5), ex => kickFailure = ex);
            var teardown = RunCapturing(() => server.Disconnect(), ex => teardownFailure = ex);
            await Task.WhenAll(kick, teardown);

            Specification.Assert(teardownFailure == null, $"Disconnect threw: {teardownFailure}");
            Specification.Assert(kickFailure == null, $"Kick threw out of the transport and the remaining OnClientKicked subscribers never ran: {kickFailure}");

            server.Shutdown();
        }

        //PromoteLocalClient raises OnLocalClientConnected outside _connectionLifecycleLock without checking again
        //a Disconnect that runs in that gap raises Disconnected first and the late Connected comes after it
        [Test]
        public static async Task Coyote_TcpTransport_PromoteVsDisconnect_ConnectedNeverRaisedAfterDisconnected()
        {
            var transport = new TcpTransport();
            transport.InitializeTransport(CreateConfig());
            transport.SetConnectedForTesting(isConnected: false, isServer: false, isClient: true);

            var events = new ConcurrentQueue<string>();
            transport.OnLocalClientConnected += _ => events.Enqueue("connected");
            transport.OnLocalClientDisconnected += _ => events.Enqueue("disconnected");

            var socket = new TcpClient();
            var promote = Task.Run(() => transport.PromoteLocalClient(ClientId, socket));
            var disconnect = Task.Run(() => transport.Disconnect());
            await Task.WhenAll(promote, disconnect);

            var seen = events.ToArray();
            bool endsConnected = seen.Length > 0 && seen[^1] == "connected";

            Specification.Assert(!(endsConnected && !transport.IsConnected),
                $"Transport is shut down but the last event the game saw was Connected: {string.Join(", ", seen)}");

            try { socket.Close(); } catch { }
        }

        //Unity bridge queues connection events into a ring buffer but packets go through the session queues
        //the transport thread connects and delivers a packet while the main thread runs Update, the game must see the event first
        [Test]
        public static async Task Coyote_UnityBridge_PacketNeverDispatchedBeforeConnectEvent()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = false, LocalClientId = ClientId };
            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            var bridge = new LiminalUnityBridge();
            bridge.Attach(client);

            var order = new ConcurrentQueue<string>();
            bridge.OnLocalClientConnected += _ => order.Enqueue("connected");
            client.Interpreter.Subscribe<ChatPacket>((_, _) => order.Enqueue("packet"), order);

            client.StartClient("127.0.0.1", 7777);

            byte[] batch = CreateChatBatch("hello");
            var transportThread = Task.Run(() =>
            {
                transport.TriggerLocalClientConnected(ClientId);
                transport.TriggerReliableReceived(batch, ILiminalTransport.SERVER_ID);
            });
            var mainThread = Task.Run(() => bridge.Update());
            await Task.WhenAll(transportThread, mainThread);

            bridge.Update();

            var seen = order.ToArray();
            Specification.Assert(seen.Length == 2, $"Expected both the event and the packet, got: {string.Join(", ", seen)}");
            Specification.Assert(seen[0] == "connected", $"The game got a packet from the server before OnLocalClientConnected: {string.Join(", ", seen)}");

            bridge.Dispose();
            client.Shutdown();
        }

        //Two kicks for the same client from different threads, each event must be raised once
        [Test]
        public static async Task Coyote_TcpTransport_ConcurrentKicks_EventsRaisedOnce()
        {
            var transport = new TcpTransport();
            transport.InitializeTransport(CreateConfig());
            transport.SetConnectedForTesting(isConnected: true, isServer: true, isClient: false);

            int kicked = 0;
            int disconnected = 0;
            transport.OnClientKicked += _ => Interlocked.Increment(ref kicked);
            transport.OnClientDisconnected += _ => Interlocked.Increment(ref disconnected);

            var socket = new TcpClient();
            transport.PrePromoteClient(5, socket, out _);

            var first = Task.Run(() => transport.Kick(5));
            var second = Task.Run(() => transport.Kick(5));
            await Task.WhenAll(first, second);

            Specification.Assert(kicked == 1, $"OnClientKicked raised {kicked} times for one connection.");
            Specification.Assert(disconnected == 1, $"OnClientDisconnected raised {disconnected} times for one connection.");
            Specification.Assert(!transport.IsClientConnected(5), "Kicked client is still registered.");
        }

        //A fragment read just before the disconnect must not leave a peer and its assembly buffer behind after the cleanup
        [Test]
        public static async Task Coyote_Fragmentor_FragmentVsDisconnect_NoAssemblyLeftBehind()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(5);
            var fragmentor = new LiminalPacketFragmentor(transport, config);

            int payload = fragmentor.MTU - LiminalTransportHeader.BaseHeaderSize - LiminalPacketFragmentor.FragmentHeaderSize;
            byte[] fragment = new byte[LiminalPacketFragmentor.FragmentHeaderSize + payload];
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(4, 2), 2);

            var ingest = Task.Run(() => fragmentor.IngestFragment(fragment, 5));
            var disconnect = Task.Run(() => transport.TriggerClientDisconnected(5));
            await Task.WhenAll(ingest, disconnect);

            Specification.Assert(fragmentor.PeersWithPendingAssemblies == 0, "A disconnected client still has a pending assembly.");
            Specification.Assert(fragmentor.OutstandingAssemblyBuffers == 0, $"{fragmentor.OutstandingAssemblyBuffers} assembly buffers leaked for a disconnected client.");

            fragmentor.Dispose();
        }

        //Loopback cap was a count check then an enqueue, concurrent senders could all pass it
        [Test]
        public static async Task Coyote_SessionManager_ConcurrentLoopbackSends_NeverExceedCap()
        {
            var config = CreateConfig();
            config.MaxPacketCount = 2;
            var transport = new MockTransport { IsServer = false, LocalClientId = ClientId };
            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            var sessions = client.SessionManager;
            ushort packetId = LiminalPacketLibrary.GetId<ChatPacket>();
            byte[] payload = MessagePackSerializer.Serialize(new ChatPacket { Message = "x" });

            var senders = new Task[3];
            for (int i = 0; i < senders.Length; i++)
            {
                senders[i] = Task.Run(() => sessions.BufferPacket(ClientId, ClientId, packetId, payload));
            }
            await Task.WhenAll(senders);

            int queued = sessions.GetGlobalSnapshot().LoopbackQueueCount;
            Specification.Assert(queued <= config.MaxPacketCount, $"Loopback queue holds {queued} packets, cap is {config.MaxPacketCount}.");

            client.Shutdown();
        }

        private static byte[] CreateChatBatch(string message)
        {
            ushort chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            if (chatPacketId == 0)
            {
                LiminalPacketLibrary.Initialize();
                chatPacketId = LiminalPacketLibrary.GetId<ChatPacket>();
            }

            byte[] payload = MessagePackSerializer.Serialize(new ChatPacket { Message = message });

            byte[] batch = new byte[4 + 2 + payload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), chatPacketId);
            payload.CopyTo(batch.AsSpan(6));
            return batch;
        }
    }
}
