using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Unity;

namespace Liminal.Net.Tests
{
    //Mock server and client wired together in memory, nothing moves until Pump ticks both
    internal sealed class InMemoryServerClient
    {
        public const ushort ClientId = 2;

        public LiminalNetworkManager Server;
        public LiminalNetworkManager Client;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static void Relay(MockTransport to, byte[] data, ushort from, TransportFlags flags)
        {
            if ((flags & TransportFlags.Fragmented) != 0) to.TriggerFragmentReceived(data, from);
            else if ((flags & TransportFlags.Reliable) != 0) to.TriggerReliableReceived(data, from);
            else to.TriggerUnreliableReceived(data, from);
        }

        public static InMemoryServerClient Connect()
        {
            var serverConfig = CreateConfig();
            var clientConfig = CreateConfig();
            var serverTransport = new MockTransport { IsServer = true };
            var clientTransport = new MockTransport { IsServer = false, LocalClientId = ClientId };

            var pair = new InMemoryServerClient
            {
                Server = new LiminalNetworkManager(serverTransport, serverConfig, customTicker: new LiminalUnityTicker(serverConfig)),
                Client = new LiminalNetworkManager(clientTransport, clientConfig, customTicker: new LiminalUnityTicker(clientConfig))
            };

            serverTransport.OnSendHook = (data, target, flags) =>
            {
                if (target == ClientId) Relay(clientTransport, data, ILiminalTransport.SERVER_ID, flags);
            };
            clientTransport.OnSendHook = (data, target, flags) => Relay(serverTransport, data, ClientId, flags);

            pair.Server.StartServer("127.0.0.1", 7777);
            pair.Client.StartClient("127.0.0.1", 7777);
            clientTransport.TriggerLocalClientConnected(ClientId);
            serverTransport.ConnectedClients.Add(ClientId);
            serverTransport.TriggerClientConnected(ClientId);
            pair.Pump();
            return pair;
        }

        public bool IsConnected => Client.IsConnected && Server.SessionManager.HasSession(ClientId);

        public void Pump(int rounds = 4)
        {
            for (int i = 0; i < rounds; i++)
            {
                Server.Ticker.TickOnce();
                Client.Ticker.TickOnce();
            }
        }

        //Same state a reconnect leaves on the client: last session's SyncVar registered again as a replaceable leftover
        public SyncVar<int> CreateClientLeftover(string token)
        {
            var leftover = new SyncVar<int>(token, 0, Client.SyncVarManager);
            Client.SyncVarManager.ResetSessionState();
            Client.SyncVarManager.AdoptOrphans();
            return leftover;
        }

        public static bool IsRegistered(LiminalNetworkManager manager, ISyncVarInternal syncVar)
            => manager.SyncVarManager.TryGetSyncVar(syncVar.Token, out var registered) && ReferenceEquals(registered, syncVar);

        public void RequestResyncFromClient(string token)
        {
            Server.Interpreter.Dispatch(
                LiminalPacketLibrary.GetId<SyncVarResyncRequestPacket>(),
                ClientId,
                MessagePack.MessagePackSerializer.Serialize(new SyncVarResyncRequestPacket { Token = token }));
        }

        public void Shutdown()
        {
            Client.Shutdown();
            Server.Shutdown();
        }
    }
}
