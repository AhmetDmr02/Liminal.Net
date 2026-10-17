using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using System.Collections.Concurrent;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class DisconnectReasonGenerationCoyoteTests
    {
        private const ushort ClientId = 5;

        private static LiminalNetworkManager StartMockServer(out MockTransport transport)
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            return server;
        }

        //ServerKickWithReason snapshots the connection, then writes the reason under the bare client id
        //a reconnect landing between the two leaves the old kick's reason on the new connection, which then resolves as Kicked when it just drops
        [Test]
        public static async Task Coyote_KickOfOldConnection_VsReconnect_ReasonNeverLeaksToNewConnection()
        {
            var server = StartMockServer(out var transport);
            var coordinator = server.DisconnectCoordinator;
            transport.TriggerClientConnected(ClientId);
            var original = transport.GetConnection(ClientId);

            //The first GetConnection inside the kick is the snapshot its grace-period kick will target
            var inKick = new AsyncLocal<bool>();
            var snapshots = new ConcurrentQueue<LiminalConnection>();
            transport.OnGetConnection = connection =>
            {
                if (inKick.Value) snapshots.Enqueue(connection);
            };

            var kick = Task.Run(() =>
            {
                inKick.Value = true;
                coordinator.ServerKickWithReason(ClientId, DisconnectReason.Kicked, "old connection kicked", graceSeconds: 0);
            });
            var reconnect = Task.Run(() =>
            {
                transport.TriggerClientDisconnected(ClientId);
                transport.TriggerClientConnected(ClientId);
            });

            await Task.WhenAll(kick, reconnect);
            transport.OnGetConnection = null;

            Specification.Assert(snapshots.TryPeek(out var kicked), "Kick never looked up its connection.");
            //A kick that saw the reconnect targets it on purpose, Kicked is then the right reason
            if (kicked.IsValid && kicked != original)
                return;

            var current = transport.GetConnection(ClientId);

            var resolved = new ConcurrentQueue<DisconnectReason>();
            coordinator.OnResolved += (id, reason, _) =>
            {
                if (id == ClientId) resolved.Enqueue(reason);
            };

            transport.TriggerClientDisconnected(ClientId);

            Specification.Assert(resolved.TryDequeue(out var newConnectionReason), $"Connection {current}'s drop never resolved, the kick targeted {kicked}.");
            Specification.Assert(newConnectionReason != DisconnectReason.Kicked,
                $"Connection {current} resolved as Kicked, the kick targeted {kicked}.");
        }
    }
}
