using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class TelemetryRttLifecycleCoyoteTests
    {
        private const ushort ClientId = 5;

        //SendE2EPing copies the session ids, then writes a 999 placeholder per id under _rttLock
        //a disconnect clearing the id between the copy and the write is undone, the gone client keeps an RTT entry forever
        [Test]
        public static async Task Coyote_E2EPingSample_VsClientDisconnect_NoRttEntrySurvivesForGoneClient()
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var telemetry = new LiminalTelemetryConfig { Flags = TelemetryFlags.End2EndRTT };
            var transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, telemetry, new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            transport.TriggerClientConnected(ClientId);
            Specification.Assert(server.SessionManager.HasSession(ClientId), "Session was not created on connect.");
            Specification.Assert(server.CanSend, "Server cannot send, Sample would skip the ping.");

            var sample = Task.Run(() => server.TelemetryManager.Sample());
            var disconnect = Task.Run(() => transport.TriggerClientDisconnected(ClientId));
            await Task.WhenAll(sample, disconnect);

            Specification.Assert(!server.SessionManager.HasSession(ClientId), "Session survived the disconnect.");
            Specification.Assert(!server.TelemetryManager.TryGetClientEnd2EndRTT(ClientId, out var rtt),
                $"Disconnected client {ClientId} still reports an E2E RTT of {rtt}ms.");
        }
    }
}
