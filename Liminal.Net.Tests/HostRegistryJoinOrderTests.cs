using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Registry;
using Liminal.Net.Transports;
using NUnit.Framework;
using System.Collections.Concurrent;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class HostRegistryJoinOrderTests
    {
        private static int _portCounter = 9650;

        //Server side OnClientConnected for the host's own client runs before ReadyConfirmed is sent
        //the client side only sets LocalClientId after ReadyConfirmed, so the server sees localID == 0 and registers it as remote
        [Test]
        public void HostMode_OwnClientJoinedEvent_IsReportedAsHost()
        {
            int port = Interlocked.Increment(ref _portCounter);
            var config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 10,
                HandshakeTimeout = 10
            };

            var host = new LiminalNetworkManager(new TcpTransport(), config);
            var joined = new ConcurrentQueue<ConnectedClient>();
            host.ClientRegistry.OnClientJoined += joined.Enqueue;
            int ready = 0;
            host.ClientRegistry.OnLocalClientReady += _ => Volatile.Write(ref ready, 1);

            try
            {
                host.StartHost("127.0.0.1", port);
                Assert.That(SpinWait.SpinUntil(() => host.LifecycleState == NetworkLifecycleState.HostActive, 5000), Is.True);
                //Local ready is raised after every join for the host's own client, a duplicate join would have landed by then
                SpinWait.SpinUntil(() => Volatile.Read(ref ready) == 1, 5000);

                var events = joined.ToArray();
                Assert.That(events, Has.Length.EqualTo(1), "Host's own client should join exactly once.");
                Assert.That(events[0].IsHost, Is.True, $"OnClientJoined reported the host's own client {events[0].ClientId} as IsHost=false, IsLocal={events[0].IsLocal}.");
                Assert.That(events[0].IsLocal, Is.True, "OnClientJoined reported the host's own client as not local.");
            }
            finally
            {
                host.Shutdown();
            }
        }
    }
}
