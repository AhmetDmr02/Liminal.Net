using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Transports;
using Microsoft.Coyote.Specifications;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class TcpServerDisconnectAccountingCoyoteTests
    {
        private const ushort ClientId = 7;

        //A kick, Shutdown and the client's own reader all try to report the same disconnect
        //exactly one of them may give the slot back and at most one disconnect event may fire
        [Test]
        public static async Task Coyote_TcpTransport_KickVsShutdownVsReader_OneSlotReleaseAtMostOneEvent()
        {
            var config = new LiminalNetworkConfig
            {
                ClientIdResolver = new BaseResolver(),
                SendResponseTimeout = 1,
                MaxPacketCount = 100
            };

            var transport = new TcpTransport();
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: true, isServer: true);

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var remote = new TcpClient();
            remote.Connect((IPEndPoint)listener.LocalEndpoint);
            var serverSide = listener.AcceptTcpClient();
            listener.Stop();

            var counterField = typeof(TcpTransport<EmptyFramingContext>).GetField("_totalConnections", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var receiveLoop = typeof(TcpTransport<EmptyFramingContext>).GetMethod("ReceiveLoop", BindingFlags.Instance | BindingFlags.NonPublic)!;

            try
            {
                //Registered like a finished handshake that reserved one slot, the reader is driven by hand below
                transport.PrePromoteClient(ClientId, serverSide, out var sendState);
                counterField.SetValue(transport, 1);

                int disconnectEvents = 0;
                transport.OnClientDisconnected += _ => Interlocked.Increment(ref disconnectEvents);

                //Remote hangs up so the reader finds the end of the stream right away
                remote.Close();

                var kick = Task.Run(async () => { await Task.Yield(); transport.Kick(ClientId); });
                var shutdown = Task.Run(async () => { await Task.Yield(); transport.Shutdown(); });
                var reader = Task.Run(async () =>
                {
                    await Task.Yield();
                    try
                    {
                        await (Task)receiveLoop.Invoke(transport, new object[] { ClientId, serverSide, sendState })!;
                    }
                    catch (ObjectDisposedException)
                    {
                        //Kick or Shutdown closed the socket before this reader started, the real reader starts on promotion so this run just has no reader
                    }
                });
                await Task.WhenAll(kick, shutdown, reader);

                int counter = transport.TotalConnections;
                Specification.Assert(counter == 0, $"Connection counter is {counter}, the one reserved slot was released {1 - counter} times.");
                Specification.Assert(disconnectEvents <= 1, $"{disconnectEvents} disconnect events for one connection.");
            }
            finally
            {
                transport.Shutdown();
                try { remote.Close(); } catch { }
                try { serverSide.Close(); } catch { }
            }
        }
    }
}
