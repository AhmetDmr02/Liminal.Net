using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using Microsoft.Coyote.Specifications;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class TcpClientLateShutdownCoyoteTests
    {
        //Old connection A's writer fails late while new connection B registers, whatever the order B has to survive
        [Test]
        public static async Task Coyote_TcpTransport_OldSocketLateFailureVsNewConnection_NewConnectionSurvives()
        {
            var config = new LiminalNetworkConfig
            {
                ClientIdResolver = new BaseResolver(),
                SendResponseTimeout = 1,
                MaxPacketCount = 100
            };

            var transport = new TcpTransport();
            transport.InitializeTransport(config);
            transport.SetConnectedForTesting(isConnected: false, isServer: false, isClient: true);

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            TcpClient newSocket;
            TcpClient serverSide;
            try
            {
                newSocket = new TcpClient();
                newSocket.Connect(IPAddress.Loopback, port);
                serverSide = listener.AcceptTcpClient();
            }
            finally
            {
                listener.Stop();
            }

            var oldSocket = new TcpClient();
            var shutdownIfCurrent = typeof(TcpTransport<EmptyFramingContext>).GetMethod("ShutdownIfCurrent", BindingFlags.Instance | BindingFlags.NonPublic)!;

            try
            {
                var promote = Task.Run(async () =>
                {
                    await Task.Yield();
                    transport.PromoteLocalClient(1, newSocket);
                });

                var lateFailure = Task.Run(async () =>
                {
                    await Task.Yield();
                    shutdownIfCurrent.Invoke(transport, new object[] { oldSocket });
                });

                await Task.WhenAll(promote, lateFailure);

                Specification.Assert(transport.IsConnected, "A late failure from the old socket tore down or blocked the new connection.");
                Specification.Assert(transport._sendQueues.ContainsKey(ILiminalTransport.SERVER_ID), "The new connection lost its send queue.");
            }
            finally
            {
                transport.Shutdown();
                try { newSocket.Close(); } catch { }
                try { serverSide.Close(); } catch { }
                oldSocket.Dispose();
            }
        }
    }
}
