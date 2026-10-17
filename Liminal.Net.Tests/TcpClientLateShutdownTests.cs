using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using NUnit.Framework;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TcpClientLateShutdownTests
    {
        private static int _port = 25700;

        private static LiminalNetworkConfig Config(int port) => new LiminalNetworkConfig
        {
            Default_Host = "127.0.0.1",
            Default_Port = port,
            TickRate = 60,
            ClientIdResolver = new BaseResolver(),
            HandshakeTimeout = 15,
            ConnectionTimeout = 15
        };

        //Runs the client send loop on its own socket and makes its next write fail, like an old connection's writer failing late
        private static async Task RunFailingSendLoop(TcpTransport transport, TcpClient socket)
        {
            var state = new TcpTransport<EmptyFramingContext>.ClientSendState(1, socket);
            var sendLoop = typeof(TcpTransport<EmptyFramingContext>).GetMethod("ProcessSendQueueAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var loop = (Task)sendLoop.Invoke(transport, new object[] { ILiminalTransport.SERVER_ID, socket, state })!;

            //Loop already grabbed the stream and is waiting for work, closing now makes the write fail
            socket.Client.Close();
            state.Channel.Writer.TryWrite(new TcpTransport<EmptyFramingContext>.OutboundPacket(new byte[16], 16));
            state.Channel.Writer.TryComplete();

            Assert.That(await Task.WhenAny(loop, Task.Delay(3000)), Is.SameAs(loop), "Send loop never finished.");
        }

        private static TcpClient Connect(TcpListener listener, out TcpClient accepted)
        {
            var socket = new TcpClient();
            socket.Connect((IPEndPoint)listener.LocalEndpoint);
            accepted = listener.AcceptTcpClient();
            return socket;
        }

        [Test]
        public async Task OldConnectionsSendLoopFailingLate_DoesNotTearDownTheCurrentConnection()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var clientTransport = new TcpTransport();
            var client = new LiminalNetworkManager(clientTransport, Config(port));

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            TcpClient staleSocket = null, accepted = null;
            try
            {
                client.StartClient("127.0.0.1", port);
                Assert.That(SpinWait.SpinUntil(() => client.IsConnected && clientTransport.IsConnected, 3000), Is.True, "Precondition: client never connected.");

                staleSocket = Connect(listener, out accepted);

                await RunFailingSendLoop(clientTransport, staleSocket);

                Assert.That(SpinWait.SpinUntil(() => !clientTransport.IsConnected, 300), Is.False,
                    "A send failure on an old socket shut down the client's current connection.");
                Assert.That(client.IsConnected, Is.True);
            }
            finally
            {
                staleSocket?.Dispose();
                accepted?.Dispose();
                listener.Stop();
                client.Shutdown();
                server.Shutdown();
            }
        }

        [Test]
        public async Task CurrentConnectionsSendLoopFailing_StillShutsTheClientDown()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var clientTransport = new TcpTransport();
            var client = new LiminalNetworkManager(clientTransport, Config(port));

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            TcpClient brokenSocket = null, accepted = null;
            try
            {
                client.StartClient("127.0.0.1", port);
                Assert.That(SpinWait.SpinUntil(() => client.IsConnected && clientTransport.IsConnected, 3000), Is.True, "Precondition: client never connected.");

                brokenSocket = Connect(listener, out accepted);

                //Make our socket the current connection, the real one stays healthy so only the send loop can shut the client down
                var sockets = (System.Collections.Concurrent.ConcurrentDictionary<ushort, TcpClient>)typeof(TcpTransport<EmptyFramingContext>)
                    .GetField("_sockets", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(clientTransport)!;
                sockets[ILiminalTransport.SERVER_ID] = brokenSocket;

                await RunFailingSendLoop(clientTransport, brokenSocket);

                Assert.That(SpinWait.SpinUntil(() => !clientTransport.IsConnected, 3000), Is.True,
                    "A send failure on the current connection no longer shuts the client down.");
            }
            finally
            {
                brokenSocket?.Dispose();
                accepted?.Dispose();
                listener.Stop();
                client.Shutdown();
                server.Shutdown();
            }
        }

        //Subclass cleanup lives in OnShutdownCompleted, it still has to run on every real shutdown
        [Test]
        public void LatencySimulator_Shutdown_StopsItsDrainThread()
        {
            var transport = new LatencySimulatorTransport();
            var threadField = typeof(LatencySimulatorTransport).GetField("_drainThread", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var drainThread = (Thread)threadField.GetValue(transport)!;
            Assert.That(drainThread.IsAlive, Is.True, "Precondition: drain thread not running.");

            transport.Shutdown();

            Assert.That(threadField.GetValue(transport), Is.Null, "Shutdown left the simulator's drain thread registered.");
            Assert.That(drainThread.Join(2000), Is.True, "Shutdown did not stop the simulator's drain thread.");
        }
    }
}
