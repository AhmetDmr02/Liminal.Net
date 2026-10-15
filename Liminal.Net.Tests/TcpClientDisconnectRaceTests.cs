using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TcpClientDisconnectRaceTests
    {
        //Right after connecting the connect task still closes the socket when cancelled, so Disconnect wakes the reader
        //the reader must stay quiet and leave the one shutdown to Disconnect, otherwise its late shutdown kills the next connection
        [Test]
        public void DisconnectRightAfterConnecting_ShutsDownOnce_OnTheCallersThread_BeforeReturning()
        {
            var connectCtsField = typeof(TcpTransport<EmptyFramingContext>).GetField("_clientConnectCts", BindingFlags.Instance | BindingFlags.NonPublic)!;
            int badRounds = 0;
            string firstProblem = null;

            for (int round = 0; round < 200; round++)
            {
                var transport = new TcpTransport();
                transport.InitializeTransport(new LiminalNetworkConfig { ClientIdResolver = new BaseResolver(), SendResponseTimeout = 1 });
                transport.SetConnectedForTesting(isConnected: false, isServer: false, isClient: true);

                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var socket = new TcpClient();
                socket.Connect((IPEndPoint)listener.LocalEndpoint);
                var serverSide = listener.AcceptTcpClient();
                listener.Stop();

                var connectCts = new CancellationTokenSource();
                connectCts.Token.Register(() => { try { socket.Close(); } catch { } });
                connectCtsField.SetValue(transport, connectCts);

                transport.PromoteLocalClient(1, socket);
                //Let the reader start and wait on the socket, like a connection that has been up for a moment
                Thread.Sleep(10);

                int callerThread = Environment.CurrentManagedThreadId;
                int shutdowns = 0, shutdownsOffCaller = 0, localDisconnects = 0;
                transport.OnShutdown += () =>
                {
                    Interlocked.Increment(ref shutdowns);
                    if (Environment.CurrentManagedThreadId != callerThread) Interlocked.Increment(ref shutdownsOffCaller);
                };
                transport.OnLocalClientDisconnected += _ => Interlocked.Increment(ref localDisconnects);
                //Stands in for the manager's own disconnect handlers and logging, they take a few ms in a real game
                transport.OnLocalClientDisconnected += _ => Thread.Sleep(5);

                transport.Disconnect();
                int shutdownsWhenReturned = Volatile.Read(ref shutdowns);
                Thread.Sleep(30);

                string problem = null;
                if (shutdownsWhenReturned != 1) problem = $"{shutdownsWhenReturned} shutdowns had run when Disconnect returned";
                else if (Volatile.Read(ref shutdowns) != 1) problem = $"{shutdowns} shutdowns in total, one arrived after Disconnect returned";
                else if (Volatile.Read(ref shutdownsOffCaller) != 0) problem = "the reader thread ran the shutdown instead of Disconnect";
                else if (Volatile.Read(ref localDisconnects) != 1) problem = $"{localDisconnects} local disconnect events";

                if (problem != null)
                {
                    badRounds++;
                    firstProblem ??= $"round {round}: {problem}";
                }

                transport.Shutdown();
                try { serverSide.Close(); } catch { }
            }

            Assert.That(badRounds, Is.EqualTo(0), $"{badRounds}/200 rounds went wrong, first: {firstProblem}");
        }
    }
}
