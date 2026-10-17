using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //A shutdown that belongs to the previous connection must never end the session the game started after Disconnect returned
    public static class TransportRestartCoyoteTests
    {
        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static void AssertNewSessionAlive(LiminalNetworkManager client, TcpTransport transport, string cause)
        {
            Specification.Assert(client.Role == NetworkRole.Client && client.LifecycleState == NetworkLifecycleState.Connecting,
                $"{cause} ended the restarted session, manager is {client.Role}/{client.LifecycleState}.");
            Specification.Assert(transport.IsClient,
                $"{cause} reset the transport after the restart, the new connect attempt is dead.");
        }

        //Old connection's writer fails and resets the transport while the game disconnects and starts again
        //its OnShutdown is held back on that thread until the restart is done, standing in for it getting preempted before the manager hears about it
        [Test]
        public static async Task Coyote_OldConnectionShutdown_VsDisconnectAndRestart_NewSessionSurvives()
        {
            var config = CreateConfig();
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var transport = new TcpTransport();
            //Attempts never finish on their own, the test drives the old connection by hand
            transport.ClientHandshaker = (_, _) => new TaskCompletionSource<HandshakeResult>().Task;

            int failingThread = 0;
            var restarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            //Subscribed before the manager so it runs ahead of the session manager's own handler
            transport.OnShutdown += () =>
            {
                if (Environment.CurrentManagedThreadId == Volatile.Read(ref failingThread))
                {
                    for (int i = 0; i < 5000 && !restarted.Task.IsCompleted; i++)
                        Task.Delay(1).Wait();
                }
            };

            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));

            var oldSocket = new TcpClient();
            TcpClient serverSide = null;
            try
            {
                client.StartClient("127.0.0.1", port);
                oldSocket.Connect(IPAddress.Loopback, port);
                serverSide = listener.AcceptTcpClient();
                transport.PromoteLocalClient(1, oldSocket);

                var shutdownIfCurrent = typeof(TcpTransport<EmptyFramingContext>).GetMethod("ShutdownIfCurrent", BindingFlags.Instance | BindingFlags.NonPublic)!;

                var oldConnectionFails = Task.Run(() =>
                {
                    Volatile.Write(ref failingThread, Environment.CurrentManagedThreadId);
                    shutdownIfCurrent.Invoke(transport, new object[] { oldSocket });
                });
                var gameRestarts = Task.Run(() =>
                {
                    client.Disconnect();
                    client.StartClient("127.0.0.1", port);
                    restarted.TrySetResult(true);
                });
                await Task.WhenAll(oldConnectionFails, gameRestarts);

                //Whatever the old connection still queued must stay with its own run once the new one drains
                client.ManualPoll();

                AssertNewSessionAlive(client, transport, "Old connection's shutdown");
            }
            finally
            {
                client.Shutdown();
                try { listener.Stop(); } catch { }
                try { serverSide?.Close(); } catch { }
                oldSocket.Dispose();
            }
        }

        //Previous connect attempt's handshake fails right as the game disconnects and starts again
        [Test]
        public static async Task Coyote_StaleConnectAttemptFailure_VsDisconnectAndRestart_NewSessionSurvives()
        {
            var config = CreateConfig();
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var attempts = new ConcurrentQueue<TaskCompletionSource<HandshakeResult>>();
            var transport = new TcpTransport();
            transport.ClientHandshaker = (_, _) =>
            {
                var attempt = new TaskCompletionSource<HandshakeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                attempts.Enqueue(attempt);
                return attempt.Task;
            };
            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));

            try
            {
                client.StartClient("127.0.0.1", port);

                TaskCompletionSource<HandshakeResult> oldAttempt = null;
                for (int i = 0; i < 200 && !attempts.TryPeek(out oldAttempt); i++)
                    await Task.Delay(5);
                //Real connect didnt land in time this iteration, nothing to race
                if (oldAttempt == null)
                    return;

                var oldAttemptFails = Task.Run(() => oldAttempt.TrySetResult(HandshakeResult.Fail(DisconnectReason.VersionMismatch, "old attempt")));
                var gameRestarts = Task.Run(() =>
                {
                    try
                    {
                    client.Disconnect();
                    client.StartClient("127.0.0.1", port);
                    }
                    catch (Exception dbgEx) { Specification.Assert(false, "DBGEX " + dbgEx.ToString().Replace(Environment.NewLine, " <- ")); }
                });
                await Task.WhenAll(oldAttemptFails, gameRestarts);

                //Old attempt's continuation runs async, give it room to finish whatever it does
                for (int i = 0; i < 20; i++)
                    await Task.Delay(5);

                client.ManualPoll();

                AssertNewSessionAlive(client, transport, "Previous connect attempt's failure");
            }
            finally
            {
                client.Shutdown();
                try { listener.Stop(); } catch { }
            }
        }
    }
}
