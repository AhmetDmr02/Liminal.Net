using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class ManagerShutdownSemanticsTests
    {
        private static int _port = 26500;

        private static LiminalNetworkConfig Config(int port) => new LiminalNetworkConfig
        {
            Default_Host = "127.0.0.1",
            Default_Port = port,
            TickRate = 60,
            ClientIdResolver = new BaseResolver(),
            HandshakeTimeout = 15,
            ConnectionTimeout = 15
        };

        private static (LiminalNetworkManager server, LiminalNetworkManager client) Connect()
        {
            int port = Interlocked.Increment(ref _port);
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            server.StartServer("127.0.0.1", port);
            var client = new LiminalNetworkManager(new TcpTransport(), Config(port));
            client.StartClient("127.0.0.1", port);
            Assert.That(SpinWait.SpinUntil(() => client.IsConnected && server.SessionManager.HasSession(client.localID), 3000), Is.True);
            return (server, client);
        }

        //A start that fails cleans up only that attempt, the game's subscriptions stay and the same manager can retry
        [Test]
        public void FailedStart_KeepsSubscriptions_AndTheManagerCanRetry()
        {
            int port = Interlocked.Increment(ref _port);
            var blocker = new TcpListener(IPAddress.Loopback, port);
            blocker.Start();
            var server = new LiminalNetworkManager(new TcpTransport(), Config(port));
            int serverStarted = 0;
            server.OnServerStarted += () => Interlocked.Increment(ref serverStarted);
            try
            {
                bool firstStart;
                try { firstStart = server.StartServer("127.0.0.1", port); }
                catch (Exception) { firstStart = false; }
                Assert.That(firstStart, Is.False, "Precondition: starting on a busy port should fail.");

                blocker.Stop();
                Assert.That(server.StartServer("127.0.0.1", port), Is.True, "The manager could not start again after a failed start.");
                Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref serverStarted) == 1, 2000), Is.True,
                    "A failed start removed the game's OnServerStarted subscription.");
            }
            finally
            {
                blocker.Stop();
                server.Shutdown();
            }
        }

        [Test]
        public void Shutdown_IsFinal_StartAfterwardsIsRefused()
        {
            var (server, client) = Connect();
            try
            {
                client.Shutdown();
                int port = _port;

                Assert.That(client.StartClient("127.0.0.1", port), Is.False, "StartClient worked on a manager that was shut down.");
                Assert.That(client.StartServer("127.0.0.1", port + 1000), Is.False, "StartServer worked on a manager that was shut down.");
                Assert.That(client.StartHost("127.0.0.1", port + 1001), Is.False, "StartHost worked on a manager that was shut down.");
                Assert.That(client.IsConnected, Is.False);
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        [Test]
        public void Dispose_IsTheSameAsShutdown()
        {
            var (server, client) = Connect();
            try
            {
                ((IDisposable)client).Dispose();
                Assert.That(client.IsConnected, Is.False);
                Assert.That(client.StartClient("127.0.0.1", _port), Is.False);
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        //Disconnect keeps the manager reusable, it is the call for leaving and reconnecting
        [Test]
        public void Disconnect_KeepsSubscriptions_AndReconnects()
        {
            var (server, client) = Connect();
            int localConnected = 0;
            client.OnLocalClientConnected += _ => Interlocked.Increment(ref localConnected);
            try
            {
                client.Disconnect();
                Assert.That(client.StartClient("127.0.0.1", _port), Is.True);
                Assert.That(SpinWait.SpinUntil(() => client.IsConnected && Volatile.Read(ref localConnected) == 1, 3000), Is.True,
                    "Reconnecting after Disconnect lost the game's subscription or failed.");
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        [TestCase("Disconnect")]
        [TestCase("Shutdown")]
        public void LeavingOnPurpose_FiresDisconnectResolved_WithClientDisconnected(string how)
        {
            var (server, client) = Connect();
            var resolved = new ConcurrentQueue<DisconnectReason>();
            client.OnDisconnectResolved += (_, reason, _) => resolved.Enqueue(reason);
            try
            {
                if (how == "Disconnect") client.Disconnect(); else client.Shutdown();

                Assert.That(SpinWait.SpinUntil(() => resolved.Count > 0, 2000), Is.True, $"{how}() did not fire OnDisconnectResolved.");
                Thread.Sleep(200);
                Assert.That(resolved.ToArray(), Is.EqualTo(new[] { DisconnectReason.ClientDisconnected }),
                    $"{how}() resolved as {string.Join(", ", resolved.ToArray())}, expected exactly one ClientDisconnected.");
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        //The kick notice arrived first, the game reacting with Disconnect must not hide the server's reason
        [Test]
        public void KickReasonAlreadyReceived_IsKeptWhenTheClientThenDisconnects()
        {
            var (server, client) = Connect();
            var resolved = new ConcurrentQueue<DisconnectReason>();
            client.OnDisconnectResolved += (_, reason, _) => resolved.Enqueue(reason);
            try
            {
                client.DisconnectCoordinator.RecordReason(client.localID, DisconnectReason.Kicked, "kicked by server");
                client.Disconnect();

                Assert.That(SpinWait.SpinUntil(() => resolved.Count > 0, 2000), Is.True);
                Assert.That(resolved.ToArray(), Is.EqualTo(new[] { DisconnectReason.Kicked }));
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        private static WeakReference ShutDownManagerOn(TcpTransport transport)
        {
            var manager = new LiminalNetworkManager(transport, Config(Interlocked.Increment(ref _port)));
            manager.StartClient("127.0.0.1", _port);
            manager.Shutdown();
            return new WeakReference(manager);
        }

        //A shut down manager is finished, a transport the game still holds must not keep it alive
        [Test]
        public void ShutDownManager_IsNotKeptAliveByItsTransport()
        {
            var transport = new TcpTransport();
            var weakManager = ShutDownManagerOn(transport);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.That(weakManager.IsAlive, Is.False, "The transport still references the shut down manager.");
            GC.KeepAlive(transport);
        }

        [Test]
        public void ServerGoingAway_StillResolvesAsConnectionLost()
        {
            var (server, client) = Connect();
            var resolved = new ConcurrentQueue<DisconnectReason>();
            client.OnDisconnectResolved += (_, reason, _) => resolved.Enqueue(reason);
            try
            {
                server.Shutdown();
                Assert.That(SpinWait.SpinUntil(() => resolved.Count > 0, 3000), Is.True);
                Assert.That(resolved.ToArray(), Is.EqualTo(new[] { DisconnectReason.ConnectionLost }));
            }
            finally
            {
                client.Shutdown();
                server.Shutdown();
            }
        }
    }
}
