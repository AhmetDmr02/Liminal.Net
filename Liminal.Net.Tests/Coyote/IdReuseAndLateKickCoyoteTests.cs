using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //A resolver that hands a reconnecting player its old id (ResolveId) makes the same id show up on a new connection
    //while the old one is still being torn down, everything below acts on that id after its own check let go
    public static class IdReuseAndLateKickCoyoteTests
    {
        private const ushort ClientId = 5;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            SyncVarMaxPageSize = 4096,
            SyncVarMaxPageCount = 16,
            ClientIdResolver = new BaseResolver()
        };

        private static LiminalNetworkManager StartMockServer(LiminalNetworkConfig config, out MockTransport transport)
        {
            transport = new MockTransport { IsServer = true };
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            server.StartServer("127.0.0.1", 7777);
            return server;
        }

        private sealed class SocketPair : IDisposable
        {
            public readonly TcpClient Remote;
            public readonly TcpClient ServerSide;

            public SocketPair(TcpListener listener)
            {
                Remote = new TcpClient();
                Remote.Connect((IPEndPoint)listener.LocalEndpoint);
                ServerSide = listener.AcceptTcpClient();
            }

            public void Dispose()
            {
                try { Remote.Close(); } catch { }
                try { ServerSide.Close(); } catch { }
            }
        }

        //Server manager on a real TcpTransport, never started so no listener or ticker runs, the session manager still hears every transport event
        private static LiminalNetworkManager CreateTcpServer(out TcpTransport transport)
        {
            var config = CreateConfig();
            transport = new TcpTransport();
            var server = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            transport.SetConnectedForTesting(isConnected: true, isServer: true);
            return server;
        }

        private static bool IsCurrentSocket(TcpTransport transport, TcpClient socket) =>
            transport._sockets.TryGetValue(ClientId, out var current) && ReferenceEquals(current, socket);

        //KickConnection claims the old socket then looks the send queue up by id, a reconnect in between hands it the new connection's queue
        [Test]
        public static async Task Coyote_Tcp_KickOldConnection_VsReconnectSameId_NewConnectionKeepsSendQueue()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var oldPair = new SocketPair(listener);
            using var newPair = new SocketPair(listener);
            listener.Stop();

            var server = CreateTcpServer(out var transport);
            try
            {
                transport.PrePromoteClient(ClientId, oldPair.ServerSide, out var oldState);

                //Old connection's own reader failing on a bad frame, it kicks only its own connection
                var oldReader = Task.Run(() => transport.KickConnection(ClientId, oldState));
                var reconnect = Task.Run(() => transport.PrePromoteClient(ClientId, newPair.ServerSide, out _));
                await Task.WhenAll(oldReader, reconnect);

                if (IsCurrentSocket(transport, newPair.ServerSide))
                {
                    bool hasQueue = transport._sendQueues.TryGetValue(ClientId, out var state) && ReferenceEquals(state.Client, newPair.ServerSide);
                    Specification.Assert(hasQueue,
                        $"Client {ClientId}'s new socket is live but its send queue was torn down by the old connection's kick, every send to it is dropped.");
                }
            }
            finally
            {
                server.Shutdown();
            }
        }

        //Same kick raises OnClientDisconnected after the claim, landing after the reconnect's OnClientConnected it removes the new session
        [Test]
        public static async Task Coyote_Tcp_KickOldConnection_VsReconnectSameId_NewConnectionKeepsSession()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var oldPair = new SocketPair(listener);
            using var newPair = new SocketPair(listener);
            listener.Stop();

            var server = CreateTcpServer(out var transport);
            try
            {
                transport.PrePromoteClient(ClientId, oldPair.ServerSide, out var oldState);

                var oldReader = Task.Run(() => transport.KickConnection(ClientId, oldState));
                var reconnect = Task.Run(() => transport.PrePromoteClient(ClientId, newPair.ServerSide, out _));
                await Task.WhenAll(oldReader, reconnect);

                if (IsCurrentSocket(transport, newPair.ServerSide))
                {
                    Specification.Assert(server.SessionManager.HasSession(ClientId),
                        $"Transport serves client {ClientId}'s new socket but the session manager has no session for it, the old kick's disconnect removed it.");
                }
            }
            finally
            {
                server.Shutdown();
            }
        }

        //ReceiveLoop's finally removes its socket, checks nobody replaced it, then raises OnClientDisconnected, a reconnect after the check gets its session removed
        [Test]
        public static async Task Coyote_Tcp_ReaderDisconnect_VsReconnectSameId_NewConnectionKeepsSession()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var oldPair = new SocketPair(listener);
            using var newPair = new SocketPair(listener);
            listener.Stop();

            var receiveLoop = typeof(TcpTransport<EmptyFramingContext>).GetMethod("ReceiveLoop", BindingFlags.Instance | BindingFlags.NonPublic)!;

            var server = CreateTcpServer(out var transport);
            try
            {
                transport.PrePromoteClient(ClientId, oldPair.ServerSide, out var oldState);

                //Player's old connection drops, its reader finds the end of the stream right away
                oldPair.Remote.Close();

                var oldReader = Task.Run(async () =>
                {
                    try
                    {
                        await (Task)receiveLoop.Invoke(transport, new object[] { ClientId, oldPair.ServerSide, oldState })!;
                    }
                    catch (Exception ex) when (ex is ObjectDisposedException or InvalidOperationException or TargetInvocationException)
                    {
                        //Reconnect already closed the old socket before this reader started, nothing to race this run
                    }
                });
                var reconnect = Task.Run(() => transport.PrePromoteClient(ClientId, newPair.ServerSide, out _));
                await Task.WhenAll(oldReader, reconnect);

                if (IsCurrentSocket(transport, newPair.ServerSide))
                {
                    Specification.Assert(server.SessionManager.HasSession(ClientId),
                        $"Transport serves client {ClientId}'s new socket but the session manager has no session for it, the old reader's disconnect removed it.");
                }
            }
            finally
            {
                server.Shutdown();
            }
        }

        private sealed class ThrowForSession : ILiminalOutboundTransformer
        {
            public volatile LiminalSession Poisoned;

            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                if (ReferenceEquals(session, Poisoned))
                    throw new InvalidOperationException("encrypt failed for the old connection");

                input.CopyTo(output);
                return input.Length;
            }
        }

        //Flush decides on the old session under SendLock, then LiminalHiccupController.Kick kicks by id after letting go
        [Test]
        public static async Task Coyote_HiccupKick_FlushFailure_VsReconnectSameId_NeverKicksNewConnection()
        {
            var config = CreateConfig();
            var transformer = new ThrowForSession();
            config.OutboundPacketProcessors.Add(transformer);

            var server = StartMockServer(config, out var transport);
            try
            {
                transport.TriggerClientConnected(ClientId);
                server.SessionManager.TryGetSession(ClientId, out var oldSession);
                transformer.Poisoned = oldSession;

                server.Interpreter.SendCommandAsServer(ClientId, new ChatPacket { Message = "for the old connection" });

                var tickThread = Task.Run(() => server.SessionManager.Flush());
                var transportThread = Task.Run(() =>
                {
                    transport.TriggerClientDisconnected(ClientId);
                    transport.TriggerClientConnected(ClientId);
                });
                await Task.WhenAll(tickThread, transportThread);

                Specification.Assert(transport.GetConnection(ClientId).IsValid,
                    $"A flush failure on client {ClientId}'s old session kicked the new connection that reused the id.");
            }
            finally
            {
                server.Shutdown();
            }
        }

        //PruneStaleAssemblies terminates the old peer under its lock, then SeverTerminatedPeer kicks by id after letting go
        [Test]
        public static async Task Coyote_FragmentorStaleSever_VsReconnectSameId_NeverKicksNewConnection()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.InitializeTransport(config);
            using var fragmentor = new LiminalPacketFragmentor(transport, config);

            transport.TriggerClientConnected(ClientId);

            //First of two fragments, the assembly is left half done
            int fullFragmentPayload = fragmentor.MTU - LiminalTransportHeader.BaseHeaderSize - LiminalPacketFragmentor.FragmentHeaderSize;
            byte[] fragment = new byte[LiminalPacketFragmentor.FragmentHeaderSize + fullFragmentPayload];
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(0, 2), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(2, 2), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(4, 2), 2);
            fragmentor.IngestFragment(fragment, ClientId);

            long farFuture = System.Diagnostics.Stopwatch.GetTimestamp() + System.Diagnostics.Stopwatch.Frequency * 3600;

            var tickThread = Task.Run(() => fragmentor.PruneStaleAssemblies(farFuture));
            var transportThread = Task.Run(() =>
            {
                transport.TriggerClientDisconnected(ClientId);
                transport.TriggerClientConnected(ClientId);
            });
            await Task.WhenAll(tickThread, transportThread);

            Specification.Assert(transport.GetConnection(ClientId).IsValid,
                $"A stale fragment assembly of client {ClientId}'s old connection got the new connection that reused the id kicked.");
        }

        //TickOnce runs InvokeTick with no running check, Stop finds nothing in flight and returns, the tick then runs under the teardown
        [Test]
        public static async Task Coyote_TickOnce_VsStop_NoTickRunsAfterStopReturned()
        {
            var config = CreateConfig();
            var ticker = new LiminalUnityTicker(config);
            ticker.Start();

            int stopReturned = 0;
            int ticksAfterStop = 0;
            ticker.OnTick += () =>
            {
                if (Volatile.Read(ref stopReturned) == 1)
                    Interlocked.Increment(ref ticksAfterStop);
            };

            var manualDriver = Task.Run(() => ticker.TickOnce());
            var teardown = Task.Run(() =>
            {
                ticker.Stop();
                Volatile.Write(ref stopReturned, 1);
            });
            await Task.WhenAll(manualDriver, teardown);

            Specification.Assert(ticksAfterStop == 0,
                $"LiminalTicker.TickOnce ran {ticksAfterStop} tick(s) after Stop() returned, teardown runs under it.");
        }

        //Holds the old run's Disconnect on its thread until the game restarted, standing in for that thread getting preempted between deciding and calling
        private sealed class HeldDisconnectTcpTransport : TcpTransport
        {
            public int HeldThread;
            public readonly TaskCompletionSource<bool> Restarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public void RaiseReliable(ReadOnlySpan<byte> data, LiminalConnection from) => _onReliable?.Invoke(data, from);

            private void HoldOnOldRunThread()
            {
                if (Environment.CurrentManagedThreadId == Volatile.Read(ref HeldThread))
                {
                    for (int i = 0; i < 5000 && !Restarted.Task.IsCompleted; i++)
                        Task.Delay(1).Wait();
                }
            }

            public override void Disconnect()
            {
                HoldOnOldRunThread();
                base.Disconnect();
            }

            public override void Disconnect(LiminalConnection server)
            {
                HoldOnOldRunThread();
                base.Disconnect(server);
            }
        }

        //Client side LiminalHiccupController.Kick calls the plain transport Disconnect, coming from the old run's receive thread it ends whatever run is current
        [Test]
        public static async Task Coyote_ClientHiccupKick_FromOldRun_VsDisconnectAndRestart_NewSessionSurvives()
        {
            var config = CreateConfig();
            config.Hiccup.Enabled = false;

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            var transport = new HeldDisconnectTcpTransport();
            //Attempts never finish on their own, the test drives the old connection by hand
            transport.ClientHandshaker = (_, _) => new TaskCompletionSource<HandshakeResult>().Task;

            var client = new LiminalNetworkManager(transport, config, customTicker: new LiminalUnityTicker(config));
            var oldSocket = new TcpClient();
            TcpClient serverSide = null;
            try
            {
                client.StartClient("127.0.0.1", port);
                oldSocket.Connect(IPAddress.Loopback, port);
                serverSide = listener.AcceptTcpClient();
                transport.PromoteLocalClient(1, oldSocket);
                var oldConnection = transport.GetConnection(ILiminalTransport.SERVER_ID);

                //Bigger than a batch with Hiccup off, the session manager kicks for it
                byte[] oversized = new byte[config.MaxPacketSizePerBatch + 1];

                var oldReceiveThread = Task.Run(() =>
                {
                    Volatile.Write(ref transport.HeldThread, Environment.CurrentManagedThreadId);
                    transport.RaiseReliable(oversized, oldConnection);
                });
                var gameRestarts = Task.Run(() =>
                {
                    client.Disconnect();
                    client.StartClient("127.0.0.1", port);
                    transport.Restarted.TrySetResult(true);
                });
                await Task.WhenAll(oldReceiveThread, gameRestarts);

                client.ManualPoll();

                Specification.Assert(transport.IsClient && client.Role == NetworkRole.Client && client.LifecycleState == NetworkLifecycleState.Connecting,
                    $"Old run's Hiccup kick ended the restarted session, manager is {client.Role}/{client.LifecycleState}, transport client={transport.IsClient}.");
            }
            finally
            {
                client.Shutdown();
                try { listener.Stop(); } catch { }
                try { serverSide?.Close(); } catch { }
                oldSocket.Dispose();
            }
        }
    }
}
