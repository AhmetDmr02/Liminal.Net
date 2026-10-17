using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using Liminal.Net.Unity;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class ConcurrencyHuntCoyoteTests
    {
        private const ushort ClientId = 5;
        private const ushort LateClientId = 7;

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

        //TcpTransport's PrePromoteClient passes its shutdown check under the lock and raises OnClientConnected after letting go
        //so the late raise always comes after the shutdown, anywhere from right after it to after the manager is done
        private static Task ShutdownSignal(MockTransport transport)
        {
            var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            transport.OnShutdown += () => signal.TrySetResult();
            return signal.Task;
        }

        private sealed class DueUnityTicker : LiminalUnityTicker
        {
            public DueUnityTicker(LiminalNetworkConfig config) : base(config)
            {
                MaxTicksPerFrame = 1;
            }

            //Puts the next tick in the past so the next TickUpdate runs exactly one
            public void MakeDue() => _nextTickTimestamp = 1;
        }

        //TickUpdate checks _isRunning and only counts the tick in flight inside InvokeTick, Stop landing between the two sees
        //nothing in flight and returns, the tick then runs after Stop already promised teardown the ticker is quiet
        [Test]
        public static async Task Coyote_UnityTicker_TickUpdateVsStop_NoTickRunsAfterStopReturned()
        {
            var config = CreateConfig();
            var ticker = new DueUnityTicker(config);
            ticker.Start();
            ticker.MakeDue();

            int stopReturned = 0;
            int ticksAfterStop = 0;
            ticker.OnTick += () =>
            {
                if (Volatile.Read(ref stopReturned) == 1)
                    Interlocked.Increment(ref ticksAfterStop);
            };

            var mainThread = Task.Run(() => ticker.TickUpdate());
            var transportThread = Task.Run(() =>
            {
                ticker.Stop();
                Volatile.Write(ref stopReturned, 1);
            });
            await Task.WhenAll(mainThread, transportThread);

            Specification.Assert(ticksAfterStop == 0,
                $"LiminalUnityTicker ran {ticksAfterStop} tick(s) after Stop() returned, teardown runs under it.");
        }

        //A handshake that passed the transport's checks right before shutdown raises OnClientConnected after it
        //with the session manager unbound the hub hands it straight to the game, which never gets a disconnect for it
        [Test]
        public static async Task Coyote_LateTransportConnect_VsDisconnect_GameNeverSeesConnectAfterStopped()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out var transport);

            int stopped = 0;
            int connectsAfterStopped = 0;
            server.OnLifecycleStateChanged += state =>
            {
                if (state == NetworkLifecycleState.Stopped)
                    Volatile.Write(ref stopped, 1);
            };
            server.OnClientConnected += id =>
            {
                if (id == LateClientId && Volatile.Read(ref stopped) == 1)
                    Interlocked.Increment(ref connectsAfterStopped);
            };

            //Coyote picks this, random scheduling almost never parks the short handshake behind the whole teardown on its own
            bool landsAfterTeardown = Microsoft.Coyote.Random.Generator.Create().NextBoolean();

            var transportShutDown = ShutdownSignal(transport);
            var gameThread = Task.Run(() => server.Disconnect());
            var handshakeThread = Task.Run(async () =>
            {
                await (landsAfterTeardown ? gameThread : transportShutDown);
                transport.TriggerClientConnected(LateClientId);
            });
            await Task.WhenAll(gameThread, handshakeThread);

            Specification.Assert(connectsAfterStopped == 0,
                $"Game got OnClientConnected({LateClientId}) after the server reported Stopped, no disconnect will ever follow it.");

            server.Shutdown();
        }

        //Same late connect landing after ShutdownSystems reset the registry but before Role goes None
        //the registry still sees a server and adds the client, a stopped server then lists a player that isnt there
        [Test]
        public static async Task Coyote_LateTransportConnect_VsDisconnect_RegistryEmptyOnceStopped()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out var transport);

            var transportShutDown = ShutdownSignal(transport);
            var gameThread = Task.Run(() => server.Disconnect());
            var handshakeThread = Task.Run(async () =>
            {
                await transportShutDown;
                transport.TriggerClientConnected(LateClientId);
            });
            await Task.WhenAll(gameThread, handshakeThread);

            int count = server.ClientRegistry.Count;
            Specification.Assert(server.LifecycleState != NetworkLifecycleState.Stopped || count == 0,
                $"Server is Stopped but ClientRegistry still lists {count} client(s): {string.Join(", ", server.ClientRegistry.AllClients)}.");

            server.Shutdown();
        }

        //FlushSession copies the session's batch under SendLock then sends it by id after letting go
        //if a new connection took the id in between, the old player's packets go out on the new connection
        [Test]
        public static async Task Coyote_Flush_VsReconnectSameId_OldSessionDataNeverSentToNewConnection()
        {
            var config = CreateConfig();
            var server = StartMockServer(config, out var transport);
            transport.TriggerClientConnected(ClientId);
            uint oldConnection = transport.GetConnection(ClientId).Generation;

            int sentToNewConnection = 0;
            transport.OnSendHook = (_, target, _) =>
            {
                uint current = transport.GetConnection(target).Generation;
                if (target == ClientId && current != 0 && current != oldConnection)
                    Interlocked.Increment(ref sentToNewConnection);
            };

            server.Interpreter.SendCommandAsServer(ClientId, new ChatPacket { Message = "your private inventory" });

            var tickThread = Task.Run(() => server.SessionManager.Flush());
            var transportThread = Task.Run(() =>
            {
                transport.TriggerClientDisconnected(ClientId);
                transport.TriggerClientConnected(ClientId);
            });
            await Task.WhenAll(tickThread, transportThread);

            Specification.Assert(sentToNewConnection == 0,
                $"A batch buffered for the old connection of client {ClientId} was sent to the new connection that reused the id.");

            server.Shutdown();
        }
    }
}
