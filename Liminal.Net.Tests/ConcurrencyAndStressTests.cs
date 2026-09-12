using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class ConcurrencyAndStressTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7800;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }
            _serverManager?.Shutdown();
        }

        private LiminalNetworkManager CreateAndStartClient(LiminalTelemetryConfig telemetryConfig = null)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };
            var client = new LiminalNetworkManager(new TcpTransport(), config, telemetryConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        [Test]
        public void Test17_Framer_Concurrency_Stress()
        {
            var racer = new RaceTriggerProcessor();
            _serverConfig.OutboundPacketProcessors.Add(racer);
            _serverConfig.InboundPacketProcessors.Add(racer);

            _serverConfig.MaxPacketSizePerBatch = ushort.MaxValue;

            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientManager = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => clientManager.Transport.IsConnected, 5000), Is.True);

            ushort assignedId = clientManager.localID;

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, this);
            clientManager.Interpreter.Subscribe<ChatPacket>((pkt, sender) => { }, this);

            int failed = 0;
            long serverSent = 0;
            long clientSent = 0;

            int iterations = 1000;

            int flushBatchSize = 20;
            int workerCount = Math.Max(4, Environment.ProcessorCount);

            var startGate = new CountdownEvent(1);
            var doneGate = new CountdownEvent(workerCount * 2);

            Task[] tasks = new Task[workerCount * 2];

            for (int w = 0; w < workerCount; w++)
            {
                int workerIndex = w;

                tasks[w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 1; i <= iterations; i++)
                    {
                        try
                        {
                            _serverManager.Interpreter.SendCommand(assignedId, new ChatPacket { Message = $"S|{workerIndex}|{i}" });
                            Interlocked.Increment(ref serverSent);

                            if (i % flushBatchSize == 0)
                            {
                                _serverManager.SessionManager.Flush();
                            }
                        }
                        catch
                        {
                            Interlocked.Exchange(ref failed, 1);
                            break;
                        }
                    }
                    _serverManager.SessionManager.Flush();
                    doneGate.Signal();
                });

                tasks[workerCount + w] = Task.Run(() =>
                {
                    startGate.Wait();
                    for (int i = 1; i <= iterations; i++)
                    {
                        try
                        {
                            clientManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"C|{workerIndex}|{i}" });
                            Interlocked.Increment(ref clientSent);

                            if (i % flushBatchSize == 0)
                            {
                                clientManager.SessionManager.Flush();
                            }
                        }
                        catch
                        {
                            Interlocked.Exchange(ref failed, 1);
                            break;
                        }
                    }
                    clientManager.SessionManager.Flush();
                    doneGate.Signal();
                });
            }

            startGate.Signal();

            bool completed = doneGate.Wait(TimeSpan.FromSeconds(60));
            Assert.That(completed, Is.True, "Timeout waiting for worker tasks to finish.");

            Assert.That(Interlocked.CompareExchange(ref failed, 0, 0), Is.EqualTo(0), "Framer collision or transport failure detected.");
            Assert.That(serverSent, Is.EqualTo((long)iterations * workerCount));
            Assert.That(clientSent, Is.EqualTo((long)iterations * workerCount));

            _serverConfig.MaxPacketSizePerBatch = 4096;
        }

        private class RaceTriggerProcessor : ILiminalInboundTransformer, ILiminalOutboundTransformer
        {
            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                for (int i = 0; i < input.Length; i++) output[i] = (byte)(input[i] ^ 0xFF);
                Thread.SpinWait(100);
                return input.Length;
            }

            public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                for (int i = 0; i < input.Length; i++) output[i] = (byte)(input[i] ^ 0xFF);
                Thread.SpinWait(100);
                return input.Length;
            }
        }

        [Test]
        public void Test18_UngracefulSocketClose_TriggersFatalLog()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var clientManager = CreateAndStartClient();

            Assert.That(SpinWait.SpinUntil(() => clientManager.Transport.IsConnected, 2000), Is.True);

            Thread.Sleep(100);

            ushort targetId = clientManager.localID;

            _serverManager.Transport.Kick(targetId);

            Thread.Sleep(200);

            Assert.That(_serverManager.SessionManager.GetActiveSessionCount(), Is.EqualTo(0), "Session should be removed.");
        }

        // Carried Over To The Coyote Tests
        //[Test]
        //public void Test19_ConcurrentClientConnections_NoDuplicateIds()
        //{
        //    ThreadPool.SetMinThreads(200, 200);
        //    _serverManager.StartServer("127.0.0.1", _currentTestPort);
        //    Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

        //    const int CLIENT_COUNT = 10;
        //    var connectedIds = new ConcurrentBag<ushort>();
        //    var connectionEvents = new CountdownEvent(CLIENT_COUNT);

        //    _serverManager.Transport.OnClientConnected += (clientId) =>
        //    {
        //        connectedIds.Add(clientId);
        //        connectionEvents.Signal();
        //    };

        //    var clientTasks = new List<Task>();

        //    var startGate = new TaskCompletionSource<bool>();

        //    for (int i = 0; i < CLIENT_COUNT; i++)
        //    {
        //        clientTasks.Add(Task.Run(async () =>
        //        {
        //            await startGate.Task;

        //            var client = CreateAndStartClient();
        //        }));
        //    }

        //    startGate.SetResult(true);

        //    bool allConnected = connectionEvents.Wait(TimeSpan.FromSeconds(15));

        //    Assert.That(allConnected, Is.True, $"Only {CLIENT_COUNT - connectionEvents.CurrentCount}/{CLIENT_COUNT} clients connected in time.");

        //    Thread.Sleep(500);

        //    var idList = connectedIds.ToList();
        //    var uniqueIds = idList.Distinct().ToList();

        //    Assert.That(idList.Count, Is.EqualTo(uniqueIds.Count),
        //        $"RACE CONDITION DETECTED: {idList.Count - uniqueIds.Count} duplicate ID(s) assigned. " +
        //        $"IDs: [{string.Join(", ", idList.OrderBy(x => x))}]");

        //    foreach (var clientId in idList)
        //    {
        //        Assert.That(_serverConfig.ClientIdResolver.IsConnectionActive(clientId), Is.True,
        //            $"Client {clientId} connected but not registered in resolver.");
        //    }

        //    LiminalLogger.Log($"[Test19] Successfully tested {CLIENT_COUNT} concurrent connections - all IDs unique.");
        //}

        [Test]
        public void Test20_RapidConnectDisconnect_NoResourceLeaks()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            const int ITERATIONS = 30;

            for (int i = 0; i < ITERATIONS; i++)
            {
                var client = CreateAndStartClient();

                bool connected = SpinWait.SpinUntil(() => client.Transport.IsConnected, 1000);

                if (connected)
                {
                    client.Disconnect();
                    Thread.Sleep(50);
                }
            }

            Thread.Sleep(500);

            int activeCount = _serverManager.Transport.ConnectedClientCount;

            Assert.That(activeCount, Is.EqualTo(0),
                $"Socket leak detected: {activeCount} clients still connected in transport after disconnect.");

            LiminalLogger.Log($"[Test20] Successfully completed {ITERATIONS} rapid connect/disconnect cycles.");
        }

        [Test]
        public void Test21_SimultaneousSameIdPromotion_OnlyOneSucceeds()
        {
            var maliciousResolver = new ForceCollisionResolver(targetId: 42, duplicateCount: 5);

            var testConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = maliciousResolver
            };

            var testServer = new LiminalNetworkManager(new TcpTransport(), testConfig);
            testServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => testServer.Transport.IsConnected, 2000), Is.True);

            var connectedIds = new ConcurrentBag<ushort>();
            testServer.Transport.OnClientConnected += (id) => connectedIds.Add(id);

            var clients = new List<LiminalNetworkManager>();
            var startGate = new ManualResetEventSlim(false);

            for (int i = 0; i < 5; i++)
            {
                Task.Run(() =>
                {
                    startGate.Wait();

                    var clientConfig = new LiminalTransportConfig
                    {
                        Default_Host = "127.0.0.1",
                        Default_Port = _currentTestPort,
                        TickRate = 60,
                        MaxPacketSizePerBatch = 4096,
                        ClientIdResolver = new BaseResolver()
                    };

                    var client = new LiminalNetworkManager(new TcpTransport(), clientConfig);
                    lock (clients) clients.Add(client);

                    client.StartClient("127.0.0.1", _currentTestPort);
                });
            }

            startGate.Set();
            Thread.Sleep(2000);

            int confirmedCount = maliciousResolver.ConfirmedCount;
            int activeSocketsInTransport = testServer.Transport.ConnectedClientCount;

            // Transport replaces colliding IDs cleanly (AddOrUpdate), so exactly 1 remains in memory
            Assert.That(activeSocketsInTransport, Is.EqualTo(1),
                $"Expected 1 active socket in transport for ID 42, but found {activeSocketsInTransport}.");

            testServer.Shutdown();
            lock (clients)
            {
                foreach (var c in clients)
                {
                    try { c.Shutdown(); } catch { }
                }
            }

            LiminalLogger.Log($"[Test21] Collision test complete. Resolver handed out ID 42 5x, transport retained exactly {activeSocketsInTransport} socket.");
        }

        private class ForceCollisionResolver : ILiminalClientIdResolver
        {
            private volatile ushort _targetId;
            private readonly int _duplicateCount;
            private int _callCount = 0;
            private int _confirmedCount = 0;

            public int ConfirmedCount => Volatile.Read(ref _confirmedCount);
            public void SetTargetId(ushort newTargetId) => _targetId = newTargetId;

            public ForceCollisionResolver(ushort targetId, int duplicateCount)
            {
                _targetId = targetId;
                _duplicateCount = duplicateCount;
            }

            public void Initialize(ILiminalTransport transport) { }

            public ushort GenerateClientId()
            {
                int count = Interlocked.Increment(ref _callCount);
                return count <= _duplicateCount ? _targetId : (ushort)0;
            }

            public void ConfirmRegistration(ushort targetId)
            {
                Interlocked.Increment(ref _confirmedCount);
            }

            public ushort ResolveId(Span<byte> payload) => 0;

            public void ResetResolver()
            {
                Interlocked.Exchange(ref _callCount, 0);
                Interlocked.Exchange(ref _confirmedCount, 0);
            }
        }

        [Test]
        public void Test22_Chaos_Teardown_Under_Heavy_Load()
        {
            var chaosTransformer = new ChaosTransformer();
            _serverConfig.InboundPacketProcessors.Add(chaosTransformer);
            _serverConfig.OutboundPacketProcessors.Add(chaosTransformer);

            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            _serverManager.Interpreter.Subscribe<ChatPacket>((pkt, id) => { }, this);

            int clientCount = 10;
            var activeClients = new List<LiminalNetworkManager>();
            var startGate = new ManualResetEventSlim(false);

            int architecturalExceptions = 0;

            for (int i = 0; i < clientCount; i++)
            {
                var c = CreateAndStartClient();
                activeClients.Add(c);
            }

            Assert.That(SpinWait.SpinUntil(() => _serverManager.SessionManager.GetActiveSessionCount() == clientCount, 3000), Is.True);

            var spamTasks = new List<Task>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            foreach (var client in activeClients)
            {
                spamTasks.Add(Task.Run(() =>
                {
                    startGate.Wait();
                    try
                    {
                        while (!cts.IsCancellationRequested)
                        {
                            if (client.Transport.IsConnected)
                            {
                                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "Chaos" });
                                client.SessionManager.Flush();
                            }

                            if (Random.Shared.Next(100) < 5) Thread.Yield();
                        }
                    }
                    catch (Exception ex)
                    {
                        if (!(ex is System.IO.IOException || ex is System.Net.Sockets.SocketException))
                        {
                            LiminalLogger.LogError($"[Chaos] Race Condition Caught: {ex.GetType().Name} - {ex.Message}");
                            Interlocked.Increment(ref architecturalExceptions);
                        }
                    }
                }));
            }

            startGate.Set();

            Thread.Sleep(500);

            var teardownTasks = new List<Task>
            {
                Task.Run(() => _serverManager.Shutdown())
            };

            for (int i = 0; i < clientCount; i++)
            {
                int index = i;
                teardownTasks.Add(Task.Run(() =>
                {
                    if (Random.Shared.Next(100) < 50) Thread.Sleep(Random.Shared.Next(1, 10));

                    if (activeClients[index] != null && activeClients[index].Transport.IsConnected)
                    {
                        activeClients[index].Disconnect();
                    }
                }));
            }

            Task.WaitAll(teardownTasks.ToArray());
            cts.Cancel();
            Task.WaitAll(spamTasks.ToArray());

            Assert.That(architecturalExceptions, Is.EqualTo(0), "RACE CONDITION DETECTED! An internal exception (like NullReference or ObjectDisposed) leaked during concurrent teardown.");
            Assert.That(_serverManager.Transport.IsConnected, Is.False, "Server failed to fully shut down.");
            Assert.That(_serverManager.SessionManager.GetActiveSessionCount(), Is.EqualTo(0), "Session leak detected after chaos shutdown.");
        }

        private class ChaosTransformer : ILiminalInboundTransformer, ILiminalOutboundTransformer
        {
            public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                input.CopyTo(output);
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                return input.Length;
            }

            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                input.CopyTo(output);
                if (Random.Shared.Next(10) < 3) Thread.Yield();
                return input.Length;
            }
        }

        [Test]
        public void Test26_InboundQueue_WithinMaxPacketCount_ProcessesSuccessfully()
        {
            const int maxPacketCount = 10;
            const int sentPackets = 5;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = maxPacketCount,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var customServer = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            customServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.IsConnected, 2000), Is.True);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            int serverReceivedCount = 0;
            customServer.Interpreter.Subscribe<ChatPacket>((pkt, id) => Interlocked.Increment(ref serverReceivedCount), this);

            for (int i = 0; i < sentPackets; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"SafeBatch_{i}" });
            }
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => serverReceivedCount == sentPackets, 2000), Is.True,
                $"Server only processed {serverReceivedCount}/{sentPackets} packets within the threshold.");
            Assert.That(client.Transport.IsConnected, Is.True, "Client was kicked unexpectedly under the packet limit.");

            customServer.Shutdown();
        }

        [Test]
        public void Test27_InboundQueue_ExceedingMaxPacketCount_KicksOffendingClient()
        {
            const int maxPacketCount = 5;
            const int overflowPackets = 15;

            var serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                MaxPacketCount = maxPacketCount,
                Hiccup = { Enabled = false },
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            var customServer = new LiminalNetworkManager(new TcpTransport(), serverConfig);
            customServer.StartServer("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => customServer.Transport.IsConnected, 2000), Is.True);

            customServer.Interpreter.Subscribe<ChatPacket>((pkt, id) => { LiminalLogger.Log($"Server received packet: {pkt.Message}"); }, this);

            var client = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client.Transport.IsConnected, 2000), Is.True);

            bool serverKickedClient = false;
            customServer.Transport.OnClientKicked += (id) => serverKickedClient = true;

            // Send more packets in a single batch than the server's InboundQueue MaxPacketCount can tolerate
            for (int i = 0; i < overflowPackets; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = $"Flood_{i}" });
            }
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => serverKickedClient, 2000), Is.True,
                "Server failed to kick client after exceeding InboundQueue MaxPacketCount.");
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 2000), Is.True,
                "Client remained connected after exceeding InboundQueue capacity limit.");

            customServer.Shutdown();
        }

        [Test]
        public void Test43_SessionManager_WhenShutDown_IsGarbageCollected()
        {
            WeakReference weakSessionManager = IsolateNetworkRun();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.That(weakSessionManager.IsAlive, Is.False, "FATAL: SessionManager was not garbage collected! Event memory leak detected.");
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        private WeakReference IsolateNetworkRun()
        {
            var config = new LiminalTransportConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var transport = new TcpTransport();
            var manager = new LiminalNetworkManager(transport, config);

            manager.StartClient("127.0.0.1", 7777);

            var weak = new WeakReference(manager.SessionManager);

            manager.Shutdown();

            manager.StartClient("127.0.0.1", 7777);

            LiminalNetworkManager.Instance = null;

            return weak;
        }
    }
}