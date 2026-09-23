using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Registry;
using Liminal.Net.Transports;
using Liminal.Net.Unity;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalUnityBridgeTests
    {
        private LiminalNetworkManager _serverManager;
        private LiminalNetworkManager _clientManager;
        private LiminalUnityBridge _bridge;

        private static int _portCounter = 9200;
        private int _testPort;

        [SetUp]
        public void Setup()
        {
            _testPort = Interlocked.Increment(ref _portCounter);

            var serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _testPort,
                ClientIdResolver = new BaseResolver()
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), serverConfig);

            var clientConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _testPort,
                ClientIdResolver = new BaseResolver()
            };

            _clientManager = new LiminalNetworkManager(new TcpTransport(), clientConfig);
            _bridge = new LiminalUnityBridge();
        }

        [TearDown]
        public void Teardown()
        {
            _bridge?.Dispose();
            try { _clientManager?.Shutdown(); } catch { }
            try { _serverManager?.Shutdown(); } catch { }
        }

        [Test]
        public void UnityBridge_Dispatches_Events_OnDrain()
        {
            var joinedClients = new List<ushort>();
            var lifecycleChanges = new List<NetworkLifecycleState>();

            _bridge.Attach(_serverManager);
            _bridge.OnClientJoined += c => joinedClients.Add(c.ClientId);
            _bridge.OnLifecycleStateChanged += s => lifecycleChanges.Add(s);

            _serverManager.StartServer("127.0.0.1", _testPort);
            _bridge.DrainEvents();

            Assert.That(lifecycleChanges, Contains.Item(NetworkLifecycleState.ServerActive));

            _clientManager.StartClient("127.0.0.1", _testPort);
            Assert.That(SpinWait.SpinUntil(() => _clientManager.IsConnected, 3000), Is.True);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.ClientRegistry.Count == 1, 3000), Is.True);

            _bridge.DrainEvents();

            Assert.That(joinedClients.Count, Is.EqualTo(1));
            Assert.That(joinedClients[0], Is.EqualTo(_clientManager.localID));
        }

        [Test]
        public void UnityBridge_Update_DrivesUnityTicker_AndPollsNetwork()
        {
            var serverConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = Interlocked.Increment(ref _portCounter),
                TickRate = 60,
                ClientIdResolver = new BaseResolver()
            };

            var unityTicker = new LiminalUnityTicker(serverConfig);
            var server = new LiminalNetworkManager(new TcpTransport(), serverConfig, customTicker: unityTicker);
            var bridge = new LiminalUnityBridge();
            bridge.Attach(server);

            int tickCount = 0;
            unityTicker.OnTick += () => tickCount++;

            server.StartServer();

            Thread.Sleep(25);
            bridge.Update();

            Assert.That(tickCount, Is.GreaterThanOrEqualTo(1));

            bridge.Dispose();
            server.Shutdown();
        }

        [Test]
        public void UnityBridge_OnLocalClientReady_CanSendInHostMode()
        {
            int port = Interlocked.Increment(ref _portCounter);
            var hostConfig = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                ClientIdResolver = new BaseResolver()
            };

            var hostManager = new LiminalNetworkManager(new TcpTransport(), hostConfig);
            LiminalNetworkManager.Instance = hostManager;
            var bridge = new LiminalUnityBridge();
            bridge.Attach(hostManager);

            int readyCount = 0;
            var canSendHistory = new List<bool>();

            bridge.OnLocalClientReady += client =>
            {
                readyCount++;
                canSendHistory.Add(hostManager.CanSend);
                Broadcaster.Send(SendTo.Server, new Liminal.Net.Test.ChatPacket { Message = "hello" });
            };

            hostManager.StartHost("127.0.0.1", port);

            for (int i = 0; i < 200; i++)
            {
                bridge.Update();
                if (readyCount > 0 && hostManager.LifecycleState == NetworkLifecycleState.HostActive)
                    break;
                Thread.Sleep(10);
            }

            try
            {
                Assert.That(canSendHistory[0], Is.True, "CanSend MUST be true on first invocation of OnLocalClientReady.");
                Assert.That(readyCount, Is.EqualTo(1), "OnLocalClientReady should fire exactly once for local client.");
                Assert.That(canSendHistory, Has.Count.EqualTo(1));
            }
            finally
            {
                bridge.Dispose();
                hostManager.Shutdown();
                LiminalNetworkManager.Instance = null;
            }
        }
    }
}
