using Liminal.Net.Interfaces;
using Liminal.Net.Core;

namespace Liminal.Net.Core
{
    public enum NetworkRole
    {
        None,
        Server,
        Client, 
        Host   
    }

    public class LiminalNetworkManager
    {
        #region Singleton
        public static LiminalNetworkManager Instance;
        #endregion

        public NetworkRole Role { get; private set; } = NetworkRole.None;

        private readonly ILiminalTransport _transport;
        public ILiminalTransport Transport => _transport;

        private readonly LiminalTransportConfig _config;

        public LiminalSessionManager SessionManager { get; private set; }
        public LiminalPacketInterpreter Interpreter { get; private set; }

        private LiminalPacketFramerPipeline _pipeline;
        private LiminalTicker _ticker;

        public ushort localID => _transport.LocalClientId;

        public LiminalNetworkManager(ILiminalTransport transport, LiminalTransportConfig config)
        {
            Instance = this;

            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _transport = transport;
            _config = config;

            Interpreter = new LiminalPacketInterpreter(_config);

            _transport.InitializeTransport(config);
            _transport.OnShutdown += HandleTransportShutdown;


            InitializeSystems();
        }

        private void HandleTransportShutdown()
        {
            LiminalLogger.Log("[Manager] Transport reported shutdown. Resetting state...");

            Shutdown();
        }

        private void InitializeSystems()
        {
            ShutdownSystems();

            LiminalLogger.Log("[Manager] Initializing Network Systems...");

            _pipeline = new LiminalPacketFramerPipeline(_config);
            SessionManager = new LiminalSessionManager(_transport, Interpreter, _config, _pipeline);

            _ticker = new LiminalTicker(_config);
        }

        private void ShutdownSystems()
        {
            _ticker?.Stop();
            SessionManager?.Dispose();

            //We actually wanna keep subscriptions around
            //Interpreter?.ClearAllHandlers();

            //_ticker = null;
            //SessionManager = null;

            _config.ClientIdResolver.ResetResolver();
        }

        #region Start Methods

        /// <summary>
        /// Starts as a Host: Acts as a Server, but also connects a local client to itself.
        /// </summary>
        public void StartHost()
        {
            if (Role != NetworkRole.None) return;

            InitializeSystems();

            LiminalLogger.Log("[Manager] Starting Host Mode...");
            Role = NetworkRole.Host;

            _transport.StartServer(_config.Default_Host, _config.Default_Port);

            _transport.StartClient("127.0.0.1", _config.Default_Port);

            _ticker.OnTick += HostTick;
            _ticker.Start();

            LiminalLogger.Log($"[Manager] Host running on {_config.Default_Host}:{_config.Default_Port}");
        }

        public void StartServer(string ip, int port)
        {
            if (Role != NetworkRole.None) return;
            Role = NetworkRole.Server;

            InitializeSystems();

            LiminalLogger.Log($"[Manager] Starting Dedicated Server on {ip}:{port}");
            _transport.StartServer(ip, port);

            _ticker.OnTick += ServerTick;
            _ticker.Start();
        }

        public void StartClient(string ip, int port)
        {
            if (Role != NetworkRole.None) return;
            Role = NetworkRole.Client;

            InitializeSystems();

            LiminalLogger.Log($"[Manager] Starting Client connecting to {ip}:{port}");
            _transport.StartClient(ip, port);

            _ticker.OnTick += ClientBackgroundTick;
            _ticker.Start();
        }

        public void Disconnect()
        {
            if (Role == NetworkRole.None) return;
            
            Role = NetworkRole.None;

            _transport.Disconnect();

            ShutdownSystems();

            LiminalLogger.Log("[Manager] Network State Disconnected.");
        }
        public void Shutdown()
        {
            if (Role == NetworkRole.None) return;

            Role = NetworkRole.None;

            _ticker?.Stop();

            //Maybe we can reset the transport here but for now just shut it down
            _transport.Shutdown();

            ShutdownSystems();

            LiminalLogger.Log("[Manager] Network State Reset.");
        }
        #endregion

        #region Ticking Logic

        private void HostTick()
        {
            var sm = SessionManager;

            sm?.Poll();
            sm?.Flush();
        }

        private void ServerTick()
        {
            var sm = SessionManager;

            sm?.Poll();
            sm?.Flush();
        }

        private void ClientBackgroundTick()
        {
            //For now
            var sm = SessionManager;

            sm?.Poll();
            sm?.Flush();
        }

        /// <summary>
        /// Call this from Unity FixedUpdate() on Client or Host.
        /// </summary>
        public void ManualPoll()
        {
            if (Role == NetworkRole.Client)
            {
                SessionManager?.Poll();
            }
        }

        #endregion
    }
}