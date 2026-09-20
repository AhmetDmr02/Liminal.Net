using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public enum NetworkRole
    {
        None,
        Server,
        Client,
        Host
    }

    public enum NetworkLifecycleState
    {
        Stopped,
        Connecting,
        Connected,
        StartingServer,
        ServerActive,
        StartingHost,
        HostActive,
        Stopping
    }

    public class LiminalNetworkManager
    {
        #region Singleton
        public static LiminalNetworkManager Instance;
        #endregion

        public NetworkRole Role { get; private set; } = NetworkRole.None;
        public NetworkLifecycleState LifecycleState { get; private set; } = NetworkLifecycleState.Stopped;

        private Action<NetworkLifecycleState> _onLifecycleStateChanged;
        public event Action<NetworkLifecycleState> OnLifecycleStateChanged
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLifecycleStateChanged, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLifecycleStateChanged, value);
        }

        public bool IsConnected => LifecycleState switch
        {
            NetworkLifecycleState.Connected => ValidateClientOperational(),
            NetworkLifecycleState.HostActive => ValidateHostOperational(),
            NetworkLifecycleState.ServerActive => ValidateServerOperational(),
            _ => false
        };

        public bool IsConnecting => LifecycleState switch
        {
            NetworkLifecycleState.Connecting => true,
            NetworkLifecycleState.StartingHost => true,
            NetworkLifecycleState.StartingServer => true,
            _ => false
        };

        public bool CanSend => IsConnected && _transport != null && _transport.IsConnected;

        public bool IsServer => Role == NetworkRole.Server || Role == NetworkRole.Host;
        public bool IsClient => Role == NetworkRole.Client || Role == NetworkRole.Host;
        public bool IsHost => Role == NetworkRole.Host;

        public Liminal.Net.Registry.ClientRegistry ClientRegistry { get; private set; }

        private Action _onConnectedAndReady;
        public event Action OnConnectedAndReady
        {
            add => Liminal.Net.Misc.LiminalAtomicHelpers.SafeAdd(ref _onConnectedAndReady, value);
            remove => Liminal.Net.Misc.LiminalAtomicHelpers.SafeRemove(ref _onConnectedAndReady, value);
        }

        private bool ValidateClientOperational()
        {
            return _transport != null
                && _transport.IsConnected
                && localID != 0
                && SessionManager != null
                && SessionManager.HasSession(ILiminalTransport.SERVER_ID)
                && _isShuttingDown == 0;
        }

        private bool ValidateHostOperational()
        {
            return _transport != null
                && _transport.IsServer
                && _transport.IsConnected
                && localID != 0
                && SessionManager != null
                && _isShuttingDown == 0;
        }

        private bool ValidateServerOperational()
        {
            return _transport != null
                && _transport.IsServer
                && SessionManager != null
                && _isShuttingDown == 0;
        }

        private readonly ILiminalTransport _transport;
        public ILiminalTransport Transport => _transport;

        private readonly LiminalNetworkConfig _config;
        public LiminalNetworkConfig Config => _config;

        public LiminalSessionManager SessionManager { get; private set; }
        public LiminalPacketInterpreter Interpreter { get; private set; }

        private LiminalPacketFramerPipeline _pipeline;

        private readonly LiminalTicker _customTicker;
        private LiminalTicker _ticker;
        public LiminalTicker Ticker => _ticker;
        private LiminalPhaseAligner _phaseAligner;

        public LiminalTelemetryManager TelemetryManager { get; private set; }
        private readonly LiminalTelemetryConfig _telemetryConfig;

        public DisconnectReasonCoordinator DisconnectCoordinator { get; private set; }

        private readonly LiminalEventHub _eventHub;
        public ILiminalEventHub Events => _eventHub;

        #region High-Level Connection Events
        public event Action<ushort> OnClientConnected
        {
            add => _eventHub.OnClientConnected += value;
            remove => _eventHub.OnClientConnected -= value;
        }

        public event Action<ushort> OnClientDisconnected
        {
            add => _eventHub.OnClientDisconnected += value;
            remove => _eventHub.OnClientDisconnected -= value;
        }

        public event Action<ushort> OnLocalClientConnected
        {
            add => _eventHub.OnLocalClientConnected += value;
            remove => _eventHub.OnLocalClientConnected -= value;
        }

        public event Action<ushort> OnLocalClientDisconnected
        {
            add => _eventHub.OnLocalClientDisconnected += value;
            remove => _eventHub.OnLocalClientDisconnected -= value;
        }

        public event Action<ushort> OnClientKicked
        {
            add => _eventHub.OnClientKicked += value;
            remove => _eventHub.OnClientKicked -= value;
        }

        public event Action OnServerStarted
        {
            add => _eventHub.OnServerStarted += value;
            remove => _eventHub.OnServerStarted -= value;
        }

        public event Action OnTransportShutdown
        {
            add => _eventHub.OnTransportShutdown += value;
            remove => _eventHub.OnTransportShutdown -= value;
        }
        #endregion

        #region Lifecycle & Diagnostics Events
        public event Action<ushort, DisconnectReason, string> OnDisconnectResolved
        {
            add => _eventHub.OnDisconnectResolved += value;
            remove => _eventHub.OnDisconnectResolved -= value;
        }

        public event Action OnManagerPreInitialize
        {
            add => _eventHub.OnManagerPreInitialize += value;
            remove => _eventHub.OnManagerPreInitialize -= value;
        }

        public event Action OnManagerPostInitialize
        {
            add => _eventHub.OnManagerPostInitialize += value;
            remove => _eventHub.OnManagerPostInitialize -= value;
        }

        public event Action OnManagerShutdown
        {
            add => _eventHub.OnManagerShutdown += value;
            remove => _eventHub.OnManagerShutdown -= value;
        }

        public event Action OnPreFlush
        {
            add => _eventHub.OnPreFlush += value;
            remove => _eventHub.OnPreFlush -= value;
        }

        public event Action OnPostFlush
        {
            add => _eventHub.OnPostFlush += value;
            remove => _eventHub.OnPostFlush -= value;
        }

        public event Action OnPrePoll
        {
            add => _eventHub.OnPrePoll += value;
            remove => _eventHub.OnPrePoll -= value;
        }

        public event Action OnPostPoll
        {
            add => _eventHub.OnPostPoll += value;
            remove => _eventHub.OnPostPoll -= value;
        }
        #endregion

        public ushort localID => _transport.LocalClientId;

        public Liminal.Net.SyncVar.SyncVarManager SyncVarManager { get; private set; }

        public LiminalNetworkManager(ILiminalTransport transport, LiminalNetworkConfig config,
            LiminalTelemetryConfig telemetryConfig = null, LiminalTicker customTicker = null)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (config.ClientIdResolver == null) new BaseResolver();

            _transport = transport;
            _config = config;
            _telemetryConfig = telemetryConfig ?? new LiminalTelemetryConfig { Flags = TelemetryFlags.None };
            _customTicker = customTicker;

            Interpreter = new LiminalPacketInterpreter(this, _config);

            _eventHub = new LiminalEventHub(_transport);

            _transport.InitializeTransport(config);
            _transport.OnShutdown += HandleTransportShutdown;

            ClientRegistry = new Liminal.Net.Registry.ClientRegistry(this);

            InitializeSystems();
        }

        private void HandleTransportShutdown()
        {
            if (_isShuttingDown != 0) return;
            LiminalLogger.Log("[Manager] Transport reported shutdown. Resetting state...");
            ResetLifecycleState();
            Shutdown();
        }

        private void HandlePreLocalClientConnected(ushort clientId)
        {
            if (Role == NetworkRole.Client)
            {
                LifecycleState = NetworkLifecycleState.Connected;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
            }
            else if (Role == NetworkRole.Host)
            {
                LifecycleState = NetworkLifecycleState.HostActive;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
            }
        }

        private void HandlePostLocalClientConnected(ushort clientId)
        {
            LiminalAtomicHelpers.SafeInvoke(_onConnectedAndReady);
        }

        private void HandlePreLocalClientDisconnected(ushort clientId)
        {
            ResetLifecycleState();
        }

        private void ResetLifecycleState()
        {
            Role = NetworkRole.None;
            LifecycleState = NetworkLifecycleState.Stopped;
            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
        }

        private void InitializeSystems()
        {
            _eventHub.RaiseManagerPreInitialize();

            ShutdownSystems();

            LiminalLogger.Log("[Manager] Initializing Network Systems...");

            _pipeline = new LiminalPacketFramerPipeline(_config);
            SessionManager = new LiminalSessionManager(_transport, Interpreter, _config, _pipeline);
            DisconnectCoordinator = new DisconnectReasonCoordinator(_transport, Interpreter);

            _eventHub.BindCoreSystems(
                SessionManager,
                DisconnectCoordinator,
                HandlePreLocalClientConnected,
                HandlePostLocalClientConnected,
                HandlePreLocalClientDisconnected);

            DisconnectCoordinator.OnResolved += HandleDisconnectResolved;

            _ticker = _customTicker ?? new LiminalTicker(_config);

            if (_transport is ITransportTelemetryProvider telemetryProvider)
            {
                telemetryProvider.NextTickProvider = () => _ticker.NextTickTimestamp;
            }

            _phaseAligner = new LiminalPhaseAligner(_config);
            TelemetryManager = new LiminalTelemetryManager(this, _ticker, _telemetryConfig);

            SyncVarManager = Liminal.Net.SyncVar.SyncVarManager.Initialize(this, _config);
            ClientRegistry?.Attach();

            _eventHub.RaiseManagerPostInitialize();
        }

        private void HandleDisconnectResolved(ushort id, DisconnectReason reason, string message)
        {
            _eventHub.RaiseDisconnectResolved(id, reason, message);
        }

        private void ShutdownSystems()
        {
            _eventHub.RaiseManagerShutdown();
            _eventHub.UnbindCoreSystems();

            if (_ticker != null)
            {
                _ticker.OnTick -= HostTick;
                _ticker.OnTick -= ServerTick;
                _ticker.OnTick -= ClientBackgroundTick;
                _ticker.Stop();
            }

            TelemetryManager?.Dispose();
            TelemetryManager = null;

            SessionManager?.Dispose();

            // Last flush to trigger internal dispose
            SessionManager?.Flush();

            if (DisconnectCoordinator != null)
            {
                DisconnectCoordinator.OnResolved -= HandleDisconnectResolved;
                DisconnectCoordinator.Dispose();
                DisconnectCoordinator = null;
            }

            //We actually wanna keep subscriptions around
            //Interpreter?.ClearAllHandlers();

            //_ticker = null;
            //SessionManager = null;
            ClientRegistry?.Reset();
            SyncVarManager = null;

            _config.ClientIdResolver?.ResetResolver();
        }

        #region Start Methods

        public bool StartHost() => StartHost(_config.Default_Host, _config.Default_Port);

        public bool StartHost(int port) => StartHost(_config.Default_Host, port);

        public bool StartHost(string host, int port)
        {
            if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
            {
                LiminalLogger.LogWarning($"[Manager] Cannot StartHost: Manager is in state '{LifecycleState}' (Role: {Role}).");
                return false;
            }

            LifecycleState = NetworkLifecycleState.StartingHost;
            Role = NetworkRole.Host;

            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

            InitializeSystems();

            LiminalLogger.Log("[Manager] Starting Host Mode...");

            string serverBindHost = host;
            string clientConnectHost = host;

            if (string.IsNullOrWhiteSpace(host) || host == "0.0.0.0" || host == "::" || host == "::0")
            {
                serverBindHost = host;
                clientConnectHost = (host == "::" || host == "::0") ? "::1" : "127.0.0.1";
            }
            else if (System.Net.IPAddress.TryParse(host, out var parsedAddr))
            {
                if (parsedAddr.Equals(System.Net.IPAddress.Any))
                {
                    serverBindHost = host;
                    clientConnectHost = "127.0.0.1";
                }
                else if (parsedAddr.Equals(System.Net.IPAddress.IPv6Any))
                {
                    serverBindHost = host;
                    clientConnectHost = "::1";
                }
            }

            try
            {
                _transport.StartServer(serverBindHost, port);
                _transport.StartClient(clientConnectHost, port);

                _ticker.OnTick += HostTick;
                _ticker.Start();

                LiminalLogger.Log($"[Manager] Host running on {serverBindHost}:{port} (client connected via {clientConnectHost}) local id = {_transport.LocalClientId}, isServer = {_transport.IsServer}, isClient = {_transport.IsClient}");
                return true;
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] StartHost failed: {ex.Message}");
                Shutdown();
                throw;
            }
        }

        public bool StartServer() => StartServer(_config.Default_Host, _config.Default_Port);

        public bool StartServer(int port) => StartServer(_config.Default_Host, port);

        public bool StartServer(string ip, int port)
        {
            if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
            {
                LiminalLogger.LogWarning($"[Manager] Cannot StartServer: Manager is in state '{LifecycleState}' (Role: {Role}).");
                return false;
            }

            LifecycleState = NetworkLifecycleState.StartingServer;
            Role = NetworkRole.Server;

            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

            InitializeSystems();

            LiminalLogger.Log($"[Manager] Starting Dedicated Server on {ip}:{port}");

            try
            {
                _transport.StartServer(ip, port);

                LifecycleState = NetworkLifecycleState.ServerActive;

                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                _ticker.OnTick += ServerTick;
                _ticker.Start();

                LiminalAtomicHelpers.SafeInvoke(_onConnectedAndReady);
                return true;
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] StartServer failed: {ex.Message}");
                Shutdown();
                throw;
            }
        }

        public bool StartClient() => StartClient(_config.Default_Host, _config.Default_Port);

        public bool StartClient(string ip) => StartClient(ip, _config.Default_Port);

        public bool StartClient(int port) => StartClient(_config.Default_Host, port);

        public bool StartClient(string ip, int port)
        {
            if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
            {
                LiminalLogger.LogWarning($"[Manager] Cannot StartClient: Manager is in state '{LifecycleState}' (Role: {Role}).");
                return false;
            }

            LifecycleState = NetworkLifecycleState.Connecting;

            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

            Role = NetworkRole.Client;

            InitializeSystems();

            LiminalLogger.Log($"[Manager] Starting Client connecting to {ip}:{port}");

            try
            {
                _transport.StartClient(ip, port);

                _ticker.OnTick += ClientBackgroundTick;
                _ticker.Start();
                return true;
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] StartClient failed: {ex.Message}");
                Shutdown();
                throw;
            }
        }

        public void Disconnect()
        {
            if (Role == NetworkRole.None && LifecycleState == NetworkLifecycleState.Stopped) return;

            LifecycleState = NetworkLifecycleState.Stopping;
            Role = NetworkRole.None;

            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

            _transport.Disconnect();

            ShutdownSystems();

            LifecycleState = NetworkLifecycleState.Stopped;

            LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

            LiminalLogger.Log("[Manager] Network State Disconnected.");
        }
        private int _isShuttingDown;

        public void Shutdown()
        {
            if (Interlocked.Exchange(ref _isShuttingDown, 1) == 1)
                return;

            try
            {
                bool wasActive = Role != NetworkRole.None || LifecycleState != NetworkLifecycleState.Stopped;
                LifecycleState = NetworkLifecycleState.Stopping;
                Role = NetworkRole.None;

                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                _ticker?.Stop();

                _transport.Shutdown();

                ShutdownSystems();

                LifecycleState = NetworkLifecycleState.Stopped;

                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                if (wasActive)
                {
                    LiminalLogger.Log("[Manager] Network State Reset.");
                }
            }
            finally
            {
                Interlocked.Exchange(ref _isShuttingDown, 0);
            }
        }
        #endregion

        #region Ticking Logic

        private void HostTick()
        {
            var sm = SessionManager;

            _eventHub.RaisePrePoll();
            sm?.Poll();
            _eventHub.RaisePostPoll();

            _eventHub.RaisePreFlush();
            sm?.Flush();
            _eventHub.RaisePostFlush();
        }

        private void ServerTick()
        {
            var sm = SessionManager;

            _eventHub.RaisePrePoll();
            sm?.Poll();
            _eventHub.RaisePostPoll();

            _eventHub.RaisePreFlush();
            sm?.Flush();
            _eventHub.RaisePostFlush();
        }

        private void ClientBackgroundTick()
        {
            //For now
            var sm = SessionManager;

            _eventHub.RaisePrePoll();
            sm?.Poll();
            _eventHub.RaisePostPoll();

            if (Role == NetworkRole.Client && _phaseAligner != null && TelemetryManager != null)
            {
                double wireRtt = TelemetryManager.WireRTT;
                if (wireRtt > 0.0)
                {
                    // The tick is firing right now, so clientCountdownMs for this batch is 0
                    long slew = _phaseAligner.CalculateSlewAdjustment(
                        wireRtt,
                        TelemetryManager.ServerCountdownMs,
                        clientCountdownMs: 0.0);

                    _ticker.ApplySlew(slew);
                }
            }

            _eventHub.RaisePreFlush();
            sm?.Flush();
            _eventHub.RaisePostFlush();
        }

        /// <summary>
        /// Call this from Unity FixedUpdate() on Client or Host.
        /// </summary>
        public void ManualPoll()
        {
            if (Role == NetworkRole.Client)
            {
                _eventHub.RaisePrePoll();
                SessionManager?.Poll();
                _eventHub.RaisePostPoll();
            }
        }

        #endregion
    }
}
