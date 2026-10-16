using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Diagnostics;
using System.Net;
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

    public class LiminalNetworkManager : IDisposable
    {
        #region Singleton
        public static volatile LiminalNetworkManager Instance;
        #endregion

        private volatile int _role = (int)NetworkRole.None;
        private volatile int _lifecycleState = (int)NetworkLifecycleState.Stopped;

        public NetworkRole Role
        {
            get => (NetworkRole)Volatile.Read(ref _role);
            private set => Volatile.Write(ref _role, (int)value);
        }

        public NetworkLifecycleState LifecycleState
        {
            get => (NetworkLifecycleState)Volatile.Read(ref _lifecycleState);
            private set => Volatile.Write(ref _lifecycleState, (int)value);
        }

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
        public EndPoint? LocalEndpoint => _transport?.LocalEndpoint;
        public int BoundPort => (_transport?.LocalEndpoint as IPEndPoint)?.Port ?? 0;

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
        public LiminalTelemetryConfig TelemetryConfig => _telemetryConfig;

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
            if (config.ClientIdResolver == null) config.ClientIdResolver = new BaseResolver();

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
            if (!TryEnterTeardownLock()) return;
            try
            {
                if (Volatile.Read(ref _isDisconnecting) != 0) return;
                if (Volatile.Read(ref _isShuttingDown) != 0) return;
                if (Role == NetworkRole.None && LifecycleState == NetworkLifecycleState.Stopped && DisconnectCoordinator == null) return;

                TeardownSession();
                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        //Transport thread, decided under the lock teardown and shutdown hold so a connect landing during or after them cant bring the state back
        private void HandlePreLocalClientConnected(ushort clientId)
        {
            if (!TryEnterTeardownLock()) return;
            try
            {
                if (Volatile.Read(ref _isDisconnecting) != 0 || Volatile.Read(ref _isTearingDown) != 0 || Volatile.Read(ref _isShuttingDown) != 0)
                    return;

                NetworkLifecycleState next;
                if (Role == NetworkRole.Client) next = NetworkLifecycleState.Connected;
                else if (Role == NetworkRole.Host) next = NetworkLifecycleState.HostActive;
                else return;

                LifecycleState = next;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, next);
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        private void HandlePostLocalClientConnected(ushort clientId)
        {
            LiminalAtomicHelpers.SafeInvoke(_onConnectedAndReady);
        }

        private void HandlePreLocalClientDisconnected(ushort clientId)
        {
            if (!TryEnterTeardownLock()) return;
            try
            {
                if (Volatile.Read(ref _isDisconnecting) != 0 || Volatile.Read(ref _isShuttingDown) != 0)
                    return;

                if (LifecycleState != NetworkLifecycleState.Stopped)
                {
                    LifecycleState = NetworkLifecycleState.Stopping;
                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
                }
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
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
                DisconnectCoordinator.Dispose();
                DisconnectCoordinator.OnResolved -= HandleDisconnectResolved;
                DisconnectCoordinator = null;
            }

            //We actually wanna keep subscriptions around
            //Interpreter?.ClearAllHandlers();
            Interpreter?.ClearAllSticky();

            //_ticker = null;
            //SessionManager = null;
            ClientRegistry?.Reset();
            SyncVarManager?.DetachFromManager();

            _config.ClientIdResolver?.ResetResolver();
        }

        #region Start Methods

        public bool StartHost() => StartHost(_config.Default_Host, _config.Default_Port);

        public bool StartHost(int port) => StartHost(_config.Default_Host, port);

        public bool StartHost(string host, int port)
        {
            if (!TryEnterTeardownLock()) return false;
            try
            {
                if (Volatile.Read(ref _isShutDown) != 0)
                {
                    LiminalLogger.LogError("[Manager] Cannot StartHost: this manager was shut down, create a new LiminalNetworkManager.");
                    return false;
                }

                if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
                {
                    LiminalLogger.LogWarning($"[Manager] Cannot StartHost: Manager is in state '{LifecycleState}' (Role: {Role}).");
                    return false;
                }

                LifecycleState = NetworkLifecycleState.StartingHost;
                Role = NetworkRole.Host;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                if (DisconnectCoordinator == null)
                {
                    InitializeSystems();
                }

                LiminalLogger.Log("[Manager] Starting Host Mode...");

                string serverBindHost = host;
                string clientConnectHost = host;

                bool isWildcard = string.IsNullOrWhiteSpace(host) || host == "0.0.0.0" || host == "::" || host == "::0";
                if (!isWildcard && System.Net.IPAddress.TryParse(host, out var parsedAddr))
                {
                    isWildcard = parsedAddr.Equals(System.Net.IPAddress.Any) || parsedAddr.Equals(System.Net.IPAddress.IPv6Any);
                }

                if (isWildcard)
                {
                    serverBindHost = host;
                    clientConnectHost = (host == "::" || host == "::0" || (System.Net.IPAddress.TryParse(host, out var addr) && addr.Equals(System.Net.IPAddress.IPv6Any))) ? "::1" : "127.0.0.1";
                }

                try
                {
                    _transport.StartServer(serverBindHost, port);
                    _transport.StartClient(clientConnectHost, port);

                    _ticker.OnTick += HostTick;
                    _ticker.Start();

                    string displayBind = (isWildcard && System.Net.Sockets.Socket.OSSupportsIPv6) ? $"[::]:{port} (DualMode IPv4/IPv6)" : $"{serverBindHost}:{port}";
                    LiminalLogger.Log($"[Manager] Host running on {displayBind} (client connected via {clientConnectHost}) local id = {_transport.LocalClientId}, isServer = {_transport.IsServer}, isClient = {_transport.IsClient}");
                    return true;
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Manager] StartHost failed: {ex.Message}");
                    AbortStart();
                    throw;
                }
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        public bool StartServer() => StartServer(_config.Default_Host, _config.Default_Port);

        public bool StartServer(int port) => StartServer(_config.Default_Host, port);

        public bool StartServer(string ip, int port)
        {
            if (!TryEnterTeardownLock()) return false;
            try
            {
                if (Volatile.Read(ref _isShutDown) != 0)
                {
                    LiminalLogger.LogError("[Manager] Cannot StartServer: this manager was shut down, create a new LiminalNetworkManager.");
                    return false;
                }

                if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
                {
                    LiminalLogger.LogWarning($"[Manager] Cannot StartServer: Manager is in state '{LifecycleState}' (Role: {Role}).");
                    return false;
                }

                LifecycleState = NetworkLifecycleState.StartingServer;
                Role = NetworkRole.Server;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                if (DisconnectCoordinator == null)
                {
                    InitializeSystems();
                }

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
                    AbortStart();
                    throw;
                }
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        public bool StartClient() => StartClient(_config.Default_Host, _config.Default_Port);

        public bool StartClient(string ip) => StartClient(ip, _config.Default_Port);

        public bool StartClient(int port) => StartClient(_config.Default_Host, port);

        public bool StartClient(string ip, int port)
        {
            if (!TryEnterTeardownLock()) return false;
            try
            {
                if (Volatile.Read(ref _isShutDown) != 0)
                {
                    LiminalLogger.LogError("[Manager] Cannot StartClient: this manager was shut down, create a new LiminalNetworkManager.");
                    return false;
                }

                if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
                {
                    LiminalLogger.LogWarning($"[Manager] Cannot StartClient: Manager is in state '{LifecycleState}' (Role: {Role}).");
                    return false;
                }

                LifecycleState = NetworkLifecycleState.Connecting;
                Role = NetworkRole.Client;
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                if (DisconnectCoordinator == null)
                {
                    InitializeSystems();
                }

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
                    AbortStart();
                    throw;
                }
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        private readonly object _teardownLock = new();

        //A teardown holding this lock stops the ticker and waits for its tick, a tick blocked here would stall that wait
        //so on the tick thread give up once the ticker is being stopped, the teardown in progress covers what was asked
        private bool TryEnterTeardownLock()
        {
            var ticker = _ticker;
            if (ticker == null || !ticker.IsTickingOnCurrentThread)
            {
                Monitor.Enter(_teardownLock);
                return true;
            }

            while (!Monitor.TryEnter(_teardownLock, 1))
            {
                if (!ticker.IsRunning)
                    return false;
            }
            return true;
        }
        private int _isDisconnecting;
        private int _isTearingDown;
        private int _isShuttingDown;
        //Set the moment Shutdown is asked for, _shutdownCompleted once ShutdownInternal actually ran
        private int _isShutDown;
        private int _shutdownCompleted;

        public void Disconnect()
        {
            if (!TryEnterTeardownLock()) return;
            try
            {
                if (LifecycleState == NetworkLifecycleState.Stopping) return;
                if (Role == NetworkRole.None && LifecycleState == NetworkLifecycleState.Stopped) return;

                Volatile.Write(ref _isDisconnecting, 1);
                try
                {
                    if (Role == NetworkRole.Client && _transport.LocalClientId != 0)
                    {
                        DisconnectCoordinator?.RecordLocalLeave(_transport.LocalClientId, "Disconnect() was called.");
                    }

                    _ticker?.Stop();
                    _transport.Disconnect();

                    TeardownSession();
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Manager] Error during Disconnect: {ex.Message}");
                    LifecycleState = NetworkLifecycleState.Stopped;
                }
                finally
                {
                    Volatile.Write(ref _isDisconnecting, 0);
                }

                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        private void TeardownSession()
        {
            // Must be called while holding _teardownLock
            Volatile.Write(ref _isTearingDown, 1);
            try
            {
                bool wasActive = Role != NetworkRole.None || LifecycleState != NetworkLifecycleState.Stopped;
                bool alreadyStopping = LifecycleState == NetworkLifecycleState.Stopping;
                LifecycleState = NetworkLifecycleState.Stopping;

                if (!alreadyStopping)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
                }

                try
                {
                    _ticker?.Stop();

                    ShutdownSystems();

                    Role = NetworkRole.None;
                    LifecycleState = NetworkLifecycleState.Stopped;

                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                    if (wasActive)
                    {
                        LiminalLogger.Log("[Manager] Network State Disconnected.");
                    }
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Manager] Error during session teardown: {ex.Message}");
                    Role = NetworkRole.None;
                    LifecycleState = NetworkLifecycleState.Stopped;
                }
            }
            finally
            {
                Volatile.Write(ref _isTearingDown, 0);
            }
        }

        //Undoes a failed start only, the manager and the game's subscriptions stay so it can be started again
        private void AbortStart()
        {
            Volatile.Write(ref _isDisconnecting, 1);
            try
            {
                _ticker?.Stop();
                _transport.Shutdown();
                TeardownSession();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] Error while undoing a failed start: {ex.Message}");
                Role = NetworkRole.None;
                LifecycleState = NetworkLifecycleState.Stopped;
            }
            finally
            {
                Volatile.Write(ref _isDisconnecting, 0);
            }

            CompleteRequestedShutdownLocked();
        }

        //Final, after this the manager cant be started again, Disconnect is the call for leaving and reconnecting
        public void Shutdown()
        {
            //Final from here on even if it cant run yet, so Start* refuses and the running teardown finishes it
            Volatile.Write(ref _isShutDown, 1);
            if (!TryEnterTeardownLock()) return;
            try
            {
                //All of these are only set while _teardownLock is held, seeing one here means this thread is inside that teardown
                if (Volatile.Read(ref _isDisconnecting) != 0) return;
                if (Volatile.Read(ref _isTearingDown) != 0) return;
                if (Volatile.Read(ref _isShuttingDown) != 0) return;

                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_teardownLock);
            }
        }

        public void Dispose() => Shutdown();

        //Must hold _teardownLock, every teardown path calls this on its way out so a Shutdown that came in during it is not lost
        private void CompleteRequestedShutdownLocked()
        {
            if (Volatile.Read(ref _isShutDown) == 0) return;
            if (Volatile.Read(ref _shutdownCompleted) != 0) return;

            ShutdownInternal();
        }

        private void ShutdownInternal()
        {
            Volatile.Write(ref _shutdownCompleted, 1);
            Volatile.Write(ref _isShuttingDown, 1);
            bool wasActive = Role != NetworkRole.None || LifecycleState != NetworkLifecycleState.Stopped || _transport.IsConnected;
            //A dropped connection already raised Stopping, this shutdown is what closes it
            bool alreadyStopping = LifecycleState == NetworkLifecycleState.Stopping;
            LifecycleState = NetworkLifecycleState.Stopping;

            if (!alreadyStopping)
            {
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);
            }

            try
            {
                if (Role == NetworkRole.Client && _transport.LocalClientId != 0)
                {
                    DisconnectCoordinator?.ResolveLocalLeave(_transport.LocalClientId, "Shutdown() was called.");
                }

                _ticker?.Stop();

                _transport.Shutdown();

                _eventHub.RaiseManagerShutdown();
                //Disconnect already unhooked it from OnManagerShutdown so tear it down directly or no new manager can adopt its syncvars
                SyncVarManager?.FullTeardown();

                ShutdownSystems();

                //Final, so the manager lets go of the transport, a transport the game keeps must not keep this manager alive
                _transport.OnShutdown -= HandleTransportShutdown;
                _eventHub.Clear();
                if (_transport is ITransportTelemetryProvider telemetryProvider)
                {
                    telemetryProvider.NextTickProvider = null;
                }

                Role = NetworkRole.None;
                LifecycleState = NetworkLifecycleState.Stopped;

                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, LifecycleState);

                if (wasActive)
                {
                    LiminalLogger.Log("[Manager] Network State Reset.");
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] Error during Shutdown: {ex.Message}");
                Role = NetworkRole.None;
                LifecycleState = NetworkLifecycleState.Stopped;
            }
            finally
            {
                Volatile.Write(ref _isShuttingDown, 0);
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
