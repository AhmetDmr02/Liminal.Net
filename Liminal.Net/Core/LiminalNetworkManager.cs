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

        private int _role = (int)NetworkRole.None;
        private int _lifecycleState = (int)NetworkLifecycleState.Stopped;

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
                && Volatile.Read(ref _isShutDown) == 0;
        }

        private bool ValidateHostOperational()
        {
            return _transport != null
                && _transport.IsServer
                && _transport.IsConnected
                && localID != 0
                && SessionManager != null
                && Volatile.Read(ref _isShutDown) == 0;
        }

        private bool ValidateServerOperational()
        {
            return _transport != null
                && _transport.IsServer
                && SessionManager != null
                && Volatile.Read(ref _isShutDown) == 0;
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
            _manualPollBody = ManualPollBody;

            Interpreter = new LiminalPacketInterpreter(this, _config);

            _eventHub = new LiminalEventHub(_transport);

            _transport.InitializeTransport(config);

            ClientRegistry = new Liminal.Net.Registry.ClientRegistry(this);

            InitializeSystems();
        }

        //A CAS so a connect landing after a teardown already moved the state on cant bring it back
        //runs from the transport thread right away and again on the drain before the event, whichever is first moves it
        private void HandlePreLocalClientConnected(ushort clientId)
        {
            NetworkLifecycleState from, to;
            switch (Role)
            {
                case NetworkRole.Client:
                    from = NetworkLifecycleState.Connecting;
                    to = NetworkLifecycleState.Connected;
                    break;
                case NetworkRole.Host:
                    from = NetworkLifecycleState.StartingHost;
                    to = NetworkLifecycleState.HostActive;
                    break;
                default:
                    return;
            }

            if (Interlocked.CompareExchange(ref _lifecycleState, (int)to, (int)from) == (int)from)
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, to);
        }

        private void HandlePostLocalClientConnected(ushort clientId)
        {
            LiminalAtomicHelpers.SafeInvoke(_onConnectedAndReady);
        }

        //So OnLocalClientDisconnected handlers already see Stopping, never overwrites a teardown's own state
        private void HandlePreLocalClientDisconnected(ushort clientId)
        {
            while (true)
            {
                int current = Volatile.Read(ref _lifecycleState);
                if (current == (int)NetworkLifecycleState.Stopped || current == (int)NetworkLifecycleState.Stopping)
                    return;

                if (Interlocked.CompareExchange(ref _lifecycleState, (int)NetworkLifecycleState.Stopping, current) == current)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, NetworkLifecycleState.Stopping);
                    return;
                }
            }
        }

        //Transport thread, only for the run that queued it, a restart after the queue took it must not have its state moved by the old run
        //skipped when the tick thread cant get the lock, the drain moves the state before raising the event anyway
        private void ApplyLocalStateFromTransport(LiminalSessionManager owner, Action<ushort> apply, ushort clientId)
        {
            if (!TryEnterStateLock())
                return;

            try
            {
                if (!_inTeardown && ReferenceEquals(SessionManager, owner))
                    apply(clientId);
            }
            finally
            {
                Monitor.Exit(_stateLock);
            }
        }

        //Transport thread, a local disconnect that missed its run's queue still reaches the game unless a newer run already started
        private void HandleOrphanLocalClientDisconnected(LiminalSessionManager owner, ushort clientId)
        {
            if (!TryEnterStateLock())
                return;

            try
            {
                if (!ReferenceEquals(SessionManager, owner) || Role != NetworkRole.None)
                    return;

                _eventHub.RaiseLocalClientDisconnected(clientId);
                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_stateLock);
            }
        }

        private void InitializeSystems()
        {
            _eventHub.RaiseManagerPreInitialize();

            ShutdownSystems();

            //The previous run's late transport events stop here, until now its closed session manager dropped them
            SessionManager?.DetachTransport();

            LiminalLogger.Log("[Manager] Initializing Network Systems...");

            _pipeline = new LiminalPacketFramerPipeline(_config);
            var sessionManager = new LiminalSessionManager(_transport, Interpreter, _config, _pipeline);
            sessionManager.OrphanLocalClientDisconnected = id => HandleOrphanLocalClientDisconnected(sessionManager, id);
            sessionManager.LocalClientConnectedQueued = id => ApplyLocalStateFromTransport(sessionManager, HandlePreLocalClientConnected, id);
            sessionManager.LocalClientDisconnectedQueued = id => ApplyLocalStateFromTransport(sessionManager, HandlePreLocalClientDisconnected, id);
            SessionManager = sessionManager;

            //Created after the session manager so it hears a disconnect after it, the resolve then queues behind the disconnect
            var coordinator = new DisconnectReasonCoordinator(_transport, Interpreter);
            DisconnectCoordinator = coordinator;

            _eventHub.BindCoreSystems(
                sessionManager,
                HandlePreLocalClientConnected,
                HandlePostLocalClientConnected,
                HandlePreLocalClientDisconnected);

            //Into this run's own queue, a resolve landing after the run ended finds it closed and is dropped
            coordinator.OnResolved += (id, reason, message) =>
                sessionManager.EnqueueLifecycle(LiminalLifecycleEntry.Resolved(id, reason, message));

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

        private void ShutdownSystems()
        {
            if (_ticker != null)
            {
                _ticker.OnTick -= HostTick;
                _ticker.OnTick -= ServerTick;
                _ticker.OnTick -= ClientBackgroundTick;
                StopTicker();
            }

            //Tick is stopped, connection events still queued (like the drop that started this teardown) are raised here with their packets
            //a kick notice in there still reaches the coordinator so it knows the reason before it goes
            SessionManager?.ProcessLifecycleEvents();

            //Reasons it resolves on dispose are queued while still bound and raised by the second drain, nothing gets raised once unbound
            //cleared first, its dispose can re-enter teardown through the transport and that nested pass must not dispose it again
            var coordinator = DisconnectCoordinator;
            if (coordinator != null)
            {
                DisconnectCoordinator = null;
                coordinator.Dispose();
            }

            SessionManager?.ProcessLifecycleEvents();

            _eventHub.UnbindCoreSystems();

            TelemetryManager?.Dispose();
            TelemetryManager = null;

            SessionManager?.Dispose();

            // Last flush to trigger internal dispose
            SessionManager?.Flush();

            //We actually wanna keep subscriptions around
            //Interpreter?.ClearAllHandlers();
            Interpreter?.ClearAllSticky();

            //_ticker = null;
            //SessionManager = null;
            ClientRegistry?.Reset();
            SyncVarManager?.DetachFromManager();

            _config.ClientIdResolver?.ResetResolver();
        }

        #region Lifecycle Lock & Drain
        //One plain lock for every start, stop and teardown. Nothing holding it ever waits on a transport thread, transport events only
        //queue into the run's session manager and get acted on when the manager drains, so a transport thread cant deadlock against it
        //the tick thread never blocks on it, a teardown holding it may be waiting for that very tick
        private readonly object _stateLock = new();

        //Only touched under _stateLock, a call coming back in from a handler while this thread tears down leaves it to that teardown
        private bool _inTeardown;

        //Requests set without the lock so one made from the tick thread while someone else holds it is never lost, the next drain does it
        private int _isShutDown;
        private int _shutdownCompleted;
        private int _disconnectRequested;

        //False only on the tick thread when the lock is taken, the caller then leaves its request to the next drain
        private bool TryEnterStateLock()
        {
            var ticker = _ticker;
            if (ticker != null && ticker.IsTickingOnCurrentThread)
                return Monitor.TryEnter(_stateLock);

            Monitor.Enter(_stateLock);
            return true;
        }

        //Caller holds _stateLock, acts on what came in since the last drain, true once the run is over
        private bool DrainLocked()
        {
            if (_inTeardown)
                return false;

            if (Volatile.Read(ref _isShutDown) != 0)
            {
                CompleteRequestedShutdownLocked();
                return true;
            }

            if (Volatile.Read(ref _disconnectRequested) != 0)
            {
                DisconnectLocked();
                return true;
            }

            var sessionManager = SessionManager;
            if (sessionManager != null && sessionManager.TransportShutDown && Role != NetworkRole.None)
            {
                TeardownSession();
                CompleteRequestedShutdownLocked();
                return true;
            }

            return false;
        }

        //Never blocks, whoever holds the lock is starting or tearing down and drains on its own
        //the peek only spares quiet ticks the lock, DrainLocked decides again under it and a request set right after the peek waits one tick
        private bool DrainFromTick()
        {
            var sessionManager = SessionManager;
            bool pending = Volatile.Read(ref _isShutDown) != 0 ||
                           Volatile.Read(ref _disconnectRequested) != 0 ||
                           (sessionManager != null && sessionManager.TransportShutDown);
            if (!pending)
                return false;

            if (!Monitor.TryEnter(_stateLock))
                return false;

            try
            {
                return DrainLocked();
            }
            finally
            {
                Monitor.Exit(_stateLock);
            }
        }

        private void StopTicker()
        {
            _ticker?.Stop();
        }

        //Caller holds _stateLock
        private bool CanStartLocked(string call)
        {
            if (_inTeardown)
            {
                LiminalLogger.LogError($"[Manager] Cannot {call} from inside a teardown, start again once OnLifecycleStateChanged reports Stopped.");
                return false;
            }

            DrainLocked();

            if (Volatile.Read(ref _isShutDown) != 0)
            {
                LiminalLogger.LogError($"[Manager] Cannot {call}: this manager was shut down, create a new LiminalNetworkManager.");
                return false;
            }

            if (LifecycleState != NetworkLifecycleState.Stopped || Role != NetworkRole.None)
            {
                LiminalLogger.LogWarning($"[Manager] Cannot {call}: Manager is in state '{LifecycleState}' (Role: {Role}).");
                return false;
            }

            Volatile.Write(ref _disconnectRequested, 0);
            return true;
        }

        private void RaiseTransportShutdownIfPending()
        {
            var sessionManager = SessionManager;
            if (sessionManager != null && sessionManager.TryTakeTransportShutdownEvent())
                _eventHub.RaiseTransportShutdown();
        }
        #endregion

        #region Start Methods

        public bool StartHost() => StartHost(_config.Default_Host, _config.Default_Port);

        public bool StartHost(int port) => StartHost(_config.Default_Host, port);

        public bool StartHost(string host, int port)
        {
            if (!TryEnterStateLock()) return false;
            try
            {
                if (!CanStartLocked("StartHost"))
                    return false;

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
                Monitor.Exit(_stateLock);
            }
        }

        public bool StartServer() => StartServer(_config.Default_Host, _config.Default_Port);

        public bool StartServer(int port) => StartServer(_config.Default_Host, port);

        public bool StartServer(string ip, int port)
        {
            if (!TryEnterStateLock()) return false;
            try
            {
                if (!CanStartLocked("StartServer"))
                    return false;

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
                Monitor.Exit(_stateLock);
            }
        }

        public bool StartClient() => StartClient(_config.Default_Host, _config.Default_Port);

        public bool StartClient(string ip) => StartClient(ip, _config.Default_Port);

        public bool StartClient(int port) => StartClient(_config.Default_Host, port);

        public bool StartClient(string ip, int port)
        {
            if (!TryEnterStateLock()) return false;
            try
            {
                if (!CanStartLocked("StartClient"))
                    return false;

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
                Monitor.Exit(_stateLock);
            }
        }

        public void Disconnect()
        {
            if (!TryEnterStateLock())
            {
                Volatile.Write(ref _disconnectRequested, 1);
                return;
            }

            try
            {
                if (_inTeardown) return;

                DisconnectLocked();
                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_stateLock);
            }
        }

        //Caller holds _stateLock
        private void DisconnectLocked()
        {
            Volatile.Write(ref _disconnectRequested, 0);

            if (Role == NetworkRole.None && LifecycleState == NetworkLifecycleState.Stopped) return;

            try
            {
                if (Role == NetworkRole.Client && _transport.LocalClientId != 0)
                {
                    DisconnectCoordinator?.RecordLocalLeave(_transport.LocalClientId, "Disconnect() was called.");
                }

                StopTicker();
                _transport.Disconnect();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] Error during Disconnect: {ex.Message}");
            }

            TeardownSession();
        }

        //Caller holds _stateLock
        private void TeardownSession()
        {
            bool wasActive = Role != NetworkRole.None || LifecycleState != NetworkLifecycleState.Stopped;
            bool stopped = false;

            _inTeardown = true;
            try
            {
                RaiseTransportShutdownIfPending();

                bool alreadyStopping = Interlocked.Exchange(ref _lifecycleState, (int)NetworkLifecycleState.Stopping) == (int)NetworkLifecycleState.Stopping;
                if (!alreadyStopping)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, NetworkLifecycleState.Stopping);
                }

                try
                {
                    StopTicker();
                    ShutdownSystems();
                    stopped = true;
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Manager] Error during session teardown: {ex.Message}");
                }

                Role = NetworkRole.None;
                LifecycleState = NetworkLifecycleState.Stopped;
            }
            finally
            {
                _inTeardown = false;
            }

            //Out of the teardown so a handler starting again from here is allowed to
            if (stopped)
            {
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, NetworkLifecycleState.Stopped);

                if (wasActive)
                {
                    LiminalLogger.Log("[Manager] Network State Disconnected.");
                }
            }
        }

        //Caller holds _stateLock, undoes a failed start only, the manager and the game's subscriptions stay so it can be started again
        private void AbortStart()
        {
            try
            {
                StopTicker();
                _transport.Shutdown();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Manager] Error while undoing a failed start: {ex.Message}");
            }

            TeardownSession();
            CompleteRequestedShutdownLocked();
        }

        //Final, after this the manager cant be started again, Disconnect is the call for leaving and reconnecting
        public void Shutdown()
        {
            if (!TryEnterStateLock())
            {
                //Tick thread while someone else holds the lock, the next drain or the running teardown finishes it
                Volatile.Write(ref _isShutDown, 1);
                return;
            }

            try
            {
                //Set under the lock so a tick only ever sees it once this thread is done or while it still holds the lock
                //final from here on even if a teardown is running, it finishes the shutdown on its way out
                Volatile.Write(ref _isShutDown, 1);
                if (_inTeardown) return;

                CompleteRequestedShutdownLocked();
            }
            finally
            {
                Monitor.Exit(_stateLock);
            }
        }

        public void Dispose() => Shutdown();

        //Caller holds _stateLock, every teardown path calls this on its way out so a Shutdown that came in during it is not lost
        private void CompleteRequestedShutdownLocked()
        {
            if (_inTeardown) return;
            if (Volatile.Read(ref _isShutDown) == 0) return;
            if (Interlocked.Exchange(ref _shutdownCompleted, 1) != 0) return;

            ShutdownInternal();
        }

        //Caller holds _stateLock
        private void ShutdownInternal()
        {
            bool wasActive = Role != NetworkRole.None || LifecycleState != NetworkLifecycleState.Stopped || _transport.IsConnected;
            bool stopped = false;

            _inTeardown = true;
            try
            {
                //A dropped connection already raised Stopping, this shutdown is what closes it
                bool alreadyStopping = Interlocked.Exchange(ref _lifecycleState, (int)NetworkLifecycleState.Stopping) == (int)NetworkLifecycleState.Stopping;
                if (!alreadyStopping)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, NetworkLifecycleState.Stopping);
                }

                try
                {
                    if (Role == NetworkRole.Client && _transport.LocalClientId != 0)
                    {
                        DisconnectCoordinator?.ResolveLocalLeave(_transport.LocalClientId, "Shutdown() was called.");
                    }

                    StopTicker();

                    _transport.Shutdown();
                    RaiseTransportShutdownIfPending();

                    _eventHub.RaiseManagerShutdown();
                    //Disconnect already unhooked it from OnManagerShutdown so tear it down directly or no new manager can adopt its syncvars
                    SyncVarManager?.FullTeardown();

                    ShutdownSystems();

                    //Final, so the manager lets go of the transport, a transport the game keeps must not keep this manager alive
                    SessionManager?.DetachTransport();
                    _eventHub.Clear();
                    if (_transport is ITransportTelemetryProvider telemetryProvider)
                    {
                        telemetryProvider.NextTickProvider = null;
                    }

                    stopped = true;
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Manager] Error during Shutdown: {ex.Message}");
                }

                Role = NetworkRole.None;
                LifecycleState = NetworkLifecycleState.Stopped;
            }
            finally
            {
                _inTeardown = false;
            }

            if (stopped)
            {
                LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, NetworkLifecycleState.Stopped);

                if (wasActive)
                {
                    LiminalLogger.Log("[Manager] Network State Reset.");
                }
            }
        }
        #endregion

        #region Ticking Logic

        private void HostTick()
        {
            if (DrainFromTick()) return;

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
            if (DrainFromTick()) return;

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
            if (DrainFromTick()) return;

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
                //Counted like a tick so a teardown on another thread waits for it instead of running under it
                _ticker?.TryRunTracked(_manualPollBody);
            }
        }

        private readonly Action _manualPollBody;

        private void ManualPollBody()
        {
            if (DrainFromTick()) return;

            _eventHub.RaisePrePoll();
            SessionManager?.Poll();
            _eventHub.RaisePostPoll();
        }

        #endregion
    }
}
