using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalEventHub : ILiminalEventHub, IDisposable
    {
        private readonly ILiminalTransport _transport;
        private LiminalSessionManager _sessionManager;
        private DisconnectReasonCoordinator _disconnectCoordinator;

        private volatile bool _disposed;

        #region High-Level Connection Events
        private Action<ushort> _onClientConnected;
        public event Action<ushort> OnClientConnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientConnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientConnected, value);
        }

        private Action<ushort> _onClientDisconnected;
        public event Action<ushort> OnClientDisconnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientDisconnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientDisconnected, value);
        }

        private Action<ushort> _onLocalClientConnected;
        public event Action<ushort> OnLocalClientConnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLocalClientConnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLocalClientConnected, value);
        }

        private Action<ushort> _onLocalClientDisconnected;
        public event Action<ushort> OnLocalClientDisconnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLocalClientDisconnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLocalClientDisconnected, value);
        }

        private Action<ushort> _onClientKicked;
        public event Action<ushort> OnClientKicked
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientKicked, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientKicked, value);
        }

        private Action _onServerStarted;
        public event Action OnServerStarted
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onServerStarted, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onServerStarted, value);
        }

        private Action _onTransportShutdown;
        public event Action OnTransportShutdown
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onTransportShutdown, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onTransportShutdown, value);
        }
        #endregion

        #region Lifecycle & Diagnostics Events
        private Action<ushort, DisconnectReason, string> _onDisconnectResolved;
        public event Action<ushort, DisconnectReason, string> OnDisconnectResolved
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onDisconnectResolved, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onDisconnectResolved, value);
        }

        private Action _onManagerPreInitialize;
        public event Action OnManagerPreInitialize
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onManagerPreInitialize, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onManagerPreInitialize, value);
        }

        private Action _onManagerPostInitialize;
        public event Action OnManagerPostInitialize
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onManagerPostInitialize, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onManagerPostInitialize, value);
        }

        private Action _onManagerShutdown;
        public event Action OnManagerShutdown
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onManagerShutdown, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onManagerShutdown, value);
        }

        private Action _onPreFlush;
        public event Action OnPreFlush
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onPreFlush, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onPreFlush, value);
        }

        private Action _onPostFlush;
        public event Action OnPostFlush
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onPostFlush, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onPostFlush, value);
        }

        private Action _onPrePoll;
        public event Action OnPrePoll
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onPrePoll, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onPrePoll, value);
        }

        private Action _onPostPoll;
        public event Action OnPostPoll
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onPostPoll, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onPostPoll, value);
        }
        #endregion

        public LiminalEventHub(ILiminalTransport transport)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));

            _transport.OnClientConnected += HandleTransportClientConnected;
            _transport.OnClientDisconnected += HandleTransportClientDisconnected;
            _transport.OnLocalClientConnected += HandleTransportLocalClientConnected;
            _transport.OnLocalClientDisconnected += HandleTransportLocalClientDisconnected;
            _transport.OnClientKicked += HandleTransportClientKicked;
            _transport.OnServerStarted += HandleTransportServerStarted;
            _transport.OnShutdown += HandleTransportShutdown;
        }

        #region Core Binding
        private Action<ushort> _onPreLocalClientConnected;
        private Action<ushort> _onPostLocalClientConnected;
        private Action<ushort> _onPreLocalClientDisconnected;

        /// <summary>
        /// Binds active core systems so they are deterministically updated before high-level events fire.
        /// </summary>
        public void BindCoreSystems(
            LiminalSessionManager sessionManager,
            DisconnectReasonCoordinator disconnectCoordinator = null,
            Action<ushort> onPreLocalClientConnected = null,
            Action<ushort> onPostLocalClientConnected = null,
            Action<ushort> onPreLocalClientDisconnected = null)
        {
            _sessionManager = sessionManager;
            _disconnectCoordinator = disconnectCoordinator;
            _onPreLocalClientConnected = onPreLocalClientConnected;
            _onPostLocalClientConnected = onPostLocalClientConnected;
            _onPreLocalClientDisconnected = onPreLocalClientDisconnected;
        }

        /// <summary>
        /// Unbinds active core systems on manager shutdown or reset.
        /// </summary>
        public void UnbindCoreSystems()
        {
            _sessionManager = null;
            _disconnectCoordinator = null;
            _onPreLocalClientConnected = null;
            _onPostLocalClientConnected = null;
            _onPreLocalClientDisconnected = null;
        }
        #endregion

        #region Staged Transport Event Handlers
        private void HandleTransportClientConnected(ushort clientId)
        {
            if (_disposed || _sessionManager == null) return;

            _sessionManager.HandleClientConnected(clientId);

            LiminalAtomicHelpers.SafeInvoke(_onClientConnected, clientId);
        }

        private void HandleTransportLocalClientConnected(ushort clientId)
        {
            if (_disposed || _sessionManager == null) return;

            _sessionManager.HandleLocalConnection(clientId);

            LiminalAtomicHelpers.SafeInvoke(_onPreLocalClientConnected, clientId);

            LiminalAtomicHelpers.SafeInvoke(_onLocalClientConnected, clientId);

            LiminalAtomicHelpers.SafeInvoke(_onPostLocalClientConnected, clientId);
        }

        private void HandleTransportClientDisconnected(ushort clientId)
        {
            if (_disposed || _sessionManager == null) return;

            _sessionManager.HandleClientDisconnected(clientId);

            LiminalAtomicHelpers.SafeInvoke(_onClientDisconnected, clientId);
        }

        private void HandleTransportLocalClientDisconnected(ushort clientId)
        {
            if (_disposed || _sessionManager == null) return;

            LiminalAtomicHelpers.SafeInvoke(_onPreLocalClientDisconnected, clientId);

            _sessionManager.HandleClientDisconnected(clientId);
            _sessionManager.HandleClientDisconnected(ILiminalTransport.SERVER_ID);

            LiminalAtomicHelpers.SafeInvoke(_onLocalClientDisconnected, clientId);
        }

        private void HandleTransportClientKicked(ushort clientId)
        {
            if (_disposed || _sessionManager == null) return;

            _sessionManager.HandleClientDisconnected(clientId);

            LiminalAtomicHelpers.SafeInvoke(_onClientKicked, clientId);
        }

        private void HandleTransportServerStarted()
        {
            if (_disposed || _sessionManager == null) return;
            LiminalAtomicHelpers.SafeInvoke(_onServerStarted);
        }

        private void HandleTransportShutdown()
        {
            if (_disposed) return;
            LiminalAtomicHelpers.SafeInvoke(_onTransportShutdown);
        }
        #endregion

        #region Internal Dispatchers for Manager
        internal void RaiseDisconnectResolved(ushort id, DisconnectReason reason, string message)
        {
            LiminalAtomicHelpers.SafeInvoke(_onDisconnectResolved, id, reason, message);
        }

        internal void RaiseManagerPreInitialize() => LiminalAtomicHelpers.SafeInvoke(_onManagerPreInitialize);
        internal void RaiseManagerPostInitialize() => LiminalAtomicHelpers.SafeInvoke(_onManagerPostInitialize);
        internal void RaiseManagerShutdown() => LiminalAtomicHelpers.SafeInvoke(_onManagerShutdown);
        internal void RaisePreFlush() => LiminalAtomicHelpers.SafeInvoke(_onPreFlush);
        internal void RaisePostFlush() => LiminalAtomicHelpers.SafeInvoke(_onPostFlush);
        internal void RaisePrePoll() => LiminalAtomicHelpers.SafeInvoke(_onPrePoll);
        internal void RaisePostPoll() => LiminalAtomicHelpers.SafeInvoke(_onPostPoll);
        #endregion

        #region Lifecycle Cleanup
        public void Clear()
        {
            Interlocked.Exchange(ref _onClientConnected, null);
            Interlocked.Exchange(ref _onClientDisconnected, null);
            Interlocked.Exchange(ref _onLocalClientConnected, null);
            Interlocked.Exchange(ref _onLocalClientDisconnected, null);
            Interlocked.Exchange(ref _onClientKicked, null);
            Interlocked.Exchange(ref _onServerStarted, null);
            Interlocked.Exchange(ref _onTransportShutdown, null);
            Interlocked.Exchange(ref _onDisconnectResolved, null);
            Interlocked.Exchange(ref _onManagerPreInitialize, null);
            Interlocked.Exchange(ref _onManagerPostInitialize, null);
            Interlocked.Exchange(ref _onManagerShutdown, null);
            Interlocked.Exchange(ref _onPreFlush, null);
            Interlocked.Exchange(ref _onPostFlush, null);
            Interlocked.Exchange(ref _onPrePoll, null);
            Interlocked.Exchange(ref _onPostPoll, null);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _transport.OnClientConnected -= HandleTransportClientConnected;
            _transport.OnClientDisconnected -= HandleTransportClientDisconnected;
            _transport.OnLocalClientConnected -= HandleTransportLocalClientConnected;
            _transport.OnLocalClientDisconnected -= HandleTransportLocalClientDisconnected;
            _transport.OnClientKicked -= HandleTransportClientKicked;
            _transport.OnServerStarted -= HandleTransportServerStarted;
            _transport.OnShutdown -= HandleTransportShutdown;

            UnbindCoreSystems();
            Clear();
        }
        #endregion
    }
}
