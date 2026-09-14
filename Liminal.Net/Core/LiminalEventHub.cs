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
        /// <summary>
        /// Binds active core systems so they are deterministically updated before high-level events fire.
        /// </summary>
        public void BindCoreSystems(LiminalSessionManager sessionManager, DisconnectReasonCoordinator disconnectCoordinator = null)
        {
            _sessionManager = sessionManager;
            _disconnectCoordinator = disconnectCoordinator;
        }

        /// <summary>
        /// Unbinds active core systems on manager shutdown or reset.
        /// </summary>
        public void UnbindCoreSystems()
        {
            _sessionManager = null;
            _disconnectCoordinator = null;
        }
        #endregion

        #region Staged Transport Event Handlers
        private void HandleTransportClientConnected(ushort clientId)
        {
            if (_disposed) return;

            _sessionManager?.HandleClientConnected(clientId);

            _onClientConnected?.Invoke(clientId);
        }

        private void HandleTransportLocalClientConnected(ushort clientId)
        {
            if (_disposed) return;

            _sessionManager?.HandleLocalConnection(clientId);

            _onLocalClientConnected?.Invoke(clientId);
        }

        private void HandleTransportClientDisconnected(ushort clientId)
        {
            if (_disposed) return;

            _sessionManager?.HandleClientDisconnected(clientId);

            _onClientDisconnected?.Invoke(clientId);
        }

        private void HandleTransportLocalClientDisconnected(ushort clientId)
        {
            if (_disposed) return;

            _sessionManager?.HandleClientDisconnected(clientId);
            _sessionManager?.HandleClientDisconnected(ILiminalTransport.SERVER_ID);

            _onLocalClientDisconnected?.Invoke(clientId);
        }

        private void HandleTransportClientKicked(ushort clientId)
        {
            if (_disposed) return;

            _sessionManager?.HandleClientDisconnected(clientId);

            _onClientKicked?.Invoke(clientId);
        }

        private void HandleTransportServerStarted()
        {
            if (_disposed) return;
            _onServerStarted?.Invoke();
        }

        private void HandleTransportShutdown()
        {
            if (_disposed) return;
            _onTransportShutdown?.Invoke();
        }
        #endregion

        #region Internal Dispatchers for Manager
        internal void RaiseDisconnectResolved(ushort id, DisconnectReason reason, string message)
        {
            _onDisconnectResolved?.Invoke(id, reason, message);
        }

        internal void RaiseManagerPreInitialize() => _onManagerPreInitialize?.Invoke();
        internal void RaiseManagerPostInitialize() => _onManagerPostInitialize?.Invoke();
        internal void RaiseManagerShutdown() => _onManagerShutdown?.Invoke();
        internal void RaisePreFlush() => _onPreFlush?.Invoke();
        internal void RaisePostFlush() => _onPostFlush?.Invoke();
        internal void RaisePrePoll() => _onPrePoll?.Invoke();
        internal void RaisePostPoll() => _onPostPoll?.Invoke();
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
