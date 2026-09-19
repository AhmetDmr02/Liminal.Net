#if UNITY_5_3_OR_NEWER || UNITY_ENGINE
using UnityEngine;
#endif
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using Liminal.Net.Registry;
using System;

namespace Liminal.Net.Unity
{
    public enum UnityNetworkEventType : byte
    {
        None = 0,
        LifecycleStateChanged,
        ConnectedAndReady,
        ClientConnected,
        ClientDisconnected,
        LocalClientConnected,
        LocalClientDisconnected,
        ClientKicked,
        ServerStarted,
        TransportShutdown,
        DisconnectResolved,
        ClientJoined,
        ClientLeft,
        LocalClientReady
    }

    public readonly struct UnityNetworkEvent
    {
        public readonly UnityNetworkEventType Type;
        public readonly ushort ClientId;
        public readonly NetworkLifecycleState LifecycleState;
        public readonly DisconnectReason DisconnectReason;
        public readonly string DisconnectMessage;
        public readonly ConnectedClient Client;

        public UnityNetworkEvent(
            UnityNetworkEventType type,
            ushort clientId = 0,
            NetworkLifecycleState lifecycleState = NetworkLifecycleState.Stopped,
            DisconnectReason disconnectReason = DisconnectReason.Unknown,
            string disconnectMessage = null,
            ConnectedClient client = null)
        {
            Type = type;
            ClientId = clientId;
            LifecycleState = lifecycleState;
            DisconnectReason = disconnectReason;
            DisconnectMessage = disconnectMessage;
            Client = client;
        }
    }

#if UNITY_5_3_OR_NEWER || UNITY_ENGINE
    [DisallowMultipleComponent]
    [DefaultExecutionOrder(-1000)]
    public class LiminalUnityBridge : MonoBehaviour
    {
        public static LiminalUnityBridge Instance { get; private set; }
#else
    public class LiminalUnityBridge : IDisposable
    {
        public static LiminalUnityBridge Instance { get; set; }
#endif

        #region Main Thread Events - LiminalNetworkManager
        private Action<NetworkLifecycleState> _onLifecycleStateChanged;
        public event Action<NetworkLifecycleState> OnLifecycleStateChanged
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLifecycleStateChanged, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLifecycleStateChanged, value);
        }

        private Action _onConnectedAndReady;
        public event Action OnConnectedAndReady
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onConnectedAndReady, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onConnectedAndReady, value);
        }
        #endregion

        #region Main Thread Events - LiminalEventHub
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

        private Action<ushort, DisconnectReason, string> _onDisconnectResolved;
        public event Action<ushort, DisconnectReason, string> OnDisconnectResolved
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onDisconnectResolved, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onDisconnectResolved, value);
        }
        #endregion

        #region Main Thread Events - ClientRegistry
        private Action<ConnectedClient> _onClientJoined;
        public event Action<ConnectedClient> OnClientJoined
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientJoined, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientJoined, value);
        }

        private Action<ConnectedClient> _onClientLeft;
        public event Action<ConnectedClient> OnClientLeft
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientLeft, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientLeft, value);
        }

        private Action<ConnectedClient> _onLocalClientReady;
        public event Action<ConnectedClient> OnLocalClientReady
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLocalClientReady, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLocalClientReady, value);
        }
        #endregion

        private LiminalNetworkManager _manager;
        private ZeroGcRingBuffer _ringBuffer;

        private void InitializeBuffer()
        {
            _ringBuffer = new ZeroGcRingBuffer(256);
        }

#if UNITY_5_3_OR_NEWER || UNITY_ENGINE
        private void Awake()
        {
            if (Instance == null)
            {
                Instance = this;
                InitializeBuffer();
            }
            else if (Instance != this)
            {
                Destroy(gameObject);
            }
        }

        private void Start()
        {
            if (_manager == null && LiminalNetworkManager.Instance != null)
            {
                Attach(LiminalNetworkManager.Instance);
            }
        }

        private void Update()
        {
            TickUnityTicker();
            DrainEvents();
        }

        private void OnDestroy()
        {
            Detach();
            if (Instance == this)
            {
                Instance = null;
            }
        }
#else
        public LiminalUnityBridge()
        {
            InitializeBuffer();
        }

        public void Update()
        {
            TickUnityTicker();
            DrainEvents();
        }

        public void Dispose()
        {
            Detach();
            if (Instance == this) Instance = null;
        }
#endif

        #region Manager Attachment
        public void Attach(LiminalNetworkManager manager)
        {
            if (manager == null) return;
            if (_manager == manager) return;

            Detach();
            _manager = manager;

            _manager.OnLifecycleStateChanged += HandleLifecycleStateChanged;
            _manager.OnConnectedAndReady += HandleConnectedAndReady;

            var events = _manager.Events;
            if (events != null)
            {
                events.OnClientConnected += HandleClientConnected;
                events.OnClientDisconnected += HandleClientDisconnected;
                events.OnLocalClientConnected += HandleLocalClientConnected;
                events.OnLocalClientDisconnected += HandleLocalClientDisconnected;
                events.OnClientKicked += HandleClientKicked;
                events.OnServerStarted += HandleServerStarted;
                events.OnTransportShutdown += HandleTransportShutdown;
                events.OnDisconnectResolved += HandleDisconnectResolved;
            }

            var registry = _manager.ClientRegistry;
            if (registry != null)
            {
                registry.OnClientJoined += HandleClientJoined;
                registry.OnClientLeft += HandleClientLeft;
                registry.OnLocalClientReady += HandleLocalClientReady;
            }
        }

        public void Detach()
        {
            if (_manager == null) return;

            _manager.OnLifecycleStateChanged -= HandleLifecycleStateChanged;
            _manager.OnConnectedAndReady -= HandleConnectedAndReady;

            var events = _manager.Events;
            if (events != null)
            {
                events.OnClientConnected -= HandleClientConnected;
                events.OnClientDisconnected -= HandleClientDisconnected;
                events.OnLocalClientConnected -= HandleLocalClientConnected;
                events.OnLocalClientDisconnected -= HandleLocalClientDisconnected;
                events.OnClientKicked -= HandleClientKicked;
                events.OnServerStarted -= HandleServerStarted;
                events.OnTransportShutdown -= HandleTransportShutdown;
                events.OnDisconnectResolved -= HandleDisconnectResolved;
            }

            var registry = _manager.ClientRegistry;
            if (registry != null)
            {
                registry.OnClientJoined -= HandleClientJoined;
                registry.OnClientLeft -= HandleClientLeft;
                registry.OnLocalClientReady -= HandleLocalClientReady;
            }

            _ringBuffer?.Clear();
            _manager = null;
        }
        #endregion

        #region Main Thread Processing
        public void DrainEvents()
        {
            if (_ringBuffer == null) return;

            while (_ringBuffer.TryDequeue(out var ev))
            {
                switch (ev.Type)
                {
                    case UnityNetworkEventType.LifecycleStateChanged:
                        LiminalAtomicHelpers.SafeInvoke(_onLifecycleStateChanged, ev.LifecycleState);
                        break;
                    case UnityNetworkEventType.ConnectedAndReady:
                        LiminalAtomicHelpers.SafeInvoke(_onConnectedAndReady);
                        break;
                    case UnityNetworkEventType.ClientConnected:
                        LiminalAtomicHelpers.SafeInvoke(_onClientConnected, ev.ClientId);
                        break;
                    case UnityNetworkEventType.ClientDisconnected:
                        LiminalAtomicHelpers.SafeInvoke(_onClientDisconnected, ev.ClientId);
                        break;
                    case UnityNetworkEventType.LocalClientConnected:
                        LiminalAtomicHelpers.SafeInvoke(_onLocalClientConnected, ev.ClientId);
                        break;
                    case UnityNetworkEventType.LocalClientDisconnected:
                        LiminalAtomicHelpers.SafeInvoke(_onLocalClientDisconnected, ev.ClientId);
                        break;
                    case UnityNetworkEventType.ClientKicked:
                        LiminalAtomicHelpers.SafeInvoke(_onClientKicked, ev.ClientId);
                        break;
                    case UnityNetworkEventType.ServerStarted:
                        LiminalAtomicHelpers.SafeInvoke(_onServerStarted);
                        break;
                    case UnityNetworkEventType.TransportShutdown:
                        LiminalAtomicHelpers.SafeInvoke(_onTransportShutdown);
                        break;
                    case UnityNetworkEventType.DisconnectResolved:
                        LiminalAtomicHelpers.SafeInvoke(_onDisconnectResolved, ev.ClientId, ev.DisconnectReason, ev.DisconnectMessage);
                        break;
                    case UnityNetworkEventType.ClientJoined:
                        LiminalAtomicHelpers.SafeInvoke(_onClientJoined, ev.Client);
                        break;
                    case UnityNetworkEventType.ClientLeft:
                        LiminalAtomicHelpers.SafeInvoke(_onClientLeft, ev.Client);
                        break;
                    case UnityNetworkEventType.LocalClientReady:
                        LiminalAtomicHelpers.SafeInvoke(_onLocalClientReady, ev.Client);
                        break;
                }
            }
        }

        public void PollPackets()
        {
            if (_manager != null && _manager.IsConnected)
            {
                _manager.ManualPoll();
            }
        }

        public void TickUnityTicker()
        {
            if (_manager?.Ticker is LiminalUnityTicker unityTicker)
            {
                unityTicker.TickUpdate();
            }
        }
        #endregion

        #region Background Event Handlers
        private void HandleLifecycleStateChanged(NetworkLifecycleState state)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.LifecycleStateChanged, lifecycleState: state));

        private void HandleConnectedAndReady()
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ConnectedAndReady));

        private void HandleClientConnected(ushort id)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ClientConnected, clientId: id));

        private void HandleClientDisconnected(ushort id)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ClientDisconnected, clientId: id));

        private void HandleLocalClientConnected(ushort id)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.LocalClientConnected, clientId: id));

        private void HandleLocalClientDisconnected(ushort id)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.LocalClientDisconnected, clientId: id));

        private void HandleClientKicked(ushort id)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ClientKicked, clientId: id));

        private void HandleServerStarted()
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ServerStarted));

        private void HandleTransportShutdown()
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.TransportShutdown));

        private void HandleDisconnectResolved(ushort id, DisconnectReason reason, string message)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.DisconnectResolved, clientId: id, disconnectReason: reason, disconnectMessage: message));

        private void HandleClientJoined(ConnectedClient client)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ClientJoined, clientId: client?.ClientId ?? 0, client: client));

        private void HandleClientLeft(ConnectedClient client)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.ClientLeft, clientId: client?.ClientId ?? 0, client: client));

        private void HandleLocalClientReady(ConnectedClient client)
            => _ringBuffer.TryEnqueue(new UnityNetworkEvent(UnityNetworkEventType.LocalClientReady, clientId: client?.ClientId ?? 0, client: client));
        #endregion

        #region Zero-GC Preallocated Ring Buffer
        private sealed class ZeroGcRingBuffer
        {
            private readonly UnityNetworkEvent[] _buffer;
            private readonly int _capacity;
            private int _head;
            private int _tail;
            private readonly object _lock = new();

            public ZeroGcRingBuffer(int capacity = 256)
            {
                _capacity = capacity;
                _buffer = new UnityNetworkEvent[capacity];
            }

            public bool TryEnqueue(in UnityNetworkEvent ev)
            {
                lock (_lock)
                {
                    int nextHead = (_head + 1) % _capacity;
                    if (nextHead == _tail)
                    {
                        return false;
                    }

                    _buffer[_head] = ev;
                    _head = nextHead;
                    return true;
                }
            }

            public bool TryDequeue(out UnityNetworkEvent ev)
            {
                lock (_lock)
                {
                    if (_tail == _head)
                    {
                        ev = default;
                        return false;
                    }

                    ev = _buffer[_tail];
                    _buffer[_tail] = default;
                    _tail = (_tail + 1) % _capacity;
                    return true;
                }
            }

            public void Clear()
            {
                lock (_lock)
                {
                    Array.Clear(_buffer, 0, _capacity);
                    _head = 0;
                    _tail = 0;
                }
            }
        }
        #endregion
    }
}
