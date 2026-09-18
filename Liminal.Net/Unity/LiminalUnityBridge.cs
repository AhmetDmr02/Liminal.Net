#if UNITY_5_3_OR_NEWER || UNITY_ENGINE
using UnityEngine;
#endif
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
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

        [Header("Packet Polling Configuration")]
        [SerializeField] private bool _pollInFixedUpdate = true;
        [SerializeField] private bool _pollInUpdate = false;

        public bool PollInFixedUpdate
        {
            get => _pollInFixedUpdate;
            set => _pollInFixedUpdate = value;
        }

        public bool PollInUpdate
        {
            get => _pollInUpdate;
            set => _pollInUpdate = value;
        }
#else
    public class LiminalUnityBridge : IDisposable
    {
        public static LiminalUnityBridge Instance { get; set; }
        public bool PollInFixedUpdate { get; set; } = true;
        public bool PollInUpdate { get; set; } = false;
#endif

        #region Main Thread Events - LiminalNetworkManager
        public event Action<NetworkLifecycleState> OnLifecycleStateChanged;
        public event Action OnConnectedAndReady;
        #endregion

        #region Main Thread Events - LiminalEventHub
        public event Action<ushort> OnClientConnected;
        public event Action<ushort> OnClientDisconnected;
        public event Action<ushort> OnLocalClientConnected;
        public event Action<ushort> OnLocalClientDisconnected;
        public event Action<ushort> OnClientKicked;
        public event Action OnServerStarted;
        public event Action OnTransportShutdown;
        public event Action<ushort, DisconnectReason, string> OnDisconnectResolved;
        #endregion

        #region Main Thread Events - ClientRegistry
        public event Action<ConnectedClient> OnClientJoined;
        public event Action<ConnectedClient> OnClientLeft;
        public event Action<ConnectedClient> OnLocalClientReady;
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
                DontDestroyOnLoad(gameObject);
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

            if (_pollInUpdate)
            {
                PollPackets();
            }
        }

        private void FixedUpdate()
        {
            if (_pollInFixedUpdate)
            {
                PollPackets();
            }
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
            if (PollInUpdate) PollPackets();
        }

        public void FixedUpdate()
        {
            if (PollInFixedUpdate) PollPackets();
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
                        OnLifecycleStateChanged?.Invoke(ev.LifecycleState);
                        break;
                    case UnityNetworkEventType.ConnectedAndReady:
                        OnConnectedAndReady?.Invoke();
                        break;
                    case UnityNetworkEventType.ClientConnected:
                        OnClientConnected?.Invoke(ev.ClientId);
                        break;
                    case UnityNetworkEventType.ClientDisconnected:
                        OnClientDisconnected?.Invoke(ev.ClientId);
                        break;
                    case UnityNetworkEventType.LocalClientConnected:
                        OnLocalClientConnected?.Invoke(ev.ClientId);
                        break;
                    case UnityNetworkEventType.LocalClientDisconnected:
                        OnLocalClientDisconnected?.Invoke(ev.ClientId);
                        break;
                    case UnityNetworkEventType.ClientKicked:
                        OnClientKicked?.Invoke(ev.ClientId);
                        break;
                    case UnityNetworkEventType.ServerStarted:
                        OnServerStarted?.Invoke();
                        break;
                    case UnityNetworkEventType.TransportShutdown:
                        OnTransportShutdown?.Invoke();
                        break;
                    case UnityNetworkEventType.DisconnectResolved:
                        OnDisconnectResolved?.Invoke(ev.ClientId, ev.DisconnectReason, ev.DisconnectMessage);
                        break;
                    case UnityNetworkEventType.ClientJoined:
                        OnClientJoined?.Invoke(ev.Client);
                        break;
                    case UnityNetworkEventType.ClientLeft:
                        OnClientLeft?.Invoke(ev.Client);
                        break;
                    case UnityNetworkEventType.LocalClientReady:
                        OnLocalClientReady?.Invoke(ev.Client);
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
