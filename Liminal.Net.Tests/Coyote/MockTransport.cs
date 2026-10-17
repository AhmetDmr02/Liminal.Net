using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Tests
{
    public class MockTransport : ILiminalTransport
    {
        public struct PacketRecord
        {
            public byte[] Data;
            public ushort TargetId;
            public TransportFlags Flags;
        }

        public class ThreadSafeSet<T> : IEnumerable<T> where T : notnull
        {
            private readonly System.Collections.Concurrent.ConcurrentDictionary<T, byte> _set = new();

            public bool Add(T item) => _set.TryAdd(item, 0);

            public bool Remove(T item) => _set.TryRemove(item, out _);

            public bool Contains(T item) => _set.ContainsKey(item);

            public void Clear() => _set.Clear();

            public int Count => _set.Count;

            public IEnumerator<T> GetEnumerator() => _set.Keys.GetEnumerator();

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public class ThreadSafeList<T> : IEnumerable<T>
        {
            private readonly ConcurrentQueue<T> _items = new();

            public void Add(T item) => _items.Enqueue(item);

            public void Clear() => _items.Clear();

            public int Count => _items.Count;

            public T this[int index] => _items.ToArray()[index];

            public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        //View over the mock's connections, adding a client opens its connection and removing one closes it in the same step
        //the local client's own connection to the server is not listed, like a real server's client list
        public sealed class ConnectionSet : IEnumerable<ushort>
        {
            private readonly MockTransport _owner;

            internal ConnectionSet(MockTransport owner) => _owner = owner;

            public bool Add(ushort clientId) => clientId != ILiminalTransport.SERVER_ID && _owner.OpenIfAbsent(clientId);

            public bool Remove(ushort clientId) => clientId != ILiminalTransport.SERVER_ID && _owner.Close(clientId).IsValid;

            public bool Contains(ushort clientId) => clientId != ILiminalTransport.SERVER_ID && _owner._connections.ContainsKey(clientId);

            public int Count
            {
                get
                {
                    int count = 0;
                    foreach (var clientId in this) count++;
                    return count;
                }
            }

            public IEnumerator<ushort> GetEnumerator()
            {
                foreach (var clientId in _owner._connections.Keys)
                {
                    if (clientId != ILiminalTransport.SERVER_ID)
                        yield return clientId;
                }
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public readonly ThreadSafeList<PacketRecord> SentPackets = new();
        public readonly ConnectionSet ConnectedClients;
        public readonly ThreadSafeSet<ushort> KickedClients = new();

        public MockTransport()
        {
            ConnectedClients = new ConnectionSet(this);
        }

        public ushort LocalClientId { get; set; } = 1;
        public bool IsServer { get; set; } = true;
        public bool IsClient => !IsServer;
        public bool IsConnected { get; set; } = true;
        public int ConnectedClientCount => ConnectedClients.Count > 0 ? ConnectedClients.Count : 1;

        public LiminalNetworkConfig Config => _conf;

        public event DataReceivedHandler? OnMessageReceivedReliable;
        public event DataReceivedHandler? OnMessageReceivedUnreliable;
        public event DataReceivedHandler? OnMessageReceivedFragmented;

        public event TransportEventHandler? OnServerStarted;
        public event TransportEventHandler? OnShutdown;
        public event TransportEventHandler? OnHandshakeInitialized;

        public event LocalConnectionHandler? OnLocalClientConnected;
        public event LocalConnectionHandler? OnLocalClientDisconnected;
        public event ClientConnectionHandler? OnClientConnected;
        public event ClientConnectionHandler? OnClientDisconnected;
        public event ClientConnectionHandler? OnClientKicked;
        public event Action<LiminalConnection, DisconnectReason, string>? OnTransportDisconnectReason;

        public void TriggerTransportDisconnectReason(ushort id, DisconnectReason reason, string message) => OnTransportDisconnectReason?.Invoke(CurrentOrStale(id), reason, message);

        public Action<byte[], ushort, TransportFlags>? OnSendHook;

        private LiminalNetworkConfig _conf;

        public void InitializeTransport(LiminalNetworkConfig config)
        {
            _conf = config;
        }

        public void StartServer(string ip, int port) => OnServerStarted?.Invoke();
        public void StartClient(string ip, int port) { }
        //New generation per connection so tests can tell a reconnect under the same client id from the old connection
        //connections change and scoped sends decide under _gate, as atomic as TcpTransport so the mock adds no races of its own
        private readonly ConcurrentDictionary<ushort, uint> _connections = new();
        private readonly object _gate = new();
        private int _lastConnectionId;

        private uint NextConnectionId() => (uint)Interlocked.Increment(ref _lastConnectionId);

        private LiminalConnection Open(ushort clientId, ushort key)
        {
            lock (_gate)
            {
                uint generation = NextConnectionId();
                _connections[key] = generation;
                return new LiminalConnection(key, generation);
            }
        }

        private bool OpenIfAbsent(ushort clientId)
        {
            lock (_gate)
            {
                return _connections.TryAdd(clientId, NextConnectionId());
            }
        }

        private LiminalConnection Close(ushort key)
        {
            lock (_gate)
            {
                return _connections.TryRemove(key, out var generation) ? new LiminalConnection(key, generation) : new LiminalConnection(key, 0);
            }
        }

        private bool CloseIfCurrent(LiminalConnection connection)
        {
            lock (_gate)
            {
                return connection.IsValid &&
                       ((ICollection<KeyValuePair<ushort, uint>>)_connections).Remove(new KeyValuePair<ushort, uint>(connection.PeerId, connection.Generation));
            }
        }

        //Events raised for an id with no connection carry generation 0 so they match nothing
        private LiminalConnection CurrentOrStale(ushort clientId)
        {
            var current = GetConnection(clientId);
            return current.IsValid ? current : new LiminalConnection(clientId, 0);
        }

        //Like a real transport every connection is gone once it shuts down
        public void Disconnect()
        {
            lock (_gate) _connections.Clear();
            OnShutdown?.Invoke();
        }

        public void Disconnect(LiminalConnection server)
        {
            if (!CloseIfCurrent(server))
                return;

            Disconnect();
        }

        public void Shutdown()
        {
            lock (_gate) _connections.Clear();
            OnShutdown?.Invoke();
        }

        public LiminalConnection GetConnection(ushort clientId)
        {
            if (IsClientConnectedFunc != null && !IsClientConnectedFunc(clientId))
                return default;

            var connection = _connections.TryGetValue(clientId, out var generation) ? new LiminalConnection(clientId, generation) : default;
            OnGetConnection?.Invoke(connection);
            return connection;
        }

        //Sees exactly what GetConnection handed back, lets a test know which connection a caller snapshotted
        public Action<LiminalConnection>? OnGetConnection;

        public void Kick(LiminalConnection client)
        {
            if (!CloseIfCurrent(client))
                return;

            KickedClients.Add(client.PeerId);
            OnClientKicked?.Invoke(client);
        }

        public void Send(Span<byte> data, LiminalConnection to, TransportFlags flags)
        {
            byte[] copy = data.ToArray();
            lock (_gate)
            {
                //Like TcpTransport, data for a connection that is no longer current never goes out
                if (!to.IsValid || !_connections.TryGetValue(to.PeerId, out var generation) || generation != to.Generation)
                    return;

                SentPackets.Add(new PacketRecord
                {
                    Data = copy,
                    TargetId = to.PeerId,
                    Flags = flags
                });
                OnSendHook?.Invoke(copy, to.PeerId, flags);
            }
        }

        public void Kick(ushort clientId)
        {
            var kicked = Close(clientId);
            KickedClients.Add(clientId);
            OnClientKicked?.Invoke(kicked);
        }

        public Func<ushort, bool>? IsClientConnectedFunc;
        public bool IsClientConnected(ushort clientId)
        {
            if (IsClientConnectedFunc != null) return IsClientConnectedFunc(clientId);
            return ConnectedClients.Contains(clientId);
        }

        #region Test Injection Triggers

        public void TriggerClientConnected(ushort id)
        {
            var connection = Open(id, id);
            OnClientConnected?.Invoke(connection);
        }

        public void TriggerClientDisconnected(ushort id)
        {
            var connection = Close(id);
            OnClientDisconnected?.Invoke(connection);
        }

        //Raises a disconnect for a connection the test kept, like an old socket's reader finishing after a reconnect
        public void TriggerClientDisconnected(LiminalConnection connection)
        {
            CloseIfCurrent(connection);
            OnClientDisconnected?.Invoke(connection);
        }

        public void TriggerClientKicked(ushort id)
        {
            Kick(id);
        }

        public void TriggerLocalClientConnected(ushort id)
        {
            var server = Open(id, ILiminalTransport.SERVER_ID);
            OnLocalClientConnected?.Invoke(id, server);
        }

        public void TriggerLocalClientDisconnected(ushort id)
        {
            var server = Close(ILiminalTransport.SERVER_ID);
            OnLocalClientDisconnected?.Invoke(id, server.IsValid ? server : default);
        }

        public void TriggerMessageReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, CurrentOrStale(id));
        public void TriggerReliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, CurrentOrStale(id));
        public void TriggerReliableReceived(ReadOnlySpan<byte> data, LiminalConnection from) => OnMessageReceivedReliable?.Invoke(data, from);
        public void TriggerUnreliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedUnreliable?.Invoke(data, CurrentOrStale(id));
        public void TriggerFragmentReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedFragmented?.Invoke(data, CurrentOrStale(id));

        public void TriggerShutdown() => OnShutdown?.Invoke();

        #endregion
    }
}