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

        public readonly ThreadSafeList<PacketRecord> SentPackets = new();
        public readonly ThreadSafeSet<ushort> ConnectedClients = new();
        public readonly ThreadSafeSet<ushort> KickedClients = new();

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

        public event ClientConnectionHandler? OnLocalClientConnected;
        public event ClientConnectionHandler? OnLocalClientDisconnected;
        public event ClientConnectionHandler? OnClientConnected;
        public event ClientConnectionHandler? OnClientDisconnected;
        public event ClientConnectionHandler? OnClientKicked;
        public event Action<ushort, DisconnectReason, string>? OnTransportDisconnectReason;

        public void TriggerTransportDisconnectReason(ushort id, DisconnectReason reason, string message) => OnTransportDisconnectReason?.Invoke(id, reason, message);

        public Action<byte[], ushort, TransportFlags>? OnSendHook;

        private LiminalNetworkConfig _conf;

        public void InitializeTransport(LiminalNetworkConfig config)
        {
            _conf = config;
        }

        public void StartServer(string ip, int port) => OnServerStarted?.Invoke();
        public void StartClient(string ip, int port) { }
        //New id per connection so tests can tell a reconnect under the same client id from the old connection
        private readonly ConcurrentDictionary<ushort, uint> _connections = new();
        private int _lastConnectionId;

        private uint NextConnectionId() => (uint)Interlocked.Increment(ref _lastConnectionId);

        //Like a real transport every connection is gone once it shuts down
        public void Disconnect()
        {
            _connections.Clear();
            OnShutdown?.Invoke();
        }

        public void Shutdown()
        {
            _connections.Clear();
            OnShutdown?.Invoke();
        }

        public uint GetConnectionId(ushort clientId) => _connections.TryGetValue(clientId, out var connection) ? connection : 0;

        public void Kick(ushort clientId, uint connectionId)
        {
            if (connectionId == 0 ||
                !((ICollection<KeyValuePair<ushort, uint>>)_connections).Remove(new KeyValuePair<ushort, uint>(clientId, connectionId)))
                return;

            KickedClients.Add(clientId);
            ConnectedClients.Remove(clientId);
            OnClientKicked?.Invoke(clientId);
        }

        public void Send(Span<byte> data, ushort clientId, TransportFlags flags)
        {
            SentPackets.Add(new PacketRecord
            {
                Data = data.ToArray(),
                TargetId = clientId,
                Flags = flags
            });
            OnSendHook?.Invoke(data.ToArray(), clientId, flags);
        }

        public void Kick(ushort clientId)
        {
            _connections.TryRemove(clientId, out _);
            KickedClients.Add(clientId);
            ConnectedClients.Remove(clientId);
            OnClientKicked?.Invoke(clientId);
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
            _connections[id] = NextConnectionId();
            ConnectedClients.Add(id);
            OnClientConnected?.Invoke(id);
        }

        public void TriggerClientDisconnected(ushort id)
        {
            _connections.TryRemove(id, out _);
            ConnectedClients.Remove(id);
            OnClientDisconnected?.Invoke(id);
        }

        public void TriggerClientKicked(ushort id)
        {
            Kick(id);
        }

        public void TriggerLocalClientConnected(ushort id)
        {
            _connections[ILiminalTransport.SERVER_ID] = NextConnectionId();
            OnLocalClientConnected?.Invoke(id);
        }

        public void TriggerLocalClientDisconnected(ushort id)
        {
            _connections.TryRemove(ILiminalTransport.SERVER_ID, out _);
            OnLocalClientDisconnected?.Invoke(id);
        }

        public void TriggerMessageReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerReliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerUnreliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedUnreliable?.Invoke(data, id);
        public void TriggerFragmentReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedFragmented?.Invoke(data, id);

        public void TriggerShutdown() => OnShutdown?.Invoke();

        #endregion
    }
}