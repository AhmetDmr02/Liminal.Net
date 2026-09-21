using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Collections.Generic;

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

        public class ThreadSafeSet<T> : IEnumerable<T>
        {
            private readonly HashSet<T> _set = new();
            private readonly object _lock = new();

            public bool Add(T item)
            {
                lock (_lock) return _set.Add(item);
            }

            public bool Remove(T item)
            {
                lock (_lock) return _set.Remove(item);
            }

            public bool Contains(T item)
            {
                lock (_lock) return _set.Contains(item);
            }

            public void Clear()
            {
                lock (_lock) _set.Clear();
            }

            public int Count
            {
                get { lock (_lock) return _set.Count; }
            }

            public IEnumerator<T> GetEnumerator()
            {
                lock (_lock) return new List<T>(_set).GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public class ThreadSafeList<T> : IEnumerable<T>
        {
            private readonly List<T> _list = new();
            private readonly object _lock = new();

            public void Add(T item)
            {
                lock (_lock) _list.Add(item);
            }

            public void Clear()
            {
                lock (_lock) _list.Clear();
            }

            public int Count
            {
                get { lock (_lock) return _list.Count; }
            }

            public T this[int index]
            {
                get { lock (_lock) return _list[index]; }
            }

            public IEnumerator<T> GetEnumerator()
            {
                lock (_lock) return new List<T>(_list).GetEnumerator();
            }

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

        public Action<byte[], ushort, TransportFlags>? OnSendHook;

        private LiminalNetworkConfig _conf;

        public void InitializeTransport(LiminalNetworkConfig config)
        {
            _conf = config;
        }

        public void StartServer(string ip, int port) => OnServerStarted?.Invoke();
        public void StartClient(string ip, int port) { }
        public void Disconnect() { }
        public void Shutdown() => OnShutdown?.Invoke();

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
            KickedClients.Add(clientId);
            ConnectedClients.Remove(clientId);
            OnClientKicked?.Invoke(clientId);
        }

        public Func<ushort, bool>? IsClientConnectedFunc;
        public bool IsClientConnected(ushort clientId)
        {
            if (IsClientConnectedFunc != null) return IsClientConnectedFunc(clientId);
            if (ConnectedClients.Count > 0) return ConnectedClients.Contains(clientId);
            return true;
        }

        #region Test Injection Triggers

        public void TriggerClientConnected(ushort id)
        {
            ConnectedClients.Add(id);
            OnClientConnected?.Invoke(id);
        }

        public void TriggerClientDisconnected(ushort id)
        {
            ConnectedClients.Remove(id);
            OnClientDisconnected?.Invoke(id);
        }

        public void TriggerClientKicked(ushort id)
        {
            Kick(id);
        }

        public void TriggerLocalClientConnected(ushort id) => OnLocalClientConnected?.Invoke(id);
        public void TriggerLocalClientDisconnected(ushort id) => OnLocalClientDisconnected?.Invoke(id);

        public void TriggerMessageReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerReliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerUnreliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedUnreliable?.Invoke(data, id);
        public void TriggerFragmentReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedFragmented?.Invoke(data, id);

        public void TriggerShutdown() => OnShutdown?.Invoke();

        #endregion
    }
}