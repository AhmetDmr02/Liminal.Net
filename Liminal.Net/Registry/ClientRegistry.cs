using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Liminal.Net.Registry
{
    public class ClientRegistry
    {
        private readonly LiminalNetworkManager _manager;
        private readonly ConcurrentDictionary<ushort, ConnectedClient> _clients = new();
        private readonly object _rosterLock = new();

        private int _rosterVersion;

        public ICollection<ConnectedClient> AllClients => _clients.Values;
        public int Count => _clients.Count;

        private ConnectedClient _localClient;
        public ConnectedClient LocalClient
        {
            get => Volatile.Read(ref _localClient);
            private set => Volatile.Write(ref _localClient, value);
        }

        public ConnectedClient HostClient => HostClientId is { } id && _clients.TryGetValue(id, out var host) ? host : null;

        private int _hostClientId;
        public ushort? HostClientId
        {
            get
            {
                int val = Volatile.Read(ref _hostClientId);
                return val == 0 ? null : (ushort)val;
            }
        }

        public bool IsDedicatedServer => Volatile.Read(ref _hostClientId) == 0;
        public uint RosterVersion => (uint)Volatile.Read(ref _rosterVersion);

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

        public ClientRegistry(LiminalNetworkManager manager)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            Attach();
        }

        public void Reset()
        {
            lock (_rosterLock)
            {
                _clients.Clear();
                LocalClient = null;
                Interlocked.Exchange(ref _hostClientId, 0);
                Volatile.Write(ref _rosterVersion, 0);
            }
        }

        public void Attach()
        {
            Detach();

            _manager.Events.OnClientConnected += HandleServerClientConnected;
            _manager.Events.OnClientDisconnected += HandleServerClientDisconnected;
            _manager.Events.OnLocalClientConnected += HandleLocalClientConnected;
            _manager.Events.OnLocalClientDisconnected += HandleLocalClientDisconnected;

            _manager.Interpreter.Subscribe<ClientRosterSnapshotPacket>(HandleRosterSnapshot, this);
            _manager.Interpreter.Subscribe<ClientJoinedNotificationPacket>(HandleRemoteClientJoined, this);
            _manager.Interpreter.Subscribe<ClientLeftNotificationPacket>(HandleRemoteClientLeft, this);
            _manager.Interpreter.Subscribe<ClientRosterRequestPacket>(HandleServerRosterRequest, this);
        }

        public void Detach()
        {
            _manager.Events.OnClientConnected -= HandleServerClientConnected;
            _manager.Events.OnClientDisconnected -= HandleServerClientDisconnected;
            _manager.Events.OnLocalClientConnected -= HandleLocalClientConnected;
            _manager.Events.OnLocalClientDisconnected -= HandleLocalClientDisconnected;

            _manager.Interpreter.UnsubscribeAll(this);

            Reset();
        }

        #region Server Side
        private void HandleServerClientConnected(ushort clientId)
        {
            if (!_manager.IsServer) return;

            bool isHost = _manager.Role == NetworkRole.Host && clientId == _manager.localID;
            if (isHost)
            {
                Interlocked.Exchange(ref _hostClientId, clientId);
            }

            var client = new ConnectedClient(clientId, isLocal: isHost, isHost: isHost);
            bool added;

            lock (_rosterLock)
            {
                added = _clients.TryAdd(clientId, client);
                if (added)
                {
                    uint newVersion = (uint)Interlocked.Increment(ref _rosterVersion);
                    if (isHost) LocalClient = client;

                    if (!isHost)
                    {
                        ushort hostId = HostClientId ?? 0;
                        ushort[] currentRoster = _clients.Keys.ToArray();

                        _manager.Interpreter.SendCommandAsServer(clientId, new ClientRosterSnapshotPacket
                        {
                            RosterVersion = newVersion,
                            HostClientId = hostId,
                            ConnectedClientIds = currentRoster
                        }, DeliveryMethod.Reliable);

                        foreach (var otherId in _clients.Keys)
                        {
                            if (otherId != clientId && otherId != ILiminalTransport.SERVER_ID)
                            {
                                _manager.Interpreter.SendCommandAsServer(otherId, new ClientJoinedNotificationPacket
                                {
                                    RosterVersion = newVersion,
                                    ClientId = clientId
                                }, DeliveryMethod.Reliable);
                            }
                        }
                    }
                }
            }

            if (added)
            {
                LiminalAtomicHelpers.SafeInvoke(_onClientJoined, client);
            }
        }

        private void HandleServerClientDisconnected(ushort clientId)
        {
            if (!_manager.IsServer) return;

            ConnectedClient removedClient = null;

            lock (_rosterLock)
            {
                if (_clients.TryRemove(clientId, out removedClient))
                {
                    uint newVersion = (uint)Interlocked.Increment(ref _rosterVersion);
                    if (Volatile.Read(ref _hostClientId) == clientId)
                    {
                        Interlocked.Exchange(ref _hostClientId, 0);
                    }

                    foreach (var remainingId in _clients.Keys)
                    {
                        if (remainingId != clientId && remainingId != ILiminalTransport.SERVER_ID)
                        {
                            _manager.Interpreter.SendCommandAsServer(remainingId, new ClientLeftNotificationPacket
                            {
                                RosterVersion = newVersion,
                                ClientId = clientId
                            }, DeliveryMethod.Reliable);
                        }
                    }
                }
            }

            if (removedClient != null)
            {
                LiminalAtomicHelpers.SafeInvoke(_onClientLeft, removedClient);
            }
        }

        private void HandleServerRosterRequest(ClientRosterRequestPacket packet, ushort senderId)
        {
            if (!_manager.IsServer) return;

            lock (_rosterLock)
            {
                ushort hostId = HostClientId ?? 0;
                ushort[] currentRoster = _clients.Keys.ToArray();
                uint currentVersion = (uint)Volatile.Read(ref _rosterVersion);

                _manager.Interpreter.SendCommandAsServer(senderId, new ClientRosterSnapshotPacket
                {
                    RosterVersion = currentVersion,
                    HostClientId = hostId,
                    ConnectedClientIds = currentRoster
                }, DeliveryMethod.Reliable);
            }
        }
        #endregion

        #region Client Side
        private void HandleLocalClientConnected(ushort localId)
        {
            if (_manager.Role == NetworkRole.Host)
            {
                Interlocked.Exchange(ref _hostClientId, localId);
                ConnectedClient hostClientJoined = null;
                ConnectedClient readyClient = null;

                lock (_rosterLock)
                {
                    if (_clients.TryGetValue(localId, out var existing))
                    {
                        if (!existing.IsHost || !existing.IsLocal)
                        {
                            var updated = new ConnectedClient(localId, isLocal: true, isHost: true);
                            _clients[localId] = updated;
                            LocalClient = updated;
                        }
                        else
                        {
                            LocalClient = existing;
                        }
                    }
                    else
                    {
                        var hostClient = new ConnectedClient(localId, isLocal: true, isHost: true);
                        _clients.TryAdd(localId, hostClient);
                        LocalClient = hostClient;
                        hostClientJoined = hostClient;
                    }

                    readyClient = LocalClient;
                }

                if (hostClientJoined != null)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onClientJoined, hostClientJoined);
                }

                LiminalAtomicHelpers.SafeInvoke(_onLocalClientReady, readyClient);
                return;
            }

            var localClient = new ConnectedClient(localId, isLocal: true, isHost: false);
            LocalClient = localClient;
            bool addedLocal;

            lock (_rosterLock)
            {
                addedLocal = _clients.TryAdd(localId, localClient);
            }

            if (addedLocal)
            {
                LiminalAtomicHelpers.SafeInvoke(_onClientJoined, localClient);
            }

            LiminalAtomicHelpers.SafeInvoke(_onLocalClientReady, localClient);
        }

        private void HandleLocalClientDisconnected(ushort localId)
        {
            Reset();
        }

        private void HandleRosterSnapshot(ClientRosterSnapshotPacket packet, ushort senderId)
        {
            if (_manager.Role != NetworkRole.Client) return;

            List<ConnectedClient> leftClients = null;
            List<ConnectedClient> joinedClients = null;

            lock (_rosterLock)
            {
                uint currentVersion = (uint)Volatile.Read(ref _rosterVersion);
                if (packet.RosterVersion < currentVersion)
                {
                    return;
                }

                Volatile.Write(ref _rosterVersion, (int)packet.RosterVersion);
                Interlocked.Exchange(ref _hostClientId, packet.HostClientId);

                var snapshotSet = new HashSet<ushort>(packet.ConnectedClientIds);
                foreach (var kvp in _clients)
                {
                    if (!snapshotSet.Contains(kvp.Key))
                    {
                        if (_clients.TryRemove(kvp.Key, out var removed))
                        {
                            leftClients ??= new List<ConnectedClient>();
                            leftClients.Add(removed);
                        }
                    }
                }

                foreach (var id in packet.ConnectedClientIds)
                {
                    bool isLocal = id == _manager.localID;
                    bool isHost = HostClientId.HasValue && id == HostClientId.Value;

                    if (_clients.TryGetValue(id, out var existing))
                    {
                        if (existing.IsHost != isHost || existing.IsLocal != isLocal)
                        {
                            var updated = new ConnectedClient(id, isLocal, isHost);
                            _clients[id] = updated;
                            if (isLocal) LocalClient = updated;
                        }
                    }
                    else
                    {
                        var newClient = new ConnectedClient(id, isLocal, isHost);
                        if (_clients.TryAdd(id, newClient))
                        {
                            if (isLocal) LocalClient = newClient;
                            joinedClients ??= new List<ConnectedClient>();
                            joinedClients.Add(newClient);
                        }
                    }
                }
            }

            if (leftClients != null)
            {
                for (int i = 0; i < leftClients.Count; i++)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onClientLeft, leftClients[i]);
                }
            }

            if (joinedClients != null)
            {
                for (int i = 0; i < joinedClients.Count; i++)
                {
                    LiminalAtomicHelpers.SafeInvoke(_onClientJoined, joinedClients[i]);
                }
            }
        }

        private void HandleRemoteClientJoined(ClientJoinedNotificationPacket packet, ushort senderId)
        {
            if (_manager.Role != NetworkRole.Client) return;

            ConnectedClient joinedClient = null;
            bool requestReconcile = false;

            lock (_rosterLock)
            {
                uint currentVersion = (uint)Volatile.Read(ref _rosterVersion);
                if (packet.RosterVersion <= currentVersion)
                {
                    return;
                }

                if (packet.RosterVersion > currentVersion + 1)
                {
                    requestReconcile = true;
                }
                else
                {
                    Volatile.Write(ref _rosterVersion, (int)packet.RosterVersion);
                    bool isHost = HostClientId.HasValue && packet.ClientId == HostClientId.Value;
                    bool isLocal = packet.ClientId == _manager.localID;

                    var client = new ConnectedClient(packet.ClientId, isLocal, isHost);
                    if (_clients.TryAdd(packet.ClientId, client))
                    {
                        if (isLocal) LocalClient = client;
                        joinedClient = client;
                    }
                }
            }

            if (requestReconcile)
            {
                RequestRosterReconciliation();
                return;
            }

            if (joinedClient != null)
            {
                LiminalAtomicHelpers.SafeInvoke(_onClientJoined, joinedClient);
            }
        }

        private void HandleRemoteClientLeft(ClientLeftNotificationPacket packet, ushort senderId)
        {
            if (_manager.Role != NetworkRole.Client) return;

            ConnectedClient leftClient = null;
            bool requestReconcile = false;

            lock (_rosterLock)
            {
                uint currentVersion = (uint)Volatile.Read(ref _rosterVersion);
                if (packet.RosterVersion <= currentVersion)
                {
                    return;
                }

                if (packet.RosterVersion > currentVersion + 1)
                {
                    requestReconcile = true;
                }
                else
                {
                    Volatile.Write(ref _rosterVersion, (int)packet.RosterVersion);
                    if (_clients.TryRemove(packet.ClientId, out var client))
                    {
                        leftClient = client;
                    }
                }
            }

            if (requestReconcile)
            {
                RequestRosterReconciliation();
                return;
            }

            if (leftClient != null)
            {
                LiminalAtomicHelpers.SafeInvoke(_onClientLeft, leftClient);
            }
        }

        public void RequestRosterReconciliation()
        {
            if (_manager.Role == NetworkRole.Client && _manager.CanSend)
            {
                _manager.Interpreter.SendCommandAsClient(ILiminalTransport.SERVER_ID, new ClientRosterRequestPacket
                {
                    LastKnownVersion = RosterVersion
                }, DeliveryMethod.Reliable);
            }
        }
        #endregion

        #region Public Queries
        public bool TryGetClient(ushort clientId, out ConnectedClient client) => _clients.TryGetValue(clientId, out client);
        public bool ContainsClient(ushort clientId) => _clients.ContainsKey(clientId);
        #endregion
    }
}
