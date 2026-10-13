using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    public class SyncVarManager
    {
        private static readonly ConditionalWeakTable<LiminalNetworkManager, SyncVarManager> _managerMap = new();
        private static readonly object _managerMapLock = new();
        private static volatile SyncVarManager _lastCreatedManager;

        public static SyncVarManager Instance => LiminalNetworkManager.Instance?.SyncVarManager ?? Volatile.Read(ref _lastCreatedManager);

        public static SyncVarManager Initialize(LiminalNetworkManager netManager, LiminalNetworkConfig config)
        {
            if (netManager == null) throw new ArgumentNullException(nameof(netManager));

            lock (_managerMapLock)
            {
                if (_managerMap.TryGetValue(netManager, out var existing))
                {
                    existing.Reattach(netManager);
                    Volatile.Write(ref _lastCreatedManager, existing);
                    return existing;
                }

                var newManager = new SyncVarManager(netManager, config);
                _managerMap.Add(netManager, newManager);
                Volatile.Write(ref _lastCreatedManager, newManager);
                return newManager;
            }
        }

        public bool IsTornDown => Volatile.Read(ref _lifecycleGate) == 2;

        internal static SyncVarManager GetManagerForRegistration()
        {
            var manager = LiminalNetworkManager.Instance;
            if (manager?.SyncVarManager != null && !manager.SyncVarManager.IsTornDown)
                return manager.SyncVarManager;

            var last = Volatile.Read(ref _lastCreatedManager);
            if (last != null && !last.IsTornDown)
                return last;

            return null;
        }

        private sealed class UnboundEntry
        {
            public uint Version;
            public byte[] Data;
            public LinkedListNode<string> Node;
        }

        private readonly Utf8TokenRegistry _tokenRegistry = new();
        private readonly object _unboundLock = new();
        private readonly object _bindLock = new();
        private readonly Dictionary<string, UnboundEntry> _unboundCache = new();
        private readonly LinkedList<string> _unboundOrder = new();
        private readonly Dictionary<string, ushort[]> _unboundAuth = new();
        private readonly LockFreeSlotBitset _slotBitset;
        private readonly ISyncVarInternal[] _localRegistry;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<ushort, byte>> _pendingClearAcks = new();
        private readonly List<ISyncVarInternal> _reusableDirtyList = new(64);
        private readonly List<SyncVarVisibilityState> _reusableStateList = new(64);
        private readonly List<ISyncVarInternal> _clientAuthorizedList = new(64);
        private readonly List<ISyncVarInternal> _tailoredVarsList = new(64);
        private readonly int _maxUnboundCapacity;

        [ThreadStatic]
        private static byte[] _reusableTokenBuffer;

        public int MaxUnboundCapacity => _maxUnboundCapacity;
        public int UnboundCacheCount
        {
            get
            {
                lock (_unboundLock) return _unboundCache.Count;
            }
        }
        public int DirtyQueueCount => DirtyBitset.CountDirtyBits();

        public SyncVarSlab Slab { get; private set; }
        public LockFreeDirtyBitset DirtyBitset { get; }
        public LockFreeSlotBitset SlotBitset => _slotBitset;

        private volatile LiminalNetworkManager _attachedManager;
        public LiminalNetworkManager AttachedManager => _attachedManager;
        private int _totalAllocatedBytes;
        private int _hasReceivedInitialSnapshot = 0;

        public readonly MessagePackSerializerOptions SerializerOptions =
            MessagePackSerializer.DefaultOptions.WithSecurity(MessagePackSecurity.UntrustedData);

        public SyncVarManager(LiminalNetworkManager manager, LiminalNetworkConfig config)
        {
            Slab = new SyncVarSlab(config);

            int maxSlots = config?.SyncVarMaxLocalSlots > 0 ? config.SyncVarMaxLocalSlots : 4096;
            DirtyBitset = new LockFreeDirtyBitset(maxSlots);
            _slotBitset = new LockFreeSlotBitset(maxSlots);
            _localRegistry = new ISyncVarInternal[maxSlots];

            int maxPlayers = config?.MaxConnectionCount > 0 ? config.MaxConnectionCount : 32;
            int scale = (config?.Hiccup != null && config.Hiccup.Enabled)
                ? Math.Max(1, config.Hiccup.MaxRecoveryScale)
                : 4;
            const int VarsPerPlayerEstimate = 128;
            _maxUnboundCapacity = Math.Max(maxPlayers * scale * VarsPerPlayerEstimate, 2048);

            if (manager != null)
            {
                AttachToManager(manager);
            }
        }

        public void AttachToManager(LiminalNetworkManager manager)
        {
            if (manager == null) return;
            DetachFromManager();

            _attachedManager = manager;
            Volatile.Write(ref _lifecycleGate, 0);

            _attachedManager.Interpreter.Subscribe<SyncVarSnapshotPacket>(HandleSnapshot, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabBatchPacket>(HandleBatchDelta, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabClientRequestPacket>(HandleClientRequest, this);
            _attachedManager.Interpreter.Subscribe<SyncVarAuthUpdatePacket>(HandleAuthUpdate, this);
            _attachedManager.Interpreter.Subscribe<SyncVarEvictPacket>(HandleEviction, this);
            _attachedManager.Interpreter.Subscribe<SyncVarClearPacket>(HandleClear, this);
            _attachedManager.Interpreter.Subscribe<SyncVarClearAckPacket>(HandleClearAck, this);

            _attachedManager.Events.OnClientConnected += HandleClientConnected;
            _attachedManager.Events.OnClientDisconnected += HandleClientDisconnected;
            _attachedManager.Events.OnLocalClientDisconnected += HandleLocalClientDisconnected;
            _attachedManager.Events.OnManagerShutdown += HandleShutdown;
            _attachedManager.OnPreFlush += FlushDirty;
        }

        public void DetachFromManager()
        {
            if (_attachedManager == null) return;

            try
            {
                _attachedManager.OnPreFlush -= FlushDirty;
                _attachedManager.Events.OnClientConnected -= HandleClientConnected;
                _attachedManager.Events.OnClientDisconnected -= HandleClientDisconnected;
                _attachedManager.Events.OnLocalClientDisconnected -= HandleLocalClientDisconnected;
                _attachedManager.Events.OnManagerShutdown -= HandleShutdown;
                _attachedManager.Interpreter.UnsubscribeAll(this);
            }
            catch { }

            _attachedManager = null;
            ResetSessionState();
        }

        public void Reattach(LiminalNetworkManager manager)
        {
            ResetPendingState();
            AttachToManager(manager);

            var allVars = new List<ISyncVarInternal>();
            _tokenRegistry.GetAll(allVars);
            for (int i = 0; i < allVars.Count; i++)
            {
                var v = allVars[i];
                if (v.LocalSlot == ushort.MaxValue)
                {
                    RegisterSyncVar(v);
                }
            }
        }

        public void ResetPendingState()
        {
            Volatile.Write(ref _hasReceivedInitialSnapshot, 0);
            DirtyBitset.Clear();
            lock (_unboundLock)
            {
                _unboundCache.Clear();
                _unboundOrder.Clear();
                _unboundAuth.Clear();
            }
            _pendingClearAcks.Clear();

            var allVars = new List<ISyncVarInternal>();
            _tokenRegistry.GetAll(allVars);
            for (int i = 0; i < allVars.Count; i++)
            {
                allVars[i].SetLocallyVisible(false);
            }
        }

        public void ResetSessionState()
        {
            ResetPendingState();

            var allVars = new List<ISyncVarInternal>();
            _tokenRegistry.GetAll(allVars);
            for (int i = 0; i < allVars.Count; i++)
            {
                allVars[i].ResetSession();
            }

            for (int i = 0; i < _localRegistry.Length; i++)
            {
                Volatile.Write(ref _localRegistry[i], null);
            }
            _slotBitset.Clear();

            Slab?.Reset();
        }

        public void FullTeardown()
        {
            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref _lifecycleGate, 2, 0) != 0)
            {
                if (Volatile.Read(ref _lifecycleGate) == 2)
                    return;
                spinner.SpinOnce();
            }

            ResetSessionState();
            Interlocked.CompareExchange(ref _lastCreatedManager, null, this);
        }

        public bool UnregisterSyncVar(ISyncVarInternal syncVar)
        {
            if (syncVar == null) return false;
            return UnregisterSyncVar(syncVar.Token);
        }

        public bool UnregisterSyncVar(string token)
        {
            if (string.IsNullOrEmpty(token)) return false;
            lock (_unboundLock)
            {
                if (_unboundCache.Remove(token, out var entry))
                {
                    _unboundOrder.Remove(entry.Node);
                }
                _unboundAuth.Remove(token);
                if (_tokenRegistry.TryRemove(token, out var syncVar))
                {
                    ushort slot = syncVar.LocalSlot;
                    if (slot != ushort.MaxValue)
                    {
                        DirtyBitset.ClearDirty(slot);
                        Volatile.Write(ref _localRegistry[slot], null);
                        _slotBitset.Free(slot);
                        syncVar.LocalSlot = ushort.MaxValue;
                    }
                    return true;
                }
                return false;
            }
        }

        internal bool UnregisterClaimedSyncVar(ISyncVarInternal syncVar, ushort claimedSlot)
        {
            string token = syncVar.Token;
            lock (_unboundLock)
            {
                if (!_tokenRegistry.TryGet(token, out var current) || !ReferenceEquals(current, syncVar))
                {
                    return false;
                }

                if (_unboundCache.Remove(token, out var entry))
                {
                    _unboundOrder.Remove(entry.Node);
                }
                _unboundAuth.Remove(token);
                _tokenRegistry.TryRemove(token, out _);

                if (claimedSlot != ushort.MaxValue && ReferenceEquals(Volatile.Read(ref _localRegistry[claimedSlot]), syncVar))
                {
                    DirtyBitset.ClearDirty(claimedSlot);
                    Volatile.Write(ref _localRegistry[claimedSlot], null);
                    _slotBitset.Free(claimedSlot);
                }
                return true;
            }
        }

        private void HandleClientDisconnected(ushort clientId)
        {
            foreach (var kvp in _pendingClearAcks)
            {
                var pending = kvp.Value;
                lock (pending)
                {
                    pending.TryRemove(clientId, out _);
                    if (pending.IsEmpty)
                    {
                        ((ICollection<KeyValuePair<string, ConcurrentDictionary<ushort, byte>>>)_pendingClearAcks)
                            .Remove(new KeyValuePair<string, ConcurrentDictionary<ushort, byte>>(kvp.Key, pending));
                    }
                }
            }
        }

        private void HandleLocalClientDisconnected(ushort clientId)
        {
            ResetPendingState();
        }

        private void HandleShutdown()
        {
            FullTeardown();
        }

        private void StoreUnboundAuth(string token, ushort[] authIds)
        {
            ISyncVarInternal registered = null;

            lock (_unboundLock)
            {
                if (_tokenRegistry.TryGet(token, out registered))
                {
                }
                else
                {
                    _unboundAuth[token] = authIds;
                }
            }

            if (registered != null)
            {
                registered.ApplyAuthUpdateFromRemote(authIds);
            }
        }

        private void StoreUnboundData(string token, uint version, byte[] data)
        {
            ISyncVarInternal registered = null;

            lock (_unboundLock)
            {
                if (_tokenRegistry.TryGet(token, out registered))
                {
                }
                else if (_unboundCache.TryGetValue(token, out var existing))
                {
                    if (version <= existing.Version && existing.Version != 0)
                    {
                        return;
                    }

                    existing.Version = version;
                    existing.Data = data;
                    _unboundOrder.Remove(existing.Node);
                    _unboundOrder.AddLast(existing.Node);
                }
                else
                {
                    while (_unboundCache.Count >= _maxUnboundCapacity && _unboundOrder.First != null)
                    {
                        string oldest = _unboundOrder.First.Value;
                        _unboundOrder.RemoveFirst();
                        _unboundCache.Remove(oldest);
                        _unboundAuth.Remove(oldest);
                    }

                    var node = _unboundOrder.AddLast(token);
                    _unboundCache[token] = new UnboundEntry { Version = version, Data = data, Node = node };
                }
            }

            if (registered != null)
            {
                registered.ApplyRemoteBytes(new ReadOnlySequence<byte>(data), version, SerializerOptions);
            }
        }

        [ThreadStatic]
        private static ArrayBufferWriter<byte> _regWriter;

        public void AttachPreExisting(ISyncVarInternal syncVar)
        {
            if (syncVar == null) return;
            syncVar.ResetSession();
            RegisterSyncVar(syncVar);
        }

        public void RegisterSyncVar(ISyncVarInternal syncVar)
        {
            if (syncVar == null) throw new ArgumentNullException(nameof(syncVar));

            if (Volatile.Read(ref _lifecycleGate) == 2 || syncVar.IsDisposed)
            {
                return;
            }

            UnboundEntry entry = null;
            ushort[] cachedAuth = null;

            lock (_unboundLock)
            {
                if (Volatile.Read(ref _lifecycleGate) == 2 || syncVar.IsDisposed)
                {
                    return;
                }

                if (syncVar.LocalSlot != ushort.MaxValue && _slotBitset.IsAllocated(syncVar.LocalSlot) && Volatile.Read(ref _localRegistry[syncVar.LocalSlot]) == syncVar)
                {
                    _tokenRegistry.TryAdd(syncVar);
                    return;
                }

                if (_tokenRegistry.TryGet(syncVar.Token, out var existingRegistered) && !ReferenceEquals(existingRegistered, syncVar))
                {
                    LiminalLogger.LogWarning($"[SyncVarManager] SyncVar with token '{syncVar.Token}' is already registered.");
                    return;
                }

                syncVar.AttachManager(this);

                if (!_slotBitset.TryAllocate(out ushort slot))
                {
                    throw new InvalidOperationException($"[SyncVarManager] Exceeded maximum local SyncVar capacity ({_slotBitset.Capacity}).");
                }

                var writer = _regWriter ??= new ArrayBufferWriter<byte>(128);
                writer.Clear();
                syncVar.SerializeInitial(writer, SerializerOptions);

                int size = writer.WrittenCount;
                if (!syncVar.TryInitializeSlabSlots(writer.WrittenSpan))
                {
                    _slotBitset.Free(slot);
                    return;
                }
                Interlocked.Add(ref _totalAllocatedBytes, size * 2);

                if (_unboundAuth.Remove(syncVar.Token, out var auth))
                {
                    cachedAuth = auth;
                }

                if (_unboundCache.Remove(syncVar.Token, out var e))
                {
                    _unboundOrder.Remove(e.Node);
                    entry = e;
                }

                if (cachedAuth != null)
                {
                    syncVar.ApplyAuthUpdateFromRemote(cachedAuth, deferEvent: true);
                }

                if (entry != null)
                {
                    syncVar.SetLocallyVisible(true);
                    bool force = !syncVar.HasAuthority;
                    syncVar.ApplyRemoteBytes(new ReadOnlySequence<byte>(entry.Data), entry.Version, SerializerOptions, force, deferEvent: true);
                }

                _tokenRegistry.TryAdd(syncVar);
                Volatile.Write(ref _localRegistry[slot], syncVar);
                syncVar.LocalSlot = slot;
            }

            syncVar.FirePendingEvents();

            var netManager = _attachedManager;
            if (syncVar.IsDirty || (netManager != null && netManager.Role != NetworkRole.Client && netManager.Role != NetworkRole.None))
            {
                DirtyBitset.SetDirty(syncVar.LocalSlot);
            }
        }

        public SyncVar<T> Bind<T>(string token, T defaultValue = default)
        {
            if (_tokenRegistry.TryGet(token, out var existing))
            {
                var sv = (SyncVar<T>)existing;
                if (sv.LocalSlot == ushort.MaxValue)
                {
                    RegisterSyncVar(sv);
                }
                return sv;
            }

            lock (_bindLock)
            {
                if (_tokenRegistry.TryGet(token, out existing))
                {
                    var sv = (SyncVar<T>)existing;
                    if (sv.LocalSlot == ushort.MaxValue)
                    {
                        RegisterSyncVar(sv);
                    }
                    return sv;
                }

                var created = new SyncVar<T>(token, defaultValue, this);
                if (_tokenRegistry.TryGet(token, out existing) && !ReferenceEquals(existing, created))
                {
                    var sv = (SyncVar<T>)existing;
                    if (sv.LocalSlot == ushort.MaxValue)
                    {
                        RegisterSyncVar(sv);
                    }
                    return sv;
                }

                return created;
            }
        }

        public SyncVar<T> Bind<T>(string token, SyncVarVisibility visibility)
            => Bind<T>(token, default, visibility);

        public SyncVar<T> Bind<T>(string token, T defaultValue, SyncVarVisibility visibility)
        {
            if (_tokenRegistry.TryGet(token, out var existing))
            {
                var sv = (SyncVar<T>)existing;
                if (sv.LocalSlot == ushort.MaxValue)
                {
                    RegisterSyncVar(sv);
                }
                if (_attachedManager == null || _attachedManager.Role != NetworkRole.Client)
                {
                    sv.SetVisibility(visibility);
                }
                return sv;
            }

            lock (_bindLock)
            {
                if (_tokenRegistry.TryGet(token, out existing))
                {
                    var sv = (SyncVar<T>)existing;
                    if (sv.LocalSlot == ushort.MaxValue)
                    {
                        RegisterSyncVar(sv);
                    }
                    if (_attachedManager == null || _attachedManager.Role != NetworkRole.Client)
                    {
                        sv.SetVisibility(visibility);
                    }
                    return sv;
                }

                var created = new SyncVar<T>(token, defaultValue, visibility, this);
                if (_tokenRegistry.TryGet(token, out existing) && !ReferenceEquals(existing, created))
                {
                    var sv = (SyncVar<T>)existing;
                    if (sv.LocalSlot == ushort.MaxValue)
                    {
                        RegisterSyncVar(sv);
                    }
                    if (_attachedManager == null || _attachedManager.Role != NetworkRole.Client)
                    {
                        sv.SetVisibility(visibility);
                    }
                    return sv;
                }

                return created;
            }
        }

        public bool TryGetSyncVar(ReadOnlySpan<byte> tokenSpan, out ISyncVarInternal syncVar)
        {
            return _tokenRegistry.TryGet(tokenSpan, out syncVar);
        }

        public bool TryGetSyncVar(string token, out ISyncVarInternal syncVar)
        {
            return _tokenRegistry.TryGet(token, out syncVar);
        }

        public void EnqueueDirty(ISyncVarInternal syncVar)
        {
            if (syncVar == null) return;
            if (syncVar.LocalSlot != ushort.MaxValue)
            {
                DirtyBitset.SetDirty(syncVar.LocalSlot);
            }
        }

        internal void BroadcastAuthChange(string token, ushort[] newAuthIds)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client || !netManager.CanSend)
                return;

            SendViaManager(SendTo.Everyone, new SyncVarAuthUpdatePacket
            {
                Token = token,
                AuthIds = newAuthIds
            });
        }

        internal void SendSyncVarToClient(ISyncVarInternal syncVar, ushort clientId)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client || !netManager.CanSend)
                return;

            if (netManager.SessionManager != null && !netManager.SessionManager.HasSession(clientId))
            {
                if (!(netManager.Role == NetworkRole.Host && clientId == netManager.localID))
                    return;
            }

            if (!syncVar.TryExtractSnapshotData(out byte[] data, out uint version))
                return;

            var packet = new SyncVarSnapshotPacket
            {
                Entries = new List<SyncVarSnapshotEntry>(1)
                {
                    new SyncVarSnapshotEntry
                    {
                        Token = syncVar.Token,
                        Version = version,
                        AuthIds = syncVar.AuthIds,
                        Data = data
                    }
                }
            };

            if (netManager.Role == NetworkRole.Host && clientId == netManager.localID)
                netManager.Interpreter.SendCommandAsClient(clientId, packet);
            else
                netManager.Interpreter.SendCommandAsServer(clientId, packet);
        }

        internal void SendEvictionToClient(ISyncVarInternal syncVar, ushort clientId)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client || !netManager.CanSend)
                return;

            if (netManager.SessionManager != null && !netManager.SessionManager.HasSession(clientId))
            {
                if (!(netManager.Role == NetworkRole.Host && clientId == netManager.localID))
                    return;
            }

            var packet = new SyncVarEvictPacket(syncVar.Token, syncVar.TokenBytes);

            if (netManager.Role == NetworkRole.Host && clientId == netManager.localID)
                netManager.Interpreter.SendCommandAsClient(clientId, packet);
            else
                netManager.Interpreter.SendCommandAsServer(clientId, packet);
        }

        private static int GetSessionCapacity(LiminalNetworkManager netManager)
        {
            int active = netManager.SessionManager?.GetActiveSessionCount() ?? 0;
            int maxConfig = (netManager.Transport?.Config?.MaxConnectionCount ?? netManager.Config?.MaxConnectionCount ?? 10) + 4;
            return Math.Max(active + 4, maxConfig);
        }

        internal void BroadcastClear(ISyncVarInternal syncVar)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client || !netManager.CanSend)
                return;

            RegisterPendingClear(syncVar.Token);

            var packet = new SyncVarClearPacket(syncVar.Token, syncVar.TokenBytes);

            if (syncVar.Visibility == SyncVarVisibility.Public && syncVar.ExclusionIds.Length == 0)
            {
                SendViaManager(SendTo.Everyone, packet);
                return;
            }

            int maxClients = GetSessionCapacity(netManager);
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = netManager.SessionManager.GetSessionIds(allSessions);

            for (int i = 0; i < totalSessions; i++)
            {
                ushort sid = allSessions[i];
                if (sid == ILiminalTransport.SERVER_ID && netManager.Role == NetworkRole.Host)
                    continue;

                if (syncVar.IsVisibleTo(sid))
                {
                    if (netManager.Role == NetworkRole.Host && sid == netManager.localID)
                        netManager.Interpreter.SendCommandAsClient(sid, packet);
                    else
                        netManager.Interpreter.SendCommandAsServer(sid, packet);
                }
            }
        }

        private void RegisterPendingClear(string token)
        {
            var netManager = _attachedManager;
            if (netManager?.SessionManager == null) return;

            int maxClients = GetSessionCapacity(netManager);
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = netManager.SessionManager.GetSessionIds(allSessions);

            var pending = _pendingClearAcks.GetOrAdd(token, _ => new ConcurrentDictionary<ushort, byte>());
            lock (pending)
            {
                _pendingClearAcks[token] = pending;
                for (int i = 0; i < totalSessions; i++)
                {
                    ushort sid = allSessions[i];
                    if (sid != ILiminalTransport.SERVER_ID)
                    {
                        pending[sid] = 1;
                    }
                }
            }
        }

        private void AcknowledgeClientClear(string token, ushort clientId)
        {
            if (_pendingClearAcks.TryGetValue(token, out var pending))
            {
                lock (pending)
                {
                    pending.TryRemove(clientId, out _);
                    if (pending.IsEmpty)
                    {
                        ((ICollection<KeyValuePair<string, ConcurrentDictionary<ushort, byte>>>)_pendingClearAcks)
                            .Remove(new KeyValuePair<string, ConcurrentDictionary<ushort, byte>>(token, pending));
                    }
                }
            }
        }

        private readonly ArrayBufferWriter<byte> _sharedFlushWriter = new(4096);
        private readonly ArrayBufferWriter<byte> _sharedPayloadWriter = new(4096);
        private readonly ArrayBufferWriter<byte> _tailoredFlushWriter = new(2048);
        private readonly ArrayBufferWriter<byte> _tailoredPayloadWriter = new(2048);
        private int _lifecycleGate = 0;

        public void FlushDirty()
        {
            var netManager = _attachedManager;
            if (netManager == null || !netManager.CanSend)
                return;

            if (netManager.Role == NetworkRole.Client && Volatile.Read(ref _hasReceivedInitialSnapshot) == 0)
                return;

            if (Interlocked.CompareExchange(ref _lifecycleGate, 1, 0) != 0)
                return;

            try
            {
                _reusableDirtyList.Clear();
                _reusableStateList.Clear();

                int bucketCount = DirtyBitset.BucketCount;
                for (int bucket = 0; bucket < bucketCount; bucket++)
                {
                    if (DirtyBitset.IsBucketEmpty(bucket))
                        continue;

                    if (!DirtyBitset.TryConsumeBucket(bucket, out long mask))
                        continue;

                    ulong umask = (ulong)mask;
                    int baseIndex = bucket << 6;

                    while (umask != 0)
                    {
                        int bitPos = BitOperationsCompat.TrailingZeroCount(umask);
                        ushort slot = (ushort)(baseIndex + bitPos);

                        var syncVar = Volatile.Read(ref _localRegistry[slot]);
                        if (syncVar != null && !syncVar.IsDisposed && syncVar.LocalSlot == slot)
                        {
                            _reusableDirtyList.Add(syncVar);
                            _reusableStateList.Add(syncVar.VisibilityState);
                        }

                        umask &= (umask - 1);
                    }
                }

                if (_reusableDirtyList.Count == 0) return;

                if (netManager.Role == NetworkRole.Client)
                {
                    _clientAuthorizedList.Clear();
                    for (int i = 0; i < _reusableDirtyList.Count; i++)
                    {
                        var syncVar = _reusableDirtyList[i];
                        if (syncVar.HasAuthority)
                        {
                            _clientAuthorizedList.Add(syncVar);
                        }
                    }

                    if (_clientAuthorizedList.Count == 0) return;

                    _sharedPayloadWriter.Clear();
                    var payloadWriter = new MessagePackWriter(_sharedPayloadWriter);
                    int writtenCount = 0;

                    for (int i = 0; i < _clientAuthorizedList.Count; i++)
                    {
                        var syncVar = _clientAuthorizedList[i];
                        if (syncVar.TryWriteSlotForWire(ref payloadWriter, out _))
                        {
                            writtenCount++;
                        }
                        else
                        {
                            ushort slot = syncVar.LocalSlot;
                            if (slot != ushort.MaxValue && !syncVar.IsDisposed && _slotBitset.IsAllocated(slot) && Volatile.Read(ref _localRegistry[slot]) == syncVar)
                            {
                                DirtyBitset.SetDirty(slot);
                            }
                        }
                    }

                    payloadWriter.Flush();
                    if (writtenCount == 0) return;

                    _sharedFlushWriter.Clear();
                    var headerWriter = new MessagePackWriter(_sharedFlushWriter);
                    headerWriter.WriteArrayHeader(writtenCount);
                    headerWriter.WriteRaw(_sharedPayloadWriter.WrittenSpan);
                    headerWriter.Flush();

                    var sequence = new ReadOnlySequence<byte>(_sharedFlushWriter.WrittenMemory);
                    netManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarSlabClientRequestPacket(sequence));
                    return;
                }

                // Server / Host Flush
                _sharedPayloadWriter.Clear();
                var masterPayloadWriter = new MessagePackWriter(_sharedPayloadWriter);
                int masterWrittenCount = 0;

                bool requiresTargeting = false;
                for (int i = 0; i < _reusableDirtyList.Count; i++)
                {
                    var state = _reusableStateList[i];
                    if (state.Mode != SyncVarVisibility.Public || state.Exclusions.Length > 0)
                    {
                        requiresTargeting = true;
                    }

                    var sv = _reusableDirtyList[i];
                    if (sv.TryWriteSlotForWire(ref masterPayloadWriter, out _))
                    {
                        masterWrittenCount++;
                    }
                    else
                    {
                        ushort slot = sv.LocalSlot;
                        if (slot != ushort.MaxValue && !sv.IsDisposed && _slotBitset.IsAllocated(slot) && Volatile.Read(ref _localRegistry[slot]) == sv)
                        {
                            DirtyBitset.SetDirty(slot);
                        }
                    }
                }

                masterPayloadWriter.Flush();
                if (masterWrittenCount == 0) return;

                _sharedFlushWriter.Clear();
                var masterHeaderWriter = new MessagePackWriter(_sharedFlushWriter);
                masterHeaderWriter.WriteArrayHeader(masterWrittenCount);
                masterHeaderWriter.WriteRaw(_sharedPayloadWriter.WrittenSpan);
                masterHeaderWriter.Flush();

                var masterPacket = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(_sharedFlushWriter.WrittenMemory));

                if (!requiresTargeting)
                {
                    SendViaManager(SendTo.Everyone, masterPacket);
                    return;
                }

                int maxClients = GetSessionCapacity(netManager);
                Span<ushort> allSessions = stackalloc ushort[maxClients];
                int totalSessions = netManager.SessionManager.GetSessionIds(allSessions);

                Span<ushort> unexcludedTargets = stackalloc ushort[totalSessions];
                int unexcludedCount = 0;

                for (int s = 0; s < totalSessions; s++)
                {
                    ushort targetId = allSessions[s];
                    if (targetId == ILiminalTransport.SERVER_ID && netManager.Role == NetworkRole.Host)
                        continue;

                    bool clientSeesAll = true;
                    for (int i = 0; i < _reusableDirtyList.Count; i++)
                    {
                        if (!_reusableStateList[i].IsVisibleTo(targetId, netManager))
                        {
                            clientSeesAll = false;
                            break;
                        }
                    }

                    if (clientSeesAll)
                    {
                        unexcludedTargets[unexcludedCount++] = targetId;
                    }
                    else
                    {
                        _tailoredVarsList.Clear();
                        for (int i = 0; i < _reusableDirtyList.Count; i++)
                        {
                            if (_reusableStateList[i].IsVisibleTo(targetId, netManager))
                                _tailoredVarsList.Add(_reusableDirtyList[i]);
                        }

                        _tailoredPayloadWriter.Clear();
                        var tailoredPayloadWriter = new MessagePackWriter(_tailoredPayloadWriter);
                        int tailoredWrittenCount = 0;

                        for (int i = 0; i < _tailoredVarsList.Count; i++)
                        {
                            var sv = _tailoredVarsList[i];
                            if (sv.TryWriteSlotForWire(ref tailoredPayloadWriter, out _))
                            {
                                tailoredWrittenCount++;
                            }
                            else
                            {
                                ushort slot = sv.LocalSlot;
                                if (slot != ushort.MaxValue && !sv.IsDisposed && _slotBitset.IsAllocated(slot) && Volatile.Read(ref _localRegistry[slot]) == sv)
                                {
                                    DirtyBitset.SetDirty(slot);
                                }
                            }
                        }

                        tailoredPayloadWriter.Flush();
                        if (tailoredWrittenCount == 0) continue;

                        _tailoredFlushWriter.Clear();
                        var tailoredHeaderWriter = new MessagePackWriter(_tailoredFlushWriter);
                        tailoredHeaderWriter.WriteArrayHeader(tailoredWrittenCount);
                        tailoredHeaderWriter.WriteRaw(_tailoredPayloadWriter.WrittenSpan);
                        tailoredHeaderWriter.Flush();

                        var tailoredPacket = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(_tailoredFlushWriter.WrittenMemory));

                        if (netManager.Role == NetworkRole.Host && targetId == netManager.localID)
                            netManager.Interpreter.SendCommandAsClient(targetId, tailoredPacket);
                        else
                            netManager.Interpreter.SendCommandAsServer(targetId, tailoredPacket);
                    }
                }

                if (unexcludedCount > 0)
                {
                    var cleanTargetsSlice = unexcludedTargets.Slice(0, unexcludedCount);

                    if (netManager.Role == NetworkRole.Host && cleanTargetsSlice.Length == 1 && cleanTargetsSlice[0] == netManager.localID)
                    {
                        netManager.Interpreter.SendCommandAsClient(cleanTargetsSlice, masterPacket);
                    }
                    else
                    {
                        netManager.Interpreter.SendCommandAsServer(cleanTargetsSlice, masterPacket);
                    }
                }
            }
            finally
            {
                _clientAuthorizedList.Clear();
                _tailoredVarsList.Clear();
                _reusableDirtyList.Clear();
                _reusableStateList.Clear();
                Interlocked.CompareExchange(ref _lifecycleGate, 0, 1);
            }
        }

        private void SendViaManager<T>(SendTo target, T packet) where T : struct
        {
            var netManager = _attachedManager;
            if (netManager == null) return;
            if (netManager.Role == NetworkRole.Client)
            {
                if (target == SendTo.Me) netManager.Interpreter.SendCommand(netManager.localID, packet);
                else if (target == SendTo.Server) netManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, packet);
                return;
            }

            int maxClients = GetSessionCapacity(netManager);
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = netManager.SessionManager.GetSessionIds(allSessions);
            if (totalSessions == 0) return;

            Span<ushort> filteredTargets = stackalloc ushort[totalSessions];
            int targetCount = 0;
            ushort localId = netManager.localID;
            bool isHost = netManager.Role == NetworkRole.Host;

            for (int i = 0; i < totalSessions; i++)
            {
                ushort id = allSessions[i];
                bool include = target switch
                {
                    SendTo.Me => isHost ? id == localId : id == ILiminalTransport.SERVER_ID,
                    SendTo.Server => id == ILiminalTransport.SERVER_ID,
                    SendTo.Everyone => true,
                    SendTo.NotMe => isHost ? id != localId : id != ILiminalTransport.SERVER_ID,
                    SendTo.NotServer => id != ILiminalTransport.SERVER_ID,
                    SendTo.NotHost => id != ILiminalTransport.SERVER_ID && (!isHost || id != localId),
                    _ => false
                };
                if (include) filteredTargets[targetCount++] = id;
            }

            if (targetCount == 0) return;
            var targets = filteredTargets.Slice(0, targetCount);
            bool sendAsClient = isHost && (target == SendTo.Me || target == SendTo.NotServer || target == SendTo.Server);
            if (sendAsClient) netManager.Interpreter.SendCommandAsClient(targets, packet);
            else netManager.Interpreter.SendCommandAsServer(targets, packet);
        }

        private void HandleBatchDelta(SyncVarSlabBatchPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager != null && netManager.Role == NetworkRole.Client && !netManager.IsConnected) return;

            if (packet.Payload.IsEmpty) return;

            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                byte tokenLen = reader.ReadByte();
                var tokenRaw = reader.ReadRaw(tokenLen);
                ReadOnlySpan<byte> tokenSpan;
                if (tokenRaw.IsSingleSegment)
                {
                    tokenSpan = tokenRaw.FirstSpan;
                }
                else
                {
                    _reusableTokenBuffer ??= new byte[256];
                    tokenRaw.CopyTo(_reusableTokenBuffer);
                    tokenSpan = new ReadOnlySpan<byte>(_reusableTokenBuffer, 0, tokenLen);
                }

                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw(length);

                if (TryGetSyncVar(tokenSpan, out var syncVar))
                {
                    syncVar.SetLocallyVisible(true);
                    syncVar.ApplyRemoteBytes(rawSlice, version, SerializerOptions);
                }
                else
                {
                    string token = System.Text.Encoding.UTF8.GetString(tokenSpan);
                    byte[] data = rawSlice.ToArray();
                    StoreUnboundData(token, version, data);
                }
            }
        }

        private void HandleClientRequest(SyncVarSlabClientRequestPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client || packet.Payload.IsEmpty) return;

            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                byte tokenLen = reader.ReadByte();
                var tokenRaw = reader.ReadRaw(tokenLen);
                ReadOnlySpan<byte> tokenSpan;
                if (tokenRaw.IsSingleSegment)
                {
                    tokenSpan = tokenRaw.FirstSpan;
                }
                else
                {
                    _reusableTokenBuffer ??= new byte[256];
                    tokenRaw.CopyTo(_reusableTokenBuffer);
                    tokenSpan = new ReadOnlySpan<byte>(_reusableTokenBuffer, 0, tokenLen);
                }

                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw(length);

                if (!TryGetSyncVar(tokenSpan, out var syncVar))
                {
                    string token = System.Text.Encoding.UTF8.GetString(tokenSpan);
                    LiminalLogger.LogError($"[SyncVarManager] Client {senderId} attempted to write to non-existent SyncVar '{token}'. Dropped.");
                    continue;
                }

                if (!syncVar.IsAuthorized(senderId))
                {
                    LiminalLogger.LogWarning($"[SyncVarManager] Client {senderId} unauthorized to modify Var '{syncVar.Token}'. Dropped.");
                    continue;
                }

                if (_pendingClearAcks.TryGetValue(syncVar.Token, out var pending))
                {
                    if (version != 1)
                    {
                        if (pending.ContainsKey(senderId))
                        {
                            LiminalLogger.LogWarning($"[SyncVarManager] Client {senderId} attempted stale write (v={version}) to cleared SyncVar '{syncVar.Token}'. Dropped.");
                            continue;
                        }
                    }
                    else
                    {
                        lock (pending)
                        {
                            pending.TryRemove(senderId, out _);
                            if (pending.IsEmpty)
                            {
                                ((ICollection<KeyValuePair<string, ConcurrentDictionary<ushort, byte>>>)_pendingClearAcks)
                                    .Remove(new KeyValuePair<string, ConcurrentDictionary<ushort, byte>>(syncVar.Token, pending));
                            }
                        }
                    }
                }

                if (syncVar.ApplyRemoteBytes(rawSlice, version, SerializerOptions, force: false, deferEvent: false, senderId: senderId))
                {
                    if (syncVar.TryMarkDirty())
                    {
                        EnqueueDirty(syncVar);
                    }
                }
                else if (!syncVar.IsAuthorized(senderId))
                {
                    LiminalLogger.LogWarning($"[SyncVarManager] Client {senderId} unauthorized to modify Var '{syncVar.Token}'. Dropped.");
                }
                else
                {
                    SendSyncVarToClient(syncVar, senderId);
                }
            }
        }

        private void HandleAuthUpdate(SyncVarAuthUpdatePacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager != null && netManager.Role == NetworkRole.Client && !netManager.IsConnected) return;

            if (TryGetSyncVar(packet.Token, out var syncVar))
            {
                syncVar.ApplyAuthUpdateFromRemote(packet.AuthIds);
            }
            else
            {
                StoreUnboundAuth(packet.Token, packet.AuthIds);
            }
        }

        private void HandleClientConnected(ushort clientId)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client) return;

            var allVars = new List<ISyncVarInternal>();
            _tokenRegistry.GetAll(allVars);

            var entries = new List<SyncVarSnapshotEntry>(allVars.Count);
            for (int i = 0; i < allVars.Count; i++)
            {
                var syncVar = allVars[i];
                if (!syncVar.IsVisibleTo(clientId)) continue;

                if (!syncVar.TryExtractSnapshotData(out byte[] data, out uint version))
                    continue;

                entries.Add(new SyncVarSnapshotEntry
                {
                    Token = syncVar.Token,
                    Version = version,
                    AuthIds = syncVar.AuthIds,
                    Data = data
                });
            }

            netManager.Interpreter.SendCommand(clientId, new SyncVarSnapshotPacket
            {
                Entries = entries
            });
        }

        private void HandleSnapshot(SyncVarSnapshotPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager != null && netManager.Role == NetworkRole.Client && !netManager.IsConnected) return;

            Volatile.Write(ref _hasReceivedInitialSnapshot, 1);
            if (packet.Entries == null || packet.Entries.Count == 0) return;

            for (int i = 0; i < packet.Entries.Count; i++)
            {
                var entry = packet.Entries[i];
                if (TryGetSyncVar(entry.Token, out var syncVar))
                {
                    syncVar.SetLocallyVisible(true);
                    syncVar.ApplyAuthUpdateFromRemote(entry.AuthIds);
                    if (entry.Data != null && entry.Data.Length > 0)
                    {
                        bool force = !syncVar.HasAuthority;
                        syncVar.ApplyRemoteBytes(new ReadOnlySequence<byte>(entry.Data), entry.Version, SerializerOptions, force);
                    }

                    if (syncVar.HasAuthority && syncVar.Version > entry.Version)
                    {
                        if (syncVar.TryMarkDirty())
                        {
                            EnqueueDirty(syncVar);
                        }
                    }
                }
                else
                {
                    if (entry.AuthIds != null)
                    {
                        StoreUnboundAuth(entry.Token, entry.AuthIds);
                    }
                    if (entry.Data != null && entry.Data.Length > 0)
                    {
                        StoreUnboundData(entry.Token, entry.Version, entry.Data);
                    }
                }
            }
        }

        private void HandleEviction(SyncVarEvictPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager != null && netManager.Role == NetworkRole.Client && (!netManager.IsConnected || senderId != ILiminalTransport.SERVER_ID))
                return;

            ReadOnlySpan<byte> tokenSpan = packet.TokenBytes.Length > 0 ? packet.TokenBytes.Span : System.Text.Encoding.UTF8.GetBytes(packet.Token);

            if (TryGetSyncVar(tokenSpan, out var syncVar))
            {
                syncVar.SetLocallyVisible(false);
            }
            else if (!string.IsNullOrEmpty(packet.Token))
            {
                lock (_unboundLock)
                {
                    if (_unboundCache.Remove(packet.Token, out var entry))
                    {
                        _unboundOrder.Remove(entry.Node);
                    }
                    _unboundAuth.Remove(packet.Token);
                }
            }
        }

        private void HandleClear(SyncVarClearPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager != null && netManager.Role == NetworkRole.Client && (!netManager.IsConnected || senderId != ILiminalTransport.SERVER_ID))
                return;

            ReadOnlySpan<byte> tokenSpan = packet.TokenBytes.Length > 0 ? packet.TokenBytes.Span : System.Text.Encoding.UTF8.GetBytes(packet.Token);

            if (TryGetSyncVar(tokenSpan, out var syncVar))
            {
                syncVar.ApplyClearFromRemote();
            }
            else if (!string.IsNullOrEmpty(packet.Token))
            {
                lock (_unboundLock)
                {
                    if (_unboundCache.Remove(packet.Token, out var entry))
                    {
                        _unboundOrder.Remove(entry.Node);
                    }
                    _unboundAuth.Remove(packet.Token);
                }
            }

            if (netManager != null && netManager.Role == NetworkRole.Client && netManager.CanSend)
            {
                netManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarClearAckPacket(packet.Token, packet.TokenBytes));
            }
        }

        private void HandleClearAck(SyncVarClearAckPacket packet, ushort senderId)
        {
            var netManager = _attachedManager;
            if (netManager == null || netManager.Role == NetworkRole.Client) return;

            AcknowledgeClientClear(packet.Token, senderId);
        }
    }
}