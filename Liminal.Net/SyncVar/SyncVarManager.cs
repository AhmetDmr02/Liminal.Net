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
        private static SyncVarManager _lastCreatedManager;

        public static SyncVarManager Instance => LiminalNetworkManager.Instance?.SyncVarManager ?? _lastCreatedManager;

        public static SyncVarManager Initialize(LiminalNetworkManager netManager, LiminalNetworkConfig config)
        {
            if (netManager == null) throw new ArgumentNullException(nameof(netManager));

            if (_managerMap.TryGetValue(netManager, out var existing))
            {
                existing.Reattach(netManager);
                _lastCreatedManager = existing;
                return existing;
            }

            var newManager = new SyncVarManager(netManager, config);
            _managerMap.Add(netManager, newManager);
            _lastCreatedManager = newManager;
            return newManager;
        }

        internal static SyncVarManager GetManagerForRegistration()
        {
            var manager = LiminalNetworkManager.Instance;
            if (manager?.SyncVarManager != null) return manager.SyncVarManager;
            if (_lastCreatedManager != null) return _lastCreatedManager;
            _lastCreatedManager = new SyncVarManager(null, null);
            return _lastCreatedManager;
        }

        private sealed class UnboundEntry
        {
            public uint Version;
            public byte[] Data;
            public LinkedListNode<string> Node;
        }

        private readonly Utf8TokenRegistry _tokenRegistry = new();
        private readonly object _unboundLock = new();
        private readonly Dictionary<string, UnboundEntry> _unboundCache = new();
        private readonly LinkedList<string> _unboundOrder = new();
        private readonly Dictionary<string, ushort[]> _unboundAuth = new();
        private readonly ConcurrentQueue<ISyncVarInternal> _dirtyQueue = new();
        private readonly List<ISyncVarInternal> _reusableDirtyList = new(64);
        private readonly int _maxUnboundCapacity;

        public int MaxUnboundCapacity => _maxUnboundCapacity;
        public int UnboundCacheCount
        {
            get
            {
                lock (_unboundLock) return _unboundCache.Count;
            }
        }
        public int DirtyQueueCount => _dirtyQueue.Count;

        public SyncVarSlab Slab { get; private set; }
        public LockFreeDirtyBitset DirtyBitset { get; } = new();

        private LiminalNetworkManager _attachedManager;
        public LiminalNetworkManager AttachedManager => _attachedManager;
        private int _totalAllocatedBytes;
        private int _hasReceivedInitialSnapshot = 0;

        public readonly MessagePackSerializerOptions SerializerOptions =
            MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

        public SyncVarManager(LiminalNetworkManager manager, LiminalNetworkConfig config)
        {
            Slab = new SyncVarSlab(config);

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

            _attachedManager.Interpreter.Subscribe<SyncVarSnapshotPacket>(HandleSnapshot, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabBatchPacket>(HandleBatchDelta, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabClientRequestPacket>(HandleClientRequest, this);
            _attachedManager.Interpreter.Subscribe<SyncVarAuthUpdatePacket>(HandleAuthUpdate, this);

            _attachedManager.Events.OnClientConnected += HandleClientConnected;
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
                _attachedManager.Events.OnLocalClientDisconnected -= HandleLocalClientDisconnected;
                _attachedManager.Events.OnManagerShutdown -= HandleShutdown;
                _attachedManager.Interpreter.UnsubscribeAll(this);
            }
            catch { }

            _attachedManager = null;
            ResetPendingState();
        }

        public void Reattach(LiminalNetworkManager manager)
        {
            ResetPendingState();
            AttachToManager(manager);
        }

        public void ResetPendingState()
        {
            Volatile.Write(ref _hasReceivedInitialSnapshot, 0);
            while (_dirtyQueue.TryDequeue(out var syncVar))
            {
                syncVar.ClearDirty();
            }
            _reusableDirtyList.Clear();
            lock (_unboundLock)
            {
                _unboundCache.Clear();
                _unboundOrder.Clear();
                _unboundAuth.Clear();
            }
            DirtyBitset.Clear();
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
                return _tokenRegistry.TryRemove(token, out _);
            }
        }

        private void HandleLocalClientDisconnected(ushort clientId)
        {
            ResetPendingState();
        }

        private void HandleShutdown()
        {
            ResetPendingState();
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

        public void RegisterSyncVar(ISyncVarInternal syncVar)
        {
            if (syncVar == null) throw new ArgumentNullException(nameof(syncVar));

            var writer = new ArrayBufferWriter<byte>(128);
            syncVar.SerializeInitial(writer, SerializerOptions);

            int size = writer.WrittenCount;
            syncVar.AllocateSlots(size);
            _totalAllocatedBytes += (size * 2);

            writer.WrittenSpan.CopyTo(Slab.GetSpan(syncVar.ActivePageIndex, syncVar.ActivePageOffset, size));

            byte[] cachedData = null;
            uint cachedVersion = 0;
            ushort[] cachedAuth = null;

            lock (_unboundLock)
            {
                if (_unboundAuth.Remove(syncVar.Token, out var a))
                {
                    cachedAuth = a;
                }

                if (_unboundCache.Remove(syncVar.Token, out var entry))
                {
                    _unboundOrder.Remove(entry.Node);
                    cachedData = entry.Data;
                    cachedVersion = entry.Version;
                }

                _tokenRegistry.TryAdd(syncVar);
            }

            if (cachedAuth != null)
            {
                syncVar.ApplyAuthUpdateFromRemote(cachedAuth);
            }

            if (cachedData != null)
            {
                bool force = !syncVar.HasAuthority;
                syncVar.ApplyRemoteBytes(new ReadOnlySequence<byte>(cachedData), cachedVersion, SerializerOptions, force);
            }

            if (_attachedManager != null && _attachedManager.Role != NetworkRole.Client && _attachedManager.Role != NetworkRole.None)
            {
                if (syncVar.TryMarkDirty())
                {
                    EnqueueDirty(syncVar);
                }
            }
        }

        public SyncVar<T> Bind<T>(string token, T defaultValue = default)
        {
            if (_tokenRegistry.TryGet(token, out var existing))
            {
                return (SyncVar<T>)existing;
            }

            return new SyncVar<T>(token, defaultValue, this);
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
            _dirtyQueue.Enqueue(syncVar);
        }

        internal void BroadcastAuthChange(string token, ushort[] newAuthIds)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client || !_attachedManager.CanSend)
                return;

            SendViaManager(SendTo.Everyone, new SyncVarAuthUpdatePacket
            {
                Token = token,
                AuthIds = newAuthIds
            });
        }

        private readonly ArrayBufferWriter<byte> _sharedFlushWriter = new(4096);
        private readonly ArrayBufferWriter<byte> _tailoredFlushWriter = new(2048);
        private int _isFlushing = 0;

        public void FlushDirty()
        {
            if (_attachedManager == null || !_attachedManager.CanSend)
                return;

            if (_attachedManager.Role == NetworkRole.Client && Volatile.Read(ref _hasReceivedInitialSnapshot) == 0)
                return;

            if (Interlocked.CompareExchange(ref _isFlushing, 1, 0) != 0)
                return;

            try
            {
                _reusableDirtyList.Clear();
                while (_dirtyQueue.TryDequeue(out var syncVar))
                {
                    syncVar.ClearDirty();
                    _reusableDirtyList.Add(syncVar);
                }

                if (_reusableDirtyList.Count == 0) return;

            if (_attachedManager.Role == NetworkRole.Client)
            {
                int authorizedCount = 0;
                for (int i = 0; i < _reusableDirtyList.Count; i++)
                {
                    if (_reusableDirtyList[i].HasAuthority)
                    {
                        authorizedCount++;
                    }
                }

                if (authorizedCount == 0) return;

                _sharedFlushWriter.Clear();
                var writer = new MessagePackWriter(_sharedFlushWriter);
                writer.WriteArrayHeader(authorizedCount);

                for (int i = 0; i < _reusableDirtyList.Count; i++)
                {
                    var syncVar = _reusableDirtyList[i];
                    if (!syncVar.HasAuthority) continue;

                    if (!syncVar.TryWriteSlotForWire(ref writer, out _))
                    {
                        if (syncVar.TryMarkDirty())
                            _dirtyQueue.Enqueue(syncVar);
                    }
                }

                writer.Flush();
                var sequence = new ReadOnlySequence<byte>(_sharedFlushWriter.WrittenMemory);
                _attachedManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarSlabClientRequestPacket(sequence));
                return;
            }

            // Server / Host Flush
            _sharedFlushWriter.Clear();
            var masterWriter = new MessagePackWriter(_sharedFlushWriter);
            masterWriter.WriteArrayHeader(_reusableDirtyList.Count);

            bool hasAnyExclusions = false;
            for (int i = 0; i < _reusableDirtyList.Count; i++)
            {
                var syncVar = _reusableDirtyList[i];
                if (syncVar.ExclusionIds.Length > 0)
                {
                    hasAnyExclusions = true;
                }

                if (!syncVar.TryWriteSlotForWire(ref masterWriter, out _))
                {
                    if (syncVar.TryMarkDirty())
                        _dirtyQueue.Enqueue(syncVar);
                }
            }

            masterWriter.Flush();
            var masterPacket = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(_sharedFlushWriter.WrittenMemory));

            if (!hasAnyExclusions)
            {
                SendViaManager(SendTo.Everyone, masterPacket);
                return;
            }

            int maxClients = _attachedManager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = _attachedManager.SessionManager.GetSessionIds(allSessions);

            Span<ushort> unexcludedTargets = stackalloc ushort[totalSessions];
            int unexcludedCount = 0;

            for (int s = 0; s < totalSessions; s++)
            {
                ushort targetId = allSessions[s];
                if (targetId == ILiminalTransport.SERVER_ID && _attachedManager.Role == NetworkRole.Host)
                    continue;

                bool clientHasExclusions = false;
                for (int i = 0; i < _reusableDirtyList.Count; i++)
                {
                    if (_reusableDirtyList[i].IsExcluded(targetId))
                    {
                        clientHasExclusions = true;
                        break;
                    }
                }

                if (!clientHasExclusions)
                {
                    unexcludedTargets[unexcludedCount++] = targetId;
                }
                else
                {
                    int allowedCount = 0;
                    for (int i = 0; i < _reusableDirtyList.Count; i++)
                    {
                        if (!_reusableDirtyList[i].IsExcluded(targetId))
                            allowedCount++;
                    }

                    if (allowedCount == 0) continue;

                    _tailoredFlushWriter.Clear();
                    var tailoredWriter = new MessagePackWriter(_tailoredFlushWriter);
                    tailoredWriter.WriteArrayHeader(allowedCount);

                    for (int i = 0; i < _reusableDirtyList.Count; i++)
                    {
                        var syncVar = _reusableDirtyList[i];
                        if (syncVar.IsExcluded(targetId)) continue;

                        syncVar.TryWriteSlotForWire(ref tailoredWriter, out _);
                    }

                    tailoredWriter.Flush();
                    var tailoredPacket = new SyncVarSlabBatchPacket(new ReadOnlySequence<byte>(_tailoredFlushWriter.WrittenMemory));

                    if (_attachedManager.Role == NetworkRole.Host && targetId == _attachedManager.localID)
                        _attachedManager.Interpreter.SendCommandAsClient(targetId, tailoredPacket);
                    else
                        _attachedManager.Interpreter.SendCommandAsServer(targetId, tailoredPacket);
                }
            }

            if (unexcludedCount > 0)
            {
                var cleanTargetsSlice = unexcludedTargets.Slice(0, unexcludedCount);

                if (_attachedManager.Role == NetworkRole.Host && cleanTargetsSlice.Length == 1 && cleanTargetsSlice[0] == _attachedManager.localID)
                {
                    _attachedManager.Interpreter.SendCommandAsClient(cleanTargetsSlice, masterPacket);
                }
                else
                {
                    _attachedManager.Interpreter.SendCommandAsServer(cleanTargetsSlice, masterPacket);
                }
            }
        }
        finally
        {
            Volatile.Write(ref _isFlushing, 0);
        }
    }

        private void SendViaManager<T>(SendTo target, T packet) where T : struct
        {
            if (_attachedManager == null) return;
            if (_attachedManager.Role == NetworkRole.Client)
            {
                if (target == SendTo.Me) _attachedManager.Interpreter.SendCommand(_attachedManager.localID, packet);
                else if (target == SendTo.Server) _attachedManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, packet);
                return;
            }

            int maxClients = _attachedManager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = _attachedManager.SessionManager.GetSessionIds(allSessions);
            if (totalSessions == 0) return;

            Span<ushort> filteredTargets = stackalloc ushort[totalSessions];
            int targetCount = 0;
            ushort localId = _attachedManager.localID;
            bool isHost = _attachedManager.Role == NetworkRole.Host;

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
            if (sendAsClient) _attachedManager.Interpreter.SendCommandAsClient(targets, packet);
            else _attachedManager.Interpreter.SendCommandAsServer(targets, packet);
        }

        private void HandleBatchDelta(SyncVarSlabBatchPacket packet, ushort senderId)
        {
            if (packet.Payload.IsEmpty) return;

            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                byte tokenLen = reader.ReadByte();
                var tokenRaw = reader.ReadRaw(tokenLen);
                ReadOnlySpan<byte> tokenSpan = tokenRaw.IsSingleSegment ? tokenRaw.FirstSpan : tokenRaw.ToArray();

                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw(length);

                if (TryGetSyncVar(tokenSpan, out var syncVar))
                {
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
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client || packet.Payload.IsEmpty) return;

            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                byte tokenLen = reader.ReadByte();
                var tokenRaw = reader.ReadRaw(tokenLen);
                ReadOnlySpan<byte> tokenSpan = tokenRaw.IsSingleSegment ? tokenRaw.FirstSpan : tokenRaw.ToArray();

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

                syncVar.ApplyRemoteBytes(rawSlice, version, SerializerOptions);
                if (syncVar.TryMarkDirty())
                {
                    EnqueueDirty(syncVar);
                }
            }
        }

        private void HandleAuthUpdate(SyncVarAuthUpdatePacket packet, ushort senderId)
        {
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
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client) return;

            var allVars = new List<ISyncVarInternal>();
            _tokenRegistry.GetAll(allVars);

            var entries = new List<SyncVarSnapshotEntry>(allVars.Count);
            for (int i = 0; i < allVars.Count; i++)
            {
                var syncVar = allVars[i];
                if (syncVar.IsExcluded(clientId)) continue;

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

            _attachedManager.Interpreter.SendCommand(clientId, new SyncVarSnapshotPacket
            {
                Entries = entries
            });
        }

        private void HandleSnapshot(SyncVarSnapshotPacket packet, ushort senderId)
        {
            Volatile.Write(ref _hasReceivedInitialSnapshot, 1);
            if (packet.Entries == null || packet.Entries.Count == 0) return;

            for (int i = 0; i < packet.Entries.Count; i++)
            {
                var entry = packet.Entries[i];
                if (TryGetSyncVar(entry.Token, out var syncVar))
                {
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
    }
}