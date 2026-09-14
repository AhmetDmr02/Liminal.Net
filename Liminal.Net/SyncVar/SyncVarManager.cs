using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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

        private readonly ConcurrentDictionary<string, ISyncVarInternal> _tokenRegistry = new();
        private readonly ConcurrentDictionary<ushort, ISyncVarInternal> _idRegistry = new();
        private readonly ConcurrentDictionary<string, SyncVarDescriptor> _receivedDescriptors = new();
        private readonly List<ISyncVarInternal> _reusableDirtyList = new(64);

        public SyncVarSlab Slab { get; private set; }
        public LockFreeDirtyBitset DirtyBitset { get; } = new();

        private ushort _nextIdCounter = 1;
        private LiminalNetworkManager _attachedManager;
        public LiminalNetworkManager AttachedManager => _attachedManager;
        private int _totalAllocatedBytes;

        public readonly MessagePackSerializerOptions SerializerOptions =
            MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

        public SyncVarManager(LiminalNetworkManager manager, LiminalNetworkConfig config)
        {
            Slab = new SyncVarSlab(config);
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

            _attachedManager.Interpreter.Subscribe<SyncVarSlabInitPacket>(HandleInitialSlabSync, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabBatchPacket>(HandleBatchDelta, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabClientRequestPacket>(HandleClientRequest, this);
            _attachedManager.Interpreter.Subscribe<SyncVarAuthUpdatePacket>(HandleAuthUpdate, this);

            _attachedManager.Events.OnClientConnected += HandleClientConnected;
            _attachedManager.OnPreFlush += FlushDirty;
        }

        public void DetachFromManager()
        {
            if (_attachedManager == null) return;

            try
            {
                _attachedManager.OnPreFlush -= FlushDirty;
                _attachedManager.Events.OnClientConnected -= HandleClientConnected;
                _attachedManager.Interpreter.UnsubscribeAll(this);
            }
            catch { }

            _attachedManager = null;
        }

        public void Reattach(LiminalNetworkManager manager)
        {
            AttachToManager(manager);
            DirtyBitset.Clear();
        }

        public void RegisterSyncVar(ISyncVarInternal syncVar)
        {
            syncVar.Id = _nextIdCounter++;
            _tokenRegistry[syncVar.Token] = syncVar;
            _idRegistry[syncVar.Id] = syncVar;

            var writer = new ArrayBufferWriter<byte>(128);
            syncVar.SerializeInitial(writer, SerializerOptions);

            int size = writer.WrittenCount;
            syncVar.AllocateSlots(size);
            _totalAllocatedBytes += (size * 2);

            writer.WrittenSpan.CopyTo(Slab.GetSpan(syncVar.ActivePageIndex, syncVar.ActivePageOffset, size));

            if (_attachedManager != null && _attachedManager.Role != NetworkRole.Client && _attachedManager.Role != NetworkRole.None)
            {
                DirtyBitset.SetDirty(syncVar.Id);
            }
        }

        public SyncVar<T> Bind<T>(string token, T defaultValue = default)
        {
            if (_tokenRegistry.TryGetValue(token, out var existing))
            {
                return (SyncVar<T>)existing;
            }

            var newVar = new SyncVar<T>(token, defaultValue, this);

            if (_receivedDescriptors.TryGetValue(token, out var desc))
            {
                _idRegistry.TryRemove(newVar.Id, out _);
                newVar.Id = desc.Id;
                newVar.ApplyAuthUpdateFromRemote(desc.AuthIds);
                _idRegistry[desc.Id] = newVar;

                var memorySeq = new ReadOnlySequence<byte>(Slab.GetPage(desc.PageIndex), desc.PageOffset, desc.Length);
                newVar.ApplyRemoteBytes(memorySeq, desc.Version, SerializerOptions);
            }

            return newVar;
        }

        public bool TryGetSyncVar(ushort id, out ISyncVarInternal syncVar)
        {
            return _idRegistry.TryGetValue(id, out syncVar);
        }

        internal void BroadcastAuthChange(ushort varId, ushort[] newAuthIds)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client)
                return;

            SendViaManager(SendTo.Everyone, new SyncVarAuthUpdatePacket
            {
                VarId = varId,
                AuthIds = newAuthIds
            });
        }

        private readonly ArrayBufferWriter<byte> _sharedFlushWriter = new(4096);
        private readonly ArrayBufferWriter<byte> _tailoredFlushWriter = new(2048);
        public void FlushDirty()
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.None)
                return;

            _reusableDirtyList.Clear();

            for (int bucket = 0; bucket < 64; bucket++)
            {
                if (!DirtyBitset.TryConsumeBucket(bucket, out long mask))
                    continue;

                ulong umask = (ulong)mask;
                while (umask != 0)
                {
                    int bitPos = BitOperationsCompat.TrailingZeroCount(umask);
                    ushort id = (ushort)((bucket << 6) + bitPos);

                    if (_idRegistry.TryGetValue(id, out var syncVar))
                    {
                        _reusableDirtyList.Add(syncVar);
                    }

                    umask &= umask - 1;
                }
            }

            if (_reusableDirtyList.Count == 0) return;

            if (_attachedManager.Role == NetworkRole.Client)
            {
                _sharedFlushWriter.Clear(); 
                var writer = new MessagePackWriter(_sharedFlushWriter);
                writer.WriteArrayHeader(_reusableDirtyList.Count);

                for (int i = 0; i < _reusableDirtyList.Count; i++)
                {
                    var syncVar = _reusableDirtyList[i];
                    if (!syncVar.TryWriteSlotForWire(ref writer, out _))
                    {
                        DirtyBitset.SetDirty(syncVar.Id);
                    }
                }

                writer.Flush();
                var sequence = new ReadOnlySequence<byte>(_sharedFlushWriter.WrittenMemory);
                _attachedManager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new SyncVarSlabClientRequestPacket(sequence));
                return;
            }

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
                    DirtyBitset.SetDirty(syncVar.Id);
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
                ushort id = reader.ReadUInt16();
                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw(length);

                if (TryGetSyncVar(id, out var syncVar))
                {
                    ReadOnlySpan<byte> span = rawSlice.IsSingleSegment ? rawSlice.FirstSpan : rawSlice.ToArray();
                    syncVar.ApplyRemoteBytes(rawSlice, version, SerializerOptions);
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
                ushort id = reader.ReadUInt16();
                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw(length);

                if (!TryGetSyncVar(id, out var syncVar)) continue;

                if (!syncVar.IsAuthorized(senderId))
                {
                    LiminalLogger.LogWarning($"[SyncVarManager] Client {senderId} unauthorized to modify Var #{id}. Dropped.");
                    continue;
                }

                ReadOnlySpan<byte> span = rawSlice.IsSingleSegment ? rawSlice.FirstSpan : rawSlice.ToArray();
                syncVar.ApplyRemoteBytes(rawSlice, version, SerializerOptions);
                DirtyBitset.SetDirty(syncVar.Id);
            }
        }

        private void HandleAuthUpdate(SyncVarAuthUpdatePacket packet, ushort senderId)
        {
            if (TryGetSyncVar(packet.VarId, out var syncVar))
            {
                syncVar.ApplyAuthUpdateFromRemote(packet.AuthIds);
            }
        }

        private void HandleClientConnected(ushort clientId)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client) return;

            var descriptors = new List<SyncVarDescriptor>(_tokenRegistry.Count);
            foreach (var kvp in _tokenRegistry)
            {
                if (kvp.Value.IsExcluded(clientId))
                {
                    continue;
                }

                descriptors.Add(new SyncVarDescriptor
                {
                    Id = kvp.Value.Id,
                    Token = kvp.Key,
                    AuthIds = kvp.Value.AuthIds,
                    PageIndex = kvp.Value.ActivePageIndex,
                    PageOffset = kvp.Value.ActivePageOffset,
                    Length = kvp.Value.Length,
                    Version = kvp.Value.Version
                });
            }

            _attachedManager.Interpreter.SendCommand(clientId, new SyncVarSlabInitPacket
            {
                Descriptors = descriptors,
                RawSlab = Slab.ExtractSnapshot(_totalAllocatedBytes)
            });
        }

        private void HandleInitialSlabSync(SyncVarSlabInitPacket packet, ushort senderId)
        {
            Slab.InitializeFromSnapshot(packet.RawSlab);

            for (int i = 0; i < packet.Descriptors.Count; i++)
            {
                var desc = packet.Descriptors[i];
                _receivedDescriptors[desc.Token] = desc;

                if (_tokenRegistry.TryGetValue(desc.Token, out var syncVar))
                {
                    _idRegistry.TryRemove(syncVar.Id, out _);
                    syncVar.Id = desc.Id;
                    syncVar.ApplyAuthUpdateFromRemote(desc.AuthIds);
                    _idRegistry[desc.Id] = syncVar;

                    var memorySeq = new ReadOnlySequence<byte>(Slab.GetPage(desc.PageIndex), desc.PageOffset, desc.Length);
                    syncVar.ApplyRemoteBytes(memorySeq, desc.Version, SerializerOptions);
                }
            }
        }
    }
}