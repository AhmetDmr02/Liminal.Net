using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Liminal.Net.SyncVar
{
    public class SyncVarManager
    {
        private static readonly object _initLock = new();
        private static SyncVarManager _instance;
        private static LiminalNetworkConfig _config;

        public static SyncVarManager Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (_initLock)
                    {
                        _instance ??= new SyncVarManager(_config);
                    }
                }
                return _instance;
            }
        }

        public static SyncVarManager Initialize(LiminalNetworkConfig config)
        {
            lock (_initLock)
            {
                _config = config;
                _instance ??= new SyncVarManager(config);
                return _instance;
            }
        }

        private readonly ConcurrentDictionary<string, ISyncVarInternal> _tokenRegistry = new();
        private readonly ConcurrentDictionary<ushort, ISyncVarInternal> _idRegistry = new();
        private readonly List<ISyncVarInternal> _reusableDirtyList = new(64);

        public SyncVarSlab Slab { get; private set; }
        public LockFreeDirtyBitset DirtyBitset { get; } = new();

        private ushort _nextIdCounter = 1;
        private LiminalNetworkManager _attachedManager;
        private int _totalAllocatedBytes;

        public readonly MessagePackSerializerOptions SerializerOptions =
            MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

        private SyncVarManager(LiminalNetworkConfig config)
        {
            _config = config;
            Slab = new SyncVarSlab(config);

            LiminalNetworkManager.OnManagerPostInitialize += HandleManagerInitialized;
            LiminalNetworkManager.OnManagerShutdown += HandleManagerShutdown;

            if (LiminalNetworkManager.Instance != null)
            {
                AttachToManager(LiminalNetworkManager.Instance);
            }
        }

        private void HandleManagerInitialized() => AttachToManager(LiminalNetworkManager.Instance);

        private void HandleManagerShutdown()
        {
            DetachFromManager();
            Slab?.Dispose();
            Slab = new SyncVarSlab(_config);
            DirtyBitset.Clear();
            _totalAllocatedBytes = 0;
        }

        private void AttachToManager(LiminalNetworkManager manager)
        {
            if (manager == null) return;
            DetachFromManager();

            _attachedManager = manager;

            _attachedManager.Interpreter.Subscribe<SyncVarSlabInitPacket>(HandleInitialSlabSync, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabBatchPacket>(HandleBatchDelta, this);
            _attachedManager.Interpreter.Subscribe<SyncVarSlabClientRequestPacket>(HandleClientRequest, this);

            _attachedManager.Interpreter.Subscribe<SyncVarAuthUpdatePacket>(HandleAuthUpdate, this);

            _attachedManager.Transport.OnClientConnected += HandleClientConnected;
            LiminalNetworkManager.OnPreFlush += FlushDirty;
        }
        internal void BroadcastAuthChange(ushort varId, ushort[] newAuthIds)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client)
                return;

            Broadcaster.Send(SendTo.Everyone, new SyncVarAuthUpdatePacket
            {
                VarId = varId,
                AuthIds = newAuthIds
            });
        }

        private void HandleAuthUpdate(SyncVarAuthUpdatePacket packet, ushort senderId)
        {
            if (TryGetSyncVar(packet.VarId, out var syncVar))
            {
                syncVar.ApplyAuthUpdateFromRemote(packet.AuthIds);
            }
        }

        private void DetachFromManager()
        {
            if (_attachedManager == null) return;

            try
            {
                LiminalNetworkManager.OnPreFlush -= FlushDirty;
                _attachedManager.Transport.OnClientConnected -= HandleClientConnected;
                _attachedManager.Interpreter.UnsubscribeAll(this);
            }
            catch { }

            _attachedManager = null;
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

            return new SyncVar<T>(token, defaultValue);
        }

        public bool TryGetSyncVar(ushort id, out ISyncVarInternal syncVar)
        {
            return _idRegistry.TryGetValue(id, out syncVar);
        }

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
                    int bitPos = BitOperations.TrailingZeroCount(umask);
                    ushort id = (ushort)((bucket << 6) + bitPos);

                    if (_idRegistry.TryGetValue(id, out var syncVar))
                    {
                        _reusableDirtyList.Add(syncVar);
                    }

                    umask &= umask - 1;
                }
            }

            if (_reusableDirtyList.Count == 0) return;

            var bufferWriter = new ArrayBufferWriter<byte>(4096);
            var writer = new MessagePackWriter(bufferWriter);

            writer.WriteArrayHeader(_reusableDirtyList.Count);

            for (int i = 0; i < _reusableDirtyList.Count; i++)
            {
                var syncVar = _reusableDirtyList[i];
                writer.WriteUInt16(syncVar.Id);
                writer.WriteUInt32(syncVar.Version);
                writer.WriteInt32(syncVar.Length);
                writer.Flush();

                Span<byte> dest = bufferWriter.GetSpan(syncVar.Length).Slice(0, syncVar.Length);
                syncVar.TryReadSlotForWire(dest, out _, out _);
                bufferWriter.Advance(syncVar.Length);
            }

            var sequence = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);

            if (_attachedManager.Role == NetworkRole.Server || _attachedManager.Role == NetworkRole.Host)
            {
                Broadcaster.Send(SendTo.Everyone, new SyncVarSlabBatchPacket(sequence));
            }
            else if (_attachedManager.Role == NetworkRole.Client)
            {
                Broadcaster.Send(SendTo.Server, new SyncVarSlabClientRequestPacket(sequence));
            }
        }

        private void HandleBatchDelta(SyncVarSlabBatchPacket packet, ushort senderId)
        {
            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                ushort id = reader.ReadUInt16();
                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw();

                if (TryGetSyncVar(id, out var syncVar))
                {
                    syncVar.ApplyRemoteBytes(rawSlice.ToArray(), version, SerializerOptions);
                }
            }
        }

        private void HandleClientRequest(SyncVarSlabClientRequestPacket packet, ushort senderId)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client) return;

            var reader = new MessagePackReader(packet.Payload);
            int count = reader.ReadArrayHeader();

            for (int i = 0; i < count; i++)
            {
                ushort id = reader.ReadUInt16();
                uint version = reader.ReadUInt32();
                int length = reader.ReadInt32();
                var rawSlice = reader.ReadRaw();

                if (!TryGetSyncVar(id, out var syncVar)) continue;

                if (!syncVar.IsAuthorized(senderId))
                {
                    LiminalLogger.LogWarning($"[SyncVarManager] Client {senderId} unauthorized to modify Var #{id}. Dropped.");
                    continue;
                }

                syncVar.ApplyRemoteBytes(rawSlice.ToArray(), version, SerializerOptions);
                DirtyBitset.SetDirty(syncVar.Id);
            }
        }

        private void HandleClientConnected(ushort clientId)
        {
            if (_attachedManager == null || _attachedManager.Role == NetworkRole.Client) return;

            var descriptors = new List<SyncVarDescriptor>(_tokenRegistry.Count);
            foreach (var kvp in _tokenRegistry)
            {
                descriptors.Add(new SyncVarDescriptor
                {
                    Id = kvp.Value.Id,
                    Token = kvp.Key,
                    AuthIds = kvp.Value.AuthIds,
                    PageIndex = kvp.Value.ActivePageIndex,
                    PageOffset = kvp.Value.ActivePageOffset,
                    Length = kvp.Value.Length
                });
            }

            Broadcaster.SendToClient(clientId, new SyncVarSlabInitPacket
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

                if (_tokenRegistry.TryGetValue(desc.Token, out var syncVar))
                {
                    syncVar.Id = desc.Id;
                    syncVar.SetAuthIds(desc.AuthIds);
                    _idRegistry[desc.Id] = syncVar;

                    var memorySpan = Slab.GetSpan(desc.PageIndex, desc.PageOffset, desc.Length);
                    syncVar.ApplyRemoteBytes(memorySpan, 1, SerializerOptions);
                }
            }
        }
    }
}