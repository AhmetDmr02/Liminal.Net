using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar.Internal;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.SyncVar
{
    public interface ISyncVarInternal
    {
        string Token { get; }
        byte[] TokenBytes { get; }
        uint Version { get; }
        (int PageIndex, int PageOffset) ActiveSlotCoordinates { get; }
        int Length { get; }
        ushort[] AuthIds { get; }
        ushort[] ExclusionIds { get; }
        ushort[] ObserverIds { get; }
        SyncVarVisibility Visibility { get; set; }
        SyncVarVisibilityState VisibilityState { get; }
        bool IsVisible { get; }

        bool IsAuthorized(ushort clientId);
        bool IsExcluded(ushort clientId);
        bool ContainsExclusion(ushort clientId);
        bool ContainsObserver(ushort clientId);
        bool IsVisibleTo(ushort clientId);
        bool HasAuthority { get; }

        void SetLocallyVisible(bool isVisible);
        void SetAuthIds(params ushort[] clientIds);
        void AddAuthority(ushort clientId);
        void RemoveAuthority(ushort clientId);
        void ApplyAuthUpdateFromRemote(ushort[] newAuthIds, bool deferEvent = false);

        void SetExclusionIds(params ushort[] clientIds);
        void SetExclusions(params ushort[] clientIds);
        void AddExclusion(ushort clientId);
        void RemoveExclusion(ushort clientId);

        void SetVisibility(SyncVarVisibility visibility);
        void SetObserverIds(params ushort[] clientIds);
        void SetObservers(params ushort[] clientIds);
        void AddObserver(ushort clientId);
        void RemoveObserver(ushort clientId);

        void SetDirty();
        bool TryMarkDirty();
        void ClearDirty();
        bool IsDirty { get; }
        bool IsDisposed { get; }
        ushort LocalSlot { get; set; }

        void AllocateSlots(int size);
        bool TryInitializeSlabSlots(ReadOnlySpan<byte> initialBytes);
        void AttachManager(SyncVarManager manager);
        void Clear();
        void ApplyClearFromRemote();
        void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options);
        bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version);
        bool TryExtractSnapshotData(out byte[] data, out uint version);
        bool ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false, ushort senderId = 0);
        void FirePendingEvents();
        void InternalTeardown();
        void ResetSession();
    }

    public class SyncVar<T> : ISyncVarInternal
    {
        private readonly SyncVarPermissions _permissions;
        private readonly SyncVarValueCell<T> _valueCell;
        private volatile SyncVarWireBuffer _wireBuffer;
        private volatile SyncVarManager _manager;
        private readonly byte[] _tokenBytes;
        private readonly object _writeLock = new();
        private readonly object _permissionBroadcastLock = new();
        private readonly object _registrationLock = new();

        private volatile ushort _localSlot = ushort.MaxValue;
        private int _fallbackDirty = 0;

        [ThreadStatic]
        private static ArrayBufferWriter<byte> _threadLocalWriter;

        public event Action<T, T> OnValueChanged;
        public event Action<bool> OnAuthorityChanged;
        public event Action<bool> OnVisibilityScopeChanged;

        public ushort LocalSlot
        {
            get => _localSlot;
            set => _localSlot = value;
        }

        public string Token { get; }
        public byte[] TokenBytes => _tokenBytes;
        public uint Version => _valueCell.Version;
        public (int PageIndex, int PageOffset) ActiveSlotCoordinates => _wireBuffer.ActiveSlotCoordinates;
        public int Length => _wireBuffer.Length;
        public ushort[] AuthIds => _permissions.Authorities;
        public ushort[] ExclusionIds => _permissions.Exclusions;
        public ushort[] ObserverIds => _permissions.Observers;

        public SyncVarVisibility Visibility
        {
            get => _permissions.Mode;
            set => SetVisibility(value);
        }

        public SyncVarVisibilityState VisibilityState => _permissions.State;
        public bool IsVisible => _permissions.IsLocallyVisible;
        public bool HasAuthority => _permissions.HasAuthority;
        public bool IsDirty
        {
            get
            {
                var mgr = _manager;
                ushort slot = _localSlot;
                if (mgr != null && slot != ushort.MaxValue)
                {
                    return mgr.DirtyBitset.IsDirty(slot);
                }
                return Volatile.Read(ref _fallbackDirty) != 0;
            }
        }
        public bool IsDisposed => _wireBuffer.IsDisposed;

        public bool IsAuthorized(ushort clientId) => _permissions.IsAuthorized(clientId);
        public bool IsExcluded(ushort clientId) => _permissions.IsExcluded(clientId);
        public bool ContainsExclusion(ushort clientId) => _permissions.ContainsExclusion(clientId);
        public bool ContainsObserver(ushort clientId) => _permissions.ContainsObserver(clientId);
        public bool IsVisibleTo(ushort clientId) => _permissions.IsVisibleTo(clientId);

        public T Value
        {
            get => _valueCell.Value;
            set
            {
                if (IsDisposed) return;
                if (!HasAuthority)
                {
                    var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                    ushort myId = netManager?.localID ?? 0;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                    return;
                }

                WriteFromOwningThread(value, force: false);
            }
        }

        public (T Value, uint Version) GetSnapshot() => _valueCell.GetSnapshot();

        public SyncVar(string token, T initialValue = default, SyncVarManager manager = null)
            : this(token, initialValue, SyncVarVisibility.Public, manager)
        {
        }

        public SyncVar(string token, SyncVarVisibility visibility, SyncVarManager manager = null)
            : this(token, default, visibility, manager)
        {
        }

        public SyncVar(string token, T initialValue, SyncVarVisibility visibility, SyncVarManager manager = null)
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            _tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
            if (_tokenBytes.Length > 255)
            {
                throw new ArgumentException($"[SyncVar] Token '{token}' UTF-8 length exceeds 255 bytes.");
            }
            Utf8TokenRegistry.RegisterToken(token, _tokenBytes);

            _manager = manager ?? SyncVarManager.GetManagerForRegistration();

            _permissions = new SyncVarPermissions(_manager, token, visibility);
            _valueCell = new SyncVarValueCell<T>(initialValue);
            _wireBuffer = new SyncVarWireBuffer(_manager?.Slab);

            _manager?.RegisterSyncVar(this);
        }

        public void SetLocallyVisible(bool isVisible)
        {
            if (_permissions.SetLocallyVisible(isVisible))
            {
                OnVisibilityScopeChanged?.Invoke(isVisible);
            }
        }

        public void SetDirty()
        {
            if (!HasAuthority)
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                ushort myId = netManager?.localID ?? 0;
                LiminalLogger.LogError($"[SyncVar] Unauthorized SetDirty blocked! Client {myId} does not have authority on '{Token}'.");
                return;
            }

            WriteFromOwningThread(Value, force: true);
        }

        public bool TryMarkDirty()
        {
            if (IsDisposed || !HasAuthority) return false;
            var mgr = _manager;
            ushort slot = _localSlot;
            if (mgr != null && slot != ushort.MaxValue)
            {
                mgr.DirtyBitset.SetDirty(slot);
                return true;
            }

            Volatile.Write(ref _fallbackDirty, 1);
            return true;
        }

        public void ClearDirty()
        {
            var mgr = _manager;
            ushort slot = _localSlot;
            if (mgr != null && slot != ushort.MaxValue)
            {
                mgr.DirtyBitset.ClearDirty(slot);
            }
            Volatile.Write(ref _fallbackDirty, 0);
        }

        internal void AttachManager(SyncVarManager manager)
        {
            if (manager == null) return;
            _manager = manager;
            _permissions.UpdateManager(manager);
            _wireBuffer.UpdateSlab(manager.Slab);
        }

        void ISyncVarInternal.AttachManager(SyncVarManager manager) => AttachManager(manager);

        bool ISyncVarInternal.TryInitializeSlabSlots(ReadOnlySpan<byte> initialBytes)
        {
            var mgr = _manager;
            var slab = mgr?.Slab;
            if (slab != null)
            {
                _wireBuffer.UpdateSlab(slab);
            }
            return _wireBuffer.AllocateAndWriteInitial(initialBytes);
        }

        void ISyncVarInternal.AllocateSlots(int size)
        {
            var mgr = _manager;
            var slab = mgr?.Slab;
            if (slab != null)
            {
                _wireBuffer.UpdateSlab(slab);
            }
            _wireBuffer.AllocateInitialSlots(size);
        }

        public void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options)
        {
            MessagePackSerializer.Serialize(writer, Value, options);
        }

        public void Reattach(SyncVarManager manager = null)
        {
            if (IsDisposed) return;
            lock (_registrationLock)
            {
                if (IsDisposed) return;
                var mgr = manager ?? SyncVarManager.GetManagerForRegistration();
                if (mgr != null && !mgr.IsTornDown)
                {
                    _manager = mgr;
                    _permissions.UpdateManager(mgr);
                    _wireBuffer.UpdateSlab(mgr.Slab);
                    mgr.RegisterSyncVar(this);
                }
            }
        }

        private void WriteFromOwningThread(T newValue, bool force = false)
        {
            if (IsDisposed) return;

            var currentMgr = _manager;
            if (currentMgr != null && currentMgr.IsTornDown)
            {
                currentMgr = null;
            }

            var activeMgr = currentMgr ?? SyncVarManager.GetManagerForRegistration();

            if (activeMgr == null)
            {
                T oldValue;
                bool fireChanged;
                lock (_writeLock)
                {
                    if (IsDisposed) return;
                    if (!_valueCell.TryUpdate(newValue, force, deferEvent: false, out oldValue, out fireChanged, out _))
                    {
                        return;
                    }
                }

                if (fireChanged)
                {
                    OnValueChanged?.Invoke(oldValue, newValue);
                }
                return;
            }

            if (IsDisposed) return;

            if (_manager != activeMgr || _localSlot == ushort.MaxValue)
            {
                if (IsDisposed) return;
                lock (_registrationLock)
                {
                    if (IsDisposed) return;
                    if (_manager != activeMgr || _localSlot == ushort.MaxValue)
                    {
                        if (activeMgr.TryGetSyncVar(Token, out var existing) && !ReferenceEquals(existing, this))
                        {
                            return;
                        }
                        _manager = activeMgr;
                        _permissions.UpdateManager(activeMgr);
                        _wireBuffer.UpdateSlab(activeMgr.Slab);
                        activeMgr.RegisterSyncVar(this);
                    }
                }
            }

            if (IsDisposed || !HasAuthority)
            {
                if (!HasAuthority && !IsDisposed)
                {
                    var netManager = activeMgr.AttachedManager ?? LiminalNetworkManager.Instance;
                    ushort myId = netManager?.localID ?? 0;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                }
                return;
            }

            T oldValueWithNetwork;
            bool fireChangedWithNetwork;

            lock (_writeLock)
            {
                if (IsDisposed || !HasAuthority) return;

                if (!_valueCell.TryPrepareUpdate(newValue, force, out uint nextVersion))
                {
                    return;
                }

                _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
                _threadLocalWriter.Clear();
                MessagePackSerializer.Serialize(_threadLocalWriter, newValue, activeMgr.SerializerOptions);

                if (!_wireBuffer.WriteBytes(_threadLocalWriter.WrittenSpan, nextVersion, force))
                {
                    return;
                }

                _valueCell.CommitUpdate(newValue, nextVersion, deferEvent: false, out oldValueWithNetwork, out fireChangedWithNetwork);

                if (TryMarkDirty())
                {
                    activeMgr.EnqueueDirty(this);
                }
            }

            if (fireChangedWithNetwork)
            {
                OnValueChanged?.Invoke(oldValueWithNetwork, newValue);
            }
        }

        public bool ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false, ushort senderId = 0)
        {
            if (IsDisposed) return false;
            uint currentVer = _valueCell.Version;
            if (!force && newVersion <= currentVer && currentVer != 0) return false;

            T deserialized = MessagePackSerializer.Deserialize<T>(incomingBytes, options);
            T oldValue;
            bool fireEvent;

            lock (_writeLock)
            {
                if (IsDisposed) return false;

                if (senderId != 0 && !_permissions.IsAuthorized(senderId))
                {
                    return false;
                }

                if (!_valueCell.TryApplyRemote(deserialized, newVersion, force, deferEvent, out oldValue, out fireEvent))
                {
                    return false;
                }

                if (incomingBytes.IsSingleSegment)
                {
                    _wireBuffer.WriteBytes(incomingBytes.FirstSpan, newVersion, force);
                }
                else
                {
                    _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
                    _threadLocalWriter.Clear();
                    foreach (var mem in incomingBytes)
                    {
                        _threadLocalWriter.Write(mem.Span);
                    }
                    _wireBuffer.WriteBytes(_threadLocalWriter.WrittenSpan, newVersion, force);
                }

                if (force)
                {
                    ClearDirty();
                }
            }

            if (fireEvent)
            {
                OnValueChanged?.Invoke(oldValue, deserialized);
            }

            return true;
        }

        public bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version)
        {
            if (!_wireBuffer.TryBeginRead(out int slotIndex, out int page, out int offset, out int length, out version))
            {
                return false;
            }

            try
            {
                var span = _wireBuffer.GetSpan(page, offset, length);
                writer.WriteUInt8((byte)_tokenBytes.Length);
                writer.WriteRaw(_tokenBytes);
                writer.WriteUInt32(version);
                writer.WriteInt32(length);
                writer.WriteRaw(span);
                return true;
            }
            finally
            {
                _wireBuffer.EndRead(slotIndex);
            }
        }

        public bool TryExtractSnapshotData(out byte[] data, out uint version)
        {
            if (!_wireBuffer.TryBeginRead(out int slotIndex, out int page, out int offset, out int length, out version))
            {
                data = Array.Empty<byte>();
                version = 0;
                return false;
            }

            try
            {
                data = _wireBuffer.GetSpan(page, offset, length).ToArray();
                return true;
            }
            finally
            {
                _wireBuffer.EndRead(slotIndex);
            }
        }

        public void SetAuthIds(params ushort[] clientIds)
        {
            ushort[] newAuths;
            ushort[] previousAuthorities;
            bool fireEvent;
            bool currentAuth;

            lock (_permissionBroadcastLock)
            {
                lock (_writeLock)
                {
                    if (!_permissions.SetAuthIds(clientIds, out previousAuthorities, out fireEvent, out currentAuth))
                    {
                        return;
                    }

                    if (!currentAuth)
                    {
                        ClearDirty();
                    }
                }

                newAuths = _permissions.Authorities;
                BroadcastAuthIfServer(newAuths);

                var mgr = _manager;
                if (Visibility == SyncVarVisibility.OwnerOnly && mgr != null)
                {
                    for (int i = 0; i < newAuths.Length; i++)
                    {
                        ushort id = newAuths[i];
                        if (IsVisibleTo(id))
                        {
                            mgr.SendSyncVarToClient(this, id);
                        }
                    }

                    for (int i = 0; i < previousAuthorities.Length; i++)
                    {
                        ushort id = previousAuthorities[i];
                        if (!IsVisibleTo(id))
                        {
                            mgr.SendEvictionToClient(this, id);
                        }
                    }
                }
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public void AddAuthority(ushort clientId)
        {
            bool fireEvent;
            bool currentAuth;
            ushort[] next;

            lock (_permissionBroadcastLock)
            {
                lock (_writeLock)
                {
                    if (!_permissions.AddAuthority(clientId, out next, out fireEvent, out currentAuth))
                    {
                        return;
                    }
                }

                if (next != null)
                {
                    BroadcastAuthIfServer(next);
                }

                if (Visibility == SyncVarVisibility.OwnerOnly && IsVisibleTo(clientId))
                {
                    _manager?.SendSyncVarToClient(this, clientId);
                }
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public void RemoveAuthority(ushort clientId)
        {
            bool fireEvent;
            bool currentAuth;
            ushort[] next;

            lock (_permissionBroadcastLock)
            {
                lock (_writeLock)
                {
                    if (!_permissions.RemoveAuthority(clientId, out next, out fireEvent, out currentAuth))
                    {
                        return;
                    }

                    if (!currentAuth)
                    {
                        ClearDirty();
                    }
                }

                if (next != null)
                {
                    BroadcastAuthIfServer(next);
                }

                if (Visibility == SyncVarVisibility.OwnerOnly && !IsVisibleTo(clientId))
                {
                    _manager?.SendEvictionToClient(this, clientId);
                }
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public void ApplyAuthUpdateFromRemote(ushort[] newAuthIds, bool deferEvent = false)
        {
            bool fireEvent;
            bool currentAuth;

            lock (_writeLock)
            {
                _permissions.ApplyAuthUpdateFromRemote(newAuthIds, deferEvent, out fireEvent, out currentAuth);
                if (!currentAuth)
                {
                    ClearDirty();
                }
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public void SetExclusionIds(params ushort[] clientIds)
        {
            lock (_permissionBroadcastLock)
            {
                if (!_permissions.SetExclusionIds(clientIds, out var newlyExcluded, out var newlyUnexcluded))
                {
                    return;
                }

                var mgr = _manager;
                if (newlyExcluded != null && mgr != null)
                {
                    for (int i = 0; i < newlyExcluded.Count; i++)
                    {
                        ushort id = newlyExcluded[i];
                        if (!IsVisibleTo(id))
                        {
                            mgr.SendEvictionToClient(this, id);
                        }
                    }
                }

                if (newlyUnexcluded != null && mgr != null)
                {
                    for (int i = 0; i < newlyUnexcluded.Count; i++)
                    {
                        ushort id = newlyUnexcluded[i];
                        if (IsVisibleTo(id))
                        {
                            mgr.SendSyncVarToClient(this, id);
                        }
                    }
                }
            }
        }

        public void SetExclusions(params ushort[] clientIds) => SetExclusionIds(clientIds);

        public void AddExclusion(ushort clientId)
        {
            lock (_permissionBroadcastLock)
            {
                if (_permissions.AddExclusion(clientId, out bool added))
                {
                    if (added && !IsVisibleTo(clientId))
                    {
                        _manager?.SendEvictionToClient(this, clientId);
                    }
                }
            }
        }

        public void RemoveExclusion(ushort clientId)
        {
            lock (_permissionBroadcastLock)
            {
                if (_permissions.RemoveExclusion(clientId, out bool removed))
                {
                    if (removed && IsVisibleTo(clientId))
                    {
                        _manager?.SendSyncVarToClient(this, clientId);
                    }
                }
            }
        }

        public void SetVisibility(SyncVarVisibility visibility)
        {
            lock (_permissionBroadcastLock)
            {
                if (!_permissions.SetVisibility(visibility, out bool changed, out var previousState, out var newState))
                {
                    return;
                }

                if (changed)
                {
                    if (TryMarkDirty())
                    {
                        _manager?.EnqueueDirty(this);
                    }

                    var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                    if (netManager != null && netManager.SessionManager != null &&
                        (netManager.Role == NetworkRole.Server || netManager.Role == NetworkRole.Host || netManager.Transport?.IsServer == true))
                    {
                        int active = netManager.SessionManager.GetActiveSessionCount();
                        int maxConfig = (netManager.Transport?.Config?.MaxConnectionCount ?? netManager.Config?.MaxConnectionCount ?? 10) + 4;
                        int maxClients = Math.Max(active + 4, maxConfig);
                        Span<ushort> allSessions = stackalloc ushort[maxClients];
                        int totalSessions = netManager.SessionManager.GetSessionIds(allSessions);

                        for (int s = 0; s < totalSessions; s++)
                        {
                            ushort sid = allSessions[s];
                            if (sid == ILiminalTransport.SERVER_ID && netManager.Role == NetworkRole.Host)
                                continue;

                            bool wasVisible = previousState.IsVisibleTo(sid, netManager);
                            bool isNowVisible = newState.IsVisibleTo(sid, netManager);

                            if (wasVisible && !isNowVisible)
                            {
                                _manager?.SendEvictionToClient(this, sid);
                            }
                            else if (!wasVisible && isNowVisible)
                            {
                                _manager?.SendSyncVarToClient(this, sid);
                            }
                        }
                    }
                }
            }
        }

        public void SetObserverIds(params ushort[] clientIds)
        {
            lock (_permissionBroadcastLock)
            {
                if (!_permissions.SetObserverIds(clientIds, out var newlyAdded, out var newlyRemoved))
                {
                    return;
                }

                var mgr = _manager;
                if (newlyAdded != null && mgr != null)
                {
                    for (int i = 0; i < newlyAdded.Count; i++)
                    {
                        ushort id = newlyAdded[i];
                        if (IsVisibleTo(id))
                        {
                            mgr.SendSyncVarToClient(this, id);
                        }
                    }
                }

                if (newlyRemoved != null && mgr != null)
                {
                    for (int i = 0; i < newlyRemoved.Count; i++)
                    {
                        ushort id = newlyRemoved[i];
                        if (!IsVisibleTo(id))
                        {
                            mgr.SendEvictionToClient(this, id);
                        }
                    }
                }
            }
        }

        public void SetObservers(params ushort[] clientIds) => SetObserverIds(clientIds);

        public void AddObserver(ushort clientId)
        {
            lock (_permissionBroadcastLock)
            {
                if (_permissions.AddObserver(clientId, out bool added))
                {
                    if (added && IsVisibleTo(clientId))
                    {
                        _manager?.SendSyncVarToClient(this, clientId);
                    }
                }
            }
        }

        public void RemoveObserver(ushort clientId)
        {
            lock (_permissionBroadcastLock)
            {
                if (_permissions.RemoveObserver(clientId, out bool removed))
                {
                    if (removed && !IsVisibleTo(clientId))
                    {
                        _manager?.SendEvictionToClient(this, clientId);
                    }
                }
            }
        }

        public void FirePendingEvents()
        {
            bool fireAuth = _permissions.TryFirePendingAuth(out bool authVal);
            bool fireVal = _valueCell.TryFirePendingValue(out T oldVal, out T newVal);

            try
            {
                if (fireAuth)
                {
                    OnAuthorityChanged?.Invoke(authVal);
                }
            }
            finally
            {
                if (fireVal)
                {
                    OnValueChanged?.Invoke(oldVal, newVal);
                }
            }
        }

        public void Clear()
        {
            if (!_permissions.IsServerAuthority("Clear")) return;

            T oldVal;
            bool fireEvent;
            lock (_writeLock)
            {
                _wireBuffer.Clear();
                _valueCell.Clear(out oldVal, out fireEvent);
                ClearDirty();
            }

            _manager?.BroadcastClear(this);

            if (fireEvent)
            {
                OnValueChanged?.Invoke(oldVal, default);
            }
        }

        public void ApplyClearFromRemote()
        {
            T oldVal;
            bool fireEvent;
            lock (_writeLock)
            {
                _wireBuffer.Clear();
                _valueCell.Clear(out oldVal, out fireEvent);
                ClearDirty();
            }

            if (fireEvent)
            {
                OnValueChanged?.Invoke(oldVal, default);
            }
        }

        internal void InternalTeardown()
        {
            Unregister();
            _localSlot = ushort.MaxValue;
            SetLocallyVisible(false);
            lock (_writeLock)
            {
                _permissions.ClearPendingAuth();
                _permissions.UpdateManager(null);
                _valueCell.ClearPendingValue();
                _wireBuffer.Dispose();
                _wireBuffer.UpdateSlab(null);
                ClearDirty();
                Volatile.Write(ref _manager, null);
            }
        }

        void ISyncVarInternal.InternalTeardown() => InternalTeardown();

        internal void ResetSession()
        {
            _localSlot = ushort.MaxValue;
            SetLocallyVisible(false);
            lock (_writeLock)
            {
                _permissions.ClearPendingAuth();
                _permissions.UpdateManager(null);
                _valueCell.ClearPendingValue();
                _wireBuffer.Clear();
                _wireBuffer.UpdateSlab(null);
                ClearDirty();
                Volatile.Write(ref _manager, null);
            }
        }

        void ISyncVarInternal.ResetSession() => ResetSession();

        private void Unregister()
        {
            _manager?.UnregisterSyncVar(this);
        }

        private void BroadcastAuthIfServer(ushort[] next)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (netManager != null && (netManager.Role == NetworkRole.Server || netManager.Role == NetworkRole.Host))
            {
                _manager?.BroadcastAuthChange(Token, next);
            }
        }
    }
}