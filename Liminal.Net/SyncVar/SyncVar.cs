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
        void SetLocallyVisibleDeferred(bool isVisible);
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
        bool TryReadSnapshotData(out byte[] data, out uint version);
        bool ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false, ushort senderId = 0);
        bool ApplyCachedBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force);
        bool TryPublishSlot(ushort slot);
        void DetachFromSlab();
        bool JoinedByAdoption { get; }
        bool ExcludedFromAdoption { get; set; }
        void Adopt(SyncVarManager manager);
        ClientWriteResult ApplyClientWrite(ReadOnlySequence<byte> incomingBytes, ushort senderId, MessagePackSerializerOptions options);
        void RefreshClientAuthority();
        object VisibilityLock { get; }
        void FirePendingEvents();
        void InternalTeardown();
        void ResetSession(bool clearRemoteAuthority);
    }

    public enum ClientWriteResult
    {
        Applied,
        Unchanged,
        Unauthorized,
        Rejected
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
        private volatile bool _joinedByAdoption;
        private volatile bool _excludedFromAdoption;

        private const int NoPendingVisibility = 0;
        private const int PendingHidden = 1;
        private const int PendingVisible = 2;
        private int _pendingVisibility = NoPendingVisibility;

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
            get
            {
                var currentMgr = _manager;
                if (_localSlot == ushort.MaxValue || currentMgr == null || currentMgr.IsTornDown)
                {
                    EnsureRegistered();
                }
                return _valueCell.Value;
            }
            set
            {
                if (IsDisposed) return;
                var currentMgr = _manager;
                if (_localSlot == ushort.MaxValue || currentMgr == null || currentMgr.IsTornDown)
                {
                    EnsureRegistered();
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

            _manager = manager;

            _permissions = new SyncVarPermissions(_manager, token, visibility);
            _valueCell = new SyncVarValueCell<T>(initialValue);
            //No slab until a manager actually registers it, an unregistered syncvar writing there would overwrite memory the manager hands out
            _wireBuffer = new SyncVarWireBuffer(null);

            //Add to directory before joining so a manager created at the same time either sees us or we see it
            SyncVarDirectory.Add(this);

            var target = manager ?? SyncVarManager.GetManagerForRegistration();
            if (target != null)
            {
                bool joined;
                lock (_registrationLock)
                {
                    joined = TryJoinLocked(target, JoinReason.Construction);
                }
                FirePendingEvents();
                if (joined)
                {
                    RefreshClientAuthority();
                }
            }
        }

        public void SetLocallyVisible(bool isVisible)
        {
            if (_permissions.SetLocallyVisible(isVisible))
            {
                OnVisibilityScopeChanged?.Invoke(isVisible);
            }
        }

        void ISyncVarInternal.SetLocallyVisibleDeferred(bool isVisible) => SetLocallyVisibleDeferred(isVisible);

        // For callers holding a lock: the event is raised later by FirePendingEvents.
        private void SetLocallyVisibleDeferred(bool isVisible)
        {
            if (_permissions.SetLocallyVisible(isVisible))
            {
                Volatile.Write(ref _pendingVisibility, isVisible ? PendingVisible : PendingHidden);
            }
        }

        public void SetDirty()
        {
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
            MessagePackSerializer.Serialize(writer, _valueCell.Value, options);
        }

        private enum JoinReason
        {
            Construction,
            Use,
            Adoption
        }

        private void EnsureRegistered(SyncVarManager activeMgr = null)
        {
            if (IsDisposed) return;

            var mgr = activeMgr;
            if (mgr == null)
            {
                var currentMgr = _manager;
                if (currentMgr != null && currentMgr.IsTornDown)
                {
                    currentMgr = null;
                }
                mgr = currentMgr ?? SyncVarManager.GetManagerForRegistration();
            }

            if (mgr == null || mgr.IsTornDown) return;

            if (_manager != mgr || _localSlot == ushort.MaxValue)
            {
                bool joined;
                lock (_registrationLock)
                {
                    joined = TryJoinLocked(mgr, JoinReason.Use);
                }
                if (joined)
                {
                    FirePendingEvents();
                    RefreshClientAuthority();
                }
            }
        }

        bool ISyncVarInternal.JoinedByAdoption => _joinedByAdoption;

        //Only written under owning manager's _unboundLock
        bool ISyncVarInternal.ExcludedFromAdoption
        {
            get => _excludedFromAdoption;
            set => _excludedFromAdoption = value;
        }

        void ISyncVarInternal.Adopt(SyncVarManager manager)
        {
            if (manager == null) return;
            bool joined;
            lock (_registrationLock)
            {
                joined = TryJoinLocked(manager, JoinReason.Adoption);
            }
            if (joined)
            {
                FirePendingEvents();
                RefreshClientAuthority();
            }
        }

        //Server side, unregistered means authorized clients lose authority, registered again means they get value version and authority back
        //under _permissionBroadcastLock so it cant get reordered with grants and revokes
        void ISyncVarInternal.RefreshClientAuthority() => RefreshClientAuthority();

        //Visibility changes and their evictions happen under this, the server flush holds it while it targets and sends
        object ISyncVarInternal.VisibilityLock => _permissionBroadcastLock;

        private void RefreshClientAuthority()
        {
            lock (_permissionBroadcastLock)
            {
                var mgr = _manager;
                if (mgr == null) return;

                var authorities = _permissions.Authorities;
                if (authorities.Length == 0) return;

                if (!IsDisposed && _localSlot != ushort.MaxValue)
                {
                    for (int i = 0; i < authorities.Length; i++)
                    {
                        if (IsVisibleTo(authorities[i]))
                        {
                            mgr.SendSyncVarToClient(this, authorities[i]);
                        }
                    }
                }
                else
                {
                    mgr.BroadcastAuthChange(Token, Array.Empty<ushort>());
                }
            }
        }

        //Only place a syncvar moves between managers, never takes it from a live manager
        //torn down owner releases it first so its slab can be reused safely
        private bool TryJoinLocked(SyncVarManager mgr, JoinReason reason)
        {
            if (IsDisposed || mgr.IsTornDown) return false;

            var current = _manager;
            bool sameManager = ReferenceEquals(current, mgr);
            if (sameManager)
            {
                if (_localSlot != ushort.MaxValue) return false;
            }
            else if (current != null && !current.IsTornDown)
            {
                return false;
            }

            if (!sameManager)
            {
                current?.ReleaseSyncVar(this);
                _manager = mgr;
                _permissions.UpdateManager(mgr);
            }

            if (reason == JoinReason.Construction)
            {
                _joinedByAdoption = false;
            }
            else if (reason == JoinReason.Adoption && !sameManager)
            {
                _joinedByAdoption = true;
            }

            return mgr.RegisterSyncVarDeferred(this,
                warnOnConflict: reason == JoinReason.Construction,
                replaceAdopted: reason == JoinReason.Construction,
                forAdoption: reason == JoinReason.Adoption);
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

            T oldValueWithNetwork = default;
            bool fireChangedWithNetwork = false;
            bool unauthorized = false;
            bool committed = false;

            //Unregister and session resets detach the slab under _writeLock, a write landing after that registers again and retries once
            //if that fails too the value stays local
            for (int attempt = 0; attempt < 2 && !committed; attempt++)
            {
                EnsureRegistered(activeMgr);

                lock (_writeLock)
                {
                    if (IsDisposed) return;

                    unauthorized = !HasAuthority;
                    if (unauthorized) break;

                    if (!_valueCell.TryPrepareUpdate(newValue, force, out uint nextVersion))
                    {
                        return;
                    }

                    _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
                    _threadLocalWriter.Clear();
                    MessagePackSerializer.Serialize(_threadLocalWriter, newValue, activeMgr.SerializerOptions);

                    var result = _wireBuffer.WriteBytes(_threadLocalWriter.WrittenSpan, nextVersion, force);
                    if (result == WireWriteResult.Stale || result == WireWriteResult.Disposed)
                    {
                        return;
                    }

                    if (result == WireWriteResult.NoSlab && attempt == 0)
                    {
                        continue;
                    }

                    _valueCell.CommitUpdate(newValue, nextVersion, deferEvent: false, out oldValueWithNetwork, out fireChangedWithNetwork);
                    committed = true;

                    if (result == WireWriteResult.Written)
                    {
                        TryMarkDirty();
                    }
                }
            }

            if (unauthorized)
            {
                var netManager = activeMgr.AttachedManager ?? LiminalNetworkManager.Instance;
                ushort myId = netManager?.localID ?? 0;
                LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                return;
            }

            if (fireChangedWithNetwork)
            {
                OnValueChanged?.Invoke(oldValueWithNetwork, newValue);
            }
        }

        public bool ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false, ushort senderId = 0)
        {
            if (IsDisposed) return false;
            var currentMgr = _manager;
            if (_localSlot == ushort.MaxValue || currentMgr == null || currentMgr.IsTornDown)
            {
                EnsureRegistered();
            }
            return ApplyRemoteBytesCore(incomingBytes, newVersion, options, force, deferEvent, senderId);
        }

        bool ISyncVarInternal.TryPublishSlot(ushort slot)
        {
            lock (_writeLock)
            {
                if (IsDisposed) return false;
                _localSlot = slot;
                return true;
            }
        }

        //Slab can get reset and reused before we register again so dont keep old slab coords, value stays in the value cell
        void ISyncVarInternal.DetachFromSlab()
        {
            lock (_writeLock)
            {
                _localSlot = ushort.MaxValue;
                _wireBuffer.UpdateSlab(null);
            }
        }

        bool ISyncVarInternal.ApplyCachedBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force)
        {
            if (IsDisposed) return false;
            return ApplyRemoteBytesCore(incomingBytes, newVersion, options, force, deferEvent: true, senderId: 0);
        }

        private bool ApplyRemoteBytesCore(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force, bool deferEvent, ushort senderId)
        {
            uint currentVer = _valueCell.Version;
            if (!force && newVersion < currentVer && currentVer != 0) return false;

            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (!force && newVersion == currentVer && currentVer != 0 && netManager != null && netManager.Role != NetworkRole.Client && senderId != 0)
            {
                return false;
            }

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

                //Server echoes our own writes back with its own versions, anything not newer than local is that echo
                //or a server write our in flight writes will overwrite anyway so keep local
                uint localVersion = _valueCell.Version;
                if (!force && senderId == 0 && localVersion != 0 && newVersion <= localVersion && _permissions.HasAuthority)
                {
                    return false;
                }

                if (!_valueCell.TryApplyRemote(deserialized, newVersion, force, deferEvent, out oldValue, out fireEvent))
                {
                    return false;
                }

                WriteWire(incomingBytes, newVersion, force);

                if (force || (netManager != null && netManager.Role == NetworkRole.Client))
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

        private WireWriteResult WriteWire(ReadOnlySequence<byte> bytes, uint version, bool force)
        {
            if (bytes.IsSingleSegment)
            {
                return _wireBuffer.WriteBytes(bytes.FirstSpan, version, force);
            }

            _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
            _threadLocalWriter.Clear();
            foreach (var mem in bytes)
            {
                _threadLocalWriter.Write(mem.Span);
            }
            return _wireBuffer.WriteBytes(_threadLocalWriter.WrittenSpan, version, force);
        }

        //Server picks the version, a client chosen one could jump to uint.MaxValue and make every later write stale
        ClientWriteResult ISyncVarInternal.ApplyClientWrite(ReadOnlySequence<byte> incomingBytes, ushort senderId, MessagePackSerializerOptions options)
        {
            if (IsDisposed) return ClientWriteResult.Rejected;

            T value = MessagePackSerializer.Deserialize<T>(incomingBytes, options);
            T oldValue;
            bool fireEvent;

            lock (_writeLock)
            {
                if (IsDisposed) return ClientWriteResult.Rejected;
                if (!_permissions.IsAuthorized(senderId)) return ClientWriteResult.Unauthorized;

                if (!_valueCell.TryPrepareUpdate(value, force: false, out uint nextVersion))
                {
                    return ClientWriteResult.Unchanged;
                }

                var result = WriteWire(incomingBytes, nextVersion, force: false);
                if (result == WireWriteResult.Disposed) return ClientWriteResult.Rejected;

                _valueCell.CommitUpdate(value, nextVersion, deferEvent: false, out oldValue, out fireEvent);
                TryMarkDirty();
            }

            if (fireEvent)
            {
                OnValueChanged?.Invoke(oldValue, value);
            }
            return ClientWriteResult.Applied;
        }

        public bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version)
        {
            var currentMgr = _manager;
            if (_localSlot == ushort.MaxValue || currentMgr == null || currentMgr.IsTornDown)
            {
                EnsureRegistered();
            }

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
            var currentMgr = _manager;
            if (_localSlot == ushort.MaxValue || currentMgr == null || currentMgr.IsTornDown)
            {
                EnsureRegistered();
            }

            return TryReadSnapshotData(out data, out version);
        }

        //Unlike TryExtractSnapshotData never registers, the server answering a client must not bring back a syncvar the game unregistered
        bool ISyncVarInternal.TryReadSnapshotData(out byte[] data, out uint version) => TryReadSnapshotData(out data, out version);

        private bool TryReadSnapshotData(out byte[] data, out uint version)
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

                var mgr = _manager;
                if (mgr != null)
                {
                    for (int i = 0; i < newAuths.Length; i++)
                    {
                        ushort id = newAuths[i];
                        if (Array.IndexOf(previousAuthorities, id) < 0 && IsVisibleTo(id))
                        {
                            mgr.SendSyncVarToClient(this, id);
                        }
                    }
                }

                BroadcastAuthIfServer(newAuths);

                if (Visibility == SyncVarVisibility.OwnerOnly && mgr != null)
                {
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
                    if (IsVisibleTo(clientId))
                    {
                        _manager?.SendSyncVarToClient(this, clientId);
                    }
                    BroadcastAuthIfServer(next);
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
            int visibility = Interlocked.Exchange(ref _pendingVisibility, NoPendingVisibility);
            bool fireAuth = _permissions.TryFirePendingAuth(out bool authVal);
            bool fireVal = _valueCell.TryFirePendingValue(out T oldVal, out T newVal);

            try
            {
                if (visibility != NoPendingVisibility)
                {
                    OnVisibilityScopeChanged?.Invoke(visibility == PendingVisible);
                }
            }
            finally
            {
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
            SyncVarManager previousManager;
            ushort claimedSlot;
            bool visibilityChanged;

            lock (_writeLock)
            {
                previousManager = _manager;
                claimedSlot = _localSlot;
                _localSlot = ushort.MaxValue;
                visibilityChanged = _permissions.SetLocallyVisible(false);
                Volatile.Write(ref _pendingVisibility, NoPendingVisibility);
                _permissions.ClearPendingAuth();
                _permissions.UpdateManager(null);
                _valueCell.ClearPendingValue();
                _wireBuffer.Dispose();
                _wireBuffer.UpdateSlab(null);
                ClearDirty();
                Volatile.Write(ref _manager, null);
            }

            previousManager?.UnregisterClaimedSyncVar(this, claimedSlot);

            if (previousManager != null && _permissions.Authorities.Length > 0)
            {
                lock (_permissionBroadcastLock)
                {
                    previousManager.BroadcastAuthChange(Token, Array.Empty<ushort>());
                }
            }

            if (visibilityChanged)
            {
                OnVisibilityScopeChanged?.Invoke(false);
            }
        }

        void ISyncVarInternal.InternalTeardown() => InternalTeardown();

        internal void ResetSession(bool clearRemoteAuthority)
        {
            lock (_writeLock)
            {
                //Session ended, a syncvar constructed later with the same token takes its place
                _joinedByAdoption = true;
                _localSlot = ushort.MaxValue;
                SetLocallyVisibleDeferred(false);
                _permissions.ClearPendingAuth();
                if (clearRemoteAuthority)
                {
                    _permissions.ClearRemoteAuthorities();
                }
                _valueCell.ResetSessionState();
                _wireBuffer.Clear();
                _wireBuffer.UpdateSlab(null);
                ClearDirty();
            }
        }

        void ISyncVarInternal.ResetSession(bool clearRemoteAuthority) => ResetSession(clearRemoteAuthority);

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