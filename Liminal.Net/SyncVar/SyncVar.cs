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

        void AllocateSlots(int size);
        void Clear();
        void ApplyClearFromRemote();
        void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options);
        bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version);
        bool TryExtractSnapshotData(out byte[] data, out uint version);
        void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false);
        void FirePendingEvents();
    }

    public class SyncVar<T> : ISyncVarInternal, IDisposable
    {
        private readonly SyncVarPermissions _permissions;
        private readonly SyncVarValueCell<T> _valueCell;
        private readonly SyncVarWireBuffer _wireBuffer;
        private readonly SyncVarManager _manager;
        private readonly byte[] _tokenBytes;
        private readonly object _writeLock = new();

        private int _isDirty = 0;

        [ThreadStatic]
        private static ArrayBufferWriter<byte> _threadLocalWriter;

        public event Action<T, T> OnValueChanged;
        public event Action<bool> OnAuthorityChanged;
        public event Action<bool> OnVisibilityScopeChanged;

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
        public bool IsDirty => Volatile.Read(ref _isDirty) != 0;
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

            _manager.RegisterSyncVar(this);
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
            if (Interlocked.Exchange(ref _isDirty, 1) != 0) return false;
            if (IsDisposed || !HasAuthority)
            {
                Volatile.Write(ref _isDirty, 0);
                return false;
            }
            return true;
        }

        public void ClearDirty()
        {
            Volatile.Write(ref _isDirty, 0);
        }

        void ISyncVarInternal.AllocateSlots(int size)
        {
            _wireBuffer.AllocateInitialSlots(size);
        }

        public void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options)
        {
            MessagePackSerializer.Serialize(writer, Value, options);
        }

        private void WriteFromOwningThread(T newValue, bool force = false)
        {
            if (IsDisposed) return;

            if (!HasAuthority)
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                ushort myId = netManager?.localID ?? 0;
                LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                return;
            }

            T oldValue;
            bool fireChanged;

            lock (_writeLock)
            {
                if (IsDisposed || !HasAuthority) return;

                _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
                _threadLocalWriter.Clear();
                MessagePackSerializer.Serialize(_threadLocalWriter, newValue, _manager.SerializerOptions);

                if (!_valueCell.TryUpdate(newValue, force, deferEvent: false, out oldValue, out fireChanged, out uint newVersion))
                {
                    return;
                }

                if (!_wireBuffer.WriteBytes(_threadLocalWriter.WrittenSpan, newVersion, force))
                {
                    return;
                }

                if (TryMarkDirty())
                {
                    _manager?.EnqueueDirty(this);
                }
            }

            if (fireChanged)
            {
                OnValueChanged?.Invoke(oldValue, newValue);
            }
        }

        public void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false)
        {
            if (IsDisposed) return;
            uint currentVer = _valueCell.Version;
            if (!force && newVersion <= currentVer && currentVer != 0) return;

            T deserialized = MessagePackSerializer.Deserialize<T>(incomingBytes, options);
            T oldValue;
            bool fireEvent;

            lock (_writeLock)
            {
                if (IsDisposed) return;

                if (!_valueCell.TryApplyRemote(deserialized, newVersion, force, deferEvent, out oldValue, out fireEvent))
                {
                    return;
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
            if (!_permissions.SetAuthIds(clientIds, out ushort[] previousAuthorities, out bool fireEvent, out bool currentAuth))
            {
                return;
            }

            if (!currentAuth)
            {
                ClearDirty();
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }

            var newAuths = _permissions.Authorities;
            BroadcastAuthIfServer(newAuths);

            if (Visibility == SyncVarVisibility.OwnerOnly && _manager != null)
            {
                for (int i = 0; i < newAuths.Length; i++)
                {
                    ushort id = newAuths[i];
                    if (IsVisibleTo(id))
                    {
                        _manager.SendSyncVarToClient(this, id);
                    }
                }

                for (int i = 0; i < previousAuthorities.Length; i++)
                {
                    ushort id = previousAuthorities[i];
                    if (!IsVisibleTo(id))
                    {
                        _manager.SendEvictionToClient(this, id);
                    }
                }
            }
        }

        public void AddAuthority(ushort clientId)
        {
            if (!_permissions.AddAuthority(clientId, out ushort[] next, out bool fireEvent, out bool currentAuth))
            {
                return;
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
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

        public void RemoveAuthority(ushort clientId)
        {
            if (!_permissions.RemoveAuthority(clientId, out ushort[] next, out bool fireEvent, out bool currentAuth))
            {
                return;
            }

            if (!currentAuth)
            {
                ClearDirty();
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
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

        public void ApplyAuthUpdateFromRemote(ushort[] newAuthIds, bool deferEvent = false)
        {
            _permissions.ApplyAuthUpdateFromRemote(newAuthIds, deferEvent, out bool fireEvent, out bool currentAuth);
            if (!currentAuth)
            {
                ClearDirty();
            }
            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public void SetExclusionIds(params ushort[] clientIds)
        {
            if (!_permissions.SetExclusionIds(clientIds, out var newlyExcluded, out var newlyUnexcluded))
            {
                return;
            }

            if (newlyExcluded != null && _manager != null)
            {
                for (int i = 0; i < newlyExcluded.Count; i++)
                {
                    ushort id = newlyExcluded[i];
                    if (!IsVisibleTo(id))
                    {
                        _manager.SendEvictionToClient(this, id);
                    }
                }
            }

            if (newlyUnexcluded != null && _manager != null)
            {
                for (int i = 0; i < newlyUnexcluded.Count; i++)
                {
                    ushort id = newlyUnexcluded[i];
                    if (IsVisibleTo(id))
                    {
                        _manager.SendSyncVarToClient(this, id);
                    }
                }
            }
        }

        public void SetExclusions(params ushort[] clientIds) => SetExclusionIds(clientIds);

        public void AddExclusion(ushort clientId)
        {
            if (_permissions.AddExclusion(clientId, out bool added))
            {
                if (added && !IsVisibleTo(clientId))
                {
                    _manager?.SendEvictionToClient(this, clientId);
                }
            }
        }

        public void RemoveExclusion(ushort clientId)
        {
            if (_permissions.RemoveExclusion(clientId, out bool removed))
            {
                if (removed && IsVisibleTo(clientId))
                {
                    _manager?.SendSyncVarToClient(this, clientId);
                }
            }
        }

        public void SetVisibility(SyncVarVisibility visibility)
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
                    int maxClients = netManager.Transport.Config.MaxConnectionCount + 2;
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

        public void SetObserverIds(params ushort[] clientIds)
        {
            if (!_permissions.SetObserverIds(clientIds, out var newlyAdded, out var newlyRemoved))
            {
                return;
            }

            if (newlyAdded != null && _manager != null)
            {
                for (int i = 0; i < newlyAdded.Count; i++)
                {
                    ushort id = newlyAdded[i];
                    if (IsVisibleTo(id))
                    {
                        _manager.SendSyncVarToClient(this, id);
                    }
                }
            }

            if (newlyRemoved != null && _manager != null)
            {
                for (int i = 0; i < newlyRemoved.Count; i++)
                {
                    ushort id = newlyRemoved[i];
                    if (!IsVisibleTo(id))
                    {
                        _manager.SendEvictionToClient(this, id);
                    }
                }
            }
        }

        public void SetObservers(params ushort[] clientIds) => SetObserverIds(clientIds);

        public void AddObserver(ushort clientId)
        {
            if (_permissions.AddObserver(clientId, out bool added))
            {
                if (added && IsVisibleTo(clientId))
                {
                    _manager?.SendSyncVarToClient(this, clientId);
                }
            }
        }

        public void RemoveObserver(ushort clientId)
        {
            if (_permissions.RemoveObserver(clientId, out bool removed))
            {
                if (removed && !IsVisibleTo(clientId))
                {
                    _manager?.SendEvictionToClient(this, clientId);
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

        public void Dispose()
        {
            Unregister();
            SetLocallyVisible(false);
            lock (_writeLock)
            {
                _permissions.ClearPendingAuth();
                _valueCell.ClearPendingValue();
                _wireBuffer.Dispose();
                ClearDirty();
            }
        }

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