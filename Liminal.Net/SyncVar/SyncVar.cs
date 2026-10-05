using Liminal.Net.Core;
using Liminal.Net.Interfaces;
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
        int ActivePageIndex { get; }
        int ActivePageOffset { get; }
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

        void AllocateSlots(int size);
        void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options);
        bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version);
        bool TryExtractSnapshotData(out byte[] data, out uint version);
        void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false);
        void FirePendingEvents();
    }

    public class SyncVar<T> : ISyncVarInternal, IDisposable
    {
        private const int STATE_IDLE = 0;
        private const int STATE_READING = 1;
        private const int STATE_SWAPPING = 2;

        private readonly object _stateLock = new();
        private readonly object _writeLock = new();
        private readonly SyncVarManager _manager;

        [ThreadStatic]
        private static ArrayBufferWriter<byte> _threadLocalWriter;

        private readonly (int PageIndex, int PageOffset)[] _slots = new (int, int)[2];
        private readonly int[] _slotCapacities = new int[2];
        private readonly int[] _slotLengths = new int[2];
        private readonly uint[] _slotVersions = new uint[2];

        private int _frontIndex = 0;
        private int _gate = STATE_IDLE;
        private int _isDirty = 0;
        private int _isLocallyVisible = 0;

        private readonly T[] _values = new T[2];
        private int _valueSeq = 0;
        private uint _version;
        private SyncVarVisibilityState _visibilityState;
        private readonly byte[] _tokenBytes;

        private T _pendingOldValue;
        private T _pendingNewValue;
        private bool _hasPendingValueChanged;
        private bool _pendingAuthValue;
        private bool _hasPendingAuthChanged;

        public event Action<bool> OnAuthorityChanged;
        public event Action<bool> OnVisibilityScopeChanged;

        public string Token { get; }
        public byte[] TokenBytes => _tokenBytes;
        public uint Version
        {
            get
            {
                var spinner = new SpinWait();
                while (true)
                {
                    int seq1 = Volatile.Read(ref _valueSeq);
                    if ((seq1 & 1) != 0)
                    {
                        spinner.SpinOnce();
                        continue;
                    }

                    uint ver = Volatile.Read(ref _version);

                    Thread.MemoryBarrier();

                    int seq2 = Volatile.Read(ref _valueSeq);
                    if (seq1 == seq2)
                    {
                        return ver;
                    }

                    spinner.SpinOnce();
                }
            }
        }
        public int ActivePageIndex => _slots[Volatile.Read(ref _frontIndex)].PageIndex;
        public int ActivePageOffset => _slots[Volatile.Read(ref _frontIndex)].PageOffset;
        public int Length => _slotLengths[Volatile.Read(ref _frontIndex)];
        public SyncVarVisibilityState VisibilityState => Volatile.Read(ref _visibilityState);
        public ushort[] AuthIds => VisibilityState.Authorities;
        public ushort[] ExclusionIds => VisibilityState.Exclusions;
        public ushort[] ObserverIds => VisibilityState.Observers;
        public SyncVarVisibility Visibility
        {
            get => VisibilityState.Mode;
            set => SetVisibility(value);
        }
        public bool IsVisible
        {
            get
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                if (netManager != null && (netManager.Role == NetworkRole.Server || netManager.Role == NetworkRole.Host || netManager.Transport?.IsServer == true))
                {
                    return true;
                }
                return Volatile.Read(ref _isLocallyVisible) != 0;
            }
        }

        public void SetLocallyVisible(bool isVisible)
        {
            int newVal = isVisible ? 1 : 0;
            int oldVal = Interlocked.Exchange(ref _isLocallyVisible, newVal);
            if (oldVal != newVal)
            {
                OnVisibilityScopeChanged?.Invoke(isVisible);
            }
        }
        public SyncVarManager Manager => _manager;
        public bool IsDirty => Volatile.Read(ref _isDirty) != 0;

        public event Action<T, T> OnValueChanged;

        public bool HasAuthority
        {
            get
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                if (netManager == null || netManager.Role == NetworkRole.None) return true;

                if (netManager.Role == NetworkRole.Client)
                {
                    ushort myId = netManager.localID;
                    if (myId == 0 || myId == ILiminalTransport.SERVER_ID) return false;
                    return IsAuthorized(myId);
                }

                // Server or Host is ALWAYS authorized
                if (netManager.Role == NetworkRole.Server ||
                    netManager.Role == NetworkRole.Host ||
                    netManager.Transport?.IsServer == true ||
                    netManager.localID == ILiminalTransport.SERVER_ID)
                {
                    return true;
                }

                return false;
            }
        }

        public T Value
        {
            get
            {
                var spinner = new SpinWait();
                while (true)
                {
                    int seq1 = Volatile.Read(ref _valueSeq);
                    if ((seq1 & 1) != 0)
                    {
                        spinner.SpinOnce();
                        continue;
                    }

                    int front = Volatile.Read(ref _frontIndex);
                    T val = _values[front];

                    Thread.MemoryBarrier();

                    int seq2 = Volatile.Read(ref _valueSeq);
                    if (seq1 == seq2)
                    {
                        return val;
                    }

                    spinner.SpinOnce();
                }
            }
            set
            {
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

            _manager = manager ?? SyncVarManager.GetManagerForRegistration();

            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            SyncVarVisibility initialMode = (netManager != null && netManager.Role == NetworkRole.Client)
                ? SyncVarVisibility.Public
                : visibility;

            _visibilityState = initialMode switch
            {
                SyncVarVisibility.ManualObservers => SyncVarVisibilityState.DefaultManualObservers,
                SyncVarVisibility.OwnerOnly => SyncVarVisibilityState.DefaultOwnerOnly,
                _ => SyncVarVisibilityState.DefaultPublic
            };

            _isLocallyVisible = (netManager != null && netManager.Role == NetworkRole.Client) ? 0 : 1;

            _values[0] = initialValue;
            _values[1] = initialValue;
            _manager.RegisterSyncVar(this);
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
            return Interlocked.Exchange(ref _isDirty, 1) == 0;
        }

        public void ClearDirty()
        {
            Volatile.Write(ref _isDirty, 0);
        }

        #region Authority Management

        public void SetAuthIds(params ushort[] clientIds)
        {
            if (!IsServerAuthority("SetAuthIds")) return;

            ushort[] next;
            bool fireEvent;
            bool currentAuth;
            ushort[] previousAuthorities;

            lock (_stateLock)
            {
                previousAuthorities = Volatile.Read(ref _visibilityState).Authorities;
                if (clientIds == null || clientIds.Length == 0)
                {
                    next = Array.Empty<ushort>();
                }
                else
                {
                    var cleanList = new List<ushort>(clientIds.Length);
                    for (int i = 0; i < clientIds.Length; i++)
                    {
                        ushort id = clientIds[i];
                        if (cleanList.Contains(id))
                        {
                            LiminalLogger.LogWarning($"[SyncVar] Duplicate authority ID {id} passed into '{Token}'. Overwriting duplicate entry.");
                            continue;
                        }
                        cleanList.Add(id);
                    }
                    next = cleanList.ToArray();
                }

                CommitAuthUpdate(next, out fireEvent, out currentAuth);
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }

            BroadcastAuthIfServer(next);

            if (Visibility == SyncVarVisibility.OwnerOnly && _manager != null)
            {
                for (int i = 0; i < next.Length; i++)
                {
                    ushort id = next[i];
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
            if (!IsServerAuthority("AddAuthority")) return;

            ushort[] next = null;
            bool fireEvent = false;
            bool currentAuth = false;

            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState).Authorities;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        LiminalLogger.LogWarning($"[SyncVar] Client {clientId} already has authority on '{Token}'. Overwriting duplicate assignment.");
                        return;
                    }
                }

                next = new ushort[current.Length + 1];
                Array.Copy(current, next, current.Length);
                next[current.Length] = clientId;

                CommitAuthUpdate(next, out fireEvent, out currentAuth);
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
            if (!IsServerAuthority("RemoveAuthority")) return;

            ushort[] next = null;
            bool fireEvent = false;
            bool currentAuth = false;

            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState).Authorities;
                int targetIndex = -1;

                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0)
                {
                    LiminalLogger.LogWarning($"[SyncVar] Cannot remove authority: Client {clientId} does not have authority on '{Token}'.");
                    return;
                }

                next = new ushort[current.Length - 1];
                if (targetIndex > 0)
                {
                    Array.Copy(current, 0, next, 0, targetIndex);
                }
                if (targetIndex < current.Length - 1)
                {
                    Array.Copy(current, targetIndex + 1, next, targetIndex, current.Length - targetIndex - 1);
                }

                CommitAuthUpdate(next, out fireEvent, out currentAuth);
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
            bool fireEvent = false;
            bool currentAuth = false;

            lock (_stateLock)
            {
                lock (_writeLock)
                {
                    bool previousAuth = HasAuthority;
                    var current = Volatile.Read(ref _visibilityState);
                    Volatile.Write(ref _visibilityState, current.WithAuthorities(newAuthIds ?? Array.Empty<ushort>()));

                    currentAuth = HasAuthority;
                    if (previousAuth != currentAuth)
                    {
                        if (!currentAuth)
                        {
                            ClearDirty();
                        }

                        if (deferEvent)
                        {
                            _pendingAuthValue = currentAuth;
                            _hasPendingAuthChanged = true;
                        }
                        else
                        {
                            _hasPendingAuthChanged = false;
                            fireEvent = true;
                        }
                    }
                }
            }

            if (fireEvent)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }
        }

        public bool IsAuthorized(ushort clientId)
        {
            if (clientId == ILiminalTransport.SERVER_ID) return true;

            var auth = VisibilityState.Authorities;
            if (auth == null || auth.Length == 0) return false;
            for (int i = 0; i < auth.Length; i++)
            {
                if (auth[i] == clientId) return true;
            }
            return false;
        }

        private void CommitAuthUpdate(ushort[] newAuthIds, out bool fireEvent, out bool currentAuth)
        {
            lock (_writeLock)
            {
                bool previousAuth = HasAuthority;
                var current = Volatile.Read(ref _visibilityState);
                Volatile.Write(ref _visibilityState, current.WithAuthorities(newAuthIds));

                currentAuth = HasAuthority;
                if (previousAuth != currentAuth && !currentAuth)
                {
                    ClearDirty();
                }

                fireEvent = previousAuth != currentAuth;
                if (fireEvent)
                {
                    _hasPendingAuthChanged = false;
                }
            }
        }

        private void BroadcastAuthIfServer(ushort[] newAuthIds)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (netManager != null && (netManager.Role == NetworkRole.Server || netManager.Role == NetworkRole.Host || netManager.Transport?.IsServer == true))
            {
                _manager.BroadcastAuthChange(Token, newAuthIds);
            }
        }

        #endregion

        #region Exclusion Management

        public bool IsExcluded(ushort clientId)
        {
            var excluded = VisibilityState.Exclusions;
            if (excluded == null || excluded.Length == 0) return false;
            for (int i = 0; i < excluded.Length; i++)
            {
                if (excluded[i] == clientId) return true;
            }
            return false;
        }

        public bool ContainsExclusion(ushort clientId) => IsExcluded(clientId);

        public void SetExclusionIds(params ushort[] clientIds)
        {
            if (!IsServerAuthority("SetExclusionIds")) return;

            List<ushort> newlyExcluded = null;
            List<ushort> newlyUnexcluded = null;

            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState).Exclusions;
                ushort[] next;

                if (clientIds == null || clientIds.Length == 0)
                {
                    next = Array.Empty<ushort>();
                }
                else
                {
                    var cleanList = new List<ushort>(clientIds.Length);
                    for (int i = 0; i < clientIds.Length; i++)
                    {
                        ushort id = clientIds[i];
                        if (cleanList.Contains(id))
                        {
                            LiminalLogger.LogWarning($"[SyncVar] Duplicate exclusion ID {id} passed into '{Token}'. Overwriting duplicate entry.");
                            continue;
                        }
                        cleanList.Add(id);
                    }
                    next = cleanList.ToArray();
                }

                for (int i = 0; i < next.Length; i++)
                {
                    ushort id = next[i];
                    bool wasInCurrent = false;
                    for (int j = 0; j < current.Length; j++)
                    {
                        if (current[j] == id) { wasInCurrent = true; break; }
                    }
                    if (!wasInCurrent)
                    {
                        newlyExcluded ??= new List<ushort>();
                        newlyExcluded.Add(id);
                    }
                }

                for (int i = 0; i < current.Length; i++)
                {
                    ushort id = current[i];
                    bool isInNext = false;
                    for (int j = 0; j < next.Length; j++)
                    {
                        if (next[j] == id) { isInNext = true; break; }
                    }
                    if (!isInNext)
                    {
                        newlyUnexcluded ??= new List<ushort>();
                        newlyUnexcluded.Add(id);
                    }
                }

                CommitExclusionUpdate(next);
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
            if (!IsServerAuthority("AddExclusion")) return;

            bool added = false;
            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState).Exclusions;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        LiminalLogger.LogWarning($"[SyncVar] Client {clientId} is already in exclusion list for '{Token}'. Overwriting duplicate assignment.");
                        return;
                    }
                }

                var next = new ushort[current.Length + 1];
                Array.Copy(current, next, current.Length);
                next[current.Length] = clientId;

                CommitExclusionUpdate(next);
                added = true;
            }

            if (added && !IsVisibleTo(clientId))
            {
                _manager?.SendEvictionToClient(this, clientId);
            }
        }

        public void RemoveExclusion(ushort clientId)
        {
            if (!IsServerAuthority("RemoveExclusion")) return;

            bool removed = false;
            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState).Exclusions;
                int targetIndex = -1;

                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0)
                {
                    LiminalLogger.LogWarning($"[SyncVar] Cannot remove exclusion: Client {clientId} is not in exclusion list for '{Token}'.");
                    return;
                }

                var next = new ushort[current.Length - 1];
                if (targetIndex > 0)
                {
                    Array.Copy(current, 0, next, 0, targetIndex);
                }
                if (targetIndex < current.Length - 1)
                {
                    Array.Copy(current, targetIndex + 1, next, targetIndex, current.Length - targetIndex - 1);
                }

                CommitExclusionUpdate(next);
                removed = true;
            }

            if (removed && IsVisibleTo(clientId))
            {
                _manager?.SendSyncVarToClient(this, clientId);
            }
        }

        private void CommitExclusionUpdate(ushort[] newExclusionIds)
        {
            var current = Volatile.Read(ref _visibilityState);
            Volatile.Write(ref _visibilityState, current.WithExclusions(newExclusionIds));
        }

        #endregion

        #region Observer Management

        public void SetVisibility(SyncVarVisibility visibility)
        {
            if (!IsServerAuthority("SetVisibility", silent: true)) return;

            bool changed = false;
            SyncVarVisibilityState previousState = null;
            SyncVarVisibilityState newState = null;

            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState);
                if (current.Mode != visibility)
                {
                    previousState = current;
                    newState = current.WithMode(visibility);
                    Volatile.Write(ref _visibilityState, newState);
                    changed = true;
                }
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

        public bool ContainsObserver(ushort clientId)
        {
            var observers = VisibilityState.Observers;
            if (observers == null || observers.Length == 0) return false;
            for (int i = 0; i < observers.Length; i++)
            {
                if (observers[i] == clientId) return true;
            }
            return false;
        }

        public bool IsVisibleTo(ushort clientId)
        {
            return VisibilityState.IsVisibleTo(clientId, _manager?.AttachedManager ?? LiminalNetworkManager.Instance);
        }

        public void SetObserverIds(params ushort[] clientIds)
        {
            if (!IsServerAuthority("SetObserverIds", silent: true)) return;

            List<ushort> newlyAdded = null;
            List<ushort> newlyRemoved = null;

            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState);
                var currentObservers = current.Observers;
                ushort[] next;

                if (clientIds == null || clientIds.Length == 0)
                {
                    next = Array.Empty<ushort>();
                }
                else
                {
                    var cleanList = new List<ushort>(clientIds.Length);
                    for (int i = 0; i < clientIds.Length; i++)
                    {
                        ushort id = clientIds[i];
                        if (!cleanList.Contains(id))
                        {
                            cleanList.Add(id);
                        }
                    }
                    next = cleanList.ToArray();
                }

                for (int i = 0; i < next.Length; i++)
                {
                    ushort id = next[i];
                    bool exists = false;
                    for (int j = 0; j < currentObservers.Length; j++)
                    {
                        if (currentObservers[j] == id) { exists = true; break; }
                    }
                    if (!exists)
                    {
                        newlyAdded ??= new List<ushort>();
                        newlyAdded.Add(id);
                    }
                }

                for (int i = 0; i < currentObservers.Length; i++)
                {
                    ushort id = currentObservers[i];
                    bool exists = false;
                    for (int j = 0; j < next.Length; j++)
                    {
                        if (next[j] == id) { exists = true; break; }
                    }
                    if (!exists)
                    {
                        newlyRemoved ??= new List<ushort>();
                        newlyRemoved.Add(id);
                    }
                }

                Volatile.Write(ref _visibilityState, current.WithObservers(next));
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
            if (!IsServerAuthority("AddObserver", silent: true)) return;

            bool added = false;
            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState);
                var currentObservers = current.Observers;
                for (int i = 0; i < currentObservers.Length; i++)
                {
                    if (currentObservers[i] == clientId)
                    {
                        return;
                    }
                }

                var next = new ushort[currentObservers.Length + 1];
                Array.Copy(currentObservers, next, currentObservers.Length);
                next[currentObservers.Length] = clientId;

                Volatile.Write(ref _visibilityState, current.WithObservers(next));
                added = true;
            }

            if (added && IsVisibleTo(clientId))
            {
                _manager?.SendSyncVarToClient(this, clientId);
            }
        }

        public void RemoveObserver(ushort clientId)
        {
            if (!IsServerAuthority("RemoveObserver", silent: true)) return;

            bool removed = false;
            lock (_stateLock)
            {
                var current = Volatile.Read(ref _visibilityState);
                var currentObservers = current.Observers;
                int targetIndex = -1;

                for (int i = 0; i < currentObservers.Length; i++)
                {
                    if (currentObservers[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0) return;

                var next = new ushort[currentObservers.Length - 1];
                if (targetIndex > 0)
                {
                    Array.Copy(currentObservers, 0, next, 0, targetIndex);
                }
                if (targetIndex < currentObservers.Length - 1)
                {
                    Array.Copy(currentObservers, targetIndex + 1, next, targetIndex, currentObservers.Length - targetIndex - 1);
                }

                Volatile.Write(ref _visibilityState, current.WithObservers(next));
                removed = true;
            }

            if (removed && !IsVisibleTo(clientId))
            {
                _manager?.SendEvictionToClient(this, clientId);
            }
        }

        #endregion

        private bool IsServerAuthority(string actionName, bool silent = false)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (netManager == null || netManager.Role == NetworkRole.None)
            {
                // Standalone / offline / initialization mode allows configuration
                return true;
            }

            if (netManager.Role == NetworkRole.Client)
            {
                if (!silent)
                {
                    ushort myId = netManager.localID;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized {actionName} blocked! Client {myId} is not the server. Modifications for '{Token}' can only be performed by the server.");
                }
                return false;
            }

            if (netManager.Role == NetworkRole.Server ||
                netManager.Role == NetworkRole.Host ||
                netManager.Transport?.IsServer == true ||
                netManager.localID == ILiminalTransport.SERVER_ID)
            {
                return true;
            }

            if (!silent)
            {
                ushort otherId = netManager.localID;
                LiminalLogger.LogError($"[SyncVar] Unauthorized {actionName} blocked! Client {otherId} is not the server. Modifications for '{Token}' can only be performed by the server.");
            }
            return false;
        }

        public void AllocateSlots(int size)
        {
            int capacity = Math.Max(size + 128, 256);

            _slots[0] = _manager.Slab.AllocateSlot(capacity);
            _slots[1] = _manager.Slab.AllocateSlot(capacity);
            _slotCapacities[0] = capacity;
            _slotCapacities[1] = capacity;
            _slotLengths[0] = size;
            _slotLengths[1] = size;
        }

        private void WriteFromOwningThread(T newValue, bool force = false)
        {
            if (!force && EqualityComparer<T>.Default.Equals(Value, newValue)) return;

            T old;
            bool shouldFireChanged = false;

            lock (_writeLock)
            {
                if (!HasAuthority)
                {
                    var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                    ushort myId = netManager?.localID ?? 0;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                    return;
                }

                int currentFront = Volatile.Read(ref _frontIndex);
                if (!force && EqualityComparer<T>.Default.Equals(_values[currentFront], newValue)) return;

                int backIndex = 1 - currentFront;

                _threadLocalWriter ??= new ArrayBufferWriter<byte>(256);
                _threadLocalWriter.Clear();
                MessagePackSerializer.Serialize(_threadLocalWriter, newValue, _manager.SerializerOptions);

                int writtenLength = _threadLocalWriter.WrittenCount;
                if (writtenLength > _slotCapacities[backIndex])
                {
                    int newCapacity = Math.Max(writtenLength + 128, _slotCapacities[backIndex] * 2);
                    _slots[backIndex] = _manager.Slab.AllocateSlot(newCapacity);
                    _slotCapacities[backIndex] = newCapacity;
                }

                var (page, offset) = _slots[backIndex];
                _threadLocalWriter.WrittenSpan.CopyTo(_manager.Slab.GetSpan(page, offset, writtenLength));

                uint newVersion = _version + 1;
                _slotLengths[backIndex] = writtenLength;
                _slotVersions[backIndex] = newVersion;
                if (_hasPendingValueChanged)
                {
                    old = _pendingOldValue;
                    _hasPendingValueChanged = false;
                    _pendingOldValue = default;
                    _pendingNewValue = default;
                }
                else
                {
                    old = _values[currentFront];
                }
                _values[backIndex] = newValue;

                var spinner = new SpinWait();
                while (Interlocked.CompareExchange(ref _gate, STATE_SWAPPING, STATE_IDLE) != STATE_IDLE)
                {
                    spinner.SpinOnce();
                }

                try
                {
                    Interlocked.Increment(ref _valueSeq);
                    Volatile.Write(ref _version, newVersion);
                    Volatile.Write(ref _frontIndex, backIndex);
                    Interlocked.Increment(ref _valueSeq);
                }
                finally
                {
                    Volatile.Write(ref _gate, STATE_IDLE);
                }

                if (TryMarkDirty())
                {
                    _manager?.EnqueueDirty(this);
                }

                shouldFireChanged = force || !EqualityComparer<T>.Default.Equals(old, newValue);
            }

            if (shouldFireChanged)
            {
                OnValueChanged?.Invoke(old, newValue);
            }
        }

        public bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version)
        {
            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref _gate, STATE_READING, STATE_IDLE) != STATE_IDLE)
            {
                spinner.SpinOnce();
            }

            try
            {
                int front = Volatile.Read(ref _frontIndex);
                var (page, offset) = _slots[front];
                int length = _slotLengths[front];
                version = _slotVersions[front];

                var span = _manager.Slab.GetSpan(page, offset, length);
                writer.WriteUInt8((byte)_tokenBytes.Length);
                writer.WriteRaw(_tokenBytes);
                writer.WriteUInt32(version);
                writer.WriteInt32(length);
                writer.WriteRaw(span);
                return true;
            }
            finally
            {
                Volatile.Write(ref _gate, STATE_IDLE);
            }
        }

        public bool TryExtractSnapshotData(out byte[] data, out uint version)
        {
            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref _gate, STATE_READING, STATE_IDLE) != STATE_IDLE)
            {
                spinner.SpinOnce();
            }

            try
            {
                int front = Volatile.Read(ref _frontIndex);
                var (page, offset) = _slots[front];
                int length = _slotLengths[front];
                version = _slotVersions[front];

                data = _manager.Slab.GetSpan(page, offset, length).ToArray();
                return true;
            }
            finally
            {
                Volatile.Write(ref _gate, STATE_IDLE);
            }
        }

        public void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options, bool force = false, bool deferEvent = false)
        {
            uint currentVer = Volatile.Read(ref _version);
            if (!force && newVersion <= currentVer && currentVer != 0) return;

            T deserialized = MessagePackSerializer.Deserialize<T>(incomingBytes, options);
            T old;
            bool fireEvent = false;

            lock (_writeLock)
            {
                currentVer = Volatile.Read(ref _version);
                if (!force && newVersion <= currentVer && currentVer != 0) return;

                int currentFront = Volatile.Read(ref _frontIndex);
                int backIndex = 1 - currentFront;
                int length = (int)incomingBytes.Length;

                if (length > _slotCapacities[backIndex])
                {
                    int newCapacity = Math.Max(length + 128, _slotCapacities[backIndex] * 2);
                    _slots[backIndex] = _manager.Slab.AllocateSlot(newCapacity);
                    _slotCapacities[backIndex] = newCapacity;
                }

                var (page, offset) = _slots[backIndex];
                incomingBytes.CopyTo(_manager.Slab.GetSpan(page, offset, length));
                _slotLengths[backIndex] = length;
                _slotVersions[backIndex] = newVersion;

                if (_hasPendingValueChanged)
                {
                    old = _pendingOldValue;
                }
                else
                {
                    old = _values[currentFront];
                }

                _values[backIndex] = deserialized;

                var spinner = new SpinWait();
                while (Interlocked.CompareExchange(ref _gate, STATE_SWAPPING, STATE_IDLE) != STATE_IDLE)
                {
                    spinner.SpinOnce();
                }

                try
                {
                    Interlocked.Increment(ref _valueSeq);
                    Volatile.Write(ref _version, newVersion);
                    Volatile.Write(ref _frontIndex, backIndex);
                    Interlocked.Increment(ref _valueSeq);
                }
                finally
                {
                    Volatile.Write(ref _gate, STATE_IDLE);
                }

                if (force)
                {
                    ClearDirty();
                }

                if (deferEvent)
                {
                    _pendingOldValue = old;
                    _pendingNewValue = deserialized;
                    _hasPendingValueChanged = true;
                }
                else
                {
                    _hasPendingValueChanged = false;
                    _pendingOldValue = default;
                    _pendingNewValue = default;
                    fireEvent = true;
                }
            }

            if (fireEvent)
            {
                OnValueChanged?.Invoke(old, deserialized);
            }

            SetLocallyVisible(true);
        }

        public void FirePendingEvents()
        {
            bool fireAuth = false;
            bool authValue = false;
            lock (_stateLock)
            {
                if (_hasPendingAuthChanged)
                {
                    fireAuth = true;
                    authValue = _pendingAuthValue;
                    _hasPendingAuthChanged = false;
                }
            }

            bool fireValue = false;
            T oldVal = default;
            T newVal = default;
            lock (_writeLock)
            {
                if (_hasPendingValueChanged)
                {
                    fireValue = true;
                    oldVal = _pendingOldValue;
                    newVal = _pendingNewValue;
                    _hasPendingValueChanged = false;
                    _pendingOldValue = default;
                    _pendingNewValue = default;
                }
            }

            try
            {
                if (fireAuth)
                {
                    OnAuthorityChanged?.Invoke(authValue);
                }
            }
            finally
            {
                if (fireValue)
                {
                    OnValueChanged?.Invoke(oldVal, newVal);
                }
            }
        }

        public void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options)
        {
            MessagePackSerializer.Serialize(writer, Value, options);
        }

        public void Unregister()
        {
            _manager?.UnregisterSyncVar(this);
        }

        public void Dispose()
        {
            Unregister();
            SetLocallyVisible(false);
            lock (_stateLock)
            {
                _hasPendingAuthChanged = false;
            }
            lock (_writeLock)
            {
                _hasPendingValueChanged = false;
                _pendingOldValue = default;
                _pendingNewValue = default;
            }
        }
    }
}