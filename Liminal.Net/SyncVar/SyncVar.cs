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

        bool IsAuthorized(ushort clientId);
        bool IsExcluded(ushort clientId);
        bool ContainsExclusion(ushort clientId);

        void SetAuthIds(params ushort[] clientIds);
        void AddAuthority(ushort clientId);
        void RemoveAuthority(ushort clientId);
        void ApplyAuthUpdateFromRemote(ushort[] newAuthIds);

        void SetExclusionIds(params ushort[] clientIds);
        void SetExclusions(params ushort[] clientIds);
        void AddExclusion(ushort clientId);
        void RemoveExclusion(ushort clientId);

        void SetDirty();
        bool TryMarkDirty();
        void ClearDirty();
        bool IsDirty { get; }

        void AllocateSlots(int size);
        void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options);
        bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version);
        bool TryExtractSnapshotData(out byte[] data, out uint version);
        void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options);
    }

    public class SyncVar<T> : ISyncVarInternal, IDisposable
    {
        private const int STATE_IDLE = 0;
        private const int STATE_READING = 1;
        private const int STATE_SWAPPING = 2;

        private readonly object _authLock = new();
        private readonly object _exclusionLock = new();
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

        private readonly T[] _values = new T[2];
        private int _valueSeq = 0;
        private uint _version;
        private ushort[] _authIds = Array.Empty<ushort>();
        private ushort[] _exclusionIds = Array.Empty<ushort>();
        private readonly byte[] _tokenBytes;

        public event Action<bool> OnAuthorityChanged;

        public string Token { get; }
        public byte[] TokenBytes => _tokenBytes;
        public uint Version => Volatile.Read(ref _version);
        public int ActivePageIndex => _slots[Volatile.Read(ref _frontIndex)].PageIndex;
        public int ActivePageOffset => _slots[Volatile.Read(ref _frontIndex)].PageOffset;
        public int Length => _slotLengths[Volatile.Read(ref _frontIndex)];
        public ushort[] AuthIds => Volatile.Read(ref _authIds);
        public ushort[] ExclusionIds => Volatile.Read(ref _exclusionIds);
        public SyncVarManager Manager => _manager;
        public bool IsDirty => Volatile.Read(ref _isDirty) != 0;

        public event Action<T, T> OnValueChanged;

        public bool HasAuthority
        {
            get
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                if (netManager == null || netManager.Role == NetworkRole.None) return true;

                // Server/Host is ALWAYS authorized
                if (netManager.Role == NetworkRole.Server ||
                    netManager.Role == NetworkRole.Host ||
                    netManager.Transport?.IsServer == true ||
                    netManager.localID == ILiminalTransport.SERVER_ID)
                {
                    return true;
                }

                return IsAuthorized(netManager.localID);
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
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            _tokenBytes = System.Text.Encoding.UTF8.GetBytes(token);
            if (_tokenBytes.Length > 255)
            {
                throw new ArgumentException($"[SyncVar] Token '{token}' UTF-8 length exceeds 255 bytes.");
            }

            _values[0] = initialValue;
            _values[1] = initialValue;
            _manager = manager ?? SyncVarManager.GetManagerForRegistration();
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

            lock (_authLock)
            {
                if (clientIds == null || clientIds.Length == 0)
                {
                    CommitAuthUpdate(Array.Empty<ushort>());
                    return;
                }

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

                CommitAuthUpdate(cleanList.ToArray());
            }
        }

        public void AddAuthority(ushort clientId)
        {
            if (!IsServerAuthority("AddAuthority")) return;

            lock (_authLock)
            {
                var current = Volatile.Read(ref _authIds);
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        LiminalLogger.LogWarning($"[SyncVar] Client {clientId} already has authority on '{Token}'. Overwriting duplicate assignment.");
                        return;
                    }
                }

                var next = new ushort[current.Length + 1];
                Array.Copy(current, next, current.Length);
                next[current.Length] = clientId;

                CommitAuthUpdate(next);
            }
        }

        public void RemoveAuthority(ushort clientId)
        {
            if (!IsServerAuthority("RemoveAuthority")) return;

            lock (_authLock)
            {
                var current = Volatile.Read(ref _authIds);
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

                var next = new ushort[current.Length - 1];
                if (targetIndex > 0)
                {
                    Array.Copy(current, 0, next, 0, targetIndex);
                }
                if (targetIndex < current.Length - 1)
                {
                    Array.Copy(current, targetIndex + 1, next, targetIndex, current.Length - targetIndex - 1);
                }

                CommitAuthUpdate(next);
            }
        }

        public void ApplyAuthUpdateFromRemote(ushort[] newAuthIds)
        {
            lock (_authLock)
            {
                bool previousAuth = HasAuthority;
                Volatile.Write(ref _authIds, newAuthIds ?? Array.Empty<ushort>());

                bool currentAuth = HasAuthority;
                if (previousAuth != currentAuth)
                {
                    OnAuthorityChanged?.Invoke(currentAuth);
                }
            }
        }

        public bool IsAuthorized(ushort clientId)
        {
            if (clientId == ILiminalTransport.SERVER_ID) return true;

            var auth = Volatile.Read(ref _authIds);
            if (auth == null || auth.Length == 0) return false;
            for (int i = 0; i < auth.Length; i++)
            {
                if (auth[i] == clientId) return true;
            }
            return false;
        }

        private void CommitAuthUpdate(ushort[] newAuthIds)
        {
            bool previousAuth = HasAuthority;
            Volatile.Write(ref _authIds, newAuthIds);

            bool currentAuth = HasAuthority;
            if (previousAuth != currentAuth)
            {
                OnAuthorityChanged?.Invoke(currentAuth);
            }

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
            var excluded = Volatile.Read(ref _exclusionIds);
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

            lock (_exclusionLock)
            {
                if (clientIds == null || clientIds.Length == 0)
                {
                    CommitExclusionUpdate(Array.Empty<ushort>());
                    return;
                }

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

                CommitExclusionUpdate(cleanList.ToArray());
            }
        }

        public void SetExclusions(params ushort[] clientIds) => SetExclusionIds(clientIds);

        public void AddExclusion(ushort clientId)
        {
            if (!IsServerAuthority("AddExclusion")) return;

            lock (_exclusionLock)
            {
                var current = Volatile.Read(ref _exclusionIds);
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
            }
        }

        public void RemoveExclusion(ushort clientId)
        {
            if (!IsServerAuthority("RemoveExclusion")) return;

            lock (_exclusionLock)
            {
                var current = Volatile.Read(ref _exclusionIds);
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
            }
        }

        private void CommitExclusionUpdate(ushort[] newExclusionIds)
        {
            Volatile.Write(ref _exclusionIds, newExclusionIds);
        }

        #endregion

        private bool IsServerAuthority(string actionName)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (netManager == null || netManager.Role == NetworkRole.None)
            {
                // Standalone / offline / initialization mode allows configuration
                return true;
            }

            if (netManager.Role == NetworkRole.Server ||
                netManager.Role == NetworkRole.Host ||
                netManager.Transport?.IsServer == true ||
                netManager.localID == ILiminalTransport.SERVER_ID)
            {
                return true;
            }

            ushort myId = netManager.localID;
            LiminalLogger.LogError($"[SyncVar] Unauthorized {actionName} blocked! Client {myId} is not the server. Modifications for '{Token}' can only be performed by the server.");
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
                old = _values[currentFront];
                _values[backIndex] = newValue;
                Volatile.Write(ref _version, newVersion);

                var spinner = new SpinWait();
                while (Interlocked.CompareExchange(ref _gate, STATE_SWAPPING, STATE_IDLE) != STATE_IDLE)
                {
                    spinner.SpinOnce();
                }

                try
                {
                    Interlocked.Increment(ref _valueSeq);
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

        public void ApplyRemoteBytes(ReadOnlySequence<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options)
        {
            if (newVersion <= _version && _version != 0) return;

            T deserialized = MessagePackSerializer.Deserialize<T>(incomingBytes, options);
            T old;

            lock (_writeLock)
            {
                if (newVersion <= _version && _version != 0) return;

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

                old = _values[currentFront];
                _values[backIndex] = deserialized;
                Volatile.Write(ref _version, newVersion);

                var spinner = new SpinWait();
                while (Interlocked.CompareExchange(ref _gate, STATE_SWAPPING, STATE_IDLE) != STATE_IDLE)
                {
                    spinner.SpinOnce();
                }

                try
                {
                    Interlocked.Increment(ref _valueSeq);
                    Volatile.Write(ref _frontIndex, backIndex);
                    Interlocked.Increment(ref _valueSeq);
                }
                finally
                {
                    Volatile.Write(ref _gate, STATE_IDLE);
                }

            }

            OnValueChanged?.Invoke(old, deserialized);
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
        }
    }
}