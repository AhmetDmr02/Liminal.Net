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
        ushort Id { get; set; }
        string Token { get; }
        uint Version { get; }
        int ActivePageIndex { get; }
        int ActivePageOffset { get; }
        int Length { get; }
        ushort[] AuthIds { get; }
        bool IsAuthorized(ushort clientId);

        void SetAuthIds(params ushort[] clientIds);
        void AddAuthority(ushort clientId);
        void RemoveAuthority(ushort clientId);
        void ApplyAuthUpdateFromRemote(ushort[] newAuthIds);

        void AllocateSlots(int size);
        void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options);
        bool TryWriteSlotForWire(ref MessagePackWriter writer, out uint version);
        void ApplyRemoteBytes(ReadOnlySpan<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options);
    }

    public class SyncVar<T> : ISyncVarInternal
    {
        private const int STATE_IDLE = 0;
        private const int STATE_READING = 1;
        private const int STATE_SWAPPING = 2;

        private readonly object _authLock = new();
        private readonly SyncVarManager _manager;

        private readonly (int PageIndex, int PageOffset)[] _slots = new (int, int)[2];
        private readonly int[] _slotCapacities = new int[2];
        private readonly int[] _slotLengths = new int[2];
        private readonly uint[] _slotVersions = new uint[2];

        private int _frontIndex = 0;
        private int _gate = STATE_IDLE;

        private T _value;
        private uint _version;
        private ushort[] _authIds = Array.Empty<ushort>();

        public event Action<bool> OnAuthorityChanged;

        public ushort Id { get; set; }
        public string Token { get; }
        public uint Version => Volatile.Read(ref _version);
        public int ActivePageIndex => _slots[Volatile.Read(ref _frontIndex)].PageIndex;
        public int ActivePageOffset => _slots[Volatile.Read(ref _frontIndex)].PageOffset;
        public int Length => _slotLengths[Volatile.Read(ref _frontIndex)];
        public ushort[] AuthIds => Volatile.Read(ref _authIds);
        public SyncVarManager Manager => _manager;

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
            get => _value;
            set
            {
                if (!HasAuthority)
                {
                    var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                    ushort myId = netManager?.localID ?? 0;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                    return;
                }

                WriteFromOwningThread(value);
            }
        }

        public SyncVar(string token, T initialValue = default, SyncVarManager manager = null)
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            _value = initialValue;

            _manager = manager ?? SyncVarManager.GetManagerForRegistration();
            _manager.RegisterSyncVar(this);
        }

        public void SetAuthIds(params ushort[] clientIds)
        {
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
                _manager.BroadcastAuthChange(Id, newAuthIds);
            }
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

        private void WriteFromOwningThread(T newValue)
        {
            if (EqualityComparer<T>.Default.Equals(_value, newValue)) return;

            int backIndex = 1 - Volatile.Read(ref _frontIndex);
            var (page, offset) = _slots[backIndex];

            var writer = FixedBufferWriter.ThreadInstance;
            byte[] pageArray = _manager.Slab.GetPage(page);
            writer.Reset(pageArray, offset, _slotCapacities[backIndex]);
            MessagePackSerializer.Serialize(writer, newValue, _manager.SerializerOptions);

            int writtenLength = writer.WrittenCount;
            uint newVersion = _version + 1;

            _slotLengths[backIndex] = writtenLength;
            _slotVersions[backIndex] = newVersion;
            T old = _value;
            _value = newValue;
            Volatile.Write(ref _version, newVersion);

            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref _gate, STATE_SWAPPING, STATE_IDLE) != STATE_IDLE)
            {
                spinner.SpinOnce();
            }

            try
            {
                Volatile.Write(ref _frontIndex, backIndex);
            }
            finally
            {
                Volatile.Write(ref _gate, STATE_IDLE);
            }

            _manager.DirtyBitset.SetDirty(Id);

            OnValueChanged?.Invoke(old, newValue);
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
                writer.WriteUInt16(Id);
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

        public void ApplyRemoteBytes(ReadOnlySpan<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options)
        {
            if (newVersion <= _version && _version != 0) return;

            T deserialized = MessagePackSerializer.Deserialize<T>(incomingBytes.ToArray(), options);
            T old = _value;
            _value = deserialized;
            Volatile.Write(ref _version, newVersion);

            int front = Volatile.Read(ref _frontIndex);
            var (page, offset) = _slots[front];
            incomingBytes.CopyTo(_manager.Slab.GetSpan(page, offset, incomingBytes.Length));
            _slotLengths[front] = incomingBytes.Length;
            _slotVersions[front] = newVersion;

            OnValueChanged?.Invoke(old, deserialized);
        }

        public void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options)
        {
            MessagePackSerializer.Serialize(writer, _value, options);
        }
    }
}