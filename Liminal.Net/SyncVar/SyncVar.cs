using Liminal.Net.Core;
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
        bool TryReadSlotForWire(Span<byte> destination, out uint version, out int length);
        void ApplyRemoteBytes(ReadOnlySpan<byte> incomingBytes, uint newVersion, MessagePackSerializerOptions options);
    }


    public class SyncVar<T> : ISyncVarInternal
    {
        private const int STATE_IDLE = 0;
        private const int STATE_READING = 1;
        private const int STATE_SWAPPING = 2;

        private readonly object _authLock = new();

        private readonly (int PageIndex, int PageOffset)[] _slots = new (int, int)[2];
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
        public uint Version => _slotVersions[Volatile.Read(ref _frontIndex)];
        public int ActivePageIndex => _slots[Volatile.Read(ref _frontIndex)].PageIndex;
        public int ActivePageOffset => _slots[Volatile.Read(ref _frontIndex)].PageOffset;
        public int Length => _slotLengths[Volatile.Read(ref _frontIndex)];
        public ushort[] AuthIds => Volatile.Read(ref _authIds);

        public event Action<T, T> OnValueChanged;

        public bool HasAuthority
        {
            get
            {
                var manager = LiminalNetworkManager.Instance;
                if (manager == null || manager.Role == NetworkRole.None) return true;
                if (manager.Role == NetworkRole.Server || manager.Role == NetworkRole.Host) return true;

                return IsAuthorized(manager.localID);
            }
        }

        public T Value
        {
            get => _value;
            set
            {
                if (!HasAuthority)
                {
                    var manager = LiminalNetworkManager.Instance;
                    ushort myId = manager?.localID ?? 0;
                LiminalLogger.LogError($"[SyncVar] Unauthorized write blocked! Client {myId} does not have authority to modify '{Token}'.");
                return; 
                }

                WriteFromOwningThread(value);
            }
        }

        public SyncVar(string token, T initialValue = default)
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            _value = initialValue;
            SyncVarManager.Instance.RegisterSyncVar(this);
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

            var manager = LiminalNetworkManager.Instance;
            if (manager != null && (manager.Role == NetworkRole.Server || manager.Role == NetworkRole.Host))
            {
                SyncVarManager.Instance.BroadcastAuthChange(Id, newAuthIds);
            }
        }

        public void AllocateSlots(int size)
        {
            _slots[0] = SyncVarManager.Instance.Slab.AllocateSlot(size);
            _slots[1] = SyncVarManager.Instance.Slab.AllocateSlot(size);
            _slotLengths[0] = size;
            _slotLengths[1] = size;
        }

        private void WriteFromOwningThread(T newValue)
        {
            if (EqualityComparer<T>.Default.Equals(_value, newValue)) return;

            int backIndex = 1 - Volatile.Read(ref _frontIndex);
            var (page, offset) = _slots[backIndex];

            // Use thread-local writer to serialize directly into slab page memory (Zero GC)
            var writer = FixedBufferWriter.ThreadInstance;
            byte[] pageArray = SyncVarManager.Instance.Slab.GetPage(page);
            writer.Reset(pageArray, offset, _slotLengths[backIndex]);
            MessagePackSerializer.Serialize(writer, newValue, SyncVarManager.Instance.SerializerOptions);

            uint newVersion = _version + 1;
            _slotVersions[backIndex] = newVersion;
            T old = _value;
            _value = newValue;
            _version = newVersion;

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

            SyncVarManager.Instance.DirtyBitset.SetDirty(Id);

            OnValueChanged?.Invoke(old, newValue);
        }

        public bool TryReadSlotForWire(Span<byte> destination, out uint version, out int length)
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
                length = _slotLengths[front];
                version = _slotVersions[front];

                var span = SyncVarManager.Instance.Slab.GetSpan(page, offset, length);
                span.CopyTo(destination);
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
            _version = newVersion;

            int front = Volatile.Read(ref _frontIndex);
            var (page, offset) = _slots[front];
            incomingBytes.CopyTo(SyncVarManager.Instance.Slab.GetSpan(page, offset, incomingBytes.Length));

            OnValueChanged?.Invoke(old, deserialized);
        }

        public void SerializeInitial(IBufferWriter<byte> writer, MessagePackSerializerOptions options)
        {
            MessagePackSerializer.Serialize(writer, _value, options);
        }
    }
}