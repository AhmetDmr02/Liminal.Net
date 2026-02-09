using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Liminal.Net.ClientIdResolvers
{
    public class BaseResolver : ILiminalClientIdResolver
    {
        protected readonly ConcurrentDictionary<ushort, ConnectionPair> _activeClients = new();
        private readonly ConcurrentDictionary<ushort, DateTime> _reservedIds = new();
        private readonly TimeSpan _reservationTimeout = TimeSpan.FromSeconds(10);

        protected int _nextClientId = 1;
        private readonly object _idLock = new();

        /// <summary>
        /// Reserves and returns the next available ID.
        /// The ID is temporarily reserved until RegisterId() or timeout occurs.
        /// Handles wrapping (ushort.MaxValue) and collisions with existing clients.
        /// </summary>
        public ushort GenerateClientId()
        {
            lock (_idLock)
            {
                CleanupExpiredReservations();

                int attempts = 0;
                int maxAttempts = ushort.MaxValue - 1;

                while (attempts < maxAttempts)
                {
                    if (_nextClientId >= ushort.MaxValue)
                    {
                        LiminalLogger.LogWarning("[Resolver] ID Counter wrapped. Searching for recycled IDs... this can result in unexpected behavior.");
                        _nextClientId = 1;
                    }

                    ushort candidate = (ushort)_nextClientId;
                    _nextClientId++;

                    if (candidate != 0 &&
                        !_activeClients.ContainsKey(candidate) &&
                        _reservedIds.TryAdd(candidate, DateTime.UtcNow))
                    {
                        LiminalLogger.Log($"[Resolver] Reserved ID {candidate}");
                        return candidate;
                    }

                    attempts++;
                }

                LiminalLogger.LogError("[Resolver] CRITICAL: Server is full! No free Client IDs available.");
                return 0;
            }
        }

        public bool IsConnectionActive(ushort clientId)
        {
            return _activeClients.ContainsKey(clientId);
        }

        /// <summary>
        /// Registers a previously reserved ID with its connection pair.
        /// Removes the ID from the reservation pool and adds it to active clients.
        /// </summary>
        public bool RegisterId(ushort clientId, ConnectionPair connectionPair)
        {
            _reservedIds.TryRemove(clientId, out _);

            if (_activeClients.TryAdd(clientId, connectionPair))
            {
                LiminalLogger.Log($"[Resolver] Registered client {clientId}");
                return true;
            }

            LiminalLogger.LogError($"[Resolver] Failed to register client {clientId}. ID already in use!");
            return false;
        }

        /// <summary>
        /// Removes a client ID from active clients and reserved pool.
        /// </summary>
        public bool UnregisterId(ushort clientId)
        {
            bool removedFromActive = _activeClients.TryRemove(clientId, out ConnectionPair connectionPair);
            bool removedFromReserved = _reservedIds.TryRemove(clientId, out _);

            if (removedFromActive)
            {
                LiminalLogger.Log($"[Resolver] Unregistered client {clientId}");
                return true;
            }

            if (removedFromReserved)
            {
                LiminalLogger.LogWarning($"[Resolver] Removed reserved (but unregistered) ID {clientId}");
                return true;
            }

            LiminalLogger.LogWarning($"[Resolver] Could not unregister {clientId} (Already removed?)");
            return false;
        }

        public void ResetResolver()
        {
            _activeClients.Clear();
            _reservedIds.Clear();

            lock (_idLock)
            {
                _nextClientId = 1;
            }

            LiminalLogger.Log("[Resolver] Resolver reset complete.");
        }

        public ushort ResolveId(Span<byte> payload)
        {
            if (payload.Length < 4)
            {
                return 0;
            }

            int resolvedId = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(0, 4));

            return (ushort)resolvedId;
        }

        public bool TryGetConnectionPair(ushort clientId, out ConnectionPair connectionPair)
        {
            return _activeClients.TryGetValue(clientId, out connectionPair);
        }

        /// <summary>
        /// Cleans up reservations that have exceeded the timeout period.
        /// Should be called periodically or before generating new IDs.
        /// </summary>
        private void CleanupExpiredReservations()
        {
            var now = DateTime.UtcNow;
            var expiredIds = new List<ushort>();

            foreach (var kvp in _reservedIds)
            {
                if (now - kvp.Value > _reservationTimeout)
                {
                    expiredIds.Add(kvp.Key);
                }
            }

            foreach (var id in expiredIds)
            {
                if (_reservedIds.TryRemove(id, out _))
                {
                    LiminalLogger.LogWarning($"[Resolver] Reservation for ID {id} expired and was cleaned up.");
                }
            }

            if (expiredIds.Count > 0)
            {
                LiminalLogger.Log($"[Resolver] Cleaned up {expiredIds.Count} expired reservation(s).");
            }
        }

        /// <summary>
        /// Gets diagnostic information about current ID usage.
        /// </summary>
        public (int Active, int Reserved, int Available) GetIdStats()
        {
            lock (_idLock)
            {
                CleanupExpiredReservations();

                int active = _activeClients.Count;
                int reserved = _reservedIds.Count;
                int available = ushort.MaxValue - 1 - active - reserved;

                return (active, reserved, available);
            }
        }
    }
}