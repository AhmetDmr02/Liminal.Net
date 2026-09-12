using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Liminal.Net.ClientIdResolvers
{
    public class BaseResolver : ILiminalClientIdResolver
    {
        protected ILiminalTransport _transport;

        private readonly ConcurrentDictionary<ushort, DateTime> _reservedIds = new();
        private readonly TimeSpan _reservationTimeout = TimeSpan.FromSeconds(10);

        protected volatile int _nextClientId = 1;
        private readonly object _idLock = new();

        public virtual void Initialize(ILiminalTransport transport)
        {
            _transport = transport;
        }

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

                    bool isAliveInTransport = _transport?.IsClientConnected(candidate) ?? false;

                    if (candidate != 0 && !isAliveInTransport && _reservedIds.TryAdd(candidate, DateTime.UtcNow))
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


        /// <summary>
        /// Removes a client Id from reserved pool.
        /// </summary>

        public virtual void ConfirmRegistration(ushort clientId)
        {
            lock (_idLock)
            {
                _reservedIds.TryRemove(clientId, out _);
            }
        }

        public virtual void ResetResolver()
        {
            lock (_idLock)
            {
                _reservedIds.Clear();
                _nextClientId = 1;
            }
            LiminalLogger.Log("[Resolver] Resolver reset complete.");
        }

        //if you wanna do for ip swapping logic etc you can do it here
        public ushort ResolveId(Span<byte> payload)
        {
            return 0;

            //dummy example

            //int resolvedId = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(0, 4));

            //return (ushort)resolvedId;
        }

        /// <summary>
        /// Cleans up reservations that have exceeded the timeout period.
        /// Should be called periodically or before generating new IDs.
        /// </summary>
        private void CleanupExpiredReservations()
        {
            lock (_idLock) 
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
        }
    }
}