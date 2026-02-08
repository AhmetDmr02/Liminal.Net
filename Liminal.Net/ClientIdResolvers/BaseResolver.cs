using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Liminal.Net.ClientIdResolvers
{
    public class BaseResolver : ILiminalClientIdResolver
    {
        protected readonly ConcurrentDictionary<ushort, ConnectionPair> _activeClients = new();

        protected int _nextClientId = 1;
        private readonly object _idLock = new();

        /// <summary>
        /// robustly finds the next available ID. 
        /// Handles wrapping (ushort.MaxValue) and collisions with existing clients.
        /// </summary>
        public ushort GenerateClientId()
        {
            lock (_idLock)
            {
                int attempts = 0;
                int maxAttempts = 65000;

                while (attempts < maxAttempts)
                {
                    if (_nextClientId >= ushort.MaxValue)
                    {
                        LiminalLogger.LogWarning("[Resolver] ID Counter wrapped. Searching for recycled IDs...");
                        _nextClientId = 1; 
                    }

                    ushort candidate = (ushort)_nextClientId;
                    _nextClientId++;

                    if (!_activeClients.ContainsKey(candidate) && candidate != 0)
                    {
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

        public bool RegisterId(ushort clientId, ConnectionPair connectionPair)
        {
            if (_activeClients.TryAdd(clientId, connectionPair))
            {
                return true;
            }

            LiminalLogger.LogError($"[Resolver] Failed to register client {clientId}. ID already in use?");
            return false;
        }

        public void ResetResolver()
        {
            _activeClients.Clear();
            lock (_idLock)
            {
                _nextClientId = 1;
            }
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

        public bool UnregisterId(ushort clientId)
        {
            if (_activeClients.TryRemove(clientId, out ConnectionPair connectionPair))
            {
                return true;
            }

            LiminalLogger.LogWarning($"[Resolver] Could not unregister {clientId} (Already removed?)");
            return false;
        }
    }
}