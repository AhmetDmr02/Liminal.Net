using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System.Collections.Concurrent;

namespace Liminal.Net.ClientIdResolvers
{
    public class BaseResolver : ILiminalClientIdResolver
    {
        protected readonly ConcurrentDictionary<ushort, ConnectionPair> _activeClients = new();

        protected ushort _nextClientId = 1;
        public ushort GenerateClientId()
        {
            if (_nextClientId == ushort.MaxValue)
            {
                LiminalLogger.LogError("Maximum number of clients reached this is not supported for Liminal.Net expect unexpected behavior");

                _nextClientId = 1;
            }

            return _nextClientId++;
        }

        public bool IsClientActive(ushort clientId)
        {
            return _activeClients.ContainsKey(clientId);
        }

        public bool RegisterClient(ushort clientId, ConnectionPair connectionPair)
        {
            if (_activeClients.TryAdd(clientId, connectionPair))
            {
                return true;
            }

            LiminalLogger.LogError($"Failed to register client {clientId}");

            return false;
        }

        public void ResetResolver()
        {
            _activeClients.Clear();
            _nextClientId = 1;
        }

        public ushort ResolveClientId(Span<byte> payload)
        {
            //Payload will be parsed here based on packet processors
            throw new NotImplementedException();
        }

        public bool TryGetClient(ushort clientId, out ConnectionPair connectionPair)
        {
            return _activeClients.TryGetValue(clientId, out connectionPair);
        }

        public bool UnregisterClientId(ushort clientId)
        {
            if(_activeClients.TryRemove(clientId, out ConnectionPair connectionPair))
            {
                return true;
            }

            LiminalLogger.LogWarning($"Failed to unregister client {clientId}");
            return false;
        }
    }
}
