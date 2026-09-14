using System;

namespace Liminal.Net.Registry
{
    public sealed class ConnectedClient
    {
        public ushort ClientId { get; }
        public bool IsLocal { get; }
        public bool IsHost { get; }
        public DateTime ConnectedAtUtc { get; }

        public ConnectedClient(ushort clientId, bool isLocal, bool isHost)
        {
            ClientId = clientId;
            IsLocal = isLocal;
            IsHost = isHost;
            ConnectedAtUtc = DateTime.UtcNow;
        }

        public override string ToString() => $"Client [{ClientId}] (Local: {IsLocal}, Host: {IsHost})";
    }
}
