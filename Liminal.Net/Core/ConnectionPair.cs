using System.Net;

namespace Liminal.Net.Core
{
    [System.Serializable]
    public class ConnectionPair
    {
        private readonly ushort _playerId;
        private readonly IPEndPoint _localEndPoint;
        
        public ushort PlayerId => _playerId;
        public IPEndPoint LocalEndPoint => _localEndPoint;

        public ConnectionPair(ushort playerId, IPEndPoint localEndPoint)
        {
            _playerId = playerId;
            _localEndPoint = localEndPoint;
        }
    }
}
