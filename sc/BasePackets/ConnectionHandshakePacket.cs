using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.BasePackets
{
    [MessagePackObject]
    [LiminalPacket(id: 1)]
    public class ConnectionHandshakePacketClient
    {
        [Key(0)]
        public string ClientName { get; set; }

        [Key(1)]
        public ushort ClientVersion { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket(id: 2)]
    public class ConnectionHandshakePacketServer
    {
        [Key(0)]
        public ushort ServerVersion { get; set; }

        [Key(1)]
        public ushort AssignedClientID { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket(id: 3)]
    public class ConnectionHandshakeClientAck
    {
        [Key(0)]
        public bool Ack { get; set; }
        [Key(1)]
        public ushort ClientID { get; set; }
    }
}
