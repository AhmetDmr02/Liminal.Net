using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.BasePackets
{
    [MessagePackObject]
    [LiminalPacket]
    public struct ConnectionHandshakePacketClient
    {
        [Key(0)]
        public string ClientName { get; set; }

        [Key(1)]
        public ushort ClientVersion { get; set; }

        [Key(2)]
        public uint PacketRegistryHash { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct ConnectionHandshakePacketServer
    {
        [Key(0)]
        public ushort ServerVersion { get; set; }

        [Key(1)]
        public ushort AssignedClientID { get; set; }

        [Key(2)] 
        public uint PacketRegistryHash { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct ConnectionHandshakeClientAck
    {
        [Key(0)]
        public bool Ack { get; set; }
        [Key(1)]
        public ushort ClientID { get; set; }
    }
}
