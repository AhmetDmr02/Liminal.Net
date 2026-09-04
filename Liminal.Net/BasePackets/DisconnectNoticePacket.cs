using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.BasePackets
{
    [MessagePackObject]
    [LiminalPacket]
    public struct DisconnectNoticePacket
    {
        [Key(0)]
        public byte Reason { get; set; }

        [Key(1)]
        public string Message { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct DisconnectAckPacket
    {
    }
}