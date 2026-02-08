using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.Test
{
    [MessagePackObject]
    [LiminalPacket(id: 5)]
    public struct ChatPacket
    {
        [Key(0)]
        public string Message;
    }
}
