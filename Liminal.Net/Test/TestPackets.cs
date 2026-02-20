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
    [LiminalPacket(6)]
    [MessagePackObject]
    public struct FilePacket
    {
        [Key(0)]
        public string FileName;

        [Key(1)]
        public byte[] Data;
    }
}
