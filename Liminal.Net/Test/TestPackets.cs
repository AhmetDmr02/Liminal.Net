using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.Test
{
    [MessagePackObject]
    [LiminalPacket]
    public struct ChatPacket
    {
        [Key(0)]
        public string Message;
    }
    [LiminalPacket]
    [MessagePackObject]
    public struct FilePacket
    {
        [Key(0)]
        public string FileName;

        [Key(1)]
        public byte[] Data;
    }

    [LiminalPacket]
    [MessagePackObject]
    public struct TestSnapshotMeta
    {
        [Key(0)] public uint Tick;
        [Key(1)] public ushort Count;
    }

    [LiminalPacket]
    [MessagePackObject]
    public struct TestTagBitPacket
    {
    }

    [StickyPacket]
    [MessagePackObject]
    public struct TestStickyStatePacket
    {
        [Key(0)] public int StateId;
        [Key(1)] public string StateName;
    }

    [StickyPacket]
    [MessagePackObject]
    public struct TestStickyCounterPacket
    {
        [Key(0)] public int Counter;
    }
}
