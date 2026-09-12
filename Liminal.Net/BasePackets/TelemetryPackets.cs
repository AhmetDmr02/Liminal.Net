using Liminal.Net.Core;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Liminal.Net.BasePackets
{
    [MessagePackObject]
    [LiminalPacket]
    public struct PingPacket
    {
        [Key(0)]
        public uint SequenceId { get; set; }

        [Key(1)]
        public long TimestampTicks { get; set; }
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct PongPacket
    {
        [Key(0)]
        public uint SequenceId { get; set; }

        [Key(1)]
        public long TimestampTicks { get; set; }
    }
}
