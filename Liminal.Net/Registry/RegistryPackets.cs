using Liminal.Net.Core;
using MessagePack;

namespace Liminal.Net.Registry
{
    [MessagePackObject]
    [LiminalPacket]
    public struct ClientRosterSnapshotPacket
    {
        [Key(0)] public uint RosterVersion;
        [Key(1)] public ushort HostClientId; // 0 if Dedicated Server
        [Key(2)] public ushort[] ConnectedClientIds;
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct ClientJoinedNotificationPacket
    {
        [Key(0)] public uint RosterVersion;
        [Key(1)] public ushort ClientId;
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct ClientLeftNotificationPacket
    {
        [Key(0)] public uint RosterVersion;
        [Key(1)] public ushort ClientId;
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct ClientRosterRequestPacket
    {
        [Key(0)] public uint LastKnownVersion;
    }
}
