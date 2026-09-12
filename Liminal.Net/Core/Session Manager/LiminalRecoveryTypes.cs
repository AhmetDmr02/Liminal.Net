using System;

namespace Liminal.Net.Core
{
    public readonly struct ServerStarvationEvent
    {
        public readonly ushort ClientId;
        public readonly LiminalRecoveryDirection Direction;
        public readonly int ActiveRecoverySessions;
        public readonly int TotalSessions;
        public readonly double RecoveryRatio;
        public readonly long TimestampTicks;

        public ServerStarvationEvent(ushort clientId, LiminalRecoveryDirection direction,
            int activeRecoverySessions, int totalSessions, double recoveryRatio, long timestampTicks)
        {
            ClientId = clientId;
            Direction = direction;
            ActiveRecoverySessions = activeRecoverySessions;
            TotalSessions = totalSessions;
            RecoveryRatio = recoveryRatio;
            TimestampTicks = timestampTicks;
        }
    }

    public enum LiminalRecoveryDirection : byte
    {
        Inbound = 0,
        Outbound = 1
    }
}
