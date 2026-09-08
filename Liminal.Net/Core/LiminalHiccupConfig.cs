using System;

namespace Liminal.Net.Core
{
    /// <summary>
    /// Configuration for bounded local Hiccup recovery. Hiccup never changes the wire protocol.
    /// </summary>
    public sealed class LiminalHiccupConfig
    {
        public bool Enabled = true;

        /// <summary>Recovery capacity multiplier applied to the normal packet size/count limits.</summary>
        public int MaxRecoveryScale = 4;

        /// <summary>Maximum isolated recovery grants permitted in one grace window.</summary>
        public int GraceCount = 3;

        /// <summary>How long an active recovery resource set remains held after its last use.</summary>
        public float RecoveryHoldSeconds = 2.0f;

        /// <summary>Time window in which GraceCount isolated recovery grants are permitted.</summary>
        public float GraceWindowSeconds = 2.0f;

        /// <summary>Minimum time between isolated recovery grants, in seconds.</summary>
        public float CooldownSeconds = 0.25f;

        /// <summary>Active recovery-session ratio at which the endpoint is considered globally starved.</summary>
        public float StarvationThreshold = 0.70f;

        /// <summary>Number of recovery buffer sets to pre-warm. Negative means ceil(MaxConnectionCount / 2).</summary>
        public int WarmRecoverySessions = 0;

        public int GetRecoverySize(ushort normalSize)
        {
            return checked((int)((long)normalSize * MaxRecoveryScale));
        }

        public int GetRecoveryPacketCount(int normalCount)
        {
            return checked(normalCount * MaxRecoveryScale);
        }

        public int ResolveWarmRecoverySessions(int maxConnectionCount)
        {
            if (WarmRecoverySessions >= 0)
                return Math.Min(WarmRecoverySessions, Math.Max(maxConnectionCount, 0));

            return Math.Max(0, (Math.Max(maxConnectionCount, 0) + 1) / 2);
        }

        internal long RecoveryHoldTicks => (long)(Math.Max(0f, RecoveryHoldSeconds) * System.Diagnostics.Stopwatch.Frequency);
        internal long GraceWindowTicks => (long)(Math.Max(0f, GraceWindowSeconds) * System.Diagnostics.Stopwatch.Frequency);
        internal long CooldownTicks => (long)(Math.Max(0f, CooldownSeconds) * System.Diagnostics.Stopwatch.Frequency);

        public void Validate(ushort normalSize, int normalPacketCount, int maxConnectionCount)
        {
            if (MaxRecoveryScale < 1)
                throw new InvalidOperationException("Hiccup.MaxRecoveryScale must be at least 1.");
            if (normalSize <= 0)
                throw new InvalidOperationException("MaxPacketSizePerBatch must be greater than zero.");
            if (normalPacketCount <= 0)
                throw new InvalidOperationException("MaxPacketCount must be greater than zero.");
            if (GraceCount < 0)
                throw new InvalidOperationException("Hiccup.GraceCount cannot be negative.");
            if (RecoveryHoldSeconds <= 0f)
                throw new InvalidOperationException("Hiccup.RecoveryHoldSeconds must be greater than zero.");
            if (GraceWindowSeconds <= 0f)
                throw new InvalidOperationException("Hiccup.GraceWindowSeconds must be greater than zero.");
            if (CooldownSeconds < 0f)
                throw new InvalidOperationException("Hiccup.CooldownSeconds cannot be negative.");
            if (StarvationThreshold < 0f || StarvationThreshold > 1f)
                throw new InvalidOperationException("Hiccup.StarvationThreshold must be between 0 and 1.");
            if (maxConnectionCount < 0)
                throw new InvalidOperationException("MaxConnectionCount cannot be negative.");

            _ = GetRecoverySize(normalSize);
            _ = GetRecoveryPacketCount(normalPacketCount);
            _ = ResolveWarmRecoverySessions(maxConnectionCount);
        }
    }
}
