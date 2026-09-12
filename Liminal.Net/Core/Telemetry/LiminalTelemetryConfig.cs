using System;

namespace Liminal.Net.Core
{
    [Flags]
    public enum TelemetryFlags : byte
    {
        None = 0,
        ByteCounting = 1 << 0, // 1
        PacketCounting = 1 << 1, // 2
        PacketLoss = 1 << 2, // 4
        End2EndRTT = 1 << 3, // 8
        WireRTT = 1 << 4, // 16

        // Presets
        All = ByteCounting | PacketCounting | PacketLoss | End2EndRTT | WireRTT,
        BasicThroughput = ByteCounting | PacketCounting
    }

    public class LiminalTelemetryConfig
    {
        public TelemetryFlags Flags { get; set; } = TelemetryFlags.None;
        public float PollIntervalInSeconds { get; set; } = 1.0f;

        public bool HasFlag(TelemetryFlags flag) => (Flags & flag) != 0;

        public bool EnableByteCounting
        {
            get => HasFlag(TelemetryFlags.ByteCounting);
            set => SetFlag(TelemetryFlags.ByteCounting, value);
        }

        public bool EnablePacketCounting
        {
            get => HasFlag(TelemetryFlags.PacketCounting);
            set => SetFlag(TelemetryFlags.PacketCounting, value);
        }

        public bool EnablePacketLoss
        {
            get => HasFlag(TelemetryFlags.PacketLoss);
            set => SetFlag(TelemetryFlags.PacketLoss, value);
        }

        public bool EnableEnd2EndRTT
        {
            get => HasFlag(TelemetryFlags.End2EndRTT);
            set => SetFlag(TelemetryFlags.End2EndRTT, value);
        }

        public bool EnableWireRTT
        {
            get => HasFlag(TelemetryFlags.WireRTT);
            set => SetFlag(TelemetryFlags.WireRTT, value);
        }

        private void SetFlag(TelemetryFlags flag, bool enabled)
        {
            if (enabled) Flags |= flag;
            else Flags &= ~flag;
        }
    }
}