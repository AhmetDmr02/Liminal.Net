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
        RTT = 1 << 3, // 8

        //Presets
        All = ByteCounting | PacketCounting | PacketLoss | RTT,
        BasicThroughput = ByteCounting | PacketCounting
    }

    public class LiminalTelemetryConfig
    {
        public TelemetryFlags Flags { get; set; } = TelemetryFlags.None;

        public float PollIntervalInSeconds { get; set; } = 1.0f;

        public bool HasFlag(TelemetryFlags flag) => (Flags & flag) != 0;

        public bool EnableByteCounting
        {
            get => (Flags & TelemetryFlags.ByteCounting) != 0;
            set => SetFlag(TelemetryFlags.ByteCounting, value);
        }

        public bool EnablePacketCounting
        {
            get => (Flags & TelemetryFlags.PacketCounting) != 0;
            set => SetFlag(TelemetryFlags.PacketCounting, value);
        }

        public bool EnablePacketLoss
        {
            get => (Flags & TelemetryFlags.PacketLoss) != 0;
            set => SetFlag(TelemetryFlags.PacketLoss, value);
        }

        public bool EnableRTT
        {
            get => (Flags & TelemetryFlags.RTT) != 0;
            set => SetFlag(TelemetryFlags.RTT, value);
        }

        private void SetFlag(TelemetryFlags flag, bool enabled)
        {
            if (enabled) Flags |= flag;
            else Flags &= ~flag;
        }
    }
}