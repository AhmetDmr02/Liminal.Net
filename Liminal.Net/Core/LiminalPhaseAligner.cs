using System;
using System.Diagnostics;

namespace Liminal.Net.Core
{
    public class LiminalPhaseAligner
    {
        private readonly LiminalTransportConfig _config;

        // Base reference: 20ms cushion on a 50ms tick (20 Hz) = 40% (0.4f)
        private const float BaseCushionRatio = 20f / 50f;

        //Max slew per tick: 1ms in stopwatch ticks
        private readonly long _maxSlewPerTickTicks;

        public LiminalPhaseAligner(LiminalTransportConfig config)
        {
            _config = config;
            _maxSlewPerTickTicks = Stopwatch.Frequency / 1000;
        }

        public double GetProportionalCushionMs()
        {
            double tickIntervalMs = 1000.0 / _config.TickRate;
            return tickIntervalMs * BaseCushionRatio;
        }

        /// <summary>
        /// Calculates the slew adjustment in Stopwatch ticks to steer client arrival into the target cushion.
        /// </summary>
        /// <param name="wireRttMs">Current wire latency</param>
        /// <param name="serverCountdownMs">Remaining time until server's next tick</param>
        /// <param name="clientCountdownMs">Remaining time until client's next tick</param>
        public long CalculateSlewAdjustment(double wireRttMs, double serverCountdownMs, double clientCountdownMs)
        {
            if (wireRttMs <= 0.0) return 0;

            double tickIntervalMs = 1000.0 / _config.TickRate;
            double owtMs = wireRttMs / 2.0;
            double targetCushionMs = GetProportionalCushionMs();

            // Wrap server countdown to [0, tickIntervalMs)
            double currentServerRemainingMs = serverCountdownMs % tickIntervalMs;
            if (currentServerRemainingMs < 0) currentServerRemainingMs += tickIntervalMs;

            // When the client flushes on its tick (clientCountdownMs = 0), the packet flies for owtMs.
            double expectedArrivalCountdownMs = (currentServerRemainingMs - clientCountdownMs - owtMs) % tickIntervalMs;
            if (expectedArrivalCountdownMs < 0) expectedArrivalCountdownMs += tickIntervalMs;

            //Difference between expected arrival margin and target cushion
            double errorMs = expectedArrivalCountdownMs - targetCushionMs;

            // Wrap to [-HalfInterval, +HalfInterval] for shortest slew path
            double halfInterval = tickIntervalMs / 2.0;
            if (errorMs > halfInterval) errorMs -= tickIntervalMs;
            if (errorMs < -halfInterval) errorMs += tickIntervalMs;

            // Deadband: within 0.5ms of target cushion, do not adjust
            if (Math.Abs(errorMs) < 0.5) return 0;

            long errorTicks = (long)((errorMs / 1000.0) * Stopwatch.Frequency);

            // Clamp to + - _maxSlewPerTickTicks per tick
            return Math.Clamp(errorTicks, -_maxSlewPerTickTicks, _maxSlewPerTickTicks);
        }
    }
}