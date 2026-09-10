using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalTicker
    {
        private readonly LiminalTransportConfig _config;
        public event Action OnTick;

        private volatile bool _isRunning;
        private Thread _tickThread;

        private long _nextTickTimestamp;
        public long NextTickTimestamp => Volatile.Read(ref _nextTickTimestamp);

        public long NominalTickTicks => Stopwatch.Frequency / _config.TickRate;

        private long _slewAdjustmentTicks = 0;

        public LiminalTicker(LiminalTransportConfig config)
        {
            _config = config;
        }

        public void ApplySlew(long adjustmentTicks)
        {
            Interlocked.Exchange(ref _slewAdjustmentTicks, adjustmentTicks);
        }

        public void Start()
        {
            if (_isRunning) return;
            _isRunning = true;

            _tickThread = new Thread(RunLoop)
            {
                Name = "Liminal Network Ticker",
                IsBackground = true,
                Priority = ThreadPriority.Highest
            };
            _tickThread.Start();
        }

        public virtual void Stop()
        {
            if (!_isRunning) return;
            _isRunning = false;

            if (_tickThread != null && _tickThread.IsAlive)
            {
                if (!_tickThread.Join(500))
                {
                    LiminalLogger.LogWarning("[Ticker] Tick thread did not stop gracefully!");
                }
            }
            _tickThread = null;
        }

        protected virtual void RunLoop()
        {
            long nominalTickTicks = NominalTickTicks;

            long nextTick = Stopwatch.GetTimestamp();
            Volatile.Write(ref _nextTickTimestamp, nextTick);

            while (_isRunning)
            {
                long currentTicks = Stopwatch.GetTimestamp();

                if (currentTicks >= nextTick)
                {
                    // Consume any pending slew adjustment
                    long currentSlew = Interlocked.Exchange(ref _slewAdjustmentTicks, 0);

                    // Advance to next boundary BEFORE invoking OnTick
                    // so OnTick sees the upcoming tick's deadline
                    nextTick += (nominalTickTicks + currentSlew);
                    Volatile.Write(ref _nextTickTimestamp, nextTick);

                    try
                    {
                        OnTick?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[Ticker] Crash: {ex}");
                    }

                    if (currentTicks > nextTick + (nominalTickTicks * 3))
                    {
                        nextTick = currentTicks + nominalTickTicks;
                        Volatile.Write(ref _nextTickTimestamp, nextTick);
                    }
                }
                else
                {
                    long ticksRemaining = nextTick - currentTicks;
                    long msRemaining = ticksRemaining * 1000 / Stopwatch.Frequency;

                    if (msRemaining > 16) Thread.Sleep(1);
                    else Thread.SpinWait(10);
                }
            }
        }
    }
}