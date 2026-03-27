using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalTicker
    {
        private readonly LiminalTransportConfig _config;
        private readonly Stopwatch _stopwatch = new();

        public event Action OnTick;

        private volatile bool _isRunning;
        private Thread _tickThread;

        public LiminalTicker(LiminalTransportConfig config)
        {
            _config = config;
        }

        public void Start()
        {
            if (_isRunning) return;
            _isRunning = true;

            _tickThread = new Thread(RunLoop)
            {
                Name = "Liminal Network Ticker",
                IsBackground = true,
                Priority = ThreadPriority.Highest // Bumped priority for stability
            };
            _tickThread.Start();
        }

        public void Stop()
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

        private void RunLoop()
        {
            long targetTickTicks = Stopwatch.Frequency / _config.TickRate;

            _stopwatch.Start();
            long nextTick = _stopwatch.ElapsedTicks;

            while (_isRunning)
            {
                long currentTicks = _stopwatch.ElapsedTicks;

                if (currentTicks >= nextTick)
                {
                    try
                    {
                        OnTick?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[Ticker] Crash: {ex}");
                    }

                    nextTick += targetTickTicks;

                    if (currentTicks > nextTick + (targetTickTicks * 3))
                    {
                        nextTick = currentTicks + targetTickTicks;
                    }
                }
                else
                {
                    long ticksRemaining = nextTick - currentTicks;

                    long msRemaining = ticksRemaining * 1000 / Stopwatch.Frequency;

                    if (msRemaining > 16)
                    {
                        Thread.Sleep(1);
                    }
                    else
                    {
                        Thread.SpinWait(10);
                    }
                }
            }
        }
    }
}