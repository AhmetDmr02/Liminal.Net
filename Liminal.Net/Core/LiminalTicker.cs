using System.Diagnostics;

namespace Liminal.Net.Core
{
    public class LiminalTicker
    {
        private readonly LiminalTransportConfig _config;
        private readonly Stopwatch _stopwatch = new();

        // The callback that runs every tick (Poll + Flush)
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
                Priority = ThreadPriority.AboveNormal
            };
            _tickThread.Start();
        }

        public void Stop()
        {
            _isRunning = false;
            if (_tickThread != null && _tickThread.IsAlive)
            {
                _tickThread.Join(500);
            }
        }

        private void RunLoop()
        {
            double targetTickTimeMs = 1000.0 / _config.TickRate;

            _stopwatch.Start();
            double nextTickTime = _stopwatch.Elapsed.TotalMilliseconds;

            while (_isRunning)
            {
                double currentTime = _stopwatch.Elapsed.TotalMilliseconds;

                if (currentTime >= nextTickTime)
                {
                    try
                    {
                        OnTick?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[Ticker] Crash: {ex}");
                    }

                    nextTickTime += targetTickTimeMs;

                    if (currentTime > nextTickTime + (targetTickTimeMs * 2))
                    {
                        nextTickTime = currentTime + targetTickTimeMs;
                    }
                }
                else
                {
                    double waitTime = nextTickTime - currentTime;

                    if (waitTime > 1.0)
                    {
                        Thread.Sleep((int)waitTime);
                    }
                    else
                    {
                        Thread.SpinWait(100);
                    }
                }
            }
        }
    }
}