using Liminal.Net.Core;
using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Unity
{
    public class LiminalUnityTicker : LiminalTicker
    {
        public int MaxTicksPerFrame { get; set; } = 5;
        public bool IsRunning => _isRunning;

        public LiminalUnityTicker(LiminalNetworkConfig config) : base(config)
        {
        }

        public override void Start()
        {
            if (_isRunning) return;
            _isRunning = true;
            _nextTickTimestamp = Stopwatch.GetTimestamp() + NominalTickTicks;
        }

        public override void Stop()
        {
            _isRunning = false;
            _nextTickTimestamp = 0;
            Interlocked.Exchange(ref _slewAdjustmentTicks, 0);
        }

        public int TickUpdate()
        {
            if (!_isRunning) return 0;

            long currentTicks = Stopwatch.GetTimestamp();
            if (_nextTickTimestamp == 0)
            {
                _nextTickTimestamp = currentTicks + NominalTickTicks;
            }

            long nominal = NominalTickTicks;
            int ticksExecuted = 0;

            while (currentTicks >= _nextTickTimestamp && ticksExecuted < MaxTicksPerFrame)
            {
                long currentSlew = Interlocked.Exchange(ref _slewAdjustmentTicks, 0);
                _nextTickTimestamp += (nominal + currentSlew);

                try
                {
                    InvokeTick();
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[UnityTicker] Tick error: {ex}");
                }

                ticksExecuted++;
            }

            if (currentTicks > _nextTickTimestamp + (nominal * 3))
            {
                _nextTickTimestamp = currentTicks + nominal;
            }

            return ticksExecuted;
        }
    }
}
