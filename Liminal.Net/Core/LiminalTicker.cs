using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalTicker
    {
        protected readonly LiminalNetworkConfig _config;
        public event Action OnTick;

        private int _running;
        protected bool _isRunning
        {
            get => Volatile.Read(ref _running) != 0;
            set => Volatile.Write(ref _running, value ? 1 : 0);
        }

        public bool IsRunning => _isRunning;

        protected bool TryMarkRunning() => Interlocked.CompareExchange(ref _running, 1, 0) == 0;

        private Thread _tickThread;

        [ThreadStatic]
        private static LiminalTicker _tickingOnThisThread;
        private int _ticksInFlight;

        //True inside OnTick on this thread, a teardown asked for from here cant wait for its own tick
        internal bool IsTickingOnCurrentThread => ReferenceEquals(_tickingOnThisThread, this);

        protected long _nextTickTimestamp;
        public long NextTickTimestamp => Volatile.Read(ref _nextTickTimestamp);

        public long NominalTickTicks => Stopwatch.Frequency / _config.TickRate;

        protected long _slewAdjustmentTicks = 0;

        private const int StopWaitMilliseconds = 500;

        public LiminalTicker(LiminalNetworkConfig config)
        {
            _config = config;
        }

        public void ApplySlew(long adjustmentTicks)
        {
            Interlocked.Exchange(ref _slewAdjustmentTicks, adjustmentTicks);
        }

        public void TickOnce()
        {
            long currentSlew = Interlocked.Exchange(
                ref _slewAdjustmentTicks,
                0);

            long nextTick = Volatile.Read(ref _nextTickTimestamp);

            if (nextTick == 0)
                nextTick = Stopwatch.GetTimestamp();

            nextTick += NominalTickTicks + currentSlew;

            Volatile.Write(ref _nextTickTimestamp, nextTick);

            InvokeTick();
        }

        protected void InvokeTick()
        {
            var previous = _tickingOnThisThread;
            _tickingOnThisThread = this;
            Interlocked.Increment(ref _ticksInFlight);
            try
            {
                OnTick?.Invoke();
            }
            finally
            {
                Interlocked.Decrement(ref _ticksInFlight);
                _tickingOnThisThread = previous;
            }
        }

        //Teardown must not run under a tick still going on another thread
        protected void WaitForTicksToFinish()
        {
            if (IsTickingOnCurrentThread)
                return;

            long deadline = Stopwatch.GetTimestamp() + Stopwatch.Frequency * StopWaitMilliseconds / 1000;
            var spinner = new SpinWait();
            while (Volatile.Read(ref _ticksInFlight) > 0)
            {
                if (Stopwatch.GetTimestamp() > deadline)
                {
                    LiminalLogger.LogWarning("[Ticker] A tick on another thread did not finish before Stop gave up waiting.");
                    return;
                }
                spinner.SpinOnce();
            }
        }

        public virtual void Start()
        {
            if (!TryMarkRunning()) return;

            var thread = new Thread(RunLoop)
            {
                Name = "Liminal Network Ticker",
                IsBackground = true,
                Priority = ThreadPriority.AboveNormal
            };
            Volatile.Write(ref _tickThread, thread);
            thread.Start();
        }

        public virtual void Stop()
        {
            _isRunning = false;

            var thread = Volatile.Read(ref _tickThread);

            if (thread != null && thread != Thread.CurrentThread && thread.IsAlive)
            {
                if (!thread.Join(StopWaitMilliseconds))
                {
                    LiminalLogger.LogWarning("[Ticker] Tick thread did not stop gracefully!");
                }
            }
            Interlocked.CompareExchange(ref _tickThread, null, thread);

            //Manual TickOnce callers are not the tick thread, wait for them too
            WaitForTicksToFinish();
        }

        protected virtual void RunLoop()
        {
            long nominalTickTicks = NominalTickTicks;

            long nextTick = Stopwatch.GetTimestamp();
            Volatile.Write(ref _nextTickTimestamp, nextTick);

            while (_isRunning && Volatile.Read(ref _tickThread) == Thread.CurrentThread)
            {
                long currentTicks = Stopwatch.GetTimestamp();

                if (currentTicks >= nextTick)
                {
                    long currentSlew = Interlocked.Exchange(ref _slewAdjustmentTicks, 0);

                    nextTick += (nominalTickTicks + currentSlew);
                    Volatile.Write(ref _nextTickTimestamp, nextTick);

                    try
                    {
                        InvokeTick();
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
