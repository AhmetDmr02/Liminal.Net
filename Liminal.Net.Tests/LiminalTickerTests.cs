using Liminal.Net.Core;
using NUnit.Framework;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalTickerTests
    {
        [Test]
        public void Stop_CalledFromInsideTick_ReturnsWithoutSelfJoinStall()
        {
            var ticker = new LiminalTicker(new LiminalNetworkConfig { TickRate = 60 });
            using var stopped = new ManualResetEventSlim();
            long elapsedMs = -1;

            ticker.OnTick += () =>
            {
                var sw = Stopwatch.StartNew();
                ticker.Stop();
                elapsedMs = sw.ElapsedMilliseconds;
                stopped.Set();
            };

            ticker.Start();

            Assert.That(stopped.Wait(2000), Is.True, "Tick never fired.");
            Assert.That(elapsedMs, Is.LessThan(250), $"Stop() from the tick thread took {elapsedMs} ms.");
        }

        [Test]
        public void StopThenStart_CalledFromInsideTick_LeavesSingleTickLoop()
        {
            var ticker = new LiminalTicker(new LiminalNetworkConfig { TickRate = 60 });
            int restarted = 0;
            int restartDone = 0;
            var tickThreadsAfterRestart = new ConcurrentDictionary<int, byte>();

            ticker.OnTick += () =>
            {
                if (Interlocked.CompareExchange(ref restarted, 1, 0) == 0)
                {
                    ticker.Stop();
                    ticker.Start();
                    Volatile.Write(ref restartDone, 1);
                    return;
                }
                if (Volatile.Read(ref restartDone) == 1)
                {
                    tickThreadsAfterRestart.TryAdd(Thread.CurrentThread.ManagedThreadId, 0);
                }
            };

            try
            {
                ticker.Start();
                Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref restartDone) == 1, 3000), Is.True, "Restart never completed.");

                Thread.Sleep(300);

                Assert.That(tickThreadsAfterRestart.Count, Is.EqualTo(1),
                    $"{tickThreadsAfterRestart.Count} threads kept ticking after a Stop+Start from inside a tick.");
            }
            finally
            {
                ticker.Stop();
            }
        }
    }
}
