using Liminal.Net.Core;
using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Transports
{
    public class LatencySimulatorTransport : TcpTransport
    {
        public double OneWayDelayMs { get; set; } = 0.0;
        public double JitterMs { get; set; } = 0.0;

        private readonly LiminalPriorityQueue<DelayedPacket, long> _sendQueue = new();
        private readonly object _sendLock = new();
        private readonly object _threadLifecycleLock = new();
        private CancellationTokenSource _cts;
        private Thread _drainThread;
        private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;

        private static readonly ThreadLocal<Random> _random = new(() => new Random(Guid.NewGuid().GetHashCode()));

        private struct DelayedPacket
        {
            public byte[] Buffer;
            public int Length;
            public ushort TargetId;
            public TransportFlags Flags;
        }

        public LatencySimulatorTransport()
        {
            EnsureDrainThreadRunning();
        }

        private void EnsureDrainThreadRunning()
        {
            var cts = _cts;
            var thread = _drainThread;
            if (cts != null && !cts.IsCancellationRequested && thread != null && thread.IsAlive)
            {
                return;
            }

            lock (_threadLifecycleLock)
            {
                if (_cts != null && !_cts.IsCancellationRequested && _drainThread != null && _drainThread.IsAlive)
                {
                    return;
                }

                if (_drainThread != null && _drainThread.IsAlive)
                {
                    _cts?.Cancel();
                    lock (_sendLock)
                    {
                        Monitor.PulseAll(_sendLock);
                    }
                    _drainThread.Join(500);
                }

                _cts?.Dispose();
                var newCts = new CancellationTokenSource();
                _cts = newCts;

                var newThread = new Thread(DrainLoop)
                {
                    IsBackground = true,
                    Name = "LatencySimulator-Drain"
                };
                _drainThread = newThread;
                newThread.Start(newCts);
            }
        }

        public override void StartServer(string ip, int port)
        {
            EnsureDrainThreadRunning();
            base.StartServer(ip, port);
        }

        public override void StartClient(string ip, int port)
        {
            EnsureDrainThreadRunning();
            base.StartClient(ip, port);
        }

        protected override void SendInternal(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            if (OneWayDelayMs <= 0.0)
            {
                base.SendInternal(data, targetId, flags);
                return;
            }

            EnsureDrainThreadRunning();

            byte[] rented = _pool.Rent(data.Length);
            data.CopyTo(rented);

            double jitter = JitterMs > 0 ? (_random.Value.NextDouble() * 2.0 - 1.0) * JitterMs : 0.0;
            double totalDelayMs = Math.Max(0.0, OneWayDelayMs + jitter);
            long deliverTimestamp = Stopwatch.GetTimestamp() + (long)(totalDelayMs * Stopwatch.Frequency / 1000.0);

            lock (_sendLock)
            {
                if (_cts == null || _cts.IsCancellationRequested)
                {
                    _pool.Return(rented);
                    return;
                }

                _sendQueue.Enqueue(new DelayedPacket
                {
                    Buffer = rented,
                    Length = data.Length,
                    TargetId = targetId,
                    Flags = flags
                }, deliverTimestamp);

                Monitor.Pulse(_sendLock);
            }
        }

        private void DrainLoop(object state)
        {
            var cts = (CancellationTokenSource)state;
            var token = cts.Token;

            while (!token.IsCancellationRequested)
            {
                DelayedPacket packet = default;
                bool hasPacket = false;

                lock (_sendLock)
                {
                    while (_sendQueue.Count == 0 && !token.IsCancellationRequested)
                    {
                        Monitor.Wait(_sendLock);
                    }

                    if (token.IsCancellationRequested) break;

                    long now = Stopwatch.GetTimestamp();
                    if (_sendQueue.TryPeek(out _, out long deliverAt))
                    {
                        if (now >= deliverAt)
                        {
                            packet = _sendQueue.Dequeue();
                            hasPacket = true;
                        }
                        else
                        {
                            long waitTicks = deliverAt - now;
                            long waitMs = (waitTicks * 1000) / Stopwatch.Frequency;

                            if (waitMs > 16)
                            {
                                Monitor.Wait(_sendLock, (int)(waitMs - 15));
                            }
                        }
                    }
                }

                if (!hasPacket && _sendQueue.TryPeek(out _, out long targetTimestamp))
                {
                    if (Stopwatch.GetTimestamp() < targetTimestamp)
                    {
                        Thread.SpinWait(20);
                        continue;
                    }
                    else
                    {
                        lock (_sendLock)
                        {
                            if (_sendQueue.Count > 0 && Stopwatch.GetTimestamp() >= targetTimestamp)
                            {
                                packet = _sendQueue.Dequeue();
                                hasPacket = true;
                            }
                        }
                    }
                }

                if (hasPacket)
                {
                    try
                    {
                        base.SendInternal(packet.Buffer.AsSpan(0, packet.Length), packet.TargetId, packet.Flags);
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogWarning($"[LatencySimulator] Error sending delayed packet: {ex.Message}");
                    }
                    finally
                    {
                        _pool.Return(packet.Buffer);
                    }
                }
            }
        }

        public override void Shutdown()
        {
            lock (_threadLifecycleLock)
            {
                _cts?.Cancel();
                lock (_sendLock)
                {
                    while (_sendQueue.Count > 0)
                    {
                        var pkt = _sendQueue.Dequeue();
                        _pool.Return(pkt.Buffer);
                    }
                    Monitor.PulseAll(_sendLock);
                }

                if (_drainThread != null && _drainThread.IsAlive)
                {
                    _drainThread.Join(500);
                }
                _drainThread = null;
                _cts?.Dispose();
                _cts = null;
            }

            base.Shutdown();
        }
    }
}