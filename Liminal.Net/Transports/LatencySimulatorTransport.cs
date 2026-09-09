using Liminal.Net.Core;
using Liminal.Net.Transports;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

public class LatencySimulatorTransport : TcpTransport
{
    public double OneWayDelayMs { get; set; } = 50.0;
    public double JitterMs { get; set; } = 0.0;

    private readonly PriorityQueue<DelayedPacket, long> _sendQueue = new();
    private readonly object _sendLock = new();
    private readonly object _streamWriteLock = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread _drainThread;
    private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;

    private struct DelayedPacket
    {
        public byte[] Buffer;
        public int Length;
        public ushort TargetId;
        public TransportFlags Flags;
    }

    public LatencySimulatorTransport()
    {
        _drainThread = new Thread(DrainLoop) { IsBackground = true, Name = "LatencySimulator-Drain" };
        _drainThread.Start();
    }

    protected override void SendInternal(Span<byte> data, ushort targetId, TransportFlags flags)
    {
        if (OneWayDelayMs <= 0.0)
        {
            lock (_streamWriteLock)
            {
                base.SendInternal(data, targetId, flags);
            }
            return;
        }

        byte[] rented = _pool.Rent(data.Length);
        data.CopyTo(rented);

        double jitter = JitterMs > 0 ? (Random.Shared.NextDouble() * 2.0 - 1.0) * JitterMs : 0.0;
        double totalDelayMs = Math.Max(0.0, OneWayDelayMs + jitter);
        long deliverTimestamp = Stopwatch.GetTimestamp() + (long)(totalDelayMs * Stopwatch.Frequency / 1000.0);

        lock (_sendLock)
        {
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

    private void DrainLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            DelayedPacket packet = default;
            bool hasPacket = false;

            lock (_sendLock)
            {
                while (_sendQueue.Count == 0 && !_cts.IsCancellationRequested)
                {
                    Monitor.Wait(_sendLock);
                }

                if (_cts.IsCancellationRequested) break;

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
                        int waitMs = Math.Max(1, (int)(waitTicks * 1000 / Stopwatch.Frequency));
                        Monitor.Wait(_sendLock, waitMs);
                    }
                }
            }

            if (hasPacket)
            {
                try
                {
                    lock (_streamWriteLock)
                    {
                        base.SendInternal(packet.Buffer.AsSpan(0, packet.Length), packet.TargetId, packet.Flags);
                    }
                }
                catch { }
                finally
                {
                    _pool.Return(packet.Buffer);
                }
            }
        }
    }

    public override void Shutdown()
    {
        _cts.Cancel();
        lock (_sendLock) { Monitor.PulseAll(_sendLock); }
        base.Shutdown();
    }
}