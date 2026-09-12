using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Liminal.Net.Core
{
    internal sealed class LiminalInboundRecoveryBuffers : IDisposable
    {
        internal readonly LiminalNativeBuffer StagingA;
        internal readonly LiminalNativeBuffer StagingB;

        internal LiminalInboundRecoveryBuffers(int size)
        {
            StagingA = new LiminalNativeBuffer(size);
            StagingB = new LiminalNativeBuffer(size);
        }

        public void Dispose()
        {
            StagingA.ManualDispose();
            StagingB.ManualDispose();
        }
    }

    internal sealed class LiminalOutboundRecoveryBuffers : IDisposable
    {
        internal readonly LiminalNativeBuffer RawSendBuffer;
        internal readonly LiminalNativeBuffer SendBuffer;
        internal readonly LiminalNativeBuffer StagingA;
        internal readonly LiminalNativeBuffer StagingB;

        internal LiminalOutboundRecoveryBuffers(int size)
        {
            RawSendBuffer = new LiminalNativeBuffer(size);
            SendBuffer = new LiminalNativeBuffer(size);
            StagingA = new LiminalNativeBuffer(size);
            StagingB = new LiminalNativeBuffer(size);
        }

        public void Dispose()
        {
            RawSendBuffer.ManualDispose();
            SendBuffer.ManualDispose();
            StagingA.ManualDispose();
            StagingB.ManualDispose();
        }
    }

    internal abstract class LiminalRecoveryPool<T> : IDisposable where T : class, IDisposable
    {
        private readonly ConcurrentBag<T> _warm = new();
        private readonly Func<T> _factory;
        private readonly int _warmCapacity;
        private int _available;

        protected LiminalRecoveryPool(int warmCount, Func<T> factory)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _warmCapacity = Math.Max(0, warmCount);

            for (int i = 0; i < _warmCapacity; i++)
            {
                _warm.Add(_factory());
                _available++;
            }
        }

        public T Rent()
        {
            if (_warm.TryTake(out var value))
            {
                Interlocked.Decrement(ref _available);
                return value;
            }

            LiminalLogger.LogWarning(
                "[Hiccup] Recovery pool warm reserve exhausted; allocating a cold recovery set.",
                LiminalLogger.LogLevel.Detailed);
            return _factory();
        }

        public void Return(T value)
        {
            if (value == null) return;

            int next = Interlocked.Increment(ref _available);

            _warm.Add(value);
            return;
        }

        public void Dispose()
        {
            while (_warm.TryTake(out var value))
            {
                try { value.Dispose(); }
                catch (Exception ex)
                {
                    LiminalLogger.LogWarning($"[Hiccup] Failed to dispose recovery buffer set: {ex.Message}");
                }
            }
        }
    }

    internal sealed class LiminalInboundRecoveryPool : LiminalRecoveryPool<LiminalInboundRecoveryBuffers>
    {
        public LiminalInboundRecoveryPool(int size, int warmCount)
            : base(warmCount, () => new LiminalInboundRecoveryBuffers(size)) { }
    }

    internal sealed class LiminalOutboundRecoveryPool : LiminalRecoveryPool<LiminalOutboundRecoveryBuffers>
    {
        public LiminalOutboundRecoveryPool(int size, int warmCount)
            : base(warmCount, () => new LiminalOutboundRecoveryBuffers(size)) { }
    }
}
