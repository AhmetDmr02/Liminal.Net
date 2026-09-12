using Liminal.Net.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    internal enum HiccupEnableResult : byte
    {
        Success = 0,
        Disabled = 1,
        CeilingExceeded = 2,
        BudgetExhausted = 3
    }

    internal sealed class LiminalHiccupController : IDisposable
    {
        private readonly LiminalTransportConfig _config;
        private readonly ILiminalTransport _transport;
        private readonly ConcurrentDictionary<ushort, LiminalSession> _sessions;
        private readonly LiminalInboundRecoveryPool _inboundPool;
        private readonly LiminalOutboundRecoveryPool _outboundPool;
        private int _activeRecoverySessions;
        private bool _disposed;

        internal event Action<ServerStarvationEvent> OnServerStarvation;

        internal LiminalHiccupController(
            ILiminalTransport transport,
            LiminalTransportConfig config,
            ConcurrentDictionary<ushort, LiminalSession> sessions)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _sessions = sessions ?? throw new ArgumentNullException(nameof(sessions));

            _config.Validate();

            int warm = _config.Hiccup.ResolveWarmRecoverySessions(_config.MaxConnectionCount);
            int size = _config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch);

            _inboundPool = new LiminalInboundRecoveryPool(size, warm);
            _outboundPool = new LiminalOutboundRecoveryPool(size, warm);
        }

        internal bool Enabled => _config.Hiccup.Enabled;

        internal int RecoverySize => _config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch);
        internal int RecoveryPacketCount => _config.Hiccup.GetRecoveryPacketCount(_config.MaxPacketCount);
        internal int ActiveRecoverySessions => Volatile.Read(ref _activeRecoverySessions);

        internal void Kick(ushort clientId, LiminalRecoveryDirection direction, string message)
        {
            LiminalLogger.LogWarning($"[Security/Hiccup/{direction}] Client {clientId}: {message}");

            if (_transport.IsServer)
                _transport.Kick(clientId);
            else
                _transport.Disconnect();
        }
        internal HiccupEnableResult TryEnableInbound(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason)
        {
            failureReason = null;

            if (!Enabled)
            {
                failureReason = "Hiccup recovery is disabled while inbound capacity is exhausted.";
                return HiccupEnableResult.Disabled;
            }

            if (requiredBytes > RecoverySize)
            {
                failureReason = $"Required inbound recovery size {requiredBytes} exceeds ceiling {RecoverySize}.";
                return HiccupEnableResult.CeilingExceeded;
            }

            if (session.InboundRecoveryActive)
            {
                session.Recovery.InboundLimiter.LastActivityTimestamp = now;
                return HiccupEnableResult.Success;
            }

            if (!Authorize(session.Recovery.InboundLimiter, now))
            {
                failureReason = "Inbound recovery suppression budget exhausted.";
                return HiccupEnableResult.BudgetExhausted;
            }

            var buffers = _inboundPool.Rent();
            try
            {
                session.AttachInboundRecovery(buffers);
            }
            catch
            {
                _inboundPool.Return(buffers);
                throw;
            }

            session.Recovery.InboundLimiter.LastActivityTimestamp = now;
            if (session.EnterRecovery(LiminalSessionRecoveryState.InboundDirectionBit))
                Interlocked.Increment(ref _activeRecoverySessions);

            EmitStarvationEventIfNecessary(clientId, LiminalRecoveryDirection.Inbound, now);
            return HiccupEnableResult.Success;
        }

        internal HiccupEnableResult TryEnableOutbound(LiminalSession session, ushort clientId, int requiredBytes, long now, out string failureReason)
        {
            failureReason = null;

            if (!Enabled)
            {
                failureReason = "Hiccup recovery is disabled while outbound capacity is exhausted.";
                return HiccupEnableResult.Disabled;
            }

            if (requiredBytes > RecoverySize)
            {
                failureReason = $"Required outbound recovery size {requiredBytes} exceeds ceiling {RecoverySize}.";
                return HiccupEnableResult.CeilingExceeded;
            }

            if (session.OutboundRecoveryActive)
            {
                session.Recovery.OutboundLimiter.LastActivityTimestamp = now;
                return HiccupEnableResult.Success;
            }

            if (!Authorize(session.Recovery.OutboundLimiter, now))
            {
                failureReason = "Outbound recovery suppression budget exhausted.";
                return HiccupEnableResult.BudgetExhausted;
            }

            var buffers = _outboundPool.Rent();
            try
            {
                if (session.RawSendCursorReliable > 0)
                {
                    session.RawSendBufferReliable.GetSpan()
                        .Slice(0, session.RawSendCursorReliable)
                        .CopyTo(buffers.RawSendBuffer.GetSpan());
                }

                session.AttachOutboundRecovery(buffers);
            }
            catch
            {
                _outboundPool.Return(buffers);
                throw;
            }

            session.Recovery.OutboundLimiter.LastActivityTimestamp = now;
            if (session.EnterRecovery(LiminalSessionRecoveryState.OutboundDirectionBit))
                Interlocked.Increment(ref _activeRecoverySessions);

            EmitStarvationEventIfNecessary(clientId, LiminalRecoveryDirection.Outbound, now);
            return HiccupEnableResult.Success;
        }

        internal void ReleaseInboundWhenSafe(LiminalSession session, long now)
        {
            if (!session.InboundRecoveryActive || session.InboundPacketCount != 0)
                return;

            lock (session.ReceiveLock)
            {
                if (session.IsDisposed() || !session.InboundRecoveryActive || session.InboundPacketCount != 0)
                    return;

                if (!HasRecoveryHoldElapsed(session.Recovery.InboundLimiter, now))
                    return;

                var buffers = session.DetachInboundRecovery();
                if (buffers == null)
                    return;

                if (session.ExitRecovery(LiminalSessionRecoveryState.InboundDirectionBit))
                    Interlocked.Decrement(ref _activeRecoverySessions);

                _inboundPool.Return(buffers);
            }
        }

        internal void ReleaseOutboundWhenSafe(LiminalSession session, long now)
        {
            if (!session.OutboundRecoveryActive)
                return;

            lock (session.SendLock)
            {
                if (session.IsDisposed() || !session.OutboundRecoveryActive || session.RawSendCursorReliable != 0)
                    return;

                if (!HasRecoveryHoldElapsed(session.Recovery.OutboundLimiter, now))
                    return;

                var buffers = session.DetachOutboundRecovery();
                if (buffers == null)
                    return;

                if (session.ExitRecovery(LiminalSessionRecoveryState.OutboundDirectionBit))
                    Interlocked.Decrement(ref _activeRecoverySessions);

                _outboundPool.Return(buffers);
            }
        }

        internal void ReleaseForSession(LiminalSession session)
        {
            lock (session.SendLock)
            {
                var outbound = session.DetachOutboundRecovery();
                if (outbound != null)
                {
                    if (session.ExitRecovery(LiminalSessionRecoveryState.OutboundDirectionBit))
                        Interlocked.Decrement(ref _activeRecoverySessions);
                    _outboundPool.Return(outbound);
                }
            }

            lock (session.ReceiveLock)
            {
                var inbound = session.DetachInboundRecovery();
                if (inbound != null)
                {
                    if (session.ExitRecovery(LiminalSessionRecoveryState.InboundDirectionBit))
                        Interlocked.Decrement(ref _activeRecoverySessions);
                    _inboundPool.Return(inbound);
                }
            }
        }

        private bool Authorize(LiminalRecoveryRateLimiter limiter, long now)
        {
            if (_transport.IsServer && IsServerStarved())
                return true;

            return limiter.TryGrant(_config.Hiccup, now);
        }

        private bool HasRecoveryHoldElapsed(LiminalRecoveryRateLimiter limiter, long now)
        {
            return now - limiter.LastActivityTimestamp >= _config.Hiccup.RecoveryHoldTicks;
        }

        private bool IsServerStarved()
        {
            int total = _sessions.Count;
            if (total == 0)
                return false;

            return (double)Volatile.Read(ref _activeRecoverySessions) / total >=
                   _config.Hiccup.StarvationThreshold;
        }

        private void EmitStarvationEventIfNecessary(ushort clientId, LiminalRecoveryDirection direction, long now)
        {
            int total = _sessions.Count;
            if (total == 0)
                return;

            int active = Volatile.Read(ref _activeRecoverySessions);
            double ratio = (double)active / total;
            if (ratio < _config.Hiccup.StarvationThreshold)
                return;

            OnServerStarvation?.Invoke(new ServerStarvationEvent(
                clientId, direction, active, total, ratio, now));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _inboundPool.Dispose();
            _outboundPool.Dispose();
        }
    }
}
