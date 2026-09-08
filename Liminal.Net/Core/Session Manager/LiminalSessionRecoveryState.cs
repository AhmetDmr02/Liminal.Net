using System;
using System.Threading;

namespace Liminal.Net.Core
{
    internal sealed class LiminalSessionRecoveryState
    {
        private const int InboundBit = 1;
        private const int OutboundBit = 2;

        private LiminalInboundRecoveryBuffers _inboundBuffers;
        private LiminalOutboundRecoveryBuffers _outboundBuffers;
        private int _activeDirections;

        internal readonly LiminalRecoveryRateLimiter InboundLimiter = new();
        internal readonly LiminalRecoveryRateLimiter OutboundLimiter = new();

        internal bool InboundActive => Volatile.Read(ref _inboundBuffers) != null;
        internal bool OutboundActive => Volatile.Read(ref _outboundBuffers) != null;
        internal bool AnyActive => Volatile.Read(ref _activeDirections) != 0;

        internal LiminalNativeBuffer InboundStagingA(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _inboundBuffers)?.StagingA ?? normal;

        internal LiminalNativeBuffer InboundStagingB(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _inboundBuffers)?.StagingB ?? normal;

        internal LiminalNativeBuffer OutboundStagingA(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _outboundBuffers)?.StagingA ?? normal;

        internal LiminalNativeBuffer OutboundStagingB(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _outboundBuffers)?.StagingB ?? normal;

        internal LiminalNativeBuffer RawSendBuffer(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _outboundBuffers)?.RawSendBuffer ?? normal;

        internal LiminalNativeBuffer SendBuffer(LiminalNativeBuffer normal) =>
            Volatile.Read(ref _outboundBuffers)?.SendBuffer ?? normal;

        internal void AttachInbound(LiminalInboundRecoveryBuffers buffers)
        {
            if (Interlocked.CompareExchange(ref _inboundBuffers, buffers, null) != null)
                throw new InvalidOperationException("Inbound recovery buffers already attached.");
        }

        internal LiminalInboundRecoveryBuffers DetachInbound() =>
            Interlocked.Exchange(ref _inboundBuffers, null);

        internal void AttachOutbound(LiminalOutboundRecoveryBuffers buffers)
        {
            if (Interlocked.CompareExchange(ref _outboundBuffers, buffers, null) != null)
                throw new InvalidOperationException("Outbound recovery buffers already attached.");
        }

        internal LiminalOutboundRecoveryBuffers DetachOutbound() =>
            Interlocked.Exchange(ref _outboundBuffers, null);

        internal bool Enter(int directionBit)
        {
            while (true)
            {
                int current = Volatile.Read(ref _activeDirections);
                if ((current & directionBit) != 0)
                    return false;

                if (Interlocked.CompareExchange(ref _activeDirections, current | directionBit, current) == current)
                    return current == 0;
            }
        }

        internal bool Exit(int directionBit)
        {
            while (true)
            {
                int current = Volatile.Read(ref _activeDirections);
                if ((current & directionBit) == 0)
                    return false;

                int next = current & ~directionBit;
                if (Interlocked.CompareExchange(ref _activeDirections, next, current) == current)
                    return next == 0;
            }
        }

        internal static int InboundDirectionBit => InboundBit;
        internal static int OutboundDirectionBit => OutboundBit;
    }

    internal sealed class LiminalRecoveryRateLimiter
    {
        internal long WindowStartTimestamp;
        internal long LastGrantTimestamp;
        internal long LastActivityTimestamp;
        internal int GrantsUsed;

        internal bool TryGrant(LiminalHiccupConfig config, long now)
        {
            if (WindowStartTimestamp == 0 || now - WindowStartTimestamp >= config.GraceWindowTicks)
            {
                WindowStartTimestamp = now;
                GrantsUsed = 0;
            }

            if (config.GraceCount <= GrantsUsed)
                return false;

            if (LastGrantTimestamp != 0 && now - LastGrantTimestamp < config.CooldownTicks)
                return false;

            GrantsUsed++;
            LastGrantTimestamp = now;
            return true;
        }

        internal void Reset()
        {
            WindowStartTimestamp = 0;
            LastGrantTimestamp = 0;
            GrantsUsed = 0;
            LastActivityTimestamp = 0;
        }
    }
}
