using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        private int _disposed;
        private int _inboundPacketCount;

        internal readonly object SendLock = new();
        internal readonly object ReceiveLock = new();

        internal readonly LiminalNativeBuffer RawSendBuffer;
        internal readonly LiminalNativeBuffer SendBuffer;
        internal readonly ConcurrentQueue<InboundPacket> InboundQueue = new();

        private readonly LiminalNativeBuffer _inboundStagingANormal;
        private readonly LiminalNativeBuffer _inboundStagingBNormal;
        private readonly LiminalNativeBuffer _outboundStagingANormal;
        private readonly LiminalNativeBuffer _outboundStagingBNormal;

        internal readonly LiminalSessionRecoveryState Recovery = new();

        internal int RawSendCursor;

        internal bool InboundRecoveryActive => Recovery.InboundActive;
        internal bool OutboundRecoveryActive => Recovery.OutboundActive;
        internal bool IsAnyRecoveryActive => Recovery.AnyActive;

        internal LiminalNativeBuffer InboundStagingA =>
            Recovery.InboundStagingA(_inboundStagingANormal);

        internal LiminalNativeBuffer InboundStagingB =>
            Recovery.InboundStagingB(_inboundStagingBNormal);

        internal LiminalNativeBuffer OutboundStagingA =>
            Recovery.OutboundStagingA(_outboundStagingANormal);

        internal LiminalNativeBuffer OutboundStagingB =>
            Recovery.OutboundStagingB(_outboundStagingBNormal);

        internal LiminalNativeBuffer ActiveRawSendBuffer =>
            Recovery.RawSendBuffer(RawSendBuffer);

        internal LiminalNativeBuffer ActiveSendBuffer =>
            Recovery.SendBuffer(SendBuffer);

        internal int InboundPacketCount => Volatile.Read(ref _inboundPacketCount);

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;
            RawSendBuffer = new LiminalNativeBuffer(bufferSize);
            SendBuffer = new LiminalNativeBuffer(bufferSize);

            _inboundStagingANormal = new LiminalNativeBuffer(bufferSize);
            _inboundStagingBNormal = new LiminalNativeBuffer(bufferSize);
            _outboundStagingANormal = new LiminalNativeBuffer(bufferSize);
            _outboundStagingBNormal = new LiminalNativeBuffer(bufferSize);
        }

        internal bool TryReserveInboundPacket(int limit)
        {
            while (true)
            {
                int current = Volatile.Read(ref _inboundPacketCount);
                if (current >= limit)
                    return false;

                if (Interlocked.CompareExchange(ref _inboundPacketCount, current + 1, current) == current)
                    return true;
            }
        }

        internal void ReleaseInboundPacket()
        {
            int after = Interlocked.Decrement(ref _inboundPacketCount);
            if (after >= 0)
                return;

            Interlocked.Exchange(ref _inboundPacketCount, 0);
            LiminalLogger.LogError($"[Session] Inbound packet accounting underflow for session {Id}.");
        }

        internal void AttachInboundRecovery(LiminalInboundRecoveryBuffers buffers) => Recovery.AttachInbound(buffers);
        internal LiminalInboundRecoveryBuffers DetachInboundRecovery() => Recovery.DetachInbound();
        internal void AttachOutboundRecovery(LiminalOutboundRecoveryBuffers buffers) => Recovery.AttachOutbound(buffers);
        internal LiminalOutboundRecoveryBuffers DetachOutboundRecovery() => Recovery.DetachOutbound();

        internal bool EnterRecovery(int directionBit) => Recovery.Enter(directionBit);
        internal bool ExitRecovery(int directionBit) => Recovery.Exit(directionBit);

        public bool IsDisposed() => Volatile.Read(ref _disposed) == 1;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

            if (Recovery.AnyActive)
            {
                LiminalLogger.LogWarning(
                    $"[Hiccup] Session {Id} disposed while recovery resources were still attached. " +
                    "SessionManager should release recovery resources first.");
            }

            lock (SendLock)
            {
                RawSendBuffer.ManualDispose();
                SendBuffer.ManualDispose();
                _outboundStagingANormal.ManualDispose();
                _outboundStagingBNormal.ManualDispose();
            }

            lock (ReceiveLock)
            {
                _inboundStagingANormal.ManualDispose();
                _inboundStagingBNormal.ManualDispose();
            }
        }
    }
}

public readonly struct InboundPacket
{
    public readonly ushort PacketId;
    public readonly byte[] BackingBuffer;
    public readonly int Length;
    public readonly bool UsesRecoveryPool;

    public InboundPacket(ushort packetId, byte[] backingBuffer, int length, bool usesRecoveryPool = false)
    {
        PacketId = packetId;
        BackingBuffer = backingBuffer;
        Length = length;
        UsesRecoveryPool = usesRecoveryPool;
    }

    public ReadOnlyMemory<byte> AsMemory() => new ReadOnlyMemory<byte>(BackingBuffer, 0, Length);
}