using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Liminal.Net.Core
{
    public sealed class LiminalSession : IDisposable
    {
        public ushort Id { get; }

        private int _disposed;

        private int _inboundPacketCountReliable;
        private int _inboundPacketCountUnreliable;

        internal readonly object SendLock = new();
        internal readonly object ReceiveLock = new();

        internal readonly LiminalNativeBuffer RawSendBufferReliable;
        internal readonly LiminalNativeBuffer RawSendBufferUnreliable;

        internal readonly LiminalNativeBuffer SendBuffer;

        internal readonly ConcurrentQueue<InboundPacket> InboundQueueReliable = new();
        internal readonly ConcurrentQueue<InboundPacket> InboundQueueUnreliable = new();

        private readonly LiminalNativeBuffer _inboundStagingANormal;
        private readonly LiminalNativeBuffer _inboundStagingBNormal;
        private readonly LiminalNativeBuffer _outboundStagingANormal;
        private readonly LiminalNativeBuffer _outboundStagingBNormal;

        internal readonly LiminalSessionRecoveryState Recovery = new();

        internal int RawSendCursorReliable;
        internal int RawSendCursorUnreliable;

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

        internal LiminalNativeBuffer ActiveRawSendBufferReliable =>
            Recovery.RawSendBuffer(RawSendBufferReliable);

        internal LiminalNativeBuffer ActiveSendBuffer =>
            Recovery.SendBuffer(SendBuffer);

        internal int InboundPacketCountReliable => Volatile.Read(ref _inboundPacketCountReliable);
        internal int InboundPacketCountUnreliable => Volatile.Read(ref _inboundPacketCountUnreliable);
        internal int InboundPacketCount => InboundPacketCountReliable + InboundPacketCountUnreliable;

        public LiminalSession(ushort id, int bufferSize)
        {
            Id = id;

            RawSendBufferReliable = new LiminalNativeBuffer(bufferSize);
            RawSendBufferUnreliable = new LiminalNativeBuffer(bufferSize);
            SendBuffer = new LiminalNativeBuffer(bufferSize);

            _inboundStagingANormal = new LiminalNativeBuffer(bufferSize);
            _inboundStagingBNormal = new LiminalNativeBuffer(bufferSize);
            _outboundStagingANormal = new LiminalNativeBuffer(bufferSize);
            _outboundStagingBNormal = new LiminalNativeBuffer(bufferSize);
        }

        internal bool TryReserveInboundPacketReliable(int limit)
        {
            while (true)
            {
                int rel = Volatile.Read(ref _inboundPacketCountReliable);
                int unrel = Volatile.Read(ref _inboundPacketCountUnreliable);
                if (rel + unrel >= limit) return false;

                if (Interlocked.CompareExchange(ref _inboundPacketCountReliable, rel + 1, rel) == rel)
                    return true;
            }
        }

        internal bool TryReserveInboundPacketUnreliable(int limit)
        {
            while (true)
            {
                int rel = Volatile.Read(ref _inboundPacketCountReliable);
                int unrel = Volatile.Read(ref _inboundPacketCountUnreliable);
                if (rel + unrel >= limit) return false;

                if (Interlocked.CompareExchange(ref _inboundPacketCountUnreliable, unrel + 1, unrel) == unrel)
                    return true;
            }
        }

        internal void ReleaseInboundPacketReliable()
        {
            int after = Interlocked.Decrement(ref _inboundPacketCountReliable);
            if (after >= 0) return;
            Interlocked.Exchange(ref _inboundPacketCountReliable, 0);
            LiminalLogger.LogError($"[Session] Inbound reliable packet accounting underflow for session {Id}.");
        }

        internal void ReleaseInboundPacketUnreliable()
        {
            int after = Interlocked.Decrement(ref _inboundPacketCountUnreliable);
            if (after >= 0) return;
            Interlocked.Exchange(ref _inboundPacketCountUnreliable, 0);
            LiminalLogger.LogError($"[Session] Inbound unreliable packet accounting underflow for session {Id}.");
        }

        internal int DropQueuedUnreliableInbound(Action<InboundPacket> returnBuffer)
        {
            int dropped = 0;
            while (InboundQueueUnreliable.TryDequeue(out var packet))
            {
                ReleaseInboundPacketUnreliable();
                returnBuffer(packet);
                dropped++;
            }
            return dropped;
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
                RawSendBufferReliable.ManualDispose();
                RawSendBufferUnreliable.ManualDispose();
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