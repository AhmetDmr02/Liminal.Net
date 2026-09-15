using System;
using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    /// <summary>
    /// A stack-allocated ref struct scope for streaming bits directly into Liminal's pooled native outbound buffer.
    /// When disposed at the end of a 'using' block, any unaligned bits are flushed, the packet is dispatched to the network layer,
    /// and the native buffer is safely returned to the pool with zero heap allocations.
    /// </summary>
    public ref struct BitStreamScope
    {
        private readonly LiminalNetworkManager _manager;
        private readonly LiminalNativeBufferWriter _writer;
        private readonly ushort _singleTargetId;
        private readonly SendTo _sendToTarget;
        private readonly ushort _packetId;
        private readonly DeliveryMethod _deliveryMethod;
        private readonly bool _isSingleTarget;
        private bool _disposed;

        /// <summary>
        /// The active bit writer for this stream.
        /// </summary>
        public BitWriter Writer;

        internal BitStreamScope(
            LiminalNetworkManager manager,
            LiminalNativeBufferWriter writer,
            ushort singleTargetId,
            ushort packetId,
            DeliveryMethod deliveryMethod)
        {
            _manager = manager;
            _writer = writer;
            _singleTargetId = singleTargetId;
            _sendToTarget = default;
            _packetId = packetId;
            _deliveryMethod = deliveryMethod;
            _isSingleTarget = true;
            _disposed = false;
            Writer = new BitWriter(writer);
        }

        internal BitStreamScope(
            LiminalNetworkManager manager,
            LiminalNativeBufferWriter writer,
            SendTo sendToTarget,
            ushort packetId,
            DeliveryMethod deliveryMethod)
        {
            _manager = manager;
            _writer = writer;
            _singleTargetId = 0;
            _sendToTarget = sendToTarget;
            _packetId = packetId;
            _deliveryMethod = deliveryMethod;
            _isSingleTarget = false;
            _disposed = false;
            Writer = new BitWriter(writer);
        }

        /// <summary>
        /// Flushes pending bits, transmits the packet payload, and recycles the native writer buffer.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                Writer.Flush();
                ReadOnlySpan<byte> payload = _writer.WrittenSpan;

                if (_isSingleTarget)
                {
                    ushort sender = _manager.Role == NetworkRole.Client ? _manager.localID : ILiminalTransport.SERVER_ID;
                    _manager.Interpreter.InvokeSendRequestSingle(sender, _singleTargetId, _packetId, payload, _deliveryMethod);
                }
                else
                {
                    Broadcaster.ResolveAndSendBitStream(_manager, _sendToTarget, _packetId, payload, _deliveryMethod);
                }
            }
            finally
            {
                _manager.Interpreter.ReturnWriter(_writer);
            }
        }
    }
}
