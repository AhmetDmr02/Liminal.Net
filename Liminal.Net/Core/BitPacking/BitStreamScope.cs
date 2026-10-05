using System;
using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    /// <summary>
    /// Stack-allocated scope for streaming bits directly into a native outbound buffer.
    /// Flushes unaligned bits, transmits the packet, and recycles the native writer buffer when disposed.
    /// </summary>
    /// <remarks>
    /// Must be managed with a mutable local variable and a try/finally block:
    /// <code>
    /// var scope = Broadcaster.BeginBitStream(...);
    /// try
    /// {
    ///     scope.Writer.WriteUInt(...);
    /// }
    /// finally
    /// {
    ///     scope.Dispose();
    /// }
    /// </code>
    /// Do not declare inside a C# <c>using</c> statement. Structs declared in <c>using</c> are marked readonly,
    /// which causes defensive copies when mutating <see cref="Writer"/>.
    /// </remarks>
    public ref struct BitStreamScope
    {
        private enum TargetMode : byte
        {
            Single,
            SendTo,
            Multicast
        }

        private readonly LiminalNetworkManager _manager;
        private readonly LiminalNativeBufferWriter _writer;
        private readonly ushort _singleTargetId;
        private readonly SendTo _sendToTarget;
        private readonly ReadOnlySpan<ushort> _multicastTargets;
        private readonly ushort _packetId;
        private readonly int _lengthOffset;
        private readonly DeliveryMethod _deliveryMethod;
        private readonly TargetMode _targetMode;
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
            int lengthOffset,
            DeliveryMethod deliveryMethod)
        {
            _manager = manager;
            _writer = writer;
            _singleTargetId = singleTargetId;
            _sendToTarget = default;
            _multicastTargets = default;
            _packetId = packetId;
            _lengthOffset = lengthOffset;
            _deliveryMethod = deliveryMethod;
            _targetMode = TargetMode.Single;
            _disposed = false;
            Writer = new BitWriter(writer);
        }

        internal BitStreamScope(
            LiminalNetworkManager manager,
            LiminalNativeBufferWriter writer,
            SendTo sendToTarget,
            ushort packetId,
            int lengthOffset,
            DeliveryMethod deliveryMethod)
        {
            _manager = manager;
            _writer = writer;
            _singleTargetId = 0;
            _sendToTarget = sendToTarget;
            _multicastTargets = default;
            _packetId = packetId;
            _lengthOffset = lengthOffset;
            _deliveryMethod = deliveryMethod;
            _targetMode = TargetMode.SendTo;
            _disposed = false;
            Writer = new BitWriter(writer);
        }

        internal BitStreamScope(
            LiminalNetworkManager manager,
            LiminalNativeBufferWriter writer,
            ReadOnlySpan<ushort> multicastTargets,
            ushort packetId,
            int lengthOffset,
            DeliveryMethod deliveryMethod)
        {
            _manager = manager;
            _writer = writer;
            _singleTargetId = 0;
            _sendToTarget = default;
            _multicastTargets = multicastTargets;
            _packetId = packetId;
            _lengthOffset = lengthOffset;
            _deliveryMethod = deliveryMethod;
            _targetMode = TargetMode.Multicast;
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
                int bitstreamLen = _writer.WrittenSpan.Length - (_lengthOffset + 4);
                _writer.WriteInt32At(_lengthOffset, bitstreamLen);
                ReadOnlySpan<byte> payload = _writer.WrittenSpan;

                if (_targetMode == TargetMode.Single)
                {
                    ushort sender = _manager.Role == NetworkRole.Client ? _manager.localID : ILiminalTransport.SERVER_ID;
                    _manager.Interpreter.InvokeSendRequestSingle(sender, _singleTargetId, _packetId, payload, _deliveryMethod);
                }
                else if (_targetMode == TargetMode.SendTo)
                {
                    Broadcaster.ResolveAndSendBitStream(_manager, _sendToTarget, _packetId, payload, _deliveryMethod);
                }
                else if (_targetMode == TargetMode.Multicast)
                {
                    if (!_multicastTargets.IsEmpty)
                    {
                        ushort sender = _manager.Role == NetworkRole.Client ? _manager.localID : ILiminalTransport.SERVER_ID;
                        for (int i = 0; i < _multicastTargets.Length; i++)
                        {
                            ushort targetId = _multicastTargets[i];
                            bool duplicate = false;
                            for (int j = 0; j < i; j++)
                            {
                                if (_multicastTargets[j] == targetId)
                                {
                                    duplicate = true;
                                    break;
                                }
                            }

                            if (duplicate)
                                continue;

                            if (Broadcaster.ValidateTargetSession(_manager, targetId))
                            {
                                _manager.Interpreter.InvokeSendRequestSingle(sender, targetId, _packetId, payload, _deliveryMethod);
                            }
                        }
                    }
                }
            }
            finally
            {
                _manager.Interpreter.ReturnWriter(_writer);
            }
        }
    }
}
