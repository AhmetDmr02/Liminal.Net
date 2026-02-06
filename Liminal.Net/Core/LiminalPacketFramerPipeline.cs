using Liminal.Net.Interfaces;
using System.Buffers.Binary;

namespace Liminal.Net.Core
{
    public class LiminalPacketFramerPipeline
    {
        private readonly List<ILiminalInboundTransformer> _inboundChain;
        private readonly List<ILiminalOutboundTransformer> _outboundChain;

        public void AddInbound(ILiminalInboundTransformer transformer) => _inboundChain.Add(transformer);
        public void AddOutbound(ILiminalOutboundTransformer transformer) => _outboundChain.Add(transformer);

        public LiminalPacketFramerPipeline(LiminalTransportConfig config)
        {
            _inboundChain = config.InboundPacketProcessors.ToList();
            _outboundChain = config.OutboundPacketProcessors.ToList();
        }
        public void ExecuteInbound(LiminalSession session, int blobLength)
        {
            ReadOnlySpan<byte> blob = session.IngestBuffer.GetSpan().Slice(0, blobLength);
            int offset = 0;

            while (offset + 4 <= blob.Length)
            {
                int innerLength = BinaryPrimitives.ReadInt32LittleEndian(blob.Slice(offset, 4));

                if (innerLength < 0 || offset + 4 + innerLength > blob.Length)
                {
                    LiminalLogger.LogError($"[Pipeline] Malformed inner packet length on client {session.Id}. Dropping batch.");
                    break;
                }

                ReadOnlySpan<byte> singlePacket = blob.Slice(offset + 4, innerLength);

                if (innerLength > session.StagingBufferA.GetSpan().Length)
                {
                    LiminalLogger.LogError($"[Pipeline] Packet {innerLength}b exceeds StagingBuffer size!");
                    offset += 4 + innerLength;
                    continue;
                }

                singlePacket.CopyTo(session.StagingBufferA.GetSpan());

                int currentLength = innerLength;
                bool isAtA = true;


                for (int i = 0; i < _inboundChain.Count; i++)
                {
                    var src = isAtA ? session.StagingBufferA.GetSpan() : session.StagingBufferB.GetSpan();
                    var dst = isAtA ? session.StagingBufferB.GetSpan() : session.StagingBufferA.GetSpan();

                    currentLength = _inboundChain[i].TransformInbound(src.Slice(0, currentLength), dst, session);
                    isAtA = !isAtA;

                    if (currentLength > session.StagingBufferA.GetSpan().Length)
                    {
                        LiminalLogger.LogError($"[Pipeline] Transformer {i} expanded packet to {currentLength}b, exceeding buffer limits!");
                        currentLength = 0;
                        break;
                    }
                }

                if (currentLength <= 0)
                {
                    offset += 4 + innerLength;
                    continue;
                }

                var finalResult = isAtA ? session.StagingBufferA.GetSpan() : session.StagingBufferB.GetSpan();

                lock (session)
                {
                    int totalWriteNeeded = 4 + currentLength;
                    int remainingSpace = session.ReceiveBuffer.GetSpan().Length - session.ReceiveCursor;

                    if (totalWriteNeeded > remainingSpace)
                    {
                        LiminalLogger.LogError($"[Pipeline] ReceiveBuffer Overflow on client {session.Id}! Cannot write {totalWriteNeeded}b.");
                    }
                    else
                    {
                        Span<byte> dest = session.ReceiveBuffer.GetSpan().Slice(session.ReceiveCursor);

                        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), currentLength);

                        finalResult.Slice(0, currentLength).CopyTo(dest.Slice(4));

                        session.ReceiveCursor += totalWriteNeeded;
                    }
                }

                offset += 4 + innerLength;
            }
        }

        public int ExecuteOutbound(LiminalSession session, int initialLength)
        {
            int currentLength = initialLength;
            bool isAtA = true;

            for (int i = 0; i < _outboundChain.Count; i++)
            {
                var source = isAtA ? session.StagingBufferA.GetSpan() : session.StagingBufferB.GetSpan();
                var dest = isAtA ? session.StagingBufferB.GetSpan() : session.StagingBufferA.GetSpan();

                currentLength = _outboundChain[i].TransformOutbound(source.Slice(0, currentLength), dest, session);
                isAtA = !isAtA;
            }

            var finalResult = isAtA ? session.StagingBufferA.GetSpan() : session.StagingBufferB.GetSpan();

            lock (session)
            {
                // 4 bytes for our 'Inner Length' + processed data
                int totalNeeded = 4 + currentLength;

                if (session.SendCursor + totalNeeded > session.SendBuffer.Memory.Length)
                {
                    LiminalLogger.LogError($"[Pipeline] SendBuffer Overflow on client {session.Id}!");
                    return -1;
                }

                Span<byte> dest = session.SendBuffer.GetSpan().Slice(session.SendCursor);

                BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(0, 4), currentLength);

                finalResult.Slice(0, currentLength).CopyTo(dest.Slice(4));

                session.SendCursor += totalNeeded;
            }

            return currentLength;
        }
    }
}
