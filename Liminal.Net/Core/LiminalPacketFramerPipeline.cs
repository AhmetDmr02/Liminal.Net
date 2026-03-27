using Liminal.Net.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Liminal.Net.Core
{
    public class LiminalPacketFramerPipeline
    {
        private readonly List<ILiminalInboundTransformer> _inboundChain;
        private readonly List<ILiminalOutboundTransformer> _outboundChain;

        public LiminalPacketFramerPipeline(LiminalTransportConfig config)
        {
            _inboundChain = config.InboundPacketProcessors.ToList();
            _outboundChain = config.OutboundPacketProcessors.ToList();
        }

        public int ExecuteOutboundBatch(LiminalSession session, int rawLength)
        {
            session.RawSendBuffer.GetSpan().Slice(0, rawLength).CopyTo(session.OutboundStagingA.GetSpan());
            int currentLength = rawLength;
            bool isAtA = true;

            for (int i = 0; i < _outboundChain.Count; i++)
            {
                var src = isAtA ? session.OutboundStagingA.GetSpan() : session.OutboundStagingB.GetSpan();
                var dst = isAtA ? session.OutboundStagingB.GetSpan() : session.OutboundStagingA.GetSpan();

                currentLength = _outboundChain[i].TransformOutbound(src.Slice(0, currentLength), dst, session);
                isAtA = !isAtA;
            }

            var finalResult = isAtA ? session.OutboundStagingA.GetSpan() : session.OutboundStagingB.GetSpan();
            finalResult.Slice(0, currentLength).CopyTo(session.SendBuffer.GetSpan());

            return currentLength;
        }

        public ReadOnlySpan<byte> ExecuteInboundBatch(LiminalSession session, ReadOnlySpan<byte> input)
        {
            input.CopyTo(session.InboundStagingA.GetSpan());
            int currentLength = input.Length;
            bool isAtA = true;

            for (int i = 0; i < _inboundChain.Count; i++)
            {
                var src = isAtA ? session.InboundStagingA.GetSpan() : session.InboundStagingB.GetSpan();
                var dst = isAtA ? session.InboundStagingB.GetSpan() : session.InboundStagingA.GetSpan();

                currentLength = _inboundChain[i].TransformInbound(src.Slice(0, currentLength), dst, session);
                isAtA = !isAtA;
                if (currentLength <= 0) return ReadOnlySpan<byte>.Empty;
            }

            var finalBuffer = isAtA ? session.InboundStagingA : session.InboundStagingB;
            return finalBuffer.GetSpan().Slice(0, currentLength);
        }
    }
}