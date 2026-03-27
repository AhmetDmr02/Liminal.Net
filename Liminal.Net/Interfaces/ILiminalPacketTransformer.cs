using Liminal.Net.Core;
using System;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalOutboundTransformer
    {
        /// <summary>
        /// Processes data from the input span and writes the result to the output span.
        /// </summary>
        /// <returns>The length of the transformed data.</returns>
        public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session);
    }
}
