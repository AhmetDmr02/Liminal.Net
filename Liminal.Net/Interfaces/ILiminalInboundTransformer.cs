using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalInboundTransformer
    {
        /// <summary>
        /// Processes data from the input span and writes the result to the output span.
        /// </summary>
        /// <returns>The length of the transformed data.</returns>
        
        public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session);
    }
}
