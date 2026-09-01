using Liminal.Net.Transports;
using System;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalTransportFramingProvider
    {
        int CustomHeaderSize { get; }
    }

    public interface ILiminalTransportFramingProvider<TContext> : ILiminalTransportFramingProvider where TContext : struct
    {
        /// <summary>
        /// Writes custom metadata into the segment immediately after the base header.
        /// </summary>
        void WriteCustomHeader(Span<byte> destination, in TContext context);

        /// <summary>
        /// Reads and validates the custom metadata segment.
        /// </summary>
        bool TryReadCustomHeader(ReadOnlySpan<byte> source, out TContext context);
    }

    public sealed class DefaultTransportFramingProvider : ILiminalTransportFramingProvider<EmptyFramingContext>
    {
        public int CustomHeaderSize => 0;
        public void WriteCustomHeader(Span<byte> destination, in EmptyFramingContext context) { }

        public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out EmptyFramingContext context)
        {
            context = default;
            return true;
        }
    }
}