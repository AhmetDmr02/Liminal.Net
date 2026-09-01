using Liminal.Net.Interfaces;
using System;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Liminal.Net.Core
{
    [Flags]
    public enum TransportFlags : byte
    {
        Unreliable = 0,
        Reliable = 1 << 0,
        Fragmented = 1 << 1
    }

    public enum HeaderReadResult
    {
        Success,
        Incomplete, 
        Malformed  
    }

    public static class LiminalTransportHeader
    {
        public const int BaseHeaderSize = 5; // 1 byte Flags + 4 bytes Length

        public static int GetHeaderSize<TContext>(ILiminalTransportFramingProvider<TContext> framing = null)
                    where TContext : struct
        {
            return BaseHeaderSize + (framing?.CustomHeaderSize ?? 0);
        }

        /// <summary>
        /// Writes both Base and Custom headers into the target span.
        /// Returns the exact number of header bytes written.
        /// </summary>
        public static int WriteHeader<TContext>(Span<byte> destination, TransportFlags flags, int rawPayloadLength, in TContext framingContext, ILiminalTransportFramingProvider<TContext> framing = null) 
            where TContext : struct
        {
            int customSize = framing?.CustomHeaderSize ?? 0;
            int totalFramedPayload = customSize + rawPayloadLength;

            //Base Header
            destination[0] = (byte)flags;
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(1, 4), totalFramedPayload);

            //Custom Header
            if (customSize > 0 && framing != null)
            {
                framing.WriteCustomHeader(destination.Slice(BaseHeaderSize, customSize), in framingContext);
            }

            return BaseHeaderSize + customSize;
        }

        /// <summary>
        /// Reads and validates the header from the stream.
        /// Outputs flags, total header length, and the actual raw payload length.
        /// </summary>
        public static HeaderReadResult TryReadHeader<TContext>(ReadOnlySpan<byte> source, ILiminalTransportFramingProvider<TContext> framing, out TransportFlags flags, out int rawPayloadLength, out TContext framingContext)
            where TContext : struct
        {
            flags = TransportFlags.Unreliable;
            rawPayloadLength = 0;
            framingContext = default;

            int headerSize = GetHeaderSize(framing);

            if (source.Length < BaseHeaderSize)
            {
                return HeaderReadResult.Incomplete;
            }

            flags = (TransportFlags)source[0];
            int totalFramedPayload = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(1, 4));

            int customSize = framing?.CustomHeaderSize ?? 0;

            if (totalFramedPayload < customSize)
            {
                return HeaderReadResult.Malformed;
            }

            if (source.Length < headerSize)
            {
                return HeaderReadResult.Incomplete;
            }

            if (customSize > 0 && framing != null)
            {
                var customSpan = source.Slice(BaseHeaderSize, customSize);
                if (!framing.TryReadCustomHeader(customSpan, out framingContext))
                {
                    return HeaderReadResult.Malformed;
                }
            }

            rawPayloadLength = totalFramedPayload - customSize;
            return HeaderReadResult.Success;
        }
    }
}