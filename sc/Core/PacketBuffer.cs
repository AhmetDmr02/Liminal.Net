namespace Liminal.Net.Core
{
    public ref struct PacketBuffer
    {
        private readonly Span<byte> _rawBuffer;

        // The current 'Window' of data the processor should care about
        public int Offset { get; private set; }
        public int Length { get; private set; }

        public PacketBuffer(Span<byte> rawBuffer, int offset, int length)
        {
            _rawBuffer = rawBuffer;
            Offset = offset;
            Length = length;
        }

        /// <summary>
        /// Returns the current segment of the packet being processed.
        /// </summary>
        public Span<byte> Payload => _rawBuffer.Slice(Offset, Length);

        /// <summary>
        /// Moves the 'Start' of the window forward (Used when stripping headers).
        /// </summary>
        public void Skip(int bytes)
        {
            Offset += bytes;
            Length -= bytes;
        }

        /// <summary>
        /// Expands the window backward (Used when adding headers/headroom).
        /// </summary>
        public void ClaimHeadroom(int bytes)
        {
            Offset -= bytes;
            Length += bytes;
        }

        /// <summary>
        /// Returns a slice of the buffer relative to the current payload.
        /// Useful for 'Look-ahead' checks without moving the offset.
        /// </summary>
        public Span<byte> Slice(int start, int length) => Payload.Slice(start, length);
    }
}