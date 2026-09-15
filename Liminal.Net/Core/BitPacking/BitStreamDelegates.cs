using System;

namespace Liminal.Net.Core
{
    /// <summary>
    /// Callback delegate for receiving a bitstream accompanied by typed metadata.
    /// </summary>
    /// <typeparam name="TMeta">The metadata struct type.</typeparam>
    /// <param name="meta">The deserialized metadata header.</param>
    /// <param name="reader">The bitstream reader, stack-allocated over the packet payload.</param>
    /// <param name="sender">The session ID of the sender.</param>
    public delegate void BitStreamHandler<TMeta>(in TMeta meta, ref BitReader reader, ushort sender);

    /// <summary>
    /// Callback delegate for receiving a bitstream tagged by a packet marker type without separate metadata.
    /// </summary>
    /// <param name="reader">The bitstream reader, stack-allocated over the packet payload.</param>
    /// <param name="sender">The session ID of the sender.</param>
    public delegate void BitStreamTagHandler(ref BitReader reader, ushort sender);

    /// <summary>
    /// Action delegate for packing bits into an outbound bitstream.
    /// </summary>
    /// <param name="writer">The bit writer reference targeting the outbound network buffer.</param>
    public delegate void BitStreamAction(ref BitWriter writer);

    /// <summary>
    /// Action delegate for packing bits into an outbound bitstream with state, guaranteeing zero GC closure allocations.
    /// </summary>
    /// <typeparam name="TState">The state struct type.</typeparam>
    /// <param name="writer">The bit writer reference targeting the outbound network buffer.</param>
    /// <param name="state">The state passed to the packer.</param>
    public delegate void BitStreamAction<TState>(ref BitWriter writer, in TState state);
}
