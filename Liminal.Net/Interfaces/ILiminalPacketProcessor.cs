using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalPacketProcessor
    {
        /// <summary>
        /// How many bytes this processor adds to the FRONT of the packet.
        /// </summary>
        int HeaderTax { get; }

        /// <summary>
        /// Called when sending data. Wraps the payload.
        /// </summary>
        /// <param name="buffer">The current packet buffer.</param>
        /// <param name="context">The session info (contains the Private Token/Cookie).</param>
        void ProcessOutgoing(ref PacketBuffer buffer, ushort targetClientId);

        /// <summary>
        /// Called when receiving data. Unwraps the payload.
        /// </summary>
        /// <returns>True if the packet is valid, false if it should be dropped.</returns>
        bool ProcessIncoming(ref PacketBuffer buffer, ref ushort targetClientId);
    }
}
