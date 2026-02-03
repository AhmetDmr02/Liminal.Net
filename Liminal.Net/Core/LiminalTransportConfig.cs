using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    public class LiminalTransportConfig
    {
        /// <summary>
        /// The default host
        /// </summary>
        public string Default_Host = "127.0.0.1";
        
        /// <summary>
        /// The default port
        /// </summary>
        public int Default_Port = 7777;

        /// <summary>
        /// Specifies the maximum time, in seconds, to wait for a connection to be established before timing out.
        /// </summary>
        public float ConnectionTimeout = 5.0f;

        /// <summary>
        /// Specifies the maximum time, in seconds, to wait for a handshake to complete before timing out.
        /// </summary>
        public float HandshakeTimeout = 5.0f;

        /// <summary>
        /// The number of ticks per second
        /// </summary>
        public uint TickRate = 30;

        /// <summary>
        /// The maximum size of a packet
        /// </summary>
        public ushort MaxPacketSize = 1024;

        /// <summary>
        /// The maximum size of a handshake
        /// </summary>
        public ushort MaxHandshakeSize = 256;

        /// <summary>
        /// The version of the transport
        /// </summary>
        public ushort Version = 1;
        
        /// <summary>
        /// The maximum number of packets that will be held in the queue per connection
        /// </summary>
        public int MaxPacketCount = 10;

        public List<ILiminalPacketProcessor> PacketProcessors = new List<ILiminalPacketProcessor>();

        public ILiminalClientIdResolver ClientIdResolver;
    }
}
