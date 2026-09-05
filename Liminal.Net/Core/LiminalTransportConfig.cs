using Liminal.Net.Interfaces;
using System.Collections.Generic;

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
        /// Specifies the maximum time, in seconds, to wait for a response before timing out.
        /// </summary>
        public float ReceiveResponseTimeout = 50.0f;

        /// <summary>
        /// Specifies the maximum time, in seconds, to wait for a response before timing out.
        /// </summary>
        public float SendResponseTimeout = 10.0f;

        /// <summary>
        /// Specifies the maximum time, in seconds, to wait for a handshake to complete before timing out.
        /// </summary>
        public float HandshakeTimeout = 5.0f;

        /// <summary>
        /// The number of ticks per second
        /// </summary>
        public uint TickRate = 30;

        /// <summary>
        /// The maximum size of a packet buffer that will be sent in a tick
        /// Exceeding this size will cause a packet drop with a warning
        /// </summary>
        public ushort MaxPacketSizePerBatch = ushort.MaxValue;

        /// <summary>
        /// The maximum size of a handshake
        /// </summary>
        public ushort MaxHandshakeSize = 256;

        /// <summary>
        /// The version of the transport
        /// </summary>
        public ushort Version = 1;
        
        /// <summary>
        /// The maximum number of packets that will be held in the inbound queue per connection
        /// </summary>
        public int MaxPacketCount = 50;

        /// <summary>
        /// The maximum number of connections that will be allowed
        /// </summary>
        public int MaxConnectionCount = 1;

        /// <summary>
        /// The number of seconds to wait for a client to disconnect before forcing a kick
        /// </summary>
        public int WaitForKickGracePeriod = 10;

        public List<ILiminalInboundTransformer> InboundPacketProcessors = new();

        public List<ILiminalOutboundTransformer> OutboundPacketProcessors = new();

        public ILiminalTransportFramingProvider TransportFramingProvider { get; set; } = new DefaultTransportFramingProvider();

        public ILiminalClientIdResolver ClientIdResolver;
    }
}
