using Liminal.Net.Core;
using System;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Interfaces
{
    public delegate void DataReceivedHandler(ReadOnlySpan<byte> data, LiminalConnection from);
    public delegate void TransportEventHandler();
    public delegate void ClientConnectionHandler(LiminalConnection client);
    //localClientId is the id the server gave this instance, server is the connection to it, default when it never got that far
    public delegate void LocalConnectionHandler(ushort localClientId, LiminalConnection server);

    public delegate Task<HandshakeResult> ClientHandshakeOrchestrator<T>(T connection, LiminalNetworkConfig config);
    public delegate Task<HandshakeResult> ServerHandshakeOrchestrator<T>(T connection, LiminalNetworkConfig config, Func<bool> canAccept);

    public interface ILiminalTransport
    {
        public ushort LocalClientId { get; }

        public const ushort SERVER_ID = 0;

        public bool IsServer { get; }
        public bool IsClient { get; }

        public bool IsConnected { get; }

        public LiminalNetworkConfig Config { get; }

        public EndPoint? LocalEndpoint => null;

        #region Connection State
        public bool IsClientConnected(ushort clientId);
        public int ConnectedClientCount { get; }

        /// <summary>
        /// The client's current connection, default when it has none. On a client SERVER_ID gives the connection to the server.
        /// </summary>
        public LiminalConnection GetConnection(ushort clientId);
        #endregion

        #region Initialization
        public void InitializeTransport(LiminalNetworkConfig config);
        #endregion

        #region Connection
        public void StartServer(string ip, int port);
        public void StartClient(string ip, int port);
        #endregion

        #region Disconnection
        /// <summary>
        /// Disconnects the transport connection
        /// </summary>
        public void Disconnect();

        /// <summary>
        /// Client Only: disconnects only if the connection to the server is still this one, decided and done atomically.
        /// Anything acting for one connection uses this so it cant end a newer one after a restart.
        /// </summary>
        public void Disconnect(LiminalConnection server);

        /// <summary>
        /// Server Only: Forcibly removes a specific client.
        /// </summary>
        public void Kick(ushort clientId);

        /// <summary>
        /// Server Only: kicks only if the client's current connection is still this one, decided and claimed atomically.
        /// A delayed kick uses this so it cant hit a newer connection that reused the client id.
        /// </summary>
        public void Kick(LiminalConnection client);

        /// <summary>
        /// Shuts down the transport 
        /// </summary>
        public void Shutdown();
        #endregion

        #region Sending
        /// <summary>
        /// Sent only if the connection is still current, data meant for an ended connection never reaches one that reused the id.
        /// </summary>
        public void Send(Span<byte> data, LiminalConnection to, TransportFlags flags);
        #endregion

        #region Events
        public event DataReceivedHandler OnMessageReceivedReliable;
        public event DataReceivedHandler OnMessageReceivedUnreliable;

        public event DataReceivedHandler OnMessageReceivedFragmented;

        public event TransportEventHandler OnServerStarted;
        public event TransportEventHandler OnShutdown;
        public event TransportEventHandler OnHandshakeInitialized;

        public event LocalConnectionHandler OnLocalClientConnected;     // When this instance connects to a server
        public event LocalConnectionHandler OnLocalClientDisconnected;  // When this instance's local connection drops
        public event ClientConnectionHandler OnClientConnected;         // When a remote client joins this server
        public event ClientConnectionHandler OnClientDisconnected;
        public event ClientConnectionHandler OnClientKicked;

        /// <summary>
        /// Why the transport itself ended a connection (protocol violation, bad size, timeout, overflow). Local only, never sent to the peer.
        /// </summary>
        public event Action<LiminalConnection, DisconnectReason, string> OnTransportDisconnectReason;
        #endregion
    }
}
