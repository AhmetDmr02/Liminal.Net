using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public delegate void DataReceivedHandler(ReadOnlySpan<byte> data, ushort clientId);
    public delegate void TransportEventHandler();
    public delegate void ClientConnectionHandler(ushort clientId);

    public delegate Task<ushort> HandshakeOrchestrator<T>(T connection, LiminalTransportConfig config);

    public interface ILiminalTransport
    {
        public ushort LocalClientId { get; }

        const ushort SERVER_ID = 0;
        public bool IsConnected { get; }

        #region Initialization
        public void InitializeTransport(LiminalTransportConfig config);
        #endregion

        #region Connection
        public void StartServer(string ip, int port);
        public void StartClient(string ip, int port);
        #endregion

        #region Disconnection
        /// <summary>
        /// Disconnects the transport connection
        /// Also can act as KickPlayer if clientId is not 0 and its server
        /// </summary>
        public void Disconnect(ushort clientId = SERVER_ID);

        /// <summary>
        /// Shuts down the transport 
        /// </summary>
        public void Shutdown();
        #endregion

        #region Sending
        public void SendReliable(Span<byte> data, ushort clientId);
        public void SendUnreliable(Span<byte> data, ushort clientId);
        #endregion

        #region Events
        public event DataReceivedHandler OnMessageReceivedReliable;
        public event DataReceivedHandler OnMessageReceivedUnreliable;

        public event TransportEventHandler OnServerStarted;
        public event TransportEventHandler OnShutdown;
        public event TransportEventHandler OnHandshakeInitialized;

        public event ClientConnectionHandler OnLocalClientConnected;    // When this instance connects to a server
        public event ClientConnectionHandler OnLocalClientDisconnected; // When this instance's local connection drops
        public event ClientConnectionHandler OnClientConnected;         // When a remote client joins this server
        public event ClientConnectionHandler OnClientDisconnected;
        #endregion
    }
}
