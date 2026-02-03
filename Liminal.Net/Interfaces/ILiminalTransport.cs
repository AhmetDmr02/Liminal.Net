namespace Liminal.Net.Interfaces
{
    public delegate void DataReceivedHandler(ReadOnlySpan<byte> data, ushort clientId);

    public interface ILiminalTransport
    {
        const ushort SERVER_ID = 0;
        public bool IsConnected { get; }

        #region Initialization
        public void InitializeClientIdResolver(ILiminalClientIdResolver clientIdResolver);
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
        #endregion
    }
}
