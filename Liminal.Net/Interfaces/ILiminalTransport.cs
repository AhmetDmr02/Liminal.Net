using Liminal.Net.Core;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Interfaces
{
    public delegate void DataReceivedHandler(ReadOnlySpan<byte> data, ushort clientId);
    public delegate void TransportEventHandler();
    public delegate void ClientConnectionHandler(ushort clientId);

    public delegate Task<HandshakeResult> ClientHandshakeOrchestrator<T>(T connection, LiminalTransportConfig config);
    public delegate Task<HandshakeResult> ServerHandshakeOrchestrator<T>(T connection, LiminalTransportConfig config, Func<bool> canAccept);

    public interface ILiminalTransport
    {
        public ushort LocalClientId { get; }

        public const ushort SERVER_ID = 0;

        public bool IsServer { get; }
        public bool IsClient { get; }

        public bool IsConnected { get; }

        public LiminalTransportConfig Config { get; }

        #region Connection State
        public bool IsClientConnected(ushort clientId);
        public int ConnectedClientCount { get; }
        #endregion

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
        /// </summary>
        public void Disconnect();

        /// <summary>
        /// Server Only: Forcibly removes a specific client.
        /// </summary>
        public void Kick(ushort clientId);

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
        public event ClientConnectionHandler OnClientKicked;
        #endregion
    }
}
