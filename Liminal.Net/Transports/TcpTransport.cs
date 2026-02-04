using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Liminal.Net.Transports
{
    /// <summary>
    /// It uses tcp by default
    /// </summary>
    public class TcpTransport : ILiminalTransport
    {
        protected ushort _localClientId = 0;
        public ushort LocalClientId => _localClientId;

        protected bool _isConnected = false;
        public bool IsConnected => _isConnected;

        protected bool _isServer = false;
        public bool IsServer => _isServer;

        public HandshakeOrchestrator<TcpClient> ServerHandshaker { get; set; } = DefaultHandshakes.ServerTcpHandshake;
        public HandshakeOrchestrator<TcpClient> ClientHandshaker { get; set; } = DefaultHandshakes.ClientTcpHandshake;

        #region Events
        protected DataReceivedHandler _onReliable;
        public event DataReceivedHandler OnMessageReceivedReliable
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onReliable, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onReliable, value);
        }

        protected DataReceivedHandler _onUnreliable;
        public event DataReceivedHandler OnMessageReceivedUnreliable
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onUnreliable, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onUnreliable, value);
        }

        protected TransportEventHandler _onServerStarted;
        public event TransportEventHandler OnServerStarted
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onServerStarted, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onServerStarted, value);
        }

        protected TransportEventHandler _onShutdown;
        public event TransportEventHandler OnShutdown
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onShutdown, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onShutdown, value);
        }

        protected TransportEventHandler _onHandshakeInitialized;
        public event TransportEventHandler OnHandshakeInitialized
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onHandshakeInitialized, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onHandshakeInitialized, value);
        }

        protected ClientConnectionHandler _onLocalClientConnected;
        public event ClientConnectionHandler OnLocalClientConnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLocalClientConnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLocalClientConnected, value);
        }

        protected ClientConnectionHandler _onLocalClientDisconnected;
        public event ClientConnectionHandler OnLocalClientDisconnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onLocalClientDisconnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onLocalClientDisconnected, value);
        }

        protected ClientConnectionHandler _onClientConnected;
        public event ClientConnectionHandler OnClientConnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientConnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientConnected, value);
        }

        protected ClientConnectionHandler _onClientDisconnected;
        public event ClientConnectionHandler OnClientDisconnected
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientDisconnected, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientDisconnected, value);
        }
        #endregion

        protected LiminalTransportConfig _config;
        public LiminalTransportConfig Config => _config;

        protected ILiminalClientIdResolver _clientIdResolver;
        public ILiminalClientIdResolver ClientIdResolver => _clientIdResolver;

        internal readonly ConcurrentDictionary<ushort, TcpClient> _sockets = new();

        public virtual void InitializeTransport(LiminalTransportConfig config)
        {
            _config = config;

            _clientIdResolver = _config.ClientIdResolver;
        }

        public virtual void StartServer(string ip, int port)
        {
            IPAddress adress = string.IsNullOrEmpty(ip) ? IPAddress.Any : IPAddress.Parse(ip);
            TcpListener listener = new TcpListener(adress, port);
            listener.Start();

            _isServer = true;
            _isConnected = true;
            _onServerStarted?.Invoke();

            _ = Task.Run(() => AcceptConnectionsAsync(listener));
            LiminalLogger.Log($"Server started on {ip}:{port}");
        }

        public virtual void StartClient(string ip, int port)
        {
            try
            {
                TcpClient client = new TcpClient();
                client.NoDelay = true;

                _ = Task.Run(() => TryToConnectAsync(client, (ip,port)));
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Client connection failed: {ex.Message}");
            }
        }

        public virtual void Disconnect(ushort clientId = 0)
        {
            //REWRITE this later
            if (clientId == 0) return;

            if (_sockets.TryRemove(clientId, out var pair) && _clientIdResolver.UnregisterId(clientId))
            {
                bool wasActive = _clientIdResolver.UnregisterId(clientId);
                pair.Close();

                if (wasActive)
                {
                    if (_isServer && clientId != ILiminalTransport.SERVER_ID)
                        _onClientDisconnected?.Invoke(clientId);
                    else
                        _onLocalClientDisconnected?.Invoke(clientId);
                }
            } 
            else
            {
                LiminalLogger.LogError($"[Transport] Failed to disconnect client {clientId}.");
            }
        }

        public virtual void SendReliable(Span<byte> data, ushort targetId)
        {

            try
            {
                if (_sockets.TryGetValue(targetId, out var socket))
                {
                    //GET IF NIC BUFFER OVERFLOWING WE NEED TO LOG
                    socket.GetStream().Write(data);

                    int payloadSize = data.Length;

                    Span<byte> fullPacket = stackalloc byte[6 + payloadSize];

                    fullPacket[0] = 1; //Reliable indicator
                    fullPacket[1] = 0; // Length/Metadata bit

                    BinaryPrimitives.WriteInt32LittleEndian(fullPacket.Slice(2, 4), payloadSize);
                    data.CopyTo(fullPacket.Slice(6));

                    socket.GetStream().Write(fullPacket);
                }
                else
                {
                    LiminalLogger.LogError($"[Transport] Couldn't find socket for client {targetId}");

                    if (IsServer)
                        Disconnect(targetId);
                    else
                        Shutdown();
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Send error: {ex.Message}");
            }

        }

        /// <summary>
        /// TCP is a reliable transport it will default to SendReliable
        /// </summary>
        public virtual void SendUnreliable(Span<byte> data, ushort clientId)
        {
            SendReliable(data, clientId);
        }

        public virtual void Shutdown()
        {
            _isConnected = false;
            _isServer = false;
            foreach (var id in _sockets.Keys) Disconnect(id);
            _onShutdown?.Invoke();
        }

        protected async Task AcceptConnectionsAsync(TcpListener listener)
        {
            while (IsConnected && _isServer)
            {
                try
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();
                    client.NoDelay = true;

                    _ = Task.Run(async () =>
                    {
                        _onHandshakeInitialized?.Invoke();
                        ushort finalId = await ServerHandshaker(client, _config);

                        if (finalId != 0)
                            PromoteClient(finalId, client);
                        else
                        {
                            client.Close();
                            LiminalLogger.LogWarning($"[Transport] Connection rejected during handshake.");
                        }

                    });
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Transport] Accept error: {ex.Message}");
                }
            }
        }

        protected async Task TryToConnectAsync(TcpClient client, (string ip, int port) connectionInfo)
        {
            try
            {
                await client.ConnectAsync(connectionInfo.ip, connectionInfo.port);

                _onHandshakeInitialized?.Invoke();

                // Run the orchestrator
                _localClientId = await ClientHandshaker(client, _config);

                if (_localClientId != 0)
                {
                    _isConnected = true;
                    _isServer = false;

                    PromoteLocalClient(_localClientId, client);
                }
                else
                {
                    client.Close();
                    _onLocalClientDisconnected?.Invoke(0);

                    Shutdown();
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Connection failed: {ex.Message}");

                Shutdown();
            }
        }

        private void PromoteClient(ushort clientId, TcpClient client)
        {
            var endpoint = (IPEndPoint)client.Client.RemoteEndPoint;
            if(endpoint == null)
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} has no remote endpoint.");
                return;
            }

            if (_clientIdResolver.IsConnectionActive(clientId))
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} is already connected. Dropping old session.");
                Disconnect(clientId);
            }

            _sockets[clientId] = client;
            _clientIdResolver.RegisterId(clientId, new ConnectionPair(clientId, endpoint));

            _onClientConnected?.Invoke(clientId);

            _ = Task.Run(() => ReceiveLoop(clientId, client));

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");
        }

        private void PromoteLocalClient(ushort assignedId, TcpClient client)
        {
            var endpoint = (IPEndPoint)client.Client.RemoteEndPoint;

            _sockets[ILiminalTransport.SERVER_ID] = client;

            _clientIdResolver.RegisterId(ILiminalTransport.SERVER_ID, new ConnectionPair(ILiminalTransport.SERVER_ID, endpoint));

            _ = Task.Run(() => ReceiveLoop(ILiminalTransport.SERVER_ID, client));

            _onLocalClientConnected?.Invoke(assignedId);

            LiminalLogger.Log($"[Transport] Successfully connected to server. Local ID: {assignedId}");
        }

        private async Task ReceiveLoop(ushort clientId, TcpClient client)
        {
            //TODO
        }
    }
}
