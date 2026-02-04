using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Liminal.Net.Misc;

namespace Liminal.Net.Transports
{
    /// <summary>
    /// It uses tcp by default
    /// </summary>
    public class TcpTransport : ILiminalTransport
    {
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

        public virtual async void StartClient(string ip, int port)
        {
            try
            {
                TcpClient client = new TcpClient();

                TimeSpan timeout = TimeSpan.FromSeconds(_config.HandshakeTimeout * 2);
                using var cts = new CancellationTokenSource(timeout);

                await client.ConnectAsync(ip, port, cts.Token);
                client.NoDelay = true;

                _onHandshakeInitialized?.Invoke();

                ushort assignedId = await ClientHandshaker(client, _config);

                if (assignedId != 0)
                {
                    _isConnected = true;
                    _isServer = false;
                    //PromoteClient(assignedId, client);
                }
                else
                {
                    client.Close();
                }
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

            if (_sockets.TryRemove(clientId, out var pair) && _clientIdResolver.UnregisterClientId(clientId))
            {
                bool wasActive = _clientIdResolver.UnregisterClientId(clientId);
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

        public virtual void SendReliable(Span<byte> data, ushort clientId)
        {
            if (_sockets.TryGetValue(clientId, out var socket))
            {
                //GET IF NIC BUFFER OVERFLOWING WE NEED TO LOG
                socket.GetStream().Write(data);
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

        private void PromoteClient(ushort clientId, TcpClient client)
        {
            var endpoint = (IPEndPoint)client.Client.RemoteEndPoint;
            if(endpoint == null)
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} has no remote endpoint.");
                return;
            }

            if (_clientIdResolver.IsClientActive(clientId))
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} is already connected. Dropping old session.");
                Disconnect(clientId);
            }

            _sockets[clientId] = client;
            _clientIdResolver.RegisterClient(clientId, new ConnectionPair(clientId, endpoint));

            _onClientConnected?.Invoke(clientId);

            _ = Task.Run(() => ReceiveLoop(clientId, client));

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");

        }

        private async Task ReceiveLoop(ushort clientId, TcpClient client)
        {
            using var stream = client.GetStream();
            try
            {
                while (client.Connected && _isConnected)
                {
                    // Logic: 
                    // 1. Use a local byte[] or ILiminalFramer to read packets
                    // 2. _onReliable?.Invoke(packetData, clientId);
                    await Task.Delay(1); // Placeholder for actual framing logic
                }
            }
            finally
            {
                Disconnect(clientId);
            }
        }
    }
}
