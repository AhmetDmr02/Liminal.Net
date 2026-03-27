using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Transports
{
    /// <summary>
    /// It uses tcp by default
    /// </summary>
    public class TcpTransport : ILiminalTransport
    {
        protected volatile ushort _localClientId = 0;
        public ushort LocalClientId => _localClientId;

        protected volatile bool _isConnected = false;
        public bool IsConnected => _isConnected;

        protected volatile bool _isServer = false;
        public bool IsServer => _isServer;

        protected volatile bool _isClient = false;
        public bool IsClient => _isClient;

        protected TcpListener _listener;

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

        protected ClientConnectionHandler _onClientKicked;
        public event ClientConnectionHandler OnClientKicked
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onClientKicked, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onClientKicked, value);
        }
        #endregion

        protected LiminalTransportConfig _config;
        public LiminalTransportConfig Config => _config;

        protected ILiminalClientIdResolver _clientIdResolver;
        public ILiminalClientIdResolver ClientIdResolver => _clientIdResolver;

        internal readonly ConcurrentDictionary<ushort, TcpClient> _sockets = new();

        private int _isShuttingDown = 0;

        public virtual void InitializeTransport(LiminalTransportConfig config)
        {
            _config = config;

            _clientIdResolver = _config.ClientIdResolver;
        }

        public virtual void StartServer(string ip, int port)
        {
            IPAddress adress = string.IsNullOrEmpty(ip) ? IPAddress.Any : IPAddress.Parse(ip);
            _listener = new TcpListener(adress, port);
            _listener.Start(100);

            _isServer = true;
            _isConnected = true;
            _onServerStarted?.Invoke();

            _ = Task.Run(() => AcceptConnectionsAsync(_listener));
            LiminalLogger.Log($"Server started on {ip}:{port}");
        }

        public virtual void StartClient(string ip, int port)
        {
            try
            {
                TcpClient client = new TcpClient();
                client.NoDelay = true;

                _isClient = true;

                _ = Task.Run(() => TryToConnectAsync(client, (ip,port)));
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Client connection failed: {ex.Message}");
            }
        }

        public virtual void Disconnect()
        {
            if (_isServer)
            {
                LiminalLogger.Log("[Transport] Server initiated Shutdown.");
                Shutdown();
                return;
            }

            if (!_isServer && _isConnected)
            {
                _onLocalClientDisconnected?.Invoke(_localClientId);

                Shutdown();
                LiminalLogger.Log($"[Transport] Disconnected from server.");
            }
        }
        public virtual void Kick(ushort clientId)
        {
            if (!_isServer) return;

            if (clientId == LocalClientId)
            {
                // This is the server, we don't want to close the socket while we're still connected
                LiminalLogger.LogWarning($"[Transport] Can't kick the server.");
                return;
            }

            if (_sockets.TryRemove(clientId, out var clientSocket))
            {
                // Close the socket (This will throw exception in ReceiveLoop)
                try
                {
                    clientSocket.Close();
                }
                catch { }

                _clientIdResolver.UnregisterId(clientId);

                _onClientKicked?.Invoke(clientId);

                LiminalLogger.Log($"[Transport] Kicked client {clientId}.");
            }
            else
            {
                LiminalLogger.LogError($"[Transport] Couldn't find socket for client {clientId}");
            }
        }

        private readonly ArrayPool<byte> _sendBytePool = ArrayPool<byte>.Create(1024 * 128, 50);
        public virtual void SendReliable(Span<byte> data, ushort targetId)
        {
            int payloadSize = data.Length;

            var rentedBuffer = _sendBytePool.Rent(data.Length + 6);
            try
            {
                if (_sockets.TryGetValue(targetId, out var socket))
                {
                    Span<byte> fullPacket = rentedBuffer.AsSpan(0, payloadSize + 6);

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
                        Kick(targetId);
                    else
                        Shutdown();
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Send error: {ex.Message}");
            }
            finally
            {
                _sendBytePool.Return(rentedBuffer);
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
            if (Interlocked.Exchange(ref _isShuttingDown, 1) == 1)
            {
                return; // Already shutting down
            }
            try
            {
                _isConnected = false;
                _isServer = false;
                _isClient = false;
                _localClientId = 0;

                foreach (var id in _sockets.Keys)
                {
                    if (_sockets.TryRemove(id, out var clientSocket))
                    {
                        try
                        {
                            clientSocket.Close();
                        }
                        catch { }

                        _clientIdResolver.UnregisterId(id);

                        LiminalLogger.Log($"[Transport] Client {id} cleared.");
                    }
                }
                if (_listener != null)
                {
                    try
                    {
                        _listener.Stop();
                    }
                    catch { }
                    _listener = null;
                }
                _onShutdown?.Invoke();

            }
            finally
            {
                Interlocked.Exchange(ref _isShuttingDown, 0);
            }
        }
        protected async Task AcceptConnectionsAsync(TcpListener listener)
        {
            while (IsConnected && _isServer && listener == _listener)
            {
                try
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();
                    client.NoDelay = true;

                    _ = Task.Run(async () =>
                    {
                        _onHandshakeInitialized?.Invoke();
                        ushort finalId = await ServerHandshaker(client, _config);

                        if (finalId != 0) PromoteClient(finalId, client);
                        else client.Close();
                    });
                }
                catch (ObjectDisposedException)
                {
                    // Listener has been disposed
                    break;
                }
                catch (SocketException ex)
                {
                    if (!_isConnected || !_isServer || listener != _listener)
                    {
                        break;
                    }

                    LiminalLogger.LogError($"[Transport] Accept Socket Error: {ex.Message}");
                }
                catch (Exception ex)
                {
                    if (!_isConnected) break;

                    LiminalLogger.LogError($"[Transport] Accept error: {ex.Message}");
                }
            }
        }
        protected async Task TryToConnectAsync(TcpClient client, (string ip, int port) connectionInfo)
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_config.ConnectionTimeout));

                await client.ConnectAsync(connectionInfo.ip, connectionInfo.port, cts.Token);

                _onHandshakeInitialized?.Invoke();

                // Run the orchestrator
                _localClientId = await ClientHandshaker(client, _config);

                if (_localClientId != 0)
                {
                    _isConnected = true;

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
            if (endpoint == null)
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} has no remote endpoint.");
                client.Close();
                return;
            }
            if (!_clientIdResolver.RegisterId(clientId, new ConnectionPair(clientId, endpoint)))
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} registration failed. Dropping connection.");
                client.Close();
                return;
            }

            var oldClient = _sockets.AddOrUpdate(clientId, client, (key, old) =>
            {
                LiminalLogger.LogWarning($"[Transport] Replacing existing socket for client {clientId}");
                try { old.Close(); } catch { }
                return client;
            });

            _onClientConnected?.Invoke(clientId);
            _ = Task.Run(async () => ReceiveLoop(clientId, client));

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
        private async Task ReceiveLoop(ushort incomingId, TcpClient client)
        {
            using var ingestBuffer = new LiminalNativeBuffer(_config.MaxPacketSizePerBatch * 2);
            var stream = client.GetStream();
            int bytesInBuffer = 0;

            client.ReceiveTimeout = (int)_config.ConnectionTimeout;
            try
            {
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }
            catch
            {
                LiminalLogger.LogWarning($"[Transport] Failed to set keep alive on socket.");
            }

            try
            {
                while (client.Connected && _isConnected)
                {
                    int remainingSpace = ingestBuffer.Memory.Length - bytesInBuffer;
                    if (remainingSpace <= 0)
                    {
                        LiminalLogger.LogError($"[Transport] Buffer overflow on {incomingId}");
                        break;
                    }

                    Memory<byte> receiveTarget = ingestBuffer.Memory.Slice(bytesInBuffer, remainingSpace);

                    int read = await stream.ReadAsync(receiveTarget);

                    if (read <= 0) break;

                    bytesInBuffer += read;

                    Span<byte> bufferSpan = ingestBuffer.GetSpan();
                    int offset = 0;

                    while (bytesInBuffer - offset >= 6)
                    {
                        int payloadSize = BinaryPrimitives.ReadInt32LittleEndian(bufferSpan.Slice(offset + 2, 4));

                        if (payloadSize < 0 || payloadSize > _config.MaxPacketSizePerBatch - 6)
                        {
                            LiminalLogger.LogError($"[Transport] Invalid payload size {payloadSize}b on client {incomingId}");
                            Kick(incomingId);
                            return;
                        }

                        int totalPacketSize = 6 + payloadSize;

                        if (bytesInBuffer - offset < totalPacketSize)
                        {
                            // Incomplete packet, wait for more data
                            break; 
                        }

                        _onReliable?.Invoke(bufferSpan.Slice(offset + 6, payloadSize), incomingId);

                        offset += totalPacketSize;
                    }

                    if (offset > 0)
                    {
                        int remaining = bytesInBuffer - offset;
                        if (remaining > 0)
                        {
                            bufferSpan.Slice(offset, remaining).CopyTo(bufferSpan.Slice(0, remaining));
                        }
                        bytesInBuffer = remaining;
                    }
                }
            }
            catch (Exception ex)
            {
                if (_sockets.ContainsKey(incomingId))
                {
                    LiminalLogger.LogWarning($"[Transport] Client {incomingId} connection dropped: {ex.Message}");
                }
                else
                {
                    LiminalLogger.Log($"[Transport] Client {incomingId} is disconnected.");
                    return;
                }
            }
            finally
            {
                if (_isShuttingDown == 0)
                {
                    if (_sockets.TryRemove(incomingId, out var clientSocket))
                    {
                        try { clientSocket.Close(); } catch { LiminalLogger.LogWarning($"[Transport] Failed to close socket for client {incomingId}."); }

                        _clientIdResolver.UnregisterId(incomingId);

                        if (incomingId != ILiminalTransport.SERVER_ID)
                        {
                            _onClientDisconnected?.Invoke(incomingId);
                            LiminalLogger.Log($"[Transport] Client {incomingId} disconnected.");
                        }
                        else if (!_isServer)
                        {
                            LiminalLogger.Log("[Transport] Lost connection to host. Shutting down...");
                            _onLocalClientDisconnected?.Invoke(_localClientId);
                            Shutdown();
                        }
                    }
                }
            }
        }
    }
}
