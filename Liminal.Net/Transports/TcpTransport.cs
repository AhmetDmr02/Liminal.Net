using Liminal.Net.Core;
using Liminal.Net.Interfaces;
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
        protected bool _isConnected = false;
        public bool IsConnected => _isConnected;

        protected bool _isServer = false;
        public bool IsServer => _isServer;

        #region Events
        protected DataReceivedHandler _onReliable;
        protected DataReceivedHandler _onUnreliable;

        public event DataReceivedHandler OnMessageReceivedReliable
        {
            add
            {
                DataReceivedHandler current = _onReliable;
                while (true)
                {
                    DataReceivedHandler combined = (DataReceivedHandler)Delegate.Combine(current, value);
                    DataReceivedHandler original = Interlocked.CompareExchange(ref _onReliable, combined, current);
                    if (original == current) break;
                    current = original;
                }
            }
            remove
            {
                DataReceivedHandler current = _onReliable;
                while (true)
                {
                    DataReceivedHandler removed = (DataReceivedHandler)Delegate.Remove(current, value);
                    DataReceivedHandler original = Interlocked.CompareExchange(ref _onReliable, removed, current);
                    if (original == current) break;
                    current = original;
                }
            }
        }
        public event DataReceivedHandler OnMessageReceivedUnreliable
        {
            add
            {
                DataReceivedHandler current = _onUnreliable;
                while (true)
                {
                    DataReceivedHandler combined = (DataReceivedHandler)Delegate.Combine(current, value);
                    DataReceivedHandler original = Interlocked.CompareExchange(ref _onUnreliable, combined, current);
                    if (original == current) break;
                    current = original;
                }
            }
            remove
            {
                DataReceivedHandler current = _onUnreliable;
                while (true)
                {
                    DataReceivedHandler removed = (DataReceivedHandler)Delegate.Remove(current, value);
                    DataReceivedHandler original = Interlocked.CompareExchange(ref _onUnreliable, removed, current);
                    if (original == current) break;
                    current = original;
                }
            }
        }
        #endregion

        protected LiminalTransportConfig _config;
        public LiminalTransportConfig Config => _config;

        protected ILiminalClientIdResolver _clientIdResolver;
        public ILiminalClientIdResolver ClientIdResolver => _clientIdResolver;

        protected readonly ConcurrentDictionary<ushort, ConnectionPair> _clients = new();

        internal readonly ConcurrentDictionary<ushort, (LiminalNativeBuffer sendBuffer, LiminalNativeBuffer receiveBuffer)> _clientBuffers = new();

        internal readonly ConcurrentDictionary<ushort, TcpClient> _sockets = new();

        public virtual void Disconnect(ushort clientId = 0)
        {
            if (clientId == 0) return;

            //Removing and Cleaning up Native Buffers
            if (_clientBuffers.Remove(clientId, out var buffers))
            {
                buffers.sendBuffer.ManualDispose();
                buffers.receiveBuffer.ManualDispose();
            }

            if (_clients.Remove(clientId, out var pair))
            {
                _clientIdResolver.ReleaseId(clientId);
            }

            if (_sockets.Remove(clientId, out var socket))
            {
                try
                {
                    socket.Close();
                }
                catch
                {
                    LiminalLogger.LogWarning($"[Transport] Failed to close socket.");
                }
            }
        }

        public virtual void InitializeTransport(LiminalTransportConfig config)
        {
            _config = config;

            _clientIdResolver = _config.ClientIdResolver;
        }

        public virtual void Poll()
        {
        }

        public virtual void SendReliable(Span<byte> data, ushort clientId)
        {
        }

        public virtual void SendUnreliable(Span<byte> data, ushort clientId)
        {
        }

        public virtual void Shutdown()
        {
        }

        public virtual void StartClient(string ip, int port)
        {

        }

        public virtual void StartServer(string ip, int port)
        {
            IPAddress adress = string.IsNullOrEmpty(ip) ? IPAddress.Any : IPAddress.Parse(ip);

            TcpListener listener = new TcpListener(adress, port);

            listener.Start();

            _isServer = true;
            _isConnected = true;

            _ = Task.Run(() => AcceptConnectionsAsync(listener));

            LiminalLogger.Log($"Server started on {ip}:{port}");
        }

        protected async Task AcceptConnectionsAsync(TcpListener listener)
        {
            var handshake = new HandshakePipeline(_clientIdResolver, _config.MaxHandshakeSize, (int)_config.HandshakeTimeout);

            while (IsConnected && _isServer)
            {
                try
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();

                    client.NoDelay = true;

                    _ = Task.Run(async () =>
                    {
                        ushort finalId = await handshake.TryVerifyClientAsync(client, _config.Version);

                        if (finalId != 0)
                        {
                            PromoteClient(finalId, client);
                        }
                        else
                        {
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

            if (_clients.ContainsKey(clientId))
            {
                LiminalLogger.LogWarning($"[Transport] Client {clientId} is already connected. Dropping old session.");
                Disconnect(clientId);
            }

            var sendBuffer = new LiminalNativeBuffer(_config.MaxPacketSize);
            var receiveBuffer = new LiminalNativeBuffer(_config.MaxPacketSize);

            _clientBuffers[clientId] = (sendBuffer, receiveBuffer);
            _clients[clientId] = new ConnectionPair(clientId,endpoint);
            _sockets[clientId] = client;

            _ = Task.Run(() => ReceiveLoop(clientId, client, receiveBuffer));

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");

        }

        private async Task ReceiveLoop(ushort clientId, TcpClient client, LiminalNativeBuffer receiveBuffer)
        {
            using var stream = client.GetStream();

            try
            {
                while (client.Connected && _isConnected)
                {
                    if(receiveBuffer.IsDisposed) break;

                    //CONT FROM HERE
                }
            }
            catch (Exception ex)
            {
                LiminalLogger.LogWarning($"[Transport] Receive loop ended for {clientId}: {ex.Message}");
            }
            finally
            {
                // Use the Disconnect method we built to clean up the native buffers and dictionaries
                Disconnect(clientId);
            }
        }
    }
}
