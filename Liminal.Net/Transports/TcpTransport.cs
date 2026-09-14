using Liminal.Net.Core;
using Liminal.Net.Handshakes;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Liminal.Net.Transports
{
    public readonly struct EmptyFramingContext { }
    public class TcpTransport : TcpTransport<EmptyFramingContext> { }

    public class TcpTransport<TContext> : ILiminalTransport, ITransportTelemetryProvider, ILiminalTransportDisconnectDiagnostics where TContext : struct
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
        private CancellationTokenSource _clientConnectCts;

        public ServerHandshakeOrchestrator<TcpClient> ServerHandshaker { get; set; } = DefaultHandshakes.ServerTcpHandshake;
        public ClientHandshakeOrchestrator<TcpClient> ClientHandshaker { get; set; } = DefaultHandshakes.ClientTcpHandshake;

        #region Events
        protected DataReceivedHandler _onFragmented;
        public event DataReceivedHandler OnMessageReceivedFragmented
        {
            add => LiminalAtomicHelpers.SafeAdd(ref _onFragmented, value);
            remove => LiminalAtomicHelpers.SafeRemove(ref _onFragmented, value);
        }

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

        protected LiminalNetworkConfig _config;
        public LiminalNetworkConfig Config => _config;

        protected ILiminalClientIdResolver _clientIdResolver;
        public ILiminalClientIdResolver ClientIdResolver => _clientIdResolver;

        internal readonly ConcurrentDictionary<ushort, TcpClient> _sockets = new();

        private readonly ConcurrentDictionary<TcpClient, byte> _finalizedConnections = new();
        private bool TryClaimDisconnect(TcpClient client) => _finalizedConnections.TryAdd(client, 0);

        private int _isShuttingDown = 0;

        public bool IsClientConnected(ushort clientId) => _sockets.ContainsKey(clientId);
        public int ConnectedClientCount => _sockets.Count;
        public int TotalConnections => Volatile.Read(ref _totalConnections);

        public TContext OutboundContext { get; set; }

        protected ILiminalTransportFramingProvider<TContext> _framing;
        private int _totalHeaderSize;

        public event Action<ushort, DisconnectReason, string> OnTransportDisconnectReason;

        internal readonly ConcurrentDictionary<ushort, ClientSendState> _sendQueues = new();
        private readonly object _connectionLifecycleLock = new();

        internal void SetConnectedForTesting(bool isConnected = true, bool isServer = true)
        {
            _isConnected = isConnected;
            _isServer = isServer;
        }

        #region Initialization
        public virtual void InitializeTransport(LiminalNetworkConfig config)
        {
            _config = config;
            _config.Validate();

            _framing = config.TransportFramingProvider as ILiminalTransportFramingProvider<TContext>;
            _totalHeaderSize = LiminalTransportHeader.GetHeaderSize(_framing);

            _clientIdResolver = _config.ClientIdResolver;
            _clientIdResolver?.Initialize(this);
        }

        public virtual void StartServer(string ip, int port)
        {
            IPAddress address = string.IsNullOrEmpty(ip) ? IPAddress.Any : IPAddress.Parse(ip);
            _listener = new TcpListener(address, port);
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
                _clientConnectCts?.Cancel();
                _clientConnectCts?.Dispose();
                _clientConnectCts = new CancellationTokenSource();

                TcpClient client = new TcpClient();
                client.NoDelay = true;

                _isClient = true;

                var token = _clientConnectCts.Token;
                _ = Task.Run(() => TryToConnectAsync(client, (ip, port), token), token);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Client connection failed: {ex.Message}");
            }
        }
        #endregion

        #region Shutdown & Disconnect & Kick
        public virtual void Disconnect()
        {
            if (_isServer)
            {
                LiminalLogger.Log("[Transport] Server initiated Shutdown.");
                Shutdown();
                return;
            }

            if (_isClient)
            {
                _clientConnectCts?.Cancel();

                ushort disconnectedId = _localClientId;
                bool wasConnected = _isConnected;
                _isConnected = false;

                if (wasConnected)
                {
                    _onLocalClientDisconnected?.Invoke(disconnectedId);
                }

                Shutdown();
                LiminalLogger.Log("[Transport] Disconnected from server.");
            }
        }

        public virtual void Kick(ushort clientId)
        {
            if (!_isServer)
                return;

            if (clientId == ILiminalTransport.SERVER_ID)
            {
                LiminalLogger.LogError("[Transport] Attempted to kick Server ID (0). Internal server error.");
                return;
            }

            if (clientId == LocalClientId && LocalClientId != ILiminalTransport.SERVER_ID)
            {
                LiminalLogger.LogWarning($"[Transport] Host local client {clientId} was kicked. Shutting down host session.");
                Shutdown();
                return;
            }

            if (_sockets.TryGetValue(clientId, out var clientSocket))
            {
                _onClientKicked?.Invoke(clientId);
                try { clientSocket.Close(); } catch { }

                ((ICollection<KeyValuePair<ushort, TcpClient>>)_sockets).Remove(new KeyValuePair<ushort, TcpClient>(clientId, clientSocket));

                if (_sendQueues.TryGetValue(clientId, out var sendState))
                {
                    TeardownSendQueue(clientId, sendState);
                }
                else
                {
                    TeardownSendQueue(clientId);
                }

                if (TryClaimDisconnect(clientSocket))
                {
                    Interlocked.Decrement(ref _totalConnections);
                    _onClientDisconnected?.Invoke(clientId);
                    LiminalLogger.Log($"[Transport] Client {clientId} disconnected.");
                }

                LiminalLogger.Log($"[Transport] Kicked client {clientId}.");
            }
            else
            {
                LiminalLogger.LogError($"[Transport] Couldn't find socket for client {clientId}");
            }
        }

        public virtual void Shutdown()
        {
            if (Interlocked.Exchange(ref _isShuttingDown, 1) == 1)
            {
                return;
            }

            LiminalLogger.Log($"[Transport-Debug] Shutdown initiated by thread '{Thread.CurrentThread.Name ?? Thread.CurrentThread.ManagedThreadId.ToString()}'. Stack:\n{Environment.StackTrace}", LiminalLogger.LogLevel.Detailed);

            try
            {
                _clientConnectCts?.Cancel();
                _clientConnectCts?.Dispose();
                _clientConnectCts = null;

                _isConnected = false;
                _isServer = false;
                _isClient = false;
                _localClientId = 0;

                foreach (var id in _sockets.Keys)
                {
                    TeardownSendQueue(id);

                    if (_sockets.TryRemove(id, out var clientSocket))
                    {
                        try { clientSocket.Close(); } catch { }
                        LiminalLogger.Log($"[Transport] Client {id} cleared.");
                    }
                }

                foreach (var id in _sendQueues.Keys)
                {
                    TeardownSendQueue(id);
                }

                if (_listener != null)
                {
                    try { _listener.Stop(); } catch { }
                    _listener = null;
                }

                _onShutdown?.Invoke();
            }
            finally
            {
                Interlocked.Exchange(ref _isShuttingDown, 0);
                Volatile.Write(ref _totalConnections, 0);
                _finalizedConnections.Clear();
            }
        }

        private void TeardownSendQueue(ushort clientId, ClientSendState ownedState = null)
        {
            ClientSendState state;

            if (ownedState != null)
            {
                if (!((ICollection<KeyValuePair<ushort, ClientSendState>>)_sendQueues)
                        .Remove(new KeyValuePair<ushort, ClientSendState>(clientId, ownedState)))
                {
                    return;
                }
                state = ownedState;
            }
            else if (!_sendQueues.TryRemove(clientId, out state))
            {
                return;
            }

            try { state.LifetimeCts.Cancel(); } catch (ObjectDisposedException) { }
            state.Channel.Writer.TryComplete();

            if (state.WriterTask != null)
                _ = state.WriterTask.ContinueWith(_ => state.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
            else
                state.Dispose();
        }
        #endregion

        #region Sending
        private readonly ArrayPool<byte> _sendBytePool = ArrayPool<byte>.Create(1024 * 128, 50);

        public virtual void Send(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            SendInternal(data, targetId, flags);
        }

        protected virtual void SendInternal(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            if (!_sendQueues.TryGetValue(targetId, out var sendState))
            {
                LiminalLogger.LogError($"[Transport] Cannot send: no outbound queue for client {targetId}");
                if (IsServer) Kick(targetId);
                else Shutdown();
                return;
            }

            int headerSize = LiminalTransportHeader.GetHeaderSize(_framing);
            int totalSize = headerSize + data.Length;
            TContext contextSnapshot = OutboundContext;

            byte[] rentedBuffer = _sendBytePool.Rent(totalSize);
            Span<byte> fullPacket = rentedBuffer.AsSpan(0, totalSize);
            bool queued = false;

            try
            {
                LiminalTransportHeader.WriteHeader(fullPacket, flags, data.Length, in contextSnapshot, _framing);
                data.CopyTo(fullPacket.Slice(headerSize));

                var packet = new OutboundPacket(rentedBuffer, totalSize);

                if (!sendState.Channel.Writer.TryWrite(packet))
                {
                    LiminalLogger.LogWarning($"[Transport] Send queue rejected packet for client {targetId}. kicking.");
                    return;
                }

                queued = true;
            }
            finally
            {
                if (!queued)
                {
                    _sendBytePool.Return(rentedBuffer);

                    if (IsServer)
                        Kick(targetId);
                    else
                        Shutdown();
                }
            }
        }

        private async Task ProcessSendQueueAsync(ushort clientId, TcpClient client, ClientSendState state)
        {
            var reader = state.Channel.Reader;
            var stream = client.GetStream();
            var lifetimeToken = state.LifetimeCts.Token;

            int timeoutSeconds = (int)_config.SendResponseTimeout;
            bool timeoutEnabled = timeoutSeconds > 0;
            TimeSpan timeoutSpan = TimeSpan.FromSeconds(timeoutSeconds);

            IoDeadlineWatchdog sendWatchdog = timeoutEnabled ? new IoDeadlineWatchdog(client) : null;

            try
            {
                while (await reader.WaitToReadAsync(lifetimeToken).ConfigureAwait(false))
                {
                    while (reader.TryRead(out OutboundPacket packet))
                    {
                        bool packetCompleted = false;

                        try
                        {
                            if (timeoutEnabled)
                                sendWatchdog.Arm(timeoutSpan);

                            try
                            {
                                await stream.WriteAsync(packet.Buffer.AsMemory(0, packet.Length), lifetimeToken).ConfigureAwait(false);
                            }
                            catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
                            {
                                if (sendWatchdog != null && sendWatchdog.TimedOut)
                                {
                                    LiminalLogger.LogWarning($"[Transport] Send timed out on client {clientId} (exceeded {timeoutSeconds}s).");
                                    OnTransportDisconnectReason?.Invoke(clientId, DisconnectReason.Timeout, $"Send timed out ({timeoutSeconds}s).");

                                    if (IsServer) Kick(clientId);
                                    else Shutdown();
                                    return;
                                }

                                throw;
                            }
                            finally
                            {
                                if (timeoutEnabled)
                                    sendWatchdog.Disarm();
                            }

                            if ((_telemetryConfig?.Flags & TelemetryFlags.ByteCounting) != 0)
                            {
                                Interlocked.Add(ref _totalBytesOutbound, packet.Length);
                            }

                            packetCompleted = true;
                        }
                        finally
                        {
                            _sendBytePool.Return(packet.Buffer);
                        }

                        if (!packetCompleted)
                            return;
                    }

                    if (timeoutEnabled)
                        sendWatchdog.Arm(timeoutSpan);

                    try
                    {
                        await stream.FlushAsync(lifetimeToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
                    {
                        if (sendWatchdog != null && sendWatchdog.TimedOut)
                        {
                            LiminalLogger.LogWarning($"[Transport] Send flush timed out on client {clientId} (exceeded {timeoutSeconds}s).");
                            OnTransportDisconnectReason?.Invoke(clientId, DisconnectReason.Timeout, $"Send flush timed out ({timeoutSeconds}s).");

                            if (IsServer) Kick(clientId);
                            else Shutdown();
                            return;
                        }

                        throw;
                    }
                    finally
                    {
                        if (timeoutEnabled)
                            sendWatchdog.Disarm();
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
            {
                LiminalLogger.LogWarning($"[Transport] Outbound writer aborted on client {clientId}: {ex.Message}");

                if (IsServer) Kick(clientId);
                else Shutdown();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Unexpected outbound writer failure on {clientId}: {ex.Message}");

                if (IsServer) Kick(clientId);
                else Shutdown();
            }
            finally
            {
                sendWatchdog?.Dispose();

                while (reader.TryRead(out OutboundPacket discarded))
                {
                    _sendBytePool.Return(discarded.Buffer);
                }
            }
        }

        protected async Task TryToConnectAsync(TcpClient client, (string ip, int port) connectionInfo, CancellationToken cancelToken = default)
        {
            try
            {
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_config.ConnectionTimeout));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancelToken);

                using (linkedCts.Token.Register(() => { try { client.Close(); } catch { } }))
                {
                    await client.ConnectAsync(connectionInfo.ip, connectionInfo.port, linkedCts.Token).ConfigureAwait(false);
                }

                if (cancelToken.IsCancellationRequested || _isShuttingDown != 0)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                _onHandshakeInitialized?.Invoke();

                HandshakeResult result = await ClientHandshaker(client, _config).ConfigureAwait(false);

                if (cancelToken.IsCancellationRequested || _isShuttingDown != 0)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                if (result.Success)
                {
                    _localClientId = result.ClientId;
                    PromoteLocalClient(_localClientId, client);
                }
                else
                {
                    try { client.Close(); } catch { }

                    OnTransportDisconnectReason?.Invoke(ILiminalTransport.SERVER_ID, result.FailureReason, result.FailureMessage);

                    _onLocalClientDisconnected?.Invoke(0);
                    Shutdown();
                }
            }
            catch (OperationCanceledException)
            {
                try { client.Close(); } catch { }
            }
            catch (Exception ex)
            {
                try { client.Close(); } catch { }
                LiminalLogger.LogError($"[Transport] Connection failed: {ex.Message}");
                Shutdown();
            }
        }
        #endregion

        #region Receiving
        private int _totalConnections = 0;

        protected async Task AcceptConnectionsAsync(TcpListener listener)
        {
            while (IsConnected && _isServer && listener == _listener)
            {
                TcpClient client = null;
                bool slotReserved = false;
                bool handedOff = false;

                try
                {
                    client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    client.NoDelay = true;

                    var acceptedClient = client;

                    _ = Task.Run(async () =>
                    {
                        bool promoted = false;
                        try
                        {
                            _onHandshakeInitialized?.Invoke();

                            var pipeline = new TcpHandshakePipeline(_clientIdResolver, _config);

                            HandshakeResult result = await pipeline.TryVerifyClientAsync(
                                acceptedClient,
                                _config.Version,
                                () =>
                                {
                                    while (true)
                                    {
                                        int current = Volatile.Read(ref _totalConnections);
                                        if (current >= _config.MaxConnectionCount)
                                            return false;

                                        if (Interlocked.CompareExchange(ref _totalConnections, current + 1, current) == current)
                                        {
                                            slotReserved = true;
                                            return true;
                                        }
                                    }
                                },
                                assignedId =>
                                {
                                    PromoteClient(assignedId, acceptedClient);
                                    promoted = true;
                                }
                            ).ConfigureAwait(false);

                            if (!result.Success)
                            {
                                LiminalLogger.LogWarning($"[Transport] Handshake rejected client: {result.FailureReason} - {result.FailureMessage}");
                                try { acceptedClient.Close(); } catch { }
                            }
                        }
                        catch (Exception ex)
                        {
                            LiminalLogger.LogError($"[Transport] Handshake error: {ex.Message}");
                            try { acceptedClient.Close(); } catch { }
                        }
                        finally
                        {
                            if (!promoted && slotReserved)
                            {
                                Interlocked.Decrement(ref _totalConnections);
                            }
                        }
                    });

                    handedOff = true;
                }
                catch (ObjectDisposedException) { break; }
                catch (SocketException ex)
                {
                    if (!_isConnected || !_isServer || listener != _listener) break;
                    LiminalLogger.LogError($"[Transport] Accept Socket Error: {ex.Message}");
                }
                catch (Exception ex)
                {
                    if (!_isConnected) break;
                    LiminalLogger.LogError($"[Transport] Accept error: {ex.Message}");
                }
                finally
                {
                    if (client != null && !handedOff)
                    {
                        try { client.Close(); } catch { }
                    }
                }
            }
        }

        internal void PromoteClient(ushort clientId, TcpClient client)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            int maxPacketCount = _config.Hiccup.Enabled ? _config.MaxPacketCount * _config.Hiccup.MaxRecoveryScale : _config.MaxPacketCount;
            maxPacketCount = Math.Max(_config.MaxPacketCount, maxPacketCount);

            ClientSendState sendState;
            ClientSendState oldSendState = null;
            TcpClient oldClient = null;

            lock (_connectionLifecycleLock)
            {
                if (_isShuttingDown != 0 || !_isConnected)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                sendState = new ClientSendState(maxPacketCount);

                _sendQueues.AddOrUpdate(clientId, sendState, (k, old) =>
                {
                    oldSendState = old;
                    return sendState;
                });

                _sockets.AddOrUpdate(clientId, client, (key, old) =>
                {
                    oldClient = old;
                    if (TryClaimDisconnect(old))
                    {
                        Interlocked.Decrement(ref _totalConnections);
                    }
                    return client;
                });

                _clientIdResolver.ConfirmRegistration(clientId);

                sendState.WriterTask = Task.Run(() => ProcessSendQueueAsync(clientId, client, sendState));
                _ = Task.Run(async () => ReceiveLoop(clientId, client, sendState));
            }

            if (oldSendState != null)
            {
                oldSendState.LifetimeCts.Cancel();
                oldSendState.Channel.Writer.TryComplete();

                if (oldSendState.WriterTask != null)
                    _ = oldSendState.WriterTask.ContinueWith(_ => oldSendState.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
                else
                    oldSendState.Dispose();
            }

            if (oldClient != null)
            {
                LiminalLogger.LogWarning($"[Transport] Replacing existing socket for client {clientId}");
                try { oldClient.Close(); } catch { }
            }

            _onClientConnected?.Invoke(clientId);

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");
        }

        private void PromoteLocalClient(ushort assignedId, TcpClient client)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            var sendState = new ClientSendState();
            _sendQueues[ILiminalTransport.SERVER_ID] = sendState;
            _sockets[ILiminalTransport.SERVER_ID] = client;

            _isConnected = true;
            _onLocalClientConnected?.Invoke(assignedId);

            if (!IsCurrentConnection(ILiminalTransport.SERVER_ID, client, sendState))
                return;

            sendState.WriterTask = Task.Run(() => ProcessSendQueueAsync(ILiminalTransport.SERVER_ID, client, sendState));
            _ = Task.Run(() => ReceiveLoop(ILiminalTransport.SERVER_ID, client, sendState));

            LiminalLogger.Log($"[Transport] Successfully connected to server. Local ID: {assignedId}");
        }

        private bool IsCurrentConnection(ushort clientId, TcpClient client, ClientSendState sendState)
        {
            return _sockets.TryGetValue(clientId, out var currentClient) &&
                   ReferenceEquals(currentClient, client) &&
                   _sendQueues.TryGetValue(clientId, out var currentSendState) &&
                   ReferenceEquals(currentSendState, sendState);
        }

        private async Task ReceiveLoop(ushort incomingId, TcpClient client, ClientSendState ownedSendState)
        {
            using var ingestBuffer = new LiminalNativeBuffer(_config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch));
            var stream = client.GetStream();
            int bytesInBuffer = 0;

            bool isHostSelfLoop = _isServer && _isClient && (incomingId == ILiminalTransport.SERVER_ID || incomingId == _localClientId);

            int timeoutSeconds = (int)_config.ReceiveResponseTimeout;
            bool timeoutEnabled = timeoutSeconds > 0 && !isHostSelfLoop;
            TimeSpan timeoutSpan = TimeSpan.FromSeconds(timeoutSeconds);

            IoDeadlineWatchdog recvWatchdog = timeoutEnabled ? new IoDeadlineWatchdog(client) : null;

            try
            {
                try { client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true); } catch { }

                while (client.Connected && _isConnected)
                {
                    int remainingSpace = ingestBuffer.Memory.Length - bytesInBuffer;
                    if (remainingSpace <= 0)
                    {
                        LiminalLogger.LogError($"[Transport] Buffer overflow on {incomingId}");
                        OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.InboundQueueOverflow, "Ingest buffer overflow.");
                        Kick(incomingId);
                        return;
                    }

                    Memory<byte> receiveTarget = ingestBuffer.Memory.Slice(bytesInBuffer, remainingSpace);
                    int read;

                    if (timeoutEnabled)
                    {
                        recvWatchdog.Arm(timeoutSpan);

                        try
                        {
                            read = await stream.ReadAsync(receiveTarget).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
                        {
                            if (recvWatchdog.TimedOut)
                            {
                                LiminalLogger.LogWarning($"[Transport] Client {incomingId} timed out (exceeded {timeoutSeconds}s).");
                                OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.Timeout, $"Receive timeout exceeded ({timeoutSeconds}s).");
                                Kick(incomingId);
                                return;
                            }
                            throw;
                        }
                        finally
                        {
                            recvWatchdog.Disarm();
                        }
                    }
                    else
                    {
                        read = await stream.ReadAsync(receiveTarget).ConfigureAwait(false);
                    }

                    if (read <= 0) break;

                    if ((_telemetryConfig?.Flags & TelemetryFlags.ByteCounting) != 0)
                        Interlocked.Add(ref _totalBytesInbound, read);

                    bytesInBuffer += read;
                    ProcessIngestBufferSynchronous(incomingId, ingestBuffer, ref bytesInBuffer);

                    if (!IsClientConnected(incomingId) && incomingId != ILiminalTransport.SERVER_ID)
                        break;
                }
            }
            catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
            {
                if (recvWatchdog == null || !recvWatchdog.TimedOut)
                    LiminalLogger.LogWarning($"[Transport-Debug] Socket closed/dropped on {incomingId}. Reason: {ex.GetType().Name} - {ex.Message}");
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport-Debug] Unexpected receive error on {incomingId}: {ex.GetType().Name} - {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                recvWatchdog?.Dispose();

                if (_isShuttingDown == 0)
                {
                    bool isServerConn = incomingId == ILiminalTransport.SERVER_ID;

                    try { client.Close(); } catch { }

                    TeardownSendQueue(incomingId, ownedSendState);

                    if (!isServerConn)
                    {
                        ((ICollection<KeyValuePair<ushort, TcpClient>>)_sockets).Remove(new KeyValuePair<ushort, TcpClient>(incomingId, client));

                        if (TryClaimDisconnect(client))
                        {
                            Interlocked.Decrement(ref _totalConnections);
                            _onClientDisconnected?.Invoke(incomingId);
                            LiminalLogger.Log($"[Transport] Client {incomingId} disconnected.");
                        }
                    }
                    else if (_isClient)
                    {
                        LiminalLogger.Log("[Transport] Local client connection to server was lost. Shutting down.");

                        ushort disconnectedId = _localClientId;
                        _isConnected = false;

                        _onLocalClientDisconnected?.Invoke(disconnectedId);
                        Shutdown();
                    }

                    _finalizedConnections.TryRemove(client, out _);
                }
            }
        }

        private void ProcessIngestBufferSynchronous(ushort incomingId, LiminalNativeBuffer ingestBuffer, ref int bytesInBuffer)
        {
            Span<byte> bufferSpan = ingestBuffer.GetSpan();
            int offset = 0;
            int headerSize = LiminalTransportHeader.GetHeaderSize(_framing);

            while (bytesInBuffer - offset >= LiminalTransportHeader.BaseHeaderSize)
            {
                var currentSlice = bufferSpan.Slice(offset, bytesInBuffer - offset);
                var result = LiminalTransportHeader.TryReadHeader(currentSlice, _framing, out var flags, out int payloadLength, out TContext framingContext);

                if (result == HeaderReadResult.Incomplete) break;

                if (result == HeaderReadResult.Malformed)
                {
                    LiminalLogger.LogError($"[Transport] Malformed frame header received from {incomingId}. Kicking connection.");
                    OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.ProtocolViolation, "Malformed transport frame header.");
                    Kick(incomingId);
                    return;
                }

                if (payloadLength < 0 || payloadLength > _config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch))
                {
                    LiminalLogger.LogError($"[Transport] Invalid payload size {payloadLength}b on client {incomingId}");
                    OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.InvalidPacketSize, $"Payload length {payloadLength}b outside allowed bounds.");
                    Kick(incomingId);
                    return;
                }

                int totalFrameSize = headerSize + payloadLength;
                if (bytesInBuffer - offset < totalFrameSize) break;

                var payloadSpan = bufferSpan.Slice(offset + headerSize, payloadLength);

                switch (flags)
                {
                    case var f when (f & TransportFlags.WirePing) != 0:
                        if (payloadSpan.Length >= 16)
                        {
                            float countdownMs = 0f;
                            if (NextTickProvider != null)
                            {
                                long nextTick = NextTickProvider();
                                long diffTicks = Math.Max(0, nextTick - Stopwatch.GetTimestamp());
                                countdownMs = (float)((diffTicks * 1000.0) / Stopwatch.Frequency);
                            }

                            Span<byte> pongPayload = stackalloc byte[16];
                            payloadSpan.Slice(0, 12).CopyTo(pongPayload);
                            BinaryPrimitives.WriteInt32LittleEndian(pongPayload.Slice(12, 4), BitConverter.SingleToInt32Bits(countdownMs));

                            SendInternal(pongPayload, incomingId, TransportFlags.WirePong);
                        }
                        break;

                    case var f when (f & TransportFlags.WirePong) != 0:
                        HandleWirePong(incomingId, payloadSpan);
                        break;

                    case var f when (f & TransportFlags.Fragmented) != 0:
                        _onFragmented?.Invoke(payloadSpan, incomingId);
                        break;

                    case var f when (f & TransportFlags.Reliable) != 0:
                        _onReliable?.Invoke(payloadSpan, incomingId);
                        break;

                    default:
                        _onUnreliable?.Invoke(payloadSpan, incomingId);
                        break;
                }

                offset += totalFrameSize;
            }

            if (offset > 0)
            {
                int remaining = bytesInBuffer - offset;
                if (remaining > 0)
                    bufferSpan.Slice(offset, remaining).CopyTo(bufferSpan.Slice(0, remaining));
                bytesInBuffer = remaining;
            }
        }
        #endregion

        #region Telemetry
        private volatile LiminalTelemetryConfig _telemetryConfig;
        private long _totalBytesInbound;
        private long _totalBytesOutbound;

        public Func<long> NextTickProvider { get; set; }

        public GlobalTransportTelemetrySnapshot GetGlobalTransportSnapshot()
        {
            return new GlobalTransportTelemetrySnapshot(
                totalBytesInbound: Volatile.Read(ref _totalBytesInbound),
                totalBytesOutbound: Volatile.Read(ref _totalBytesOutbound),
                packetLossRate: 0.0f
            );
        }

        public void InitializeConfig(LiminalTelemetryConfig config)
        {
            _telemetryConfig = config;
        }

        private readonly ConcurrentDictionary<ushort, double> _wireRttMap = new();
        private readonly ConcurrentDictionary<ushort, (uint Seq, long SentTicks)> _wireInFlight = new();
        private uint _wireSeqCounter;

        public bool TryGetWireRTT(ushort clientId, out double rttMs)
        {
            return _wireRttMap.TryGetValue(clientId, out rttMs);
        }

        private static readonly long WirePingTimeoutTicks = Stopwatch.Frequency * 3;

        public void SendWirePing(ushort targetId)
        {
            if (!_sockets.ContainsKey(targetId)) return;

            long now = Stopwatch.GetTimestamp();

            if (_wireInFlight.TryGetValue(targetId, out var existing))
            {
                if (existing.SentTicks != 0)
                {
                    if ((now - existing.SentTicks) < WirePingTimeoutTicks)
                    {
                        return;
                    }

                    _wireRttMap[targetId] = 999.0;
                }
            }

            uint seq = unchecked(++_wireSeqCounter);
            _wireInFlight[targetId] = (seq, now);

            Span<byte> pingPayload = stackalloc byte[16];
            BinaryPrimitives.WriteUInt32LittleEndian(pingPayload.Slice(0, 4), seq);
            BinaryPrimitives.WriteInt64LittleEndian(pingPayload.Slice(4, 8), now);
            BinaryPrimitives.WriteInt32LittleEndian(pingPayload.Slice(12, 4), BitConverter.SingleToInt32Bits(0f));

            SendInternal(pingPayload, targetId, TransportFlags.WirePing);
        }

        private double _serverCountdownSnapshotMs;
        private long _serverCountdownReceivedTicks;

        public double ServerCountdownMs
        {
            get
            {
                long receivedAt = Volatile.Read(ref _serverCountdownReceivedTicks);
                if (receivedAt == 0) return 0.0;

                long elapsedTicks = Stopwatch.GetTimestamp() - receivedAt;
                double elapsedMs = (elapsedTicks * 1000.0) / Stopwatch.Frequency;

                double tickIntervalMs = 1000.0 / (_config?.TickRate ?? 20);
                double liveCountdown = (Volatile.Read(ref _serverCountdownSnapshotMs) - elapsedMs) % tickIntervalMs;
                if (liveCountdown < 0) liveCountdown += tickIntervalMs;

                return liveCountdown;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void HandleWirePong(ushort peerId, ReadOnlySpan<byte> payload)
        {
            if (payload.Length < 16) return;

            uint seq = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4));
            long sentTicks = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(4, 8));
            float serverRemainingMs = BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(12, 4)));

            if (_wireInFlight.TryGetValue(peerId, out var state) && state.Seq == seq)
            {
                long now = Stopwatch.GetTimestamp();
                double rttMs = Math.Max(0, (now - sentTicks) * 1000.0 / Stopwatch.Frequency);

                _wireRttMap[peerId] = rttMs;
                _wireInFlight.TryRemove(peerId, out _);

                double tickIntervalMs = 1000.0 / (_config?.TickRate ?? 20);
                double owtMs = rttMs / 2.0;

                double adjustedCountdown = (serverRemainingMs - owtMs) % tickIntervalMs;
                if (adjustedCountdown < 0) adjustedCountdown += tickIntervalMs;

                Volatile.Write(ref _serverCountdownSnapshotMs, adjustedCountdown);
                Volatile.Write(ref _serverCountdownReceivedTicks, now);
            }
        }
        #endregion

        internal readonly struct OutboundPacket
        {
            public readonly byte[] Buffer;
            public readonly int Length;

            public OutboundPacket(byte[] buffer, int length)
            {
                Buffer = buffer;
                Length = length;
            }
        }

        internal sealed class ClientSendState
        {
            public readonly Channel<OutboundPacket> Channel;
            public readonly CancellationTokenSource LifetimeCts;
            public Task WriterTask;

            public ClientSendState(int capacity = 1024)
            {
                Channel = System.Threading.Channels.Channel.CreateBounded<OutboundPacket>(
                    new BoundedChannelOptions(capacity)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.Wait
                    });

                LifetimeCts = new CancellationTokenSource();
            }

            public void Dispose()
            {
                LifetimeCts.Dispose();
            }
        }

        private sealed class IoDeadlineWatchdog : IDisposable
        {
            private readonly object _gate = new object();
            private readonly TcpClient _client;
            private readonly Timer _timer;

            private bool _armed;
            private bool _disposed;
            private long _deadlineTicks;
            private long _generation;
            private int _timedOut;

            public bool TimedOut => Volatile.Read(ref _timedOut) != 0;

            public IoDeadlineWatchdog(TcpClient client)
            {
                _client = client;
                _timer = new Timer(OnTimer, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }

            public void Arm(TimeSpan timeout)
            {
                if (timeout <= TimeSpan.Zero)
                    return;

                lock (_gate)
                {
                    if (_disposed)
                        return;

                    unchecked { _generation++; }

                    _armed = true;
                    Volatile.Write(ref _timedOut, 0);

                    _deadlineTicks = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
                    _timer.Change(timeout, Timeout.InfiniteTimeSpan);
                }
            }

            public void Disarm()
            {
                lock (_gate)
                {
                    if (_disposed)
                        return;

                    unchecked { _generation++; }

                    _armed = false;
                    _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                }
            }

            private void OnTimer(object state)
            {
                long generation;

                lock (_gate)
                {
                    if (_disposed || !_armed)
                        return;

                    generation = _generation;

                    long now = Stopwatch.GetTimestamp();

                    if (now < _deadlineTicks)
                    {
                        double remainingMs = (_deadlineTicks - now) * 1000.0 / Stopwatch.Frequency;
                        if (remainingMs < 1.0)
                            remainingMs = 1.0;

                        _timer.Change(TimeSpan.FromMilliseconds(remainingMs), Timeout.InfiniteTimeSpan);
                        return;
                    }

                    if (generation != _generation)
                        return;

                    _armed = false;
                    Volatile.Write(ref _timedOut, 1);
                }

                try
                {
                    _client.Close();
                }
                catch { }
            }

            public void Dispose()
            {
                lock (_gate)
                {
                    if (_disposed)
                        return;

                    _disposed = true;
                    _armed = false;

                    unchecked { _generation++; }
                }

                _timer.Dispose();
            }
        }
    }
}
