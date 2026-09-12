using Liminal.Net.Core;
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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Liminal.Net.Transports
{
    //Example empty framing context
    public readonly struct EmptyFramingContext { }
    public class TcpTransport : TcpTransport<EmptyFramingContext> { }

    /// <summary>
    /// It uses tcp by default
    /// </summary>
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

        protected LiminalTransportConfig _config;
        public LiminalTransportConfig Config => _config;

        protected ILiminalClientIdResolver _clientIdResolver;
        public ILiminalClientIdResolver ClientIdResolver => _clientIdResolver;

        internal readonly ConcurrentDictionary<ushort, TcpClient> _sockets = new();


        private readonly ConcurrentDictionary<TcpClient, byte> _finalizedConnections = new();
        private bool TryClaimDisconnect(TcpClient client) => _finalizedConnections.TryAdd(client, 0);

        private int _isShuttingDown = 0;

        public bool IsClientConnected(ushort clientId) => _sockets.ContainsKey(clientId);
        public int ConnectedClientCount => _sockets.Count;

        /// <summary>
        /// Lifecycle state for outbound frames.
        /// </summary>
        public TContext OutboundContext { get; set; }

        protected ILiminalTransportFramingProvider<TContext> _framing;
        private int _totalHeaderSize;

        public event Action<ushort, DisconnectReason, string> OnTransportDisconnectReason;

        private readonly ConcurrentDictionary<ushort, ClientSendState> _sendQueues = new();

        #region Initialization
        public virtual void InitializeTransport(LiminalTransportConfig config)
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

            if (!_isServer && _isConnected)
            {
                _onLocalClientDisconnected?.Invoke(_localClientId);

                Shutdown();
                LiminalLogger.Log($"[Transport] Disconnected from server.");
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
                LiminalLogger.LogWarning(
                    $"[Transport] Host local client {clientId} was kicked. Shutting down host session.");

                Shutdown();
                return;
            }
            if (_sockets.TryGetValue(clientId, out var clientSocket))
            {
                _onClientKicked?.Invoke(clientId);
                try { clientSocket.Close(); } catch { }
                _sockets.TryRemove(clientId, out _);

                TeardownSendQueue(clientId);

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
                LiminalLogger.LogError(
                    $"[Transport] Couldn't find socket for client {clientId}");
            }
        }

        public virtual void Shutdown()
        {
            if (Interlocked.Exchange(ref _isShuttingDown, 1) == 1)
            {
                return; // Already shutting down
            }

            LiminalLogger.Log($"[Transport-Debug] Shutdown initiated by thread '{Thread.CurrentThread.Name ?? Thread.CurrentThread.ManagedThreadId.ToString()}'. Stack:\n{Environment.StackTrace}", LiminalLogger.LogLevel.Detailed);

            try
            {
                _isConnected = false;
                _isServer = false;
                _isClient = false;
                _localClientId = 0;

                foreach (var id in _sockets.Keys)
                {
                    TeardownSendQueue(id);

                    if (_sockets.TryRemove(id, out var clientSocket))
                    {
                        try
                        {
                            clientSocket.Close();
                        }
                        catch { }

                        LiminalLogger.Log($"[Transport] Client {id} cleared.");
                    }
                }

                foreach (var id in _sendQueues.Keys)
                {
                    TeardownSendQueue(id);
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
                Volatile.Write(ref _totalConnections, 0);
                _finalizedConnections.Clear();
            }
        }

        private void TeardownSendQueue(ushort clientId)
        {
            if (_sendQueues.TryRemove(clientId, out var state))
            {
                try
                {
                    state.LifetimeCts.Cancel();
                }
                catch (ObjectDisposedException) { }

                state.Channel.Writer.TryComplete();

                if (state.WriterTask != null)
                {
                    _ = state.WriterTask.ContinueWith(
                        _ => state.Dispose(),
                        TaskContinuationOptions.ExecuteSynchronously);
                }
                else
                {
                    state.Dispose();
                }
            }
        }
        #endregion

        #region Sending
        private readonly ArrayPool<byte> _sendBytePool = ArrayPool<byte>.Create(1024 * 128, 50);
        public virtual void Send(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            if (flags == TransportFlags.Unreliable)
            {
                //TcpTransport is inherently reliable
                flags = TransportFlags.Reliable;
            }

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

            void ArmTimeout()
            {
                if (timeoutEnabled) state.SendTimeoutCts.CancelAfter(timeoutSpan);
            }

            void DisarmTimeout()
            {
                if (!timeoutEnabled) return;

                state.SendTimeoutCts.CancelAfter(Timeout.InfiniteTimeSpan);
                if (!state.SendTimeoutCts.TryReset())
                {
                    state.SendTimeoutCts.Dispose();
                    state.SendTimeoutCts = new CancellationTokenSource();
                }
            }

            try
            {
                while (await reader.WaitToReadAsync(lifetimeToken).ConfigureAwait(false))
                {
                    try
                    {
                        while (reader.TryRead(out OutboundPacket packet))
                        {
                            try
                            {
                                ArmTimeout();
                                await stream.WriteAsync(packet.Buffer.AsMemory(0, packet.Length), state.SendTimeoutCts.Token).ConfigureAwait(false);

                                if ((_telemetryConfig?.Flags & TelemetryFlags.ByteCounting) != 0)
                                    Interlocked.Add(ref _totalBytesOutbound, packet.Length);
                            }
                            finally
                            {
                                _sendBytePool.Return(packet.Buffer);
                                DisarmTimeout();
                            }
                        }

                        ArmTimeout();
                        await stream.FlushAsync(state.SendTimeoutCts.Token).ConfigureAwait(false);
                    }
                    finally
                    {
                        DisarmTimeout();
                    }
                }
            }
            catch (OperationCanceledException) when (!lifetimeToken.IsCancellationRequested)
            {
                LiminalLogger.LogWarning($"[Transport] Send/Flush timed out on client {clientId} (exceeded {timeoutSeconds}s).");
                OnTransportDisconnectReason?.Invoke(clientId, DisconnectReason.Timeout, $"Send/Flush timed out ({timeoutSeconds}s).");
                if (IsServer) Kick(clientId); else Shutdown();
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
            {
                LiminalLogger.LogWarning($"[Transport] Outbound writer aborted on client {clientId}: {ex.Message}");
                if (IsServer) Kick(clientId); else Shutdown();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Unexpected outbound writer failure on {clientId}: {ex.Message}");
                if (IsServer) Kick(clientId); else Shutdown();
            }
            finally
            {
                while (reader.TryRead(out OutboundPacket discarded))
                    _sendBytePool.Return(discarded.Buffer);
            }
        }

        protected async Task TryToConnectAsync(TcpClient client, (string ip, int port) connectionInfo)
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_config.ConnectionTimeout));

                await client.ConnectAsync(connectionInfo.ip, connectionInfo.port, cts.Token);

                _onHandshakeInitialized?.Invoke();

                HandshakeResult result = await ClientHandshaker(client, _config);

                if (result.Success)
                {
                    _localClientId = result.ClientId;
                    _isConnected = true;
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
                    client = await listener.AcceptTcpClientAsync();
                    client.NoDelay = true;

                    var acceptedClient = client;

                    _ = Task.Run(async () =>
                    {
                        bool promoted = false;
                        try
                        {
                            _onHandshakeInitialized?.Invoke();

                            HandshakeResult result = await ServerHandshaker(
                                acceptedClient,
                                _config,
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
                                }
                            );

                            if (result.Success)
                            {
                                PromoteClient(result.ClientId, acceptedClient);
                                promoted = true;
                            }
                            else
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

        private void PromoteClient(ushort clientId, TcpClient client)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            int maxPacketCount = _config.Hiccup.Enabled ? _config.MaxPacketCount * _config.Hiccup.MaxRecoveryScale : _config.MaxPacketCount;
            maxPacketCount = Math.Max(_config.MaxPacketCount, maxPacketCount);

            var sendState = new ClientSendState(maxPacketCount);

            _sendQueues.AddOrUpdate(clientId, sendState, (k, old) =>
            {
                old.LifetimeCts.Cancel();
                old.Channel.Writer.TryComplete();

                if (old.WriterTask != null)
                    _ = old.WriterTask.ContinueWith(_ => old.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
                else
                    old.Dispose();

                return sendState;
            });

            _sockets.AddOrUpdate(clientId, client, (key, old) =>
            {
                LiminalLogger.LogWarning($"[Transport] Replacing existing socket for client {clientId}");

                TryClaimDisconnect(old);

                try { old.Close(); } catch { }
                Interlocked.Decrement(ref _totalConnections);
                return client;
            });

            sendState.WriterTask = Task.Run(() => ProcessSendQueueAsync(clientId, client, sendState));

            _clientIdResolver.ConfirmRegistration(clientId);

            _onClientConnected?.Invoke(clientId);

            _ = Task.Run(async () => ReceiveLoop(clientId, client));

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");
        }
        private void PromoteLocalClient(ushort assignedId, TcpClient client)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            var sendState = new ClientSendState();
            _sendQueues[ILiminalTransport.SERVER_ID] = sendState;

            _sockets[ILiminalTransport.SERVER_ID] = client;

            sendState.WriterTask = Task.Run(() => ProcessSendQueueAsync(ILiminalTransport.SERVER_ID, client, sendState));

            _ = Task.Run(() => ReceiveLoop(ILiminalTransport.SERVER_ID, client));

            _onLocalClientConnected?.Invoke(assignedId);

            LiminalLogger.Log($"[Transport] Successfully connected to server. Local ID: {assignedId}");
        }

        private enum LoopExitReason
        {
            GracefulClosure,
            BufferOverflow,
            MalformedHeader,
            InvalidPayloadSize,
            Timeout,
            SocketError
        }

        private async Task ReceiveLoop(ushort incomingId, TcpClient client)
        {
            // Fixed for the lifetime of this receive loop. Hiccup recovery never swaps
            // or resizes the transport buffer at runtime.
            using var ingestBuffer = new LiminalNativeBuffer(_config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch));
            var stream = client.GetStream();
            int bytesInBuffer = 0;

            bool isHostSelfLoop = _isServer && _isClient &&
                (incomingId == ILiminalTransport.SERVER_ID || incomingId == _localClientId);

            int timeoutSeconds = (int)_config.ReceiveResponseTimeout;
            bool timeoutEnabled = timeoutSeconds > 0 && !isHostSelfLoop;
            TimeSpan timeoutSpan = TimeSpan.FromSeconds(timeoutSeconds);

            CancellationTokenSource recvTimeoutCts = timeoutEnabled ? new CancellationTokenSource() : null;

            void ArmTimeout()
            {
                if (timeoutEnabled) recvTimeoutCts.CancelAfter(timeoutSpan);
            }

            void DisarmTimeout()
            {
                if (!timeoutEnabled) return;

                recvTimeoutCts.CancelAfter(Timeout.InfiniteTimeSpan);
                if (!recvTimeoutCts.TryReset())
                {
                    recvTimeoutCts.Dispose();
                    recvTimeoutCts = new CancellationTokenSource();
                }
            }

            try { client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true); }
            catch { }

            try
            {
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
                        try
                        {
                            ArmTimeout();
                            read = await stream.ReadAsync(receiveTarget, recvTimeoutCts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            LiminalLogger.LogWarning($"[Transport] Client {incomingId} timed out (exceeded {timeoutSeconds}s).");
                            OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.Timeout, $"Receive timeout exceeded ({timeoutSeconds}s).");
                            Kick(incomingId);
                            return;
                        }
                        finally
                        {
                            DisarmTimeout();
                        }
                    }
                    else
                    {
                        read = await stream.ReadAsync(receiveTarget).ConfigureAwait(false);
                    }

                    if (read <= 0) break;

                    if ((_telemetryConfig?.Flags & TelemetryFlags.ByteCounting) != 0)
                    {
                        Interlocked.Add(ref _totalBytesInbound, read);
                    }

                    bytesInBuffer += read;

                    ProcessIngestBufferSynchronous(incomingId, ingestBuffer, ref bytesInBuffer);
                    if (!IsClientConnected(incomingId) && incomingId != ILiminalTransport.SERVER_ID) break;
                }
            }
            catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
            {
                LiminalLogger.LogWarning($"[Transport-Debug] Socket closed/dropped on {incomingId}. Reason: {ex.GetType().Name} - {ex.Message}");
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport-Debug] Unexpected receive error on {incomingId}: {ex.GetType().Name} - {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                recvTimeoutCts?.Dispose();

                if (_isShuttingDown == 0)
                {
                    bool isServerConn = incomingId == ILiminalTransport.SERVER_ID;

                    try { client.Close(); } catch { }

                    TeardownSendQueue(incomingId);

                    if (!isServerConn)
                    {
                        ((ICollection<KeyValuePair<ushort, TcpClient>>)_sockets)
                            .Remove(new KeyValuePair<ushort, TcpClient>(incomingId, client));

                        if (TryClaimDisconnect(client))
                        {
                            Interlocked.Decrement(ref _totalConnections);
                            _onClientDisconnected?.Invoke(incomingId);
                            LiminalLogger.Log($"[Transport] Client {incomingId} disconnected.");
                        }
                    }
                    else if (!_isServer)
                    {
                        LiminalLogger.Log("[Transport] Lost connection to host. Shutting down...");
                        _onLocalClientDisconnected?.Invoke(_localClientId);
                        Shutdown();
                    }

                    _finalizedConnections.TryRemove(client, out _);
                }
            }
        }

        /// <summary>
        /// Helper class to help us migrate for previous versions of C#
        /// </summary>
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
                            BinaryPrimitives.WriteSingleLittleEndian(pongPayload.Slice(12, 4), countdownMs);

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
                    packetLossRate: 0.0f // We have no way to reach internal status of the OS stack so we can't calculate this in tcp
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

        private static readonly long WirePingTimeoutTicks = Stopwatch.Frequency * 3; // 3 sec timeout

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

            // [0.4] Seq | [4.12] SentTicks | [12.16] ServerCountdownMs
            Span<byte> pingPayload = stackalloc byte[16];
            BinaryPrimitives.WriteUInt32LittleEndian(pingPayload.Slice(0, 4), seq);
            BinaryPrimitives.WriteInt64LittleEndian(pingPayload.Slice(4, 8), now);
            BinaryPrimitives.WriteSingleLittleEndian(pingPayload.Slice(12, 4), 0f);

            SendInternal(pingPayload, targetId, TransportFlags.WirePing);
        }

        private double _serverCountdownSnapshotMs;
        private long _serverCountdownReceivedTicks;

        /// <summary>
        /// Real-time server countdown accounting for elapsed time since the last pong.
        /// </summary>
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
            float serverRemainingMs = BinaryPrimitives.ReadSingleLittleEndian(payload.Slice(12, 4));

            if (_wireInFlight.TryGetValue(peerId, out var state) && state.Seq == seq)
            {
                long now = Stopwatch.GetTimestamp();
                double rttMs = Math.Max(0, (now - sentTicks) * 1000.0 / Stopwatch.Frequency);

                _wireRttMap[peerId] = rttMs;
                _wireInFlight.TryRemove(peerId, out _);

                double tickIntervalMs = 1000.0 / (_config?.TickRate ?? 20);
                double owtMs = rttMs / 2.0;

                // Adjust for one-way wire transit
                double adjustedCountdown = (serverRemainingMs - owtMs) % tickIntervalMs;
                if (adjustedCountdown < 0) adjustedCountdown += tickIntervalMs;

                Volatile.Write(ref _serverCountdownSnapshotMs, adjustedCountdown);
                Volatile.Write(ref _serverCountdownReceivedTicks, now);
            }
        }
        #endregion


        private readonly struct OutboundPacket
        {
            public readonly byte[] Buffer;
            public readonly int Length;

            public OutboundPacket(byte[] buffer, int length)
            {
                Buffer = buffer;
                Length = length;
            }
        }

        private sealed class ClientSendState
        {
            public readonly Channel<OutboundPacket> Channel;
            public readonly CancellationTokenSource LifetimeCts;
            public CancellationTokenSource SendTimeoutCts; 
            public Task WriterTask;

            public ClientSendState(int capacity = 1024)
            {
                Channel = System.Threading.Channels.Channel.CreateBounded<OutboundPacket>(
                    new BoundedChannelOptions(capacity) { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });
                LifetimeCts = new CancellationTokenSource();
                SendTimeoutCts = new CancellationTokenSource();
            }

            public void Dispose()
            {
                LifetimeCts.Dispose();
                SendTimeoutCts.Dispose();
            }
        }
    }
}

