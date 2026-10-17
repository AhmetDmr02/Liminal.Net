using Liminal.Net.ClientIdResolvers;
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

    public class TcpTransport<TContext> : ILiminalTransport, ITransportTelemetryProvider where TContext : struct
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
        public EndPoint? LocalEndpoint => _listener?.LocalEndpoint;
        private CancellationTokenSource _clientConnectCts;

        //Local end of our own client connection, in host mode the accepted socket with this remote end is our own client
        private EndPoint _pendingLocalEndpoint;
        private LocalAnnouncement _localAnnouncement;

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

        //Only bumped under _connectionLifecycleLock
        private uint _lastConnectionId;

        public bool IsClientConnected(ushort clientId) => _sockets.ContainsKey(clientId);

        public uint GetConnectionId(ushort clientId) =>
            _sendQueues.TryGetValue(clientId, out var state) ? state.ConnectionId : 0;
        public int ConnectedClientCount => _sockets.Count;
        public int TotalConnections => Volatile.Read(ref _totalConnections);

        public TContext OutboundContext { get; set; }

        protected ILiminalTransportFramingProvider<TContext> _framing;
        private int _totalHeaderSize;

        public event Action<ushort, DisconnectReason, string> OnTransportDisconnectReason;

        internal readonly ConcurrentDictionary<ushort, ClientSendState> _sendQueues = new();
        private readonly object _connectionLifecycleLock = new();

        internal void SetConnectedForTesting(bool isConnected = true, bool isServer = true, bool isClient = false)
        {
            _isConnected = isConnected;
            _isServer = isServer;
            _isClient = isClient;
        }

        #region Initialization
        public virtual void InitializeTransport(LiminalNetworkConfig config)
        {
            _config = config;
            _config.Validate();

            _framing = config.TransportFramingProvider as ILiminalTransportFramingProvider<TContext>;
            _totalHeaderSize = LiminalTransportHeader.GetHeaderSize(_framing);

            _clientIdResolver = _config.ClientIdResolver ?? new BaseResolver();
            _clientIdResolver.Initialize(this);
        }

        public virtual void StartServer(string ip, int port)
        {
            bool isWildcard = string.IsNullOrWhiteSpace(ip) || ip == "0.0.0.0" || ip == "::" || ip == "::0";
            if (!isWildcard && IPAddress.TryParse(ip, out var parsedBind))
            {
                isWildcard = parsedBind.Equals(IPAddress.Any) || parsedBind.Equals(IPAddress.IPv6Any);
            }

            var listener = isWildcard
                ? (Socket.OSSupportsIPv6 ? TcpListener.Create(port) : new TcpListener(IPAddress.Any, port))
                : new TcpListener(IPAddress.Parse(ip), port);

            try
            {
                listener.ExclusiveAddressUse = true;
            }
            catch { }

            TcpListener previous;
            lock (_connectionLifecycleLock)
            {
                previous = _listener;
                _listener = null;
            }

            if (previous != null)
            {
                try { previous.Stop(); } catch { }
            }

            try
            {
                listener.Start(100);
            }
            catch
            {
                try { listener.Stop(); } catch { }
                throw;
            }

            lock (_connectionLifecycleLock)
            {
                _listener = listener;
                _isServer = true;
                _isConnected = true;
            }

            _onServerStarted?.Invoke();

            _ = Task.Run(() => AcceptConnectionsAsync(listener));
            string dualModeInfo = (isWildcard && Socket.OSSupportsIPv6) ? " (Dual-Mode: IPv4 + IPv6)" : "";
            LiminalLogger.Log($"Server started on {listener.LocalEndpoint}{dualModeInfo}");
        }

        public virtual void StartClient(string ip, int port)
        {
            try
            {
                CancellationToken token;
                lock (_connectionLifecycleLock)
                {
                    _clientConnectCts?.Cancel();
                    _clientConnectCts?.Dispose();
                    _clientConnectCts = new CancellationTokenSource();
                    token = _clientConnectCts.Token;
                    _pendingLocalEndpoint = null;
                    _isClient = true;
                }

                string targetIp = ip;
                if (string.IsNullOrWhiteSpace(targetIp) || targetIp == "0.0.0.0")
                {
                    targetIp = "127.0.0.1";
                }
                else if (targetIp == "::" || targetIp == "::0")
                {
                    targetIp = "::1";
                }
                else if (IPAddress.TryParse(targetIp, out var parsedTarget))
                {
                    if (parsedTarget.Equals(IPAddress.Any))
                        targetIp = "127.0.0.1";
                    else if (parsedTarget.Equals(IPAddress.IPv6Any))
                        targetIp = "::1";
                }

                TcpClient client;
                if (IPAddress.TryParse(targetIp, out var addr) && addr.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    client = new TcpClient(AddressFamily.InterNetworkV6);
                }
                else
                {
                    client = new TcpClient();
                }
                client.NoDelay = true;

                _ = Task.Run(() => TryToConnectAsync(client, (targetIp, port), token), token);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Client connection failed: {ex.Message}");
            }
        }
        #endregion

        #region Shutdown & Disconnect & Kick
        //Every way this transport ends goes through ResetLocked under _connectionLifecycleLock, whoever ends something checks under that
        //same lock that it is still current, a late failure of an old connection finds its socket gone and does nothing
        //so a shutdown never has to be waited for and cant reach into a run started after it
        public virtual void Disconnect()
        {
            bool clientSide;
            ushort disconnectedId = 0;
            bool wasConnected = false;
            LocalAnnouncement announcement = null;

            lock (_connectionLifecycleLock)
            {
                clientSide = _isClient && !_isServer;
                if (clientSide)
                {
                    disconnectedId = _localClientId;
                    wasConnected = _isConnected;
                    announcement = _localAnnouncement;
                    _localAnnouncement = null;
                }

                ResetLocked();
            }

            try
            {
                bool raiseDisconnected = clientSide && (announcement != null ? announcement.TryHandOffDisconnect() : wasConnected);
                if (raiseDisconnected)
                {
                    _onLocalClientDisconnected?.Invoke(disconnectedId);
                }
            }
            finally
            {
                FinishShutdown();
            }

            LiminalLogger.Log(clientSide ? "[Transport] Disconnected from server." : "[Transport] Shut down.");
        }

        public virtual void Kick(ushort clientId) => KickConnection(clientId, null);

        //The send state is read once and its own socket kicked, a replacement after the read turns KickConnection into a no op
        public virtual void Kick(ushort clientId, uint connectionId)
        {
            if (connectionId != 0 && _sendQueues.TryGetValue(clientId, out var state) && state.ConnectionId == connectionId)
                KickConnection(clientId, state.Client);
        }

        //expected set means only that socket may be kicked, a late failure of a replaced connection must not kick the new one
        internal void KickConnection(ushort clientId, TcpClient expected)
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
                if (expected != null)
                {
                    //Connections are only added under this lock so a match here cant be a newer host session that reused the id
                    lock (_connectionLifecycleLock)
                    {
                        if (!_sockets.TryGetValue(clientId, out var current) || !ReferenceEquals(current, expected))
                            return;

                        ResetLocked();
                    }

                    LiminalLogger.LogWarning($"[Transport] Host local client {clientId} was kicked. Shutting down host session.");
                    FinishShutdown();
                    return;
                }

                LiminalLogger.LogWarning($"[Transport] Host local client {clientId} was kicked. Shutting down host session.");
                Shutdown();
                return;
            }

            if (!_sockets.TryGetValue(clientId, out var clientSocket) || (expected != null && !ReferenceEquals(clientSocket, expected)))
            {
                if (expected == null)
                    LiminalLogger.LogError($"[Transport] Couldn't find socket for client {clientId}");
                return;
            }

            //Removing it is the claim, a racing kick or the reader finds it gone and leaves the events to us
            if (!((ICollection<KeyValuePair<ushort, TcpClient>>)_sockets).Remove(new KeyValuePair<ushort, TcpClient>(clientId, clientSocket)))
                return;

            try
            {
                _onClientKicked?.Invoke(clientId);
            }
            finally
            {
                try { clientSocket.Close(); } catch { }

                //Only the kicked socket's own queue, a reconnect under the same id may already have put its own in
                if (_sendQueues.TryGetValue(clientId, out var sendState) && ReferenceEquals(sendState.Client, clientSocket))
                {
                    TeardownSendQueue(clientId, sendState);
                }

                if (TryClaimDisconnect(clientSocket))
                {
                    Interlocked.Decrement(ref _totalConnections);
                    _onClientDisconnected?.Invoke(clientId);
                    LiminalLogger.Log($"[Transport] Client {clientId} disconnected.");
                }

                LiminalLogger.Log($"[Transport] Kicked client {clientId}.");
            }
        }

        public virtual void Shutdown()
        {
            lock (_connectionLifecycleLock)
            {
                ResetLocked();
            }

            FinishShutdown();
        }

        //For late failures from a connection's own loops, an old socket failing late finds a newer connection in its place and leaves it alone
        private void ShutdownIfCurrent(TcpClient connection)
        {
            lock (_connectionLifecycleLock)
            {
                if (!_isClient || !_sockets.TryGetValue(ILiminalTransport.SERVER_ID, out var active) || !ReferenceEquals(active, connection))
                {
                    return;
                }

                ResetLocked();
            }

            FinishShutdown();
        }

        //Subclasses release their own resources here, runs after the reset and before OnShutdown
        protected virtual void OnShutdownCompleted()
        {
        }

        //Caller holds _connectionLifecycleLock
        private void ResetLocked()
        {
            _clientConnectCts?.Cancel();
            _clientConnectCts?.Dispose();
            _clientConnectCts = null;

            _isConnected = false;
            _isServer = false;
            _isClient = false;
            _localClientId = 0;
            _pendingLocalEndpoint = null;
            _localAnnouncement?.TryCancelPending();
            _localAnnouncement = null;

            foreach (var id in _sockets.Keys)
            {
                TeardownSendQueue(id);

                if (_sockets.TryRemove(id, out var clientSocket))
                {
                    //Removing it from _sockets makes this its disconnect, its late reader finds it gone and stays quiet
                    //a client's own connection to the server was never counted
                    if (id != ILiminalTransport.SERVER_ID && TryClaimDisconnect(clientSocket))
                    {
                        Interlocked.Decrement(ref _totalConnections);
                    }
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

            _finalizedConnections.Clear();
            lock (_wireLock)
            {
                _wireInFlight.Clear();
                _wireRttMap.Clear();
            }
        }

        //After the lock is let go so neither the subclass cleanup nor OnShutdown handlers run under it
        private void FinishShutdown()
        {
            LiminalLogger.Log($"[Transport-Debug] Shutdown by thread '{Thread.CurrentThread.Name ?? Thread.CurrentThread.ManagedThreadId.ToString()}'. Stack:\n{Environment.StackTrace}", LiminalLogger.LogLevel.Detailed);

            try
            {
                OnShutdownCompleted();
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Shutdown cleanup failed: {ex.Message}");
            }

            _onShutdown?.Invoke();
        }

        //Caller holds _connectionLifecycleLock
        private uint NextConnectionIdLocked()
        {
            if (++_lastConnectionId == 0)
                _lastConnectionId = 1;
            return _lastConnectionId;
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

            //After the claim, a teardown that lost to a newer connection must not wipe that connection's ping state
            lock (_wireLock)
            {
                _wireInFlight.Remove(clientId);
                _wireRttMap.Remove(clientId);
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
            //No queue means that connection already ended and whoever ended it raised its events, kicking by id here could hit a newer one
            if (!_sendQueues.TryGetValue(targetId, out var sendState))
            {
                if ((flags & TransportFlags.WirePing) == 0)
                    LiminalLogger.LogWarning($"[Transport] Dropped send to client {targetId}, it has no open connection.", LiminalLogger.LogLevel.Detailed);
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
                    FailConnection(targetId, sendState);
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

                                    if (IsServer) KickConnection(clientId, client);
                                    else ShutdownIfCurrent(client);
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

                            if (IsServer) KickConnection(clientId, client);
                            else ShutdownIfCurrent(client);
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

                if (IsServer)
                {
                    if (IsCurrentConnection(clientId, client, state))
                        KickConnection(clientId, client);
                }
                else ShutdownIfCurrent(client);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Unexpected outbound writer failure on {clientId}: {ex.Message}");

                if (IsServer)
                {
                    if (IsCurrentConnection(clientId, client, state))
                        KickConnection(clientId, client);
                }
                else ShutdownIfCurrent(client);
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
                using var cancelReg = cancelToken.Register(() => { try { client.Close(); } catch { } });
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_config.ConnectionTimeout));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancelToken);

#if NET5_0_OR_GREATER
                await client.ConnectAsync(connectionInfo.ip, connectionInfo.port, linkedCts.Token).ConfigureAwait(false);
#else
                await client.ConnectAsync(connectionInfo.ip, connectionInfo.port).ConfigureAwait(false);
#endif

                if (cancelToken.IsCancellationRequested)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                lock (_connectionLifecycleLock)
                {
                    if (!cancelToken.IsCancellationRequested)
                    {
                        _pendingLocalEndpoint = client.Client.LocalEndPoint;
                    }
                }

                _onHandshakeInitialized?.Invoke();

                HandshakeResult result = await ClientHandshaker(client, _config).ConfigureAwait(false);

                if (cancelToken.IsCancellationRequested)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                if (result.Success)
                {
                    PromoteLocalClient(result.ClientId, client, cancelToken);
                }
                else
                {
                    try { client.Close(); } catch { }

                    if (!TryResetForAttempt(cancelToken))
                        return;

                    try
                    {
                        OnTransportDisconnectReason?.Invoke(ILiminalTransport.SERVER_ID, result.FailureReason, result.FailureMessage);
                        _onLocalClientDisconnected?.Invoke(0);
                    }
                    finally
                    {
                        FinishShutdown();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                try { client.Close(); } catch { }
                if (TryResetForAttempt(cancelToken))
                {
                    LiminalLogger.LogError("[Transport] Connection timed out.");
                    FinishShutdown();
                }
            }
            catch (Exception ex)
            {
                try { client.Close(); } catch { }
                if (!TryResetForAttempt(cancelToken))
                    return;

                LiminalLogger.LogError($"[Transport] Connection failed: {ex.Message}");
                FinishShutdown();
            }
        }
        #endregion

        //A failed connect attempt may only shut down the run it belongs to, StartClient and every reset cancel the attempt under this lock
        //so checking the token and resetting together here cant hit a run that started after the game restarted
        private bool TryResetForAttempt(CancellationToken attempt)
        {
            lock (_connectionLifecycleLock)
            {
                if (attempt.IsCancellationRequested)
                    return false;

                ResetLocked();
                return true;
            }
        }

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
                        ushort assignedClientId = 0;
                        try
                        {
                            _onHandshakeInitialized?.Invoke();

                            var pipeline = new TcpHandshakePipeline(_clientIdResolver, _config);

                            ClientSendState clientSendState = null;

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
                                onClientValidated: null,
                                onClientPreValidated: assignedId =>
                                {
                                    assignedClientId = assignedId;
                                    PrePromoteClient(assignedId, acceptedClient, out clientSendState);
                                    promoted = clientSendState != null;
                                    return clientSendState;
                                },
                                onClientPostValidated: (assignedId, state) =>
                                {
                                    PostPromoteClient(assignedId, acceptedClient, (ClientSendState)state);
                                }
                            ).ConfigureAwait(false);

                            if (!result.Success)
                            {
                                if (!result.Silent)
                                {
                                    LiminalLogger.LogWarning($"[Transport] Handshake rejected client: {result.FailureReason} - {result.FailureMessage}");
                                }
                                if (promoted && assignedClientId != 0)
                                {
                                    KickConnection(assignedClientId, acceptedClient);
                                    try { acceptedClient.Close(); } catch { }
                                }
                                else
                                {
                                    try { acceptedClient.Close(); } catch { }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            LiminalLogger.LogError($"[Transport] Handshake error: {ex.Message}");
                            if (promoted && assignedClientId != 0)
                            {
                                KickConnection(assignedClientId, acceptedClient);
                                try { acceptedClient.Close(); } catch { }
                            }
                            else
                            {
                                try { acceptedClient.Close(); } catch { }
                            }
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

        internal void PrePromoteClient(ushort clientId, TcpClient client, out ClientSendState sendState)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            int maxPacketCount = _config.Hiccup.Enabled ? _config.MaxPacketCount * _config.Hiccup.MaxRecoveryScale : _config.MaxPacketCount;
            maxPacketCount = Math.Max(_config.MaxPacketCount, maxPacketCount);

            ClientSendState oldSendState = null;
            TcpClient oldClient = null;

            lock (_connectionLifecycleLock)
            {
                if (!_isConnected)
                {
                    try { client.Close(); } catch { }
                    sendState = null;
                    return;
                }

                var newSendState = new ClientSendState(NextConnectionIdLocked(), client, maxPacketCount);

                //A kick or reader can remove the old entry without this lock, so the old value only counts when our swap actually replaced it
                //AddOrUpdate's factory would keep an old value from a try that lost and the add that won would then tear down a state it never owned
                oldSendState = SwapIn(_sendQueues, clientId, newSendState);
                sendState = newSendState;

                oldClient = SwapIn(_sockets, clientId, client);
                if (oldClient != null && TryClaimDisconnect(oldClient))
                {
                    Interlocked.Decrement(ref _totalConnections);
                }

                _clientIdResolver.ConfirmRegistration(clientId);

                //Our own client only learns its id after ReadyConfirmed, server side handlers run before that and must already see it as local
                if (_isClient && _localClientId == 0 && IsOwnClientConnection(client))
                {
                    _localClientId = clientId;
                }
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

            try
            {
                _onClientConnected?.Invoke(clientId);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Error during OnClientConnected for {clientId}: {ex.Message}");
                KickConnection(clientId, client);
                try { client.Close(); } catch { }
                sendState = null;
                return;
            }
        }

        //Returns the value it replaced, null when it added
        private static TValue SwapIn<TValue>(ConcurrentDictionary<ushort, TValue> map, ushort key, TValue value) where TValue : class
        {
            while (true)
            {
                if (map.TryGetValue(key, out var current))
                {
                    if (map.TryUpdate(key, value, current))
                        return current;
                }
                else if (map.TryAdd(key, value))
                {
                    return null;
                }
            }
        }

        internal void PostPromoteClient(ushort clientId, TcpClient client, ClientSendState sendState)
        {
            if (sendState == null)
                return;

            if (!IsCurrentConnection(clientId, client, sendState))
                return;

            sendState.WriterTask = Task.Run(() => ProcessSendQueueAsync(clientId, client, sendState));
            _ = Task.Run(async () => ReceiveLoop(clientId, client, sendState));

            LiminalLogger.Log($"[Transport] Client {clientId} successfully promoted to Game Loop.");
        }

        internal void PromoteClient(ushort clientId, TcpClient client)
        {
            PrePromoteClient(clientId, client, out var sendState);
            PostPromoteClient(clientId, client, sendState);
        }

        internal void PromoteLocalClient(ushort assignedId, TcpClient client, CancellationToken attempt = default)
        {
            client.SendTimeout = (int)_config.SendResponseTimeout * 1000;

            ClientSendState sendState;
            LocalAnnouncement announcement;
            lock (_connectionLifecycleLock)
            {
                //A cancelled attempt finished its handshake after a restart, its socket is not this run's connection
                if (!_isClient || attempt.IsCancellationRequested)
                {
                    try { client.Close(); } catch { }
                    return;
                }

                _localClientId = assignedId;
                sendState = new ClientSendState(NextConnectionIdLocked(), client);
                _sendQueues[ILiminalTransport.SERVER_ID] = sendState;
                _sockets[ILiminalTransport.SERVER_ID] = client;
                _isConnected = true;
                announcement = new LocalAnnouncement();
                _localAnnouncement = announcement;
            }

            //A Disconnect or Shutdown that got in after the lock cancelled it, Connected must not follow their Disconnected
            if (!announcement.TryBegin())
                return;

            try
            {
                _onLocalClientConnected?.Invoke(assignedId);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Transport] Error during OnLocalClientConnected: {ex.Message}");
                if (announcement.TryComplete())
                    Disconnect();
                else
                    _onLocalClientDisconnected?.Invoke(assignedId);
                return;
            }

            //Disconnect came in while Connected handlers ran, it left Disconnected to us so it comes after them
            if (!announcement.TryComplete())
            {
                _onLocalClientDisconnected?.Invoke(assignedId);
                return;
            }

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

        //Ends the connection that owns sendState, a replaced one is left alone
        private void FailConnection(ushort clientId, ClientSendState sendState)
        {
            if (!_sockets.TryGetValue(clientId, out var client) || !IsCurrentConnection(clientId, client, sendState))
                return;

            if (IsServer) KickConnection(clientId, client);
            else ShutdownIfCurrent(client);
        }

        private static bool IsSameEndpoint(EndPoint a, EndPoint b)
        {
            if (!(a is IPEndPoint x) || !(b is IPEndPoint y) || x.Port != y.Port)
                return false;

            var ax = x.Address.IsIPv4MappedToIPv6 ? x.Address.MapToIPv4() : x.Address;
            var ay = y.Address.IsIPv4MappedToIPv6 ? y.Address.MapToIPv4() : y.Address;
            return ax.Equals(ay);
        }

        private bool IsOwnClientConnection(TcpClient accepted)
        {
            var local = _pendingLocalEndpoint;
            if (local == null) return false;

            try
            {
                return IsSameEndpoint(accepted.Client.RemoteEndPoint, local);
            }
            catch (Exception ex) when (ex is ObjectDisposedException or SocketException)
            {
                return false;
            }
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
                        KickConnection(incomingId, client);
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
                                KickConnection(incomingId, client);
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
                    ProcessIngestBufferSynchronous(incomingId, client, ingestBuffer, ref bytesInBuffer);

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

                bool isServerConn = incomingId == ILiminalTransport.SERVER_ID;

                try { client.Close(); } catch { }

                TeardownSendQueue(incomingId, ownedSendState);

                if (!isServerConn)
                {
                    //A reset, a kick or a replacement may have removed this socket already, then its disconnect is theirs
                    bool removed = ((ICollection<KeyValuePair<ushort, TcpClient>>)_sockets).Remove(new KeyValuePair<ushort, TcpClient>(incomingId, client));

                    if (removed && TryClaimDisconnect(client))
                    {
                        Interlocked.Decrement(ref _totalConnections);
                        if (_sockets.TryGetValue(incomingId, out var activeClient) && !ReferenceEquals(activeClient, client))
                        {
                            LiminalLogger.Log($"[Transport] Old socket for client {incomingId} closed after replacement.");
                        }
                        else
                        {
                            _onClientDisconnected?.Invoke(incomingId);
                            LiminalLogger.Log($"[Transport] Client {incomingId} disconnected.");
                        }
                    }
                }
                else
                {
                    bool isCurrent = false;
                    ushort disconnectedId = 0;
                    LocalAnnouncement announcement = null;

                    //Decided and reset in the same lock, a Disconnect and restart in between would otherwise let this old socket end the new run
                    lock (_connectionLifecycleLock)
                    {
                        if (_isClient &&
                            _sockets.TryGetValue(ILiminalTransport.SERVER_ID, out var activeClient) &&
                            ReferenceEquals(activeClient, client))
                        {
                            disconnectedId = _localClientId;
                            isCurrent = true;
                            announcement = _localAnnouncement;
                            _localAnnouncement = null;
                            ResetLocked();
                        }
                    }

                    if (isCurrent)
                    {
                        LiminalLogger.Log("[Transport] Local client connection to server was lost. Shutting down.");
                        try
                        {
                            if (announcement == null || announcement.TryHandOffDisconnect())
                            {
                                _onLocalClientDisconnected?.Invoke(disconnectedId);
                            }
                        }
                        finally
                        {
                            FinishShutdown();
                        }
                    }
                }

                _finalizedConnections.TryRemove(client, out _);
            }
        }

        private void ProcessIngestBufferSynchronous(ushort incomingId, TcpClient client, LiminalNativeBuffer ingestBuffer, ref int bytesInBuffer)
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
                    KickConnection(incomingId, client);
                    return;
                }

                if (payloadLength < 0 || payloadLength > _config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch))
                {
                    LiminalLogger.LogError($"[Transport] Invalid payload size {payloadLength}b on client {incomingId}");
                    OnTransportDisconnectReason?.Invoke(incomingId, DisconnectReason.InvalidPacketSize, $"Payload length {payloadLength}b outside allowed bounds.");
                    KickConnection(incomingId, client);
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
                            var nextTickProvider = NextTickProvider;
                            if (nextTickProvider != null)
                            {
                                long nextTick = nextTickProvider();
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

        private readonly object _wireLock = new();
        private readonly Dictionary<ushort, double> _wireRttMap = new();
        private readonly Dictionary<ushort, (uint Seq, long SentTicks)> _wireInFlight = new();
        private uint _wireSeqCounter;

        public bool TryGetWireRTT(ushort clientId, out double rttMs)
        {
            lock (_wireLock)
            {
                return _wireRttMap.TryGetValue(clientId, out rttMs);
            }
        }

        private static readonly long WirePingTimeoutTicks = Stopwatch.Frequency * 3;

        public void SendWirePing(ushort targetId)
        {
            long now = Stopwatch.GetTimestamp();
            uint seq;

            lock (_wireLock)
            {
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

                seq = unchecked(++_wireSeqCounter);
                _wireInFlight[targetId] = (seq, now);
            }

            Span<byte> pingPayload = stackalloc byte[16];
            BinaryPrimitives.WriteUInt32LittleEndian(pingPayload.Slice(0, 4), seq);
            BinaryPrimitives.WriteInt64LittleEndian(pingPayload.Slice(4, 8), now);
            BinaryPrimitives.WriteInt32LittleEndian(pingPayload.Slice(12, 4), BitConverter.SingleToInt32Bits(0f));

            SendInternal(pingPayload, targetId, TransportFlags.WirePing);
        }

        private sealed class ServerCountdownSnapshot
        {
            public readonly double SnapshotMs;
            public readonly long ReceivedTicks;

            public ServerCountdownSnapshot(double snapshotMs, long receivedTicks)
            {
                SnapshotMs = snapshotMs;
                ReceivedTicks = receivedTicks;
            }
        }

        private ServerCountdownSnapshot _serverCountdownSnapshot;

        public double ServerCountdownMs
        {
            get
            {
                var snapshot = Volatile.Read(ref _serverCountdownSnapshot);
                if (snapshot == null || snapshot.ReceivedTicks == 0) return 0.0;

                long elapsedTicks = Stopwatch.GetTimestamp() - snapshot.ReceivedTicks;
                double elapsedMs = (elapsedTicks * 1000.0) / Stopwatch.Frequency;

                double tickIntervalMs = 1000.0 / (_config?.TickRate ?? 20);
                double liveCountdown = (snapshot.SnapshotMs - elapsedMs) % tickIntervalMs;
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

            long now = Stopwatch.GetTimestamp();
            double rttMs;

            lock (_wireLock)
            {
                if (!_wireInFlight.TryGetValue(peerId, out var state) || state.Seq != seq || state.SentTicks == 0)
                {
                    return;
                }

                rttMs = Math.Max(0, (now - sentTicks) * 1000.0 / Stopwatch.Frequency);
                _wireRttMap[peerId] = rttMs;
                _wireInFlight[peerId] = (seq, 0);
            }

            double tickIntervalMs = 1000.0 / (_config?.TickRate ?? 20);
            double owtMs = rttMs / 2.0;

            double adjustedCountdown = (serverRemainingMs - owtMs) % tickIntervalMs;
            if (adjustedCountdown < 0) adjustedCountdown += tickIntervalMs;

            Volatile.Write(ref _serverCountdownSnapshot, new ServerCountdownSnapshot(adjustedCountdown, now));
        }
        #endregion

        //Orders the local Connected and Disconnected events without anyone waiting on the other's handlers
        private sealed class LocalAnnouncement
        {
            private const int Pending = 0;
            private const int Announcing = 1;
            private const int Announced = 2;
            private const int Cancelled = 3;
            private const int DisconnectedWhileAnnouncing = 4;
            private const int Closed = 5;

            private int _state;

            public bool TryBegin() => Interlocked.CompareExchange(ref _state, Announcing, Pending) == Pending;

            //False when a disconnect came in during the Connected handlers, the announcer raises Disconnected then
            public bool TryComplete() => Interlocked.CompareExchange(ref _state, Announced, Announcing) == Announcing;

            public void TryCancelPending() => Interlocked.CompareExchange(ref _state, Cancelled, Pending);

            //True when the caller raises Disconnected itself, true at most once
            public bool TryHandOffDisconnect()
            {
                while (true)
                {
                    int current = Volatile.Read(ref _state);
                    int next;
                    switch (current)
                    {
                        case Pending: next = Cancelled; break;
                        case Announcing: next = DisconnectedWhileAnnouncing; break;
                        case Announced: next = Closed; break;
                        default: return false;
                    }

                    if (Interlocked.CompareExchange(ref _state, next, current) == current)
                        return current == Announced;
                }
            }
        }

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
            public readonly uint ConnectionId;
            public readonly TcpClient Client;
            public readonly Channel<OutboundPacket> Channel;
            public readonly CancellationTokenSource LifetimeCts;
            public Task WriterTask;

            public ClientSendState(uint connectionId, TcpClient client, int capacity = 1024)
            {
                ConnectionId = connectionId;
                Client = client;
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
