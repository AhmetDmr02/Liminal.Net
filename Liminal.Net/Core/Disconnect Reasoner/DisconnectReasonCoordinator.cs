using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Liminal.Net.BasePackets;
using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    /// <summary>
    /// Single place game code subscribes to for "why did this connection end."
    /// App-level kicks (KickWithReason / DisconnectWithReason) reason travels to the peer over the wire via DisconnectNoticePacket, with a grace period before the socket is force-closed. this period can be set in LiminalTransportConfig.
    /// Transport level kicks (protocol violation, invalid size, queue/buffer) captured locally only, via ILiminalTransportDiagnostics if the transport implements it. Never sent to the peer
    /// A socket that just dies with nothing on record falls back to ConnectionLost.
    /// </summary>

    public class DisconnectReasonCoordinator : IDisposable
    {
        private readonly ILiminalTransport _transport;
        private readonly LiminalPacketInterpreter _interpreter;
        private readonly LiminalNetworkConfig _config;

        private readonly ConcurrentDictionary<ushort, (DisconnectReason Reason, string Message)> _resolved = new();
        private readonly ConcurrentDictionary<ushort, TaskCompletionSource<bool>> _pendingAcks = new();

        //Newest connection seen per client id, an event of an older one belongs to a connection the id already moved on from and is ignored
        private readonly ConcurrentDictionary<ushort, uint> _latestGeneration = new();
        //Newest connection a resolve fired for, each connection resolves once and an old one can never block a newer one
        private readonly ConcurrentDictionary<ushort, uint> _resolvedGeneration = new();

        private readonly object _disposeLock = new();
        private volatile bool _disposed;

        public event Action<ushort, DisconnectReason, string> OnResolved;
        private readonly CancellationTokenSource _disposeCts = new();

        public DisconnectReasonCoordinator(ILiminalTransport transport, LiminalPacketInterpreter interpreter)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _interpreter = interpreter ?? throw new ArgumentNullException(nameof(interpreter));

            _config = _transport.Config;

            _transport.OnTransportDisconnectReason += HandleTransportReason;

            _interpreter.Subscribe<DisconnectNoticePacket>(HandleNoticePacket, this);
            _interpreter.Subscribe<DisconnectAckPacket>(HandleAckPacket, this);

            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnLocalClientConnected += HandleLocalClientConnected;
            _transport.OnClientDisconnected += HandleClientDisconnected;
            _transport.OnClientKicked += HandleClientDisconnected;
            _transport.OnLocalClientDisconnected += HandleLocalClientDisconnected;
        }

        //Moves the stored generation up to generation, false when it is already there or past it
        private static bool TryRaiseGeneration(ConcurrentDictionary<ushort, uint> map, ushort clientId, uint generation)
        {
            while (true)
            {
                if (map.TryGetValue(clientId, out var current))
                {
                    if (current >= generation) return false;
                    if (map.TryUpdate(clientId, generation, current)) return true;
                }
                else if (map.TryAdd(clientId, generation))
                {
                    return true;
                }
            }
        }

        private bool IsStale(ushort clientId, uint generation) =>
            generation != 0 && _latestGeneration.TryGetValue(clientId, out var latest) && latest > generation;

        private void HandleClientConnected(LiminalConnection client)
        {
            TryRaiseGeneration(_latestGeneration, client.ClientId, client.Generation);
            _resolved.TryRemove(client.ClientId, out _);
        }

        private void HandleLocalClientConnected(ushort localId, LiminalConnection server)
        {
            TryRaiseGeneration(_latestGeneration, localId, server.Generation);
            _resolved.TryRemove(localId, out _);
        }

        private void HandleLocalClientDisconnected(ushort localId, LiminalConnection server)
        {
            if (IsStale(localId, server.Generation)) return;

            // If the client never received a specific reason from the server,
            // resolve under its local ID as ConnectionLost.
            _resolved.TryAdd(localId, (DisconnectReason.ConnectionLost, "Connection to server was lost."));
            HandleDisconnected(localId, server.Generation);
        }

        public void ServerKickWithReason(ushort clientId, DisconnectReason reason, string message = null, int? graceSeconds = null)
        {
            if (_disposed) return;

            //Taken now, the kick below runs after the grace period and must only hit this connection
            LiminalConnection connection = _transport.GetConnection(clientId);

            _resolved[clientId] = (reason, message);

            var ackTcs = _pendingAcks.GetOrAdd(clientId, _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

            try
            {
                _interpreter.SendCommand(clientId, new DisconnectNoticePacket { Reason = (byte)reason, Message = message });
            }
            catch
            {
                LiminalLogger.Log($"[DisconnectReasonCoordinator] Failed to send kick notice to {clientId}. Socket unusable.");
                KickConnection(connection);
                return;
            }

            var timeout = TimeSpan.FromSeconds(graceSeconds ?? _config.WaitForKickGracePeriod);

            _ = Task.Run(async () =>
            {
                try
                {
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_disposeCts.Token);
                    var delayTask = Task.Delay(timeout, linkedCts.Token);
                    var completedTask = await Task.WhenAny(ackTcs.Task, delayTask).ConfigureAwait(false);

                    if (completedTask == ackTcs.Task && await ackTcs.Task.ConfigureAwait(false))
                    {
                        await Task.Delay(20, linkedCts.Token).ConfigureAwait(false);
                    }
                    else
                    {
                        LiminalLogger.Log($"[DisconnectReasonCoordinator] Kick ACK timed out for client {clientId}. Forcing kick.");
                    }

                    linkedCts.Cancel();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    LiminalLogger.LogWarning($"[DisconnectReasonCoordinator] Kick sequence error for client {clientId}: {ex.Message}");
                }
                finally
                {
                    RemovePendingAck(clientId, ackTcs);
                    KickConnection(connection);
                }
            });
        }

        public void ClientDisconnectWithReason(DisconnectReason reason, string message = null, int? graceSeconds = null)
        {
            if (_disposed || !_transport.IsConnected || (_transport.IsServer && _transport.LocalClientId == ILiminalTransport.SERVER_ID))
            {
                _transport.Disconnect();
                return;
            }

            ushort myId = _transport.LocalClientId;
            //Taken now, the disconnect below runs after the grace period and must only end this connection, not one a restart made since
            LiminalConnection server = _transport.GetConnection(ILiminalTransport.SERVER_ID);
            _resolved[myId] = (reason, message);

            var ackTcs = _pendingAcks.GetOrAdd(ILiminalTransport.SERVER_ID, _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

            try
            {
                _interpreter.SendCommand(ILiminalTransport.SERVER_ID, new DisconnectNoticePacket { Reason = (byte)reason, Message = message });
            }
            catch
            {
                _transport.Disconnect();
                return;
            }

            var timeout = TimeSpan.FromSeconds(graceSeconds ?? _config.WaitForKickGracePeriod);

            _ = Task.Run(async () =>
            {
                try
                {
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_disposeCts.Token);
                    var delayTask = Task.Delay(timeout, linkedCts.Token);
                    var completedTask = await Task.WhenAny(ackTcs.Task, delayTask).ConfigureAwait(false);

                    if (completedTask == ackTcs.Task)
                    {
                        await Task.Delay(20, linkedCts.Token).ConfigureAwait(false);
                    }
                    linkedCts.Cancel();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    LiminalLogger.LogWarning($"[DisconnectReasonCoordinator] Client disconnect error: {ex.Message}");
                }
                finally
                {
                    RemovePendingAck(ILiminalTransport.SERVER_ID, ackTcs);
                    Resolve(myId, server.Generation);
                    _transport.Disconnect(server);
                }
            });
        }

        //A newer kick for the same id may have put its own ack in, only ours goes
        private void RemovePendingAck(ushort id, TaskCompletionSource<bool> ackTcs) =>
            ((ICollection<KeyValuePair<ushort, TaskCompletionSource<bool>>>)_pendingAcks)
                .Remove(new KeyValuePair<ushort, TaskCompletionSource<bool>>(id, ackTcs));

        //No connection when the kick started means the client was already gone, nothing to kick
        private void KickConnection(LiminalConnection connection)
        {
            if (connection.IsValid)
                _transport.Kick(connection);
        }

        public void RecordReason(ushort clientId, DisconnectReason reason, string message)
        {
            if (_disposed) return;
            _resolved[clientId] = (reason, message);
        }

        //The local side left on purpose, a reason the server already sent (a kick) is kept
        internal void RecordLocalLeave(ushort localId, string message)
        {
            if (_disposed) return;
            _resolved.TryAdd(localId, (DisconnectReason.ClientDisconnected, message));
        }

        //For a local leave the transport wont report, like Shutdown
        internal void ResolveLocalLeave(ushort localId, string message)
        {
            RecordLocalLeave(localId, message);
            Resolve(localId, _transport.GetConnection(ILiminalTransport.SERVER_ID).Generation);
        }

        public bool TryGetResolvedReason(ushort id, out DisconnectReason reason, out string message)
        {
            if (_resolved.TryGetValue(id, out var entry))
            {
                reason = entry.Reason;
                message = entry.Message;
                return true;
            }
            reason = DisconnectReason.Unknown;
            message = null;
            return false;
        }

        internal void HandleTransportReason(LiminalConnection connection, DisconnectReason reason, string msg)
        {
            if (IsStale(connection.ClientId, connection.Generation)) return;

            ushort id = connection.ClientId;
            while (true)
            {
                if (_resolved.TryGetValue(id, out var existing))
                {
                    if (existing.Reason != DisconnectReason.Unknown &&
                        existing.Reason != DisconnectReason.ConnectionLost)
                    {
                        return;
                    }

                    if (reason == DisconnectReason.Unknown || reason == DisconnectReason.ConnectionLost)
                    {
                        return;
                    }

                    if (_resolved.TryUpdate(id, (reason, msg), existing))
                    {
                        return;
                    }
                }
                else
                {
                    if (_resolved.TryAdd(id, (reason, msg)))
                    {
                        return;
                    }
                }
            }
        }

        private void HandleNoticePacket(DisconnectNoticePacket packet, ushort sender)
        {
            var reason = (DisconnectReason)packet.Reason;

            if (!_transport.IsServer)
            {
                _resolved[_transport.LocalClientId] = (reason, packet.Message);
            }
            else
            {
                _resolved[sender] = (reason, packet.Message);
            }

            try
            {
                _interpreter.SendCommand(sender, new DisconnectAckPacket());
            }
            catch { }
        }

        private void HandleAckPacket(DisconnectAckPacket packet, ushort sender)
        {
            if (_pendingAcks.TryRemove(sender, out var tcs))
                tcs.TrySetResult(true);
            else if (_pendingAcks.TryRemove(ILiminalTransport.SERVER_ID, out var serverTcs))
                serverTcs.TrySetResult(true);
        }

        private void HandleClientDisconnected(LiminalConnection client)
        {
            if (IsStale(client.ClientId, client.Generation)) return;

            HandleDisconnected(client.ClientId, client.Generation);
        }

        private void HandleDisconnected(ushort id, uint generation)
        {
            if (_pendingAcks.TryRemove(id, out var tcs))
                tcs.TrySetCanceled();

            if (_pendingAcks.TryRemove(ILiminalTransport.SERVER_ID, out var sTcs))
                sTcs.TrySetCanceled();

            Resolve(id, generation);
        }

        //generation 0 is a leave with no connection behind it, like a connect that failed, it fires only when nothing fired for the id yet
        private void Resolve(ushort id, uint generation)
        {
            Action<ushort, DisconnectReason, string> handlerSnapshot;
            DisconnectReason reason;
            string msg;

            lock (_disposeLock)
            {
                if (_disposed) return;

                if (!TryRaiseGeneration(_resolvedGeneration, id, generation))
                    return;

                var (r, m) = _resolved.TryRemove(id, out var recorded)
                    ? recorded
                    : (DisconnectReason.ConnectionLost, null);

                reason = r;
                msg = m;
                handlerSnapshot = OnResolved;
            }

            LiminalLogger.Log($"[DisconnectReasonCoordinator] Resolved client disconnect {id} with reason {reason}: {msg}", LiminalLogger.LogLevel.Detailed);
            handlerSnapshot?.Invoke(id, reason, msg);
        }

        public void Dispose()
        {
            Action<ushort, DisconnectReason, string> handlerSnapshot;
            ConcurrentDictionary<ushort, (DisconnectReason Reason, string Message)> remaining;

            lock (_disposeLock)
            {
                if (_disposed) return;
                _disposed = true;

                _transport.OnTransportDisconnectReason -= HandleTransportReason;

                _transport.OnClientConnected -= HandleClientConnected;
                _transport.OnLocalClientConnected -= HandleLocalClientConnected;
                _transport.OnClientDisconnected -= HandleClientDisconnected;
                _transport.OnClientKicked -= HandleClientDisconnected;
                _transport.OnLocalClientDisconnected -= HandleLocalClientDisconnected;

                handlerSnapshot = OnResolved;

                // If local client dropped and was never resolved, ensure it fires ConnectionLost before dying
                if (!_transport.IsServer && _transport.LocalClientId != 0 && !_resolvedGeneration.ContainsKey(_transport.LocalClientId))
                {
                    _resolved.TryAdd(_transport.LocalClientId, (DisconnectReason.ConnectionLost, "Connection to server was lost."));
                }

                remaining = new ConcurrentDictionary<ushort, (DisconnectReason, string)>(_resolved);
            }

            _disposeCts.Cancel();

            foreach (var kvp in remaining)
            {
                _latestGeneration.TryGetValue(kvp.Key, out var latest);
                if (TryRaiseGeneration(_resolvedGeneration, kvp.Key, latest))
                {
                    handlerSnapshot?.Invoke(kvp.Key, kvp.Value.Reason, kvp.Value.Message);
                }
            }

            foreach (var kvp in _pendingAcks)
            {
                kvp.Value.TrySetCanceled();
            }
            _pendingAcks.Clear();

            _interpreter.UnsubscribeAll(this);
            _resolved.Clear();
            _latestGeneration.Clear();
            _resolvedGeneration.Clear();

            OnResolved = null;
            _disposeCts.Dispose();
        }
    }

    public enum DisconnectReason : byte
    {
        Unknown = 0,

        ClientDisconnected = 1,

        Kicked = 2,

        ServerShuttingDown = 3,
        Timeout = 4,

        /// <summary>Socket dropped with no reason on record.</summary>
        ConnectionLost = 5,

        /// <summary>Malformed frame/header. Local-diagnostics only never sent to the peer.</summary>
        ProtocolViolation = 6,

        /// <summary>Payload size outside allowed bounds. Local-diagnostics only.</summary>
        InvalidPacketSize = 7,

        /// <summary>Inbound queue overflow. Local-diagnostics only.</summary>
        InboundQueueOverflow = 8,

        /// <summary>Outbound buffer overflow. Local-diagnostics only.</summary>
        OutboundBufferOverflow = 9,

        /// <summary>Rejected at capacity (MaxConnectionCount reached). Safe to disclose to the peer.</summary>
        ServerFull = 10,

        VersionMismatch = 11,

        /// <summary>Pair with a message string for anything not covered above.</summary>
        Custom = 255
    }
}