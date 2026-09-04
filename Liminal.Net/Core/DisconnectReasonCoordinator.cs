using System;
using System.Collections.Concurrent;
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
        private readonly ILiminalTransportDiagnostics _diagnostics;
        private readonly LiminalTransportConfig _config;

        private readonly ConcurrentDictionary<ushort, (DisconnectReason Reason, string Message)> _resolved = new();
        private readonly ConcurrentDictionary<ushort, TaskCompletionSource<bool>> _pendingAcks = new();
        private readonly ConcurrentDictionary<ushort, bool> _alreadyFired = new();

        private readonly object _disposeLock = new();
        private volatile bool _disposed;

        public event Action<ushort, DisconnectReason, string> OnResolved;
        private readonly CancellationTokenSource _disposeCts = new();

        public DisconnectReasonCoordinator(ILiminalTransport transport, LiminalPacketInterpreter interpreter)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _interpreter = interpreter ?? throw new ArgumentNullException(nameof(interpreter));

            _config = _transport.Config;

            _diagnostics = transport as ILiminalTransportDiagnostics;
            if (_diagnostics != null)
                _diagnostics.OnTransportDisconnectReason += HandleTransportReason;

            _interpreter.Subscribe<DisconnectNoticePacket>(HandleNoticePacket, this);
            _interpreter.Subscribe<DisconnectAckPacket>(HandleAckPacket, this);

            _transport.OnClientDisconnected += HandleDisconnected;
            _transport.OnClientKicked += HandleDisconnected;
            _transport.OnLocalClientDisconnected += HandleDisconnected;
        }

        public void ServerKickWithReason(ushort clientId, DisconnectReason reason, string message = null, int? graceSeconds = null)
        {
            if (_disposed) return;

            _resolved[clientId] = (reason, message);

            var ackTcs = _pendingAcks.GetOrAdd(clientId, _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

            try
            {
                _interpreter.SendCommand(clientId, new DisconnectNoticePacket { Reason = (byte)reason, Message = message });
            }
            catch
            {
                LiminalLogger.Log($"[DisconnectReasonCoordinator] Failed to send kick notice to {clientId}. Socket unusable.");
                _transport.Kick(clientId);
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
                        // Brief drain so the transport has time to complete socket writes
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
                    _pendingAcks.TryRemove(clientId, out _);
                    _transport.Kick(clientId);
                }
            });
        }

        public void ClientDisconnectWithReason(DisconnectReason reason, string message = null, int? graceSeconds = null)
        {
            if (_disposed || _transport.IsServer && _transport.LocalClientId == ILiminalTransport.SERVER_ID)
            {
                // Dedicated servers don't shouldnt call this but just in case we directly shut down the transport!

                _transport.Disconnect();
                return;
            }

            // The client itself is disconnecting, so record it under its own LocalClientId
            ushort myId = _transport.LocalClientId;
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
                    _pendingAcks.TryRemove(ILiminalTransport.SERVER_ID, out _);

                    OnResolved?.Invoke(myId, reason, message);

                    _transport.Disconnect();
                }
            });
        }

        private void HandleTransportReason(ushort id, DisconnectReason reason, string msg)
        {
            if (_resolved.TryGetValue(id, out var existing))
            {
                if (existing.Reason != DisconnectReason.Unknown &&
                    existing.Reason != DisconnectReason.ConnectionLost &&
                    reason == DisconnectReason.Kicked)
                {
                    return;
                }
            }

            _resolved[id] = (reason, msg);
        }

        private void HandleNoticePacket(DisconnectNoticePacket packet, ushort sender)
        {
            var reason = (DisconnectReason)packet.Reason;

            if (!_transport.IsServer)
            {
                // Client received kick notice store it under my local ID so when my socket drops, it resolves cleanly
                _resolved[_transport.LocalClientId] = (reason, packet.Message);
            }
            else
            {
                // Server received notice from client store under the sender's client ID
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

        private void HandleDisconnected(ushort id)
        {
            if (_pendingAcks.TryRemove(id, out var tcs))
                tcs.TrySetCanceled();

            if (_pendingAcks.TryRemove(ILiminalTransport.SERVER_ID, out var sTcs))
                sTcs.TrySetCanceled();

            Resolve(id);
        }

        private void Resolve(ushort id)
        {
            Action<ushort, DisconnectReason, string> handlerSnapshot;
            DisconnectReason reason;
            string msg;

            lock (_disposeLock)
            {
                if (_disposed) return;

                if (!_alreadyFired.TryAdd(id, true))
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

                if (_diagnostics != null)
                    _diagnostics.OnTransportDisconnectReason -= HandleTransportReason;

                _transport.OnClientDisconnected -= HandleDisconnected;
                _transport.OnClientKicked -= HandleDisconnected;
                _transport.OnLocalClientDisconnected -= HandleDisconnected;

                handlerSnapshot = OnResolved;
                remaining = new ConcurrentDictionary<ushort, (DisconnectReason, string)>(_resolved);
            }

            _disposeCts.Cancel();

            foreach (var kvp in remaining)
            {
                if (_alreadyFired.TryAdd(kvp.Key, true))
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
            _alreadyFired.Clear();

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