using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Misc;
using MessagePack;
using System;
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Handshakes
{
    internal class TcpHandshakePipeline
    {
        public const uint HandshakeMagic = 0x4E4D494C; // ASCII "LIMN" in little-endian
        public const int HeaderSize = 12; // Magic (4B) + Length (4B) + PacketId (4B)

        private readonly ILiminalClientIdResolver _resolver;
        private readonly float _timeoutSeconds;
        private readonly int _maxHandshakeSize;
        private readonly LiminalNetworkConfig _config;

        public TcpHandshakePipeline(ILiminalClientIdResolver resolver, LiminalNetworkConfig config, int maxHandshakeSize = 256, float timeoutS = 5)
        {
            _resolver = resolver ?? new BaseResolver();
            _timeoutSeconds = timeoutS;
            _maxHandshakeSize = maxHandshakeSize;
            _config = config;
        }

        public virtual async Task<HandshakeResult> TryVerifyClientAsync(TcpClient client, ushort serverVersion, Func<bool> canAcceptConnection, Action<ushort> onClientValidated = null, Func<ushort, object> onClientPreValidated = null, Action<ushort, object> onClientPostValidated = null)
        {
            bool magicMatched = false;
            try
            {
                var stream = client.GetStream();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

                byte[] header = new byte[HeaderSize];
                int totalRead = 0;
                while (totalRead < HeaderSize)
                {
                    int read = await stream.ReadAsync(header.AsMemory(totalRead, HeaderSize - totalRead), cts.Token).ConfigureAwait(false);
                    if (read == 0)
                    {
                        bool hadMagic = totalRead >= 4 && BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4)) == HandshakeMagic;
                        if (!hadMagic)
                        {
                            Drop(client, $"Probe disconnected before handshake (read {totalRead} bytes)", forceRst: false, silent: true);
                            return HandshakeResult.Fail(DisconnectReason.ConnectionLost, "Probe disconnected", silent: true);
                        }
                        else
                        {
                            Drop(client, $"Fatal Handshake Error: Incomplete handshake header (read {totalRead}/{HeaderSize} bytes)", forceRst: true, silent: false);
                            return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Incomplete handshake header", silent: false);
                        }
                    }
                    totalRead += read;
                }

                uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
                if (magic != HandshakeMagic)
                {
                    Drop(client, $"Non-Liminal connection (Magic mismatch: 0x{magic:X8})", forceRst: true, silent: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Magic mismatch", silent: true);
                }

                magicMatched = true;

                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));
                int packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, 4));
                ushort firstPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();

                if (length <= 0 || length > _maxHandshakeSize || packetId != firstPacketId)
                {
                    Drop(client, $"Security Violation: ID {packetId}, Length {length}", forceRst: true, silent: false);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed handshake header", silent: false);
                }

                byte[] payload = new byte[length];
                await stream.LiminalReadExactlyAsync(payload, 0, length, cts.Token).ConfigureAwait(false);

                var clientInfo = DeserializeSafe<ConnectionHandshakePacketClient>(payload, out bool success);
                if (!success)
                {
                    Drop(client, "Malformed Packet 1", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed handshake payload");
                }

                if (clientInfo.ClientVersion != serverVersion)
                {
                    await SendRejectionAsync(stream, DisconnectReason.VersionMismatch, $"Server requires v{serverVersion}").ConfigureAwait(false);
                    Drop(client, $"Version Mismatch: {clientInfo.ClientVersion}");
                    return HandshakeResult.Fail(DisconnectReason.VersionMismatch, $"Server version: {serverVersion}");
                }

                if (clientInfo.PacketRegistryHash != LiminalPacketLibrary.RegistryHash)
                {
                    await SendRejectionAsync(stream, DisconnectReason.ProtocolViolation, "Packet registry mismatch.").ConfigureAwait(false);
                    Drop(client, "Packet Registry Mismatch");
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Packet Registry Mismatch");
                }

                if (!canAcceptConnection())
                {
                    await SendRejectionAsync(stream, DisconnectReason.ServerFull, "Server reached maximum player capacity.").ConfigureAwait(false);
                    Drop(client, "Server full");
                    return HandshakeResult.Fail(DisconnectReason.ServerFull, "Server is full");
                }

                ushort assignedId = _resolver.ResolveId(payload);
                assignedId = assignedId == 0 ? _resolver.GenerateClientId() : assignedId;
                if (assignedId == 0)
                {
                    await SendRejectionAsync(stream, DisconnectReason.Custom, "Unable to assign Client ID.").ConfigureAwait(false);
                    Drop(client, "Unable to Assign Client ID");
                    return HandshakeResult.Fail(DisconnectReason.Custom, "Failed to allocate ID");
                }

                var serverResponse = new ConnectionHandshakePacketServer
                {
                    ServerVersion = serverVersion,
                    AssignedClientID = assignedId,
                    PacketRegistryHash = LiminalPacketLibrary.RegistryHash,
                    RejectReason = DisconnectReason.Unknown,
                    RejectMessage = null
                };

                ushort secondPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
                await SendPacketAsync(stream, secondPacketId, serverResponse, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);

                await stream.LiminalReadExactlyAsync(header, 0, HeaderSize, cts.Token).ConfigureAwait(false);
                uint ackMagic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
                length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));
                packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, 4));

                ushort thirdPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();
                if (ackMagic != HandshakeMagic || packetId != thirdPacketId || length > _maxHandshakeSize || length <= 0)
                {
                    Drop(client, $"Protocol Violation: Expected ACK (ID {thirdPacketId}, Length {length})", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "ACK violation");
                }

                byte[] ackPayload = new byte[length];
                await stream.LiminalReadExactlyAsync(ackPayload, 0, length, cts.Token).ConfigureAwait(false);

                var ack = DeserializeSafe<ConnectionHandshakeClientAck>(ackPayload, out success);
                if (!success || !ack.Ack || ack.ClientID != assignedId)
                {
                    Drop(client, "Client Rejected ID/ACK", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Invalid ACK");
                }


                object preState = null;
                if (onClientPreValidated != null)
                {
                    preState = onClientPreValidated(assignedId);
                }

                var readyPacket = new ConnectionHandshakeReadyConfirmed();
                ushort readyPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeReadyConfirmed>();
                await SendPacketAsync(stream, readyPacketId, readyPacket, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);

                if (onClientPostValidated != null)
                {
                    onClientPostValidated(assignedId, preState);
                }

                onClientValidated?.Invoke(assignedId);

                return HandshakeResult.Ok(assignedId);
            }
            catch (OperationCanceledException)
            {
                bool silent = !magicMatched;
                Drop(client, "Handshake Timeout", forceRst: false, silent: silent);
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Handshake timed out", silent: silent);
            }
            catch (ObjectDisposedException)
            {
                bool silent = !magicMatched;
                Drop(client, "Handshake Timeout (CTS teardown race)", forceRst: false, silent: silent);
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Handshake timed out", silent: silent);
            }
            catch (Exception ex)
            {
                bool silent = !magicMatched;
                Drop(client, $"Fatal Handshake Error: {ex}", forceRst: false, silent: silent);
                return HandshakeResult.Fail(DisconnectReason.Custom, ex.Message, silent: silent);
            }
        }

        public virtual async Task<HandshakeResult> TryConnectToServerAsync(TcpClient client, ushort clientVersion, Action<ushort> onAssignedId = null)
        {
            try
            {
                var stream = client.GetStream();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

                ushort firstPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();
                var clientInfo = new ConnectionHandshakePacketClient
                {
                    ClientVersion = clientVersion,
                    PacketRegistryHash = LiminalPacketLibrary.RegistryHash
                };

                await SendPacketAsync(stream, firstPacketId, clientInfo, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);

                byte[] header = new byte[HeaderSize];
                await stream.LiminalReadExactlyAsync(header, 0, HeaderSize, cts.Token).ConfigureAwait(false);

                uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));
                int packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, 4));

                ushort secondPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
                if (magic != HandshakeMagic || packetId != secondPacketId || length <= 0 || length > _maxHandshakeSize)
                {
                    Drop(client, $"Unexpected Server Response. ID: {packetId}, Len: {length}", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Invalid server packet");
                }

                byte[] payload = new byte[length];
                await stream.LiminalReadExactlyAsync(payload, 0, length, cts.Token).ConfigureAwait(false);

                var serverResponse = DeserializeSafe<ConnectionHandshakePacketServer>(payload, out bool success);
                if (!success)
                {
                    Drop(client, "Malformed Server Response.", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed server response");
                }

                if (serverResponse.AssignedClientID == 0)
                {
                    Drop(client, $"Server rejected connection: {serverResponse.RejectReason} ({serverResponse.RejectMessage})");
                    return HandshakeResult.Fail(serverResponse.RejectReason, serverResponse.RejectMessage);
                }

                if (serverResponse.ServerVersion != clientVersion)
                {
                    Drop(client, $"Server Version Mismatch: {serverResponse.ServerVersion}");
                    return HandshakeResult.Fail(DisconnectReason.VersionMismatch, $"Server version: {serverResponse.ServerVersion}");
                }

                ushort assignedId = serverResponse.AssignedClientID;
                onAssignedId?.Invoke(assignedId);

                ushort thirdPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();
                var ack = new ConnectionHandshakeClientAck
                {
                    ClientID = assignedId,
                    Ack = true
                };
                await SendPacketAsync(stream, thirdPacketId, ack, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);

                await stream.LiminalReadExactlyAsync(header, 0, HeaderSize, cts.Token).ConfigureAwait(false);
                magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
                length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));
                packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, 4));

                ushort fourthPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeReadyConfirmed>();
                if (magic != HandshakeMagic || packetId != fourthPacketId || length < 0 || length > _maxHandshakeSize)
                {
                    Drop(client, $"Protocol Violation: Expected ReadyConfirmed (ID {fourthPacketId}, Length {length})", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Ready violation");
                }

                if (length > 0)
                {
                    byte[] readyPayload = new byte[length];
                    await stream.LiminalReadExactlyAsync(readyPayload, 0, length, cts.Token).ConfigureAwait(false);
                }

                return HandshakeResult.Ok(assignedId);
            }
            catch (OperationCanceledException)
            {
                Drop(client, "Connection attempt timed out.");
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Connection attempt timed out.");
            }
            catch (ObjectDisposedException)
            {
                Drop(client, "Connection attempt timed out (CTS teardown race).");
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Connection attempt timed out.");
            }
            catch (Exception ex)
            {
                Drop(client, $"Connection failed: {ex.Message}");
                return HandshakeResult.Fail(DisconnectReason.ConnectionLost, ex.Message);
            }
        }

        private ushort Drop(TcpClient client, string reason, bool forceRst = false, bool silent = false)
        {
            if (!silent)
            {
                string remoteEp = "Unknown";
                try { remoteEp = client.Client?.RemoteEndPoint?.ToString() ?? "Disconnected"; } catch { }

                string rstSuffix = forceRst ? " (Forced RST)" : string.Empty;
                LiminalLogger.LogWarning($"[Handshake] Dropping {remoteEp}: {reason}{rstSuffix}");
            }

            try
            {
                if (client.Client != null && client.Client.Connected)
                {
                    if (forceRst)
                    {
                        client.Client.LingerState = new LingerOption(true, 0);
                    }
                    else
                    {
                        client.Client.Shutdown(SocketShutdown.Both);
                    }
                }
            }
            catch { }
            finally
            {
                try { client.Close(); } catch { }
            }

            return 0;
        }

        private async Task SendRejectionAsync(NetworkStream stream, DisconnectReason reason, string message)
        {
            var rejection = new ConnectionHandshakePacketServer
            {
                AssignedClientID = 0,
                RejectReason = reason,
                RejectMessage = message
            };

            ushort packetId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();

            using var sendCts = new CancellationTokenSource(TimeSpan.FromSeconds(_config.SendResponseTimeout));

            try
            {
                await SendPacketAsync(stream, packetId, rejection, sendCts.Token).ConfigureAwait(false);
                await stream.FlushAsync(sendCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                LiminalLogger.LogWarning("[Handshake] Timed out pushing rejection packet; socket write stalled.");
            }
            catch (Exception ex)
            {
                LiminalLogger.LogWarning($"[Handshake] Failed to deliver rejection: {ex.Message}");
            }
        }

        private T DeserializeSafe<T>(byte[] data, out bool success)
        {
            try
            {
                var options = MessagePackSerializerOptions.Standard
                    .WithSecurity(MessagePackSecurity.UntrustedData);

                success = true;
                return MessagePackSerializer.Deserialize<T>(data, options);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogWarning($"[Security] Deserialization failure for {typeof(T).Name}: {ex.Message}");
                success = false;
                return default;
            }
        }

        private async Task SendPacketAsync<T>(NetworkStream stream, int id, T packet, CancellationToken token)
        {
            byte[] body = MessagePackSerializer.Serialize(packet);
            byte[] full = new byte[HeaderSize + body.Length];

            BinaryPrimitives.WriteUInt32LittleEndian(full.AsSpan(0, 4), HandshakeMagic);
            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(4, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(8, 4), id);
            body.CopyTo(full.AsSpan(HeaderSize));

            await stream.WriteAsync(full.AsMemory(), token).ConfigureAwait(false);
        }
    }
}
