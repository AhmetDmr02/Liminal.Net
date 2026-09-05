using Liminal.Net.BasePackets;
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
        private readonly ILiminalClientIdResolver _resolver;
        private readonly float _timeoutSeconds;
        private readonly int _maxHandshakeSize;
        private readonly LiminalTransportConfig _config;

        public TcpHandshakePipeline(ILiminalClientIdResolver resolver, LiminalTransportConfig config, int maxHandshakeSize = 256, float timeoutS = 5)
        {
            _resolver = resolver;
            _timeoutSeconds = timeoutS;
            _maxHandshakeSize = maxHandshakeSize;
            _config = config;
        }

        public virtual async Task<HandshakeResult> TryVerifyClientAsync(TcpClient client, ushort serverVersion, Func<bool> canAcceptConnection)
        {
            try
            {
                var stream = client.GetStream();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

                byte[] header = new byte[8];
                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);

                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                int packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));
                ushort firstPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>();

                // Protocol / security violations drop with forceRst: true
                if (length <= 0 || length > _maxHandshakeSize || packetId != firstPacketId)
                {
                    Drop(client, $"Security Violation: ID {packetId}, Length {length}", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed handshake header");
                }

                byte[] payload = new byte[length];
                await stream.ReadExactlyAsync(payload, 0, length, cts.Token);

                var clientInfo = DeserializeSafe<ConnectionHandshakePacketClient>(payload, out bool success);
                if (!success)
                {
                    Drop(client, "Malformed Packet 1", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed handshake payload");
                }

                if (clientInfo.ClientVersion != serverVersion)
                {
                    await SendRejectionAsync(stream, DisconnectReason.VersionMismatch, $"Server requires v{serverVersion}");
                    Drop(client, $"Version Mismatch: {clientInfo.ClientVersion}");
                    return HandshakeResult.Fail(DisconnectReason.VersionMismatch, $"Server version: {serverVersion}");
                }

                if (clientInfo.PacketRegistryHash != LiminalPacketLibrary.RegistryHash)
                {
                    await SendRejectionAsync(stream, DisconnectReason.ProtocolViolation, "Packet registry mismatch.");
                    Drop(client, "Packet Registry Mismatch");
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Packet Registry Mismatch");
                }

                if (!canAcceptConnection())
                {
                    await SendRejectionAsync(stream, DisconnectReason.ServerFull, "Server reached maximum player capacity.");
                    Drop(client, "Server full");
                    return HandshakeResult.Fail(DisconnectReason.ServerFull, "Server is full");
                }

                ushort assignedId = _resolver.ResolveId(payload);
                assignedId = assignedId == 0 ? _resolver.GenerateClientId() : assignedId;
                if (assignedId == 0)
                {
                    await SendRejectionAsync(stream, DisconnectReason.Custom, "Unable to assign Client ID.");
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
                await SendPacketAsync(stream, secondPacketId, serverResponse, cts.Token);

                // Wait for Client ACK
                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);
                length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));

                ushort thirdPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();
                if (packetId != thirdPacketId || length > _maxHandshakeSize || length <= 0)
                {
                    Drop(client, $"Protocol Violation: Expected ACK (ID {thirdPacketId}, Length {length})", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "ACK violation");
                }

                byte[] ackPayload = new byte[length];
                await stream.ReadExactlyAsync(ackPayload, 0, length, cts.Token);

                var ack = DeserializeSafe<ConnectionHandshakeClientAck>(ackPayload, out success);
                if (!success || !ack.Ack || ack.ClientID != assignedId)
                {
                    Drop(client, "Client Rejected ID/ACK", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Invalid ACK");
                }

                return HandshakeResult.Ok(assignedId);
            }
            catch (OperationCanceledException)
            {
                Drop(client, "Handshake Timeout");
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Handshake timed out");
            }
            catch (Exception ex)
            {
                Drop(client, $"Fatal Handshake Error: {ex.Message}");
                return HandshakeResult.Fail(DisconnectReason.Custom, ex.Message);
            }
        }

        public virtual async Task<HandshakeResult> TryConnectToServerAsync(TcpClient client, ushort clientVersion)
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

                await SendPacketAsync(stream, firstPacketId, clientInfo, cts.Token);

                byte[] header = new byte[8];
                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);

                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                int packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));

                ushort secondPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakePacketServer>();
                if (packetId != secondPacketId || length <= 0 || length > _maxHandshakeSize)
                {
                    Drop(client, $"Unexpected Server Response. ID: {packetId}, Len: {length}", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Invalid server packet");
                }

                byte[] payload = new byte[length];
                await stream.ReadExactlyAsync(payload, 0, length, cts.Token);

                var serverResponse = DeserializeSafe<ConnectionHandshakePacketServer>(payload, out bool success);
                if (!success)
                {
                    Drop(client, "Malformed Server Response.", forceRst: true);
                    return HandshakeResult.Fail(DisconnectReason.ProtocolViolation, "Malformed server response");
                }

                // Explicit server rejection
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

                ushort thirdPacketId = LiminalPacketLibrary.GetId<ConnectionHandshakeClientAck>();
                var ack = new ConnectionHandshakeClientAck
                {
                    ClientID = assignedId,
                    Ack = true
                };
                await SendPacketAsync(stream, thirdPacketId, ack, cts.Token);

                return HandshakeResult.Ok(assignedId);
            }
            catch (OperationCanceledException)
            {
                Drop(client, "Connection attempt timed out.");
                return HandshakeResult.Fail(DisconnectReason.Timeout, "Connection attempt timed out.");
            }
            catch (Exception ex)
            {
                Drop(client, $"Connection failed: {ex.Message}");
                return HandshakeResult.Fail(DisconnectReason.ConnectionLost, ex.Message);
            }
        }

        private ushort Drop(TcpClient client, string reason, bool forceRst = false)
        {
            string remoteEp = "Unknown";
            try { remoteEp = client.Client?.RemoteEndPoint?.ToString() ?? "Disconnected"; } catch { }

            LiminalLogger.LogWarning($"[Handshake] Dropping {remoteEp}: {reason}{(forceRst ? " (Forced RST)" : "")}");

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
            byte[] full = new byte[8 + body.Length];

            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(4, 4), id);
            body.CopyTo(full.AsSpan(8));

            await stream.WriteAsync(full.AsMemory(), token).ConfigureAwait(false);
        }
    }
}