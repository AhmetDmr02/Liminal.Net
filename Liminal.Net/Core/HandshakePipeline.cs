using Liminal.Net.BasePackets;
using Liminal.Net.Interfaces;
using MessagePack;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace Liminal.Net.Core
{
    internal class HandshakePipeline
    {
        private readonly ILiminalClientIdResolver _resolver;
        private readonly float _timeoutMs;
        private readonly int _maxHandshakeSize;

        public HandshakePipeline(ILiminalClientIdResolver resolver,int maxHandshakeSize = 256, float timeoutMs = 5000)
        {
            _resolver = resolver;
            _timeoutMs = timeoutMs;
            _maxHandshakeSize = maxHandshakeSize;
        }

        public virtual async Task<ushort> TryVerifyClientAsync(TcpClient client, ushort serverVersion)
        {
            try
            {
                var stream = client.GetStream();
                TimeSpan timeout = TimeSpan.FromMilliseconds(_timeoutMs);
                using var cts = new CancellationTokenSource(timeout);

                byte[] header = new byte[8];

                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);

                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                int packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));

                if (length <= 0 || length > _maxHandshakeSize || packetId != 1)
                {
                    return Drop(client, $"Security Violation: ID {packetId}, Length {length}");
                }

                byte[] payload = new byte[length];
                await stream.ReadExactlyAsync(payload, 0, length, cts.Token);

                var clientInfo = DeserializeSafe<ConnectionHandshakePacketClient>(payload);
                if (clientInfo == null) return Drop(client, "Malformed Packet 1");

                if (clientInfo.ClientVersion != serverVersion)
                    return Drop(client, $"Version Mismatch: {clientInfo.ClientVersion}");

                ushort assignedId = _resolver.ResolveClientId(payload);
                //For encrypted stuff we maybe reassign a cookie or something


                assignedId = assignedId == 0 ? _resolver.GenerateClientId() : assignedId;
                if (assignedId == 0)
                {
                    return Drop(client, "Unable to Assign Client ID");
                }

                var serverResponse = new ConnectionHandshakePacketServer
                {
                    ServerVersion = serverVersion,
                    AssignedClientID = assignedId
                };

                await SendPacketAsync(stream, 2, serverResponse, cts.Token);

                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);
                length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                packetId = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4));

                if (packetId != 3 || length > _maxHandshakeSize || length <= 0)
                    return Drop(client, $"Protocol Violation: Expected ACK (ID 3) under {_maxHandshakeSize} bytes.");

                byte[] ackPayload = new byte[length];
                await stream.ReadExactlyAsync(ackPayload, 0, length, cts.Token);

                var ack = DeserializeSafe<ConnectionHandshakeClientAck>(ackPayload);
                if (ack == null || !ack.Ack) return Drop(client, "Client Rejected ID/ACK");

                if(ack.ClientID != assignedId) return Drop(client, "Client Rejected Wrong ID/ACK");

                return assignedId;
            }
            catch (OperationCanceledException) { return Drop(client, "Handshake Timeout"); }
            catch (Exception ex) { return Drop(client, $"Fatal Handshake Error: {ex.Message}"); }
        }

        private T DeserializeSafe<T>(byte[] data)
        {
            try
            {
                var options = MessagePackSerializerOptions.Standard
                    .WithSecurity(MessagePackSecurity.UntrustedData);

                return MessagePackSerializer.Deserialize<T>(data, options);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogWarning($"[Security] Deserialization failure for {typeof(T).Name}: {ex.Message}");
                return default;
            }
        }

        private ushort Drop(TcpClient client, string reason)
        {
            LiminalLogger.LogWarning($"[Handshake] Dropping {client.Client.RemoteEndPoint}: {reason}");

            try
            {
                client.Client.LingerState = new LingerOption(true, 0); // Force RST
                client.Close();
            }
            catch {  }

            return 0;
        }

        private async Task SendPacketAsync<T>(NetworkStream stream, int id, T packet, CancellationToken token)
        {
            byte[] body = MessagePackSerializer.Serialize(packet);
            byte[] full = new byte[8 + body.Length];

            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(0, 4), body.Length);
            BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(4, 4), id);
            body.CopyTo(full.AsSpan(8));

            await stream.WriteAsync(full, token);
        }
    }
}
