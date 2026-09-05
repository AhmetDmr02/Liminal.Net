using Liminal.Net.Handshakes;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public static class DefaultHandshakes
    {
        public static async Task<HandshakeResult> ServerTcpHandshake(TcpClient client, LiminalTransportConfig config, Func<bool> canAccept)
        {
            var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config, config.MaxHandshakeSize, (int)config.HandshakeTimeout);
            return await pipeline.TryVerifyClientAsync(client, config.Version, canAccept);
        }

        public static async Task<HandshakeResult> ClientTcpHandshake(TcpClient client, LiminalTransportConfig config)
        {
            try
            {
                var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config, config.MaxHandshakeSize, (int)config.HandshakeTimeout);

                return await pipeline.TryConnectToServerAsync(client, config.Version);
            }
            catch (Exception ex)
            {
                return HandshakeResult.Fail(DisconnectReason.ConnectionLost, ex.Message);
            }
        }
    }

    public readonly struct HandshakeResult
    {
        public ushort ClientId { get; }
        public bool Success => ClientId != 0;
        public DisconnectReason FailureReason { get; }
        public string FailureMessage { get; }

        private HandshakeResult(ushort id, DisconnectReason reason, string message)
        {
            ClientId = id;
            FailureReason = reason;
            FailureMessage = message;
        }

        public static HandshakeResult Ok(ushort id)
            => new(id, DisconnectReason.Unknown, null);

        public static HandshakeResult Fail(DisconnectReason reason, string message = null)
            => new(0, reason, message);
    }
}