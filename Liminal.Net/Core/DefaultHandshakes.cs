using Liminal.Net.Handshakes;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public static class DefaultHandshakes
    {
        public static async Task<HandshakeResult> ServerTcpHandshake(TcpClient client, LiminalNetworkConfig config, Func<bool> canAccept)
        {
            var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config, config.MaxHandshakeSize, (int)config.HandshakeTimeout);
            return await pipeline.TryVerifyClientAsync(client, config.Version, canAccept);
        }

        public static Task<HandshakeResult> ClientTcpHandshake(TcpClient client, LiminalNetworkConfig config)
            => ClientTcpHandshake(client, config, null);

        public static async Task<HandshakeResult> ClientTcpHandshake(TcpClient client, LiminalNetworkConfig config, Action<ushort> onAssignedId)
        {
            try
            {
                var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config, config.MaxHandshakeSize, (int)config.HandshakeTimeout);

                return await pipeline.TryConnectToServerAsync(client, config.Version, onAssignedId);
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
        public bool Silent { get; }

        private HandshakeResult(ushort id, DisconnectReason reason, string message, bool silent = false)
        {
            ClientId = id;
            FailureReason = reason;
            FailureMessage = message;
            Silent = silent;
        }

        public static HandshakeResult Ok(ushort id)
            => new(id, DisconnectReason.Unknown, null, false);

        public static HandshakeResult Fail(DisconnectReason reason, string message = null, bool silent = false)
            => new(0, reason, message, silent);
    }
}