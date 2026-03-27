using System.Net.Sockets;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public static class DefaultHandshakes
    {
        public static async Task<ushort> ServerTcpHandshake(TcpClient client, LiminalTransportConfig config)
        {
            var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config.MaxHandshakeSize, (int)config.HandshakeTimeout);
            return await pipeline.TryVerifyClientAsync(client, config.Version);
        }

        public static async Task<ushort> ClientTcpHandshake(TcpClient client, LiminalTransportConfig config)
        {
            try
            {
                var pipeline = new TcpHandshakePipeline(config.ClientIdResolver, config.MaxHandshakeSize, (int)config.HandshakeTimeout);
                return await pipeline.TryConnectToServerAsync(client, config.Version);
            }
            catch
            {
                return 0;
            }
        }
    }
}