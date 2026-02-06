using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    public class LiminalNetworkManager
    {
        #region Singleton
        public static LiminalNetworkManager Instance;

        public LiminalNetworkManager(ILiminalTransport transport, LiminalTransportConfig config)
        {
            Instance = this;

            if (transport == null)
            {
                LiminalLogger.LogError("Transport cannot be null.");
                return;
            }
                
            if (config == null)
            {
                LiminalLogger.LogError("Config cannot be null.");
                return;
            }

            if (config.InboundPacketProcessors == null)
            {
                LiminalLogger.LogError("PacketProcessors cannot be null.");
                return;
            }

            if(config.ClientIdResolver == null)
            {
                LiminalLogger.LogError("ClientIdResolver cannot be null.");
                return;
            }

            _config = config;

            _transport = transport;

            transport.InitializeTransport(config);
        }
        #endregion

        private readonly ILiminalTransport _transport;
        public ILiminalTransport Transport => _transport;
 
        private readonly LiminalTransportConfig _config;

        #region Methods
        public void StartHost()
        {
            StartServer(_config.Default_Host, _config.Default_Port);
        }
        public void StartServer(string ip, int port)
        {
            _transport.StartServer(ip, port);
        }
        public void StartClient(string ip, int port)
        {

        }
        #endregion
    }
}
