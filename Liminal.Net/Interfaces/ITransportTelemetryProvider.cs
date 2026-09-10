using Liminal.Net.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Liminal.Net.Interfaces
{
    public interface ITransportTelemetryProvider
    {
        public GlobalTransportTelemetrySnapshot GetGlobalTransportSnapshot();
        public void InitializeConfig(LiminalTelemetryConfig config);

        bool TryGetWireRTT(ushort clientId, out double rttMs);
        void SendWirePing(ushort targetId);

        /// <summary>
        /// How many milliseconds remain until the server's next tick, evaluated at the moment of the last pong.
        /// </summary>
        double ServerCountdownMs { get; }

        /// <summary>
        /// Delegate provided by the engine so the transport can read the local ticker's NextTickTimestamp.
        /// </summary>
        Func<long> NextTickProvider { get; set; }
    }
}
