using Liminal.Net.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Liminal.Net.Interfaces
{
    public interface ISessionTelemetryProvider
    {
        /// <summary>
        /// Reads aggregate engine metrics across all sessions and queues.
        /// </summary>
        public GlobalSessionTelemetrySnapshot GetGlobalSnapshot();

        public void InitializeConfig (LiminalTelemetryConfig config);

        public event Action<ushort,Memory<byte>> OnPacketBuffered;
    }
}
