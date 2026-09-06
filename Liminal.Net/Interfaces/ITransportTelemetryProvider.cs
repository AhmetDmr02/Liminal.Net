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
    }
}
