using System;
using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalTransportDiagnostics
    {
        event Action<ushort, DisconnectReason, string> OnTransportDisconnectReason;
    }
}