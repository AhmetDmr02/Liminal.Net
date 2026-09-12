using System;
using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalTransportDisconnectDiagnostics
    {
        event Action<ushort, DisconnectReason, string> OnTransportDisconnectReason;
    }
}