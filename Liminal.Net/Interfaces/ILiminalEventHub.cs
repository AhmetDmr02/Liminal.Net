using Liminal.Net.Core;
using System;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalEventHub
    {
        #region High-Level Connection Events (Staged after Core Systems)
        event Action<ushort> OnClientConnected;
        event Action<ushort> OnClientDisconnected;
        event Action<ushort> OnLocalClientConnected;
        event Action<ushort> OnLocalClientDisconnected;
        event Action<ushort> OnClientKicked;
        event Action OnServerStarted;
        event Action OnTransportShutdown;
        #endregion

        #region Lifecycle & Diagnostics Events
        event Action<ushort, DisconnectReason, string> OnDisconnectResolved;
        event Action OnManagerPreInitialize;
        event Action OnManagerPostInitialize;
        event Action OnManagerShutdown;
        event Action OnPreFlush;
        event Action OnPostFlush;
        event Action OnPrePoll;
        event Action OnPostPoll;
        #endregion

        #region Lifecycle Cleanup
        /// <summary>
        /// Clears all registered high-level event subscribers to prevent memory leaks across restarts.
        /// </summary>
        void Clear();
        #endregion
    }
}
