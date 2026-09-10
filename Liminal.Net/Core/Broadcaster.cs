using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;

namespace Liminal.Net.Core
{
    public enum SendTo
    {
        /// <summary>
        /// Targets solely the calling peer:
        /// - Host: Targets the local client session.
        /// - Dedicated Server: Targets the server authority instance.
        /// - Client: Targets the local client instance.
        /// </summary>
        Me,

        /// <summary>
        /// Targets solely the server authority (SERVER_ID / 0).
        /// </summary>
        Server,

        /// <summary>
        /// Targets every active network session, including connected remote clients,
        /// the local host player (if running in Host mode), and the server authority itself.
        /// </summary>
        Everyone,

        /// <summary>
        /// Targets everyone except the caller:
        /// - Host: Excludes the local host client, keeping remote clients and server authority.
        /// - Dedicated Server: Excludes the server authority (SERVER_ID), keeping all remote clients.
        /// </summary>
        NotMe,

        /// <summary>
        /// Targets all connected clients and the local host client, excluding the server authority (SERVER_ID).
        /// </summary>
        NotServer,

        /// <summary>
        /// Targets only external remote clients.
        /// Excludes both the server authority (SERVER_ID) and the local host player client.
        /// </summary>
        NotHost
    }

    public static class Broadcaster
    {
        #region Subscription Relay
        public static void Subscribe<T>(Action<T, ushort> callback, object subscriber)
        {
            var manager = LiminalNetworkManager.Instance;
            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Manager instance is null. Cannot subscribe.");
                return;
            }

            manager.Interpreter.Subscribe(callback, subscriber);
        }

        public static void Unsubscribe<T>(object subscriber)
        {
            LiminalNetworkManager.Instance?.Interpreter.Unsubscribe<T>(subscriber);
        }

        public static void UnsubscribeAll(object subscriber)
        {
            LiminalNetworkManager.Instance?.Interpreter.UnsubscribeAll(subscriber);
        }
        #endregion

        #region Send Path
        public static void Send<T>(SendTo target, T packet) where T : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send packet: LiminalNetworkManager.Instance is null.");
                return;
            }

            if (manager.Role == NetworkRole.None || !manager.Transport.IsConnected)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send packet: Network is not active or connected.");
                return;
            }

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        manager.Interpreter.SendCommand(manager.localID, packet);
                        return;

                    case SendTo.Server:
                        manager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, packet);
                        return;

                    default:
                        LiminalLogger.LogError($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                        return;
                }
            }

            ResolveAndSendMulticast(manager, target, packet);
        }

        public static void SendToClient<T>(ushort targetClientId, T packet) where T : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (!ValidateManager(manager, out var role)) return;

            if (role == NetworkRole.Client)
            {
                LiminalLogger.LogError("[Broadcaster] Standalone clients cannot use SendToClient. Route packets through SendTo.Server instead.");
                return;
            }

            if (!ValidateTargetSession(manager, targetClientId)) return;

            manager.Interpreter.SendCommandAsServer(targetClientId, packet);
        }

        private static void ResolveAndSendMulticast<T>(LiminalNetworkManager manager, SendTo target, T packet) where T : struct
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2; // +2 safety buffer for Local/Server IDs
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = manager.SessionManager.GetSessionIds(allSessions);

            if (totalSessions == 0) return;

            Span<ushort> filteredTargets = stackalloc ushort[totalSessions];
            int targetCount = 0;

            ushort localId = manager.localID;
            bool isHost = manager.Role == NetworkRole.Host;

            for (int i = 0; i < totalSessions; i++)
            {
                ushort id = allSessions[i];
                bool include = false;

                switch (target)
                {
                    case SendTo.Me:
                        include = isHost ? (id == localId) : (id == ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.Server:
                        include = (id == ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.Everyone:
                        // Includes every active session: server authority, host player, and all clients
                        include = true;
                        break;

                    case SendTo.NotMe:
                        // Host: Excludes local host client (localId), keeps Server and remotes
                        // Server: Excludes Server authority (SERVER_ID), keeps all clients
                        include = isHost ? (id != localId) : (id != ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.NotServer:
                        // Excludes Server authority (SERVER_ID), keeps all clients and Host local client
                        include = (id != ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.NotHost:
                        // Excludes Server authority (SERVER_ID) and Host local client if running as Host
                        include = (id != ILiminalTransport.SERVER_ID) && (!isHost || id != localId);
                        break;
                }

                if (include)
                {
                    filteredTargets[targetCount++] = id;
                }
            }

            if (targetCount == 0) return;

            var targetsSlice = filteredTargets.Slice(0, targetCount);

            // If running in Host mode and targeting client-side scopes (.Me, .NotServer, .Server),
            // dispatch from the host's client identity (localID).
            // Otherwise, dispatch with server authority (SERVER_ID / 0).
            bool sendAsClient = isHost && (target == SendTo.Me || target == SendTo.NotServer || target == SendTo.Server);

            if (sendAsClient)
            {
                manager.Interpreter.SendCommandAsClient(targetsSlice, packet);
            }
            else
            {
                manager.Interpreter.SendCommandAsServer(targetsSlice, packet);
            }
        }
        #endregion

        #region Helpers
        private static bool ValidateManager(LiminalNetworkManager manager, out NetworkRole role)
        {
            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send packet: LiminalNetworkManager.Instance is null.");
                role = NetworkRole.None;
                return false;
            }

            role = manager.Role;

            if (role == NetworkRole.None || !manager.Transport.IsConnected)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send packet: Network is not active or connected.");
                return false;
            }

            return true;
        }

        private static bool ValidateTargetSession(LiminalNetworkManager manager, ushort targetClientId)
        {
            // Allow targeting SERVER_ID or check if client socket is connected
            if (targetClientId == ILiminalTransport.SERVER_ID || manager.Transport.IsClientConnected(targetClientId) || (manager.Role == NetworkRole.Host && targetClientId == manager.localID))
            {
                return true;
            }

            LiminalLogger.LogWarning($"[Broadcaster] Cannot send packet: Client ID {targetClientId} is not a valid active session.");
            return false;
        }
        #endregion
    }
}