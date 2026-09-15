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

        public static void SubscribeBitStream<TMeta>(BitStreamHandler<TMeta> callback, object subscriber) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;
            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Manager instance is null. Cannot subscribe.");
                return;
            }

            manager.Interpreter.SubscribeBitStream(callback, subscriber);
        }

        public static void SubscribeBitStream<TPacket>(BitStreamTagHandler callback, object subscriber) where TPacket : struct
        {
            var manager = LiminalNetworkManager.Instance;
            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Manager instance is null. Cannot subscribe.");
                return;
            }

            manager.Interpreter.SubscribeBitStream<TPacket>(callback, subscriber);
        }

        public static void UnsubscribeBitStream<TMeta>(object subscriber) where TMeta : struct
        {
            LiminalNetworkManager.Instance?.Interpreter.UnsubscribeBitStream<TMeta>(subscriber);
        }

        public static void UnsubscribeAll(object subscriber)
        {
            LiminalNetworkManager.Instance?.Interpreter.UnsubscribeAll(subscriber);
        }
        #endregion

        #region Send Path
        public static void Send<T>(SendTo target, T packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where T : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send packet: LiminalNetworkManager.Instance is null.");
                return;
            }

            if (!manager.CanSend)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send packet: Network is not active or ready to send.");
                return;
            }

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        manager.Interpreter.SendCommand(manager.localID, packet, deliveryMethod);
                        return;

                    case SendTo.Server:
                        manager.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, packet, deliveryMethod);
                        return;

                    default:
                        LiminalLogger.LogError($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                        return;
                }
            }

            ResolveAndSendMulticast(manager, target, packet, deliveryMethod);
        }

        public static void SendToClient<T>(ushort targetClientId, T packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where T : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (!ValidateManager(manager, out var role)) return;

            if (role == NetworkRole.Client)
            {
                LiminalLogger.LogError("[Broadcaster] Standalone clients cannot use SendToClient. Route packets through SendTo.Server instead.");
                return;
            }

            if (!ValidateTargetSession(manager, targetClientId)) return;

            manager.Interpreter.SendCommandAsServer(targetClientId, packet, deliveryMethod);
        }

        public static void SendBitStream<TMeta>(SendTo target, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send bitstream: LiminalNetworkManager.Instance is null.");
                return;
            }

            if (!manager.CanSend)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send bitstream: Network is not active or ready to send.");
                return;
            }

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        manager.Interpreter.SendBitStream(manager.localID, in meta, bitstream, deliveryMethod);
                        return;

                    case SendTo.Server:
                        manager.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, bitstream, deliveryMethod);
                        return;

                    default:
                        LiminalLogger.LogError($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                        return;
                }
            }

            ResolveAndSendBitStreamMulticast(manager, target, in meta, bitstream, deliveryMethod);
        }

        public static void SendBitStreamToClient<TMeta>(ushort targetClientId, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (!ValidateManager(manager, out var role)) return;

            if (role == NetworkRole.Client)
            {
                LiminalLogger.LogError("[Broadcaster] Standalone clients cannot use SendBitStreamToClient. Route packets through SendTo.Server instead.");
                return;
            }

            if (!ValidateTargetSession(manager, targetClientId)) return;

            manager.Interpreter.SendBitStreamAsServer(targetClientId, in meta, bitstream, deliveryMethod);
        }

        public static void SendBitStream<TMeta>(SendTo target, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send bitstream: LiminalNetworkManager.Instance is null.");
                return;
            }

            if (!manager.CanSend)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send bitstream: Network is not active or ready to send.");
                return;
            }

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        manager.Interpreter.SendBitStream(manager.localID, in meta, packAction, deliveryMethod);
                        return;

                    case SendTo.Server:
                        manager.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, packAction, deliveryMethod);
                        return;

                    default:
                        LiminalLogger.LogError($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                        return;
                }
            }

            ResolveAndSendBitStreamActionMulticast(manager, target, in meta, packAction, deliveryMethod);
        }

        public static void SendBitStream<TMeta, TState>(SendTo target, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
            {
                LiminalLogger.LogError("[Broadcaster] Cannot send bitstream: LiminalNetworkManager.Instance is null.");
                return;
            }

            if (!manager.CanSend)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send bitstream: Network is not active or ready to send.");
                return;
            }

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        manager.Interpreter.SendBitStream(manager.localID, in meta, in state, packAction, deliveryMethod);
                        return;

                    case SendTo.Server:
                        manager.Interpreter.SendBitStream(ILiminalTransport.SERVER_ID, in meta, in state, packAction, deliveryMethod);
                        return;

                    default:
                        LiminalLogger.LogError($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                        return;
                }
            }

            ResolveAndSendBitStreamActionStateMulticast(manager, target, in meta, in state, packAction, deliveryMethod);
        }

        public static void SendBitStreamToClient<TMeta>(ushort targetClientId, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (!ValidateManager(manager, out var role)) return;

            if (role == NetworkRole.Client)
            {
                LiminalLogger.LogError("[Broadcaster] Standalone clients cannot use SendBitStreamToClient. Route packets through SendTo.Server instead.");
                return;
            }

            if (!ValidateTargetSession(manager, targetClientId)) return;

            manager.Interpreter.SendBitStreamFrom(ILiminalTransport.SERVER_ID, targetClientId, in meta, packAction, deliveryMethod);
        }

        public static void SendBitStreamToClient<TMeta, TState>(ushort targetClientId, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (!ValidateManager(manager, out var role)) return;

            if (role == NetworkRole.Client)
            {
                LiminalLogger.LogError("[Broadcaster] Standalone clients cannot use SendBitStreamToClient. Route packets through SendTo.Server instead.");
                return;
            }

            if (!ValidateTargetSession(manager, targetClientId)) return;

            manager.Interpreter.SendBitStreamFrom(ILiminalTransport.SERVER_ID, targetClientId, in meta, in state, packAction, deliveryMethod);
        }

        public static BitStreamScope BeginBitStream<TMeta>(SendTo target, in TMeta meta, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
                throw new InvalidOperationException("[Broadcaster] Cannot begin bitstream: LiminalNetworkManager.Instance is null.");

            if (!manager.CanSend)
                throw new InvalidOperationException("[Broadcaster] Cannot begin bitstream: Network is not active or ready to send.");

            if (manager.Role == NetworkRole.Client)
            {
                switch (target)
                {
                    case SendTo.Me:
                        return manager.Interpreter.BeginBitStreamScope(manager.localID, in meta, deliveryMethod);

                    case SendTo.Server:
                        return manager.Interpreter.BeginBitStreamScope(ILiminalTransport.SERVER_ID, in meta, deliveryMethod);

                    default:
                        throw new InvalidOperationException($"[Broadcaster] Standalone clients can only use SendTo.Server or SendTo.Me. Attempted: {target}");
                }
            }

            return manager.Interpreter.BeginBitStreamScope(target, in meta, deliveryMethod);
        }

        public static BitStreamScope BeginBitStreamToClient<TMeta>(ushort targetClientId, in TMeta meta, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            var manager = LiminalNetworkManager.Instance;

            if (manager == null)
                throw new InvalidOperationException("[Broadcaster] Cannot begin bitstream: LiminalNetworkManager.Instance is null.");

            if (manager.Role == NetworkRole.Client)
                throw new InvalidOperationException("[Broadcaster] Standalone clients cannot use BeginBitStreamToClient. Route packets through SendTo.Server instead.");

            if (!ValidateTargetSession(manager, targetClientId))
                throw new InvalidOperationException($"[Broadcaster] Client ID {targetClientId} is not a valid active session.");

            return manager.Interpreter.BeginBitStreamScope(targetClientId, in meta, deliveryMethod);
        }

        private static void ResolveAndSendMulticast<T>(LiminalNetworkManager manager, SendTo target, T packet, DeliveryMethod deliveryMethod) where T : struct
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> filteredTargets = stackalloc ushort[maxClients];
            int targetCount = ResolveTargets(manager, target, filteredTargets, out bool sendAsClient);

            if (targetCount == 0) return;

            var targetsSlice = filteredTargets.Slice(0, targetCount);

            if (sendAsClient)
            {
                manager.Interpreter.SendCommandAsClient(targetsSlice, packet, deliveryMethod);
            }
            else
            {
                manager.Interpreter.SendCommandAsServer(targetsSlice, packet, deliveryMethod);
            }
        }

        private static void ResolveAndSendBitStreamMulticast<TMeta>(LiminalNetworkManager manager, SendTo target, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod) where TMeta : struct
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> filteredTargets = stackalloc ushort[maxClients];
            int targetCount = ResolveTargets(manager, target, filteredTargets, out bool sendAsClient);

            if (targetCount == 0) return;

            var targetsSlice = filteredTargets.Slice(0, targetCount);

            if (sendAsClient)
            {
                manager.Interpreter.SendBitStreamAsClient(targetsSlice, in meta, bitstream, deliveryMethod);
            }
            else
            {
                manager.Interpreter.SendBitStreamAsServer(targetsSlice, in meta, bitstream, deliveryMethod);
            }
        }

        private static void ResolveAndSendBitStreamActionMulticast<TMeta>(LiminalNetworkManager manager, SendTo target, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod) where TMeta : struct
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> filteredTargets = stackalloc ushort[maxClients];
            int targetCount = ResolveTargets(manager, target, filteredTargets, out bool sendAsClient);

            if (targetCount == 0) return;

            var targetsSlice = filteredTargets.Slice(0, targetCount);
            ushort sender = sendAsClient ? manager.localID : ILiminalTransport.SERVER_ID;
            manager.Interpreter.SendBitStreamFrom(sender, targetsSlice, in meta, packAction, deliveryMethod);
        }

        private static void ResolveAndSendBitStreamActionStateMulticast<TMeta, TState>(LiminalNetworkManager manager, SendTo target, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod) where TMeta : struct
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> filteredTargets = stackalloc ushort[maxClients];
            int targetCount = ResolveTargets(manager, target, filteredTargets, out bool sendAsClient);

            if (targetCount == 0) return;

            var targetsSlice = filteredTargets.Slice(0, targetCount);
            ushort sender = sendAsClient ? manager.localID : ILiminalTransport.SERVER_ID;

            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0) return;
            ushort packetId = checked((ushort)idInt);
            var writer = manager.Interpreter.RentWriter();

            try
            {
                MessagePack.MessagePackSerializer.Serialize(writer, meta);
                var bitWriter = new BitWriter(writer);
                packAction(ref bitWriter, in state);
                bitWriter.Flush();

                for (int i = 0; i < targetsSlice.Length; i++)
                    manager.Interpreter.InvokeSendRequestSingle(sender, targetsSlice[i], packetId, writer.WrittenSpan, deliveryMethod);
            }
            finally
            {
                manager.Interpreter.ReturnWriter(writer);
            }
        }

        internal static void ResolveAndSendBitStream(LiminalNetworkManager manager, SendTo target, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod)
        {
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> filteredTargets = stackalloc ushort[maxClients];
            int targetCount = ResolveTargets(manager, target, filteredTargets, out bool sendAsClient);

            if (targetCount == 0) return;

            ushort sender = sendAsClient ? manager.localID : ILiminalTransport.SERVER_ID;
            manager.Interpreter.InvokeSendRequestMulticast(sender, filteredTargets.Slice(0, targetCount), packetId, payload, deliveryMethod);
        }

        private static int ResolveTargets(LiminalNetworkManager manager, SendTo target, Span<ushort> destination, out bool sendAsClient)
        {
            sendAsClient = false;
            int maxClients = manager.Transport.Config.MaxConnectionCount + 2;
            Span<ushort> allSessions = stackalloc ushort[maxClients];
            int totalSessions = manager.SessionManager.GetSessionIds(allSessions);

            if (totalSessions == 0) return 0;

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
                        include = true;
                        break;

                    case SendTo.NotMe:
                        include = isHost ? (id != localId) : (id != ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.NotServer:
                        include = (id != ILiminalTransport.SERVER_ID);
                        break;

                    case SendTo.NotHost:
                        include = (id != ILiminalTransport.SERVER_ID) && (!isHost || id != localId);
                        break;
                }

                if (include)
                {
                    destination[targetCount++] = id;
                }
            }

            sendAsClient = isHost && (target == SendTo.Me || target == SendTo.NotServer || target == SendTo.Server);
            return targetCount;
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

            if (!manager.CanSend)
            {
                LiminalLogger.LogWarning("[Broadcaster] Cannot send packet: Network is not active or ready to send.");
                return false;
            }

            return true;
        }

        private static bool ValidateTargetSession(LiminalNetworkManager manager, ushort targetClientId)
        {
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