using Liminal.Net.Interfaces;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.Core
{
    public class LiminalPacketInterpreter
    {
        private readonly ConcurrentBag<LiminalNativeBufferWriter> _writerPool = new();

        private readonly ConcurrentDictionary<ushort, IPacketDispatcher> _handlers = new();
        private readonly ConcurrentDictionary<object, SubscriptionList> _subscribers = new();
        private readonly object _subscriptionGate = new();

        private readonly LiminalTransportConfig _config;

        private interface IPacketDispatcher
        {
            void Add<TPacket>(Action<TPacket, ushort> callback);
            void RemoveUntyped(Delegate callback);
            void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options);
            bool HasCallbacks { get; }
        }

        private sealed class TypedPacketDispatcher<T> : IPacketDispatcher
        {
            private readonly object _lock = new();

            private Action<T, ushort>[] _callbacks = Array.Empty<Action<T, ushort>>();

            public bool HasCallbacks => Volatile.Read(ref _callbacks).Length != 0;

            public void Add<TPacket>(Action<TPacket, ushort> callback)
            {
                if (callback is not Action<T, ushort> typedCallback)
                    throw new InvalidOperationException($"Invalid callback type for dispatcher {typeof(T).Name}.");

                lock (_lock)
                {
                    var current = _callbacks;
                    var next = new Action<T, ushort>[current.Length + 1];

                    Array.Copy(current, next, current.Length);
                    next[current.Length] = typedCallback;

                    Volatile.Write(ref _callbacks, next);
                }
            }

            public void RemoveUntyped(Delegate callback)
            {
                if (callback is not Action<T, ushort> typedCallback)
                    return;

                lock (_lock)
                {
                    var current = _callbacks;
                    int index = -1;

                    for (int i = 0; i < current.Length; i++)
                    {
                        if (current[i] == typedCallback)
                        {
                            index = i;
                            break;
                        }
                    }

                    if (index < 0)
                        return;

                    if (current.Length == 1)
                    {
                        Volatile.Write(ref _callbacks, Array.Empty<Action<T, ushort>>());
                        return;
                    }

                    var next = new Action<T, ushort>[current.Length - 1];

                    if (index > 0)
                        Array.Copy(current, 0, next, 0, index);

                    if (index < current.Length - 1)
                        Array.Copy(current, index + 1, next, index, current.Length - index - 1);

                    Volatile.Write(ref _callbacks, next);
                }
            }

            public void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options)
            {
                var callbacks = Volatile.Read(ref _callbacks);

                if (callbacks.Length == 0)
                    return;

                T packet = DeserializeSafe(rawData, options, out bool success);

                if (!success)
                    return;

                for (int i = 0; i < callbacks.Length; i++)
                {
                    try
                    {
                        callbacks[i](packet, sender);
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[Interpreter] Exception in {typeof(T).Name} handler: {ex}");
                    }
                }
            }

            private static T DeserializeSafe(ReadOnlyMemory<byte> data, MessagePackSerializerOptions options, out bool success)
            {
                try
                {
                    success = true;
                    return MessagePackSerializer.Deserialize<T>(data, options);
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogWarning($"[Security] Malformed packet {typeof(T).Name}: {ex.Message}");
                    success = false;
                    return default;
                }
            }
        }

        private struct Subscription
        {
            public ushort PacketId;
            public Delegate Callback;
        }

        private sealed class SubscriptionList
        {
            private readonly List<Subscription> _list = new();
            public readonly object Lock = new();

            public int Count => _list.Count;

            public void Add(Subscription sub) => _list.Add(sub);

            public List<Subscription> Clear()
            {
                if (_list.Count == 0)
                    return null;

                var copy = new List<Subscription>(_list);
                _list.Clear();
                return copy;
            }

            public List<Delegate> RemovePacketAndGetCallbacks(ushort packetId)
            {
                var removedCallbacks = new List<Delegate>();

                for (int i = _list.Count - 1; i >= 0; i--)
                {
                    if (_list[i].PacketId != packetId)
                        continue;

                    removedCallbacks.Add(_list[i].Callback);
                    _list.RemoveAt(i);
                }

                return removedCallbacks;
            }
        }

        public LiminalPacketInterpreter(LiminalTransportConfig config)
        {
            _config = config;
        }

        private LiminalNativeBufferWriter RentWriter()
        {
            if (_writerPool.TryTake(out var writer))
            {
                writer.Clear();
                return writer;
            }

            return new LiminalNativeBufferWriter(_config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch));
        }

        #region Subscriptions

        public void Subscribe<T>(Action<T, ushort> callback, object subscriber)
        {
            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            if (subscriber == null)
                throw new ArgumentNullException(nameof(subscriber));

            int idInt = LiminalPacketLibrary.GetId<T>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot subscribe to {typeof(T).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var subscription = new Subscription { PacketId = packetId, Callback = callback };

            lock (_subscriptionGate)
            {
                var subList = _subscribers.GetOrAdd(subscriber, _ => new SubscriptionList());

                lock (subList.Lock)
                {
                    var dispatcher = GetOrCreateDispatcher<T>(packetId);
                    dispatcher.Add(callback);
                    subList.Add(subscription);
                }

                LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to {typeof(T).Name} (ID: {packetId})");
            }
        }

        public void Unsubscribe<T>(object subscriber)
        {
            if (subscriber == null)
                return;

            int idInt = LiminalPacketLibrary.GetId<T>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot unsubscribe from {typeof(T).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            List<Delegate> callbacksToRemove = null;

            lock (_subscriptionGate)
            {
                if (!_subscribers.TryGetValue(subscriber, out var subList))
                    return;

                lock (subList.Lock)
                {
                    callbacksToRemove = subList.RemovePacketAndGetCallbacks(packetId);

                    if (subList.Count == 0)
                        _subscribers.TryRemove(new KeyValuePair<object, SubscriptionList>(subscriber, subList));
                }

                if (callbacksToRemove != null)
                {
                    foreach (Delegate callback in callbacksToRemove)
                        RemoveFromHandlers_NoLock(packetId, callback);
                }
            }
        }

        public void UnsubscribeAll(object subscriber)
        {
            if (subscriber == null)
                return;

            lock (_subscriptionGate)
            {
                if (!_subscribers.TryRemove(subscriber, out var subList))
                    return;

                List<Subscription> subscriptions;

                lock (subList.Lock)
                    subscriptions = subList.Clear();

                if (subscriptions != null)
                {
                    foreach (Subscription sub in subscriptions)
                        RemoveFromHandlers_NoLock(sub.PacketId, sub.Callback);
                }

                LiminalLogger.Log($"[Interpreter] Unsubscribed all handlers for {subscriber.GetType().Name}");
            }
        }

        public void ClearAllHandlers()
        {
            lock (_subscriptionGate)
            {
                foreach (var pair in _subscribers)
                {
                    lock (pair.Value.Lock)
                        pair.Value.Clear();
                }

                _subscribers.Clear();
                _handlers.Clear();
            }
        }

        private TypedPacketDispatcher<T> GetOrCreateDispatcher<T>(ushort packetId)
        {
            return (TypedPacketDispatcher<T>)_handlers.GetOrAdd(packetId, _ => new TypedPacketDispatcher<T>());
        }

        private void RemoveFromHandlers_NoLock(ushort packetId, Delegate callback)
        {
            if (!_handlers.TryGetValue(packetId, out var dispatcher))
                return;

            dispatcher.RemoveUntyped(callback);

            if (!dispatcher.HasCallbacks)
            {
                ((ICollection<KeyValuePair<ushort, IPacketDispatcher>>)_handlers)
                    .Remove(new KeyValuePair<ushort, IPacketDispatcher>(packetId, dispatcher));
            }
        }

        #endregion

        #region Sending

        public delegate void SendRequestHandler(ushort senderId, ushort targetSessionId, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable);

        private readonly object _sendHandlerLock = new();
        private SendRequestHandler[] _sendHandlers = Array.Empty<SendRequestHandler>();

        public event SendRequestHandler OnSendRequest
        {
            add
            {
                if (value == null)
                    return;

                lock (_sendHandlerLock)
                {
                    var current = _sendHandlers;
                    var next = new SendRequestHandler[current.Length + 1];

                    Array.Copy(current, next, current.Length);
                    next[current.Length] = value;

                    Volatile.Write(ref _sendHandlers, next);
                }
            }
            remove
            {
                if (value == null)
                    return;

                lock (_sendHandlerLock)
                {
                    var current = _sendHandlers;
                    int index = -1;

                    for (int i = 0; i < current.Length; i++)
                    {
                        if (current[i] == value)
                        {
                            index = i;
                            break;
                        }
                    }

                    if (index < 0)
                        return;

                    if (current.Length == 1)
                    {
                        Volatile.Write(ref _sendHandlers, Array.Empty<SendRequestHandler>());
                        return;
                    }

                    var next = new SendRequestHandler[current.Length - 1];

                    if (index > 0)
                        Array.Copy(current, 0, next, 0, index);

                    if (index < current.Length - 1)
                        Array.Copy(current, index + 1, next, index, current.Length - index - 1);

                    Volatile.Write(ref _sendHandlers, next);
                }
            }
        }

        private void InvokeSendRequest(ushort senderId, ushort targetSessionId, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod)
        {
            var handlers = Volatile.Read(ref _sendHandlers);

            for (int i = 0; i < handlers.Length; i++)
            {
                try
                {
                    handlers[i](senderId, targetSessionId, packetId, payload, deliveryMethod);
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Interpreter] OnSendRequest handler failed for packet ID {packetId}, target {targetSessionId}: {ex}");
                }
            }
        }

        #region SingleSends

        public void SendCommand<TSendStruct>(ushort targetSessionId, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            ushort defaultSender;
            var manager = LiminalNetworkManager.Instance;

            if (manager.Role == NetworkRole.Client)
            {
                defaultSender = manager.localID;
            }
            else if (manager.Role == NetworkRole.Host)
            {
                defaultSender = targetSessionId == ILiminalTransport.SERVER_ID ? manager.localID : ILiminalTransport.SERVER_ID;
            }
            else
            {
                defaultSender = ILiminalTransport.SERVER_ID;
            }

            SendCommandFrom(defaultSender, targetSessionId, packet, deliveryMethod);
        }

        public void SendCommandAsClient<TSendStruct>(ushort targetSessionId, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            SendCommandFrom(LiminalNetworkManager.Instance.localID, targetSessionId, packet, deliveryMethod);
        }

        public void SendCommandAsServer<TSendStruct>(ushort targetSessionId, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            SendCommandFrom(ILiminalTransport.SERVER_ID, targetSessionId, packet, deliveryMethod);
        }

        public void SendCommandFrom<TSendStruct>(ushort senderId, ushort targetSessionId, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TSendStruct>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send {typeof(TSendStruct).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, packet);
                InvokeSendRequest(senderId, targetSessionId, checked((ushort)idInt), writer.WrittenSpan, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} failed to send: {ex.InnerException?.Message ?? ex.Message}");
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        #endregion

        #region Multicast Sends

        public void SendCommand<TSendStruct>(ReadOnlySpan<ushort> targetSessionIds, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            ushort defaultSender;
            var manager = LiminalNetworkManager.Instance;

            if (manager.Role == NetworkRole.Client)
            {
                defaultSender = manager.localID;
            }
            else if (manager.Role == NetworkRole.Host)
            {
                bool targetsOnlyServer = targetSessionIds.Length == 1 && targetSessionIds[0] == ILiminalTransport.SERVER_ID;
                defaultSender = targetsOnlyServer ? manager.localID : ILiminalTransport.SERVER_ID;
            }
            else
            {
                defaultSender = ILiminalTransport.SERVER_ID;
            }

            SendCommandFrom(defaultSender, targetSessionIds, packet, deliveryMethod);
        }

        public void SendCommandAsServer<TSendStruct>(ReadOnlySpan<ushort> targetSessionIds, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            SendCommandFrom(ILiminalTransport.SERVER_ID, targetSessionIds, packet, deliveryMethod);
        }

        public void SendCommandAsClient<TSendStruct>(ReadOnlySpan<ushort> targetSessionIds, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            SendCommandFrom(LiminalNetworkManager.Instance.localID, targetSessionIds, packet, deliveryMethod);
        }

        public void SendCommandFrom<TSendStruct>(ushort senderId, ReadOnlySpan<ushort> targetSessionIds, TSendStruct packet, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TSendStruct : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            int idInt = LiminalPacketLibrary.GetId<TSendStruct>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send {typeof(TSendStruct).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, packet);
                ReadOnlySpan<byte> payload = writer.WrittenSpan;

                for (int i = 0; i < targetSessionIds.Length; i++)
                    InvokeSendRequest(senderId, targetSessionIds[i], packetId, payload, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} failed to send: {ex.InnerException?.Message ?? ex.Message}");
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        #endregion

        #endregion

        #region Dispatch

        public void Dispatch(ushort packetId, ushort sender, ReadOnlyMemory<byte> rawData)
        {
            if (!_handlers.TryGetValue(packetId, out var dispatcher))
            {
                LiminalLogger.LogWarning($"[Interpreter] Unhandled Packet ID {packetId}");
                return;
            }

            try
            {
                dispatcher.Dispatch(rawData, sender, _options);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Interpreter] Error in handler for ID {packetId}: {ex}");
            }
        }

        #endregion

        private readonly MessagePackSerializerOptions _options =
            MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);
    }
}