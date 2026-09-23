using Liminal.Net.Interfaces;
using MessagePack;
using System;
using System.Buffers.Binary;
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

        private readonly LiminalNetworkConfig _config;
        private readonly LiminalNetworkManager _manager;

        private interface IPacketDispatcher
        {
            void RemoveUntyped(Delegate callback);
            void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options);
            bool HasCallbacks { get; }
        }

        private sealed class TypedPacketDispatcher<T> : IPacketDispatcher
        {
            private readonly object _lock = new();

            private Action<T, ushort>[] _callbacks = Array.Empty<Action<T, ushort>>();
            private BitStreamHandler<T>[] _bitStreamHandlers = Array.Empty<BitStreamHandler<T>>();
            private BitStreamTagHandler[] _tagHandlers = Array.Empty<BitStreamTagHandler>();

            public bool HasCallbacks =>
                Volatile.Read(ref _callbacks).Length != 0 ||
                Volatile.Read(ref _bitStreamHandlers).Length != 0 ||
                Volatile.Read(ref _tagHandlers).Length != 0;

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

            public void Add(BitStreamHandler<T> handler)
            {
                lock (_lock)
                {
                    var current = _bitStreamHandlers;
                    var next = new BitStreamHandler<T>[current.Length + 1];

                    Array.Copy(current, next, current.Length);
                    next[current.Length] = handler;

                    Volatile.Write(ref _bitStreamHandlers, next);
                }
            }

            public void Add(BitStreamTagHandler handler)
            {
                lock (_lock)
                {
                    var current = _tagHandlers;
                    var next = new BitStreamTagHandler[current.Length + 1];

                    Array.Copy(current, next, current.Length);
                    next[current.Length] = handler;

                    Volatile.Write(ref _tagHandlers, next);
                }
            }

            public void RemoveUntyped(Delegate callback)
            {
                if (callback is Action<T, ushort> typedCallback)
                {
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
                else if (callback is BitStreamHandler<T> bitStreamHandler)
                {
                    lock (_lock)
                    {
                        var current = _bitStreamHandlers;
                        int index = -1;

                        for (int i = 0; i < current.Length; i++)
                        {
                            if (current[i] == bitStreamHandler)
                            {
                                index = i;
                                break;
                            }
                        }

                        if (index < 0)
                            return;

                        if (current.Length == 1)
                        {
                            Volatile.Write(ref _bitStreamHandlers, Array.Empty<BitStreamHandler<T>>());
                            return;
                        }

                        var next = new BitStreamHandler<T>[current.Length - 1];

                        if (index > 0)
                            Array.Copy(current, 0, next, 0, index);

                        if (index < current.Length - 1)
                            Array.Copy(current, index + 1, next, index, current.Length - index - 1);

                        Volatile.Write(ref _bitStreamHandlers, next);
                    }
                }
                else if (callback is BitStreamTagHandler tagHandler)
                {
                    lock (_lock)
                    {
                        var current = _tagHandlers;
                        int index = -1;

                        for (int i = 0; i < current.Length; i++)
                        {
                            if (current[i] == tagHandler)
                            {
                                index = i;
                                break;
                            }
                        }

                        if (index < 0)
                            return;

                        if (current.Length == 1)
                        {
                            Volatile.Write(ref _tagHandlers, Array.Empty<BitStreamTagHandler>());
                            return;
                        }

                        var next = new BitStreamTagHandler[current.Length - 1];

                        if (index > 0)
                            Array.Copy(current, 0, next, 0, index);

                        if (index < current.Length - 1)
                            Array.Copy(current, index + 1, next, index, current.Length - index - 1);

                        Volatile.Write(ref _tagHandlers, next);
                    }
                }
            }

            public void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options)
            {
                var callbacks = Volatile.Read(ref _callbacks);
                var bitStreamHandlers = Volatile.Read(ref _bitStreamHandlers);
                var tagHandlers = Volatile.Read(ref _tagHandlers);

                if (callbacks.Length == 0 && bitStreamHandlers.Length == 0 && tagHandlers.Length == 0)
                    return;

                T packet;
                bool hasBitStream = false;
                ReadOnlySpan<byte> bitstreamSpan = default;

                try
                {
                    var mpReader = new MessagePackReader(rawData);
                    packet = MessagePackSerializer.Deserialize<T>(ref mpReader, options);
                    int consumed = (int)mpReader.Consumed;

                    if (rawData.Length == consumed)
                    {
                        // Standard packet without bitstream payload
                        hasBitStream = false;
                    }
                    else if (rawData.Length >= consumed + 4)
                    {
                        int bitstreamLength = BinaryPrimitives.ReadInt32LittleEndian(rawData.Span.Slice(consumed, 4));
                        if (bitstreamLength < 0 || rawData.Length < consumed + 4 + bitstreamLength)
                        {
                            LiminalLogger.LogWarning($"[Security] Corrupted/cut bitstream packet {typeof(T).Name}: expected {bitstreamLength} bytes, but only {rawData.Length - (consumed + 4)} available.");
                            return;
                        }

                        bitstreamSpan = rawData.Span.Slice(consumed + 4, bitstreamLength);
                        hasBitStream = true;
                    }
                    else
                    {
                        LiminalLogger.LogWarning($"[Security] Malformed packet {typeof(T).Name}: incomplete bitstream header.");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogWarning($"[Security] Malformed packet {typeof(T).Name}: {ex.Message}");
                    return;
                }

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

                if (hasBitStream)
                {
                    for (int i = 0; i < bitStreamHandlers.Length; i++)
                    {
                        try
                        {
                            var reader = new BitReader(bitstreamSpan);
                            bitStreamHandlers[i](in packet, ref reader, sender);
                        }
                        catch (Exception ex)
                        {
                            LiminalLogger.LogError($"[Interpreter] Exception in {typeof(T).Name} bitstream handler: {ex}");
                        }
                    }

                    for (int i = 0; i < tagHandlers.Length; i++)
                    {
                        try
                        {
                            var reader = new BitReader(bitstreamSpan);
                            tagHandlers[i](ref reader, sender);
                        }
                        catch (Exception ex)
                        {
                            LiminalLogger.LogError($"[Interpreter] Exception in {typeof(T).Name} bitstream tag handler: {ex}");
                        }
                    }
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
            public bool Contains(ushort packetId, Delegate callback)
            {
                for (int i = 0; i < _list.Count; i++)
                {
                    var item = _list[i];
                    if (item.PacketId == packetId && item.Callback.Equals(callback))
                        return true;
                }

                return false;
            }

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

        public LiminalPacketInterpreter(LiminalNetworkManager manager, LiminalNetworkConfig config)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        internal LiminalNativeBufferWriter RentWriter()
        {
            if (_writerPool.TryTake(out var writer))
            {
                writer.Clear();
                return writer;
            }

            return new LiminalNativeBufferWriter(_config.Hiccup.GetRecoverySize(_config.MaxPacketSizePerBatch));
        }

        internal void ReturnWriter(LiminalNativeBufferWriter writer)
        {
            _writerPool.Add(writer);
        }

        internal void InvokeSendRequestSingle(ushort senderId, ushort targetSessionId, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod)
        {
            InvokeSendRequest(senderId, targetSessionId, packetId, payload, deliveryMethod);
        }

        internal void InvokeSendRequestMulticast(ushort senderId, ReadOnlySpan<ushort> targetSessionIds, ushort packetId, ReadOnlySpan<byte> payload, DeliveryMethod deliveryMethod)
        {
            for (int i = 0; i < targetSessionIds.Length; i++)
                InvokeSendRequest(senderId, targetSessionIds[i], packetId, payload, deliveryMethod);
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
                    if (subList.Contains(packetId, callback))
                    {
                        LiminalLogger.LogWarning($"[Interpreter] Duplicate subscription suppressed: {subscriber.GetType().Name} -> {typeof(T).Name}");
                        return;
                    }

                    var dispatcher = GetOrCreateDispatcher<T>(packetId);
                    dispatcher.Add(callback);
                    subList.Add(subscription);
                }

                LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to {typeof(T).Name} (ID: {packetId})", LiminalLogger.LogLevel.Detailed);
            }
        }

        public void SubscribeBitStream<TMeta>(BitStreamHandler<TMeta> callback, object subscriber) where TMeta : struct
        {
            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            if (subscriber == null)
                throw new ArgumentNullException(nameof(subscriber));

            int idInt = LiminalPacketLibrary.GetId<TMeta>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot subscribe to bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var subscription = new Subscription { PacketId = packetId, Callback = callback };

            lock (_subscriptionGate)
            {
                var subList = _subscribers.GetOrAdd(subscriber, _ => new SubscriptionList());

                lock (subList.Lock)
                {
                    if (subList.Contains(packetId, callback))
                    {
                        LiminalLogger.LogWarning($"[Interpreter] Duplicate bitstream subscription suppressed: {subscriber.GetType().Name} -> {typeof(TMeta).Name}");
                        return;
                    }

                    var dispatcher = GetOrCreateDispatcher<TMeta>(packetId);
                    dispatcher.Add(callback);
                    subList.Add(subscription);
                }

                LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to bitstream {typeof(TMeta).Name} (ID: {packetId})");
            }
        }

        public void SubscribeBitStream<TPacket>(BitStreamTagHandler callback, object subscriber) where TPacket : struct
        {
            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            if (subscriber == null)
                throw new ArgumentNullException(nameof(subscriber));

            int idInt = LiminalPacketLibrary.GetId<TPacket>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot subscribe to bitstream tag {typeof(TPacket).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var subscription = new Subscription { PacketId = packetId, Callback = callback };

            lock (_subscriptionGate)
            {
                var subList = _subscribers.GetOrAdd(subscriber, _ => new SubscriptionList());

                lock (subList.Lock)
                {
                    if (subList.Contains(packetId, callback))
                    {
                        LiminalLogger.LogWarning($"[Interpreter] Duplicate bitstream tag subscription suppressed: {subscriber.GetType().Name} -> {typeof(TPacket).Name}");
                        return;
                    }

                    var dispatcher = GetOrCreateDispatcher<TPacket>(packetId);
                    dispatcher.Add(callback);
                    subList.Add(subscription);
                }

                LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to bitstream tag {typeof(TPacket).Name} (ID: {packetId})");
            }
        }

        public void UnsubscribeBitStream<TMeta>(object subscriber) where TMeta : struct => Unsubscribe<TMeta>(subscriber);

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
                        _subscribers.TryRemove(subscriber, out _);
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
            var dispatcher = _handlers.GetOrAdd(packetId, _ => new TypedPacketDispatcher<T>());
            if (dispatcher is not TypedPacketDispatcher<T> typedDispatcher)
                throw new InvalidOperationException($"Packet ID {packetId} ({typeof(T).Name}) is already registered with a different dispatcher ({dispatcher.GetType().Name}).");
            return typedDispatcher;
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
            var manager = _manager;

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
            SendCommandFrom(_manager.localID, targetSessionId, packet, deliveryMethod);
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
                HandleOutboundSerializationFailure(targetSessionId, typeof(TSendStruct).Name, ex, deliveryMethod);
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
            var manager = _manager;

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
            SendCommandFrom(_manager.localID, targetSessionIds, packet, deliveryMethod);
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
                HandleOutboundSerializationFailure(targetSessionIds, typeof(TSendStruct).Name, ex, deliveryMethod);
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        #endregion

        #region BitStream Sends

        public void SendBitStream<TMeta>(ushort targetSessionId, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            ushort defaultSender;
            var manager = _manager;

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

            SendBitStreamFrom(defaultSender, targetSessionId, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamAsClient<TMeta>(ushort targetSessionId, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            SendBitStreamFrom(_manager.localID, targetSessionId, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamAsServer<TMeta>(ushort targetSessionId, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            SendBitStreamFrom(ILiminalTransport.SERVER_ID, targetSessionId, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamFrom<TMeta>(ushort senderId, ushort targetSessionId, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TMeta>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                var lenSpan = writer.GetSpan(4);
                BinaryPrimitives.WriteInt32LittleEndian(lenSpan, bitstream.Length);
                writer.Advance(4);

                if (!bitstream.IsEmpty)
                {
                    var span = writer.GetSpan(bitstream.Length);
                    bitstream.CopyTo(span);
                    writer.Advance(bitstream.Length);
                }

                InvokeSendRequest(senderId, targetSessionId, packetId, writer.WrittenSpan, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionId, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public void SendBitStream<TMeta>(ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            ushort defaultSender;
            var manager = _manager;

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

            SendBitStreamFrom(defaultSender, targetSessionIds, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamAsServer<TMeta>(ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            SendBitStreamFrom(ILiminalTransport.SERVER_ID, targetSessionIds, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamAsClient<TMeta>(ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            SendBitStreamFrom(_manager.localID, targetSessionIds, in meta, bitstream, deliveryMethod);
        }

        public void SendBitStreamFrom<TMeta>(ushort senderId, ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, ReadOnlySpan<byte> bitstream, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            int idInt = LiminalPacketLibrary.GetId<TMeta>();

            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                var lenSpan = writer.GetSpan(4);
                BinaryPrimitives.WriteInt32LittleEndian(lenSpan, bitstream.Length);
                writer.Advance(4);

                if (!bitstream.IsEmpty)
                {
                    var span = writer.GetSpan(bitstream.Length);
                    bitstream.CopyTo(span);
                    writer.Advance(bitstream.Length);
                }

                ReadOnlySpan<byte> payload = writer.WrittenSpan;

                for (int i = 0; i < targetSessionIds.Length; i++)
                    InvokeSendRequest(senderId, targetSessionIds[i], packetId, payload, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionIds, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public void SendBitStream<TMeta>(ushort targetSessionId, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            ushort defaultSender;
            var manager = _manager;

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

            SendBitStreamFrom(defaultSender, targetSessionId, in meta, packAction, deliveryMethod);
        }

        public void SendBitStreamFrom<TMeta>(ushort senderId, ushort targetSessionId, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (packAction == null)
                throw new ArgumentNullException(nameof(packAction));

            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);

                var bitWriter = new BitWriter(writer);
                packAction(ref bitWriter);
                bitWriter.Flush();

                int bitstreamLen = writer.WrittenSpan.Length - (lengthOffset + 4);
                writer.WriteInt32At(lengthOffset, bitstreamLen);

                InvokeSendRequest(senderId, targetSessionId, packetId, writer.WrittenSpan, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionId, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public void SendBitStream<TMeta, TState>(ushort targetSessionId, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            ushort defaultSender;
            var manager = _manager;

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

            SendBitStreamFrom(defaultSender, targetSessionId, in meta, in state, packAction, deliveryMethod);
        }

        public void SendBitStreamFrom<TMeta, TState>(ushort senderId, ushort targetSessionId, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (packAction == null)
                throw new ArgumentNullException(nameof(packAction));

            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);

                var bitWriter = new BitWriter(writer);
                packAction(ref bitWriter, in state);
                bitWriter.Flush();

                int bitstreamLen = writer.WrittenSpan.Length - (lengthOffset + 4);
                writer.WriteInt32At(lengthOffset, bitstreamLen);

                InvokeSendRequest(senderId, targetSessionId, packetId, writer.WrittenSpan, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionId, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public void SendBitStream<TMeta>(ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            ushort defaultSender;
            var manager = _manager;

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

            SendBitStreamFrom(defaultSender, targetSessionIds, in meta, packAction, deliveryMethod);
        }

        public void SendBitStreamFrom<TMeta>(ushort senderId, ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, BitStreamAction packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            if (packAction == null)
                throw new ArgumentNullException(nameof(packAction));

            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);

                var bitWriter = new BitWriter(writer);
                packAction(ref bitWriter);
                bitWriter.Flush();

                int bitstreamLen = writer.WrittenSpan.Length - (lengthOffset + 4);
                writer.WriteInt32At(lengthOffset, bitstreamLen);

                ReadOnlySpan<byte> payload = writer.WrittenSpan;
                for (int i = 0; i < targetSessionIds.Length; i++)
                    InvokeSendRequest(senderId, targetSessionIds[i], packetId, payload, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionIds, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public void SendBitStreamFrom<TMeta, TState>(ushort senderId, ReadOnlySpan<ushort> targetSessionIds, in TMeta meta, in TState state, BitStreamAction<TState> packAction, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            if (targetSessionIds.IsEmpty)
                return;

            if (packAction == null)
                throw new ArgumentNullException(nameof(packAction));

            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);

                var bitWriter = new BitWriter(writer);
                packAction(ref bitWriter, in state);
                bitWriter.Flush();

                int bitstreamLen = writer.WrittenSpan.Length - (lengthOffset + 4);
                writer.WriteInt32At(lengthOffset, bitstreamLen);

                ReadOnlySpan<byte> payload = writer.WrittenSpan;
                for (int i = 0; i < targetSessionIds.Length; i++)
                    InvokeSendRequest(senderId, targetSessionIds[i], packetId, payload, deliveryMethod);
            }
            catch (MessagePackSerializationException ex)
            {
                HandleOutboundSerializationFailure(targetSessionIds, typeof(TMeta).Name, ex, deliveryMethod);
            }
            finally
            {
                ReturnWriter(writer);
            }
        }

        public BitStreamScope BeginBitStreamScope<TMeta>(ushort singleTargetId, in TMeta meta, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
                throw new InvalidOperationException($"Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);
                return new BitStreamScope(_manager, writer, singleTargetId, packetId, lengthOffset, deliveryMethod);
            }
            catch
            {
                ReturnWriter(writer);
                throw;
            }
        }

        public BitStreamScope BeginBitStreamScope<TMeta>(SendTo sendToTarget, in TMeta meta, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable) where TMeta : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TMeta>();
            if (idInt == 0)
                throw new InvalidOperationException($"Cannot send bitstream {typeof(TMeta).Name}. Missing [LiminalPacket] attribute?");

            ushort packetId = checked((ushort)idInt);
            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, meta);
                int lengthOffset = writer.WrittenSpan.Length;
                writer.Advance(4);
                return new BitStreamScope(_manager, writer, sendToTarget, packetId, lengthOffset, deliveryMethod);
            }
            catch
            {
                ReturnWriter(writer);
                throw;
            }
        }

        #endregion

        #endregion

        private void HandleOutboundSerializationFailure(ushort targetSessionId, string packetName, Exception ex, DeliveryMethod deliveryMethod)
        {
            Span<ushort> targets = stackalloc ushort[1] { targetSessionId };
            HandleOutboundSerializationFailure(targets, packetName, ex, deliveryMethod);
        }

        private void HandleOutboundSerializationFailure(ReadOnlySpan<ushort> targetSessionIds, string packetName, Exception ex, DeliveryMethod deliveryMethod)
        {
            string msg = ex.InnerException?.Message ?? ex.Message;
            LiminalLogger.LogError($"[Interpreter] Packet {packetName} failed to send: {msg}");

            if (deliveryMethod == DeliveryMethod.Unreliable)
            {
                // Unreliable packets may be safely dropped without breaking determinism
                return;
            }

            LiminalLogger.LogError($"[Interpreter] Reliable packet {packetName} failed serialization. Severing connection to preserve determinism.");

            var reason = DisconnectReason.OutboundBufferOverflow;
            string reasonMsg = $"Reliable packet {packetName} exceeded buffer capacity: {msg}";

            if (_manager.Role == NetworkRole.Client)
            {
                ushort myId = _manager.localID != 0 ? _manager.localID : _manager.Transport?.LocalClientId ?? 0;
                _manager.DisconnectCoordinator?.RecordReason(myId, reason, reasonMsg);
                _manager.Disconnect();
            }
            else if (_manager.Role == NetworkRole.Server)
            {
                for (int i = 0; i < targetSessionIds.Length; i++)
                {
                    ushort targetId = targetSessionIds[i];
                    _manager.DisconnectCoordinator?.RecordReason(targetId, reason, reasonMsg);
                    _manager.Transport?.Kick(targetId);
                }
            }
            else if (_manager.Role == NetworkRole.Host)
            {
                bool containsLocalOrServer = false;
                for (int i = 0; i < targetSessionIds.Length; i++)
                {
                    ushort targetId = targetSessionIds[i];
                    if (targetId == ILiminalTransport.SERVER_ID || targetId == _manager.localID)
                    {
                        containsLocalOrServer = true;
                    }
                    else
                    {
                        _manager.DisconnectCoordinator?.RecordReason(targetId, reason, reasonMsg);
                        _manager.Transport?.Kick(targetId);
                    }
                }

                if (containsLocalOrServer)
                {
                    ushort myId = _manager.localID != 0 ? _manager.localID : _manager.Transport?.LocalClientId ?? 0;
                    _manager.DisconnectCoordinator?.RecordReason(myId, reason, reasonMsg);
                    _manager.Disconnect();
                }
            }
        }

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
            MessagePackSerializer.DefaultOptions.WithSecurity(MessagePackSecurity.UntrustedData);
    }
}
