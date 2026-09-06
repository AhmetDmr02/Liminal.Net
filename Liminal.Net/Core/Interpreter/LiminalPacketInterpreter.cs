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
        private readonly LiminalTransportConfig _config;

        private interface IPacketDispatcher
        {
            void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options);
            void RemoveUntyped(Delegate callback);
        }

        private sealed class TypedPacketDispatcher<T> : IPacketDispatcher
        {
            private Action<T, ushort> _callbacks;
            private readonly object _lock = new();

            public void Add(Action<T, ushort> callback)
            {
                lock (_lock) { _callbacks += callback; }
            }

            public void Remove(Action<T, ushort> callback)
            {
                lock (_lock) { _callbacks -= callback; }
            }

            public void RemoveUntyped(Delegate callback)
            {
                if (callback is Action<T, ushort> typedCallback)
                {
                    Remove(typedCallback);
                }
            }

            public void Dispatch(ReadOnlyMemory<byte> rawData, ushort sender, MessagePackSerializerOptions options)
            {
                Action<T, ushort> targets;
                lock (_lock) { targets = _callbacks; }

                if (targets == null) return;

                T packet = DeserializeSafe(rawData, options, out bool success);
                if (success)
                {
                    targets(packet, sender);
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
            public bool IsDisposed = false;

            public int Count => _list.Count;

            public void Add(Subscription sub)
            {
                _list.Add(sub);
            }

            public List<Subscription> DisposeAndClear()
            {
                IsDisposed = true;
                if (_list.Count == 0) return null;
                var copy = new List<Subscription>(_list);
                _list.Clear();
                return copy;
            }

            public List<Delegate> RemoveAndGetCallbacks(ushort packetId)
            {
                var removedCallbacks = new List<Delegate>();
                if (IsDisposed) return removedCallbacks;

                for (int i = _list.Count - 1; i >= 0; i--)
                {
                    if (_list[i].PacketId == packetId)
                    {
                        removedCallbacks.Add(_list[i].Callback);
                        _list.RemoveAt(i);
                    }
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
            return new LiminalNativeBufferWriter(_config.MaxPacketSizePerBatch);
        }

        public void Subscribe<T>(Action<T, ushort> callback, object subscriber)
        {
            int idInt = LiminalPacketLibrary.GetId<T>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot subscribe to {typeof(T).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = (ushort)idInt;
            var subscription = new Subscription { PacketId = packetId, Callback = callback };
            var spin = new SpinWait();

            while (true)
            {
                var subList = _subscribers.GetOrAdd(subscriber, _ => new SubscriptionList());

                lock (subList.Lock)
                {
                    if (!subList.IsDisposed)
                    {
                        var dispatcher = (TypedPacketDispatcher<T>)_handlers.GetOrAdd(packetId, _ => new TypedPacketDispatcher<T>());
                        dispatcher.Add(callback);

                        subList.Add(subscription);

                        LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to {typeof(T).Name} (ID: {packetId})");
                        return;
                    }
                }

                // If subList was disposed by UnsubscribeAll or an empty Unsubscribe<T>,
                // evict the dead instance so the next GetOrAdd generates a fresh one.
                var entry = new KeyValuePair<object, SubscriptionList>(subscriber, subList);
                ((ICollection<KeyValuePair<object, SubscriptionList>>)_subscribers).Remove(entry);

                spin.SpinOnce();
            }
        }

        public void Unsubscribe<T>(object subscriber)
        {
            int idInt = LiminalPacketLibrary.GetId<T>();
            if (idInt == 0)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot unsubscribe from {typeof(T).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = (ushort)idInt;

            if (_subscribers.TryGetValue(subscriber, out var subList))
            {
                List<Delegate> callbacksToRemove;
                lock (subList.Lock)
                {
                    callbacksToRemove = subList.RemoveAndGetCallbacks(packetId);

                    // If all subscriptions are gone, mark disposed and evict under lock
                    if (subList.Count == 0)
                    {
                        subList.IsDisposed = true;

                        var entry = new KeyValuePair<object, SubscriptionList>(subscriber, subList);
                        ((ICollection<KeyValuePair<object, SubscriptionList>>)_subscribers).Remove(entry);
                    }
                }

                foreach (var cb in callbacksToRemove)
                {
                    RemoveFromHandlers(packetId, cb);
                }
            }
        }

        public void UnsubscribeAll(object subscriber)
        {
            if (_subscribers.TryRemove(subscriber, out var subList))
            {
                List<Subscription> subscriptions;
                lock (subList.Lock)
                {
                    subscriptions = subList.DisposeAndClear();
                }

                if (subscriptions != null)
                {
                    foreach (var sub in subscriptions)
                    {
                        RemoveFromHandlers(sub.PacketId, sub.Callback);
                    }
                }

                LiminalLogger.Log($"[Interpreter] Unsubscribed all handlers for {subscriber.GetType().Name}");
            }
        }

        public void ClearAllHandlers()
        {
            _handlers.Clear();
            _subscribers.Clear();
        }

        private void RemoveFromHandlers(ushort packetId, Delegate callback)
        {
            if (_handlers.TryGetValue(packetId, out var dispatcher))
            {
                dispatcher.RemoveUntyped(callback);
            }
        }

#if NET9_0_OR_GREATER
        public event Action<ushort, ushort, ReadOnlySpan<byte>> OnSendRequest;
#else
        public delegate void SendRequestHandler(ushort targetSessionId, ushort packetId, ReadOnlySpan<byte> payload);
        public event SendRequestHandler OnSendRequest;
#endif

        public void SendCommand<TSendStruct>(ushort targetSessionId, TSendStruct packet) where TSendStruct : struct
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
                OnSendRequest?.Invoke(targetSessionId, (ushort)idInt, writer.WrittenSpan);
            }
            catch (MessagePackSerializationException ex)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} failed to send! {ex.InnerException?.Message}");
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        public void SendCommand<TSendStruct>(ReadOnlySpan<ushort> targetSessionIds, TSendStruct packet) where TSendStruct : struct
        {
            if (targetSessionIds.IsEmpty) return;

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
                var payload = writer.WrittenSpan;

                for (int i = 0; i < targetSessionIds.Length; i++)
                {
                    OnSendRequest?.Invoke(targetSessionIds[i], (ushort)idInt, payload);
                }
            }
            catch (MessagePackSerializationException ex)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} failed to send! {ex.InnerException?.Message}");
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        public void Dispatch(ushort packetId, ushort sender, ReadOnlyMemory<byte> rawData)
        {
            if (_handlers.TryGetValue(packetId, out var dispatcher))
            {
                try
                {
                    dispatcher.Dispatch(rawData, sender, _options);
                    return;
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Interpreter] Error in handler for ID {packetId}: {ex}");
                    return;
                }
            }

            LiminalLogger.LogWarning($"[Interpreter] Unhandled Packet ID {packetId}");
        }

        private readonly MessagePackSerializerOptions _options = MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);
    }
}