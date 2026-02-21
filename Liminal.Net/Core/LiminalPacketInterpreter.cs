using MessagePack;
using System.Collections.Concurrent;

namespace Liminal.Net.Core
{
    public class LiminalPacketInterpreter
    {
        private readonly ConcurrentBag<LiminalNativeBufferWriter> _writerPool = new();

        private readonly ConcurrentDictionary<ushort, PacketEvent> _handlers = new();

        private readonly ConcurrentDictionary<object, SubscriptionList> _subscribers = new();

        private readonly LiminalTransportConfig _config;

        private class PacketEvent
        {
            public Action<byte[], ushort> Handler;

            // A tiny lock just for modifying this specific packets subscription list
            private readonly object _lock = new();

            public void Add(Action<byte[], ushort> action)
            {
                lock (_lock) { Handler += action; }
            }

            public void Remove(Action<byte[], ushort> action)
            {
                lock (_lock) { Handler -= action; }
            }
        }

        private struct Subscription
        {
            public ushort PacketId;
            public Action<byte[], ushort> Wrapper;
        }

        private class SubscriptionList
        {
            private readonly List<Subscription> _list = new();
            private readonly object _lock = new();
            public bool IsDisposed { get; private set; } = false;

            public bool TryAdd(Subscription sub)
            {
                lock (_lock)
                {
                    if (IsDisposed) return false;
                    _list.Add(sub);
                    return true;
                }
            }

            public List<Subscription> DisposeAndClear()
            {
                lock (_lock)
                {
                    IsDisposed = true;
                    var copy = new List<Subscription>(_list);
                    _list.Clear();
                    return copy;
                }
            }

            public List<Action<byte[], ushort>> RemoveAndGetWrappers(ushort packetId)
            {
                var removedWrappers = new List<Action<byte[], ushort>>();
                lock (_lock)
                {
                    if (IsDisposed) return removedWrappers;

                    for (int i = _list.Count - 1; i >= 0; i--)
                    {
                        if (_list[i].PacketId == packetId)
                        {
                            removedWrappers.Add(_list[i].Wrapper);
                            _list.RemoveAt(i);
                        }
                    }
                }
                return removedWrappers;
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
            if (idInt == -1)
            {
                LiminalLogger.LogError($"[Interpreter] Cannot subscribe to {typeof(T).Name}. Missing [LiminalPacket] attribute?");
                return;
            }

            ushort packetId = (ushort)idInt;

            Action<byte[], ushort> wrapper = (byte[] rawData, ushort sender) =>
            {
                T packet = DeserializeSafe<T>(rawData, out bool success);
                if (success) callback(packet, sender);
            };

            var packetEvent = _handlers.GetOrAdd(packetId, _ => new PacketEvent());
            packetEvent.Add(wrapper);

            int maxRetries = 100;
            int attempts = 0;

            while (attempts < maxRetries)
            {
                var subList = _subscribers.GetOrAdd(subscriber, _ => new SubscriptionList());

                if (subList.TryAdd(new Subscription { PacketId = packetId, Wrapper = wrapper }))
                {
                    LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to {typeof(T).Name} (ID: {packetId})");
                    return;
                }

                attempts++;
            }

            if (attempts >= maxRetries)
            {
                RemoveFromHandlers(packetId, wrapper);
                LiminalLogger.LogError($"[Interpreter] Failed to subscribe {subscriber.GetType().Name} to {typeof(T).Name} after {maxRetries} attempts!");
            }
        }

        public void Unsubscribe<T>(object subscriber)
        {
            int idInt = LiminalPacketLibrary.GetId<T>();
            if (idInt == -1) return;
            ushort packetId = (ushort)idInt;

            if (_subscribers.TryGetValue(subscriber, out var subList))
            {
                var wrappersToRemove = subList.RemoveAndGetWrappers(packetId);
                foreach (var wrapper in wrappersToRemove)
                {
                    RemoveFromHandlers(packetId, wrapper);
                }
            }
        }

        public void UnsubscribeAll(object subscriber)
        {
            if (_subscribers.TryRemove(subscriber, out var subList))
            {
                var subscriptions = subList.DisposeAndClear();

                foreach (var sub in subscriptions)
                {
                    RemoveFromHandlers(sub.PacketId, sub.Wrapper);
                }

                LiminalLogger.Log($"[Interpreter] Unsubscribed all handlers for {subscriber.GetType().Name}");
            }
        }

        public void ClearAllHandlers()
        {
            _handlers.Clear();
            _subscribers.Clear();
        }

        private void RemoveFromHandlers(ushort packetId, Action<byte[], ushort> wrapper)
        {
            if (_handlers.TryGetValue(packetId, out var packetEvent))
            {
                packetEvent.Remove(wrapper);
            }
        }

        public event Action<ushort, ushort, ReadOnlySpan<byte>> OnSendRequest;

        public void SendCommand<TSendStruct>(ushort targetSessionId, TSendStruct packet) where TSendStruct : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TSendStruct>();
            if (idInt == -1) return;

            var writer = RentWriter();

            try
            {
                MessagePackSerializer.Serialize(writer, packet);
                OnSendRequest?.Invoke(targetSessionId, (ushort)idInt, writer.WrittenSpan);
            }
            // MessagePackSerializer wraps the OutOfMemoryException
            catch (MessagePackSerializationException ex)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} failed to send! {ex.InnerException?.Message}");
            }
            finally
            {
                _writerPool.Add(writer);
            }
        }

        public void Dispatch(ushort packetId, ushort sender, byte[] rawData)
        {
            if (_handlers.TryGetValue(packetId, out var packetEvent))
            {
                var currentHandler = packetEvent.Handler;

                if (currentHandler != null)
                {
                    try
                    {
                        currentHandler(rawData, sender);
                    }
                    catch (Exception ex)
                    {
                        LiminalLogger.LogError($"[Interpreter] Error in handler for ID {packetId}: {ex}");
                    }
                    return;
                }
            }

            LiminalLogger.LogWarning($"[Interpreter] Unhandled Packet ID {packetId}");
        }

        private T DeserializeSafe<T>(byte[] data, out bool success)
        {
            try
            {
                var options = MessagePackSerializerOptions.Standard
                    .WithSecurity(MessagePackSecurity.UntrustedData);

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
}