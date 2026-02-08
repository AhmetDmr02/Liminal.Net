using MessagePack;

namespace Liminal.Net.Core
{
    public class LiminalPacketInterpreter
    {
        [ThreadStatic]
        private static LiminalNativeBufferWriter _nativeWriter;

        private readonly Dictionary<ushort, Action<byte[], ushort>> _handlers = new();
        private readonly Dictionary<object, List<Subscription>> _subscribers = new();

        private readonly LiminalTransportConfig _config;
        private struct Subscription
        {
            public ushort PacketId;
            public Action<byte[], ushort> Wrapper;
        }

        public LiminalPacketInterpreter(LiminalTransportConfig config)
        {
            _config = config;
        }

        private LiminalNativeBufferWriter GetThreadWriter(int maxPacketSize)
        {
            if (_nativeWriter == null)
            {
                _nativeWriter = new LiminalNativeBufferWriter(maxPacketSize);
            }

            _nativeWriter.Clear();
            return _nativeWriter;
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

            if (_handlers.TryGetValue(packetId, out var existingHandler))
            {
                _handlers[packetId] = existingHandler + wrapper;
            }
            else
            {
                _handlers[packetId] = wrapper;
            }

            if (!_subscribers.TryGetValue(subscriber, out var subList))
            {
                subList = new List<Subscription>();
                _subscribers[subscriber] = subList;
            }

            subList.Add(new Subscription { PacketId = packetId, Wrapper = wrapper });

            LiminalLogger.Log($"[Interpreter] {subscriber.GetType().Name} subscribed to {typeof(T).Name} (ID: {packetId})");
        }

        public void Unsubscribe<T>(object subscriber)
        {
            int idInt = LiminalPacketLibrary.GetId<T>();
            if (idInt == -1) return;
            ushort packetId = (ushort)idInt;

            if (_subscribers.TryGetValue(subscriber, out var subList))
            {
                for (int i = subList.Count - 1; i >= 0; i--)
                {
                    if (subList[i].PacketId == packetId)
                    {
                        var wrapperToRemove = subList[i].Wrapper;
                        RemoveFromHandlers(packetId, wrapperToRemove);
                        subList.RemoveAt(i);
                    }
                }
            }
        }

        public void UnsubscribeAll(object subscriber)
        {
            if (_subscribers.TryGetValue(subscriber, out var subList))
            {
                foreach (var sub in subList)
                {
                    RemoveFromHandlers(sub.PacketId, sub.Wrapper);
                }

                _subscribers.Remove(subscriber);
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
            if (_handlers.TryGetValue(packetId, out var handlerChain))
            {
                var newChain = (Action<byte[], ushort>)Delegate.Remove(handlerChain, wrapper);

                if (newChain == null)
                {
                    _handlers.Remove(packetId);
                }
                else
                {
                    _handlers[packetId] = newChain;
                }
            }
        }

        public event Action<ushort, ushort, ReadOnlySpan<byte>> OnSendRequest;
        public void SendCommand<TSendStruct>(ushort targetSessionId, TSendStruct packet) where TSendStruct : struct
        {
            int idInt = LiminalPacketLibrary.GetId<TSendStruct>();
            if (idInt == -1) return;

            var writer = GetThreadWriter(_config.MaxPacketSizePerBatch);

            try
            {
                MessagePackSerializer.Serialize(writer, packet);

                OnSendRequest?.Invoke(targetSessionId, (ushort)idInt, writer.WrittenSpan);
            }
            catch (OutOfMemoryException)
            {
                LiminalLogger.LogError($"[Interpreter] Packet {typeof(TSendStruct).Name} is too large to send!");
            }
        }

        public void Dispatch(ushort packetId, ushort sender, byte[] rawData)
        {
            if (_handlers.TryGetValue(packetId, out var handlerAction))
            {
                try
                {
                    handlerAction(rawData, sender);
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Interpreter] Error in handler for ID {packetId}: {ex}");
                }
            }
            else
            {
                LiminalLogger.LogWarning($"[Interpreter] Unhandled Packet ID {packetId}");
            }
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