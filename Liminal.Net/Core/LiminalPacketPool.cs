using System.Collections.Concurrent;

namespace Liminal.Net.Core
{
    public static class LiminalPacketPool
    {
        private static readonly ConcurrentQueue<InboundPacket> _pool = new();

        public static InboundPacket Rent(byte[] buffer, int length, ushort packetId)
        {
            if (_pool.TryDequeue(out var packet))
            {
                packet.Init(buffer, length, packetId);
                return packet;
            }

            var newPacket = new InboundPacket();
            newPacket.Init(buffer, length, packetId);
            return newPacket;
        }

        public static void Return(InboundPacket packet)
        {
            packet.Reset();
            _pool.Enqueue(packet);
        }
    }
}