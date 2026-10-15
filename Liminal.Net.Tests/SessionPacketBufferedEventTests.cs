using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using NUnit.Framework;
using System.Collections.Generic;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SessionPacketBufferedEventTests
    {
        //Reports the buffered frame size, length prefix and packet id included, and never the frame's memory
        [Test]
        public void OnPacketBuffered_ReportsFrameSize_ForReliableAndUnreliable()
        {
            var config = new LiminalNetworkConfig { TickRate = 60, ClientIdResolver = new BaseResolver() };
            var transport = new MockTransport();
            var netManager = new LiminalNetworkManager(transport, config);
            netManager.SyncVarManager?.DetachFromManager();
            var manager = netManager.SessionManager;
            manager.InitializeConfig(new LiminalTelemetryConfig { Flags = TelemetryFlags.PacketCounting, EnablePacketCounting = true });
            transport.TriggerClientConnected(77);

            var reported = new List<(ushort id, int size)>();
            manager.OnPacketBuffered += (id, size) => reported.Add((id, size));
            try
            {
                manager.BufferPacket(transport.LocalClientId, 77, 100, new byte[16], DeliveryMethod.Reliable);
                manager.BufferPacket(transport.LocalClientId, 77, 101, new byte[40], DeliveryMethod.Unreliable);

                Assert.That(reported, Is.EqualTo(new[] { ((ushort)100, 6 + 16), ((ushort)101, 6 + 40) }));
            }
            finally { netManager.Shutdown(); }
        }
    }
}
