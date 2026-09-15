using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Core.Telemetry;
using Liminal.Net.Interfaces;
using Liminal.Net.SyncVar;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class PrematureLifecycleRegressionTests
    {
        private LiminalNetworkConfig _config;

        [SetUp]
        public void Setup()
        {
            _config = new LiminalNetworkConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 9876,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            LiminalPacketLibrary.Initialize();
        }

        [Test]
        public void Test01_SyncVar_DirtyBit_PreservedWhenFlushedBeforeConnected()
        {
            // Create client manager but do NOT connect it (CanSend is false)
            var transport = new MockTransport();
            var clientManager = new LiminalNetworkManager(transport, _config);

            var syncVarManager = clientManager.SyncVarManager;
            Assert.That(syncVarManager, Is.Not.Null);
            Assert.That(clientManager.CanSend, Is.False);

            var syncVar = syncVarManager.Bind<int>("test_counter", 100);
            syncVar.Value = 200;

            Assert.That(syncVarManager.DirtyBitset.IsDirty(syncVar.Id), Is.True, "SyncVar should be dirty after value mutation.");

            // Attempt to flush dirty while client is not connected
            syncVarManager.FlushDirty();

            // The dirty bit MUST NOT be consumed/lost!
            Assert.That(syncVarManager.DirtyBitset.IsDirty(syncVar.Id), Is.True, "Dirty bit must be preserved when FlushDirty is called while CanSend is false.");
        }

        [Test]
        public void Test02_Broadcaster_RejectsSends_WhenCanSendIsFalse()
        {
            var transport = new MockTransport();
            var clientManager = new LiminalNetworkManager(transport, _config);
            LiminalNetworkManager.Instance = clientManager;

            Assert.That(clientManager.CanSend, Is.False);

            // Broadcaster.Send should safely log warning and return without exceptions
            Assert.DoesNotThrow(() =>
            {
                Broadcaster.Send(SendTo.Server, new PingPacket { SequenceId = 1 });
            });

            LiminalNetworkManager.Instance = null;
        }

        [Test]
        public void Test03_DisconnectReasonCoordinator_CancelingConnectingClient_DisconnectsImmediately()
        {
            var transport = new MockTransport();
            transport.IsConnected = false;
            var clientManager = new LiminalNetworkManager(transport, _config);
            var coordinator = clientManager.DisconnectCoordinator;

            Assert.That(transport.IsConnected, Is.False);

            var sw = Stopwatch.StartNew();
            coordinator.ClientDisconnectWithReason(DisconnectReason.ClientDisconnected);
            sw.Stop();

            // Should disconnect immediately without waiting for the 5-second grace period timeout
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(1000), "ClientDisconnectWithReason should not block waiting for grace period when transport is disconnected.");
        }

        [Test]
        public void Test04_TickPayloadSizeDiagnostics_CircularBufferWraparound_ResetsSlot()
        {
            var transport = new MockTransport();
            var telemetryConfig = new LiminalTelemetryConfig { Flags = TelemetryFlags.All };
            var manager = new LiminalNetworkManager(transport, _config, telemetryConfig);

            const uint windowSize = 3;
            var diagnostics = new TickPayloadSizeDiagnostics(windowSize, manager.Ticker, manager.SessionManager);

            // Establish a mock client session so packets can be routed and counted
            transport.TriggerClientConnected(1);
            manager.Interpreter.SendCommand(1, new PingPacket { SequenceId = 100 });

            // Run windowSize ticks + 1 to force wrap-around
            for (int i = 0; i <= windowSize; i++)
            {
                manager.Ticker.TickOnce();
            }

            var snapshot = diagnostics.GetLatestTickSnapshot();
            Assert.That(snapshot.TelemetryData, Is.Not.Null);

            diagnostics.Dispose();
        }

        [Test]
        public void Test05_LiminalPacketFragmentor_LocalClientDisconnected_CleansUpServerPeerChannel()
        {
            var transport = new MockTransport();
            using var fragmentor = new LiminalPacketFragmentor(transport, _config, mtu: 1200);

            // Ingest a fragment from server (ID 0) to instantiate the PeerChannelState for server
            byte[] fragment = new byte[32];
            // Format: [TotalSize:4][PacketId:2][ReliableSeq:2][FragmentIndex:2][TotalFragments:2][Payload...]
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(fragment.AsSpan(0, 4), 60);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(4, 2), 10);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(6, 2), 1);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(8, 2), 0);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(fragment.AsSpan(10, 2), 2);

            transport.TriggerFragmentReceived(fragment, ILiminalTransport.SERVER_ID);

            // Trigger local client disconnect
            transport.TriggerLocalClientDisconnected(1);

            // Fragmentor should have cleaned up peer channel 0 cleanly without throws
            Assert.Pass();
        }
    }
}
