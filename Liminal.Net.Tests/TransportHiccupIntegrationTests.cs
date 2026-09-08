using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class TransportHiccupIntegrationTests
    {
        private static int _portCounter = 7870;
        private int _currentTestPort;
        private readonly ConcurrentBag<LiminalNetworkManager> _managers = new();

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
        }

        [TearDown]
        public void TearDown()
        {
            foreach (var manager in _managers)
            {
                try { manager?.Shutdown(); }
                catch { }
            }
        }

        [Test]
        public void Test01_InboundRecovery_AcceptsPacketBeyondNormalBatchSizeAndPreservesData()
        {
            var serverConfig = CreateConfig(_currentTestPort, maxPacketSize: 256, maxPacketCount: 20, recoveryScale: 4);
            var server = StartServer(serverConfig);
            var client = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 20, 4));

            var received = new ManualResetEventSlim(false);
            byte[] expected = CreateData(700, 17);
            byte[] actual = null;

            server.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                actual = packet.Data;
                received.Set();
            }, this);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "recovery-inbound",
                Data = expected
            });
            client.SessionManager.Flush();

            Assert.That(received.Wait(3000), Is.True, "Server never received the recovery-sized packet.");
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void Test02_OutboundRecovery_PreservesAlreadyBufferedNormalFrames()
        {
            var server = StartServer(CreateConfig(_currentTestPort, 256, 20, 4));
            var client = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 20, 4));

            var messages = new ConcurrentQueue<string>();
            var received = new ManualResetEventSlim(false);
            byte[] expectedData = CreateData(700, 31);
            byte[] actualData = null;

            client.Interpreter.Subscribe<ChatPacket>((packet, _) => messages.Enqueue(packet.Message), this);
            client.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                actualData = packet.Data;
                received.Set();
            }, this);

            server.Interpreter.SendCommand(client.localID, new ChatPacket { Message = "before-recovery" });
            server.Interpreter.SendCommand(client.localID, new FilePacket
            {
                FileName = "recovery-outbound",
                Data = expectedData
            });
            server.SessionManager.Flush();

            Assert.That(received.Wait(3000), Is.True, "Client never received the recovery-sized outbound packet.");
            Assert.That(actualData, Is.EqualTo(expectedData));
            Assert.That(messages.TryPeek(out var first) && first == "before-recovery", Is.True);
        }

        [Test]
        public void Test03_InboundRecovery_PacketCountExpandsToConfiguredRecoveryLimit()
        {
            const int normalCount = 3;
            const int recoveryScale = 2;
            const int expectedCount = normalCount * recoveryScale;

            var server = StartServer(CreateConfig(_currentTestPort, 256, normalCount, recoveryScale));
            var client = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, normalCount, recoveryScale));

            var received = new ConcurrentQueue<string>();
            var completed = new ManualResetEventSlim(false);

            server.Interpreter.Subscribe<ChatPacket>((packet, _) =>
            {
                received.Enqueue(packet.Message);
                if (received.Count >= expectedCount)
                    completed.Set();
            }, this);

            for (int i = 0; i < expectedCount; i++)
            {
                client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket
                {
                    Message = $"recovery-count-{i}"
                });
            }

            client.SessionManager.Flush();

            Assert.That(completed.Wait(3000), Is.True,
                $"Only {received.Count} of {expectedCount} packets arrived before recovery count limit.");
            Assert.That(received.OrderBy(x => x).ToArray(), Is.EqualTo(
                Enumerable.Range(0, expectedCount).Select(i => $"recovery-count-{i}").OrderBy(x => x).ToArray()));
        }

        [Test]
        public void Test04_HardRecoveryCeiling_KicksWhenPeerExceedsConfiguredMaximum()
        {
            var serverConfig = CreateConfig(_currentTestPort, 256, 20, recoveryScale: 2);
            var clientConfig = CreateConfig(_currentTestPort, 256, 20, recoveryScale: 4);
            var server = StartServer(serverConfig);
            var client = StartClient(_currentTestPort, clientConfig);

            bool kicked = false;
            server.Transport.OnClientKicked += _ => kicked = true;

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "too-large-for-server",
                Data = CreateData(700, 43)
            });
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => kicked, 3000), Is.True,
                "Server did not kick the client after the recovery hard ceiling was exceeded.");
            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 3000), Is.True);
        }

        [Test]
        public void Test05_Transformers_RunThroughRecoveryPingPongWithoutCorruptingData()
        {
            var inbound1 = new RecordingCopyTransformer();
            var inbound2 = new RecordingCopyTransformer();
            var outbound1 = new RecordingCopyTransformer();
            var outbound2 = new RecordingCopyTransformer();
            var clientInbound1 = new RecordingCopyTransformer();
            var clientInbound2 = new RecordingCopyTransformer();

            var serverConfig = CreateConfig(_currentTestPort, 256, 20, 4);
            serverConfig.InboundPacketProcessors.Add(inbound1);
            serverConfig.InboundPacketProcessors.Add(inbound2);
            serverConfig.OutboundPacketProcessors.Add(outbound1);
            serverConfig.OutboundPacketProcessors.Add(outbound2);

            var clientConfig = CreateConfig(_currentTestPort, 256, 20, 4);
            clientConfig.InboundPacketProcessors.Add(clientInbound1);
            clientConfig.InboundPacketProcessors.Add(clientInbound2);
            clientConfig.OutboundPacketProcessors.Add(new RecordingCopyTransformer());
            clientConfig.OutboundPacketProcessors.Add(new RecordingCopyTransformer());

            var server = StartServer(serverConfig);
            var client = StartClient(_currentTestPort, clientConfig);

            var received = new ManualResetEventSlim(false);
            var responseReceived = new ManualResetEventSlim(false);
            byte[] expected = CreateData(700, 59);
            byte[] actual = null;
            byte[] response = CreateData(700, 67);
            byte[] responseActual = null;

            server.Interpreter.Subscribe<FilePacket>((packet, sender) =>
            {
                actual = packet.Data;
                received.Set();
                server.Interpreter.SendCommand(sender, new FilePacket
                {
                    FileName = "transform-response",
                    Data = response
                });
            }, this);

            client.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                if (packet.FileName == "transform-response")
                {
                    responseActual = packet.Data;
                    responseReceived.Set();
                }
            }, this);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "transform-recovery",
                Data = expected
            });
            client.SessionManager.Flush();

            Assert.That(received.Wait(3000), Is.True);
            Assert.That(responseReceived.Wait(3000), Is.True);
            Assert.That(actual, Is.EqualTo(expected));
            Assert.That(responseActual, Is.EqualTo(response));
            Assert.That(inbound1.Calls, Is.GreaterThan(0));
            Assert.That(inbound2.Calls, Is.GreaterThan(0));
            Assert.That(outbound1.Calls, Is.GreaterThan(0));
            Assert.That(outbound2.Calls, Is.GreaterThan(0));
            Assert.That(clientInbound1.Calls, Is.GreaterThan(0));
            Assert.That(clientInbound2.Calls, Is.GreaterThan(0));
        }

        [Test]
        public void Test06_RecoveryResources_CanBeReusedAfterRecoveryWithoutDataBleed()
        {
            var server = StartServer(CreateConfig(_currentTestPort, 256, 10, 4));
            var client = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 10, 4));

            var received = new ConcurrentQueue<byte[]>();
            var completed = new ManualResetEventSlim(false);

            server.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                received.Enqueue(packet.Data);
                if (received.Count >= 3)
                    completed.Set();
            }, this);

            byte[] first = CreateData(700, 71);
            byte[] second = CreateData(700, 97);
            byte[] normal = CreateData(80, 123);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket { FileName = "first", Data = first });
            client.SessionManager.Flush();
            Assert.That(SpinWait.SpinUntil(() => received.Count >= 1, 3000), Is.True);

            Thread.Sleep(250);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket { FileName = "second", Data = second });
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket { FileName = "normal", Data = normal });
            client.SessionManager.Flush();

            Assert.That(completed.Wait(3000), Is.True);
            var arrays = received.ToArray();
            Assert.That(arrays.Any(x => x.SequenceEqual(first)), Is.True);
            Assert.That(arrays.Any(x => x.SequenceEqual(second)), Is.True);
            Assert.That(arrays.Any(x => x.SequenceEqual(normal)), Is.True);
        }

        [Test]
        public void Test07_InboundAndOutboundRecovery_CanBeActiveTogether()
        {
            var server = StartServer(CreateConfig(_currentTestPort, 256, 20, 4));
            var client = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 20, 4));

            var clientReceived = new ManualResetEventSlim(false);
            var serverReceived = new ManualResetEventSlim(false);
            byte[] clientExpected = CreateData(700, 11);
            byte[] serverExpected = CreateData(700, 29);
            byte[] clientActual = null;
            byte[] serverActual = null;

            client.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                clientActual = packet.Data;
                clientReceived.Set();
            }, this);

            server.Interpreter.Subscribe<FilePacket>((packet, _) =>
            {
                serverActual = packet.Data;
                serverReceived.Set();
            }, this);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket { FileName = "c2s", Data = clientExpected });
            server.Interpreter.SendCommand(client.localID, new FilePacket { FileName = "s2c", Data = serverExpected });
            client.SessionManager.Flush();
            server.SessionManager.Flush();

            Assert.That(serverReceived.Wait(3000), Is.True);
            Assert.That(clientReceived.Wait(3000), Is.True);
            Assert.That(serverActual, Is.EqualTo(clientExpected));
            Assert.That(clientActual, Is.EqualTo(serverExpected));
        }

        [Test]
        public void Test08_HealthyEndpoint_ExhaustingGraceCount_KicksOnNextRecoveryAttempt()
        {
            var serverConfig = CreateConfig(_currentTestPort, 256, 10, 4);
            serverConfig.Hiccup.GraceCount = 1;
            serverConfig.Hiccup.GraceWindowSeconds = 10f;
            serverConfig.Hiccup.RecoveryHoldSeconds = 0.05f;
            serverConfig.Hiccup.CooldownSeconds = 0f;

            var clientConfig = CreateConfig(_currentTestPort, 256, 10, 4);
            clientConfig.Hiccup.GraceCount = 10;

            var server = StartServer(serverConfig);
            var client = StartClient(_currentTestPort, clientConfig);
            bool kicked = false;
            server.Transport.OnClientKicked += _ => kicked = true;

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "first",
                Data = CreateData(700, 151)
            });
            client.SessionManager.Flush();

            Thread.Sleep(250);

            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "second",
                Data = CreateData(700, 173)
            });
            client.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => kicked, 3000), Is.True,
                "Server did not kick after the isolated recovery grace count was exhausted.");
        }

        [Test]
        public void Test09_ServerStarvation_AtFiftyPercent_BypassesIsolatedSuppression()
        {
            var serverConfig = CreateConfig(_currentTestPort, 256, 10, 4);
            serverConfig.MaxConnectionCount = 2;

            serverConfig.Hiccup.GraceCount = 1;
            serverConfig.Hiccup.RecoveryHoldSeconds = 2.0f;

            var server = StartServer(serverConfig);
            var client1 = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 10, 4));
            var client2 = StartClient(_currentTestPort, CreateConfig(_currentTestPort, 256, 10, 4));

            var client1Recovered = new ManualResetEventSlim(false);
            var starvation = new ManualResetEventSlim(false);

            server.Interpreter.Subscribe<FilePacket>((packet, sender) =>
            {
                if (packet.FileName == "starvation-1")
                    client1Recovered.Set();
            }, this);

            server.SessionManager.OnServerStarvation += _ => starvation.Set();

            client1.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "starvation-1",
                Data = CreateData(700, 191)
            });
            client1.SessionManager.Flush();

            Assert.That(client1Recovered.Wait(3000), Is.True, "Client 1 failed to deliver recovery packet.");

            client2.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "burn-grace-c2",
                Data = CreateData(700, 205)
            });
            client2.SessionManager.Flush();

            client2.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new FilePacket
            {
                FileName = "starvation-2",
                Data = CreateData(700, 211)
            });
            client2.SessionManager.Flush();

            Assert.That(starvation.Wait(3000), Is.True,
                "ServerStarvationEvent was not raised at the configured 50% recovery ratio.");
            Assert.That(server.Transport.ConnectedClientCount, Is.EqualTo(2),
                "Client 2 was kicked instead of bypassing suppression via server starvation.");
        }

        [Test]
        public void Test10_ClientHardCeiling_RejectsOversizedInboundRecovery()
        {
            var server = StartServer(CreateConfig(_currentTestPort, 256, 10, 4));
            var clientConfig = CreateConfig(_currentTestPort, 256, 10, 2);
            var client = StartClient(_currentTestPort, clientConfig);

            server.Interpreter.SendCommand(client.localID, new FilePacket
            {
                FileName = "too-large-for-client",
                Data = CreateData(700, 223)
            });
            server.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 3000), Is.True,
                "Client did not disconnect after receiving a payload above its recovery ceiling.");
        }

        [Test]
        public void Test11_ClientHealthyEndpoint_GraceCountIsEnforcedForRepeatedRecovery()
        {
            var server = StartServer(CreateConfig(_currentTestPort, 256, 10, 4));
            var clientConfig = CreateConfig(_currentTestPort, 256, 10, 4);
            clientConfig.Hiccup.GraceCount = 1;
            clientConfig.Hiccup.GraceWindowSeconds = 10f;
            clientConfig.Hiccup.RecoveryHoldSeconds = 0.05f;
            clientConfig.Hiccup.CooldownSeconds = 0f;
            var client = StartClient(_currentTestPort, clientConfig);

            server.Interpreter.SendCommand(client.localID, new FilePacket
            {
                FileName = "first-client-recovery",
                Data = CreateData(700, 239)
            });
            server.SessionManager.Flush();

            Thread.Sleep(250);

            server.Interpreter.SendCommand(client.localID, new FilePacket
            {
                FileName = "second-client-recovery",
                Data = CreateData(700, 251)
            });
            server.SessionManager.Flush();

            Assert.That(SpinWait.SpinUntil(() => !client.Transport.IsConnected, 3000), Is.True,
                "Client did not enforce its local recovery grace count.");
        }

        private LiminalNetworkManager StartServer(LiminalTransportConfig config)
        {
            var manager = new LiminalNetworkManager(new TcpTransport(), config);
            _managers.Add(manager);
            manager.StartServer(config.Default_Host, config.Default_Port);
            Assert.That(SpinWait.SpinUntil(() => manager.Transport.IsConnected, 2000), Is.True);
            return manager;
        }

        private LiminalNetworkManager StartClient(int port, LiminalTransportConfig config)
        {
            config.Default_Port = port;
            var manager = new LiminalNetworkManager(new TcpTransport(), config);
            _managers.Add(manager);
            manager.StartClient(config.Default_Host, port);
            Assert.That(SpinWait.SpinUntil(() => manager.Transport.IsConnected, 3000), Is.True);
            return manager;
        }

        private static LiminalTransportConfig CreateConfig(int port, int maxPacketSize, int maxPacketCount, int recoveryScale)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = port,
                TickRate = 60,
                MaxPacketSizePerBatch = (ushort)maxPacketSize,
                MaxPacketCount = maxPacketCount,
                MaxConnectionCount = 10,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            config.Hiccup.MaxRecoveryScale = recoveryScale;
            config.Hiccup.GraceCount = 10;
            config.Hiccup.RecoveryHoldSeconds = 0.10f;
            config.Hiccup.GraceWindowSeconds = 10.0f;
            config.Hiccup.CooldownSeconds = 0f;
            return config;
        }

        private static byte[] CreateData(int length, byte seed)
        {
            var data = new byte[length];
            for (int i = 0; i < data.Length; i++)
                data[i] = unchecked((byte)(seed + i * 17));
            return data;
        }

        private sealed class RecordingCopyTransformer : ILiminalInboundTransformer, ILiminalOutboundTransformer
        {
            private int _calls;
            public int Calls => Volatile.Read(ref _calls);

            public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                Interlocked.Increment(ref _calls);
                input.CopyTo(output);
                return input.Length;
            }

            public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
            {
                Interlocked.Increment(ref _calls);
                input.CopyTo(output);
                return input.Length;
            }
        }
    }
}
