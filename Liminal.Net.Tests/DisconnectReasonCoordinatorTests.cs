using Liminal.Net.BasePackets;
using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using MessagePack;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class DisconnectReasonCoordinatorTests
    {
        private LiminalNetworkManager _serverManager;
        private ConcurrentBag<LiminalNetworkManager> _clientManagers;
        private LiminalTransportConfig _serverConfig;

        // Prevent port exhaustion between tests
        private static int _portCounter = 7910;
        private int _currentTestPort;

        [SetUp]
        public void Setup()
        {
            _currentTestPort = Interlocked.Increment(ref _portCounter);
            _clientManagers = new();

            _serverConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 15,
                HandshakeTimeout = 15
            };

            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
        }

        [TearDown]
        public void Teardown()
        {
            foreach (var client in _clientManagers)
            {
                client?.Shutdown();
            }
            _serverManager?.Shutdown();
        }

        private LiminalNetworkManager CreateAndStartClient(LiminalTelemetryConfig telemetryConfig = null)
        {
            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                HandshakeTimeout = 15,
                ConnectionTimeout = 15
            };
            var client = new LiminalNetworkManager(new TcpTransport(), config, telemetryConfig);
            _clientManagers.Add(client);
            client.StartClient("127.0.0.1", _currentTestPort);
            return client;
        }

        #region DisconnectReasonCoordinator Scenarios & Teardown Stress Tests 

        private static bool PollUntil(Func<bool> condition, LiminalNetworkManager server, LiminalNetworkManager client, int timeoutMs = 3000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (server?.SessionManager != null)
                {
                    server.SessionManager.Poll();
                    server.SessionManager.Flush();
                }

                if (client?.SessionManager != null)
                {
                    client.SessionManager.Poll();
                    client.SessionManager.Flush();
                }

                if (condition()) return true;

                Thread.Sleep(5);
            }
            return false;
        }

        [Test]
        public void Test44_DisconnectReason_Scenario1_ClientInitiatedDisconnect_ResolvesGracefullyWithAck()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;
            string serverResolvedMessage = null;
            bool serverResolved = false;

            DisconnectReason clientResolvedReason = DisconnectReason.Unknown;
            string clientResolvedMessage = null;
            bool clientResolved = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverResolvedReason = reason;
                    serverResolvedMessage = msg;
                    serverResolved = true;
                }
            };

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientResolvedReason = reason;
                clientResolvedMessage = msg;
                clientResolved = true;
            };

            // Client enqueues disconnect notice
            client.DisconnectCoordinator.ClientDisconnectWithReason(DisconnectReason.ClientDisconnected, "PlayerQuitCleanly");

            bool resolved = PollUntil(() => serverResolved && clientResolved, _serverManager, client, 3000);
            Assert.That(resolved, Is.True, "Handshake ACK or termination timed out between client and server.");

            Assert.Multiple(() =>
            {
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ClientDisconnected));
                Assert.That(serverResolvedMessage, Is.EqualTo("PlayerQuitCleanly"));
                Assert.That(clientResolvedReason, Is.EqualTo(DisconnectReason.ClientDisconnected));
                Assert.That(clientResolvedMessage, Is.EqualTo("PlayerQuitCleanly"));
                Assert.That(client.Transport.IsConnected, Is.False);
            });
        }

        [Test]
        public void Test45_DisconnectReason_Scenario2_ServerKick_UnblocksEarlyOnAck()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason clientReceivedReason = DisconnectReason.Unknown;
            string clientReceivedMessage = null;
            bool clientResolved = false;

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientReceivedReason = reason;
                clientReceivedMessage = msg;
                clientResolved = true;
            };

            var sw = System.Diagnostics.Stopwatch.StartNew();

            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.Kicked, "RuleViolation_SpeedHack", graceSeconds: 10);

            bool kicked = PollUntil(() => clientResolved && !client.Transport.IsConnected, _serverManager, client, 3000);
            sw.Stop();

            Assert.That(kicked, Is.True, "Client was not kicked within early ACK timeframe.");
            Assert.Multiple(() =>
            {
                Assert.That(sw.ElapsedMilliseconds, Is.LessThan(4000), "Kick stalled for full grace duration instead of unblocking on ACK.");
                Assert.That(clientReceivedReason, Is.EqualTo(DisconnectReason.Kicked));
                Assert.That(clientReceivedMessage, Is.EqualTo("RuleViolation_SpeedHack"));
                Assert.That(_serverManager.Transport.IsClientConnected(clientId), Is.False);
            });
        }

        [Test]
        public void Test46_DisconnectReason_Scenario3_UnresponsiveClient_FallsBackToGraceTimeout()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            client.Interpreter.UnsubscribeAll(client.DisconnectCoordinator);

            bool serverResolved = false;
            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverResolvedReason = reason;
                    serverResolved = true;
                }
            };

            var sw = System.Diagnostics.Stopwatch.StartNew();

            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.ServerShuttingDown, "Rebooting", graceSeconds: 1);

            bool resolved = PollUntil(() => serverResolved, _serverManager, client, 4000);
            sw.Stop();

            Assert.That(resolved, Is.True, "Server failed to kick unresponsive client after timeout.");
            Assert.Multiple(() =>
            {
                Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(950), "Server did not wait for the configured grace timeout.");
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ServerShuttingDown));
                Assert.That(_serverManager.Transport.IsClientConnected(clientId), Is.False);
            });
        }

        [Test]
        public void Test47_DisconnectReason_Scenario4_PhysicalDrop_FallsBackToConnectionLost()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            DisconnectReason serverDetectedReason = DisconnectReason.Unknown;
            bool serverFired = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                if (id == clientId)
                {
                    serverDetectedReason = reason;
                    serverFired = true;
                }
            };

            ((TcpTransport)client.Transport).Disconnect();

            bool fired = PollUntil(() => serverFired, _serverManager, client, 2000);

            Assert.That(fired, Is.True, "Server missed abrupt connection drop.");
            Assert.That(serverDetectedReason, Is.EqualTo(DisconnectReason.ConnectionLost),
                "Abrupt drop should fall back to ConnectionLost when no reason was registered.");
        }

        [Test]
        public void Test48_DisconnectReason_Scenario5_TransportDiagnostics_ResolvesLocalOnlyReason()
        {
            var serverFramer = new SecureFramingProvider();
            var badFramer = new BadMagicFramingProvider();

            _serverConfig.TransportFramingProvider = serverFramer;
            _serverManager = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                TransportFramingProvider = badFramer
            };

            var client = new LiminalNetworkManager(new TcpTransport<SecureFramingContext>(), clientConfig);
            _clientManagers.Add(client);

            client.StartClient("127.0.0.1", _currentTestPort);

            DisconnectReason serverResolvedReason = DisconnectReason.Unknown;
            bool serverResolved = false;

            _serverManager.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                serverResolvedReason = reason;
                serverResolved = true;
            };

            DisconnectReason clientResolvedReason = DisconnectReason.Unknown;
            bool clientResolved = false;

            client.DisconnectCoordinator.OnResolved += (id, reason, msg) =>
            {
                clientResolvedReason = reason;
                clientResolved = true;
            };

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);

            // Send malformed payload from client to server to trip framer
            client.Interpreter.SendCommand(ILiminalTransport.SERVER_ID, new ChatPacket { Message = "ExploitPayload" });

            // Server detects bad magic cookie and terminates socket
            bool serverFired = PollUntil(() => serverResolved, _serverManager, client, 2000);
            Assert.That(serverFired, Is.True, "Server failed to resolve transport diagnostic reason.");

            // Client observes socket drop and resolves ConnectionLost
            bool clientFired = PollUntil(() => clientResolved, _serverManager, client, 2000);
            Assert.That(clientFired, Is.True, "Client did not resolve disconnect.");

            Assert.Multiple(() =>
            {
                Assert.That(serverResolvedReason, Is.EqualTo(DisconnectReason.ProtocolViolation));
                Assert.That(clientResolvedReason, Is.EqualTo(DisconnectReason.ConnectionLost));
            });
        }

        [Test]
        public void Test49_TeardownStress_CoordinatorDispose_WhileInAckWaitWindow_CleansUpNeatly()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);
            ushort clientId = client.localID;

            client.Interpreter.UnsubscribeAll(client.DisconnectCoordinator);

            // Kick with long timeout window
            _serverManager.DisconnectCoordinator.ServerKickWithReason(clientId, DisconnectReason.Kicked, "GraceWindowTest", graceSeconds: 30);

            // Flush once so the server actually pushes the packet out
            _serverManager.SessionManager.Flush();
            Thread.Sleep(50);

            Assert.DoesNotThrow(() =>
            {
                _serverManager.DisconnectCoordinator.Dispose();
            }, "Disposing coordinator during an active ACK wait window threw an exception.");

            Assert.DoesNotThrow(() =>
            {
                _serverManager.Shutdown();
            });

            Assert.That(_serverManager.Transport.IsConnected, Is.False);
        }

        [Test]
        public void Test50_TeardownStress_TransportKilledConcurrently_DuringDrainDelay_CancelsCleanly()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            var client = CreateAndStartClient();

            Assert.That(PollUntil(() => client.Transport.IsConnected, _serverManager, client, 2000), Is.True);

            int unhandledExceptions = 0;

            client.DisconnectCoordinator.ClientDisconnectWithReason(DisconnectReason.ClientDisconnected, "QuickTear");

            client.SessionManager.Flush();
            _serverManager.SessionManager.Flush();

            var killTask = Task.Run(() =>
            {
                try
                {
                    Thread.Sleep(10); // Land in the tick drain delay
                    client.DisconnectCoordinator.Dispose();
                    client.Transport.Disconnect();
                }
                catch (Exception ex)
                {
                    LiminalLogger.LogError($"[Stress] Caught unexpected teardown leak: {ex}");
                    Interlocked.Increment(ref unhandledExceptions);
                }
            });

            Assert.That(killTask.Wait(TimeSpan.FromSeconds(2)), Is.True, "Concurrent kill task deadlocked.");
            Assert.That(unhandledExceptions, Is.EqualTo(0), "Architectural exception leaked during concurrent mid-drain disposal.");
            Assert.That(client.Transport.IsConnected, Is.False);
        }

        #endregion

        #region Premature Handshake Diagnostics & Coordinator Integration Tests

        [Test]
        public void Test51_HandshakeRejection_ServerFull_ResolvesOnClientCoordinator()
        {
            _serverConfig.MaxConnectionCount = 1;
            _serverManager.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            // Establish primary connection to fill slot
            var client1 = CreateAndStartClient();
            Assert.That(SpinWait.SpinUntil(() => client1.Transport.IsConnected, 2000), Is.True);

            // Prepare second client that should get rejected due to capacity
            var client2Config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var client2 = new LiminalNetworkManager(new TcpTransport(), client2Config);
            _clientManagers.Add(client2);

            DisconnectReason client2Reason = DisconnectReason.Unknown;
            string client2Message = null;
            bool client2Resolved = false;

            client2.OnDisconnectResolved += (id, reason, msg) =>
            {
                client2Reason = reason;
                client2Message = msg;
                client2Resolved = true;
            };

            client2.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => client2Resolved, 3000), Is.True,
                "Client 2 failed to resolve disconnect reason from handshake rejection.");

            Assert.Multiple(() =>
            {
                Assert.That(client2Reason, Is.EqualTo(DisconnectReason.ServerFull),
                    "Expected ServerFull reason when connecting to a full server.");
                Assert.That(client2Message, Is.Not.Null.And.Not.Empty);
                Assert.That(client2.Transport.IsConnected, Is.False);
            });
        }

        [Test]
        public void Test52_HandshakeRejection_VersionMismatch_ResolvesOnClientCoordinator()
        {
            _serverConfig.Version = 2;
            _serverManager.Shutdown();
            _serverManager = new LiminalNetworkManager(new TcpTransport(), _serverConfig);
            _serverManager.StartServer("127.0.0.1", _currentTestPort);

            var outdatedClientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5,
                Version = 1 // Intentionally mismatched
            };

            var client = new LiminalNetworkManager(new TcpTransport(), outdatedClientConfig);
            _clientManagers.Add(client);

            DisconnectReason resolvedReason = DisconnectReason.Unknown;
            string resolvedMessage = null;
            bool resolved = false;

            client.OnDisconnectResolved += (id, reason, msg) =>
            {
                resolvedReason = reason;
                resolvedMessage = msg;
                resolved = true;
            };

            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => resolved, 3000), Is.True,
                "Client failed to resolve disconnect reason on version mismatch.");

            Assert.Multiple(() =>
            {
                Assert.That(resolvedReason, Is.EqualTo(DisconnectReason.VersionMismatch));
                Assert.That(resolvedMessage, Does.Contain("v2").Or.Contain("version"));
                Assert.That(client.Transport.IsConnected, Is.False);
            });
        }

        [Test]
        public void Test53_HandshakeRejection_RegistryMismatch_ResolvesProtocolViolation()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var clientTransport = new TcpTransport();

            clientTransport.ClientHandshaker = async (tcpClient, cfg) =>
            {
                var stream = tcpClient.GetStream();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                var clientInfo = new ConnectionHandshakePacketClient
                {
                    ClientVersion = cfg.Version,
                    PacketRegistryHash = LiminalPacketLibrary.RegistryHash ^ 0x1337
                };

                byte[] body = MessagePackSerializer.Serialize(clientInfo);
                byte[] full = new byte[8 + body.Length];
                BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(0, 4), body.Length);
                BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());
                body.CopyTo(full.AsSpan(8));
                await stream.WriteAsync(full, cts.Token);

                byte[] header = new byte[8];
                await stream.ReadExactlyAsync(header, 0, 8, cts.Token);
                int len = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                byte[] respPayload = new byte[len];
                await stream.ReadExactlyAsync(respPayload, 0, len, cts.Token);

                var serverResp = MessagePackSerializer.Deserialize<ConnectionHandshakePacketServer>(respPayload);
                return HandshakeResult.Fail(serverResp.RejectReason, serverResp.RejectMessage);
            };

            var client = new LiminalNetworkManager(clientTransport, clientConfig);
            _clientManagers.Add(client);

            DisconnectReason clientResolvedReason = DisconnectReason.Unknown;
            bool clientResolved = false;

            client.OnDisconnectResolved += (id, reason, msg) =>
            {
                clientResolvedReason = reason;
                clientResolved = true;
            };

            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => clientResolved, 3000), Is.True,
                "Client coordinator failed to resolve registry rejection.");
            Assert.That(clientResolvedReason, Is.EqualTo(DisconnectReason.ProtocolViolation));
        }

        [Test]
        public void Test54_HandshakeSecurity_HardRstOnOversizedHeader_CoordinatorResolvesConnectionLost()
        {
            _serverManager.StartServer("127.0.0.1", _currentTestPort);
            Assert.That(SpinWait.SpinUntil(() => _serverManager.Transport.IsConnected, 2000), Is.True);

            var clientConfig = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = _currentTestPort,
                TickRate = 60,
                MaxPacketSizePerBatch = 4096,
                ClientIdResolver = new BaseResolver(),
                ConnectionTimeout = 5,
                HandshakeTimeout = 5
            };

            var clientTransport = new TcpTransport();
            clientTransport.ClientHandshaker = async (tcpClient, cfg) =>
            {
                var stream = tcpClient.GetStream();
                byte[] maliciousHeader = new byte[8];
                BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(0, 4), 1024 * 1024);
                BinaryPrimitives.WriteInt32LittleEndian(maliciousHeader.AsSpan(4, 4), LiminalPacketLibrary.GetId<ConnectionHandshakePacketClient>());

                await stream.WriteAsync(maliciousHeader);

                byte[] dummy = new byte[8];
                int read = await stream.ReadAsync(dummy);
                return read <= 0
                    ? HandshakeResult.Fail(DisconnectReason.ConnectionLost, "Socket severed abruptly")
                    : HandshakeResult.Ok(1);
            };

            var client = new LiminalNetworkManager(clientTransport, clientConfig);
            _clientManagers.Add(client);

            DisconnectReason reason = DisconnectReason.Unknown;
            bool fired = false;

            client.OnDisconnectResolved += (id, r, msg) =>
            {
                reason = r;
                fired = true;
            };

            client.StartClient("127.0.0.1", _currentTestPort);

            Assert.That(SpinWait.SpinUntil(() => fired, 3000), Is.True);
            Assert.That(reason, Is.EqualTo(DisconnectReason.ConnectionLost));
            Assert.That(_serverManager.Transport.ConnectedClientCount, Is.EqualTo(0));
        }

        #endregion

        #region Supporting Types for Test 48

        public readonly struct SecureFramingContext
        {
            public readonly uint MagicCookie;
            public readonly ushort SequenceNumber;

            public SecureFramingContext(uint magicCookie, ushort sequenceNumber)
            {
                MagicCookie = magicCookie;
                SequenceNumber = sequenceNumber;
            }
        }

        public class SecureFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public const uint ExpectedMagic = 0xDEADBEEF;
            public int CustomHeaderSize => 6;

            public int CorruptedPacketsCount = 0;
            public int ValidPacketsCount = 0;
            public readonly ConcurrentBag<ushort> ReceivedSequences = new();

            private int _outboundSeq = 0;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                uint cookie = context.MagicCookie != 0 ? context.MagicCookie : ExpectedMagic;
                ushort seq = context.SequenceNumber != 0 ? context.SequenceNumber : (ushort)Interlocked.Increment(ref _outboundSeq);

                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), cookie);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), seq);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
                ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));

                if (cookie != ExpectedMagic)
                {
                    Interlocked.Increment(ref CorruptedPacketsCount);
                    context = default;
                    return false;
                }

                Interlocked.Increment(ref ValidPacketsCount);
                ReceivedSequences.Add(seq);
                context = new SecureFramingContext(cookie, seq);
                return true;
            }
        }

        public class BadMagicFramingProvider : ILiminalTransportFramingProvider<SecureFramingContext>
        {
            public int CustomHeaderSize => 6;

            public void WriteCustomHeader(Span<byte> destination, in SecureFramingContext context)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), 0xBAADC0DE);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), 999);
            }

            public bool TryReadCustomHeader(ReadOnlySpan<byte> source, out SecureFramingContext context)
            {
                context = default;
                return true;
            }
        }

        #endregion
    }
}