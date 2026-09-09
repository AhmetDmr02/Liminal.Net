using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net
{
    public static class Program
    {
        private static LiminalNetworkManager _manager;
        private static ChatPacket? _lastPacket;
        private static ushort _lastTargetId;
        private static CancellationTokenSource _spamCts;

        // Dual rolling averagers for E2E and Wire RTT
        private static readonly RttAverager _e2eAverager = new RttAverager(sampleWindowSize: 10);
        private static readonly RttAverager _wireAverager = new RttAverager(sampleWindowSize: 10);

        private static long _totalReceived = 0;
        private static long _totalSent = 0;

        public static void Main()
        {
            Console.Title = "Liminal.Net Console";
            LiminalLogger.Log("Initializing...");

            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 7777,
                TickRate = 20,
                MaxPacketSizePerBatch = 4096,
                InboundPacketProcessors = new(),
                OutboundPacketProcessors = new(),
                ReceiveResponseTimeout = 5.0f,
                SendResponseTimeout = 5.0f,
                ClientIdResolver = new BaseResolver()
            };

            // Flags = All now enables both End2EndRTT and WireRTT
            var telemetryConfig = new LiminalTelemetryConfig
            {
                Flags = TelemetryFlags.All,
                PollIntervalInSeconds = 1.33f
            };

            var transport = new TcpTransport();
            _manager = new LiminalNetworkManager(transport, config, telemetryConfig);
            _manager.Interpreter.Subscribe<ChatPacket>(OnChatReceived, "Program");
            _manager.Interpreter.Subscribe<FilePacket>(OnFileReceived, "Program");

            Console.WriteLine("Commands: host, server, connect, rtt, send {t} {id}, sendfile {file} {id}, spam {pps}, stopspam, reset, disconnect, kick {id}");

            bool running = true;
            string inputBuffer = "";

            while (running)
            {
                if (Console.KeyAvailable)
                {
                    var key = Console.ReadKey(intercept: true);
                    if (key.Key == ConsoleKey.Enter)
                    {
                        Console.WriteLine();
                        ProcessCommand(inputBuffer, config);
                        inputBuffer = "";
                    }
                    else if (key.Key == ConsoleKey.Backspace && inputBuffer.Length > 0)
                    {
                        inputBuffer = inputBuffer[..^1];
                        Console.Write("\b \b");
                    }
                    else if (key.Key == ConsoleKey.Escape)
                    {
                        running = false;
                    }
                    else if (key.Key != ConsoleKey.Backspace && key.Key != ConsoleKey.Enter)
                    {
                        inputBuffer += key.KeyChar;
                        Console.Write(key.KeyChar);
                    }
                }

                // Sample and update both rolling buffers
                if (_manager.TelemetryManager != null)
                {
                    double liveE2E = _manager.TelemetryManager.End2EndRTT;
                    if (liveE2E > 0.0) _e2eAverager.AddSample(liveE2E);

                    double liveWire = _manager.TelemetryManager.WireRTT;
                    if (liveWire > 0.0) _wireAverager.AddSample(liveWire);
                }

                UpdateConsoleTitle();
                Thread.Sleep(15);
            }

            _manager.Shutdown();
        }

        private static void UpdateConsoleTitle()
        {
            string roleLabel = _manager.Role != NetworkRole.None ? $"[{_manager.Role}] " : "";
            double avgE2E = _e2eAverager.GetAverageRtt();
            double avgWire = _wireAverager.GetAverageRtt();

            string e2eText = avgE2E > 0.0 ? $"{avgE2E:F1}ms" : "--";
            string wireText = avgWire > 0.0 ? $"{avgWire:F1}ms" : "--";

            Console.Title = $"{roleLabel}Wire: {wireText} | E2E: {e2eText} | Sent: {_totalSent} | Recv: {Interlocked.Read(ref _totalReceived)}";
        }

        private static void ProcessCommand(string input, LiminalTransportConfig config)
        {
            if (string.IsNullOrWhiteSpace(input)) return;
            string[] args = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            string cmd = args[0].ToLower();

            switch (cmd)
            {
                case "host": _manager.StartHost(); break;
                case "server": _manager.StartServer(config.Default_Host, config.Default_Port); break;
                case "connect": _manager.StartClient(config.Default_Host, config.Default_Port); break;
                case "disconnect":
                    StopSpam();
                    _manager.Disconnect();
                    _e2eAverager.Reset();
                    _wireAverager.Reset();
                    break;
                case "rtt": HandleRttCommand(); break;
                case "send": HandleSendCommand(args); break;
                case "sendasclient": HandleSendAsClientCommand(args); break;
                case "sendfile": HandleSendFileCommand(args); break;
                case "spam": HandleSpamCommand(args); break;
                case "stopspam": StopSpam(); break;
                case "kick": HandleKickCommand(args); break;
                case "telemetry": WriteTelemetry(); break;
                case "reset":
                    _e2eAverager.Reset();
                    _wireAverager.Reset();
                    Interlocked.Exchange(ref _totalSent, 0);
                    Interlocked.Exchange(ref _totalReceived, 0);
                    Console.WriteLine("Counters and RTT rolling buffers reset.");
                    break;
                case "localid":
                    Console.WriteLine($"Local ID: {_manager.Transport.LocalClientId}");
                    break;
            }
        }

        private static void WriteTelemetry()
        {
            if (_manager.Role == NetworkRole.None)
            {
                Console.WriteLine("Network is not active.");
                return;
            }

            Console.WriteLine($"Outbound: {_manager.TelemetryManager?.LatestTransportSnapshot.TotalBytesOutbound} bytes | Inbound: {_manager.TelemetryManager?.LatestTransportSnapshot.TotalBytesInbound} bytes");
        }

        private static void HandleRttCommand()
        {
            if (_manager.Role == NetworkRole.None)
            {
                Console.WriteLine("Network is not active.");
                return;
            }

            Console.ForegroundColor = ConsoleColor.Yellow;

            if (_manager.Role == NetworkRole.Client)
            {
                double curE2E = _manager.TelemetryManager?.End2EndRTT ?? 0.0;
                double curWire = _manager.TelemetryManager?.WireRTT ?? 0.0;
                double avgE2E = _e2eAverager.GetAverageRtt();
                double avgWire = _wireAverager.GetAverageRtt();

                Console.WriteLine("=== Latency to Server ===");
                Console.WriteLine($" [Wire RTT]    Current: {curWire:F2} ms | Avg: {avgWire:F2} ms");
                Console.WriteLine($" [End2End RTT] Current: {curE2E:F2} ms | Avg: {avgE2E:F2} ms");
                Console.WriteLine("=========================");
                Console.ResetColor();
                return;
            }

            Console.WriteLine("=== Connected Client Latencies ===");

            Span<ushort> clientIds = stackalloc ushort[_manager.Transport.Config.MaxConnectionCount];
            int count = _manager.SessionManager.GetSessionIds(clientIds);

            int listed = 0;
            for (int i = 0; i < count; i++)
            {
                ushort id = clientIds[i];
                if (id == _manager.localID) continue;

                listed++;

                double wire = 0.0;
                double e2e = 0.0;

                bool hasE2E = _manager.TelemetryManager != null && _manager.TelemetryManager.TryGetClientEnd2EndRTT(id, out e2e);
                bool hasWire = _manager.TelemetryManager != null && _manager.TelemetryManager.TryGetClientWireRTT(id, out wire);

                string wireStr = hasWire ? $"{wire:F2} ms" : "[Sampling...]";
                string e2eStr = hasE2E ? $"{e2e:F2} ms" : "[Sampling...]";

                Console.WriteLine($" Client {id,-5} | Wire: {wireStr,-12} | E2E: {e2eStr,-12}");
            }

            if (listed == 0)
            {
                Console.WriteLine(" No remote clients connected.");
            }

            Console.WriteLine("________________________________________________");
            Console.ResetColor();
        }

        private static void StopSpam()
        {
            if (_spamCts != null)
            {
                _spamCts.Cancel();
                _spamCts.Dispose();
                _spamCts = null;
                Console.WriteLine("Spam Task Terminated.");
            }
        }

        private static void HandleKickCommand(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: kick {targetid}");
                return;
            }

            if (ushort.TryParse(args[1], out ushort targetId))
            {
                _manager.Transport.Kick(targetId);
                Console.WriteLine($"Kicked client {targetId}.");
            }
            else
            {
                Console.WriteLine("Invalid target ID.");
            }
        }

        private static void HandleSendCommand(string[] args)
        {
            if (args.Length < 3) return;
            if (ushort.TryParse(args[^1], out ushort targetId))
            {
                string message = string.Join(" ", args[1..^1]);
                _lastPacket = new ChatPacket { Message = message };
                _lastTargetId = targetId;
                _manager.Interpreter.SendCommand(targetId, _lastPacket.Value);
                Interlocked.Increment(ref _totalSent);
            }
        }

        private static void HandleSendAsClientCommand(string[] args)
        {
            if (args.Length < 3) return;
            if (ushort.TryParse(args[^1], out ushort targetId))
            {
                string message = string.Join(" ", args[1..^1]);
                _lastPacket = new ChatPacket { Message = message };
                _lastTargetId = targetId;
                _manager.Interpreter.SendCommandAsClient(targetId, _lastPacket.Value);
                Interlocked.Increment(ref _totalSent);
            }
        }

        private static void HandleSendFileCommand(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Usage: sendfile {filename} {targetid}");
                return;
            }

            string fileName = args[1];
            if (!ushort.TryParse(args[2], out ushort targetId))
            {
                Console.WriteLine("Invalid target ID.");
                return;
            }

            string filePath = Path.Combine(Environment.CurrentDirectory, fileName);

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File not found: {filePath}");
                return;
            }

            try
            {
                byte[] fileData = File.ReadAllBytes(filePath);

                var packet = new FilePacket
                {
                    FileName = Path.GetFileName(filePath),
                    Data = fileData
                };

                _manager.Interpreter.SendCommand(targetId, packet);
                Interlocked.Increment(ref _totalSent);

                Console.WriteLine($"Sent file '{packet.FileName}' ({fileData.Length} bytes) to {targetId}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to send file: {ex.Message}");
            }
        }

        private static void HandleSpamCommand(string[] args)
        {
            if (!_lastPacket.HasValue || args.Length < 2 || !int.TryParse(args[1], out int rate)) return;

            StopSpam();
            _spamCts = new CancellationTokenSource();
            var token = _spamCts.Token;

            long frequency = Stopwatch.Frequency;
            long ticksPerPacket = frequency / rate;

            Task.Run(() =>
            {
                Console.WriteLine($"Spamming {rate}/s to {_lastTargetId} (High Precision Mode)...");
                try
                {
                    long nextPacketTime = Stopwatch.GetTimestamp();

                    while (!token.IsCancellationRequested && _manager.Role != NetworkRole.None)
                    {
                        long currentTime = Stopwatch.GetTimestamp();

                        if (currentTime >= nextPacketTime)
                        {
                            _manager.Interpreter.SendCommand(_lastTargetId, _lastPacket.Value);
                            Interlocked.Increment(ref _totalSent);

                            nextPacketTime += ticksPerPacket;

                            if (currentTime > nextPacketTime + (ticksPerPacket * 5))
                            {
                                nextPacketTime = currentTime + ticksPerPacket;
                            }
                        }
                        else
                        {
                            long ticksRemaining = nextPacketTime - currentTime;

                            if (ticksRemaining > (frequency / 64))
                            {
                                Thread.Sleep(1);
                            }
                            else
                            {
                                Thread.SpinWait(10);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Spam error: " + ex.Message);
                }
            }, token);
        }

        private static void OnChatReceived(ChatPacket packet, ushort senderId)
        {
            Interlocked.Increment(ref _totalReceived);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"\n[MSG] {senderId}: {packet.Message}");
            Console.ResetColor();
        }

        private static void OnFileReceived(FilePacket packet, ushort senderId)
        {
            Interlocked.Increment(ref _totalReceived);
            try
            {
                string saveDir = Path.Combine(Environment.CurrentDirectory, "ReceivedFiles");
                Directory.CreateDirectory(saveDir);

                string savePath = Path.Combine(saveDir, packet.FileName);
                File.WriteAllBytes(savePath, packet.Data);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"\n[FILE] Received '{packet.FileName}' ({packet.Data.Length} bytes) from {senderId}. Saved to /ReceivedFiles/");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n[FILE ERROR] Failed to save received file: {ex.Message}");
                Console.ResetColor();
            }
        }
    }

    public class RttAverager
    {
        private readonly double[] _samples;
        private int _index;
        private int _count;
        private readonly object _lock = new();

        public RttAverager(int sampleWindowSize = 10)
        {
            _samples = new double[Math.Max(1, sampleWindowSize)];
        }

        public void AddSample(double rttMs)
        {
            lock (_lock)
            {
                _samples[_index] = rttMs;
                _index = (_index + 1) % _samples.Length;
                if (_count < _samples.Length)
                {
                    _count++;
                }
            }
        }

        public double GetAverageRtt()
        {
            lock (_lock)
            {
                if (_count == 0) return 0.0;

                double sum = 0.0;
                for (int i = 0; i < _count; i++)
                {
                    sum += _samples[i];
                }
                return sum / _count;
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                _index = 0;
                _count = 0;
                Array.Clear(_samples, 0, _samples.Length);
            }
        }
    }
}