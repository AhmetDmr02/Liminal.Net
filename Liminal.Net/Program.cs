using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using Liminal.Net.Transports;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace Liminal.Net
{
    public static class Program
    {
        private static LiminalNetworkManager _manager;
        private static ChatPacket? _lastPacket;
        private static ushort _lastTargetId;
        private static CancellationTokenSource _spamCts;
        private static readonly PacketMonitor _monitor = new PacketMonitor();
        private static long _totalSent = 0;

        public static void Main()
        {
            Console.Title = "Liminal.Net Console";
            LiminalLogger.Log("Initializing...");

            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 7777,
                TickRate = 10,
                MaxPacketSizePerBatch = 4096,
                InboundPacketProcessors = new(),
                OutboundPacketProcessors = new(),
                ClientIdResolver = new BaseResolver()
            };

            var transport = new TcpTransport();
            _manager = new LiminalNetworkManager(transport, config);
            _manager.Interpreter.Subscribe<ChatPacket>(OnChatReceived, "Program");
            _manager.Interpreter.Subscribe<FilePacket>(OnFileReceived, "Program");

            Console.WriteLine("Commands: host, server, connect, send {t} {id}, sendfile {file} {id}, spam {pps}, stopspam, reset, disconnect, kick {id}");

            bool running = true;
            string inputBuffer = "";

            while (running)
            {
                if (_manager.Role != NetworkRole.None)
                {
                    _manager.ManualPoll();
                }

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

                _monitor.UpdateTitle(_totalSent);
            }

            _manager.Shutdown();
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
                    break;
                case "send": HandleSendCommand(args); break;
                case "sendfile": HandleSendFileCommand(args); break;
                case "spam": HandleSpamCommand(args); break;
                case "stopspam": StopSpam(); break;
                case "kick": HandleKickCommand(args); break;
                case "reset":
                    _monitor.Reset();
                    Interlocked.Exchange(ref _totalSent, 0);
                    Console.WriteLine("Counters reset.");
                    break;
            }
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
            _monitor.RecordPacket();
            if (_monitor.GetCurrentPPS() < 3)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"\n[MSG] {senderId}: {packet.Message}");
                Console.ResetColor();
            }
        }

        private static void OnFileReceived(FilePacket packet, ushort senderId)
        {
            _monitor.RecordPacket();
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

    public class PacketMonitor
    {
        private readonly ConcurrentQueue<DateTime> _arrivalTimes = new ConcurrentQueue<DateTime>();
        private long _totalReceived = 0;

        public void RecordPacket()
        {
            _arrivalTimes.Enqueue(DateTime.Now);
            Interlocked.Increment(ref _totalReceived);
        }

        public void Reset()
        {
            Interlocked.Exchange(ref _totalReceived, 0);
            while (_arrivalTimes.TryDequeue(out _)) { }
        }

        public int GetCurrentPPS()
        {
            DateTime cutoff = DateTime.Now.AddSeconds(-1);
            while (_arrivalTimes.TryPeek(out DateTime time) && time < cutoff) _arrivalTimes.TryDequeue(out _);
            return _arrivalTimes.Count;
        }

        public void UpdateTitle(long sentByMe)
        {
            int pps = GetCurrentPPS();
            long recv = Interlocked.Read(ref _totalReceived);

            Console.Title = $"PPS: {pps} | Sent: {sentByMe} | Recv: {recv}";
        }
    }
}