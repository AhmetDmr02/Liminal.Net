using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Test;
using Liminal.Net.Transports;

namespace Liminal.Net
{
    public static class Program
    {
        private static LiminalNetworkManager _manager;

        public static void Main()
        {
            Console.Title = "Liminal.Net Console";
            LiminalLogger.Log("Initializing...");

            var config = new LiminalTransportConfig
            {
                Default_Host = "127.0.0.1",
                Default_Port = 7777,
                TickRate = 30,
                MaxPacketSizePerBatch = 4096,
                InboundPacketProcessors = new(),
                OutboundPacketProcessors = new(),
                ClientIdResolver = new BaseResolver()
            };

            var transport = new TcpTransport();
            _manager = new LiminalNetworkManager(transport, config);

            _manager.Interpreter.Subscribe<ChatPacket>(OnChatReceived, "Program");

            Console.WriteLine("Commands:");
            Console.WriteLine(" - host / server / connect");
            Console.WriteLine(" - send {text} {targetId}");
            Console.WriteLine(" - kick {targetId}");
            Console.WriteLine(" - disconnect");
            Console.WriteLine(" - stresstest {path} {targetId}");

            bool running = true;
            string inputBuffer = "";

            while (running)
            {
                if (_manager.Role == NetworkRole.Client || _manager.Role == NetworkRole.Host)
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
                    else if (key.Key == ConsoleKey.Backspace)
                    {
                        if (inputBuffer.Length > 0)
                        {
                            inputBuffer = inputBuffer[..^1];
                            Console.Write("\b \b");
                        }
                    }
                    else if (key.Key == ConsoleKey.Escape)
                    {
                        running = false;
                    }
                    else
                    {
                        inputBuffer += key.KeyChar;
                        Console.Write(key.KeyChar);
                    }
                }

                Thread.Sleep(1);
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
                case "host":
                    _manager.StartHost();
                    Console.Title = "Liminal Host";
                    break;

                case "server":
                case "startserver":
                    _manager.StartServer(config.Default_Host, config.Default_Port);
                    Console.Title = "Liminal Server";
                    break;

                case "connect":
                    _manager.StartClient(config.Default_Host, config.Default_Port);
                    Console.Title = "Liminal Client";
                    break;

                case "disconnect":
                    Console.WriteLine("Disconnecting...");
                    _manager.Disconnect();
                    break;


                case "shutdown":
                    Console.WriteLine("shutting down...");
                    _manager.Shutdown();
                    break;

                case "kick":
                    if (args.Length < 2)
                    {
                        Console.WriteLine("Usage: kick {targetId}");
                        break;
                    }
                    if (ushort.TryParse(args[1], out ushort kickId))
                    {
                        Console.WriteLine($"Kicking Client {kickId}...");
                        (_manager.Transport).Kick(kickId);
                    }
                    break;

                case "send":
                    HandleSendCommand(args);
                    break;

                case "stresstest":
                    HandleStressTest(args);
                    break;
                case "lgo":
                    Console.WriteLine($"Client {LiminalNetworkManager.Instance.localID}...");
                    break;

                default:
                    Console.WriteLine("Unknown command.");
                    break;
            }
        }

        private static void HandleSendCommand(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Usage: send {text} {targetId}");
                return;
            }

            if (ushort.TryParse(args[^1], out ushort targetId))
            {
                string message = string.Join(" ", args[1..^1]);
                var packet = new ChatPacket { Message = message };
                _manager.Interpreter.SendCommand(targetId, packet);
                Console.WriteLine($"Sent '{message}' to {targetId}");
            }
            else
            {
                Console.WriteLine("Invalid Target ID.");
            }
        }

        private static void HandleStressTest(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Usage: stresstest {filePath} {targetId}");
                return;
            }

            string path = args[1];
            if (!File.Exists(path))
            {
                Console.WriteLine($"File not found: {path}");
                return;
            }

            if (ushort.TryParse(args[2], out ushort targetId))
            {
                try
                {
                    string content = File.ReadAllText(path);
                    Console.WriteLine($"Attempting to send {content.Length} bytes from file to {targetId}...");

                    var packet = new ChatPacket { Message = content };
                    _manager.Interpreter.SendCommand(targetId, packet);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Stress test failed: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("Invalid Target ID.");
            }
        }

        private static void OnChatReceived(ChatPacket packet, ushort senderId)
        {
            int currentLeft = Console.CursorLeft;
            Console.CursorLeft = 0;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"[MSG] {senderId}: {packet.Message}");
            Console.ResetColor();

            Console.CursorLeft = currentLeft;
        }
    }
}