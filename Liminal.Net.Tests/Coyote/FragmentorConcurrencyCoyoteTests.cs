using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Liminal.Net.Test;
using MessagePack;
using Microsoft.Coyote.Specifications;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    //Every test disposes the fragmentor and checks rented buffer count is 0, positive is a leak negative is a double return
    public static class FragmentorConcurrencyCoyoteTests
    {
        private const int Mtu = 500;
        private const long ForceStale = long.MaxValue / 2;

        private static LiminalNetworkConfig CreateConfig() => new LiminalNetworkConfig
        {
            TickRate = 60,
            MaxPacketSizePerBatch = 4096,
            MaxPacketCount = 256,
            MaxConnectionCount = 16,
            ReceiveResponseTimeout = 50f,
            ClientIdResolver = new BaseResolver()
        };

        private static byte[] CreatePayload(int size, int seed)
        {
            var payload = new byte[size];
            new Random(seed).NextBytes(payload);
            return payload;
        }

        //Separate sender so payloads get consecutive sequence numbers
        private static List<byte[]> Fragment(LiminalNetworkConfig config, params byte[][] payloads)
        {
            var senderTransport = new MockTransport();
            var sender = new LiminalPacketFragmentor(senderTransport, config, Mtu);
            foreach (var payload in payloads)
            {
                sender.Send(payload, 1, TransportFlags.Reliable);
            }
            var chunks = new List<byte[]>();
            foreach (var packet in senderTransport.SentPackets)
            {
                chunks.Add(packet.Data);
            }
            sender.Dispose();
            return chunks;
        }

        private static void AssertNoBufferLeaks(LiminalPacketFragmentor fragmentor)
        {
            fragmentor.Dispose();
            Specification.Assert(fragmentor.OutstandingAssemblyBuffers == 0,
                $"Assembly buffer accounting is {fragmentor.OutstandingAssemblyBuffers} after dispose (positive = leak, negative = double return).");
            Specification.Assert(fragmentor.PeersWithPendingAssemblies == 0,
                $"{fragmentor.PeersWithPendingAssemblies} peers still counted as having pending assemblies after dispose.");
        }

        [Test]
        public static async Task Coyote_Fragmentor_IngestVsDisconnectCleanup_NoLeakNoCorruption()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(1);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var expected = CreatePayload(1300, 1);
            var chunks = Fragment(config, expected);
            int deliveries = 0;
            bool corrupt = false;
            fragmentor.OnMessageReassembled += (data, sender) =>
            {
                Interlocked.Increment(ref deliveries);
                if (!data.SequenceEqual(expected)) corrupt = true;
            };

            var ingest = Task.Run(async () =>
            {
                foreach (var chunk in chunks)
                {
                    fragmentor.IngestFragment(chunk, 1);
                    await Task.Yield();
                }
            });
            var cleanup = Task.Run(() => transport.TriggerClientDisconnected(1));

            await Task.WhenAll(ingest, cleanup);

            Specification.Assert(deliveries <= 1, $"Payload delivered {deliveries} times.");
            Specification.Assert(!corrupt, "Delivered payload does not match what was sent.");
            AssertNoBufferLeaks(fragmentor);
        }

        [Test]
        public static async Task Coyote_Fragmentor_KickDuringDelivery_DeliveredBufferIsNotRecycled()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(1);
            transport.ConnectedClients.Add(2);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var expected1 = CreatePayload(1300, 1);
            var expected2 = CreatePayload(1300, 2);
            var chunks1 = Fragment(config, expected1);
            var chunks2 = Fragment(config, expected2);
            int delivered1 = 0, delivered2 = 0;
            bool corrupt1 = false, corrupt2 = false;

            fragmentor.OnMessageReassembled += (data, sender) =>
            {
                if (sender == 1)
                {
                    //Kick peer 1 inside its own delivery then let peer 2 rent buffers before peer 1's bytes get read
                    transport.Kick(1);
                    Task.Run(() =>
                    {
                        foreach (var chunk in chunks2) fragmentor.IngestFragment(chunk, 2);
                    }).Wait();

                    if (!data.SequenceEqual(expected1)) corrupt1 = true;
                    Interlocked.Increment(ref delivered1);
                }
                else
                {
                    if (!data.SequenceEqual(expected2)) corrupt2 = true;
                    Interlocked.Increment(ref delivered2);
                }
            };

            await Task.Run(() =>
            {
                foreach (var chunk in chunks1) fragmentor.IngestFragment(chunk, 1);
            });

            Specification.Assert(delivered1 == 1 && delivered2 == 1, $"Expected one delivery per peer, got {delivered1}/{delivered2}.");
            Specification.Assert(!corrupt1, "Peer 1's payload was overwritten while it was being delivered (buffer returned early).");
            Specification.Assert(!corrupt2, "Peer 2's payload is corrupt.");
            AssertNoBufferLeaks(fragmentor);
        }

        [Test]
        public static async Task Coyote_Fragmentor_SweepVsCompletingIngest_DeliveredXorSevered()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(1);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var expected = CreatePayload(1300, 1);
            var chunks = Fragment(config, expected);
            int deliveries = 0;
            fragmentor.OnMessageReassembled += (data, sender) => Interlocked.Increment(ref deliveries);

            fragmentor.IngestFragment(chunks[0], 1);

            var ingestRest = Task.Run(async () =>
            {
                for (int i = 1; i < chunks.Count; i++)
                {
                    fragmentor.IngestFragment(chunks[i], 1);
                    await Task.Yield();
                }
            });
            var sweep = Task.Run(() => fragmentor.PruneStaleAssemblies(ForceStale));

            await Task.WhenAll(ingestRest, sweep);

            bool kicked = transport.KickedClients.Contains(1);
            Specification.Assert((deliveries == 1) != kicked,
                $"A peer must be either delivered or severed, never both or neither (delivered {deliveries}, kicked {kicked}).");
            AssertNoBufferLeaks(fragmentor);
        }

        [Test]
        public static async Task Coyote_Fragmentor_MultiPeerIngestWithCleanupAndSweep_PerPeerOrderAndIntegrity()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            for (ushort id = 1; id <= 3; id++) transport.ConnectedClients.Add(id);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var expected = new Dictionary<ushort, byte[][]>();
            var chunks = new Dictionary<ushort, List<byte[]>>();
            for (ushort id = 1; id <= 3; id++)
            {
                expected[id] = new[] { CreatePayload(1300, id * 10), CreatePayload(1100, id * 10 + 1) };
                chunks[id] = Fragment(config, expected[id]);
            }

            var received = new ConcurrentDictionary<ushort, ConcurrentQueue<byte[]>>();
            fragmentor.OnMessageReassembled += (data, sender) =>
                received.GetOrAdd(sender, _ => new ConcurrentQueue<byte[]>()).Enqueue(data.ToArray());

            var tasks = new List<Task>();
            for (ushort id = 1; id <= 3; id++)
            {
                ushort peer = id;
                tasks.Add(Task.Run(async () =>
                {
                    foreach (var chunk in chunks[peer])
                    {
                        fragmentor.IngestFragment(chunk, peer);
                        await Task.Yield();
                    }
                }));
            }
            tasks.Add(Task.Run(() => transport.TriggerClientDisconnected(2)));
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < 3; i++)
                {
                    fragmentor.PruneStaleAssemblies();
                    await Task.Yield();
                }
            }));

            await Task.WhenAll(tasks);

            foreach (ushort id in new ushort[] { 1, 3 })
            {
                var got = received.TryGetValue(id, out var q) ? q.ToArray() : Array.Empty<byte[]>();
                Specification.Assert(got.Length == 2, $"Peer {id} received {got.Length} payloads, expected 2.");
                Specification.Assert(got[0].AsSpan().SequenceEqual(expected[id][0]) && got[1].AsSpan().SequenceEqual(expected[id][1]),
                    $"Peer {id} payloads are corrupt or out of order.");
            }

            var peer2 = received.TryGetValue(2, out var q2) ? q2.ToArray() : Array.Empty<byte[]>();
            Specification.Assert(peer2.Length <= 2, $"Peer 2 received {peer2.Length} payloads.");
            for (int i = 0; i < peer2.Length; i++)
            {
                Specification.Assert(peer2[i].AsSpan().SequenceEqual(expected[2][i]), "Peer 2 payload is corrupt or out of order.");
            }

            AssertNoBufferLeaks(fragmentor);
        }

        [Test]
        public static async Task Coyote_Fragmentor_DisposeConcurrentWithIngestAndKick_NoLeak()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(1);
            transport.ConnectedClients.Add(2);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var chunks1 = Fragment(config, CreatePayload(1300, 1), CreatePayload(1300, 2));
            var chunks2 = Fragment(config, CreatePayload(1300, 3));
            int deliveries = 0;
            fragmentor.OnMessageReassembled += (data, sender) => Interlocked.Increment(ref deliveries);

            var ingest1 = Task.Run(async () =>
            {
                foreach (var chunk in chunks1) { fragmentor.IngestFragment(chunk, 1); await Task.Yield(); }
            });
            var ingest2 = Task.Run(async () =>
            {
                foreach (var chunk in chunks2) { fragmentor.IngestFragment(chunk, 2); await Task.Yield(); }
            });
            var kick = Task.Run(() => transport.Kick(1));
            var dispose = Task.Run(() => fragmentor.Dispose());

            await Task.WhenAll(ingest1, ingest2, kick, dispose);

            Specification.Assert(deliveries <= 3, $"{deliveries} deliveries from 3 payloads.");
            AssertNoBufferLeaks(fragmentor);
        }

        [Test]
        public static async Task Coyote_Fragmentor_AssemblyCapExceeded_SeversAndReleasesEverything()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            transport.ConnectedClients.Add(1);
            var fragmentor = new LiminalPacketFragmentor(transport, config, Mtu);

            var payloads = new byte[LiminalPacketFragmentor.MaxPendingAssembliesPerPeer + 1][];
            for (int i = 0; i < payloads.Length; i++) payloads[i] = CreatePayload(1300, i);
            var chunks = Fragment(config, payloads);
            int fragmentsPerPayload = chunks.Count / payloads.Length;

            //Only the first fragment of each payload so every one opens an assembly that never completes
            var opener = Task.Run(async () =>
            {
                for (int i = 0; i < payloads.Length; i++)
                {
                    fragmentor.IngestFragment(chunks[i * fragmentsPerPayload], 1);
                    await Task.Yield();
                }
            });
            var sweep = Task.Run(() => fragmentor.PruneStaleAssemblies());

            await Task.WhenAll(opener, sweep);

            Specification.Assert(transport.KickedClients.Contains(1),
                $"Opening {payloads.Length} concurrent assemblies (cap {LiminalPacketFragmentor.MaxPendingAssembliesPerPeer}) did not sever the peer.");
            AssertNoBufferLeaks(fragmentor);
        }

        private static byte[] CreateChatBatch(string message)
        {
            ushort chatId = (ushort)LiminalPacketLibrary.GetId<ChatPacket>();
            byte[] payload = MessagePackSerializer.Serialize(new ChatPacket { Message = message });
            var batch = new byte[4 + 2 + payload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(batch.AsSpan(0, 4), payload.Length + 2);
            BinaryPrimitives.WriteUInt16LittleEndian(batch.AsSpan(4, 2), chatId);
            payload.CopyTo(batch.AsSpan(6));
            return batch;
        }

        [Test]
        public static async Task Coyote_SessionManager_FragmentedThenSmallReliableBatch_DispatchedInWireOrder()
        {
            var config = CreateConfig();
            var transport = new MockTransport { IsServer = true };
            var manager = new LiminalNetworkManager(transport, config, customTicker: new Liminal.Net.Unity.LiminalUnityTicker(config));
            manager.StartServer("127.0.0.1", 7777);
            transport.TriggerClientConnected(5);

            var order = new ConcurrentQueue<string>();
            manager.Interpreter.Subscribe<ChatPacket>((packet, sender) => order.Enqueue(packet.Message.Substring(0, 5)), "OrderProbe");

            var bigBatch = CreateChatBatch("BIG__" + new string('x', 3000));
            var smallBatch = CreateChatBatch("SMALL");

            var senderTransport = new MockTransport();
            var sender = new LiminalPacketFragmentor(senderTransport, config);
            sender.Send(bigBatch, 5, TransportFlags.Reliable);
            var bigFragments = new List<byte[]>();
            foreach (var packet in senderTransport.SentPackets) bigFragments.Add(packet.Data);
            sender.Dispose();
            Specification.Assert(bigFragments.Count > 1, "Big batch was expected to be fragmented.");

            //One task is the peer receive thread (wire order), the other polls like the tick does
            var receiveThread = Task.Run(async () =>
            {
                foreach (var fragment in bigFragments)
                {
                    transport.TriggerFragmentReceived(fragment, 5);
                    await Task.Yield();
                }
                transport.TriggerReliableReceived(smallBatch, 5);
            });
            var tick = Task.Run(async () =>
            {
                for (int i = 0; i < 6; i++)
                {
                    manager.SessionManager.Poll();
                    await Task.Yield();
                }
            });

            await Task.WhenAll(receiveThread, tick);
            manager.SessionManager.Poll();

            var result = order.ToArray();
            Specification.Assert(result.Length == 2 && result[0] == "BIG__" && result[1] == "SMALL",
                $"Reliable batches dispatched out of wire order: [{string.Join(", ", result)}].");

            manager.Shutdown();
        }
    }
}
