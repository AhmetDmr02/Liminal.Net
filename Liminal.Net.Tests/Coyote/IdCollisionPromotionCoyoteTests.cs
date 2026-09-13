using Liminal.Net.ClientIdResolvers;
using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    public static class IdCollisionPromotionCoyoteTests
    {
        /// <summary>
        /// Models the internal socket and queue table of TcpTransport under ID collision.
        /// </summary>
        private class TransportSocketRegistry
        {
            public readonly struct ConnectionHandle
            {
                public readonly int InstanceGeneration;
                public readonly ushort ClientId;

                public ConnectionHandle(ushort id, int generation)
                {
                    ClientId = id;
                    InstanceGeneration = generation;
                }
            }

            private readonly ConcurrentDictionary<ushort, ConnectionHandle> _activeSockets = new();
            private readonly ConcurrentDictionary<ushort, object> _outboundQueues = new();

            public ConnectionHandle PromoteClient(ushort id, int generation, out ConnectionHandle? oldHandle)
            {
                var newHandle = new ConnectionHandle(id, generation);
                var newQueue = new object();

                ConnectionHandle? capturedOld = null;

                _activeSockets.AddOrUpdate(id,
                    _ => newHandle,
                    (_, existing) =>
                    {
                        capturedOld = existing;
                        return newHandle;
                    });

                oldHandle = capturedOld;
                _outboundQueues.AddOrUpdate(id, _ => newQueue, (_, _) => newQueue);

                return newHandle;
            }

            public void TeardownSocket_Buggy(ushort id)
            {
                _activeSockets.TryRemove(id, out _);
                _outboundQueues.TryRemove(id, out _);
            }

            public void TeardownSocket_Fixed(ConnectionHandle handle)
            {
                ((System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<ushort, ConnectionHandle>>)_activeSockets)
                    .Remove(new System.Collections.Generic.KeyValuePair<ushort, ConnectionHandle>(handle.ClientId, handle));

            }

            public int ActiveSocketCount => _activeSockets.Count;
            public bool HasOutboundQueue(ushort id) => _outboundQueues.ContainsKey(id);
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Coyote_Replicate_CollidingId_TeardownCannibalization()
        {
            var registry = new TransportSocketRegistry();
            const ushort targetId = 42;

            int generationCounter = 0;
            var startGate = new TaskCompletionSource<bool>();

            var client1Handle = registry.PromoteClient(targetId, Interlocked.Increment(ref generationCounter), out _);

            var promoteClient2Task = Task.Run(async () =>
            {
                await Task.Yield();
                registry.PromoteClient(targetId, Interlocked.Increment(ref generationCounter), out var evicted);

                if (evicted.HasValue)
                {
                    startGate.TrySetResult(true);
                }
            });

            var client1TeardownTask = Task.Run(async () =>
            {
                await startGate.Task;
                await Task.Yield();

                registry.TeardownSocket_Buggy(targetId);
            });

            await Task.WhenAll(promoteClient2Task, client1TeardownTask);

            Specification.Assert(registry.ActiveSocketCount == 1,
                $"RACE OCCURRED: Active socket count was {registry.ActiveSocketCount}, expected 1! Client 1's teardown deleted Client 2's socket.");

            Specification.Assert(registry.HasOutboundQueue(targetId),
                "RACE OCCURRED: Client 2 lost its outbound queue due to Client 1's concurrent eviction!");
        }
    }
}