using Liminal.Net.Interfaces;
using Liminal.Net.Transports;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public sealed class LiminalPacketFragmentor : IDisposable
    {
        public const int FragmentHeaderSize = 6; // ReliableSeq(2) + FragmentIndex(2) + TotalFragments(2)

        private const int DisposalSignaled = -1_000_000;
        private static readonly long AssemblyTimeoutTicks = Stopwatch.Frequency * 5;

        private readonly ILiminalTransport _transport;
        private readonly LiminalTransportConfig _config;
        private readonly int _mtu;
        private readonly int _transportHeaderSize;
        private readonly ArrayPool<byte> _assemblyPool;
        private readonly ArrayPool<byte> _fragmentPool;

        private readonly int _maxAssemblySize;

        private int _disposedState = 0;

        private readonly ConcurrentDictionary<ushort, PeerChannelState> _peerChannels = new();
        private readonly ConcurrentDictionary<ushort, ushort> _outboundSequences = new();

        public int MTU => _mtu;

        public event DataReceivedHandler OnMessageReassembled;

        public LiminalPacketFragmentor(ILiminalTransport transport, LiminalTransportConfig config, int mtu = 1400)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            if (mtu <= FragmentHeaderSize + LiminalTransportHeader.BaseHeaderSize)
            {
                throw new ArgumentException($"MTU must be larger than headers ({FragmentHeaderSize + LiminalTransportHeader.BaseHeaderSize}b)", nameof(mtu));
            }

            _mtu = mtu;
            _transportHeaderSize = LiminalTransportHeader.BaseHeaderSize + (config.TransportFramingProvider?.CustomHeaderSize ?? 0);

            int minimumMtu = FragmentHeaderSize + _transportHeaderSize;
            if (mtu <= minimumMtu)
            {
                throw new ArgumentException($"MTU ({mtu}b) must be larger than all combined headers ({minimumMtu}b)", nameof(mtu));
            }

            int maxRecovery = config.Hiccup?.GetRecoverySize(config.MaxPacketSizePerBatch) ?? config.MaxPacketSizePerBatch;
            _maxAssemblySize = maxRecovery;

            _assemblyPool = ArrayPool<byte>.Create(maxRecovery, _config.MaxConnectionCount * 2);

            int recoverySize = _config.Hiccup.Enabled ? config.Hiccup.MaxRecoveryScale : 1;
            _fragmentPool = ArrayPool<byte>.Create(mtu, ((int)((_config.MaxPacketSizePerBatch * recoverySize) / mtu) + 1));

            _transport.OnClientDisconnected += HandleClientCleanup;
            _transport.OnClientKicked += HandleClientCleanup;
        }

        #region Gate & Lifetime Synchronization

        private bool TryEnter()
        {
            var spinner = new SpinWait();
            while (true)
            {
                int current = Volatile.Read(ref _disposedState);
                if (current < 0) return false;

                if (Interlocked.CompareExchange(ref _disposedState, current + 1, current) == current)
                {
                    return true;
                }

                spinner.SpinOnce();
            }
        }

        private void Exit()
        {
            Interlocked.Decrement(ref _disposedState);
        }

        #endregion

        #region Outbound / Send Path

        public void Send(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            if (!TryEnter()) return;

            try
            {
                int effectivePayloadMtu = _mtu - _transportHeaderSize;

                if (data.Length <= effectivePayloadMtu)
                {
                    if ((flags & TransportFlags.Reliable) != 0)
                    {
                        _transport.SendReliable(data, targetId);
                    }
                    else
                    {
                        _transport.SendUnreliable(data, targetId);
                    }
                    return;
                }

                if ((flags & TransportFlags.Reliable) == 0)
                {
                    LiminalLogger.LogWarning(
                        $"[Fragmentor] Payload ({data.Length}b) exceeds MTU ({_mtu}b) on Unreliable channel. Auto-swapping to Reliable + Fragmented.");
                    flags |= TransportFlags.Reliable;
                }

                flags |= TransportFlags.Fragmented;

                int maxFragmentPayload = effectivePayloadMtu - FragmentHeaderSize;
                int totalFragments = (data.Length + maxFragmentPayload - 1) / maxFragmentPayload;

                if (totalFragments > ushort.MaxValue)
                {
                    LiminalLogger.LogError($"[Fragmentor] Packet size {data.Length}b requires {totalFragments} fragments. Fatal bounds breach.");
                    TriggerSever(targetId, "Fragment count exceeded 65535.");
                    return;
                }

                ushort seq = _outboundSequences.AddOrUpdate(targetId, 1, (_, current) => unchecked((ushort)(current + 1)));

                byte[] rentedChunkBuffer = ArrayPool<byte>.Shared.Rent(effectivePayloadMtu);

                try
                {
                    Span<byte> chunkSpan = rentedChunkBuffer.AsSpan(0, effectivePayloadMtu);

                    for (ushort index = 0; index < totalFragments; index++)
                    {
                        int offset = index * maxFragmentPayload;
                        int length = Math.Min(maxFragmentPayload, data.Length - offset);

                        BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(0, 2), seq);
                        BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(2, 2), index);
                        BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(4, 2), (ushort)totalFragments);

                        data.Slice(offset, length).CopyTo(chunkSpan.Slice(FragmentHeaderSize));

                        Span<byte> outgoingFrame = chunkSpan.Slice(0, FragmentHeaderSize + length);
                        _transport.SendReliable(outgoingFrame, targetId);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedChunkBuffer);
                }
            }
            finally
            {
                Exit();
            }
        }

        #endregion

        #region Inbound / Producer-Consumer Assembly Path

        public void IngestFragment(ReadOnlySpan<byte> fragmentSpan, ushort senderId)
        {
            if (!TryEnter()) return;

            try
            {
                if (fragmentSpan.Length < FragmentHeaderSize || fragmentSpan.Length > _mtu)
                {
                    TriggerSever(senderId, $"Fragment payload ({fragmentSpan.Length}b) is out of valid bounds [{FragmentHeaderSize}b - {_mtu}b].");
                    return;
                }

                if (!_peerChannels.TryGetValue(senderId, out var peerState))
                {
                    if (!_transport.IsClientConnected(senderId)) return;

                    lock (_peerChannels)
                    {
                        if (!_peerChannels.TryGetValue(senderId, out peerState))
                        {
                            if (!_transport.IsClientConnected(senderId)) return;

                            peerState = CreatePeerChannel(senderId);
                            _peerChannels[senderId] = peerState;
                        }
                    }
                }

                if (Volatile.Read(ref peerState.IsTerminated) != 0)
                {
                    return;
                }

                byte[] rentedFragmentBuffer = _fragmentPool.Rent(fragmentSpan.Length);
                fragmentSpan.CopyTo(rentedFragmentBuffer);

                var inboundItem = new InboundFragment(rentedFragmentBuffer, fragmentSpan.Length);

                if (!peerState.Channel.Writer.TryWrite(inboundItem))
                {
                    _fragmentPool.Return(rentedFragmentBuffer);

                    if (Volatile.Read(ref peerState.IsTerminated) == 0)
                    {
                        TriggerSever(senderId, "Inbound fragment queue overrun. Backpressure failure.");
                    }
                }
            }
            finally
            {
                Exit();
            }
        }

        private PeerChannelState CreatePeerChannel(ushort clientId)
        {
            var options = new BoundedChannelOptions(_config.MaxPacketCount * 4)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            };

            var channel = Channel.CreateBounded<InboundFragment>(options);
            var state = new PeerChannelState(clientId, channel);

            state.WorkerTask = Task.Run(() => ProcessPeerFragmentsAsync(state));
            return state;
        }
        private async Task ProcessPeerFragmentsAsync(PeerChannelState state)
        {
            var reader = state.Channel.Reader;
            var assemblies = new Dictionary<ushort, PendingAssembly>();
            var token = state.LifetimeCts.Token;

            int effectiveMaxFragmentPayload = _mtu - _transportHeaderSize - FragmentHeaderSize;

            try
            {
                while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (reader.TryRead(out InboundFragment item))
                    {
                        try
                        {
                            if (!ProcessFragmentSync(state, item, assemblies, effectiveMaxFragmentPayload))
                            {
                                return;
                            }
                        }
                        finally
                        {
                            _fragmentPool.Return(item.Buffer);
                        }
                    }

                    PruneStaleAssemblies(assemblies);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Fragmentor-Consumer] Worker failed for {state.ClientId}: {ex}");
                TriggerSever(state.ClientId, "Assembly worker crash.");
            }
            finally
            {
                foreach (var pending in assemblies.Values)
                {
                    if (pending.Buffer != null)
                    {
                        _assemblyPool.Return(pending.Buffer);

                        pending.Buffer = null!;
                    }
                }
                assemblies.Clear();

                while (reader.TryRead(out InboundFragment leftover))
                {
                    _fragmentPool.Return(leftover.Buffer);
                }
            }
        }

        private bool ProcessFragmentSync(PeerChannelState state, InboundFragment item, Dictionary<ushort, PendingAssembly> assemblies, int effectiveMaxFragmentPayload)
        {
            Span<byte> fragmentSpan = item.Buffer.AsSpan(0, item.Length);

            ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(0, 2));
            ushort fragmentIndex = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(2, 2));
            ushort totalFragments = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(4, 2));
            Span<byte> payloadChunk = fragmentSpan.Slice(FragmentHeaderSize);

            if (totalFragments == 0 || fragmentIndex >= totalFragments)
            {
                TriggerSever(state.ClientId, $"Corrupt indices ({fragmentIndex}/{totalFragments}) on seq {seq}.");
                return false;
            }

            bool isLastFragment = fragmentIndex == totalFragments - 1;

            if (!isLastFragment && payloadChunk.Length != effectiveMaxFragmentPayload)
            {
                TriggerSever(state.ClientId, $"Intermediate fragment {fragmentIndex} invalid payload length ({payloadChunk.Length}b != {effectiveMaxFragmentPayload}b).");
                return false;
            }

            if (isLastFragment && payloadChunk.Length > effectiveMaxFragmentPayload)
            {
                TriggerSever(state.ClientId, $"Terminal fragment {fragmentIndex} payload length ({payloadChunk.Length}b) exceeds max ({effectiveMaxFragmentPayload}b).");
                return false;
            }

            long now = Stopwatch.GetTimestamp();

            if (assemblies.TryGetValue(seq, out var assembly))
            {
                if (now - assembly.LastActivityTicks > AssemblyTimeoutTicks || assembly.TotalFragments != totalFragments)
                {
                    if (assembly.Buffer != null)
                    {
                        _assemblyPool.Return(assembly.Buffer);
                        assembly.Buffer = null!;
                    }
                    assemblies.Remove(seq);
                    assembly = null;
                }
            }

            if (assembly == null)
            {
                long calculatedMaxExpected = (long)totalFragments * effectiveMaxFragmentPayload;

                if (calculatedMaxExpected > _maxAssemblySize)
                {
                    TriggerSever(state.ClientId, $"Expected assembly size ({calculatedMaxExpected}b) exceeds max allowed packet size ({_maxAssemblySize}b).");
                    return false;
                }

                int maxExpected = (int)calculatedMaxExpected;
                byte[] rentedAssemblyBuffer = _assemblyPool.Rent(maxExpected);
                assembly = new PendingAssembly(rentedAssemblyBuffer, totalFragments);
                assemblies[seq] = assembly;
            }

            if (assembly.Received[fragmentIndex])
            {
                TriggerSever(state.ClientId, $"Duplicate fragment {fragmentIndex} for seq {seq}. Wire sequence invalid.");
                return false;
            }

            int targetOffset = fragmentIndex * effectiveMaxFragmentPayload;
            if (targetOffset + payloadChunk.Length > assembly.Buffer.Length)
            {
                TriggerSever(state.ClientId, $"Buffer overrun on seq {seq}. Bounds calculation violation.");
                return false;
            }

            payloadChunk.CopyTo(assembly.Buffer.AsSpan(targetOffset));
            assembly.Received[fragmentIndex] = true;
            assembly.FragmentsReceivedCount++;
            assembly.LastActivityTicks = now;

            if (isLastFragment)
            {
                assembly.FinalFragmentLength = payloadChunk.Length;
            }

            if (assembly.FragmentsReceivedCount == assembly.TotalFragments)
            {
                assemblies.Remove(seq);

                int totalReconstructedLength = ((assembly.TotalFragments - 1) * effectiveMaxFragmentPayload) + assembly.FinalFragmentLength;

                try
                {
                    OnMessageReassembled?.Invoke(assembly.Buffer.AsSpan(0, totalReconstructedLength), state.ClientId);
                }
                finally
                {
                    if (assembly.Buffer != null)
                    {
                        _assemblyPool.Return(assembly.Buffer);
                        assembly.Buffer = null!;
                    }
                }
            }

            return true;
        }

        private void PruneStaleAssemblies(Dictionary<ushort, PendingAssembly> assemblies)
        {
            if (assemblies.Count == 0) return;

            long now = Stopwatch.GetTimestamp();
            List<ushort>? expiredKeys = null;

            foreach (var (seq, pending) in assemblies)
            {
                if (now - pending.LastActivityTicks > AssemblyTimeoutTicks)
                {
                    (expiredKeys ??= new List<ushort>()).Add(seq);
                    if (pending.Buffer != null)
                    {
                        _assemblyPool.Return(pending.Buffer);

                        pending.Buffer = null!;
                    }
                }
            }

            if (expiredKeys != null)
            {
                for (int i = 0; i < expiredKeys.Count; i++)
                {
                    assemblies.Remove(expiredKeys[i]);
                }
            }
        }
        #endregion

        #region Kicking & Fault Invalidation

        private void TriggerSever(ushort clientId, string reason)
        {
            LiminalLogger.LogError($"[Fragmentor-Fault] Severing client {clientId}. Reason: {reason}");

            HandleClientCleanup(clientId);

            if (_transport.IsServer)
            {
                _transport.Kick(clientId);
            }
            else
            {
                _transport.Shutdown();
            }
        }

        private void HandleClientCleanup(ushort clientId)
        {
            _outboundSequences.TryRemove(clientId, out _);

            PeerChannelState? peerState;
            lock (_peerChannels)
            {
                _peerChannels.TryRemove(clientId, out peerState);
            }

            if (peerState != null && Interlocked.Exchange(ref peerState.IsTerminated, 1) == 0)
            {
                try { peerState.LifetimeCts.Cancel(); } catch (ObjectDisposedException) { }
                peerState.Channel.Writer.TryComplete();

                var worker = peerState.WorkerTask;
                if (worker != null)
                {
                    _ = worker.ContinueWith(_ => peerState.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
                }
                else
                {
                    peerState.Dispose();
                }
            }
        }

        public void Dispose()
        {
            int current;
            while (true)
            {
                current = Volatile.Read(ref _disposedState);
                if (current < 0) return; // Already disposing/disposed

                if (Interlocked.CompareExchange(ref _disposedState, current + DisposalSignaled, current) == current)
                {
                    break;
                }
            }

            var spinner = new SpinWait();
            while (Volatile.Read(ref _disposedState) != DisposalSignaled)
            {
                spinner.SpinOnce();
            }

            _transport.OnClientDisconnected -= HandleClientCleanup;
            _transport.OnClientKicked -= HandleClientCleanup;

            foreach (var id in _peerChannels.Keys)
            {
                HandleClientCleanup(id);
            }

            _peerChannels.Clear();
            _outboundSequences.Clear();
        }

        #endregion

        private readonly struct InboundFragment
        {
            public readonly byte[] Buffer;
            public readonly int Length;

            public InboundFragment(byte[] buffer, int length)
            {
                Buffer = buffer;
                Length = length;
            }
        }

        private sealed class PeerChannelState : IDisposable
        {
            public readonly ushort ClientId;
            public readonly Channel<InboundFragment> Channel;
            public readonly CancellationTokenSource LifetimeCts;
            public Task WorkerTask;
            public int IsTerminated = 0;

            public PeerChannelState(ushort clientId, Channel<InboundFragment> channel)
            {
                ClientId = clientId;
                Channel = channel;
                LifetimeCts = new CancellationTokenSource();
            }

            public void Dispose()
            {
                LifetimeCts.Dispose();
            }
        }

        private sealed class PendingAssembly
        {
            public byte[]? Buffer;
            public readonly bool[] Received;
            public readonly ushort TotalFragments;
            public ushort FragmentsReceivedCount;
            public int FinalFragmentLength;
            public long LastActivityTicks;

            public PendingAssembly(byte[] buffer, ushort totalFragments)
            {
                Buffer = buffer;
                TotalFragments = totalFragments;
                Received = new bool[totalFragments];
                LastActivityTicks = Stopwatch.GetTimestamp();
            }
        }
    }
}