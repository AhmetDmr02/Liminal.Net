using Liminal.Net.Interfaces;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Liminal.Net.Core
{
    public sealed class LiminalPacketFragmentor : IDisposable
    {
        public const int FragmentHeaderSize = 6; // ReliableSeq(2) + FragmentIndex(2) + TotalFragments(2)

        //Normal sender finishes one fragmented batch before starting the next one, more than this is abuse
        internal const int MaxPendingAssembliesPerPeer = 8;

        private readonly ILiminalTransport _transport;
        private readonly LiminalNetworkConfig _config;
        private readonly int _mtu;
        private readonly int _transportHeaderSize;
        private readonly ArrayPool<byte> _assemblyPool;
        private readonly int _maxAssemblySize;
        private readonly long _assemblyTimeoutTicks;

        // 0 = Active, 1 = Disposed
        private int _disposed = 0;
        public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        private readonly ConcurrentDictionary<ushort, PeerAssemblyState> _peers = new();
        private readonly ConcurrentDictionary<ushort, ushort> _outboundSequences = new();
        private static readonly Func<ushort, PeerAssemblyState> PeerFactory = id => new PeerAssemblyState(id);

        private int _peersWithPendingAssemblies;
        private int _outstandingAssemblyBuffers;

        internal int OutstandingAssemblyBuffers => Volatile.Read(ref _outstandingAssemblyBuffers);
        internal int PeersWithPendingAssemblies => Volatile.Read(ref _peersWithPendingAssemblies);

        public int MTU => _mtu;

        public event DataReceivedHandler OnMessageReassembled;

        public LiminalPacketFragmentor(ILiminalTransport transport, LiminalNetworkConfig config, int mtu = 1200)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            _mtu = mtu;
            _transportHeaderSize = LiminalTransportHeader.BaseHeaderSize + (config.TransportFramingProvider?.CustomHeaderSize ?? 0);

            int minimumMtu = FragmentHeaderSize + _transportHeaderSize;
            if (mtu <= minimumMtu)
            {
                throw new ArgumentException($"MTU ({mtu}b) must be larger than all combined headers ({minimumMtu}b)", nameof(mtu));
            }

            double timeoutSeconds = config.ReceiveResponseTimeout > 0 ? config.ReceiveResponseTimeout : 5.0;
            _assemblyTimeoutTicks = (long)(Stopwatch.Frequency * timeoutSeconds);

            int maxRecovery = config.Hiccup?.GetRecoverySize(config.MaxPacketSizePerBatch) ?? config.MaxPacketSizePerBatch;
            _maxAssemblySize = maxRecovery;

            _assemblyPool = ArrayPool<byte>.Create(maxRecovery, _config.MaxConnectionCount * 2);

            _transport.OnClientConnected += HandleClientCleanup;
            _transport.OnLocalClientConnected += HandleLocalClientConnectionChanged;
            _transport.OnClientDisconnected += HandleClientCleanup;
            _transport.OnClientKicked += HandleClientCleanup;
            _transport.OnLocalClientDisconnected += HandleLocalClientConnectionChanged;
            _transport.OnMessageReceivedFragmented += IngestFragment;
        }

        #region Outbound / Send Path

        public void Send(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            if (Volatile.Read(ref _disposed) != 0) return;

            int effectivePayloadMtu = _mtu - _transportHeaderSize;

            if (data.Length <= effectivePayloadMtu)
            {
                if ((flags & TransportFlags.Reliable) != 0)
                    _transport.Send(data, targetId, TransportFlags.Reliable);
                else
                    _transport.Send(data, targetId, TransportFlags.Unreliable);

                return;
            }

            if ((flags & TransportFlags.Reliable) == 0)
            {
                LiminalLogger.LogWarning($"[Fragmentor] Payload ({data.Length}b) exceeds MTU ({_mtu}b) on Unreliable channel. Auto-swapping to Reliable + Fragmented.");
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
                    // If disposed while fragmenting, abort early
                    if (Volatile.Read(ref _disposed) != 0) return;

                    int offset = index * maxFragmentPayload;
                    int length = Math.Min(maxFragmentPayload, data.Length - offset);

                    BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(0, 2), seq);
                    BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(2, 2), index);
                    BinaryPrimitives.WriteUInt16LittleEndian(chunkSpan.Slice(4, 2), (ushort)totalFragments);

                    data.Slice(offset, length).CopyTo(chunkSpan.Slice(FragmentHeaderSize));

                    Span<byte> outgoingFrame = chunkSpan.Slice(0, FragmentHeaderSize + length);

                    _transport.Send(outgoingFrame, targetId, flags);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedChunkBuffer);
            }
        }

        #endregion

        #region Inbound / Assembly Path

        //Reassembly and delivery run on the peer's receive thread, this keeps fragmented and normal reliable batches in wire order
        //dont move delivery to another thread
        public void IngestFragment(ReadOnlySpan<byte> fragmentSpan, ushort senderId)
        {
            if (Volatile.Read(ref _disposed) != 0) return;

            if (fragmentSpan.Length < FragmentHeaderSize || fragmentSpan.Length > _mtu)
            {
                TriggerSever(senderId, $"Fragment payload ({fragmentSpan.Length}b) is out of valid bounds [{FragmentHeaderSize}b - {_mtu}b].");
                return;
            }

            if (!_transport.IsClientConnected(senderId)) return;

            var peer = _peers.GetOrAdd(senderId, PeerFactory);
            long now = Stopwatch.GetTimestamp();
            PendingAssembly completed = null;
            string severReason;

            lock (peer.Lock)
            {
                if (peer.IsTerminated || Volatile.Read(ref _disposed) != 0) return;

                //Checked after the peer is published, transports remove the socket before raising the disconnect that cleans peers up
                //so a peer made after that cleanup always sees the client gone here
                if (!_transport.IsClientConnected(senderId))
                {
                    TerminatePeerLocked(peer);
                    ((ICollection<KeyValuePair<ushort, PeerAssemblyState>>)_peers)
                        .Remove(new KeyValuePair<ushort, PeerAssemblyState>(senderId, peer));
                    return;
                }

                severReason = FindStaleAssemblyLocked(peer, now) ?? AddFragmentLocked(peer, fragmentSpan, now, out completed);
                if (severReason != null)
                {
                    TerminatePeerLocked(peer);
                }
            }

            if (severReason != null)
            {
                SeverTerminatedPeer(peer, severReason);
                return;
            }

            if (completed == null) return;

            try
            {
                OnMessageReassembled?.Invoke(completed.Buffer.AsSpan(0, completed.ReassembledLength), senderId);
            }
            catch (Exception ex)
            {
                LiminalLogger.LogError($"[Fragmentor] Reassembled message handler failed for {senderId}: {ex}");
                TriggerSever(peer, "Reassembled message handler failure.");
            }
            finally
            {
                ReturnAssemblyBuffer(completed);
            }
        }

        //Returns sever reason or null, completed assembly gets detached first so a cleanup during delivery cant return its buffer
        private string AddFragmentLocked(PeerAssemblyState peer, ReadOnlySpan<byte> fragmentSpan, long now, out PendingAssembly completed)
        {
            completed = null;

            ushort seq = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(0, 2));
            ushort fragmentIndex = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(2, 2));
            ushort totalFragments = BinaryPrimitives.ReadUInt16LittleEndian(fragmentSpan.Slice(4, 2));
            ReadOnlySpan<byte> payloadChunk = fragmentSpan.Slice(FragmentHeaderSize);

            int effectiveMaxFragmentPayload = _mtu - _transportHeaderSize - FragmentHeaderSize;

            if (totalFragments == 0 || fragmentIndex >= totalFragments)
                return $"Corrupt indices ({fragmentIndex}/{totalFragments}) on seq {seq}.";

            bool isLastFragment = fragmentIndex == totalFragments - 1;

            if (!isLastFragment && payloadChunk.Length != effectiveMaxFragmentPayload)
                return $"Intermediate fragment {fragmentIndex} invalid payload length ({payloadChunk.Length}b != {effectiveMaxFragmentPayload}b).";

            if (isLastFragment && payloadChunk.Length > effectiveMaxFragmentPayload)
                return $"Terminal fragment {fragmentIndex} payload length ({payloadChunk.Length}b) exceeds max ({effectiveMaxFragmentPayload}b).";

            if (peer.Assemblies.TryGetValue(seq, out var assembly))
            {
                if (assembly.TotalFragments != totalFragments)
                    return $"Sequence collision or total fragment mismatch on seq {seq} ({totalFragments} != {assembly.TotalFragments}).";
            }
            else
            {
                if (peer.Assemblies.Count >= MaxPendingAssembliesPerPeer)
                    return $"Too many concurrent fragment assemblies ({peer.Assemblies.Count}).";

                long calculatedMaxExpected = (long)totalFragments * effectiveMaxFragmentPayload;
                if (calculatedMaxExpected > _maxAssemblySize)
                    return $"Expected assembly size ({calculatedMaxExpected}b) exceeds max allowed packet size ({_maxAssemblySize}b).";

                assembly = new PendingAssembly(RentAssemblyBuffer((int)calculatedMaxExpected), totalFragments, now);
                peer.Assemblies[seq] = assembly;
                UpdatePendingCountLocked(peer);
            }

            if (assembly.Received[fragmentIndex])
                return $"Duplicate fragment {fragmentIndex} for seq {seq}. Wire sequence invalid.";

            int targetOffset = fragmentIndex * effectiveMaxFragmentPayload;
            if (targetOffset + payloadChunk.Length > assembly.Buffer.Length)
                return $"Buffer overrun on seq {seq}. Bounds calculation violation.";

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
                peer.Assemblies.Remove(seq);
                UpdatePendingCountLocked(peer);
                assembly.ReassembledLength = ((assembly.TotalFragments - 1) * effectiveMaxFragmentPayload) + assembly.FinalFragmentLength;
                completed = assembly;
            }

            return null;
        }

        private string FindStaleAssemblyLocked(PeerAssemblyState peer, long now)
        {
            foreach (var pair in peer.Assemblies)
            {
                if (now - pair.Value.LastActivityTicks >= _assemblyTimeoutTicks)
                {
                    return $"Fragment reassembly timed out on sequence {pair.Key}.";
                }
            }
            return null;
        }

        private void UpdatePendingCountLocked(PeerAssemblyState peer)
        {
            int previous = peer.PendingCount;
            int current = peer.Assemblies.Count;
            if (previous == current) return;

            Volatile.Write(ref peer.PendingCount, current);

            if (previous == 0)
                Interlocked.Increment(ref _peersWithPendingAssemblies);
            else if (current == 0)
                Interlocked.Decrement(ref _peersWithPendingAssemblies);
        }

        /// <summary>
        /// Kicks peers whose half finished assemblies stopped getting fragments, session manager calls this every poll
        /// </summary>
        public void PruneStaleAssemblies() => PruneStaleAssemblies(Stopwatch.GetTimestamp());

        internal void PruneStaleAssemblies(long now)
        {
            if (Volatile.Read(ref _disposed) != 0) return;
            if (Volatile.Read(ref _peersWithPendingAssemblies) == 0) return;

            foreach (var pair in _peers)
            {
                var peer = pair.Value;
                if (Volatile.Read(ref peer.PendingCount) == 0) continue;

                string severReason;
                lock (peer.Lock)
                {
                    if (peer.IsTerminated) continue;
                    severReason = FindStaleAssemblyLocked(peer, now);
                    if (severReason != null)
                    {
                        TerminatePeerLocked(peer);
                    }
                }

                if (severReason != null)
                {
                    SeverTerminatedPeer(peer, severReason);
                }
            }
        }

        #endregion

        #region Kicking & Fault Invalidation

        private void TriggerSever(ushort clientId, string reason)
        {
            if (Volatile.Read(ref _disposed) != 0) return;

            LiminalLogger.LogError($"[Fragmentor-Fault] Severing client {clientId}. Reason: {reason}");

            HandleClientCleanup(clientId);
            KickOrShutdown(clientId);
        }

        private void TriggerSever(PeerAssemblyState peer, string reason)
        {
            TerminatePeer(peer);
            SeverTerminatedPeer(peer, reason);
        }

        //Decide and terminate under the same peer lock or a completing fragment could slip in and get delivered for a peer we are severing
        private void SeverTerminatedPeer(PeerAssemblyState peer, string reason)
        {
            ((ICollection<KeyValuePair<ushort, PeerAssemblyState>>)_peers)
                .Remove(new KeyValuePair<ushort, PeerAssemblyState>(peer.ClientId, peer));

            if (Volatile.Read(ref _disposed) != 0) return;

            LiminalLogger.LogError($"[Fragmentor-Fault] Severing client {peer.ClientId}. Reason: {reason}");

            _outboundSequences.TryRemove(peer.ClientId, out _);
            KickOrShutdown(peer.ClientId);
        }

        private void KickOrShutdown(ushort clientId)
        {
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

            if (_peers.TryRemove(clientId, out var peer))
            {
                TerminatePeer(peer);
            }
        }

        private void TerminatePeer(PeerAssemblyState peer)
        {
            lock (peer.Lock)
            {
                TerminatePeerLocked(peer);
            }
        }

        private void TerminatePeerLocked(PeerAssemblyState peer)
        {
            if (peer.IsTerminated) return;
            peer.IsTerminated = true;

            foreach (var pair in peer.Assemblies)
            {
                ReturnAssemblyBuffer(pair.Value);
            }

            peer.Assemblies.Clear();
            UpdatePendingCountLocked(peer);
        }

        private void HandleLocalClientConnectionChanged(ushort localId)
        {
            HandleClientCleanup(ILiminalTransport.SERVER_ID);
        }

        private byte[] RentAssemblyBuffer(int size)
        {
            var buffer = _assemblyPool.Rent(size);
            Interlocked.Increment(ref _outstandingAssemblyBuffers);
            return buffer;
        }

        //Only the thread owning the assembly calls this, under peer lock while its in the dictionary or the delivering thread after detach
        private void ReturnAssemblyBuffer(PendingAssembly assembly)
        {
            var buffer = assembly.Buffer;
            if (buffer == null) return;

            assembly.Buffer = null;
            _assemblyPool.Return(buffer);
            Interlocked.Decrement(ref _outstandingAssemblyBuffers);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _transport.OnClientConnected -= HandleClientCleanup;
            _transport.OnLocalClientConnected -= HandleLocalClientConnectionChanged;
            _transport.OnClientDisconnected -= HandleClientCleanup;
            _transport.OnClientKicked -= HandleClientCleanup;
            _transport.OnLocalClientDisconnected -= HandleLocalClientConnectionChanged;
            _transport.OnMessageReceivedFragmented -= IngestFragment;

            foreach (var id in _peers.Keys)
            {
                HandleClientCleanup(id);
            }

            _outboundSequences.Clear();
        }

        #endregion

        private sealed class PeerAssemblyState
        {
            public readonly ushort ClientId;
            public readonly object Lock = new();
            public readonly Dictionary<ushort, PendingAssembly> Assemblies = new();
            public bool IsTerminated;
            public int PendingCount;

            public PeerAssemblyState(ushort clientId)
            {
                ClientId = clientId;
            }
        }

        private sealed class PendingAssembly
        {
            public byte[]? Buffer;
            public readonly bool[] Received;
            public readonly ushort TotalFragments;
            public ushort FragmentsReceivedCount;
            public int FinalFragmentLength;
            public int ReassembledLength;
            public long LastActivityTicks;

            public PendingAssembly(byte[] buffer, ushort totalFragments, long now)
            {
                Buffer = buffer;
                TotalFragments = totalFragments;
                Received = new bool[totalFragments];
                LastActivityTicks = now;
            }
        }
    }
}
