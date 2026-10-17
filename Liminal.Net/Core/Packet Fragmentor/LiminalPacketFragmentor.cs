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

        //Per connection, a reconnect under the same client id starts clean and an old connection's leftovers cant sever or desync it
        //keyed by LiminalConnection.Key so the dictionary compares plain ulongs and no code of ours runs inside its internal locks
        private readonly ConcurrentDictionary<ulong, PeerAssemblyState> _peers = new();
        private readonly ConcurrentDictionary<ulong, ushort> _outboundSequences = new();

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

            _transport.OnClientConnected += HandleClientConnected;
            _transport.OnLocalClientConnected += HandleLocalClientConnectionChanged;
            _transport.OnClientDisconnected += HandleConnectionEnded;
            _transport.OnClientKicked += HandleConnectionEnded;
            _transport.OnLocalClientDisconnected += HandleLocalClientConnectionChanged;
            _transport.OnMessageReceivedFragmented += IngestFragment;
        }

        #region Outbound / Send Path

        public void Send(Span<byte> data, LiminalConnection to, TransportFlags flags)
        {
            if (Volatile.Read(ref _disposed) != 0) return;

            int effectivePayloadMtu = _mtu - _transportHeaderSize;

            if (data.Length <= effectivePayloadMtu)
            {
                if ((flags & TransportFlags.Reliable) != 0)
                    _transport.Send(data, to, TransportFlags.Reliable);
                else
                    _transport.Send(data, to, TransportFlags.Unreliable);

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
                TriggerSever(to, "Fragment count exceeded 65535.");
                return;
            }

            ushort seq = _outboundSequences.AddOrUpdate(to.Key, 1, (_, current) => unchecked((ushort)(current + 1)));

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

                    _transport.Send(outgoingFrame, to, flags);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedChunkBuffer);
            }
        }

        //For tests driving the fragmentor by client id, an id with no connection keeps its id so test transports can still record it
        internal void Send(Span<byte> data, ushort targetId, TransportFlags flags)
        {
            var to = _transport.GetConnection(targetId);
            Send(data, to.IsValid ? to : new LiminalConnection(targetId, 0), flags);
        }

        #endregion

        #region Inbound / Assembly Path

        //Reassembly and delivery run on the peer's receive thread, this keeps fragmented and normal reliable batches in wire order
        //dont move delivery to another thread
        public void IngestFragment(ReadOnlySpan<byte> fragmentSpan, LiminalConnection from)
        {
            if (Volatile.Read(ref _disposed) != 0 || !from.IsValid) return;

            ushort senderId = from.ClientId;

            if (fragmentSpan.Length < FragmentHeaderSize || fragmentSpan.Length > _mtu)
            {
                TriggerSever(from, $"Fragment payload ({fragmentSpan.Length}b) is out of valid bounds [{FragmentHeaderSize}b - {_mtu}b].");
                return;
            }

            //An old socket still working through bytes it buffered, its fragments belong to nobody now
            if (_transport.GetConnection(senderId) != from) return;

            var peer = _peers.GetOrAdd(from.Key, _ => new PeerAssemblyState(from));
            long now = Stopwatch.GetTimestamp();
            PendingAssembly completed = null;
            string severReason;

            lock (peer.Lock)
            {
                if (peer.IsTerminated || Volatile.Read(ref _disposed) != 0) return;

                //Checked after the peer is published, transports remove the connection before raising the disconnect that cleans peers up
                //so a peer made after that cleanup always sees the connection gone here
                if (_transport.GetConnection(senderId) != from)
                {
                    TerminatePeerLocked(peer);
                    ((ICollection<KeyValuePair<ulong, PeerAssemblyState>>)_peers)
                        .Remove(new KeyValuePair<ulong, PeerAssemblyState>(from.Key, peer));
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
                OnMessageReassembled?.Invoke(completed.Buffer.AsSpan(0, completed.ReassembledLength), from);
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

        //For tests driving the fragmentor by client id, takes whatever connection holds the id now
        internal void IngestFragment(ReadOnlySpan<byte> fragmentSpan, ushort senderId) =>
            IngestFragment(fragmentSpan, _transport.GetConnection(senderId));

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

        private void TriggerSever(LiminalConnection connection, string reason)
        {
            if (Volatile.Read(ref _disposed) != 0) return;

            LiminalLogger.LogError($"[Fragmentor-Fault] Severing client {connection}. Reason: {reason}");

            HandleConnectionEnded(connection);
            KickOrDisconnect(connection);
        }

        private void TriggerSever(PeerAssemblyState peer, string reason)
        {
            TerminatePeer(peer);
            SeverTerminatedPeer(peer, reason);
        }

        //Decide and terminate under the same peer lock or a completing fragment could slip in and get delivered for a peer we are severing
        private void SeverTerminatedPeer(PeerAssemblyState peer, string reason)
        {
            ((ICollection<KeyValuePair<ulong, PeerAssemblyState>>)_peers)
                .Remove(new KeyValuePair<ulong, PeerAssemblyState>(peer.Connection.Key, peer));

            if (Volatile.Read(ref _disposed) != 0) return;

            LiminalLogger.LogError($"[Fragmentor-Fault] Severing client {peer.Connection}. Reason: {reason}");

            _outboundSequences.TryRemove(peer.Connection.Key, out _);
            KickOrDisconnect(peer.Connection);
        }

        //Scoped to the connection, a sever decided for an old connection cant end a reconnect under the same id or a newer run
        private void KickOrDisconnect(LiminalConnection connection)
        {
            if (_transport.IsServer)
            {
                _transport.Kick(connection);
            }
            else
            {
                _transport.Disconnect(connection);
            }
        }

        private void HandleConnectionEnded(LiminalConnection connection)
        {
            _outboundSequences.TryRemove(connection.Key, out _);

            if (_peers.TryRemove(connection.Key, out var peer))
            {
                TerminatePeer(peer);
            }
        }

        //A replaced connection raises no disconnect, its state goes when the id shows up on a new one
        private void HandleClientConnected(LiminalConnection connection) => DropOtherConnectionsOf(connection.ClientId, connection);

        private void DropOtherConnectionsOf(ushort clientId, LiminalConnection keep)
        {
            foreach (var pair in _peers)
            {
                var connection = pair.Value.Connection;
                if (connection.ClientId == clientId && connection != keep)
                    HandleConnectionEnded(connection);
            }

            foreach (var pair in _outboundSequences)
            {
                var connection = LiminalConnection.FromKey(pair.Key);
                if (connection.ClientId == clientId && connection != keep)
                    _outboundSequences.TryRemove(pair.Key, out _);
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

        private void HandleLocalClientConnectionChanged(ushort localId, LiminalConnection server)
        {
            DropOtherConnectionsOf(ILiminalTransport.SERVER_ID, default);
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

            _transport.OnClientConnected -= HandleClientConnected;
            _transport.OnLocalClientConnected -= HandleLocalClientConnectionChanged;
            _transport.OnClientDisconnected -= HandleConnectionEnded;
            _transport.OnClientKicked -= HandleConnectionEnded;
            _transport.OnLocalClientDisconnected -= HandleLocalClientConnectionChanged;
            _transport.OnMessageReceivedFragmented -= IngestFragment;

            foreach (var pair in _peers)
            {
                HandleConnectionEnded(pair.Value.Connection);
            }

            _outboundSequences.Clear();
        }

        #endregion

        private sealed class PeerAssemblyState
        {
            public readonly LiminalConnection Connection;
            public readonly object Lock = new();
            public readonly Dictionary<ushort, PendingAssembly> Assemblies = new();
            public bool IsTerminated;
            public int PendingCount;

            public PeerAssemblyState(LiminalConnection connection)
            {
                Connection = connection;
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
