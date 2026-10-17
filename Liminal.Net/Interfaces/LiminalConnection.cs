using System;

namespace Liminal.Net.Interfaces
{
    /// <summary>
    /// One transport connection. PeerId is the id of whoever is on the other end, a client's id on a server and SERVER_ID on a client.
    /// Generation tells sockets apart, a reconnect under the same id gets a new one.
    /// default means no connection. Holding one after its connection ended is safe, scoped calls with it just do nothing.
    /// </summary>
    public readonly struct LiminalConnection : IEquatable<LiminalConnection>
    {
        public readonly ushort PeerId;
        public readonly uint Generation;

        public LiminalConnection(ushort peerId, uint generation)
        {
            PeerId = peerId;
            Generation = generation;
        }

        public bool IsValid => Generation != 0;

        //Both fields packed, for dictionary keys that should not call back into our Equals
        internal ulong Key => ((ulong)PeerId << 32) | Generation;

        internal static LiminalConnection FromKey(ulong key) => new LiminalConnection((ushort)(key >> 32), (uint)key);

        public bool Equals(LiminalConnection other) => PeerId == other.PeerId && Generation == other.Generation;
        public override bool Equals(object obj) => obj is LiminalConnection other && Equals(other);
        public override int GetHashCode() => (int)Generation * 397 ^ PeerId;

        public static bool operator ==(LiminalConnection left, LiminalConnection right) => left.Equals(right);
        public static bool operator !=(LiminalConnection left, LiminalConnection right) => !left.Equals(right);

        public override string ToString() => IsValid ? $"{PeerId}#{Generation}" : $"{PeerId}#none";
    }
}
