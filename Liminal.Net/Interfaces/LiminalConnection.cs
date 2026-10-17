using System;

namespace Liminal.Net.Interfaces
{
    /// <summary>
    /// One transport connection. ClientId says which player, Generation which socket, a reconnect under the same client id gets a new generation.
    /// default means no connection. Holding one after its connection ended is safe, scoped calls with it just do nothing.
    /// For the local client's own connection ClientId is ILiminalTransport.SERVER_ID, the peer it talks to.
    /// </summary>
    public readonly struct LiminalConnection : IEquatable<LiminalConnection>
    {
        public readonly ushort ClientId;
        public readonly uint Generation;

        public LiminalConnection(ushort clientId, uint generation)
        {
            ClientId = clientId;
            Generation = generation;
        }

        public bool IsValid => Generation != 0;

        //Both fields packed, for dictionary keys that should not call back into our Equals
        internal ulong Key => ((ulong)ClientId << 32) | Generation;

        internal static LiminalConnection FromKey(ulong key) => new LiminalConnection((ushort)(key >> 32), (uint)key);

        public bool Equals(LiminalConnection other) => ClientId == other.ClientId && Generation == other.Generation;
        public override bool Equals(object obj) => obj is LiminalConnection other && Equals(other);
        public override int GetHashCode() => (int)Generation * 397 ^ ClientId;

        public static bool operator ==(LiminalConnection left, LiminalConnection right) => left.Equals(right);
        public static bool operator !=(LiminalConnection left, LiminalConnection right) => !left.Equals(right);

        public override string ToString() => IsValid ? $"{ClientId}#{Generation}" : $"{ClientId}#none";
    }
}
