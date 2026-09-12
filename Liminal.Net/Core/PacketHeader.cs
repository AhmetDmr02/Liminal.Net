using System;

namespace Liminal.Net.Core
{
    [AttributeUsage(AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class LiminalPacketAttribute : Attribute
    {
        /// <summary>
        /// Leave 0 (default) for every normal packet; those get an automatic,
        /// deterministic sequential id from reflection-based discovery.
        /// </summary>
        public ushort ReservedId { get; set; } = 0;

        public LiminalPacketAttribute() { }

        public LiminalPacketAttribute(ushort reservedId)
        {
            ReservedId = reservedId;
        }
    }
}