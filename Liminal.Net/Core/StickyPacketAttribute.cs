using System;

namespace Liminal.Net.Core
{
    [AttributeUsage(AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class StickyPacketAttribute : Attribute
    {
        public ushort ReservedId { get; set; } = 0;

        public StickyPacketAttribute() { }

        public StickyPacketAttribute(ushort reservedId)
        {
            ReservedId = reservedId;
        }
    }
}
