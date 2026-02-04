using System;

namespace Liminal.Net.Core
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class LiminalPacketAttribute : Attribute
    {
        public int Id { get; }

        public LiminalPacketAttribute(int id)
        {
            Id = id;
        }
    }
}