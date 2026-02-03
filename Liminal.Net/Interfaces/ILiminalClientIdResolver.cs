namespace Liminal.Net.Interfaces
{
    public interface ILiminalClientIdResolver
    {
        public ushort ResolveClientId(Span<byte> payload);
        public void ReleaseId(ushort clientId);
    }
}
