using Liminal.Net.Interfaces;

namespace Liminal.Net.ClientIdResolvers
{
    public class BaseResolver : ILiminalClientIdResolver
    {
        public ushort GenerateClientId()
        {
            throw new NotImplementedException();
        }

        public virtual void ReleaseId(ushort clientId)
        {
            throw new NotImplementedException();
        }

        public virtual ushort ResolveClientId(Span<byte> payload)
        {
            throw new NotImplementedException();
        }
    }
}
