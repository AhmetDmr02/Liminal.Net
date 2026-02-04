using Liminal.Net.Core;

namespace Liminal.Net.Interfaces
{
    public interface ILiminalClientIdResolver
    {
        /// <summary>
        /// Resolves the client id from the payload 
        /// NOTE: It can only read from plain text part of the payload
        /// if the payload is encrypted, you need your own logic
        /// </summary>
        /// <param name="payload">The payload</param>
        /// <returns>The client id</returns>
        public ushort ResolveClientId(Span<byte> payload);

        /// <summary>
        /// Creates a new client id
        /// </summary>
        /// <returns>The client id</returns>
        public ushort GenerateClientId();

        /// <summary>
        /// Releases the client id
        /// </summary>
        /// <param name="clientId">The client id</param>
        public bool UnregisterClientId(ushort clientId);

        /// <summary>
        /// Registers the client
        /// </summary>
        public bool RegisterClient(ushort clientId,ConnectionPair connectionPair);
        public bool TryGetClient(ushort clientId, out ConnectionPair connectionPair);
        public bool IsClientActive(ushort clientId);
        public void ResetResolver();
    }
}
