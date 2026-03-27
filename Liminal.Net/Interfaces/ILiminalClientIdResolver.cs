using Liminal.Net.Core;
using System;

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
        public ushort ResolveId(Span<byte> payload);

        /// <summary>
        /// Creates a new client id
        /// </summary>
        /// <returns>The client id</returns>
        public ushort GenerateClientId();

        /// <summary>
        /// Releases the client id
        /// </summary>
        /// <param name="targetId">The client id</param>
        public bool UnregisterId(ushort targetId);

        /// <summary>
        /// Registers the client
        /// </summary>
        public bool RegisterId(ushort targetId,ConnectionPair connectionPair);
        public bool TryGetConnectionPair(ushort targetId, out ConnectionPair connectionPair);
        public bool IsConnectionActive(ushort targetId);
        public void ResetResolver();
    }
}
