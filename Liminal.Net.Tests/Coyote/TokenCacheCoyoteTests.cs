using Liminal.Net.SyncVar;
using MessagePack;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class TokenCacheCoyoteTests
    {
        private static T RoundTrip<T>(T packet) => MessagePackSerializer.Deserialize<T>(MessagePackSerializer.Serialize(packet));

        //Decode is lock free on a copy on write table, register publishes a new table under lock
        [Test]
        public static async Task Coyote_TokenCache_RegisterVsNetworkDecode_DecodesCorrectlyAndNeverCachesJunk()
        {
            string local = $"local_{Guid.NewGuid():N}";
            string junk = $"junk_{Guid.NewGuid():N}";
            int before = Utf8TokenRegistry.CachedTokenCount;
            string decodedLocal = null, decodedJunk = null, decodedLocalAgain = null;

            var register = Task.Run(() => Utf8TokenRegistry.RegisterToken(local));
            var decode = Task.Run(() =>
            {
                decodedLocal = RoundTrip(new SyncVarClearPacket(local)).Token;
                decodedJunk = RoundTrip(new SyncVarClearAckPacket(junk)).Token;
            });
            var decodeAgain = Task.Run(() => { decodedLocalAgain = RoundTrip(new SyncVarEvictPacket(local)).Token; });
            await Task.WhenAll(register, decode, decodeAgain);

            Specification.Assert(decodedLocal == local && decodedLocalAgain == local, "A local token decoded to the wrong string.");
            Specification.Assert(decodedJunk == junk, "A network token decoded to the wrong string.");
            Specification.Assert(Utf8TokenRegistry.CachedTokenCount == before + 1,
                $"Cache grew by {Utf8TokenRegistry.CachedTokenCount - before}, expected only the local token.");
            Specification.Assert(ReferenceEquals(RoundTrip(new SyncVarEvictPacket(local)).Token, local),
                "The registered token is not served from the cache.");
        }
    }
}
