using Liminal.Net.SyncVar;
using MessagePack;
using NUnit.Framework;
using System;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class SyncVarTokenCacheTests
    {
        private static T RoundTrip<T>(T packet) => MessagePackSerializer.Deserialize<T>(MessagePackSerializer.Serialize(packet));

        [Test]
        public void TokensFromTheNetwork_AreNotAddedToTheProcessWideCache()
        {
            int before = Utf8TokenRegistry.CachedTokenCount;

            for (int i = 0; i < 300; i++)
            {
                string junk = $"junk_{Guid.NewGuid():N}";
                Assert.That(RoundTrip(new SyncVarEvictPacket(junk)).Token, Is.EqualTo(junk));
                Assert.That(RoundTrip(new SyncVarClearPacket(junk)).Token, Is.EqualTo(junk));
                Assert.That(RoundTrip(new SyncVarClearAckPacket(junk)).Token, Is.EqualTo(junk));
            }

            Assert.That(Utf8TokenRegistry.CachedTokenCount, Is.EqualTo(before), "Names received from the network were cached.");
        }

        [Test]
        public void TokensOfLocalSyncVars_DecodeToTheCachedString()
        {
            string token = $"cached_{Guid.NewGuid():N}";
            var syncVar = new SyncVar<int>(token, 1, new SyncVarManager(null, new Liminal.Net.Core.LiminalNetworkConfig()));

            string decoded = RoundTrip(new SyncVarEvictPacket(token)).Token;

            Assert.That(decoded, Is.EqualTo(syncVar.Token));
            Assert.That(ReferenceEquals(decoded, syncVar.Token), Is.True, "A known name was decoded into a new string instead of the cached one.");
        }
    }
}
