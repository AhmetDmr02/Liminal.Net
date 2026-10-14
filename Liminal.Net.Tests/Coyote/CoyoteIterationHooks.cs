using Liminal.Net.SyncVar.Internal;
using Microsoft.Coyote.SystematicTesting;

namespace Liminal.Net.Tests.Coyote
{
    //Coyote allows one iteration dispose hook per assembly and it runs after every iteration of every test
    public static class CoyoteIterationHooks
    {
        [TestIterationDispose]
        public static void ResetProcessWideState()
        {
            SyncVarDirectory.ResetForTesting();
        }
    }
}
