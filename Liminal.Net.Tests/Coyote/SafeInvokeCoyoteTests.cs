using Liminal.Net.Misc;
using Microsoft.Coyote.Specifications;
using TestAttribute = Microsoft.Coyote.SystematicTesting.TestAttribute;

namespace Liminal.Net.Tests.Coyote
{
    public static class SafeInvokeCoyoteTests
    {
        private sealed class EventOwner
        {
            public Action<int> Event;
        }

        //Handlers come and go while the event fires, a handler that was there the whole time runs once per raise
        //and a throwing handler never stops the others
        [Test]
        public static async Task Coyote_SafeInvoke_SubscribeUnsubscribeVsRaise_StableHandlersRunOncePerRaise()
        {
            var owner = new EventOwner();
            int stableCalls = 0;
            int transientCalls = 0;
            Exception failure = null;

            Action<int> thrower = _ => throw new InvalidOperationException("handler failed");
            Action<int> stable = _ => Interlocked.Increment(ref stableCalls);
            Action<int> transient = _ => Interlocked.Increment(ref transientCalls);

            LiminalAtomicHelpers.SafeAdd(ref owner.Event, thrower);
            LiminalAtomicHelpers.SafeAdd(ref owner.Event, stable);

            const int raises = 3;
            var raiser = Task.Run(async () =>
            {
                try
                {
                    for (int i = 0; i < raises; i++)
                    {
                        LiminalAtomicHelpers.SafeInvoke(owner.Event, i);
                        await Task.Yield();
                    }
                }
                catch (Exception ex) { failure = ex; }
            });

            var churn = Task.Run(async () =>
            {
                try
                {
                    for (int i = 0; i < 2; i++)
                    {
                        LiminalAtomicHelpers.SafeAdd(ref owner.Event, transient);
                        await Task.Yield();
                        LiminalAtomicHelpers.SafeRemove(ref owner.Event, transient);
                    }
                }
                catch (Exception ex) { failure = ex; }
            });

            await Task.WhenAll(raiser, churn);

            Specification.Assert(failure == null, $"Unexpected exception: {failure}");
            Specification.Assert(stableCalls == raises, $"Stable handler ran {stableCalls} times over {raises} raises.");
            Specification.Assert(transientCalls <= raises, $"Transient handler ran {transientCalls} times, more than {raises} raises.");

            LiminalAtomicHelpers.SafeInvoke(owner.Event, 99);
            Specification.Assert(stableCalls == raises + 1, "A throwing handler stopped the stable handler from running.");
            Specification.Assert(transientCalls <= raises, "A removed handler still ran.");
        }
    }
}
