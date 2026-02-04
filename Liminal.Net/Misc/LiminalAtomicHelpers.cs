namespace Liminal.Net.Misc
{
    public static class LiminalAtomicHelpers
    {
        #region Atomic Helper Methods
        public static void SafeAdd<TDelegate>(ref TDelegate field, TDelegate value) where TDelegate : Delegate
        {
            TDelegate current = field;
            while (true)
            {
                TDelegate combined = (TDelegate)Delegate.Combine(current, value);
                TDelegate original = Interlocked.CompareExchange(ref field, combined, current);
                if (original == current) break;
                current = original;
            }
        }
        public static void SafeRemove<TDelegate>(ref TDelegate field, TDelegate value) where TDelegate : Delegate
        {
            TDelegate current = field;
            while (true)
            {
                TDelegate removed = (TDelegate)Delegate.Remove(current, value);
                TDelegate original = Interlocked.CompareExchange(ref field, removed, current);
                if (original == current) break;
                current = original;
            }
        }
        #endregion
    }
}
