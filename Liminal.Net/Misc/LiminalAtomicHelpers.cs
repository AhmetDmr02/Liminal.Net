using System;
using System.Threading;

namespace Liminal.Net.Misc
{
    public static class LiminalAtomicHelpers
    {
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

        public static void SafeInvoke(Action action)
        {
            if (action == null) return;
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                var list = action.GetInvocationList();
                for (int i = 1; i < list.Length; i++)
                {
                    try
                    {
                        ((Action)list[i])();
                    }
                    catch (Exception nextEx)
                    {
                        Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {nextEx}");
                    }
                }
            }
        }

        public static void SafeInvoke<T>(Action<T> action, T arg)
        {
            if (action == null) return;
            try
            {
                action(arg);
            }
            catch (Exception ex)
            {
                Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                var list = action.GetInvocationList();
                for (int i = 1; i < list.Length; i++)
                {
                    try
                    {
                        ((Action<T>)list[i])(arg);
                    }
                    catch (Exception nextEx)
                    {
                        Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {nextEx}");
                    }
                }
            }
        }

        public static void SafeInvoke<T1, T2, T3>(Action<T1, T2, T3> action, T1 arg1, T2 arg2, T3 arg3)
        {
            if (action == null) return;
            try
            {
                action(arg1, arg2, arg3);
            }
            catch (Exception ex)
            {
                Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                var list = action.GetInvocationList();
                for (int i = 1; i < list.Length; i++)
                {
                    try
                    {
                        ((Action<T1, T2, T3>)list[i])(arg1, arg2, arg3);
                    }
                    catch (Exception nextEx)
                    {
                        Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {nextEx}");
                    }
                }
            }
        }
    }
}
