using System;
using System.Reflection;
using System.Runtime.CompilerServices;
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

        private static readonly FieldInfo _invocationListField =
            typeof(MulticastDelegate).GetField("_invocationList", BindingFlags.NonPublic | BindingFlags.Instance);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsMulticast(Delegate d)
        {
            if (d == null) return false;
            return _invocationListField != null && _invocationListField.GetValue(d) != null;
        }

        public static void SafeInvoke(Action action)
        {
            if (action == null) return;

            if (!IsMulticast(action))
            {
                try
                {
                    action();
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
                return;
            }

            var list = action.GetInvocationList();
            for (int i = 0; i < list.Length; i++)
            {
                try
                {
                    ((Action)list[i])();
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
            }
        }

        public static void SafeInvoke<T>(Action<T> action, T arg)
        {
            if (action == null) return;

            if (!IsMulticast(action))
            {
                try
                {
                    action(arg);
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
                return;
            }

            var list = action.GetInvocationList();
            for (int i = 0; i < list.Length; i++)
            {
                try
                {
                    ((Action<T>)list[i])(arg);
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
            }
        }

        public static void SafeInvoke<T1, T2, T3>(Action<T1, T2, T3> action, T1 arg1, T2 arg2, T3 arg3)
        {
            if (action == null) return;

            if (!IsMulticast(action))
            {
                try
                {
                    action(arg1, arg2, arg3);
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
                return;
            }

            var list = action.GetInvocationList();
            for (int i = 0; i < list.Length; i++)
            {
                try
                {
                    ((Action<T1, T2, T3>)list[i])(arg1, arg2, arg3);
                }
                catch (Exception ex)
                {
                    Liminal.Net.Core.LiminalLogger.LogError($"[EventHub] Exception in event handler: {ex}");
                }
            }
        }
    }
}
