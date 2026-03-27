using System;

namespace Liminal.Net.Core
{
    public static class LiminalLogger
    {
        public static void Log(string message)
        {
#if UNITY_5_3_OR_NEWER
            Debug.Log(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine(message);
#endif
        }

        public static void LogWarning(string message)
        {
#if UNITY_5_3_OR_NEWER
            Debug.LogWarning(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine("WARNING: " + message);
#endif
        }

        public static void LogError(string message)
        {
#if UNITY_5_3_OR_NEWER
            Debug.LogError(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine("ERROR: " + message);
#endif
        }
    }
}
