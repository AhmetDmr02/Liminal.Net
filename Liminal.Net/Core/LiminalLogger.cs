using System;

namespace Liminal.Net.Core
{
    public static class LiminalLogger
    {
        private static LogLevel _logLevel = LogLevel.Detailed;

        public enum LogLevel
        {
            Default = 0,
            Detailed = 1,
        }

        public static void SetLogLevel(LogLevel logLevel) => _logLevel = logLevel;

        public static void Log(string message, LogLevel logLevel = LogLevel.Default)
        {
            if (logLevel > _logLevel) return;

#if UNITY_5_3_OR_NEWER
            Debug.Log(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine(message);
#endif
        }

        public static void LogWarning(string message, LogLevel logLevel = LogLevel.Default)
        {
            if (logLevel > _logLevel) return;

#if UNITY_5_3_OR_NEWER
            Debug.LogWarning(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine("WARNING: " + message);
#endif
        }

        public static void LogError(string message, LogLevel logLevel = LogLevel.Default)
        {
            if (logLevel > _logLevel) return;

#if UNITY_5_3_OR_NEWER
            Debug.LogError(message);
#elif !UNITY_5_3_OR_NEWER
            Console.WriteLine("ERROR: " + message);
#endif
        }
    }
}
