using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Liminal.Net.Core
{
    public static class LiminalPacketLibrary
    {
        private static readonly Dictionary<ushort, Type> IdToType = new();
        private static readonly Dictionary<Type, ushort> TypeToId = new();
        public static uint RegistryHash { get; private set; }

        static LiminalPacketLibrary() => Initialize();

        private static readonly object InitLock = new();
        private static bool _isInitialized;

        public static void Initialize()
        {
            if (_isInitialized) return;
            lock (InitLock)
            {
                if (_isInitialized) return;
                InternalInitialize();
                _isInitialized = true;
            }
        }

        private static void InternalInitialize()
        {
            if (IdToType.Count > 0) return;

            ForceLoadAllReferencedAssemblies();

            var discovered = new List<(Type Type, ushort Reserved)>();

            // Dont forget to add references to link.xml for unity or otherwise it might get deleted by the stripper
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (assembly.IsDynamic) continue;

                var name = assembly.GetName().Name;
                if (name == null ||
                    name.StartsWith("System", StringComparison.Ordinal) ||
                    name.StartsWith("Microsoft", StringComparison.Ordinal) ||
                    name.StartsWith("UnityEngine", StringComparison.Ordinal) ||
                    name.StartsWith("UnityEditor", StringComparison.Ordinal) ||
                    name.StartsWith("mscorlib", StringComparison.Ordinal) ||
                    name.StartsWith("Mono", StringComparison.Ordinal) ||
                    name.StartsWith("netstandard", StringComparison.Ordinal))

                {
                    continue;
                }

                Type[] types;
                try
                {
                    types = assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException e)
                {
                    types = e.Types.Where(t => t != null).ToArray()!;
                }
                catch
                {
                    continue;
                }

                foreach (var type in types)
                {
                    if (!type.IsValueType)
                    {
                        continue;
                    }

                    var attr = type.GetCustomAttribute<LiminalPacketAttribute>(false);
                    if (attr != null && (type.IsGenericTypeDefinition))
                    {
                        LiminalLogger.LogWarning($"[PacketLibrary] Generic packet type '{type.FullName}' is not supported.");
                        continue;
                    }

                    if (attr != null) discovered.Add((type, attr.ReservedId));
                }
            }

            foreach (var (type, reserved) in discovered.Where(d => d.Reserved != 0))
            {
                if (!IdToType.TryAdd(reserved, type))
                    throw new InvalidOperationException(
                        $"[Liminal] Reserved packet ID {reserved} claimed by both " +
                        $"'{type.FullName}' and '{IdToType[reserved].FullName}'.");

                LiminalLogger.Log($"[PacketLibrary] Reserved packet ID {reserved} for '{type.FullName}'.", LiminalLogger.LogLevel.Detailed);

                TypeToId.Add(type, reserved);
            }

            var auto = discovered.Where(d => d.Reserved == 0)
                                 .Select(d => d.Type)
                                 .OrderBy(t => t.FullName, StringComparer.Ordinal)
                                 .ToArray();

            ushort next = 1;
            var hashInput = new StringBuilder();

            // Include reserved packets in the hash input too, in a fixed order,
            // so a reserved-id collision or removal also changes the hash.
            foreach (var (type, reserved) in discovered.Where(d => d.Reserved != 0).OrderBy(d => d.Reserved))
                hashInput.Append(reserved).Append(':').Append(type.FullName).Append('|');

            foreach (var type in auto)
            {
                while (IdToType.ContainsKey(next))
                {
                    if (next == ushort.MaxValue)
                        throw new InvalidOperationException("[Liminal] Exceeded maximum packet registry capacity (65535).");
                    next++;
                }

                IdToType.Add(next, type);
                TypeToId.Add(type, next);
                hashInput.Append(next).Append(':').Append(type.FullName).Append('|');

                LiminalLogger.Log($"[PacketLibrary] Registered packet ID {next} for '{type.FullName}'.", LiminalLogger.LogLevel.Detailed);

                if (next == ushort.MaxValue)
                {
                    if (auto.Length > IdToType.Count)
                        throw new InvalidOperationException("[Liminal] Exceeded maximum packet registry capacity (65535).");
                }
                else
                {
                    next++;
                }
            }

            RegistryHash = ComputeFnv1a(hashInput.ToString());

            LiminalLogger.Log($"[PacketLibrary] Registered {IdToType.Count} packets. RegistryHash={RegistryHash:X8}");
            return;
        }

        private static uint ComputeFnv1a(string s)
        {
            uint hash = 2166136261;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                hash ^= (byte)(c & 0xFF);
                hash *= 16777619;
                hash ^= (byte)(c >> 8);
                hash *= 16777619;
            }
            return hash;
        }

        /// <summary>
        /// Traverses all compile-time referenced assemblies starting from the entry or library assembly
        /// to ensure CoreCLR loads them into the current AppDomain before packet type scanning.
        /// NOTE: This only discovers assemblies that are directly or indirectly referenced in code
        /// DLLs not mentioned by any type in the call graph won't be discovered and must be loaded explicitly.
        /// </summary>
        private static void ForceLoadAllReferencedAssemblies()
        {
            var visited = new HashSet<string>();
            void Walk(Assembly asm)
            {
                if (asm == null || asm.IsDynamic || asm.FullName == null || !visited.Add(asm.FullName)) return;
                foreach (var refName in asm.GetReferencedAssemblies())
                {
                    try { Walk(Assembly.Load(refName)); }
                    catch { /* native/platform assemblies, safe to skip */ }
                }
            }

            var entry = Assembly.GetEntryAssembly()
                     ?? Assembly.GetCallingAssembly()
                     ?? typeof(LiminalPacketLibrary).Assembly;

            Walk(entry);
        }

        /// <returns>0 if T is not a registered packet 0 is never a valid packet id.</returns>
        public static ushort GetId<T>() => TypeToId.TryGetValue(typeof(T), out var id) ? id : (ushort)0;
        public static bool TryGetType(ushort id, out Type type) => IdToType.TryGetValue(id, out type);
    }
}