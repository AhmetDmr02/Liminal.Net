using System.Reflection;

namespace Liminal.Net.Core
{
    /// <summary>
    /// The central registry for all network packets. 
    /// Scans the entire application for [LiminalPacket] attributes at startup.
    /// </summary>
    public static class LiminalPacketLibrary
    {
        private static readonly Dictionary<int, Type> IdToType = new();
        private static readonly Dictionary<Type, int> TypeToId = new();

        static LiminalPacketLibrary()
        {
            Initialize();
        }

        public static void Initialize()
        {
            if (IdToType.Count > 0) return;

            const BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic |
                                      BindingFlags.Instance | BindingFlags.Static;

            var assemblies = AppDomain.CurrentDomain.GetAssemblies();

            foreach (var assembly in assemblies)
            {
                Type[] types;
                try
                {
                    types = assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException e)
                {
                    types = e.Types.Where(t => t != null).ToArray();
                }

                foreach (var type in types)
                {
                    var attr = type.GetCustomAttribute<LiminalPacketAttribute>(false);

                    if (attr != null)
                    {
                        RegisterPacket(attr.Id, type);
                    }
                }
            }

            LiminalLogger.Log($"[PacketLibrary] Successfully indexed {IdToType.Count} unique packets.");
        }

        private static void RegisterPacket(int id, Type type)
        {
            if (IdToType.TryGetValue(id, out var existingType))
            {
                string error = $"[Liminal] CRITICAL COLLISION: Packet ID {id} is claimed by '{type.FullName}' " +
                               $"but is already owned by '{existingType.FullName}'.";

                LiminalLogger.LogError(error);
                throw new InvalidOperationException(error);
            }

            IdToType.Add(id, type);
            TypeToId.Add(type, id);
        }


        public static int GetId(Type type)
        {
            if (TypeToId.TryGetValue(type, out int id)) return id;
            throw new KeyNotFoundException($"Type {type.Name} is not registered as a LiminalPacket.");
        }

        public static int GetId<T>() => GetId(typeof(T));

        public static Type GetType(int id)
        {
            if (IdToType.TryGetValue(id, out var type)) return type;
            throw new KeyNotFoundException($"Packet ID {id} is not registered in the PacketLibrary.");
        }

        public static bool TryGetType(int id, out Type type) => IdToType.TryGetValue(id, out type);
    }
}