using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Runtime.CompilerServices;

namespace Liminal.Net.SyncVar
{
    public sealed class SyncVarVisibilityState
    {
        public static readonly SyncVarVisibilityState DefaultPublic = new(
            SyncVarVisibility.Public,
            Array.Empty<ushort>(),
            Array.Empty<ushort>(),
            Array.Empty<ushort>());

        public static readonly SyncVarVisibilityState DefaultManualObservers = new(
            SyncVarVisibility.ManualObservers,
            Array.Empty<ushort>(),
            Array.Empty<ushort>(),
            Array.Empty<ushort>());

        public static readonly SyncVarVisibilityState DefaultOwnerOnly = new(
            SyncVarVisibility.OwnerOnly,
            Array.Empty<ushort>(),
            Array.Empty<ushort>(),
            Array.Empty<ushort>());

        public readonly SyncVarVisibility Mode;
        public readonly ushort[] Observers;
        public readonly ushort[] Exclusions;
        public readonly ushort[] Authorities;

        public SyncVarVisibilityState(SyncVarVisibility mode, ushort[] observers, ushort[] exclusions, ushort[] authorities)
        {
            Mode = mode;
            Observers = observers ?? Array.Empty<ushort>();
            Exclusions = exclusions ?? Array.Empty<ushort>();
            Authorities = authorities ?? Array.Empty<ushort>();
        }

        public SyncVarVisibilityState WithMode(SyncVarVisibility mode)
        {
            if (Mode == mode) return this;
            return new SyncVarVisibilityState(mode, Observers, Exclusions, Authorities);
        }

        public SyncVarVisibilityState WithObservers(ushort[] observers)
        {
            return new SyncVarVisibilityState(Mode, observers, Exclusions, Authorities);
        }

        public SyncVarVisibilityState WithExclusions(ushort[] exclusions)
        {
            return new SyncVarVisibilityState(Mode, Observers, exclusions, Authorities);
        }

        public SyncVarVisibilityState WithAuthorities(ushort[] authorities)
        {
            return new SyncVarVisibilityState(Mode, Observers, Exclusions, authorities);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsVisibleTo(ushort clientId, LiminalNetworkManager netManager)
        {
            if (clientId == ILiminalTransport.SERVER_ID) return true;
            if (netManager != null && netManager.Role == NetworkRole.Host && clientId == netManager.localID)
                return true;

            if (Mode == SyncVarVisibility.OwnerOnly)
            {
                return Contains(Authorities, clientId);
            }

            var exclusions = Exclusions;
            for (int i = 0; i < exclusions.Length; i++)
            {
                if (exclusions[i] == clientId) return false;
            }

            return Mode switch
            {
                SyncVarVisibility.Public => true,
                SyncVarVisibility.ManualObservers => Contains(Observers, clientId),
                _ => false
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Contains(ushort[] array, ushort id)
        {
            for (int i = 0; i < array.Length; i++)
            {
                if (array[i] == id) return true;
            }
            return false;
        }
    }
}
