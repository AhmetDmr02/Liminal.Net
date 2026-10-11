using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Liminal.Net.SyncVar.Internal
{
    internal sealed class SyncVarPermissions
    {
        private readonly object _lock = new();
        private readonly SyncVarManager _manager;
        private readonly string _token;

        private SyncVarVisibilityState _state;
        private int _isLocallyVisible = 0;
        private bool _hasPendingAuthChanged;
        private bool _pendingAuthValue;

        public SyncVarPermissions(SyncVarManager manager, string token, SyncVarVisibility visibility)
        {
            _manager = manager;
            _token = token;

            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            SyncVarVisibility initialMode = (netManager != null && netManager.Role == NetworkRole.Client)
                ? SyncVarVisibility.Public
                : visibility;

            _state = initialMode switch
            {
                SyncVarVisibility.ManualObservers => SyncVarVisibilityState.DefaultManualObservers,
                SyncVarVisibility.OwnerOnly => SyncVarVisibilityState.DefaultOwnerOnly,
                _ => SyncVarVisibilityState.DefaultPublic
            };

            _isLocallyVisible = (netManager != null && netManager.Role == NetworkRole.Client) ? 0 : 1;
        }

        public SyncVarVisibilityState State => Volatile.Read(ref _state);
        public SyncVarVisibility Mode => Volatile.Read(ref _state).Mode;
        public ushort[] Authorities => Volatile.Read(ref _state).Authorities;
        public ushort[] Exclusions => Volatile.Read(ref _state).Exclusions;
        public ushort[] Observers => Volatile.Read(ref _state).Observers;
        public bool IsLocallyVisible => Volatile.Read(ref _isLocallyVisible) != 0;

        public bool HasAuthority
        {
            get
            {
                var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
                if (netManager == null || netManager.Role == NetworkRole.None) return true;

                if (netManager.Role == NetworkRole.Client)
                {
                    ushort myId = netManager.localID;
                    if (myId == 0 || myId == ILiminalTransport.SERVER_ID) return false;
                    return IsAuthorized(myId);
                }

                if (netManager.Role == NetworkRole.Server ||
                    netManager.Role == NetworkRole.Host ||
                    netManager.Transport?.IsServer == true ||
                    netManager.localID == ILiminalTransport.SERVER_ID)
                {
                    return true;
                }

                return false;
            }
        }

        public bool IsAuthorized(ushort clientId)
        {
            var auths = Volatile.Read(ref _state).Authorities;
            for (int i = 0; i < auths.Length; i++)
            {
                if (auths[i] == clientId) return true;
            }
            return false;
        }

        public bool IsExcluded(ushort clientId) => Contains(Volatile.Read(ref _state).Exclusions, clientId);
        public bool ContainsExclusion(ushort clientId) => Contains(Volatile.Read(ref _state).Exclusions, clientId);
        public bool ContainsObserver(ushort clientId) => Contains(Volatile.Read(ref _state).Observers, clientId);

        public bool IsVisibleTo(ushort clientId)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            return Volatile.Read(ref _state).IsVisibleTo(clientId, netManager);
        }

        public bool SetLocallyVisible(bool isVisible)
        {
            int newVal = isVisible ? 1 : 0;
            int oldVal = Interlocked.Exchange(ref _isLocallyVisible, newVal);
            return oldVal != newVal;
        }

        public bool SetAuthIds(ushort[] clientIds, out ushort[] previousAuthorities, out bool fireEvent, out bool currentAuth)
        {
            previousAuthorities = Array.Empty<ushort>();
            fireEvent = false;
            currentAuth = false;

            if (!IsServerAuthority("SetAuthIds")) return false;

            lock (_lock)
            {
                previousAuthorities = Volatile.Read(ref _state).Authorities;
                if (clientIds == null || clientIds.Length == 0)
                {
                    CommitAuthUpdate(Array.Empty<ushort>(), out fireEvent, out currentAuth);
                    return true;
                }

                ushort[] distinct = new ushort[clientIds.Length];
                int distinctCount = 0;
                for (int i = 0; i < clientIds.Length; i++)
                {
                    ushort id = clientIds[i];
                    if (!Contains(distinct, id, distinctCount))
                    {
                        distinct[distinctCount++] = id;
                    }
                }

                ushort[] finalAuthIds = distinct;
                if (distinctCount != clientIds.Length)
                {
                    finalAuthIds = new ushort[distinctCount];
                    Array.Copy(distinct, finalAuthIds, distinctCount);
                }

                CommitAuthUpdate(finalAuthIds, out fireEvent, out currentAuth);
                return true;
            }
        }

        public bool AddAuthority(ushort clientId, out ushort[] nextAuthIds, out bool fireEvent, out bool currentAuth)
        {
            nextAuthIds = null;
            fireEvent = false;
            currentAuth = false;

            if (!IsServerAuthority("AddAuthority")) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state).Authorities;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        LiminalLogger.LogWarning($"[SyncVar] Client {clientId} already has authority on '{_token}'. Overwriting duplicate assignment.");
                        return false;
                    }
                }

                nextAuthIds = new ushort[current.Length + 1];
                Array.Copy(current, nextAuthIds, current.Length);
                nextAuthIds[current.Length] = clientId;

                CommitAuthUpdate(nextAuthIds, out fireEvent, out currentAuth);
                return true;
            }
        }

        public bool RemoveAuthority(ushort clientId, out ushort[] nextAuthIds, out bool fireEvent, out bool currentAuth)
        {
            nextAuthIds = null;
            fireEvent = false;
            currentAuth = false;

            if (!IsServerAuthority("RemoveAuthority")) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state).Authorities;
                int targetIndex = -1;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0) return false;

                nextAuthIds = new ushort[current.Length - 1];
                for (int i = 0, j = 0; i < current.Length; i++)
                {
                    if (i != targetIndex)
                    {
                        nextAuthIds[j++] = current[i];
                    }
                }

                CommitAuthUpdate(nextAuthIds, out fireEvent, out currentAuth);
                return true;
            }
        }

        public void ApplyAuthUpdateFromRemote(ushort[] newAuthIds, bool deferEvent, out bool fireEvent, out bool currentAuth)
        {
            fireEvent = false;
            currentAuth = false;

            lock (_lock)
            {
                bool previousAuth = HasAuthority;
                var current = Volatile.Read(ref _state);
                Volatile.Write(ref _state, current.WithAuthorities(newAuthIds ?? Array.Empty<ushort>()));

                currentAuth = HasAuthority;
                if (previousAuth != currentAuth)
                {
                    if (deferEvent)
                    {
                        _pendingAuthValue = currentAuth;
                        _hasPendingAuthChanged = true;
                    }
                    else
                    {
                        _hasPendingAuthChanged = false;
                        fireEvent = true;
                    }
                }
            }
        }

        public bool SetExclusionIds(ushort[] clientIds, out List<ushort> newlyExcluded, out List<ushort> newlyUnexcluded)
        {
            newlyExcluded = null;
            newlyUnexcluded = null;

            if (!IsServerAuthority("SetExclusionIds")) return false;

            ushort[] next;
            if (clientIds == null || clientIds.Length == 0)
            {
                next = Array.Empty<ushort>();
            }
            else
            {
                ushort[] distinct = new ushort[clientIds.Length];
                int distinctCount = 0;
                for (int i = 0; i < clientIds.Length; i++)
                {
                    ushort id = clientIds[i];
                    if (!Contains(distinct, id, distinctCount))
                    {
                        distinct[distinctCount++] = id;
                    }
                }
                next = distinctCount == clientIds.Length ? distinct : new ushort[distinctCount];
                if (distinctCount != clientIds.Length)
                {
                    Array.Copy(distinct, next, distinctCount);
                }
            }

            lock (_lock)
            {
                var current = Volatile.Read(ref _state);
                var currentExclusions = current.Exclusions;

                for (int i = 0; i < next.Length; i++)
                {
                    ushort id = next[i];
                    bool wasInCurrent = false;
                    for (int j = 0; j < currentExclusions.Length; j++)
                    {
                        if (currentExclusions[j] == id) { wasInCurrent = true; break; }
                    }
                    if (!wasInCurrent)
                    {
                        newlyExcluded ??= new List<ushort>();
                        newlyExcluded.Add(id);
                    }
                }

                for (int i = 0; i < currentExclusions.Length; i++)
                {
                    ushort id = currentExclusions[i];
                    bool isInNext = false;
                    for (int j = 0; j < next.Length; j++)
                    {
                        if (next[j] == id) { isInNext = true; break; }
                    }
                    if (!isInNext)
                    {
                        newlyUnexcluded ??= new List<ushort>();
                        newlyUnexcluded.Add(id);
                    }
                }

                Volatile.Write(ref _state, current.WithExclusions(next));
                return true;
            }
        }

        public bool AddExclusion(ushort clientId, out bool wasAdded)
        {
            wasAdded = false;
            if (!IsServerAuthority("AddExclusion")) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state).Exclusions;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId) return true;
                }

                ushort[] next = new ushort[current.Length + 1];
                Array.Copy(current, next, current.Length);
                next[current.Length] = clientId;

                var state = Volatile.Read(ref _state);
                Volatile.Write(ref _state, state.WithExclusions(next));
                wasAdded = true;
                return true;
            }
        }

        public bool RemoveExclusion(ushort clientId, out bool wasRemoved)
        {
            wasRemoved = false;
            if (!IsServerAuthority("RemoveExclusion")) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state).Exclusions;
                int targetIndex = -1;
                for (int i = 0; i < current.Length; i++)
                {
                    if (current[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0) return true;

                ushort[] next = new ushort[current.Length - 1];
                for (int i = 0, j = 0; i < current.Length; i++)
                {
                    if (i != targetIndex) next[j++] = current[i];
                }

                var state = Volatile.Read(ref _state);
                Volatile.Write(ref _state, state.WithExclusions(next));
                wasRemoved = true;
                return true;
            }
        }

        public bool SetVisibility(SyncVarVisibility visibility, out bool changed, out SyncVarVisibilityState previousState, out SyncVarVisibilityState newState)
        {
            changed = false;
            previousState = null;
            newState = null;

            if (!IsServerAuthority("SetVisibility", silent: true)) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state);
                if (current.Mode != visibility)
                {
                    previousState = current;
                    newState = current.WithMode(visibility);
                    Volatile.Write(ref _state, newState);
                    changed = true;
                }
                return true;
            }
        }

        public bool SetObserverIds(ushort[] clientIds, out List<ushort> newlyAdded, out List<ushort> newlyRemoved)
        {
            newlyAdded = null;
            newlyRemoved = null;

            if (!IsServerAuthority("SetObserverIds", silent: true)) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state);
                var currentObservers = current.Observers;
                ushort[] next;

                if (clientIds == null || clientIds.Length == 0)
                {
                    next = Array.Empty<ushort>();
                }
                else
                {
                    var cleanList = new List<ushort>(clientIds.Length);
                    for (int i = 0; i < clientIds.Length; i++)
                    {
                        ushort id = clientIds[i];
                        if (!cleanList.Contains(id))
                        {
                            cleanList.Add(id);
                        }
                    }
                    next = cleanList.ToArray();
                }

                for (int i = 0; i < next.Length; i++)
                {
                    ushort id = next[i];
                    bool exists = false;
                    for (int j = 0; j < currentObservers.Length; j++)
                    {
                        if (currentObservers[j] == id) { exists = true; break; }
                    }
                    if (!exists)
                    {
                        newlyAdded ??= new List<ushort>();
                        newlyAdded.Add(id);
                    }
                }

                for (int i = 0; i < currentObservers.Length; i++)
                {
                    ushort id = currentObservers[i];
                    bool exists = false;
                    for (int j = 0; j < next.Length; j++)
                    {
                        if (next[j] == id) { exists = true; break; }
                    }
                    if (!exists)
                    {
                        newlyRemoved ??= new List<ushort>();
                        newlyRemoved.Add(id);
                    }
                }

                Volatile.Write(ref _state, current.WithObservers(next));
                return true;
            }
        }

        public bool AddObserver(ushort clientId, out bool wasAdded)
        {
            wasAdded = false;
            if (!IsServerAuthority("AddObserver", silent: true)) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state);
                var currentObservers = current.Observers;
                for (int i = 0; i < currentObservers.Length; i++)
                {
                    if (currentObservers[i] == clientId) return true;
                }

                ushort[] next = new ushort[currentObservers.Length + 1];
                Array.Copy(currentObservers, next, currentObservers.Length);
                next[currentObservers.Length] = clientId;

                Volatile.Write(ref _state, current.WithObservers(next));
                wasAdded = true;
                return true;
            }
        }

        public bool RemoveObserver(ushort clientId, out bool wasRemoved)
        {
            wasRemoved = false;
            if (!IsServerAuthority("RemoveObserver", silent: true)) return false;

            lock (_lock)
            {
                var current = Volatile.Read(ref _state);
                var currentObservers = current.Observers;
                int targetIndex = -1;
                for (int i = 0; i < currentObservers.Length; i++)
                {
                    if (currentObservers[i] == clientId)
                    {
                        targetIndex = i;
                        break;
                    }
                }

                if (targetIndex < 0) return true;

                ushort[] next = new ushort[currentObservers.Length - 1];
                for (int i = 0, j = 0; i < currentObservers.Length; i++)
                {
                    if (i != targetIndex) next[j++] = currentObservers[i];
                }

                Volatile.Write(ref _state, current.WithObservers(next));
                wasRemoved = true;
                return true;
            }
        }

        public bool TryFirePendingAuth(out bool authValue)
        {
            authValue = false;
            lock (_lock)
            {
                if (!_hasPendingAuthChanged) return false;
                authValue = _pendingAuthValue;
                _hasPendingAuthChanged = false;
                return true;
            }
        }

        public void ClearPendingAuth()
        {
            lock (_lock)
            {
                _hasPendingAuthChanged = false;
            }
        }

        private void CommitAuthUpdate(ushort[] newAuthIds, out bool fireEvent, out bool currentAuth)
        {
            bool previousAuth = HasAuthority;
            var current = Volatile.Read(ref _state);
            Volatile.Write(ref _state, current.WithAuthorities(newAuthIds));

            currentAuth = HasAuthority;
            fireEvent = previousAuth != currentAuth;
            if (fireEvent)
            {
                _hasPendingAuthChanged = false;
            }
        }

        public bool IsServerAuthority(string actionName, bool silent = false)
        {
            var netManager = _manager?.AttachedManager ?? LiminalNetworkManager.Instance;
            if (netManager == null || netManager.Role == NetworkRole.None)
            {
                return true;
            }

            if (netManager.Role == NetworkRole.Client)
            {
                if (!silent)
                {
                    ushort myId = netManager.localID;
                    LiminalLogger.LogError($"[SyncVar] Unauthorized {actionName} blocked! Client {myId} is not the server. Modifications for '{_token}' can only be performed by the server.");
                }
                return false;
            }

            if (netManager.Role == NetworkRole.Server ||
                netManager.Role == NetworkRole.Host ||
                netManager.Transport?.IsServer == true ||
                netManager.localID == ILiminalTransport.SERVER_ID)
            {
                return true;
            }

            if (!silent)
            {
                ushort otherId = netManager.localID;
                LiminalLogger.LogError($"[SyncVar] Unauthorized {actionName} blocked! Client {otherId} is not the server. Modifications for '{_token}' can only be performed by the server.");
            }
            return false;
        }

        private static bool Contains(ushort[] array, ushort id, int length = -1)
        {
            int count = length >= 0 ? length : array.Length;
            for (int i = 0; i < count; i++)
            {
                if (array[i] == id) return true;
            }
            return false;
        }
    }
}
