using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;

namespace Liminal.Net.Tests
{
    public class MockTransport : ILiminalTransport
    {
        public ushort LocalClientId => 1;
        public bool IsServer => true;
        public bool IsClient => false;
        public bool IsConnected => true;
        public int ConnectedClientCount => 1;

        public LiminalTransportConfig Config => _conf;

        public event DataReceivedHandler? OnMessageReceivedReliable;
        public event DataReceivedHandler? OnMessageReceivedUnreliable;
        public event DataReceivedHandler? OnMessageReceivedFragmented;

        public event TransportEventHandler? OnServerStarted;
        public event TransportEventHandler? OnShutdown;
        public event TransportEventHandler? OnHandshakeInitialized;

        public event ClientConnectionHandler? OnLocalClientConnected;
        public event ClientConnectionHandler? OnLocalClientDisconnected;
        public event ClientConnectionHandler? OnClientConnected;
        public event ClientConnectionHandler? OnClientDisconnected;
        public event ClientConnectionHandler? OnClientKicked;

        public Action<byte[], ushort, TransportFlags>? OnSendHook;

        private LiminalTransportConfig _conf;

        public void InitializeTransport(LiminalTransportConfig config)
        {
            _conf = config;
        }

        public void StartServer(string ip, int port) => OnServerStarted?.Invoke();
        public void StartClient(string ip, int port) { }
        public void Disconnect() { }
        public void Shutdown() => OnShutdown?.Invoke();

        public void Send(Span<byte> data, ushort clientId, TransportFlags flags)
        {
            OnSendHook?.Invoke(data.ToArray(), clientId, flags);
        }

        public void Kick(ushort clientId) => OnClientKicked?.Invoke(clientId);
        public bool IsClientConnected(ushort clientId) => true;

        #region Test Injection Triggers

        public void TriggerClientConnected(ushort id) => OnClientConnected?.Invoke(id);
        public void TriggerClientDisconnected(ushort id) => OnClientDisconnected?.Invoke(id);
        public void TriggerClientKicked(ushort id) => OnClientKicked?.Invoke(id);

        public void TriggerMessageReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerReliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerUnreliableReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedUnreliable?.Invoke(data, id);
        public void TriggerFragmentReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedFragmented?.Invoke(data, id);

        public void TriggerShutdown() => OnShutdown?.Invoke();

        #endregion
    }
}