using Liminal.Net.Core;
using Liminal.Net.Interfaces;
using System;
using System.Threading.Tasks;

namespace Liminal.Net.Tests
{
    public class MockTransport : ILiminalTransport
    {
        public ushort LocalClientId => 1;
        public bool IsServer => true;
        public bool IsClient => false;
        public bool IsConnected => true;
        public int ConnectedClientCount => 1;

        public event DataReceivedHandler? OnMessageReceivedReliable;
        public event DataReceivedHandler? OnMessageReceivedUnreliable;
        public event TransportEventHandler? OnServerStarted;
        public event TransportEventHandler? OnShutdown;
        public event TransportEventHandler? OnHandshakeInitialized;
        public event ClientConnectionHandler? OnLocalClientConnected;
        public event ClientConnectionHandler? OnLocalClientDisconnected;
        public event ClientConnectionHandler? OnClientConnected;
        public event ClientConnectionHandler? OnClientDisconnected;
        public event ClientConnectionHandler? OnClientKicked;

        public void InitializeTransport(LiminalTransportConfig config) { }
        public void StartServer(string ip, int port) { }
        public void StartClient(string ip, int port) { }
        public void Disconnect() { }
        public void Shutdown() => OnShutdown?.Invoke();
        public void SendReliable(Span<byte> data, ushort clientId) { }
        public void SendUnreliable(Span<byte> data, ushort clientId) { }
        public void Kick(ushort clientId) => OnClientKicked?.Invoke(clientId);
        public bool IsClientConnected(ushort clientId) => true;

        public void TriggerClientConnected(ushort id) => OnClientConnected?.Invoke(id);
        public void TriggerMessageReceived(ReadOnlySpan<byte> data, ushort id) => OnMessageReceivedReliable?.Invoke(data, id);
        public void TriggerShutdown() => OnShutdown?.Invoke();
    }
}