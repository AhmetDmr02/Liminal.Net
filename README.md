# Liminal.Net

A lightweight, POCO-based netcode library for C#. Define plain structs, wire up callbacks, and go. Currently targets standalone .NET 9.
Unity support is planned for 6.8 once CoreCLR lands.

---

# Prerequisites

.NET 9.0 SDK or later.

MessagePack for C# (Liminal.Net relies on MessagePack POCOs for serialization).

## How it fits together

| # | Inbound | Outbound |
|---|---|---|
| 1 | `ILiminalTransport` raw bytes arrive from the network | Your code calls `Interpreter.SendCommand(...)` |
| 2 | `LiminalSessionManager` routes data to the right session | Packet is serialized and buffered into the session |
| 3 | `LiminalPacketFramerPipeline` runs inbound transformers (decrypt, decompress…) | `LiminalTicker` fires `Flush()` drains the send buffer |
| 4 | Packet lands in the session's `InboundQueue` | `LiminalPacketFramerPipeline` runs outbound transformers (compress, encrypt…) |
| 5 | `LiminalTicker` fires `TickEvent` which drains queues inside `LiminalSession` | `ILiminalTransport` bytes go out over the wire |
| 6 | `LiminalPacketInterpreter` deserializes and fires your subscriber callbacks | |

`LiminalNetworkManager` owns the lifecycle and exposes three roles: `StartServer`, `StartClient`, `StartHost`.

---

## Getting started

**Define a packet.** Just a struct.

```csharp
[MessagePackObject]
[LiminalPacket(id: 100)]
public struct PlayerMovePacket
{
    [Key(0)] public float X { get; set; }
    [Key(1)] public float Y { get; set; }
}
```

The library scans your assemblies at startup and registers everything tagged `[LiminalPacket]` automatically. Duplicate IDs throw immediately.

**Configure and start.**

```csharp
var config = new LiminalTransportConfig
{
    Default_Host = "127.0.0.1",
    Default_Port = 7777,
    TickRate = 30,
    ClientIdResolver = new BaseResolver(),
};

var manager = new LiminalNetworkManager(new TcpTransport(), config);
manager.StartServer("0.0.0.0", 7777);
// or manager.StartClient(...)
// or manager.StartHost() server + local client on same instance
```

**Subscribe and send.**

```csharp
manager.Interpreter.Subscribe<PlayerMovePacket>((packet, senderId) =>
{
    Console.WriteLine($"{senderId} moved to {packet.X}, {packet.Y}");
}, owner: this);

// Helper is planned for the future!
manager.Interpreter.SendCommand(targetId, new PlayerMovePacket { X = 1f, Y = 2f });

// Clean up everything this object subscribed to
manager.Interpreter.UnsubscribeAll(this);
```

---

## Player ID

The library does not stamp a client ID into the packet payload. It never writes identity into the wire bytes.

What you get instead is a `ushort senderId` in your callback, resolved by the transport from which socket the data came in on. That's it.

If your game logic needs an identity field in a packet (say, the server broadcasting another player's position), you add it yourself:

```csharp
[MessagePackObject]
[LiminalPacket(id: 101)]
public struct PlayerStateBroadcast
{
    [Key(0)] public ushort PlayerId { get; set; }
    [Key(1)] public float X { get; set; }
    [Key(2)] public float Y { get; set; }
}
```

This is a deliberate choice. But if you want some lower level access you can directly write your own transformer and include custom data inside the payload with `Ping-pong pipeline`

---

## Ping-pong pipeline

Packets go through a transform chain before sending and after receiving then you can do: compression, encryption, whatever you need. The pipeline uses a ping-pong pattern to avoid allocations: two staging buffers per session (A and B), and each transformer reads from one and writes to the other, alternating.

```
input → [Transform 0]: A→B → [Transform 1]: B→A → [Transform 2]: A→B → final output
```

No heap allocations mid-chain. The buffers are native unmanaged memory (`NativeMemory.Alloc`), completely off the GC.

**Writing a transformer** implement `ILiminalInboundTransformer`, `ILiminalOutboundTransformer`, or both:

```csharp
public class XorObfuscator : ILiminalInboundTransformer, ILiminalOutboundTransformer
{
    private readonly byte _key;
    public XorObfuscator(byte key) => _key = key;

    public int TransformInbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
    {
        for (int i = 0; i < input.Length; i++)
            output[i] = (byte)(input[i] ^ _key);
        return input.Length;
    }

    public int TransformOutbound(ReadOnlySpan<byte> input, Span<byte> output, LiminalSession session)
    {
        for (int i = 0; i < input.Length; i++)
            output[i] = (byte)(input[i] ^ _key);
        return input.Length;
    }
}
```

Register it before starting:

```csharp
var xor = new XorObfuscator(0xAB);
config.InboundPacketProcessors.Add(xor);
config.OutboundPacketProcessors.Add(xor);
```

A couple of rules to keep in mind:
- Write into `output`, never touch `input`
- Return the number of bytes written (can differ from input length, e.g. after compression)
- Returning `<= 0` on inbound drops the packet silently
- The `LiminalSession` has the session ID if your transform needs per-client state (e.g. per-client keys)

---

## Custom transport

Implement `ILiminalTransport` to plug in anything like: QUIC, WebSockets, raw UDP, or a fake in-memory transport for tests:

```csharp
public interface ILiminalTransport
{
    void InitializeTransport(LiminalTransportConfig config);
    void StartServer(string ip, int port);
    void StartClient(string ip, int port);
    void SendReliable(Span<byte> data, ushort clientId);
    void SendUnreliable(Span<byte> data, ushort clientId);
    void Disconnect();
    void Kick(ushort clientId);
    void Shutdown();

    event DataReceivedHandler OnMessageReceivedReliable;
    event DataReceivedHandler OnMessageReceivedUnreliable;
    event ClientConnectionHandler OnClientConnected;
    event ClientConnectionHandler OnClientDisconnected;
    // ... (see interface for full list)
}
```

The only real contract: by the time you fire `OnClientConnected`, the handshake is done and the client has a registered `ushort` ID. Everything else is up to you.

---

## Client ID resolver

The default `BaseResolver` hands out monotonically increasing `ushort` IDs. Works fine for direct connections, and also fine behind a standard Layer 4 TCP proxy, those just forward raw bytes, so each client still has its own socket from the server's perspective and identity resolution works as normal.

Where it gets complicated is merging relays, UDP scenarios where 50 players' packets all arrive from the same source IP:port, or old/custom proxies that might misinterpret the packet framing in which case you may need a transformer to re-frame things correctly on ingress. The `ResolveId(Span<byte> payload)` method exists for these cases. It runs during the handshake and lets you pull an identity token out of the client's opening payload (a lobby slot, a session cookie, a pre-assigned ID from your matchmaking system) and map it to the correct client ID before the connection is fully established.

```csharp
public class ProxyAwareResolver : BaseResolver
{
    private readonly ConcurrentDictionary<uint, ushort> _tokenMap = new();

    public void PreAssign(uint token, ushort id) => _tokenMap[token] = id;

    public override ushort ResolveId(Span<byte> payload)
    {
        if (payload.Length < 4) return 0;
        uint token = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4));
        return _tokenMap.TryRemove(token, out ushort id) ? id : 0;
    }
}
```

---

## Concurrency

The session lifecycle, subscription management, and ID reservation all touch shared state from multiple threads. The concurrent-critical paths are tested with [Microsoft Coyote](https://microsoft.github.io/coyote/), which takes control of thread scheduling and systematically explores interleavings you'd almost never hit with a normal stress test things like the reservation cleanup window racing with a new handshake, or a subscriber being disposed mid-dispatch.

---

## Roadmap

**PacketFragmentor**: UDP has a ~1400 byte practical MTU limit. The fragmentor will split large payloads into sequenced fragments and reassemble them on the other end, transparent to the rest of the pipeline.

**QUIC transport (C++)**: A native transport using `msquic` is in progress. Same `ILiminalTransport` interface, drop-in replacement for `TcpTransport`. Multiplexed streams and connection migration without TCP's head-of-line blocking.

**RPC helper**: A small static helper library to reduce boilerplate around common send patterns. Things like broadcasting to everyone except the server, or sending to a subset of sessions, without manually iterating IDs every time. Subscriptions still work end-to-end the same way, this just makes the sending side less repetitive:

```csharp
// planned, not yet implemented
LiminalRpc.Send(manager, new PlayerMovePacket { X = 1f, Y = 2f }, Target.NotServer);
```

**Unity 6.8 (CoreCLR)**: Unity support is blocked on this. The library currently relies on runtime features that aren't available under Unity's Mono backend. Once 6.8 ships CoreCLR, the plan is to target it. And the native buffer and tight tick loop paths should benefit meaningfully from the improved runtime too.

---

## License

GNU General Public License v3.0 (GPLv3)
