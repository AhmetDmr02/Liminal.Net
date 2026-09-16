# Liminal.Net

A low-level, tick-based networking library for C# games and real-time apps. It relies on MessagePack for serialization and focuses on zero-allocation buffers, explicit transport control, and straightforward struct payloads.

---

## Prerequisites

- .NET 9.0 SDK+
- [MessagePack-CSharp](https://github.com/MessagePack-CSharp/MessagePack-CSharp)

---

## Architecture Overview

| Stage | Inbound Flow | Outbound Flow |
|---|---|---|
| 1 | `ILiminalTransport` receives bytes from socket | User calls `Broadcaster.Send(...)` / `Interpreter.SendCommand(...)` |
| 2 | `LiminalSessionManager` resolves session | Packet is serialized into the session's staging buffer |
| 3 | Inbound transformers execute (decrypt, decompress) | `LiminalTicker` calls `Flush()` |
| 4 | Packet queued in `InboundQueue` | Outbound transformers execute (compress, encrypt) |
| 5 | `LiminalTicker` fires `TickEvent` to drain queues | `ILiminalTransport` writes to the socket |
| 6 | `LiminalPacketInterpreter` invokes user handlers | — |

`LiminalNetworkManager` manages instance lifecycles via `StartServer()`, `StartClient()`, and `StartHost()`.

---

## Quick Start

### Define Packets

Packets are plain structs decorated with MessagePack attributes. Liminal scans assemblies on startup and validates packet IDs; duplicates throw at initialization.

```csharp
[MessagePackObject]
[LiminalPacket(id: 100)] // Omit id parameter for auto-assignment
public struct PlayerMovePacket
{
    [Key(0)] public float X { get; set; }
    [Key(1)] public float Y { get; set; }
}
```

> **Unity IL2CPP**: If using auto-assigned IDs or runtime reflection, preserve packet types in your `link.xml`:
> ```xml
> <assembly fullname="YourAssembly" preserve="all"/>
> ```

### Initialization

```csharp
var config = new LiminalNetworkConfig
{
    Default_Host = "127.0.0.1",
    Default_Port = 7777,
    TickRate = 60,
    ClientIdResolver = new BaseResolver()
};

var manager = new LiminalNetworkManager(new TcpTransport(), config);

// Start options:
manager.StartServer("0.0.0.0", 7777);
// manager.StartClient("127.0.0.1", 7777);
// manager.StartHost(); // Server + loopback client
```

### Messaging & Routing

Subscribe via the static `Broadcaster` utility or directly through `manager.Interpreter`.

```csharp
// Subscribe
Broadcaster.Subscribe<PlayerMovePacket>((packet, senderId) =>
{
    Console.WriteLine($"Client {senderId}: {packet.X}, {packet.Y}");
}, subscriber: this);

// Send patterns
Broadcaster.Send(SendTo.Server, new PlayerMovePacket { X = 1f, Y = 2f });
Broadcaster.Send(SendTo.NotServer, new PlayerMovePacket { X = 1f, Y = 2f });
Broadcaster.SendToClient(targetId, new PlayerMovePacket { X = 1f, Y = 2f });

// Unsubscribe
Broadcaster.UnsubscribeAll(this);
```

**Common Targets:**
* `SendTo.Server`: Target ID 0.
* `SendTo.Me`: Loopback.
* `SendTo.Everyone`: Server, host, and all clients.
* `SendTo.NotMe`: Broadcast excluding caller.
* `SendTo.NotServer`: Connected clients only.
* `SendTo.NotHost`: Remote peers only.

---

## Identity & Client IDs

Liminal does not append a client ID prefix to packet payloads over the wire. Instead, the transport associates inbound socket sources with a registered `ushort senderId` and passes it directly to your callback. 

If your game logic requires broadcasting identity to other clients, include it explicitly in the packet contract:

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

---

## Transformer Pipeline

Packets pass through an unmanaged transform chain before egress and after ingress. Buffers alternate across two staging buffers allocated via `NativeMemory.Alloc` to eliminate mid-pipeline GC allocations:

```
Input -> [Stage 0: Buf A -> Buf B] -> [Stage 1: Buf B -> Buf A] -> Output
```

Implement `ILiminalInboundTransformer`, `ILiminalOutboundTransformer`, or both:

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

// Registration
config.InboundPacketProcessors.Add(new XorObfuscator(0xAB));
config.OutboundPacketProcessors.Add(new XorObfuscator(0xAB));
```

**Implementation Rules:**
* Never mutate `input`; write transformed bytes directly into `output`.
* Return the exact byte count written to `output`.
* Inbound transforms returning `<= 0` drop the packet.
* Session IDs are available on `session.Id` for per-client crypto contexts.

---

## Custom Transports

Implement `ILiminalTransport` to wrap custom protocols (UDP/ENet/QUIC/WebSockets) or in-memory test mocks:

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
}
```

*Contract:* The underlying connection handshake must be complete and assign a valid `ushort` client ID before invoking `OnClientConnected`.

---

## Client ID Resolution & Proxies

The default `BaseResolver` allocates incremental `ushort` identifiers suitable for typical direct connections and Layer-4 reverse proxies.

For multiplexed relays or custom proxies where client endpoints share sockets or require pre-auth tokens, override `ResolveId`:

```csharp
public class ProxyAwareResolver : BaseResolver
{
    private readonly ConcurrentDictionary<uint, ushort> _tokenMap = new();

    public void PreAssign(uint token, ushort id) => _tokenMap[token] = id;

    public override ushort ResolveId(Span<byte> payload)
    {
        if (payload.Length < 4) return 0;
        uint token = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        return _tokenMap.TryRemove(token, out ushort id) ? id : (ushort)0;
    }
}
```

---

## SyncVar

`SyncVar<T>` provides dirty-checked property replication on tick flushes:

```csharp
var health = new SyncVar<int>("player_health", 100);

health.OnValueChanged += (oldVal, newVal) =>
{
    Console.WriteLine($"Health: {oldVal} -> {newVal}");
};

health.Value = 80; // Marks dirty automatically
health.SetDirty(); // Forces synchronization flag
```

* **Authority:** Defaults to Server-authoritative. Unauthorized updates are discarded. Use `AddAuthority(clientId)` or `SetAuthIds(...)` to grant client authority.
* **Targeting:** Exclude specific peers using `health.AddExclusion(targetClientId)`.
* **Thread Safety:** While internal buffer pointers are synchronized to avoid segfaults, **do not mutate SyncVars from worker threads**. `OnValueChanged` triggers synchronously on the calling thread; background mutations will invoke UI or game-engine callbacks off-thread and can cause unbatched race writes. Defer updates to `LiminalEventHub.OnPreFlush` or `OnPrePoll`.

---

## BitStreams

For performance-critical payloads requiring manual bit-packing and float quantization without heap allocs:

```csharp
Span<byte> buffer = stackalloc byte[64];
var writer = new BitWriter(buffer);

writer.WriteBits(5u, 3);
writer.WriteBool(true);
writer.WriteInt(42, 16);
writer.WriteQuantizedFloat(position.X, -100f, 100f, 16);
writer.Flush();

var reader = new BitReader(buffer[..writer.BytesWritten]);
uint val = reader.ReadBits(3);
bool flag = reader.ReadBool();
int num = reader.ReadInt(16);
float posX = reader.ReadQuantizedFloat(-100f, 100f, 16);
```

### BitStream Messaging

Bind a raw bitstream directly to a metadata header:

```csharp
[MessagePackObject]
[LiminalPacket(id: 200)]
public struct SnapshotMeta
{
    [Key(0)] public uint Tick;
    [Key(1)] public ushort Count;
}

// Send
Broadcaster.SendBitStream(SendTo.NotServer, new SnapshotMeta { Tick = 10, Count = 1 }, (ref BitWriter writer) =>
{
    writer.WriteQuantizedFloat(playerX, -500f, 500f, 16);
    writer.WriteQuantizedFloat(playerY, -500f, 500f, 16);
    writer.WriteBits(stateFlags, 8);
}, DeliveryMethod.Unreliable);

// Receive
Broadcaster.SubscribeBitStream<SnapshotMeta>((in SnapshotMeta meta, ref BitReader reader, ushort sender) =>
{
    float x = reader.ReadQuantizedFloat(-500f, 500f, 16);
    float y = reader.ReadQuantizedFloat(-500f, 500f, 16);
    byte flags = (byte)reader.ReadBits(8);
}, subscriber: this);
```

---

## Packet Fragmentation

`LiminalPacketFragmentor` manages slicing, transmission, and reassembly when payloads exceed network MTU.

* Payloads exceeding MTU are split into chunks with 6-byte sequence headers (Sequence: 2B, Index: 2B, Total: 2B).
* Reassembly allocates out of pooled buffers per-client.
* Payloads exceeding `MaxPacketSizePerBatch` or presenting malformed chunk sequences will kick the offending client.
* Incomplete transfers timeout after `ReceiveResponseTimeout`.
* Unreliable payloads exceeding MTU are promoted to `Reliable | Fragmented` to prevent corrupted partial assemblies.

```csharp
var config = new LiminalNetworkConfig
{
    TickRate = 60,
    MaxPacketSizePerBatch = 32768,
    ReceiveResponseTimeout = 5.0f
};
```

---

## Concurrency & Testing

Shared critical paths—including ID reuse, multi-client session updates, and internal buffer pooling—are verified using **Microsoft Coyote** and systematic concurrency exploration (**CUZZ**). Tests specifically validate:

* Inbound unsubscription races during active tick interpreter loops.
* Dynamic authority handover under parallel packet ingestion.
* Concurrent dirty-marking and buffer swap sequences during `Flush()`.

### Handling Post-Unsubscribe Invocations

To maintain zero allocations in the tick loop, `LiminalPacketInterpreter` snapshots delegates prior to iteration. An `Unsubscribe()` call executing on another thread while a dispatch is underway will not mutate the active dispatch snapshot. Guard disposal state on destroying objects:

```csharp
private volatile bool _disposed;

public void OnDestroy()
{
    _disposed = true;
    Broadcaster.UnsubscribeAll(this);
}

private void HandlePacket(PlayerMovePacket pkt, ushort sender)
{
    if (_disposed) return;
    // Process payload
}
```

---

## Diagnostics & Telemetry

```csharp
var telemetryConfig = new LiminalTelemetryConfig
{
    Flags = TelemetryFlags.ByteCounting | TelemetryFlags.PacketCounting | TelemetryFlags.End2EndRTT,
    PollIntervalInSeconds = 1.0f
};

var manager = new LiminalNetworkManager(transport, networkConfig, telemetryConfig);

manager.TelemetryManager.OnTelemetryUpdated += (sessionSnap, transportSnap) =>
{
    Console.WriteLine($"RTT: {manager.TelemetryManager.End2EndRTT:F1}ms");
    Console.WriteLine($"I/O: {transportSnap.InboundKB:F1} KB/s In | {transportSnap.OutboundKB:F1} KB/s Out");
};
```

---

## License

GNU General Public License v3.0 (GPLv3)