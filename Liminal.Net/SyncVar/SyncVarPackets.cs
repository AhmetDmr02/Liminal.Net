using System;
using System.Buffers;
using System.Collections.Generic;
using MessagePack;
using MessagePack.Formatters;
using Liminal.Net.Core;

namespace Liminal.Net.SyncVar
{
    #region Snapshot Packets

    [MessagePackObject]
    public struct SyncVarSnapshotEntry
    {
        [Key(0)] public string Token;
        [Key(1)] public uint Version;
        [Key(2)] public ushort[] AuthIds;
        [Key(3)] public byte[] Data;
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct SyncVarSnapshotPacket
    {
        [Key(0)] public List<SyncVarSnapshotEntry> Entries;
    }

    #endregion

    #region Tick Packets

    [LiminalPacket]
    [MessagePackFormatter(typeof(SyncVarBatchWireFormatter))]
    public readonly struct SyncVarSlabBatchPacket
    {
        public readonly ReadOnlySequence<byte> Payload;
        public SyncVarSlabBatchPacket(ReadOnlySequence<byte> payload) => Payload = payload;
        public SyncVarSlabBatchPacket(ReadOnlyMemory<byte> memory) => Payload = new ReadOnlySequence<byte>(memory);
    }

    [LiminalPacket]
    [MessagePackFormatter(typeof(SyncVarClientRequestWireFormatter))]
    public readonly struct SyncVarSlabClientRequestPacket
    {
        public readonly ReadOnlySequence<byte> Payload;
        public SyncVarSlabClientRequestPacket(ReadOnlySequence<byte> payload) => Payload = payload;
        public SyncVarSlabClientRequestPacket(ReadOnlyMemory<byte> memory) => Payload = new ReadOnlySequence<byte>(memory);
    }

    public sealed class SyncVarBatchWireFormatter : IMessagePackFormatter<SyncVarSlabBatchPacket>
    {
        public void Serialize(ref MessagePackWriter writer, SyncVarSlabBatchPacket value, MessagePackSerializerOptions options)
        {
            if (value.Payload.IsEmpty)
            {
                writer.WriteNil();
                return;
            }

            writer.WriteBinHeader((int)value.Payload.Length);
            foreach (var segment in value.Payload)
            {
                writer.WriteRaw(segment.Span);
            }
        }

        public SyncVarSlabBatchPacket Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil())
            {
                return new SyncVarSlabBatchPacket(ReadOnlySequence<byte>.Empty);
            }

            var bytes = reader.ReadBytes();
            return new SyncVarSlabBatchPacket(bytes.GetValueOrDefault());
        }
    }

    public sealed class SyncVarClientRequestWireFormatter : IMessagePackFormatter<SyncVarSlabClientRequestPacket>
    {
        public void Serialize(ref MessagePackWriter writer, SyncVarSlabClientRequestPacket value, MessagePackSerializerOptions options)
        {
            if (value.Payload.IsEmpty)
            {
                writer.WriteNil();
                return;
            }

            writer.WriteBinHeader((int)value.Payload.Length);
            foreach (var segment in value.Payload)
            {
                writer.WriteRaw(segment.Span);
            }
        }

        public SyncVarSlabClientRequestPacket Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil())
            {
                return new SyncVarSlabClientRequestPacket(ReadOnlySequence<byte>.Empty);
            }

            var bytes = reader.ReadBytes();
            return new SyncVarSlabClientRequestPacket(bytes.GetValueOrDefault());
        }
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct SyncVarAuthUpdatePacket
    {
        [Key(0)] public string Token;
        [Key(1)] public ushort[] AuthIds;
    }

    #endregion
}