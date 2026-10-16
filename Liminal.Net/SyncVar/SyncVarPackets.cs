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

    //Client replaced a leftover that already got this session's state, asks the server to send that SyncVar again
    [MessagePackObject]
    [LiminalPacket]
    public struct SyncVarResyncRequestPacket
    {
        [Key(0)] public string Token;
    }

    [MessagePackObject]
    [LiminalPacket]
    public struct SyncVarAuthUpdatePacket
    {
        [Key(0)] public string Token;
        [Key(1)] public ushort[] AuthIds;
    }

    [LiminalPacket]
    [MessagePackFormatter(typeof(SyncVarEvictWireFormatter))]
    public struct SyncVarEvictPacket
    {
        public string Token;
        public ReadOnlyMemory<byte> TokenBytes;

        public SyncVarEvictPacket(string token, byte[] tokenBytes = null)
        {
            Token = token;
            TokenBytes = tokenBytes != null ? tokenBytes : (token != null ? System.Text.Encoding.UTF8.GetBytes(token) : ReadOnlyMemory<byte>.Empty);
        }

        public SyncVarEvictPacket(string token, ReadOnlyMemory<byte> tokenBytes)
        {
            Token = token;
            TokenBytes = tokenBytes;
        }
    }

    public sealed class SyncVarEvictWireFormatter : IMessagePackFormatter<SyncVarEvictPacket>
    {
        public void Serialize(ref MessagePackWriter writer, SyncVarEvictPacket value, MessagePackSerializerOptions options)
        {
            if (value.TokenBytes.Length > 0)
            {
                writer.WriteString(value.TokenBytes.Span);
            }
            else
            {
                writer.Write(value.Token);
            }
        }

        public SyncVarEvictPacket Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            var seq = reader.ReadStringSequence();
            if (!seq.HasValue)
            {
                return new SyncVarEvictPacket(null, ReadOnlyMemory<byte>.Empty);
            }

            var sequence = seq.Value;
            if (sequence.IsSingleSegment)
            {
                var span = sequence.FirstSpan;
                string token = Utf8TokenRegistry.GetCachedOrDecodeToken(span);
                return new SyncVarEvictPacket(token, sequence.First);
            }

            Span<byte> stackBuffer = stackalloc byte[(int)Math.Min(sequence.Length, 256)];
            sequence.CopyTo(stackBuffer);
            string multiToken = Utf8TokenRegistry.GetCachedOrDecodeToken(stackBuffer);
            return new SyncVarEvictPacket(multiToken, stackBuffer.ToArray());
        }
    }

    [LiminalPacket]
    [MessagePackFormatter(typeof(SyncVarClearWireFormatter))]
    public struct SyncVarClearPacket
    {
        public string Token;
        public ReadOnlyMemory<byte> TokenBytes;

        public SyncVarClearPacket(string token, byte[] tokenBytes = null)
        {
            Token = token;
            TokenBytes = tokenBytes != null ? tokenBytes : (token != null ? System.Text.Encoding.UTF8.GetBytes(token) : ReadOnlyMemory<byte>.Empty);
        }

        public SyncVarClearPacket(string token, ReadOnlyMemory<byte> tokenBytes)
        {
            Token = token;
            TokenBytes = tokenBytes;
        }
    }

    public sealed class SyncVarClearWireFormatter : IMessagePackFormatter<SyncVarClearPacket>
    {
        public void Serialize(ref MessagePackWriter writer, SyncVarClearPacket value, MessagePackSerializerOptions options)
        {
            if (value.TokenBytes.Length > 0)
            {
                writer.WriteString(value.TokenBytes.Span);
            }
            else
            {
                writer.Write(value.Token);
            }
        }

        public SyncVarClearPacket Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            var seq = reader.ReadStringSequence();
            if (!seq.HasValue)
            {
                return new SyncVarClearPacket(null, ReadOnlyMemory<byte>.Empty);
            }

            var sequence = seq.Value;
            if (sequence.IsSingleSegment)
            {
                var span = sequence.FirstSpan;
                string token = Utf8TokenRegistry.GetCachedOrDecodeToken(span);
                return new SyncVarClearPacket(token, sequence.First);
            }

            Span<byte> stackBuffer = stackalloc byte[(int)Math.Min(sequence.Length, 256)];
            sequence.CopyTo(stackBuffer);
            string multiToken = Utf8TokenRegistry.GetCachedOrDecodeToken(stackBuffer);
            return new SyncVarClearPacket(multiToken, stackBuffer.ToArray());
        }
    }

    [LiminalPacket]
    [MessagePackFormatter(typeof(SyncVarClearAckWireFormatter))]
    public struct SyncVarClearAckPacket
    {
        public string Token;
        public ReadOnlyMemory<byte> TokenBytes;

        public SyncVarClearAckPacket(string token, byte[] tokenBytes = null)
        {
            Token = token;
            TokenBytes = tokenBytes != null ? tokenBytes : (token != null ? System.Text.Encoding.UTF8.GetBytes(token) : ReadOnlyMemory<byte>.Empty);
        }

        public SyncVarClearAckPacket(string token, ReadOnlyMemory<byte> tokenBytes)
        {
            Token = token;
            TokenBytes = tokenBytes;
        }
    }

    public sealed class SyncVarClearAckWireFormatter : IMessagePackFormatter<SyncVarClearAckPacket>
    {
        public void Serialize(ref MessagePackWriter writer, SyncVarClearAckPacket value, MessagePackSerializerOptions options)
        {
            if (value.TokenBytes.Length > 0)
            {
                writer.WriteString(value.TokenBytes.Span);
            }
            else
            {
                writer.Write(value.Token);
            }
        }

        public SyncVarClearAckPacket Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            var seq = reader.ReadStringSequence();
            if (!seq.HasValue)
            {
                return new SyncVarClearAckPacket(null, ReadOnlyMemory<byte>.Empty);
            }

            var sequence = seq.Value;
            if (sequence.IsSingleSegment)
            {
                var span = sequence.FirstSpan;
                string token = Utf8TokenRegistry.GetCachedOrDecodeToken(span);
                return new SyncVarClearAckPacket(token, sequence.First);
            }

            Span<byte> stackBuffer = stackalloc byte[(int)Math.Min(sequence.Length, 256)];
            sequence.CopyTo(stackBuffer);
            string multiToken = Utf8TokenRegistry.GetCachedOrDecodeToken(stackBuffer);
            return new SyncVarClearAckPacket(multiToken, stackBuffer.ToArray());
        }
    }

    #endregion
}