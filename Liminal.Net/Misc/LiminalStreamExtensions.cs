using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Liminal.Net.Core
{
    public static class LiminalStreamExtensions
    {
        public static async ValueTask LiminalReadExactlyAsync(
            this Stream stream,
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken = default)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException();

            int totalRead = 0;
            while (totalRead < count)
            {
                int read = await stream.ReadAsync(
                    buffer,
                    offset + totalRead,
                    count - totalRead,
                    cancellationToken).ConfigureAwait(false);

                if (read == 0)
                {
                    throw new EndOfStreamException(
                        $"Reached end of stream before reading requested {count} bytes (read {totalRead} bytes).");
                }

                totalRead += read;
            }
        }

        public static async ValueTask LiminalReadExactlyAsync(
            this Stream stream,
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            int totalRead = 0;
            while (totalRead < buffer.Length)
            {
                int read = await stream.ReadAsync(
                    buffer.Slice(totalRead),
                    cancellationToken).ConfigureAwait(false);

                if (read == 0)
                {
                    throw new EndOfStreamException(
                        $"Reached end of stream before reading requested {buffer.Length} bytes (read {totalRead} bytes).");
                }

                totalRead += read;
            }
        }
    }
}