using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;

namespace ModelContextProtocol.Server;

/// <summary>
/// Provides an <see cref="ITransport"/> implemented using a pair of input and output streams.
/// </summary>
/// <remarks>
/// The <see cref="StreamServerTransport"/> class implements bidirectional JSON-RPC messaging over arbitrary
/// streams, allowing MCP communication with clients through various I/O channels such as network sockets,
/// memory streams, or pipes.
/// </remarks>
public class StreamServerTransport : TransportBase
{
    /// <summary>The size of the buffers the input <see cref="PipeReader"/> rents for reading from the input stream.</summary>
    private const int InputBufferSize = 64 * 1024;

    private static readonly byte[] s_newlineBytes = "\n"u8.ToArray();

    /// <summary>
    /// Reader options equivalent to those <see cref="JsonSerializer"/> derives from <see cref="McpJsonUtilities.ProtocolOptions"/>.
    /// Messages are deserialized from a <see cref="Utf8JsonReader"/>, which uses the reader's options rather than the
    /// serializer's, so these must stay in sync for depth limits, comments, and trailing commas to behave the same.
    /// </summary>
    private static readonly JsonReaderOptions s_protocolReaderOptions = new()
    {
        MaxDepth = McpJsonUtilities.ProtocolOptions.MaxDepth,
        AllowTrailingCommas = McpJsonUtilities.ProtocolOptions.AllowTrailingCommas,
        CommentHandling = McpJsonUtilities.ProtocolOptions.ReadCommentHandling,
    };

    private readonly ILogger _logger;

    private readonly Stream _inputStream;
    private readonly PipeReader _inputReader;
    private readonly Stream _outputStream;
    private readonly long? _maxMessageSize;

    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _shutdownCts = new();

    private readonly Task _readLoopCompleted;
    private int _disposed = 0;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamServerTransport"/> class with explicit input/output streams.
    /// </summary>
    /// <param name="inputStream">The input <see cref="Stream"/> to use as standard input.</param>
    /// <param name="outputStream">The output <see cref="Stream"/> to use as standard output.</param>
    /// <param name="serverName">Optional name of the server, used for diagnostic purposes, like logging.</param>
    /// <param name="loggerFactory">Optional logger factory used for logging employed by the transport.</param>
    /// <exception cref="ArgumentNullException"><paramref name="inputStream"/> or <paramref name="outputStream"/> is <see langword="null"/>.</exception>
    public StreamServerTransport(Stream inputStream, Stream outputStream, string? serverName = null, ILoggerFactory? loggerFactory = null)
        : this(inputStream, outputStream, new StreamServerTransportOptions { ServerName = serverName }, loggerFactory)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamServerTransport"/> class with explicit input/output streams
    /// and transport options.
    /// </summary>
    /// <param name="inputStream">The input <see cref="Stream"/> to use as standard input.</param>
    /// <param name="outputStream">The output <see cref="Stream"/> to use as standard output.</param>
    /// <param name="options">Options that configure the transport, such as the server name and maximum message size.</param>
    /// <param name="loggerFactory">Optional logger factory used for logging employed by the transport.</param>
    /// <exception cref="ArgumentNullException"><paramref name="inputStream"/>, <paramref name="outputStream"/>, or <paramref name="options"/> is <see langword="null"/>.</exception>
    public StreamServerTransport(Stream inputStream, Stream outputStream, StreamServerTransportOptions options, ILoggerFactory? loggerFactory = null)
        : base(GetEndpointName(options), loggerFactory)
    {
        Throw.IfNull(inputStream);
        Throw.IfNull(outputStream);

        _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;

        _inputStream = inputStream;

        // Read raw UTF-8 bytes through a pipe instead of decoding lines into strings with a StreamReader. Messages are
        // deserialized directly from the pipe's pooled buffers, avoiding a UTF-16 string (and the StringBuilder growth
        // behind it) per message. leaveOpen keeps ownership of the stream with this transport: DisposeAsync disposes it
        // directly, which is what unblocks reads that don't observe cancellation.
        _inputReader = PipeReader.Create(inputStream, new StreamPipeReaderOptions(bufferSize: InputBufferSize, leaveOpen: true));
        _outputStream = outputStream;
        _maxMessageSize = options.MaxMessageSize;

        SetConnected();
        _readLoopCompleted = Task.Run(ReadMessagesAsync, _shutdownCts.Token);
    }

    private static string GetEndpointName(StreamServerTransportOptions options)
    {
        Throw.IfNull(options);

        return options.ServerName is { } serverName ? $"Server (stream) ({serverName})" : "Server (stream)";
    }

    /// <inheritdoc/>
    public override async Task SendMessageAsync(JsonRpcMessage message, CancellationToken cancellationToken = default)
    {
        if (!IsConnected)
        {
            return;
        }

        using var _ = await _sendLock.LockAsync(cancellationToken).ConfigureAwait(false);

        string id = "(no id)";
        if (message is JsonRpcMessageWithId messageWithId)
        {
            id = messageWithId.Id.ToString();
        }

        try
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(message, McpJsonUtilities.JsonContext.Default.JsonRpcMessage);
            LogTransportSendingMessageSensitive(message);
            await _outputStream.WriteAsync(json, cancellationToken).ConfigureAwait(false);
            await _outputStream.WriteAsync(s_newlineBytes, cancellationToken).ConfigureAwait(false);
            await _outputStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogTransportSendFailed(Name, id, ex);
            throw new IOException("Failed to send message.", ex);
        }
    }

    private async Task ReadMessagesAsync()
    {
        CancellationToken shutdownToken = _shutdownCts.Token;
        Exception? error = null;
        try
        {
            LogTransportEnteringReadMessagesLoop(Name);

            // How many bytes at the start of the unconsumed buffer have already been searched for a newline. A message
            // that spans many reads is then scanned once in total instead of from its beginning after every read.
            long scannedBytes = 0;

            // While an oversized message is being discarded, scans its bytes as they go by to recover its id and kind.
            // Null when not inside an oversized message.
            JsonRpcEnvelopeScanner? oversizedMessage = null;

            bool atStreamStart = true;

            while (!shutdownToken.IsCancellationRequested)
            {
                ReadResult result = await _inputReader.ReadAsync(shutdownToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                if (atStreamStart)
                {
                    // StreamReader stripped a leading UTF-8 byte order mark; keep doing so. Every message is longer
                    // than the BOM, so waiting for enough bytes to tell whether one is present delays nothing.
                    if (buffer.Length < 3 && !result.IsCompleted)
                    {
                        _inputReader.AdvanceTo(buffer.Start, buffer.End);
                        continue;
                    }

                    buffer = SkipUtf8ByteOrderMark(buffer);
                    atStreamStart = false;
                }

                while (buffer.Slice(scannedBytes).PositionOf((byte)'\n') is SequencePosition newline)
                {
                    ReadOnlySequence<byte> line = buffer.Slice(0, newline);
                    buffer = buffer.Slice(buffer.GetPosition(1, newline));
                    scannedBytes = 0;

                    if (oversizedMessage is not null)
                    {
                        // This newline terminates an oversized message whose earlier bytes were already discarded.
                        oversizedMessage.Scan(line);
                        await RejectOversizedMessageAsync(oversizedMessage, shutdownToken).ConfigureAwait(false);
                        oversizedMessage = null;
                        continue;
                    }

                    await ProcessLineAsync(line, shutdownToken).ConfigureAwait(false);
                }

                if (oversizedMessage is not null)
                {
                    // Still inside an oversized message: scan what arrived, then drop it.
                    oversizedMessage.Scan(buffer);
                    buffer = buffer.Slice(buffer.End);
                }
                else if (_maxMessageSize is long maxMessageSize && buffer.Length > maxMessageSize + 1)
                {
                    // The incomplete message already exceeds the limit. (It may legitimately be one byte over while
                    // its last byte is a '\r' still waiting for its '\n'.) Stop buffering it: scan what's buffered,
                    // drop it, and scan the rest as it arrives, so the response can still be correlated.
                    LogTransportMessageTooLarge(Name, maxMessageSize);
                    oversizedMessage = new JsonRpcEnvelopeScanner();
                    oversizedMessage.Scan(buffer);
                    buffer = buffer.Slice(buffer.End);
                }

                scannedBytes = buffer.Length;

                if (result.IsCompleted)
                {
                    if (oversizedMessage is not null)
                    {
                        // The stream ended inside an oversized message.
                        await RejectOversizedMessageAsync(oversizedMessage, shutdownToken).ConfigureAwait(false);
                    }
                    else if (!buffer.IsEmpty)
                    {
                        // Like StreamReader.ReadLine, treat data left without a trailing newline at the end of the
                        // stream as a final message.
                        await ProcessLineAsync(buffer, shutdownToken).ConfigureAwait(false);
                    }

                    LogTransportEndOfStream(Name);
                    break;
                }

                // Consume complete and discarded messages, but mark everything as examined so the next read waits
                // for more data rather than returning the incomplete message again.
                _inputReader.AdvanceTo(buffer.Start, result.Buffer.End);
            }
        }
        catch (OperationCanceledException)
        {
            LogTransportReadMessagesCancelled(Name);
        }
        catch (Exception ex)
        {
            LogTransportReadMessagesFailed(Name, ex);
            error = ex;
        }
        finally
        {
            // Return the pipe's pooled buffers. The input stream is left open and is disposed by DisposeAsync.
            _inputReader.Complete();
            SetDisconnected(error);
        }
    }

    /// <summary>
    /// Processes a single newline-delimited message, excluding its terminating <c>\n</c>.
    /// </summary>
    private async Task ProcessLineAsync(ReadOnlySequence<byte> line, CancellationToken cancellationToken)
    {
        line = TrimTrailingCarriageReturn(line);

        if (_maxMessageSize is long maxMessageSize && line.Length > maxMessageSize)
        {
            LogTransportMessageTooLarge(Name, maxMessageSize);
            var scanner = new JsonRpcEnvelopeScanner();
            scanner.Scan(line);
            await RejectOversizedMessageAsync(scanner, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (IsJsonWhiteSpace(line))
        {
            return;
        }

        if (Logger.IsEnabled(LogLevel.Trace))
        {
            LogTransportReceivedMessageSensitive(Name, GetString(line));
        }

        try
        {
            if (DeserializeMessage(line) is JsonRpcMessage message)
            {
                await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else if (Logger.IsEnabled(LogLevel.Trace))
            {
                LogTransportMessageParseUnexpectedTypeSensitive(Name, GetString(line));
            }
        }
        catch (JsonException ex)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
            {
                LogTransportMessageParseFailedSensitive(Name, GetString(line), ex);
            }
            else
            {
                LogTransportMessageParseFailed(Name, ex);
            }

            // Deserializing the full message failed, for example because the params object was nested more deeply
            // than the JSON reader's MaxDepth allows. The scanner has no depth limit, so it can usually still recover
            // the id and tell a request from a response, which lets the waiting side fail instead of hanging until it
            // times out. If no id can be recovered, the message was either a notification or too malformed to
            // correlate, so we just continue reading.
            var scanner = new JsonRpcEnvelopeScanner();
            scanner.Scan(line);
            await FailUnprocessableMessageAsync(
                scanner,
                McpErrorCode.ParseError,
                requestErrorMessage: "Failed to parse the JSON-RPC request.",
                responseErrorMessage: "Failed to parse the JSON-RPC response.",
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Fails a message larger than <see cref="StreamServerTransportOptions.MaxMessageSize"/> with an
    /// <see cref="McpErrorCode.InvalidRequest"/> error, once the scanner has seen the whole message.
    /// </summary>
    private Task RejectOversizedMessageAsync(JsonRpcEnvelopeScanner scanner, CancellationToken cancellationToken)
    {
        long maxMessageSize = _maxMessageSize.GetValueOrDefault();
        return FailUnprocessableMessageAsync(
            scanner,
            McpErrorCode.InvalidRequest,
            requestErrorMessage: $"The JSON-RPC request exceeds the maximum message size of {maxMessageSize} bytes.",
            responseErrorMessage: $"The JSON-RPC response exceeds the maximum message size of {maxMessageSize} bytes.",
            cancellationToken);
    }

    /// <summary>
    /// Reports an error for a message that can't be delivered, so that whoever is waiting on it doesn't hang.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For a request from the peer, the error is sent back to the peer as the response to that request.
    /// </para>
    /// <para>
    /// For a response from the peer to a request this server sent (for example, a large sampling result), the id
    /// belongs to this server's id space, not the peer's. Replying to the peer would be meaningless, and could even
    /// complete an unrelated pending request of the peer that happens to use the same id. Instead, the error is
    /// delivered locally as if the peer had responded with it, so the server's pending request fails.
    /// </para>
    /// <para>
    /// Notifications and messages without a recoverable id are dropped.
    /// </para>
    /// </remarks>
    private async Task FailUnprocessableMessageAsync(
        JsonRpcEnvelopeScanner scanner,
        McpErrorCode code,
        string requestErrorMessage,
        string responseErrorMessage,
        CancellationToken cancellationToken)
    {
        if (!scanner.TryGetId(out RequestId id))
        {
            return;
        }

        if (scanner.IsResponse)
        {
            await WriteMessageAsync(CreateError(id, code, responseErrorMessage), cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await SendMessageAsync(CreateError(id, code, requestErrorMessage), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception sendEx) when (sendEx is not OperationCanceledException)
        {
            // Swallow so a failed error-send does not tear down the read loop. No logging
            // here because SendMessageAsync already logs send failures before it throws.
        }

        static JsonRpcError CreateError(RequestId id, McpErrorCode code, string message) => new()
        {
            Id = id,
            Error = new JsonRpcErrorDetail
            {
                Code = (int)code,
                Message = message,
            },
        };
    }

    private static JsonRpcMessage? DeserializeMessage(in ReadOnlySequence<byte> utf8Json)
    {
        var reader = new Utf8JsonReader(utf8Json, s_protocolReaderOptions);
        var message = (JsonRpcMessage?)JsonSerializer.Deserialize(ref reader, McpJsonUtilities.ProtocolOptions.GetTypeInfo(typeof(JsonRpcMessage)));

        // Unlike the string and span overloads, deserializing from a reader stops after the first JSON value without
        // validating what follows it. Read once more so trailing content is rejected as it was before. The reader
        // throws for any non-whitespace content after a single top-level value.
        if (reader.Read())
        {
            throw new JsonException("Unexpected content after the JSON-RPC message.");
        }

        return message;
    }

    private static ReadOnlySequence<byte> SkipUtf8ByteOrderMark(ReadOnlySequence<byte> buffer)
    {
        ReadOnlySpan<byte> utf8Bom = [0xEF, 0xBB, 0xBF];
        if (buffer.Length < utf8Bom.Length)
        {
            return buffer;
        }

        Span<byte> prefix = stackalloc byte[3];
        buffer.Slice(0, prefix.Length).CopyTo(prefix);
        return prefix.SequenceEqual(utf8Bom) ? buffer.Slice(utf8Bom.Length) : buffer;
    }

    private static ReadOnlySequence<byte> TrimTrailingCarriageReturn(ReadOnlySequence<byte> line)
    {
        if (!line.IsEmpty && line.Slice(line.Length - 1).First.Span[0] == (byte)'\r')
        {
            return line.Slice(0, line.Length - 1);
        }

        return line;
    }

    private static bool IsJsonWhiteSpace(in ReadOnlySequence<byte> line)
    {
        foreach (ReadOnlyMemory<byte> segment in line)
        {
            foreach (byte b in segment.Span)
            {
                if (b is not ((byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n'))
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static string GetString(in ReadOnlySequence<byte> utf8) =>
#if NET
        Encoding.UTF8.GetString(utf8);
#else
        Encoding.UTF8.GetString(utf8.ToArray());
#endif

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            LogTransportShuttingDown(Name);

            // Signal to the stdin reading loop to stop.
            await _shutdownCts.CancelAsync().ConfigureAwait(false);
            _shutdownCts.Dispose();

            // Dispose of stdin/out. Cancellation may not be able to wake up operations
            // synchronously blocked in a syscall; we need to forcefully close the handle / file descriptor.
            _inputStream?.Dispose();
            _outputStream?.Dispose();

            // Make sure the work has quiesced.
            try
            {
                await _readLoopCompleted.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                LogTransportCleanupReadTaskFailed(Name, ex);
            }
        }
        finally
        {
            SetDisconnected();
            LogTransportShutDown(Name);
        }

        GC.SuppressFinalize(this);
    }
}
