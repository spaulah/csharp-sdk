namespace ModelContextProtocol.Server;

/// <summary>
/// Provides options for configuring <see cref="StreamServerTransport"/> instances.
/// </summary>
public sealed class StreamServerTransportOptions
{
    /// <summary>
    /// Gets or sets the name of the server, used for diagnostic purposes, like logging.
    /// </summary>
    public string? ServerName { get; set; }

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of a single incoming newline-delimited JSON-RPC message.
    /// </summary>
    /// <value>
    /// The maximum message size in bytes, excluding the trailing newline delimiter, or <see langword="null"/>
    /// to accept messages of any size. The default is <see langword="null"/>.
    /// </value>
    /// <remarks>
    /// <para>
    /// When a message exceeds this limit, the transport stops buffering it, discards the remainder of the
    /// message up to the next newline, and continues reading subsequent messages. This bounds the memory the
    /// transport holds for any single message to roughly this limit plus one read buffer. The limit applies to
    /// the message's UTF-8 bytes, excluding the trailing <c>\n</c> or <c>\r\n</c>.
    /// </para>
    /// <para>
    /// While discarding, the transport scans the message's top level for its <c>"id"</c>, wherever it appears,
    /// so the message can still be correlated:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// For a request, the transport replies to the client with a JSON-RPC error with code
    /// <see cref="McpErrorCode.InvalidRequest"/> (-32600) for that id, so the client does not wait for a response
    /// that will never arrive.
    /// </description></item>
    /// <item><description>
    /// For a response to a request the server sent, such as a large sampling result, the server's pending
    /// request fails with an <see cref="McpErrorCode.InvalidRequest"/> error instead of waiting indefinitely.
    /// </description></item>
    /// <item><description>
    /// Notifications and messages without a recoverable id are dropped.
    /// </description></item>
    /// </list>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is less than or equal to zero.</exception>
    public long? MaxMessageSize
    {
        get => _maxMessageSize;
        set
        {
            if (value is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "The maximum message size must be greater than zero.");
            }

            _maxMessageSize = value;
        }
    }

    private long? _maxMessageSize;
}
