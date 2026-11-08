using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using ModelContextProtocol.Tests.Utils;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ModelContextProtocol.Tests.Transport;

public class StdioServerTransportTests : LoggedTest
{
    private readonly McpServerOptions _serverOptions;

    public StdioServerTransportTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
        _serverOptions = new McpServerOptions
        {
            ProtocolVersion = "2.0",
            InitializationTimeout = TimeSpan.FromSeconds(10),
            ServerInstructions = "Test Instructions"
        };
    }

    [Fact]
    public async Task Constructor_Should_Initialize_With_Valid_Parameters()
    {
        // Use StreamServerTransport with Stream.Null rather than StdioServerTransport.
        // StdioServerTransport opens Console.OpenStandardInput() which permanently
        // blocks a thread pool thread on the test host's stdin. StdioServerTransport
        // should only be instantiated in a dedicated child process.
        await using var transport = new StreamServerTransport(Stream.Null, Stream.Null, _serverOptions.ServerInfo?.Name);

        // Assert
        Assert.NotNull(transport);
    }

    [Fact]
    public void Constructor_Throws_For_Null_Options()
    {
        Assert.Throws<ArgumentNullException>("serverName", () => new StdioServerTransport((string)null!));

        Assert.Throws<ArgumentNullException>("serverOptions", () => new StdioServerTransport((McpServerOptions)null!));
    }

    [Fact]
    public async Task Should_Start_In_Connected_State()
    {
        await using var transport = new StreamServerTransport(new Pipe().Reader.AsStream(), Stream.Null, loggerFactory: LoggerFactory);

        Assert.True(transport.IsConnected);
    }

    [Fact]
    public async Task SendMessageAsync_Should_Send_Message()
    {
        using var output = new MemoryStream();

        await using var transport = new StreamServerTransport(
            new Pipe().Reader.AsStream(),
            output,
            loggerFactory: LoggerFactory);

        // Verify transport is connected
        Assert.True(transport.IsConnected, "Transport should be connected after StartListeningAsync");

        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(44) };

        await transport.SendMessageAsync(message, TestContext.Current.CancellationToken);

        var result = Encoding.UTF8.GetString(output.ToArray()).Trim();
        var expected = JsonSerializer.Serialize(message, McpJsonUtilities.DefaultOptions);

        Assert.Equal(expected, result);
    }

    [Fact]
    public async Task DisposeAsync_Should_Dispose_Resources()
    {
        await using var transport = new StreamServerTransport(Stream.Null, Stream.Null, loggerFactory: LoggerFactory);

        await transport.DisposeAsync();

        Assert.False(transport.IsConnected);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Read_Messages()
    {
        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(44) };
        var json = JsonSerializer.Serialize(message, McpJsonUtilities.DefaultOptions);

        // Use a reader that won't terminate
        Pipe pipe = new();
        using var input = pipe.Reader.AsStream();

        await using var transport = new StreamServerTransport(
            input,
            Stream.Null,
            loggerFactory: LoggerFactory);

        // Verify transport is connected
        Assert.True(transport.IsConnected, "Transport should be connected after StartListeningAsync");

        // Write the message to the reader
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes($"{json}\n"), TestContext.Current.CancellationToken);

        var canRead = await transport.MessageReader.WaitToReadAsync(TestContext.Current.CancellationToken);

        Assert.True(canRead, "Nothing to read here from transport message reader");
        Assert.True(transport.MessageReader.TryPeek(out var readMessage));
        Assert.NotNull(readMessage);
        Assert.IsType<JsonRpcRequest>(readMessage);
        Assert.Equal("44", ((JsonRpcRequest)readMessage).Id.ToString());
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Match_Error_Properties_Case_Sensitively()
    {
        Pipe pipe = new();
        using var input = pipe.Reader.AsStream();

        await using var transport = new StreamServerTransport(
            input,
            Stream.Null,
            loggerFactory: LoggerFactory);

        await pipe.Writer.WriteAsync(
            Encoding.UTF8.GetBytes("""{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"boom","Code":1,"Message":"other"}}""" + "\n"),
            TestContext.Current.CancellationToken);

        var canRead = await transport.MessageReader.WaitToReadAsync(TestContext.Current.CancellationToken);

        Assert.True(canRead, "Nothing to read here from transport message reader");
        Assert.True(transport.MessageReader.TryPeek(out var readMessage));
        var error = Assert.IsType<JsonRpcError>(readMessage);
        Assert.Equal(-32603, error.Error.Code);
        Assert.Equal("boom", error.Error.Message);
    }

    [Fact]
    public async Task CleanupAsync_Should_Cleanup_Resources()
    {
        var transport = new StreamServerTransport(Stream.Null, Stream.Null, loggerFactory: LoggerFactory);

        await transport.DisposeAsync();

        Assert.False(transport.IsConnected);
    }

    [Fact]
    public async Task SendMessageAsync_Should_Preserve_Unicode_Characters()
    {
        // Use a reader that won't terminate
        using var output = new MemoryStream();

        await using var transport = new StreamServerTransport(
            new Pipe().Reader.AsStream(),
            output,
            loggerFactory: LoggerFactory);

        // Verify transport is connected
        Assert.True(transport.IsConnected, "Transport should be connected after StartListeningAsync");

        // Test 1: Chinese characters (BMP Unicode)
        var chineseText = "上下文伺服器"; // "Context Server" in Chinese
        var chineseMessage = new JsonRpcRequest
        {
            Method = "test",
            Id = new RequestId(44),
            Params = new JsonObject
            {
                ["text"] = chineseText
            },
        };

        // Clear output and send message
        output.SetLength(0);
        await transport.SendMessageAsync(chineseMessage, TestContext.Current.CancellationToken);

        // Verify Chinese characters preserved but encoded
        var chineseResult = Encoding.UTF8.GetString(output.ToArray()).Trim();
        var expectedChinese = JsonSerializer.Serialize(chineseMessage, McpJsonUtilities.DefaultOptions);
        Assert.Equal(expectedChinese, chineseResult);
        Assert.Contains(JsonSerializer.Serialize(chineseText, McpJsonUtilities.DefaultOptions), chineseResult);

        // Test 2: Emoji (non-BMP Unicode using surrogate pairs)
        var emojiText = "🔍 🚀 👍"; // Magnifying glass, rocket, thumbs up
        var emojiMessage = new JsonRpcRequest
        {
            Method = "test",
            Id = new RequestId(45),
            Params = new JsonObject
            {
                ["text"] = emojiText
            },
        };

        // Clear output and send message
        output.SetLength(0);
        await transport.SendMessageAsync(emojiMessage, TestContext.Current.CancellationToken);

        // Verify emoji preserved - might be as either direct characters or escape sequences
        var emojiResult = Encoding.UTF8.GetString(output.ToArray()).Trim();
        var expectedEmoji = JsonSerializer.Serialize(emojiMessage, McpJsonUtilities.DefaultOptions);
        Assert.Equal(expectedEmoji, emojiResult);

        // Verify surrogate pairs in different possible formats
        // Magnifying glass emoji: 🔍 (U+1F50D)
        bool magnifyingGlassFound =
            emojiResult.Contains("🔍") ||
            emojiResult.IndexOf("\\ud83d\\udd0d", StringComparison.OrdinalIgnoreCase) >= 0;

        // Rocket emoji: 🚀 (U+1F680)
        bool rocketFound =
            emojiResult.Contains("🚀") ||
            emojiResult.IndexOf("\\ud83d\\ude80", StringComparison.OrdinalIgnoreCase) >= 0;

        Assert.True(magnifyingGlassFound, "Magnifying glass emoji not found in result");
        Assert.True(rocketFound, "Rocket emoji not found in result");
    }

    [Fact]
    public async Task SendMessageAsync_Should_Log_At_Trace_Level()
    {
        // Arrange
        var mockLoggerProvider = new MockLoggerProvider();
        using var traceLoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddProvider(XunitLoggerProvider);
            builder.AddProvider(mockLoggerProvider);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        using var output = new MemoryStream();

        await using var transport = new StreamServerTransport(
            new Pipe().Reader.AsStream(),
            output,
            loggerFactory: traceLoggerFactory);

        // Act
        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(44) };
        await transport.SendMessageAsync(message, TestContext.Current.CancellationToken);

        // Assert
        var traceLogMessages = mockLoggerProvider.LogMessages
            .Where(x => x.LogLevel == LogLevel.Trace && x.Message.Contains("transport sending message"))
            .ToList();

        Assert.NotEmpty(traceLogMessages);
        Assert.Contains(traceLogMessages, x => x.Message.Contains("\"method\":\"test\"") && x.Message.Contains("\"id\":44"));
    }

    [Fact]
    public async Task SendMessageAsync_Should_Use_LF_Not_CRLF()
    {
        using var output = new MemoryStream();

        await using var transport = new StreamServerTransport(
            new Pipe().Reader.AsStream(),
            output,
            loggerFactory: LoggerFactory);

        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(44) };

        await transport.SendMessageAsync(message, TestContext.Current.CancellationToken);

        byte[] bytes = output.ToArray();

        // The output should end with exactly \n (0x0A), not \r\n (0x0D 0x0A).
        Assert.True(bytes.Length > 1, "Output should contain message data");
        Assert.Equal((byte)'\n', bytes[^1]);
        Assert.NotEqual((byte)'\r', bytes[^2]);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Accept_CRLF_Delimited_Messages()
    {
        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(44) };
        var json = JsonSerializer.Serialize(message, McpJsonUtilities.DefaultOptions);

        Pipe pipe = new();
        using var input = pipe.Reader.AsStream();

        await using var transport = new StreamServerTransport(
            input,
            Stream.Null,
            loggerFactory: LoggerFactory);

        // Write the message with \r\n line ending
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes($"{json}\r\n"), TestContext.Current.CancellationToken);

        var canRead = await transport.MessageReader.WaitToReadAsync(TestContext.Current.CancellationToken);

        Assert.True(canRead, "Should be able to read a \\r\\n-delimited message");
        Assert.True(transport.MessageReader.TryPeek(out var readMessage));
        Assert.NotNull(readMessage);
        Assert.IsType<JsonRpcRequest>(readMessage);
        Assert.Equal("44", ((JsonRpcRequest)readMessage).Id.ToString());
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Respond_With_ParseError_For_Request_Exceeding_MaxDepth()
    {
        // Build a ping request whose params nest more deeply than System.Text.Json's default
        // reader MaxDepth of 64, which is what makes full deserialization throw. The request still
        // carries an id, so the transport should reply with a JSON-RPC parse error for that id rather
        // than dropping the request and leaving the caller pending.
        var nested = new StringBuilder();
        const int depth = 100;
        for (int i = 0; i < depth; i++)
        {
            nested.Append("{\"p").Append(i).Append("\":");
        }
        nested.Append("{\"leaf\":true}");
        nested.Append('}', depth);

        var requestLine = $"{{\"jsonrpc\":\"2.0\",\"id\":900100,\"method\":\"ping\",\"params\":{nested}}}";

        Pipe inputPipe = new();
        Pipe outputPipe = new();
        using var input = inputPipe.Reader.AsStream();
        using var output = outputPipe.Writer.AsStream();

        await using var transport = new StreamServerTransport(
            input,
            output,
            loggerFactory: LoggerFactory);

        await inputPipe.Writer.WriteAsync(Encoding.UTF8.GetBytes($"{requestLine}\n"), TestContext.Current.CancellationToken);

        // Read the single response line the transport writes back to the output stream.
        using var responseReader = new StreamReader(outputPipe.Reader.AsStream(), Encoding.UTF8);
        var responseLine = await responseReader.ReadLineAsync(
#if NET
            TestContext.Current.CancellationToken
#endif
        );

        Assert.NotNull(responseLine);

        var response = JsonSerializer.Deserialize<JsonRpcMessage>(responseLine!, McpJsonUtilities.DefaultOptions);
        var error = Assert.IsType<JsonRpcError>(response);
        Assert.Equal("900100", error.Id.ToString());
        Assert.Equal((int)McpErrorCode.ParseError, error.Error.Code);

        // The transport should still be reading further messages after recovering from the bad one.
        Assert.True(transport.IsConnected);
    }

    [Theory]
    [InlineData(4 * 1024, false)]   // The whole message, newline included, arrives in a single read buffer.
    [InlineData(512 * 1024, false)] // The message spans many read buffers and is discarded as it arrives.
    [InlineData(512 * 1024, true)]  // As above, with "id" written before "params", as some other SDKs do.
    public async Task ReadMessagesAsync_Should_Respond_With_InvalidRequest_For_Request_Exceeding_MaxMessageSize(int payloadSize, bool idFirst)
    {
        const long MaxMessageSize = 1024;

        // By default this is serialized the way the C# SDK client sends it: "id" comes after the large "params",
        // so it can only be recovered by scanning the whole message, not just its first MaxMessageSize bytes.
        var payload = new string('A', payloadSize);
        string oversizedLine = idFirst
            ? $"{{\"jsonrpc\":\"2.0\",\"id\":\"too-big\",\"method\":\"tools/call\",\"params\":{{\"data\":\"{payload}\"}}}}"
            : SerializeMessage(new JsonRpcRequest { Id = new RequestId("too-big"), Method = "tools/call", Params = new JsonObject { ["data"] = payload } });
        Assert.True(Encoding.UTF8.GetByteCount(oversizedLine) > MaxMessageSize);
        Assert.Equal(idFirst, oversizedLine.IndexOf("\"id\"", StringComparison.Ordinal) < oversizedLine.IndexOf("\"params\"", StringComparison.Ordinal));

        await using var harness = new TransportHarness(MaxMessageSize, LoggerFactory);

        var followUp = new JsonRpcRequest { Method = "test", Id = new RequestId(45) };
        await harness.WriteAsync($"{oversizedLine}\n{SerializeMessage(followUp)}\n");

        // The oversized request is answered with an invalid request error for its id.
        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId("too-big"), error.Id);
        Assert.Equal((int)McpErrorCode.InvalidRequest, error.Error.Code);

        // The oversized message is never surfaced, and the transport keeps reading: the next message
        // is the well-formed request that followed the discarded one.
        var request = Assert.IsType<JsonRpcRequest>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(45), request.Id);
        Assert.True(harness.Transport.IsConnected);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Recover_Id_From_Oversized_Request_Written_In_Single_Bytes()
    {
        // Every byte arrives in its own read, so the scanner must carry its state across every possible split,
        // including inside property names, escape sequences, and the id itself.
        const long MaxMessageSize = 256;
        var oversizedLine = SerializeMessage(new JsonRpcRequest
        {
            Id = new RequestId("split \"id\" \\ value"),
            Method = "tools/call",
            Params = new JsonObject { ["data"] = new string('A', 2 * 1024) },
        });

        await using var harness = new TransportHarness(MaxMessageSize, LoggerFactory);

        foreach (byte b in Encoding.UTF8.GetBytes(oversizedLine + "\n"))
        {
            await harness.Input.Writer.WriteAsync(new[] { b }, TestContext.Current.CancellationToken);
        }

        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId("split \"id\" \\ value"), error.Id);
        Assert.Equal((int)McpErrorCode.InvalidRequest, error.Error.Code);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Ignore_Nested_And_Quoted_Ids_In_Oversized_Request()
    {
        // Only the top-level "id" identifies the request. Ids nested inside params, or that appear inside
        // string values (including after escaped quotes), must not be mistaken for it.
        const long MaxMessageSize = 1024;
        var oversizedLine =
            "{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{" +
            "\"id\":\"nested\"," +
            "\"items\":[{\"id\":1},{\"id\":2}]," +
            "\"text\":\"he said \\\"id\\\":3, \\\"}\\\" and left\"," +
            $"\"data\":\"{new string('A', 4 * 1024)}\"" +
            "},\"id\":42}";

        await using var harness = new TransportHarness(MaxMessageSize, LoggerFactory);
        await harness.WriteAsync(oversizedLine + "\n");

        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId(42), error.Id);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Drop_Oversized_Notification_Without_Response()
    {
        const long MaxMessageSize = 1024;

        // A notification has no id to correlate, so it is dropped without a response. The oversized
        // request that follows proves that the first line written to the output belongs to it.
        var oversizedNotification = SerializeMessage(new JsonRpcNotification { Method = "notifications/test", Params = new JsonObject { ["data"] = new string('A', 200 * 1024) } });
        var oversizedRequest = SerializeMessage(new JsonRpcRequest { Id = new RequestId(7), Method = "test", Params = new JsonObject { ["data"] = new string('B', 200 * 1024) } });

        await using var harness = new TransportHarness(MaxMessageSize, LoggerFactory);
        await harness.WriteAsync($"{oversizedNotification}\n{oversizedRequest}\n");

        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId(7), error.Id);
        Assert.Equal((int)McpErrorCode.InvalidRequest, error.Error.Code);

        Assert.False(harness.Transport.MessageReader.TryPeek(out _));
        Assert.True(harness.Transport.IsConnected);
    }

    [Theory]
    [InlineData(4 * 1024)]
    [InlineData(512 * 1024)]
    public async Task ReadMessagesAsync_Should_Fail_Locally_For_Response_Exceeding_MaxMessageSize(int payloadSize)
    {
        // An oversized response to a request the server sent (for example, a large sampling result) carries an
        // id from the server's own id space. Replying to the client with it would be wrong, so the transport
        // instead surfaces an error response locally so the server's pending request fails.
        const long MaxMessageSize = 1024;
        var oversizedResponse = SerializeMessage(new JsonRpcResponse { Id = new RequestId(5), Result = new JsonObject { ["data"] = new string('A', payloadSize) } });
        var sentinel = SerializeMessage(new JsonRpcRequest { Id = new RequestId("sentinel"), Method = "test", Params = new JsonObject { ["data"] = new string('B', 4 * 1024) } });

        await using var harness = new TransportHarness(MaxMessageSize, LoggerFactory);
        await harness.WriteAsync($"{oversizedResponse}\n{sentinel}\n");

        var localError = Assert.IsType<JsonRpcError>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(5), localError.Id);
        Assert.Equal((int)McpErrorCode.InvalidRequest, localError.Error.Code);

        // Nothing was sent to the client for the response: the first line it receives answers the sentinel.
        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId("sentinel"), error.Id);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Fail_Locally_For_Response_That_Fails_To_Parse()
    {
        // A response nested more deeply than the reader allows can't be deserialized. Like an oversized response,
        // it must fail the server's pending request locally rather than send a parse error to the client.
        var nested = new StringBuilder();
        const int Depth = 100;
        for (int i = 0; i < Depth; i++)
        {
            nested.Append("{\"r").Append(i).Append("\":");
        }
        nested.Append("true").Append('}', Depth);

        var sentinel = SerializeMessage(new JsonRpcRequest { Id = new RequestId("sentinel"), Method = "test", Params = new JsonObject { ["data"] = new string('B', 4 * 1024) } });

        await using var harness = new TransportHarness(maxMessageSize: 1024, LoggerFactory);
        await harness.WriteAsync($"{{\"result\":{nested},\"id\":6,\"jsonrpc\":\"2.0\"}}\n{sentinel}\n");

        var localError = Assert.IsType<JsonRpcError>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(6), localError.Id);
        Assert.Equal((int)McpErrorCode.ParseError, localError.Error.Code);

        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId("sentinel"), error.Id);
    }

    [Theory]
    [InlineData("\n")]
    [InlineData("\r\n")]
    public async Task ReadMessagesAsync_Should_Accept_Message_Of_Exactly_MaxMessageSize(string newline)
    {
        var atLimit = SerializeMessage(new JsonRpcRequest { Id = new RequestId(1), Method = "test", Params = new JsonObject { ["data"] = new string('A', 100 * 1024) } });
        var overLimit = SerializeMessage(new JsonRpcRequest { Id = new RequestId(2), Method = "test", Params = new JsonObject { ["data"] = new string('A', (100 * 1024) + 1) } });
        long maxMessageSize = Encoding.UTF8.GetByteCount(atLimit);
        Assert.Equal(maxMessageSize + 1, Encoding.UTF8.GetByteCount(overLimit));

        await using var harness = new TransportHarness(maxMessageSize, LoggerFactory);
        await harness.WriteAsync($"{atLimit}{newline}{overLimit}{newline}");

        // A message of exactly MaxMessageSize bytes, not counting the line terminator, is accepted.
        var request = Assert.IsType<JsonRpcRequest>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(1), request.Id);

        // One byte more is rejected.
        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId(2), error.Id);
        Assert.Equal((int)McpErrorCode.InvalidRequest, error.Error.Code);
    }

    [Fact]
    public async Task SampleAsync_Should_Fail_When_Client_Response_Exceeds_MaxMessageSize()
    {
        // End to end: a client's sampling result larger than the server's MaxMessageSize must fail the server's
        // SampleAsync call instead of leaving it waiting forever for a response that was discarded.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TestConstants.DefaultTimeout);

        Pipe clientToServer = new();
        Pipe serverToClient = new();
        await using var server = McpServer.Create(
            new StreamServerTransport(
                clientToServer.Reader.AsStream(),
                serverToClient.Writer.AsStream(),
                new StreamServerTransportOptions { MaxMessageSize = 64 * 1024 },
                LoggerFactory),
            new McpServerOptions(),
            LoggerFactory);
        Task serverTask = server.RunAsync(cts.Token);

        try
        {
            var clientOptions = TestMcpClient.CreateOptions();

            // Use a protocol version in which the server sends sampling as an ordinary request to the client.
            clientOptions.ProtocolVersion = McpProtocolVersions.November2025ProtocolVersion;
            clientOptions.Handlers.SamplingHandler = (_, _, _) => new ValueTask<CreateMessageResult>(new CreateMessageResult
            {
                Model = "test-model",
                Role = Role.Assistant,
                Content = [new TextContentBlock { Text = new string('A', 256 * 1024) }],
            });

            await using var client = await TestMcpClient.CreateAsync(
                new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()),
                clientOptions,
                LoggerFactory,
                cts.Token);

            var ex = await Assert.ThrowsAsync<McpProtocolException>(async () => await server.SampleAsync(
                new CreateMessageRequestParams
                {
                    Messages = [new SamplingMessage { Role = Role.User, Content = [new TextContentBlock { Text = "Hello" }] }],
                    MaxTokens = 10,
                },
                cts.Token));
            Assert.Equal(McpErrorCode.InvalidRequest, ex.ErrorCode);
        }
        finally
        {
            await cts.CancelAsync();
            await serverTask;
        }
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Read_Large_Message_Written_In_Chunks()
    {
        // Without a MaxMessageSize, a message far larger than the transport's read buffer that arrives
        // in many small writes is reassembled intact.
        var payload = new string('A', 1024 * 1024);
        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(46), Params = new JsonObject { ["data"] = payload } };
        byte[] bytes = Encoding.UTF8.GetBytes(SerializeMessage(message) + "\n");

        await using var harness = new TransportHarness(maxMessageSize: null, LoggerFactory);

        const int ChunkSize = 7 * 1024;
        for (int offset = 0; offset < bytes.Length; offset += ChunkSize)
        {
            await harness.Input.Writer.WriteAsync(bytes.AsMemory(offset, Math.Min(ChunkSize, bytes.Length - offset)), TestContext.Current.CancellationToken);
        }

        var request = Assert.IsType<JsonRpcRequest>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(46), request.Id);
        Assert.Equal(payload, request.Params?["data"]?.GetValue<string>());
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Read_Multiple_Messages_And_Skip_Blank_Lines()
    {
        await using var harness = new TransportHarness(maxMessageSize: null, LoggerFactory);

        var first = SerializeMessage(new JsonRpcRequest { Method = "test", Id = new RequestId(1) });
        var second = SerializeMessage(new JsonRpcNotification { Method = "notifications/test" });
        var third = SerializeMessage(new JsonRpcRequest { Method = "test", Id = new RequestId(3) });
        await harness.WriteAsync($"{first}\n\n   \n{second}\r\n\t\n{third}\n");

        var reader = harness.Transport.MessageReader;
        Assert.Equal(new RequestId(1), Assert.IsType<JsonRpcRequest>(await reader.ReadAsync(harness.TimeoutToken)).Id);
        Assert.Equal("notifications/test", Assert.IsType<JsonRpcNotification>(await reader.ReadAsync(harness.TimeoutToken)).Method);
        Assert.Equal(new RequestId(3), Assert.IsType<JsonRpcRequest>(await reader.ReadAsync(harness.TimeoutToken)).Id);
        Assert.False(reader.TryPeek(out _));
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Skip_Leading_Utf8_Byte_Order_Mark()
    {
        await using var harness = new TransportHarness(maxMessageSize: null, LoggerFactory);

        byte[] message = Encoding.UTF8.GetBytes(SerializeMessage(new JsonRpcRequest { Method = "test", Id = new RequestId(9) }) + "\n");
        await harness.Input.Writer.WriteAsync(new byte[] { 0xEF, 0xBB, 0xBF }.Concat(message).ToArray(), TestContext.Current.CancellationToken);

        var request = Assert.IsType<JsonRpcRequest>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(9), request.Id);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Read_Final_Message_Without_Trailing_Newline()
    {
        // Like StreamReader.ReadLine, data left at the end of the stream without a newline is a final message.
        var input = new MemoryStream(Encoding.UTF8.GetBytes(SerializeMessage(new JsonRpcRequest { Method = "test", Id = new RequestId(10) })));

        await using var transport = new StreamServerTransport(input, Stream.Null, loggerFactory: LoggerFactory);

        var messages = new List<JsonRpcMessage>();
        await foreach (var message in transport.MessageReader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            messages.Add(message);
        }

        var request = Assert.IsType<JsonRpcRequest>(Assert.Single(messages));
        Assert.Equal(new RequestId(10), request.Id);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Treat_Lone_Carriage_Return_As_Whitespace()
    {
        // Only '\n' delimits messages. A lone '\r' is JSON whitespace within the message, not a line break.
        await using var harness = new TransportHarness(maxMessageSize: null, LoggerFactory);
        await harness.WriteAsync("{\"jsonrpc\":\"2.0\",\r\"id\":11,\r\"method\":\"test\"}\n");

        var request = Assert.IsType<JsonRpcRequest>(await harness.Transport.MessageReader.ReadAsync(harness.TimeoutToken));
        Assert.Equal(new RequestId(11), request.Id);
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Respond_With_ParseError_For_Trailing_Content()
    {
        await using var harness = new TransportHarness(maxMessageSize: null, LoggerFactory);
        await harness.WriteAsync("""{"jsonrpc":"2.0","id":8,"method":"ping"} {}""" + "\n");

        var error = Assert.IsType<JsonRpcError>(await harness.ReadResponseAsync());
        Assert.Equal(new RequestId(8), error.Id);
        Assert.Equal((int)McpErrorCode.ParseError, error.Error.Code);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StreamServerTransportOptions_MaxMessageSize_Rejects_NonPositive_Values(long maxMessageSize)
    {
        var options = new StreamServerTransportOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxMessageSize = maxMessageSize);
    }

    private static string SerializeMessage(JsonRpcMessage message) =>
        JsonSerializer.Serialize(message, McpJsonUtilities.DefaultOptions);

    /// <summary>A <see cref="StreamServerTransport"/> over in-memory pipes, for writing raw input and reading raw output.</summary>
    /// <remarks>
    /// Reads time out after <see cref="TestConstants.DefaultTimeout"/>, so a missing message fails the test instead of hanging it.
    /// </remarks>
    private sealed class TransportHarness : IAsyncDisposable
    {
        private readonly CancellationTokenSource _timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        private readonly Stream _input;
        private readonly StreamReader _output;

        public TransportHarness(long? maxMessageSize, ILoggerFactory loggerFactory)
        {
            _timeoutCts.CancelAfter(TestConstants.DefaultTimeout);

            // Ending the output on timeout makes a pending ReadResponseAsync return no line, on every target framework.
            _timeoutCts.Token.Register(() => Output.Writer.Complete());

            _input = Input.Reader.AsStream();
            _output = new StreamReader(Output.Reader.AsStream(), Encoding.UTF8);
            Transport = new StreamServerTransport(
                _input,
                Output.Writer.AsStream(),
                new StreamServerTransportOptions { MaxMessageSize = maxMessageSize },
                loggerFactory);
        }

        public Pipe Input { get; } = new();

        public Pipe Output { get; } = new();

        public StreamServerTransport Transport { get; }

        /// <summary>Gets a token that is canceled when the test times out.</summary>
        public CancellationToken TimeoutToken => _timeoutCts.Token;

        public async Task WriteAsync(string text) =>
            await Input.Writer.WriteAsync(Encoding.UTF8.GetBytes(text), TestContext.Current.CancellationToken);

        /// <summary>Reads the next message the transport wrote to its output.</summary>
        public async Task<JsonRpcMessage?> ReadResponseAsync()
        {
            var line = await _output.ReadLineAsync(
#if NET
                TestContext.Current.CancellationToken
#endif
            );

            Assert.True(line is not null, "Timed out waiting for the transport to write a message.");
            return JsonSerializer.Deserialize<JsonRpcMessage>(line!, McpJsonUtilities.DefaultOptions);
        }

        public async ValueTask DisposeAsync()
        {
            _timeoutCts.Dispose();
            await Transport.DisposeAsync();
            _output.Dispose();
            _input.Dispose();
        }
    }

    [Fact]
    public async Task ReadMessagesAsync_Should_Log_Received_At_Trace_Level()
    {
        // Arrange
        var mockLoggerProvider = new MockLoggerProvider();
        using var traceLoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddProvider(XunitLoggerProvider);
            builder.AddProvider(mockLoggerProvider);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        var message = new JsonRpcRequest { Method = "test", Id = new RequestId(99) };
        var json = JsonSerializer.Serialize(message, McpJsonUtilities.DefaultOptions);

        Pipe pipe = new();
        using var input = pipe.Reader.AsStream();

        await using var transport = new StreamServerTransport(
            input,
            Stream.Null,
            loggerFactory: traceLoggerFactory);

        // Act
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes($"{json}\n"), TestContext.Current.CancellationToken);

        // Wait for the message to be processed
        var canRead = await transport.MessageReader.WaitToReadAsync(TestContext.Current.CancellationToken);
        Assert.True(canRead, "Nothing to read here from transport message reader");

        // Assert
        var traceLogMessages = mockLoggerProvider.LogMessages
            .Where(x => x.LogLevel == LogLevel.Trace && x.Message.Contains("transport received message"))
            .ToList();

        Assert.NotEmpty(traceLogMessages);
        Assert.Contains(traceLogMessages, x => x.Message.Contains("\"method\":\"test\"") && x.Message.Contains("\"id\":99"));
    }
}
