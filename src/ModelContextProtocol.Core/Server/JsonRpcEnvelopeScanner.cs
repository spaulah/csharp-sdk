using ModelContextProtocol.Protocol;
using System.Buffers;
using System.Text.Json;

namespace ModelContextProtocol.Server;

/// <summary>
/// Incrementally scans the top level of a JSON-RPC message to recover its id and whether it is a request or a
/// response, without buffering the message or fully parsing it.
/// </summary>
/// <remarks>
/// <para>
/// This is used for messages that can't be deserialized normally: messages larger than the configured maximum size,
/// whose bytes are discarded as they arrive, and messages that failed to parse, for example because they nest more
/// deeply than the JSON reader allows. In both cases the transport still wants to correlate the message to a request
/// so that nobody waits on a response that will never arrive.
/// </para>
/// <para>
/// The scanner consumes the message in chunks of any size, split at any byte, and holds only a constant amount of
/// state: the nesting depth and string state of the value being skipped, the current top-level property name (only
/// as far as needed to recognize <c>id</c>, <c>method</c>, <c>result</c>, and <c>error</c>), and the raw bytes of
/// the id value, up to <see cref="MaxIdLength"/>. Only top-level properties are considered, so an <c>"id"</c> nested
/// inside <c>params</c> or appearing inside a string is ignored, and the top-level id is found wherever it appears,
/// including after a large <c>params</c> or <c>result</c> value.
/// </para>
/// <para>
/// Recovery is best effort. Input that isn't a JSON object stops the scan, and the outcome reflects whatever was seen
/// before that point. Property names containing escape sequences are not recognized.
/// </para>
/// </remarks>
internal sealed class JsonRpcEnvelopeScanner
{
    /// <summary>The longest raw id value, including quotes and escape sequences, that is captured.</summary>
    internal const int MaxIdLength = 1024;

    // Long enough for the longest property name of interest ("method", "result").
    private const int MaxPropertyNameLength = 6;

    private enum State
    {
        BeforeRoot,
        BeforePropertyName,
        InPropertyName,
        AfterPropertyName,
        BeforeValue,
        InStringId,
        InNumberId,
        InSkippedValue,
        AfterValue,
        Done,
    }

    private enum Property
    {
        Other,
        Id,
        Method,
        Result,
        Error,
    }

    private State _state = State.BeforeRoot;

    private readonly byte[] _propertyName = new byte[MaxPropertyNameLength];
    private int _propertyNameLength;
    private bool _propertyNameUnrecognizable;
    private Property _property;

    // State of the value being skipped, or of the string id being captured.
    private int _depth;
    private bool _inString;
    private bool _escaped;

    private byte[]? _id;
    private int _idLength;
    private bool _idSeen;
    private bool _idCaptured;

    /// <summary>Gets whether a top-level <c>method</c> property was seen, indicating a request or notification.</summary>
    public bool HasMethod { get; private set; }

    /// <summary>Gets whether a top-level <c>result</c> or <c>error</c> property was seen, indicating a response.</summary>
    public bool HasResultOrError { get; private set; }

    /// <summary>
    /// Gets whether the message appears to be a response to a request sent by this endpoint, rather than a request
    /// or notification sent by the peer.
    /// </summary>
    public bool IsResponse => HasResultOrError && !HasMethod;

    /// <summary>Scans the next chunk of the message.</summary>
    public void Scan(in ReadOnlySequence<byte> chunk)
    {
        if (chunk.IsSingleSegment)
        {
            Scan(chunk.First.Span);
            return;
        }

        foreach (ReadOnlyMemory<byte> segment in chunk)
        {
            if (_state == State.Done)
            {
                return;
            }

            Scan(segment.Span);
        }
    }

    /// <summary>Scans the next chunk of the message.</summary>
    public void Scan(ReadOnlySpan<byte> chunk)
    {
        int i = 0;
        while (i < chunk.Length && _state != State.Done)
        {
            byte b = chunk[i];
            switch (_state)
            {
                case State.BeforeRoot:
                    if (!IsWhiteSpace(b))
                    {
                        _state = b == (byte)'{' ? State.BeforePropertyName : State.Done;
                    }
                    break;

                case State.BeforePropertyName:
                    if (b == (byte)'"')
                    {
                        _propertyNameLength = 0;
                        _propertyNameUnrecognizable = false;
                        _escaped = false;
                        _state = State.InPropertyName;
                    }
                    else if (!IsWhiteSpace(b))
                    {
                        // '}' closes an empty object; anything else is malformed. Either way, nothing more to learn.
                        _state = State.Done;
                    }
                    break;

                case State.InPropertyName:
                    if (_escaped)
                    {
                        _escaped = false;
                    }
                    else if (b == (byte)'\\')
                    {
                        _escaped = true;
                        _propertyNameUnrecognizable = true;
                    }
                    else if (b == (byte)'"')
                    {
                        _property = ClassifyPropertyName();
                        _state = State.AfterPropertyName;
                    }
                    else if (_propertyNameLength < _propertyName.Length)
                    {
                        _propertyName[_propertyNameLength++] = b;
                    }
                    else
                    {
                        _propertyNameUnrecognizable = true;
                    }
                    break;

                case State.AfterPropertyName:
                    if (b == (byte)':')
                    {
                        _state = State.BeforeValue;
                    }
                    else if (!IsWhiteSpace(b))
                    {
                        _state = State.Done;
                    }
                    break;

                case State.BeforeValue:
                    if (IsWhiteSpace(b))
                    {
                        break;
                    }

                    switch (_property)
                    {
                        case Property.Method:
                            HasMethod = true;
                            break;

                        case Property.Result or Property.Error:
                            HasResultOrError = true;
                            break;

                        case Property.Id when !_idSeen:
                            _idSeen = true;
                            if (b == (byte)'"')
                            {
                                _escaped = false;
                                AppendIdByte(b);
                                _state = State.InStringId;
                                i++;
                                continue;
                            }

                            if (b == (byte)'-' || IsDigit(b))
                            {
                                AppendIdByte(b);
                                _state = State.InNumberId;
                                i++;
                                continue;
                            }

                            // Any other id (null, an object, ...) can't be correlated. Skip it like any other value.
                            break;
                    }

                    _depth = 0;
                    _inString = false;
                    _escaped = false;
                    _state = State.InSkippedValue;
                    continue; // Reprocess this byte as the first byte of the skipped value.

                case State.InStringId:
                    if (_id is null)
                    {
                        // The id was too long to capture; skip the rest of the string.
                        if (_escaped)
                        {
                            _escaped = false;
                        }
                        else if (b == (byte)'\\')
                        {
                            _escaped = true;
                        }
                        else if (b == (byte)'"')
                        {
                            _state = State.AfterValue;
                        }
                        break;
                    }

                    AppendIdByte(b);
                    if (_escaped)
                    {
                        _escaped = false;
                    }
                    else if (b == (byte)'\\')
                    {
                        _escaped = true;
                    }
                    else if (b == (byte)'"')
                    {
                        _idCaptured = _id is not null;
                        _state = State.AfterValue;
                    }
                    break;

                case State.InNumberId:
                    if (IsDigit(b) || b is (byte)'-' or (byte)'+' or (byte)'.' or (byte)'e' or (byte)'E')
                    {
                        AppendIdByte(b);
                        break;
                    }

                    _idCaptured = _id is not null;
                    _state = State.AfterValue;
                    continue; // Reprocess the delimiter that ended the number.

                case State.InSkippedValue:
                    if (_inString)
                    {
                        if (_escaped)
                        {
                            _escaped = false;
                        }
                        else if (b == (byte)'\\')
                        {
                            _escaped = true;
                        }
                        else if (b == (byte)'"')
                        {
                            _inString = false;
                            if (_depth == 0)
                            {
                                _state = State.AfterValue;
                            }
                        }
                        break;
                    }

                    switch (b)
                    {
                        case (byte)'"':
                            _inString = true;
                            break;

                        case (byte)'{' or (byte)'[':
                            _depth++;
                            break;

                        case (byte)'}' or (byte)']':
                            if (_depth == 0)
                            {
                                // A scalar value ended by the root object closing.
                                _state = State.Done;
                            }
                            else if (--_depth == 0)
                            {
                                _state = State.AfterValue;
                            }
                            break;

                        case (byte)',' when _depth == 0:
                            // A scalar value ended by the next property.
                            _state = State.BeforePropertyName;
                            break;
                    }
                    break;

                case State.AfterValue:
                    if (b == (byte)',')
                    {
                        _state = State.BeforePropertyName;
                    }
                    else if (!IsWhiteSpace(b))
                    {
                        // '}' closes the root object; anything else is malformed.
                        _state = State.Done;
                    }
                    break;
            }

            i++;
        }
    }

    /// <summary>
    /// Attempts to get the request id of the scanned message.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if a top-level string or integer id was found; <see langword="false"/> if the message
    /// has no id (for example, a notification), an id of another type, or an id that couldn't be captured.
    /// </returns>
    public bool TryGetId(out RequestId id)
    {
        id = default;

        // A number id can end at the end of the input, for example when a message is truncated at end of stream.
        bool captured = _idCaptured || (_state == State.InNumberId && _id is not null);
        if (!captured)
        {
            return false;
        }

        try
        {
            var reader = new Utf8JsonReader(_id!.AsSpan(0, _idLength));
            if (!reader.Read())
            {
                return false;
            }

            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    id = new RequestId(reader.GetString()!);
                    return true;

                case JsonTokenType.Number when reader.TryGetInt64(out long longId):
                    id = new RequestId(longId);
                    return true;

                default:
                    return false;
            }
        }
        catch (Exception)
        {
            // The captured bytes weren't a valid JSON string or number, for example an invalid escape sequence.
            return false;
        }
    }

    private void AppendIdByte(byte b)
    {
        if (_idLength == MaxIdLength)
        {
            // Too long to be a reasonable id. Stop capturing; TryGetId reports no id.
            _id = null;
            return;
        }

        _id ??= new byte[MaxIdLength];
        _id[_idLength++] = b;
    }

    private Property ClassifyPropertyName()
    {
        if (_propertyNameUnrecognizable)
        {
            return Property.Other;
        }

        ReadOnlySpan<byte> name = _propertyName.AsSpan(0, _propertyNameLength);
        return
            name.SequenceEqual("id"u8) ? Property.Id :
            name.SequenceEqual("method"u8) ? Property.Method :
            name.SequenceEqual("result"u8) ? Property.Result :
            name.SequenceEqual("error"u8) ? Property.Error :
            Property.Other;
    }

    private static bool IsWhiteSpace(byte b) => b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';

    private static bool IsDigit(byte b) => (uint)(b - '0') <= 9;
}
