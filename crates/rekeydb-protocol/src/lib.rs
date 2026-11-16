//! RESP2 and RESP3 wire protocol.
//!
//! The codec is deliberately independent of the server. It exposes an
//! incremental decoder for client requests and an encoder that can emit either
//! RESP version, which is what `HELLO` needs.

use std::fmt;

/// Maximum nesting depth accepted by the decoder.
pub const DEFAULT_MAX_DEPTH: usize = 128;
/// Maximum number of elements in a single array/map/set/push.
pub const DEFAULT_MAX_ELEMENTS: usize = 1 << 20;
/// Redis' default `proto-max-bulk-len` is 512 MiB.
pub const DEFAULT_MAX_BULK_LEN: usize = 512 * 1024 * 1024;

/// Which RESP dialect a connection is using.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespVersion {
    #[default]
    V2,
    V3,
}

/// A decoded RESP value.
///
/// `Bytes` are kept as `Vec<u8>` because Redis keys and values are arbitrary
/// binary data and are not necessarily UTF-8.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
    NullArray,
    Boolean(bool),
    Double(f64),
    BigNumber(Vec<u8>),
    VerbatimString {
        format: [u8; 3],
        data: Vec<u8>,
    },
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
    Attribute {
        pairs: Vec<(RespValue, RespValue)>,
        value: Box<RespValue>,
    },
}

impl RespValue {
    pub fn ok() -> Self {
        Self::SimpleString(b"OK".to_vec())
    }

    pub fn simple(s: impl Into<Vec<u8>>) -> Self {
        Self::SimpleString(s.into())
    }

    pub fn error(s: impl Into<Vec<u8>>) -> Self {
        Self::Error(s.into())
    }

    pub fn bulk(s: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(s.into())
    }

    pub fn null() -> Self {
        Self::Null
    }

    pub fn array(values: Vec<RespValue>) -> Self {
        Self::Array(values)
    }

    /// Encode this value, using RESP3-only types when `version` is `V3` and
    /// Redis' RESP2 fallbacks otherwise.
    pub fn encode(&self, version: RespVersion, out: &mut Vec<u8>) {
        match self {
            Self::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            Self::Integer(i) => {
                out.push(b':');
                out.extend_from_slice(i.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::BulkString(s) => write_bulk(s, out),
            Self::Array(values) => write_aggregate(b'*', values, version, out),
            Self::Null => match version {
                RespVersion::V2 => out.extend_from_slice(b"$-1\r\n"),
                RespVersion::V3 => out.extend_from_slice(b"_\r\n"),
            },
            Self::NullArray => match version {
                RespVersion::V2 => out.extend_from_slice(b"*-1\r\n"),
                RespVersion::V3 => out.extend_from_slice(b"_\r\n"),
            },
            Self::Boolean(value) => match version {
                RespVersion::V2 => {
                    out.push(b':');
                    out.extend_from_slice(if *value { b"1" } else { b"0" });
                    out.extend_from_slice(b"\r\n");
                }
                RespVersion::V3 => {
                    out.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
                }
            },
            Self::Double(value) => match version {
                RespVersion::V2 => {
                    let text = format_double(*value);
                    write_bulk(text.as_bytes(), out);
                }
                RespVersion::V3 => {
                    out.push(b',');
                    out.extend_from_slice(format_double(*value).as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            },
            Self::BigNumber(value) => match version {
                RespVersion::V2 => write_bulk(value, out),
                RespVersion::V3 => {
                    out.push(b'(');
                    out.extend_from_slice(value);
                    out.extend_from_slice(b"\r\n");
                }
            },
            Self::VerbatimString { format, data } => match version {
                RespVersion::V2 => write_bulk(data, out),
                RespVersion::V3 => {
                    out.push(b'=');
                    out.extend_from_slice((data.len() + 4).to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(format);
                    out.push(b':');
                    out.extend_from_slice(data);
                    out.extend_from_slice(b"\r\n");
                }
            },
            Self::Map(pairs) => match version {
                RespVersion::V2 => {
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        flat.push(k.clone());
                        flat.push(v.clone());
                    }
                    write_aggregate(b'*', &flat, version, out);
                }
                RespVersion::V3 => {
                    out.push(b'%');
                    out.extend_from_slice(pairs.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(version, out);
                        v.encode(version, out);
                    }
                }
            },
            Self::Set(values) => match version {
                RespVersion::V2 => write_aggregate(b'*', values, version, out),
                RespVersion::V3 => write_aggregate(b'~', values, version, out),
            },
            Self::Push(values) => match version {
                RespVersion::V2 => write_aggregate(b'*', values, version, out),
                RespVersion::V3 => write_aggregate(b'>', values, version, out),
            },
            Self::Attribute { pairs, value } => match version {
                RespVersion::V2 => value.encode(version, out),
                RespVersion::V3 => {
                    out.push(b'|');
                    out.extend_from_slice(pairs.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for (key, value) in pairs {
                        key.encode(version, out);
                        value.encode(version, out);
                    }
                    value.encode(version, out);
                }
            },
        }
    }

    pub fn encoded(&self, version: RespVersion) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(version, &mut out);
        out
    }
}

fn write_bulk(data: &[u8], out: &mut Vec<u8>) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

fn write_aggregate(prefix: u8, values: &[RespValue], version: RespVersion, out: &mut Vec<u8>) {
    out.push(prefix);
    out.extend_from_slice(values.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for value in values {
        value.encode(version, out);
    }
}

fn format_double(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value == f64::INFINITY {
        return "inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    // Redis uses a shortest-roundtrip representation with up to 17 digits.
    let mut text = format!("{value}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text == "-0" {
        text = "0".to_string();
    }
    text
}

/// A client command: an array of binary arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub args: Vec<Vec<u8>>,
}

impl Command {
    pub fn new(args: Vec<Vec<u8>>) -> Self {
        Self { args }
    }

    pub fn name(&self) -> Option<&[u8]> {
        self.args.first().map(Vec::as_slice)
    }
}

/// Decoder limits. These are wire limits, not config limits; the server
/// applies configuration on top.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_depth: usize,
    pub max_elements: usize,
    pub max_bulk_len: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_elements: DEFAULT_MAX_ELEMENTS,
            max_bulk_len: DEFAULT_MAX_BULK_LEN,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub message: String,
}

impl ProtocolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn syntax() -> Self {
        Self::new("syntax error")
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug)]
enum ParseError {
    Protocol(ProtocolError),
}

impl From<ProtocolError> for ParseError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

/// Incremental RESP decoder.
///
/// Feed bytes as they arrive from a socket, then call [`RespDecoder::next`]
/// until it returns `Ok(None)`.
#[derive(Debug)]
pub struct RespDecoder {
    buffer: Vec<u8>,
    limits: Limits,
}

impl Default for RespDecoder {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

impl RespDecoder {
    pub fn new(limits: Limits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
        }
    }

    /// Append bytes received from the network.
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Number of buffered bytes not yet consumed.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Decode the next command, returning `Ok(None)` when more bytes are
    /// needed. A protocol error is fatal for the connection.
    pub fn next_command(&mut self) -> Result<Option<Command>, ProtocolError> {
        let parsed = match parse_command(&self.buffer, &self.limits) {
            Ok(value) => value,
            Err(ParseError::Protocol(err)) => return Err(err),
        };
        let Some((command, consumed)) = parsed else {
            return Ok(None);
        };
        self.buffer.drain(..consumed);
        Ok(Some(command))
    }

    /// Decode the next generic RESP value. Used by tests, modules, and tools.
    pub fn next_value(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let parsed = match parse_value(&self.buffer, 0, &self.limits) {
            Ok(value) => value,
            Err(ParseError::Protocol(err)) => return Err(err),
        };
        let Some((value, consumed)) = parsed else {
            return Ok(None);
        };
        self.buffer.drain(..consumed);
        Ok(Some(value))
    }
}

fn parse_command(input: &[u8], limits: &Limits) -> Result<Option<(Command, usize)>, ParseError> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        // Inline command. Redis accepts inline commands for telnet-style use.
        let Some((line, consumed)) = take_line(input)? else {
            return Ok(None);
        };
        let args = split_inline(line)?;
        return Ok(Some((Command::new(args), consumed)));
    }

    let Some((line, consumed)) = take_line(&input[1..])? else {
        return Ok(None);
    };
    let count = parse_i64(line)
        .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid multibulk length")))?;
    if count <= 0 {
        // Redis treats a non-positive multibulk length as an empty request.
        return Ok(Some((Command::new(Vec::new()), consumed + 1)));
    }
    let count = usize::try_from(count)
        .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid multibulk length")))?;
    if count > limits.max_elements {
        return Err(ParseError::Protocol(ProtocolError::new(
            "invalid multibulk length",
        )));
    }

    let mut offset = consumed + 1;
    let mut args = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if offset >= input.len() {
            return Ok(None);
        }
        let prefix = input[offset];
        if prefix != b'$' {
            let got = if prefix.is_ascii_graphic() {
                (prefix as char).to_string()
            } else {
                format!("\\x{prefix:02x}")
            };
            return Err(ParseError::Protocol(ProtocolError::new(format!(
                "expected '$', got '{got}'"
            ))));
        }
        let Some((length_line, used)) = take_line(&input[offset + 1..])? else {
            return Ok(None);
        };
        let length = parse_i64(length_line)
            .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid bulk length")))?;
        if length < 0 {
            return Err(ParseError::Protocol(ProtocolError::new(
                "invalid bulk length",
            )));
        }
        let length = usize::try_from(length)
            .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid bulk length")))?;
        if length > limits.max_bulk_len {
            return Err(ParseError::Protocol(ProtocolError::new(
                "invalid bulk length",
            )));
        }
        let start = offset + 1 + used;
        if input.len() < start + length + 2 {
            return Ok(None);
        }
        if &input[start + length..start + length + 2] != b"\r\n" {
            return Err(ParseError::Protocol(ProtocolError::new(
                "invalid bulk string terminator",
            )));
        }
        args.push(input[start..start + length].to_vec());
        offset = start + length + 2;
    }
    Ok(Some((Command::new(args), offset)))
}

fn parse_value(
    input: &[u8],
    depth: usize,
    limits: &Limits,
) -> Result<Option<(RespValue, usize)>, ParseError> {
    if depth > limits.max_depth {
        return Err(ParseError::Protocol(ProtocolError::new(
            "nesting depth exceeded",
        )));
    }
    if input.is_empty() {
        return Ok(None);
    }
    match input[0] {
        b'+' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            Ok(Some((RespValue::SimpleString(line.to_vec()), consumed + 1)))
        }
        b'-' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            Ok(Some((RespValue::Error(line.to_vec()), consumed + 1)))
        }
        b':' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let number = parse_i64(line)?;
            Ok(Some((RespValue::Integer(number), consumed + 1)))
        }
        b'$' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let len = parse_i64(line)?;
            if len == -1 {
                return Ok(Some((RespValue::Null, consumed + 1)));
            }
            let len = usize::try_from(len)
                .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid bulk length")))?;
            if len > limits.max_bulk_len {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "bulk length exceeds maximum",
                )));
            }
            let start = 1 + consumed;
            if input.len() < start + len + 2 {
                return Ok(None);
            }
            if &input[start + len..start + len + 2] != b"\r\n" {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid bulk string terminator",
                )));
            }
            Ok(Some((
                RespValue::BulkString(input[start..start + len].to_vec()),
                start + len + 2,
            )))
        }
        b'*' | b'~' | b'>' => {
            let prefix = input[0];
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let count = parse_i64(line)?;
            if count < 0 && prefix == b'*' {
                return Ok(Some((RespValue::NullArray, consumed + 1)));
            }
            let count = usize::try_from(count).map_err(|_| {
                ParseError::Protocol(ProtocolError::new("invalid aggregate length"))
            })?;
            if count > limits.max_elements {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "aggregate length exceeds maximum",
                )));
            }
            let mut offset = 1 + consumed;
            let mut values = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let Some((value, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                    return Ok(None);
                };
                values.push(value);
                offset += used;
            }
            let value = match prefix {
                b'*' => RespValue::Array(values),
                b'~' => RespValue::Set(values),
                b'>' => RespValue::Push(values),
                _ => unreachable!(),
            };
            Ok(Some((value, offset)))
        }
        b'%' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let count = parse_i64(line)?;
            let count = usize::try_from(count)
                .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid map length")))?;
            if count > limits.max_elements {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "map length exceeds maximum",
                )));
            }
            let mut offset = 1 + consumed;
            let mut pairs = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let Some((key, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                    return Ok(None);
                };
                offset += used;
                let Some((value, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                    return Ok(None);
                };
                offset += used;
                pairs.push((key, value));
            }
            Ok(Some((RespValue::Map(pairs), offset)))
        }
        b'|' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let count = parse_i64(line)?;
            let count = usize::try_from(count).map_err(|_| {
                ParseError::Protocol(ProtocolError::new("invalid attribute length"))
            })?;
            if count > limits.max_elements {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "attribute length exceeds maximum",
                )));
            }
            let mut offset = 1 + consumed;
            let mut pairs = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let Some((key, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                    return Ok(None);
                };
                offset += used;
                let Some((value, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                    return Ok(None);
                };
                offset += used;
                pairs.push((key, value));
            }
            let Some((value, used)) = parse_value(&input[offset..], depth + 1, limits)? else {
                return Ok(None);
            };
            offset += used;
            Ok(Some((
                RespValue::Attribute {
                    pairs,
                    value: Box::new(value),
                },
                offset,
            )))
        }
        b'_' => {
            if input.len() < 3 {
                return Ok(None);
            }
            if &input[1..3] != b"\r\n" {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid null terminator",
                )));
            }
            Ok(Some((RespValue::Null, 3)))
        }
        b'#' => {
            if input.len() < 4 {
                return Ok(None);
            }
            let value = match input[1] {
                b't' => true,
                b'f' => false,
                _ => {
                    return Err(ParseError::Protocol(ProtocolError::new(
                        "invalid boolean value",
                    )));
                }
            };
            if &input[2..4] != b"\r\n" {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid boolean terminator",
                )));
            }
            Ok(Some((RespValue::Boolean(value), 4)))
        }
        b',' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let text = std::str::from_utf8(line)
                .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid double")))?;
            let value = match text {
                "inf" | "+inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                _ => text
                    .parse::<f64>()
                    .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid double")))?,
            };
            Ok(Some((RespValue::Double(value), consumed + 1)))
        }
        b'(' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            if line.is_empty()
                || !line
                    .iter()
                    .all(|b| b.is_ascii_digit() || *b == b'-' || *b == b'+')
            {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid big number",
                )));
            }
            Ok(Some((RespValue::BigNumber(line.to_vec()), consumed + 1)))
        }
        b'=' => {
            let Some((line, consumed)) = take_line(&input[1..])? else {
                return Ok(None);
            };
            let len = parse_i64(line)?;
            let len = usize::try_from(len)
                .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid verbatim length")))?;
            if len > limits.max_bulk_len {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "verbatim length exceeds maximum",
                )));
            }
            let start = 1 + consumed;
            if input.len() < start + len + 2 {
                return Ok(None);
            }
            if &input[start + len..start + len + 2] != b"\r\n" {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid verbatim terminator",
                )));
            }
            if len < 4 || input[start + 3] != b':' {
                return Err(ParseError::Protocol(ProtocolError::new(
                    "invalid verbatim format",
                )));
            }
            let mut format = [0u8; 3];
            format.copy_from_slice(&input[start..start + 3]);
            Ok(Some((
                RespValue::VerbatimString {
                    format,
                    data: input[start + 4..start + len].to_vec(),
                },
                start + len + 2,
            )))
        }
        _ => Err(ParseError::Protocol(ProtocolError::new(
            "invalid RESP type byte",
        ))),
    }
}

fn take_line(input: &[u8]) -> Result<Option<(&[u8], usize)>, ParseError> {
    let Some(pos) = input.windows(2).position(|w| w == b"\r\n") else {
        if input.len() > 64 * 1024 {
            return Err(ParseError::Protocol(ProtocolError::new(
                "too big inline request",
            )));
        }
        return Ok(None);
    };
    Ok(Some((&input[..pos], pos + 2)))
}

fn parse_i64(bytes: &[u8]) -> Result<i64, ParseError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid integer")))?;
    text.parse::<i64>()
        .map_err(|_| ParseError::Protocol(ProtocolError::new("invalid integer")))
}

/// Split an inline command using Redis' `sdssplitargs` rules.
pub fn split_inline(line: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
    let mut args = Vec::new();
    let mut current = Vec::new();
    let mut in_double = false;
    let mut in_single = false;
    let mut saw_token = false;
    let mut i = 0;

    while i < line.len() {
        let b = line[i];
        if in_double {
            if b == b'\\' && i + 1 < line.len() {
                let next = line[i + 1];
                match next {
                    b'x' if i + 3 < line.len() => {
                        let hi = hex_value(line[i + 2])?;
                        let lo = hex_value(line[i + 3])?;
                        current.push((hi << 4) | lo);
                        i += 4;
                        continue;
                    }
                    b'n' => current.push(b'\n'),
                    b'r' => current.push(b'\r'),
                    b't' => current.push(b'\t'),
                    b'b' => current.push(0x08),
                    b'a' => current.push(0x07),
                    other => current.push(other),
                }
                i += 2;
                continue;
            }
            if b == b'"' {
                in_double = false;
                i += 1;
                if i < line.len() && !line[i].is_ascii_whitespace() {
                    return Err(ProtocolError::new("unbalanced quotes in request"));
                }
                continue;
            }
            current.push(b);
            i += 1;
            continue;
        }
        if in_single {
            if b == b'\\' && i + 1 < line.len() && line[i + 1] == b'\'' {
                current.push(b'\'');
                i += 2;
                continue;
            }
            if b == b'\'' {
                in_single = false;
                i += 1;
                if i < line.len() && !line[i].is_ascii_whitespace() {
                    return Err(ProtocolError::new("unbalanced quotes in request"));
                }
                continue;
            }
            current.push(b);
            i += 1;
            continue;
        }

        match b {
            b' ' | b'\t' | b'\r' | b'\n' => {
                if saw_token {
                    args.push(std::mem::take(&mut current));
                    saw_token = false;
                }
                i += 1;
            }
            b'"' => {
                in_double = true;
                saw_token = true;
                i += 1;
            }
            b'\'' => {
                in_single = true;
                saw_token = true;
                i += 1;
            }
            _ => {
                current.push(b);
                saw_token = true;
                i += 1;
            }
        }
    }

    if in_double || in_single {
        return Err(ProtocolError::new("unbalanced quotes in request"));
    }
    if saw_token {
        args.push(current);
    }
    Ok(args)
}

fn hex_value(byte: u8) -> Result<u8, ProtocolError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ProtocolError::new("invalid hex digit in request")),
    }
}

/// Encode a client command. Useful for tests and tools.
pub fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        write_bulk(arg, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_one(input: &[u8]) -> Result<Option<Command>, ProtocolError> {
        let mut decoder = RespDecoder::default();
        decoder.feed(input);
        decoder.next_command()
    }

    #[test]
    fn decodes_multibulk_command() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let command = decode_one(input).unwrap().unwrap();
        assert_eq!(command.args, vec![b"GET".to_vec(), b"key".to_vec()]);
    }

    #[test]
    fn handles_partial_input() {
        let mut decoder = RespDecoder::default();
        decoder.feed(b"*2\r\n$3\r\nGE");
        assert!(decoder.next_command().unwrap().is_none());
        decoder.feed(b"T\r\n$3\r\nkey\r\n");
        let command = decoder.next_command().unwrap().unwrap();
        assert_eq!(command.args, vec![b"GET".to_vec(), b"key".to_vec()]);
    }

    #[test]
    fn decodes_inline_command() {
        let command = decode_one(b"PING\r\n").unwrap().unwrap();
        assert_eq!(command.args, vec![b"PING".to_vec()]);
    }

    #[test]
    fn decodes_inline_quotes_and_escapes() {
        let command = decode_one(b"SET \"a b\" \"\\x41\\n\"\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            command.args,
            vec![b"SET".to_vec(), b"a b".to_vec(), b"A\n".to_vec()]
        );
    }

    #[test]
    fn rejects_unbalanced_inline_quotes() {
        let err = decode_one(b"SET \"a\r\n").unwrap_err();
        assert!(err.message.contains("unbalanced"));
    }

    #[test]
    fn decodes_null_bulk() {
        let mut decoder = RespDecoder::default();
        decoder.feed(b"$-1\r\n");
        assert_eq!(decoder.next_value().unwrap(), Some(RespValue::Null));
    }

    #[test]
    fn decodes_resp3_types() {
        let mut decoder = RespDecoder::default();
        decoder.feed(b"%1\r\n+key\r\n:42\r\n");
        assert_eq!(
            decoder.next_value().unwrap(),
            Some(RespValue::Map(vec![(
                RespValue::SimpleString(b"key".to_vec()),
                RespValue::Integer(42)
            )]))
        );
        decoder.feed(b"~2\r\n+a\r\n+b\r\n");
        assert_eq!(
            decoder.next_value().unwrap(),
            Some(RespValue::Set(vec![
                RespValue::SimpleString(b"a".to_vec()),
                RespValue::SimpleString(b"b".to_vec())
            ]))
        );
    }

    #[test]
    fn encodes_resp2_fallbacks() {
        let value = RespValue::Map(vec![(
            RespValue::SimpleString(b"proto".to_vec()),
            RespValue::Integer(3),
        )]);
        assert_eq!(value.encoded(RespVersion::V2), b"*2\r\n+proto\r\n:3\r\n");
        assert_eq!(RespValue::Null.encoded(RespVersion::V2), b"$-1\r\n");
        assert_eq!(RespValue::Boolean(true).encoded(RespVersion::V2), b":1\r\n");
    }

    #[test]
    fn encodes_resp3_types() {
        assert_eq!(RespValue::Null.encoded(RespVersion::V3), b"_\r\n");
        assert_eq!(
            RespValue::Boolean(false).encoded(RespVersion::V3),
            b"#f\r\n"
        );
        assert_eq!(
            RespValue::Map(vec![(
                RespValue::SimpleString(b"proto".to_vec()),
                RespValue::Integer(3)
            )])
            .encoded(RespVersion::V3),
            b"%1\r\n+proto\r\n:3\r\n"
        );
    }

    #[test]
    fn encodes_resp3_attributes() {
        let value = RespValue::Attribute {
            pairs: vec![(
                RespValue::bulk(b"key-popularity".to_vec()),
                RespValue::Array(vec![
                    RespValue::bulk(b"key:123".to_vec()),
                    RespValue::Integer(90),
                ]),
            )],
            value: Box::new(RespValue::bulk(
                b"Some real reply following the attribute".to_vec(),
            )),
        };
        assert_eq!(
            value.encoded(RespVersion::V2),
            b"$39\r\nSome real reply following the attribute\r\n"
        );
        let resp3 = value.encoded(RespVersion::V3);
        assert!(resp3.starts_with(b"|1\r\n$14\r\nkey-popularity\r\n*2\r\n"));
        assert!(resp3.ends_with(b"$39\r\nSome real reply following the attribute\r\n"));
    }

    #[test]
    fn decodes_resp3_attributes() {
        let mut decoder = RespDecoder::default();
        decoder.feed(b"|1\r\n+k\r\n+v\r\n:42\r\n");
        let value = decoder.next_value().unwrap().unwrap();
        assert_eq!(
            value,
            RespValue::Attribute {
                pairs: vec![(
                    RespValue::SimpleString(b"k".to_vec()),
                    RespValue::SimpleString(b"v".to_vec())
                )],
                value: Box::new(RespValue::Integer(42))
            }
        );
    }

    #[test]
    fn command_encoder_roundtrips() {
        let encoded = encode_command(&[b"SET", b"key", b"value"]);
        let mut decoder = RespDecoder::default();
        decoder.feed(&encoded);
        let command = decoder.next_command().unwrap().unwrap();
        assert_eq!(
            command.args,
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
        );
    }
}
