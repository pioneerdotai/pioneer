//! Legacy-only streaming projection. New writes continue to use the original
//! Value parsers. Every input byte is checked, but strings in ignored fields
//! and inline message content are never accumulated. Known object labels use
//! last-value-wins, including semantically invalid values later overwritten.
//!
//! Storage is O(depth + selected identity bytes), independent of message count,
//! delivered item count, padding and historyJson length. Each parser has at
//! most 127 frames, 64 bytes per pending key, and one numeric token <=1024
//! bytes. Only a fixed set of schema fields is retained; selected scalar
//! strings are <=64 KiB each. Arrays retain one projected item and an invalid
//! bit. CLI history strings add one history parser, never recursive CLI
//! parsers. Oversized selected strings/numbers are diagnosed as unsupported;
//! oversized unknown keys are discarded (and still reject a descriptor).
use super::PreparedFrozenRoot;
use anyhow::{Result, bail, ensure};
use serde_json::{Map, Value};

// serde_json's default remaining_depth starts at 128 and rejects the 128th
// open container. Discarded subtrees must retain the same syntax boundary.
const MAX_DEPTH: usize = 127;
const MAX_ID_BYTES: usize = 64 * 1024;
const MAX_KEY_BYTES: usize = 64;
const MAX_NUMBER_BYTES: usize = 1024;

pub(crate) struct FrozenRootStream {
    parser: Parser,
    receipt: Option<bool>,
}
impl FrozenRootStream {
    pub(crate) fn new_history() -> Self {
        Self {
            parser: Parser::new(Schema::History),
            receipt: None,
        }
    }
    pub(crate) fn new_cli(thread_receipt: bool) -> Self {
        Self {
            parser: Parser::new(Schema::Cli(thread_receipt)),
            receipt: Some(thread_receipt),
        }
    }
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        self.parser.feed(bytes)
    }
    pub(crate) fn finish(self) -> Result<PreparedFrozenRoot> {
        let json = serde_json::to_string(&self.parser.finish()?)?;
        match self.receipt {
            Some(receipt) => PreparedFrozenRoot::cli(&json, receipt),
            None => PreparedFrozenRoot::history(&json),
        }
    }
}

#[derive(Clone, Copy)]
enum Schema {
    History,
    Messages,
    Message,
    Descriptor,
    Cli(bool),
    Basis(bool),
    Turns,
    Sources,
    Turn,
    Source,
    HistoryString,
    Scalar,
    StringType,
    Discard,
}
struct Parser {
    schema: Schema,
    stack: Vec<Frame>,
    token: Option<Token>,
    root: Option<Value>,
}
struct Frame {
    schema: Schema,
    object: bool,
    // Object: 0 key-or-end, 1 key, 2 colon, 3 value, 4 comma-or-end.
    // Array: 0 value-or-end, 3 value, 4 comma-or-end.
    state: u8,
    key: Option<String>,
    fields: Map<String, Value>,
    invalid_item: bool,
}
enum Token {
    String(StringToken),
    Number(Vec<u8>),
    Literal(&'static [u8], usize),
}
impl Parser {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            stack: Vec::new(),
            token: None,
            root: None,
        }
    }
    fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        for &byte in bytes {
            self.byte(byte)?;
        }
        Ok(())
    }
    fn finish(mut self) -> Result<Value> {
        match self.token.take() {
            Some(Token::Number(bytes)) => self.value(serde_json::from_slice(&bytes)?)?,
            Some(Token::Literal(word, n)) if n == word.len() => {
                self.value(serde_json::from_slice(word)?)?
            }
            None => {}
            _ => bail!("incomplete legacy root JSON token"),
        }
        ensure!(
            self.stack.is_empty(),
            "incomplete legacy root JSON container"
        );
        self.root
            .ok_or_else(|| anyhow::anyhow!("empty legacy root JSON"))
    }
    fn byte(&mut self, byte: u8) -> Result<()> {
        if let Some(token) = self.token.take() {
            match token {
                Token::String(mut string) => {
                    if string.byte(byte)? {
                        let key = string.key;
                        let value = string.finish()?;
                        if key {
                            let frame = self.stack.last_mut().expect("object key frame");
                            frame.key = value.as_str().map(str::to_owned);
                            frame.state = 2;
                        } else {
                            self.value(value)?;
                        }
                    } else {
                        self.token = Some(Token::String(string));
                    }
                    return Ok(());
                }
                Token::Number(mut bytes) => {
                    if matches!(byte, b'0'..=b'9' | b'.' | b'-' | b'+' | b'e' | b'E') {
                        ensure!(
                            bytes.len() < MAX_NUMBER_BYTES,
                            "legacy root number exceeds streaming token bound"
                        );
                        bytes.push(byte);
                        self.token = Some(Token::Number(bytes));
                        return Ok(());
                    }
                    ensure!(delimiter(byte), "invalid legacy root number delimiter");
                    self.value(serde_json::from_slice(&bytes)?)?;
                }
                Token::Literal(word, n) => {
                    if n < word.len() {
                        ensure!(byte == word[n], "invalid legacy root literal");
                        self.token = Some(Token::Literal(word, n + 1));
                        return Ok(());
                    }
                    ensure!(delimiter(byte), "invalid legacy root literal delimiter");
                    self.value(serde_json::from_slice(word)?)?;
                }
            }
        }
        if byte.is_ascii_whitespace() {
            ensure!(
                matches!(byte, b' ' | b'\n' | b'\r' | b'\t'),
                "invalid JSON whitespace"
            );
            return Ok(());
        }
        if let Some(frame) = self.stack.last_mut() {
            if frame.object {
                match frame.state {
                    0 if byte == b'}' => return self.close(),
                    0 | 1 if byte == b'"' => {
                        self.token = Some(Token::String(StringToken::key()));
                        return Ok(());
                    }
                    2 if byte == b':' => {
                        frame.state = 3;
                        return Ok(());
                    }
                    4 if byte == b',' => {
                        frame.state = 1;
                        return Ok(());
                    }
                    4 if byte == b'}' => return self.close(),
                    3 => {}
                    _ => bail!("invalid legacy root JSON object"),
                }
            } else {
                match frame.state {
                    0 if byte == b']' => return self.close(),
                    4 if byte == b',' => {
                        frame.state = 3;
                        return Ok(());
                    }
                    4 if byte == b']' => return self.close(),
                    0 | 3 => {}
                    _ => bail!("invalid legacy root JSON array"),
                }
            }
        } else {
            ensure!(self.root.is_none(), "trailing legacy root JSON value");
        }
        let schema = self.stack.last().map(Frame::child).unwrap_or(self.schema);
        match byte {
            b'{' | b'[' => {
                ensure!(
                    self.stack.len() < MAX_DEPTH,
                    "legacy root exceeds streaming depth bound"
                );
                self.stack.push(Frame::new(schema, byte == b'{'));
            }
            b'"' => self.token = Some(Token::String(StringToken::value(schema))),
            b'n' => self.token = Some(Token::Literal(b"null", 1)),
            b't' => self.token = Some(Token::Literal(b"true", 1)),
            b'f' => self.token = Some(Token::Literal(b"false", 1)),
            b'-' | b'0'..=b'9' => self.token = Some(Token::Number(vec![byte])),
            _ => bail!("invalid legacy root JSON value"),
        }
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        let frame = self.stack.pop().expect("closing JSON frame");
        self.value(frame.finish())
    }
    fn value(&mut self, value: Value) -> Result<()> {
        if let Some(frame) = self.stack.last_mut() {
            frame.value(value)?;
            frame.state = 4;
        } else {
            ensure!(self.root.is_none(), "multiple legacy root JSON values");
            self.root = Some(value);
        }
        Ok(())
    }
}
impl Frame {
    fn new(schema: Schema, object: bool) -> Self {
        let schema = match (schema, object) {
            (Schema::History, true) => Schema::Descriptor,
            (Schema::History, false) => Schema::Messages,
            (
                Schema::Cli(_)
                | Schema::Basis(_)
                | Schema::Message
                | Schema::Descriptor
                | Schema::Turn
                | Schema::Source,
                true,
            ) => schema,
            (Schema::Messages | Schema::Turns | Schema::Sources, false) => schema,
            _ => Schema::Discard,
        };
        Self {
            schema,
            object,
            state: 0,
            key: None,
            fields: Map::new(),
            invalid_item: false,
        }
    }
    fn child(&self) -> Schema {
        let key = self.key.as_deref().unwrap_or("");
        match self.schema {
            Schema::Messages => Schema::Message,
            Schema::Turns => Schema::Turn,
            Schema::Sources => Schema::Source,
            Schema::Cli(receipt)
                if key
                    == if receipt {
                        "pioneerContext"
                    } else {
                        "pioneerContextBasis"
                    } =>
            {
                Schema::Basis(receipt)
            }
            Schema::Basis(receipt) => match key {
                "deliveredTurns" => Schema::Turns,
                "deliveredSources" => Schema::Sources,
                "pendingTurn" => Schema::Turn,
                "continuationHead" if receipt => Schema::Turn,
                "contextHistoryJson" if receipt => Schema::HistoryString,
                "historyJson" if !receipt => Schema::HistoryString,
                "contextManifestOwnerThreadId"
                | "contextOwnerThreadId"
                | "nativeThreadId"
                | "acceptedTurnId"
                | "acceptedTurnRevision"
                | "acceptedTurnDeleted"
                | "version"
                    if receipt =>
                {
                    Schema::Scalar
                }
                "manifestOwnerThreadId" | "executionThreadId" if !receipt => Schema::Scalar,
                _ => Schema::Discard,
            },
            Schema::Descriptor => match key {
                "format" | "manifest_id" | "messages" | "identity_sha256" => Schema::Scalar,
                _ => Schema::Discard,
            },
            Schema::Message => match key {
                "role" => Schema::Scalar,
                "content" => Schema::StringType,
                _ => Schema::Discard,
            },
            Schema::Turn => match key {
                "turnId" | "threadId" | "messageRevision" | "messageDeleted" => Schema::Scalar,
                _ => Schema::Discard,
            },
            Schema::Source => match key {
                "sourceThreadId" | "scope" | "id" | "version" => Schema::Scalar,
                _ => Schema::Discard,
            },
            _ => Schema::Discard,
        }
    }
    fn value(&mut self, value: Value) -> Result<()> {
        if self.object {
            if !matches!(self.child(), Schema::Discard) {
                self.fields
                    .insert(self.key.take().expect("selected JSON key"), value);
            } else if matches!(self.schema, Schema::Descriptor) {
                // FrozenHistoryRef denies unknown fields. One fixed label is
                // enough to retain that rejection, without retaining names.
                self.fields.insert("__unknown".into(), Value::Null);
            }
            self.key = None;
        } else {
            let valid = match self.schema {
                Schema::Messages => {
                    PreparedFrozenRoot::history(&Value::Array(vec![value]).to_string()).is_ok()
                }
                Schema::Turns | Schema::Sources => {
                    // Use the original validator per item, then collapse the
                    // arbitrarily long array. Invalid items cannot be hidden
                    // by later valid items; a later duplicate array CAN replace
                    // the entire result, just as Value::Object does.
                    let key = if matches!(self.schema, Schema::Turns) {
                        "deliveredTurns"
                    } else {
                        "deliveredSources"
                    };
                    let mut basis = serde_json::json!({"version":4,"nativeThreadId":"_","acceptedTurnId":"_","acceptedTurnRevision":0,"acceptedTurnDeleted":false});
                    basis[key] = Value::Array(vec![value]);
                    PreparedFrozenRoot::cli(
                        &serde_json::json!({"pioneerContext":basis}).to_string(),
                        true,
                    )
                    .is_ok()
                }
                _ => true,
            };
            self.invalid_item |= !valid;
        }
        Ok(())
    }
    fn finish(self) -> Value {
        if self.object {
            Value::Object(self.fields)
        } else if self.invalid_item {
            Value::Array(vec![Value::Null])
        } else {
            Value::Array(Vec::new())
        }
    }
}

struct StringToken {
    key: bool,
    sink: Sink,
    state: Escape,
    utf8: [u8; 4],
    utf8_len: usize,
    utf8_need: usize,
}
enum Sink {
    Key(Vec<u8>, bool),
    Small(Vec<u8>),
    Discard,
    History(Option<Box<Parser>>),
}
enum Escape {
    Plain,
    Slash,
    Hex(u16, u8),
    LowSlash(u16),
    LowU(u16),
    LowHex(u16, u16, u8),
}
impl StringToken {
    fn key() -> Self {
        Self::new(true, Sink::Key(Vec::new(), false))
    }
    fn value(schema: Schema) -> Self {
        let sink = match schema {
            Schema::Scalar => Sink::Small(Vec::new()),
            Schema::HistoryString => Sink::History(Some(Box::new(Parser::new(Schema::History)))),
            _ => Sink::Discard,
        };
        Self::new(false, sink)
    }
    fn new(key: bool, sink: Sink) -> Self {
        Self {
            key,
            sink,
            state: Escape::Plain,
            utf8: [0; 4],
            utf8_len: 0,
            utf8_need: 0,
        }
    }
    fn emit(&mut self, bytes: &[u8]) -> Result<()> {
        match &mut self.sink {
            Sink::Key(value, overflow) => {
                if value.len() + bytes.len() <= MAX_KEY_BYTES && !*overflow {
                    value.extend_from_slice(bytes);
                } else {
                    *overflow = true;
                    value.clear();
                }
            }
            Sink::Small(value) => {
                ensure!(
                    value.len() + bytes.len() <= MAX_ID_BYTES,
                    "legacy root identity exceeds streaming string bound"
                );
                value.extend_from_slice(bytes);
            }
            Sink::History(parser) => {
                // Nested semantic/syntax failures belong to this field value.
                // Keep a non-string marker so a last duplicate can overwrite
                // it; never turn a failed nested history into absent/null.
                if parser.as_mut().is_some_and(|p| p.feed(bytes).is_err()) {
                    *parser = None;
                }
            }
            Sink::Discard => {}
        }
        Ok(())
    }
    fn scalar(&mut self, value: u32) -> Result<()> {
        let c =
            char::from_u32(value).ok_or_else(|| anyhow::anyhow!("invalid JSON Unicode scalar"))?;
        self.emit(c.encode_utf8(&mut [0; 4]).as_bytes())
    }
    fn byte(&mut self, byte: u8) -> Result<bool> {
        if self.utf8_len != 0 {
            ensure!(
                (0x80..=0xbf).contains(&byte),
                "invalid legacy root UTF-8 continuation"
            );
            self.utf8[self.utf8_len] = byte;
            self.utf8_len += 1;
            if self.utf8_len == self.utf8_need {
                let bytes = self.utf8;
                std::str::from_utf8(&bytes[..self.utf8_len])?;
                self.emit(&bytes[..self.utf8_len])?;
                self.utf8_len = 0;
            }
            return Ok(false);
        }
        let state = std::mem::replace(&mut self.state, Escape::Plain);
        match state {
            Escape::Plain => match byte {
                b'"' => return Ok(true),
                b'\\' => self.state = Escape::Slash,
                0..=0x1f => bail!("unescaped control in legacy root JSON string"),
                0x20..=0x7f => self.emit(&[byte])?,
                _ => {
                    self.utf8_need = match byte {
                        0xc2..=0xdf => 2,
                        0xe0..=0xef => 3,
                        0xf0..=0xf4 => 4,
                        _ => bail!("invalid legacy root UTF-8 lead"),
                    };
                    self.utf8[0] = byte;
                    self.utf8_len = 1;
                }
            },
            Escape::Slash => match byte {
                b'"' | b'\\' | b'/' => self.emit(&[byte])?,
                b'b' => self.emit(&[8])?,
                b'f' => self.emit(&[12])?,
                b'n' => self.emit(&[10])?,
                b'r' => self.emit(&[13])?,
                b't' => self.emit(&[9])?,
                b'u' => self.state = Escape::Hex(0, 0),
                _ => bail!("invalid legacy root JSON escape"),
            },
            Escape::Hex(value, digits) => {
                let value = (value << 4) | hex_digit(byte)?;
                if digits == 3 {
                    if (0xd800..=0xdbff).contains(&value) {
                        self.state = Escape::LowSlash(value);
                    } else {
                        self.scalar(value as u32)?;
                    }
                } else {
                    self.state = Escape::Hex(value, digits + 1);
                }
            }
            Escape::LowSlash(high) => {
                ensure!(byte == b'\\', "missing JSON low surrogate escape");
                self.state = Escape::LowU(high);
            }
            Escape::LowU(high) => {
                ensure!(byte == b'u', "missing JSON low surrogate");
                self.state = Escape::LowHex(high, 0, 0);
            }
            Escape::LowHex(high, low, digits) => {
                let low = (low << 4) | hex_digit(byte)?;
                if digits == 3 {
                    ensure!(
                        (0xdc00..=0xdfff).contains(&low),
                        "invalid JSON low surrogate"
                    );
                    self.scalar(
                        0x10000 + (((high as u32 - 0xd800) << 10) | (low as u32 - 0xdc00)),
                    )?;
                } else {
                    self.state = Escape::LowHex(high, low, digits + 1);
                }
            }
        }
        Ok(false)
    }
    fn finish(self) -> Result<Value> {
        Ok(match self.sink {
            Sink::Key(_, true) => Value::Null,
            Sink::Key(bytes, false) | Sink::Small(bytes) => {
                Value::String(String::from_utf8(bytes)?)
            }
            Sink::Discard => Value::String(String::new()),
            Sink::History(parser) => match parser.and_then(|p| p.finish().ok()) {
                Some(value) => Value::String(serde_json::to_string(&value)?),
                None => Value::Bool(false),
            },
        })
    }
}
fn hex_digit(byte: u8) -> Result<u16> {
    match byte {
        b'0'..=b'9' => Ok((byte - b'0') as u16),
        b'a'..=b'f' => Ok((byte - b'a' + 10) as u16),
        b'A'..=b'F' => Ok((byte - b'A' + 10) as u16),
        _ => bail!("invalid legacy root JSON Unicode escape"),
    }
}
fn delimiter(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history(bytes: &[u8]) -> Result<PreparedFrozenRoot> {
        let mut stream = FrozenRootStream::new_history();
        // Single-byte delivery crosses every UTF-8, escape and token boundary.
        for byte in bytes {
            stream.feed(std::slice::from_ref(byte))?;
        }
        stream.finish()
    }
    fn cli(bytes: &[u8], receipt: bool) -> Result<PreparedFrozenRoot> {
        let mut stream = FrozenRootStream::new_cli(receipt);
        for byte in bytes {
            stream.feed(std::slice::from_ref(byte))?;
        }
        stream.finish()
    }

    #[test]
    fn inline_history_retains_last_fields_and_validates_discarded_json() {
        for json in [
            r#"[]"#,
            r#"[{"role":"bad","role":"user","content":null,"content":"ok"}]"#,
            r#"[{"role":"\u0075ser","content":"я\uD83D\uDE00","ignored":{"x":[false,null,-1.25e+2]}}]"#,
        ] {
            assert_eq!(history(json.as_bytes()).unwrap().locator(), None, "{json}");
        }
        for bytes in [
            br#"[{"role":"user","content":"ok","role":"bad"}]"#.as_slice(),
            br#"[{"role":"user","content":"ok"},null]"#,
            br#"[{"role":"user","content":"ok","ignored":"\uD800"}]"#,
            br#"[{"role":"user","content":"ok","ignored":"\uDC00"}]"#,
            br#"[{"role":"user","content":"ok","ignored":1e999}]"#,
            br#"[{"role":"user","content":"ok","ignored":01}]"#,
            b"[{\"role\":\"user\",\"content\":\"\xc0\x80\"}]",
            br#"[{"role":"user","content":"ok",}]"#,
            br#"[] true"#,
        ] {
            assert!(history(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn descriptor_rejects_unknown_fields_and_non_integer_counts() {
        let descriptor = format!(
            r#"{{"format":1,"manifest_id":"m","messages":2,"identity_sha256":"{}"}}"#,
            "a".repeat(64)
        );
        assert_eq!(
            history(descriptor.as_bytes()).unwrap().locator().as_deref(),
            Some("m")
        );
        let value: Value = serde_json::from_str(&descriptor).unwrap();
        for (key, replacement) in [
            ("messages", serde_json::json!(2.0)),
            ("messages", serde_json::json!(u64::MAX)),
            ("format", serde_json::json!(2)),
            ("identity_sha256", serde_json::json!("z".repeat(64))),
            ("unexpected", Value::Null),
        ] {
            let mut invalid = value.clone();
            invalid[key] = replacement;
            assert!(
                history(invalid.to_string().as_bytes()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn cli_duplicates_can_replace_invalid_nested_history_and_delivered_arrays() {
        let json = br#"{"pioneerContextBasis":false,"pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":18446744073709551615,"messageDeleted":false},"deliveredTurns":[null],"deliveredTurns":[{"turnId":"x","turnId":"last","threadId":"t","messageRevision":0,"messageDeleted":true}],"deliveredSources":[false],"deliveredSources":[{"sourceThreadId":"t","scope":"s","id":"i","version":"v"}],"historyJson":"not JSON","historyJson":"[{\"role\":\"user\",\"content\":\"\\uD83D\\uDE00\"}]"}}"#;
        assert_eq!(cli(json, false).unwrap().locator(), None);
        for json in [
            r#"{"pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":0.0,"messageDeleted":false},"historyJson":"[]"}}"#,
            r#"{"pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":0,"messageDeleted":false},"historyJson":"[]","historyJson":"not JSON"}}"#,
            r#"{"pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":0,"messageDeleted":false},"deliveredTurns":[{"turnId":"x","threadId":"","messageRevision":0,"messageDeleted":false}],"historyJson":"[]"}}"#,
            r#"{"pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":0,"messageDeleted":false},"deliveredSources":[{"sourceThreadId":"t","scope":"s","id":"i","version":""}],"historyJson":"[]"}}"#,
        ] {
            assert!(cli(json.as_bytes(), false).is_err(), "{json}");
        }
    }

    #[test]
    fn receipt_preserves_owner_fallback_and_continuation_type_rules() {
        let descriptor = serde_json::json!({"format":1,"manifest_id":"m","messages":0,"identity_sha256":"a".repeat(64)}).to_string();
        let mut root = serde_json::json!({"pioneerContext":{"version":4,"nativeThreadId":"n","acceptedTurnId":"a","acceptedTurnRevision":u64::MAX,"acceptedTurnDeleted":false,"contextOwnerThreadId":"owner","contextHistoryJson":descriptor,"continuationHead":{"turnId":"c","threadId":"","messageRevision":0,"messageDeleted":false}}});
        assert_eq!(
            cli(root.to_string().as_bytes(), true)
                .unwrap()
                .locator()
                .as_deref(),
            Some("m")
        );
        // An explicit empty string prevents fallback, whereas a wrong-type
        // owner still falls back exactly as Value::as_str does.
        root["pioneerContext"]["contextManifestOwnerThreadId"] = serde_json::json!(false);
        assert!(cli(root.to_string().as_bytes(), true).is_ok());
        root["pioneerContext"]["contextManifestOwnerThreadId"] = serde_json::json!("");
        assert!(cli(root.to_string().as_bytes(), true).is_err());
        root["pioneerContext"]["contextManifestOwnerThreadId"] = serde_json::json!("owner");
        root["pioneerContext"]["version"] = serde_json::json!(4.0);
        assert!(cli(root.to_string().as_bytes(), true).is_err());
    }

    #[test]
    fn nested_inline_history_and_padding_above_eight_mib_are_streamed() {
        let mut stream = FrozenRootStream::new_cli(false);
        let mut direct = FrozenRootStream::new_history();
        direct.feed(br#"[{"role":"user","content":""#).unwrap();
        stream.feed(br#"{"padding":""#).unwrap();
        let fragment = [b'x'; 64 * 1024];
        for _ in 0..129 {
            stream.feed(&fragment).unwrap();
        }
        stream.feed(br#"","pioneerContextBasis":{"executionThreadId":"t","pendingTurn":{"turnId":"p","messageRevision":0,"messageDeleted":false},"historyJson":"[{\"role\":\"user\",\"content\":\""#).unwrap();
        for _ in 0..129 {
            stream.feed(&fragment).unwrap();
            direct.feed(&fragment).unwrap();
        }
        stream.feed(br#"\"}]"}}"#).unwrap();
        direct.feed(br#""}]"#).unwrap();
        assert_eq!(stream.finish().unwrap().locator(), None);
        assert_eq!(direct.finish().unwrap().locator(), None);
    }
}
