//! Incremental disclosure inspection of canonical saved observer envelopes.
//! Payload text is never assembled. Only wrapper keys, one reference, and the
//! structural path are retained; embedded JSON strings are decoded as a stream.
use anyhow::{Result, ensure};

const WRAPPERS: &[&str] = &[
    "value",
    "output",
    "result",
    "structuredContent",
    "content",
    "text",
    "item",
    "storage",
    "metadata",
    "stdout",
    "aggregated_output",
];
#[derive(Clone, Copy, PartialEq)]
enum Path {
    Wrapper,
    Threads,
    Thread,
    Events,
    Event,
    Content,
    Reference,
    Ignore,
}
struct Frame {
    path: Path,
    object: bool,
    key: String,
    key_expected: bool,
}
enum StringValue {
    Key(String, bool),
    Reference(String),
    ResultText,
    Embedded(Box<SourceReferences>),
    Ignore,
}
struct StringToken {
    value: StringValue,
    escape: String,
}
#[derive(Default)]
pub(super) struct SourceReferences {
    frames: Vec<Frame>,
    string: Option<StringToken>,
    primitive: bool,
    started: bool,
    finished: bool,
    ignored: bool,
    pub found_response: bool,
    references: Vec<String>,
    capture_text: bool,
    result_text: String,
    found_text: bool,
}
impl SourceReferences {
    pub fn result_text() -> Self {
        Self {
            capture_text: true,
            ..Self::default()
        }
    }
    pub fn take_result_text(&mut self) -> String {
        let mut text = std::mem::take(&mut self.result_text);
        if let Some(StringToken {
            value: StringValue::Embedded(child),
            ..
        }) = &mut self.string
        {
            text.push_str(&child.take_result_text());
        }
        text
    }
    pub fn feed(&mut self, text: &str) -> Result<()> {
        for ch in text.chars() {
            self.character(ch)?;
        }
        Ok(())
    }
    pub fn take_references(&mut self) -> Vec<String> {
        let mut references = std::mem::take(&mut self.references);
        if let Some(StringToken {
            value: StringValue::Embedded(child),
            ..
        }) = &mut self.string
        {
            references.extend(child.take_references());
        }
        references
    }
    pub fn finish(&self) -> Result<bool> {
        ensure!(
            self.ignored || (self.finished && self.string.is_none() && self.frames.is_empty()),
            "saved observer envelope is incomplete"
        );
        Ok(self.found_response || (self.capture_text && self.found_text))
    }
    fn path(&self) -> Path {
        let Some(frame) = self.frames.last() else {
            return Path::Wrapper;
        };
        match frame.path {
            Path::Wrapper if frame.key == "threads" => Path::Threads,
            Path::Wrapper if frame.key == "events" => Path::Events,
            Path::Wrapper if WRAPPERS.contains(&frame.key.as_str()) || !frame.object => {
                Path::Wrapper
            }
            Path::Threads => Path::Thread,
            Path::Events => Path::Event,
            Path::Event if frame.key == "content" => Path::Content,
            Path::Event | Path::Content
                if ["sourceThreadReference", "childReference"].contains(&frame.key.as_str()) =>
            {
                Path::Reference
            }
            _ => Path::Ignore,
        }
    }
    fn character(&mut self, ch: char) -> Result<()> {
        if self.ignored {
            return Ok(());
        }
        if let Some(mut token) = self.string.take() {
            if !token.escape.is_empty() {
                token.escape.push(ch);
                let escape = &token.escape;
                let needed = if escape.starts_with("\\u") { 6 } else { 2 };
                if escape.len() < needed {
                    self.string = Some(token);
                    return Ok(());
                }
                // A high surrogate is decoded together with its low surrogate.
                if escape.len() == 6 && escape.starts_with("\\u") {
                    let code = u16::from_str_radix(&escape[2..], 16)?;
                    if (0xd800..=0xdbff).contains(&code) {
                        self.string = Some(token);
                        return Ok(());
                    }
                }
                if escape.len() > 6 && escape.len() < 12 {
                    self.string = Some(token);
                    return Ok(());
                }
                let decoded: String = serde_json::from_str(&format!("\"{}\"", token.escape))?;
                token.escape.clear();
                for decoded in decoded.chars() {
                    self.string_character(&mut token.value, decoded)?;
                }
                self.string = Some(token);
                return Ok(());
            }
            if ch == '\\' {
                token.escape.push(ch);
                self.string = Some(token);
                return Ok(());
            }
            if ch != '"' {
                self.string_character(&mut token.value, ch)?;
                self.string = Some(token);
                return Ok(());
            }
            match token.value {
                StringValue::Key(key, discarded) => {
                    let frame = self.frames.last_mut().expect("key has an object");
                    frame.key = if discarded { String::new() } else { key };
                    frame.key_expected = false;
                }
                StringValue::Reference(reference) => self.references.push(reference),
                StringValue::Embedded(mut child) => {
                    self.found_response |= child.finish()?;
                    self.found_text |= child.found_text;
                    self.references.extend(child.take_references());
                    self.result_text.push_str(&child.take_result_text());
                }
                StringValue::Ignore | StringValue::ResultText => {}
            }
            return Ok(());
        }
        if self.primitive {
            if !matches!(ch, ',' | ']' | '}') {
                return Ok(());
            }
            self.primitive = false;
        }
        if ch.is_whitespace() {
            return Ok(());
        }
        if !self.started {
            self.started = true;
            if !matches!(ch, '{' | '[') {
                self.ignored = true;
                return Ok(());
            }
        }
        ensure!(!self.finished, "invalid saved observer envelope");
        match ch {
            '{' | '[' => {
                let path = self.path();
                if matches!(path, Path::Threads | Path::Events) {
                    self.found_response = true;
                }
                self.frames.push(Frame {
                    path,
                    object: ch == '{',
                    key: String::new(),
                    key_expected: ch == '{',
                });
            }
            '}' | ']' => {
                let frame = self
                    .frames
                    .pop()
                    .ok_or_else(|| anyhow::anyhow!("invalid observer envelope"))?;
                ensure!(frame.object == (ch == '}'), "invalid observer envelope");
                if self.frames.is_empty() {
                    self.finished = true;
                }
            }
            ',' => {
                if let Some(frame) = self.frames.last_mut() {
                    frame.key_expected = frame.object;
                    frame.key.clear();
                }
            }
            ':' => {}
            '"' => {
                let value = if self.frames.last().is_some_and(|frame| frame.key_expected) {
                    StringValue::Key(String::new(), false)
                } else if self.capture_text
                    && self
                        .frames
                        .last()
                        .is_some_and(|frame| frame.path == Path::Wrapper && frame.key == "text")
                {
                    self.found_text = true;
                    StringValue::ResultText
                } else if self.frames.last().is_some_and(|frame| {
                    matches!(frame.path, Path::Wrapper | Path::Thread | Path::Reference)
                        && ["threadId", "parentThreadId", "rootThreadId"]
                            .contains(&frame.key.as_str())
                }) {
                    StringValue::Reference(String::new())
                } else if self.path() == Path::Wrapper {
                    StringValue::Embedded(Box::new(Self {
                        capture_text: self.capture_text,
                        ..Self::default()
                    }))
                } else {
                    StringValue::Ignore
                };
                self.string = Some(StringToken {
                    value,
                    escape: String::new(),
                });
            }
            _ => self.primitive = true,
        }
        Ok(())
    }
    fn string_character(&mut self, value: &mut StringValue, ch: char) -> Result<()> {
        match value {
            StringValue::Key(key, discarded) => {
                // Keys outside this vocabulary cannot affect disclosure. Stop
                // retaining them as soon as they cannot match a known key.
                if !*discarded {
                    key.push(ch);
                    let known = WRAPPERS.iter().copied().chain([
                        "threads",
                        "events",
                        "threadId",
                        "parentThreadId",
                        "rootThreadId",
                        "sourceThreadReference",
                        "childReference",
                    ]);
                    if !known
                        .into_iter()
                        .any(|candidate| candidate.starts_with(key.as_str()))
                    {
                        key.clear();
                        *discarded = true;
                    }
                }
            }
            StringValue::Reference(reference) => reference.push(ch),
            StringValue::ResultText => self.result_text.push(ch),
            StringValue::Embedded(child) => child.character(ch)?,
            StringValue::Ignore => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escaped_wrappers_preserve_references_and_never_inspect_user_message_json() {
        let history = serde_json::json!({"threadId":"source", "events":[
            {"content":{"item":{"text":"{\"threadId\":\"private-user-text\",\"threads\":[]}"},
                "sourceThreadReference":{"threadId":"referenced-🌍"}}},
            {"childReference":{"threadId":"child"}}
        ],"eof":true});
        let mut wrapper = history;
        for _ in 0..20 {
            wrapper = serde_json::json!({"value":wrapper});
        }
        let encoded = serde_json::json!({"content":serde_json::json!({"output":wrapper.to_string()}).to_string()}).to_string();
        let mut scanner = SourceReferences::default();
        let mut found = Vec::new();
        for ch in encoded.chars() {
            scanner.feed(&ch.to_string()).unwrap();
            found.extend(scanner.take_references());
        }
        assert!(scanner.finish().unwrap());
        found.sort();
        assert_eq!(found, vec!["child", "referenced-🌍", "source"]);
    }
}
