use anyhow::{Result, anyhow};

fn incomplete(detail: &'static str) -> anyhow::Error {
    anyhow::Error::from(crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker)
        .context(detail)
}

/// Incremental line decoder for provider SSE and NDJSON transports.
///
/// Network chunks are arbitrary byte ranges. They are deliberately retained as
/// bytes until a protocol line delimiter is observed, so a UTF-8 code point may
/// span any number of reads without data loss.
#[derive(Debug, Default)]
pub(crate) struct IncrementalLineDecoder {
    pending: Vec<u8>,
    skip_lf: bool,
}

impl IncrementalLineDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>> {
        let mut lines = Vec::new();
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                let raw = std::mem::take(&mut self.pending);
                let line = String::from_utf8(raw)
                    .map_err(|_| anyhow!("provider stream contains invalid UTF-8"))?;
                lines.push(line);
                self.skip_lf = byte == b'\r';
            } else {
                self.pending.push(byte);
            }
        }
        Ok(lines)
    }

    pub(crate) fn finish(self) -> Result<()> {
        if self.pending.iter().all(u8::is_ascii_whitespace) {
            return Ok(());
        }
        if std::str::from_utf8(&self.pending).is_err() {
            return Err(incomplete(
                "provider stream ended inside an invalid UTF-8 sequence",
            ));
        }
        Err(incomplete(
            "provider stream ended with an incomplete protocol frame",
        ))
    }
}

/// SSE dispatches only at a blank line; data fields within one event are joined
/// with newlines. No reconnect/retry is performed for inference streams.
/// The owning HTTP transport bounds the total bytes, including unfinished data.
/// Framing: https://html.spec.whatwg.org/multipage/server-sent-events.html
/// Each adapter's terminal contract takes precedence over a clean transport EOF.
#[derive(Debug, Default)]
pub(crate) struct IncrementalSseDecoder {
    lines: IncrementalLineDecoder,
    data: Vec<String>,
    event: Option<String>,
    started: bool,
}

#[derive(Debug)]
pub(crate) struct SseEvent {
    pub(crate) event: Option<String>,
    pub(crate) data: String,
}

impl IncrementalSseDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>> {
        let mut events = Vec::new();
        for line in self.lines.push(bytes)? {
            let line = if !self.started {
                self.started = true;
                line.strip_prefix('\u{feff}').unwrap_or(&line)
            } else {
                &line
            };
            if line.is_empty() {
                let event = self.event.take();
                if !self.data.is_empty() {
                    events.push(SseEvent {
                        event,
                        data: std::mem::take(&mut self.data).join("\n"),
                    });
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "data" => self.data.push(value.to_owned()),
                "event" => self.event = Some(value.to_owned()),
                // id/retry are EventSource reconnect controls, not generation IDs.
                _ => {}
            }
        }
        Ok(events)
    }

    pub(crate) fn finish(self) -> Result<()> {
        self.lines.finish()?;
        if !self.data.is_empty() || self.event.is_some() {
            return Err(incomplete(
                "provider stream ended with an incomplete SSE event",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_utf8_split_reassembles_identically() {
        let payload = "data: {\"text\":\"Привет 🌍\"}\r\n\r\n".as_bytes();
        for split in 0..=payload.len() {
            let mut decoder = IncrementalLineDecoder::default();
            let mut lines = decoder.push(&payload[..split]).unwrap();
            lines.extend(decoder.push(&payload[split..]).unwrap());
            decoder.finish().unwrap();
            assert_eq!(lines, vec!["data: {\"text\":\"Привет 🌍\"}", ""]);
        }
    }

    #[test]
    fn trailing_frame_and_invalid_utf8_fail_closed() {
        let mut trailing = IncrementalLineDecoder::default();
        trailing.push(b"data: {\"partial\":true}").unwrap();
        assert!(trailing.finish().is_err());

        let mut invalid = IncrementalLineDecoder::default();
        assert!(invalid.push(&[0xff, b'\n']).is_err());
    }
    #[test]
    fn sse_multiline_bom_comments_and_all_line_endings_survive_every_split() {
        let input = "\u{feff}: heartbeat\r\nevent: error\rdata: {\"type\":\"error\",\rdata: \"error\":{\"type\":\"overloaded_error\"}}\r\r".as_bytes();
        for split in 0..=input.len() {
            let mut decoder = IncrementalSseDecoder::default();
            let mut events = decoder.push(&input[..split]).unwrap();
            events.extend(decoder.push(&input[split..]).unwrap());
            decoder.finish().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event.as_deref(), Some("error"));
            let value: serde_json::Value = serde_json::from_str(&events[0].data).unwrap();
            assert_eq!(value["error"]["type"], "overloaded_error");
        }
    }

    #[test]
    fn complete_data_line_without_dispatch_delimiter_is_not_an_event() {
        let mut decoder = IncrementalSseDecoder::default();
        assert!(decoder.push(b"data: [DONE]\n").unwrap().is_empty());
        assert!(decoder.finish().is_err());
    }
}
