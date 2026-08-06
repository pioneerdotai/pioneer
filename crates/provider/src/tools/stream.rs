use anyhow::{Result, anyhow, bail};

/// Incremental line decoder for provider SSE and NDJSON transports.
///
/// Network chunks are arbitrary byte ranges. They are deliberately retained as
/// bytes until a protocol line delimiter is observed, so a UTF-8 code point may
/// span any number of reads without data loss.
#[derive(Debug, Default)]
pub(crate) struct IncrementalLineDecoder {
    pending: Vec<u8>,
}

impl IncrementalLineDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>> {
        self.pending.extend_from_slice(bytes);
        let mut lines = Vec::new();
        while let Some(index) = self.pending.iter().position(|byte| *byte == b'\n') {
            let mut raw = self.pending.drain(..=index).collect::<Vec<_>>();
            raw.pop();
            if raw.last() == Some(&b'\r') {
                raw.pop();
            }
            let line = String::from_utf8(raw)
                .map_err(|error| anyhow!("provider stream contains invalid UTF-8: {error}"))?;
            lines.push(line);
        }
        Ok(lines)
    }

    pub(crate) fn finish(self) -> Result<()> {
        if self.pending.iter().all(u8::is_ascii_whitespace) {
            return Ok(());
        }
        if std::str::from_utf8(&self.pending).is_err() {
            bail!("provider stream ended inside an invalid UTF-8 sequence");
        }
        bail!("provider stream ended with an incomplete protocol frame")
    }
}

pub(crate) fn sse_data(line: &str) -> Option<&str> {
    let line = line.trim();
    line.strip_prefix("data:").map(str::trim_start)
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
}
