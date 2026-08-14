use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEventBudget {
    pub profile_version: u32,
    pub max_frame_bytes: usize,
    pub max_json_depth: usize,
    pub max_json_nodes: usize,
    pub max_string_bytes: usize,
    pub max_journal_payload_bytes: usize,
}

impl Default for NativeEventBudget {
    fn default() -> Self {
        Self {
            profile_version: 1,
            max_frame_bytes: 1024 * 1024,
            max_json_depth: 64,
            max_json_nodes: 16_384,
            max_string_bytes: 256 * 1024,
            max_journal_payload_bytes: 256 * 1024,
        }
    }
}

impl NativeEventBudget {
    pub const fn is_valid(self) -> bool {
        self.profile_version > 0
            && self.max_frame_bytes > 0
            && self.max_json_depth > 0
            && self.max_json_nodes > 0
            && self.max_string_bytes > 0
            && self.max_string_bytes <= self.max_frame_bytes
            && self.max_journal_payload_bytes > 0
    }

    /// Intersects an immutable admitted ceiling with the currently installed
    /// role ceiling. A policy update may narrow an existing execution, but it
    /// can never widen the process generation that was originally admitted.
    pub const fn intersect(self, current: Self) -> Option<Self> {
        if !self.is_valid()
            || !current.is_valid()
            || self.profile_version != current.profile_version
        {
            return None;
        }
        Some(Self {
            profile_version: self.profile_version,
            max_frame_bytes: min_usize(self.max_frame_bytes, current.max_frame_bytes),
            max_json_depth: min_usize(self.max_json_depth, current.max_json_depth),
            max_json_nodes: min_usize(self.max_json_nodes, current.max_json_nodes),
            max_string_bytes: min_usize(self.max_string_bytes, current.max_string_bytes),
            max_journal_payload_bytes: min_usize(
                self.max_journal_payload_bytes,
                current.max_journal_payload_bytes,
            ),
        })
    }
}

const fn min_usize(left: usize, right: usize) -> usize {
    if left < right { left } else { right }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIngressErrorKind {
    FrameTooLarge,
    InvalidUtf8,
    InvalidJson,
    JsonTooDeep,
    JsonTooManyNodes,
    JsonStringTooLarge,
    Io,
}

#[derive(Debug)]
pub struct NativeIngressError {
    pub kind: NativeIngressErrorKind,
    message: String,
}

impl NativeIngressError {
    fn new(kind: NativeIngressErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for NativeIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for NativeIngressError {}

#[derive(Debug, Clone, Copy)]
pub struct BoundedNativeEventCodec {
    budget: NativeEventBudget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedNativeFrame {
    Complete(Vec<u8>),
    Oversized {
        prefix: Vec<u8>,
        total_bytes: usize,
        last_non_whitespace: Option<u8>,
    },
}

impl BoundedNativeEventCodec {
    pub const fn new(budget: NativeEventBudget) -> Self {
        Self { budget }
    }

    pub const fn budget(self) -> NativeEventBudget {
        self.budget
    }

    pub async fn read_frame<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Option<Vec<u8>>, NativeIngressError> {
        let mut frame = Vec::with_capacity(self.budget.max_frame_bytes.min(8 * 1024));
        loop {
            let available = reader.fill_buf().await.map_err(|error| {
                NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
            })?;
            if available.is_empty() {
                return if frame.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(frame))
                };
            }
            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            if frame.len().saturating_add(consumed) > self.budget.max_frame_bytes {
                return Err(NativeIngressError::new(
                    NativeIngressErrorKind::FrameTooLarge,
                    format!(
                        "native JSONL frame exceeds {} bytes",
                        self.budget.max_frame_bytes
                    ),
                ));
            }
            let ended = available.get(consumed.saturating_sub(1)) == Some(&b'\n');
            frame.extend_from_slice(&available[..consumed]);
            reader.consume(consumed);
            if ended {
                return Ok(Some(frame));
            }
        }
    }

    /// Reads one JSONL frame while retaining at most `max_frame_bytes`.
    ///
    /// Unlike [`Self::read_frame`], this method drains an oversized frame to
    /// its newline so a caller can make a narrow, protocol-specific recovery
    /// decision without losing alignment with the next frame.
    pub async fn read_frame_or_drain<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Option<BoundedNativeFrame>, NativeIngressError> {
        self.read_frame_or_drain_if(reader, |_| true).await
    }

    /// Selective form of [`Self::read_frame_or_drain`]. The predicate is
    /// evaluated once, against the retained prefix, when the frame first
    /// crosses the byte budget. Rejected frames fail immediately instead of
    /// spending unbounded time draining an untrusted producer.
    pub async fn read_frame_or_drain_if<R, F>(
        &self,
        reader: &mut R,
        should_drain: F,
    ) -> Result<Option<BoundedNativeFrame>, NativeIngressError>
    where
        R: AsyncBufRead + Unpin,
        F: FnOnce(&[u8]) -> bool,
    {
        let mut prefix = Vec::with_capacity(self.budget.max_frame_bytes.min(8 * 1024));
        let mut total_bytes = 0usize;
        let mut oversized = false;
        let mut last_non_whitespace = None;
        let mut should_drain = Some(should_drain);
        loop {
            let available = reader.fill_buf().await.map_err(|error| {
                NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
            })?;
            if available.is_empty() {
                return if total_bytes == 0 {
                    Ok(None)
                } else if oversized {
                    Ok(Some(BoundedNativeFrame::Oversized {
                        prefix,
                        total_bytes,
                        last_non_whitespace,
                    }))
                } else {
                    Ok(Some(BoundedNativeFrame::Complete(prefix)))
                };
            }

            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            let chunk = &available[..consumed];
            total_bytes = total_bytes.saturating_add(consumed);
            if let Some(byte) = chunk
                .iter()
                .rev()
                .copied()
                .find(|byte| !byte.is_ascii_whitespace())
            {
                last_non_whitespace = Some(byte);
            }

            if prefix.len() < self.budget.max_frame_bytes {
                let retained = (self.budget.max_frame_bytes - prefix.len()).min(chunk.len());
                prefix.extend_from_slice(&chunk[..retained]);
            }
            if !oversized && total_bytes > self.budget.max_frame_bytes {
                let approved = should_drain
                    .take()
                    .expect("oversized frame drain predicate should be evaluated once")(
                    prefix.as_slice(),
                );
                if !approved {
                    return Err(NativeIngressError::new(
                        NativeIngressErrorKind::FrameTooLarge,
                        format!(
                            "native JSONL frame exceeds {} bytes",
                            self.budget.max_frame_bytes
                        ),
                    ));
                }
                oversized = true;
            }

            let ended = chunk.last() == Some(&b'\n');
            reader.consume(consumed);
            if ended {
                return if oversized {
                    Ok(Some(BoundedNativeFrame::Oversized {
                        prefix,
                        total_bytes,
                        last_non_whitespace,
                    }))
                } else {
                    Ok(Some(BoundedNativeFrame::Complete(prefix)))
                };
            }
        }
    }

    pub async fn read_json<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Option<JsonValue>, NativeIngressError> {
        let Some(frame) = self.read_frame(reader).await? else {
            return Ok(None);
        };
        let text = std::str::from_utf8(&frame).map_err(|_| {
            NativeIngressError::new(
                NativeIngressErrorKind::InvalidUtf8,
                "native frame is not UTF-8",
            )
        })?;
        let value = serde_json::from_str(text.trim_end_matches(['\r', '\n'])).map_err(|error| {
            NativeIngressError::new(
                NativeIngressErrorKind::InvalidJson,
                format!("native frame contains invalid JSON: {error}"),
            )
        })?;
        self.validate_value(&value)?;
        Ok(Some(value))
    }

    pub fn validate_value(&self, root: &JsonValue) -> Result<(), NativeIngressError> {
        let mut stack = vec![(root, 1usize)];
        let mut nodes = 0usize;
        while let Some((value, depth)) = stack.pop() {
            nodes = nodes.saturating_add(1);
            if nodes > self.budget.max_json_nodes {
                return Err(NativeIngressError::new(
                    NativeIngressErrorKind::JsonTooManyNodes,
                    "native JSON exceeds node budget",
                ));
            }
            if depth > self.budget.max_json_depth {
                return Err(NativeIngressError::new(
                    NativeIngressErrorKind::JsonTooDeep,
                    "native JSON exceeds depth budget",
                ));
            }
            match value {
                JsonValue::String(value) if value.len() > self.budget.max_string_bytes => {
                    return Err(NativeIngressError::new(
                        NativeIngressErrorKind::JsonStringTooLarge,
                        "native JSON string exceeds byte budget",
                    ));
                }
                JsonValue::Array(values) => {
                    stack.extend(values.iter().map(|value| (value, depth + 1)));
                }
                JsonValue::Object(values) => {
                    if values
                        .keys()
                        .any(|key| key.len() > self.budget.max_string_bytes)
                    {
                        return Err(NativeIngressError::new(
                            NativeIngressErrorKind::JsonStringTooLarge,
                            "native JSON object key exceeds byte budget",
                        ));
                    }
                    stack.extend(values.values().map(|value| (value, depth + 1)));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Default for BoundedNativeEventCodec {
    fn default() -> Self {
        Self::new(NativeEventBudget::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedNativeEventCodec, BoundedNativeFrame, NativeEventBudget};
    use tokio::io::BufReader;

    #[test]
    fn native_event_budget_intersection_never_widens_admitted_limits() {
        let admitted = NativeEventBudget::default();
        let current = NativeEventBudget {
            max_frame_bytes: 4_096,
            max_json_depth: 8,
            max_json_nodes: 128,
            max_string_bytes: 1_024,
            max_journal_payload_bytes: 2_048,
            ..NativeEventBudget::default()
        };
        let effective = admitted
            .intersect(current)
            .expect("compatible policies intersect");
        assert_eq!(effective, current);
        assert_eq!(current.intersect(admitted), Some(current));
    }

    #[test]
    fn native_event_budget_rejects_incompatible_profiles() {
        let admitted = NativeEventBudget::default();
        let current = NativeEventBudget {
            profile_version: admitted.profile_version + 1,
            ..admitted
        };
        assert!(admitted.intersect(current).is_none());
    }

    #[tokio::test]
    async fn oversized_frame_can_be_drained_without_losing_jsonl_alignment() {
        let budget = NativeEventBudget {
            max_frame_bytes: 16,
            max_json_depth: 8,
            max_json_nodes: 32,
            max_string_bytes: 8,
            max_journal_payload_bytes: 16,
            ..NativeEventBudget::default()
        };
        let codec = BoundedNativeEventCodec::new(budget);
        let first = b"{\"result\":\"this frame is oversized\"}\n";
        let mut input = Vec::from(first.as_slice());
        input.extend_from_slice(b"{}\n");
        let mut reader = BufReader::new(input.as_slice());

        let frame = codec
            .read_frame_or_drain(&mut reader)
            .await
            .expect("oversized frame should drain")
            .expect("first frame should exist");
        assert!(matches!(
            frame,
            BoundedNativeFrame::Oversized {
                total_bytes,
                last_non_whitespace: Some(b'}'),
                ..
            } if total_bytes == first.len()
        ));
        assert_eq!(
            codec
                .read_frame_or_drain(&mut reader)
                .await
                .expect("next frame should decode"),
            Some(BoundedNativeFrame::Complete(b"{}\n".to_vec()))
        );
    }
}
