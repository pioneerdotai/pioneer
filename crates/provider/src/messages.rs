//! Provider-neutral message lifecycle and bounded result projection.
use crate::{ChatResponse, ProviderResponseLimits, StreamChunk};
use anyhow::{Result, ensure};
use pioneer_protocol::AgentMessagePhase;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderMessageId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMessage {
    pub id: ProviderMessageId,
    pub phase: Option<AgentMessagePhase>,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMessageBoundary {
    Start,
    Delta,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageMetadata {
    pub id: ProviderMessageId,
    pub phase: Option<AgentMessagePhase>,
    pub boundary: ProviderMessageBoundary,
}

fn matches_result(messages: &[ProviderMessage], text: &str) -> bool {
    let mut remaining = text;
    let mut first = true;
    for message in messages
        .iter()
        .filter(|m| m.phase != Some(AgentMessagePhase::Commentary))
    {
        if !first {
            let Some(rest) = remaining.strip_prefix("\n\n") else {
                return false;
            };
            remaining = rest;
        }
        first = false;
        let Some(rest) = remaining.strip_prefix(message.text.as_str()) else {
            return false;
        };
        remaining = rest;
    }
    remaining.is_empty()
}

/// One collector per request. Boundaries never imply request completion.
#[derive(Debug, Default)]
pub struct ProviderMessageCollector {
    messages: Vec<ProviderMessage>,
    ended: Vec<bool>,
    result: String,
    result_cursor: usize,
    result_offsets: Vec<usize>,
    result_started: Vec<bool>,
    last_append: usize,
    legacy_seen: bool,
    bytes: usize,
}
impl ProviderMessageCollector {
    pub const MAX_MESSAGES: usize = 128;

    pub fn push(&mut self, chunk: &StreamChunk) -> Result<()> {
        self.last_append = self.result.len();
        let limits = ProviderResponseLimits::default();
        limits.validate_text_bytes(self.bytes.saturating_add(chunk.delta.len()), "message_text")?;
        if let Some(meta) = &chunk.message {
            ensure!(!self.legacy_seen, "mixed legacy and typed provider text");
            let index = self.messages.iter().position(|m| m.id == meta.id);
            match meta.boundary {
                ProviderMessageBoundary::Start => {
                    ensure!(
                        chunk.delta.is_empty() && index.is_none(),
                        "invalid provider message start"
                    );
                    ensure!(
                        self.messages.len() < Self::MAX_MESSAGES,
                        "provider message count limit"
                    );
                    self.messages.push(ProviderMessage {
                        id: meta.id,
                        phase: meta.phase.clone(),
                        text: String::new(),
                    });
                    self.ended.push(false);
                    self.result_offsets.push(0);
                    self.result_started.push(false);
                }
                boundary => {
                    let index =
                        index.ok_or_else(|| anyhow::anyhow!("provider message before start"))?;
                    ensure!(
                        !self.ended[index] && self.messages[index].phase == meta.phase,
                        "provider message lifecycle or phase conflict"
                    );
                    if boundary == ProviderMessageBoundary::End {
                        ensure!(chunk.delta.is_empty(), "provider message end carries text");
                        self.ended[index] = true;
                    } else {
                        self.messages[index].text.push_str(&chunk.delta);
                    }
                }
            }
        } else if !chunk.delta.is_empty() {
            ensure!(
                self.messages.is_empty(),
                "mixed typed and legacy provider text"
            );
            self.legacy_seen = true;
            self.result.push_str(&chunk.delta);
        }
        self.bytes = self.bytes.saturating_add(chunk.delta.len());
        while self.result_cursor < self.messages.len() {
            let index = self.result_cursor;
            let message = &self.messages[index];
            if message.phase == Some(AgentMessagePhase::Commentary) {
                self.result_cursor += 1;
                continue;
            }
            if !self.result_started[index] {
                if self.result_started.iter().any(|started| *started) {
                    self.result.push_str("\n\n");
                }
                self.result_started[index] = true;
            }
            self.result
                .push_str(&message.text[self.result_offsets[index]..]);
            self.result_offsets[index] = message.text.len();
            if !self.ended[index] {
                break;
            }
            self.result_cursor += 1;
        }
        limits.validate_text(&self.result, "result_text")?;
        limits.validate_retained_bytes(self.retained_bytes())?;
        Ok(())
    }
    pub fn finish(&self) -> Result<()> {
        ensure!(self.ended.iter().all(|v| *v), "unfinished provider message");
        ensure!(
            self.messages.is_empty() || matches_result(&self.messages, &self.result),
            "provider result projection conflict"
        );
        Ok(())
    }
    pub fn text(&self) -> String {
        self.result.clone()
    }
    pub fn text_ref(&self) -> &str {
        &self.result
    }
    /// Newly available result prefix, including separators. Later messages can
    /// be buffered while an earlier result message is still open.
    pub fn result_delta(&self) -> &str {
        &self.result[self.last_append..]
    }
    pub fn into_messages(self) -> Vec<ProviderMessage> {
        self.messages
    }
    pub fn messages(&self) -> &[ProviderMessage] {
        &self.messages
    }
    pub fn retained_bytes(&self) -> usize {
        self.bytes
            .saturating_add(if self.messages.is_empty() {
                0
            } else {
                self.result.len()
            })
            .saturating_add(self.messages.len().saturating_mul(
                std::mem::size_of::<ProviderMessage>() + 2 + std::mem::size_of::<usize>(),
            ))
    }
    pub fn validate_response(response: &ChatResponse) -> Result<()> {
        ProviderResponseLimits::default().validate_chat_response(response)?;
        if response.messages.is_empty() {
            return Ok(());
        }
        ensure!(
            response.messages.len() <= Self::MAX_MESSAGES,
            "provider message count limit"
        );
        let mut ids = std::collections::HashSet::new();
        ensure!(
            response.messages.iter().all(|m| ids.insert(m.id)),
            "duplicate provider message id"
        );
        ensure!(
            matches_result(&response.messages, &response.text),
            "provider result projection conflict"
        );
        Ok(())
    }
}

/// An explicit relationship, authored by the UI producer before immutable ACK.
/// A digest verifies the named completion; it never discovers ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRoundUiCopy {
    pub turn_item_id: String,
    pub kind: ProviderRoundUiCopyKind,
    pub content_sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderRoundUiCopyKind {
    AgentMessage { phase: AgentMessagePhase },
    Reasoning,
}
impl ProviderRoundUiCopy {
    pub fn from_item(item: &pioneer_protocol::TurnItem) -> Option<Self> {
        use pioneer_protocol::TurnItem;
        let (kind, semantic) = match item {
            TurnItem::AgentMessage { phase, text, .. } => (
                ProviderRoundUiCopyKind::AgentMessage {
                    phase: phase.clone(),
                },
                serde_json::json!([1, "agent_message", phase, text]),
            ),
            TurnItem::Reasoning {
                summary, content, ..
            } => (
                ProviderRoundUiCopyKind::Reasoning,
                serde_json::json!([1, "reasoning", summary, content]),
            ),
            _ => return None,
        };
        use sha2::Digest;
        Some(Self {
            turn_item_id: item.item_id().to_owned(),
            kind,
            content_sha256: hex::encode(sha2::Sha256::digest(serde_json::to_vec(&semantic).ok()?)),
        })
    }
    pub fn matches(&self, item: &pioneer_protocol::TurnItem) -> bool {
        Self::from_item(item).as_ref() == Some(self)
    }
}

pub(crate) fn message_bytes(messages: &[ProviderMessage]) -> usize {
    messages.iter().fold(0usize, |n, m| {
        n.saturating_add(m.text.len())
            .saturating_add(std::mem::size_of::<ProviderMessage>())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn part(
        id: u32,
        phase: Option<AgentMessagePhase>,
        boundary: ProviderMessageBoundary,
        text: &str,
    ) -> StreamChunk {
        let mut chunk = StreamChunk::delta(text);
        chunk.message = Some(ProviderMessageMetadata {
            id: ProviderMessageId(id),
            phase,
            boundary,
        });
        chunk
    }
    #[test]
    fn typed_projection_and_lifecycle() {
        use ProviderMessageBoundary::*;
        let mut c = ProviderMessageCollector::default();
        for (id, phase, text) in [
            (0, Some(AgentMessagePhase::Commentary), "working"),
            (1, None, "one"),
            (2, Some(AgentMessagePhase::FinalAnswer), "two"),
        ] {
            c.push(&part(id, phase.clone(), Start, "")).unwrap();
            c.push(&part(id, phase.clone(), Delta, text)).unwrap();
            c.push(&part(id, phase, End, "")).unwrap();
        }
        c.finish().unwrap();
        assert_eq!(c.text(), "one\n\ntwo");
        assert!(c.push(&StreamChunk::delta("legacy")).is_err());
        assert!(c.push(&part(0, None, Start, "")).is_err());
        assert!(
            c.push(&part(
                2,
                Some(AgentMessagePhase::FinalAnswer),
                Delta,
                "late"
            ))
            .is_err()
        );
    }
    #[test]
    fn missing_end_phase_change_and_legacy_mix_are_errors() {
        use ProviderMessageBoundary::*;
        let mut c = ProviderMessageCollector::default();
        assert!(c.push(&part(0, None, Delta, "x")).is_err());
        c.push(&part(0, None, Start, "")).unwrap();
        assert!(c.finish().is_err());
        assert!(
            c.push(&part(0, Some(AgentMessagePhase::Commentary), Delta, "x"))
                .is_err()
        );
        let mut c = ProviderMessageCollector::default();
        c.push(&StreamChunk::delta("a")).unwrap();
        assert!(c.push(&part(0, None, Start, "")).is_err());
        assert_eq!(c.text(), "a");
    }
    #[test]
    fn copy_digest_verifies_identity_phase_and_semantics() {
        let mut item = pioneer_protocol::TurnItem::AgentMessage {
            id: "named".into(),
            text: "same".into(),
            phase: AgentMessagePhase::Commentary,
            markdown: None,
            markdown_version: None,
        };
        let copy = ProviderRoundUiCopy::from_item(&item).unwrap();
        assert!(copy.matches(&item));
        if let pioneer_protocol::TurnItem::AgentMessage { text, .. } = &mut item {
            text.push('!');
        }
        assert!(!copy.matches(&item));
    }
}

#[cfg(test)]
mod limit_and_compatibility_tests {
    use super::*;
    #[test]
    fn empty_message_records_and_commentary_bytes_are_bounded() {
        let mut collector = ProviderMessageCollector::default();
        for id in 0..ProviderMessageCollector::MAX_MESSAGES {
            let mut chunk = StreamChunk::delta("");
            chunk.message = Some(ProviderMessageMetadata {
                id: ProviderMessageId(id as u32),
                phase: None,
                boundary: ProviderMessageBoundary::Start,
            });
            collector.push(&chunk).unwrap();
        }
        let mut chunk = StreamChunk::delta("");
        chunk.message = Some(ProviderMessageMetadata {
            id: ProviderMessageId(999),
            phase: None,
            boundary: ProviderMessageBoundary::Start,
        });
        assert!(collector.push(&chunk).is_err());
        let mut collector = ProviderMessageCollector::default();
        chunk.message = Some(ProviderMessageMetadata {
            id: ProviderMessageId(0),
            phase: Some(AgentMessagePhase::Commentary),
            boundary: ProviderMessageBoundary::Start,
        });
        collector.push(&chunk).unwrap();
        chunk.message.as_mut().unwrap().boundary = ProviderMessageBoundary::Delta;
        chunk.delta = "x".repeat(ProviderResponseLimits::default().max_text_bytes + 1);
        assert!(collector.push(&chunk).is_err());
    }
    #[test]
    fn old_envelope_omits_copies_and_explicit_empty_is_distinct() {
        let raw = serde_json::json!({"version":1,"round_id":"request","termination":{"kind":"complete"},"message":{"role":"assistant","content":"answer"},"calls":[]});
        let mut envelope: crate::CanonicalProviderRoundEnvelope =
            serde_json::from_value(raw.clone()).unwrap();
        assert!(envelope.ui_copies.is_none());
        assert_eq!(serde_json::to_value(&envelope).unwrap(), raw);
        envelope.ui_copies = Some(vec![]);
        let value = serde_json::to_value(&envelope).unwrap();
        assert_eq!(value["ui_copies"], serde_json::json!([]));
    }
}

#[cfg(test)]
mod interleaved_projection_tests {
    use super::*;
    #[test]
    fn later_result_is_buffered_until_earlier_result_end_without_rebuilding_prefix() {
        let mut collector = ProviderMessageCollector::default();
        let mut projected = String::new();
        for (id, boundary, text) in [
            (0, ProviderMessageBoundary::Start, ""),
            (1, ProviderMessageBoundary::Start, ""),
            (1, ProviderMessageBoundary::Delta, "later"),
            (0, ProviderMessageBoundary::Delta, "first"),
            (0, ProviderMessageBoundary::End, ""),
            (1, ProviderMessageBoundary::End, ""),
        ] {
            let mut chunk = StreamChunk::delta(text);
            chunk.message = Some(ProviderMessageMetadata {
                id: ProviderMessageId(id),
                phase: None,
                boundary,
            });
            collector.push(&chunk).unwrap();
            projected.push_str(collector.result_delta());
        }
        collector.finish().unwrap();
        assert_eq!(projected, "first\n\nlater");
        assert_eq!(collector.text_ref(), projected);
    }
}
