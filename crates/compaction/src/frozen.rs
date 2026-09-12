//! A frozen context owns ordered references, never a second transcript. A wire
//! digest verifies deterministic rematerialization; it is not a coverage guess.
use crate::SourceRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenMessageRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_turn_id: Option<String>,
    pub source_thread: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_thread: Option<String>,
    pub unit_id: String,
    pub sources: Vec<SourceRef>,
    pub inherited: bool,
    pub complete: bool,
    pub protected_input: bool,
    pub wire_sha256: String,
    /// Exact bounded provider representation when the full original lives in a
    /// terminal tool item. The latter remains the actual coverage source.
    pub replay_source: Option<SourceRef>,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
}
impl FrozenMessageRef {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.logical_turn_id
                .as_ref()
                .is_none_or(|id| !id.is_empty()),
            "frozen logical turn identity is empty"
        );
        anyhow::ensure!(
            !self.source_thread.is_empty() && !self.unit_id.is_empty() && !self.sources.is_empty(),
            "frozen message identity is missing"
        );
        anyhow::ensure!(
            self.wire_sha256.len() == 64 && self.wire_sha256.bytes().all(|c| c.is_ascii_hexdigit()),
            "frozen wire digest is invalid"
        );
        anyhow::ensure!(
            self.context_thread
                .as_ref()
                .is_none_or(|owner| !owner.is_empty()),
            "frozen context owner is missing"
        );
        for source in self.sources.iter().chain(self.replay_source.iter()) {
            anyhow::ensure!(
                !source.id.is_empty()
                    && !source.version.is_empty()
                    && source
                        .scope
                        .split_once(':')
                        .is_some_and(|(kind, owner)| !owner.is_empty()
                            && matches!(
                                kind,
                                "input"
                                    | "event"
                                    | "context"
                                    | "item"
                                    | "checkpoint"
                                    | "task-basis"
                            )),
                "frozen source is not canonical"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenHistoryRef {
    pub format: u32,
    pub manifest_id: String,
    pub messages: u64,
    pub identity_sha256: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn logical_alias_extension_preserves_existing_frozen_digest_bytes() {
        let previous = format!(
            r#"{{"source_thread":"thread","unit_id":"unit","sources":[{{"scope":"event:turn","id":"event","version":"event-revision:1"}}],"inherited":false,"complete":true,"protected_input":false,"wire_sha256":"{}","replay_source":null,"tool_call_id":null,"tool_name":null}}"#,
            "a".repeat(64)
        );
        let mut value: FrozenMessageRef = serde_json::from_str(&previous).unwrap();
        assert!(value.logical_turn_id.is_none());
        value.validate().unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), previous);
        value.logical_turn_id = Some("command-turn".into());
        let restored: FrozenMessageRef =
            serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
        assert_eq!(restored, value);
        value.logical_turn_id = Some(String::new());
        assert!(value.validate().is_err());
    }
}
