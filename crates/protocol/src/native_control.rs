//! Ordered runtime control is distinct from the transport's native chat role.
//! Immutable Turn events own payloads; Agent checkpoints retain refs and cursors.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const NATIVE_CONTROL_RECORD_MAX_BYTES: usize = 64 * 1024;
pub const NATIVE_CONTROL_MAX_PENDING: usize = 64;
pub const NATIVE_CONTROL_STORAGE_MAX_BYTES: usize = NATIVE_CONTROL_RECORD_MAX_BYTES + 2 * 1024;

/// Known missing/incompatible immutable control data, distinct from a DB read failure.
#[derive(Debug)]
pub struct NativeControlSourceUnavailable;
impl std::fmt::Display for NativeControlSourceUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native control source is unavailable or incompatible")
    }
}
impl std::error::Error for NativeControlSourceUnavailable {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NativeControlOrigin {
    PioneerRuntime,
    TaskService,
    ToolService,
    ArtifactService,
    RuntimeDiagnostic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NativeControlTrust {
    Pioneer,
    Untrusted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NativeHostDirective {
    RetryTaskStateRead,
    ReviewTaskResults,
    ObserveTaskResults,
    AwaitAttachedTasks,
    RepairFinalAnswer,
    FinalizationRetry,
    RetryTools,
    ExhaustedTools,
    ChangeToolStrategy,
}
impl NativeHostDirective {
    /// The elevated channel never includes observation/display text.
    pub fn instruction(self) -> &'static str {
        match self {
            Self::RetryTaskStateRead => {
                "Authoritative attached Task state is unavailable. Retry the state read before finalization; do not infer completion or report success while it is unknown."
            }
            Self::ReviewTaskResults => {
                "Review the attached Task candidates using their advertised allowed actions. Do not finalize until each candidate is accepted, revised, cancelled, or no longer requires review."
            }
            Self::ObserveTaskResults => {
                "Account for the attached Task results and errors in the next response. Use task_get for authoritative full details when needed."
            }
            Self::AwaitAttachedTasks => {
                "Attached Tasks remain unfinished. Continue permitted work or use task_wait for their current waitable runs before finalization; do not claim they have completed."
            }
            Self::RepairFinalAnswer => {
                "The proposed final answer was rejected by the runtime contract. Resolve the reported requirements using the declared tools before submitting another final answer."
            }
            Self::FinalizationRetry => {
                "Finalization has remaining runtime requirements. Resolve the reported obligations using the declared tools before returning the final answer."
            }
            Self::RetryTools => {
                "Tool attempts have typed recoverable or partial failures. Inspect their untrusted observations, correct the failed operation and retry within the remaining budgets; do not report success without a confirmed result."
            }
            Self::ExhaustedTools => {
                "The retry budget for tool failures is exhausted. Do not retry those failures. Report the confirmed outcome and remaining limitations in the final answer."
            }
            Self::ChangeToolStrategy => {
                "The tool progress guard requires a changed approach. Inspect its untrusted observations and perform a permitted operation that advances the original task; do not repeat a no-progress operation."
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeHostControl {
    pub origin: NativeControlOrigin,
    pub trust: NativeControlTrust,
    pub directive: NativeHostDirective,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeControlObservation {
    pub origin: NativeControlOrigin,
    pub trust: NativeControlTrust,
    pub value: serde_json::Value,
}
impl std::fmt::Debug for NativeControlObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeControlObservation")
            .field("origin", &self.origin)
            .field("trust", &self.trust)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeControlRecord {
    pub schema_version: u32,
    pub id: String,
    pub sequence: u64,
    pub host: NativeHostControl,
    pub observation: Option<NativeControlObservation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeControlRef {
    pub item_id: String,
    pub sequence: u64,
    pub content_sha256: String,
}
fn valid_id(id: &str) -> bool {
    id.len() == 21 && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
}
impl NativeControlRef {
    pub fn valid(&self) -> bool {
        valid_id(&self.item_id)
            && self.sequence > 0
            && self.content_sha256.len() == 64
            && self
                .content_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}
/// Agent-owned application cursor and retained immutable source identities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeControlCheckpointState {
    pub schema_version: u32,
    pub next_sequence: u64,
    pub applied_sequence: u64,
    pub retained: Vec<NativeControlRef>,
}
impl Default for NativeControlCheckpointState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            next_sequence: 0,
            applied_sequence: 0,
            retained: Vec::new(),
        }
    }
}
impl NativeControlCheckpointState {
    pub fn valid(&self) -> bool {
        if self.schema_version != 1
            || self.next_sequence == u64::MAX
            || self.applied_sequence > self.next_sequence
            || self.retained.len() > NATIVE_CONTROL_MAX_PENDING
            || self.next_sequence - self.applied_sequence > NATIVE_CONTROL_MAX_PENDING as u64
        {
            return false;
        }
        let mut last = 0;
        let mut pending = self.applied_sequence;
        let mut ids = std::collections::BTreeSet::new();
        for reference in &self.retained {
            if !reference.valid()
                || reference.sequence <= last
                || reference.sequence > self.next_sequence
                || !ids.insert(&reference.item_id)
            {
                return false;
            }
            last = reference.sequence;
            if reference.sequence > self.applied_sequence {
                pending += 1;
                if reference.sequence != pending {
                    return false;
                }
            }
        }
        pending == self.next_sequence
    }
}
/// Bound real wire bytes before allocating a complete serialized buffer.
pub fn encode_native_control<T: Serialize>(value: &T) -> Result<Vec<u8>, &'static str> {
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > NATIVE_CONTROL_RECORD_MAX_BYTES)
            {
                return Err(std::io::Error::other("native control byte limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Bounded(Vec::new());
    serde_json::to_writer(&mut bytes, value).map_err(|_| "native control wire unavailable")?;
    Ok(bytes.0)
}
impl NativeControlRecord {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.sequence == 0
            || !valid_id(&self.id)
            || self.host.origin != NativeControlOrigin::PioneerRuntime
            || self.host.trust != NativeControlTrust::Pioneer
            || self.observation.as_ref().is_some_and(|observation| {
                observation.trust != NativeControlTrust::Untrusted
                    || observation.origin == NativeControlOrigin::PioneerRuntime
            })
        {
            return Err("native control origin, trust or identity incompatible");
        }
        if let Some(observation) = &self.observation {
            let mut pending = vec![(&observation.value, 0_usize)];
            let mut visited = 0_usize;
            while let Some((value, depth)) = pending.pop() {
                visited += 1;
                if visited > 4096 || depth > 32 {
                    return Err("native control observation structure limit");
                }
                let children = match value {
                    serde_json::Value::Object(map) => map.len(),
                    serde_json::Value::Array(values) => values.len(),
                    _ => 0,
                };
                if pending
                    .len()
                    .saturating_add(visited)
                    .saturating_add(children)
                    > 4096
                {
                    return Err("native control observation structure limit");
                }
                match value {
                    serde_json::Value::Object(map) => {
                        pending.extend(map.values().map(|value| (value, depth + 1)))
                    }
                    serde_json::Value::Array(values) => {
                        pending.extend(values.iter().map(|value| (value, depth + 1)))
                    }
                    _ => {}
                }
            }
        }
        encode_native_control(self)?;
        Ok(())
    }
    pub fn content_sha256(&self) -> Result<String, &'static str> {
        let bytes = encode_native_control(&(self.schema_version, &self.host, &self.observation))?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }
    pub fn reference(&self) -> Result<NativeControlRef, &'static str> {
        self.validate()?;
        Ok(NativeControlRef {
            item_id: self.id.clone(),
            sequence: self.sequence,
            content_sha256: self.content_sha256()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> NativeControlRecord {
        NativeControlRecord {
            schema_version: 1,
            id: crate::generate_id(21),
            sequence: 1,
            host: NativeHostControl {
                origin: NativeControlOrigin::PioneerRuntime,
                trust: NativeControlTrust::Pioneer,
                directive: NativeHostDirective::ReviewTaskResults,
            },
            observation: Some(NativeControlObservation {
                origin: NativeControlOrigin::TaskService,
                trust: NativeControlTrust::Untrusted,
                value: serde_json::json!({"candidate":"untrusted"}),
            }),
        }
    }
    #[test]
    fn control_wire_enforces_origin_schema_physical_bytes_and_pending_sequence_coverage() {
        let record = record();
        let reference = record.reference().unwrap();
        let state = NativeControlCheckpointState {
            schema_version: 1,
            next_sequence: 1,
            applied_sequence: 0,
            retained: vec![reference.clone()],
        };
        assert!(state.valid());
        let mut gap = state.clone();
        gap.next_sequence = 2;
        assert!(!gap.valid());
        let mut missing = state.clone();
        missing.retained.clear();
        assert!(!missing.valid());
        let mut duplicate = state.clone();
        duplicate.retained.push(reference);
        assert!(!duplicate.valid());
        let mut future = record.clone();
        future.schema_version = 2;
        assert!(future.reference().is_err());
        let mut promoted = record.clone();
        promoted.observation.as_mut().unwrap().trust = NativeControlTrust::Pioneer;
        assert!(promoted.validate().is_err());
        let mut host = record.clone();
        host.host.origin = NativeControlOrigin::ToolService;
        assert!(host.validate().is_err());
        let mut oversized = record.clone();
        oversized.observation.as_mut().unwrap().value = serde_json::json!("界".repeat(32 * 1024));
        assert!(oversized.validate().is_err());
        let mut deep = record;
        let mut value = serde_json::Value::Null;
        for _ in 0..34 {
            value = serde_json::json!([value]);
        }
        deep.observation.as_mut().unwrap().value = value;
        assert!(deep.validate().is_err());
    }
}
