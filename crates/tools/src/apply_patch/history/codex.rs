//! Codex app-server aggregate-diff authority for Apply Patch history.
//!
//! Codex owns filesystem execution in its CLI runtime.  Pioneer therefore
//! accepts only the provider's bounded `turn/diff/updated` aggregate and keeps
//! it in a separate projection.  This module never reads the workspace,
//! invokes the native PatchEngine, or manufactures AppliedPatchRecords.

use crate::apply_patch::history::{PatchHistoryCoverage, TurnDiffAuthority, TurnDiffExactness};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock};

pub const CODEX_AGGREGATE_SCHEMA_VERSION: u16 = 1;
pub const CODEX_TURN_DIFF_PROTOCOL_REVISION: &str = "codex-app-server:turn/diff/updated:v1";
pub const CODEX_MAX_AGGREGATE_BYTES: usize = 8 * 1024 * 1024;
pub const CODEX_MAX_EVENT_IDS: usize = 4096;
pub const CODEX_MAX_CONTEXT_FIELD_BYTES: usize = 4096;
pub const CODEX_MAX_EVENT_ID_BYTES: usize = 4096;
pub const CODEX_MAX_FILE_PATH_BYTES: usize = 4096;
pub const CODEX_MAX_FILES: usize = 4096;
pub const CODEX_MAX_STATE_JSON_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexProtocolSupport {
    Supported { revision: String, evidence: String },
    Unsupported { reason: String },
}

/// Selects the only protocol revision that has an audited Pioneer adapter.
/// A version string is evidence, not permission to guess a new event shape.
pub fn select_codex_protocol(version: &str) -> CodexProtocolSupport {
    let version = version.trim();
    if version.is_empty() {
        return CodexProtocolSupport::Unsupported {
            reason: "Codex version is missing".to_owned(),
        };
    }
    CodexProtocolSupport::Supported {
        revision: CODEX_TURN_DIFF_PROTOCOL_REVISION.to_owned(),
        evidence: format!("codex app-server version {version}"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodexAggregateEventContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub protocol_revision: String,
    pub event_id: Option<String>,
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodexAggregateEvent {
    pub schema_version: u16,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub protocol_revision: String,
    pub event_id: String,
    pub revision: u64,
    pub diff: String,
    pub exact: bool,
    pub files: Vec<CodexAggregateFile>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CodexAggregateFileKind {
    Add,
    Modify,
    Delete,
    Rename { destination: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodexAggregateFile {
    pub path: String,
    pub kind: CodexAggregateFileKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodexAggregateState {
    pub schema_version: u16,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub protocol_revision: String,
    pub authority: TurnDiffAuthority,
    pub exactness: TurnDiffExactness,
    /// Derived machine-friendly flag validated against canonical `exactness`.
    pub exact: bool,
    pub coverage: PatchHistoryCoverage,
    pub revision: u64,
    pub event_count: u64,
    pub final_state: bool,
    pub diff: String,
    pub files: Vec<CodexAggregateFile>,
    pub event_ids: Vec<String>,
    /// Digest of each accepted event payload, aligned with `event_ids`.
    /// The canonical v1 state always persists one digest per event so a
    /// duplicate event ID with different bytes fails closed after restart.
    pub event_payload_digests: Vec<[u8; 32]>,
}

impl CodexAggregateState {
    /// Build the durable aggregate projection for one provider event. Codex
    /// owns filesystem execution, so `exact` remains provider metadata and
    /// never becomes engine proof.
    pub fn from_event(event: &CodexAggregateEvent, revision: u64) -> Self {
        Self {
            schema_version: CODEX_AGGREGATE_SCHEMA_VERSION,
            workspace_id: event.workspace_id.clone(),
            thread_id: event.thread_id.clone(),
            turn_id: event.turn_id.clone(),
            protocol_revision: event.protocol_revision.clone(),
            authority: TurnDiffAuthority::CodexAggregateEvent,
            exactness: if event.exact {
                TurnDiffExactness::ProviderReported {
                    provider: "codex".to_owned(),
                    protocol: event.protocol_revision.clone(),
                }
            } else {
                TurnDiffExactness::Incomplete {
                    reason: "Codex reported an inexact aggregate".to_owned(),
                }
            },
            exact: event.exact,
            coverage: PatchHistoryCoverage::aggregate_only(
                "codex",
                event.protocol_revision.clone(),
            ),
            revision,
            event_count: 1,
            final_state: false,
            diff: event.diff.clone(),
            files: event.files.clone(),
            event_ids: vec![event.event_id.clone()],
            event_payload_digests: vec![event_payload_digest(event)],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodexAggregateIngest {
    Applied(CodexAggregateState),
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodexAggregateError {
    InvalidContext(String),
    InvalidPayload(String),
    UnsupportedProtocol(String),
    Oversized { actual: usize, maximum: usize },
    EventHistoryLimitExceeded { maximum: usize },
    ConflictingRevision(u64),
    ConflictingEvent(String),
    LateEventAfterFinalization,
    DifferentTurn,
    LockPoisoned,
}

impl fmt::Display for CodexAggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContext(message) => {
                write!(f, "invalid Codex aggregate context: {message}")
            }
            Self::InvalidPayload(message) => {
                write!(f, "invalid Codex aggregate payload: {message}")
            }
            Self::UnsupportedProtocol(protocol) => {
                write!(f, "unsupported Codex aggregate protocol `{protocol}`")
            }
            Self::Oversized { actual, maximum } => {
                write!(f, "Codex aggregate is {actual} bytes; maximum is {maximum}")
            }
            Self::EventHistoryLimitExceeded { maximum } => {
                write!(f, "Codex aggregate event history exceeds maximum {maximum}")
            }
            Self::ConflictingRevision(revision) => {
                write!(f, "conflicting Codex aggregate revision {revision}")
            }
            Self::ConflictingEvent(event_id) => {
                write!(
                    f,
                    "conflicting duplicate Codex aggregate event `{event_id}`"
                )
            }
            Self::LateEventAfterFinalization => {
                f.write_str("late Codex aggregate event cannot change finalized state")
            }
            Self::DifferentTurn => f.write_str("Codex event belongs to another turn"),
            Self::LockPoisoned => f.write_str("Codex aggregate lock is poisoned"),
        }
    }
}

impl std::error::Error for CodexAggregateError {}

/// Normalize exactly one supported Codex notification.  The caller supplies
/// identity and protocol facts from the trusted runtime; model payloads never
/// control those fields.
pub fn normalize_codex_diff_updated(
    context: CodexAggregateEventContext,
    params: &JsonValue,
) -> Result<CodexAggregateEvent, CodexAggregateError> {
    validate_context(&context)?;
    if context.protocol_revision != CODEX_TURN_DIFF_PROTOCOL_REVISION {
        return Err(CodexAggregateError::UnsupportedProtocol(
            context.protocol_revision,
        ));
    }
    let object = params.as_object().ok_or_else(|| {
        CodexAggregateError::InvalidPayload("params must be an object".to_owned())
    })?;
    if let Some(thread_id) = string_field(object, "threadId") {
        if thread_id != context.thread_id {
            return Err(CodexAggregateError::DifferentTurn);
        }
    }
    if let Some(turn_id) = string_field(object, "turnId") {
        if turn_id != context.turn_id {
            return Err(CodexAggregateError::DifferentTurn);
        }
    }
    let diff = object
        .get("diff")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| CodexAggregateError::InvalidPayload("diff must be a string".to_owned()))?;
    if diff.len() > CODEX_MAX_AGGREGATE_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: diff.len(),
            maximum: CODEX_MAX_AGGREGATE_BYTES,
        });
    }
    let event_id = context
        .event_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| stable_event_id(&context, diff));
    let revision = context.revision.unwrap_or(0);
    let event = CodexAggregateEvent {
        schema_version: CODEX_AGGREGATE_SCHEMA_VERSION,
        workspace_id: context.workspace_id,
        thread_id: context.thread_id,
        turn_id: context.turn_id,
        protocol_revision: CODEX_TURN_DIFF_PROTOCOL_REVISION.to_owned(),
        event_id,
        revision,
        diff: diff.to_owned(),
        // Codex reports an aggregate, not independently verifiable committed
        // bytes.  `exact` is therefore provider-reported, never engine proof.
        exact: object
            .get("exact")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        files: parse_changed_files(diff),
    };
    validate_event(&event)?;
    Ok(event)
}

fn validate_context(context: &CodexAggregateEventContext) -> Result<(), CodexAggregateError> {
    for (label, value) in [
        ("workspace_id", context.workspace_id.as_str()),
        ("thread_id", context.thread_id.as_str()),
        ("turn_id", context.turn_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(CodexAggregateError::InvalidContext(format!(
                "{label} is empty"
            )));
        }
        if value.len() > CODEX_MAX_CONTEXT_FIELD_BYTES {
            return Err(CodexAggregateError::Oversized {
                actual: value.len(),
                maximum: CODEX_MAX_CONTEXT_FIELD_BYTES,
            });
        }
    }
    if context.protocol_revision.len() > CODEX_MAX_CONTEXT_FIELD_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: context.protocol_revision.len(),
            maximum: CODEX_MAX_CONTEXT_FIELD_BYTES,
        });
    }
    if let Some(event_id) = &context.event_id
        && event_id.len() > CODEX_MAX_EVENT_ID_BYTES
    {
        return Err(CodexAggregateError::Oversized {
            actual: event_id.len(),
            maximum: CODEX_MAX_EVENT_ID_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn validate_event(event: &CodexAggregateEvent) -> Result<(), CodexAggregateError> {
    if event.schema_version != CODEX_AGGREGATE_SCHEMA_VERSION {
        return Err(CodexAggregateError::InvalidPayload(format!(
            "unsupported aggregate schema version {}",
            event.schema_version
        )));
    }
    if event.protocol_revision != CODEX_TURN_DIFF_PROTOCOL_REVISION {
        return Err(CodexAggregateError::UnsupportedProtocol(
            event.protocol_revision.clone(),
        ));
    }
    for (label, value) in [
        ("workspace_id", event.workspace_id.as_str()),
        ("thread_id", event.thread_id.as_str()),
        ("turn_id", event.turn_id.as_str()),
        ("event_id", event.event_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(CodexAggregateError::InvalidPayload(format!(
                "{label} is empty"
            )));
        }
        let maximum = if label == "event_id" {
            CODEX_MAX_EVENT_ID_BYTES
        } else {
            CODEX_MAX_CONTEXT_FIELD_BYTES
        };
        if value.len() > maximum {
            return Err(CodexAggregateError::Oversized {
                actual: value.len(),
                maximum,
            });
        }
    }
    if event.diff.len() > CODEX_MAX_AGGREGATE_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: event.diff.len(),
            maximum: CODEX_MAX_AGGREGATE_BYTES,
        });
    }
    validate_files(&event.files)?;
    let encoded = serde_json::to_vec(event)
        .map_err(|error| CodexAggregateError::InvalidPayload(error.to_string()))?;
    if encoded.len() > CODEX_MAX_STATE_JSON_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: encoded.len(),
            maximum: CODEX_MAX_STATE_JSON_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn validate_state(state: &CodexAggregateState) -> Result<(), CodexAggregateError> {
    if state.schema_version != CODEX_AGGREGATE_SCHEMA_VERSION {
        return Err(CodexAggregateError::InvalidPayload(format!(
            "unsupported aggregate schema version {}",
            state.schema_version
        )));
    }
    if state.protocol_revision != CODEX_TURN_DIFF_PROTOCOL_REVISION {
        return Err(CodexAggregateError::UnsupportedProtocol(
            state.protocol_revision.clone(),
        ));
    }
    if state.authority != TurnDiffAuthority::CodexAggregateEvent {
        return Err(CodexAggregateError::InvalidPayload(
            "Codex aggregate state has a non-Codex authority".to_owned(),
        ));
    }
    if state.coverage
        != PatchHistoryCoverage::aggregate_only("codex", state.protocol_revision.clone())
    {
        return Err(CodexAggregateError::InvalidPayload(
            "Codex aggregate state must use AggregateOnly coverage".to_owned(),
        ));
    }
    let expected_exactness = if state.exact {
        TurnDiffExactness::ProviderReported {
            provider: "codex".to_owned(),
            protocol: state.protocol_revision.clone(),
        }
    } else {
        TurnDiffExactness::Incomplete {
            reason: "Codex reported an inexact aggregate".to_owned(),
        }
    };
    if state.exactness != expected_exactness {
        return Err(CodexAggregateError::InvalidPayload(
            "Codex aggregate exactness provenance is inconsistent".to_owned(),
        ));
    }
    if state.revision > i64::MAX as u64 {
        return Err(CodexAggregateError::InvalidPayload(
            "aggregate revision exceeds SQLite integer range".to_owned(),
        ));
    }
    for (label, value) in [
        ("workspace_id", state.workspace_id.as_str()),
        ("thread_id", state.thread_id.as_str()),
        ("turn_id", state.turn_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(CodexAggregateError::InvalidPayload(format!(
                "{label} is empty"
            )));
        }
        if value.len() > CODEX_MAX_CONTEXT_FIELD_BYTES {
            return Err(CodexAggregateError::Oversized {
                actual: value.len(),
                maximum: CODEX_MAX_CONTEXT_FIELD_BYTES,
            });
        }
    }
    if state.diff.len() > CODEX_MAX_AGGREGATE_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: state.diff.len(),
            maximum: CODEX_MAX_AGGREGATE_BYTES,
        });
    }
    if state.event_ids.len() > CODEX_MAX_EVENT_IDS {
        return Err(CodexAggregateError::EventHistoryLimitExceeded {
            maximum: CODEX_MAX_EVENT_IDS,
        });
    }
    if state.event_payload_digests.len() != state.event_ids.len() {
        return Err(CodexAggregateError::InvalidPayload(
            "event payload digest history is not aligned with event IDs".to_owned(),
        ));
    }
    if state.event_count != state.event_ids.len() as u64 {
        return Err(CodexAggregateError::InvalidPayload(
            "aggregate event count disagrees with event ids".to_owned(),
        ));
    }
    let mut unique_event_ids = std::collections::HashSet::with_capacity(state.event_ids.len());
    for event_id in &state.event_ids {
        if event_id.trim().is_empty() {
            return Err(CodexAggregateError::InvalidPayload(
                "event_id is empty".to_owned(),
            ));
        }
        if event_id.len() > CODEX_MAX_EVENT_ID_BYTES {
            return Err(CodexAggregateError::Oversized {
                actual: event_id.len(),
                maximum: CODEX_MAX_EVENT_ID_BYTES,
            });
        }
        if !unique_event_ids.insert(event_id) {
            return Err(CodexAggregateError::InvalidPayload(
                "aggregate event ids contain a duplicate".to_owned(),
            ));
        }
    }
    validate_files(&state.files)?;
    let encoded = serde_json::to_vec(state)
        .map_err(|error| CodexAggregateError::InvalidPayload(error.to_string()))?;
    if encoded.len() > CODEX_MAX_STATE_JSON_BYTES {
        return Err(CodexAggregateError::Oversized {
            actual: encoded.len(),
            maximum: CODEX_MAX_STATE_JSON_BYTES,
        });
    }
    Ok(())
}

fn validate_files(files: &[CodexAggregateFile]) -> Result<(), CodexAggregateError> {
    if files.len() > CODEX_MAX_FILES {
        return Err(CodexAggregateError::Oversized {
            actual: files.len(),
            maximum: CODEX_MAX_FILES,
        });
    }
    for file in files {
        if file.path.trim().is_empty() || file.path.len() > CODEX_MAX_FILE_PATH_BYTES {
            return Err(CodexAggregateError::Oversized {
                actual: file.path.len(),
                maximum: CODEX_MAX_FILE_PATH_BYTES,
            });
        }
        if let CodexAggregateFileKind::Rename { destination } = &file.kind
            && (destination.trim().is_empty() || destination.len() > CODEX_MAX_FILE_PATH_BYTES)
        {
            return Err(CodexAggregateError::Oversized {
                actual: destination.len(),
                maximum: CODEX_MAX_FILE_PATH_BYTES,
            });
        }
    }
    Ok(())
}

fn string_field<'a>(object: &'a serde_json::Map<String, JsonValue>, name: &str) -> Option<&'a str> {
    object.get(name).and_then(JsonValue::as_str)
}

fn stable_event_id(context: &CodexAggregateEventContext, diff: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(context.workspace_id.as_bytes());
    hasher.update([0]);
    hasher.update(context.thread_id.as_bytes());
    hasher.update([0]);
    hasher.update(context.turn_id.as_bytes());
    hasher.update([0]);
    hasher.update(diff.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Extract only path-level changed-file facts from a unified aggregate.  File
/// content is retained verbatim in `diff`; this parser never claims a before /
/// after snapshot or creates a native applied record.
pub fn parse_changed_files(diff: &str) -> Vec<CodexAggregateFile> {
    let mut files = BTreeMap::<String, CodexAggregateFileKind>::new();
    let mut pending_old = None::<Option<String>>;
    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            let mut paths = rest.split_whitespace();
            let old = paths.next().and_then(|value| normalize_diff_path(value));
            let new = paths.next().and_then(|value| normalize_diff_path(value));
            if let (Some(old), Some(new)) = (old, new) {
                if old != new {
                    files.insert(
                        old.clone(),
                        CodexAggregateFileKind::Rename { destination: new },
                    );
                } else {
                    files.entry(old).or_insert(CodexAggregateFileKind::Modify);
                }
            }
            continue;
        }
        if let Some(path) = line.strip_prefix("--- ") {
            pending_old = Some(parse_diff_path(path));
            continue;
        }
        if let Some(path) = line.strip_prefix("+++ ") {
            let new = parse_diff_path(path);
            match (pending_old.take(), new) {
                (Some(Some(old)), Some(new)) if old == new => {
                    files.entry(old).or_insert(CodexAggregateFileKind::Modify);
                }
                (Some(Some(old)), Some(new)) => {
                    files.insert(old, CodexAggregateFileKind::Rename { destination: new });
                }
                (Some(None), Some(path)) => {
                    files.entry(path).or_insert(CodexAggregateFileKind::Add);
                }
                (Some(Some(path)), None) => {
                    files.entry(path).or_insert(CodexAggregateFileKind::Delete);
                }
                _ => {}
            }
        }
    }
    let mut result = files
        .into_iter()
        .map(|(path, kind)| CodexAggregateFile { path, kind })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.path.cmp(&right.path));
    result
}

fn parse_diff_path(raw: &str) -> Option<String> {
    let value = raw
        .trim()
        .trim_matches('"')
        .split('\t')
        .next()
        .unwrap_or_default();
    if value == "/dev/null" || value.is_empty() {
        return None;
    }
    Some(
        value
            .strip_prefix("a/")
            .or_else(|| value.strip_prefix("b/"))
            .unwrap_or(value)
            .to_owned(),
    )
}

fn normalize_diff_path(raw: &str) -> Option<String> {
    parse_diff_path(raw)
}

#[derive(Clone, Debug, Default)]
pub struct CodexAggregateTracker {
    events: BTreeMap<(u64, String), CodexAggregateEvent>,
    state: Option<CodexAggregateState>,
    finalized: bool,
}

impl CodexAggregateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest(
        &mut self,
        mut event: CodexAggregateEvent,
    ) -> Result<CodexAggregateIngest, CodexAggregateError> {
        validate_event(&event)?;
        if event.protocol_revision != CODEX_TURN_DIFF_PROTOCOL_REVISION {
            return Err(CodexAggregateError::UnsupportedProtocol(
                event.protocol_revision,
            ));
        }
        if let Some(state) = &self.state {
            if state.workspace_id != event.workspace_id
                || state.thread_id != event.thread_id
                || state.turn_id != event.turn_id
            {
                return Err(CodexAggregateError::DifferentTurn);
            }
        }
        if self.finalized {
            if self.events.values().any(|existing| {
                existing.event_id == event.event_id && same_event_payload(existing, &event)
            }) {
                return Ok(CodexAggregateIngest::Duplicate);
            }
            return Err(CodexAggregateError::LateEventAfterFinalization);
        }
        if let Some(existing) = self
            .events
            .values()
            .find(|item| item.event_id == event.event_id)
        {
            if same_event_payload(existing, &event) {
                return Ok(CodexAggregateIngest::Duplicate);
            }
            return Err(CodexAggregateError::ConflictingEvent(event.event_id));
        }
        if self.events.len() >= CODEX_MAX_EVENT_IDS {
            return Err(CodexAggregateError::EventHistoryLimitExceeded {
                maximum: CODEX_MAX_EVENT_IDS,
            });
        }
        if event.revision == 0 {
            event.revision = self
                .events
                .keys()
                .map(|(revision, _)| *revision)
                .max()
                .unwrap_or(0)
                .saturating_add(1);
        }
        if self
            .events
            .keys()
            .any(|(revision, _)| *revision == event.revision)
        {
            return Err(CodexAggregateError::ConflictingRevision(event.revision));
        }
        let key = (event.revision, event.event_id.clone());
        self.events.insert(key, event);
        let latest = self
            .events
            .values()
            .max_by_key(|item| item.revision)
            .expect("inserted event");
        let event_ids = self
            .events
            .values()
            .map(|item| item.event_id.clone())
            .collect::<Vec<_>>();
        let state = CodexAggregateState {
            schema_version: CODEX_AGGREGATE_SCHEMA_VERSION,
            workspace_id: latest.workspace_id.clone(),
            thread_id: latest.thread_id.clone(),
            turn_id: latest.turn_id.clone(),
            protocol_revision: latest.protocol_revision.clone(),
            authority: TurnDiffAuthority::CodexAggregateEvent,
            exactness: if latest.exact {
                TurnDiffExactness::ProviderReported {
                    provider: "codex".to_owned(),
                    protocol: latest.protocol_revision.clone(),
                }
            } else {
                TurnDiffExactness::Incomplete {
                    reason: "Codex reported an inexact aggregate".to_owned(),
                }
            },
            exact: latest.exact,
            coverage: PatchHistoryCoverage::aggregate_only(
                "codex",
                latest.protocol_revision.clone(),
            ),
            revision: latest.revision,
            event_count: self.events.len() as u64,
            final_state: false,
            diff: latest.diff.clone(),
            files: latest.files.clone(),
            event_ids,
            event_payload_digests: self
                .events
                .values()
                .map(event_payload_digest)
                .collect::<Vec<_>>(),
        };
        self.state = Some(state.clone());
        Ok(CodexAggregateIngest::Applied(state))
    }

    pub fn finalize(&mut self) -> Result<Option<CodexAggregateState>, CodexAggregateError> {
        self.finalized = true;
        if let Some(state) = &mut self.state {
            state.final_state = true;
            return Ok(Some(state.clone()));
        }
        Ok(None)
    }

    pub fn state(&self) -> Option<&CodexAggregateState> {
        self.state.as_ref()
    }

    pub fn replay<I>(events: I, finalize: bool) -> Result<Self, CodexAggregateError>
    where
        I: IntoIterator<Item = CodexAggregateEvent>,
    {
        let mut events = events.into_iter().collect::<Vec<_>>();
        events.sort_by(|left, right| {
            left.revision
                .cmp(&right.revision)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        let mut tracker = Self::new();
        for event in events {
            tracker.ingest(event)?;
        }
        if finalize {
            tracker.finalize()?;
        }
        Ok(tracker)
    }
}

fn same_event_payload(left: &CodexAggregateEvent, right: &CodexAggregateEvent) -> bool {
    left.schema_version == right.schema_version
        && left.workspace_id == right.workspace_id
        && left.thread_id == right.thread_id
        && left.turn_id == right.turn_id
        && left.protocol_revision == right.protocol_revision
        && left.event_id == right.event_id
        && (left.revision == right.revision || right.revision == 0)
        && left.diff == right.diff
        && left.exact == right.exact
        && left.files == right.files
}

/// Stable duplicate-detection digest for one event ID. Revision zero is an
/// explicitly supported "unspecified" value, so it is excluded from the
/// digest; the revision ordering rules are checked separately by the tracker.
pub(crate) fn event_payload_digest(event: &CodexAggregateEvent) -> [u8; 32] {
    let mut canonical = event.clone();
    canonical.revision = 0;
    let encoded = serde_json::to_vec(&canonical).expect("validated Codex event is serializable");
    Sha256::digest(encoded).into()
}

#[derive(Clone, Debug, Default)]
pub struct CodexAggregateProjectionStore {
    states: Arc<RwLock<HashMap<(String, String), CodexAggregateState>>>,
}

impl CodexAggregateProjectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, state: CodexAggregateState) -> Result<bool, CodexAggregateError> {
        validate_state(&state)?;
        let mut states = self
            .states
            .write()
            .map_err(|_| CodexAggregateError::LockPoisoned)?;
        let key = (state.thread_id.clone(), state.turn_id.clone());
        if let Some(existing) = states.get(&key) {
            if existing.final_state {
                if existing == &state {
                    return Ok(false);
                }
                return Err(CodexAggregateError::LateEventAfterFinalization);
            }
            if state.revision < existing.revision {
                return Ok(false);
            }
            if state.revision == existing.revision {
                if existing == &state {
                    return Ok(false);
                }
                return Err(CodexAggregateError::ConflictingRevision(state.revision));
            }
        }
        states.insert(key, state);
        Ok(true)
    }

    pub fn get(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<CodexAggregateState>, CodexAggregateError> {
        let states = self
            .states
            .read()
            .map_err(|_| CodexAggregateError::LockPoisoned)?;
        Ok(states
            .get(&(thread_id.to_owned(), turn_id.to_owned()))
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(revision: Option<u64>, event_id: Option<&str>) -> CodexAggregateEventContext {
        CodexAggregateEventContext {
            workspace_id: "workspace".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            protocol_revision: CODEX_TURN_DIFF_PROTOCOL_REVISION.to_owned(),
            event_id: event_id.map(str::to_owned),
            revision,
        }
    }

    fn event(revision: u64, id: &str, diff: &str) -> CodexAggregateEvent {
        normalize_codex_diff_updated(
            context(Some(revision), Some(id)),
            &serde_json::json!({"diff": diff}),
        )
        .unwrap()
    }

    #[test]
    fn protocol_selection_fails_closed_only_for_missing_evidence() {
        assert!(matches!(
            select_codex_protocol("0.144.1"),
            CodexProtocolSupport::Supported { .. }
        ));
        assert!(matches!(
            select_codex_protocol(""),
            CodexProtocolSupport::Unsupported { .. }
        ));
    }

    #[test]
    fn normalization_is_bounded_and_does_not_scan_workspace() {
        let normalized = normalize_codex_diff_updated(
            context(Some(1), Some("event-1")),
            &serde_json::json!({
                "threadId": "thread",
                "turnId": "turn",
                "diff": "diff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs"
            }),
        )
        .unwrap();
        assert_eq!(normalized.files[0].path, "src/lib.rs");
        assert_eq!(normalized.event_id, "event-1");
    }

    #[test]
    fn duplicate_and_out_of_order_events_converge_without_record_synthesis() {
        let first = event(1, "one", "diff --git a/a.txt b/a.txt");
        let second = event(2, "two", "diff --git a/b.txt b/b.txt");
        let mut tracker = CodexAggregateTracker::new();
        assert!(matches!(
            tracker.ingest(second.clone()).unwrap(),
            CodexAggregateIngest::Applied(_)
        ));
        assert!(matches!(
            tracker.ingest(first).unwrap(),
            CodexAggregateIngest::Applied(_)
        ));
        assert!(matches!(
            tracker.ingest(second).unwrap(),
            CodexAggregateIngest::Duplicate
        ));
        assert_eq!(tracker.state().unwrap().revision, 2);
        assert_eq!(tracker.state().unwrap().event_count, 2);
    }

    #[test]
    fn finalization_rejects_late_non_replay_events() {
        let mut tracker = CodexAggregateTracker::new();
        tracker
            .ingest(event(1, "one", "diff --git a/a b/a"))
            .unwrap();
        let final_state = tracker.finalize().unwrap().unwrap();
        assert!(final_state.final_state);
        assert_eq!(
            tracker.ingest(event(2, "two", "diff --git a/b b/b")),
            Err(CodexAggregateError::LateEventAfterFinalization)
        );
    }

    #[test]
    fn malformed_or_injected_identity_is_rejected() {
        assert!(matches!(
            normalize_codex_diff_updated(
                context(Some(1), None),
                &serde_json::json!({"threadId": "other", "diff": "x"})
            ),
            Err(CodexAggregateError::DifferentTurn)
        ));
        assert!(
            normalize_codex_diff_updated(context(Some(1), None), &serde_json::json!({"diff": 42}))
                .is_err()
        );
    }

    #[test]
    fn aggregate_metadata_and_file_shape_are_bounded() {
        let oversized_id = "x".repeat(CODEX_MAX_EVENT_ID_BYTES + 1);
        assert!(matches!(
            normalize_codex_diff_updated(
                context(Some(1), Some(&oversized_id)),
                &serde_json::json!({"diff": ""})
            ),
            Err(CodexAggregateError::Oversized { .. })
        ));

        let mut oversized_files = event(1, "one", "");
        oversized_files.files = (0..=CODEX_MAX_FILES)
            .map(|index| CodexAggregateFile {
                path: format!("file-{index}"),
                kind: CodexAggregateFileKind::Modify,
            })
            .collect();
        assert!(matches!(
            validate_event(&oversized_files),
            Err(CodexAggregateError::Oversized { .. })
        ));
    }
}
