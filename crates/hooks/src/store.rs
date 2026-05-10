use crate::{
    HookAgentId, HookAuditEventKind, HookContext, HookContributionHash, HookDiagnosticPreview,
    HookExecutionPolicy, HookFailurePolicy, HookId, HookInput, HookInputKind, HookPhase,
    HookPolicySet, HookPromptContextSet, HookRetryPolicy, HookRunAttemptId, HookRunErrorSummary,
    HookRunId, HookRunIdempotencyKey, HookRunScopeId, HookRunStatus, HookSubscriptionId,
    HookTaskId, HookThreadId, HookTurnId, HookValue, HookWorkspaceId,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

pub type HookRunStoreResult<T> = Result<T, HookRunStoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookRunStoreError {
    Unavailable { message: String },
    Conflict { message: String },
    InvalidRecord { message: String },
    Internal { message: String },
}

impl HookRunStoreError {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn invalid_record(message: impl Into<String>) -> Self {
        Self::InvalidRecord {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn safe_message(&self) -> &str {
        match self {
            Self::Unavailable { message }
            | Self::Conflict { message }
            | Self::InvalidRecord { message }
            | Self::Internal { message } => message.as_str(),
        }
    }
}

impl fmt::Display for HookRunStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { message } => {
                write!(formatter, "hook run store unavailable: {message}")
            }
            Self::Conflict { message } => write!(formatter, "hook run store conflict: {message}"),
            Self::InvalidRecord { message } => {
                write!(formatter, "invalid hook run store record: {message}")
            }
            Self::Internal { message } => {
                write!(formatter, "hook run store internal error: {message}")
            }
        }
    }
}

impl std::error::Error for HookRunStoreError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookRunScopeKind {
    Workspace,
    Thread,
    Turn,
    Task,
    Agent,
    Hook,
    Custom(String),
}

impl HookRunScopeKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Workspace => "workspace",
            Self::Thread => "thread",
            Self::Turn => "turn",
            Self::Task => "task",
            Self::Agent => "agent",
            Self::Hook => "hook",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<&str> for HookRunScopeKind {
    fn from(value: &str) -> Self {
        match value {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "hook" => Self::Hook,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookRunScopeKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "hook" => Self::Hook,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookRunScopeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRunScope {
    pub kind: HookRunScopeKind,
    pub id: HookRunScopeId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookRunStoreRecord {
    pub idempotency_key: HookRunIdempotencyKey,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub scope: Option<HookRunScope>,
    pub context: HookContext,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub queued_at_unix_ms: Option<i64>,
    pub started_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub deadline_at_unix_ms: Option<i64>,
    pub resume_state: Option<HookRunResumeState>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunStoreRecord {
    pub id: HookRunId,
    pub idempotency_key: HookRunIdempotencyKey,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub scope: Option<HookRunScope>,
    pub context: HookContext,
    pub attempt_count: u16,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub queued_at_unix_ms: Option<i64>,
    pub started_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub deadline_at_unix_ms: Option<i64>,
    pub resume_state: Option<HookRunResumeState>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunStoreCompletion {
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub completed_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookRunAttemptStoreRecord {
    pub hook_run_id: HookRunId,
    pub attempt_number: u16,
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub started_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunAttemptStoreRecord {
    pub id: HookRunAttemptId,
    pub hook_run_id: HookRunId,
    pub attempt_number: u16,
    pub status: HookRunStatus,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub started_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunAttemptStoreCompletion {
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub completed_at_unix_ms: i64,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookAuditEventStoreRecord {
    pub hook_run_id: HookRunId,
    pub hook_run_attempt_id: Option<HookRunAttemptId>,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub event_kind: HookAuditEventKind,
    pub contribution_hash: Option<HookContributionHash>,
    pub details: HookValue,
    pub safe_for_user: bool,
    pub created_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookAuditEventStoreRecord {
    pub id: String,
    pub hook_run_id: HookRunId,
    pub hook_run_attempt_id: Option<HookRunAttemptId>,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub event_kind: HookAuditEventKind,
    pub contribution_hash: Option<HookContributionHash>,
    pub details: HookValue,
    pub safe_for_user: bool,
    pub created_at_unix_ms: i64,
}

pub const HOOK_RUN_RESUME_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookRunResumeState {
    pub schema_version: u16,
    pub execution_policy: HookExecutionPolicy,
    pub failure_policy: HookFailurePolicy,
    pub retry_policy: HookRetryPolicy,
    pub handler_version: u32,
    pub input_contract_version: u32,
    pub output_contract_version: u32,
    pub payload: HookRunResumePayload,
}

impl HookRunResumeState {
    pub fn input_snapshot(
        execution_policy: HookExecutionPolicy,
        failure_policy: HookFailurePolicy,
        retry_policy: HookRetryPolicy,
        handler_version: u32,
        input_contract_version: u32,
        output_contract_version: u32,
        snapshot: HookRunInputSnapshot,
    ) -> Self {
        Self {
            schema_version: HOOK_RUN_RESUME_SCHEMA_VERSION,
            execution_policy,
            failure_policy,
            retry_policy,
            handler_version,
            input_contract_version,
            output_contract_version,
            payload: HookRunResumePayload::InputSnapshot(snapshot),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum HookRunResumePayload {
    Reference(HookRunResumeReference),
    InputSnapshot(HookRunInputSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRunResumeReference {
    pub phase: HookPhase,
    pub input_kind: HookInputKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<HookWorkspaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<HookThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<HookTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<HookTaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<HookAgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_hash: Option<String>,
    pub input_hash: HookContributionHash,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookRunInputSnapshot {
    pub phase: HookPhase,
    pub context: HookContext,
    pub input: HookInput,
    #[serde(default, skip_serializing_if = "HookPolicySet::is_empty")]
    pub policy_set: HookPolicySet,
    #[serde(default, skip_serializing_if = "HookPromptContextSet::is_empty")]
    pub prompt_context_set: HookPromptContextSet,
    pub snapshot_hash: HookContributionHash,
}

impl HookRunInputSnapshot {
    pub fn new(
        phase: HookPhase,
        context: HookContext,
        input: HookInput,
        policy_set: HookPolicySet,
        prompt_context_set: HookPromptContextSet,
    ) -> Self {
        let snapshot_hash =
            Self::hash_parts(phase, &context, &input, &policy_set, &prompt_context_set);
        Self {
            phase,
            context,
            input,
            policy_set,
            prompt_context_set,
            snapshot_hash,
        }
    }

    pub fn hash_parts(
        phase: HookPhase,
        context: &HookContext,
        input: &HookInput,
        policy_set: &HookPolicySet,
        prompt_context_set: &HookPromptContextSet,
    ) -> HookContributionHash {
        #[derive(Serialize)]
        struct SnapshotHashInput<'a> {
            phase: HookPhase,
            context: &'a HookContext,
            input: &'a HookInput,
            policy_set: &'a HookPolicySet,
            prompt_context_set: &'a HookPromptContextSet,
        }

        let bytes = serde_json::to_vec(&SnapshotHashInput {
            phase,
            context,
            input,
            policy_set,
            prompt_context_set,
        })
        .unwrap_or_default();
        let digest = Sha256::digest(bytes);
        HookContributionHash::new(format!("sha256:{}", hex::encode(digest)))
            .expect("sha256 hook input snapshot hash is valid")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRecoverableRunRecord {
    pub run: HookRunStoreRecord,
    pub resume_state: Option<HookRunResumeState>,
    pub attempts: Vec<HookRunAttemptStoreRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRecoveryScan {
    pub now_unix_ms: i64,
    pub batch_size: usize,
    pub stale_running_after_ms: u64,
    pub phases: Option<Vec<HookPhase>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRetrySchedule {
    pub queued_at_unix_ms: i64,
    pub deadline_at_unix_ms: Option<i64>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
}

#[async_trait]
pub trait HookRunStore: Send + Sync {
    async fn create_or_load_run(
        &self,
        run: NewHookRunStoreRecord,
    ) -> HookRunStoreResult<HookRunStoreRecord>;

    async fn mark_run_running(
        &self,
        run_id: &HookRunId,
        started_at_unix_ms: i64,
    ) -> HookRunStoreResult<HookRunStoreRecord>;

    async fn complete_run(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord>;

    async fn append_attempt(
        &self,
        attempt: NewHookRunAttemptStoreRecord,
    ) -> HookRunStoreResult<HookRunAttemptStoreRecord>;

    async fn complete_attempt(
        &self,
        attempt_id: &HookRunAttemptId,
        completion: HookRunAttemptStoreCompletion,
    ) -> HookRunStoreResult<HookRunAttemptStoreRecord>;

    async fn append_audit_events(
        &self,
        _events: Vec<NewHookAuditEventStoreRecord>,
    ) -> HookRunStoreResult<Vec<HookAuditEventStoreRecord>> {
        Ok(Vec::new())
    }

    async fn list_recoverable_runs(
        &self,
        _scan: HookRecoveryScan,
    ) -> HookRunStoreResult<Vec<HookRecoverableRunRecord>>;

    async fn schedule_run_retry(
        &self,
        run_id: &HookRunId,
        schedule: HookRetrySchedule,
    ) -> HookRunStoreResult<HookRunStoreRecord>;

    async fn mark_stale_run_timed_out(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord>;

    async fn mark_run_unrecoverable(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HookInputPayload, HookPolicySet, HookPromptContextSet};

    #[test]
    fn phase_21_resume_state_roundtrips_as_typed_snapshot() {
        let input = HookInput {
            kind: HookInputKind::TurnPostTurn,
            payload: HookInputPayload::Empty,
        };
        let snapshot = HookRunInputSnapshot::new(
            HookPhase::TurnPostTurn,
            HookContext::default(),
            input,
            HookPolicySet::empty(),
            HookPromptContextSet::empty(),
        );
        let state = HookRunResumeState::input_snapshot(
            HookExecutionPolicy::default(),
            HookFailurePolicy::BestEffort,
            HookRetryPolicy::default(),
            1,
            1,
            1,
            snapshot.clone(),
        );

        let encoded = serde_json::to_string(&state).expect("resume state serializes");
        let decoded: HookRunResumeState =
            serde_json::from_str(encoded.as_str()).expect("resume state deserializes");

        assert_eq!(decoded, state);
        let HookRunResumePayload::InputSnapshot(decoded_snapshot) = decoded.payload else {
            panic!("expected input snapshot payload");
        };
        assert_eq!(decoded_snapshot.snapshot_hash, snapshot.snapshot_hash);
        assert!(
            decoded_snapshot
                .snapshot_hash
                .as_str()
                .starts_with("sha256:")
        );
    }

    #[test]
    fn phase_21_input_snapshot_hash_is_stable() {
        let input = HookInput::empty(HookInputKind::TurnPostTurn);
        let first = HookRunInputSnapshot::new(
            HookPhase::TurnPostTurn,
            HookContext::default(),
            input.clone(),
            HookPolicySet::empty(),
            HookPromptContextSet::empty(),
        );
        let second = HookRunInputSnapshot::new(
            HookPhase::TurnPostTurn,
            HookContext::default(),
            input,
            HookPolicySet::empty(),
            HookPromptContextSet::empty(),
        );

        assert_eq!(first.snapshot_hash, second.snapshot_hash);
    }
}
