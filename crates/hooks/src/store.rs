use crate::{
    HookAuditEventKind, HookContext, HookContributionHash, HookDiagnosticPreview, HookId,
    HookPhase, HookRunAttemptId, HookRunErrorSummary, HookRunId, HookRunIdempotencyKey,
    HookRunScopeId, HookRunStatus, HookSubscriptionId, HookValue,
};
use async_trait::async_trait;
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
}
