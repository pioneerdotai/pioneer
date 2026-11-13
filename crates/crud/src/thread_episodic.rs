use anyhow::{Result, bail};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::mem::size_of;

pub const THREAD_EPISODIC_SOURCE_VERSION_SUPERSEDED_ERROR: &str =
    "thread episodic source version superseded during reconciliation";
pub const THREAD_EPISODIC_USER_DELETED_ERROR: &str = "thread episodic source deleted by user";
pub const THREAD_EPISODIC_USER_EXCLUDED_ERROR: &str = "thread episodic source excluded by user";
pub const THREAD_EPISODIC_LEGACY_SOURCE_HASH_MISMATCH_ERROR: &str =
    "thread episodic source text hash changed before indexing";

// A request was resolved for a projection the workspace cannot accept. Keep
// that transition obligation on its existing job even at the old attempt limit.
pub const THREAD_EPISODIC_PROJECTION_CHANGED_ERROR: &str =
    "thread episodic request requires projection replacement";

#[derive(Debug, Default)]
pub struct ThreadEpisodicIndexClaimBatch {
    pub claimed: Vec<ThreadEpisodicIndexJobRecord>,
    // Input snapshots, not dispatch receipts. Only writer-side observation may
    // settle a possibly committed attempt; the static class excludes panic data.
    pub failures: Vec<(ThreadEpisodicIndexJobRecord, &'static str)>,
}

#[cfg(any(test, feature = "test-support"))]
impl ThreadEpisodicIndexClaimBatch {
    pub fn assert_no_failures_for_test(self) -> Vec<ThreadEpisodicIndexJobRecord> {
        assert!(
            self.failures.is_empty(),
            "unexpected episodic claim failure"
        );
        self.claimed
    }
}

/// Progress for the existing workspace projection-reset checkpoint. The files
/// are removed before rebudgeting jobs; the source cursor commits with each
/// replacement batch, so a restart never grants that source another budget.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadEpisodicProjectionResetProgress {
    pub reset_started: bool,
    pub after_source: Option<[String; 4]>,
}

pub fn thread_episodic_projection_reset_key(workspace_id: &str) -> Result<String> {
    Ok(format!(
        "thread_episodic_projection_reset:{}",
        thread_episodic_key_hash("workspace", workspace_id)?
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicSourceActorRole {
    User,
    Assistant,
    Task,
    SystemVisible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicSourceRuntimeKind {
    UserTurn,
    AssistantTurn,
    TaskResult,
    CompactionSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicItemVisibility {
    UserVisible,
    ParentVisible,
    InternalHidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicItemStatus {
    PendingIndex,
    Active,
    Excluded,
    /// Automatically retired because the canonical source moved to another version.
    /// Unlike `Deleted`, this state may be restored by source reconciliation.
    Superseded,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicIndexJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicExclusionReason {
    UserRequested,
    Deletion,
    Privacy,
    Policy,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicThreadDirectoryVisibility {
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicThreadDirectoryStatus {
    Active,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ThreadEpisodicRefillSourceCounts {
    pub source_thread_count: i64,
    pub source_turn_count: i64,
    pub source_turn_item_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicRefillThread {
    pub workspace_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicItemRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_runtime_kind: ThreadEpisodicSourceRuntimeKind,
    pub source_context: pioneer_protocol::ThreadEpisodicSourceContext,
    pub visibility: ThreadEpisodicItemVisibility,
    pub status: ThreadEpisodicItemStatus,
    pub text_hash: String,
    pub source_text_hash: String,
    pub projection_group_id: String,
    pub embedding_artifact_id: Option<String>,
    pub language_hint: Option<String>,
    pub token_estimate: i64,
    pub indexed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub deleted_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicItemRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_runtime_kind: ThreadEpisodicSourceRuntimeKind,
    pub source_context: pioneer_protocol::ThreadEpisodicSourceContext,
    pub visibility: ThreadEpisodicItemVisibility,
    pub status: ThreadEpisodicItemStatus,
    pub text_hash: String,
    pub source_text_hash: String,
    pub projection_group_id: String,
    pub language_hint: Option<String>,
    pub token_estimate: i64,
    pub indexed_at: Option<DateTimeWithTimeZone>,
    pub deleted_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicCanonicalItem {
    pub committed: bool,
    pub item: pioneer_protocol::TurnItem,
    pub source_payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicSourceReconcileOutcome {
    Current,
    MoreWork,
    PreservedDeletion,
    PreservedExclusion,
    SourceChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicIndexAttemptOutcome {
    Applied,
    StaleAttempt,
    ProjectionChanged,
    SourceChanged,
    Excluded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub index_item_id: String,
    pub status: ThreadEpisodicIndexJobStatus,
    pub attempt_count: i64,
    pub last_attempt_latency_ms: Option<i64>,
    pub next_run_at: DateTimeWithTimeZone,
    pub last_error: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub completed_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicIndexJobRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub index_item_id: String,
    pub status: ThreadEpisodicIndexJobStatus,
    pub next_run_at: DateTimeWithTimeZone,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobCompletionUpdate {
    pub last_attempt_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobFailureUpdate {
    pub retryable: bool,
    pub next_run_at_unix: Option<i64>,
    pub last_error: Option<String>,
    pub last_attempt_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicItemIndexedUpdate {
    pub projection_guard: Option<ThreadEpisodicPublicationGuard>,
    pub search_text: String,
    pub embedding_artifact_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicEmbeddingArtifactRecord {
    pub id: String,
    pub workspace_id: String,
    pub pipeline_identity_hash: String,
    pub input_hash: String,
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
    pub created_at: DateTimeWithTimeZone,
    pub last_used_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewThreadEpisodicEmbeddingArtifactRecord {
    pub id: String,
    pub workspace_id: String,
    pub pipeline_identity_hash: String,
    pub input_hash: String,
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicExclusionRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub index_item_id: String,
    pub reason: ThreadEpisodicExclusionReason,
    pub created_by: String,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicExclusionRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub index_item_id: String,
    pub reason: ThreadEpisodicExclusionReason,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicRecallEventRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub query_hash: Option<String>,
    pub search_profile_json: Option<String>,
    pub search_mode: Option<String>,
    pub adaptive_strategy: Option<String>,
    pub cutoff_json: Option<String>,
    pub candidate_count: i64,
    pub returned_count: i64,
    pub latency_ms: i64,
    pub fallback_used: bool,
    pub error: Option<String>,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicRecallEventRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub query_hash: Option<String>,
    pub search_profile_json: Option<String>,
    pub search_mode: Option<String>,
    pub adaptive_strategy: Option<String>,
    pub cutoff_json: Option<String>,
    pub candidate_count: i64,
    pub returned_count: i64,
    pub latency_ms: i64,
    pub fallback_used: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicThreadDirectoryRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub title: Option<String>,
    pub summary_hash: Option<String>,
    pub summary_ref: Option<String>,
    pub thread_created_at: Option<DateTimeWithTimeZone>,
    pub thread_updated_at: Option<DateTimeWithTimeZone>,
    pub last_indexed_at: Option<DateTimeWithTimeZone>,
    pub indexed_item_count: i64,
    pub task_affinity_json: Option<String>,
    pub project_affinity_json: Option<String>,
    pub visibility: ThreadEpisodicThreadDirectoryVisibility,
    pub status: ThreadEpisodicThreadDirectoryStatus,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicThreadDirectoryRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub title: Option<String>,
    pub summary_hash: Option<String>,
    pub summary_ref: Option<String>,
    pub thread_created_at: Option<DateTimeWithTimeZone>,
    pub thread_updated_at: Option<DateTimeWithTimeZone>,
    pub last_indexed_at: Option<DateTimeWithTimeZone>,
    pub indexed_item_count: i64,
    pub task_affinity_json: Option<String>,
    pub project_affinity_json: Option<String>,
    pub visibility: ThreadEpisodicThreadDirectoryVisibility,
    pub status: ThreadEpisodicThreadDirectoryStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicThreadDirectorySelection {
    pub workspace_id: String,
    pub query_text: Option<String>,
    pub task_affinity_json: Option<String>,
    pub project_affinity_json: Option<String>,
    pub exclude_thread_ids: Vec<String>,
    pub limit: u64,
}

pub fn thread_episodic_key_hash(kind: &str, key: &str) -> Result<String> {
    let kind = kind.trim();
    let key = key.trim();
    if kind.is_empty() {
        bail!("thread episodic hash kind cannot be empty");
    }
    if key.is_empty() {
        bail!("thread episodic hash key cannot be empty");
    }
    let mut hasher = Sha256::new();
    hasher.update(b"thread_episodic_key\0");
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn thread_episodic_item_record_from_model(
    model: pioneer_entity::thread_episodic_items::Model,
) -> Result<ThreadEpisodicItemRecord> {
    let projection_group_id = model
        .projection_group_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| model.id.clone());
    Ok(ThreadEpisodicItemRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        source_actor_role: source_actor_role_from_db(model.source_actor_role.as_str())?,
        source_runtime_kind: source_runtime_kind_from_db(model.source_runtime_kind.as_str())?,
        source_context: serde_json::from_str(model.source_context_json.as_str())?,
        visibility: item_visibility_from_db(model.visibility.as_str())?,
        status: item_status_from_db(model.status.as_str())?,
        text_hash: model.text_hash,
        source_text_hash: model.source_text_hash,
        projection_group_id,
        embedding_artifact_id: model.embedding_artifact_id,
        language_hint: model.language_hint,
        token_estimate: model.token_estimate,
        indexed_at: model.indexed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
        deleted_at: model.deleted_at,
    })
}

pub(crate) fn thread_episodic_embedding_artifact_record_from_model(
    model: pioneer_entity::thread_episodic_embedding_artifacts::Model,
) -> Result<ThreadEpisodicEmbeddingArtifactRecord> {
    let dimension = usize::try_from(model.dimension)
        .map_err(|_| anyhow::anyhow!("thread episodic embedding dimension is negative"))?;
    let expected_bytes = dimension
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| anyhow::anyhow!("thread episodic embedding byte length overflow"))?;
    if model.vector_bytes.len() != expected_bytes {
        bail!(
            "thread episodic embedding artifact `{}` has {} bytes, expected {}",
            model.id,
            model.vector_bytes.len(),
            expected_bytes
        );
    }
    let vector = model
        .vector_bytes
        .chunks_exact(size_of::<f32>())
        .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        dimension > 0
            && vector.iter().all(|value| value.is_finite())
            && vector.iter().any(|value| *value != 0.0),
        "invalid stored embedding artifact"
    );
    Ok(ThreadEpisodicEmbeddingArtifactRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        pipeline_identity_hash: model.pipeline_identity_hash,
        input_hash: model.input_hash,
        provider_id: model.provider_id,
        model: model.model,
        dimension,
        normalized: model.normalized,
        vector,
        created_at: model.created_at,
        last_used_at: model.last_used_at,
    })
}

pub(crate) fn thread_episodic_index_job_record_from_model(
    model: pioneer_entity::thread_episodic_index_jobs::Model,
) -> Result<ThreadEpisodicIndexJobRecord> {
    Ok(ThreadEpisodicIndexJobRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        index_item_id: model.index_item_id,
        status: index_job_status_from_db(model.status.as_str())?,
        attempt_count: model.attempt_count,
        last_attempt_latency_ms: model.last_attempt_latency_ms,
        next_run_at: model.next_run_at,
        last_error: model.last_error,
        created_at: model.created_at,
        updated_at: model.updated_at,
        completed_at: model.completed_at,
    })
}

pub(crate) fn thread_episodic_exclusion_record_from_model(
    model: pioneer_entity::thread_episodic_exclusions::Model,
) -> Result<ThreadEpisodicExclusionRecord> {
    Ok(ThreadEpisodicExclusionRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        index_item_id: model.index_item_id,
        reason: exclusion_reason_from_db(model.reason.as_str())?,
        created_by: model.created_by,
        created_at: model.created_at,
    })
}

pub(crate) fn thread_episodic_recall_event_record_from_model(
    model: pioneer_entity::thread_episodic_recall_events::Model,
) -> ThreadEpisodicRecallEventRecord {
    ThreadEpisodicRecallEventRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        query_hash: model.query_hash,
        search_profile_json: model.search_profile_json,
        search_mode: model.search_mode,
        adaptive_strategy: model.adaptive_strategy,
        cutoff_json: model.cutoff_json,
        candidate_count: model.candidate_count,
        returned_count: model.returned_count,
        latency_ms: model.latency_ms,
        fallback_used: model.fallback_used,
        error: model.error,
        created_at: model.created_at,
    }
}

pub(crate) fn thread_episodic_thread_directory_record_from_model(
    model: pioneer_entity::thread_episodic_thread_directory::Model,
) -> ThreadEpisodicThreadDirectoryRecord {
    ThreadEpisodicThreadDirectoryRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        title: model.title,
        summary_hash: model.summary_hash,
        summary_ref: model.summary_ref,
        thread_created_at: model.thread_created_at,
        thread_updated_at: model.thread_updated_at,
        last_indexed_at: model.last_indexed_at,
        indexed_item_count: model.indexed_item_count,
        task_affinity_json: model.task_affinity_json,
        project_affinity_json: model.project_affinity_json,
        visibility: thread_directory_visibility_from_db(model.visibility.as_str()),
        status: thread_directory_status_from_db(model.status.as_str()),
        created_at: model.created_at,
        updated_at: model.updated_at,
    }
}

pub(crate) fn source_actor_role_to_db(role: ThreadEpisodicSourceActorRole) -> &'static str {
    match role {
        ThreadEpisodicSourceActorRole::User => "user",
        ThreadEpisodicSourceActorRole::Assistant => "assistant",
        ThreadEpisodicSourceActorRole::Task => "task",
        ThreadEpisodicSourceActorRole::SystemVisible => "system_visible",
    }
}

pub(crate) fn source_actor_role_from_db(value: &str) -> Result<ThreadEpisodicSourceActorRole> {
    match value {
        "user" => Ok(ThreadEpisodicSourceActorRole::User),
        "assistant" => Ok(ThreadEpisodicSourceActorRole::Assistant),
        "task" => Ok(ThreadEpisodicSourceActorRole::Task),
        "system_visible" => Ok(ThreadEpisodicSourceActorRole::SystemVisible),
        other => bail!("unknown thread episodic source actor role `{other}`"),
    }
}

pub(crate) fn source_runtime_kind_to_db(kind: ThreadEpisodicSourceRuntimeKind) -> &'static str {
    match kind {
        ThreadEpisodicSourceRuntimeKind::UserTurn => "user_turn",
        ThreadEpisodicSourceRuntimeKind::AssistantTurn => "assistant_turn",
        ThreadEpisodicSourceRuntimeKind::TaskResult => "task_result",
        ThreadEpisodicSourceRuntimeKind::CompactionSummary => "compaction_summary",
    }
}

pub(crate) fn source_runtime_kind_from_db(value: &str) -> Result<ThreadEpisodicSourceRuntimeKind> {
    match value {
        "user_turn" => Ok(ThreadEpisodicSourceRuntimeKind::UserTurn),
        "assistant_turn" => Ok(ThreadEpisodicSourceRuntimeKind::AssistantTurn),
        "task_result" => Ok(ThreadEpisodicSourceRuntimeKind::TaskResult),
        "compaction_summary" => Ok(ThreadEpisodicSourceRuntimeKind::CompactionSummary),
        other => bail!("unknown thread episodic source runtime kind `{other}`"),
    }
}

pub(crate) fn item_visibility_to_db(visibility: ThreadEpisodicItemVisibility) -> &'static str {
    match visibility {
        ThreadEpisodicItemVisibility::UserVisible => "user_visible",
        ThreadEpisodicItemVisibility::ParentVisible => "parent_visible",
        ThreadEpisodicItemVisibility::InternalHidden => "internal_hidden",
    }
}

pub(crate) fn item_visibility_from_db(value: &str) -> Result<ThreadEpisodicItemVisibility> {
    match value {
        "user_visible" => Ok(ThreadEpisodicItemVisibility::UserVisible),
        "parent_visible" => Ok(ThreadEpisodicItemVisibility::ParentVisible),
        "internal_hidden" => Ok(ThreadEpisodicItemVisibility::InternalHidden),
        other => bail!("unknown thread episodic item visibility `{other}`"),
    }
}

pub(crate) fn item_status_to_db(status: ThreadEpisodicItemStatus) -> &'static str {
    match status {
        ThreadEpisodicItemStatus::PendingIndex => "pending_index",
        ThreadEpisodicItemStatus::Active => "active",
        ThreadEpisodicItemStatus::Excluded => "excluded",
        ThreadEpisodicItemStatus::Superseded => "superseded",
        ThreadEpisodicItemStatus::Deleted => "deleted",
        ThreadEpisodicItemStatus::Failed => "failed",
    }
}

pub(crate) fn item_status_from_db(value: &str) -> Result<ThreadEpisodicItemStatus> {
    match value {
        "pending_index" => Ok(ThreadEpisodicItemStatus::PendingIndex),
        "active" => Ok(ThreadEpisodicItemStatus::Active),
        "excluded" => Ok(ThreadEpisodicItemStatus::Excluded),
        "superseded" => Ok(ThreadEpisodicItemStatus::Superseded),
        "deleted" => Ok(ThreadEpisodicItemStatus::Deleted),
        "failed" => Ok(ThreadEpisodicItemStatus::Failed),
        other => bail!("unknown thread episodic item status `{other}`"),
    }
}

#[allow(dead_code)]
pub(crate) fn index_job_status_to_db(status: ThreadEpisodicIndexJobStatus) -> &'static str {
    match status {
        ThreadEpisodicIndexJobStatus::Queued => "queued",
        ThreadEpisodicIndexJobStatus::Running => "running",
        ThreadEpisodicIndexJobStatus::Completed => "completed",
        ThreadEpisodicIndexJobStatus::Failed => "failed",
        ThreadEpisodicIndexJobStatus::Canceled => "canceled",
    }
}

pub(crate) fn index_job_status_from_db(value: &str) -> Result<ThreadEpisodicIndexJobStatus> {
    match value {
        "queued" => Ok(ThreadEpisodicIndexJobStatus::Queued),
        "running" => Ok(ThreadEpisodicIndexJobStatus::Running),
        "completed" => Ok(ThreadEpisodicIndexJobStatus::Completed),
        "failed" => Ok(ThreadEpisodicIndexJobStatus::Failed),
        "canceled" => Ok(ThreadEpisodicIndexJobStatus::Canceled),
        other => bail!("unknown thread episodic index job status `{other}`"),
    }
}

#[allow(dead_code)]
pub(crate) fn exclusion_reason_to_db(reason: ThreadEpisodicExclusionReason) -> &'static str {
    match reason {
        ThreadEpisodicExclusionReason::UserRequested => "user_requested",
        ThreadEpisodicExclusionReason::Deletion => "deletion",
        ThreadEpisodicExclusionReason::Privacy => "privacy",
        ThreadEpisodicExclusionReason::Policy => "policy",
        ThreadEpisodicExclusionReason::Admin => "admin",
    }
}

pub(crate) fn exclusion_reason_from_db(value: &str) -> Result<ThreadEpisodicExclusionReason> {
    match value {
        "user_requested" => Ok(ThreadEpisodicExclusionReason::UserRequested),
        "deletion" => Ok(ThreadEpisodicExclusionReason::Deletion),
        "privacy" => Ok(ThreadEpisodicExclusionReason::Privacy),
        "policy" => Ok(ThreadEpisodicExclusionReason::Policy),
        "admin" => Ok(ThreadEpisodicExclusionReason::Admin),
        other => bail!("unknown thread episodic exclusion reason `{other}`"),
    }
}

pub(crate) fn thread_directory_visibility_to_db(
    visibility: ThreadEpisodicThreadDirectoryVisibility,
) -> &'static str {
    match visibility {
        ThreadEpisodicThreadDirectoryVisibility::Visible => "visible",
        ThreadEpisodicThreadDirectoryVisibility::Hidden => "hidden",
    }
}

pub(crate) fn thread_directory_visibility_from_db(
    value: &str,
) -> ThreadEpisodicThreadDirectoryVisibility {
    match value {
        "hidden" => ThreadEpisodicThreadDirectoryVisibility::Hidden,
        _ => ThreadEpisodicThreadDirectoryVisibility::Visible,
    }
}

pub(crate) fn thread_directory_status_to_db(
    status: ThreadEpisodicThreadDirectoryStatus,
) -> &'static str {
    match status {
        ThreadEpisodicThreadDirectoryStatus::Active => "active",
        ThreadEpisodicThreadDirectoryStatus::Deleted => "deleted",
    }
}

pub(crate) fn thread_directory_status_from_db(value: &str) -> ThreadEpisodicThreadDirectoryStatus {
    match value {
        "deleted" => ThreadEpisodicThreadDirectoryStatus::Deleted,
        _ => ThreadEpisodicThreadDirectoryStatus::Active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_entity::thread_episodic_items;
    use sea_orm::entity::prelude::DateTimeWithTimeZone;

    fn timestamp() -> DateTimeWithTimeZone {
        DateTimeWithTimeZone::parse_from_rfc3339("2026-06-14T00:00:00Z").expect("valid timestamp")
    }

    #[test]
    fn item_model_conversion_preserves_typed_boundaries_without_text_payload() {
        let model = thread_episodic_items::Model {
            search_text: Some("derived text".to_owned()),
            id: "item_1".to_owned(),
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: "item_1".to_owned(),
            source_actor_role: "assistant".to_owned(),
            source_runtime_kind: "assistant_turn".to_owned(),
            source_context_json: serde_json::to_string(
                &pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem,
            )
            .expect("serialize source context"),
            visibility: "user_visible".to_owned(),
            status: "active".to_owned(),
            text_hash: "text_hash".to_owned(),
            source_text_hash: "source_hash".to_owned(),
            projection_group_id: Some("projection_group".to_owned()),
            embedding_artifact_id: Some("embedding_artifact".to_owned()),
            language_hint: Some("ru".to_owned()),
            token_estimate: 8,
            indexed_at: Some(timestamp()),
            created_at: timestamp(),
            updated_at: timestamp(),
            deleted_at: None,
        };

        let record = thread_episodic_item_record_from_model(model).expect("convert item");
        assert_eq!(
            record.source_context,
            pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem
        );
        assert_eq!(record.visibility, ThreadEpisodicItemVisibility::UserVisible);
        assert_eq!(record.status, ThreadEpisodicItemStatus::Active);
        assert_eq!(record.text_hash, "text_hash");
        assert_eq!(record.projection_group_id, "projection_group");
        assert_eq!(
            record.embedding_artifact_id.as_deref(),
            Some("embedding_artifact")
        );
    }
}

/// Conditions supplied by the product's authorization boundary. A missing
/// principal restricts no ACL only when the caller has an absolute read grant.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchAccess {
    pub principal_id: Option<String>,
    pub root_thread_id: Option<String>,
    pub include_internal: bool,
    pub current_thread_id: Option<String>,
}

/// Equal-weight reciprocal-rank policy, independent of the storage engine.
/// Its top score is 1 when both branches rank a result first. A single first
/// rank scores 0.5; this scale is suitable for the existing 0..1 thresholds.
pub const THREAD_SEARCH_RRF_K: f64 = 10.0;
/// Invalidates continuation positions when ranking/grouping changes.
pub const THREAD_SEARCH_RANKING_VERSION: u32 = 2;
pub fn thread_search_rank_score(lexical_rank: Option<i64>, semantic_rank: Option<i64>) -> f32 {
    let contribution = |rank: Option<i64>| {
        rank.filter(|rank| *rank > 0).map_or(0.0, |rank| {
            (THREAD_SEARCH_RRF_K + 1.0) / (THREAD_SEARCH_RRF_K + rank as f64)
        })
    };
    let score = (contribution(lexical_rank) + contribution(semantic_rank)) / 2.0;
    // Monotonic calibration preserves SQL order and the existing recall scale:
    // a lone rank r still scores 61/(2*(60+r)), including 0.25 at rank 62.
    // Above the single-branch ceiling, retain the 0.5..1 agreement range.
    // This avoids silently dropping lexical/semantic-only recall candidates
    // when sharpening fusion, without changing any caller's cutoff settings.
    if score <= 0.5 {
        const SINGLE_BRANCH_SCORE_K: f64 = 60.0;
        ((SINGLE_BRANCH_SCORE_K + 1.0) * score
            / (2.0 * (SINGLE_BRANCH_SCORE_K - THREAD_SEARCH_RRF_K) * score
                + THREAD_SEARCH_RRF_K
                + 1.0)) as f32
    } else {
        score as f32
    }
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchQuery {
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub access: ThreadEpisodicSearchAccess,
    pub query: String,
    pub lexical: bool,
    pub embedding: Option<ThreadEpisodicSearchEmbedding>,
    pub created_after_unix: Option<i64>,
    pub offset: u64,
    pub limit: u32,
    pub snippet_chars: u32,
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchEmbedding {
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchRow {
    pub item: ThreadEpisodicItemRecord,
    pub text: String,
    pub lexical_rank: Option<i64>,
    pub semantic_rank: Option<i64>,
}

/// Snapshot of the existing projection owner marker, prepared before acquiring
/// database capacity and revalidated with source/attempt in the publish commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicPublicationGuard {
    pub projection_key: String,
    pub reset_checkpoint_key: String,
    pub projection_version: i64,
    pub config_hash: String,
    pub config_json: String,
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchResult {
    pub rows: Vec<ThreadEpisodicSearchRow>,
    pub coverage_incomplete: bool,
}

pub fn thread_episodic_refill_projection_key(workspace_id: &str) -> Result<String> {
    if workspace_id == "__default__" {
        return Ok("thread_episodic_workspace_capsule_refill".to_owned());
    }
    Ok(format!(
        "thread_episodic_workspace_capsule_refill:{}",
        thread_episodic_key_hash("workspace", workspace_id)?
    ))
}
