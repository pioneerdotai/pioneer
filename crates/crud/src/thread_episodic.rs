use anyhow::{Result, bail};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicCapsuleWriteState {
    ActiveWrite,
    ReadOnly,
    Full,
    Compacting,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicCapsuleStatus {
    Active,
    Missing,
    RepairNeeded,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicRepairStatus {
    Ok,
    RepairNeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicSourceActorRole {
    User,
    Assistant,
    Tool,
    Task,
    SystemVisible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicSourceRuntimeKind {
    UserTurn,
    AssistantTurn,
    TaskResult,
    ToolSummary,
    CompactionSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicChunkVisibility {
    UserVisible,
    ParentVisible,
    InternalHidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicChunkStatus {
    PendingIndex,
    Active,
    Excluded,
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
pub enum ThreadEpisodicGraphEnrichmentState {
    NotSupported,
    Disabled,
    Pending,
    Completed,
    Failed,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicCapsuleRecord {
    pub id: String,
    pub workspace_id: String,
    pub workspace_key_hash: String,
    pub thread_id: String,
    pub thread_key_hash: String,
    pub segment_index: i64,
    pub write_state: ThreadEpisodicCapsuleWriteState,
    pub capsule_ref: String,
    pub storage_uri: String,
    pub backend: String,
    pub format: String,
    pub encrypted: bool,
    pub status: ThreadEpisodicCapsuleStatus,
    pub repair_status: ThreadEpisodicRepairStatus,
    pub active_chunk_count: i64,
    pub capacity_bytes: Option<i64>,
    pub size_bytes: Option<i64>,
    pub utilization_percent: Option<f64>,
    pub last_capacity_check_at: Option<DateTimeWithTimeZone>,
    pub near_capacity_at: Option<DateTimeWithTimeZone>,
    pub capacity_exceeded_at: Option<DateTimeWithTimeZone>,
    pub last_vacuumed_at: Option<DateTimeWithTimeZone>,
    pub last_compacted_at: Option<DateTimeWithTimeZone>,
    pub content_hash: Option<String>,
    pub metadata_json: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicActiveWriteSegmentRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub storage_uri_root: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewThreadEpisodicCapsuleRecord {
    pub id: String,
    pub workspace_id: String,
    pub workspace_key_hash: String,
    pub thread_id: String,
    pub thread_key_hash: String,
    pub segment_index: i64,
    pub write_state: ThreadEpisodicCapsuleWriteState,
    pub capsule_ref: String,
    pub storage_uri: String,
    pub backend: String,
    pub format: String,
    pub encrypted: bool,
    pub status: ThreadEpisodicCapsuleStatus,
    pub repair_status: ThreadEpisodicRepairStatus,
    pub active_chunk_count: i64,
    pub capacity_bytes: Option<i64>,
    pub size_bytes: Option<i64>,
    pub utilization_percent: Option<f64>,
    pub content_hash: Option<String>,
    pub metadata_json: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicCapsuleCapacityUpdate {
    pub capacity_bytes: Option<i64>,
    pub size_bytes: Option<i64>,
    pub utilization_percent: Option<f64>,
    pub active_chunk_count: Option<i64>,
    pub near_capacity_at: Option<DateTimeWithTimeZone>,
    pub capacity_exceeded_at: Option<DateTimeWithTimeZone>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicChunkRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub chunk_index: i64,
    pub chunk_count: i64,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_runtime_kind: ThreadEpisodicSourceRuntimeKind,
    pub source_context: pioneer_protocol::ThreadEpisodicSourceContext,
    pub visibility: ThreadEpisodicChunkVisibility,
    pub status: ThreadEpisodicChunkStatus,
    pub text_hash: String,
    pub source_text_hash: String,
    pub char_start: i64,
    pub char_end: i64,
    pub byte_start: Option<i64>,
    pub byte_end: Option<i64>,
    pub language_hint: Option<String>,
    pub token_estimate: i64,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub segment_index: Option<i64>,
    pub frame_id: Option<i64>,
    pub frame_uri: Option<String>,
    pub indexed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub deleted_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicChunkRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub chunk_index: i64,
    pub chunk_count: i64,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_runtime_kind: ThreadEpisodicSourceRuntimeKind,
    pub source_context: pioneer_protocol::ThreadEpisodicSourceContext,
    pub visibility: ThreadEpisodicChunkVisibility,
    pub status: ThreadEpisodicChunkStatus,
    pub text_hash: String,
    pub source_text_hash: String,
    pub char_start: i64,
    pub char_end: i64,
    pub byte_start: Option<i64>,
    pub byte_end: Option<i64>,
    pub language_hint: Option<String>,
    pub token_estimate: i64,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub segment_index: Option<i64>,
    pub frame_id: Option<i64>,
    pub frame_uri: Option<String>,
    pub indexed_at: Option<DateTimeWithTimeZone>,
    pub deleted_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub chunk_id: String,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub segment_index: Option<i64>,
    pub frame_uri: Option<String>,
    pub status: ThreadEpisodicIndexJobStatus,
    pub graph_enrichment_state: ThreadEpisodicGraphEnrichmentState,
    pub attempt_count: i64,
    pub capacity_error_count: i64,
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
    pub chunk_id: String,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub segment_index: Option<i64>,
    pub frame_uri: Option<String>,
    pub status: ThreadEpisodicIndexJobStatus,
    pub graph_enrichment_state: ThreadEpisodicGraphEnrichmentState,
    pub next_run_at: DateTimeWithTimeZone,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobCompletionUpdate {
    pub capsule_id: String,
    pub capsule_ref: String,
    pub segment_index: i64,
    pub frame_uri: String,
    pub last_attempt_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexJobFailureUpdate {
    pub retryable: bool,
    pub next_run_at_unix: Option<i64>,
    pub last_error: Option<String>,
    pub capacity_error: bool,
    pub last_attempt_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicChunkIndexedUpdate {
    pub capsule_id: String,
    pub capsule_ref: String,
    pub segment_index: i64,
    pub frame_id: i64,
    pub frame_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicExclusionRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub chunk_id: String,
    pub reason: ThreadEpisodicExclusionReason,
    pub created_by: String,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadEpisodicExclusionRecord {
    pub id: Option<String>,
    pub workspace_id: String,
    pub thread_id: String,
    pub chunk_id: String,
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
    pub indexed_chunk_count: i64,
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
    pub indexed_chunk_count: i64,
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

pub fn thread_episodic_capsule_ref(
    workspace_key_hash: &str,
    thread_key_hash: &str,
    segment_index: i64,
    capsule_id: &str,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    ensure_ref_part("thread_key_hash", thread_key_hash)?;
    ensure_ref_part("capsule_id", capsule_id)?;
    if segment_index < 0 {
        bail!("thread episodic segment index cannot be negative");
    }
    Ok(format!(
        "mv2://pioneer/thread_episodic/{}/{}/segments/{:06}/capsules/{}",
        workspace_key_hash, thread_key_hash, segment_index, capsule_id
    ))
}

pub fn thread_episodic_capsule_storage_uri(
    storage_uri_root: &str,
    workspace_key_hash: &str,
    thread_key_hash: &str,
    segment_index: i64,
    capsule_id: &str,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    ensure_ref_part("thread_key_hash", thread_key_hash)?;
    ensure_ref_part("capsule_id", capsule_id)?;
    if storage_uri_root.trim().is_empty() {
        bail!("thread episodic storage uri root cannot be empty");
    }
    if segment_index < 0 {
        bail!("thread episodic segment index cannot be negative");
    }
    Ok(format!(
        "{}/thread_episodic/workspace/{}/thread/{}/segments/{:06}/{}.mv2",
        storage_uri_root.trim_end_matches('/'),
        workspace_key_hash,
        thread_key_hash,
        segment_index,
        capsule_id
    ))
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

pub fn deterministic_thread_episodic_capsule_id(
    workspace_key_hash: &str,
    thread_key_hash: &str,
    segment_index: i64,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    ensure_ref_part("thread_key_hash", thread_key_hash)?;
    if segment_index < 0 {
        bail!("thread episodic segment index cannot be negative");
    }
    let mut hasher = Sha256::new();
    hasher.update(b"thread_episodic_capsule\0");
    hasher.update(workspace_key_hash.as_bytes());
    hasher.update([0]);
    hasher.update(thread_key_hash.as_bytes());
    hasher.update([0]);
    hasher.update(segment_index.to_string().as_bytes());
    let hash = hex::encode(hasher.finalize());
    Ok(hash.chars().take(21).collect())
}

pub fn thread_episodic_frame_uri(capsule_ref: &str, chunk_id: &str) -> Result<String> {
    ensure_ref_part("capsule_ref", capsule_ref)?;
    ensure_ref_part("chunk_id", chunk_id)?;
    Ok(format!("{capsule_ref}/chunk/{chunk_id}"))
}

pub(crate) fn thread_episodic_capsule_record_from_model(
    model: pioneer_entity::thread_episodic_capsules::Model,
) -> Result<ThreadEpisodicCapsuleRecord> {
    Ok(ThreadEpisodicCapsuleRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        workspace_key_hash: model.workspace_key_hash,
        thread_id: model.thread_id,
        thread_key_hash: model.thread_key_hash,
        segment_index: model.segment_index,
        write_state: capsule_write_state_from_db(model.write_state.as_str())?,
        capsule_ref: model.capsule_ref,
        storage_uri: model.storage_uri,
        backend: model.backend,
        format: model.format,
        encrypted: model.encrypted,
        status: capsule_status_from_db(model.status.as_str())?,
        repair_status: repair_status_from_db(model.repair_status.as_str())?,
        active_chunk_count: model.active_chunk_count,
        capacity_bytes: model.capacity_bytes,
        size_bytes: model.size_bytes,
        utilization_percent: model.utilization_percent,
        last_capacity_check_at: model.last_capacity_check_at,
        near_capacity_at: model.near_capacity_at,
        capacity_exceeded_at: model.capacity_exceeded_at,
        last_vacuumed_at: model.last_vacuumed_at,
        last_compacted_at: model.last_compacted_at,
        content_hash: model.content_hash,
        metadata_json: model.metadata_json,
        last_error: model.last_error,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

pub(crate) fn thread_episodic_chunk_record_from_model(
    model: pioneer_entity::thread_episodic_chunks::Model,
) -> Result<ThreadEpisodicChunkRecord> {
    Ok(ThreadEpisodicChunkRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        chunk_index: model.chunk_index,
        chunk_count: model.chunk_count,
        source_actor_role: source_actor_role_from_db(model.source_actor_role.as_str())?,
        source_runtime_kind: source_runtime_kind_from_db(model.source_runtime_kind.as_str())?,
        source_context: serde_json::from_str(model.source_context_json.as_str())?,
        visibility: chunk_visibility_from_db(model.visibility.as_str())?,
        status: chunk_status_from_db(model.status.as_str())?,
        text_hash: model.text_hash,
        source_text_hash: model.source_text_hash,
        char_start: model.char_start,
        char_end: model.char_end,
        byte_start: model.byte_start,
        byte_end: model.byte_end,
        language_hint: model.language_hint,
        token_estimate: model.token_estimate,
        capsule_id: model.capsule_id,
        capsule_ref: model.capsule_ref,
        segment_index: model.segment_index,
        frame_id: model.frame_id,
        frame_uri: model.frame_uri,
        indexed_at: model.indexed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
        deleted_at: model.deleted_at,
    })
}

pub(crate) fn thread_episodic_index_job_record_from_model(
    model: pioneer_entity::thread_episodic_index_jobs::Model,
) -> Result<ThreadEpisodicIndexJobRecord> {
    Ok(ThreadEpisodicIndexJobRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        chunk_id: model.chunk_id,
        capsule_id: model.capsule_id,
        capsule_ref: model.capsule_ref,
        segment_index: model.segment_index,
        frame_uri: model.frame_uri,
        status: index_job_status_from_db(model.status.as_str())?,
        graph_enrichment_state: graph_enrichment_state_from_db(
            model.graph_enrichment_state.as_str(),
        )?,
        attempt_count: model.attempt_count,
        capacity_error_count: model.capacity_error_count,
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
        chunk_id: model.chunk_id,
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
        indexed_chunk_count: model.indexed_chunk_count,
        task_affinity_json: model.task_affinity_json,
        project_affinity_json: model.project_affinity_json,
        visibility: thread_directory_visibility_from_db(model.visibility.as_str()),
        status: thread_directory_status_from_db(model.status.as_str()),
        created_at: model.created_at,
        updated_at: model.updated_at,
    }
}

pub(crate) fn capsule_write_state_to_db(state: ThreadEpisodicCapsuleWriteState) -> &'static str {
    match state {
        ThreadEpisodicCapsuleWriteState::ActiveWrite => "active_write",
        ThreadEpisodicCapsuleWriteState::ReadOnly => "read_only",
        ThreadEpisodicCapsuleWriteState::Full => "full",
        ThreadEpisodicCapsuleWriteState::Compacting => "compacting",
        ThreadEpisodicCapsuleWriteState::Deleted => "deleted",
    }
}

pub(crate) fn capsule_write_state_from_db(value: &str) -> Result<ThreadEpisodicCapsuleWriteState> {
    match value {
        "active_write" => Ok(ThreadEpisodicCapsuleWriteState::ActiveWrite),
        "read_only" => Ok(ThreadEpisodicCapsuleWriteState::ReadOnly),
        "full" => Ok(ThreadEpisodicCapsuleWriteState::Full),
        "compacting" => Ok(ThreadEpisodicCapsuleWriteState::Compacting),
        "deleted" => Ok(ThreadEpisodicCapsuleWriteState::Deleted),
        other => bail!("unknown thread episodic capsule write state `{other}`"),
    }
}

pub(crate) fn capsule_status_to_db(status: ThreadEpisodicCapsuleStatus) -> &'static str {
    match status {
        ThreadEpisodicCapsuleStatus::Active => "active",
        ThreadEpisodicCapsuleStatus::Missing => "missing",
        ThreadEpisodicCapsuleStatus::RepairNeeded => "repair_needed",
        ThreadEpisodicCapsuleStatus::Deleted => "deleted",
    }
}

pub(crate) fn capsule_status_from_db(value: &str) -> Result<ThreadEpisodicCapsuleStatus> {
    match value {
        "active" => Ok(ThreadEpisodicCapsuleStatus::Active),
        "missing" => Ok(ThreadEpisodicCapsuleStatus::Missing),
        "repair_needed" => Ok(ThreadEpisodicCapsuleStatus::RepairNeeded),
        "deleted" => Ok(ThreadEpisodicCapsuleStatus::Deleted),
        other => bail!("unknown thread episodic capsule status `{other}`"),
    }
}

pub(crate) fn repair_status_to_db(status: ThreadEpisodicRepairStatus) -> &'static str {
    match status {
        ThreadEpisodicRepairStatus::Ok => "ok",
        ThreadEpisodicRepairStatus::RepairNeeded => "repair_needed",
        ThreadEpisodicRepairStatus::Failed => "failed",
    }
}

pub(crate) fn repair_status_from_db(value: &str) -> Result<ThreadEpisodicRepairStatus> {
    match value {
        "ok" => Ok(ThreadEpisodicRepairStatus::Ok),
        "repair_needed" => Ok(ThreadEpisodicRepairStatus::RepairNeeded),
        "failed" => Ok(ThreadEpisodicRepairStatus::Failed),
        other => bail!("unknown thread episodic repair status `{other}`"),
    }
}

pub(crate) fn source_actor_role_to_db(role: ThreadEpisodicSourceActorRole) -> &'static str {
    match role {
        ThreadEpisodicSourceActorRole::User => "user",
        ThreadEpisodicSourceActorRole::Assistant => "assistant",
        ThreadEpisodicSourceActorRole::Tool => "tool",
        ThreadEpisodicSourceActorRole::Task => "task",
        ThreadEpisodicSourceActorRole::SystemVisible => "system_visible",
    }
}

pub(crate) fn source_actor_role_from_db(value: &str) -> Result<ThreadEpisodicSourceActorRole> {
    match value {
        "user" => Ok(ThreadEpisodicSourceActorRole::User),
        "assistant" => Ok(ThreadEpisodicSourceActorRole::Assistant),
        "tool" => Ok(ThreadEpisodicSourceActorRole::Tool),
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
        ThreadEpisodicSourceRuntimeKind::ToolSummary => "tool_summary",
        ThreadEpisodicSourceRuntimeKind::CompactionSummary => "compaction_summary",
    }
}

pub(crate) fn source_runtime_kind_from_db(value: &str) -> Result<ThreadEpisodicSourceRuntimeKind> {
    match value {
        "user_turn" => Ok(ThreadEpisodicSourceRuntimeKind::UserTurn),
        "assistant_turn" => Ok(ThreadEpisodicSourceRuntimeKind::AssistantTurn),
        "task_result" => Ok(ThreadEpisodicSourceRuntimeKind::TaskResult),
        "tool_summary" => Ok(ThreadEpisodicSourceRuntimeKind::ToolSummary),
        "compaction_summary" => Ok(ThreadEpisodicSourceRuntimeKind::CompactionSummary),
        other => bail!("unknown thread episodic source runtime kind `{other}`"),
    }
}

pub(crate) fn chunk_visibility_to_db(visibility: ThreadEpisodicChunkVisibility) -> &'static str {
    match visibility {
        ThreadEpisodicChunkVisibility::UserVisible => "user_visible",
        ThreadEpisodicChunkVisibility::ParentVisible => "parent_visible",
        ThreadEpisodicChunkVisibility::InternalHidden => "internal_hidden",
    }
}

pub(crate) fn chunk_visibility_from_db(value: &str) -> Result<ThreadEpisodicChunkVisibility> {
    match value {
        "user_visible" => Ok(ThreadEpisodicChunkVisibility::UserVisible),
        "parent_visible" => Ok(ThreadEpisodicChunkVisibility::ParentVisible),
        "internal_hidden" => Ok(ThreadEpisodicChunkVisibility::InternalHidden),
        other => bail!("unknown thread episodic chunk visibility `{other}`"),
    }
}

pub(crate) fn chunk_status_to_db(status: ThreadEpisodicChunkStatus) -> &'static str {
    match status {
        ThreadEpisodicChunkStatus::PendingIndex => "pending_index",
        ThreadEpisodicChunkStatus::Active => "active",
        ThreadEpisodicChunkStatus::Excluded => "excluded",
        ThreadEpisodicChunkStatus::Deleted => "deleted",
        ThreadEpisodicChunkStatus::Failed => "failed",
    }
}

pub(crate) fn chunk_status_from_db(value: &str) -> Result<ThreadEpisodicChunkStatus> {
    match value {
        "pending_index" => Ok(ThreadEpisodicChunkStatus::PendingIndex),
        "active" => Ok(ThreadEpisodicChunkStatus::Active),
        "excluded" => Ok(ThreadEpisodicChunkStatus::Excluded),
        "deleted" => Ok(ThreadEpisodicChunkStatus::Deleted),
        "failed" => Ok(ThreadEpisodicChunkStatus::Failed),
        other => bail!("unknown thread episodic chunk status `{other}`"),
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
pub(crate) fn graph_enrichment_state_to_db(
    state: ThreadEpisodicGraphEnrichmentState,
) -> &'static str {
    match state {
        ThreadEpisodicGraphEnrichmentState::NotSupported => "not_supported",
        ThreadEpisodicGraphEnrichmentState::Disabled => "disabled",
        ThreadEpisodicGraphEnrichmentState::Pending => "pending",
        ThreadEpisodicGraphEnrichmentState::Completed => "completed",
        ThreadEpisodicGraphEnrichmentState::Failed => "failed",
    }
}

pub(crate) fn graph_enrichment_state_from_db(
    value: &str,
) -> Result<ThreadEpisodicGraphEnrichmentState> {
    match value {
        "not_supported" => Ok(ThreadEpisodicGraphEnrichmentState::NotSupported),
        "disabled" => Ok(ThreadEpisodicGraphEnrichmentState::Disabled),
        "pending" => Ok(ThreadEpisodicGraphEnrichmentState::Pending),
        "completed" => Ok(ThreadEpisodicGraphEnrichmentState::Completed),
        "failed" => Ok(ThreadEpisodicGraphEnrichmentState::Failed),
        other => bail!("unknown thread episodic graph enrichment state `{other}`"),
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

fn ensure_ref_part(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("thread episodic {field} cannot be empty");
    }
    if value.chars().any(char::is_whitespace) {
        bail!("thread episodic {field} cannot contain whitespace");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_entity::thread_episodic_chunks;
    use sea_orm::entity::prelude::DateTimeWithTimeZone;

    fn timestamp() -> DateTimeWithTimeZone {
        DateTimeWithTimeZone::parse_from_rfc3339("2026-06-14T00:00:00Z").expect("valid timestamp")
    }

    #[test]
    fn chunk_model_conversion_preserves_typed_boundaries_without_text_payload() {
        let model = thread_episodic_chunks::Model {
            id: "chunk_1".to_owned(),
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: "item_1".to_owned(),
            chunk_index: 0,
            chunk_count: 1,
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
            char_start: 0,
            char_end: 32,
            byte_start: Some(0),
            byte_end: Some(32),
            language_hint: Some("ru".to_owned()),
            token_estimate: 8,
            capsule_id: Some("capsule_1".to_owned()),
            capsule_ref: Some(
                "mv2://pioneer/thread_episodic/ws/thread/segments/000000/capsules/capsule_1"
                    .to_owned(),
            ),
            segment_index: Some(0),
            frame_id: Some(42),
            frame_uri: Some("mv2://frame".to_owned()),
            indexed_at: Some(timestamp()),
            created_at: timestamp(),
            updated_at: timestamp(),
            deleted_at: None,
        };

        let record = thread_episodic_chunk_record_from_model(model).expect("convert chunk");
        assert_eq!(
            record.source_context,
            pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem
        );
        assert_eq!(
            record.visibility,
            ThreadEpisodicChunkVisibility::UserVisible
        );
        assert_eq!(record.status, ThreadEpisodicChunkStatus::Active);
        assert_eq!(record.text_hash, "text_hash");
    }

    #[test]
    fn capsule_and_frame_refs_are_logical_mv2_refs() {
        let capsule_ref =
            thread_episodic_capsule_ref("workspace_hash", "thread_hash", 3, "capsule_id")
                .expect("capsule ref");
        assert_eq!(
            capsule_ref,
            "mv2://pioneer/thread_episodic/workspace_hash/thread_hash/segments/000003/capsules/capsule_id"
        );

        let frame_uri =
            thread_episodic_frame_uri(capsule_ref.as_str(), "chunk_id").expect("frame uri");
        assert_eq!(
            frame_uri,
            "mv2://pioneer/thread_episodic/workspace_hash/thread_hash/segments/000003/capsules/capsule_id/chunk/chunk_id"
        );
    }
}
