use anyhow::{Result, bail};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sha2::{Digest, Sha256};

pub const THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID: &str = "__workspace__";
pub const THREAD_EPISODIC_WORKSPACE_SEGMENT_CAPACITY_BYTES: i64 = 50 * 1024 * 1024;

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
    pub active_frame_count: i64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicWorkspaceActiveWriteSegmentRequest {
    pub workspace_id: String,
    pub storage_uri_root: String,
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
    pub active_frame_count: i64,
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
    pub active_frame_count: Option<i64>,
    pub near_capacity_at: Option<DateTimeWithTimeZone>,
    pub capacity_exceeded_at: Option<DateTimeWithTimeZone>,
    pub last_error: Option<String>,
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
    pub index_item_id: String,
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
    pub index_item_id: String,
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
pub struct ThreadEpisodicItemIndexedUpdate {
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

pub fn deterministic_thread_episodic_workspace_capsule_id(
    workspace_key_hash: &str,
    segment_index: i64,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    if segment_index < 0 {
        bail!("thread episodic workspace segment index cannot be negative");
    }
    let mut hasher = Sha256::new();
    hasher.update(b"thread_episodic_workspace_capsule\0");
    hasher.update(workspace_key_hash.as_bytes());
    hasher.update([0]);
    hasher.update(segment_index.to_string().as_bytes());
    let hash = hex::encode(hasher.finalize());
    Ok(hash.chars().take(21).collect())
}

pub fn thread_episodic_workspace_capsule_ref(
    workspace_key_hash: &str,
    segment_index: i64,
    capsule_id: &str,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    ensure_ref_part("capsule_id", capsule_id)?;
    if segment_index < 0 {
        bail!("thread episodic workspace segment index cannot be negative");
    }
    Ok(format!(
        "mv2://pioneer/thread_episodic/workspace/{}/segments/{:06}/capsules/{}",
        workspace_key_hash, segment_index, capsule_id
    ))
}

pub fn thread_episodic_workspace_capsule_storage_uri(
    storage_uri_root: &str,
    workspace_key_hash: &str,
    segment_index: i64,
    capsule_id: &str,
) -> Result<String> {
    ensure_ref_part("workspace_key_hash", workspace_key_hash)?;
    ensure_ref_part("capsule_id", capsule_id)?;
    if storage_uri_root.trim().is_empty() {
        bail!("thread episodic storage uri root cannot be empty");
    }
    if segment_index < 0 {
        bail!("thread episodic workspace segment index cannot be negative");
    }
    Ok(format!(
        "{}/thread_episodic/workspace/{}/segments/{:06}/{}.mv2",
        storage_uri_root.trim_end_matches('/'),
        workspace_key_hash,
        segment_index,
        capsule_id
    ))
}

pub fn thread_episodic_frame_uri(capsule_ref: &str, index_item_id: &str) -> Result<String> {
    ensure_ref_part("capsule_ref", capsule_ref)?;
    ensure_ref_part("index_item_id", index_item_id)?;
    Ok(format!("{capsule_ref}/index/{index_item_id}"))
}

pub fn thread_episodic_workspace_uri_prefix(workspace_id: &str) -> Result<String> {
    Ok(format!(
        "mv2://workspace/{}/",
        encode_thread_episodic_uri_segment("workspace_id", workspace_id)?
    ))
}

pub fn thread_episodic_thread_uri_prefix(workspace_id: &str, thread_id: &str) -> Result<String> {
    Ok(format!(
        "{}thread/{}/",
        thread_episodic_workspace_uri_prefix(workspace_id)?,
        encode_thread_episodic_uri_segment("thread_id", thread_id)?
    ))
}

pub fn thread_episodic_turn_uri_prefix(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<String> {
    Ok(format!(
        "{}turn/{}/",
        thread_episodic_thread_uri_prefix(workspace_id, thread_id)?,
        encode_thread_episodic_uri_segment("turn_id", turn_id)?
    ))
}

pub fn thread_episodic_item_uri(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    index_item_id: &str,
) -> Result<String> {
    Ok(format!(
        "{}item/{}/index/{}",
        thread_episodic_turn_uri_prefix(workspace_id, thread_id, turn_id)?,
        encode_thread_episodic_uri_segment("item_id", item_id)?,
        encode_thread_episodic_uri_segment("index_item_id", index_item_id)?
    ))
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
        active_frame_count: model.active_frame_count,
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

pub(crate) fn thread_episodic_item_record_from_model(
    model: pioneer_entity::thread_episodic_items::Model,
) -> Result<ThreadEpisodicItemRecord> {
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
        index_item_id: model.index_item_id,
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
        ThreadEpisodicItemStatus::Deleted => "deleted",
        ThreadEpisodicItemStatus::Failed => "failed",
    }
}

pub(crate) fn item_status_from_db(value: &str) -> Result<ThreadEpisodicItemStatus> {
    match value {
        "pending_index" => Ok(ThreadEpisodicItemStatus::PendingIndex),
        "active" => Ok(ThreadEpisodicItemStatus::Active),
        "excluded" => Ok(ThreadEpisodicItemStatus::Excluded),
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

fn encode_thread_episodic_uri_segment(field: &str, value: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("thread episodic {field} cannot be empty");
    }
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push_str(format!("{byte:02X}").as_str());
            }
        }
    }
    Ok(encoded)
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

        let record = thread_episodic_item_record_from_model(model).expect("convert item");
        assert_eq!(
            record.source_context,
            pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem
        );
        assert_eq!(record.visibility, ThreadEpisodicItemVisibility::UserVisible);
        assert_eq!(record.status, ThreadEpisodicItemStatus::Active);
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
            thread_episodic_frame_uri(capsule_ref.as_str(), "index_item_id").expect("frame uri");
        assert_eq!(
            frame_uri,
            "mv2://pioneer/thread_episodic/workspace_hash/thread_hash/segments/000003/capsules/capsule_id/index/index_item_id"
        );
    }

    #[test]
    fn workspace_episodic_uri_helpers_build_prefix_chain() {
        let workspace_prefix =
            thread_episodic_workspace_uri_prefix("workspace_1").expect("workspace prefix");
        assert_eq!(workspace_prefix, "mv2://workspace/workspace_1/");

        let thread_prefix =
            thread_episodic_thread_uri_prefix("workspace_1", "thread_1").expect("thread prefix");
        assert_eq!(
            thread_prefix,
            "mv2://workspace/workspace_1/thread/thread_1/"
        );
        assert!(thread_prefix.starts_with(workspace_prefix.as_str()));

        let turn_prefix = thread_episodic_turn_uri_prefix("workspace_1", "thread_1", "turn_1")
            .expect("turn prefix");
        assert_eq!(
            turn_prefix,
            "mv2://workspace/workspace_1/thread/thread_1/turn/turn_1/"
        );
        assert!(turn_prefix.starts_with(thread_prefix.as_str()));

        let item_uri =
            thread_episodic_item_uri("workspace_1", "thread_1", "turn_1", "item_1", "index_1")
                .expect("item uri");
        assert_eq!(
            item_uri,
            "mv2://workspace/workspace_1/thread/thread_1/turn/turn_1/item/item_1/index/index_1"
        );
        assert!(item_uri.starts_with(turn_prefix.as_str()));
    }

    #[test]
    fn workspace_episodic_uri_helpers_escape_unsafe_segments() {
        let item_uri = thread_episodic_item_uri(
            "workspace 1/日本",
            "thread/1",
            "turn?x=1",
            "item #1",
            "index/1",
        )
        .expect("item uri");

        assert_eq!(
            item_uri,
            "mv2://workspace/workspace%201%2F%E6%97%A5%E6%9C%AC/thread/thread%2F1/turn/turn%3Fx%3D1/item/item%20%231/index/index%2F1"
        );
        assert!(thread_episodic_workspace_uri_prefix("  ").is_err());
    }

    #[test]
    fn workspace_capsule_helpers_are_workspace_scoped_and_deterministic() {
        let first = deterministic_thread_episodic_workspace_capsule_id("workspace_hash", 1)
            .expect("workspace capsule id");
        let second = deterministic_thread_episodic_workspace_capsule_id("workspace_hash", 1)
            .expect("workspace capsule id");
        let next = deterministic_thread_episodic_workspace_capsule_id("workspace_hash", 2)
            .expect("workspace capsule id");
        assert_eq!(first, second);
        assert_ne!(first, next);
        assert_eq!(first.len(), 21);

        let capsule_ref =
            thread_episodic_workspace_capsule_ref("workspace_hash", 1, first.as_str())
                .expect("workspace capsule ref");
        assert_eq!(
            capsule_ref,
            format!(
                "mv2://pioneer/thread_episodic/workspace/workspace_hash/segments/000001/capsules/{first}"
            )
        );

        let storage_uri = thread_episodic_workspace_capsule_storage_uri(
            "file:///memory/root/",
            "workspace_hash",
            1,
            first.as_str(),
        )
        .expect("workspace storage uri");
        assert_eq!(
            storage_uri,
            format!(
                "file:///memory/root/thread_episodic/workspace/workspace_hash/segments/000001/{first}.mv2"
            )
        );
        assert!(!storage_uri.contains("thread_1"));
        assert!(!capsule_ref.contains("thread_1"));
    }
}
