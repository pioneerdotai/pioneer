mod convention;
mod events;
mod memory;
mod projector;
mod repositories;
mod task_events;
mod task_projector;
mod thread_episodic;
mod timeline_live_projection;
mod timeline_projection;
mod timeline_projection_model;
mod turn_item_terminal;
mod util;

use anyhow::{Context, Result};
use pioneer_protocol::{
    ArtifactBindingSummary, ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactStatus,
    ArtifactSummary, MemoryCandidateDecision, MemoryCandidateStatus, MemoryScope, MemoryScopeKind,
    PromptManifest, ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus,
    RecoveryTrigger, SandboxMode, StorageOutputPolicy, Task, TaskAgendaItem, TaskAgendaParams,
    TaskAgendaResponse, TaskAgentSpec, TaskDeliveriesParams, TaskDeliveriesResponse, TaskDelivery,
    TaskDeliveryAttempt, TaskDependency, TaskError, TaskEventsResponse, TaskExecutorKind,
    TaskGetResponse, TaskListParams, TaskResult, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewEvent, TaskRun, TaskRunExecution, TaskRunExecutionStatus, TaskRunStatus,
    TaskRunThreadBinding, TaskRunThreadBindingKind, TaskRunTurn, TaskRunTurnStatus,
    TaskThreadLineage, TaskTree, TaskTrigger, TaskTriggerKind, TaskTriggerSpec, TaskWriteLock,
    Thread, ThreadFolder, ThreadHistoryEvent, ThreadHistoryEventPayload, ThreadPlacement,
    TimelineOutputPolicy, ToolCallStatus, ToolDisplayPayload, ToolStoragePayload, Turn, TurnItem,
    TurnItemEvent, TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnItemsResponse,
    TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStatus, UserInput, generate_id,
};
use pioneer_sqlite::{SqliteWriteCoordinator, is_anyhow_sqlite_lock};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;

use crate::convention::{
    ATTEMPT_STATUS_INTERRUPTED, ATTEMPT_STATUS_RUNNING, DB_ID_LEN, MEMORY_EVENT_ACCESSED,
    MEMORY_EVENT_CANDIDATE_APPROVED, MEMORY_EVENT_CANDIDATE_CREATED,
    MEMORY_EVENT_CANDIDATE_EXPIRED, MEMORY_EVENT_CANDIDATE_REJECTED,
    MEMORY_EVENT_CAPSULE_REPAIR_STATUS_CHANGED, MEMORY_EVENT_CREATED, MEMORY_EVENT_EXPIRED,
    MEMORY_EVENT_FORGOTTEN, MEMORY_EVENT_QUARANTINED, MEMORY_EVENT_REPAIR_STATUS_CHANGED,
    MEMORY_EVENT_RESTORED, MEMORY_EVENT_SUPERSEDED, MEMORY_EVENT_UPDATED,
    TURN_ITEM_STATUS_CANCELLED, TURN_ITEM_STATUS_COMPLETED, TURN_ITEM_STATUS_FAILED,
    TURN_ITEM_STATUS_TIMED_OUT, is_terminal_task_run_status_db, is_terminal_task_status_db,
    prompt_manifest_profile_to_db, provider_failure_class_from_db, provider_failure_stage_from_db,
    recovery_action_from_db, recovery_action_to_db, recovery_job_status_from_db,
    recovery_trigger_from_db, task_concurrency_conflict_policy_from_db,
    task_delivery_attempt_status_from_db, task_delivery_mode_from_db, task_delivery_status_from_db,
    task_executor_kind_from_db, task_owner_kind_from_db, task_owner_kind_to_db,
    task_result_candidate_status_from_db, task_result_review_decision_from_db,
    task_result_review_event_kind_from_db, task_result_reviewer_kind_from_db,
    task_run_execution_status_from_db, task_run_status_from_db,
    task_run_thread_binding_kind_from_db, task_run_turn_kind_from_db, task_run_turn_status_from_db,
    task_status_from_db, task_status_to_db, task_trigger_kind_from_db, task_trigger_status_from_db,
    task_write_lock_scope_kind_from_db, task_write_lock_status_from_db, thread_mode_from_db,
    thread_origin_kind_from_db, thread_sidebar_visibility_from_db, thread_status_from_db,
    turn_item_type_from_db, turn_kind_from_db, turn_origin_from_db, turn_permission_mode_from_db,
    turn_permission_profile_source_from_db, turn_status_from_db, turn_status_to_db,
};
use crate::events::{TurnEventPayload, TurnStartedEventPayload};
use crate::projector::TurnProjector;

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantSnapshot {
    pub thread_lineage: Vec<TaskReviewInvariantThreadLineageRecord>,
    pub primary_bindings: Vec<TaskReviewInvariantBindingRecord>,
    pub agent_specs: Vec<TaskReviewInvariantAgentSpecRecord>,
    pub task_run_turns: Vec<TaskReviewInvariantTurnRecord>,
    pub task_result_candidates: Vec<TaskReviewInvariantCandidateRecord>,
    pub task_result_review_events: Vec<TaskReviewInvariantReviewEventRecord>,
    pub task_runs: Vec<TaskReviewInvariantRunRecord>,
    pub write_locks: Vec<TaskReviewInvariantWriteLockRecord>,
    pub turn_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantThreadLineageRecord {
    pub child_thread_id: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantBindingRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub execution_id: Option<String>,
    pub thread_id: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantAgentSpecRecord {
    pub task_id: String,
    pub run_id: Option<String>,
    pub tool_policy_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantTurnRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub kind: String,
    pub round: i64,
    pub sequence: i64,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantCandidateRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub round: i64,
    pub status: String,
    pub result_json: Option<String>,
    pub final_review_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantReviewEventRecord {
    pub id: String,
    pub candidate_id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub decision: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantRunRecord {
    pub id: String,
    pub task_id: String,
    pub status: String,
    pub result_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantWriteLockRecord {
    pub task_id: String,
    pub run_id: String,
    pub status: String,
    pub expires_at_unix: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantSnapshot {
    pub task_events: Vec<TaskRuntimeInvariantEventRecord>,
    pub delivered_task_results: Vec<TaskRuntimeInvariantDeliveryRecord>,
    pub in_progress_turns: Vec<TaskRuntimeInvariantTurnRecord>,
    pub stale_turn_item_attempts: Vec<TaskRuntimeInvariantStaleAttemptRecord>,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantEventRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantDeliveryRecord {
    pub delivery_id: String,
    pub task_id: String,
    pub run_id: String,
    pub run_status: Option<String>,
    pub result_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantTurnRecord {
    pub turn_id: String,
    pub thread_id: Option<String>,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantStaleAttemptRecord {
    pub turn_id: String,
    pub item_id: String,
    pub item_status: String,
    pub attempt_id: String,
    pub attempt_status: String,
    pub attempt_number: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnEventCompactionSummary {
    pub dry_run: bool,
    pub batch_limit: u64,
    pub candidate_rows: u64,
    pub deleted_rows: u64,
    pub payload_bytes: u64,
    pub turns_touched: u64,
    pub latest_snapshots_kept: u64,
    pub skipped_unprojected: u64,
    pub skipped_failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentDiffCompactionCandidate {
    event_id: String,
    turn_id: String,
    item_id: String,
    payload_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentDiffCompactionStats {
    latest_snapshots_kept: u64,
    skipped_unprojected: u64,
    skipped_failed: u64,
}

pub use crate::repositories::artifact::{
    ArtifactBindingTargetRecord, ArtifactBlobRecord, ArtifactCrudError, ArtifactExternalRefKey,
    ArtifactExternalRefRecord, ArtifactGcBlobCandidateRecord, ArtifactGcPlanRecord,
    ArtifactListFilterRecord, ArtifactListPageRecord, ArtifactProjectionBlobRecord,
    ArtifactProjectionRecord, ArtifactRecord, ArtifactVersionBlobRecord, ArtifactVersionRecord,
    ArtifactWorkspaceUsageRecord, ConversationArtifactRef, ConversationArtifactRefLimits,
    ConversationTurnArtifactRefs, IngestArtifactMetadataRecord, IngestedArtifactRecord,
    NewArtifactBlobRecord, UpsertArtifactExternalRefRequest,
};
pub use crate::repositories::cli_runtime_binding::{
    CliRuntimeNativeEventListFilter, CliRuntimeNativeEventRecord,
    CliRuntimePendingRequestListFilter, CliRuntimePendingRequestRecord,
    CliRuntimePendingRequestStatus, CliRuntimeThreadBindingRecord, CliRuntimeTurnBindingListFilter,
    CliRuntimeTurnBindingRecord, NewCliRuntimeNativeEvent, NewCliRuntimePendingRequest,
    NewCliRuntimeThreadBinding, NewCliRuntimeTurnBinding, ResolveCliRuntimePendingRequest,
    deserialize_cli_runtime_json, serialize_cli_runtime_json,
};
pub use crate::repositories::thread_agents_doc::{
    ResolvedThreadAgentsDocRecord, ThreadAgentsDocError, ThreadAgentsDocRecord,
    ThreadAgentsDocRevisionRecord, ThreadAgentsDocSaveReason, ThreadAgentsDocScope,
    ThreadAgentsDocScopeContext, ThreadAgentsDocStatus, ThreadAgentsDocSummaryRecord,
};
pub use crate::repositories::thread_timeline_projection::{
    BLOCK_KIND_APPROVAL, BLOCK_KIND_ASSISTANT_MESSAGE, BLOCK_KIND_RUNNING, BLOCK_KIND_SYSTEM,
    BLOCK_KIND_TURN_WORK, BLOCK_KIND_USER_MESSAGE, PROJECTION_META_STATUS_BACKFILLING,
    PROJECTION_META_STATUS_COMPLETE, PROJECTION_META_STATUS_FAILED, PROJECTION_META_STATUS_PENDING,
    ProjectionMetaRecord, ProjectionPageAnchor, SEMANTIC_TIMELINE_PROJECTION_KEY,
    SEMANTIC_TIMELINE_PROJECTION_VERSION, ThreadTimelineBlockRecord, TurnWorkItemProjectionRecord,
    TurnWorkProjectionRecord, WORK_VISIBILITY_HIDDEN, WORK_VISIBILITY_VISIBLE,
    count_thread_timeline_blocks, count_turn_work_items, delete_thread_timeline_blocks_for_thread,
    delete_thread_timeline_blocks_for_turn, delete_turn_work_items_for_turn,
    delete_turn_work_projection, find_projection_meta, find_thread_timeline_block_by_sort_key,
    find_turn_work_item_projection, find_turn_work_item_projection_by_order_key,
    find_turn_work_projection, list_thread_timeline_blocks_page, list_turn_work_items_page,
    update_projection_meta_status, upsert_projection_meta, upsert_thread_timeline_block,
    upsert_turn_work_item_projection, upsert_turn_work_projection,
};
use crate::repositories::{
    agent_memory, agent_memory_candidate, agent_memory_capsule, agent_memory_event,
    agent_memory_policy_decision, agent_memory_quality_decision, agent_memory_quarantine,
    agent_memory_repair_job, artifact as artifact_repository, cli_runtime_binding, hook_run,
    mcp_audit_event, mcp_server_catalog_snapshot, mcp_server_installation, policy, recovery_job,
    skill_audit_event, skill_dependency_snapshot, skill_installation, skill_upload_session,
    skill_workspace_policy, task as task_repository, task_agent_spec, task_delivery,
    task_dependency, task_event, task_result_candidate, task_result_review_event, task_run,
    task_run_execution, task_run_thread_binding, task_run_turn, task_trigger, task_write_lock,
    thread, thread_agents_doc, thread_episodic as thread_episodic_repository, thread_lineage,
    thread_tree, turn, turn_event, turn_event_projection_state, turn_execution_window,
    turn_item_attempt, turn_llm_context, turn_mcp_binding, turn_runtime_snapshot,
    turn_skill_binding,
};
pub use crate::task_events::{AppendedTaskEvent, TaskEventAppendStatus, TaskEventPayload};
use crate::task_projector::TaskProjector;
pub use crate::timeline_projection::{
    ProjectionPlacement, ProjectionVisibility, TurnItemProjectionClassification,
    WORK_ITEM_STATUS_CANCELLED, WORK_ITEM_STATUS_COMPLETED, WORK_ITEM_STATUS_FAILED,
    WORK_ITEM_STATUS_RUNNING, WorkItemClassification, classify_turn_item_row,
    classify_turn_item_with_db_status,
};
pub use crate::timeline_projection_model::{
    approval_block_id, assistant_block_id, user_block_id, work_block_id, work_item_projection_id,
};
use crate::turn_item_terminal::{
    TurnItemTerminalState, terminalize_turn_item_payload, tool_call_status,
};

pub use crate::memory::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryCandidateRecord,
    AgentMemoryCandidateStatusUpdateRecord, AgentMemoryCapsuleRecord, AgentMemoryControlRecord,
    AgentMemoryEventRecord, AgentMemoryListFilter, AgentMemoryPolicyDecisionRecord,
    AgentMemoryQualityDecisionRecord, AgentMemoryQuarantineRecord, AgentMemoryRepairJobRecord,
    MemoryActorRecord, MemoryLifecycleActorRecord, MemoryScopeResolution, MemoryWorkspaceGuard,
    NewAgentMemoryCandidate, NewAgentMemoryControlRecord, NewAgentMemoryEvent,
    NewAgentMemoryPolicyDecision, NewAgentMemoryQualityDecision, NewAgentMemoryQuarantine,
    NewAgentMemoryRepairJob, ResolveAgentMemoryQuarantine, global_agent_memory_scope_key,
    memory_scope_key_hash, workspace_agent_memory_scope_key,
};
pub use crate::repositories::hook_run::{
    HOOK_RUN_CONTRIBUTION_HASH_MAX_COUNT, HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS,
    HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT, HOOK_RUN_ERROR_MESSAGE_MAX_CHARS,
    HOOK_RUN_IDEMPOTENCY_KEY_MAX_CHARS, HookAuditEventRecord, HookRunAttemptCompletionRecord,
    HookRunAttemptRecord, HookRunCompletionRecord, HookRunRecord, HookRunScope, HookRunScopeKind,
    NewHookAuditEventRecord, NewHookRunAttemptRecord, NewHookRunRecord, RecoverableHookRunRecord,
};
pub use crate::repositories::turn_execution_window::{
    NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord,
    TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES, TurnExecutionCheckpointKind,
    TurnExecutionCheckpointRecord, TurnExecutionDataCleanupRecord, TurnExecutionWindowRecord,
    TurnExecutionWindowStatsRecord, TurnExecutionWindowTerminalItemCountsRecord,
    TurnExecutionWindowUsageAggregateRecord,
};
pub use crate::repositories::turn_llm_context::{NewTurnLlmContextEntry, TurnLlmContextEntry};
pub use crate::repositories::turn_runtime_snapshot::{
    NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord,
};
pub use crate::thread_episodic::{
    NewThreadEpisodicCapsuleRecord, NewThreadEpisodicChunkRecord, NewThreadEpisodicExclusionRecord,
    NewThreadEpisodicIndexJobRecord, NewThreadEpisodicRecallEventRecord,
    NewThreadEpisodicThreadDirectoryRecord, ThreadEpisodicActiveWriteSegmentRequest,
    ThreadEpisodicCapsuleCapacityUpdate, ThreadEpisodicCapsuleRecord, ThreadEpisodicCapsuleStatus,
    ThreadEpisodicCapsuleWriteState, ThreadEpisodicChunkIndexedUpdate, ThreadEpisodicChunkRecord,
    ThreadEpisodicChunkStatus, ThreadEpisodicChunkVisibility, ThreadEpisodicExclusionReason,
    ThreadEpisodicExclusionRecord, ThreadEpisodicGraphEnrichmentState,
    ThreadEpisodicIndexJobCompletionUpdate, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobRecord, ThreadEpisodicIndexJobStatus, ThreadEpisodicRecallEventRecord,
    ThreadEpisodicRepairStatus, ThreadEpisodicSourceActorRole, ThreadEpisodicSourceRuntimeKind,
    ThreadEpisodicThreadDirectoryRecord, ThreadEpisodicThreadDirectorySelection,
    ThreadEpisodicThreadDirectoryStatus, ThreadEpisodicThreadDirectoryVisibility,
    deterministic_thread_episodic_capsule_id, thread_episodic_capsule_ref,
    thread_episodic_capsule_storage_uri, thread_episodic_frame_uri, thread_episodic_key_hash,
};
use crate::util::{optional_typed_json_from_db, typed_json_from_db, unix_to_datetime};
use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskRunChildAnchor {
    pub child_thread_id: Option<String>,
    pub child_turn_id: Option<String>,
}

fn memory_candidate_status_event_kind(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Approved => MEMORY_EVENT_CANDIDATE_APPROVED,
        MemoryCandidateStatus::Rejected
        | MemoryCandidateStatus::AutoRejected
        | MemoryCandidateStatus::ReviewDisabledRejected => MEMORY_EVENT_CANDIDATE_REJECTED,
        MemoryCandidateStatus::Expired => MEMORY_EVENT_CANDIDATE_EXPIRED,
        MemoryCandidateStatus::Pending
        | MemoryCandidateStatus::PendingSilent
        | MemoryCandidateStatus::AskOnUse
        | MemoryCandidateStatus::NeedsReview => MEMORY_EVENT_CANDIDATE_CREATED,
        MemoryCandidateStatus::Superseded => "candidate_superseded",
        MemoryCandidateStatus::MergedDuplicate => "candidate_merged_duplicate",
    }
}

async fn reserve_execution_for_run_in_connection<C: ConnectionTrait>(
    db: &C,
    run_id: String,
    executor_kind: TaskExecutorKind,
    now: i64,
) -> Result<TaskRunExecution> {
    if let Some(existing) = task_run_execution::find_execution_by_run(db, run_id.as_str()).await? {
        let execution = task_run_execution_from_db_model(existing)?;
        if execution.executor_kind != executor_kind {
            anyhow::bail!(
                "task run execution `{}` already exists for executor kind `{:?}`, requested `{:?}`",
                execution.id,
                execution.executor_kind,
                executor_kind
            );
        }
        return Ok(execution);
    }

    let Some(run_model) = task_run::find_run_by_id(db, run_id.as_str()).await? else {
        anyhow::bail!("task run `{run_id}` not found for execution reservation");
    };
    let run = task_run_from_db_model(run_model)?;
    if run.executor_kind != executor_kind {
        anyhow::bail!(
            "task run `{}` has executor kind `{:?}`, requested `{:?}`",
            run.id,
            run.executor_kind,
            executor_kind
        );
    }

    task_run_execution::insert_execution_if_absent(
        db,
        task_run_execution::NewTaskRunExecution {
            id: generate_id(DB_ID_LEN),
            task_id: run.task_id.clone(),
            task_run_id: run.id.clone(),
            executor_kind,
            status: TaskRunExecutionStatus::Reserved,
            worker_id: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await?;

    let execution = task_run_execution::find_execution_by_run(db, run.id.as_str())
        .await?
        .context("task run execution missing after reservation")?;
    task_run_execution_from_db_model(execution)
}

/// A single turn's conversation content: user input + assistant reply.
#[derive(Debug, Clone)]
pub struct ConversationEntry {
    pub turn_id: String,
    pub user_text: Option<String>,
    pub assistant_text: Option<String>,
    pub user_artifacts: Vec<ConversationArtifactRef>,
    pub assistant_artifacts: Vec<ConversationArtifactRef>,
}

#[derive(Debug, Clone)]
pub struct ThreadHistorySnapshot {
    pub workspace_id: String,
    pub events: Vec<ThreadHistoryEvent>,
}

#[derive(Debug, Clone)]
pub struct TimeoutCandidate {
    pub attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: i64,
    pub timeout_reason: TurnItemTimeoutReason,
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
pub struct TurnItemAttemptDeadlines {
    pub lease_expires_at_unix: Option<i64>,
    pub idle_deadline_at_unix: Option<i64>,
    pub hard_deadline_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnProjectionReplaySummary {
    pub claimed: usize,
    pub projected: usize,
    pub failed: usize,
    pub exhausted: usize,
    pub missing_events: usize,
    pub exhausted_records: Vec<TurnProjectionReplayExhaustedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnProjectionReplayExhaustedRecord {
    pub event_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub error_message: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
struct TurnEventProjectionContext {
    #[serde(default)]
    item_started_deadlines: Option<TurnItemAttemptDeadlines>,
}

#[derive(Debug, Clone)]
pub struct RunningAttemptDeadlineRepairCandidate {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub started_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct RunningTurnItemAttempt {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadModelInvariantKind {
    TerminalToolPayloadInProgress,
    TimedOutToolPayloadInProgress,
    TerminalTurnHasRunningAttempts,
    TerminalTaskMissingCompletedAt,
    TerminalRunMissingCompletedAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadModelInvariantViolation {
    pub kind: ReadModelInvariantKind,
    pub entity_id: String,
    pub details: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairSummary {
    pub detected: usize,
    pub repaired: usize,
    pub remaining: usize,
}

#[derive(Debug, Clone)]
pub struct RecoveryJobRecord {
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub source_attempt_id: Option<String>,
    pub status: RecoveryJobStatus,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub reason: Option<String>,
    pub error_class: Option<ProviderFailureClass>,
    pub transport_stage: Option<ProviderFailureStage>,
    pub retry_after_ms: Option<i64>,
    pub provider_attempt_number: i64,
    pub policy_json: serde_json::Value,
    pub policy_snapshot: serde_json::Value,
    pub last_error: Option<String>,
    pub run_count: i64,
    pub max_attempts: i64,
    pub scheduled_at_unix: i64,
    pub updated_at_unix: i64,
    pub claim_token: Option<String>,
    pub active_attempt_id: Option<String>,
    pub active_attempt_started_at_unix: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum BlockedTurnRecoveryResumeOutcome {
    Resumed(RecoveryJobRecord),
    NotFound,
    MissingRuntimeSnapshot { recovery_job_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedRecoveryActivation {
    Activated,
    BlockedByActiveRecovery,
    ClaimNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSkillBindingRecord {
    pub skill_slug: String,
    pub skill_version: Option<String>,
    pub fingerprint: String,
    pub source_kind: String,
    pub resolved_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSkillPolicyRecord {
    pub workspace_id: String,
    pub skill_slug: String,
    pub source_kind: String,
    pub enabled: Option<bool>,
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillInstallationRecord {
    pub slug: String,
    pub version: Option<String>,
    pub source_kind: String,
    pub scope_key: String,
    pub source_ref: String,
    pub install_path: String,
    pub trust_level: String,
    pub fingerprint: String,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillAuditEventRecord {
    pub turn_id: Option<String>,
    pub skill_slug: String,
    pub source_kind: String,
    pub action: String,
    pub decision: String,
    pub reason_code: Option<String>,
    pub details_json: String,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDependencySnapshotRecord {
    pub turn_id: Option<String>,
    pub skill_slug: String,
    pub source_kind: String,
    pub diagnostics_json: String,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillUploadSessionRecord {
    pub upload_id: String,
    pub workspace_id: String,
    pub connection_id: u64,
    pub status: String,
    pub file_name: String,
    pub archive_format: String,
    pub compressed_size_bytes: u64,
    pub received_bytes: u64,
    pub sha256: String,
    pub payload_path: String,
    pub created_at_unix: i64,
    pub expires_at_unix: i64,
    pub finalized_at_unix: Option<i64>,
    pub consumed_at_unix: Option<i64>,
    pub aborted_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerInstallationRecord {
    pub id: Option<String>,
    pub scope_kind: String,
    pub scope_key: String,
    pub name: String,
    pub display_name: Option<String>,
    pub source_kind: String,
    pub source_ref: String,
    pub transport_kind: String,
    pub transport_json: String,
    pub auth_json: String,
    pub secret_refs_json: String,
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
    pub required: bool,
    pub fingerprint: String,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerCatalogSnapshotRecord {
    pub server_installation_id: String,
    pub catalog_version: String,
    pub server_info_json: String,
    pub server_instructions_hash: Option<String>,
    pub tools_json: String,
    pub resources_json: String,
    pub resource_templates_json: String,
    pub prompts_json: String,
    pub generated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuditEventRecord {
    pub turn_id: Option<String>,
    pub server_installation_id: Option<String>,
    pub server_name: String,
    pub raw_tool_name: Option<String>,
    pub callable_name: Option<String>,
    pub catalog_version: Option<String>,
    pub action: String,
    pub decision: String,
    pub reason_code: Option<String>,
    pub details_json: String,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMcpBindingRecord {
    pub server_installation_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub callable_name: String,
    pub catalog_version: String,
    pub fingerprint: String,
    pub selection_reason: String,
    pub capability_id: Option<String>,
}

#[derive(Clone)]
pub struct CrudStore {
    connection: DatabaseConnection,
    projector: TurnProjector,
    task_projector: TaskProjector,
    write_coordinator: SqliteWriteCoordinator,
}

fn skill_upload_session_record_from_model(
    model: pioneer_entity::skill_upload_session::Model,
) -> SkillUploadSessionRecord {
    SkillUploadSessionRecord {
        upload_id: model.upload_id,
        workspace_id: model.workspace_id,
        connection_id: u64::try_from(model.connection_id).unwrap_or_default(),
        status: model.status,
        file_name: model.file_name,
        archive_format: model.archive_format,
        compressed_size_bytes: u64::try_from(model.compressed_size_bytes).unwrap_or_default(),
        received_bytes: u64::try_from(model.received_bytes).unwrap_or_default(),
        sha256: model.sha256,
        payload_path: model.payload_path,
        created_at_unix: model.created_at_unix,
        expires_at_unix: model.expires_at_unix,
        finalized_at_unix: model.finalized_at_unix,
        consumed_at_unix: model.consumed_at_unix,
        aborted_at_unix: model.aborted_at_unix,
    }
}

impl CrudStore {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            connection,
            projector: TurnProjector::new(),
            task_projector: TaskProjector::new(),
            write_coordinator: SqliteWriteCoordinator::default(),
        }
    }

    pub fn database_connection(&self) -> DatabaseConnection {
        self.connection.clone()
    }

    pub async fn insert_turn_llm_context(
        &self,
        entry: NewTurnLlmContextEntry,
    ) -> Result<pioneer_entity::turn_llm_context::Model> {
        turn_llm_context::insert_turn_llm_context(&self.connection, entry).await
    }

    pub async fn list_turn_llm_context(&self, turn_id: &str) -> Result<Vec<TurnLlmContextEntry>> {
        turn_llm_context::list_turn_llm_context(&self.connection, turn_id).await
    }

    pub async fn delete_turn_llm_context_for_turn(&self, turn_id: &str) -> Result<u64> {
        turn_llm_context::delete_turn_llm_context_for_turn(&self.connection, turn_id).await
    }

    pub async fn delete_expired_turn_llm_context(&self) -> Result<u64> {
        turn_llm_context::delete_expired_turn_llm_context(&self.connection).await
    }

    pub async fn delete_turn_llm_context_for_terminal_turns(&self) -> Result<u64> {
        turn_llm_context::delete_turn_llm_context_for_terminal_turns(&self.connection).await
    }

    pub async fn upsert_turn_runtime_snapshot(
        &self,
        snapshot: NewTurnRuntimeSnapshot,
    ) -> Result<TurnRuntimeSnapshotRecord> {
        self.run_serialized_write(|| async {
            turn_runtime_snapshot::upsert_turn_runtime_snapshot(&self.connection, snapshot.clone())
                .await
        })
        .await
    }

    pub async fn get_turn_runtime_snapshot(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnRuntimeSnapshotRecord>> {
        let turn_id = turn_id.to_owned();
        self.run_serialized_write(|| async {
            turn_runtime_snapshot::find_turn_runtime_snapshot(&self.connection, turn_id.as_str())
                .await
        })
        .await
    }

    pub async fn delete_turn_runtime_snapshot(&self, turn_id: &str) -> Result<u64> {
        let turn_id = turn_id.to_owned();
        self.run_serialized_write(|| async {
            turn_runtime_snapshot::delete_turn_runtime_snapshot(&self.connection, turn_id.as_str())
                .await
        })
        .await
    }

    pub async fn delete_turn_runtime_snapshots_for_closed_turns(&self) -> Result<u64> {
        self.run_serialized_write(|| async {
            turn_runtime_snapshot::delete_turn_runtime_snapshots_for_closed_turns(&self.connection)
                .await
        })
        .await
    }

    pub async fn upsert_cli_runtime_thread_binding(
        &self,
        binding: NewCliRuntimeThreadBinding,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.run_serialized_write(|| async {
            cli_runtime_binding::upsert_thread_binding(&self.connection, binding.clone()).await
        })
        .await
    }

    pub async fn get_cli_runtime_thread_binding(
        &self,
        thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        let thread_id = thread_id.to_owned();
        cli_runtime_binding::find_thread_binding(&self.connection, thread_id.as_str()).await
    }

    pub async fn get_cli_runtime_thread_binding_by_native_thread(
        &self,
        runtime_id: &str,
        native_thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        let runtime_id = runtime_id.to_owned();
        let native_thread_id = native_thread_id.to_owned();
        cli_runtime_binding::find_thread_binding_by_native_thread(
            &self.connection,
            runtime_id.as_str(),
            native_thread_id.as_str(),
        )
        .await
    }

    pub async fn list_cli_runtime_thread_bindings_for_runtime(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<CliRuntimeThreadBindingRecord>> {
        let workspace_id = workspace_id.to_owned();
        let runtime_id = runtime_id.to_owned();
        cli_runtime_binding::list_thread_bindings_for_runtime(
            &self.connection,
            workspace_id.as_str(),
            runtime_id.as_str(),
        )
        .await
    }

    pub async fn upsert_cli_runtime_turn_binding(
        &self,
        binding: NewCliRuntimeTurnBinding,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        self.run_serialized_write(|| async {
            cli_runtime_binding::upsert_turn_binding(&self.connection, binding.clone()).await
        })
        .await
    }

    pub async fn get_cli_runtime_turn_binding(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        let turn_id = turn_id.to_owned();
        cli_runtime_binding::find_turn_binding(&self.connection, turn_id.as_str()).await
    }

    pub async fn get_cli_runtime_turn_binding_by_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        let request_id = request_id.to_owned();
        cli_runtime_binding::find_turn_binding_by_request(&self.connection, request_id.as_str())
            .await
    }

    pub async fn get_cli_runtime_turn_binding_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        let runtime_id = runtime_id.to_owned();
        let native_turn_id = native_turn_id.to_owned();
        cli_runtime_binding::find_turn_binding_by_native_turn(
            &self.connection,
            runtime_id.as_str(),
            native_turn_id.as_str(),
        )
        .await
    }

    pub async fn list_cli_runtime_turn_bindings_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        let thread_id = thread_id.to_owned();
        cli_runtime_binding::list_turn_bindings_for_thread(&self.connection, thread_id.as_str())
            .await
    }

    pub async fn list_cli_runtime_turn_bindings(
        &self,
        filter: CliRuntimeTurnBindingListFilter,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        cli_runtime_binding::list_turn_bindings(&self.connection, filter).await
    }

    pub async fn create_cli_runtime_pending_request(
        &self,
        request: NewCliRuntimePendingRequest,
    ) -> Result<CliRuntimePendingRequestRecord> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI runtime pending request create transaction")?;
            let record =
                cli_runtime_binding::create_pending_request(&transaction, request.clone()).await?;
            if let Err(error) =
                crate::timeline_live_projection::project_cli_runtime_pending_request(
                    &transaction,
                    &record,
                )
                .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit CLI runtime pending request create transaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn open_cli_runtime_pending_request(
        &self,
        request: NewCliRuntimePendingRequest,
    ) -> Result<CliRuntimePendingRequestRecord> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI runtime pending request open transaction")?;
            let record =
                cli_runtime_binding::open_pending_request(&transaction, request.clone()).await?;
            if let Err(error) =
                crate::timeline_live_projection::project_cli_runtime_pending_request(
                    &transaction,
                    &record,
                )
                .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit CLI runtime pending request open transaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn get_cli_runtime_pending_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let request_id = request_id.to_owned();
        cli_runtime_binding::find_pending_request(&self.connection, request_id.as_str()).await
    }

    pub async fn list_cli_runtime_pending_requests(
        &self,
        filter: CliRuntimePendingRequestListFilter,
    ) -> Result<Vec<CliRuntimePendingRequestRecord>> {
        cli_runtime_binding::list_pending_requests(&self.connection, filter).await
    }

    pub async fn resolve_cli_runtime_pending_request(
        &self,
        resolution: ResolveCliRuntimePendingRequest,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI runtime pending request resolve transaction")?;
            let record =
                cli_runtime_binding::resolve_pending_request(&transaction, resolution.clone())
                    .await?;
            if let Some(record) = &record
                && let Err(error) =
                    crate::timeline_live_projection::project_cli_runtime_pending_request(
                        &transaction,
                        record,
                    )
                    .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit CLI runtime pending request resolve transaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn cancel_cli_runtime_pending_request(
        &self,
        request_id: &str,
        response_json: Option<String>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let request_id = request_id.to_owned();
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI runtime pending request cancel transaction")?;
            let record = cli_runtime_binding::cancel_pending_request(
                &transaction,
                request_id.clone(),
                response_json.clone(),
                updated_at,
            )
            .await?;
            if let Some(record) = &record
                && let Err(error) =
                    crate::timeline_live_projection::project_cli_runtime_pending_request(
                        &transaction,
                        record,
                    )
                    .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit CLI runtime pending request cancel transaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn expire_cli_runtime_pending_request(
        &self,
        request_id: &str,
        response_json: Option<String>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let request_id = request_id.to_owned();
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI runtime pending request expire transaction")?;
            let record = cli_runtime_binding::expire_pending_request(
                &transaction,
                request_id.clone(),
                response_json.clone(),
                updated_at,
            )
            .await?;
            if let Some(record) = &record
                && let Err(error) =
                    crate::timeline_live_projection::project_cli_runtime_pending_request(
                        &transaction,
                        record,
                    )
                    .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit CLI runtime pending request expire transaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn append_cli_runtime_native_event(
        &self,
        event: NewCliRuntimeNativeEvent,
    ) -> Result<CliRuntimeNativeEventRecord> {
        self.run_serialized_write(|| async {
            cli_runtime_binding::append_native_event(&self.connection, event.clone()).await
        })
        .await
    }

    pub async fn list_cli_runtime_native_events(
        &self,
        filter: CliRuntimeNativeEventListFilter,
    ) -> Result<Vec<CliRuntimeNativeEventRecord>> {
        cli_runtime_binding::list_native_events(&self.connection, filter).await
    }

    pub async fn create_turn_execution_window(
        &self,
        record: NewTurnExecutionWindowRecord,
        created_at: DateTimeWithTimeZone,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::create_turn_execution_window(
            &self.connection,
            record,
            created_at,
            updated_at,
        )
        .await
    }

    pub async fn get_turn_execution_window(
        &self,
        window_id: &str,
    ) -> Result<Option<TurnExecutionWindowRecord>> {
        turn_execution_window::get_turn_execution_window(&self.connection, window_id).await
    }

    pub async fn list_turn_execution_windows(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnExecutionWindowRecord>> {
        turn_execution_window::list_turn_execution_windows(&self.connection, turn_id).await
    }

    pub async fn latest_turn_execution_window(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionWindowRecord>> {
        turn_execution_window::latest_turn_execution_window(&self.connection, turn_id).await
    }

    pub async fn aggregate_turn_execution_window_usage(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionWindowUsageAggregateRecord> {
        turn_execution_window::aggregate_turn_execution_window_usage(&self.connection, turn_id)
            .await
    }

    pub async fn mark_turn_execution_window_exhausted(
        &self,
        window_id: &str,
        reason: pioneer_protocol::ExecutionWindowExhaustionReason,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_exhausted(
            &self.connection,
            window_id,
            reason,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_checkpointed(
        &self,
        window_id: &str,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_checkpointed(
            &self.connection,
            window_id,
            updated_at,
        )
        .await
    }

    pub async fn mark_turn_execution_window_continued(
        &self,
        window_id: &str,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_continued(
            &self.connection,
            window_id,
            updated_at,
        )
        .await
    }

    pub async fn mark_turn_execution_window_completed(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_completed(
            &self.connection,
            window_id,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_failed(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_failed(&self.connection, window_id, stats)
            .await
    }

    pub async fn mark_turn_execution_window_interrupted(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_interrupted(
            &self.connection,
            window_id,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_blocked(
        &self,
        window_id: &str,
        reason: Option<pioneer_protocol::ExecutionWindowExhaustionReason>,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_blocked(
            &self.connection,
            window_id,
            reason,
            stats,
        )
        .await
    }

    pub async fn close_active_execution_windows_for_terminal_turns(
        &self,
        now: DateTimeWithTimeZone,
    ) -> Result<u64> {
        self.run_serialized_write(|| async {
            turn_execution_window::close_active_execution_windows_for_terminal_turns(
                &self.connection,
                now,
            )
            .await
        })
        .await
    }

    pub async fn count_turn_execution_window_terminal_items(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
        turn_execution_window::count_turn_execution_window_terminal_items(&self.connection, turn_id)
            .await
    }

    pub async fn save_turn_execution_checkpoint(
        &self,
        record: NewTurnExecutionCheckpointRecord,
    ) -> Result<TurnExecutionCheckpointRecord> {
        turn_execution_window::save_turn_execution_checkpoint(&self.connection, record).await
    }

    pub async fn get_turn_execution_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<TurnExecutionCheckpointRecord>> {
        turn_execution_window::get_turn_execution_checkpoint(&self.connection, checkpoint_id).await
    }

    pub async fn list_turn_execution_checkpoints_for_window(
        &self,
        window_id: &str,
    ) -> Result<Vec<TurnExecutionCheckpointRecord>> {
        turn_execution_window::list_turn_execution_checkpoints_for_window(
            &self.connection,
            window_id,
        )
        .await
    }

    pub async fn latest_turn_execution_checkpoint_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionCheckpointRecord>> {
        turn_execution_window::latest_turn_execution_checkpoint_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_checkpoints_for_turn(&self, turn_id: &str) -> Result<u64> {
        turn_execution_window::delete_turn_execution_checkpoints_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_checkpoints_for_window(
        &self,
        window_id: &str,
    ) -> Result<u64> {
        turn_execution_window::delete_turn_execution_checkpoints_for_window(
            &self.connection,
            window_id,
        )
        .await
    }

    pub async fn delete_turn_execution_windows_for_turn(&self, turn_id: &str) -> Result<u64> {
        turn_execution_window::delete_turn_execution_windows_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_data_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionDataCleanupRecord> {
        turn_execution_window::delete_turn_execution_data_for_turn(&self.connection, turn_id).await
    }

    pub async fn ingest_artifact_metadata(
        &self,
        blob: NewArtifactBlobRecord,
        artifact: IngestArtifactMetadataRecord,
        binding: Option<ArtifactBindingTargetRecord>,
        version_metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<IngestedArtifactRecord> {
        self.run_serialized_write(|| {
            let blob = blob.clone();
            let artifact = artifact.clone();
            let binding = binding.clone();
            let version_metadata = version_metadata.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin artifact ingest transaction")?;
                let repository = artifact_repository::ArtifactRepository::new();
                let result = async {
                    let blob = repository.find_or_create_blob(&transaction, blob).await?;
                    let artifact = repository.create_artifact(&transaction, &artifact).await?;
                    let version = repository
                        .create_version(
                            &transaction,
                            &artifact,
                            &blob,
                            binding.as_ref(),
                            &version_metadata,
                        )
                        .await?;
                    let artifact = repository
                        .update_current_version(&transaction, artifact, &version.id)
                        .await?;
                    if let Some(binding) = &binding {
                        repository
                            .create_binding(
                                &transaction,
                                &artifact.workspace_id,
                                &artifact.id,
                                Some(&version.id),
                                binding,
                                &BTreeMap::new(),
                            )
                            .await?;
                    }
                    Ok::<_, ArtifactCrudError>(IngestedArtifactRecord {
                        artifact,
                        version,
                        blob,
                    })
                }
                .await;

                let record = match result {
                    Ok(record) => record,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error.into());
                    }
                };

                transaction
                    .commit()
                    .await
                    .context("failed to commit artifact ingest transaction")?;
                Ok(record)
            }
        })
        .await
    }

    pub async fn find_or_create_artifact_blob(
        &self,
        blob: NewArtifactBlobRecord,
    ) -> Result<ArtifactBlobRecord> {
        self.run_serialized_write(|| {
            let blob = blob.clone();
            async move {
                artifact_repository::ArtifactRepository::new()
                    .find_or_create_blob(&self.connection, blob)
                    .await
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn bind_artifact(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
        target: ArtifactBindingTargetRecord,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactBindingSummary> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            let artifact_id = artifact_id.to_owned();
            let version_id = version_id.map(ToOwned::to_owned);
            let target = target.clone();
            let metadata = metadata.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin artifact bind transaction")?;
                let repository = artifact_repository::ArtifactRepository::new();
                let result = async {
                    let summary = repository
                        .get_artifact_summary(
                            &transaction,
                            &workspace_id,
                            &artifact_id,
                            version_id.as_deref(),
                        )
                        .await?;
                    let resolved_version_id = version_id
                        .as_deref()
                        .or(summary.artifact.version_id.as_deref())
                        .map(ToOwned::to_owned);
                    repository
                        .create_binding(
                            &transaction,
                            &workspace_id,
                            &artifact_id,
                            resolved_version_id.as_deref(),
                            &target,
                            &metadata,
                        )
                        .await
                }
                .await;

                let binding = match result {
                    Ok(binding) => binding,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error.into());
                    }
                };

                transaction
                    .commit()
                    .await
                    .context("failed to commit artifact bind transaction")?;
                Ok(binding)
            }
        })
        .await
    }

    pub async fn get_artifact_summary(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> Result<ArtifactSummary> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_summary(&self.connection, workspace_id, artifact_id, version_id)
            .await
            .map_err(Into::into)
    }

    pub async fn get_artifact_version_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> Result<ArtifactVersionBlobRecord> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_version_blob(&self.connection, workspace_id, artifact_id, version_id)
            .await
            .map_err(Into::into)
    }

    pub async fn get_artifact_projection_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
        projection_kind: ArtifactProjectionKind,
    ) -> Result<ArtifactProjectionBlobRecord> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_projection_blob(
                &self.connection,
                workspace_id,
                artifact_id,
                version_id,
                projection_kind,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn list_thread_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> Result<ArtifactListPageRecord> {
        artifact_repository::ArtifactRepository::new()
            .list_thread_artifacts(&self.connection, workspace_id, thread_id, filter)
            .await
            .map_err(Into::into)
    }

    pub async fn list_artifacts(
        &self,
        workspace_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> Result<ArtifactListPageRecord> {
        artifact_repository::ArtifactRepository::new()
            .list_artifacts(&self.connection, workspace_id, filter)
            .await
            .map_err(Into::into)
    }

    pub async fn list_conversation_artifact_refs(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_ids: &[String],
        limits: ConversationArtifactRefLimits,
    ) -> Result<BTreeMap<String, ConversationTurnArtifactRefs>> {
        artifact_repository::ArtifactRepository::new()
            .list_conversation_artifact_refs(
                &self.connection,
                workspace_id,
                thread_id,
                turn_ids,
                limits,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn update_artifact_status(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        status: ArtifactStatus,
        deleted_at: Option<DateTimeWithTimeZone>,
    ) -> Result<ArtifactRecord> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            let artifact_id = artifact_id.to_owned();
            let deleted_at = deleted_at.clone();
            async move {
                artifact_repository::ArtifactRepository::new()
                    .update_artifact_status(
                        &self.connection,
                        &workspace_id,
                        &artifact_id,
                        status,
                        deleted_at,
                    )
                    .await
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn replace_artifact_projection(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
        projection_kind: ArtifactProjectionKind,
        status: ArtifactProjectionStatus,
        text_content: Option<String>,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactProjectionRecord> {
        self.replace_artifact_projection_with_blob(
            workspace_id,
            artifact_id,
            artifact_version_id,
            projection_kind,
            status,
            text_content,
            None,
            metadata,
        )
        .await
    }

    pub async fn replace_artifact_projection_with_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
        projection_kind: ArtifactProjectionKind,
        status: ArtifactProjectionStatus,
        text_content: Option<String>,
        blob_id: Option<String>,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactProjectionRecord> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            let artifact_id = artifact_id.to_owned();
            let artifact_version_id = artifact_version_id.to_owned();
            let text_content = text_content.clone();
            let blob_id = blob_id.clone();
            let metadata = metadata.clone();
            async move {
                artifact_repository::replace_projection(
                    &self.connection,
                    &workspace_id,
                    &artifact_id,
                    &artifact_version_id,
                    projection_kind,
                    status,
                    text_content,
                    blob_id,
                    metadata,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn list_artifact_projections(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: Option<&str>,
    ) -> Result<Vec<ArtifactProjectionRecord>> {
        artifact_repository::list_projections(
            &self.connection,
            workspace_id,
            artifact_id,
            artifact_version_id,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn find_active_artifact_external_ref(
        &self,
        key: &ArtifactExternalRefKey,
        now_unix_ms: i64,
    ) -> Result<Option<ArtifactExternalRefRecord>> {
        artifact_repository::find_active_external_ref(&self.connection, key, now_unix_ms)
            .await
            .map_err(Into::into)
    }

    pub async fn upsert_artifact_external_ref(
        &self,
        request: UpsertArtifactExternalRefRequest,
    ) -> Result<ArtifactExternalRefRecord> {
        self.run_serialized_write(|| {
            let request = request.clone();
            async move {
                artifact_repository::upsert_external_ref(&self.connection, request)
                    .await
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn prune_expired_artifact_external_refs(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
    ) -> Result<u64> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                artifact_repository::prune_expired_external_refs(
                    &self.connection,
                    &workspace_id,
                    now_unix_ms,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn artifact_workspace_usage(
        &self,
        workspace_id: &str,
    ) -> Result<ArtifactWorkspaceUsageRecord> {
        artifact_repository::workspace_usage(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn plan_artifact_gc(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
        grace_secs: u64,
    ) -> Result<ArtifactGcPlanRecord> {
        artifact_repository::plan_gc_with_grace(
            &self.connection,
            workspace_id,
            now_unix_ms,
            grace_secs,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn delete_artifact_blob_row(&self, workspace_id: &str, blob_id: &str) -> Result<u64> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            let blob_id = blob_id.to_owned();
            async move {
                artifact_repository::delete_blob_row(&self.connection, &workspace_id, &blob_id)
                    .await
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn delete_artifact_projection_row(
        &self,
        workspace_id: &str,
        projection_id: &str,
    ) -> Result<u64> {
        self.run_serialized_write(|| {
            let workspace_id = workspace_id.to_owned();
            let projection_id = projection_id.to_owned();
            async move {
                artifact_repository::delete_projection_row(
                    &self.connection,
                    &workspace_id,
                    &projection_id,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn count_artifacts_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_artifacts_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_blobs_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_blobs_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_versions_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_versions_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_bindings_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_bindings_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn insert_test_artifact_blob(
        &self,
        record: NewArtifactBlobRecord,
        created_at_unix_ms: i64,
        id: String,
    ) -> Result<ArtifactBlobRecord> {
        self.run_serialized_write(|| {
            let record = record.clone();
            let id = id.clone();
            async move {
                artifact_repository::insert_test_blob(
                    &self.connection,
                    record,
                    created_at_unix_ms,
                    id,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn update_test_artifact_status(&self, artifact_id: &str, status: &str) -> Result<()> {
        self.run_serialized_write(|| {
            let artifact_id = artifact_id.to_owned();
            let status = status.to_owned();
            async move {
                artifact_repository::update_test_artifact_status(
                    &self.connection,
                    &artifact_id,
                    &status,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn create_hook_run(
        &self,
        run: NewHookRunRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunRecord> {
        self.run_serialized_write(|| {
            let run = run.clone();
            let now = now.clone();
            async move { hook_run::create_hook_run(&self.connection, run, now).await }
        })
        .await
    }

    pub async fn find_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Option<HookRunRecord>> {
        hook_run::find_hook_run_by_id(&self.connection, run_id).await
    }

    pub async fn find_hook_run_by_idempotency_key(
        &self,
        idempotency_key: &pioneer_hooks::HookRunIdempotencyKey,
    ) -> Result<Option<HookRunRecord>> {
        hook_run::find_hook_run_by_idempotency_key(&self.connection, idempotency_key).await
    }

    pub async fn list_hook_runs_for_turn(
        &self,
        turn_id: &str,
        phase: Option<pioneer_hooks::HookPhase>,
        limit: u64,
    ) -> Result<Vec<HookRunRecord>> {
        hook_run::list_hook_runs_for_turn(&self.connection, turn_id, phase, limit).await
    }

    pub async fn mark_hook_run_running(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        self.run_serialized_write(|| {
            let now = now.clone();
            async move { hook_run::mark_hook_run_running(&self.connection, run_id, now).await }
        })
        .await
    }

    pub async fn complete_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        self.run_serialized_write(|| {
            let completion = completion.clone();
            let now = now.clone();
            async move { hook_run::complete_hook_run(&self.connection, run_id, completion, now).await }
        })
        .await
    }

    pub async fn append_hook_run_attempt(
        &self,
        attempt: NewHookRunAttemptRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunAttemptRecord> {
        self.run_serialized_write(|| {
            let attempt = attempt.clone();
            let now = now.clone();
            async move { hook_run::append_hook_run_attempt(&self.connection, attempt, now).await }
        })
        .await
    }

    pub async fn complete_hook_run_attempt(
        &self,
        attempt_id: &pioneer_hooks::HookRunAttemptId,
        completion: HookRunAttemptCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunAttemptRecord>> {
        self.run_serialized_write(|| {
            let completion = completion.clone();
            let now = now.clone();
            async move {
                hook_run::complete_hook_run_attempt(&self.connection, attempt_id, completion, now)
                    .await
            }
        })
        .await
    }

    pub async fn list_hook_run_attempts(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookRunAttemptRecord>> {
        hook_run::list_hook_run_attempts(&self.connection, run_id).await
    }

    pub async fn list_recoverable_hook_runs(
        &self,
        scan: pioneer_hooks::HookRecoveryScan,
    ) -> Result<Vec<RecoverableHookRunRecord>> {
        hook_run::list_recoverable_hook_runs(&self.connection, scan).await
    }

    pub async fn schedule_hook_run_retry(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        schedule: pioneer_hooks::HookRetrySchedule,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        self.run_serialized_write(|| {
            let schedule = schedule.clone();
            let now = now.clone();
            async move {
                hook_run::schedule_hook_run_retry(&self.connection, run_id, schedule, now).await
            }
        })
        .await
    }

    pub async fn mark_stale_hook_run_timed_out(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        self.run_serialized_write(|| {
            let completion = completion.clone();
            let now = now.clone();
            async move {
                hook_run::mark_stale_hook_run_timed_out(&self.connection, run_id, completion, now)
                    .await
            }
        })
        .await
    }

    pub async fn mark_hook_run_unrecoverable(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        self.run_serialized_write(|| {
            let completion = completion.clone();
            let now = now.clone();
            async move {
                hook_run::mark_hook_run_unrecoverable(&self.connection, run_id, completion, now)
                    .await
            }
        })
        .await
    }

    pub async fn append_hook_audit_events(
        &self,
        records: Vec<NewHookAuditEventRecord>,
        now: DateTimeWithTimeZone,
    ) -> Result<Vec<HookAuditEventRecord>> {
        self.run_serialized_write(|| {
            let records = records.clone();
            let now = now.clone();
            async move { hook_run::append_hook_audit_events(&self.connection, records, now).await }
        })
        .await
    }

    pub async fn list_hook_audit_events_for_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookAuditEventRecord>> {
        hook_run::list_hook_audit_events_for_run(&self.connection, run_id).await
    }

    pub async fn resolve_memory_scope(&self, scope: MemoryScope) -> Result<MemoryScopeResolution> {
        let key = crate::memory::normalized_scope_key(scope.key.as_str())?;
        let normalized_scope = MemoryScope {
            kind: scope.kind,
            key: key.clone(),
        };
        let scope_key_hash = memory_scope_key_hash(scope.kind, key.as_str())?;
        let workspace_id = match scope.kind {
            MemoryScopeKind::User => None,
            MemoryScopeKind::Workspace => Some(key),
            MemoryScopeKind::Thread => {
                let thread = pioneer_entity::thread::Entity::find_by_id(key.clone())
                    .one(&self.connection)
                    .await
                    .with_context(|| format!("failed to resolve thread memory scope `{key}`"))?
                    .with_context(|| format!("thread memory scope `{key}` does not exist"))?;
                Some(thread.workspace_id)
            }
            MemoryScopeKind::Task => {
                let task = pioneer_entity::task::Entity::find_by_id(key.clone())
                    .one(&self.connection)
                    .await
                    .with_context(|| format!("failed to resolve task memory scope `{key}`"))?
                    .with_context(|| format!("task memory scope `{key}` does not exist"))?;
                Some(task.workspace_id)
            }
            MemoryScopeKind::Agent => {
                if let Some(workspace_id) = crate::memory::parse_workspace_agent_scope_key(&key) {
                    Some(workspace_id)
                } else if crate::memory::is_global_agent_scope_key(&key) {
                    None
                } else {
                    anyhow::bail!(
                        "agent memory scope `{key}` must be `workspace:{{workspace_id}}:agent:{{agent_id}}` or `global:agent:{{agent_id}}`"
                    );
                }
            }
        };

        Ok(MemoryScopeResolution {
            scope: normalized_scope,
            scope_key_hash,
            workspace_id,
        })
    }

    pub async fn resolve_memory_scopes(
        &self,
        scopes: impl IntoIterator<Item = MemoryScope>,
    ) -> Result<Vec<MemoryScopeResolution>> {
        let mut resolved = Vec::new();
        for scope in scopes {
            resolved.push(self.resolve_memory_scope(scope).await?);
        }
        Ok(resolved)
    }

    pub async fn insert_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        self.run_serialized_write(|| {
            let record = record.clone();
            let event = event.clone();
            async move {
                let resolved = self.resolve_memory_scope(record.scope.clone()).await?;
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory insert transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let row =
                    agent_memory::insert_memory_record(&transaction, record, resolved, now).await?;
                let event = memory_event_for_record(
                    event,
                    row.id.clone(),
                    row.workspace_id.clone(),
                    MEMORY_EVENT_CREATED,
                    event_timestamp_secs,
                );
                agent_memory_event::append_memory_event(&transaction, event).await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory insert transaction")?;
                crate::memory::agent_memory_control_record_from_model(row)
            }
        })
        .await
    }

    pub async fn upsert_active_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        self.run_serialized_write(|| {
            let record = record.clone();
            let event = event.clone();
            async move {
                let resolved = self.resolve_memory_scope(record.scope.clone()).await?;
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory upsert transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let row =
                    agent_memory::upsert_active_memory_record(&transaction, record, resolved, now)
                        .await?;
                let default_event_kind = if row.created_at == now {
                    MEMORY_EVENT_CREATED
                } else {
                    MEMORY_EVENT_UPDATED
                };
                let event = memory_event_for_record(
                    event,
                    row.id.clone(),
                    row.workspace_id.clone(),
                    default_event_kind,
                    event_timestamp_secs,
                );
                agent_memory_event::append_memory_event(&transaction, event).await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory upsert transaction")?;
                crate::memory::agent_memory_control_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_agent_memory_record(
        &self,
        memory_id: &str,
        include_non_active: bool,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        Ok(
            agent_memory::find_memory_by_id(&self.connection, memory_id, include_non_active)
                .await?
                .map(crate::memory::agent_memory_control_record_from_model)
                .transpose()?,
        )
    }

    pub async fn get_active_agent_memory_by_key(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        key: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        let namespace = crate::memory::normalized_memory_namespace(namespace)?;
        let Some(row) = agent_memory::find_active_memory_by_scoped_key(
            &self.connection,
            &resolved,
            namespace.as_str(),
            key,
        )
        .await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_control_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn update_agent_memory_metadata(
        &self,
        memory_id: &str,
        metadata_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.run_serialized_write(|| {
            let metadata_json = metadata_json.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory metadata update transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let Some(row) = agent_memory::update_memory_metadata(
                    &transaction,
                    memory_id,
                    metadata_json,
                    now,
                )
                .await?
                else {
                    transaction.commit().await.context(
                        "failed to commit empty agent memory metadata update transaction",
                    )?;
                    return Ok(None);
                };
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: Some(row.id.clone()),
                        candidate_id: None,
                        workspace_id: row.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_UPDATED.to_owned(),
                        actor: None,
                        thread_id: row.source_thread_id.clone(),
                        turn_id: row.source_turn_id.clone(),
                        item_id: row.source_item_id.clone(),
                        details_json: Some(
                            serde_json::json!({ "reason": "semantic_evidence_merge" }).to_string(),
                        ),
                        created_at_unix: event_timestamp_secs,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory metadata update transaction")?;
                Ok(Some(crate::memory::agent_memory_control_record_from_model(
                    row,
                )?))
            }
        })
        .await
    }

    pub async fn list_agent_memory_records(
        &self,
        filter: AgentMemoryListFilter,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        let rows = agent_memory::list_memory_records(
            &self.connection,
            filter,
            resolved,
            chrono::Utc::now().fixed_offset(),
        )
        .await?;
        rows.into_iter()
            .map(crate::memory::agent_memory_control_record_from_model)
            .collect()
    }

    pub async fn mark_agent_memory_deleted(
        &self,
        memory_id: &str,
        actor: Option<MemoryActorRecord>,
        reason: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.run_serialized_write(|| {
            let actor = actor.clone();
            let reason = reason.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory delete transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let Some(row) = agent_memory::mark_memory_deleted(
                    &transaction,
                    memory_id,
                    actor.clone(),
                    reason.clone(),
                    now,
                )
                .await?
                else {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit empty agent memory delete transaction")?;
                    return Ok(None);
                };
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: Some(row.id.clone()),
                        candidate_id: None,
                        workspace_id: row.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_FORGOTTEN.to_owned(),
                        actor,
                        thread_id: row.source_thread_id.clone(),
                        turn_id: row.source_turn_id.clone(),
                        item_id: row.source_item_id.clone(),
                        details_json: reason
                            .map(|reason| serde_json::json!({ "reason": reason }).to_string()),
                        created_at_unix: event_timestamp_secs,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory delete transaction")?;
                Ok(Some(crate::memory::agent_memory_control_record_from_model(
                    row,
                )?))
            }
        })
        .await
    }

    pub async fn mark_agent_memory_superseded(
        &self,
        memory_id: &str,
        superseded_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent memory supersede transaction")?;
            let now = unix_to_datetime(event_timestamp_secs);
            let Some(row) =
                agent_memory::mark_memory_superseded(&transaction, memory_id, superseded_by, now)
                    .await?
            else {
                transaction
                    .commit()
                    .await
                    .context("failed to commit empty agent memory supersede transaction")?;
                return Ok(None);
            };
            agent_memory_event::append_memory_event(
                &transaction,
                NewAgentMemoryEvent {
                    memory_id: Some(row.id.clone()),
                    candidate_id: None,
                    workspace_id: row.workspace_id.clone(),
                    event_kind: MEMORY_EVENT_SUPERSEDED.to_owned(),
                    actor: None,
                    thread_id: row.source_thread_id.clone(),
                    turn_id: row.source_turn_id.clone(),
                    item_id: row.source_item_id.clone(),
                    details_json: Some(
                        serde_json::json!({ "superseded_by": superseded_by }).to_string(),
                    ),
                    created_at_unix: event_timestamp_secs,
                },
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit agent memory supersede transaction")?;
            Ok(Some(crate::memory::agent_memory_control_record_from_model(
                row,
            )?))
        })
        .await
    }

    pub async fn mark_agent_memory_expired(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent memory expire transaction")?;
            let now = unix_to_datetime(event_timestamp_secs);
            let Some(row) = agent_memory::mark_memory_expired(&transaction, memory_id, now).await?
            else {
                transaction
                    .commit()
                    .await
                    .context("failed to commit empty agent memory expire transaction")?;
                return Ok(None);
            };
            agent_memory_event::append_memory_event(
                &transaction,
                NewAgentMemoryEvent {
                    memory_id: Some(row.id.clone()),
                    candidate_id: None,
                    workspace_id: row.workspace_id.clone(),
                    event_kind: MEMORY_EVENT_EXPIRED.to_owned(),
                    actor: None,
                    thread_id: row.source_thread_id.clone(),
                    turn_id: row.source_turn_id.clone(),
                    item_id: row.source_item_id.clone(),
                    details_json: None,
                    created_at_unix: event_timestamp_secs,
                },
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit agent memory expire transaction")?;
            Ok(Some(crate::memory::agent_memory_control_record_from_model(
                row,
            )?))
        })
        .await
    }

    pub async fn mark_agent_memory_repair_status(
        &self,
        memory_id: &str,
        repair_status: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent memory repair status transaction")?;
            let row = agent_memory::mark_memory_repair_status(
                &transaction,
                memory_id,
                repair_status,
                unix_to_datetime(event_timestamp_secs),
            )
            .await?;
            let Some(row) = row else {
                transaction
                    .commit()
                    .await
                    .context("failed to commit empty agent memory repair status transaction")?;
                return Ok(None);
            };
            agent_memory_event::append_memory_event(
                &transaction,
                NewAgentMemoryEvent {
                    memory_id: Some(row.id.clone()),
                    candidate_id: None,
                    workspace_id: row.workspace_id.clone(),
                    event_kind: MEMORY_EVENT_REPAIR_STATUS_CHANGED.to_owned(),
                    actor: None,
                    thread_id: row.source_thread_id.clone(),
                    turn_id: row.source_turn_id.clone(),
                    item_id: row.source_item_id.clone(),
                    details_json: Some(
                        serde_json::json!({ "repair_status": repair_status }).to_string(),
                    ),
                    created_at_unix: event_timestamp_secs,
                },
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit agent memory repair status transaction")?;
            Ok(Some(crate::memory::agent_memory_control_record_from_model(
                row,
            )?))
        })
        .await
    }

    pub async fn record_agent_memory_access(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent memory access transaction")?;
            let now = unix_to_datetime(event_timestamp_secs);
            let updated =
                agent_memory::increment_memory_access(&transaction, memory_id, now).await?;
            if updated {
                let row = agent_memory::find_memory_by_id(&transaction, memory_id, true)
                    .await?
                    .context("accessed memory row missing after update")?;
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: Some(row.id),
                        candidate_id: None,
                        workspace_id: row.workspace_id,
                        event_kind: MEMORY_EVENT_ACCESSED.to_owned(),
                        actor: None,
                        thread_id: None,
                        turn_id: None,
                        item_id: None,
                        details_json: None,
                        created_at_unix: event_timestamp_secs,
                    },
                )
                .await?;
            }
            transaction
                .commit()
                .await
                .context("failed to commit agent memory access transaction")?;
            Ok(updated)
        })
        .await
    }

    pub async fn list_agent_memory_events(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_memory_events(&self.connection, memory_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn list_agent_memory_candidate_events(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_candidate_events(&self.connection, candidate_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn list_workspace_agent_memory_events(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_workspace_memory_events(&self.connection, workspace_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn create_agent_memory_quarantine_marker(
        &self,
        quarantine: NewAgentMemoryQuarantine,
    ) -> Result<AgentMemoryQuarantineRecord> {
        self.run_serialized_write(|| {
            let quarantine = quarantine.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory quarantine transaction")?;
                let row =
                    agent_memory_quarantine::create_active_quarantine(&transaction, quarantine)
                        .await?;
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: Some(row.memory_id.clone()),
                        candidate_id: None,
                        workspace_id: row.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_QUARANTINED.to_owned(),
                        actor: None,
                        thread_id: None,
                        turn_id: None,
                        item_id: None,
                        details_json: Some(
                            serde_json::json!({
                                "quarantine_id": row.id,
                                "reason_code": row.reason_code,
                                "actor": {
                                    "kind": row.actor_kind,
                                    "id": row.actor_id,
                                }
                            })
                            .to_string(),
                        ),
                        created_at_unix: row
                            .created_at
                            .as_ref()
                            .with_context(|| {
                                format!("quarantine marker `{}` is missing created_at", row.id)
                            })?
                            .timestamp(),
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory quarantine transaction")?;
                crate::memory::agent_memory_quarantine_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_active_agent_memory_quarantine(
        &self,
        memory_id: &str,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::find_active_quarantine_by_memory(&self.connection, memory_id)
            .await?
            .map(crate::memory::agent_memory_quarantine_record_from_model)
            .transpose()
    }

    pub async fn list_active_agent_memory_quarantines(
        &self,
        memory_ids: &[String],
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::list_active_quarantines_by_memory_ids(&self.connection, memory_ids)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_quarantine_record_from_model)
            .collect()
    }

    pub async fn resolve_agent_memory_quarantine(
        &self,
        resolution: ResolveAgentMemoryQuarantine,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        self.run_serialized_write(|| {
            let resolution = resolution.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory quarantine restore transaction")?;
                let row =
                    agent_memory_quarantine::resolve_active_quarantine(&transaction, resolution)
                        .await?;
                let Some(row) = row else {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit empty memory quarantine restore transaction")?;
                    return Ok(None);
                };
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: Some(row.memory_id.clone()),
                        candidate_id: None,
                        workspace_id: row.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_RESTORED.to_owned(),
                        actor: None,
                        thread_id: None,
                        turn_id: None,
                        item_id: None,
                        details_json: Some(
                            serde_json::json!({
                                "quarantine_id": row.id,
                                "reason_code": row.resolved_reason_code,
                                "actor": {
                                    "kind": row.resolved_actor_kind,
                                    "id": row.resolved_actor_id,
                                }
                            })
                            .to_string(),
                        ),
                        created_at_unix: row
                            .resolved_at
                            .map(|timestamp| timestamp.timestamp())
                            .unwrap_or_else(|| chrono::Utc::now().timestamp()),
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory quarantine restore transaction")?;
                Ok(Some(
                    crate::memory::agent_memory_quarantine_record_from_model(row)?,
                ))
            }
        })
        .await
    }

    pub async fn list_agent_memory_quarantine_history(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::list_quarantine_history_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quarantine_record_from_model)
        .collect()
    }

    pub async fn insert_agent_memory_candidate(
        &self,
        candidate: NewAgentMemoryCandidate,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryCandidateRecord> {
        self.run_serialized_write(|| {
            let candidate = candidate.clone();
            async move {
                let resolved = self.resolve_memory_scope(candidate.scope.clone()).await?;
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory candidate insert transaction")?;
                let row = agent_memory_candidate::insert_candidate(
                    &transaction,
                    candidate,
                    resolved,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?;
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: None,
                        candidate_id: Some(row.id.clone()),
                        workspace_id: row.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_CANDIDATE_CREATED.to_owned(),
                        actor: None,
                        thread_id: row.source_thread_id.clone(),
                        turn_id: row.source_turn_id.clone(),
                        item_id: row.source_item_id.clone(),
                        details_json: None,
                        created_at_unix: event_timestamp_secs,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory candidate insert transaction")?;
                crate::memory::agent_memory_candidate_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_agent_memory_candidate_by_dedupe(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        dedupe_key: &str,
        statuses: Vec<MemoryCandidateStatus>,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        let namespace = crate::memory::normalized_memory_namespace(namespace)?;
        let Some(row) = agent_memory_candidate::find_candidate_by_dedupe(
            &self.connection,
            &resolved,
            namespace.as_str(),
            dedupe_key,
            statuses.as_slice(),
        )
        .await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_candidate_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn get_agent_memory_candidate(
        &self,
        candidate_id: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let Some(row) =
            agent_memory_candidate::find_candidate_by_id(&self.connection, candidate_id).await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_candidate_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn update_agent_memory_candidate_metadata(
        &self,
        candidate_id: &str,
        reason: String,
        metadata_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.run_serialized_write(|| {
            let reason = reason.clone();
            let metadata_json = metadata_json.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory candidate metadata update transaction")?;
                let Some(row) = agent_memory_candidate::update_candidate_metadata(
                    &transaction,
                    candidate_id,
                    reason,
                    metadata_json,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?
                else {
                    transaction.commit().await.context(
                        "failed to commit empty memory candidate metadata update transaction",
                    )?;
                    return Ok(None);
                };
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory candidate metadata update transaction")?;
                Ok(Some(
                    crate::memory::agent_memory_candidate_record_from_model(row)?,
                ))
            }
        })
        .await
    }

    pub async fn update_agent_memory_candidate_status(
        &self,
        update: AgentMemoryCandidateStatusUpdateRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.run_serialized_write(|| {
            let update = update.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory candidate status update transaction")?;
                let Some(row) =
                    agent_memory_candidate::update_candidate_status(&transaction, update.clone())
                        .await?
                else {
                    transaction.commit().await.context(
                        "failed to commit empty memory candidate status update transaction",
                    )?;
                    return Ok(None);
                };
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: update.promoted_memory_id.clone(),
                        candidate_id: Some(row.id.clone()),
                        workspace_id: row.workspace_id.clone(),
                        event_kind: memory_candidate_status_event_kind(update.status).to_owned(),
                        actor: update.decided_by.clone(),
                        thread_id: row.source_thread_id.clone(),
                        turn_id: row.source_turn_id.clone(),
                        item_id: row.source_item_id.clone(),
                        details_json: update
                            .decision_reason
                            .clone()
                            .map(|reason| serde_json::json!({ "reason": reason }).to_string()),
                        created_at_unix: update.decided_at_unix,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory candidate status update transaction")?;
                Ok(Some(
                    crate::memory::agent_memory_candidate_record_from_model(row)?,
                ))
            }
        })
        .await
    }

    pub async fn list_agent_memory_candidates(
        &self,
        filter: AgentMemoryCandidateListFilter,
    ) -> Result<Vec<AgentMemoryCandidateRecord>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        agent_memory_candidate::list_candidates(&self.connection, filter, resolved)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_candidate_record_from_model)
            .collect()
    }

    pub async fn decide_agent_memory_candidate(
        &self,
        decision: AgentMemoryCandidateDecisionRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.run_serialized_write(|| {
            let decision = decision.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory candidate decision transaction")?;
                let Some(row) =
                    agent_memory_candidate::decide_candidate(&transaction, decision.clone())
                        .await?
                else {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit empty memory candidate decision transaction")?;
                    return Ok(None);
                };
                let event_kind = match decision.decision {
                    MemoryCandidateDecision::Approve => MEMORY_EVENT_CANDIDATE_APPROVED,
                    MemoryCandidateDecision::Reject => MEMORY_EVENT_CANDIDATE_REJECTED,
                    MemoryCandidateDecision::Expire => MEMORY_EVENT_CANDIDATE_EXPIRED,
                };
                agent_memory_event::append_memory_event(
                    &transaction,
                    NewAgentMemoryEvent {
                        memory_id: decision.promoted_memory_id.clone(),
                        candidate_id: Some(row.id.clone()),
                        workspace_id: row.workspace_id.clone(),
                        event_kind: event_kind.to_owned(),
                        actor: decision.decided_by.clone(),
                        thread_id: row.source_thread_id.clone(),
                        turn_id: row.source_turn_id.clone(),
                        item_id: row.source_item_id.clone(),
                        details_json: decision
                            .decision_reason
                            .clone()
                            .map(|reason| serde_json::json!({ "reason": reason }).to_string()),
                        created_at_unix: decision.decided_at_unix,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory candidate decision transaction")?;
                Ok(Some(
                    crate::memory::agent_memory_candidate_record_from_model(row)?,
                ))
            }
        })
        .await
    }

    pub async fn upsert_agent_memory_capsule(
        &self,
        capsule: AgentMemoryCapsuleRecord,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryCapsuleRecord> {
        self.run_serialized_write(|| {
            let capsule = capsule.clone();
            async move {
                let resolved = self.resolve_memory_scope(capsule.scope.clone()).await?;
                let row = agent_memory_capsule::upsert_capsule(
                    &self.connection,
                    capsule,
                    resolved,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?;
                crate::memory::agent_memory_capsule_record_from_model(row)
            }
        })
        .await
    }

    pub async fn find_primary_agent_memory_capsule(
        &self,
        scope: MemoryScope,
    ) -> Result<Option<AgentMemoryCapsuleRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        Ok(
            agent_memory_capsule::find_primary_capsule(&self.connection, &resolved)
                .await?
                .map(crate::memory::agent_memory_capsule_record_from_model)
                .transpose()?,
        )
    }

    pub async fn find_agent_memory_capsule_by_ref(
        &self,
        capsule_ref: &str,
    ) -> Result<Option<AgentMemoryCapsuleRecord>> {
        Ok(
            agent_memory_capsule::find_capsule_by_ref(&self.connection, capsule_ref)
                .await?
                .map(crate::memory::agent_memory_capsule_record_from_model)
                .transpose()?,
        )
    }

    pub async fn mark_agent_memory_capsule_repair_status(
        &self,
        capsule_id: &str,
        repair_status: &str,
        last_error: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryCapsuleRecord>> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent memory capsule repair status transaction")?;
            let row = agent_memory_capsule::mark_capsule_repair_status(
                &transaction,
                capsule_id,
                repair_status,
                last_error_value.clone(),
                unix_to_datetime(event_timestamp_secs),
            )
            .await?;
            let Some(row) = row else {
                transaction.commit().await.context(
                    "failed to commit empty agent memory capsule repair status transaction",
                )?;
                return Ok(None);
            };
            agent_memory_event::append_memory_event(
                &transaction,
                NewAgentMemoryEvent {
                    memory_id: None,
                    candidate_id: None,
                    workspace_id: row.workspace_id.clone(),
                    event_kind: MEMORY_EVENT_CAPSULE_REPAIR_STATUS_CHANGED.to_owned(),
                    actor: None,
                    thread_id: None,
                    turn_id: None,
                    item_id: None,
                    details_json: Some(
                        serde_json::json!({
                            "capsule_id": row.id.clone(),
                            "repair_status": repair_status,
                            "last_error": last_error_value.clone(),
                        })
                        .to_string(),
                    ),
                    created_at_unix: event_timestamp_secs,
                },
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit agent memory capsule repair status transaction")?;
            crate::memory::agent_memory_capsule_record_from_model(row).map(Some)
        })
        .await
    }

    pub async fn list_agent_memory_capsules_needing_repair(
        &self,
        workspace_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<AgentMemoryCapsuleRecord>> {
        agent_memory_capsule::list_capsules_needing_repair(&self.connection, workspace_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_capsule_record_from_model)
            .collect()
    }

    pub async fn find_thread_episodic_capsule(
        &self,
        capsule_id: &str,
    ) -> Result<Option<ThreadEpisodicCapsuleRecord>> {
        thread_episodic_repository::find_capsule_by_id(&self.connection, capsule_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_capsule_record_from_model)
            .transpose()
    }

    pub async fn list_thread_episodic_capsules_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicCapsuleRecord>> {
        thread_episodic_repository::list_capsules_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_capsule_record_from_model)
        .collect()
    }

    pub async fn resolve_thread_episodic_active_write_segment(
        &self,
        request: ThreadEpisodicActiveWriteSegmentRequest,
        now_unix: i64,
    ) -> Result<ThreadEpisodicCapsuleRecord> {
        self.run_serialized_write(|| {
            let request = request.clone();
            async move {
                if let Some(row) = thread_episodic_repository::find_active_write_capsule(
                    &self.connection,
                    request.workspace_id.as_str(),
                    request.thread_id.as_str(),
                )
                .await?
                {
                    return crate::thread_episodic::thread_episodic_capsule_record_from_model(row);
                }

                let workspace_key_hash = crate::thread_episodic::thread_episodic_key_hash(
                    "workspace",
                    request.workspace_id.as_str(),
                )?;
                let thread_key_hash = crate::thread_episodic::thread_episodic_key_hash(
                    "thread",
                    request.thread_id.as_str(),
                )?;
                let next_segment_index = thread_episodic_repository::max_segment_index_for_thread(
                    &self.connection,
                    request.workspace_id.as_str(),
                    request.thread_id.as_str(),
                )
                .await?
                .unwrap_or(0)
                    + 1;
                let capsule_id = crate::thread_episodic::deterministic_thread_episodic_capsule_id(
                    workspace_key_hash.as_str(),
                    thread_key_hash.as_str(),
                    next_segment_index,
                )?;
                let capsule_ref = crate::thread_episodic::thread_episodic_capsule_ref(
                    workspace_key_hash.as_str(),
                    thread_key_hash.as_str(),
                    next_segment_index,
                    capsule_id.as_str(),
                )?;
                let storage_uri = crate::thread_episodic::thread_episodic_capsule_storage_uri(
                    request.storage_uri_root.as_str(),
                    workspace_key_hash.as_str(),
                    thread_key_hash.as_str(),
                    next_segment_index,
                    capsule_id.as_str(),
                )?;
                let now = unix_to_datetime(now_unix);
                thread_episodic_repository::insert_capsule_if_absent(
                    &self.connection,
                    NewThreadEpisodicCapsuleRecord {
                        id: capsule_id,
                        workspace_id: request.workspace_id.clone(),
                        workspace_key_hash,
                        thread_id: request.thread_id.clone(),
                        thread_key_hash,
                        segment_index: next_segment_index,
                        write_state: ThreadEpisodicCapsuleWriteState::ActiveWrite,
                        capsule_ref,
                        storage_uri,
                        backend: "memvid".to_owned(),
                        format: "mv2".to_owned(),
                        encrypted: false,
                        status: ThreadEpisodicCapsuleStatus::Active,
                        repair_status: ThreadEpisodicRepairStatus::Ok,
                        active_chunk_count: 0,
                        capacity_bytes: None,
                        size_bytes: None,
                        utilization_percent: None,
                        content_hash: None,
                        metadata_json: None,
                        last_error: None,
                    },
                    now,
                )
                .await?;

                let row = thread_episodic_repository::find_active_write_capsule(
                    &self.connection,
                    request.workspace_id.as_str(),
                    request.thread_id.as_str(),
                )
                .await?
                .context("resolved thread episodic active write capsule missing")?;
                crate::thread_episodic::thread_episodic_capsule_record_from_model(row)
            }
        })
        .await
    }

    pub async fn transition_thread_episodic_active_write_segment(
        &self,
        capsule_id: &str,
        target_state: ThreadEpisodicCapsuleWriteState,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicCapsuleRecord>> {
        if !matches!(
            target_state,
            ThreadEpisodicCapsuleWriteState::ReadOnly | ThreadEpisodicCapsuleWriteState::Full
        ) {
            anyhow::bail!(
                "thread episodic active segment can only transition to read_only or full"
            );
        }
        thread_episodic_repository::transition_capsule_write_state(
            &self.connection,
            capsule_id,
            ThreadEpisodicCapsuleWriteState::ActiveWrite,
            target_state,
            unix_to_datetime(now_unix),
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_capsule_record_from_model)
        .transpose()
    }

    pub async fn update_thread_episodic_capsule_capacity(
        &self,
        capsule_id: &str,
        update: ThreadEpisodicCapsuleCapacityUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicCapsuleRecord>> {
        thread_episodic_repository::update_capsule_capacity_metadata(
            &self.connection,
            capsule_id,
            update,
            unix_to_datetime(now_unix),
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_capsule_record_from_model)
        .transpose()
    }

    pub async fn update_thread_episodic_capsule_metadata_json(
        &self,
        capsule_id: &str,
        metadata_json: String,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicCapsuleRecord>> {
        thread_episodic_repository::update_capsule_metadata_json(
            &self.connection,
            capsule_id,
            metadata_json,
            unix_to_datetime(now_unix),
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_capsule_record_from_model)
        .transpose()
    }

    pub async fn find_thread_episodic_chunk(
        &self,
        chunk_id: &str,
    ) -> Result<Option<ThreadEpisodicChunkRecord>> {
        thread_episodic_repository::find_chunk_by_id(&self.connection, chunk_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
            .transpose()
    }

    pub async fn upsert_thread_episodic_chunk(
        &self,
        chunk: NewThreadEpisodicChunkRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicChunkRecord> {
        self.run_serialized_write(|| {
            let chunk = chunk.clone();
            async move {
                let row = thread_episodic_repository::upsert_chunk_by_source_identity(
                    &self.connection,
                    chunk,
                    unix_to_datetime(now_unix),
                )
                .await?;
                crate::thread_episodic::thread_episodic_chunk_record_from_model(row)
            }
        })
        .await
    }

    pub async fn find_thread_episodic_chunk_by_source_identity(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        chunk_index: i64,
        text_hash: &str,
    ) -> Result<Option<ThreadEpisodicChunkRecord>> {
        thread_episodic_repository::find_chunk_by_source_identity(
            &self.connection,
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            chunk_index,
            text_hash,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
        .transpose()
    }

    pub async fn list_thread_episodic_chunks_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicChunkRecord>> {
        thread_episodic_repository::list_chunks_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
        .collect()
    }

    pub async fn list_recallable_thread_episodic_chunks_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicChunkRecord>> {
        thread_episodic_repository::list_recallable_chunks_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
        .collect()
    }

    pub async fn find_thread_episodic_index_job(
        &self,
        job_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::find_index_job_by_id(&self.connection, job_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
            .transpose()
    }

    pub async fn find_thread_episodic_index_job_by_chunk(
        &self,
        chunk_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::find_index_job_by_chunk(&self.connection, chunk_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
            .transpose()
    }

    pub async fn insert_thread_episodic_index_job_if_absent(
        &self,
        job: NewThreadEpisodicIndexJobRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexJobRecord> {
        self.run_serialized_write(|| {
            let job = job.clone();
            async move {
                let row = thread_episodic_repository::insert_index_job_if_absent(
                    &self.connection,
                    job,
                    unix_to_datetime(now_unix),
                )
                .await?;
                crate::thread_episodic::thread_episodic_index_job_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_index_jobs_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn list_failed_or_stale_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        stale_before_unix: i64,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_failed_or_stale_index_jobs_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            unix_to_datetime(stale_before_unix),
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn retry_failed_or_stale_thread_episodic_index_job(
        &self,
        job_id: &str,
        stale_before_unix: i64,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.run_serialized_write(|| {
            let job_id = job_id.to_owned();
            async move {
                thread_episodic_repository::retry_failed_or_stale_index_job(
                    &self.connection,
                    job_id.as_str(),
                    unix_to_datetime(stale_before_unix),
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn claim_due_thread_episodic_index_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.run_serialized_write(|| async move {
            let now = unix_to_datetime(now_unix);
            let rows =
                thread_episodic_repository::list_due_index_jobs(&self.connection, now, limit)
                    .await?;
            let mut claimed = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(row) = thread_episodic_repository::mark_index_job_running(
                    &self.connection,
                    row.id.as_str(),
                    now,
                )
                .await?
                {
                    claimed.push(
                        crate::thread_episodic::thread_episodic_index_job_record_from_model(row)?,
                    );
                }
            }
            Ok(claimed)
        })
        .await
    }

    pub async fn complete_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobCompletionUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.run_serialized_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_index_job_completed(
                    &self.connection,
                    job_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn fail_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.run_serialized_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_index_job_failed(
                    &self.connection,
                    job_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn cancel_thread_episodic_index_job(
        &self,
        job_id: &str,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.run_serialized_write(|| {
            let job_id = job_id.to_owned();
            let last_error = last_error.clone();
            async move {
                thread_episodic_repository::mark_index_job_canceled(
                    &self.connection,
                    job_id.as_str(),
                    last_error,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn mark_thread_episodic_chunk_indexed(
        &self,
        chunk_id: &str,
        update: ThreadEpisodicChunkIndexedUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicChunkRecord>> {
        self.run_serialized_write(|| {
            let chunk_id = chunk_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_chunk_indexed(
                    &self.connection,
                    chunk_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn mark_thread_episodic_chunk_failed(
        &self,
        chunk_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicChunkRecord>> {
        self.run_serialized_write(|| {
            let chunk_id = chunk_id.to_owned();
            async move {
                thread_episodic_repository::mark_chunk_failed(
                    &self.connection,
                    chunk_id.as_str(),
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn tombstone_thread_episodic_chunks_for_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicChunkRecord>> {
        self.run_serialized_write(|| async move {
            thread_episodic_repository::mark_chunks_deleted_by_source_item(
                &self.connection,
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                unix_to_datetime(now_unix),
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
            .collect()
        })
        .await
    }

    pub async fn tombstone_thread_episodic_chunks_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicChunkRecord>> {
        self.run_serialized_write(|| async move {
            thread_episodic_repository::mark_chunks_deleted_for_thread(
                &self.connection,
                workspace_id,
                thread_id,
                unix_to_datetime(now_unix),
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_chunk_record_from_model)
            .collect()
        })
        .await
    }

    pub async fn find_thread_episodic_exclusion_by_chunk(
        &self,
        workspace_id: &str,
        thread_id: &str,
        chunk_id: &str,
    ) -> Result<Option<ThreadEpisodicExclusionRecord>> {
        thread_episodic_repository::find_exclusion_by_chunk(
            &self.connection,
            workspace_id,
            thread_id,
            chunk_id,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_exclusion_record_from_model)
        .transpose()
    }

    pub async fn exclude_thread_episodic_chunk(
        &self,
        exclusion: NewThreadEpisodicExclusionRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicExclusionRecord> {
        self.run_serialized_write(|| {
            let exclusion = exclusion.clone();
            async move {
                let row = thread_episodic_repository::insert_exclusion_if_absent(
                    &self.connection,
                    exclusion,
                    unix_to_datetime(now_unix),
                )
                .await?;
                crate::thread_episodic::thread_episodic_exclusion_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_thread_episodic_exclusions_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicExclusionRecord>> {
        thread_episodic_repository::list_exclusions_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_exclusion_record_from_model)
        .collect()
    }

    pub async fn list_thread_episodic_recall_events_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicRecallEventRecord>> {
        Ok(thread_episodic_repository::list_recall_events_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_recall_event_record_from_model)
        .collect())
    }

    pub async fn insert_thread_episodic_recall_event(
        &self,
        event: NewThreadEpisodicRecallEventRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicRecallEventRecord> {
        self.run_serialized_write(|| {
            let event = event.clone();
            async move {
                let row = thread_episodic_repository::insert_recall_event(
                    &self.connection,
                    event,
                    unix_to_datetime(now_unix),
                )
                .await?;
                Ok(crate::thread_episodic::thread_episodic_recall_event_record_from_model(row))
            }
        })
        .await
    }

    pub async fn find_thread_episodic_thread_directory_entry(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Option<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(thread_episodic_repository::find_thread_directory_entry(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model))
    }

    pub async fn upsert_thread_episodic_thread_directory_entry(
        &self,
        record: NewThreadEpisodicThreadDirectoryRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicThreadDirectoryRecord> {
        self.run_serialized_write(|| {
            let record = record.clone();
            async move {
                let row = thread_episodic_repository::upsert_thread_directory_entry(
                    &self.connection,
                    record,
                    unix_to_datetime(now_unix),
                )
                .await?;
                Ok(crate::thread_episodic::thread_episodic_thread_directory_record_from_model(row))
            }
        })
        .await
    }

    pub async fn list_selectable_thread_episodic_thread_directory_entries(
        &self,
        selection: ThreadEpisodicThreadDirectorySelection,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(
            thread_episodic_repository::list_selectable_thread_directory_entries(
                &self.connection,
                selection,
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model)
            .collect(),
        )
    }

    pub async fn list_thread_episodic_thread_directory_entries_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(
            thread_episodic_repository::list_thread_directory_entries_for_workspace(
                &self.connection,
                workspace_id,
                limit,
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model)
            .collect(),
        )
    }

    pub async fn count_active_thread_episodic_chunks_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<i64> {
        thread_episodic_repository::count_active_chunks_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await
    }

    pub async fn insert_agent_memory_policy_decision(
        &self,
        decision: NewAgentMemoryPolicyDecision,
    ) -> Result<AgentMemoryPolicyDecisionRecord> {
        self.run_serialized_write(|| {
            let decision = decision.clone();
            async move {
                let row = agent_memory_policy_decision::insert_policy_decision(
                    &self.connection,
                    decision,
                )
                .await?;
                crate::memory::agent_memory_policy_decision_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_agent_memory_policy_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_policy_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_candidate(
            &self.connection,
            candidate_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_policy_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_thread(
            &self.connection,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn insert_agent_memory_quality_decision(
        &self,
        decision: NewAgentMemoryQualityDecision,
    ) -> Result<AgentMemoryQualityDecisionRecord> {
        self.run_serialized_write(|| {
            let decision = decision.clone();
            async move {
                let row = agent_memory_quality_decision::insert_quality_decision(
                    &self.connection,
                    decision,
                )
                .await?;
                crate::memory::agent_memory_quality_decision_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_agent_memory_quality_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_quality_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_candidate(
            &self.connection,
            candidate_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_quality_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_thread(
            &self.connection,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn enqueue_agent_memory_repair_job(
        &self,
        job: NewAgentMemoryRepairJob,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        self.run_serialized_write(|| {
            let job = job.clone();
            async move {
                let row = agent_memory_repair_job::enqueue_repair_job(
                    &self.connection,
                    job,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?;
                crate::memory::agent_memory_repair_job_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_agent_memory_repair_job(
        &self,
        job_id: &str,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        agent_memory_repair_job::find_repair_job_by_id(&self.connection, job_id)
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
    }

    pub async fn list_agent_memory_repair_jobs_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        agent_memory_repair_job::list_repair_jobs_for_memory(&self.connection, memory_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .collect()
    }

    pub async fn claim_due_agent_memory_repair_jobs(
        &self,
        now_unix: i64,
        lock_ttl_secs: i64,
        locked_by: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(now_unix);
            let lock_expires_at = unix_to_datetime(now_unix.saturating_add(lock_ttl_secs));
            agent_memory_repair_job::claim_due_repair_jobs(
                &self.connection,
                now,
                lock_expires_at,
                locked_by,
                limit,
            )
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .collect()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_running(
        &self,
        job_id: &str,
        locked_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.run_serialized_write(|| async {
            agent_memory_repair_job::mark_repair_job_running(
                &self.connection,
                job_id,
                locked_by,
                unix_to_datetime(event_timestamp_secs),
            )
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_completed(
        &self,
        job_id: &str,
        locked_by: &str,
        result_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        let result_json_value = result_json.clone();
        self.run_serialized_write(|| async {
            agent_memory_repair_job::mark_repair_job_completed(
                &self.connection,
                job_id,
                locked_by,
                result_json_value.clone(),
                unix_to_datetime(event_timestamp_secs),
            )
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_failed(
        &self,
        job_id: &str,
        locked_by: &str,
        last_error: String,
        retry_at_unix: Option<i64>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            agent_memory_repair_job::mark_repair_job_failed(
                &self.connection,
                job_id,
                locked_by,
                last_error_value.clone(),
                retry_at_unix.map(unix_to_datetime),
                unix_to_datetime(event_timestamp_secs),
            )
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
        })
        .await
    }

    /// Persists the full turn/start write-set atomically through append-only events + projection.
    pub async fn materialize_turn_start(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
    ) -> Result<()> {
        self.materialize_turn_start_with_reasoning_effort(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            None,
        )
        .await
    }

    /// Persists the full turn/start write-set plus explicit reasoning effort.
    pub async fn materialize_turn_start_with_reasoning_effort(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
    ) -> Result<()> {
        let event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: input.to_vec(),
            reasoning_effort: reasoning_effort.map(str::to_owned),
        });

        self.materialize_turn_event(event, thread_model.updated_at)
            .await
    }

    /// Persists turn/start and its caller-owned permission audit as one write-set.
    pub async fn materialize_turn_start_with_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    ) -> Result<()> {
        self.materialize_turn_start_with_reasoning_effort_and_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            None,
            audit_event,
        )
        .await
    }

    /// Persists turn/start, explicit reasoning effort, and caller-owned permission audit atomically.
    pub async fn materialize_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    ) -> Result<()> {
        let started_event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: input.to_vec(),
            reasoning_effort: reasoning_effort.map(str::to_owned),
        });
        let audit_event = TurnEventPayload::TurnPermissionAudit(audit_event);

        self.materialize_turn_events_atomically(
            vec![started_event, audit_event],
            thread_model.updated_at,
        )
        .await
    }

    pub async fn materialize_item_started(
        &self,
        notification: pioneer_protocol::ItemStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(
            TurnEventPayload::ItemStarted(notification),
            event_timestamp_secs,
            None,
        )
        .await
    }

    pub async fn materialize_item_started_with_attempt_deadlines(
        &self,
        notification: pioneer_protocol::ItemStartedNotification,
        event_timestamp_secs: i64,
        deadlines: TurnItemAttemptDeadlines,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(
            TurnEventPayload::ItemStarted(notification),
            event_timestamp_secs,
            Some(deadlines),
        )
        .await
    }

    pub async fn materialize_item_completed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemCompleted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_agent_diff_final_snapshot_if_changed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        if !is_agent_diff_updated_item(&notification.item) {
            anyhow::bail!(
                "final diff snapshot expected agent_diff_updated item, got `{}`",
                notification.item.item_id()
            );
        }

        let item_payload_json = serde_json::to_string(&notification.item)
            .context("failed to serialize final agent diff snapshot payload")?;
        let latest_raw_payload = latest_agent_diff_raw_payload_for_item(
            &self.connection,
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
            notification.item.item_id(),
        )
        .await?;
        if latest_raw_payload.as_deref() == Some(item_payload_json.as_str()) {
            return Ok(false);
        }

        self.materialize_item_completed(notification, event_timestamp_secs)
            .await?;
        Ok(true)
    }

    pub async fn materialize_item_updated(
        &self,
        notification: pioneer_protocol::ItemUpdatedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemUpdated(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_snapshot_updated(
        &self,
        notification: pioneer_protocol::ItemUpdatedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let updated_at = unix_to_datetime(event_timestamp_secs);
        self.run_serialized_write(|| {
            let notification = notification.clone();
            let connection = self.connection.clone();
            async move {
                let transaction = connection
                    .begin()
                    .await
                    .context("failed to begin item snapshot update transaction")?;

                let result: Result<()> = async {
                    let Some(_turn_model) = turn::find_turn_by_thread_and_id(
                        &transaction,
                        notification.thread_id.as_str(),
                        notification.turn_id.as_str(),
                    )
                    .await?
                    else {
                        anyhow::bail!(
                            "item snapshot update cannot find turn `{}` in thread `{}`",
                            notification.turn_id,
                            notification.thread_id
                        );
                    };
                    let Some(thread_model) =
                        thread::find_thread_by_id(&transaction, notification.thread_id.as_str())
                            .await?
                    else {
                        anyhow::bail!(
                            "item snapshot update cannot find thread `{}`",
                            notification.thread_id
                        );
                    };
                    if thread_model.workspace_id != notification.workspace_id {
                        anyhow::bail!(
                            "item snapshot update workspace mismatch for thread `{}`: expected `{}`, got `{}`",
                            notification.thread_id,
                            thread_model.workspace_id,
                            notification.workspace_id
                        );
                    }

                    let source_sequence = turn_event::latest_event_for_turn(
                        &transaction,
                        notification.turn_id.as_str(),
                    )
                    .await?
                    .map(|event| event.sequence)
                    .unwrap_or(0);
                    let status =
                        crate::turn_item_terminal::terminal_turn_item_status_from_payload(
                            &notification.item,
                        );
                    turn::upsert_turn_item(
                        &transaction,
                        notification.turn_id.as_str(),
                        &notification.item,
                        Some(status),
                        updated_at,
                        updated_at,
                    )
                    .await?;
                    crate::timeline_live_projection::project_semantic_timeline_snapshot_turn_item(
                        &transaction,
                        notification.turn_id.as_str(),
                        notification.item.item_id(),
                        source_sequence,
                        updated_at,
                    )
                    .await
                }
                .await;

                match result {
                    Ok(()) => transaction
                        .commit()
                        .await
                        .context("failed to commit item snapshot update transaction"),
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }

    pub async fn materialize_item_timeout_detected(
        &self,
        notification: pioneer_protocol::ItemTimeoutDetectedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemTimeoutDetected(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_opened(
        &self,
        notification: pioneer_protocol::ItemRecoveryOpenedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryOpened(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_attached(
        &self,
        notification: pioneer_protocol::ItemRecoveryAttachedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryAttached(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemRetryScheduledNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRetryScheduled(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_retry_attempt_started(
        &self,
        notification: pioneer_protocol::ItemRetryAttemptStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRetryAttemptStarted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_succeeded(
        &self,
        notification: pioneer_protocol::ItemRecoverySucceededNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoverySucceeded(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_exhausted(
        &self,
        notification: pioneer_protocol::ItemRecoveryExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemToolRetryScheduledNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryScheduled(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_resolved(
        &self,
        notification: pioneer_protocol::ItemToolRetryResolvedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryResolved(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_exhausted(
        &self,
        notification: pioneer_protocol::ItemToolRetryExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_tool_loop_budget_exceeded(
        &self,
        notification: pioneer_protocol::TurnToolLoopBudgetExceededNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnToolLoopBudgetExceeded(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_started(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowStarted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_exhausted(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_checkpointed(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowCheckpointedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowCheckpointed(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_continued(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowContinuedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowContinued(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_blocked(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowBlockedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowBlocked(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_permission_audit(
        &self,
        event: pioneer_protocol::TurnPermissionAuditEvent,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnPermissionAudit(event),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_completed(
        &self,
        notification: pioneer_protocol::TurnCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnCompleted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_failed(
        &self,
        notification: pioneer_protocol::TurnFailedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnFailed(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_blocked(
        &self,
        notification: pioneer_protocol::TurnBlockedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnBlocked(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn append_task_event(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<AppendedTaskEvent> {
        self.run_serialized_write(|| {
            self.append_task_event_once(event.clone(), event_timestamp_secs)
        })
        .await
    }

    pub async fn append_task_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.run_serialized_write(|| {
            self.append_task_events_once(events.clone(), event_timestamp_secs)
        })
        .await
    }

    pub async fn append_due_trigger_task_events(
        &self,
        trigger_id: &str,
        expected_next_fire_at: i64,
        now: i64,
        events: Vec<TaskEventPayload>,
        reserve_executions: Vec<(String, TaskExecutorKind)>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.run_serialized_write(|| {
            self.append_due_trigger_task_events_once(
                trigger_id.to_owned(),
                expected_next_fire_at,
                now,
                events.clone(),
                reserve_executions.clone(),
            )
        })
        .await
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<TaskGetResponse>> {
        let Some(task_model) = task_repository::find_task_by_id(&self.connection, task_id).await?
        else {
            return Ok(None);
        };

        let task = task_from_db_model(task_model)?;
        let triggers = task_trigger::list_triggers_by_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let runs = task_run::list_runs_by_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let agent_specs = task_agent_spec::list_agent_specs_by_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_agent_spec_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let dependencies = task_dependency::list_dependencies_for_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_dependency_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let write_locks = task_write_lock::list_locks_by_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let mut task_run_thread_bindings = Vec::new();
        let mut task_run_turns = Vec::new();
        let mut result_candidates = Vec::new();
        let mut result_review_events = Vec::new();
        for run in &runs {
            task_run_thread_bindings.extend(self.list_task_run_thread_bindings(&run.id).await?);
            task_run_turns.extend(self.list_task_run_turns(&run.id).await?);
            result_candidates.extend(self.list_task_result_candidates(&run.id).await?);
            result_review_events
                .extend(self.list_task_result_review_events_for_run(&run.id).await?);
        }
        let mut thread_lineage = Vec::new();
        for binding in &task_run_thread_bindings {
            if thread_lineage
                .iter()
                .any(|lineage: &TaskThreadLineage| lineage.child_thread_id == binding.thread_id)
            {
                continue;
            }
            if let Some(lineage) =
                thread_lineage::find_lineage_by_child_thread(&self.connection, &binding.thread_id)
                    .await?
            {
                thread_lineage.push(task_thread_lineage_from_db_model(lineage));
            }
        }

        Ok(Some(TaskGetResponse {
            task,
            triggers,
            runs,
            agent_specs,
            dependencies,
            write_locks,
            thread_lineage,
            task_run_thread_bindings,
            task_run_turns,
            result_candidates,
            result_review_events,
        }))
    }

    pub async fn list_tasks(&self, params: TaskListParams) -> Result<Vec<Task>> {
        let limit = params.limit.map(u64::from);
        let rows = if let Some(parent_task_id) = params.parent_task_id.as_deref() {
            task_repository::list_tasks_by_parent(&self.connection, parent_task_id).await?
        } else if let Some(root_task_id) = params.root_task_id.as_deref() {
            task_repository::list_tasks_by_root(&self.connection, root_task_id).await?
        } else if let Some(owner_kind) = params.owner_kind {
            let owner_kind = task_owner_kind_to_db(owner_kind);
            task_repository::list_tasks_by_owner(
                &self.connection,
                params.workspace_id.as_str(),
                owner_kind,
                params.owner_id.as_deref(),
                limit,
            )
            .await?
        } else {
            let status = params.status.map(task_status_to_db);
            task_repository::list_tasks_by_workspace_status(
                &self.connection,
                params.workspace_id.as_str(),
                status,
                limit,
            )
            .await?
        };

        rows.into_iter().map(task_from_db_model).collect()
    }

    pub async fn get_task_tree(&self, task_id: &str) -> Result<Option<TaskTree>> {
        let Some(root_model) = task_repository::find_task_by_id(&self.connection, task_id).await?
        else {
            return Ok(None);
        };

        let mut task_models = vec![root_model.clone()];
        let mut child_models =
            task_repository::list_tasks_by_root(&self.connection, task_id).await?;
        task_models.append(&mut child_models);

        let task_ids = task_models
            .iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>();

        let mut triggers_by_task: HashMap<String, Vec<TaskTrigger>> = HashMap::new();
        let mut runs_by_task: HashMap<String, Vec<TaskRun>> = HashMap::new();
        let mut specs_by_task: HashMap<String, Vec<TaskAgentSpec>> = HashMap::new();
        let mut dependencies_by_task: HashMap<String, Vec<TaskDependency>> = HashMap::new();
        let mut write_locks_by_task: HashMap<String, Vec<TaskWriteLock>> = HashMap::new();

        for task_id in &task_ids {
            triggers_by_task.insert(
                task_id.clone(),
                task_trigger::list_triggers_by_task(&self.connection, task_id)
                    .await?
                    .into_iter()
                    .map(task_trigger_from_db_model)
                    .collect::<Result<Vec<_>>>()?,
            );
            runs_by_task.insert(
                task_id.clone(),
                task_run::list_runs_by_task(&self.connection, task_id)
                    .await?
                    .into_iter()
                    .map(task_run_from_db_model)
                    .collect::<Result<Vec<_>>>()?,
            );
            specs_by_task.insert(
                task_id.clone(),
                task_agent_spec::list_agent_specs_by_task(&self.connection, task_id)
                    .await?
                    .into_iter()
                    .map(task_agent_spec_from_db_model)
                    .collect::<Result<Vec<_>>>()?,
            );
            dependencies_by_task.insert(
                task_id.clone(),
                task_dependency::list_dependencies_for_task(&self.connection, task_id)
                    .await?
                    .into_iter()
                    .map(task_dependency_from_db_model)
                    .collect::<Result<Vec<_>>>()?,
            );
            write_locks_by_task.insert(
                task_id.clone(),
                task_write_lock::list_locks_by_task(&self.connection, task_id)
                    .await?
                    .into_iter()
                    .map(task_write_lock_from_db_model)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        let mut children_by_parent: HashMap<String, Vec<Task>> = HashMap::new();
        for model in task_models {
            let task = task_from_db_model(model)?;
            if let Some(parent_task_id) = task.parent_task_id.clone() {
                children_by_parent
                    .entry(parent_task_id)
                    .or_default()
                    .push(task);
            }
        }

        let root = task_from_db_model(root_model)?;
        Ok(Some(build_task_tree(
            root,
            &mut children_by_parent,
            &mut triggers_by_task,
            &mut runs_by_task,
            &mut specs_by_task,
            &mut dependencies_by_task,
            &mut write_locks_by_task,
        )))
    }

    pub async fn get_task_events(
        &self,
        task_id: &str,
        after_sequence: Option<i64>,
    ) -> Result<TaskEventsResponse> {
        let rows =
            task_event::list_events_for_task(&self.connection, task_id, after_sequence).await?;
        let mut events = Vec::with_capacity(rows.len());
        let mut last_sequence = after_sequence.unwrap_or(0);

        for row in rows {
            last_sequence = row.sequence;
            events.push(task_event::task_event_from_model(row)?);
        }

        Ok(TaskEventsResponse {
            task_id: task_id.to_owned(),
            events,
            last_sequence,
        })
    }

    pub async fn list_task_events_after(
        &self,
        task_id: &str,
        after_sequence: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let rows =
            task_event::list_events_for_task(&self.connection, task_id, Some(after_sequence))
                .await?;
        let mut events = Vec::with_capacity(rows.len());

        for row in rows {
            let mut event =
                task_event::appended_task_event_from_model(row, TaskEventAppendStatus::Inserted)?;
            hydrate_task_event_metadata(&self.connection, &mut event).await?;
            events.push(event);
        }

        Ok(events)
    }

    pub async fn list_task_event_task_ids(&self) -> Result<Vec<String>> {
        task_event::list_event_task_ids(&self.connection).await
    }

    pub async fn list_task_events_for_thread_turn(
        &self,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Vec<pioneer_protocol::TaskEvent>> {
        task_event::list_events_for_thread_turn(&self.connection, thread_id, turn_id)
            .await?
            .into_iter()
            .map(task_event::task_event_from_model)
            .collect()
    }

    pub async fn get_task_runs(&self, task_id: &str) -> Result<Vec<TaskRun>> {
        task_run::list_runs_by_task(&self.connection, task_id)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn get_task_run(&self, run_id: &str) -> Result<Option<TaskRun>> {
        task_run::find_run_by_id(&self.connection, run_id)
            .await?
            .map(task_run_from_db_model)
            .transpose()
    }

    pub async fn reserve_execution_for_run(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        now: i64,
    ) -> Result<TaskRunExecution> {
        self.run_serialized_write(|| {
            self.reserve_execution_for_run_once(run_id.to_owned(), executor_kind, now)
        })
        .await
    }

    async fn reserve_execution_for_run_once(
        &self,
        run_id: String,
        executor_kind: TaskExecutorKind,
        now: i64,
    ) -> Result<TaskRunExecution> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task run execution reservation transaction")?;

        let result =
            reserve_execution_for_run_in_connection(&transaction, run_id, executor_kind, now).await;

        match result {
            Ok(execution) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit task run execution reservation transaction")?;
                Ok(execution)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn load_execution_for_run(&self, run_id: &str) -> Result<Option<TaskRunExecution>> {
        task_run_execution::find_execution_by_run(&self.connection, run_id)
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
    }

    pub async fn claim_execution(
        &self,
        execution_id: &str,
        worker_id: &str,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.claim_execution_at(execution_id, worker_id, lease_until, lease_until)
            .await
    }

    pub async fn claim_execution_at(
        &self,
        execution_id: &str,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.run_serialized_write(|| async {
            task_run_execution::claim_execution(
                &self.connection,
                execution_id,
                worker_id,
                unix_to_datetime(now),
                unix_to_datetime(lease_until),
            )
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_execution_running(
        &self,
        execution_id: &str,
        started_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.run_serialized_write(|| async {
            task_run_execution::mark_execution_running(
                &self.connection,
                execution_id,
                unix_to_datetime(started_at),
                lease_until.map(unix_to_datetime),
            )
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_execution_terminal(
        &self,
        execution_id: &str,
        status: TaskRunExecutionStatus,
        completed_at: i64,
        result: Option<&TaskResult>,
        error: Option<&TaskError>,
    ) -> Result<Option<TaskRunExecution>> {
        self.run_serialized_write(|| async {
            task_run_execution::mark_execution_terminal(
                &self.connection,
                execution_id,
                status,
                unix_to_datetime(completed_at),
                result,
                error,
            )
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
        })
        .await
    }

    pub async fn heartbeat_execution(
        &self,
        execution_id: &str,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.run_serialized_write(|| async {
            task_run_execution::heartbeat_execution(
                &self.connection,
                execution_id,
                unix_to_datetime(heartbeat_at),
                lease_until.map(unix_to_datetime),
            )
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
        })
        .await
    }

    pub async fn upsert_task_run_thread_binding(
        &self,
        binding: TaskRunThreadBinding,
    ) -> Result<TaskRunThreadBinding> {
        self.run_serialized_write(|| async {
            task_run_thread_binding::upsert_binding(
                &self.connection,
                task_run_thread_binding::NewTaskRunThreadBinding {
                    id: binding.id.clone(),
                    task_id: binding.task_id.clone(),
                    run_id: binding.run_id.clone(),
                    execution_id: binding.execution_id.clone(),
                    thread_id: binding.thread_id.clone(),
                    binding_kind: binding.binding_kind,
                    created_at: binding.created_at,
                },
            )
            .await?;
            let row = task_run_thread_binding::find_binding_by_id(&self.connection, &binding.id)
                .await?
                .context("task run thread binding missing after upsert")?;
            task_run_thread_binding_from_db_model(row)
        })
        .await
    }

    pub async fn get_task_run_thread_binding(
        &self,
        id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_id(&self.connection, id)
            .await?
            .map(task_run_thread_binding_from_db_model)
            .transpose()
    }

    pub async fn get_task_run_primary_thread_binding(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_run_and_kind(
            &self.connection,
            run_id,
            TaskRunThreadBindingKind::PrimaryExecutor,
        )
        .await?
        .map(task_run_thread_binding_from_db_model)
        .transpose()
    }

    pub async fn get_task_run_thread_binding_by_thread(
        &self,
        thread_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_thread(&self.connection, thread_id)
            .await?
            .map(task_run_thread_binding_from_db_model)
            .transpose()
    }

    pub async fn list_task_run_thread_bindings(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskRunThreadBinding>> {
        task_run_thread_binding::list_bindings_by_run(&self.connection, run_id)
            .await?
            .into_iter()
            .map(task_run_thread_binding_from_db_model)
            .collect()
    }

    pub async fn upsert_task_run_turn(&self, turn: TaskRunTurn) -> Result<TaskRunTurn> {
        self.run_serialized_write(|| async {
            task_run_turn::upsert_turn(
                &self.connection,
                task_run_turn::NewTaskRunTurn {
                    id: turn.id.clone(),
                    task_id: turn.task_id.clone(),
                    run_id: turn.run_id.clone(),
                    execution_id: turn.execution_id.clone(),
                    thread_id: turn.thread_id.clone(),
                    turn_id: turn.turn_id.clone(),
                    kind: turn.kind,
                    round: turn.round,
                    sequence: turn.sequence,
                    status: turn.status,
                    reviews_candidate_id: turn.reviews_candidate_id.clone(),
                    requested_by_candidate_id: turn.requested_by_candidate_id.clone(),
                    requested_by_review_event_id: turn.requested_by_review_event_id.clone(),
                    created_at: turn.created_at,
                    started_at: turn.started_at,
                    completed_at: turn.completed_at,
                },
            )
            .await?;
            let row = task_run_turn::find_turn_by_id(&self.connection, &turn.id)
                .await?
                .context("task run turn missing after upsert")?;
            task_run_turn_from_db_model(row)
        })
        .await
    }

    pub async fn get_task_run_turn(&self, id: &str) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_turn_by_id(&self.connection, id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }

    pub async fn get_task_run_turn_by_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_turn_by_thread_and_turn(&self.connection, thread_id, turn_id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }

    pub async fn list_task_run_turns(&self, run_id: &str) -> Result<Vec<TaskRunTurn>> {
        task_run_turn::list_turns_by_run(&self.connection, run_id)
            .await?
            .into_iter()
            .map(task_run_turn_from_db_model)
            .collect()
    }

    pub async fn get_latest_task_run_turn(&self, run_id: &str) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_latest_turn_by_run(&self.connection, run_id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }

    pub async fn update_task_run_turn_status(
        &self,
        id: &str,
        status: TaskRunTurnStatus,
        completed_at: Option<i64>,
    ) -> Result<Option<TaskRunTurn>> {
        self.run_serialized_write(|| async {
            task_run_turn::update_turn_status(&self.connection, id, status, completed_at)
                .await?
                .map(task_run_turn_from_db_model)
                .transpose()
        })
        .await
    }

    pub async fn upsert_task_result_candidate(
        &self,
        candidate: TaskResultCandidate,
    ) -> Result<TaskResultCandidate> {
        self.run_serialized_write(|| async {
            task_result_candidate::upsert_candidate(
                &self.connection,
                task_result_candidate::NewTaskResultCandidate {
                    id: candidate.id.clone(),
                    task_id: candidate.task_id.clone(),
                    run_id: candidate.run_id.clone(),
                    task_run_turn_id: candidate.task_run_turn_id.clone(),
                    thread_id: candidate.thread_id.clone(),
                    turn_id: candidate.turn_id.clone(),
                    round: candidate.round,
                    status: candidate.status,
                    result: candidate.result.clone(),
                    extraction_error: candidate.extraction_error.clone(),
                    summary: candidate.summary.clone(),
                    diagnostics: candidate.diagnostics.clone(),
                    final_review_event_id: candidate.final_review_event_id.clone(),
                    created_at: candidate.created_at,
                    updated_at: candidate.updated_at,
                    resolved_at: candidate.resolved_at,
                },
            )
            .await?;
            let row = task_result_candidate::find_candidate_by_id(&self.connection, &candidate.id)
                .await?
                .context("task result candidate missing after upsert")?;
            task_result_candidate_from_db_model(row)
        })
        .await
    }

    pub async fn get_task_result_candidate(&self, id: &str) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_id(&self.connection, id)
            .await?
            .map(task_result_candidate_from_db_model)
            .transpose()
    }

    pub async fn get_task_result_candidate_by_turn(
        &self,
        task_run_turn_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_turn(&self.connection, task_run_turn_id)
            .await?
            .map(task_result_candidate_from_db_model)
            .transpose()
    }

    pub async fn list_task_result_candidates(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultCandidate>> {
        task_result_candidate::list_candidates_by_run(&self.connection, run_id)
            .await?
            .into_iter()
            .map(task_result_candidate_from_db_model)
            .collect()
    }

    pub async fn get_accepted_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_run_and_status(
            &self.connection,
            run_id,
            TaskResultCandidateStatus::Accepted,
        )
        .await?
        .map(task_result_candidate_from_db_model)
        .transpose()
    }

    pub async fn get_pending_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_run_and_status(
            &self.connection,
            run_id,
            TaskResultCandidateStatus::PendingReview,
        )
        .await?
        .map(task_result_candidate_from_db_model)
        .transpose()
    }

    pub async fn update_task_result_candidate_resolution(
        &self,
        id: &str,
        status: TaskResultCandidateStatus,
        final_review_event_id: Option<&str>,
        resolved_at: Option<i64>,
        updated_at: i64,
    ) -> Result<Option<TaskResultCandidate>> {
        self.run_serialized_write(|| async {
            task_result_candidate::update_candidate_resolution(
                &self.connection,
                id,
                status,
                final_review_event_id,
                resolved_at,
                updated_at,
            )
            .await?
            .map(task_result_candidate_from_db_model)
            .transpose()
        })
        .await
    }

    pub async fn upsert_task_result_review_event(
        &self,
        review_event: TaskResultReviewEvent,
    ) -> Result<TaskResultReviewEvent> {
        self.run_serialized_write(|| async {
            task_result_review_event::upsert_review_event(
                &self.connection,
                task_result_review_event::NewTaskResultReviewEvent {
                    id: review_event.id.clone(),
                    candidate_id: review_event.candidate_id.clone(),
                    task_id: review_event.task_id.clone(),
                    run_id: review_event.run_id.clone(),
                    task_run_turn_id: review_event.task_run_turn_id.clone(),
                    reviewer_kind: review_event.reviewer_kind,
                    reviewer_thread_id: review_event.reviewer_thread_id.clone(),
                    reviewer_turn_id: review_event.reviewer_turn_id.clone(),
                    reviewer_user_id: review_event.reviewer_user_id.clone(),
                    reviewer_agent_spec_id: review_event.reviewer_agent_spec_id.clone(),
                    event_kind: review_event.event_kind,
                    decision: review_event.decision,
                    feedback_text: review_event.feedback_text.clone(),
                    feedback: review_event.feedback.clone(),
                    confidence: review_event.confidence,
                    supersedes_review_event_id: review_event.supersedes_review_event_id.clone(),
                    next_task_run_turn_id: review_event.next_task_run_turn_id.clone(),
                    created_at: review_event.created_at,
                },
            )
            .await?;
            let row = task_result_review_event::find_review_event_by_id(
                &self.connection,
                &review_event.id,
            )
            .await?
            .context("task result review event missing after upsert")?;
            task_result_review_event_from_db_model(row)
        })
        .await
    }

    pub async fn get_task_result_review_event(
        &self,
        id: &str,
    ) -> Result<Option<TaskResultReviewEvent>> {
        task_result_review_event::find_review_event_by_id(&self.connection, id)
            .await?
            .map(task_result_review_event_from_db_model)
            .transpose()
    }

    pub async fn list_task_result_review_events(
        &self,
        candidate_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        task_result_review_event::list_review_events_by_candidate(&self.connection, candidate_id)
            .await?
            .into_iter()
            .map(task_result_review_event_from_db_model)
            .collect()
    }

    pub async fn list_task_result_review_events_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        task_result_review_event::list_review_events_by_run(&self.connection, run_id)
            .await?
            .into_iter()
            .map(task_result_review_event_from_db_model)
            .collect()
    }

    pub async fn get_task_run_child_anchor(&self, run_id: &str) -> Result<TaskRunChildAnchor> {
        let primary_binding = self.get_task_run_primary_thread_binding(run_id).await?;
        let accepted_candidate = self.get_accepted_task_result_candidate(run_id).await?;
        let accepted_turn = match accepted_candidate.as_ref() {
            Some(candidate) => {
                self.get_task_run_turn(candidate.task_run_turn_id.as_str())
                    .await?
            }
            None => None,
        };
        let latest_turn = self.get_latest_task_run_turn(run_id).await?;

        let target_anchor = TaskRunChildAnchor {
            child_thread_id: primary_binding
                .as_ref()
                .map(|binding| binding.thread_id.clone())
                .or_else(|| accepted_turn.as_ref().map(|turn| turn.thread_id.clone()))
                .or_else(|| latest_turn.as_ref().map(|turn| turn.thread_id.clone())),
            child_turn_id: accepted_turn
                .as_ref()
                .map(|turn| turn.turn_id.clone())
                .or_else(|| latest_turn.as_ref().map(|turn| turn.turn_id.clone())),
        };
        Ok(target_anchor)
    }

    pub async fn count_task_run_executions_for_run(&self, run_id: &str) -> Result<u64> {
        task_run_execution::count_executions_by_run(&self.connection, run_id).await
    }

    pub async fn load_task_runtime_invariant_snapshot(
        &self,
    ) -> Result<TaskRuntimeInvariantSnapshot> {
        let task_events = pioneer_entity::task_event::Entity::find()
            .order_by_asc(pioneer_entity::task_event::Column::TaskId)
            .order_by_asc(pioneer_entity::task_event::Column::Sequence)
            .all(&self.connection)
            .await
            .context("failed to load task events for runtime invariant scan")?
            .into_iter()
            .map(|row| TaskRuntimeInvariantEventRecord {
                id: row.id,
                task_id: row.task_id,
                run_id: row.run_id,
                sequence: row.sequence,
                event_type: row.event_type,
                payload_json: row.payload_json,
            })
            .collect::<Vec<_>>();

        let task_runs_by_id = pioneer_entity::task_run::Entity::find()
            .all(&self.connection)
            .await
            .context("failed to load task runs for runtime invariant scan")?
            .into_iter()
            .map(|row| (row.id.clone(), row))
            .collect::<HashMap<_, _>>();

        let delivered_task_results = pioneer_entity::task_delivery::Entity::find()
            .filter(pioneer_entity::task_delivery::Column::Status.eq("delivered"))
            .order_by_asc(pioneer_entity::task_delivery::Column::CreatedAt)
            .all(&self.connection)
            .await
            .context("failed to load delivered task deliveries for runtime invariant scan")?
            .into_iter()
            .map(|delivery| {
                let run = task_runs_by_id.get(delivery.run_id.as_str());
                TaskRuntimeInvariantDeliveryRecord {
                    delivery_id: delivery.id,
                    task_id: delivery.task_id,
                    run_id: delivery.run_id,
                    run_status: run.map(|run| run.status.clone()),
                    result_json: run.and_then(|run| run.result_json.clone()),
                }
            })
            .collect::<Vec<_>>();

        let in_progress_turns = pioneer_entity::turn::Entity::find()
            .filter(pioneer_entity::turn::Column::Status.eq("in_progress"))
            .order_by_asc(pioneer_entity::turn::Column::UpdatedAt)
            .all(&self.connection)
            .await
            .context("failed to load in-progress turns for runtime invariant scan")?
            .into_iter()
            .map(|row| TaskRuntimeInvariantTurnRecord {
                turn_id: row.id,
                thread_id: Some(row.thread_id),
                updated_at_unix: row.updated_at.timestamp(),
            })
            .collect::<Vec<_>>();

        let terminal_items = pioneer_entity::turn_item::Entity::find()
            .filter(pioneer_entity::turn_item::Column::Status.is_in([
                TURN_ITEM_STATUS_COMPLETED,
                TURN_ITEM_STATUS_FAILED,
                TURN_ITEM_STATUS_TIMED_OUT,
                TURN_ITEM_STATUS_CANCELLED,
            ]))
            .all(&self.connection)
            .await
            .context("failed to load terminal turn items for runtime invariant scan")?
            .into_iter()
            .filter_map(|row| {
                let status = row.status?;
                Some(((row.turn_id, row.item_id), status))
            })
            .collect::<HashMap<_, _>>();

        let stale_turn_item_attempts = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(
                pioneer_entity::turn_item_attempt::Column::Status
                    .is_in([ATTEMPT_STATUS_RUNNING, TURN_ITEM_STATUS_TIMED_OUT]),
            )
            .order_by_asc(pioneer_entity::turn_item_attempt::Column::TurnId)
            .order_by_asc(pioneer_entity::turn_item_attempt::Column::ItemId)
            .order_by_asc(pioneer_entity::turn_item_attempt::Column::AttemptNumber)
            .all(&self.connection)
            .await
            .context("failed to load turn item attempts for runtime invariant scan")?
            .into_iter()
            .filter_map(|attempt| {
                let key = (attempt.turn_id.clone(), attempt.item_id.clone());
                let item_status = terminal_items.get(&key)?;
                Some(TaskRuntimeInvariantStaleAttemptRecord {
                    turn_id: attempt.turn_id,
                    item_id: attempt.item_id,
                    item_status: item_status.clone(),
                    attempt_id: attempt.id,
                    attempt_status: attempt.status,
                    attempt_number: attempt.attempt_number,
                })
            })
            .collect::<Vec<_>>();

        Ok(TaskRuntimeInvariantSnapshot {
            task_events,
            delivered_task_results,
            in_progress_turns,
            stale_turn_item_attempts,
        })
    }

    pub async fn load_task_review_invariant_snapshot(
        &self,
    ) -> Result<Option<TaskReviewInvariantSnapshot>> {
        let thread_lineage = match pioneer_entity::thread_lineage::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantThreadLineageRecord {
                    child_thread_id: row.child_thread_id,
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task review thread lineage"),
        };
        let primary_bindings = match pioneer_entity::task_run_thread_binding::Entity::find()
            .filter(
                pioneer_entity::task_run_thread_binding::Column::BindingKind.eq("primary_executor"),
            )
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantBindingRecord {
                    id: row.id,
                    task_id: row.task_id,
                    run_id: row.run_id,
                    execution_id: row.execution_id,
                    thread_id: row.thread_id,
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => {
                return Err(error).context("failed to load task review thread bindings");
            }
        };
        let agent_specs = match pioneer_entity::task_agent_spec::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantAgentSpecRecord {
                    task_id: row.task_id,
                    run_id: row.run_id,
                    tool_policy_json: row.tool_policy_json,
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task review agent specs"),
        };
        let task_run_turns = match pioneer_entity::task_run_turn::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantTurnRecord {
                    id: row.id,
                    task_id: row.task_id,
                    run_id: row.run_id,
                    thread_id: row.thread_id,
                    turn_id: row.turn_id,
                    kind: row.kind,
                    round: i64::from(row.round),
                    sequence: i64::from(row.sequence),
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task review turns"),
        };
        let task_result_candidates = match pioneer_entity::task_result_candidate::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantCandidateRecord {
                    id: row.id,
                    task_id: row.task_id,
                    run_id: row.run_id,
                    task_run_turn_id: row.task_run_turn_id,
                    thread_id: row.thread_id,
                    turn_id: row.turn_id,
                    round: i64::from(row.round),
                    status: row.status,
                    result_json: row.result_json,
                    final_review_event_id: row.final_review_event_id,
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task result candidates"),
        };
        let task_result_review_events =
            match pioneer_entity::task_result_review_event::Entity::find()
                .all(&self.connection)
                .await
            {
                Ok(rows) => rows
                    .into_iter()
                    .map(|row| TaskReviewInvariantReviewEventRecord {
                        id: row.id,
                        candidate_id: row.candidate_id,
                        task_id: row.task_id,
                        run_id: row.run_id,
                        task_run_turn_id: row.task_run_turn_id,
                        decision: row.decision,
                    })
                    .collect(),
                Err(error) if is_missing_table_error(&error) => return Ok(None),
                Err(error) => {
                    return Err(error).context("failed to load task result review events");
                }
            };
        let task_runs = match pioneer_entity::task_run::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantRunRecord {
                    id: row.id,
                    task_id: row.task_id,
                    status: row.status,
                    result_json: row.result_json,
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task runs"),
        };
        let write_locks = match pioneer_entity::task_write_lock::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| TaskReviewInvariantWriteLockRecord {
                    task_id: row.task_id,
                    run_id: row.run_id,
                    status: row.status,
                    expires_at_unix: row.expires_at.map(|value| value.timestamp()),
                })
                .collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load task review write locks"),
        };
        let turn_ids = match pioneer_entity::turn::Entity::find()
            .all(&self.connection)
            .await
        {
            Ok(rows) => rows.into_iter().map(|row| row.id).collect(),
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(error).context("failed to load turn ids"),
        };

        Ok(Some(TaskReviewInvariantSnapshot {
            thread_lineage,
            primary_bindings,
            agent_specs,
            task_run_turns,
            task_result_candidates,
            task_result_review_events,
            task_runs,
            write_locks,
            turn_ids,
        }))
    }

    pub async fn claim_task_run_for_dispatch(
        &self,
        run_id: &str,
        claimed_at: i64,
    ) -> Result<Option<TaskRun>> {
        self.run_serialized_write(|| {
            self.claim_task_run_for_dispatch_once(run_id.to_owned(), claimed_at)
        })
        .await
    }

    async fn claim_task_run_for_dispatch_once(
        &self,
        run_id: String,
        claimed_at: i64,
    ) -> Result<Option<TaskRun>> {
        task_run::claim_run_for_dispatch(
            &self.connection,
            run_id.as_str(),
            unix_to_datetime(claimed_at),
        )
        .await?
        .map(task_run_from_db_model)
        .transpose()
    }

    pub async fn append_task_run_started_once(
        &self,
        task_id: String,
        run_id: String,
        started_at: i64,
    ) -> Result<Option<AppendedTaskEvent>> {
        self.run_serialized_write(|| {
            self.append_task_run_started_once_inner(task_id.clone(), run_id.clone(), started_at)
        })
        .await
    }

    async fn append_task_run_started_once_inner(
        &self,
        task_id: String,
        run_id: String,
        started_at: i64,
    ) -> Result<Option<AppendedTaskEvent>> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task run started transaction")?;

        let Some(run_model) = task_run::find_run_by_id(&transaction, run_id.as_str()).await? else {
            transaction
                .rollback()
                .await
                .context("failed to rollback missing task run started transaction")?;
            return Ok(None);
        };
        let Some(status) = task_run_status_from_db(run_model.status.as_str()) else {
            transaction
                .rollback()
                .await
                .context("failed to rollback invalid task run started transaction")?;
            anyhow::bail!(
                "task run `{}` has unknown status `{}`",
                run_id,
                run_model.status
            );
        };
        if matches!(status, TaskRunStatus::Running) || status.is_terminal() {
            transaction
                .rollback()
                .await
                .context("failed to rollback duplicate task run started transaction")?;
            return Ok(None);
        }
        if !matches!(status, TaskRunStatus::Queued | TaskRunStatus::Starting) {
            transaction
                .rollback()
                .await
                .context("failed to rollback non-startable task run started transaction")?;
            return Ok(None);
        }

        let created_at = unix_to_datetime(started_at);
        let payload = TaskEventPayload::RunStarted {
            task_id,
            run_id: run_id.clone(),
            started_at,
        };
        let idempotency_key = payload.idempotency_key();
        let mut appended_event = match task_event::append_event(
            &transaction,
            &payload,
            created_at,
            idempotency_key.as_deref(),
        )
        .await
        {
            Ok(event) => event,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if appended_event.append_status.is_inserted() {
            if let Err(error) = self
                .task_projector
                .project(&transaction, &appended_event)
                .await
                .context("failed to project task run started event")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        if let Err(error) = hydrate_task_event_metadata(&transaction, &mut appended_event)
            .await
            .context("failed to hydrate task run started event metadata")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        transaction
            .commit()
            .await
            .context("failed to commit task run started transaction")?;

        Ok(Some(appended_event))
    }

    pub async fn list_task_runs_by_status(
        &self,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        task_run::list_runs_by_status(&self.connection, status, limit)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn list_due_retry_task_runs(&self, now: i64, limit: u64) -> Result<Vec<TaskRun>> {
        task_run::list_due_retry_runs(&self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn list_task_write_locks_by_run(&self, run_id: &str) -> Result<Vec<TaskWriteLock>> {
        task_write_lock::list_locks_by_run(&self.connection, run_id)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect()
    }

    pub async fn list_active_task_write_locks(
        &self,
        workspace_id: &str,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        task_write_lock::list_active_locks_for_workspace(
            &self.connection,
            workspace_id,
            unix_to_datetime(now),
            limit,
        )
        .await?
        .into_iter()
        .map(task_write_lock_from_db_model)
        .collect()
    }

    pub async fn list_stale_task_write_locks(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        task_write_lock::list_stale_locks(&self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect()
    }

    pub async fn list_due_active_task_triggers(&self, now: i64) -> Result<Vec<TaskTrigger>> {
        task_trigger::list_due_active_triggers(&self.connection, unix_to_datetime(now))
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect()
    }

    pub async fn list_active_task_triggers(&self) -> Result<Vec<TaskTrigger>> {
        task_trigger::list_active_triggers(&self.connection)
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect()
    }

    pub async fn get_task_delivery(&self, delivery_id: &str) -> Result<Option<TaskDelivery>> {
        task_delivery::find_delivery_by_id(&self.connection, delivery_id)
            .await?
            .map(task_delivery_from_db_model)
            .transpose()
    }

    pub async fn list_due_task_deliveries(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskDelivery>> {
        task_delivery::list_due_deliveries(&self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_delivery_from_db_model)
            .collect()
    }

    pub async fn list_stuck_task_deliveries(
        &self,
        before: i64,
        limit: u64,
    ) -> Result<Vec<TaskDelivery>> {
        task_delivery::list_stuck_deliveries(&self.connection, unix_to_datetime(before), limit)
            .await?
            .into_iter()
            .map(task_delivery_from_db_model)
            .collect()
    }

    pub async fn list_task_deliveries(
        &self,
        params: TaskDeliveriesParams,
    ) -> Result<TaskDeliveriesResponse> {
        let deliveries = task_delivery::list_deliveries(&self.connection, &params)
            .await?
            .into_iter()
            .map(task_delivery_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let delivery_ids = deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        let attempts = task_delivery::list_attempts_for_deliveries(&self.connection, &delivery_ids)
            .await?
            .into_iter()
            .map(task_delivery_attempt_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        Ok(TaskDeliveriesResponse {
            deliveries,
            attempts,
        })
    }

    pub async fn list_task_agenda(&self, params: TaskAgendaParams) -> Result<TaskAgendaResponse> {
        let limit = params.limit.unwrap_or(100).max(1).min(500);
        let tasks = self
            .list_tasks(TaskListParams {
                workspace_id: params.workspace_id.clone(),
                owner_kind: params.owner_kind,
                owner_id: params.owner_id.clone(),
                parent_task_id: None,
                root_task_id: None,
                status: None,
                limit: None,
            })
            .await?;
        let mut items = Vec::new();
        for task in tasks {
            if !params.statuses.is_empty() && !params.statuses.contains(&task.status) {
                continue;
            }
            if !params.include_completed && task.status.is_terminal() {
                continue;
            }
            let triggers = task_trigger::list_triggers_by_task(&self.connection, task.id.as_str())
                .await?
                .into_iter()
                .map(task_trigger_from_db_model)
                .collect::<Result<Vec<_>>>()?;
            let trigger = triggers.iter().rev().find(|trigger| {
                if !params.include_paused
                    && trigger.status == pioneer_protocol::TaskTriggerStatus::Paused
                {
                    return false;
                }
                if !params.trigger_kinds.is_empty()
                    && !params.trigger_kinds.contains(&trigger.kind())
                {
                    return false;
                }
                if let Some(from) = params.from
                    && trigger.next_fire_at.is_some_and(|next| next < from)
                {
                    return false;
                }
                if let Some(to) = params.to
                    && trigger.next_fire_at.is_some_and(|next| next > to)
                {
                    return false;
                }
                true
            });
            let Some(trigger) = trigger.cloned() else {
                continue;
            };
            let latest_run = task_run::list_runs_by_task(&self.connection, task.id.as_str())
                .await?
                .into_iter()
                .map(task_run_from_db_model)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .last();
            let latest_delivery =
                task_delivery::list_deliveries_for_task(&self.connection, task.id.as_str())
                    .await?
                    .into_iter()
                    .map(task_delivery_from_db_model)
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .last();
            let result = latest_run
                .as_ref()
                .and_then(|run| run.result.as_ref())
                .or(task.result.as_ref());
            let error = latest_run
                .as_ref()
                .and_then(|run| run.error.as_ref())
                .or(task.error.as_ref());
            items.push(TaskAgendaItem {
                goal_preview: Some(bounded_preview(task.goal.as_str(), 240)),
                trigger_kind: Some(trigger.kind()),
                trigger_status: Some(trigger.status),
                next_fire_at: trigger.next_fire_at,
                last_fire_at: trigger.last_fire_at,
                timezone: trigger_timezone(&trigger.spec),
                recurring: matches!(
                    trigger.kind(),
                    TaskTriggerKind::Interval | TaskTriggerKind::Cron
                ),
                delivery_mode: task
                    .delivery_policy
                    .as_ref()
                    .map(|policy| policy.mode)
                    .unwrap_or(pioneer_protocol::TaskDeliveryMode::None),
                result_preview: result.and_then(|result| result.summary.clone()),
                error_preview: error.map(|error| bounded_preview(error.message.as_str(), 240)),
                task,
                trigger: Some(trigger),
                latest_run,
                latest_delivery,
            });
        }
        items.sort_by(|left, right| {
            left.next_fire_at
                .unwrap_or(i64::MAX)
                .cmp(&right.next_fire_at.unwrap_or(i64::MAX))
                .then_with(|| left.task.created_at.cmp(&right.task.created_at))
        });
        items.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(TaskAgendaResponse { items })
    }

    pub async fn get_task_thread_lineage(
        &self,
        child_thread_id: &str,
    ) -> Result<Option<TaskThreadLineage>> {
        let row =
            thread_lineage::find_lineage_by_child_thread(&self.connection, child_thread_id).await?;
        Ok(row.map(task_thread_lineage_from_db_model))
    }

    pub async fn list_task_thread_lineage_for_parent(
        &self,
        parent_thread_id: &str,
    ) -> Result<Vec<TaskThreadLineage>> {
        let rows =
            thread_lineage::list_children_for_parent_thread(&self.connection, parent_thread_id)
                .await?;
        Ok(rows
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect())
    }

    pub async fn list_task_thread_lineage_by_root_thread(
        &self,
        root_thread_id: &str,
    ) -> Result<Vec<TaskThreadLineage>> {
        let rows =
            thread_lineage::list_lineage_by_root_thread(&self.connection, root_thread_id).await?;
        Ok(rows
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect())
    }

    pub async fn get_turn(&self, thread_id: &str, turn_id: &str) -> Result<Option<(String, Turn)>> {
        let Some(thread_model) = thread::find_thread_by_id(&self.connection, thread_id).await?
        else {
            return Ok(None);
        };

        let Some(turn_model) =
            turn::find_turn_by_thread_and_id(&self.connection, thread_id, turn_id).await?
        else {
            return Ok(None);
        };

        let Some(status) = turn_status_from_db(turn_model.status.as_str()) else {
            return Ok(None);
        };

        let prompt_manifest = parse_turn_prompt_manifest(&turn_model)?;
        let permission_profile = parse_turn_permission_profile(&turn_model)?;

        Ok(Some((
            thread_model.workspace_id,
            Turn {
                id: turn_model.id,
                status,
                turn_kind: turn_kind_from_db(turn_model.turn_kind.as_str()).unwrap_or_default(),
                origin: turn_origin_from_db(turn_model.origin.as_str()).unwrap_or_default(),
                error: turn_model.error,
                prompt_manifest,
                permission_profile,
            },
        )))
    }

    pub async fn complete_in_progress_turns_after_final_agent_message(
        &self,
        event_timestamp_secs: i64,
    ) -> Result<u64> {
        let turns = pioneer_entity::turn::Entity::find()
            .filter(
                pioneer_entity::turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)),
            )
            .all(&self.connection)
            .await
            .context("failed to query in-progress turns for final-agent-message repair")?;

        let mut completed = 0u64;
        for turn_model in turns {
            let Some(latest_event) =
                turn_event::latest_event_for_turn(&self.connection, turn_model.id.as_str()).await?
            else {
                continue;
            };
            let TurnEventPayload::ItemCompleted(notification) = latest_event.payload else {
                continue;
            };
            if !matches!(&notification.item, TurnItem::AgentMessage { .. }) {
                continue;
            }
            let Some(thread_model) =
                thread::find_thread_by_id(&self.connection, turn_model.thread_id.as_str()).await?
            else {
                continue;
            };

            let prompt_manifest = parse_turn_prompt_manifest(&turn_model)?;
            let permission_profile = parse_turn_permission_profile(&turn_model)?;
            self.materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: thread_model.workspace_id,
                    thread_id: turn_model.thread_id.clone(),
                    turn: Turn {
                        id: turn_model.id,
                        status: TurnStatus::Completed,
                        turn_kind: turn_kind_from_db(turn_model.turn_kind.as_str())
                            .unwrap_or_default(),
                        origin: turn_origin_from_db(turn_model.origin.as_str()).unwrap_or_default(),
                        error: None,
                        prompt_manifest,
                        permission_profile,
                    },
                },
                event_timestamp_secs,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to materialize completion repair for turn `{}`",
                    notification.turn_id
                )
            })?;
            completed = completed.saturating_add(1);
        }

        Ok(completed)
    }

    pub async fn get_turn_inputs(&self, turn_id: &str) -> Result<Vec<UserInput>> {
        let rows = turn::find_turn_inputs(&self.connection, turn_id).await?;
        let mut inputs = Vec::with_capacity(rows.len());

        for row in rows {
            match serde_json::from_str::<UserInput>(row.payload.as_str()) {
                Ok(input) => inputs.push(input),
                Err(error) if row.input_type == "text" => {
                    if let Some(text) = row.text {
                        inputs.push(UserInput::Text {
                            text,
                            text_elements: Vec::new(),
                        });
                    } else {
                        return Err(error)
                            .with_context(|| format!("failed to decode turn input `{}`", row.id));
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to decode turn input `{}`", row.id));
                }
            }
        }

        Ok(inputs)
    }

    pub async fn update_turn_prompt_manifest(
        &self,
        thread_id: &str,
        turn_id: &str,
        manifest: &PromptManifest,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        let manifest_columns = build_turn_prompt_manifest_columns(manifest)?;
        self.run_serialized_write(|| async {
            turn::update_turn_prompt_manifest(
                &self.connection,
                thread_id,
                turn_id,
                &manifest_columns,
                unix_to_datetime(event_timestamp_secs),
            )
            .await
        })
        .await
    }

    pub async fn update_turn_status(
        &self,
        thread_id: &str,
        turn_id: &str,
        status: TurnStatus,
        error: Option<&str>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            let updated_at = unix_to_datetime(event_timestamp_secs);
            let updated = turn::update_turn_status(
                &self.connection,
                thread_id,
                turn_id,
                status,
                error,
                updated_at,
            )
            .await?;
            if updated {
                turn::append_turn_status_history(
                    &self.connection,
                    turn_id,
                    status,
                    error.map(str::to_owned),
                    updated_at,
                )
                .await?;
            }
            Ok(updated)
        })
        .await
    }

    pub async fn get_turn_location(&self, turn_id: &str) -> Result<Option<(String, String)>> {
        let Some(turn_model) = turn::find_turn_by_id(&self.connection, turn_id).await? else {
            return Ok(None);
        };
        let Some(thread_model) =
            thread::find_thread_by_id(&self.connection, turn_model.thread_id.as_str()).await?
        else {
            return Ok(None);
        };
        Ok(Some((turn_model.thread_id, thread_model.workspace_id)))
    }

    pub async fn get_turn_item_type(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<TurnItemType>> {
        let value = turn::find_turn_item_type(&self.connection, turn_id, item_id).await?;
        Ok(match value {
            Some(value) => Some(
                turn_item_type_from_db(value.as_str()).unwrap_or(TurnItemType::DynamicToolCall),
            ),
            None => None,
        })
    }

    pub async fn get_turn_item(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<pioneer_protocol::TurnItem>> {
        let Some(model) = turn::find_turn_item(&self.connection, turn_id, item_id).await? else {
            return Ok(None);
        };
        let parsed = serde_json::from_str::<pioneer_protocol::TurnItem>(model.payload.as_str())
            .with_context(|| {
                format!("failed to decode turn_item payload for turn `{turn_id}` item `{item_id}`")
            })?;
        Ok(Some(parsed))
    }

    pub async fn list_turn_items_by_type(
        &self,
        turn_id: &str,
        item_type: &str,
    ) -> Result<Vec<TurnItem>> {
        let rows = turn::list_turn_items_by_type(&self.connection, turn_id, item_type).await?;
        rows.into_iter()
            .map(|model| {
                serde_json::from_str::<TurnItem>(model.payload.as_str()).with_context(|| {
                    format!(
                        "failed to decode turn item payload for turn `{turn_id}` item `{}`",
                        model.item_id
                    )
                })
            })
            .collect()
    }

    pub async fn list_completed_agent_messages(&self, turn_id: &str) -> Result<Vec<TurnItem>> {
        let rows = turn::find_completed_turn_items(&self.connection, turn_id).await?;
        rows.into_iter()
            .map(|model| {
                serde_json::from_str::<TurnItem>(model.payload.as_str()).with_context(|| {
                    format!(
                        "failed to decode completed agent message for turn `{turn_id}` item `{}`",
                        model.item_id
                    )
                })
            })
            .collect()
    }

    pub async fn get_turn_item_events(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnItemsResponse>> {
        let Some(thread_model) = thread::find_thread_by_id(&self.connection, thread_id).await?
        else {
            return Ok(None);
        };

        let workspace_id = thread_model.workspace_id;

        let events_rows =
            turn_event::list_events_for_turn(&self.connection, thread_id, turn_id).await?;

        let mut events = Vec::new();
        let mut last_sequence = 0i64;
        let mut latest_agent_diff_payload_by_item_id = HashMap::<String, String>::new();

        for row in events_rows {
            last_sequence = row.sequence;

            let payload = serde_json::from_str::<TurnEventPayload>(row.payload.as_str())
                .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;

            let mapped_payload = match payload {
                TurnEventPayload::ItemStarted(notification) => TurnItemEventPayload::ItemStarted {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                },
                TurnEventPayload::ItemCompleted(notification) => {
                    remember_agent_diff_snapshot_payload(
                        &mut latest_agent_diff_payload_by_item_id,
                        &notification.item,
                    )?;
                    TurnItemEventPayload::ItemCompleted {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemUpdated(notification) => {
                    remember_agent_diff_snapshot_payload(
                        &mut latest_agent_diff_payload_by_item_id,
                        &notification.item,
                    )?;
                    TurnItemEventPayload::ItemUpdated {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemTimeoutDetected(notification) => {
                    TurnItemEventPayload::ItemTimeoutDetected {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        attempt_number: notification.attempt_number,
                        reason: notification.reason,
                        recovery_job_id: notification.recovery_job_id,
                    }
                }
                TurnEventPayload::ItemRecoveryOpened(notification) => {
                    TurnItemEventPayload::ItemRecoveryOpened {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        trigger: notification.trigger,
                        action: notification.action,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoveryAttached(notification) => {
                    TurnItemEventPayload::ItemRecoveryAttached {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        recovery_item_id: notification.recovery_item_id,
                        recovery_item_type: notification.recovery_item_type,
                        trigger: notification.trigger,
                        action: notification.action,
                        existing_status: notification.existing_status,
                        next_attempt_number: notification.next_attempt_number,
                    }
                }
                TurnEventPayload::ItemRetryScheduled(notification) => {
                    TurnItemEventPayload::ItemRetryScheduled {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                        next_run_at_unix: notification.next_run_at_unix,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemRetryAttemptStarted(notification) => {
                    TurnItemEventPayload::ItemRetryAttemptStarted {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoverySucceeded(notification) => {
                    TurnItemEventPayload::ItemRecoverySucceeded {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoveryExhausted(notification) => {
                    TurnItemEventPayload::ItemRecoveryExhausted {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                        status: notification.status,
                        error_message: notification.error_message,
                    }
                }
                TurnEventPayload::ItemToolRetryScheduled(notification) => {
                    TurnItemEventPayload::ItemToolRetryScheduled {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        error_class: notification.error_class,
                        retry_hint: notification.retry_hint,
                        budgets: notification.budgets,
                        failure_signature_fingerprint: notification.failure_signature_fingerprint,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemToolRetryResolved(notification) => {
                    TurnItemEventPayload::ItemToolRetryResolved {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        resolution: notification.resolution,
                        budgets: notification.budgets,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemToolRetryExhausted(notification) => {
                    TurnItemEventPayload::ItemToolRetryExhausted {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        error_class: notification.error_class,
                        exhaustion_kind: notification.exhaustion_kind,
                        budgets: notification.budgets,
                        failure_signature_fingerprint: notification.failure_signature_fingerprint,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::TurnToolLoopBudgetExceeded(notification) => {
                    TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        limit_kind: notification.limit_kind,
                        limit: notification.limit,
                        observed: notification.observed,
                        action: notification.action,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::TurnExecutionWindowStarted(notification) => {
                    TurnItemEventPayload::TurnExecutionWindowStarted(notification)
                }
                TurnEventPayload::TurnExecutionWindowExhausted(notification) => {
                    TurnItemEventPayload::TurnExecutionWindowExhausted(notification)
                }
                TurnEventPayload::TurnExecutionWindowCheckpointed(notification) => {
                    TurnItemEventPayload::TurnExecutionWindowCheckpointed(notification)
                }
                TurnEventPayload::TurnExecutionWindowContinued(notification) => {
                    TurnItemEventPayload::TurnExecutionWindowContinued(notification)
                }
                TurnEventPayload::TurnExecutionWindowBlocked(notification) => {
                    TurnItemEventPayload::TurnExecutionWindowBlocked(notification)
                }
                TurnEventPayload::TurnPermissionAudit(event) => {
                    TurnItemEventPayload::TurnPermissionAudit(event)
                }
                TurnEventPayload::TurnStarted(_)
                | TurnEventPayload::TurnCompleted(_)
                | TurnEventPayload::TurnFailed(_)
                | TurnEventPayload::TurnBlocked(_) => continue,
            };

            events.push(TurnItemEvent {
                sequence: row.sequence,
                created_at: row.created_at.timestamp_millis(),
                payload: mapped_payload,
            });
        }

        append_agent_diff_snapshot_turn_item_events(
            &self.connection,
            &mut events,
            &latest_agent_diff_payload_by_item_id,
            workspace_id.as_str(),
            thread_id,
            turn_id,
            last_sequence,
        )
        .await?;

        Ok(Some(TurnItemsResponse {
            thread_id: thread_id.to_owned(),
            workspace_id,
            turn_id: turn_id.to_owned(),
            events,
            last_sequence,
        }))
    }

    pub async fn compact_superseded_agent_diff_turn_events(
        &self,
        batch_limit: u64,
        dry_run: bool,
    ) -> Result<TurnEventCompactionSummary> {
        let batch_limit = batch_limit.max(1);
        let stats = self.agent_diff_turn_event_compaction_stats().await?;
        let candidates = self
            .superseded_agent_diff_turn_event_candidates(batch_limit)
            .await?;
        let turns_touched = candidates
            .iter()
            .map(|candidate| candidate.turn_id.as_str())
            .collect::<HashSet<_>>()
            .len() as u64;
        let mut summary = TurnEventCompactionSummary {
            dry_run,
            batch_limit,
            candidate_rows: candidates.len() as u64,
            deleted_rows: 0,
            payload_bytes: candidates
                .iter()
                .map(|candidate| candidate.payload_bytes)
                .sum(),
            turns_touched,
            latest_snapshots_kept: stats.latest_snapshots_kept,
            skipped_unprojected: stats.skipped_unprojected,
            skipped_failed: stats.skipped_failed,
        };

        if dry_run || candidates.is_empty() {
            return Ok(summary);
        }

        let deleted_rows = self
            .run_serialized_write(|| {
                let candidates = candidates.clone();
                let connection = self.connection.clone();
                async move {
                    let transaction = connection
                        .begin()
                        .await
                        .context("failed to begin turn_event compaction transaction")?;

                    let result: Result<u64> = async {
                        let mut deleted_rows = 0u64;
                        for candidate in candidates {
                            turn_event_projection_state::delete_by_event_id(
                                &transaction,
                                candidate.event_id.as_str(),
                            )
                            .await?;
                            deleted_rows = deleted_rows.saturating_add(
                                turn_event::delete_event_by_id(
                                    &transaction,
                                    candidate.event_id.as_str(),
                                )
                                .await?,
                            );
                        }
                        Ok(deleted_rows)
                    }
                    .await;

                    match result {
                        Ok(deleted_rows) => {
                            transaction
                                .commit()
                                .await
                                .context("failed to commit turn_event compaction transaction")?;
                            Ok(deleted_rows)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await?;

        summary.deleted_rows = deleted_rows;
        Ok(summary)
    }

    async fn superseded_agent_diff_turn_event_candidates(
        &self,
        batch_limit: u64,
    ) -> Result<Vec<AgentDiffCompactionCandidate>> {
        let rows = self
            .connection
            .query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                r#"
WITH ranked_diff_events AS (
    SELECT
        e.id AS event_id,
        e.turn_id AS turn_id,
        e.sequence AS sequence,
        json_extract(e.payload, '$.payload.item.id') AS item_id,
        length(e.payload) AS payload_bytes,
        COALESCE(ps.status, 'missing') AS projection_status,
        ROW_NUMBER() OVER (
            PARTITION BY e.turn_id, json_extract(e.payload, '$.payload.item.id')
            ORDER BY e.sequence DESC, e.id DESC
        ) AS row_rank
    FROM turn_event e
    LEFT JOIN turn_event_projection_state ps ON ps.event_id = e.id
    WHERE json_extract(e.payload, '$.kind') = 'item_completed'
      AND json_extract(e.payload, '$.payload.item.type') = 'systemEvent'
      AND json_extract(e.payload, '$.payload.item.code') = 'agent_diff_updated'
      AND json_extract(e.payload, '$.payload.item.id') IS NOT NULL
)
SELECT event_id, turn_id, item_id, payload_bytes
FROM ranked_diff_events
WHERE row_rank > 1
  AND projection_status = 'projected'
ORDER BY turn_id ASC, sequence ASC
LIMIT ?
"#,
                [batch_limit.into()],
            ))
            .await
            .context("failed to query superseded agent diff turn events")?;

        rows.into_iter()
            .map(|row| {
                let event_id = row
                    .try_get::<String>("", "event_id")
                    .context("failed to decode compactable turn_event id")?;
                let turn_id = row
                    .try_get::<String>("", "turn_id")
                    .context("failed to decode compactable turn_event turn id")?;
                let item_id = row
                    .try_get::<String>("", "item_id")
                    .context("failed to decode compactable turn_event item id")?;
                let payload_bytes = row
                    .try_get::<i64>("", "payload_bytes")
                    .context("failed to decode compactable turn_event payload size")?
                    .max(0) as u64;
                Ok(AgentDiffCompactionCandidate {
                    event_id,
                    turn_id,
                    item_id,
                    payload_bytes,
                })
            })
            .collect()
    }

    async fn agent_diff_turn_event_compaction_stats(&self) -> Result<AgentDiffCompactionStats> {
        let Some(row) = self
            .connection
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"
WITH ranked_diff_events AS (
    SELECT
        e.id AS event_id,
        e.turn_id AS turn_id,
        e.sequence AS sequence,
        json_extract(e.payload, '$.payload.item.id') AS item_id,
        COALESCE(ps.status, 'missing') AS projection_status,
        ROW_NUMBER() OVER (
            PARTITION BY e.turn_id, json_extract(e.payload, '$.payload.item.id')
            ORDER BY e.sequence DESC, e.id DESC
        ) AS row_rank
    FROM turn_event e
    LEFT JOIN turn_event_projection_state ps ON ps.event_id = e.id
    WHERE json_extract(e.payload, '$.kind') = 'item_completed'
      AND json_extract(e.payload, '$.payload.item.type') = 'systemEvent'
      AND json_extract(e.payload, '$.payload.item.code') = 'agent_diff_updated'
      AND json_extract(e.payload, '$.payload.item.id') IS NOT NULL
)
SELECT
    COALESCE(SUM(CASE WHEN row_rank = 1 THEN 1 ELSE 0 END), 0) AS latest_snapshots_kept,
    COALESCE(SUM(CASE
        WHEN row_rank > 1
         AND projection_status IN ('pending', 'projecting', 'missing')
        THEN 1 ELSE 0 END), 0) AS skipped_unprojected,
    COALESCE(SUM(CASE
        WHEN row_rank > 1
         AND projection_status IN ('failed', 'exhausted')
        THEN 1 ELSE 0 END), 0) AS skipped_failed
FROM ranked_diff_events
"#
                .to_owned(),
            ))
            .await
            .context("failed to query agent diff compaction stats")?
        else {
            return Ok(AgentDiffCompactionStats::default());
        };

        Ok(AgentDiffCompactionStats {
            latest_snapshots_kept: row
                .try_get::<i64>("", "latest_snapshots_kept")
                .context("failed to decode latest agent diff snapshots kept")?
                .max(0) as u64,
            skipped_unprojected: row
                .try_get::<i64>("", "skipped_unprojected")
                .context("failed to decode skipped unprojected agent diff snapshots")?
                .max(0) as u64,
            skipped_failed: row
                .try_get::<i64>("", "skipped_failed")
                .context("failed to decode skipped failed agent diff snapshots")?
                .max(0) as u64,
        })
    }

    pub async fn get_thread_conversation_history(
        &self,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_conversation_history_inner(None, thread_id, max_turns)
            .await
    }

    pub async fn get_thread_conversation_history_with_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_conversation_history_inner(Some(workspace_id), thread_id, max_turns)
            .await
    }

    async fn get_thread_conversation_history_inner(
        &self,
        workspace_id: Option<&str>,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        let turns =
            turn::find_terminal_turns_for_thread(&self.connection, thread_id, max_turns as u64)
                .await?;
        let artifact_refs = if let Some(workspace_id) = workspace_id {
            let turn_ids = turns
                .iter()
                .map(|turn_model| turn_model.id.clone())
                .collect::<Vec<_>>();
            self.list_conversation_artifact_refs(
                workspace_id,
                thread_id,
                &turn_ids,
                ConversationArtifactRefLimits::default(),
            )
            .await
            .unwrap_or_default()
        } else {
            BTreeMap::new()
        };

        let mut entries = Vec::with_capacity(turns.len());

        for turn_model in &turns {
            let inputs = turn::find_turn_inputs(&self.connection, &turn_model.id).await?;
            let user_text: String = inputs
                .iter()
                .filter(|i| i.input_type == "text")
                .filter_map(|i| i.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n");

            let items = turn::find_completed_turn_items(&self.connection, &turn_model.id).await?;
            let assistant_text: String = items
                .iter()
                .filter_map(|item| {
                    serde_json::from_str::<pioneer_protocol::TurnItem>(&item.payload).ok()
                })
                .filter_map(|item| match item {
                    pioneer_protocol::TurnItem::AgentMessage { text, .. } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            let refs = artifact_refs
                .get(&turn_model.id)
                .cloned()
                .unwrap_or_default();

            entries.push(ConversationEntry {
                turn_id: turn_model.id.clone(),
                user_text: if user_text.is_empty() {
                    None
                } else {
                    Some(user_text)
                },
                assistant_text: if assistant_text.is_empty() {
                    None
                } else {
                    Some(assistant_text)
                },
                user_artifacts: refs.user,
                assistant_artifacts: refs.assistant,
            });
        }

        Ok(entries)
    }

    pub async fn get_first_thread_user_text(&self, thread_id: &str) -> Result<Option<String>> {
        let Some(turn_model) =
            turn::find_oldest_turn_for_thread(&self.connection, thread_id).await?
        else {
            return Ok(None);
        };

        let inputs = turn::find_turn_inputs(&self.connection, &turn_model.id).await?;
        for input in inputs {
            if input.input_type != "text" {
                continue;
            }
            if let Some(text) = input.text {
                let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
                if !normalized.is_empty() {
                    return Ok(Some(normalized));
                }
            }
        }

        Ok(None)
    }

    pub async fn replace_turn_skill_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnSkillBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            turn_skill_binding::replace_turn_skill_bindings(
                &self.connection,
                turn_id,
                bindings,
                unix_to_datetime(event_timestamp_secs),
            )
            .await
        })
        .await
    }

    pub async fn list_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        let rows = turn_skill_binding::list_turn_skill_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(|row| TurnSkillBindingRecord {
                skill_slug: row.skill_slug,
                skill_version: row.skill_version,
                fingerprint: row.fingerprint,
                source_kind: row.source_kind,
                resolved_reason: row.resolved_reason,
            })
            .collect())
    }

    pub async fn find_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        let rows = turn_skill_binding::find_turn_skill_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(|row| TurnSkillBindingRecord {
                skill_slug: row.skill_slug,
                skill_version: row.skill_version,
                fingerprint: row.fingerprint,
                source_kind: row.source_kind,
                resolved_reason: row.resolved_reason,
            })
            .collect())
    }

    pub async fn list_workspace_skill_policies(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceSkillPolicyRecord>> {
        let rows =
            skill_workspace_policy::list_workspace_skill_policies(&self.connection, workspace_id)
                .await?;
        Ok(rows
            .into_iter()
            .map(|row| WorkspaceSkillPolicyRecord {
                workspace_id: row.workspace_id,
                skill_slug: row.skill_slug,
                source_kind: row.source_kind,
                enabled: row.enabled,
                allow_implicit_invocation: row.allow_implicit_invocation,
            })
            .collect())
    }

    pub async fn upsert_workspace_skill_policy(
        &self,
        record: &WorkspaceSkillPolicyRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            skill_workspace_policy::upsert_workspace_skill_policy(
                &self.connection,
                record,
                now,
                now,
            )
            .await
        })
        .await
    }

    pub async fn delete_workspace_skill_policy(
        &self,
        workspace_id: &str,
        skill_slug: &str,
        source_kind: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            skill_workspace_policy::delete_workspace_skill_policy(
                &self.connection,
                workspace_id,
                skill_slug,
                source_kind,
            )
            .await
        })
        .await
    }

    pub async fn upsert_skill_installation(
        &self,
        record: &SkillInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            skill_installation::upsert_skill_installation(&self.connection, record, now, now).await
        })
        .await
    }

    pub async fn delete_skill_installation(
        &self,
        slug: &str,
        source_kind: &str,
        scope_key: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            skill_installation::delete_skill_installation(
                &self.connection,
                slug,
                source_kind,
                scope_key,
            )
            .await
        })
        .await
    }

    pub async fn find_skill_installation(
        &self,
        slug: &str,
        source_kind: &str,
        scope_key: &str,
    ) -> Result<Option<SkillInstallationRecord>> {
        let row = skill_installation::find_skill_installation(
            &self.connection,
            slug,
            source_kind,
            scope_key,
        )
        .await?;
        Ok(row.map(|model| SkillInstallationRecord {
            slug: model.slug,
            version: model.version,
            source_kind: model.source_kind,
            scope_key: model.scope_key,
            source_ref: model.source_ref,
            install_path: model.install_path,
            trust_level: model.trust_level,
            fingerprint: model.fingerprint,
            updated_at_unix: model.updated_at.timestamp(),
        }))
    }

    pub async fn list_skill_installations(&self) -> Result<Vec<SkillInstallationRecord>> {
        let rows = skill_installation::list_skill_installations(&self.connection).await?;
        Ok(rows
            .into_iter()
            .map(|model| SkillInstallationRecord {
                slug: model.slug,
                version: model.version,
                source_kind: model.source_kind,
                scope_key: model.scope_key,
                source_ref: model.source_ref,
                install_path: model.install_path,
                trust_level: model.trust_level,
                fingerprint: model.fingerprint,
                updated_at_unix: model.updated_at.timestamp(),
            })
            .collect())
    }

    pub async fn upsert_skill_upload_session(
        &self,
        record: &SkillUploadSessionRecord,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let created_at = unix_to_datetime(record.created_at_unix);
            let updated_at = unix_to_datetime(record.created_at_unix);
            skill_upload_session::upsert_skill_upload_session(
                &self.connection,
                record,
                created_at,
                updated_at,
            )
            .await
        })
        .await
    }

    pub async fn find_skill_upload_session(
        &self,
        upload_id: &str,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        let row =
            skill_upload_session::find_skill_upload_session(&self.connection, upload_id).await?;
        Ok(row.map(skill_upload_session_record_from_model))
    }

    pub async fn update_skill_upload_received_bytes(
        &self,
        upload_id: &str,
        received_bytes: u64,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.run_serialized_write(|| async {
            let updated_at = unix_to_datetime(updated_at_unix);
            let row = skill_upload_session::update_skill_upload_received_bytes(
                &self.connection,
                upload_id,
                received_bytes,
                updated_at,
            )
            .await?;
            Ok(row.map(skill_upload_session_record_from_model))
        })
        .await
    }

    pub async fn update_skill_upload_status(
        &self,
        upload_id: &str,
        status: &str,
        finalized_at_unix: Option<i64>,
        consumed_at_unix: Option<i64>,
        aborted_at_unix: Option<i64>,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.run_serialized_write(|| async {
            let updated_at = unix_to_datetime(updated_at_unix);
            let row = skill_upload_session::update_skill_upload_status(
                &self.connection,
                upload_id,
                status,
                finalized_at_unix,
                consumed_at_unix,
                aborted_at_unix,
                updated_at,
            )
            .await?;
            Ok(row.map(skill_upload_session_record_from_model))
        })
        .await
    }

    pub async fn list_expired_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        let rows =
            skill_upload_session::list_expired_skill_upload_sessions(&self.connection, now_unix)
                .await?;
        Ok(rows
            .into_iter()
            .map(skill_upload_session_record_from_model)
            .collect())
    }

    pub async fn list_stale_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        let rows =
            skill_upload_session::list_stale_skill_upload_sessions(&self.connection, now_unix)
                .await?;
        Ok(rows
            .into_iter()
            .map(skill_upload_session_record_from_model)
            .collect())
    }

    pub async fn upsert_mcp_server_installation(
        &self,
        record: &McpServerInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            mcp_server_installation::upsert_mcp_server_installation(
                &self.connection,
                record,
                now,
                now,
            )
            .await
        })
        .await
    }

    pub async fn upsert_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin MCP installation transaction")?;
            let now = unix_to_datetime(event_timestamp_secs);

            let installation_id = match mcp_server_installation::upsert_mcp_server_installation(
                &transaction,
                record,
                now,
                now,
            )
            .await
            {
                Ok(id) => id,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            let mut audit = audit.clone();
            audit.server_installation_id = Some(installation_id.clone());
            if let Err(error) = mcp_audit_event::insert_mcp_audit_event(&transaction, &audit).await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            transaction
                .commit()
                .await
                .context("failed to commit MCP installation transaction")?;

            Ok(installation_id)
        })
        .await
    }

    pub async fn list_mcp_server_installations(
        &self,
        scope_kind: &str,
        scope_key: &str,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        let rows = mcp_server_installation::list_mcp_server_installations(
            &self.connection,
            scope_kind,
            scope_key,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(mcp_server_installation_record_from_model)
            .collect())
    }

    pub async fn list_all_mcp_server_installations(
        &self,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        let rows =
            mcp_server_installation::list_all_mcp_server_installations(&self.connection).await?;
        Ok(rows
            .into_iter()
            .map(mcp_server_installation_record_from_model)
            .collect())
    }

    pub async fn find_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        let row = mcp_server_installation::find_mcp_server_installation(
            &self.connection,
            scope_kind,
            scope_key,
            name,
        )
        .await?;
        Ok(row.map(mcp_server_installation_record_from_model))
    }

    pub async fn delete_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            mcp_server_installation::delete_mcp_server_installation(
                &self.connection,
                scope_kind,
                scope_key,
                name,
            )
            .await
        })
        .await
    }

    pub async fn delete_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
    ) -> Result<()> {
        let scope_kind = record.scope_kind.clone();
        let scope_key = record.scope_key.clone();
        let name = record.name.clone();
        let server_installation_id = record.id.clone();
        let audit = audit.clone();

        self.run_serialized_write(|| {
            let scope_kind = scope_kind.clone();
            let scope_key = scope_key.clone();
            let name = name.clone();
            let server_installation_id = server_installation_id.clone();
            let audit = audit.clone();

            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin MCP uninstall transaction")?;

                if let Some(server_installation_id) = server_installation_id.as_deref() {
                    if let Err(error) =
                        mcp_server_catalog_snapshot::delete_mcp_server_catalog_snapshot(
                            &transaction,
                            server_installation_id,
                        )
                        .await
                    {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                }

                if let Err(error) =
                    mcp_audit_event::insert_mcp_audit_event(&transaction, &audit).await
                {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }

                if let Err(error) = mcp_server_installation::delete_mcp_server_installation(
                    &transaction,
                    scope_kind.as_str(),
                    scope_key.as_str(),
                    name.as_str(),
                )
                .await
                {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }

                transaction
                    .commit()
                    .await
                    .context("failed to commit MCP uninstall transaction")
            }
        })
        .await
    }

    pub async fn upsert_mcp_server_catalog_snapshot(
        &self,
        record: &McpServerCatalogSnapshotRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            mcp_server_catalog_snapshot::upsert_mcp_server_catalog_snapshot(
                &self.connection,
                record,
                unix_to_datetime(record.generated_at_unix),
                now,
            )
            .await
        })
        .await
    }

    pub async fn find_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<Option<McpServerCatalogSnapshotRecord>> {
        let row = mcp_server_catalog_snapshot::find_mcp_server_catalog_snapshot(
            &self.connection,
            server_installation_id,
        )
        .await?;
        Ok(row.map(|model| McpServerCatalogSnapshotRecord {
            server_installation_id: model.server_installation_id,
            catalog_version: model.catalog_version,
            server_info_json: model.server_info_json,
            server_instructions_hash: model.server_instructions_hash,
            tools_json: model.tools_json,
            resources_json: model.resources_json,
            resource_templates_json: model.resource_templates_json,
            prompts_json: model.prompts_json,
            generated_at_unix: model.generated_at.timestamp(),
        }))
    }

    pub async fn delete_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<u64> {
        self.run_serialized_write(|| async {
            mcp_server_catalog_snapshot::delete_mcp_server_catalog_snapshot(
                &self.connection,
                server_installation_id,
            )
            .await
        })
        .await
    }

    pub async fn insert_mcp_audit_event_record(&self, record: &McpAuditEventRecord) -> Result<()> {
        self.run_serialized_write(|| async {
            mcp_audit_event::insert_mcp_audit_event(&self.connection, record).await
        })
        .await
    }

    pub async fn list_recent_mcp_audit_event_records(
        &self,
        server_name: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        let rows =
            mcp_audit_event::list_recent_mcp_audit_events(&self.connection, server_name, limit)
                .await?;
        Ok(rows
            .into_iter()
            .map(|model| McpAuditEventRecord {
                turn_id: model.turn_id,
                server_installation_id: model.server_installation_id,
                server_name: model.server_name,
                raw_tool_name: model.raw_tool_name,
                callable_name: model.callable_name,
                catalog_version: model.catalog_version,
                action: model.action,
                decision: model.decision,
                reason_code: model.reason_code,
                details_json: model.details_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn list_recent_mcp_audit_event_records_for_server_id(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        let rows = mcp_audit_event::list_recent_mcp_audit_events_for_server_id(
            &self.connection,
            server_installation_id,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|model| McpAuditEventRecord {
                turn_id: model.turn_id,
                server_installation_id: model.server_installation_id,
                server_name: model.server_name,
                raw_tool_name: model.raw_tool_name,
                callable_name: model.callable_name,
                catalog_version: model.catalog_version,
                action: model.action,
                decision: model.decision,
                reason_code: model.reason_code,
                details_json: model.details_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn replace_turn_mcp_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnMcpBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            turn_mcp_binding::replace_turn_mcp_bindings(
                &self.connection,
                turn_id,
                bindings,
                unix_to_datetime(event_timestamp_secs),
            )
            .await
        })
        .await
    }

    pub async fn list_turn_mcp_bindings(&self, turn_id: &str) -> Result<Vec<TurnMcpBindingRecord>> {
        let rows = turn_mcp_binding::list_turn_mcp_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(|model| TurnMcpBindingRecord {
                server_installation_id: model.server_installation_id,
                server_name: model.server_name,
                raw_tool_name: model.raw_tool_name,
                callable_name: model.callable_name,
                catalog_version: model.catalog_version,
                fingerprint: model.fingerprint,
                selection_reason: model.selection_reason,
                capability_id: model.capability_id,
            })
            .collect())
    }

    pub async fn list_recent_turn_mcp_bindings_for_server(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<TurnMcpBindingRecord>> {
        let rows = turn_mcp_binding::list_recent_turn_mcp_bindings_for_server(
            &self.connection,
            server_installation_id,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|model| TurnMcpBindingRecord {
                server_installation_id: model.server_installation_id,
                server_name: model.server_name,
                raw_tool_name: model.raw_tool_name,
                callable_name: model.callable_name,
                catalog_version: model.catalog_version,
                fingerprint: model.fingerprint,
                selection_reason: model.selection_reason,
                capability_id: model.capability_id,
            })
            .collect())
    }

    pub async fn insert_skill_audit_event_records(
        &self,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            skill_audit_event::insert_skill_audit_events(&self.connection, None, records).await
        })
        .await
    }

    pub async fn append_skill_audit_event_records(
        &self,
        turn_id: &str,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            skill_audit_event::insert_skill_audit_events(&self.connection, Some(turn_id), records)
                .await
        })
        .await
    }

    pub async fn list_turn_skill_audit_event_records(
        &self,
        turn_id: &str,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        let rows =
            skill_audit_event::list_turn_skill_audit_events(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(|model| SkillAuditEventRecord {
                turn_id: model.turn_id,
                skill_slug: model.skill_slug,
                source_kind: model.source_kind,
                action: model.action,
                decision: model.decision,
                reason_code: model.reason_code,
                details_json: model.details_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn list_skill_audit_event_records(
        &self,
        skill_slug: &str,
        limit: u64,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        let rows =
            skill_audit_event::list_skill_audit_events(&self.connection, skill_slug, limit).await?;
        Ok(rows
            .into_iter()
            .map(|model| SkillAuditEventRecord {
                turn_id: model.turn_id,
                skill_slug: model.skill_slug,
                source_kind: model.source_kind,
                action: model.action,
                decision: model.decision,
                reason_code: model.reason_code,
                details_json: model.details_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn list_skill_audit_event_records_for_source(
        &self,
        skill_slug: &str,
        source_kind: &str,
        limit: u64,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        let rows = skill_audit_event::list_skill_audit_events_for_source(
            &self.connection,
            skill_slug,
            source_kind,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|model| SkillAuditEventRecord {
                turn_id: model.turn_id,
                skill_slug: model.skill_slug,
                source_kind: model.source_kind,
                action: model.action,
                decision: model.decision,
                reason_code: model.reason_code,
                details_json: model.details_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn insert_skill_dependency_snapshot_record(
        &self,
        record: &SkillDependencySnapshotRecord,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            skill_dependency_snapshot::insert_skill_dependency_snapshot(&self.connection, record)
                .await
        })
        .await
    }

    pub async fn list_turn_skill_dependency_snapshot_records(
        &self,
        turn_id: &str,
    ) -> Result<Vec<SkillDependencySnapshotRecord>> {
        let rows = skill_dependency_snapshot::list_turn_skill_dependency_snapshots(
            &self.connection,
            turn_id,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|model| SkillDependencySnapshotRecord {
                turn_id: model.turn_id,
                skill_slug: model.skill_slug,
                source_kind: model.source_kind,
                diagnostics_json: model.diagnostics_json,
                created_at_unix: model.created_at.timestamp(),
            })
            .collect())
    }

    pub async fn get_thread_by_id(
        &self,
        thread_id: &str,
    ) -> Result<Option<pioneer_entity::thread::Model>> {
        thread::find_thread_by_id(&self.connection, thread_id).await
    }

    pub async fn list_thread_timeline_projection_page(
        &self,
        thread_id: &str,
        anchor: ProjectionPageAnchor<'_>,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::thread_timeline_block::Model>> {
        list_thread_timeline_blocks_page(&self.connection, thread_id, anchor, limit).await
    }

    pub async fn find_thread_timeline_projection_block_by_sort_key(
        &self,
        thread_id: &str,
        sort_key: &str,
    ) -> Result<Option<pioneer_entity::thread_timeline_block::Model>> {
        find_thread_timeline_block_by_sort_key(&self.connection, thread_id, sort_key).await
    }

    pub async fn get_turn_work_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_entity::turn_work_projection::Model>> {
        find_turn_work_projection(&self.connection, turn_id).await
    }

    pub async fn get_turn_work_item_projection(
        &self,
        work_item_id: &str,
    ) -> Result<Option<pioneer_entity::turn_work_item_projection::Model>> {
        find_turn_work_item_projection(&self.connection, work_item_id).await
    }

    pub async fn find_turn_work_item_projection_by_order_key(
        &self,
        turn_id: &str,
        order_key: &str,
        visibility: Option<&str>,
    ) -> Result<Option<pioneer_entity::turn_work_item_projection::Model>> {
        find_turn_work_item_projection_by_order_key(
            &self.connection,
            turn_id,
            order_key,
            visibility,
        )
        .await
    }

    pub async fn list_turn_work_item_projection_page(
        &self,
        turn_id: &str,
        visibility: Option<&str>,
        anchor: ProjectionPageAnchor<'_>,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::turn_work_item_projection::Model>> {
        list_turn_work_items_page(&self.connection, turn_id, visibility, anchor, limit).await
    }

    pub async fn get_thread_sandbox_mode(&self, thread_id: &str) -> Result<Option<SandboxMode>> {
        policy::find_thread_sandbox_mode(&self.connection, thread_id).await
    }

    pub async fn get_thread_model(&self, thread_id: &str) -> Result<Option<Thread>> {
        let Some(model) = thread::find_thread_by_id(&self.connection, thread_id).await? else {
            return Ok(None);
        };
        let Some(mut thread) = thread_from_db_model(model) else {
            return Ok(None);
        };

        self.attach_latest_thread_turn_snapshot(&mut thread).await?;

        Ok(Some(thread))
    }

    pub async fn upsert_thread_model(&self, thread_model: &Thread) -> Result<()> {
        let thread_model = thread_model.clone();
        let created_at = unix_to_datetime(thread_model.created_at);
        let updated_at = unix_to_datetime(thread_model.updated_at);
        self.run_serialized_write(|| {
            let thread_model = thread_model.clone();
            async move {
                thread::upsert_thread(&self.connection, &thread_model, created_at, updated_at).await
            }
        })
        .await
    }

    pub async fn list_threads_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<Thread>> {
        let models =
            thread::list_threads_by_workspace(&self.connection, workspace_id, limit).await?;
        let mut threads = Vec::with_capacity(models.len());

        for model in models {
            let Some(mut thread) = thread_from_db_model(model) else {
                continue;
            };

            self.attach_latest_thread_turn_snapshot(&mut thread).await?;

            threads.push(thread);
        }

        Ok(threads)
    }

    async fn attach_latest_thread_turn_snapshot(&self, thread: &mut Thread) -> Result<()> {
        let Some(turn_model) =
            turn::find_latest_turn_for_thread(&self.connection, thread.id.as_str()).await?
        else {
            return Ok(());
        };

        let reasoning_effort = turn_model.reasoning_effort.clone();
        if let Some(turn) = thread_snapshot_turn_from_db_model(turn_model)? {
            thread.reasoning_effort = reasoning_effort;
            thread.turns.push(turn);
        }

        Ok(())
    }

    pub async fn list_thread_folders(&self, workspace_id: &str) -> Result<Vec<ThreadFolder>> {
        let models = thread_tree::list_folders_by_workspace(&self.connection, workspace_id).await?;
        Ok(models
            .into_iter()
            .map(thread_folder_from_db_model)
            .collect())
    }

    pub async fn list_thread_placements(&self, workspace_id: &str) -> Result<Vec<ThreadPlacement>> {
        let models =
            thread_tree::list_placements_by_workspace(&self.connection, workspace_id).await?;
        Ok(models
            .into_iter()
            .map(thread_placement_from_db_model)
            .collect())
    }

    pub async fn get_thread_agents_doc_explicit(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .find_explicit(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn create_thread_agents_doc_draft(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        actor_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        self.write_coordinator
            .run_serialized_with_retry(
                || async {
                    thread_agents_doc::ThreadAgentsDocRepository::new()
                        .create_draft(
                            &self.connection,
                            workspace_id,
                            folder_id,
                            thread_agents_doc::now(),
                            actor_id,
                        )
                        .await
                },
                |_| false,
            )
            .await
    }

    pub async fn save_thread_agents_doc(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        content: &str,
        expected_version: Option<i64>,
        actor_id: Option<&str>,
        save_reason: ThreadAgentsDocSaveReason,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        self.write_coordinator
            .run_serialized_with_retry(
                || async {
                    thread_agents_doc::ThreadAgentsDocRepository::new()
                        .save_content(
                            &self.connection,
                            workspace_id,
                            folder_id,
                            content,
                            expected_version,
                            thread_agents_doc::now(),
                            actor_id,
                            save_reason,
                        )
                        .await
                },
                |_| false,
            )
            .await
    }

    pub async fn archive_thread_agents_doc(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        expected_version: Option<i64>,
        actor_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        self.write_coordinator
            .run_serialized_with_retry(
                || async {
                    thread_agents_doc::ThreadAgentsDocRepository::new()
                        .archive(
                            &self.connection,
                            workspace_id,
                            folder_id,
                            expected_version,
                            thread_agents_doc::now(),
                            actor_id,
                        )
                        .await
                },
                |_| false,
            )
            .await
    }

    pub async fn list_thread_agents_doc_revisions(
        &self,
        doc_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Vec<ThreadAgentsDocRevisionRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .list_revisions(&self.connection, doc_id)
            .await
    }

    pub async fn list_thread_agents_doc_summaries(
        &self,
        workspace_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Vec<ThreadAgentsDocSummaryRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .list_summaries(&self.connection, workspace_id)
            .await
    }

    pub async fn resolve_thread_agents_doc_for_folder(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .resolve_for_folder(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn resolve_thread_agents_doc_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .resolve_for_thread(&self.connection, workspace_id, thread_id)
            .await
    }

    pub async fn get_thread_agents_doc_scope_context(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocScopeContext> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .scope_context(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn create_thread_folder(
        &self,
        workspace_id: &str,
        parent_folder_id: Option<&str>,
        name: &str,
    ) -> Result<ThreadFolder> {
        self.run_serialized_write(|| async {
            if let Some(parent_folder_id) = parent_folder_id {
                let Some(parent) =
                    thread_tree::find_folder_by_id(&self.connection, parent_folder_id).await?
                else {
                    anyhow::bail!("parent folder `{parent_folder_id}` was not found");
                };

                if parent.workspace_id != workspace_id {
                    anyhow::bail!(
                        "parent folder `{parent_folder_id}` belongs to workspace `{}`",
                        parent.workspace_id
                    );
                }
            }

            let now = chrono::Utc::now().timestamp();
            let created_at = unix_to_datetime(now);
            let folder_id = generate_id(21);
            thread_tree::insert_folder(
                &self.connection,
                folder_id.as_str(),
                workspace_id,
                parent_folder_id,
                name,
                created_at,
                created_at,
            )
            .await?;

            Ok(ThreadFolder {
                id: folder_id,
                workspace_id: workspace_id.to_owned(),
                parent_folder_id: parent_folder_id.map(str::to_owned),
                name: name.to_owned(),
                created_at: now,
                updated_at: now,
            })
        })
        .await
    }

    pub async fn delete_thread_folder_promote(
        &self,
        workspace_id: &str,
        folder_id: &str,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin folder delete transaction")?;

            let folder = thread_tree::find_folder_by_id(&transaction, folder_id).await?;
            let Some(folder) = folder else {
                transaction
                    .rollback()
                    .await
                    .context("failed to rollback folder delete transaction")?;
                return Ok(false);
            };

            if folder.workspace_id != workspace_id {
                transaction
                    .rollback()
                    .await
                    .context("failed to rollback folder delete transaction")?;
                anyhow::bail!(
                    "folder `{folder_id}` belongs to workspace `{}`",
                    folder.workspace_id
                );
            }

            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_tree::reparent_child_folders(
                &transaction,
                folder_id,
                folder.parent_folder_id.as_deref(),
                now,
            )
            .await?;

            thread_tree::move_thread_placements_to_folder(
                &transaction,
                folder_id,
                folder.parent_folder_id.as_deref(),
                now,
            )
            .await?;

            thread_tree::delete_folder(&transaction, folder_id).await?;

            transaction
                .commit()
                .await
                .context("failed to commit folder delete transaction")?;

            Ok(true)
        })
        .await
    }

    pub async fn move_thread_to_folder(
        &self,
        workspace_id: &str,
        thread_id: &str,
        folder_id: Option<&str>,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            if let Some(folder_id) = folder_id {
                let Some(folder) =
                    thread_tree::find_folder_by_id(&self.connection, folder_id).await?
                else {
                    anyhow::bail!("folder `{folder_id}` was not found");
                };

                if folder.workspace_id != workspace_id {
                    anyhow::bail!(
                        "folder `{folder_id}` belongs to workspace `{}`",
                        folder.workspace_id
                    );
                }
            }

            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_tree::upsert_thread_placement(
                &self.connection,
                workspace_id,
                thread_id,
                folder_id,
                now,
                now,
            )
            .await
        })
        .await
    }

    pub async fn move_folder(
        &self,
        workspace_id: &str,
        folder_id: &str,
        parent_folder_id: Option<&str>,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            if parent_folder_id == Some(folder_id) {
                anyhow::bail!("cannot move folder into itself");
            }

            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin folder move transaction")?;

            let folders = thread_tree::list_folders_by_workspace(&transaction, workspace_id).await?;
            let folders_by_id: HashMap<&str, &pioneer_entity::thread_folder::Model> = folders
                .iter()
                .map(|folder| (folder.id.as_str(), folder))
                .collect();

            let Some(folder) = folders_by_id.get(folder_id) else {
                transaction
                    .rollback()
                    .await
                    .context("failed to rollback folder move transaction")?;
                anyhow::bail!("folder `{folder_id}` was not found");
            };

            if let Some(parent_folder_id) = parent_folder_id {
                let Some(_) = folders_by_id.get(parent_folder_id) else {
                    transaction
                        .rollback()
                        .await
                        .context("failed to rollback folder move transaction")?;
                    anyhow::bail!("parent folder `{parent_folder_id}` was not found");
                };

                let mut cursor = Some(parent_folder_id);
                while let Some(current_id) = cursor {
                    if current_id == folder_id {
                        transaction
                            .rollback()
                            .await
                            .context("failed to rollback folder move transaction")?;
                        anyhow::bail!(
                            "cannot move folder `{folder_id}` into its descendant `{parent_folder_id}`"
                        );
                    }
                    cursor = folders_by_id
                        .get(current_id)
                        .and_then(|model| model.parent_folder_id.as_deref());
                }
            }

            if folder.parent_folder_id.as_deref() == parent_folder_id {
                transaction
                    .rollback()
                    .await
                    .context("failed to rollback folder move transaction")?;
                return Ok(());
            }

            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_tree::update_folder_parent(&transaction, folder_id, parent_folder_id, now).await?;

            transaction
                .commit()
                .await
                .context("failed to commit folder move transaction")?;

            Ok(())
        })
        .await
    }

    pub async fn update_thread_name(&self, thread_id: &str, name: &str) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread::update_thread_name(&self.connection, thread_id, name, now).await
        })
        .await
    }

    pub async fn update_thread_name_if_changed(&self, thread_id: &str, name: &str) -> Result<bool> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread::update_thread_name_if_changed(&self.connection, thread_id, name, now).await
        })
        .await
    }

    pub async fn get_thread_summary(&self, thread_id: &str) -> Result<Option<(String, i64)>> {
        let model = thread::find_thread_by_id(&self.connection, thread_id).await?;
        match model {
            Some(m) => match m.summary {
                Some(s) if !s.is_empty() => Ok(Some((s, m.summary_turn_count.unwrap_or(0)))),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    pub async fn update_thread_summary(
        &self,
        thread_id: &str,
        summary: &str,
        turn_count: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread::update_thread_summary(&self.connection, thread_id, summary, turn_count, now)
                .await
        })
        .await
    }

    pub async fn count_completed_turns(&self, thread_id: &str) -> Result<u64> {
        turn::count_completed_turns_for_thread(&self.connection, thread_id).await
    }

    pub async fn count_recovery_jobs_for_turn(&self, turn_id: &str) -> Result<u64> {
        recovery_job::count_jobs_for_turn(&self.connection, turn_id).await
    }

    pub async fn get_turns_for_summary(
        &self,
        thread_id: &str,
        skip: u64,
        take: u64,
    ) -> Result<Vec<ConversationEntry>> {
        let turns =
            turn::find_completed_turns_in_range(&self.connection, thread_id, skip, take).await?;

        let mut entries = Vec::with_capacity(turns.len());
        for turn_model in &turns {
            let inputs = turn::find_turn_inputs(&self.connection, &turn_model.id).await?;
            let user_text: String = inputs
                .iter()
                .filter(|i| i.input_type == "text")
                .filter_map(|i| i.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n");

            let items = turn::find_completed_turn_items(&self.connection, &turn_model.id).await?;
            let assistant_text: String = items
                .iter()
                .filter_map(|item| {
                    serde_json::from_str::<pioneer_protocol::TurnItem>(&item.payload).ok()
                })
                .filter_map(|item| match item {
                    pioneer_protocol::TurnItem::AgentMessage { text, .. } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");

            entries.push(ConversationEntry {
                turn_id: turn_model.id.clone(),
                user_text: if user_text.is_empty() {
                    None
                } else {
                    Some(user_text)
                },
                assistant_text: if assistant_text.is_empty() {
                    None
                } else {
                    Some(assistant_text)
                },
                user_artifacts: Vec::new(),
                assistant_artifacts: Vec::new(),
            });
        }

        Ok(entries)
    }

    pub async fn get_thread_history(
        &self,
        thread_id: &str,
        limit_events: Option<u64>,
    ) -> Result<Option<ThreadHistorySnapshot>> {
        let Some(thread_model) = thread::find_thread_by_id(&self.connection, thread_id).await?
        else {
            return Ok(None);
        };

        let workspace_id = thread_model.workspace_id.clone();
        let event_rows =
            turn_event::list_events_for_thread(&self.connection, thread_id, limit_events).await?;

        let mut events = Vec::with_capacity(event_rows.len());
        for row in event_rows {
            let payload = serde_json::from_str::<TurnEventPayload>(row.payload.as_str())
                .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;

            let mapped_payload = match payload {
                TurnEventPayload::TurnStarted(payload) => ThreadHistoryEventPayload::TurnStarted {
                    workspace_id: payload.thread.workspace_id.clone(),
                    thread_id: payload.thread.id.clone(),
                    turn: payload.turn,
                    input: payload.input,
                },
                TurnEventPayload::ItemStarted(notification) => {
                    ThreadHistoryEventPayload::ItemStarted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemCompleted(notification) => {
                    ThreadHistoryEventPayload::ItemCompleted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemUpdated(notification) => {
                    ThreadHistoryEventPayload::ItemUpdated {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemTimeoutDetected(notification) => {
                    ThreadHistoryEventPayload::ItemTimeoutDetected {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        attempt_number: notification.attempt_number,
                        reason: notification.reason,
                        recovery_job_id: notification.recovery_job_id,
                    }
                }
                TurnEventPayload::ItemRecoveryOpened(notification) => {
                    ThreadHistoryEventPayload::ItemRecoveryOpened {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        trigger: notification.trigger,
                        action: notification.action,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoveryAttached(notification) => {
                    ThreadHistoryEventPayload::ItemRecoveryAttached {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        recovery_item_id: notification.recovery_item_id,
                        recovery_item_type: notification.recovery_item_type,
                        trigger: notification.trigger,
                        action: notification.action,
                        existing_status: notification.existing_status,
                        next_attempt_number: notification.next_attempt_number,
                    }
                }
                TurnEventPayload::ItemRetryScheduled(notification) => {
                    ThreadHistoryEventPayload::ItemRetryScheduled {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                        next_run_at_unix: notification.next_run_at_unix,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemRetryAttemptStarted(notification) => {
                    ThreadHistoryEventPayload::ItemRetryAttemptStarted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoverySucceeded(notification) => {
                    ThreadHistoryEventPayload::ItemRecoverySucceeded {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                    }
                }
                TurnEventPayload::ItemRecoveryExhausted(notification) => {
                    ThreadHistoryEventPayload::ItemRecoveryExhausted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        recovery_job_id: notification.recovery_job_id,
                        attempt_number: notification.attempt_number,
                        status: notification.status,
                        error_message: notification.error_message,
                    }
                }
                TurnEventPayload::ItemToolRetryScheduled(notification) => {
                    ThreadHistoryEventPayload::ItemToolRetryScheduled {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        error_class: notification.error_class,
                        retry_hint: notification.retry_hint,
                        budgets: notification.budgets,
                        failure_signature_fingerprint: notification.failure_signature_fingerprint,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemToolRetryResolved(notification) => {
                    ThreadHistoryEventPayload::ItemToolRetryResolved {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        resolution: notification.resolution,
                        budgets: notification.budgets,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::ItemToolRetryExhausted(notification) => {
                    ThreadHistoryEventPayload::ItemToolRetryExhausted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item_id: notification.item_id,
                        item_type: notification.item_type,
                        tool_retry_episode_id: notification.tool_retry_episode_id,
                        tool_name: notification.tool_name,
                        attempt_number: notification.attempt_number,
                        error_class: notification.error_class,
                        exhaustion_kind: notification.exhaustion_kind,
                        budgets: notification.budgets,
                        failure_signature_fingerprint: notification.failure_signature_fingerprint,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::TurnToolLoopBudgetExceeded(notification) => {
                    ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        limit_kind: notification.limit_kind,
                        limit: notification.limit,
                        observed: notification.observed,
                        action: notification.action,
                        reason: notification.reason,
                    }
                }
                TurnEventPayload::TurnExecutionWindowStarted(notification) => {
                    ThreadHistoryEventPayload::TurnExecutionWindowStarted(notification)
                }
                TurnEventPayload::TurnExecutionWindowExhausted(notification) => {
                    ThreadHistoryEventPayload::TurnExecutionWindowExhausted(notification)
                }
                TurnEventPayload::TurnExecutionWindowCheckpointed(notification) => {
                    ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(notification)
                }
                TurnEventPayload::TurnExecutionWindowContinued(notification) => {
                    ThreadHistoryEventPayload::TurnExecutionWindowContinued(notification)
                }
                TurnEventPayload::TurnExecutionWindowBlocked(notification) => {
                    ThreadHistoryEventPayload::TurnExecutionWindowBlocked(notification)
                }
                TurnEventPayload::TurnPermissionAudit(event) => {
                    ThreadHistoryEventPayload::TurnPermissionAudit(event)
                }
                TurnEventPayload::TurnCompleted(notification) => {
                    ThreadHistoryEventPayload::TurnCompleted {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn: notification.turn,
                    }
                }
                TurnEventPayload::TurnFailed(notification) => {
                    ThreadHistoryEventPayload::TurnFailed {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn: notification.turn,
                    }
                }
                TurnEventPayload::TurnBlocked(notification) => {
                    ThreadHistoryEventPayload::TurnBlocked {
                        workspace_id: notification.workspace_id,
                        thread_id: notification.thread_id,
                        turn: notification.turn,
                        resume: notification.resume,
                    }
                }
            };

            events.push(ThreadHistoryEvent {
                turn_id: row.turn_id,
                sequence: row.sequence,
                created_at: row.created_at.timestamp_millis(),
                payload: mapped_payload,
            });
        }

        Ok(Some(ThreadHistorySnapshot {
            workspace_id,
            events,
        }))
    }

    pub async fn configure_turn_item_attempt_deadlines(
        &self,
        turn_id: &str,
        item_id: &str,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            turn_item_attempt::configure_running_attempt_deadlines(
                &self.connection,
                turn_id,
                item_id,
                unix_to_datetime(heartbeat_at_unix),
                lease_expires_at_unix.map(unix_to_datetime),
                idle_deadline_at_unix.map(unix_to_datetime),
                hard_deadline_at_unix.map(unix_to_datetime),
            )
            .await
        })
        .await
    }

    pub async fn heartbeat_turn_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            turn_item_attempt::heartbeat_running_attempt(
                &self.connection,
                turn_id,
                item_id,
                unix_to_datetime(heartbeat_at_unix),
                lease_expires_at_unix.map(unix_to_datetime),
                idle_deadline_at_unix.map(unix_to_datetime),
            )
            .await
        })
        .await
    }

    pub async fn list_timeout_candidates(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        let rows = turn_item_attempt::list_expired_running_attempts(
            &self.connection,
            unix_to_datetime(now_unix),
            limit,
        )
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| TimeoutCandidate {
                attempt_id: row.id,
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: row.item_type,
                attempt_number: row.attempt_number,
                timeout_reason: infer_timeout_reason(
                    row.lease_expires_at,
                    row.idle_deadline_at,
                    row.hard_deadline_at,
                    now_unix,
                ),
            })
            .collect())
    }

    pub async fn list_running_attempts_missing_deadlines(
        &self,
        limit: u64,
    ) -> Result<Vec<RunningAttemptDeadlineRepairCandidate>> {
        let rows = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_RUNNING))
            .filter(
                Condition::any()
                    .add(pioneer_entity::turn_item_attempt::Column::LeaseExpiresAt.is_null())
                    .add(pioneer_entity::turn_item_attempt::Column::IdleDeadlineAt.is_null())
                    .add(pioneer_entity::turn_item_attempt::Column::HardDeadlineAt.is_null()),
            )
            .limit(limit)
            .all(&self.connection)
            .await
            .context("failed to list running attempts missing deadlines")?;

        Ok(rows
            .into_iter()
            .map(|row| RunningAttemptDeadlineRepairCandidate {
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: turn_item_type_from_db(row.item_type.as_str())
                    .unwrap_or(TurnItemType::DynamicToolCall),
                started_at_unix: row.started_at.timestamp(),
            })
            .collect())
    }

    pub async fn list_running_turn_item_attempts_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RunningTurnItemAttempt>> {
        let rows =
            turn_item_attempt::list_running_attempts_for_turn(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(|row| RunningTurnItemAttempt {
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: row.item_type,
            })
            .collect())
    }

    pub async fn list_unqueued_timeout_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        let rows =
            turn_item_attempt::list_timed_out_without_recovery(&self.connection, limit).await?;
        Ok(rows
            .into_iter()
            .map(|row| TimeoutCandidate {
                attempt_id: row.id,
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: row.item_type,
                attempt_number: row.attempt_number,
                timeout_reason: row.timeout_reason,
            })
            .collect())
    }

    pub async fn list_read_model_invariant_violations(
        &self,
    ) -> Result<Vec<ReadModelInvariantViolation>> {
        let terminal_turn_item_rows = pioneer_entity::turn_item::Entity::find()
            .filter(pioneer_entity::turn_item::Column::Status.is_in([
                TURN_ITEM_STATUS_COMPLETED,
                TURN_ITEM_STATUS_FAILED,
                TURN_ITEM_STATUS_TIMED_OUT,
                TURN_ITEM_STATUS_CANCELLED,
            ]))
            .all(&self.connection)
            .await
            .context("failed to list terminal turn_item rows for invariant check")?;

        let mut violations = Vec::new();
        for row in terminal_turn_item_rows {
            let item: TurnItem = serde_json::from_str(row.payload.as_str()).with_context(|| {
                format!(
                    "failed to decode turn_item payload during invariant check (turn `{}` item `{}`)",
                    row.turn_id, row.item_id
                )
            })?;

            if tool_call_status(&item) == Some(ToolCallStatus::InProgress) {
                violations.push(ReadModelInvariantViolation {
                    kind: if row.status.as_deref() == Some(TURN_ITEM_STATUS_TIMED_OUT) {
                        ReadModelInvariantKind::TimedOutToolPayloadInProgress
                    } else {
                        ReadModelInvariantKind::TerminalToolPayloadInProgress
                    },
                    entity_id: format!("{}:{}", row.turn_id, row.item_id),
                    details: format!(
                        "turn_item.status=`{}` while tool payload status is in_progress",
                        row.status.as_deref().unwrap_or("null")
                    ),
                });
            }
        }

        let running_attempt_rows = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_RUNNING))
            .all(&self.connection)
            .await
            .context("failed to list running attempts for invariant check")?;

        for running_attempt in running_attempt_rows {
            let Some(turn_model) =
                pioneer_entity::turn::Entity::find_by_id(running_attempt.turn_id.clone())
                    .one(&self.connection)
                    .await
                    .context("failed to load turn for running attempt invariant check")?
            else {
                continue;
            };
            if matches!(
                turn_model.status.as_str(),
                "completed" | "failed" | "interrupted"
            ) {
                violations.push(ReadModelInvariantViolation {
                    kind: ReadModelInvariantKind::TerminalTurnHasRunningAttempts,
                    entity_id: running_attempt.id,
                    details: format!(
                        "turn `{}` is `{}` while attempt for item `{}` remains running",
                        turn_model.id, turn_model.status, running_attempt.item_id
                    ),
                });
            }
        }

        let terminal_tasks_missing_completed_at = pioneer_entity::task::Entity::find()
            .filter(pioneer_entity::task::Column::CompletedAt.is_null())
            .all(&self.connection)
            .await
            .context("failed to list terminal tasks missing completed_at")?;

        for task in terminal_tasks_missing_completed_at {
            if !is_terminal_task_status_db(task.status.as_str()) {
                continue;
            }
            violations.push(ReadModelInvariantViolation {
                kind: ReadModelInvariantKind::TerminalTaskMissingCompletedAt,
                entity_id: task.id,
                details: "terminal task row has null completed_at".to_owned(),
            });
        }

        let terminal_runs_missing_completed_at = pioneer_entity::task_run::Entity::find()
            .filter(pioneer_entity::task_run::Column::CompletedAt.is_null())
            .all(&self.connection)
            .await
            .context("failed to list terminal task runs missing completed_at")?;

        for run in terminal_runs_missing_completed_at {
            if !is_terminal_task_run_status_db(run.status.as_str()) {
                continue;
            }
            violations.push(ReadModelInvariantViolation {
                kind: ReadModelInvariantKind::TerminalRunMissingCompletedAt,
                entity_id: run.id,
                details: "terminal task_run row has null completed_at".to_owned(),
            });
        }

        Ok(violations)
    }

    pub async fn repair_deterministic_read_model_violations(&self) -> Result<RepairSummary> {
        self.run_serialized_write(|| async {
            let before = self.list_read_model_invariant_violations().await?;
            let mut repaired = 0usize;
            let now: sea_orm::entity::prelude::DateTimeWithTimeZone =
                chrono::Utc::now().into();

            let terminal_turn_item_rows = pioneer_entity::turn_item::Entity::find()
                .filter(
                    pioneer_entity::turn_item::Column::Status.is_in([
                        TURN_ITEM_STATUS_COMPLETED,
                        TURN_ITEM_STATUS_FAILED,
                        TURN_ITEM_STATUS_TIMED_OUT,
                        TURN_ITEM_STATUS_CANCELLED,
                    ]),
                )
                .all(&self.connection)
                .await
                .context("failed to list terminal turn_item rows for repair")?;

            for row in terminal_turn_item_rows {
                let mut item: TurnItem = serde_json::from_str(row.payload.as_str()).with_context(|| {
                    format!(
                        "failed to decode turn_item payload during repair (turn `{}` item `{}`)",
                        row.turn_id, row.item_id
                    )
                })?;
                if tool_call_status(&item) != Some(ToolCallStatus::InProgress) {
                    continue;
                }
                let terminal_state = match row.status.as_deref() {
                    Some(TURN_ITEM_STATUS_COMPLETED) => TurnItemTerminalState::Completed,
                    Some(TURN_ITEM_STATUS_TIMED_OUT) => TurnItemTerminalState::TimedOut {
                        reason: TurnItemTimeoutReason::HardDeadlineExceeded,
                    },
                    Some(TURN_ITEM_STATUS_CANCELLED) => TurnItemTerminalState::Cancelled {
                        reason: Some("read_model_invariant_repair".to_owned()),
                    },
                    _ => TurnItemTerminalState::Failed {
                        reason: Some("read_model_invariant_repair".to_owned()),
                    },
                };
                terminalize_turn_item_payload(&mut item, terminal_state);
                let payload_json = serde_json::to_string(&item)
                    .context("failed to encode repaired turn_item payload")?;

                let result = pioneer_entity::turn_item::Entity::update_many()
                    .filter(pioneer_entity::turn_item::Column::Id.eq(row.id.clone()))
                    .col_expr(
                        pioneer_entity::turn_item::Column::Payload,
                        sea_orm::sea_query::Expr::value(payload_json),
                    )
                    .col_expr(
                        pioneer_entity::turn_item::Column::UpdatedAt,
                        sea_orm::sea_query::Expr::value(now),
                    )
                    .exec(&self.connection)
                    .await
                    .context("failed to update repaired turn_item payload")?;
                repaired = repaired.saturating_add(result.rows_affected as usize);
            }

            let running_attempt_rows = pioneer_entity::turn_item_attempt::Entity::find()
                .filter(
                    pioneer_entity::turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_RUNNING),
                )
                .all(&self.connection)
                .await
                .context("failed to list running attempts for repair")?;

            for running_attempt in running_attempt_rows {
                let Some(turn_model) =
                    pioneer_entity::turn::Entity::find_by_id(running_attempt.turn_id.clone())
                        .one(&self.connection)
                        .await
                        .context("failed to load turn for running-attempt repair")?
                else {
                    continue;
                };
                if !matches!(
                    turn_model.status.as_str(),
                    "completed" | "failed" | "interrupted" | "blocked"
                ) {
                    continue;
                }

                let attempt_result = pioneer_entity::turn_item_attempt::Entity::update_many()
                    .filter(
                        pioneer_entity::turn_item_attempt::Column::Id.eq(running_attempt.id.clone()),
                    )
                    .filter(
                        pioneer_entity::turn_item_attempt::Column::Status
                            .eq(ATTEMPT_STATUS_RUNNING),
                    )
                    .col_expr(
                        pioneer_entity::turn_item_attempt::Column::Status,
                        sea_orm::sea_query::Expr::value(ATTEMPT_STATUS_INTERRUPTED),
                    )
                    .col_expr(
                        pioneer_entity::turn_item_attempt::Column::FailureReason,
                        sea_orm::sea_query::Expr::value(Some(
                            "read_model_invariant_repair".to_owned(),
                        )),
                    )
                    .col_expr(
                        pioneer_entity::turn_item_attempt::Column::UpdatedAt,
                        sea_orm::sea_query::Expr::value(now),
                    )
                    .exec(&self.connection)
                    .await
                    .context("failed to interrupt running attempt during repair")?;
                if attempt_result.rows_affected == 0 {
                    continue;
                }
                repaired = repaired.saturating_add(attempt_result.rows_affected as usize);

                let item_result = pioneer_entity::turn_item::Entity::update_many()
                    .filter(
                        pioneer_entity::turn_item::Column::TurnId.eq(running_attempt.turn_id.clone()),
                    )
                    .filter(
                        pioneer_entity::turn_item::Column::ItemId.eq(running_attempt.item_id.clone()),
                    )
                    .col_expr(
                        pioneer_entity::turn_item::Column::ActiveAttemptStatus,
                        sea_orm::sea_query::Expr::value(Some(ATTEMPT_STATUS_INTERRUPTED)),
                    )
                    .col_expr(
                        pioneer_entity::turn_item::Column::UpdatedAt,
                        sea_orm::sea_query::Expr::value(now),
                    )
                    .exec(&self.connection)
                    .await
                    .context("failed to repair turn_item active attempt status")?;
                repaired = repaired.saturating_add(item_result.rows_affected as usize);
            }

            let task_result = pioneer_entity::task::Entity::update_many()
                .filter(
                    pioneer_entity::task::Column::Status
                        .is_in(["completed", "failed", "cancelled"]),
                )
                .filter(pioneer_entity::task::Column::CompletedAt.is_null())
                .col_expr(
                    pioneer_entity::task::Column::CompletedAt,
                    sea_orm::sea_query::Expr::value(Some(now)),
                )
                .exec(&self.connection)
                .await
                .context("failed to repair terminal tasks missing completed_at")?;
            repaired = repaired.saturating_add(task_result.rows_affected as usize);

            let run_result = pioneer_entity::task_run::Entity::update_many()
                .filter(
                    pioneer_entity::task_run::Column::Status
                        .is_in(["succeeded", "failed", "cancelled", "timed_out"]),
                )
                .filter(pioneer_entity::task_run::Column::CompletedAt.is_null())
                .col_expr(
                    pioneer_entity::task_run::Column::CompletedAt,
                    sea_orm::sea_query::Expr::value(Some(now)),
                )
                .exec(&self.connection)
                .await
                .context("failed to repair terminal runs missing completed_at")?;
            repaired = repaired.saturating_add(run_result.rows_affected as usize);

            let after = self.list_read_model_invariant_violations().await?;

            Ok(RepairSummary {
                detected: before.len(),
                repaired,
                remaining: after.len(),
            })
        })
        .await
    }

    pub async fn transition_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(now_unix);
            let tx = self
                .connection
                .begin()
                .await
                .context("failed to begin timeout transition transaction")?;

            let snapshot = turn_item_attempt::RunningAttemptSnapshot {
                id: candidate.attempt_id.clone(),
                turn_id: candidate.turn_id.clone(),
                item_id: candidate.item_id.clone(),
                item_type: candidate.item_type,
                attempt_number: candidate.attempt_number,
                lease_expires_at: None,
                idle_deadline_at: None,
                hard_deadline_at: None,
            };

            let transitioned = turn_item_attempt::transition_running_attempt_to_timed_out(
                &tx,
                &snapshot,
                candidate.timeout_reason,
                now,
            )
            .await?;

            if !transitioned {
                tx.rollback()
                    .await
                    .context("failed to rollback timeout transition transaction")?;
                return Ok(false);
            }

            tx.commit()
                .await
                .context("failed to commit timeout transition transaction")?;

            Ok(true)
        })
        .await
    }

    pub async fn enqueue_recovery_job(
        &self,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        source_attempt_id: Option<String>,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        reason: Option<String>,
        error_class: Option<ProviderFailureClass>,
        transport_stage: Option<ProviderFailureStage>,
        retry_after_ms: Option<i64>,
        provider_attempt_number: i64,
        max_attempts: i64,
        policy_json: serde_json::Value,
        policy_snapshot: serde_json::Value,
        now_unix: i64,
    ) -> Result<RecoveryJobRecord> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(now_unix);
            let policy_json = serde_json::to_string(&policy_json)
                .context("failed to serialize recovery policy json")?;
            let policy_snapshot_json = serde_json::to_string(&policy_snapshot)
                .context("failed to serialize recovery policy snapshot json")?;
            let row = recovery_job::enqueue_recovery_job(
                &self.connection,
                recovery_job::NewRecoveryJob {
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type,
                    source_attempt_id: source_attempt_id.clone(),
                    trigger,
                    action,
                    reason: reason.clone(),
                    policy_json,
                    error_class,
                    transport_stage,
                    retry_after_ms,
                    provider_attempt_number,
                    policy_snapshot_json,
                    max_attempts,
                    scheduled_at: now,
                    next_run_at: now,
                },
            )
            .await?;
            Ok(recovery_job_record_from_model(row))
        })
        .await
    }

    pub async fn mark_attempt_recovery_action(
        &self,
        attempt_id: &str,
        action: RecoveryAction,
        now_unix: i64,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            turn_item_attempt::mark_recovery_action(
                &self.connection,
                attempt_id,
                recovery_action_to_db(action),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn claim_due_recovery_jobs(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(now_unix);
            let claim_expires_at = i64::try_from(claim_lease_secs)
                .ok()
                .and_then(|secs| now_unix.checked_add(secs))
                .map(unix_to_datetime)
                .unwrap_or(now);
            let jobs = recovery_job::claim_due_jobs(&self.connection, now, claim_expires_at, limit)
                .await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn list_due_pending_recovery_jobs_by_action(
        &self,
        action: RecoveryAction,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            let jobs = recovery_job::list_due_pending_jobs_by_action(
                &self.connection,
                action,
                unix_to_datetime(now_unix),
                limit,
            )
            .await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn get_recovery_job(&self, job_id: &str) -> Result<Option<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            Ok(recovery_job::find_job_by_id(&self.connection, job_id)
                .await?
                .map(recovery_job_record_from_model))
        })
        .await
    }

    pub async fn mark_recovery_job_retrying(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_job_retrying(
                &self.connection,
                job_id,
                active_attempt_id,
                unix_to_datetime(next_run_at_unix),
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_claimed_recovery_job_retrying(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_claimed_job_retrying(
                &self.connection,
                job_id,
                claim_token,
                unix_to_datetime(next_run_at_unix),
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_claimed_recovery_job_active(
        &self,
        job_id: &str,
        claim_token: &str,
        active_attempt_id: &str,
        now_unix: i64,
    ) -> Result<ClaimedRecoveryActivation> {
        self.run_serialized_write(|| async {
            let outcome = recovery_job::mark_claimed_job_active(
                &self.connection,
                job_id,
                claim_token,
                active_attempt_id,
                unix_to_datetime(now_unix),
            )
            .await?;
            Ok(match outcome {
                recovery_job::ClaimedJobActivation::Activated => {
                    ClaimedRecoveryActivation::Activated
                }
                recovery_job::ClaimedJobActivation::BlockedByActiveRecovery => {
                    ClaimedRecoveryActivation::BlockedByActiveRecovery
                }
                recovery_job::ClaimedJobActivation::ClaimNotFound => {
                    ClaimedRecoveryActivation::ClaimNotFound
                }
            })
        })
        .await
    }

    pub async fn release_claimed_recovery_job(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::release_claimed_job(
                &self.connection,
                job_id,
                claim_token,
                unix_to_datetime(next_run_at_unix),
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_due_pending_recovery_job_terminal_if_turn_idle(
        &self,
        job_id: &str,
        action: RecoveryAction,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            let tx = self
                .connection
                .begin()
                .await
                .context("failed to begin due pending recovery terminal transaction")?;
            let affected = match recovery_job::mark_due_pending_job_terminal_if_turn_idle(
                &tx,
                job_id,
                action,
                status,
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
            {
                Ok(affected) => affected,
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            };
            tx.commit()
                .await
                .context("failed to commit due pending recovery terminal transaction")?;
            Ok(affected)
        })
        .await
    }

    pub async fn mark_claimed_recovery_job_terminal(
        &self,
        job_id: &str,
        claim_token: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_claimed_job_terminal(
                &self.connection,
                job_id,
                claim_token,
                status,
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_job_terminal(
                &self.connection,
                job_id,
                status,
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_malformed_active_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_active_without_attempt_terminal(
                &self.connection,
                job_id,
                status,
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn mark_recovery_job_terminal_after_attempt(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let last_error_value = last_error.clone();
        self.run_serialized_write(|| async {
            recovery_job::mark_job_terminal_after_attempt(
                &self.connection,
                job_id,
                active_attempt_id,
                status,
                last_error_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn find_recovery_jobs_by_turn_and_status(
        &self,
        turn_id: &str,
        status: RecoveryJobStatus,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            let jobs =
                recovery_job::find_jobs_by_turn_and_status(&self.connection, turn_id, status)
                    .await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn resume_blocked_turn_recovery(
        &self,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
    ) -> Result<BlockedTurnRecoveryResumeOutcome> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(now_unix);
            let tx = self
                .connection
                .begin()
                .await
                .context("failed to begin blocked turn resume transaction")?;

            let Some(turn_model) =
                turn::find_turn_by_thread_and_id(&tx, thread_id, turn_id).await?
            else {
                tx.rollback()
                    .await
                    .context("failed to rollback missing blocked turn resume transaction")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
            };
            if turn_status_from_db(turn_model.status.as_str()) != Some(TurnStatus::Blocked) {
                tx.rollback()
                    .await
                    .context("failed to rollback non-blocked turn resume transaction")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
            }

            let Some(job) =
                recovery_job::find_blocked_job_by_turn(&tx, turn_id, recovery_job_id).await?
            else {
                tx.rollback()
                    .await
                    .context("failed to rollback blocked turn resume without recovery job")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
            };

            let item_type = turn_item_type_from_db(job.item_type.as_str())
                .unwrap_or(TurnItemType::DynamicToolCall);
            if item_type.is_tool_item()
                && turn_runtime_snapshot::find_turn_runtime_snapshot(&tx, turn_id)
                    .await?
                    .is_none()
            {
                let recovery_job_id = job.id.clone();
                tx.rollback()
                    .await
                    .context("failed to rollback blocked tool recovery without snapshot")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot {
                    recovery_job_id,
                });
            }

            let max_attempts = job.max_attempts.max(job.run_count.saturating_add(1));
            let resumed = recovery_job::resume_blocked_job(
                &tx,
                &job,
                RecoveryAction::RestartTurn,
                max_attempts,
                now,
            )
            .await?;
            if !resumed {
                tx.rollback()
                    .await
                    .context("failed to rollback stale blocked recovery resume transaction")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
            }

            let updated = turn::update_turn_status(
                &tx,
                thread_id,
                turn_id,
                TurnStatus::InProgress,
                None,
                now,
            )
            .await?;
            if !updated {
                tx.rollback()
                    .await
                    .context("failed to rollback blocked turn resume after status miss")?;
                return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
            }
            turn::append_turn_status_history(
                &tx,
                turn_id,
                TurnStatus::InProgress,
                Some("blocked turn resumed by explicit request".to_owned()),
                now,
            )
            .await?;

            tx.commit()
                .await
                .context("failed to commit blocked turn resume transaction")?;

            Ok(
                recovery_job::find_job_by_id(&self.connection, job.id.as_str())
                    .await?
                    .map(recovery_job_record_from_model)
                    .map(BlockedTurnRecoveryResumeOutcome::Resumed)
                    .unwrap_or(BlockedTurnRecoveryResumeOutcome::NotFound),
            )
        })
        .await
    }

    pub async fn list_active_recovery_jobs(&self, limit: u64) -> Result<Vec<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            let jobs = recovery_job::list_active_jobs(&self.connection, limit).await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn find_open_recovery_jobs_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.run_serialized_write(|| async {
            let jobs = recovery_job::find_open_jobs_by_turn(&self.connection, turn_id).await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn cancel_open_recovery_jobs_for_turn(
        &self,
        turn_id: &str,
        exclude_job_id: Option<&str>,
        reason: Option<String>,
        now_unix: i64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        let reason_value = reason.clone();
        self.run_serialized_write(|| async {
            let jobs = recovery_job::cancel_open_jobs_for_turn(
                &self.connection,
                turn_id,
                exclude_job_id,
                reason_value.clone(),
                unix_to_datetime(now_unix),
            )
            .await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    async fn materialize_turn_event(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(event, event_timestamp_secs, None)
            .await
    }

    async fn materialize_turn_event_with_attempt_deadlines(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
    ) -> Result<()> {
        let projection_context = TurnEventProjectionContext {
            item_started_deadlines,
        };
        let claim_token = generate_id(DB_ID_LEN);
        let created_at = unix_to_datetime(event_timestamp_secs);
        let claim_expires_at =
            unix_to_datetime(event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));

        let appended_event = self
            .run_serialized_write(|| {
                self.append_claimed_turn_event_projection_once(
                    event.clone(),
                    created_at,
                    projection_context.clone(),
                    claim_token.clone(),
                    claim_expires_at,
                )
            })
            .await?;

        if let Err(error) = self
            .run_serialized_write(|| {
                self.project_claimed_turn_event_once(
                    appended_event.clone(),
                    projection_context.clone(),
                    claim_token.clone(),
                    created_at,
                )
            })
            .await
        {
            let error_message = format!("{error:#}");
            if let Err(mark_error) = self
                .run_serialized_write(|| {
                    self.mark_turn_event_projection_failure_once(
                        appended_event.id.clone(),
                        claim_token.clone(),
                        0,
                        error_message.clone(),
                        event_timestamp_secs,
                        false,
                    )
                })
                .await
            {
                return Err(error).with_context(|| {
                    format!(
                        "failed to persist turn event projection failure for event `{}`: {mark_error:#}",
                        appended_event.id
                    )
                });
            }
            return Err(error);
        }

        Ok(())
    }

    async fn materialize_turn_events_atomically(
        &self,
        events: Vec<TurnEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let created_at = unix_to_datetime(event_timestamp_secs);
        let claim_expires_at =
            unix_to_datetime(event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));

        self.run_serialized_write(|| {
            self.materialize_turn_events_atomically_once(
                events.clone(),
                created_at,
                claim_expires_at,
            )
        })
        .await
    }

    async fn materialize_turn_events_atomically_once(
        &self,
        events: Vec<TurnEventPayload>,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: DateTimeWithTimeZone,
    ) -> Result<()> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event batch materialization transaction")?;

        for event in events {
            if let Err(error) = validate_turn_event_for_permanent_storage(&event).await {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            let projection_context = TurnEventProjectionContext::default();
            let claim_token = generate_id(DB_ID_LEN);
            let appended_event =
                match turn_event::append_event(&transaction, &event, created_at).await {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };

            let projection_context_json = match serialize_turn_event_projection_context(
                &projection_context,
                appended_event.id.as_str(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            if let Err(error) = turn_event_projection_state::insert_claimed(
                &transaction,
                turn_event_projection_state::NewTurnEventProjectionState {
                    event_id: appended_event.id.clone(),
                    thread_id: appended_event.thread_id.clone(),
                    turn_id: appended_event.turn_id.clone(),
                    sequence: appended_event.sequence,
                    projection_context_json,
                    claim_token: claim_token.clone(),
                    claim_expires_at,
                    created_at,
                },
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            let has_unprojected_predecessor =
                match turn_event_projection_state::has_unprojected_predecessor(
                    &transaction,
                    appended_event.turn_id.as_str(),
                    appended_event.sequence,
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
            if has_unprojected_predecessor {
                let _ = transaction.rollback().await;
                anyhow::bail!(
                    "turn event projection `{}` is waiting for an earlier event in turn `{}`",
                    appended_event.id,
                    appended_event.turn_id
                );
            }

            if let Err(error) = self
                .projector
                .project(&transaction, &appended_event)
                .await
                .context("failed to project turn event to read models")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            if let Err(error) =
                crate::timeline_live_projection::project_semantic_timeline_live_turn_event(
                    &transaction,
                    &appended_event,
                )
                .await
                .context("failed to project turn event to semantic timeline")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            let projected = match turn_event_projection_state::mark_projected_claimed(
                &transaction,
                appended_event.id.as_str(),
                claim_token.as_str(),
                created_at,
            )
            .await
            {
                Ok(projected) => projected,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            if !projected {
                let _ = transaction.rollback().await;
                anyhow::bail!(
                    "turn event projection `{}` is no longer claimed by this worker",
                    appended_event.id
                );
            }
        }

        transaction
            .commit()
            .await
            .context("failed to commit turn event batch materialization transaction")?;

        Ok(())
    }

    async fn append_claimed_turn_event_projection_once(
        &self,
        event: TurnEventPayload,
        created_at: DateTimeWithTimeZone,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        claim_expires_at: DateTimeWithTimeZone,
    ) -> Result<crate::events::AppendedTurnEvent> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event append transaction")?;

        validate_turn_event_for_permanent_storage(&event).await?;

        let appended_event = match turn_event::append_event(&transaction, &event, created_at).await
        {
            Ok(event) => event,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        let projection_context_json = match serialize_turn_event_projection_context(
            &projection_context,
            appended_event.id.as_str(),
        ) {
            Ok(value) => value,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if let Err(error) = turn_event_projection_state::insert_claimed(
            &transaction,
            turn_event_projection_state::NewTurnEventProjectionState {
                event_id: appended_event.id.clone(),
                thread_id: appended_event.thread_id.clone(),
                turn_id: appended_event.turn_id.clone(),
                sequence: appended_event.sequence,
                projection_context_json,
                claim_token,
                claim_expires_at,
                created_at,
            },
        )
        .await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        transaction
            .commit()
            .await
            .context("failed to commit turn event append transaction")?;

        Ok(appended_event)
    }

    async fn project_claimed_turn_event_once(
        &self,
        appended_event: crate::events::AppendedTurnEvent,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        projected_at: DateTimeWithTimeZone,
    ) -> Result<()> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event projection transaction")?;

        let has_unprojected_predecessor =
            match turn_event_projection_state::has_unprojected_predecessor(
                &transaction,
                appended_event.turn_id.as_str(),
                appended_event.sequence,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
        if has_unprojected_predecessor {
            let _ = transaction.rollback().await;
            anyhow::bail!(
                "turn event projection `{}` is waiting for an earlier event in turn `{}`",
                appended_event.id,
                appended_event.turn_id
            );
        }

        if let Err(error) = self
            .projector
            .project(&transaction, &appended_event)
            .await
            .context("failed to project turn event to read models")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        if let (TurnEventPayload::ItemStarted(notification), Some(deadlines)) = (
            &appended_event.payload,
            projection_context.item_started_deadlines,
        ) {
            let configured = turn_item_attempt::configure_running_attempt_deadlines(
                &transaction,
                notification.turn_id.as_str(),
                notification.item.item_id(),
                appended_event.created_at,
                deadlines.lease_expires_at_unix.map(unix_to_datetime),
                deadlines.idle_deadline_at_unix.map(unix_to_datetime),
                deadlines.hard_deadline_at_unix.map(unix_to_datetime),
            )
            .await
            .context("failed to configure item attempt deadlines during item/started projection")?;
            if !configured {
                let _ = transaction.rollback().await;
                anyhow::bail!(
                    "item/started projection did not create a running attempt for item `{}`",
                    notification.item.item_id()
                );
            }
        }

        if let Err(error) =
            crate::timeline_live_projection::project_semantic_timeline_live_turn_event(
                &transaction,
                &appended_event,
            )
            .await
            .context("failed to project turn event to semantic timeline")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        let projected = match turn_event_projection_state::mark_projected_claimed(
            &transaction,
            appended_event.id.as_str(),
            claim_token.as_str(),
            projected_at,
        )
        .await
        {
            Ok(projected) => projected,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if !projected {
            let _ = transaction.rollback().await;
            anyhow::bail!(
                "turn event projection `{}` is no longer claimed by this worker",
                appended_event.id
            );
        }

        transaction
            .commit()
            .await
            .context("failed to commit turn event projection transaction")?;

        Ok(())
    }

    async fn mark_turn_event_projection_failure_once(
        &self,
        event_id: String,
        claim_token: String,
        attempt_count: i64,
        error_message: String,
        failed_at_unix: i64,
        force_exhausted: bool,
    ) -> Result<bool> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event projection failure transaction")?;

        let failed_at = unix_to_datetime(failed_at_unix);
        let exhausted = force_exhausted
            || attempt_count.saturating_add(1) >= TURN_EVENT_PROJECTION_MAX_ATTEMPTS;
        let marked = if exhausted {
            turn_event_projection_state::mark_exhausted_claimed(
                &transaction,
                event_id.as_str(),
                claim_token.as_str(),
                error_message,
                failed_at,
            )
            .await
        } else {
            let next_run_at = unix_to_datetime(
                failed_at_unix
                    .saturating_add(turn_event_projection_retry_delay_secs(attempt_count)),
            );
            turn_event_projection_state::mark_failed_claimed(
                &transaction,
                event_id.as_str(),
                claim_token.as_str(),
                error_message,
                next_run_at,
                failed_at,
            )
            .await
        };

        let marked = match marked {
            Ok(marked) => marked,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        transaction
            .commit()
            .await
            .context("failed to commit turn event projection failure transaction")?;

        Ok(marked)
    }

    pub async fn replay_due_turn_event_projections(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<TurnProjectionReplaySummary> {
        let now = unix_to_datetime(now_unix);
        let claim_expires_at =
            unix_to_datetime(now_unix.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
        let claimed = self
            .run_serialized_write(|| {
                turn_event_projection_state::claim_due(
                    &self.connection,
                    now,
                    claim_expires_at,
                    limit,
                )
            })
            .await?;

        let mut summary = TurnProjectionReplaySummary {
            claimed: claimed.len(),
            ..TurnProjectionReplaySummary::default()
        };

        for claimed_projection in claimed {
            let event_id = claimed_projection.state.event_id.clone();
            let thread_id = claimed_projection.state.thread_id.clone();
            let turn_id = claimed_projection.state.turn_id.clone();
            let claim_token = claimed_projection.claim_token.clone();
            let attempt_count = claimed_projection.state.attempt_count;

            let Some(appended_event) = turn_event::find_event_by_id(&self.connection, &event_id)
                .await
                .with_context(|| {
                    format!("failed to load raw turn event `{event_id}` for projection replay")
                })?
            else {
                let marked = self
                    .run_serialized_write(|| {
                        self.mark_turn_event_projection_failure_once(
                            event_id.clone(),
                            claim_token.clone(),
                            attempt_count,
                            "raw turn_event row is missing for projection replay".to_owned(),
                            now_unix,
                            true,
                        )
                    })
                    .await?;
                summary.missing_events += 1;
                if marked {
                    summary.exhausted += 1;
                    summary
                        .exhausted_records
                        .push(TurnProjectionReplayExhaustedRecord {
                            event_id: event_id.clone(),
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            error_message: "raw turn_event row is missing for projection replay"
                                .to_owned(),
                        });
                }
                continue;
            };

            let projection_context = match deserialize_turn_event_projection_context(
                claimed_projection.state.projection_context_json.as_str(),
                event_id.as_str(),
            ) {
                Ok(context) => context,
                Err(error) => {
                    let error_message = format!("{error:#}");
                    let marked = self
                        .run_serialized_write(|| {
                            self.mark_turn_event_projection_failure_once(
                                event_id.clone(),
                                claim_token.clone(),
                                attempt_count,
                                error_message.clone(),
                                now_unix,
                                true,
                            )
                        })
                        .await?;
                    if marked {
                        summary.exhausted += 1;
                        summary
                            .exhausted_records
                            .push(TurnProjectionReplayExhaustedRecord {
                                event_id: event_id.clone(),
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                error_message,
                            });
                    }
                    continue;
                }
            };

            let projected = self
                .run_serialized_write(|| {
                    self.project_claimed_turn_event_once(
                        appended_event.clone(),
                        projection_context.clone(),
                        claim_token.clone(),
                        now,
                    )
                })
                .await;

            match projected {
                Ok(()) => {
                    summary.projected += 1;
                }
                Err(error) => {
                    let error_message = format!("{error:#}");
                    let will_exhaust =
                        attempt_count.saturating_add(1) >= TURN_EVENT_PROJECTION_MAX_ATTEMPTS;
                    let marked = self
                        .run_serialized_write(|| {
                            self.mark_turn_event_projection_failure_once(
                                event_id.clone(),
                                claim_token.clone(),
                                attempt_count,
                                error_message.clone(),
                                now_unix,
                                false,
                            )
                        })
                        .await?;
                    if marked {
                        if will_exhaust {
                            summary.exhausted += 1;
                            summary
                                .exhausted_records
                                .push(TurnProjectionReplayExhaustedRecord {
                                    event_id: event_id.clone(),
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    error_message,
                                });
                        } else {
                            summary.failed += 1;
                        }
                    }
                }
            }
        }

        Ok(summary)
    }

    async fn append_task_event_once(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<AppendedTaskEvent> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task event materialization transaction")?;

        let created_at = unix_to_datetime(event_timestamp_secs);
        let idempotency_key = event.idempotency_key();

        let mut appended_event = match task_event::append_event(
            &transaction,
            &event,
            created_at,
            idempotency_key.as_deref(),
        )
        .await
        {
            Ok(event) => event,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if appended_event.append_status.is_inserted() {
            if let Err(error) = self
                .task_projector
                .project(&transaction, &appended_event)
                .await
                .context("failed to project task event to read models")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        if let Err(error) = hydrate_task_event_metadata(&transaction, &mut appended_event)
            .await
            .context("failed to hydrate task event metadata")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        transaction
            .commit()
            .await
            .context("failed to commit task event materialization transaction")?;

        Ok(appended_event)
    }

    async fn append_task_events_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task event batch materialization transaction")?;

        let appended_events = match self
            .append_task_events_in_connection(&transaction, events, event_timestamp_secs)
            .await
        {
            Ok(appended_events) => appended_events,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        transaction
            .commit()
            .await
            .context("failed to commit task event batch materialization transaction")?;

        Ok(appended_events)
    }

    async fn append_due_trigger_task_events_once(
        &self,
        trigger_id: String,
        expected_next_fire_at: i64,
        now: i64,
        events: Vec<TaskEventPayload>,
        reserve_executions: Vec<(String, TaskExecutorKind)>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin due task trigger materialization transaction")?;

        let result = async {
            let Some(trigger) =
                task_trigger::find_trigger_by_id(&transaction, trigger_id.as_str()).await?
            else {
                return Ok(Vec::new());
            };
            if trigger.status != "active"
                || trigger.next_fire_at.map(|value| value.timestamp())
                    != Some(expected_next_fire_at)
                || expected_next_fire_at > now
            {
                return Ok(Vec::new());
            }

            let appended_events = self
                .append_task_events_in_connection(&transaction, events, now)
                .await?;
            for (run_id, executor_kind) in reserve_executions {
                let _ = reserve_execution_for_run_in_connection(
                    &transaction,
                    run_id,
                    executor_kind,
                    now,
                )
                .await?;
            }
            Ok(appended_events)
        }
        .await;

        match result {
            Ok(appended_events) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit due task trigger materialization transaction")?;
                Ok(appended_events)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn append_task_events_in_connection<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let created_at = unix_to_datetime(event_timestamp_secs);
        let mut appended_events = Vec::with_capacity(events.len());

        for event in events {
            let idempotency_key = event.idempotency_key();
            let mut appended_event =
                task_event::append_event(db, &event, created_at, idempotency_key.as_deref())
                    .await?;

            if appended_event.append_status.is_inserted() {
                self.task_projector
                    .project(db, &appended_event)
                    .await
                    .context("failed to project task event to read models")?;
            }

            hydrate_task_event_metadata(db, &mut appended_event)
                .await
                .context("failed to hydrate task event metadata")?;
            appended_events.push(appended_event);
        }

        Ok(appended_events)
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.write_coordinator
            .run_serialized_with_retry(operation, is_anyhow_sqlite_lock)
            .await
    }
}

const TURN_EVENT_PROJECTION_LEASE_SECS: i64 = 120;
const TURN_EVENT_PROJECTION_MAX_ATTEMPTS: i64 = 10;

fn turn_event_projection_retry_delay_secs(attempt_count: i64) -> i64 {
    let exponent = attempt_count.clamp(0, 6) as u32;
    2_i64.saturating_pow(exponent).max(2)
}

fn serialize_turn_event_projection_context(
    context: &TurnEventProjectionContext,
    event_id: &str,
) -> Result<String> {
    serde_json::to_string(context).with_context(|| {
        format!("failed to serialize turn event projection context for event `{event_id}`")
    })
}

fn deserialize_turn_event_projection_context(
    context_json: &str,
    event_id: &str,
) -> Result<TurnEventProjectionContext> {
    serde_json::from_str(context_json).with_context(|| {
        format!("failed to deserialize turn event projection context for event `{event_id}`")
    })
}

fn remember_agent_diff_snapshot_payload(
    latest_agent_diff_payload_by_item_id: &mut HashMap<String, String>,
    item: &TurnItem,
) -> Result<()> {
    if !is_agent_diff_updated_item(item) {
        return Ok(());
    }

    let payload_json =
        serde_json::to_string(item).context("failed to serialize agent diff turn item payload")?;
    latest_agent_diff_payload_by_item_id.insert(item.item_id().to_owned(), payload_json);
    Ok(())
}

async fn latest_agent_diff_raw_payload_for_item<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<String>> {
    let rows = turn_event::list_events_for_turn(db, thread_id, turn_id).await?;
    let mut latest_payload = None;

    for row in rows {
        let payload = serde_json::from_str::<TurnEventPayload>(row.payload.as_str())
            .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;
        let item = match payload {
            TurnEventPayload::ItemCompleted(notification) => notification.item,
            TurnEventPayload::ItemUpdated(notification) => notification.item,
            _ => continue,
        };
        if item.item_id() != item_id || !is_agent_diff_updated_item(&item) {
            continue;
        }
        latest_payload = Some(
            serde_json::to_string(&item)
                .context("failed to serialize raw agent diff turn item payload")?,
        );
    }

    Ok(latest_payload)
}

async fn append_agent_diff_snapshot_turn_item_events<C: ConnectionTrait>(
    db: &C,
    events: &mut Vec<TurnItemEvent>,
    latest_agent_diff_payload_by_item_id: &HashMap<String, String>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    last_sequence: i64,
) -> Result<()> {
    let rows = turn::list_turn_items_by_type(db, turn_id, "system_event").await?;
    for row in rows {
        let item = serde_json::from_str::<TurnItem>(row.payload.as_str()).with_context(|| {
            format!(
                "failed to decode snapshot turn_item payload for turn `{turn_id}` item `{}`",
                row.item_id
            )
        })?;
        if !is_agent_diff_updated_item(&item) {
            continue;
        }

        let payload_json = serde_json::to_string(&item)
            .context("failed to serialize snapshot agent diff turn item payload")?;
        if latest_agent_diff_payload_by_item_id
            .get(item.item_id())
            .is_some_and(|latest_payload| latest_payload == &payload_json)
        {
            continue;
        }

        events.push(TurnItemEvent {
            sequence: last_sequence,
            created_at: row.updated_at.timestamp_millis(),
            payload: TurnItemEventPayload::ItemUpdated {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item,
            },
        });
    }

    Ok(())
}

fn is_agent_diff_updated_item(item: &TurnItem) -> bool {
    matches!(
        item,
        TurnItem::SystemEvent {
            code: Some(code),
            ..
        } if code == "agent_diff_updated"
    )
}

fn memory_event_for_record(
    event: Option<NewAgentMemoryEvent>,
    memory_id: String,
    workspace_id: Option<String>,
    default_event_kind: &str,
    created_at_unix: i64,
) -> NewAgentMemoryEvent {
    match event {
        Some(mut event) => {
            if event.memory_id.is_none() {
                event.memory_id = Some(memory_id);
            }
            if event.workspace_id.is_none() {
                event.workspace_id = workspace_id;
            }
            event
        }
        None => NewAgentMemoryEvent {
            memory_id: Some(memory_id),
            candidate_id: None,
            workspace_id,
            event_kind: default_event_kind.to_owned(),
            actor: None,
            thread_id: None,
            turn_id: None,
            item_id: None,
            details_json: None,
            created_at_unix,
        },
    }
}

fn mcp_server_installation_record_from_model(
    model: pioneer_entity::mcp_server_installation::Model,
) -> McpServerInstallationRecord {
    McpServerInstallationRecord {
        id: Some(model.id),
        scope_kind: model.scope_kind,
        scope_key: model.scope_key,
        name: model.name,
        display_name: model.display_name,
        source_kind: model.source_kind,
        source_ref: model.source_ref,
        transport_kind: model.transport_kind,
        transport_json: model.transport_json,
        auth_json: model.auth_json,
        secret_refs_json: model.secret_refs_json,
        enabled: model.enabled,
        allow_implicit_invocation: model.allow_implicit_invocation,
        required: model.required,
        fingerprint: model.fingerprint,
        updated_at_unix: model.updated_at.timestamp(),
    }
}

fn task_from_db_model(model: pioneer_entity::task::Model) -> Result<Task> {
    let owner_kind = task_owner_kind_from_db(model.owner_kind.as_str())
        .with_context(|| format!("unknown task owner kind `{}`", model.owner_kind))?;
    let executor_kind = task_executor_kind_from_db(model.executor_kind.as_str())
        .with_context(|| format!("unknown task executor kind `{}`", model.executor_kind))?;
    let status = task_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task status `{}`", model.status))?;

    Ok(Task {
        id: model.id,
        workspace_id: model.workspace_id,
        owner_kind,
        owner_id: model.owner_id,
        created_by_thread_id: model.created_by_thread_id,
        created_by_turn_id: model.created_by_turn_id,
        root_task_id: model.root_task_id,
        parent_task_id: model.parent_task_id,
        executor_kind,
        status,
        title: model.title,
        goal: model.goal,
        priority: model.priority,
        lifecycle_policy: optional_typed_json_from_db(model.lifecycle_policy_json)?,
        delivery_policy: optional_typed_json_from_db(model.delivery_policy_json)?,
        retry_policy: optional_typed_json_from_db(model.retry_policy_json)?,
        timeout_policy: optional_typed_json_from_db(model.timeout_policy_json)?,
        concurrency_policy: optional_typed_json_from_db(model.concurrency_policy_json)?,
        metadata: optional_typed_json_from_db(model.metadata_json)?,
        result: optional_typed_json_from_db(model.result_json)?,
        error: optional_typed_json_from_db(model.error_json)?,
        revision: model.revision,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
        completed_at: model.completed_at.map(|value| value.timestamp()),
    })
}

async fn hydrate_task_event_metadata<C: ConnectionTrait>(
    db: &C,
    event: &mut AppendedTaskEvent,
) -> Result<()> {
    if let Some(task) = task_repository::find_task_by_id(db, event.task_id.as_str()).await? {
        event.workspace_id = Some(task.workspace_id);
        event.root_task_id = task.root_task_id;
        event.parent_task_id = task.parent_task_id;
    }
    Ok(())
}

fn task_trigger_from_db_model(model: pioneer_entity::task_trigger::Model) -> Result<TaskTrigger> {
    let stored_kind = task_trigger_kind_from_db(model.kind.as_str())
        .with_context(|| format!("unknown task trigger kind `{}`", model.kind))?;
    let status = task_trigger_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task trigger status `{}`", model.status))?;
    let spec: TaskTriggerSpec = serde_json::from_str(model.spec_json.as_str())
        .with_context(|| format!("failed to decode task trigger spec `{}`", model.id))?;
    anyhow::ensure!(
        stored_kind == spec.kind(),
        "task trigger `{}` has kind `{}` but spec kind `{:?}`",
        model.id,
        model.kind,
        spec.kind()
    );

    Ok(TaskTrigger {
        id: model.id,
        task_id: model.task_id,
        status,
        spec,
        next_fire_at: model.next_fire_at.map(|value| value.timestamp()),
        last_fire_at: model.last_fire_at.map(|value| value.timestamp()),
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_run_from_db_model(model: pioneer_entity::task_run::Model) -> Result<TaskRun> {
    let status = task_run_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task run status `{}`", model.status))?;
    let executor_kind = task_executor_kind_from_db(model.executor_kind.as_str())
        .with_context(|| format!("unknown task run executor kind `{}`", model.executor_kind))?;

    Ok(TaskRun {
        id: model.id,
        task_id: model.task_id,
        trigger_id: model.trigger_id,
        parent_run_id: model.parent_run_id,
        run_group_id: model.run_group_id,
        attempt_number: u32::try_from(model.attempt_number)
            .context("task run attempt_number is out of range")?,
        retry_of_run_id: model.retry_of_run_id,
        ready_at: model.ready_at.map(|value| value.timestamp()),
        run_number: model.run_number,
        status,
        executor_kind,
        started_at: model.started_at.map(|value| value.timestamp()),
        completed_at: model.completed_at.map(|value| value.timestamp()),
        heartbeat_at: model.heartbeat_at.map(|value| value.timestamp()),
        locked_by: model.locked_by,
        lock_expires_at: model.lock_expires_at.map(|value| value.timestamp()),
        result: optional_typed_json_from_db(model.result_json)?,
        error: optional_typed_json_from_db(model.error_json)?,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_run_execution_from_db_model(
    model: pioneer_entity::task_run_execution::Model,
) -> Result<TaskRunExecution> {
    let executor_kind =
        task_executor_kind_from_db(model.executor_kind.as_str()).with_context(|| {
            format!(
                "unknown task run execution executor kind `{}`",
                model.executor_kind
            )
        })?;
    let status = task_run_execution_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task run execution status `{}`", model.status))?;

    Ok(TaskRunExecution {
        id: model.id,
        task_id: model.task_id,
        task_run_id: model.task_run_id,
        executor_kind,
        status,
        worker_id: model.worker_id,
        lease_until: model.lease_until.map(|value| value.timestamp()),
        heartbeat_at: model.heartbeat_at.map(|value| value.timestamp()),
        started_at: model.started_at.map(|value| value.timestamp()),
        completed_at: model.completed_at.map(|value| value.timestamp()),
        result: optional_typed_json_from_db(model.result_json)?,
        error: optional_typed_json_from_db(model.error_json)?,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_run_thread_binding_from_db_model(
    model: pioneer_entity::task_run_thread_binding::Model,
) -> Result<TaskRunThreadBinding> {
    Ok(TaskRunThreadBinding {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        execution_id: model.execution_id,
        thread_id: model.thread_id,
        binding_kind: task_run_thread_binding_kind_from_db(model.binding_kind.as_str())?,
        created_at: model.created_at.timestamp(),
    })
}

fn task_run_turn_from_db_model(model: pioneer_entity::task_run_turn::Model) -> Result<TaskRunTurn> {
    Ok(TaskRunTurn {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        execution_id: model.execution_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        kind: task_run_turn_kind_from_db(model.kind.as_str())?,
        round: u32::try_from(model.round).context("task run turn round is out of range")?,
        sequence: u32::try_from(model.sequence)
            .context("task run turn sequence is out of range")?,
        status: task_run_turn_status_from_db(model.status.as_str())?,
        reviews_candidate_id: model.reviews_candidate_id,
        requested_by_candidate_id: model.requested_by_candidate_id,
        requested_by_review_event_id: model.requested_by_review_event_id,
        created_at: model.created_at.timestamp(),
        started_at: model.started_at.map(|value| value.timestamp()),
        completed_at: model.completed_at.map(|value| value.timestamp()),
    })
}

fn task_result_candidate_from_db_model(
    model: pioneer_entity::task_result_candidate::Model,
) -> Result<TaskResultCandidate> {
    Ok(TaskResultCandidate {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        task_run_turn_id: model.task_run_turn_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        round: u32::try_from(model.round).context("task result candidate round is out of range")?,
        status: task_result_candidate_status_from_db(model.status.as_str())?,
        result: optional_typed_json_from_db(model.result_json)?,
        extraction_error: optional_typed_json_from_db(model.extraction_error_json)?,
        summary: model.summary,
        diagnostics: optional_typed_json_from_db(model.diagnostics_json)?.unwrap_or_default(),
        final_review_event_id: model.final_review_event_id,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
        resolved_at: model.resolved_at.map(|value| value.timestamp()),
    })
}

fn task_result_review_event_from_db_model(
    model: pioneer_entity::task_result_review_event::Model,
) -> Result<TaskResultReviewEvent> {
    Ok(TaskResultReviewEvent {
        id: model.id,
        candidate_id: model.candidate_id,
        task_id: model.task_id,
        run_id: model.run_id,
        task_run_turn_id: model.task_run_turn_id,
        reviewer_kind: task_result_reviewer_kind_from_db(model.reviewer_kind.as_str())?,
        reviewer_thread_id: model.reviewer_thread_id,
        reviewer_turn_id: model.reviewer_turn_id,
        reviewer_user_id: model.reviewer_user_id,
        reviewer_agent_spec_id: model.reviewer_agent_spec_id,
        event_kind: task_result_review_event_kind_from_db(model.event_kind.as_str())?,
        decision: task_result_review_decision_from_db(model.decision.as_str())?,
        feedback_text: model.feedback_text,
        feedback: optional_typed_json_from_db(model.feedback_json)?,
        confidence: model.confidence,
        supersedes_review_event_id: model.supersedes_review_event_id,
        next_task_run_turn_id: model.next_task_run_turn_id,
        created_at: model.created_at.timestamp(),
    })
}

fn task_delivery_from_db_model(
    model: pioneer_entity::task_delivery::Model,
) -> Result<TaskDelivery> {
    let mode = task_delivery_mode_from_db(model.mode.as_str())
        .with_context(|| format!("unknown task delivery mode `{}`", model.mode))?;
    let status = task_delivery_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task delivery status `{}`", model.status))?;
    Ok(TaskDelivery {
        id: model.id,
        workspace_id: model.workspace_id,
        task_id: model.task_id,
        run_id: model.run_id,
        delivery_key: model.delivery_key,
        mode,
        target_thread_id: model.target_thread_id,
        target_user_id: model.target_user_id,
        webhook_url: model.webhook_url,
        webhook_url_fingerprint: model.webhook_url_fingerprint,
        status,
        next_attempt_at: model.next_attempt_at.map(|value| value.timestamp()),
        attempt_count: u32::try_from(model.attempt_count).unwrap_or(0),
        max_attempts: u32::try_from(model.max_attempts).unwrap_or(1),
        result_snapshot: optional_typed_json_from_db(model.result_snapshot_json)?,
        error_snapshot: optional_typed_json_from_db(model.error_snapshot_json)?,
        delivered_turn_id: model.delivered_turn_id,
        delivered_notification_id: model.delivered_notification_id,
        delivered_at: model.delivered_at.map(|value| value.timestamp()),
        last_error: model.last_error,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_delivery_attempt_from_db_model(
    model: pioneer_entity::task_delivery_attempt::Model,
) -> Result<TaskDeliveryAttempt> {
    let status = task_delivery_attempt_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task delivery attempt status `{}`", model.status))?;
    Ok(TaskDeliveryAttempt {
        id: model.id,
        delivery_id: model.delivery_id,
        attempt_number: u32::try_from(model.attempt_number).unwrap_or(0),
        status,
        started_at: model.started_at.timestamp(),
        completed_at: model.completed_at.map(|value| value.timestamp()),
        http_status: model
            .http_status
            .and_then(|value| u16::try_from(value).ok()),
        error: model.error,
        response_fingerprint: model.response_fingerprint,
    })
}

fn task_agent_spec_from_db_model(
    model: pioneer_entity::task_agent_spec::Model,
) -> Result<TaskAgentSpec> {
    Ok(TaskAgentSpec {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        agent_role: model.agent_role,
        agent_nickname: model.agent_nickname,
        model: model.model,
        model_provider: model.model_provider,
        prompt: typed_json_from_db(model.prompt_json)?,
        context_policy: optional_typed_json_from_db(model.context_policy_json)?,
        tool_policy: optional_typed_json_from_db(model.tool_policy_json)?,
        permission_cap: optional_typed_json_from_db(model.permission_cap_json)?,
        result_contract: optional_typed_json_from_db(model.result_contract_json)?,
        review_policy: optional_typed_json_from_db(model.review_policy_json)?,
        depth: model.depth,
        max_depth: model.max_depth,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_dependency_from_db_model(
    model: pioneer_entity::task_dependency::Model,
) -> Result<TaskDependency> {
    Ok(TaskDependency {
        id: model.id,
        task_id: model.task_id,
        depends_on_task_id: model.depends_on_task_id,
        kind: model.kind,
        condition: optional_typed_json_from_db(model.condition_json)?,
        created_at: model.created_at.timestamp(),
    })
}

fn task_write_lock_from_db_model(
    model: pioneer_entity::task_write_lock::Model,
) -> Result<TaskWriteLock> {
    let scope_kind = task_write_lock_scope_kind_from_db(model.scope_kind.as_str())
        .with_context(|| format!("unknown task write lock scope kind `{}`", model.scope_kind))?;
    let status = task_write_lock_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task write lock status `{}`", model.status))?;
    let conflict_policy = task_concurrency_conflict_policy_from_db(model.conflict_policy.as_str())
        .with_context(|| {
            format!(
                "unknown task write lock conflict policy `{}`",
                model.conflict_policy
            )
        })?;
    Ok(TaskWriteLock {
        id: model.id,
        workspace_id: model.workspace_id,
        task_id: model.task_id,
        run_id: model.run_id,
        scope_kind,
        scope_path: model.scope_path,
        status,
        acquired_at: model.acquired_at.timestamp(),
        expires_at: model.expires_at.map(|value| value.timestamp()),
        released_at: model.released_at.map(|value| value.timestamp()),
        conflict_policy,
        reason: model.reason,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

fn task_thread_lineage_from_db_model(
    model: pioneer_entity::thread_lineage::Model,
) -> TaskThreadLineage {
    TaskThreadLineage {
        child_thread_id: model.child_thread_id,
        parent_thread_id: model.parent_thread_id,
        root_thread_id: model.root_thread_id,
        depth: model.depth,
        origin_kind: model.origin_kind,
        created_by_thread_id: model.created_by_thread_id,
        created_by_turn_id: model.created_by_turn_id,
        created_at: model.created_at.timestamp(),
    }
}

fn is_missing_table_error(error: &DbErr) -> bool {
    let message = error.to_string();
    message.contains("no such table") || message.contains("no table found")
}

fn trigger_timezone(spec: &TaskTriggerSpec) -> Option<String> {
    match spec {
        TaskTriggerSpec::ScheduledAt { timezone, .. } => timezone.clone(),
        TaskTriggerSpec::Cron { timezone, .. } => Some(timezone.clone()),
        _ => None,
    }
}

fn bounded_preview(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn build_task_tree(
    task: Task,
    children_by_parent: &mut HashMap<String, Vec<Task>>,
    triggers_by_task: &mut HashMap<String, Vec<TaskTrigger>>,
    runs_by_task: &mut HashMap<String, Vec<TaskRun>>,
    specs_by_task: &mut HashMap<String, Vec<TaskAgentSpec>>,
    dependencies_by_task: &mut HashMap<String, Vec<TaskDependency>>,
    write_locks_by_task: &mut HashMap<String, Vec<TaskWriteLock>>,
) -> TaskTree {
    let task_id = task.id.clone();
    let children = children_by_parent
        .remove(task_id.as_str())
        .unwrap_or_default()
        .into_iter()
        .map(|child| {
            build_task_tree(
                child,
                children_by_parent,
                triggers_by_task,
                runs_by_task,
                specs_by_task,
                dependencies_by_task,
                write_locks_by_task,
            )
        })
        .collect();

    TaskTree {
        task,
        triggers: triggers_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        runs: runs_by_task.remove(task_id.as_str()).unwrap_or_default(),
        agent_specs: specs_by_task.remove(task_id.as_str()).unwrap_or_default(),
        dependencies: dependencies_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        write_locks: write_locks_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        children,
    }
}

async fn validate_turn_event_for_permanent_storage(event: &TurnEventPayload) -> Result<()> {
    match event {
        TurnEventPayload::ItemStarted(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)
        }
        TurnEventPayload::ItemCompleted(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)?;
            validate_terminal_tool_payload(&notification.item)
        }
        TurnEventPayload::ItemUpdated(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)
        }
        _ => Ok(()),
    }
}

fn validate_tool_payload_for_permanent_storage(item: &TurnItem) -> Result<()> {
    let Some(tool_payload) = tool_payload_parts(item) else {
        return Ok(());
    };

    let is_in_progress = tool_payload.status == ToolCallStatus::InProgress;

    validate_tool_display_policy_shape(&tool_payload, is_in_progress)?;
    validate_tool_storage_policy_shape(&tool_payload, is_in_progress)?;

    let storage_json = serde_json::to_value(tool_payload.storage)
        .context("failed to serialize tool storage for validation")?;
    let display_json = serde_json::to_value(tool_payload.display)
        .context("failed to serialize tool display for validation")?;

    if let Some(key) = contains_any_key(&storage_json, &["llmView", "llm_view"]) {
        anyhow::bail!(
            "tool item `{}` attempted to persist retained llm context key `{key}`",
            tool_payload.tool_name
        );
    }
    if let Some(key) = contains_any_key(&display_json, &["llmView", "llm_view"]) {
        anyhow::bail!(
            "tool item `{}` attempted to display retained llm context key `{key}`",
            tool_payload.tool_name
        );
    }

    Ok(())
}

fn validate_terminal_tool_payload(item: &TurnItem) -> Result<()> {
    let Some(tool_payload) = tool_payload_parts(item) else {
        return Ok(());
    };

    if tool_payload.status == ToolCallStatus::InProgress {
        anyhow::bail!(
            "terminal tool item `{}` cannot remain in_progress",
            tool_payload.tool_name
        );
    }

    Ok(())
}

struct ToolPayloadParts<'a> {
    tool_name: &'a str,
    status: ToolCallStatus,
    output_policy: &'a pioneer_protocol::ToolOutputPolicySnapshot,
    display: &'a ToolDisplayPayload,
    storage: &'a ToolStoragePayload,
}

fn tool_payload_parts(item: &TurnItem) -> Option<ToolPayloadParts<'_>> {
    match item {
        TurnItem::CommandExecution {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::FileChange {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::WebSearch {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::WebFetch {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::Download {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::DynamicToolCall {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        } => Some(ToolPayloadParts {
            tool_name,
            status: *status,
            output_policy,
            display,
            storage,
        }),
        _ => None,
    }
}

fn validate_tool_display_policy_shape(
    tool_payload: &ToolPayloadParts<'_>,
    is_in_progress: bool,
) -> Result<()> {
    if is_in_progress && matches!(tool_payload.display, ToolDisplayPayload::Progress { .. }) {
        return Ok(());
    }

    match &tool_payload.output_policy.timeline {
        TimelineOutputPolicy::Full { max_bytes } => {
            if let ToolDisplayPayload::Shell { .. } = tool_payload.display {
                validate_json_size(
                    "display",
                    tool_payload.tool_name,
                    tool_payload.display,
                    *max_bytes,
                )?;
            }
            Ok(())
        }
        TimelineOutputPolicy::Summary { max_chars } => match tool_payload.display {
            ToolDisplayPayload::Summary(summary) => {
                validate_summary_chars("display", tool_payload.tool_name, summary, *max_chars)
            }
            ToolDisplayPayload::Hidden => Ok(()),
            _ => anyhow::bail!(
                "tool item `{}` display payload does not match summary timeline policy",
                tool_payload.tool_name
            ),
        },
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            if matches!(tool_payload.display, ToolDisplayPayload::Hidden) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` display payload must be hidden for metadata-only/hidden timeline policy",
                    tool_payload.tool_name
                )
            }
        }
    }
}

fn validate_tool_storage_policy_shape(
    tool_payload: &ToolPayloadParts<'_>,
    is_in_progress: bool,
) -> Result<()> {
    if is_in_progress && matches!(tool_payload.storage, ToolStoragePayload::Metadata { .. }) {
        return Ok(());
    }

    match &tool_payload.output_policy.storage {
        StorageOutputPolicy::Full { max_bytes } => {
            match tool_payload.storage {
                ToolStoragePayload::Shell { .. } => {
                    validate_json_size(
                        "storage",
                        tool_payload.tool_name,
                        tool_payload.storage,
                        *max_bytes,
                    )?;
                }
                ToolStoragePayload::Summary(_)
                | ToolStoragePayload::Metadata { .. }
                | ToolStoragePayload::None => {}
            }
            Ok(())
        }
        StorageOutputPolicy::Summary { max_chars } => match tool_payload.storage {
            ToolStoragePayload::Summary(summary) => {
                validate_summary_chars("storage", tool_payload.tool_name, summary, *max_chars)
            }
            ToolStoragePayload::None => Ok(()),
            _ => anyhow::bail!(
                "tool item `{}` storage payload does not match summary storage policy",
                tool_payload.tool_name
            ),
        },
        StorageOutputPolicy::MetadataOnly => {
            if matches!(
                tool_payload.storage,
                ToolStoragePayload::Metadata { .. } | ToolStoragePayload::None
            ) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` storage payload must be metadata-only",
                    tool_payload.tool_name
                )
            }
        }
        StorageOutputPolicy::None => {
            if matches!(tool_payload.storage, ToolStoragePayload::None) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` storage payload must be empty",
                    tool_payload.tool_name
                )
            }
        }
    }
}

fn validate_summary_chars(
    channel: &str,
    tool_name: &str,
    summary: &pioneer_protocol::ToolOutputSummary,
    max_chars: usize,
) -> Result<()> {
    let visible_chars = summary.title.chars().count()
        + summary
            .lines
            .iter()
            .map(|line| line.chars().count())
            .sum::<usize>();
    if visible_chars > max_chars {
        anyhow::bail!(
            "tool item `{tool_name}` {channel} summary exceeds policy limit: {visible_chars} > {max_chars}"
        );
    }
    Ok(())
}

fn validate_json_size<T: serde::Serialize>(
    channel: &str,
    tool_name: &str,
    payload: &T,
    max_bytes: usize,
) -> Result<()> {
    let size = serde_json::to_vec(payload)
        .context("failed to serialize tool payload for size validation")?
        .len();
    if size > max_bytes {
        anyhow::bail!(
            "tool item `{tool_name}` {channel} payload exceeds policy limit: {size} > {max_bytes}"
        );
    }
    Ok(())
}

fn contains_any_key(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, nested) in map {
                if keys.iter().any(|candidate| key == candidate) {
                    return Some(key.clone());
                }
                if let Some(found) = contains_any_key(nested, keys) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            items.iter().find_map(|item| contains_any_key(item, keys))
        }
        _ => None,
    }
}

fn thread_from_db_model(model: pioneer_entity::thread::Model) -> Option<Thread> {
    let mode = thread_mode_from_db(model.mode.as_str())?;
    let status = thread_status_from_db(model.status.as_str())?;
    let origin_kind = thread_origin_kind_from_db(model.origin_kind.as_str())?;
    let sidebar_visibility = thread_sidebar_visibility_from_db(model.sidebar_visibility.as_str())?;

    Some(Thread {
        workspace_id: model.workspace_id,
        id: model.id,
        name: model.name,
        preview: model.preview,
        mode,
        model: model.model,
        model_provider: model.model_provider,
        reasoning_effort: None,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
        status,
        origin_kind,
        sidebar_visibility,
        agent_nickname: model.agent_nickname,
        agent_role: model.agent_role,
        turns: Vec::new(),
    })
}

fn thread_snapshot_turn_from_db_model(model: pioneer_entity::turn::Model) -> Result<Option<Turn>> {
    let Some(status) = turn_status_from_db(model.status.as_str()) else {
        return Ok(None);
    };
    let permission_profile = parse_turn_permission_profile(&model)?;

    Ok(Some(Turn {
        id: model.id,
        status,
        turn_kind: turn_kind_from_db(model.turn_kind.as_str()).unwrap_or_default(),
        origin: turn_origin_from_db(model.origin.as_str()).unwrap_or_default(),
        error: model.error,
        prompt_manifest: None,
        permission_profile,
    }))
}

fn thread_folder_from_db_model(model: pioneer_entity::thread_folder::Model) -> ThreadFolder {
    ThreadFolder {
        id: model.id,
        workspace_id: model.workspace_id,
        parent_folder_id: model.parent_folder_id,
        name: model.name,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    }
}

fn thread_placement_from_db_model(
    model: pioneer_entity::thread_placement::Model,
) -> ThreadPlacement {
    ThreadPlacement {
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        folder_id: model.folder_id,
    }
}

fn parse_turn_prompt_manifest(
    model: &pioneer_entity::turn::Model,
) -> Result<Option<PromptManifest>> {
    let manifest_json = model.prompt_manifest_json.trim();
    if manifest_json.is_empty() || manifest_json == "{}" || manifest_json == "null" {
        return Ok(None);
    }

    let manifest = serde_json::from_str::<PromptManifest>(manifest_json).with_context(|| {
        format!(
            "failed to decode prompt manifest for turn `{}` in thread `{}`",
            model.id, model.thread_id
        )
    })?;

    Ok(Some(manifest))
}

fn parse_turn_permission_profile(
    model: &pioneer_entity::turn::Model,
) -> Result<TurnPermissionProfileSnapshot> {
    if let Some(snapshot_json) = model
        .permission_profile_snapshot_json
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "{}" && *value != "null")
    {
        return serde_json::from_str::<TurnPermissionProfileSnapshot>(snapshot_json).with_context(
            || {
                format!(
                    "failed to decode permission profile snapshot for turn `{}` in thread `{}`",
                    model.id, model.thread_id
                )
            },
        );
    }

    let Some(mode) = model
        .permission_profile_mode
        .as_deref()
        .and_then(turn_permission_mode_from_db)
    else {
        return Ok(pioneer_protocol::resolve_turn_permission_profile(None));
    };
    let source = model
        .permission_profile_source
        .as_deref()
        .and_then(turn_permission_profile_source_from_db)
        .unwrap_or(TurnPermissionProfileSource::Defaulted);

    Ok(TurnPermissionProfileSnapshot::from_mode(mode, source))
}

fn build_turn_prompt_manifest_columns(
    manifest: &PromptManifest,
) -> Result<turn::TurnPromptManifestColumns> {
    Ok(turn::TurnPromptManifestColumns {
        prompt_manifest_json: serde_json::to_string(manifest)
            .context("failed to serialize prompt manifest to json")?,
        prompt_compiler_version: manifest.compiler_version.clone(),
        prompt_profile: prompt_manifest_profile_to_db(manifest.profile).to_owned(),
        prompt_fingerprint_stable: manifest.fingerprint_stable.clone(),
        prompt_fingerprint_dynamic: manifest.fingerprint_dynamic.clone(),
        prompt_fingerprint_full: manifest.fingerprint_full.clone(),
    })
}

fn infer_timeout_reason(
    lease_expires_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    idle_deadline_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    hard_deadline_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    now_unix: i64,
) -> TurnItemTimeoutReason {
    let now = unix_to_datetime(now_unix);
    if hard_deadline_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::HardDeadlineExceeded;
    }
    if idle_deadline_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::IdleDeadlineExceeded;
    }
    if lease_expires_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::LeaseExpired;
    }
    TurnItemTimeoutReason::HardDeadlineExceeded
}

fn recovery_job_record_from_model(model: pioneer_entity::recovery_job::Model) -> RecoveryJobRecord {
    RecoveryJobRecord {
        id: model.id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        item_type: turn_item_type_from_db(model.item_type.as_str())
            .unwrap_or(TurnItemType::DynamicToolCall),
        source_attempt_id: model.source_attempt_id,
        status: recovery_job_status_from_db(model.status.as_str())
            .unwrap_or(RecoveryJobStatus::Pending),
        trigger: recovery_trigger_from_db(model.trigger.as_str())
            .unwrap_or(RecoveryTrigger::Unknown),
        action: recovery_action_from_db(model.action.as_str())
            .unwrap_or(RecoveryAction::RetryAttempt),
        reason: model.reason,
        error_class: model
            .error_class
            .as_deref()
            .and_then(provider_failure_class_from_db),
        transport_stage: model
            .transport_stage
            .as_deref()
            .and_then(provider_failure_stage_from_db),
        retry_after_ms: model.retry_after_ms,
        provider_attempt_number: model.provider_attempt_number,
        policy_json: serde_json::from_str(&model.policy).unwrap_or_else(|_| serde_json::json!({})),
        policy_snapshot: serde_json::from_str(&model.policy_snapshot)
            .unwrap_or_else(|_| serde_json::json!({})),
        last_error: model.last_error,
        run_count: model.run_count,
        max_attempts: model.max_attempts,
        scheduled_at_unix: model.scheduled_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
        claim_token: model.claim_token,
        active_attempt_id: model.active_attempt_id,
        active_attempt_started_at_unix: model
            .active_attempt_started_at
            .map(|timestamp| timestamp.timestamp()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactBindingTargetRecord, BlockedTurnRecoveryResumeOutcome, ClaimedRecoveryActivation,
        CliRuntimeNativeEventListFilter, CliRuntimePendingRequestListFilter,
        CliRuntimePendingRequestStatus, CliRuntimeTurnBindingListFilter,
        ConversationArtifactRefLimits, CrudStore, IngestArtifactMetadataRecord,
        McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
        NewArtifactBlobRecord, NewCliRuntimeNativeEvent, NewCliRuntimePendingRequest,
        NewCliRuntimeThreadBinding, NewCliRuntimeTurnBinding, NewThreadEpisodicChunkRecord,
        NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord, NewTurnLlmContextEntry,
        NewTurnRuntimeSnapshot, ResolveCliRuntimePendingRequest, SkillAuditEventRecord,
        SkillInstallationRecord, TaskEventPayload, TaskRunChildAnchor, ThreadAgentsDocError,
        ThreadAgentsDocSaveReason, ThreadAgentsDocStatus, ThreadEpisodicActiveWriteSegmentRequest,
        ThreadEpisodicCapsuleCapacityUpdate, ThreadEpisodicCapsuleWriteState,
        ThreadEpisodicChunkStatus, ThreadEpisodicChunkVisibility, ThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceRuntimeKind, TurnExecutionCheckpointKind,
        TurnExecutionWindowStatsRecord, TurnExecutionWindowUsageAggregateRecord,
        TurnItemAttemptDeadlines, TurnMcpBindingRecord, TurnSkillBindingRecord,
        WorkspaceSkillPolicyRecord,
    };
    use crate::util::unix_to_datetime;
    use migration::{Migrator, MigratorTrait};
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
        ArtifactRole, ExecutionWindowExhaustionReason, ExecutionWindowStatus,
        ItemCompletedNotification, ItemRecoveryAttachedNotification,
        ItemRecoveryExhaustedNotification, ItemRecoveryOpenedNotification,
        ItemRecoverySucceededNotification, ItemRetryAttemptStartedNotification,
        ItemRetryScheduledNotification, ItemStartedNotification, ItemTimeoutDetectedNotification,
        ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
        ItemToolRetryScheduledNotification, ItemUpdatedNotification, PermissionBehavior,
        PromptManifest, PromptManifestDiagnostic, PromptManifestDiagnosticCode,
        PromptManifestHookContributionKind, PromptManifestHookPhase, PromptManifestHookSource,
        PromptManifestHookSourceEntry, PromptManifestHookTruncation, PromptManifestProfile,
        RecoveryAction, RecoveryJobStatus, RecoveryTrigger, SandboxMode, SystemEventLevel, Task,
        TaskAgentPrompt, TaskAgentResultContract, TaskAgentResultFormat, TaskAgentSpec,
        TaskExecutorKind, TaskMetadata, TaskOwnerKind, TaskResult, TaskResultCandidate,
        TaskResultCandidateStatus, TaskResultReviewDecision, TaskResultReviewEvent,
        TaskResultReviewEventKind, TaskResultReviewerKind, TaskRun, TaskRunExecutionStatus,
        TaskRunStatus, TaskRunThreadBinding, TaskRunThreadBindingKind, TaskRunTurn,
        TaskRunTurnKind, TaskRunTurnStatus, TaskSchema, TaskStatus, TaskTrigger, TaskTriggerSpec,
        TaskTriggerStatus, TaskValue, Thread, ThreadEpisodicSourceContext,
        ThreadHistoryEventPayload, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, ToolCallStatus, ToolDisplayPayload, ToolLoopBudgetAction,
        ToolLoopBudgetLimitKind, ToolMetadata, ToolOutputPolicySnapshot,
        ToolPermissionPolicySnapshot, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
        ToolRecoveryRetryClass, ToolRetryBudgetKind, ToolRetryBudgetUsage, ToolRetryErrorClass,
        ToolRetryExhaustionKind, ToolRetryResolution, ToolStoragePayload, Turn,
        TurnCompletedNotification, TurnItem, TurnItemEventPayload, TurnItemTimeoutReason,
        TurnItemType, TurnKind, TurnOrigin, TurnPermissionAuditEventKind, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStatus,
        TurnToolLoopBudgetExceededNotification, UserInput,
    };
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, DatabaseBackend, EntityTrait, QueryFilter,
        QueryOrder, Set, Statement,
    };
    use std::collections::BTreeMap;

    async fn test_store_with_workspace(workspace_id: &str) -> CrudStore {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let timestamp = unix_to_datetime(1_700_000_000);
        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Test Workspace".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(timestamp),
            updated_at: Set(timestamp),
        })
        .exec(&connection)
        .await
        .expect("workspace insert should succeed");

        CrudStore::new(connection)
    }

    fn test_artifact_binding(
        binding_kind: ArtifactBindingKind,
        message_id: Option<&str>,
    ) -> ArtifactBindingTargetRecord {
        ArtifactBindingTargetRecord {
            thread_id: Some("thread_artifact_refs".to_owned()),
            turn_id: Some("turn_artifact_refs".to_owned()),
            message_id: message_id.map(ToOwned::to_owned),
            turn_item_id: Some("item_artifact_refs".to_owned()),
            tool_call_id: None,
            task_id: None,
            task_run_id: None,
            binding_kind,
            direction: ArtifactBindingDirection::Input,
            role: Some(ArtifactRole::User),
            item_index: Some(0),
        }
    }

    #[tokio::test]
    async fn conversation_artifact_refs_exclude_draft_upload_bindings() {
        let store = test_store_with_workspace("ws_artifact_refs").await;
        let ingested = store
            .ingest_artifact_metadata(
                NewArtifactBlobRecord {
                    workspace_id: "ws_artifact_refs".to_owned(),
                    sha256: "sha256_artifact_refs".to_owned(),
                    size_bytes: 128,
                    mime_type: Some("image/png".to_owned()),
                    storage_backend: "memory".to_owned(),
                    storage_key: "blob_artifact_refs".to_owned(),
                    metadata: BTreeMap::new(),
                },
                IngestArtifactMetadataRecord {
                    workspace_id: "ws_artifact_refs".to_owned(),
                    primary_thread_id: Some("thread_artifact_refs".to_owned()),
                    display_name: "image.png".to_owned(),
                    kind: ArtifactKind::Image,
                    mime_type: Some("image/png".to_owned()),
                    created_by_kind: ArtifactCreatedByKind::User,
                    created_by_actor_id: Some("user_1".to_owned()),
                    metadata: BTreeMap::new(),
                },
                Some(test_artifact_binding(
                    ArtifactBindingKind::DraftUpload,
                    None,
                )),
                BTreeMap::new(),
            )
            .await
            .expect("artifact ingest should succeed");

        store
            .bind_artifact(
                "ws_artifact_refs",
                &ingested.artifact.id,
                Some(&ingested.version.id),
                test_artifact_binding(ArtifactBindingKind::UserInput, Some("user_message_1")),
                BTreeMap::new(),
            )
            .await
            .expect("user input binding should succeed");

        let refs = store
            .list_conversation_artifact_refs(
                "ws_artifact_refs",
                "thread_artifact_refs",
                &["turn_artifact_refs".to_owned()],
                ConversationArtifactRefLimits::default(),
            )
            .await
            .expect("conversation refs should load");
        let turn_refs = refs.get("turn_artifact_refs").expect("turn refs");

        assert_eq!(turn_refs.user.len(), 1);
        assert_eq!(
            turn_refs.user[0].binding_kind,
            ArtifactBindingKind::UserInput
        );
        assert_eq!(
            turn_refs.user[0].message_id.as_deref(),
            Some("user_message_1")
        );
        assert!(turn_refs.assistant.is_empty());
    }

    fn thread_episodic_chunk_fixture(
        id: &str,
        chunk_index: i64,
        text_hash: &str,
        status: ThreadEpisodicChunkStatus,
        visibility: ThreadEpisodicChunkVisibility,
    ) -> NewThreadEpisodicChunkRecord {
        NewThreadEpisodicChunkRecord {
            id: Some(id.to_owned()),
            workspace_id: "ws_thread_episodic".to_owned(),
            thread_id: "thread_episodic_1".to_owned(),
            turn_id: "turn_episodic_1".to_owned(),
            item_id: "assistant_message_1".to_owned(),
            chunk_index,
            chunk_count: 4,
            source_actor_role: ThreadEpisodicSourceActorRole::Assistant,
            source_runtime_kind: ThreadEpisodicSourceRuntimeKind::AssistantTurn,
            source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
            visibility,
            status,
            text_hash: text_hash.to_owned(),
            source_text_hash: "source_hash_1".to_owned(),
            char_start: chunk_index * 100,
            char_end: chunk_index * 100 + 50,
            byte_start: Some(chunk_index * 100),
            byte_end: Some(chunk_index * 100 + 50),
            language_hint: Some("ru".to_owned()),
            token_estimate: 12,
            capsule_id: None,
            capsule_ref: None,
            segment_index: None,
            frame_id: None,
            frame_uri: None,
            indexed_at: None,
            deleted_at: None,
        }
    }

    #[tokio::test]
    async fn thread_episodic_chunk_upsert_is_idempotent_by_source_identity_and_hash() {
        let store = test_store_with_workspace("ws_thread_episodic").await;
        let first = thread_episodic_chunk_fixture(
            "chunk_episodic_1",
            0,
            "same_text_hash",
            ThreadEpisodicChunkStatus::Active,
            ThreadEpisodicChunkVisibility::UserVisible,
        );
        let inserted = store
            .upsert_thread_episodic_chunk(first.clone(), 1_700_000_000)
            .await
            .expect("first upsert should insert");

        let mut duplicate = first;
        duplicate.id = Some("chunk_episodic_2".to_owned());
        let second = store
            .upsert_thread_episodic_chunk(duplicate, 1_700_000_100)
            .await
            .expect("duplicate upsert should return existing row");

        assert_eq!(second.id, inserted.id);
        let rows = store
            .list_thread_episodic_chunks_for_thread("ws_thread_episodic", "thread_episodic_1", 10)
            .await
            .expect("admin list should succeed");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn thread_episodic_recallable_chunks_suppress_deleted_excluded_and_internal_rows() {
        let store = test_store_with_workspace("ws_thread_episodic").await;
        for chunk in [
            thread_episodic_chunk_fixture(
                "chunk_active",
                0,
                "active_hash",
                ThreadEpisodicChunkStatus::Active,
                ThreadEpisodicChunkVisibility::UserVisible,
            ),
            thread_episodic_chunk_fixture(
                "chunk_excluded",
                1,
                "excluded_hash",
                ThreadEpisodicChunkStatus::Excluded,
                ThreadEpisodicChunkVisibility::UserVisible,
            ),
            thread_episodic_chunk_fixture(
                "chunk_deleted",
                2,
                "deleted_hash",
                ThreadEpisodicChunkStatus::Deleted,
                ThreadEpisodicChunkVisibility::UserVisible,
            ),
            thread_episodic_chunk_fixture(
                "chunk_internal",
                3,
                "internal_hash",
                ThreadEpisodicChunkStatus::Active,
                ThreadEpisodicChunkVisibility::InternalHidden,
            ),
        ] {
            store
                .upsert_thread_episodic_chunk(chunk, 1_700_000_000)
                .await
                .expect("chunk upsert should succeed");
        }

        let admin_rows = store
            .list_thread_episodic_chunks_for_thread("ws_thread_episodic", "thread_episodic_1", 10)
            .await
            .expect("admin list should include all rows");
        assert_eq!(admin_rows.len(), 4);

        let recallable = store
            .list_recallable_thread_episodic_chunks_for_thread(
                "ws_thread_episodic",
                "thread_episodic_1",
                10,
            )
            .await
            .expect("recallable list should succeed");
        assert_eq!(recallable.len(), 1);
        assert_eq!(recallable[0].id, "chunk_active");
    }

    #[tokio::test]
    async fn thread_episodic_active_write_resolution_is_idempotent() {
        let store = test_store_with_workspace("ws_thread_episodic").await;
        let request = ThreadEpisodicActiveWriteSegmentRequest {
            workspace_id: "ws_thread_episodic".to_owned(),
            thread_id: "thread_episodic_1".to_owned(),
            storage_uri_root: "file:///tmp/pioneer-memory/capsules".to_owned(),
        };

        let first = store
            .resolve_thread_episodic_active_write_segment(request.clone(), 1_700_000_000)
            .await
            .expect("first active write segment should resolve");
        let second = store
            .resolve_thread_episodic_active_write_segment(request, 1_700_000_100)
            .await
            .expect("second active write segment should resolve");

        assert_eq!(second.id, first.id);
        assert_eq!(first.segment_index, 1);
        assert_eq!(
            first.write_state,
            ThreadEpisodicCapsuleWriteState::ActiveWrite
        );
        let rows = store
            .list_thread_episodic_capsules_for_thread("ws_thread_episodic", "thread_episodic_1", 10)
            .await
            .expect("capsule list should succeed");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn thread_episodic_segment_rotation_creates_next_active_write_segment() {
        let store = test_store_with_workspace("ws_thread_episodic").await;
        let request = ThreadEpisodicActiveWriteSegmentRequest {
            workspace_id: "ws_thread_episodic".to_owned(),
            thread_id: "thread_episodic_1".to_owned(),
            storage_uri_root: "file:///tmp/pioneer-memory/capsules".to_owned(),
        };
        let first = store
            .resolve_thread_episodic_active_write_segment(request.clone(), 1_700_000_000)
            .await
            .expect("first active segment should resolve");
        let first_ref = first.capsule_ref.clone();

        let capacity = store
            .update_thread_episodic_capsule_capacity(
                first.id.as_str(),
                ThreadEpisodicCapsuleCapacityUpdate {
                    capacity_bytes: Some(1_000),
                    size_bytes: Some(900),
                    utilization_percent: Some(90.0),
                    active_chunk_count: Some(12),
                    near_capacity_at: Some(unix_to_datetime(1_700_000_010)),
                    capacity_exceeded_at: None,
                    last_error: None,
                },
                1_700_000_010,
            )
            .await
            .expect("capacity update should succeed")
            .expect("capsule should exist");
        assert_eq!(capacity.capsule_ref, first_ref);
        assert_eq!(capacity.active_chunk_count, 12);
        assert_eq!(capacity.utilization_percent, Some(90.0));

        let rotated = store
            .transition_thread_episodic_active_write_segment(
                first.id.as_str(),
                ThreadEpisodicCapsuleWriteState::Full,
                1_700_000_020,
            )
            .await
            .expect("rotation transition should succeed")
            .expect("capsule should exist");
        assert_eq!(rotated.capsule_ref, first_ref);
        assert_eq!(rotated.write_state, ThreadEpisodicCapsuleWriteState::Full);

        let second = store
            .resolve_thread_episodic_active_write_segment(request, 1_700_000_030)
            .await
            .expect("next active segment should resolve");
        assert_ne!(second.id, first.id);
        assert_eq!(second.segment_index, 2);
        assert_eq!(
            second.write_state,
            ThreadEpisodicCapsuleWriteState::ActiveWrite
        );

        let rows = store
            .list_thread_episodic_capsules_for_thread("ws_thread_episodic", "thread_episodic_1", 10)
            .await
            .expect("capsule list should succeed");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .filter(|row| row.write_state == ThreadEpisodicCapsuleWriteState::ActiveWrite)
                .count(),
            1
        );
    }

    fn sample_tool_recovery_policy() -> ToolRecoveryPolicySnapshot {
        ToolRecoveryPolicySnapshot {
            retry_class: ToolRecoveryRetryClass::Network,
            idempotency_mode: ToolRecoveryIdempotencyMode::Safe,
            max_attempts: 5,
            can_resume: true,
            resolved_action: RecoveryAction::RetryWithBackoff,
            base_backoff_secs: 3,
            max_wall_clock_secs: 240,
            no_progress_limit: 3,
        }
    }

    fn sample_thread(workspace_id: &str, thread_id: &str, timestamp: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    #[tokio::test]
    async fn turn_runtime_snapshot_upsert_round_trips_and_updates() {
        let store = test_store_with_workspace("ws_turn_runtime_snapshot").await;
        let created_at = unix_to_datetime(1_700_000_000);
        let updated_at = unix_to_datetime(1_700_000_010);
        let snapshot = NewTurnRuntimeSnapshot {
            turn_id: "turn_runtime_snapshot".to_owned(),
            thread_id: "thread_runtime_snapshot".to_owned(),
            workspace_id: "ws_turn_runtime_snapshot".to_owned(),
            mode_json: r#""Agent""#.to_owned(),
            model: "model-a".to_owned(),
            provider_name: "provider-a".to_owned(),
            reasoning_effort: None,
            hook_runtime_context_json: r#"{"mode":"agent","actor_kind":"agent"}"#.to_owned(),
            workspace_skill_policies_json: "[]".to_owned(),
            input_json: r#"[{"type":"text","text":"hello","textElements":[]}]"#.to_owned(),
            capabilities_json: "[]".to_owned(),
            resolved_artifacts_json: "[]".to_owned(),
            runtime_environment_json: r#"{"PIONEER_ARTIFACT_OUTPUT_DIR":"/tmp/a"}"#.to_owned(),
            history_json: r#"[{"role":"User","content":"hello"}]"#.to_owned(),
            created_at,
            updated_at: created_at,
        };

        let inserted = store
            .upsert_turn_runtime_snapshot(snapshot.clone())
            .await
            .expect("snapshot insert should succeed");
        assert_eq!(inserted.model, "model-a");
        assert_eq!(inserted.created_at, created_at);

        let mut replacement = snapshot;
        replacement.model = "model-b".to_owned();
        replacement.provider_name = "provider-b".to_owned();
        replacement.reasoning_effort = Some("max".to_owned());
        replacement.runtime_environment_json =
            r#"{"PIONEER_ARTIFACT_OUTPUT_DIR":"/tmp/b"}"#.to_owned();
        replacement.created_at = updated_at;
        replacement.updated_at = updated_at;

        let updated = store
            .upsert_turn_runtime_snapshot(replacement)
            .await
            .expect("snapshot update should succeed");
        assert_eq!(updated.model, "model-b");
        assert_eq!(updated.provider_name, "provider-b");
        assert_eq!(updated.reasoning_effort.as_deref(), Some("max"));
        assert_eq!(updated.created_at, created_at);
        assert_eq!(updated.updated_at, updated_at);

        let fetched = store
            .get_turn_runtime_snapshot("turn_runtime_snapshot")
            .await
            .expect("snapshot read should succeed")
            .expect("snapshot should exist");
        assert_eq!(
            fetched.runtime_environment_json,
            updated.runtime_environment_json
        );
        assert_eq!(fetched.reasoning_effort.as_deref(), Some("max"));

        assert_eq!(
            store
                .delete_turn_runtime_snapshot("turn_runtime_snapshot")
                .await
                .expect("snapshot delete should succeed"),
            1
        );
        assert!(
            store
                .get_turn_runtime_snapshot("turn_runtime_snapshot")
                .await
                .expect("snapshot read after delete should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn turn_runtime_snapshot_cleanup_removes_closed_turns_only() {
        let store = test_store_with_workspace("ws_turn_runtime_snapshot_cleanup").await;
        let now = unix_to_datetime(1_700_000_000);
        for (turn_id, status) in [
            ("completed_turn", "completed"),
            ("failed_turn", "failed"),
            ("interrupted_turn", "interrupted"),
            ("blocked_turn", "blocked"),
            ("active_turn", "in_progress"),
        ] {
            pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
                id: Set(turn_id.to_owned()),
                thread_id: Set("thread_runtime_snapshot_cleanup".to_owned()),
                status: Set(status.to_owned()),
                turn_kind: Set("conversation".to_owned()),
                origin: Set("user".to_owned()),
                error: Set(None),
                prompt_manifest_json: Set("{}".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            })
            .exec(&store.connection)
            .await
            .expect("turn insert should succeed");
        }

        for turn_id in [
            "completed_turn",
            "failed_turn",
            "interrupted_turn",
            "blocked_turn",
            "active_turn",
        ] {
            store
                .upsert_turn_runtime_snapshot(NewTurnRuntimeSnapshot {
                    turn_id: turn_id.to_owned(),
                    thread_id: "thread_runtime_snapshot_cleanup".to_owned(),
                    workspace_id: "ws_turn_runtime_snapshot_cleanup".to_owned(),
                    mode_json: r#""Agent""#.to_owned(),
                    model: "model-a".to_owned(),
                    provider_name: "provider-a".to_owned(),
                    reasoning_effort: None,
                    hook_runtime_context_json: r#"{"mode":"agent","actor_kind":"agent"}"#
                        .to_owned(),
                    workspace_skill_policies_json: "[]".to_owned(),
                    input_json: "[]".to_owned(),
                    capabilities_json: "[]".to_owned(),
                    resolved_artifacts_json: "[]".to_owned(),
                    runtime_environment_json: "{}".to_owned(),
                    history_json: "[]".to_owned(),
                    created_at: now,
                    updated_at: now,
                })
                .await
                .expect("runtime snapshot insert should succeed");
        }

        let deleted = store
            .delete_turn_runtime_snapshots_for_closed_turns()
            .await
            .expect("runtime snapshot cleanup should succeed");
        assert_eq!(deleted, 3);

        for turn_id in ["completed_turn", "failed_turn", "interrupted_turn"] {
            assert!(
                store
                    .get_turn_runtime_snapshot(turn_id)
                    .await
                    .expect("runtime snapshot read should succeed")
                    .is_none(),
                "{turn_id} snapshot should be removed"
            );
        }
        for turn_id in ["blocked_turn", "active_turn"] {
            assert!(
                store
                    .get_turn_runtime_snapshot(turn_id)
                    .await
                    .expect("runtime snapshot read should succeed")
                    .is_some(),
                "{turn_id} snapshot should be retained"
            );
        }
    }

    #[tokio::test]
    async fn cli_runtime_thread_and_turn_bindings_upsert_idempotently() {
        let store = test_store_with_workspace("ws_cli_bind").await;
        let created_at = unix_to_datetime(1_700_010_000);
        let updated_at = unix_to_datetime(1_700_010_030);

        let thread_binding = NewCliRuntimeThreadBinding {
            thread_id: "thread_cli_bind".to_owned(),
            workspace_id: "ws_cli_bind".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "codex-thread-a".to_owned(),
            native_session_id: Some("session-a".to_owned()),
            native_root_thread_id: None,
            native_cwd: Some("/tmp/project-a".to_owned()),
            native_model: Some("gpt-5".to_owned()),
            resume_cursor_json: "{}".to_owned(),
            status: "active".to_owned(),
            created_at,
            updated_at: created_at,
        };
        let inserted_thread = store
            .upsert_cli_runtime_thread_binding(thread_binding.clone())
            .await
            .expect("thread binding insert should succeed");
        assert_eq!(
            inserted_thread.native_session_id.as_deref(),
            Some("session-a")
        );

        let mut replacement_thread = thread_binding;
        replacement_thread.native_session_id = Some("session-b".to_owned());
        replacement_thread.native_model = Some("gpt-5.1".to_owned());
        replacement_thread.resume_cursor_json = r#"{"cursor":"after-turn-1"}"#.to_owned();
        replacement_thread.created_at = updated_at;
        replacement_thread.updated_at = updated_at;
        let updated_thread = store
            .upsert_cli_runtime_thread_binding(replacement_thread)
            .await
            .expect("thread binding update should succeed");
        assert_eq!(updated_thread.created_at, created_at);
        assert_eq!(updated_thread.updated_at, updated_at);
        assert_eq!(
            updated_thread.native_session_id.as_deref(),
            Some("session-b")
        );

        let by_native = store
            .get_cli_runtime_thread_binding_by_native_thread("codex", "codex-thread-a")
            .await
            .expect("native thread lookup should succeed")
            .expect("native thread binding should exist");
        assert_eq!(by_native.thread_id, "thread_cli_bind");
        assert_eq!(
            store
                .list_cli_runtime_thread_bindings_for_runtime("ws_cli_bind", "codex")
                .await
                .expect("runtime thread list should succeed")
                .len(),
            1
        );

        let turn_binding = NewCliRuntimeTurnBinding {
            turn_id: "turn_cli_bind".to_owned(),
            thread_id: "thread_cli_bind".to_owned(),
            workspace_id: "ws_cli_bind".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "codex-thread-a".to_owned(),
            native_turn_id: Some("codex-turn-a".to_owned()),
            request_id: Some("rpc-request-a".to_owned()),
            status: "running".to_owned(),
            model: Some("gpt-5".to_owned()),
            cwd: Some("/tmp/project-a".to_owned()),
            sandbox_json: Some(r#"{"mode":"workspace-write"}"#.to_owned()),
            approval_policy: Some("on-request".to_owned()),
            input_mapping_json: r#"{"items":1}"#.to_owned(),
            created_at,
            updated_at: created_at,
        };
        store
            .upsert_cli_runtime_turn_binding(turn_binding.clone())
            .await
            .expect("turn binding insert should succeed");

        let mut replacement_turn = turn_binding;
        replacement_turn.status = "completed".to_owned();
        replacement_turn.model = Some("gpt-5.1".to_owned());
        replacement_turn.created_at = updated_at;
        replacement_turn.updated_at = updated_at;
        let updated_turn = store
            .upsert_cli_runtime_turn_binding(replacement_turn)
            .await
            .expect("turn binding update should succeed");
        assert_eq!(updated_turn.created_at, created_at);
        assert_eq!(updated_turn.status, "completed");

        let by_request = store
            .get_cli_runtime_turn_binding_by_request("rpc-request-a")
            .await
            .expect("request lookup should succeed")
            .expect("request turn binding should exist");
        assert_eq!(by_request.turn_id, "turn_cli_bind");
        let by_native_turn = store
            .get_cli_runtime_turn_binding_by_native_turn("codex", "codex-turn-a")
            .await
            .expect("native turn lookup should succeed")
            .expect("native turn binding should exist");
        assert_eq!(by_native_turn.thread_id, "thread_cli_bind");
        assert_eq!(
            store
                .list_cli_runtime_turn_bindings_for_thread("thread_cli_bind")
                .await
                .expect("thread turn list should succeed")
                .len(),
            1
        );

        store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: "turn_cli_running".to_owned(),
                thread_id: "thread_cli_bind".to_owned(),
                workspace_id: "ws_cli_bind".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "codex-thread-a".to_owned(),
                native_turn_id: None,
                request_id: Some("rpc-request-running".to_owned()),
                status: "running".to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project-a".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: r#"{"items":2}"#.to_owned(),
                created_at: updated_at,
                updated_at,
            })
            .await
            .expect("running turn binding insert should succeed");
        let active_turn_bindings = store
            .list_cli_runtime_turn_bindings(CliRuntimeTurnBindingListFilter {
                workspace_id: Some("ws_cli_bind".to_owned()),
                runtime_id: Some("codex".to_owned()),
                statuses: vec!["starting".to_owned(), "running".to_owned()],
                ..Default::default()
            })
            .await
            .expect("active CLI runtime turn bindings should list");
        assert_eq!(active_turn_bindings.len(), 1);
        assert_eq!(active_turn_bindings[0].turn_id, "turn_cli_running");
    }

    #[tokio::test]
    async fn cli_runtime_pending_request_resolution_rejects_second_answer() {
        let store = test_store_with_workspace("ws_cli_pending").await;
        let created_at = unix_to_datetime(1_700_020_000);
        let updated_at = unix_to_datetime(1_700_020_010);
        let payload_json = super::serialize_cli_runtime_json(&serde_json::json!({
            "command": "cargo test",
            "cwd": "/tmp/project"
        }))
        .expect("payload json should serialize");

        let pending = store
            .create_cli_runtime_pending_request(NewCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                workspace_id: "ws_cli_pending".to_owned(),
                thread_id: "thread_cli_pending".to_owned(),
                turn_id: Some("turn_cli_pending".to_owned()),
                native_thread_id: Some("codex-thread-pending".to_owned()),
                native_turn_id: Some("codex-turn-pending".to_owned()),
                native_item_id: Some("item-1".to_owned()),
                request_kind: "command_approval".to_owned(),
                payload_json: payload_json.clone(),
                created_at,
                updated_at: created_at,
            })
            .await
            .expect("pending request create should succeed");
        assert_eq!(pending.status, CliRuntimePendingRequestStatus::Pending);

        let duplicate_pending = store
            .create_cli_runtime_pending_request(NewCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                workspace_id: "ws_cli_pending".to_owned(),
                thread_id: "thread_cli_pending".to_owned(),
                turn_id: Some("turn_cli_pending".to_owned()),
                native_thread_id: Some("codex-thread-pending".to_owned()),
                native_turn_id: Some("codex-turn-pending".to_owned()),
                native_item_id: Some("item-1".to_owned()),
                request_kind: "command_approval".to_owned(),
                payload_json: super::serialize_cli_runtime_json(&serde_json::json!({
                    "command": "cargo check",
                    "cwd": "/tmp/project"
                }))
                .expect("duplicate payload should serialize"),
                created_at,
                updated_at,
            })
            .await
            .expect("duplicate pending request should update metadata");
        assert_eq!(duplicate_pending.created_at, created_at);
        assert_eq!(duplicate_pending.updated_at, updated_at);
        assert!(duplicate_pending.payload_json.contains("cargo check"));

        let response_json = super::serialize_cli_runtime_json(&serde_json::json!({
            "approved": true,
            "scope": "once"
        }))
        .expect("response json should serialize");
        let answered = store
            .resolve_cli_runtime_pending_request(ResolveCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                status: CliRuntimePendingRequestStatus::Answered,
                response_json: Some(response_json.clone()),
                updated_at,
                resolved_at: updated_at,
            })
            .await
            .expect("pending request answer should succeed")
            .expect("pending request should exist");
        assert_eq!(answered.status, CliRuntimePendingRequestStatus::Answered);
        assert_eq!(
            answered.response_json.as_deref(),
            Some(response_json.as_str())
        );

        let second_answer = store
            .resolve_cli_runtime_pending_request(ResolveCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                status: CliRuntimePendingRequestStatus::Answered,
                response_json: Some(response_json),
                updated_at,
                resolved_at: updated_at,
            })
            .await;
        assert!(
            second_answer.is_err(),
            "answered pending request must reject a second answer"
        );

        let answered_rows = store
            .list_cli_runtime_pending_requests(CliRuntimePendingRequestListFilter {
                workspace_id: Some("ws_cli_pending".to_owned()),
                runtime_id: Some("codex".to_owned()),
                status: Some(CliRuntimePendingRequestStatus::Answered),
                ..Default::default()
            })
            .await
            .expect("answered request list should succeed");
        assert_eq!(answered_rows.len(), 1);
    }

    #[tokio::test]
    async fn cli_runtime_pending_request_cancel_and_expire_helpers_are_terminal() {
        let store = test_store_with_workspace("ws_cli_pending_terminal").await;
        let created_at = unix_to_datetime(1_700_021_000);
        let updated_at = unix_to_datetime(1_700_021_030);
        let request_payload = super::serialize_cli_runtime_json(&serde_json::json!({
            "kind": "user_input",
            "title": "Need input",
            "payload": { "prompt": "Continue?" }
        }))
        .expect("request payload should serialize");

        for request_id in ["approval-cancel-a", "approval-expire-a"] {
            store
                .open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
                    request_id: request_id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: "ws_cli_pending_terminal".to_owned(),
                    thread_id: "thread_cli_pending_terminal".to_owned(),
                    turn_id: Some("turn_cli_pending_terminal".to_owned()),
                    native_thread_id: Some("codex-thread-terminal".to_owned()),
                    native_turn_id: Some("codex-turn-terminal".to_owned()),
                    native_item_id: Some("item-terminal".to_owned()),
                    request_kind: "user_input".to_owned(),
                    payload_json: request_payload.clone(),
                    created_at,
                    updated_at: created_at,
                })
                .await
                .expect("pending request should open");
        }

        let cancelled = store
            .cancel_cli_runtime_pending_request(
                "approval-cancel-a",
                Some(
                    super::serialize_cli_runtime_json(&serde_json::json!({
                        "status": "cancelled"
                    }))
                    .expect("cancel payload should serialize"),
                ),
                updated_at,
            )
            .await
            .expect("cancel should succeed")
            .expect("cancelled request should exist");
        assert_eq!(cancelled.status, CliRuntimePendingRequestStatus::Cancelled);
        assert_eq!(cancelled.resolved_at, Some(updated_at));

        let second_cancel = store
            .cancel_cli_runtime_pending_request("approval-cancel-a", None, updated_at)
            .await;
        assert!(
            second_cancel.is_err(),
            "cancelled request must reject a second terminal transition"
        );

        let expired = store
            .expire_cli_runtime_pending_request("approval-expire-a", None, updated_at)
            .await
            .expect("expire should succeed")
            .expect("expired request should exist");
        assert_eq!(expired.status, CliRuntimePendingRequestStatus::Expired);

        let missing = store
            .expire_cli_runtime_pending_request("approval-missing-a", None, updated_at)
            .await
            .expect("missing expire should not fail");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn cli_runtime_json_helpers_and_native_events_round_trip() {
        let store = test_store_with_workspace("ws_cli_native").await;
        let created_at = unix_to_datetime(1_700_030_000);
        let payload = serde_json::json!({
            "method": "item/completed",
            "redacted": true
        });
        let encoded = super::serialize_cli_runtime_json(&payload)
            .expect("native event payload should serialize");
        let decoded: serde_json::Value = super::deserialize_cli_runtime_json(encoded.as_str())
            .expect("native event payload should deserialize");
        assert_eq!(decoded, payload);

        for (id, sequence, method) in [
            ("native-event-1", 1_i64, "thread/start"),
            ("native-event-2", 2_i64, "item/completed"),
        ] {
            store
                .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                    id: id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: Some("ws_cli_native".to_owned()),
                    thread_id: Some("thread_cli_native".to_owned()),
                    turn_id: Some("turn_cli_native".to_owned()),
                    native_thread_id: Some("codex-thread-native".to_owned()),
                    native_turn_id: Some("codex-turn-native".to_owned()),
                    native_method: method.to_owned(),
                    payload_redacted_json: encoded.clone(),
                    sequence,
                    created_at,
                })
                .await
                .expect("native event append should succeed");
        }

        let events = store
            .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some("thread_cli_native".to_owned()),
                turn_id: Some("turn_cli_native".to_owned()),
                native_thread_id: Some("codex-thread-native".to_owned()),
                ..Default::default()
            })
            .await
            .expect("native event list should succeed");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].native_method, "item/completed");
    }

    #[tokio::test]
    async fn blocked_tool_recovery_resume_without_snapshot_leaves_turn_blocked() {
        let store = test_store_with_workspace("ws_resume_missing_snapshot").await;
        let now = unix_to_datetime(1_700_000_000);
        let thread_id = "thread_resume_missing_snapshot";
        let turn_id = "turn_resume_missing_snapshot";
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            status: Set("blocked".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(Some("waiting for runtime snapshot".to_owned())),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&store.connection)
        .await
        .expect("turn insert should succeed");

        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "tool_item_missing_snapshot".to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::BlockResumable,
                Some("tool recovery blocked".to_owned()),
                None,
                None,
                None,
                0,
                0,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_001,
            )
            .await
            .expect("recovery job should enqueue");
        store
            .mark_recovery_job_terminal(
                job.id.as_str(),
                RecoveryJobStatus::Blocked,
                Some("waiting for operator resume".to_owned()),
                1_700_000_002,
            )
            .await
            .expect("recovery job should be marked blocked");

        let outcome = store
            .resume_blocked_turn_recovery(thread_id, turn_id, Some(job.id.as_str()), 1_700_000_003)
            .await
            .expect("blocked resume should evaluate");

        match outcome {
            BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot { recovery_job_id } => {
                assert_eq!(recovery_job_id, job.id);
            }
            other => panic!("expected missing runtime snapshot, got {other:?}"),
        }
        let turn = pioneer_entity::turn::Entity::find_by_id(turn_id.to_owned())
            .one(&store.connection)
            .await
            .expect("turn read should succeed")
            .expect("turn should exist");
        assert_eq!(turn.status, "blocked");
        let blocked_job = store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job read should succeed")
            .expect("job should exist");
        assert_eq!(blocked_job.status, RecoveryJobStatus::Blocked);
    }

    #[tokio::test]
    async fn task_review_target_repositories_round_trip_and_update() {
        let store = test_store_with_workspace("ws_task_review_target_round_trip").await;
        let binding = TaskRunThreadBinding {
            id: "binding_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            execution_id: Some("execution_round_trip".to_owned()),
            thread_id: "thread_round_trip".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: 1_700_000_010,
        };
        let inserted_binding = store
            .upsert_task_run_thread_binding(binding.clone())
            .await
            .expect("binding upsert should succeed");
        assert_eq!(inserted_binding, binding);

        let turn = TaskRunTurn {
            id: "task_run_turn_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            execution_id: Some("execution_round_trip".to_owned()),
            thread_id: "thread_round_trip".to_owned(),
            turn_id: "turn_round_trip".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::InProgress,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1_700_000_011,
            started_at: Some(1_700_000_011),
            completed_at: None,
        };
        store
            .upsert_task_run_turn(turn.clone())
            .await
            .expect("turn upsert should succeed");
        let completed_turn = store
            .update_task_run_turn_status(
                turn.id.as_str(),
                TaskRunTurnStatus::CandidateCreated,
                Some(1_700_000_012),
            )
            .await
            .expect("turn status update should succeed")
            .expect("turn should exist");
        assert_eq!(completed_turn.status, TaskRunTurnStatus::CandidateCreated);
        assert_eq!(completed_turn.completed_at, Some(1_700_000_012));

        let candidate = TaskResultCandidate {
            id: "candidate_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            task_run_turn_id: turn.id.clone(),
            thread_id: "thread_round_trip".to_owned(),
            turn_id: "turn_round_trip".to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("done".to_owned()),
                data: Some(TaskValue::String("ok".to_owned())),
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_round_trip".to_owned()),
            }),
            extraction_error: None,
            summary: Some("done".to_owned()),
            diagnostics: vec!["parsed".to_owned()],
            final_review_event_id: None,
            created_at: 1_700_000_013,
            updated_at: 1_700_000_013,
            resolved_at: None,
        };
        store
            .upsert_task_result_candidate(candidate.clone())
            .await
            .expect("candidate upsert should succeed");

        let review_event = TaskResultReviewEvent {
            id: "review_event_round_trip".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            task_run_turn_id: turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: 1_700_000_014,
        };
        store
            .upsert_task_result_review_event(review_event.clone())
            .await
            .expect("review event upsert should succeed");
        let accepted = store
            .update_task_result_candidate_resolution(
                candidate.id.as_str(),
                TaskResultCandidateStatus::Accepted,
                Some(review_event.id.as_str()),
                Some(1_700_000_014),
                1_700_000_014,
            )
            .await
            .expect("candidate resolution update should succeed")
            .expect("candidate should exist");
        assert_eq!(accepted.status, TaskResultCandidateStatus::Accepted);
        assert_eq!(
            accepted.final_review_event_id.as_deref(),
            Some(review_event.id.as_str())
        );
        assert_eq!(accepted.resolved_at, Some(1_700_000_014));
        assert_eq!(
            store
                .get_task_result_review_event(review_event.id.as_str())
                .await
                .expect("review lookup should succeed"),
            Some(review_event)
        );
    }

    #[tokio::test]
    async fn task_review_canonical_helpers_return_target_rows_and_ordering() {
        let store = test_store_with_workspace("ws_task_review_helpers").await;
        assert_eq!(
            store
                .get_task_run_primary_thread_binding("run_helpers")
                .await
                .expect("missing primary binding lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_task_run_thread_binding_by_thread("thread_helpers")
                .await
                .expect("missing thread binding lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_task_run_turn_by_turn("thread_helpers", "turn_helpers_initial")
                .await
                .expect("missing turn lookup should succeed"),
            None
        );
        assert!(
            store
                .list_task_run_turns("run_helpers")
                .await
                .expect("missing run turn list should succeed")
                .is_empty()
        );
        assert_eq!(
            store
                .get_latest_task_run_turn("run_helpers")
                .await
                .expect("missing latest turn lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_pending_task_result_candidate("run_helpers")
                .await
                .expect("missing pending candidate lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate("run_helpers")
                .await
                .expect("missing accepted candidate lookup should succeed"),
            None
        );
        assert!(
            store
                .list_task_result_review_events("candidate_helpers_accepted")
                .await
                .expect("missing candidate review list should succeed")
                .is_empty()
        );
        assert!(
            store
                .list_task_result_review_events_for_run("run_helpers")
                .await
                .expect("missing run review list should succeed")
                .is_empty()
        );

        let binding = TaskRunThreadBinding {
            id: "binding_helpers".to_owned(),
            task_id: "task_helpers".to_owned(),
            run_id: "run_helpers".to_owned(),
            execution_id: None,
            thread_id: "thread_helpers".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: 1_700_001_000,
        };
        store
            .upsert_task_run_thread_binding(binding.clone())
            .await
            .expect("binding upsert should succeed");
        assert_eq!(
            store
                .get_task_run_primary_thread_binding("run_helpers")
                .await
                .expect("primary binding lookup should succeed"),
            Some(binding.clone())
        );
        assert_eq!(
            store
                .get_task_run_thread_binding_by_thread("thread_helpers")
                .await
                .expect("thread binding lookup should succeed"),
            Some(binding)
        );
        let duplicate_primary = store
            .upsert_task_run_thread_binding(TaskRunThreadBinding {
                id: "binding_helpers_duplicate_primary".to_owned(),
                task_id: "task_helpers".to_owned(),
                run_id: "run_helpers".to_owned(),
                execution_id: None,
                thread_id: "thread_helpers_duplicate_primary".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 1_700_001_000,
            })
            .await;
        assert!(
            duplicate_primary.is_err(),
            "duplicate primary_executor binding should be rejected"
        );

        for (index, turn_id) in ["turn_helpers_initial", "turn_helpers_revision"]
            .into_iter()
            .enumerate()
        {
            store
                .upsert_task_run_turn(TaskRunTurn {
                    id: format!("task_run_turn_helpers_{index}"),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    execution_id: None,
                    thread_id: "thread_helpers".to_owned(),
                    turn_id: turn_id.to_owned(),
                    kind: if index == 0 {
                        TaskRunTurnKind::Initial
                    } else {
                        TaskRunTurnKind::Revision
                    },
                    round: u32::try_from(index).expect("index should fit"),
                    sequence: u32::try_from(index).expect("index should fit"),
                    status: TaskRunTurnStatus::CandidateCreated,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_001 + i64::try_from(index).expect("index should fit"),
                    started_at: None,
                    completed_at: None,
                })
                .await
                .expect("turn upsert should succeed");
        }
        assert!(
            store
                .upsert_task_run_turn(TaskRunTurn {
                    id: "task_run_turn_helpers_duplicate_sequence".to_owned(),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    execution_id: None,
                    thread_id: "thread_helpers".to_owned(),
                    turn_id: "turn_helpers_duplicate_sequence".to_owned(),
                    kind: TaskRunTurnKind::Review,
                    round: 99,
                    sequence: 1,
                    status: TaskRunTurnStatus::ReviewRecorded,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_009,
                    started_at: None,
                    completed_at: None,
                })
                .await
                .is_err(),
            "duplicate run/sequence turn should be rejected"
        );
        let turns = store
            .list_task_run_turns("run_helpers")
            .await
            .expect("turn list should succeed");
        assert_eq!(
            turns
                .iter()
                .map(|turn| turn.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn_helpers_initial", "turn_helpers_revision"]
        );
        assert_eq!(
            store
                .get_latest_task_run_turn("run_helpers")
                .await
                .expect("latest turn lookup should succeed")
                .map(|turn| turn.turn_id),
            Some("turn_helpers_revision".to_owned())
        );

        let pending = TaskResultCandidate {
            id: "candidate_helpers_pending".to_owned(),
            task_id: "task_helpers".to_owned(),
            run_id: "run_helpers".to_owned(),
            task_run_turn_id: "task_run_turn_helpers_0".to_owned(),
            thread_id: "thread_helpers".to_owned(),
            turn_id: "turn_helpers_initial".to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("pending".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: None,
            }),
            extraction_error: None,
            summary: Some("pending".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: 1_700_001_010,
            updated_at: 1_700_001_010,
            resolved_at: None,
        };
        let accepted = TaskResultCandidate {
            id: "candidate_helpers_accepted".to_owned(),
            status: TaskResultCandidateStatus::Accepted,
            task_run_turn_id: "task_run_turn_helpers_1".to_owned(),
            turn_id: "turn_helpers_revision".to_owned(),
            round: 1,
            final_review_event_id: Some("review_helpers_accept".to_owned()),
            created_at: 1_700_001_020,
            updated_at: 1_700_001_021,
            resolved_at: Some(1_700_001_021),
            ..pending.clone()
        };
        store
            .upsert_task_result_candidate(pending.clone())
            .await
            .expect("pending candidate upsert should succeed");
        store
            .upsert_task_result_candidate(accepted.clone())
            .await
            .expect("accepted candidate upsert should succeed");
        assert_eq!(
            store
                .get_pending_task_result_candidate("run_helpers")
                .await
                .expect("pending candidate lookup should succeed")
                .map(|candidate| candidate.id),
            Some(pending.id)
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate("run_helpers")
                .await
                .expect("accepted candidate lookup should succeed")
                .map(|candidate| candidate.id),
            Some(accepted.id.clone())
        );
        assert_eq!(
            store
                .get_task_run_child_anchor("run_helpers")
                .await
                .expect("child anchor should load"),
            TaskRunChildAnchor {
                child_thread_id: Some("thread_helpers".to_owned()),
                child_turn_id: Some("turn_helpers_revision".to_owned())
            }
        );

        for (id, created_at) in [
            ("review_helpers_first", 1_700_001_030),
            ("review_helpers_second", 1_700_001_031),
        ] {
            store
                .upsert_task_result_review_event(TaskResultReviewEvent {
                    id: id.to_owned(),
                    candidate_id: accepted.id.clone(),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    task_run_turn_id: "task_run_turn_helpers_1".to_owned(),
                    reviewer_kind: TaskResultReviewerKind::ParentAgent,
                    reviewer_thread_id: Some("parent_thread_helpers".to_owned()),
                    reviewer_turn_id: Some("parent_turn_helpers".to_owned()),
                    reviewer_user_id: None,
                    reviewer_agent_spec_id: None,
                    event_kind: TaskResultReviewEventKind::Decision,
                    decision: TaskResultReviewDecision::Accept,
                    feedback_text: None,
                    feedback: None,
                    confidence: None,
                    supersedes_review_event_id: None,
                    next_task_run_turn_id: None,
                    created_at,
                })
                .await
                .expect("review event upsert should succeed");
        }
        assert_eq!(
            store
                .list_task_result_review_events(accepted.id.as_str())
                .await
                .expect("candidate reviews should list")
                .into_iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            vec!["review_helpers_first", "review_helpers_second"]
        );
        assert_eq!(
            store
                .list_task_result_review_events_for_run("run_helpers")
                .await
                .expect("run reviews should list")
                .into_iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            vec!["review_helpers_first", "review_helpers_second"]
        );
    }

    #[tokio::test]
    async fn task_run_child_anchor_uses_accepted_task_run_turn() {
        let store = test_store_with_workspace("ws_task_review_anchor_accepted_turn").await;
        store
            .upsert_task_run_thread_binding(TaskRunThreadBinding {
                id: "binding_anchor_accepted_turn".to_owned(),
                task_id: "task_anchor_accepted_turn".to_owned(),
                run_id: "run_anchor_accepted_turn".to_owned(),
                execution_id: None,
                thread_id: "thread_anchor_accepted_turn".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 1_700_001_100,
            })
            .await
            .expect("binding upsert should succeed");

        for (id, turn_id, sequence) in [
            ("task_run_turn_anchor_accepted", "turn_anchor_accepted", 0),
            ("task_run_turn_anchor_latest", "turn_anchor_latest", 1),
        ] {
            store
                .upsert_task_run_turn(TaskRunTurn {
                    id: id.to_owned(),
                    task_id: "task_anchor_accepted_turn".to_owned(),
                    run_id: "run_anchor_accepted_turn".to_owned(),
                    execution_id: None,
                    thread_id: "thread_anchor_accepted_turn".to_owned(),
                    turn_id: turn_id.to_owned(),
                    kind: if sequence == 0 {
                        TaskRunTurnKind::Initial
                    } else {
                        TaskRunTurnKind::Revision
                    },
                    round: sequence,
                    sequence,
                    status: TaskRunTurnStatus::CandidateCreated,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_101 + i64::from(sequence),
                    started_at: None,
                    completed_at: None,
                })
                .await
                .expect("turn upsert should succeed");
        }

        store
            .upsert_task_result_candidate(TaskResultCandidate {
                id: "candidate_anchor_accepted_turn".to_owned(),
                task_id: "task_anchor_accepted_turn".to_owned(),
                run_id: "run_anchor_accepted_turn".to_owned(),
                task_run_turn_id: "task_run_turn_anchor_accepted".to_owned(),
                thread_id: "thread_anchor_accepted_turn".to_owned(),
                turn_id: "stale_denormalized_candidate_turn".to_owned(),
                round: 0,
                status: TaskResultCandidateStatus::Accepted,
                result: Some(TaskResult {
                    summary: Some("accepted".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some("run_anchor_accepted_turn".to_owned()),
                }),
                extraction_error: None,
                summary: Some("accepted".to_owned()),
                diagnostics: Vec::new(),
                final_review_event_id: Some("review_anchor_accepted_turn".to_owned()),
                created_at: 1_700_001_110,
                updated_at: 1_700_001_111,
                resolved_at: Some(1_700_001_111),
            })
            .await
            .expect("candidate upsert should succeed");

        assert_eq!(
            store
                .get_task_run_child_anchor("run_anchor_accepted_turn")
                .await
                .expect("child anchor should load"),
            TaskRunChildAnchor {
                child_thread_id: Some("thread_anchor_accepted_turn".to_owned()),
                child_turn_id: Some("turn_anchor_accepted".to_owned())
            }
        );
    }

    #[tokio::test]
    async fn task_review_projector_replays_new_runtime_events_idempotently() {
        let store = test_store_with_workspace("ws_task_review_projector").await;
        let timestamp = 1_700_002_000;
        let mut task = sample_task(timestamp);
        task.id = "task_projector".to_owned();
        task.workspace_id = "ws_task_review_projector".to_owned();
        let mut run = sample_task_run(timestamp);
        run.id = "run_projector".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                ],
                timestamp,
            )
            .await
            .expect("task and run should project");
        let execution = store
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, timestamp + 1)
            .await
            .expect("execution should reserve");

        let binding = TaskRunThreadBinding {
            id: "binding_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id.clone()),
            thread_id: "thread_projector".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: timestamp + 1,
        };
        let turn = TaskRunTurn {
            id: "task_run_turn_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id.clone()),
            thread_id: binding.thread_id.clone(),
            turn_id: "turn_projector".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 2,
            started_at: Some(timestamp + 2),
            completed_at: Some(timestamp + 3),
        };
        let candidate = TaskResultCandidate {
            id: "candidate_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: turn.id.clone(),
            thread_id: binding.thread_id.clone(),
            turn_id: turn.turn_id.clone(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("candidate".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run.id.clone()),
            }),
            extraction_error: None,
            summary: Some("candidate".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: timestamp + 4,
            updated_at: timestamp + 4,
            resolved_at: None,
        };
        let review_event = TaskResultReviewEvent {
            id: "review_projector_accept".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: timestamp + 5,
        };
        let accepted_candidate = TaskResultCandidate {
            status: TaskResultCandidateStatus::Accepted,
            final_review_event_id: Some(review_event.id.clone()),
            updated_at: timestamp + 5,
            resolved_at: Some(timestamp + 5),
            ..candidate.clone()
        };

        let events = vec![
            TaskEventPayload::TaskRunThreadBindingCreated {
                binding: binding.clone(),
            },
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: TaskRunTurn {
                    status: TaskRunTurnStatus::InProgress,
                    completed_at: None,
                    ..turn.clone()
                },
            },
            TaskEventPayload::TaskRunTurnCompleted {
                task_run_turn: turn.clone(),
            },
            TaskEventPayload::TaskResultCandidateCreated {
                candidate: candidate.clone(),
            },
            TaskEventPayload::TaskRunEnteredReview {
                task_id: run.task_id.clone(),
                run_id: run.id.clone(),
                candidate_id: candidate.id.clone(),
                entered_at: timestamp + 4,
            },
            TaskEventPayload::TaskResultReviewEventRecorded {
                review_event: review_event.clone(),
            },
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate: accepted_candidate.clone(),
                review_event_id: review_event.id.clone(),
            },
        ];

        store
            .append_task_events(events.clone(), timestamp + 10)
            .await
            .expect("new runtime events should project");
        store
            .append_task_events(events, timestamp + 10)
            .await
            .expect("replay should be idempotent");

        assert_eq!(
            store
                .get_task_run_primary_thread_binding(run.id.as_str())
                .await
                .expect("primary binding should load"),
            Some(binding)
        );
        assert_eq!(
            store
                .list_task_run_turns(run.id.as_str())
                .await
                .expect("turns should list"),
            vec![turn]
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate(run.id.as_str())
                .await
                .expect("accepted candidate should load")
                .map(|candidate| candidate.final_review_event_id),
            Some(Some(review_event.id.clone()))
        );
        assert_eq!(
            store
                .list_task_result_review_events(candidate.id.as_str())
                .await
                .expect("review events should list"),
            vec![review_event]
        );
        assert_eq!(
            store
                .get_task_run(run.id.as_str())
                .await
                .expect("run lookup should succeed")
                .expect("run should exist")
                .status,
            TaskRunStatus::WaitingReview
        );
        assert_eq!(
            store
                .get_task(task.id.as_str())
                .await
                .expect("task lookup should succeed")
                .expect("task should exist")
                .task
                .status,
            TaskStatus::WaitingReview
        );
        assert_eq!(
            store
                .load_execution_for_run(run.id.as_str())
                .await
                .expect("execution lookup should succeed")
                .expect("execution should exist")
                .status,
            TaskRunExecutionStatus::WaitingReview
        );
    }

    #[tokio::test]
    async fn task_review_projector_replays_legacy_child_thread_linked_json() {
        let store = test_store_with_workspace("ws_task_review_legacy_projector").await;
        let timestamp = 1_700_003_000;
        let mut run = sample_task_run(timestamp);
        run.id = "run_legacy_projector".to_owned();
        run.task_id = "task_legacy_projector".to_owned();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        let legacy_link_json = serde_json::json!({
            "kind": "child_thread_linked",
            "payload": {
                "lineage": {
                    "childThreadId": "child_thread_legacy_projector",
                    "childTurnId": "child_turn_legacy_projector",
                    "parentThreadId": "parent_thread_legacy_projector",
                    "parentTurnId": "parent_turn_legacy_projector",
                    "taskId": run.task_id.clone(),
                    "taskRunId": run.id.clone(),
                    "rootThreadId": "root_thread_legacy_projector",
                    "depth": 1,
                    "createdAt": timestamp + 1
                }
            }
        });
        let legacy_link: TaskEventPayload =
            serde_json::from_value(legacy_link_json).expect("legacy child link should decode");

        let completed_result = TaskResult {
            summary: Some("legacy accepted".to_owned()),
            data: Some(TaskValue::String("legacy".to_owned())),
            artifacts: Vec::new(),
            completed_by_run_id: Some(run.id.clone()),
        };
        let events = vec![
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: None,
            },
            legacy_link,
            TaskEventPayload::RunCompleted {
                task_id: run.task_id.clone(),
                run_id: run.id.clone(),
                result: Some(completed_result.clone()),
                completed_at: timestamp + 2,
            },
        ];
        store
            .append_task_events(events.clone(), timestamp + 10)
            .await
            .expect("legacy events should project");
        store
            .append_task_events(events, timestamp + 10)
            .await
            .expect("legacy replay should be idempotent");

        let binding = store
            .get_task_run_primary_thread_binding(run.id.as_str())
            .await
            .expect("legacy binding should load")
            .expect("legacy binding should exist");
        assert_eq!(binding.execution_id, None);
        assert_eq!(binding.thread_id, "child_thread_legacy_projector");

        let turns = store
            .list_task_run_turns(run.id.as_str())
            .await
            .expect("legacy turns should list");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].turn_id, "child_turn_legacy_projector");
        assert_eq!(turns[0].status, TaskRunTurnStatus::InProgress);

        let accepted = store
            .get_accepted_task_result_candidate(run.id.as_str())
            .await
            .expect("legacy accepted candidate should load")
            .expect("legacy accepted candidate should exist");
        assert_eq!(accepted.result, Some(completed_result));
        assert_eq!(
            store
                .list_task_result_review_events(accepted.id.as_str())
                .await
                .expect("legacy auto review should list")
                .into_iter()
                .map(|event| (event.event_kind, event.reviewer_kind, event.decision))
                .collect::<Vec<_>>(),
            vec![(
                TaskResultReviewEventKind::SystemAuto,
                TaskResultReviewerKind::RuntimeAuto,
                TaskResultReviewDecision::Accept
            )]
        );
    }

    #[tokio::test]
    async fn task_review_projector_replays_rejection_revision_and_failed_turn_events() {
        let store = test_store_with_workspace("ws_task_review_revision_projector").await;
        let timestamp = 1_700_004_000;
        let mut task = sample_task(timestamp);
        task.id = "task_revision_projector".to_owned();
        task.workspace_id = "ws_task_review_revision_projector".to_owned();
        let mut run = sample_task_run(timestamp);
        run.id = "run_revision_projector".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        let initial_turn = TaskRunTurn {
            id: "task_run_turn_revision_initial".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: "thread_revision_projector".to_owned(),
            turn_id: "turn_revision_initial".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 1,
            started_at: Some(timestamp + 1),
            completed_at: Some(timestamp + 2),
        };
        let candidate = TaskResultCandidate {
            id: "candidate_revision_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: initial_turn.id.clone(),
            thread_id: initial_turn.thread_id.clone(),
            turn_id: initial_turn.turn_id.clone(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("needs changes".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run.id.clone()),
            }),
            extraction_error: None,
            summary: Some("needs changes".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: timestamp + 3,
            updated_at: timestamp + 3,
            resolved_at: None,
        };
        let review_event = TaskResultReviewEvent {
            id: "review_revision_projector".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: initial_turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::ParentAgent,
            reviewer_thread_id: Some("parent_thread_revision_projector".to_owned()),
            reviewer_turn_id: Some("parent_turn_revision_projector".to_owned()),
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::RequestChanges,
            feedback_text: Some("tighten result".to_owned()),
            feedback: None,
            confidence: Some(0.8),
            supersedes_review_event_id: None,
            next_task_run_turn_id: Some("task_run_turn_revision_retry".to_owned()),
            created_at: timestamp + 4,
        };
        let rejected_candidate = TaskResultCandidate {
            status: TaskResultCandidateStatus::Rejected,
            final_review_event_id: Some(review_event.id.clone()),
            updated_at: timestamp + 4,
            resolved_at: Some(timestamp + 4),
            ..candidate.clone()
        };
        let failed_revision_turn = TaskRunTurn {
            id: "task_run_turn_revision_retry".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: initial_turn.thread_id.clone(),
            turn_id: "turn_revision_retry".to_owned(),
            kind: TaskRunTurnKind::Revision,
            round: 1,
            sequence: 1,
            status: TaskRunTurnStatus::Failed,
            reviews_candidate_id: None,
            requested_by_candidate_id: Some(candidate.id.clone()),
            requested_by_review_event_id: Some(review_event.id.clone()),
            created_at: timestamp + 5,
            started_at: Some(timestamp + 5),
            completed_at: Some(timestamp + 6),
        };

        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                    TaskEventPayload::TaskRunTurnCompleted {
                        task_run_turn: initial_turn,
                    },
                    TaskEventPayload::TaskResultCandidateCreated {
                        candidate: candidate.clone(),
                    },
                    TaskEventPayload::TaskResultReviewEventRecorded {
                        review_event: review_event.clone(),
                    },
                    TaskEventPayload::TaskResultCandidateRejected {
                        candidate: rejected_candidate,
                        review_event_id: review_event.id.clone(),
                    },
                    TaskEventPayload::TaskRevisionRequested {
                        task_id: run.task_id.clone(),
                        run_id: run.id.clone(),
                        previous_candidate_id: candidate.id.clone(),
                        requested_by_review_event_id: review_event.id.clone(),
                        task_run_turn_id: failed_revision_turn.id.clone(),
                        thread_id: failed_revision_turn.thread_id.clone(),
                        turn_id: failed_revision_turn.turn_id.clone(),
                        round: failed_revision_turn.round,
                        feedback: "tighten result".to_owned(),
                        requested_at: timestamp + 5,
                    },
                    TaskEventPayload::TaskRunTurnFailed {
                        task_run_turn: failed_revision_turn.clone(),
                        error: None,
                    },
                ],
                timestamp + 10,
            )
            .await
            .expect("revision events should project");

        let rejected = store
            .get_task_result_candidate(candidate.id.as_str())
            .await
            .expect("candidate lookup should succeed")
            .expect("candidate should exist");
        assert_eq!(rejected.status, TaskResultCandidateStatus::Rejected);
        assert_eq!(
            rejected.final_review_event_id.as_deref(),
            Some(review_event.id.as_str())
        );
        assert_eq!(
            store
                .get_latest_task_run_turn(run.id.as_str())
                .await
                .expect("latest turn should load"),
            Some(failed_revision_turn)
        );
    }

    #[tokio::test]
    async fn thread_agents_doc_repository_round_trips_draft_save_archive() {
        let store = test_store_with_workspace("ws_agents_doc_crud").await;

        let draft = store
            .create_thread_agents_doc_draft("ws_agents_doc_crud", None, Some("user-1"))
            .await
            .expect("draft should create");
        assert_eq!(draft.status, ThreadAgentsDocStatus::Draft);
        assert_eq!(draft.version, 1);
        assert!(draft.content.is_empty());

        let duplicate = store
            .create_thread_agents_doc_draft("ws_agents_doc_crud", None, Some("user-1"))
            .await
            .expect("duplicate draft create should return existing draft");
        assert_eq!(duplicate.id, draft.id);

        let active = store
            .save_thread_agents_doc(
                "ws_agents_doc_crud",
                None,
                "Use cargo test.\r\nKeep docs short.",
                Some(draft.version),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("non-empty save should activate doc");
        assert_eq!(active.status, ThreadAgentsDocStatus::Active);
        assert_eq!(active.version, 2);
        assert_eq!(active.content, "Use cargo test.\nKeep docs short.");

        let revisions = store
            .list_thread_agents_doc_revisions(active.id.as_str())
            .await
            .expect("revisions should list");
        assert_eq!(revisions.len(), 1);
        assert_eq!(revisions[0].version, active.version);
        assert_eq!(revisions[0].save_reason, ThreadAgentsDocSaveReason::Manual);

        let unchanged = store
            .save_thread_agents_doc(
                "ws_agents_doc_crud",
                None,
                "Use cargo test.\nKeep docs short.",
                Some(active.version),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Autosave,
            )
            .await
            .expect("unchanged save should no-op");
        assert_eq!(unchanged.version, active.version);
        assert_eq!(
            store
                .list_thread_agents_doc_revisions(active.id.as_str())
                .await
                .expect("revisions should list")
                .len(),
            1
        );

        let conflict = store
            .save_thread_agents_doc(
                "ws_agents_doc_crud",
                None,
                "Changed",
                Some(1),
                Some("user-2"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect_err("stale expected version should conflict");
        assert!(matches!(
            conflict,
            ThreadAgentsDocError::VersionConflict {
                expected: 1,
                actual: 2
            }
        ));

        let archived = store
            .archive_thread_agents_doc(
                "ws_agents_doc_crud",
                None,
                Some(active.version),
                Some("user-1"),
            )
            .await
            .expect("archive should succeed")
            .expect("archive should return archived doc");
        assert_eq!(archived.status, ThreadAgentsDocStatus::Archived);
        assert_eq!(archived.version, 3);

        let after_archive = store
            .get_thread_agents_doc_explicit("ws_agents_doc_crud", None)
            .await
            .expect("explicit lookup should succeed");
        assert!(after_archive.is_none());

        let replacement = store
            .create_thread_agents_doc_draft("ws_agents_doc_crud", None, Some("user-1"))
            .await
            .expect("archived doc should not block replacement draft");
        assert_eq!(replacement.status, ThreadAgentsDocStatus::Draft);
        assert_ne!(replacement.id, archived.id);
    }

    #[tokio::test]
    async fn thread_agents_doc_resolver_uses_nearest_active_ancestor() {
        let store = test_store_with_workspace("ws_agents_doc_resolve").await;

        let root = store
            .save_thread_agents_doc(
                "ws_agents_doc_resolve",
                None,
                "root instructions",
                None,
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("root doc should save");

        let parent = store
            .create_thread_folder("ws_agents_doc_resolve", None, "Parent")
            .await
            .expect("parent folder should create");
        let child = store
            .create_thread_folder("ws_agents_doc_resolve", Some(parent.id.as_str()), "Child")
            .await
            .expect("child folder should create");

        let root_resolution = store
            .resolve_thread_agents_doc_for_folder("ws_agents_doc_resolve", None)
            .await
            .expect("root resolution should succeed")
            .expect("root doc should resolve");
        assert_eq!(root_resolution.doc.id, root.id);
        assert!(!root_resolution.inherited);

        store
            .save_thread_agents_doc(
                "ws_agents_doc_resolve",
                Some(parent.id.as_str()),
                "parent instructions",
                None,
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("parent doc should save");

        store
            .create_thread_agents_doc_draft(
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                Some("user-1"),
            )
            .await
            .expect("child blank draft should create");

        let inherited = store
            .resolve_thread_agents_doc_for_folder("ws_agents_doc_resolve", Some(child.id.as_str()))
            .await
            .expect("child resolution should succeed")
            .expect("parent doc should resolve for child");
        assert_eq!(inherited.doc.content, "parent instructions");
        assert_eq!(
            inherited.source_folder_id.as_deref(),
            Some(parent.id.as_str())
        );
        assert_eq!(inherited.source_path, vec!["Parent".to_owned()]);
        assert!(inherited.inherited);

        let child_doc = store
            .save_thread_agents_doc(
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                "child instructions",
                None,
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("child doc should save");
        let child_resolution = store
            .resolve_thread_agents_doc_for_folder("ws_agents_doc_resolve", Some(child.id.as_str()))
            .await
            .expect("child resolution should succeed")
            .expect("child doc should resolve");
        assert_eq!(child_resolution.doc.id, child_doc.id);
        assert_eq!(
            child_resolution.source_path,
            vec!["Parent".to_owned(), "Child".to_owned()]
        );
        assert!(!child_resolution.inherited);

        store
            .archive_thread_agents_doc(
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                Some(child_doc.version),
                Some("user-1"),
            )
            .await
            .expect("child archive should succeed");

        store
            .move_thread_to_folder(
                "ws_agents_doc_resolve",
                "thread_agents_doc_resolve_thread",
                Some(child.id.as_str()),
            )
            .await
            .expect("thread placement should save");
        let thread_resolution = store
            .resolve_thread_agents_doc_for_thread(
                "ws_agents_doc_resolve",
                "thread_agents_doc_resolve_thread",
            )
            .await
            .expect("thread resolution should succeed")
            .expect("parent doc should resolve after child archive");
        assert_eq!(thread_resolution.doc.content, "parent instructions");
        assert_eq!(
            thread_resolution.resolved_for_folder_id,
            Some(child.id.clone())
        );

        let mismatch = store
            .resolve_thread_agents_doc_for_folder("other_workspace", Some(child.id.as_str()))
            .await
            .expect_err("folder workspace mismatch should fail");
        assert!(matches!(
            mismatch,
            ThreadAgentsDocError::WorkspaceMismatch { .. }
        ));
    }

    fn sample_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        }
    }

    fn sample_task(timestamp: i64) -> Task {
        Task {
            id: "task_0000000000000001".to_owned(),
            workspace_id: "ws_task".to_owned(),
            owner_kind: TaskOwnerKind::Thread,
            owner_id: Some("thr_task".to_owned()),
            created_by_thread_id: Some("thr_task".to_owned()),
            created_by_turn_id: Some("turn_task".to_owned()),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status: TaskStatus::Scheduled,
            title: "Check weather".to_owned(),
            goal: "Send the daily weather summary".to_owned(),
            priority: 10,
            lifecycle_policy: None,
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: Some(TaskMetadata {
                labels: vec!["weather".to_owned()],
                data: Some(TaskValue::Object(BTreeMap::from([(
                    "city".to_owned(),
                    TaskValue::String("Berlin".to_owned()),
                )]))),
            }),
            result: None,
            error: None,
            revision: 0,
            created_at: timestamp,
            updated_at: timestamp,
            completed_at: None,
        }
    }

    fn sample_task_trigger(timestamp: i64) -> TaskTrigger {
        TaskTrigger {
            id: "trg_00000000000000001".to_owned(),
            task_id: "task_0000000000000001".to_owned(),
            status: TaskTriggerStatus::Active,
            spec: TaskTriggerSpec::ScheduledAt {
                scheduled_at: timestamp + 3600,
                timezone: Some("Europe/Berlin".to_owned()),
                catch_up_policy: None,
            },
            next_fire_at: Some(timestamp + 3600),
            last_fire_at: None,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    fn sample_task_run(timestamp: i64) -> TaskRun {
        TaskRun {
            id: "run_00000000000000001".to_owned(),
            task_id: "task_0000000000000001".to_owned(),
            trigger_id: Some("trg_00000000000000001".to_owned()),
            parent_run_id: None,
            run_group_id: "run_00000000000000001".to_owned(),
            attempt_number: 1,
            retry_of_run_id: None,
            ready_at: Some(timestamp),
            run_number: 1,
            status: TaskRunStatus::Queued,
            executor_kind: TaskExecutorKind::Agent,
            started_at: None,
            completed_at: None,
            heartbeat_at: None,
            locked_by: None,
            lock_expires_at: None,
            result: None,
            error: None,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    fn sample_task_agent_spec(timestamp: i64) -> TaskAgentSpec {
        TaskAgentSpec {
            id: "ags_00000000000000001".to_owned(),
            task_id: "task_0000000000000001".to_owned(),
            run_id: Some("run_00000000000000001".to_owned()),
            agent_role: Some("worker".to_owned()),
            agent_nickname: Some("Weather worker".to_owned()),
            model: Some("gpt-5.4".to_owned()),
            model_provider: Some("openai".to_owned()),
            prompt: TaskAgentPrompt {
                goal: "Check weather".to_owned(),
                instructions: Vec::new(),
                input: None,
                output_instructions: None,
            },
            context_policy: None,
            tool_policy: None,
            permission_cap: None,
            result_contract: Some(TaskAgentResultContract {
                format: TaskAgentResultFormat::Json,
                required: true,
                schema: Some(TaskSchema {
                    name: Some("weather_summary".to_owned()),
                    description: None,
                    schema: TaskValue::Object(BTreeMap::from([
                        ("type".to_owned(), TaskValue::String("object".to_owned())),
                        (
                            "required".to_owned(),
                            TaskValue::List(vec![TaskValue::String("summary".to_owned())]),
                        ),
                    ])),
                }),
            }),
            review_policy: None,
            depth: 0,
            max_depth: 3,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    #[tokio::test]
    async fn task_events_append_project_and_read_back_lifecycle_state() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let task = sample_task(timestamp);
        let trigger = sample_task_trigger(timestamp);
        let run = sample_task_run(timestamp);
        let agent_spec = sample_task_agent_spec(timestamp);

        let created = store
            .append_task_event(
                TaskEventPayload::TaskCreated { task: task.clone() },
                timestamp,
            )
            .await
            .expect("task created event should append");
        assert_eq!(created.sequence, 1);

        let scheduled = store
            .append_task_event(
                TaskEventPayload::TriggerCreated {
                    trigger: trigger.clone(),
                },
                timestamp + 1,
            )
            .await
            .expect("trigger event should append");
        assert_eq!(scheduled.sequence, 2);

        let run_created = store
            .append_task_event(
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: Some(agent_spec),
                },
                timestamp + 2,
            )
            .await
            .expect("run created event should append");
        assert_eq!(run_created.sequence, 3);

        store
            .append_task_event(
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    started_at: timestamp + 3,
                },
                timestamp + 3,
            )
            .await
            .expect("run started event should append");

        store
            .append_task_event(
                TaskEventPayload::RunCompleted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    result: Some(TaskResult {
                        summary: Some("Clear".to_owned()),
                        data: None,
                        artifacts: Vec::new(),
                        completed_by_run_id: Some(run.id.clone()),
                    }),
                    completed_at: timestamp + 4,
                },
                timestamp + 4,
            )
            .await
            .expect("run completed event should append");

        let completed = store
            .append_task_event(
                TaskEventPayload::TaskCompleted {
                    task_id: task.id.clone(),
                    result: Some(TaskResult {
                        summary: Some("Clear".to_owned()),
                        data: None,
                        artifacts: Vec::new(),
                        completed_by_run_id: Some(run.id.clone()),
                    }),
                    completed_at: timestamp + 5,
                },
                timestamp + 5,
            )
            .await
            .expect("task completed event should append");
        assert_eq!(completed.sequence, 6);

        let response = store
            .get_task(task.id.as_str())
            .await
            .expect("task read should succeed")
            .expect("task should exist");
        assert_eq!(response.task.status, TaskStatus::Completed);
        assert_eq!(
            response
                .task
                .result
                .as_ref()
                .and_then(|result| result.summary.as_deref()),
            Some("Clear")
        );
        assert_eq!(response.triggers.len(), 1);
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].status, TaskRunStatus::Succeeded);
        assert_eq!(response.agent_specs.len(), 1);

        let events = store
            .get_task_events(task.id.as_str(), None)
            .await
            .expect("task events read should succeed");
        assert_eq!(events.last_sequence, 6);
        assert_eq!(events.events.len(), 6);

        let duplicate = connection
            .execute_unprepared(&format!(
                "INSERT INTO task_event \
                 (id, task_id, sequence, event_type, payload_json, created_at) \
                 VALUES ('evt_duplicate_sequence', '{}', 1, 'task/created', '{{}}', CURRENT_TIMESTAMP)",
                task.id
            ))
            .await;
        assert!(
            duplicate.is_err(),
            "duplicate (task_id, sequence) must be rejected"
        );

        let duplicate_run_number = connection
            .execute_unprepared(&format!(
                "INSERT INTO task_run \
                 (id, task_id, run_number, status, executor_kind) \
                 VALUES ('run_duplicate_number', '{}', {}, 'queued', 'agent')",
                task.id, run.run_number
            ))
            .await;
        assert!(
            duplicate_run_number.is_err(),
            "duplicate (task_id, run_number) must be rejected"
        );
    }

    fn sample_mcp_installation(name: &str) -> McpServerInstallationRecord {
        McpServerInstallationRecord {
            id: None,
            scope_kind: "workspace".to_owned(),
            scope_key: "ws_mcp".to_owned(),
            name: name.to_owned(),
            display_name: None,
            source_kind: "config".to_owned(),
            source_ref: serde_json::json!({
                "source_kind": "config",
                "server": name,
                "transport": "stdio"
            })
            .to_string(),
            transport_kind: "stdio".to_owned(),
            transport_json: serde_json::json!({
                "type": "stdio",
                "command": "npx",
                "args": [],
                "env": {},
                "startup_timeout_ms": 10_000,
                "tool_timeout_ms": 60_000
            })
            .to_string(),
            auth_json: "{}".to_owned(),
            secret_refs_json: "[]".to_owned(),
            enabled: true,
            allow_implicit_invocation: true,
            required: false,
            fingerprint: format!("fingerprint-{name}"),
            updated_at_unix: 0,
        }
    }

    fn safe_web_fetch_item(item_id: &str) -> TurnItem {
        TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({"url": "https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com",
                    "statusCode": 200
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: Some("https://example.com".to_owned()),
            status_code: Some(200),
            content_type: Some("text/html".to_owned()),
            extract_mode: None,
            resolved_mode: None,
            bytes_received: Some(1024),
            elapsed_ms: Some(42),
            truncated: Some(serde_json::json!(false)),
            title: Some("Example Domain".to_owned()),
            word_count: Some(12),
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        }
    }

    #[tokio::test]
    async fn item_snapshot_update_does_not_append_turn_event_but_surfaces_in_turn_items() {
        let workspace_id = "ws_diff_snapshot";
        let thread_id = "thr_diff_snapshot";
        let turn_id = "turn_diff_snapshot";
        let timestamp = 1_700_000_000;
        let store = test_store_with_workspace(workspace_id).await;
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let first_diff = TurnItem::SystemEvent {
            id: "agent_diff_native_turn".to_owned(),
            level: SystemEventLevel::Info,
            message: "Diff updated".to_owned(),
            code: Some("agent_diff_updated".to_owned()),
            details: Some(serde_json::json!({"payload": "first diff"})),
        };
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: first_diff,
                },
                timestamp + 1,
            )
            .await
            .expect("first snapshot should persist");

        let second_diff = TurnItem::SystemEvent {
            id: "agent_diff_native_turn".to_owned(),
            level: SystemEventLevel::Info,
            message: "Diff updated".to_owned(),
            code: Some("agent_diff_updated".to_owned()),
            details: Some(serde_json::json!({"payload": "second diff"})),
        };
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: second_diff,
                },
                timestamp + 2,
            )
            .await
            .expect("second snapshot should persist");

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .all(&store.connection)
            .await
            .expect("raw events should query");
        assert_eq!(
            raw_events.len(),
            1,
            "snapshot updates must not append extra turn_event rows"
        );

        let response = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn should exist");
        assert_eq!(response.events.len(), 1);
        let TurnItemEventPayload::ItemUpdated { item, .. } = &response.events[0].payload else {
            panic!("snapshot should surface as item/updated");
        };
        let TurnItem::SystemEvent { details, .. } = item else {
            panic!("snapshot should be a system event");
        };
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&serde_json::json!("second diff"))
        );
        assert_eq!(response.last_sequence, raw_events[0].sequence);

        let current_diff = store
            .get_turn_item(turn_id, "agent_diff_native_turn")
            .await
            .expect("current diff should load")
            .expect("current diff should exist");
        let committed = store
            .materialize_agent_diff_final_snapshot_if_changed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: current_diff.clone(),
                },
                timestamp + 3,
            )
            .await
            .expect("final diff snapshot should persist");
        assert!(committed, "first final snapshot should append raw event");
        let skipped = store
            .materialize_agent_diff_final_snapshot_if_changed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: current_diff,
                },
                timestamp + 4,
            )
            .await
            .expect("duplicate final diff snapshot should compare payload");
        assert!(!skipped, "unchanged final snapshot should be skipped");

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .all(&store.connection)
            .await
            .expect("raw events should query after final snapshot");
        assert_eq!(
            raw_events.len(),
            2,
            "turn/start plus one final diff snapshot should remain"
        );
    }

    #[tokio::test]
    async fn compact_superseded_agent_diff_events_removes_only_old_projected_snapshots() {
        let workspace_id = "ws_diff_compaction";
        let thread_id = "thr_diff_compaction";
        let turn_id = "turn_diff_compaction";
        let timestamp = 1_700_000_000;
        let store = test_store_with_workspace(workspace_id).await;
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        for (index, payload) in ["first diff", "second diff", "third diff"]
            .into_iter()
            .enumerate()
        {
            store
                .materialize_item_completed(
                    ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: TurnItem::SystemEvent {
                            id: "agent_diff_native_turn".to_owned(),
                            level: SystemEventLevel::Info,
                            message: "Diff updated".to_owned(),
                            code: Some("agent_diff_updated".to_owned()),
                            details: Some(serde_json::json!({"payload": payload})),
                        },
                    },
                    timestamp + 1 + index as i64,
                )
                .await
                .expect("historical diff event should persist");
        }

        let dry_run = store
            .compact_superseded_agent_diff_turn_events(100, true)
            .await
            .expect("dry run should succeed");
        assert_eq!(dry_run.candidate_rows, 2);
        assert_eq!(dry_run.deleted_rows, 0);
        assert!(dry_run.payload_bytes > 0);
        assert_eq!(dry_run.latest_snapshots_kept, 1);
        assert_eq!(dry_run.skipped_unprojected, 0);
        assert_eq!(dry_run.skipped_failed, 0);

        let compacted = store
            .compact_superseded_agent_diff_turn_events(100, false)
            .await
            .expect("compaction should succeed");
        assert_eq!(compacted.candidate_rows, 2);
        assert_eq!(compacted.deleted_rows, 2);
        assert_eq!(compacted.latest_snapshots_kept, 1);

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("raw events should query");
        assert_eq!(
            raw_events.len(),
            2,
            "turn/start and the latest diff event should remain"
        );

        let response = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn should exist");
        assert_eq!(response.events.len(), 1);
        let TurnItemEventPayload::ItemCompleted { item, .. } = &response.events[0].payload else {
            panic!("latest historical diff should remain durable");
        };
        let TurnItem::SystemEvent { details, .. } = item else {
            panic!("latest diff should be a system event");
        };
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&serde_json::json!("third diff"))
        );
    }

    #[tokio::test]
    async fn compact_agent_diff_summary_counts_skipped_projection_states() {
        let workspace_id = "ws_diff_compaction_skips";
        let thread_id = "thr_diff_compaction_skips";
        let turn_id = "turn_diff_compaction_skips";
        let timestamp = 1_700_000_000;
        let store = test_store_with_workspace(workspace_id).await;
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        for (index, payload) in [
            "failed old diff",
            "pending old diff",
            "projected old diff",
            "latest diff",
        ]
        .into_iter()
        .enumerate()
        {
            store
                .materialize_item_completed(
                    ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: TurnItem::SystemEvent {
                            id: "agent_diff_native_turn".to_owned(),
                            level: SystemEventLevel::Info,
                            message: "Diff updated".to_owned(),
                            code: Some("agent_diff_updated".to_owned()),
                            details: Some(serde_json::json!({"payload": payload})),
                        },
                    },
                    timestamp + 1 + index as i64,
                )
                .await
                .expect("historical diff event should persist");
        }

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("raw events should query");
        assert_eq!(raw_events.len(), 5);

        for (event, status) in [
            (
                &raw_events[1],
                crate::repositories::turn_event_projection_state::PROJECTION_STATUS_FAILED,
            ),
            (
                &raw_events[2],
                crate::repositories::turn_event_projection_state::PROJECTION_STATUS_PENDING,
            ),
        ] {
            pioneer_entity::turn_event_projection_state::Entity::update_many()
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::Status,
                    sea_orm::sea_query::Expr::value(status.to_owned()),
                )
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::EventId
                        .eq(event.id.clone()),
                )
                .exec(&store.connection)
                .await
                .expect("projection state status should update");
        }

        let dry_run = store
            .compact_superseded_agent_diff_turn_events(100, true)
            .await
            .expect("dry run should succeed");
        assert_eq!(dry_run.candidate_rows, 1);
        assert_eq!(dry_run.deleted_rows, 0);
        assert_eq!(dry_run.latest_snapshots_kept, 1);
        assert_eq!(dry_run.skipped_unprojected, 1);
        assert_eq!(dry_run.skipped_failed, 1);

        let compacted = store
            .compact_superseded_agent_diff_turn_events(100, false)
            .await
            .expect("compaction should succeed");
        assert_eq!(compacted.candidate_rows, 1);
        assert_eq!(compacted.deleted_rows, 1);
        assert_eq!(compacted.skipped_unprojected, 1);
        assert_eq!(compacted.skipped_failed, 1);

        let remaining_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("remaining raw events should query");
        let sequences = remaining_events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            sequences,
            vec![1, 2, 3, 5],
            "compaction must leave sequence gaps instead of renumbering"
        );

        let response = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn should exist");
        assert_eq!(response.events.len(), 3);
        let TurnItemEventPayload::ItemCompleted { item, .. } = &response
            .events
            .last()
            .expect("latest event should exist")
            .payload
        else {
            panic!("latest raw snapshot should remain durable");
        };
        let TurnItem::SystemEvent { details, .. } = item else {
            panic!("latest diff should be a system event");
        };
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&serde_json::json!("latest diff"))
        );
    }

    #[tokio::test]
    async fn claimed_recovery_job_can_be_marked_active_and_retried() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let turn_id = "turn_recovery_budget";

        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_1".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::Fallback,
                Some("provider failed".to_owned()),
                None,
                None,
                None,
                0,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_000,
            )
            .await
            .expect("job should enqueue");

        let claimed = store
            .claim_due_recovery_jobs(1_700_000_001, 45, 1)
            .await
            .expect("job should claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job.id);
        let claim_token = claimed[0]
            .claim_token
            .as_deref()
            .expect("claimed job should have claim token");
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    job.id.as_str(),
                    claim_token,
                    "recovery_attempt_1",
                    1_700_000_001,
                )
                .await
                .expect("job should transition to active"),
            ClaimedRecoveryActivation::Activated
        ));
        assert!(
            store
                .mark_recovery_job_retrying(
                    job.id.as_str(),
                    "recovery_attempt_1",
                    1_700_000_010,
                    Some("provider failed during recovery".to_owned()),
                    1_700_000_002,
                )
                .await
                .expect("active job should requeue")
        );

        let job = store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(job.status, RecoveryJobStatus::Pending);
        assert_eq!(job.run_count, 1);
        assert_eq!(job.provider_attempt_number, 1);
    }

    #[tokio::test]
    async fn claimed_recovery_job_cannot_activate_while_turn_has_active_recovery() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let turn_id = "turn_single_active_recovery";

        for index in 0..2 {
            store
                .enqueue_recovery_job(
                    turn_id.to_owned(),
                    format!("reasoning_{index}"),
                    TurnItemType::Reasoning,
                    None,
                    RecoveryTrigger::ProviderError,
                    RecoveryAction::RetryWithBackoff,
                    Some("provider failed".to_owned()),
                    None,
                    None,
                    None,
                    0,
                    3,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    1_700_000_000,
                )
                .await
                .expect("job should enqueue");
        }

        let claimed = store
            .claim_due_recovery_jobs(1_700_000_001, 45, 2)
            .await
            .expect("jobs should claim");
        assert_eq!(claimed.len(), 2);

        let first_token = claimed[0]
            .claim_token
            .as_deref()
            .expect("first claimed job should have claim token");
        let second_token = claimed[1]
            .claim_token
            .as_deref()
            .expect("second claimed job should have claim token");

        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    claimed[0].id.as_str(),
                    first_token,
                    "recovery_attempt_1",
                    1_700_000_001,
                )
                .await
                .expect("first job should activate"),
            ClaimedRecoveryActivation::Activated
        ));
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    claimed[1].id.as_str(),
                    second_token,
                    "recovery_attempt_2",
                    1_700_000_001,
                )
                .await
                .expect("second job should be blocked"),
            ClaimedRecoveryActivation::BlockedByActiveRecovery
        ));

        assert!(
            store
                .release_claimed_recovery_job(
                    claimed[1].id.as_str(),
                    second_token,
                    1_700_000_003,
                    Some("another recovery is already active for this turn".to_owned()),
                    1_700_000_001,
                )
                .await
                .expect("blocked job should release")
        );

        let second = store
            .get_recovery_job(claimed[1].id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(second.status, RecoveryJobStatus::Pending);
        assert_eq!(second.run_count, 0);
        assert!(second.claim_token.is_none());
        assert!(second.active_attempt_id.is_none());
    }

    #[tokio::test]
    async fn tool_recovery_policy_snapshot_round_trips_through_items_and_history() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_tool_policy";
        let thread_id = "thr_tool_policy";
        let turn_id = "turn_tool_policy";
        let item_id = "item_tool_policy";
        let recovery_policy = sample_tool_recovery_policy();

        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let started_item = TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({"url": "https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: Some(recovery_policy.clone()),
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com"
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        };
        let mut completed_item = started_item.clone();
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }

        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: started_item,
                },
                timestamp + 1,
            )
            .await
            .expect("item started should persist");
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                timestamp + 2,
            )
            .await
            .expect("item completed should persist");

        let stored_item = store
            .get_turn_item(turn_id, item_id)
            .await
            .expect("item lookup should succeed")
            .expect("item should exist");
        assert_eq!(stored_item.recovery_policy(), Some(&recovery_policy));
        if let TurnItem::WebFetch { output_policy, .. } = stored_item {
            assert_eq!(
                output_policy,
                ToolOutputPolicySnapshot::for_tool_name("web_fetch")
            );
        } else {
            panic!("expected web_fetch item");
        }

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items query should succeed")
            .expect("turn items should exist");
        let item_events_have_snapshot = turn_items.events.iter().filter(|event| {
            matches!(
                &event.payload,
                TurnItemEventPayload::ItemStarted { item, .. }
                    | TurnItemEventPayload::ItemCompleted { item, .. }
                        if item.recovery_policy() == Some(&recovery_policy)
            )
        });
        assert_eq!(item_events_have_snapshot.count(), 2);

        let history = store
            .get_thread_history(thread_id, Some(16))
            .await
            .expect("thread history query should succeed")
            .expect("thread history should exist");
        let history_events_have_snapshot = history.events.iter().filter(|event| {
            matches!(
                &event.payload,
                ThreadHistoryEventPayload::ItemStarted { item, .. }
                    | ThreadHistoryEventPayload::ItemCompleted { item, .. }
                        if item.recovery_policy() == Some(&recovery_policy)
            )
        });
        assert_eq!(history_events_have_snapshot.count(), 2);
    }

    #[tokio::test]
    async fn item_started_can_atomically_persist_attempt_deadlines() {
        let store = test_store_with_workspace("ws_attempt_deadlines").await;
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_attempt_deadlines";
        let thread_id = "thr_attempt_deadlines";
        let turn_id = "turn_attempt_deadlines";
        let item_id = "call_attempt_deadlines";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let item = TurnItem::DynamicToolCall {
            id: item_id.to_owned(),
            tool_name: "task_create".to_owned(),
            arguments: serde_json::json!({ "title": "Daily weather" }),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_create"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            success: None,
            outcome: None,
            observation: None,
        };
        let deadlines = TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(timestamp + 121),
            idle_deadline_at_unix: Some(timestamp + 91),
            hard_deadline_at_unix: Some(timestamp + 301),
        };

        store
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
                timestamp + 1,
                deadlines,
            )
            .await
            .expect("item started should persist with deadlines");

        let attempt = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
            .one(&store.connection)
            .await
            .expect("attempt lookup should succeed")
            .expect("attempt should exist");

        assert_eq!(
            attempt.lease_expires_at,
            deadlines.lease_expires_at_unix.map(unix_to_datetime)
        );
        assert_eq!(
            attempt.idle_deadline_at,
            deadlines.idle_deadline_at_unix.map(unix_to_datetime)
        );
        assert_eq!(
            attempt.hard_deadline_at,
            deadlines.hard_deadline_at_unix.map(unix_to_datetime)
        );

        let missing = store
            .list_running_attempts_missing_deadlines(10)
            .await
            .expect("deadline repair candidates should query");
        assert!(
            missing.is_empty(),
            "attempt with all deadlines should not require repair"
        );
    }

    #[tokio::test]
    async fn turn_event_projection_failure_keeps_raw_event_and_replays_in_order() {
        let store = test_store_with_workspace("ws_projection_replay").await;
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_projection_replay";
        let thread_id = "thr_projection_replay";
        let turn_id = "turn_projection_replay";
        let item_id = "call_projection_replay";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let start_event = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_event::Column::Sequence.eq(1))
            .one(&store.connection)
            .await
            .expect("turn event lookup should succeed")
            .expect("turn/start event should exist");

        pioneer_entity::turn_event_projection_state::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::Status,
                sea_orm::sea_query::Expr::value(
                    crate::repositories::turn_event_projection_state::PROJECTION_STATUS_FAILED,
                ),
            )
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::NextRunAt,
                sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 2)),
            )
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(
                    Option::<sea_orm::entity::prelude::DateTimeWithTimeZone>::None,
                ),
            )
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 2)),
            )
            .filter(pioneer_entity::turn_event_projection_state::Column::EventId.eq(start_event.id))
            .exec(&store.connection)
            .await
            .expect("projection state should be marked failed");

        let deadlines = TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(timestamp + 121),
            idle_deadline_at_unix: Some(timestamp + 91),
            hard_deadline_at_unix: Some(timestamp + 301),
        };

        let projection_error = store
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item(item_id),
                },
                timestamp + 1,
                deadlines,
            )
            .await
            .expect_err("item projection should wait for earlier turn event");
        assert!(
            format!("{projection_error:#}").contains("waiting for an earlier event"),
            "projection should fail because an earlier event is not projected"
        );

        let events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("turn event list should succeed");
        assert_eq!(
            events.len(),
            2,
            "failed projection must not roll back raw event"
        );

        let item_state =
            pioneer_entity::turn_event_projection_state::Entity::find_by_id(events[1].id.clone())
                .one(&store.connection)
                .await
                .expect("projection state lookup should succeed")
                .expect("item projection state should exist");
        assert_eq!(
            item_state.status,
            crate::repositories::turn_event_projection_state::PROJECTION_STATUS_FAILED
        );

        let replay = store
            .replay_due_turn_event_projections(timestamp + 10, 10)
            .await
            .expect("projection replay should succeed");
        assert_eq!(replay.claimed, 2);
        assert_eq!(replay.projected, 2);
        assert_eq!(replay.failed, 0);
        assert_eq!(replay.exhausted, 0);

        let states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn_id))
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("projection states should query");
        assert_eq!(states.len(), 2);
        assert!(states.iter().all(|state| {
            state.status
                == crate::repositories::turn_event_projection_state::PROJECTION_STATUS_PROJECTED
        }));

        let attempt = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
            .one(&store.connection)
            .await
            .expect("attempt lookup should succeed")
            .expect("attempt should be projected during replay");
        assert_eq!(
            attempt.lease_expires_at,
            deadlines.lease_expires_at_unix.map(unix_to_datetime)
        );
        assert_eq!(
            attempt.idle_deadline_at,
            deadlines.idle_deadline_at_unix.map(unix_to_datetime)
        );
        assert_eq!(
            attempt.hard_deadline_at,
            deadlines.hard_deadline_at_unix.map(unix_to_datetime)
        );
    }

    #[tokio::test]
    async fn permanent_storage_rejects_tool_payload_policy_shape_violations() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_raw_payload";
        let thread_id = "thr_raw_payload";
        let turn_id = "turn_raw_payload";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let non_shell_shell_storage_item = TurnItem::DynamicToolCall {
            id: "item_shell_storage".to_owned(),
            tool_name: "read_file".to_owned(),
            arguments: serde_json::json!({"path": "/tmp/secret.txt"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Shell {
                stdout: Some("not allowed".to_owned()),
                stderr: None,
                aggregated_output: Some("not allowed".to_owned()),
                exit_code: Some(0),
                duration_ms: None,
                timed_out: None,
                truncated: false,
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: non_shell_shell_storage_item,
                },
                timestamp + 1,
            )
            .await
            .expect_err("non-shell shell storage should be rejected");
        assert!(format!("{error:#}").contains("storage payload must be metadata-only"));

        let metadata_only_summary_storage_item = TurnItem::DynamicToolCall {
            id: "item_summary_storage".to_owned(),
            tool_name: "read_skill".to_owned(),
            arguments: serde_json::json!({"slug": "secret-skill"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_skill"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Summary(pioneer_protocol::ToolOutputSummary {
                title: "read_skill completed".to_owned(),
                lines: vec!["not allowed by metadata-only storage policy".to_owned()],
                metadata: ToolMetadata::empty(),
                truncated: false,
            }),
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: metadata_only_summary_storage_item,
                },
                timestamp + 2,
            )
            .await
            .expect_err("summary storage should be rejected for metadata-only policy");
        assert!(format!("{error:#}").contains("storage payload must be metadata-only"));

        let external_mcp_summary = pioneer_protocol::ToolOutputSummary {
            title: "Tool: mcp:Resend/send-email".to_owned(),
            lines: vec!["Sending email".to_owned()],
            metadata: ToolMetadata::from_json(serde_json::json!({
                "server": "Resend",
                "tool": "send-email",
                "arguments": {
                    "to": "alexander.oskin@gmail.com"
                }
            })),
            truncated: false,
        };
        let external_mcp_item = TurnItem::DynamicToolCall {
            id: "item_external_mcp".to_owned(),
            tool_name: "mcp:Resend/send-email".to_owned(),
            arguments: serde_json::json!({"to": "alexander.oskin@gmail.com"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_external_runtime_tool_name(
                "mcp:Resend/send-email",
            ),
            display: ToolDisplayPayload::Summary(external_mcp_summary.clone()),
            storage: ToolStoragePayload::Metadata {
                metadata: external_mcp_summary.metadata.clone(),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: external_mcp_item,
                },
                timestamp + 3,
            )
            .await
            .expect("external runtime MCP metadata storage should be accepted");

        let retained_llm_context_item = TurnItem::DynamicToolCall {
            id: "item_llm_context".to_owned(),
            tool_name: "read_file".to_owned(),
            arguments: serde_json::json!({"path": "/tmp/secret.txt"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "llmView": {
                        "kind": "json",
                        "value": "not allowed in permanent storage"
                    }
                })),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: retained_llm_context_item,
                },
                timestamp + 4,
            )
            .await
            .expect_err("retained llm context should be rejected");
        assert!(format!("{error:#}").contains("retained llm context key `llmView`"));
    }

    #[tokio::test]
    async fn item_completed_rejects_terminal_tool_payload_still_in_progress() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_terminal_payload";
        let thread_id = "thr_terminal_payload";
        let turn_id = "turn_terminal_payload";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let stuck_tool_item = TurnItem::WebFetch {
            id: "item_stuck_tool".to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({"url": "https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com"
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        };

        let error = store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: stuck_tool_item,
                },
                timestamp + 1,
            )
            .await
            .expect_err("terminal item completion must reject active tool payload");

        assert!(format!("{error:#}").contains("cannot remain in_progress"));
    }

    #[tokio::test]
    async fn timeout_transition_terminalizes_payload_and_attempt_metadata() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_timeout_terminalize";
        let thread_id = "thr_timeout_terminalize";
        let turn_id = "turn_timeout_terminalize";
        let item_id = "item_timeout_terminalize";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item(item_id),
                },
                timestamp + 1,
            )
            .await
            .expect("item start should persist");
        store
            .configure_turn_item_attempt_deadlines(
                turn_id,
                item_id,
                timestamp + 1,
                Some(timestamp + 2),
                Some(timestamp + 2),
                Some(timestamp + 2),
            )
            .await
            .expect("deadlines should be configured");

        let candidates = store
            .list_timeout_candidates(timestamp + 3, 8)
            .await
            .expect("timeout candidate list should succeed");
        assert_eq!(candidates.len(), 1);
        assert!(
            store
                .transition_timeout_candidate(&candidates[0], timestamp + 3)
                .await
                .expect("timeout transition should succeed")
        );

        let row = crate::repositories::turn::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
        assert_eq!(row.status.as_deref(), Some("timed_out"));
        assert_eq!(row.active_attempt_status.as_deref(), Some("timed_out"));

        let payload: TurnItem =
            serde_json::from_str(row.payload.as_str()).expect("payload should decode");
        let TurnItem::WebFetch { status, .. } = payload else {
            panic!("expected web_fetch payload");
        };
        assert_eq!(status, ToolCallStatus::Failed);
    }

    #[tokio::test]
    async fn terminal_turn_projection_closes_running_attempts() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_turn_terminal_cleanup";
        let thread_id = "thr_turn_terminal_cleanup";
        let turn_id = "turn_turn_terminal_cleanup";
        let item_id = "item_turn_terminal_cleanup";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item(item_id),
                },
                timestamp + 1,
            )
            .await
            .expect("item start should persist");

        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::Completed,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                        permission_profile:
                            pioneer_protocol::default_turn_permission_profile_snapshot(),
                    },
                },
                timestamp + 2,
            )
            .await
            .expect("turn completion should persist");

        let running_attempts = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
            .filter(pioneer_entity::turn_item_attempt::Column::Status.eq("running"))
            .all(&store.connection)
            .await
            .expect("running attempt query should succeed");
        assert!(running_attempts.is_empty());

        let row = crate::repositories::turn::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
        assert_eq!(row.status.as_deref(), Some("failed"));
        assert_eq!(row.active_attempt_status.as_deref(), Some("interrupted"));

        let payload: TurnItem =
            serde_json::from_str(row.payload.as_str()).expect("payload should decode");
        let TurnItem::WebFetch { status, .. } = payload else {
            panic!("expected web_fetch payload");
        };
        assert_eq!(status, ToolCallStatus::Failed);
    }

    #[tokio::test]
    async fn read_model_invariant_verifier_detects_and_repairs_terminal_tool_payload() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_invariant_repair";
        let thread_id = "thr_invariant_repair";
        let turn_id = "turn_invariant_repair";
        let item_id = "item_invariant_repair";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item(item_id),
                },
                timestamp + 1,
            )
            .await
            .expect("item start should persist");

        pioneer_entity::turn_item::Entity::update_many()
            .filter(pioneer_entity::turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(pioneer_entity::turn_item::Column::ItemId.eq(item_id.to_owned()))
            .col_expr(
                pioneer_entity::turn_item::Column::Status,
                sea_orm::sea_query::Expr::value(Some("completed")),
            )
            .exec(&store.connection)
            .await
            .expect("status mutation should succeed");

        let violations = store
            .list_read_model_invariant_violations()
            .await
            .expect("invariant list should succeed");
        assert!(violations.iter().any(|violation| {
            matches!(
                violation.kind,
                super::ReadModelInvariantKind::TerminalToolPayloadInProgress
            )
        }));

        let summary = store
            .repair_deterministic_read_model_violations()
            .await
            .expect("repair should succeed");
        assert!(summary.detected >= 1);
        assert_eq!(summary.remaining, 0);

        let row = crate::repositories::turn::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
        let payload: TurnItem =
            serde_json::from_str(row.payload.as_str()).expect("payload should decode");
        let TurnItem::WebFetch { status, .. } = payload else {
            panic!("expected web_fetch payload");
        };
        assert_ne!(status, ToolCallStatus::InProgress);
    }

    #[tokio::test]
    async fn thread_history_never_exposes_retained_turn_llm_context() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_history_llm_context";
        let thread_id = "thr_history_llm_context";
        let turn_id = "turn_history_llm_context";
        let item_id = "item_history_llm_context";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let mut completed_item = safe_web_fetch_item(item_id);
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                timestamp + 1,
            )
            .await
            .expect("safe completed item should persist");

        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: turn_id.to_owned(),
                item_id: Some(item_id.to_owned()),
                attempt_id: Some("1".to_owned()),
                sequence: 1,
                source: "tool_result".to_owned(),
                tool_name: Some("web_fetch".to_owned()),
                payload: serde_json::json!({
                    "output": "SECRET_WEB_FETCH_BODY_SENTINEL"
                })
                .to_string(),
                output_policy_snapshot: serde_json::json!(ToolOutputPolicySnapshot::for_tool_name(
                    "web_fetch"
                ))
                .to_string(),
                created_at: unix_to_datetime(timestamp + 2),
                expires_at: None,
            })
            .await
            .expect("retained llm context should persist");

        let history = store
            .get_thread_history(thread_id, Some(16))
            .await
            .expect("thread history query should succeed")
            .expect("thread history should exist");
        let history_json = serde_json::to_string(&history.events)
            .expect("thread history should serialize for leakage assertion");
        assert!(!history_json.contains("SECRET_WEB_FETCH_BODY_SENTINEL"));
    }

    #[tokio::test]
    async fn migration_creates_turn_llm_context_table_indexes_and_entity_columns() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let table = connection
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'turn_llm_context'"
                    .to_owned(),
            ))
            .await
            .expect("table lookup should succeed");
        assert!(table.is_some(), "turn_llm_context table should exist");

        let columns = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA table_info('turn_llm_context')".to_owned(),
            ))
            .await
            .expect("column lookup should succeed")
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").expect("column name"))
            .collect::<Vec<_>>();
        for expected in [
            "id",
            "turn_id",
            "item_id",
            "attempt_id",
            "sequence",
            "source",
            "tool_name",
            "payload",
            "output_policy_snapshot",
            "created_at",
            "expires_at",
        ] {
            assert!(
                columns.iter().any(|column| column == expected),
                "missing turn_llm_context column {expected}"
            );
        }

        let index_rows = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA index_list('turn_llm_context')".to_owned(),
            ))
            .await
            .expect("index lookup should succeed");
        let indexes = index_rows
            .iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "name").expect("index name"),
                    row.try_get::<i64>("", "unique").expect("index unique flag"),
                )
            })
            .collect::<Vec<_>>();
        for expected in [
            "idx_turn_llm_context_turn_id",
            "uq_turn_llm_context_turn_id_sequence",
            "idx_turn_llm_context_turn_item",
            "idx_turn_llm_context_expires_at",
        ] {
            assert!(
                indexes.iter().any(|(name, _)| name == expected),
                "missing turn_llm_context index {expected}"
            );
        }
        assert!(
            indexes.iter().any(
                |(name, unique)| name == "uq_turn_llm_context_turn_id_sequence" && *unique == 1
            ),
            "turn_id/sequence index should be unique"
        );

        let rows = pioneer_entity::turn_llm_context::Entity::find()
            .all(&connection)
            .await
            .expect("turn_llm_context entity should match migration columns");
        assert!(rows.is_empty());

        Migrator::down(&connection, None)
            .await
            .expect("migration down should succeed");
        let table_after_down = connection
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'turn_llm_context'"
                    .to_owned(),
            ))
            .await
            .expect("table lookup after down should succeed");
        assert!(
            table_after_down.is_none(),
            "turn_llm_context table should be dropped by down migration"
        );
    }

    #[tokio::test]
    async fn migration_creates_execution_window_tables_indexes_and_entity_columns() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        for table_name in ["turn_execution_window", "turn_execution_checkpoint"] {
            let table = connection
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name}'"
                    ),
                ))
                .await
                .expect("table lookup should succeed");
            assert!(table.is_some(), "{table_name} table should exist");
        }

        let window_columns = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA table_info('turn_execution_window')".to_owned(),
            ))
            .await
            .expect("window column lookup should succeed")
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").expect("column name"))
            .collect::<Vec<_>>();
        for expected in [
            "id",
            "workspace_id",
            "thread_id",
            "turn_id",
            "window_index",
            "status",
            "exhaustion_reason",
            "agent_round_count",
            "tool_call_count",
            "provider_token_count",
            "metadata_json",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
        ] {
            assert!(
                window_columns.iter().any(|column| column == expected),
                "missing turn_execution_window column {expected}"
            );
        }

        let checkpoint_columns = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA table_info('turn_execution_checkpoint')".to_owned(),
            ))
            .await
            .expect("checkpoint column lookup should succeed")
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").expect("column name"))
            .collect::<Vec<_>>();
        for expected in [
            "id",
            "window_id",
            "workspace_id",
            "thread_id",
            "turn_id",
            "checkpoint_kind",
            "payload_json",
            "created_at",
        ] {
            assert!(
                checkpoint_columns.iter().any(|column| column == expected),
                "missing turn_execution_checkpoint column {expected}"
            );
        }

        let window_indexes = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA index_list('turn_execution_window')".to_owned(),
            ))
            .await
            .expect("window index lookup should succeed")
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "name").expect("index name"),
                    row.try_get::<i64>("", "unique").expect("index unique flag"),
                )
            })
            .collect::<Vec<_>>();
        for expected in [
            "uidx_turn_execution_window_turn_index",
            "idx_turn_execution_window_turn_id",
            "idx_turn_execution_window_thread_turn",
            "idx_turn_execution_window_status",
        ] {
            assert!(
                window_indexes.iter().any(|(name, _)| name == expected),
                "missing turn_execution_window index {expected}"
            );
        }
        assert!(
            window_indexes.iter().any(|(name, unique)| name
                == "uidx_turn_execution_window_turn_index"
                && *unique == 1),
            "turn_id/window_index index should be unique"
        );

        let checkpoint_indexes = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA index_list('turn_execution_checkpoint')".to_owned(),
            ))
            .await
            .expect("checkpoint index lookup should succeed")
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").expect("index name"))
            .collect::<Vec<_>>();
        for expected in [
            "idx_turn_execution_checkpoint_window",
            "idx_turn_execution_checkpoint_turn",
            "idx_turn_execution_checkpoint_thread_turn",
            "idx_turn_execution_checkpoint_kind",
        ] {
            assert!(
                checkpoint_indexes.iter().any(|name| name == expected),
                "missing turn_execution_checkpoint index {expected}"
            );
        }

        assert!(
            pioneer_entity::turn_execution_window::Entity::find()
                .all(&connection)
                .await
                .expect("window entity should match migration columns")
                .is_empty()
        );
        assert!(
            pioneer_entity::turn_execution_checkpoint::Entity::find()
                .all(&connection)
                .await
                .expect("checkpoint entity should match migration columns")
                .is_empty()
        );

        Migrator::down(&connection, None)
            .await
            .expect("migration down should succeed");
        for table_name in ["turn_execution_window", "turn_execution_checkpoint"] {
            let table = connection
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name}'"
                    ),
                ))
                .await
                .expect("table lookup after down should succeed");
            assert!(table.is_none(), "{table_name} table should be dropped");
        }
    }

    #[tokio::test]
    async fn execution_window_repository_round_trips_lifecycle_checkpoints_and_cleanup() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let store = CrudStore::new(connection);
        let timestamp = unix_to_datetime(1_700_000_000);

        let first_window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_window".to_owned(),
                    thread_id: "thr_window".to_owned(),
                    turn_id: "turn_window".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({ "phase": "start" }),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("first window should insert");
        assert_eq!(first_window.window_index, 1);

        let duplicate = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_window".to_owned(),
                    thread_id: "thr_window".to_owned(),
                    turn_id: "turn_window".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await;
        assert!(
            duplicate.is_err(),
            "duplicate window index should be rejected before insert"
        );

        let exhausted = store
            .mark_turn_execution_window_exhausted(
                first_window.id.as_str(),
                ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 7,
                    tool_call_count: 11,
                    provider_token_count: 13,
                    metadata_json: serde_json::json!({ "phase": "exhausted" }),
                    completed_at: timestamp + chrono::Duration::seconds(10),
                    updated_at: timestamp + chrono::Duration::seconds(10),
                },
            )
            .await
            .expect("window should mark exhausted");
        assert_eq!(exhausted.status, ExecutionWindowStatus::Exhausted);
        assert_eq!(
            exhausted.exhaustion_reason,
            Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow)
        );

        let checkpoint = store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                window_id: first_window.id.clone(),
                workspace_id: first_window.workspace_id.clone(),
                thread_id: first_window.thread_id.clone(),
                turn_id: first_window.turn_id.clone(),
                checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                payload_json: serde_json::json!({
                    "turn_id": first_window.turn_id.clone(),
                    "window_index": first_window.window_index,
                    "summary": "bounded runtime facts only"
                }),
                created_at: timestamp + chrono::Duration::seconds(11),
            })
            .await
            .expect("checkpoint should save");
        assert_eq!(
            checkpoint.checkpoint_kind,
            TurnExecutionCheckpointKind::WindowExhausted
        );

        assert_eq!(
            store
                .list_turn_execution_checkpoints_for_window(first_window.id.as_str())
                .await
                .expect("window checkpoints should list")
                .len(),
            1
        );
        assert_eq!(
            store
                .latest_turn_execution_checkpoint_for_turn("turn_window")
                .await
                .expect("latest checkpoint should load")
                .expect("latest checkpoint should exist")
                .id,
            checkpoint.id
        );

        let oversized = serde_json::json!({
            "payload": "x".repeat(super::TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES + 1)
        });
        let oversized_result = store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                window_id: first_window.id.clone(),
                workspace_id: first_window.workspace_id.clone(),
                thread_id: first_window.thread_id.clone(),
                turn_id: first_window.turn_id.clone(),
                checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                payload_json: oversized,
                created_at: timestamp,
            })
            .await;
        assert!(
            oversized_result.is_err(),
            "oversized checkpoint payload should be rejected"
        );

        store
            .mark_turn_execution_window_checkpointed(
                first_window.id.as_str(),
                timestamp + chrono::Duration::seconds(12),
            )
            .await
            .expect("window should mark checkpointed");
        store
            .mark_turn_execution_window_continued(
                first_window.id.as_str(),
                timestamp + chrono::Duration::seconds(13),
            )
            .await
            .expect("window should mark continued");

        let second_window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_window".to_owned(),
                    thread_id: "thr_window".to_owned(),
                    turn_id: "turn_window".to_owned(),
                    window_index: 2,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp + chrono::Duration::seconds(14),
                },
                timestamp + chrono::Duration::seconds(14),
                timestamp + chrono::Duration::seconds(14),
            )
            .await
            .expect("second monotonic window should insert");
        let completed = store
            .mark_turn_execution_window_completed(
                second_window.id.as_str(),
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 3,
                    tool_call_count: 5,
                    provider_token_count: 8,
                    metadata_json: serde_json::json!({ "phase": "completed" }),
                    completed_at: timestamp + chrono::Duration::seconds(15),
                    updated_at: timestamp + chrono::Duration::seconds(15),
                },
            )
            .await
            .expect("window should mark completed");
        assert_eq!(completed.status, ExecutionWindowStatus::Completed);
        assert_eq!(completed.exhaustion_reason, None);

        let blocked = store
            .mark_turn_execution_window_blocked(
                second_window.id.as_str(),
                Some(ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow),
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 3,
                    tool_call_count: 5,
                    provider_token_count: 8,
                    metadata_json: serde_json::json!({ "phase": "blocked" }),
                    completed_at: timestamp + chrono::Duration::seconds(16),
                    updated_at: timestamp + chrono::Duration::seconds(16),
                },
            )
            .await
            .expect("window should mark blocked");
        assert_eq!(blocked.status, ExecutionWindowStatus::Blocked);
        assert_eq!(
            blocked.exhaustion_reason,
            Some(ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow)
        );
        assert_eq!(
            store
                .latest_turn_execution_window("turn_window")
                .await
                .expect("latest window should load")
                .expect("latest window should exist")
                .id,
            second_window.id
        );

        let aggregate = store
            .aggregate_turn_execution_window_usage("turn_window")
            .await
            .expect("execution window usage should aggregate");
        assert_eq!(
            aggregate,
            TurnExecutionWindowUsageAggregateRecord {
                total_windows: 2,
                total_agent_rounds: 10,
                total_tool_calls: 16,
                total_wall_clock_ms: 12_000,
                wall_clock_window_count: 2,
                total_provider_tokens: 21,
            }
        );

        store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_window".to_owned(),
                    thread_id: "thr_window".to_owned(),
                    turn_id: "turn_window_missing_tokens".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 2,
                    tool_call_count: 4,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({ "phase": "running_without_tokens" }),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("window with missing token usage should insert");
        let missing_token_aggregate = store
            .aggregate_turn_execution_window_usage("turn_window_missing_tokens")
            .await
            .expect("missing token usage should not block aggregate counters");
        assert_eq!(
            missing_token_aggregate,
            TurnExecutionWindowUsageAggregateRecord {
                total_windows: 1,
                total_agent_rounds: 2,
                total_tool_calls: 4,
                total_wall_clock_ms: 0,
                wall_clock_window_count: 0,
                total_provider_tokens: 0,
            }
        );

        let cleanup = store
            .delete_turn_execution_data_for_turn("turn_window")
            .await
            .expect("execution data cleanup should succeed");
        assert_eq!(cleanup.checkpoints_deleted, 1);
        assert_eq!(cleanup.windows_deleted, 2);
        assert!(
            store
                .list_turn_execution_windows("turn_window")
                .await
                .expect("windows should list after cleanup")
                .is_empty()
        );
        assert!(
            store
                .latest_turn_execution_checkpoint_for_turn("turn_window")
                .await
                .expect("latest checkpoint should load after cleanup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn migration_creates_mcp_tables_indexes_and_down_drops_them() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        for table_name in [
            "mcp_server_installation",
            "mcp_server_catalog_snapshot",
            "mcp_audit_event",
            "turn_mcp_binding",
        ] {
            let table = connection
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name}'"
                    ),
                ))
                .await
                .expect("table lookup should succeed");
            assert!(table.is_some(), "{table_name} table should exist");
        }

        let indexes = connection
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA index_list('mcp_server_installation')".to_owned(),
            ))
            .await
            .expect("index lookup should succeed")
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "name").expect("index name"),
                    row.try_get::<i64>("", "unique").expect("index unique flag"),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            indexes.iter().any(
                |(name, unique)| name == "uq_mcp_server_installation_scope_name" && *unique == 1
            ),
            "MCP installation scope/name index should be unique"
        );

        connection
            .execute_unprepared(
                r#"
                INSERT INTO mcp_server_installation (
                    id, scope_kind, scope_key, name, source_kind, source_ref,
                    transport_kind, transport_json, auth_json, secret_refs_json,
                    enabled, allow_implicit_invocation, required, fingerprint
                ) VALUES (
                    'mcp_installation_one',
                    'workspace',
                    'ws_mcp',
                    'resend',
                    'config',
                    '{}',
                    'stdio',
                    '{}',
                    '{}',
                    '[]',
                    1,
                    1,
                    0,
                    'fingerprint-one'
                )
                "#,
            )
            .await
            .expect("first MCP installation insert should succeed");
        let duplicate = connection
            .execute_unprepared(
                r#"
                INSERT INTO mcp_server_installation (
                    id, scope_kind, scope_key, name, source_kind, source_ref,
                    transport_kind, transport_json, auth_json, secret_refs_json,
                    enabled, allow_implicit_invocation, required, fingerprint
                ) VALUES (
                    'mcp_installation_two',
                    'workspace',
                    'ws_mcp',
                    'resend',
                    'config',
                    '{}',
                    'stdio',
                    '{}',
                    '{}',
                    '[]',
                    1,
                    1,
                    0,
                    'fingerprint-two'
                )
                "#,
            )
            .await;
        assert!(
            duplicate.is_err(),
            "unique scope/name index should reject duplicate MCP installations"
        );

        Migrator::down(&connection, None)
            .await
            .expect("migration down should succeed");
        for table_name in [
            "turn_mcp_binding",
            "mcp_audit_event",
            "mcp_server_catalog_snapshot",
            "mcp_server_installation",
        ] {
            let table = connection
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name}'"
                    ),
                ))
                .await
                .expect("table lookup after down should succeed");
            assert!(table.is_none(), "{table_name} table should be dropped");
        }
    }

    #[tokio::test]
    async fn turn_llm_context_repository_round_trips_and_cleans_terminal_turns() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_llm_context";
        let thread_id = "thr_llm_context";
        let active_turn_id = "turn_llm_active";
        let terminal_turn_id = "turn_llm_terminal";
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };

        for turn_id in [active_turn_id, terminal_turn_id] {
            let turn = Turn {
                id: turn_id.to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: Default::default(),
                origin: Default::default(),
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            };
            store
                .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
                .await
                .expect("turn start should persist");
        }

        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: terminal_turn_id.to_owned(),
                        status: TurnStatus::Completed,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                        permission_profile:
                            pioneer_protocol::default_turn_permission_profile_snapshot(),
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("terminal turn should persist");

        let read_file_policy =
            serde_json::to_string(&ToolOutputPolicySnapshot::for_tool_name("read_file"))
                .expect("policy should serialize");
        let shell_policy =
            serde_json::to_string(&ToolOutputPolicySnapshot::for_tool_name("exec_command"))
                .expect("policy should serialize");
        let future_expiry = chrono::Utc::now().fixed_offset() + chrono::Duration::days(1);
        let expired_expiry = chrono::Utc::now().fixed_offset() - chrono::Duration::days(1);

        for (turn_id, sequence, tool_name, payload, policy) in [
            (
                active_turn_id,
                2,
                "read_file",
                r#"{"kind":"text","text":"later"}"#,
                read_file_policy.as_str(),
            ),
            (
                active_turn_id,
                1,
                "exec_command",
                r#"{"kind":"text","text":"earlier"}"#,
                shell_policy.as_str(),
            ),
            (
                terminal_turn_id,
                1,
                "web_fetch",
                r#"{"kind":"text","text":"temporary"}"#,
                read_file_policy.as_str(),
            ),
        ] {
            store
                .insert_turn_llm_context(NewTurnLlmContextEntry {
                    turn_id: turn_id.to_owned(),
                    item_id: Some(format!("item_{sequence}")),
                    attempt_id: Some(format!("attempt_{sequence}")),
                    sequence,
                    source: "tool_result".to_owned(),
                    tool_name: Some(tool_name.to_owned()),
                    payload: payload.to_owned(),
                    output_policy_snapshot: policy.to_owned(),
                    created_at: unix_to_datetime(timestamp + sequence),
                    expires_at: Some(future_expiry),
                })
                .await
                .expect("llm context row should insert");
        }
        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: active_turn_id.to_owned(),
                item_id: Some("item_expired".to_owned()),
                attempt_id: Some("attempt_expired".to_owned()),
                sequence: 3,
                source: "tool_result".to_owned(),
                tool_name: Some("read_file".to_owned()),
                payload: r#"{"kind":"text","text":"expired"}"#.to_owned(),
                output_policy_snapshot: read_file_policy.clone(),
                created_at: unix_to_datetime(timestamp + 3),
                expires_at: Some(expired_expiry),
            })
            .await
            .expect("expired llm context row should insert");

        let active_rows = store
            .list_turn_llm_context(active_turn_id)
            .await
            .expect("active context should list");
        assert_eq!(
            active_rows
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(active_rows[0].tool_name.as_deref(), Some("exec_command"));
        assert_eq!(active_rows[1].tool_name.as_deref(), Some("read_file"));

        let deleted_expired = store
            .delete_expired_turn_llm_context()
            .await
            .expect("expired cleanup should succeed");
        assert_eq!(deleted_expired, 1);
        assert_eq!(
            store
                .list_turn_llm_context(active_turn_id)
                .await
                .expect("active context should survive expired cleanup")
                .len(),
            2
        );

        let deleted_terminal = store
            .delete_turn_llm_context_for_terminal_turns()
            .await
            .expect("terminal cleanup should succeed");
        assert_eq!(deleted_terminal, 1);
        assert!(
            store
                .list_turn_llm_context(terminal_turn_id)
                .await
                .expect("terminal context should list")
                .is_empty()
        );
        assert_eq!(
            store
                .list_turn_llm_context(active_turn_id)
                .await
                .expect("active context should survive terminal cleanup")
                .len(),
            2
        );

        let deleted_active = store
            .delete_turn_llm_context_for_turn(active_turn_id)
            .await
            .expect("turn cleanup should succeed");
        assert_eq!(deleted_active, 2);
        assert!(
            store
                .list_turn_llm_context(active_turn_id)
                .await
                .expect("active context should list after delete")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn recovery_lifecycle_events_round_trip_through_turn_history() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_000000000000000099";
        let thread_id = "thr_000000000000000099";
        let turn_id = "turn_000000000000000099";
        let item_id = "reasoning_recovery";
        let recovery_job_id = "recovery_job_99";

        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");
        store
            .materialize_item_timeout_detected(
                ItemTimeoutDetectedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    attempt_number: 1,
                    reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                    recovery_job_id: Some(recovery_job_id.to_owned()),
                },
                timestamp + 1,
            )
            .await
            .expect("timeout detected should persist");
        store
            .materialize_item_recovery_opened(
                ItemRecoveryOpenedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    trigger: RecoveryTrigger::Timeout,
                    action: RecoveryAction::RetryAttempt,
                    attempt_number: 1,
                },
                timestamp + 2,
            )
            .await
            .expect("recovery opened should persist");
        store
            .materialize_item_recovery_attached(
                ItemRecoveryAttachedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_second_failure".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    recovery_item_id: item_id.to_owned(),
                    recovery_item_type: TurnItemType::Reasoning,
                    trigger: RecoveryTrigger::ProviderError,
                    action: RecoveryAction::RetryAttempt,
                    existing_status: RecoveryJobStatus::Pending,
                    next_attempt_number: 1,
                },
                timestamp + 3,
            )
            .await
            .expect("recovery attached should persist");
        store
            .materialize_item_retry_scheduled(
                ItemRetryScheduledNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                    next_run_at_unix: timestamp + 30,
                    reason: Some("retry later".to_owned()),
                },
                timestamp + 4,
            )
            .await
            .expect("retry scheduled should persist");
        store
            .materialize_item_retry_attempt_started(
                ItemRetryAttemptStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                },
                timestamp + 5,
            )
            .await
            .expect("retry attempt started should persist");
        store
            .materialize_item_recovery_succeeded(
                ItemRecoverySucceededNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                },
                timestamp + 6,
            )
            .await
            .expect("recovery succeeded should persist");
        store
            .materialize_item_recovery_exhausted(
                ItemRecoveryExhaustedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 3,
                    status: RecoveryJobStatus::Exhausted,
                    error_message: "attempts exhausted".to_owned(),
                },
                timestamp + 7,
            )
            .await
            .expect("recovery exhausted should persist");

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn items should exist");
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemTimeoutDetected { recovery_job_id: Some(job_id), .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryOpened { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryAttached { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRetryScheduled { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRetryAttemptStarted { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoverySucceeded { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryExhausted { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));

        let history = store
            .get_thread_history(thread_id, Some(32))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryOpened { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryAttached { recovery_job_id: job_id, recovery_item_id, .. }
                if job_id == recovery_job_id && recovery_item_id == item_id
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRetryScheduled { recovery_job_id: job_id, next_run_at_unix, .. }
                if job_id == recovery_job_id && *next_run_at_unix == timestamp + 30
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRetryAttemptStarted { recovery_job_id: job_id, attempt_number, .. }
                if job_id == recovery_job_id && *attempt_number == 2
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoverySucceeded { recovery_job_id: job_id, attempt_number, .. }
                if job_id == recovery_job_id && *attempt_number == 2
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryExhausted { recovery_job_id: job_id, status, error_message, .. }
                if job_id == recovery_job_id
                    && *status == RecoveryJobStatus::Exhausted
                    && error_message == "attempts exhausted"
        )));
    }

    #[tokio::test]
    async fn tool_retry_lifecycle_events_round_trip_without_recovery_jobs() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_100_000;
        let workspace_id = "ws_000000000000000199";
        let thread_id = "thr_000000000000000199";
        let turn_id = "turn_000000000000000199";
        let item_id = "item_tool_retry";
        let episode_id = "tool_retry_turn_199_1";

        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let budgets = vec![ToolRetryBudgetUsage {
            kind: ToolRetryBudgetKind::Episode,
            used: 1,
            limit: 2,
        }];

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");
        store
            .materialize_item_tool_retry_scheduled(
                ItemToolRetryScheduledNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 1,
                    error_class: ToolRetryErrorClass::Timeout,
                    retry_hint: "retry with a smaller request".to_owned(),
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: "sig_timeout".to_owned(),
                    reason: "recoverable_tool_output".to_owned(),
                },
                timestamp + 1,
            )
            .await
            .expect("tool retry scheduled should persist");
        store
            .materialize_item_tool_retry_resolved(
                ItemToolRetryResolvedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 2,
                    resolution: ToolRetryResolution::Succeeded,
                    budgets: budgets.clone(),
                    reason: "successful_tool_output".to_owned(),
                },
                timestamp + 2,
            )
            .await
            .expect("tool retry resolved should persist");
        store
            .materialize_item_tool_retry_exhausted(
                ItemToolRetryExhaustedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 3,
                    error_class: ToolRetryErrorClass::Timeout,
                    exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: "sig_timeout".to_owned(),
                    reason: "same_failure_signature".to_owned(),
                },
                timestamp + 3,
            )
            .await
            .expect("tool retry exhausted should persist");
        store
            .materialize_turn_tool_loop_budget_exceeded(
                TurnToolLoopBudgetExceededNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                    limit: 32,
                    observed: 33,
                    action: ToolLoopBudgetAction::ContinueInNextWindow,
                    reason: "agent_rounds_exceeded".to_owned(),
                },
                timestamp + 4,
            )
            .await
            .expect("tool loop budget event should persist");

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn items should exist");
        let payloads = turn_items
            .events
            .iter()
            .map(|event| &event.payload)
            .collect::<Vec<_>>();
        assert!(matches!(
            payloads[0],
            TurnItemEventPayload::ItemToolRetryScheduled {
                tool_retry_episode_id,
                error_class: ToolRetryErrorClass::Timeout,
                ..
            } if tool_retry_episode_id == episode_id
        ));
        assert!(matches!(
            payloads[1],
            TurnItemEventPayload::ItemToolRetryResolved {
                resolution: ToolRetryResolution::Succeeded,
                ..
            }
        ));
        assert!(matches!(
            payloads[2],
            TurnItemEventPayload::ItemToolRetryExhausted {
                exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
                ..
            }
        ));
        assert!(matches!(
            payloads[3],
            TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                action: ToolLoopBudgetAction::ContinueInNextWindow,
                ..
            }
        ));

        let history = store
            .get_thread_history(thread_id, Some(16))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(
            history
                .events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryScheduled {
                tool_retry_episode_id,
                budgets,
                ..
            } if tool_retry_episode_id == episode_id
                && budgets.first().is_some_and(|budget| budget.kind == ToolRetryBudgetKind::Episode)
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryResolved {
                resolution: ToolRetryResolution::Succeeded,
                ..
            }
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryExhausted {
                failure_signature_fingerprint,
                ..
            } if failure_signature_fingerprint == "sig_timeout"
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                observed: 33,
                ..
            }
        )));

        let recovery_jobs = pioneer_entity::recovery_job::Entity::find()
            .all(&connection)
            .await
            .expect("recovery job query should succeed");
        assert!(
            recovery_jobs.is_empty(),
            "tool retry lifecycle must not create recovery jobs"
        );
    }

    #[tokio::test]
    async fn replace_and_find_turn_skill_bindings_round_trip() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let turn_id = "turn_000000000000000001";

        let first = vec![
            TurnSkillBindingRecord {
                skill_slug: "pioneer/alpha-skill".to_owned(),
                skill_version: Some("1.0.0".to_owned()),
                fingerprint: "fp-alpha".to_owned(),
                source_kind: "registry".to_owned(),
                resolved_reason: "explicit_composer_capability".to_owned(),
            },
            TurnSkillBindingRecord {
                skill_slug: "pioneer/beta-skill".to_owned(),
                skill_version: None,
                fingerprint: "fp-beta".to_owned(),
                source_kind: "user".to_owned(),
                resolved_reason: "path_match".to_owned(),
            },
        ];

        store
            .replace_turn_skill_bindings(turn_id, first.as_slice(), 1_700_000_000)
            .await
            .expect("initial turn skill bindings should persist");

        let first_read = store
            .find_turn_skill_bindings(turn_id)
            .await
            .expect("must read persisted turn skill bindings");
        assert_eq!(first_read, first);

        let second = vec![TurnSkillBindingRecord {
            skill_slug: "pioneer/gamma-skill".to_owned(),
            skill_version: Some("2.1.0".to_owned()),
            fingerprint: "fp-gamma".to_owned(),
            source_kind: "system".to_owned(),
            resolved_reason: "explicit_composer_capability".to_owned(),
        }];

        store
            .replace_turn_skill_bindings(turn_id, second.as_slice(), 1_700_000_100)
            .await
            .expect("second replacement should overwrite prior bindings");

        let second_read = store
            .find_turn_skill_bindings(turn_id)
            .await
            .expect("must read replaced turn skill bindings");
        assert_eq!(second_read, second);
    }

    #[tokio::test]
    async fn skill_installation_upsert_is_unique_by_slug_source_and_scope() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);

        let first = SkillInstallationRecord {
            slug: "pioneer/agent-browser".to_owned(),
            version: Some("1.0.0".to_owned()),
            source_kind: "registry".to_owned(),
            scope_key: "ws_one".to_owned(),
            source_ref: "github.com/example/agent-browser".to_owned(),
            install_path: "/tmp/skills/pioneer/agent-browser".to_owned(),
            trust_level: "verified".to_owned(),
            fingerprint: "fp-1".to_owned(),
            updated_at_unix: 1_700_000_000,
        };
        store
            .upsert_skill_installation(&first, 1_700_000_000)
            .await
            .expect("first upsert");

        let second = SkillInstallationRecord {
            fingerprint: "fp-2".to_owned(),
            version: Some("1.1.0".to_owned()),
            ..first.clone()
        };
        store
            .upsert_skill_installation(&second, 1_700_000_100)
            .await
            .expect("second upsert");

        let rows = store
            .list_skill_installations()
            .await
            .expect("list skill installations");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fingerprint, "fp-2");
        assert_eq!(rows[0].version.as_deref(), Some("1.1.0"));

        let scoped = SkillInstallationRecord {
            scope_key: "ws_two".to_owned(),
            fingerprint: "fp-3".to_owned(),
            ..first.clone()
        };
        store
            .upsert_skill_installation(&scoped, 1_700_000_200)
            .await
            .expect("scoped upsert");

        let rows = store
            .list_skill_installations()
            .await
            .expect("list scoped skill installations");
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn mcp_repositories_round_trip_installation_audit_catalog_and_bindings() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);

        let beta = sample_mcp_installation("beta");
        let beta_id = store
            .upsert_mcp_server_installation(&beta, 1_700_000_000)
            .await
            .expect("first MCP installation upsert should succeed");
        let mut alpha = sample_mcp_installation("alpha");
        alpha.enabled = false;
        alpha.allow_implicit_invocation = false;
        store
            .upsert_mcp_server_installation(&alpha, 1_700_000_001)
            .await
            .expect("second MCP installation upsert should succeed");

        let mut beta_updated = beta.clone();
        beta_updated.fingerprint = "fingerprint-beta-updated".to_owned();
        beta_updated.required = true;
        let beta_updated_id = store
            .upsert_mcp_server_installation(&beta_updated, 1_700_000_002)
            .await
            .expect("MCP installation update should succeed");
        assert_eq!(beta_id, beta_updated_id);

        let rows = store
            .list_mcp_server_installations("workspace", "ws_mcp")
            .await
            .expect("MCP installations list should succeed");
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert!(!rows[0].enabled);
        assert!(!rows[0].allow_implicit_invocation);
        let beta_row = store
            .find_mcp_server_installation("workspace", "ws_mcp", "beta")
            .await
            .expect("MCP installation find should succeed")
            .expect("beta MCP installation should exist");
        assert_eq!(beta_row.fingerprint, "fingerprint-beta-updated");
        assert!(beta_row.required);

        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: Some(beta_updated_id.clone()),
            server_name: "beta".to_owned(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: "install".to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: "{\"transport_kind\":\"stdio\"}".to_owned(),
            created_at_unix: 1_700_000_003,
        };
        store
            .insert_mcp_audit_event_record(&audit)
            .await
            .expect("MCP audit insert should succeed");
        let audit_rows = store
            .list_recent_mcp_audit_event_records("beta", 10)
            .await
            .expect("MCP audit list should succeed");
        assert_eq!(audit_rows.len(), 1);
        assert_eq!(audit_rows[0].action, "install");

        let catalog = McpServerCatalogSnapshotRecord {
            server_installation_id: beta_updated_id.clone(),
            catalog_version: "catalog-v1".to_owned(),
            server_info_json: "{\"name\":\"beta\"}".to_owned(),
            server_instructions_hash: Some("instructions-hash".to_owned()),
            tools_json: "[{\"name\":\"send\"}]".to_owned(),
            resources_json: "[]".to_owned(),
            resource_templates_json: "[]".to_owned(),
            prompts_json: "[]".to_owned(),
            generated_at_unix: 1_700_000_004,
        };
        store
            .upsert_mcp_server_catalog_snapshot(&catalog, 1_700_000_004)
            .await
            .expect("MCP catalog snapshot upsert should succeed");
        let read_catalog = store
            .find_mcp_server_catalog_snapshot(beta_updated_id.as_str())
            .await
            .expect("MCP catalog snapshot find should succeed")
            .expect("MCP catalog snapshot should exist");
        assert_eq!(read_catalog.catalog_version, "catalog-v1");
        assert_eq!(read_catalog.tools_json, "[{\"name\":\"send\"}]");

        let turn_bindings = vec![TurnMcpBindingRecord {
            server_installation_id: beta_updated_id.clone(),
            server_name: "beta".to_owned(),
            raw_tool_name: "send".to_owned(),
            callable_name: "mcp_beta_send".to_owned(),
            catalog_version: "catalog-v1".to_owned(),
            fingerprint: "fingerprint-beta-updated".to_owned(),
            selection_reason: "explicit_composer_capability".to_owned(),
            capability_id: Some("mcp:workspace:beta:send".to_owned()),
        }];
        store
            .replace_turn_mcp_bindings("turn_mcp_roundtrip", &turn_bindings, 1_700_000_005)
            .await
            .expect("MCP turn bindings replace should succeed");
        let read_bindings = store
            .list_turn_mcp_bindings("turn_mcp_roundtrip")
            .await
            .expect("MCP turn bindings list should succeed");
        assert_eq!(read_bindings, turn_bindings);

        store
            .delete_mcp_server_installation("workspace", "ws_mcp", "alpha")
            .await
            .expect("MCP installation delete should succeed");
        let rows = store
            .list_mcp_server_installations("workspace", "ws_mcp")
            .await
            .expect("MCP installations list after delete should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "beta");
    }

    #[tokio::test]
    async fn skill_audit_event_persistence_orders_by_created_at() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let turn_id = "turn_000000000000000077";

        let older = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_slug: "pioneer/agent-browser".to_owned(),
            source_kind: "registry".to_owned(),
            action: "resolve_blocked".to_owned(),
            decision: "blocked".to_owned(),
            reason_code: Some("dependency_missing".to_owned()),
            details_json: "{\"reason\":\"dependency_missing\"}".to_owned(),
            created_at_unix: 1_700_000_000,
        };
        let newer = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_slug: "pioneer/agent-browser".to_owned(),
            source_kind: "registry".to_owned(),
            action: "runtime_blocked".to_owned(),
            decision: "blocked".to_owned(),
            reason_code: Some("runtime.dependency_missing".to_owned()),
            details_json: "{\"reason\":\"runtime.dependency_missing\"}".to_owned(),
            created_at_unix: 1_700_000_100,
        };

        store
            .append_skill_audit_event_records(turn_id, &[older.clone(), newer.clone()])
            .await
            .expect("append audit events");

        let rows = store
            .list_turn_skill_audit_event_records(turn_id)
            .await
            .expect("list turn audit events");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].action, older.action);
        assert_eq!(rows[1].action, newer.action);

        let timeline = store
            .list_skill_audit_event_records("pioneer/agent-browser", 16)
            .await
            .expect("list audit timeline");
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].action, newer.action);
        assert_eq!(timeline[1].action, older.action);
    }

    #[tokio::test]
    async fn workspace_skill_policy_upsert_and_delete_round_trip() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);

        let first = WorkspaceSkillPolicyRecord {
            workspace_id: "ws_000000000000000001".to_owned(),
            skill_slug: "pioneer/agent-browser".to_owned(),
            source_kind: "registry".to_owned(),
            enabled: Some(false),
            allow_implicit_invocation: Some(false),
        };

        store
            .upsert_workspace_skill_policy(&first, 1_700_000_000)
            .await
            .expect("first policy upsert");

        let second = WorkspaceSkillPolicyRecord {
            enabled: Some(true),
            allow_implicit_invocation: Some(true),
            ..first.clone()
        };
        store
            .upsert_workspace_skill_policy(&second, 1_700_000_100)
            .await
            .expect("second policy upsert");

        let rows = store
            .list_workspace_skill_policies(first.workspace_id.as_str())
            .await
            .expect("list workspace policies");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], second);

        store
            .delete_workspace_skill_policy(
                first.workspace_id.as_str(),
                first.skill_slug.as_str(),
                first.source_kind.as_str(),
            )
            .await
            .expect("delete workspace policy");

        let rows = store
            .list_workspace_skill_policies(first.workspace_id.as_str())
            .await
            .expect("list workspace policies after delete");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn materialize_turn_start_without_prompt_manifest_persists_turn() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000001".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: "turn_000000000000000001".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let input = vec![UserInput::Text {
            text: "hello".to_owned(),
            text_elements: Vec::new(),
        }];

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, input.as_slice())
            .await
            .expect("turn start without manifest should persist");

        let (workspace_id, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");

        assert_eq!(workspace_id, thread.workspace_id);
        assert_eq!(fetched_turn.id, turn.id);
        assert_eq!(fetched_turn.status, TurnStatus::InProgress);
        assert_eq!(fetched_turn.prompt_manifest, None);
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(persisted_turn.prompt_manifest_json, "{}");
        assert_eq!(persisted_turn.reasoning_effort, None);
        assert_eq!(
            persisted_turn.permission_profile_mode.as_deref(),
            Some("full_access")
        );
        assert_eq!(
            persisted_turn.permission_profile_source.as_deref(),
            Some("defaulted")
        );
        assert!(persisted_turn.permission_profile_snapshot_json.is_some());
    }

    #[tokio::test]
    async fn materialize_turn_start_persists_explicit_permission_profile() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_permission_profile".to_owned(),
            id: "thr_permission_profile".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let turn = Turn {
            id: "turn_permission_profile".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: permission_profile.clone(),
        };

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start with profile should persist");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");
        assert_eq!(fetched_turn.permission_profile, permission_profile);

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(
            persisted_turn.permission_profile_mode.as_deref(),
            Some("supervised")
        );
        assert_eq!(
            persisted_turn.permission_profile_source.as_deref(),
            Some("composer")
        );
        assert_eq!(
            serde_json::from_str::<TurnPermissionProfileSnapshot>(
                persisted_turn
                    .permission_profile_snapshot_json
                    .as_deref()
                    .expect("permission profile snapshot should persist")
            )
            .expect("snapshot should decode"),
            permission_profile
        );

        let audit_events_before_explicit_audit = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .filter(
                pioneer_entity::turn_event::Column::EventType
                    .eq(pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT),
            )
            .all(&connection)
            .await
            .expect("must query turn permission audit events");
        assert!(
            audit_events_before_explicit_audit.is_empty(),
            "CRUD must not synthesize profile-selected audit events during turn/start"
        );

        store
            .materialize_turn_permission_audit(
                pioneer_protocol::TurnPermissionAuditEvent {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    event_kind: TurnPermissionAuditEventKind::ProfileSelected,
                    profile_mode: permission_profile.mode,
                    profile_source: permission_profile.source,
                    item_id: None,
                    tool_call_id: None,
                    tool_name: None,
                    action_kind: None,
                    request_key: None,
                    decision: None,
                    reason: None,
                    cached: false,
                },
                timestamp,
            )
            .await
            .expect("explicit profile-selected audit should persist");

        let audit_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .filter(
                pioneer_entity::turn_event::Column::EventType
                    .eq(pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT),
            )
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn permission audit events");
        assert_eq!(audit_events.len(), 1);
        let audit_payload: crate::events::TurnEventPayload =
            serde_json::from_str(audit_events[0].payload.as_str())
                .expect("audit event payload should decode");
        let crate::events::TurnEventPayload::TurnPermissionAudit(audit) = audit_payload else {
            panic!("expected turn permission audit event");
        };
        assert_eq!(
            audit.event_kind,
            TurnPermissionAuditEventKind::ProfileSelected
        );
        assert_eq!(audit.workspace_id, thread.workspace_id);
        assert_eq!(audit.thread_id, thread.id);
        assert_eq!(audit.turn_id, turn.id);
        assert_eq!(audit.profile_mode, TurnPermissionMode::Supervised);
        assert_eq!(audit.profile_source, TurnPermissionProfileSource::Composer);
        assert!(audit.tool_name.is_none());
        assert!(audit.request_key.is_none());

        let turn_item_events = store
            .get_turn_item_events(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn item events should load")
            .expect("turn item events should exist");
        assert!(
            turn_item_events.events.iter().any(|event| matches!(
                &event.payload,
                TurnItemEventPayload::TurnPermissionAudit(audit)
                    if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
            )),
            "permission audit should be exposed through turn item event replay"
        );

        let history = store
            .get_thread_history(thread.id.as_str(), Some(16))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(
            history.events.iter().any(|event| matches!(
                &event.payload,
                ThreadHistoryEventPayload::TurnPermissionAudit(audit)
                    if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
            )),
            "permission audit should be exposed through thread history"
        );
    }

    #[tokio::test]
    async fn materialize_turn_start_with_permission_audit_projects_both_events_atomically() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_atomic_profile".to_owned(),
            id: "thr_atomic_profile".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let turn = Turn {
            id: "turn_atomic_profile".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: permission_profile.clone(),
        };
        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };

        store
            .materialize_turn_start_with_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                audit,
            )
            .await
            .expect("turn start and profile audit should persist atomically");

        let events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn events");
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].event_type,
            pioneer_protocol::constants::events::TURN_STARTED
        );
        assert_eq!(
            events[1].event_type,
            pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT
        );

        let projection_states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn.id.clone()))
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn event projection states");
        assert_eq!(projection_states.len(), 2);
        assert!(projection_states.iter().all(|state| {
            state.status
                == crate::repositories::turn_event_projection_state::PROJECTION_STATUS_PROJECTED
        }));

        let turn_item_events = store
            .get_turn_item_events(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn item events should load")
            .expect("turn item events should exist");
        assert!(turn_item_events.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::TurnPermissionAudit(audit)
                if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
        )));
    }

    #[tokio::test]
    async fn old_turn_without_permission_profile_reads_as_defaulted_full_access() {
        let store = test_store_with_workspace("ws_old_permission_profile").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_old_permission_profile".to_owned(),
            id: "thr_old_permission_profile".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        store
            .upsert_thread_model(&thread)
            .await
            .expect("thread should persist");

        let now = unix_to_datetime(timestamp);
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set("turn_old_permission_profile".to_owned()),
            thread_id: Set(thread.id.clone()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&store.connection)
        .await
        .expect("old-style turn insert should succeed");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), "turn_old_permission_profile")
            .await
            .expect("must read old turn")
            .expect("turn should exist");
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
    }

    #[tokio::test]
    async fn unknown_permission_profile_columns_read_as_defaulted_full_access() {
        let store = test_store_with_workspace("ws_unknown_permission_profile").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_unknown_permission_profile".to_owned(),
            id: "thr_unknown_permission_profile".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        store
            .upsert_thread_model(&thread)
            .await
            .expect("thread should persist");

        let now = unix_to_datetime(timestamp);
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set("turn_unknown_permission_profile".to_owned()),
            thread_id: Set(thread.id.clone()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            permission_profile_mode: Set(Some("future_mode".to_owned())),
            permission_profile_source: Set(Some("future_source".to_owned())),
            permission_profile_snapshot_json: Set(Some("{}".to_owned())),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&store.connection)
        .await
        .expect("future-style turn insert should succeed");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), "turn_unknown_permission_profile")
            .await
            .expect("must read turn with unknown permission columns")
            .expect("turn should exist");
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
    }

    #[tokio::test]
    async fn materialize_turn_start_persists_explicit_reasoning_effort() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_reasoning_effort".to_owned(),
            id: "thr_reasoning_effort".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: "turn_reasoning_effort".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start_with_reasoning_effort(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                Some("high"),
            )
            .await
            .expect("turn start with reasoning effort should persist");

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(persisted_turn.reasoning_effort.as_deref(), Some("high"));
    }

    #[tokio::test]
    async fn task_run_occurrence_turn_kind_and_origin_roundtrip() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000001".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: "run_0000000000000000001".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::TaskRun,
            origin: TurnOrigin::ScheduledTask,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("task run occurrence turn start should persist");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");
        assert_eq!(fetched_turn.turn_kind, TurnKind::TaskRun);
        assert_eq!(fetched_turn.origin, TurnOrigin::ScheduledTask);

        let threads = store
            .list_threads_for_workspace(thread.workspace_id.as_str(), 10)
            .await
            .expect("must list threads");
        let listed_thread = threads
            .iter()
            .find(|candidate| candidate.id == thread.id)
            .expect("thread should exist");
        let snapshot_turn = listed_thread
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .expect("task run occurrence turn should appear in owner thread");
        assert_eq!(snapshot_turn.turn_kind, TurnKind::TaskRun);
        assert_eq!(snapshot_turn.origin, TurnOrigin::ScheduledTask);
    }

    #[tokio::test]
    async fn list_threads_for_workspace_includes_latest_turn_marker() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection);
        let workspace_id = "ws_000000000000000001";
        let thread_id = "thr_000000000000000003";
        let first_timestamp = 1_700_000_000;
        let second_timestamp = 1_700_000_100;

        let first_thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: first_timestamp,
            updated_at: first_timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let first_turn = Turn {
            id: "turn_000000000000000003".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start_with_reasoning_effort(
                &first_thread,
                SandboxMode::FullAccess,
                &first_turn,
                &[],
                Some("low"),
            )
            .await
            .expect("first turn start should persist");

        let second_thread = Thread {
            model: "o3".to_owned(),
            model_provider: "custom-provider".to_owned(),
            updated_at: second_timestamp,
            ..first_thread
        };
        let second_turn = Turn {
            id: "turn_000000000000000004".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start_with_reasoning_effort(
                &second_thread,
                SandboxMode::FullAccess,
                &second_turn,
                &[],
                Some("high"),
            )
            .await
            .expect("second turn start should persist");

        let threads = store
            .list_threads_for_workspace(workspace_id, 10)
            .await
            .expect("list threads should succeed");
        let listed = threads
            .iter()
            .find(|thread| thread.id == thread_id)
            .expect("thread should be listed");

        assert_eq!(listed.model, "o3");
        assert_eq!(listed.model_provider, "custom-provider");
        assert_eq!(listed.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(listed.turns.len(), 1);
        assert_eq!(listed.turns[0].id, second_turn.id);

        let fetched = store
            .get_thread_model(thread_id)
            .await
            .expect("get thread model should succeed")
            .expect("thread should exist");
        assert_eq!(fetched.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(fetched.turns.len(), 1);
        assert_eq!(fetched.turns[0].id, second_turn.id);
    }

    #[tokio::test]
    async fn update_turn_prompt_manifest_roundtrips_via_prompt_manifest_json() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000002".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: "turn_000000000000000002".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
            .await
            .expect("turn start should persist");

        let manifest = PromptManifest {
            compiler_version: "0.1.0-test".to_owned(),
            profile: PromptManifestProfile::AssistantFull,
            section_ids: vec![
                "identity_base".to_owned(),
                "assistant_safety".to_owned(),
                "soul_core".to_owned(),
            ],
            fingerprint_stable: "stable".to_owned(),
            fingerprint_dynamic: "dynamic".to_owned(),
            fingerprint_full: "full".to_owned(),
            diagnostics: vec![PromptManifestDiagnostic {
                code: PromptManifestDiagnosticCode::MissingFile,
                message: "bootstrap file `SOUL.md` is missing".to_owned(),
                file: Some("/tmp/SOUL.md".to_owned()),
                section_id: None,
                hook_source: Some(PromptManifestHookSource {
                    hook_id: "test.crud_hook".to_owned(),
                    subscription_id: "test.crud_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:cruddiagnostic".to_owned()),
                }),
            }],
            hook_sources: vec![PromptManifestHookSourceEntry {
                source: PromptManifestHookSource {
                    hook_id: "test.crud_hook".to_owned(),
                    subscription_id: "test.crud_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:crudsource".to_owned()),
                },
                section_id: Some("identity_base".to_owned()),
                contribution_kind: PromptManifestHookContributionKind::PromptSection,
                priority: Some(10),
                source_count: Some(1),
                truncation: PromptManifestHookTruncation::None,
            }],
        };

        let updated = store
            .update_turn_prompt_manifest(thread.id.as_str(), turn.id.as_str(), &manifest, timestamp)
            .await
            .expect("update should succeed");
        assert!(updated, "turn row must be updated");

        let (_workspace_id, roundtrip_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn/get should succeed")
            .expect("turn should exist");
        assert_eq!(roundtrip_turn.prompt_manifest, Some(manifest));
    }
}
