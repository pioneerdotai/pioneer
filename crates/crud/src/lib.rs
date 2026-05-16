mod convention;
mod events;
mod memory;
mod projector;
mod repositories;
mod task_events;
mod task_projector;
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
    TaskGetResponse, TaskListParams, TaskResult, TaskRun, TaskRunExecution, TaskRunExecutionStatus,
    TaskRunStatus, TaskTree, TaskTrigger, TaskTriggerKind, TaskTriggerSpec, TaskWriteLock, Thread,
    ThreadFolder, ThreadHistoryEvent, ThreadHistoryEventPayload, ThreadLineage, ThreadPlacement,
    TimelineOutputPolicy, ToolCallStatus, ToolDisplayPayload, ToolStoragePayload, Turn, TurnItem,
    TurnItemEvent, TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnItemsResponse,
    UserInput, generate_id,
};
use pioneer_sqlite::{SqliteWriteCoordinator, is_anyhow_sqlite_lock};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QuerySelect, TransactionTrait,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;

use crate::convention::{
    ATTEMPT_STATUS_INTERRUPTED, ATTEMPT_STATUS_RUNNING, DB_ID_LEN, MEMORY_EVENT_ACCESSED,
    MEMORY_EVENT_CANDIDATE_APPROVED, MEMORY_EVENT_CANDIDATE_CREATED,
    MEMORY_EVENT_CANDIDATE_EXPIRED, MEMORY_EVENT_CANDIDATE_REJECTED,
    MEMORY_EVENT_CAPSULE_REPAIR_STATUS_CHANGED, MEMORY_EVENT_CREATED, MEMORY_EVENT_EXPIRED,
    MEMORY_EVENT_FORGOTTEN, MEMORY_EVENT_REPAIR_STATUS_CHANGED, MEMORY_EVENT_SUPERSEDED,
    MEMORY_EVENT_UPDATED, TURN_ITEM_STATUS_CANCELLED, TURN_ITEM_STATUS_COMPLETED,
    TURN_ITEM_STATUS_FAILED, TURN_ITEM_STATUS_TIMED_OUT, is_terminal_task_run_status_db,
    is_terminal_task_status_db, prompt_manifest_profile_to_db, provider_failure_class_from_db,
    provider_failure_stage_from_db, recovery_action_from_db, recovery_action_to_db,
    recovery_job_status_from_db, recovery_trigger_from_db,
    task_concurrency_conflict_policy_from_db, task_delivery_attempt_status_from_db,
    task_delivery_mode_from_db, task_delivery_status_from_db, task_executor_kind_from_db,
    task_owner_kind_from_db, task_owner_kind_to_db, task_run_execution_status_from_db,
    task_run_status_from_db, task_status_from_db, task_status_to_db, task_trigger_kind_from_db,
    task_trigger_status_from_db, task_write_lock_scope_kind_from_db,
    task_write_lock_status_from_db, thread_mode_from_db, thread_origin_kind_from_db,
    thread_sidebar_visibility_from_db, thread_status_from_db, turn_item_type_from_db,
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};
use crate::events::{TurnEventPayload, TurnStartedEventPayload};
use crate::projector::TurnProjector;
pub use crate::repositories::artifact::{
    ArtifactBindingTargetRecord, ArtifactBlobRecord, ArtifactCrudError, ArtifactExternalRefKey,
    ArtifactExternalRefRecord, ArtifactGcBlobCandidateRecord, ArtifactGcPlanRecord,
    ArtifactListFilterRecord, ArtifactListPageRecord, ArtifactProjectionBlobRecord,
    ArtifactProjectionRecord, ArtifactRecord, ArtifactVersionBlobRecord, ArtifactVersionRecord,
    ArtifactWorkspaceUsageRecord, IngestArtifactMetadataRecord, IngestedArtifactRecord,
    NewArtifactBlobRecord, UpsertArtifactExternalRefRequest,
};
pub use crate::repositories::thread_agents_doc::{
    ResolvedThreadAgentsDocRecord, ThreadAgentsDocError, ThreadAgentsDocRecord,
    ThreadAgentsDocRevisionRecord, ThreadAgentsDocSaveReason, ThreadAgentsDocScope,
    ThreadAgentsDocScopeContext, ThreadAgentsDocStatus, ThreadAgentsDocSummaryRecord,
};
use crate::repositories::{
    agent_memory, agent_memory_candidate, agent_memory_capsule, agent_memory_event,
    agent_memory_policy_decision, agent_memory_repair_job, artifact as artifact_repository,
    hook_run, mcp_audit_event, mcp_server_catalog_snapshot, mcp_server_installation, policy,
    recovery_job, skill_audit_event, skill_dependency_snapshot, skill_installation,
    skill_upload_session, skill_workspace_policy, task as task_repository, task_agent_spec,
    task_delivery, task_dependency, task_event, task_run, task_run_execution, task_trigger,
    task_write_lock, thread, thread_agents_doc, thread_lineage, thread_tree, turn, turn_event,
    turn_item_attempt, turn_llm_context, turn_mcp_binding, turn_skill_binding,
};
pub use crate::task_events::{AppendedTaskEvent, TaskEventAppendStatus, TaskEventPayload};
use crate::task_projector::TaskProjector;
use crate::turn_item_terminal::{
    TurnItemTerminalState, terminalize_turn_item_payload, tool_call_status,
};

pub use crate::memory::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryCandidateRecord,
    AgentMemoryCandidateStatusUpdateRecord, AgentMemoryCapsuleRecord, AgentMemoryControlRecord,
    AgentMemoryEventRecord, AgentMemoryListFilter, AgentMemoryPolicyDecisionRecord,
    AgentMemoryRepairJobRecord, MemoryActorRecord, MemoryScopeResolution, MemoryWorkspaceGuard,
    NewAgentMemoryCandidate, NewAgentMemoryControlRecord, NewAgentMemoryEvent,
    NewAgentMemoryPolicyDecision, NewAgentMemoryRepairJob, global_agent_memory_scope_key,
    memory_scope_key_hash, workspace_agent_memory_scope_key,
};
pub use crate::repositories::hook_run::{
    HOOK_RUN_CONTRIBUTION_HASH_MAX_COUNT, HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS,
    HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT, HOOK_RUN_ERROR_MESSAGE_MAX_CHARS,
    HOOK_RUN_IDEMPOTENCY_KEY_MAX_CHARS, HookAuditEventRecord, HookRunAttemptCompletionRecord,
    HookRunAttemptRecord, HookRunCompletionRecord, HookRunRecord, HookRunScope, HookRunScopeKind,
    NewHookAuditEventRecord, NewHookRunAttemptRecord, NewHookRunRecord, RecoverableHookRunRecord,
};
pub use crate::repositories::turn_llm_context::{NewTurnLlmContextEntry, TurnLlmContextEntry};
use crate::util::{optional_typed_json_from_db, typed_json_from_db, unix_to_datetime};
use sea_orm::entity::prelude::DateTimeWithTimeZone;

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

    let (child_thread_id, child_turn_id) = if executor_kind == TaskExecutorKind::Agent {
        (Some(generate_id(DB_ID_LEN)), Some(generate_id(DB_ID_LEN)))
    } else {
        (None, None)
    };

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
            child_thread_id,
            child_turn_id,
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
    pub user_text: Option<String>,
    pub assistant_text: Option<String>,
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

#[derive(Debug, Clone, Copy, Default)]
pub struct TurnItemAttemptDeadlines {
    pub lease_expires_at_unix: Option<i64>,
    pub idle_deadline_at_unix: Option<i64>,
    pub hard_deadline_at_unix: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct RunningAttemptDeadlineRepairCandidate {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub started_at_unix: i64,
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
        let event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: input.to_vec(),
        });

        self.materialize_turn_event(event, thread_model.updated_at)
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

        Ok(Some(TaskGetResponse {
            task,
            triggers,
            runs,
            agent_specs,
            dependencies,
            write_locks,
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
            if !params.include_completed
                && matches!(
                    task.status,
                    pioneer_protocol::TaskStatus::Completed
                        | pioneer_protocol::TaskStatus::Failed
                        | pioneer_protocol::TaskStatus::Cancelled
                )
            {
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

    pub async fn get_thread_lineage(&self, child_thread_id: &str) -> Result<Option<ThreadLineage>> {
        let row =
            thread_lineage::find_lineage_by_child_thread(&self.connection, child_thread_id).await?;
        Ok(row.map(thread_lineage_from_db_model))
    }

    pub async fn list_child_thread_lineage_for_parent(
        &self,
        parent_thread_id: &str,
    ) -> Result<Vec<ThreadLineage>> {
        let rows =
            thread_lineage::list_children_for_parent_thread(&self.connection, parent_thread_id)
                .await?;
        Ok(rows.into_iter().map(thread_lineage_from_db_model).collect())
    }

    pub async fn list_thread_lineage_for_task(&self, task_id: &str) -> Result<Vec<ThreadLineage>> {
        let rows = thread_lineage::list_lineage_for_task(&self.connection, task_id).await?;
        Ok(rows.into_iter().map(thread_lineage_from_db_model).collect())
    }

    pub async fn list_thread_lineage_for_run(&self, run_id: &str) -> Result<Vec<ThreadLineage>> {
        let rows = thread_lineage::list_lineage_for_run(&self.connection, run_id).await?;
        Ok(rows.into_iter().map(thread_lineage_from_db_model).collect())
    }

    pub async fn list_thread_lineage_by_root_thread(
        &self,
        root_thread_id: &str,
    ) -> Result<Vec<ThreadLineage>> {
        let rows =
            thread_lineage::list_lineage_by_root_thread(&self.connection, root_thread_id).await?;
        Ok(rows.into_iter().map(thread_lineage_from_db_model).collect())
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

        Ok(Some((
            thread_model.workspace_id,
            Turn {
                id: turn_model.id,
                status,
                turn_kind: turn_kind_from_db(turn_model.turn_kind.as_str()).unwrap_or_default(),
                origin: turn_origin_from_db(turn_model.origin.as_str()).unwrap_or_default(),
                error: turn_model.error,
                prompt_manifest,
            },
        )))
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

        if events_rows.is_empty() {
            return Ok(Some(TurnItemsResponse {
                thread_id: thread_id.to_owned(),
                workspace_id,
                turn_id: turn_id.to_owned(),
                events: Vec::new(),
                last_sequence: 0,
            }));
        }

        let mut events = Vec::new();
        let mut last_sequence = 0i64;

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
                    TurnItemEventPayload::ItemCompleted {
                        workspace_id: workspace_id.clone(),
                        thread_id: notification.thread_id,
                        turn_id: notification.turn_id,
                        item: notification.item,
                    }
                }
                TurnEventPayload::ItemUpdated(notification) => TurnItemEventPayload::ItemUpdated {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                },
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
                TurnEventPayload::TurnStarted(_)
                | TurnEventPayload::TurnCompleted(_)
                | TurnEventPayload::TurnFailed(_) => continue,
            };

            events.push(TurnItemEvent {
                sequence: row.sequence,
                created_at: row.created_at.timestamp_millis(),
                payload: mapped_payload,
            });
        }

        Ok(Some(TurnItemsResponse {
            thread_id: thread_id.to_owned(),
            workspace_id,
            turn_id: turn_id.to_owned(),
            events,
            last_sequence,
        }))
    }

    pub async fn get_thread_conversation_history(
        &self,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        let turns =
            turn::find_terminal_turns_for_thread(&self.connection, thread_id, max_turns as u64)
                .await?;

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

    pub async fn get_thread_sandbox_mode(&self, thread_id: &str) -> Result<Option<SandboxMode>> {
        policy::find_thread_sandbox_mode(&self.connection, thread_id).await
    }

    pub async fn get_thread_model(&self, thread_id: &str) -> Result<Option<Thread>> {
        let Some(model) = thread::find_thread_by_id(&self.connection, thread_id).await? else {
            return Ok(None);
        };

        Ok(thread_from_db_model(model))
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

            if let Some(turn_model) =
                turn::find_latest_turn_for_thread(&self.connection, thread.id.as_str()).await?
                && let Some(turn) = thread_snapshot_turn_from_db_model(turn_model)
            {
                thread.turns.push(turn);
            }

            threads.push(thread);
        }

        Ok(threads)
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
                if !matches!(turn_model.status.as_str(), "completed" | "failed" | "interrupted") {
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
        self.run_serialized_write(|| {
            self.materialize_turn_event_once(
                event.clone(),
                event_timestamp_secs,
                item_started_deadlines,
            )
        })
        .await
    }

    async fn materialize_turn_event_once(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
    ) -> Result<()> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin materialization transaction")?;

        let created_at = unix_to_datetime(event_timestamp_secs);

        validate_turn_event_for_permanent_storage(&event).await?;

        let appended_event = match turn_event::append_event(&transaction, &event, created_at).await
        {
            Ok(event) => event,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .projector
            .project(&transaction, &appended_event)
            .await
            .context("failed to project turn event to read models")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        if let (TurnEventPayload::ItemStarted(notification), Some(deadlines)) =
            (&event, item_started_deadlines)
        {
            let configured = turn_item_attempt::configure_running_attempt_deadlines(
                &transaction,
                notification.turn_id.as_str(),
                notification.item.item_id(),
                created_at,
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

        transaction
            .commit()
            .await
            .context("failed to commit turn event materialization transaction")?;

        Ok(())
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
        child_thread_id: model.child_thread_id,
        child_turn_id: model.child_turn_id,
        started_at: model.started_at.map(|value| value.timestamp()),
        completed_at: model.completed_at.map(|value| value.timestamp()),
        result: optional_typed_json_from_db(model.result_json)?,
        error: optional_typed_json_from_db(model.error_json)?,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
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
        result_contract: optional_typed_json_from_db(model.result_contract_json)?,
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

fn thread_lineage_from_db_model(model: pioneer_entity::thread_lineage::Model) -> ThreadLineage {
    ThreadLineage {
        child_thread_id: model.child_thread_id,
        child_turn_id: model.child_turn_id,
        parent_thread_id: model.parent_thread_id,
        parent_turn_id: model.parent_turn_id,
        task_id: model.task_id,
        task_run_id: model.task_run_id,
        root_thread_id: model.root_thread_id,
        depth: model.depth,
        created_at: model.created_at.timestamp(),
    }
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

fn thread_snapshot_turn_from_db_model(model: pioneer_entity::turn::Model) -> Option<Turn> {
    let status = turn_status_from_db(model.status.as_str())?;

    Some(Turn {
        id: model.id,
        status,
        turn_kind: turn_kind_from_db(model.turn_kind.as_str()).unwrap_or_default(),
        origin: turn_origin_from_db(model.origin.as_str()).unwrap_or_default(),
        error: model.error,
        prompt_manifest: None,
    })
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
        ClaimedRecoveryActivation, CrudStore, McpAuditEventRecord, McpServerCatalogSnapshotRecord,
        McpServerInstallationRecord, NewTurnLlmContextEntry, SkillAuditEventRecord,
        SkillInstallationRecord, TaskEventPayload, ThreadAgentsDocError, ThreadAgentsDocSaveReason,
        ThreadAgentsDocStatus, TurnItemAttemptDeadlines, TurnMcpBindingRecord,
        TurnSkillBindingRecord, WorkspaceSkillPolicyRecord,
    };
    use crate::util::unix_to_datetime;
    use migration::{Migrator, MigratorTrait};
    use pioneer_protocol::{
        ItemCompletedNotification, ItemRecoveryAttachedNotification,
        ItemRecoveryExhaustedNotification, ItemRecoveryOpenedNotification,
        ItemRecoverySucceededNotification, ItemRetryAttemptStartedNotification,
        ItemRetryScheduledNotification, ItemStartedNotification, ItemTimeoutDetectedNotification,
        ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
        ItemToolRetryScheduledNotification, PromptManifest, PromptManifestDiagnostic,
        PromptManifestDiagnosticCode, PromptManifestHookContributionKind, PromptManifestHookPhase,
        PromptManifestHookSource, PromptManifestHookSourceEntry, PromptManifestHookTruncation,
        PromptManifestProfile, RecoveryAction, RecoveryJobStatus, RecoveryTrigger, SandboxMode,
        Task, TaskAgentPrompt, TaskAgentResultContract, TaskAgentResultFormat, TaskAgentSpec,
        TaskExecutorKind, TaskMetadata, TaskOwnerKind, TaskResult, TaskRun, TaskRunStatus,
        TaskSchema, TaskStatus, TaskTrigger, TaskTriggerSpec, TaskTriggerStatus, TaskValue, Thread,
        ThreadHistoryEventPayload, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, ToolCallStatus, ToolDisplayPayload, ToolLoopBudgetAction,
        ToolLoopBudgetLimitKind, ToolMetadata, ToolOutputPolicySnapshot,
        ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass,
        ToolRetryBudgetKind, ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind,
        ToolRetryResolution, ToolStoragePayload, Turn, TurnCompletedNotification, TurnItem,
        TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnKind, TurnOrigin,
        TurnStatus, TurnToolLoopBudgetExceededNotification, UserInput,
    };
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, DatabaseBackend, EntityTrait, QueryFilter, Set,
        Statement,
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
                timestamp + 3,
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
                    action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
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
                action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
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
                resolved_reason: "explicit_mention".to_owned(),
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
            resolved_reason: "explicit_mention".to_owned(),
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

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(persisted_turn.prompt_manifest_json, "{}");
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
        };
        store
            .materialize_turn_start(&first_thread, SandboxMode::FullAccess, &first_turn, &[])
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
        };
        store
            .materialize_turn_start(&second_thread, SandboxMode::FullAccess, &second_turn, &[])
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
        assert_eq!(listed.turns.len(), 1);
        assert_eq!(listed.turns[0].id, second_turn.id);
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
