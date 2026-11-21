mod fixtures;
use super::{
    AgentExecutionInput, AgentResourceStateInput, ArtifactBindingTargetRecord,
    AtomicRecoveryJobEnqueueOutcome, BLOCK_KIND_APPROVAL, BLOCK_KIND_USER_MESSAGE,
    BlockedTurnRecoveryResumeOutcome, CanonicalTurnEventPayload, ClaimedRecoveryActivation,
    CliRuntimeExecutionSegmentStatus, CliRuntimeNativeEventListFilter,
    CliRuntimePendingRequestListFilter, CliRuntimePendingRequestStatus,
    CliRuntimeProviderSessionLifecycle, CliRuntimeRequestAuthorizationBinding,
    CliRuntimeThreadMcpMetadata, CliRuntimeTurnAttemptStatus, CliRuntimeTurnBindingListFilter,
    CliRuntimeTurnMcpMetadata, CompletedMessageTurnWrite, ConversationArtifactRefLimits, CrudStore,
    DeleteTurnMessageRequest, EditTurnMessageRequest, IngestArtifactMetadataRecord,
    NativeExecutionWindowTransition, NewArtifactBlobRecord, NewCliRuntimeNativeEvent,
    NewCliRuntimePendingRequest, NewCliRuntimeThreadBinding, NewCliRuntimeTurnBinding,
    NewExecutionAdmissionLease, NewMemberPrincipalRow, NewTaskExecutionAdmission,
    NewThreadEpisodicItemRecord, NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord,
    NewTurnLlmContextEntry, NewTurnRuntimeSnapshot, NewWorkspaceMembership,
    PrepareClaudeProviderSessionBinding, PrepareTurnFinalizationOutcome,
    PreparedClaudeProviderSessionMode, ProjectionPageAnchor, RecoveryJobRecord,
    RecoveryTerminalCleanupPlan, RecoveryTerminalizationApplyOutcome,
    RecoveryTerminalizationClaimDisposition, RestoreTurnProjectionStreamOutcome,
    SkillAuditEventRecord, SkillDependencySnapshotRecord, SkillInstallationPatch,
    SkillInstallationRecord, SkillPackChildDiff, SkillPackInstallationRecord,
    TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES, TaskEventPayload, TaskOwnedTurnResumeOutcome,
    TaskRunChildAnchor, TaskRunOccurrenceReconcileCandidate,
    TaskRunOccurrenceTerminalizationOutcome, ThreadAgentsDocError, ThreadAgentsDocSaveReason,
    ThreadAgentsDocStatus, ThreadEpisodicItemStatus, ThreadEpisodicItemVisibility,
    ThreadEpisodicSourceActorRole, ThreadEpisodicSourceRuntimeKind, ThreadTimelineApprovalScope,
    ThreadTimelineBlockRecord, TurnExecutionCheckpointKind, TurnExecutionWindowStatsRecord,
    TurnExecutionWindowUsageAggregateRecord, TurnFinalizationCommitOutcome,
    TurnItemAttemptDeadlines, TurnMcpBindingRecord, TurnMcpProjectionPersistenceError,
    TurnMcpProjectionRecord, TurnMcpProjectionReplacement, TurnMessageMutationFailure,
    TurnProjectionStreamHealth, TurnSkillBindingRecord, TurnWorkItemProjectionRecord,
    TurnWorkOwner, WORK_ITEM_STATUS_COMPLETED, WORK_ITEM_STATUS_RUNNING, WORK_VISIBILITY_VISIBLE,
    WorkspaceSkillPolicyRecord, create_gateway_singleton, create_member_principal,
    create_superuser, delete_workspace_membership, ensure_gateway_singleton,
    ensure_pioneer_for_workspace, insert_workspace_membership,
    list_abandoned_runtime_draft_artifact_ids, lock_gateway_singleton,
    message_mutation_actor_current_thread_write_kind, principal_current_thread_access_kind,
    resolve_artifact_authorization_scope, resolve_runtime_draft_artifact_authorization_scope,
    upsert_thread_timeline_block, work_item_projection_id,
};
use pioneer_crud_execution::attempt_values::ATTEMPT_STATUS_COMPLETED;
use pioneer_crud_execution::time::unix_to_datetime;
use crate::{
    AppendedTaskEvent, NewThreadEpisodicExclusionRecord, NewTurnExecution,
    PROJECTION_META_STATUS_PENDING, ProjectionMetaConfigRecord, ProjectionMetaRecord,
    SkillUploadSessionRecord, TASK_EVENT_FANOUT_BYTE_BUDGET,
    THREAD_EPISODIC_PROJECTION_CHANGED_ERROR, THREAD_EPISODIC_SOURCE_VERSION_SUPERSEDED_ERROR,
    THREAD_EPISODIC_USER_DELETED_ERROR, THREAD_EPISODIC_USER_EXCLUDED_ERROR,
    TaskEventFanoutOutcome, TaskEventFanoutPage, ThreadEpisodicExclusionReason,
    ThreadEpisodicIndexAttemptOutcome, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobRecord, ThreadEpisodicIndexJobStatus,
    ThreadEpisodicProjectionResetProgress, TurnEventDeliveryOutcome, TurnEventDeliverySettlement,
    TurnEventPayload, TurnExecutionStatus, TurnExecutorKind, find_projection_meta,
    thread_episodic_projection_reset_key, turn_execution, upsert_projection_meta_with_config,
    utc_now,
};
pub(crate) use fixtures::postgres_store_with_workspace;
use fixtures::*;
use migration::{Migrator, MigratorTrait};
use pioneer_crud_conversations::{read_model_repair, turn_finalization};
use pioneer_crud_execution::{
    cli_runtime_binding, event_append as turn_event, native_terminal_effect_outbox,
};
pub(crate) use pioneer_crud_tasks::test_support::task_projection::{
    sample_task, sample_task_agent_spec, sample_task_run, sample_task_trigger,
};
use pioneer_protocol::{
    AgentMessagePhase, ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind,
    ArtifactKind, ArtifactRole, ExecutionWindowExhaustionReason, ExecutionWindowStatus, GatewayId,
    ItemCompletedNotification, ItemRecoveryAttachedNotification, ItemRecoveryExhaustedNotification,
    ItemRecoveryOpenedNotification, ItemRecoverySucceededNotification,
    ItemRetryAttemptStartedNotification, ItemRetryScheduledNotification, ItemStartedNotification,
    ItemTimeoutDetectedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, ItemUpdatedNotification,
    PermissionBehavior, PersistedActorRef, PrincipalId, PromptManifest, PromptManifestDiagnostic,
    PromptManifestDiagnosticCode, PromptManifestHookContributionKind, PromptManifestHookPhase,
    PromptManifestHookSource, PromptManifestHookSourceEntry, PromptManifestHookTruncation,
    PromptManifestProfile, RecoveryAction, RecoveryJobStatus, RecoveryTrigger, RoleKey,
    SandboxMode, SkillId, SkillPackId, SystemEventLevel, Task, TaskAgentPrompt,
    TaskAgentResultContract, TaskAgentResultFormat, TaskAgentSpec, TaskAttachmentMode,
    TaskCompletionBehavior, TaskConcurrencyConflictPolicy, TaskExecutorKind, TaskLifecyclePolicy,
    TaskMetadata, TaskOccurrenceContract, TaskOccurrenceStatus, TaskOwnerKind,
    TaskParentTerminalAction, TaskResult, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEvent, TaskResultReviewEventKind,
    TaskResultReviewerKind, TaskRun, TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBinding,
    TaskRunThreadBindingKind, TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus, TaskSchema,
    TaskStatus, TaskTrigger, TaskTriggerSpec, TaskTriggerStatus, TaskValue, TaskWriteLock,
    TaskWriteLockScopeKind, TaskWriteLockStatus, Thread, ThreadEpisodicSourceContext,
    ThreadHistoryEventPayload, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    ToolCallStatus, ToolDisplayPayload, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
    ToolMetadata, ToolOutputPolicySnapshot, ToolPermissionPolicySnapshot,
    ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass,
    ToolRetryBudgetKind, ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind,
    ToolRetryResolution, ToolStoragePayload, Turn, TurnBlockedResumeMetadata,
    TurnCompletedNotification, TurnExecutionSecuritySnapshot,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
    TurnFailedNotification, TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnItem,
    TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnKind, TurnMention, TurnOrigin,
    TurnPermissionAuditEventKind, TurnPermissionMode, TurnPermissionProfileSnapshot,
    TurnPermissionProfileSource, TurnSandboxMode, TurnStatus,
    TurnToolLoopBudgetExceededNotification, UserInput, generate_id,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseBackend, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, QueryTrait, Set, Statement, TransactionTrait,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod attempt_projection;
mod canonical_delivery;
mod canonical_projection;
mod connection_scope;
mod context_retention;
mod episodic_projection;
mod execution_windows;
mod gateway_control;
mod message_transactions;
mod read_cursors;
mod recovery_history;
mod recovery_terminalization;
mod skill_mcp;
mod task_turns;
mod terminal_effects;
mod timeline_state;
mod turn_authorization;
mod turn_finalization;
mod turn_start;

mod execution_observability;

mod task_child_discovery;
mod task_child_portable_producers;
mod task_child_producers;

mod unread_start;

mod cli_runtime_active_bindings;

mod finalization_wakes;
#[path = "task_run_occurrence.rs"]
mod occurrence_tracker;
mod projection_recovery;

#[path = "native_terminal_effect_gates.rs"]
mod terminal_effect_gates;

mod episodic_delivery_tests;

// Transcript identity tests do not observe background consumers. Protect
// the semantic wake boundary here, through the public store write APIs.

mod native_cancellation_tests;

// Exercise the durable dispatcher boundary, including claim, decode and ACK.
// Callers can park after draining setup, then prove a custom publisher alone
// wakes the consumer and makes its newly committed events deliverable.

mod history_order;
