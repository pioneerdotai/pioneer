mod access_invalidation;
pub(crate) mod agent_action_tools;
mod agent_route_handlers;
mod agent_runtime;
mod artifact_finalization_diagnostics;
mod artifact_registration;
mod artifact_tools;
mod artifacts;
mod auth_handlers;
mod binary;
mod cli_post_turn;
#[path = "../cli_runtime/handlers.rs"]
mod cli_runtime;
mod compaction_background;
mod dispatch;
pub(crate) mod html_tools;
mod invitation_handlers;
mod invitation_lifecycle;
mod markdown;
mod mcp;
mod member_handlers;
mod memory_handlers;
mod message_mutations;
mod message_turn;
mod native_health;
mod notifications;
mod patch_history_handlers;
mod permission_handlers;
mod provider_handlers;
mod provider_readiness;
mod reconciliation_diagnostics;
mod scoped_push;
pub(crate) mod skills;
mod summary;
mod task_agent_executor;
pub(crate) use task_agent_executor::task_cache_routing_context;
mod task_artifacts;
mod task_completion_inbox;
mod task_delivery;
mod task_handlers;
mod tasks;
#[cfg(test)]
mod tests;
mod webhook_endpoint;
#[cfg(test)]
pub(crate) use tests::{test_summary_config, test_tool_loop_config};
mod thread_agents_doc_handlers;
mod thread_file_handlers;
mod thread_handlers;
mod thread_history_read;
mod thread_launch_pages;
mod thread_read_cursor;
pub(crate) mod thread_read_tools;
mod thread_result_sources;
pub(crate) mod thread_search_tools;
pub(crate) mod thread_tool_results;
mod timeline_cursor;
mod timeline_handlers;
mod timeline_notifications;
mod turn_handlers;
mod voice_handlers;

const NATIVE_HUMAN_INTERACTION_RUNTIME_ID: &str =
    pioneer_protocol::constants::runtime_ids::NATIVE_PERMISSION;
const NATIVE_HUMAN_INTERACTION_RUNTIME_KIND: &str = "native";
const NATIVE_HUMAN_INTERACTION_REQUEST_KIND: &str = "native_permission";

/// Turn-start history keeps its caller's read contour with ordinary writes.
/// A surrounding Task correctness transition must not promote metadata
/// registration, restore, or compaction to Critical.
pub(crate) fn ordinary_history_store(store: &CrudStore) -> CrudStore {
    match store.database_connection().read_class() {
        SqliteReadClass::Interactive => store.with_interactive_writes(),
        SqliteReadClass::Maintenance => store.with_maintenance_access(),
    }
}
mod workspace_handlers;

pub use summary::SummaryConfig;

pub(crate) trait ArtifactStreamInvalidation: Send + Sync {
    fn cancel_artifact(&self, workspace_id: &str, artifact_id: &str) -> usize;
    fn cancel_authorization_signal(&self, signal: &AccessChangeSignal) -> usize;
}

/// Identifies whether Task child reconciliation is part of the live Turn
/// lifecycle or a durable background repair pass. The distinction controls
/// read admission independently from the correctness-critical writes shared
/// by both paths.
#[derive(Clone, Copy)]
enum TaskChildReconciliationOrigin {
    Live,
    DurableBackground,
}

fn gateway_stage_for_read_model_repair(
    stage: pioneer_crud::ReadModelRepairStage,
) -> pioneer_observability::GatewayOperationStage {
    use pioneer_crud::ReadModelRepairStage as RepairStage;
    use pioneer_observability::GatewayOperationStage as OperationStage;

    match stage {
        RepairStage::TerminalPayloadCheckpointLoad => {
            OperationStage::ResilienceReadModelCheckpointLoad
        }
        RepairStage::TerminalPayloadCheckpointReset => {
            OperationStage::ResilienceReadModelCheckpointReset
        }
        RepairStage::TerminalPayloadFullScanBatchLoad => {
            OperationStage::ResilienceReadModelFullScanBatchLoad
        }
        RepairStage::TerminalPayloadFullScanBatchEvaluate => {
            OperationStage::ResilienceReadModelFullScanBatchEvaluate
        }
        RepairStage::TerminalPayloadFullScanBatchCommit => {
            OperationStage::ResilienceReadModelFullScanBatchCommit
        }
        RepairStage::TerminalPayloadFullScanComplete => {
            OperationStage::ResilienceReadModelFullScanComplete
        }
        RepairStage::TerminalPayloadIncrementalBegin => {
            OperationStage::ResilienceReadModelIncrementalBegin
        }
        RepairStage::TerminalPayloadIncrementalDirtyBatchLoad => {
            OperationStage::ResilienceReadModelIncrementalDirtyBatchLoad
        }
        RepairStage::TerminalPayloadIncrementalSourceBatchLoad => {
            OperationStage::ResilienceReadModelIncrementalSourceBatchLoad
        }
        RepairStage::TerminalPayloadIncrementalBatchEvaluate => {
            OperationStage::ResilienceReadModelIncrementalBatchEvaluate
        }
        RepairStage::TerminalPayloadIncrementalBatchCommit => {
            OperationStage::ResilienceReadModelIncrementalBatchCommit
        }
        RepairStage::TerminalPayloadIncrementalComplete => {
            OperationStage::ResilienceReadModelIncrementalComplete
        }
        RepairStage::TerminalTurnRunningAttempts => {
            OperationStage::ResilienceReadModelRunningAttempts
        }
        RepairStage::TerminalTasksMissingCompletedAt => {
            OperationStage::ResilienceReadModelTerminalTasks
        }
        RepairStage::TerminalRunsMissingCompletedAt => {
            OperationStage::ResilienceReadModelTerminalRuns
        }
    }
}

use crate::hook_runtime::GatewayHookRuntimeBuilder;
use crate::keep_awake::GatewayKeepAwake;
use crate::prompt_hooks::agents_doc_prompt_hook_package;
use crate::thread_episodic::{
    ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver,
    RuntimeVectorThreadEpisodicIndexPayloadProvider, StoreThreadEpisodicIndexPayloadProvider,
    StoreThreadEpisodicIngestor, ThreadEpisodicIndexEmbeddingProviderResolver,
    ThreadEpisodicIndexExecutor, ThreadEpisodicIngestor, ThreadEpisodicRecallService,
    ThreadEpisodicRuntimeConfig,
};
use anyhow::Context as AnyhowContext;
use futures_util::FutureExt;
use pioneer_agent::MemoryLoopConfig;
use pioneer_agent::{AgentManager, ResolvedArtifactInput, ToolLoopConfig};
use pioneer_artifacts::{
    ArtifactBindingTarget, ArtifactGcPolicy, ArtifactQuotaPolicy, ArtifactRegistrationCandidate,
    ArtifactRegistrationContext, ArtifactRegistrationSource, ArtifactService, ArtifactToolState,
    BindArtifactRequest, LocalArtifactBlobStore,
};
use pioneer_config::{
    GatewayArtifactsConfig, GatewayCliAgentRuntimeCommandHeartbeatConfig,
    GatewayCommandExecutionTimeoutConfig, GatewayContextCompactionConfig,
    GatewayContextCompactionTimeoutConfig, GatewayHookRecoveryConfig,
    GatewayProviderStreamItemTimeoutConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use pioneer_crud::{
    CliRuntimePendingRequestRecord,
    CliRuntimePendingRequestStatus as StoredCliRuntimePendingRequestStatus, ConversationEntry,
    CrudStore, NewCliRuntimePendingRequest, NewCliRuntimeThreadBinding,
    ResolveCliRuntimePendingRequest, TimeoutCandidate, TurnFinalizationCommitOutcome,
    TurnFinalizationReconciliationSummary,
};
use pioneer_hooks::{HookRecoveryOptions, HookRuntime};
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicMemvidBackend, thread_episodic_storage_uri_from_path,
};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ArtifactBindingDirection, ArtifactBindingKind,
    ArtifactCreatedByKind, ArtifactCreatedNotification, ArtifactKind, ArtifactRole,
    CLIAgentRuntimeKind, CLIRuntimeGetParams, CLIRuntimeGetResponse, CLIRuntimeListModelsParams,
    CLIRuntimeListModelsResponse, CLIRuntimeListParams, CLIRuntimeListResponse,
    CLIRuntimeLoginCancelParams, CLIRuntimeLoginCancelResponse, CLIRuntimeLoginStartParams,
    CLIRuntimeLoginStartResponse, CLIRuntimeLoginStartType, CLIRuntimePendingRequest,
    CLIRuntimePendingRequestStatus, CLIRuntimeProxyDeleteParams, CLIRuntimeProxyDeleteResponse,
    CLIRuntimeProxySetParams, CLIRuntimeProxySetResponse, CLIRuntimeRefreshParams,
    CLIRuntimeRefreshResponse, CLIRuntimeRequestKind, CLIRuntimeRequestOpenedNotification,
    CLIRuntimeRequestResolution, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams,
    CLIRuntimeStatusChangedNotification, CLIRuntimeStatusParams, CLIRuntimeStatusResponse,
    CLIRuntimeThreadBinding, CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadBindingGetResponse,
    CLIRuntimeThreadBindingManagement, CLIRuntimeThreadCompactParams, CLIRuntimeThreadForkParams,
    CLIRuntimeThreadForkResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    GatewayRemoteAccessStatusChangedNotification,
    GatewayThreadEpisodicVectorRefillStatusChangedNotification, GatewayVoiceInputSettings,
    GatewayVoiceInputStatusChangedNotification, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE,
    ItemCompletedNotification, ItemDeltaNotification, ItemDeltaStream, ItemStartedNotification,
    ItemTimeoutDetectedNotification, JSONRPC_VERSION, JsonRpcErrorResponse, JsonRpcNotification,
    JsonRpcRequest, JsonRpcResponse, MARKDOWN_AST_VERSION, METHOD_NOT_FOUND_CODE,
    McpAuditEventSummary, McpChangedAction, McpChangedItem, McpChangedNotification,
    McpDiagnosticLevel, McpInstallParams, McpInstallResponse, McpInstallResult,
    McpInstallResultStatus, McpInstallStatus, McpLifecycleAuditSummary, McpListItem, McpListParams,
    McpListResponse, McpManagementDetails, McpPolicySetParams, McpPolicySetResponse,
    McpPolicyState, McpPromptCatalogItem, McpResourceCatalogItem, McpResourceTemplateCatalogItem,
    McpRuntimeState, McpRuntimeStatus, McpServerCatalogDetails, McpServerDetailsParams,
    McpServerDetailsResponse, McpServerHealthDetails, McpServerPolicy, McpServerRestartParams,
    McpServerRestartResponse, McpServerStatus, McpSourceKind, McpToolAnnotationSummary,
    McpToolCatalogItem, McpTransportSummary, McpTurnBindingSummary, McpUninstallParams,
    McpUninstallResponse, McpValidationDiagnostic, PARSE_ERROR_CODE, ProviderConfigureParams,
    ProviderConfigureResponse, ProviderDefinition, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListModelsParams, ProviderListModelsResponse,
    ProviderListParams, ProviderListResponse, ProviderModelCapabilities, ProviderModelInfo,
    ProviderModelLimits, ProviderModelPricing, ProviderModelReasoningCapabilities,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, ProviderSummary,
    ProviderSummaryCapabilities, ReasoningCapabilitySource, RequestId, RuntimeAccountSnapshot,
    RuntimeCapabilities, RuntimeDiagnostic, RuntimeDiagnosticLevel, RuntimeModelInfo,
    RuntimeStatus, RuntimeSummary, SkillsUploadAbortParams, SkillsUploadFinishParams,
    SkillsUploadStartParams, SystemEventLevel, TaskAcceptParams, TaskAgendaParams,
    TaskCancelParams, TaskCreateParams, TaskDeliveriesParams, TaskDelivery, TaskDeliveryAttempt,
    TaskDeliveryMode, TaskDetachParams, TaskEventsParams, TaskFinishParams, TaskGetParams,
    TaskListParams, TaskPauseParams, TaskRescheduleParams, TaskResumeParams, TaskReviseParams,
    TaskTreeParams as TaskTreeTaskParams, TaskWaitParams, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocArchiveResponse, ThreadAgentsDocChangedNotification, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocPayload, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocResolvedPayload,
    ThreadAgentsDocSaveParams, ThreadAgentsDocSaveReason, ThreadAgentsDocSaveResponse,
    ThreadAgentsDocStatus, ThreadAgentsDocSummary, ThreadFolderCreateParams,
    ThreadFolderCreateResponse, ThreadFolderDeleteParams, ThreadFolderDeleteResponse,
    ThreadFolderMoveParams, ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse,
    ThreadMoveParams, ThreadMoveResponse, ThreadOriginKind, ThreadParticipantMutationParams,
    ThreadParticipantsListParams, ThreadSidebarVisibility, ThreadStartParams,
    ThreadTreeChangedNotification, ThreadTreeParams, ThreadTreeResponse, ThreadUnreadSummary,
    ThreadUnsubscribeParams, ThreadUpdateParams, ThreadUpdateResponse, ThreadUpdatedNotification,
    ThreadVisibility, ToolCallStatus, ToolStoragePayload, Turn, TurnBlockedNotification,
    TurnCancelParams, TurnCancelResponse, TurnCompletedNotification, TurnFailedNotification,
    TurnGetParams, TurnGetResponse, TurnItem, TurnItemEvent, TurnItemEventPayload, TurnItemType,
    TurnItemsParams, TurnMessageDeleteParams, TurnMessageEditParams,
    TurnMessageRevisionsPageParams, TurnPermissionApprovalRequest,
    TurnPermissionApprovalResolution, TurnPermissionRequestOpenedNotification,
    TurnPermissionRequestResolvedNotification, TurnPermissionRequestRespondParams,
    TurnPermissionRequestRespondResponse, TurnResumeParams, TurnResumeResponse, TurnStartParams,
    TurnStatus, VoiceError, VoiceErrorKind, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatus, VoiceStatusParams, VoiceStatusResponse, Workspace,
    WorkspaceChangeKind, WorkspaceChangedNotification, WorkspaceCreateParams,
    WorkspaceCreateResponse, WorkspaceDefaultParams, WorkspaceDefaultResponse, WorkspaceListParams,
    WorkspaceListResponse, WorkspaceSelectParams, WorkspaceSelectResponse, WorkspaceUpdateParams,
    WorkspaceUpdateResponse,
    constants::{events, methods},
    generate_id, sanitize_runtime_diagnostic_line, sanitize_runtime_diagnostic_lines,
    validate_voice_streaming_audio_format,
};
use pioneer_provider::{ChatMessage, ProviderRegistry};
use pioneer_runtime_events::{DurableCommitRejection, ExecutionEventHub};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteReadClass,
    SqliteWriteClass, is_anyhow_sqlite_transient_access, retry_with_backoff,
};
use pioneer_tasks::{TaskRuntime, TaskRuntimeConfig};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock, broadcast, oneshot};
use tokio::task::JoinHandle;

use pioneer_cli_agent_runtime::event::RuntimeEvent;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use crate::auth::GatewayAuthService;
use crate::authorization::{AccessChangeKind, AccessChangeSignal, AuthorizationInvalidationHub};
use crate::cli_runtime::command_heartbeat::CliRuntimeCommandHeartbeatTracker;
use crate::cli_runtime::manager::{CLIAgentRuntimeMachineRequestResponder, CLIAgentRuntimeManager};
use crate::cli_runtime::skills::{
    CliRuntimeSkillDestinationLocks, new_cli_runtime_skill_destination_locks,
};
use crate::mcp_service::McpService;
use crate::memory_runtime::GatewayMemoryRuntime;
use crate::request_context::RequestContext;
use crate::resilience::{
    RecoveryCoordinator, RecoveryPolicyRegistry, RecoveryTerminalOutcome, TimeoutPolicyRegistry,
    TimeoutSupervisor,
};
use crate::secrets::GatewaySecrets;
use crate::session::{ConnectionId, SessionManager};
use crate::thread::ThreadManager;
use crate::voice::session_buffer::GatewayVoiceSessionBufferStore;
use crate::voice::session_store::GatewayVoiceSessionStore;
use crate::voice::supervisor::VoiceInputSupervisor;
use crate::workspace::{WorkspaceError, WorkspaceManager};

#[cfg(test)]
static TEST_EPISODIC_STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn isolated_test_thread_episodic_storage_root(runtime_home: &std::path::Path) -> PathBuf {
    let sequence = TEST_EPISODIC_STORAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    runtime_home.join("memory").join(format!(
        "capsules-{}-{now_nanos}-{sequence}",
        std::process::id()
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthenticatedTransferOwner {
    pub(crate) principal_id: pioneer_protocol::PrincipalId,
    pub(crate) auth_session_id: pioneer_protocol::AuthSessionId,
    pub(crate) connection_id: ConnectionId,
}

impl AuthenticatedTransferOwner {
    pub(crate) fn from_request_context(context: &RequestContext) -> Self {
        Self {
            principal_id: context.principal().principal_id.clone(),
            auth_session_id: context.principal().session_id.clone(),
            connection_id: context.connection_id(),
        }
    }
}

impl From<&AuthenticatedTransferOwner> for AuthenticatedTransferOwner {
    fn from(owner: &AuthenticatedTransferOwner) -> Self {
        owner.clone()
    }
}

#[cfg(test)]
impl From<ConnectionId> for AuthenticatedTransferOwner {
    fn from(connection_id: ConnectionId) -> Self {
        Self {
            principal_id: pioneer_protocol::PrincipalId::new("P".repeat(21))
                .expect("test principal id"),
            auth_session_id: pioneer_protocol::AuthSessionId::new("S".repeat(21))
                .expect("test auth session id"),
            connection_id,
        }
    }
}

pub(crate) type MessageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Gateway handlers compose large protocol and CRUD futures; this is the explicit heap boundary.
pub(crate) fn message_future<'a, F, T>(future: F) -> MessageFuture<'a, T>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(future)
}

pub(crate) struct AbortOnDropMessageTask<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropMessageTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    pub(crate) async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("join handle should be present")
            .await;
        self.handle = None;
        result
    }

    pub(crate) async fn abort_and_join(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.abort();
            let _ = self.join().await;
        }
    }
}

impl<T> Drop for AbortOnDropMessageTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

// Heavy CRUD/projection calls otherwise inherit a deeply nested message-handler poll stack.
pub(crate) async fn message_fresh_task<F, T>(future: F) -> Result<T, tokio::task::JoinError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    spawn_message_task(future).join().await
}

fn spawn_message_task<F, T>(future: F) -> AbortOnDropMessageTask<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let startup_queued_at = std::time::Instant::now();
    let future = pioneer_observability::turn_startup::scope(
        pioneer_observability::turn_startup::current_key(),
        async move {
            pioneer_observability::turn_startup::record_current_duration(
                pioneer_observability::turn_startup::Stage::GatewayWorkerWait,
                startup_queued_at.elapsed(),
            );
            future.await
        },
    );
    // Test-local reporting scopes must follow the same owned request tasks as
    // the admission path. This does not install a global subscriber or change
    // production reporting; it only preserves the fixture's local capture.
    #[cfg(test)]
    let future = {
        use sentry::SentryFutureExt;
        use tracing::instrument::WithSubscriber;
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        future
            .with_subscriber(dispatch)
            .bind_hub(sentry::Hub::current())
    };
    let future = pioneer_observability::work::inherit(
        pioneer_observability::work::Boundary::MessageFreshTask,
        pioneer_observability::work::WorkKind::Message,
        pioneer_observability::work::ExecutorRole::Gateway,
        future,
    );
    AbortOnDropMessageTask::new(tokio::spawn(pioneer_sqlite::background::inherit_origin(
        future,
    )))
}

const RESILIENCE_WORKER_POLL_INTERVAL_SECONDS: u64 = 2;
pub(super) const TURN_EXECUTION_OWNER_LEASE_SECONDS: i64 = 15;
const RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS: u64 = 60;
const PROGRESS_DELTA_HEARTBEAT_INTERVAL_SECONDS: i64 = 5;
const PROGRESS_ITEM_TYPE_MISS_RETRY_SECONDS: i64 = 1;
const PROGRESS_ITEM_TYPE_MISS_CACHE_CAPACITY: usize = 1_024;
#[cfg(test)]
const CLI_RUNTIME_COMMAND_REHYDRATION_INTERVAL_SECONDS: i64 = 30;
const AGENT_ACTION_LEDGER_COMPACTION_RETRY_SECONDS: i64 = 60 * 60;
const NATIVE_TERMINAL_EFFECT_RETENTION_SECONDS: i64 = 30 * 24 * 60 * 60;
const NATIVE_TERMINAL_EFFECT_PURGE_RETRY_SECONDS: i64 = 60 * 60;
const NATIVE_TERMINAL_EFFECT_PURGE_BATCH_SIZE: u64 = 256;
const NATIVE_TERMINAL_EFFECT_RECOVERY_RETRY_SECONDS: i64 = 60;
const NATIVE_TERMINAL_EFFECT_RECOVERY_BATCH_SIZE: u64 = 64;

#[derive(Clone, Copy, Debug)]
pub(crate) struct MessageProcessorResilienceConfig {
    pub command_execution_timeout: GatewayCommandExecutionTimeoutConfig,
    pub provider_stream_item_timeout: GatewayProviderStreamItemTimeoutConfig,
    pub context_compaction_timeout: GatewayContextCompactionTimeoutConfig,
    pub context_compaction: GatewayContextCompactionConfig,
    pub cli_runtime_command_heartbeat: GatewayCliAgentRuntimeCommandHeartbeatConfig,
}

impl Default for MessageProcessorResilienceConfig {
    fn default() -> Self {
        Self {
            command_execution_timeout: GatewayCommandExecutionTimeoutConfig::default(),
            provider_stream_item_timeout: GatewayProviderStreamItemTimeoutConfig::default(),
            context_compaction_timeout: GatewayContextCompactionTimeoutConfig::default(),
            context_compaction: GatewayContextCompactionConfig::default(),
            cli_runtime_command_heartbeat: GatewayCliAgentRuntimeCommandHeartbeatConfig::default(),
        }
    }
}

async fn retry_transient_storage_access<T, F, Fut>(operation: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    retry_with_backoff(
        operation,
        is_anyhow_sqlite_transient_access,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResilienceWorkerFailureImpact {
    CriticalPoll,
    Maintenance,
}

fn record_resilience_worker_error(
    operation: &'static str,
    error: &anyhow::Error,
    impact: ResilienceWorkerFailureImpact,
) -> bool {
    if is_anyhow_sqlite_transient_access(error) {
        warn!(
            operation,
            error = %format!("{error:#}"),
            "resilience worker poll deferred by transient storage access failure"
        );
        impact == ResilienceWorkerFailureImpact::CriticalPoll
    } else {
        error!(
            operation,
            error = %format!("{error:#}"),
            "resilience worker poll failed"
        );
        false
    }
}

fn record_resilience_worker_poll_error(
    operation: &'static str,
    error: &anyhow::Error,
    transient_storage_poll_failed: &mut bool,
) {
    if record_resilience_worker_error(
        operation,
        error,
        ResilienceWorkerFailureImpact::CriticalPoll,
    ) {
        *transient_storage_poll_failed = true;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ThreadTitleJobState {
    Pending,
    Running,
    Succeeded,
    FailedRetriable,
    FailedNonRetriable,
    // Retain the original user text only while waiting for recovery. This is
    // not name provenance or a queue of completion triggers.
    DeferredUntilSuccessfulTurn(Arc<str>),
    AwaitingSuccessfulTurn(Arc<str>),
}

#[derive(Clone)]
struct UserTurnCancellationIntent {
    pending_requests: usize,
    reason: String,
    actor_id: Option<String>,
}

#[derive(Clone)]
pub struct MessageProcessor {
    thread_manager: Arc<ThreadManager>,
    agent_manager: Arc<AgentManager>,
    provider_registry: Arc<ProviderRegistry>,
    session_manager: Arc<SessionManager>,
    push_scopes: Arc<scoped_push::PushScopes>,
    authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
    execution_leases: Arc<crate::authorization::ExecutionLeaseRegistry>,
    observation_governor: Arc<crate::authorization::ObservationAdmissionGovernor>,
    turn_execution_owner_id: Arc<str>,
    artifact_stream_invalidation:
        Arc<StdRwLock<Option<Weak<dyn ArtifactStreamInvalidation>>>>,
    view_grant_service: Arc<StdRwLock<Option<Weak<crate::view_grants::ViewGrantService>>>>,
    auth_service: Option<Arc<GatewayAuthService>>,
    epic5_rate_limits: Arc<crate::epic5_observability::Epic5RateLimits>,
    cli_runtime_manager: Option<Arc<CLIAgentRuntimeManager>>,
    remote_access_supervisor: Option<Arc<pioneer_tunnel::RemoteAccessSupervisor>>,
    thread_episodic_vector_refill_status_tx: crate::database::startup::thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillStatusSender,
    thread_episodic_workspace_refill_supervisor:
        Arc<crate::database::startup::ThreadEpisodicWorkspaceRefillSupervisor>,
    workspace_manager: Arc<WorkspaceManager>,
    pub(crate) crud_store: Arc<CrudStore>,
    gateway_secrets: Arc<GatewaySecrets>,
    invitation_gateway_base_url: Arc<pioneer_protocol::GatewayBaseUrl>,
    gateway_listen_addr: Arc<std::sync::RwLock<Option<std::net::SocketAddr>>>,
    summary_config: Arc<summary::SummaryConfig>,
    compaction_settings: Arc<StdRwLock<pioneer_compaction::CompactionSettings>>,
    task_child_point_cursor: Arc<Mutex<tasks::ChildDiscoveryPass<(i64, String)>>>,
    task_child_scope_cursor: Arc<Mutex<tasks::ChildDiscoveryPass<(i64, String, String)>>>,
    completed_history_checks: Arc<Mutex<HashMap<(String, String), compaction_background::OwnedHistoryCheck>>>,
    compaction_lifecycle_not_before: Arc<StdRwLock<Option<tokio::time::Instant>>>,
    #[cfg(test)]
    completed_history_preparation_barrier:
        Arc<compaction_background::CompletedHistoryPreparationBarrier>,
    #[cfg(test)]
    turn_execution_heartbeat_barrier:
        Arc<compaction_background::CompletedHistoryPreparationBarrier>,
    #[cfg(test)]
    recovery_due_wait_barrier:
        Arc<compaction_background::CompletedHistoryPreparationBarrier>,
    #[cfg(test)]
    recovery_admission_failure: Arc<std::sync::Mutex<Option<anyhow::Error>>>,
    #[cfg(test)]
    task_history_preparation_failure: Arc<std::sync::Mutex<Option<anyhow::Error>>>,
    #[cfg(test)]
    task_cli_terminal_preparation_finished: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    task_cli_admission_failure: Arc<std::sync::Mutex<Option<turn_handlers::TurnStartFailure>>>,
    #[cfg(test)]
    task_cli_preparation_attempts: Arc<AtomicU64>,
    #[cfg(test)]
    task_cli_history_revalidation_failure: Arc<std::sync::Mutex<Option<anyhow::Error>>>,
    #[cfg(test)]
    task_cli_readiness_failure: Arc<std::sync::Mutex<Option<anyhow::Error>>>,
    #[cfg(test)]
    task_output_capture_failures: Arc<std::sync::Mutex<HashMap<String, anyhow::Error>>>,
    pub(crate) context_compaction_config: GatewayContextCompactionConfig,
    pub(crate) compaction_clock: Arc<dyn crate::compaction::CompactionClock>,
    pub(crate) compaction_coordinator: Arc<crate::compaction::ContextCompactionCoordinator>,
    cli_history_shutdown: tokio_util::sync::CancellationToken,
    #[cfg(test)]
    cli_transfer_capture_count: Arc<AtomicU64>,
    #[cfg(test)]
    cli_transfer_test_deadline: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    cli_history_monitor_drops: Arc<AtomicU64>,
    #[cfg(test)]
    cli_history_monitor_cleanup_drops: Arc<AtomicU64>,
    #[cfg(test)]
    predispatch_cli_turn_read_failures: Arc<Mutex<HashSet<String>>>,
    #[cfg(test)]
    claude_boundary_write_failures: Arc<Mutex<HashSet<String>>>,
    #[cfg(test)]
    native_cancellation_finish_barrier: Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
    #[cfg(test)]
    native_cancellation_materialization_failure: Arc<Mutex<Option<sea_orm::DbErr>>>,
    #[cfg(test)]
    native_cancellation_materialization_barrier: Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
    workspace_compaction_settings: Arc<StdRwLock<std::collections::BTreeMap<String, crate::settings::WorkspaceCompactionSettings>>>,
    agent_listener_tasks: Arc<Mutex<HashMap<String, AgentListenerTask>>>,
    agent_listener_generation: Arc<AtomicU64>,
    agent_message_buffers: Arc<Mutex<HashMap<String, AgentMarkdownBuffer>>>,
    #[cfg(test)]
    markdown_delta_parse_count: Arc<AtomicU64>,
    #[cfg(test)]
    progress_item_type_lookup_count: Arc<AtomicU64>,
    progress_items: Arc<Mutex<ProgressItemRegistry>>,
    agent_action_bindings:
        Arc<Mutex<HashMap<String, agent_action_tools::AgentActionRuntimeBinding>>>,
    parent_timeline_targets: Arc<Mutex<HashMap<String, agent_runtime::ParentTimelineTarget>>>,
    user_turn_cancel_intents: Arc<Mutex<HashMap<(String, String), UserTurnCancellationIntent>>>,
    cli_runtime_pending_turn_events:
        Arc<Mutex<HashMap<CLIRuntimePendingTurnEventKey, Vec<CLIRuntimePendingTurnEvent>>>>,
    cli_runtime_pending_turn_server_requests:
        Arc<Mutex<HashMap<CLIRuntimePendingTurnEventKey, Vec<CLIRuntimePendingTurnServerRequest>>>>,
    cli_runtime_machine_requests: Arc<Mutex<CLIRuntimeMachineRequestRegistry>>,
    cli_runtime_proxy_cache: Arc<Mutex<HashMap<(String, String), Option<String>>>>,
    provider_readiness: Arc<provider_readiness::ProviderReadinessSupervisor>,
    cli_runtime_pending_turn_activity_sequence: Arc<AtomicU64>,
    cli_runtime_event_hubs:
        Arc<Mutex<HashMap<crate::cli_runtime::session_instance::CliSessionInstanceId, Arc<ExecutionEventHub>>>>,
    cli_runtime_turn_binding_cache:
        Arc<Mutex<HashMap<CLIRuntimeNativeTurnKey, pioneer_crud::CliRuntimeTurnBindingRecord>>>,
    cli_runtime_session_turn_mutexes: Arc<
        Mutex<
            HashMap<
                crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
                turn_handlers::CliRuntimeSessionTurnLocks,
            >,
        >,
    >,
    cli_runtime_session_turn_leases:
        Arc<Mutex<HashMap<String, (tokio::sync::OwnedMutexGuard<()>, crate::cli_runtime::manager::CLIAgentRuntimeSessionHandle)>>>,
    cli_runtime_command_heartbeats: CliRuntimeCommandHeartbeatTracker,
    artifact_tool_states: Arc<Mutex<HashMap<String, Arc<ArtifactToolState>>>>,
    artifact_output_dirs: Arc<Mutex<HashMap<String, pioneer_artifacts::ArtifactOutputDir>>>,
    turn_final_assistant_texts: Arc<Mutex<HashMap<String, String>>>,
    native_permission_pending_requests:
        Arc<Mutex<HashMap<String, PendingNativePermissionApprovalRequest>>>,
    native_permission_approval_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    artifact_finalization_retry_turns: Arc<Mutex<HashSet<String>>>,
    title_job_runtime: Arc<Mutex<HashMap<String, ThreadTitleJobState>>>,
    thread_title_publication_mutexes: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    #[cfg(test)]
    thread_title_publication_barrier: Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
    #[cfg(test)]
    thread_title_publication_attempted: Arc<tokio::sync::Notify>,
    timeout_supervisor: Arc<TimeoutSupervisor>,
    recovery_coordinator: Arc<RecoveryCoordinator>,
    native_turn_event_delivery_workers: Arc<agent_runtime::NativeTurnEventDeliveryWorkers>,
    native_terminal_effect_kick_running: Arc<AtomicBool>,
    native_terminal_effect_kick_pending: Arc<AtomicBool>,
    native_lifecycle_readiness: Arc<native_health::NativeLifecycleReadinessState>,
    resilience_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    hook_recovery_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    task_event_listener_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    skills_watcher_worker: Arc<Mutex<Option<skills::watcher::SkillsWatcherWorker>>>,
    tool_loop_config: ToolLoopConfig,
    memory_loop_config: Arc<StdRwLock<MemoryLoopConfig>>,
    thread_episodic_runtime_config: Arc<StdRwLock<ThreadEpisodicRuntimeConfig>>,
    administration_snapshot_version: Arc<AtomicU64>,
    skills_snapshot_version: Arc<AtomicU64>,
    mcp_snapshot_version: Arc<AtomicU64>,
    mcp_service: Arc<McpService>,
    skills_write_lock: Arc<tokio::sync::Mutex<()>>,
    cli_runtime_skill_destination_locks: CliRuntimeSkillDestinationLocks,
    #[cfg(test)]
    cli_runtime_skill_preflight_test_events: Arc<Mutex<Vec<String>>>,
    #[cfg(test)]
    cli_mcp_readiness_overrides: [Option<pioneer_protocol::CliMcpAdapterReadiness>; 3],
    skill_upload_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    skill_upload_owners: Arc<Mutex<HashMap<String, AuthenticatedTransferOwner>>>,
    pub(crate) task_agent_executor: Arc<task_agent_executor::TaskAgentExecutor>,
    pub(crate) task_runtime: Arc<TaskRuntime>,
    memory_runtime: Arc<GatewayMemoryRuntime>,
    memory_bridge_providers: Arc<RwLock<Option<MemoryBridgeProviders>>>,
    hook_runtime: Arc<RwLock<Option<Arc<HookRuntime>>>>,
    hook_recovery_config: Arc<RwLock<GatewayHookRecoveryConfig>>,
    keepawake: Arc<GatewayKeepAwake>,
    artifact_runtime_home: PathBuf,
    thread_episodic_storage_root: PathBuf,
    pub(crate) artifact_service: Arc<ArtifactService>,
    artifact_uploads: Arc<artifacts::upload::ArtifactUploadSessionManager>,
    thread_episodic_ingestor: Arc<RwLock<Arc<dyn ThreadEpisodicIngestor>>>,
    thread_episodic_index_executor: Arc<ThreadEpisodicIndexExecutor>,
    thread_episodic_embedding_provider_resolver:
        Arc<ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver>,
    thread_episodic_recall_service: Arc<ThreadEpisodicRecallService>,
    voice_input_supervisor: Option<Arc<VoiceInputSupervisor>>,
    self_improvement_supervisor: Option<Arc<crate::self_improvement::supervisor::SelfImprovementSupervisor>>,
    gateway_settings_update_lock: Arc<Mutex<()>>,
    pub(crate) voice_finalizations: crate::voice::finalization::VoiceFinalizations,
    #[cfg(test)]
    pub(crate) voice_test_transcriber: Option<crate::voice::transcription::TestVoiceSpeechTranscriber>,
    pub(crate) voice_sessions: GatewayVoiceSessionStore,
    pub(crate) voice_session_buffers: GatewayVoiceSessionBufferStore,
}

struct AgentListenerTask {
    actor_generation: u64,
    listener_generation: u64,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct AgentMarkdownBuffer {
    turn_id: String,
    text: String,
    stats: Arc<AgentMarkdownBufferStats>,
}

impl AgentMarkdownBuffer {
    fn new(turn_id: String, text: String) -> Self {
        Self {
            turn_id,
            text,
            stats: Arc::new(AgentMarkdownBufferStats::default()),
        }
    }
}

#[derive(Default)]
struct AgentMarkdownBufferStats {
    delta_count: AtomicU64,
    cumulative_parse_input_bytes: AtomicU64,
    cumulative_parse_duration_ns: AtomicU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ProgressItemKey {
    turn_id: String,
    item_id: String,
}

#[derive(Clone, Copy, Debug)]
struct ProgressItemState {
    item_type: TurnItemType,
    last_persisted_heartbeat_at: Option<i64>,
    pending_heartbeat_claim: Option<u64>,
}

#[derive(Clone, Debug)]
struct ProgressHeartbeatClaim {
    key: ProgressItemKey,
    claim_id: u64,
    item_type: TurnItemType,
    previous_heartbeat_at: Option<i64>,
}

#[derive(Default)]
struct ProgressItemRegistry {
    items: HashMap<ProgressItemKey, ProgressItemState>,
    missing_item_types: HashMap<ProgressItemKey, i64>,
    next_heartbeat_claim: u64,
}

impl ProgressItemRegistry {
    fn item_type(&self, turn_id: &str, item_id: &str) -> Option<TurnItemType> {
        self.items
            .get(&ProgressItemKey {
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
            })
            .map(|state| state.item_type)
    }

    fn remember_item(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        persisted_heartbeat_at: Option<i64>,
    ) {
        let key = ProgressItemKey {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        self.missing_item_types.remove(&key);
        let state = self.items.entry(key).or_insert(ProgressItemState {
            item_type,
            last_persisted_heartbeat_at: persisted_heartbeat_at,
            pending_heartbeat_claim: None,
        });
        state.item_type = item_type;
        if let Some(persisted_heartbeat_at) = persisted_heartbeat_at {
            state.last_persisted_heartbeat_at = Some(
                state
                    .last_persisted_heartbeat_at
                    .map_or(persisted_heartbeat_at, |current| {
                        current.max(persisted_heartbeat_at)
                    }),
            );
            state.pending_heartbeat_claim = None;
        }
    }

    fn missing_item_type_is_fresh(&self, turn_id: &str, item_id: &str, now_unix: i64) -> bool {
        self.missing_item_types
            .get(&ProgressItemKey {
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
            })
            .is_some_and(|last_lookup_at| {
                now_unix.saturating_sub(*last_lookup_at) < PROGRESS_ITEM_TYPE_MISS_RETRY_SECONDS
            })
    }

    fn remember_missing_item_type(&mut self, turn_id: &str, item_id: &str, now_unix: i64) {
        self.missing_item_types.retain(|_, last_lookup_at| {
            now_unix.saturating_sub(*last_lookup_at) < PROGRESS_ITEM_TYPE_MISS_RETRY_SECONDS
        });
        let key = ProgressItemKey {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        if !self.missing_item_types.contains_key(&key)
            && self.missing_item_types.len() >= PROGRESS_ITEM_TYPE_MISS_CACHE_CAPACITY
            && let Some(oldest) = self
                .missing_item_types
                .iter()
                .min_by_key(|(_, last_lookup_at)| **last_lookup_at)
                .map(|(key, _)| key.clone())
        {
            self.missing_item_types.remove(&oldest);
        }
        self.missing_item_types.insert(key, now_unix);
    }

    fn claim_delta_heartbeat(
        &mut self,
        turn_id: &str,
        item_id: &str,
        now_unix: i64,
    ) -> Option<ProgressHeartbeatClaim> {
        let key = ProgressItemKey {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        let state = self.items.get(&key)?;
        if state.pending_heartbeat_claim.is_some()
            || state.last_persisted_heartbeat_at.is_some_and(|last| {
                now_unix.saturating_sub(last) < PROGRESS_DELTA_HEARTBEAT_INTERVAL_SECONDS
            })
        {
            return None;
        }
        let previous_heartbeat_at = state.last_persisted_heartbeat_at;
        let item_type = state.item_type;
        self.next_heartbeat_claim = self.next_heartbeat_claim.wrapping_add(1).max(1);
        let claim_id = self.next_heartbeat_claim;
        let state = self
            .items
            .get_mut(&key)
            .expect("progress item must remain present while claiming heartbeat");
        state.last_persisted_heartbeat_at = Some(now_unix);
        state.pending_heartbeat_claim = Some(claim_id);
        Some(ProgressHeartbeatClaim {
            key,
            claim_id,
            item_type,
            previous_heartbeat_at,
        })
    }

    fn finish_heartbeat_claim(&mut self, claim: &ProgressHeartbeatClaim, persisted: bool) {
        let Some(state) = self.items.get_mut(&claim.key) else {
            return;
        };
        if state.pending_heartbeat_claim != Some(claim.claim_id) {
            return;
        }
        state.pending_heartbeat_claim = None;
        if !persisted {
            state.last_persisted_heartbeat_at = claim.previous_heartbeat_at;
        }
    }

    fn forget_item(&mut self, turn_id: &str, item_id: &str) {
        let key = ProgressItemKey {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        self.items.remove(&key);
        self.missing_item_types.remove(&key);
    }

    fn forget_turn(&mut self, turn_id: &str) {
        self.items.retain(|key, _| key.turn_id != turn_id);
        self.missing_item_types
            .retain(|key, _| key.turn_id != turn_id);
    }
}

impl AgentMarkdownBufferStats {
    fn record_input(&self, source_bytes: usize) {
        self.delta_count.fetch_add(1, Ordering::Relaxed);
        self.cumulative_parse_input_bytes.fetch_add(
            u64::try_from(source_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn record_parse_duration(&self, elapsed: Duration) {
        self.cumulative_parse_duration_ns.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> (u64, u64, Duration) {
        (
            self.delta_count.load(Ordering::Relaxed),
            self.cumulative_parse_input_bytes.load(Ordering::Relaxed),
            Duration::from_nanos(self.cumulative_parse_duration_ns.load(Ordering::Relaxed)),
        )
    }
}

#[derive(Clone)]
struct MemoryBridgeProviders {
    memory_provider: Arc<crate::memory_tools::GatewayMemoryProvider>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CLIRuntimePendingTurnEventKey {
    workspace_id: String,
    runtime_id: String,
    thread_id: String,
    session_generation: u64,
    native_thread_id: String,
    native_turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CLIRuntimeNativeTurnKey {
    workspace_id: String,
    runtime_id: String,
    thread_id: String,
    session_generation: u64,
    native_thread_id: String,
    native_turn_id: String,
}

#[derive(Clone, Debug)]
struct CLIRuntimePendingTurnEvent {
    event: RuntimeEvent,
    received_at_unix_ms: i64,
    received_sequence: u64,
}

#[derive(Clone, Debug)]
struct CLIRuntimePendingTurnServerRequest {
    request: pioneer_cli_agent_runtime::codex::CodexJsonlRpcServerRequest,
    responder: CLIAgentRuntimeMachineRequestResponder,
    event: RuntimeEvent,
    received_at_unix_ms: i64,
    received_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CLIRuntimeMachineRequestKey {
    instance: crate::cli_runtime::session_instance::CliSessionInstanceId,
    provider_request_id_json: String,
}

#[derive(Clone, Debug)]
struct CLIRuntimePendingMachineRequest {
    pioneer_request_id: String,
    method: String,
    responder: CLIAgentRuntimeMachineRequestResponder,
    registered_sequence: u64,
}

#[derive(Default)]
struct CLIRuntimeMachineRequestRegistry {
    by_lane: HashMap<CLIRuntimeMachineRequestKey, CLIRuntimePendingMachineRequest>,
    by_pioneer_request_id: HashMap<String, CLIRuntimeMachineRequestKey>,
}

struct PendingNativePermissionApprovalRequest {
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    initiating_principal_id: pioneer_protocol::PrincipalId,
    initiating_session_id: pioneer_protocol::AuthSessionId,
    authorization_context_fingerprint: String,
    request: TurnPermissionApprovalRequest,
    respond_to: oneshot::Sender<pioneer_tools::PermissionApprovalResolution>,
}

#[derive(Default)]
struct TaskEventFanoutSummary {
    selected: usize,
    event_inputs: usize,
    emitted: usize,
    errors: usize,
    pending: Option<bool>,
    deadline: Option<i64>,
}

impl MessageProcessor {
    pub(crate) fn computer_use_config(&self) -> pioneer_tools::ComputerUseToolsConfig {
        self.tool_loop_config.computer_use.clone()
    }
    #[cfg(test)]
    pub fn new_with_memory_runtime(
        thread_manager: Arc<ThreadManager>,
        provider_registry: Arc<ProviderRegistry>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
        gateway_secrets: Arc<GatewaySecrets>,
        summary_config: summary::SummaryConfig,
        tool_loop_config: ToolLoopConfig,
        memory_runtime: Arc<GatewayMemoryRuntime>,
        runtime_home: PathBuf,
        artifacts_config: GatewayArtifactsConfig,
    ) -> Self {
        let thread_episodic_storage_root =
            isolated_test_thread_episodic_storage_root(&runtime_home);
        Self::new_with_memory_runtime_and_task_config(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_secrets,
            summary_config,
            tool_loop_config,
            memory_runtime,
            runtime_home,
            thread_episodic_storage_root,
            artifacts_config,
            TaskRuntimeConfig::default(),
            ThreadEpisodicRuntimeConfig::default(),
            MessageProcessorResilienceConfig::default(),
        )
    }

    pub(crate) fn new_with_memory_runtime_and_task_config(
        thread_manager: Arc<ThreadManager>,
        provider_registry: Arc<ProviderRegistry>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
        gateway_secrets: Arc<GatewaySecrets>,
        summary_config: summary::SummaryConfig,
        tool_loop_config: ToolLoopConfig,
        memory_runtime: Arc<GatewayMemoryRuntime>,
        runtime_home: PathBuf,
        thread_episodic_storage_root: PathBuf,
        artifacts_config: GatewayArtifactsConfig,
        task_runtime_config: TaskRuntimeConfig,
        thread_episodic_runtime_config: ThreadEpisodicRuntimeConfig,
        resilience_config: MessageProcessorResilienceConfig,
    ) -> Self {
        let now_snapshot = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        };
        let maintenance_crud_store = Arc::new(crud_store.with_maintenance_access());
        let mcp_snapshot_version = Arc::new(AtomicU64::new(0));
        let authorization_invalidation_hub =
            Arc::new(AuthorizationInvalidationHub::durable(crud_store.clone()));
        let execution_leases = Arc::new(crate::authorization::ExecutionLeaseRegistry::default());
        let observation_governor =
            Arc::new(crate::authorization::ObservationAdmissionGovernor::default());
        let turn_execution_owner_id: Arc<str> =
            format!("gateway-turn-owner-{}", generate_id(21)).into();
        let task_agent_executor = Arc::new(task_agent_executor::TaskAgentExecutor::new());
        let task_runtime = Arc::new(TaskRuntime::new_with_config(
            crud_store.clone(),
            task_runtime_config,
        ));
        let mcp_service = Arc::new(McpService::new(
            crud_store.clone(),
            session_manager.clone(),
            gateway_secrets.clone(),
            mcp_snapshot_version.clone(),
            authorization_invalidation_hub.clone(),
            execution_leases.clone(),
        ));
        let normalized_tool_loop_config = tool_loop_config.normalized();
        let memory_loop_config =
            Arc::new(StdRwLock::new(normalized_tool_loop_config.memory.clone()));
        let agent_manager = Arc::new(AgentManager::new_with_mcp(
            provider_registry.clone(),
            normalized_tool_loop_config.clone(),
            Some(mcp_service.clone()),
        ));
        let timeout_supervisor = Arc::new(TimeoutSupervisor::new(
            crud_store.clone(),
            TimeoutPolicyRegistry::with_configured_timeout_policies(
                normalized_tool_loop_config.provider,
                resilience_config.command_execution_timeout,
                resilience_config.provider_stream_item_timeout,
                resilience_config.context_compaction_timeout,
            ),
        ));
        let recovery_coordinator = Arc::new(
            RecoveryCoordinator::new(
                crud_store.clone(),
                agent_manager.clone(),
                provider_registry.clone(),
                RecoveryPolicyRegistry::with_command_execution_timeout_config(
                    resilience_config.command_execution_timeout,
                ),
                normalized_tool_loop_config.clone(),
            )
            .with_authorization_invalidation_hub(authorization_invalidation_hub.clone())
            .with_execution_leases(execution_leases.clone())
            .with_turn_execution_owner_id(turn_execution_owner_id.clone()),
        );
        let cli_runtime_command_heartbeats = CliRuntimeCommandHeartbeatTracker::new(
            resilience_config
                .cli_runtime_command_heartbeat
                .interval_secs,
        );
        let artifact_service = Arc::new(ArtifactService::new_with_policies(
            crud_store.clone(),
            Arc::new(LocalArtifactBlobStore::new(runtime_home.clone())),
            artifact_quota_policy_from_config(&artifacts_config),
            ArtifactGcPolicy {
                grace_secs: artifacts_config.gc_grace_secs,
                output_dir_ttl_secs: artifacts_config.output_dir_ttl_secs,
                readable_copy_ttl_secs: artifacts_config.readable_copy_ttl_secs,
            },
        ));
        let artifact_uploads = Arc::new(artifacts::upload::ArtifactUploadSessionManager::new(
            runtime_home.join("artifacts").join("upload_sessions"),
        ));
        let thread_episodic_backend: Arc<dyn ThreadEpisodicMemvidBackend> =
            Arc::new(MemvidThreadEpisodicBackend::new());
        let thread_episodic_embedding_provider_resolver = Arc::new(
            ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver::new(
                provider_registry.clone(),
                runtime_home.clone(),
                thread_episodic_runtime_config.vector_search.clone(),
            ),
        );
        let thread_episodic_workspace_refill_supervisor =
            Arc::new(crate::database::startup::ThreadEpisodicWorkspaceRefillSupervisor::default());
        let thread_episodic_index_executor = Arc::new(
            ThreadEpisodicIndexExecutor::new(
                maintenance_crud_store.clone(),
                thread_episodic_backend.clone(),
                Arc::new(RuntimeVectorThreadEpisodicIndexPayloadProvider::new(
                    Arc::new(StoreThreadEpisodicIndexPayloadProvider::new(
                        maintenance_crud_store.clone(),
                        thread_episodic_storage_uri_from_path(
                            thread_episodic_storage_root.as_path(),
                        ),
                    )),
                    thread_episodic_embedding_provider_resolver.clone(),
                    maintenance_crud_store,
                )),
            )
            .with_projection_runtime(
                thread_episodic_storage_root.clone(),
                thread_episodic_embedding_provider_resolver.clone(),
                thread_episodic_workspace_refill_supervisor.clone(),
            ),
        );
        thread_episodic_index_executor.apply_config(thread_episodic_runtime_config.index_executor);
        thread_episodic_index_executor.set_indexing_enabled(
            thread_episodic_runtime_config.enabled
                && thread_episodic_runtime_config.indexing_enabled,
        );
        let thread_episodic_recall_embedding_provider_resolver: Arc<
            dyn ThreadEpisodicIndexEmbeddingProviderResolver,
        > = thread_episodic_embedding_provider_resolver.clone();
        let thread_episodic_recall_service = Arc::new(
            ThreadEpisodicRecallService::with_embedding_provider_resolver(
                crud_store.clone(),
                thread_episodic_backend,
                Some(thread_episodic_recall_embedding_provider_resolver),
            ),
        );
        thread_episodic_recall_service
            .apply_config(thread_episodic_runtime_config.recall_service.clone());
        let thread_episodic_ingestion_enabled = thread_episodic_runtime_config.enabled
            && thread_episodic_runtime_config.indexing_enabled;
        let (thread_episodic_vector_refill_status_tx, _) = broadcast::channel(64);

        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            push_scopes: Arc::new(scoped_push::PushScopes::default()),
            authorization_invalidation_hub,
            execution_leases,
            observation_governor,
            turn_execution_owner_id,
            artifact_stream_invalidation: Arc::new(StdRwLock::new(None)),
            view_grant_service: Arc::new(StdRwLock::new(None)),
            auth_service: None,
            epic5_rate_limits: Arc::new(crate::epic5_observability::Epic5RateLimits::default()),
            cli_runtime_manager: None,
            remote_access_supervisor: None,
            thread_episodic_vector_refill_status_tx,
            thread_episodic_workspace_refill_supervisor,
            workspace_manager,
            crud_store: crud_store.clone(),
            gateway_secrets,
            gateway_listen_addr: Arc::new(std::sync::RwLock::new(None)),
            invitation_gateway_base_url: Arc::new(
                pioneer_protocol::GatewayBaseUrl::parse_presentation("http://127.0.0.1:17878")
                    .expect("static Gateway base URL is valid"),
            ),
            task_child_point_cursor: Arc::new(Mutex::new(Default::default())),
            task_child_scope_cursor: Arc::new(Mutex::new(Default::default())),
            completed_history_checks: compaction_background::shared_history_jobs(&crud_store),
            native_turn_event_delivery_workers: agent_runtime::shared_delivery_workers(&crud_store),
            compaction_lifecycle_not_before: Arc::new(StdRwLock::new(None)),
            #[cfg(test)]
            completed_history_preparation_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            turn_execution_heartbeat_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            recovery_due_wait_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            recovery_admission_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_history_preparation_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_terminal_preparation_finished: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            task_cli_admission_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_preparation_attempts: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            task_cli_history_revalidation_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_readiness_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_output_capture_failures: Arc::new(std::sync::Mutex::new(HashMap::new())),
            context_compaction_config: resilience_config.context_compaction,
            compaction_clock: Arc::new(crate::compaction::SystemCompactionClock::default()),
            compaction_coordinator: crate::compaction::ContextCompactionCoordinator::for_store(
                &crud_store,
            ),
            cli_history_shutdown: tokio_util::sync::CancellationToken::new(),
            #[cfg(test)]
            cli_transfer_capture_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cli_transfer_test_deadline: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cli_history_monitor_drops: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cli_history_monitor_cleanup_drops: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            predispatch_cli_turn_read_failures: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(test)]
            claude_boundary_write_failures: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(test)]
            native_cancellation_finish_barrier: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            native_cancellation_materialization_failure: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            native_cancellation_materialization_barrier: Arc::new(Mutex::new(None)),
            workspace_compaction_settings: Arc::new(StdRwLock::new(
                std::collections::BTreeMap::new(),
            )),
            compaction_settings: Arc::new(StdRwLock::new(pioneer_compaction::CompactionSettings {
                selection: if summary_config.summary_model.is_some()
                    || summary_config.summary_model_provider.is_some()
                {
                    Some(pioneer_compaction::ModelSelection {
                        transport: pioneer_compaction::Transport::Api,
                        instance: summary_config
                            .summary_model_provider
                            .clone()
                            .unwrap_or_default(),
                        model: summary_config.summary_model.clone().unwrap_or_default(),
                        effort: None,
                    })
                } else {
                    None
                },
            })),
            summary_config: Arc::new(summary_config),
            agent_listener_tasks: Arc::new(Mutex::new(HashMap::new())),
            agent_listener_generation: Arc::new(AtomicU64::new(1)),
            agent_message_buffers: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            markdown_delta_parse_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            progress_item_type_lookup_count: Arc::new(AtomicU64::new(0)),
            progress_items: Arc::new(Mutex::new(ProgressItemRegistry::default())),
            agent_action_bindings: Arc::new(Mutex::new(HashMap::new())),
            parent_timeline_targets: Arc::new(Mutex::new(HashMap::new())),
            user_turn_cancel_intents: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_pending_turn_events: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_pending_turn_server_requests: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_machine_requests: Arc::new(Mutex::new(
                CLIRuntimeMachineRequestRegistry::default(),
            )),
            cli_runtime_proxy_cache: Arc::new(Mutex::new(HashMap::new())),
            provider_readiness: Arc::new(provider_readiness::ProviderReadinessSupervisor::default()),
            cli_runtime_pending_turn_activity_sequence: Arc::new(AtomicU64::new(0)),
            cli_runtime_event_hubs: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_turn_binding_cache: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_session_turn_mutexes: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_session_turn_leases: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_command_heartbeats,
            artifact_tool_states: Arc::new(Mutex::new(HashMap::new())),
            artifact_output_dirs: Arc::new(Mutex::new(HashMap::new())),
            turn_final_assistant_texts: Arc::new(Mutex::new(HashMap::new())),
            native_permission_pending_requests: Arc::new(Mutex::new(HashMap::new())),
            native_permission_approval_worker: Arc::new(Mutex::new(None)),
            artifact_finalization_retry_turns: Arc::new(Mutex::new(HashSet::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            thread_title_publication_mutexes: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            thread_title_publication_barrier: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            thread_title_publication_attempted: Arc::new(tokio::sync::Notify::new()),
            timeout_supervisor,
            recovery_coordinator,
            native_terminal_effect_kick_running: Arc::new(AtomicBool::new(false)),
            native_terminal_effect_kick_pending: Arc::new(AtomicBool::new(false)),
            native_lifecycle_readiness: Arc::new(
                native_health::NativeLifecycleReadinessState::default(),
            ),
            resilience_worker: Arc::new(Mutex::new(None)),
            hook_recovery_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config: normalized_tool_loop_config,
            memory_loop_config,
            thread_episodic_runtime_config: Arc::new(StdRwLock::new(
                thread_episodic_runtime_config,
            )),
            administration_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            cli_runtime_skill_destination_locks: new_cli_runtime_skill_destination_locks(),
            #[cfg(test)]
            cli_runtime_skill_preflight_test_events: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            cli_mcp_readiness_overrides: [None, None, None],
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            skill_upload_owners: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
            memory_runtime,
            memory_bridge_providers: Arc::new(RwLock::new(None)),
            hook_runtime: Arc::new(RwLock::new(None)),
            hook_recovery_config: Arc::new(RwLock::new(GatewayHookRecoveryConfig::default())),
            keepawake: Arc::new(GatewayKeepAwake::default()),
            artifact_runtime_home: runtime_home,
            thread_episodic_storage_root,
            artifact_service,
            artifact_uploads,
            thread_episodic_ingestor: Arc::new(RwLock::new(Arc::new(
                StoreThreadEpisodicIngestor::with_config(
                    crud_store,
                    thread_episodic_ingestion_enabled,
                ),
            ))),
            thread_episodic_index_executor,
            thread_episodic_embedding_provider_resolver,
            thread_episodic_recall_service,
            voice_input_supervisor: None,
            self_improvement_supervisor: None,
            gateway_settings_update_lock: Arc::new(Mutex::new(())),
            voice_finalizations: Default::default(),
            #[cfg(test)]
            voice_test_transcriber: None,
            voice_sessions: GatewayVoiceSessionStore::default(),
            voice_session_buffers: GatewayVoiceSessionBufferStore::default(),
        }
    }

    pub async fn set_hook_recovery_config(&self, config: GatewayHookRecoveryConfig) {
        *self.hook_recovery_config.write().await = config;
        self.crud_store
            .background_work("hooks.recovery")
            .invalidate();
    }

    pub(crate) fn with_remote_access_supervisor(
        mut self,
        supervisor: Arc<pioneer_tunnel::RemoteAccessSupervisor>,
    ) -> Self {
        self.remote_access_supervisor = Some(supervisor);
        self
    }

    pub(crate) fn with_voice_input_supervisor(
        mut self,
        supervisor: Arc<VoiceInputSupervisor>,
    ) -> Self {
        self.voice_input_supervisor = Some(supervisor);
        self
    }

    pub(crate) fn with_self_improvement_supervisor(
        mut self,
        supervisor: Arc<crate::self_improvement::supervisor::SelfImprovementSupervisor>,
    ) -> Self {
        self.self_improvement_supervisor = Some(supervisor);
        self
    }

    pub fn start_remote_access_status_notifications(self: &Arc<Self>) {
        let Some(supervisor) = self.remote_access_supervisor.as_ref() else {
            return;
        };
        let mut status_rx = supervisor.subscribe_status();
        let processor = Arc::downgrade(self);
        let _status_notifications = tokio::spawn(async move {
            let mut previous = status_rx.borrow().clone();

            loop {
                if status_rx.changed().await.is_err() {
                    break;
                }

                let Some(processor) = processor.upgrade() else {
                    break;
                };
                let status = match processor
                    .persist_current_remote_access_public_base_url()
                    .await
                {
                    Ok(status) => status,
                    Err(_) => {
                        tracing::warn!("failed to persist current relay public address");
                        processor
                            .remote_access_supervisor
                            .as_ref()
                            .expect("remote access supervisor")
                            .status_snapshot()
                    }
                };
                if status == previous {
                    continue;
                }
                previous = status.clone();

                processor
                    .send_gateway_management_notification(
                        events::GATEWAY_REMOTE_ACCESS_STATUS_CHANGED,
                        &GatewayRemoteAccessStatusChangedNotification { status },
                    )
                    .await;
            }
        });
    }

    pub fn start_voice_input_status_notifications(self: &Arc<Self>) {
        let Some(supervisor) = self.voice_input_supervisor.as_ref() else {
            return;
        };
        let mut status_rx = supervisor.subscribe_settings();
        let initial_settings = status_rx.borrow().clone();
        let processor = Arc::downgrade(self);
        let _status_notifications = tokio::spawn(async move {
            const PROGRESS_THROTTLE: Duration = Duration::from_millis(250);
            let mut previous = initial_settings;
            let mut last_notification = tokio::time::Instant::now() - PROGRESS_THROTTLE;

            loop {
                if status_rx.changed().await.is_err() {
                    break;
                }
                let mut settings = status_rx.borrow_and_update().clone();
                if settings == previous {
                    continue;
                }

                if !voice_input_semantic_status_changed(&previous, &settings)
                    && last_notification.elapsed() < PROGRESS_THROTTLE
                {
                    let deadline = last_notification + PROGRESS_THROTTLE;
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep_until(deadline) => break,
                            changed = status_rx.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let latest = status_rx.borrow_and_update().clone();
                                let semantic_change =
                                    voice_input_semantic_status_changed(&previous, &latest);
                                settings = latest;
                                if semantic_change {
                                    break;
                                }
                            }
                        }
                    }
                }

                if settings == previous {
                    continue;
                }
                let Some(processor) = processor.upgrade() else {
                    break;
                };
                // Serialize with settings saves so an older engine event cannot
                // overwrite a newly persisted session mode on another client.
                let _settings_guard = processor.gateway_settings_update_lock.lock().await;
                settings = processor
                    .voice_input_supervisor
                    .as_ref()
                    .map(|supervisor| supervisor.settings_snapshot())
                    .unwrap_or_default();
                if processor
                    .apply_persisted_voice_input_mode(&mut settings)
                    .is_err()
                {
                    warn!("failed to read Voice Input mode for status publication");
                    continue;
                }
                processor
                    .send_gateway_management_notification(
                        events::GATEWAY_VOICE_INPUT_STATUS_CHANGED,
                        &GatewayVoiceInputStatusChangedNotification {
                            settings: settings.clone(),
                        },
                    )
                    .await;
                previous = settings;
                last_notification = tokio::time::Instant::now();
            }
        });
    }

    fn apply_persisted_voice_input_mode(
        &self,
        settings: &mut pioneer_protocol::GatewayVoiceInputSettings,
    ) -> anyhow::Result<()> {
        #[cfg(not(test))]
        let config = pioneer_config::AppConfig::load()?;
        #[cfg(test)]
        let config = crate::isolated_test_app_config()?;
        let file_name = crate::settings::normalize_settings_file_name(
            config.gateway.settings_file_name.as_str(),
        )?;
        let persisted = crate::settings::load_or_create_gateway_settings(
            &self.artifact_runtime_home.join(&file_name),
            config.gateway.settings_version,
            &file_name,
        )?;
        settings.transcribe_while_recording = persisted
            .effective_voice_input_config(&config.gateway.voice)
            .transcribe_while_recording;
        Ok(())
    }

    pub(crate) fn thread_episodic_vector_refill_status_sender(
        &self,
    ) -> crate::database::startup::thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillStatusSender{
        self.thread_episodic_vector_refill_status_tx.clone()
    }

    async fn publish_committed_authorization_invalidation(
        &self,
        kind: AccessChangeKind,
        affected_principal_id: Option<pioneer_protocol::PrincipalId>,
        workspace_id: impl Into<String>,
        thread_id: Option<String>,
    ) -> AccessChangeSignal {
        let signal = self
            .authorization_invalidation_hub
            .publish(kind, affected_principal_id, workspace_id, thread_id)
            .await
            .expect("committed ACL change must advance durable policy generation");
        self.apply_committed_authorization_invalidation(&signal)
            .await;
        signal
    }

    /// Data notifications have their own sequence. Profile and invitation changes
    /// do not change role grants or ACLs and must not advance the policy fence.
    fn next_administration_revision(&self) -> u64 {
        self.administration_snapshot_version
            .fetch_add(1, Ordering::SeqCst)
            .checked_add(1)
            .expect("administration revision exhausted")
    }

    #[cfg(test)]
    /// Simulates a committed membership mutation in lifecycle tests.
    pub(crate) async fn publish_committed_workspace_membership_invalidation(
        &self,
        affected_principal_id: pioneer_protocol::PrincipalId,
        workspace_id: impl Into<String>,
    ) -> AccessChangeSignal {
        self.publish_committed_authorization_invalidation(
            AccessChangeKind::WorkspaceMembership,
            Some(affected_principal_id),
            workspace_id,
            None,
        )
        .await
    }

    pub(crate) async fn shutdown_thread_episodic_index_runner(&self) {
        self.thread_episodic_index_executor.shutdown().await;
    }

    pub(crate) fn thread_episodic_workspace_refill_supervisor(
        &self,
    ) -> Arc<crate::database::startup::ThreadEpisodicWorkspaceRefillSupervisor> {
        self.thread_episodic_workspace_refill_supervisor.clone()
    }

    pub fn start_thread_episodic_vector_refill_status_notifications(self: &Arc<Self>) {
        let mut status_rx = self.thread_episodic_vector_refill_status_tx.subscribe();
        let processor = Arc::downgrade(self);
        let _status_notifications = tokio::spawn(async move {
            loop {
                let event = match status_rx.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            "thread episodic vector refill status notification receiver lagged"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };

                let Some(processor) = processor.upgrade() else {
                    break;
                };
                let workspace_id = event.workspace_id;
                let notification = GatewayThreadEpisodicVectorRefillStatusChangedNotification {
                    workspace_id: workspace_id.clone(),
                    status: event.status,
                    local_model_status: event.local_model_status,
                    downloaded_bytes: event.downloaded_bytes,
                    total_bytes: event.total_bytes,
                };
                processor
                    .send_gateway_management_notification(
                        events::GATEWAY_THREAD_EPISODIC_VECTOR_REFILL_STATUS_CHANGED,
                        &notification,
                    )
                    .await;
            }
        });
    }

    pub async fn shutdown_remote_access_supervisor(&self) {
        let Some(supervisor) = self.remote_access_supervisor.as_ref() else {
            return;
        };
        supervisor.shutdown().await;
    }

    pub async fn shutdown_cli_runtime_manager(&self) {
        self.cli_history_shutdown.cancel();
        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            return;
        };
        match manager.close_all().await {
            Ok(closed) if closed > 0 => {
                info!(
                    closed,
                    "closed CLI runtime sessions during gateway shutdown"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to close CLI runtime sessions during gateway shutdown"
                );
            }
        }
    }

    /// Close ingress slots, suppress unclaimed outcomes, and drain the bounded
    /// workers before database shutdown. Native calls retain ownership to completion.
    pub async fn shutdown_voice_finalizations(&self) {
        self.voice_finalizations.close();
        self.voice_session_buffers.cancel_all();
        self.voice_finalizations.tasks.wait().await;
    }

    pub async fn shutdown_mcp_service(&self) {
        self.mcp_service.shutdown().await;
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn cli_runtime_manager_enabled(&self) -> bool {
        self.cli_runtime_manager.is_some()
    }

    pub(crate) fn with_cli_runtime_manager(mut self, manager: Arc<CLIAgentRuntimeManager>) -> Self {
        self.cli_runtime_manager = Some(manager);
        #[cfg(test)]
        if self.tool_loop_config.skills.system_roots.is_empty() {
            // CLI fixtures need the same mandatory canonical orchestration
            // packages as production. Keep all materialization in a test tempdir.
            static SYSTEM_SKILLS: std::sync::OnceLock<(tempfile::TempDir, Vec<String>)> =
                std::sync::OnceLock::new();
            let (_, roots) = SYSTEM_SKILLS.get_or_init(|| {
                let root = tempfile::tempdir().expect("CLI system skill fixture root");
                let roots =
                    crate::system_skills::materialize_bundled_system_skill_roots(root.path())
                        .expect("canonical CLI system skills");
                (root, roots)
            });
            self.tool_loop_config.skills.system_roots = roots.clone();
        }
        self
    }

    pub(crate) fn with_auth_service(mut self, service: Arc<GatewayAuthService>) -> Self {
        self.mcp_service.set_auth_service(service.clone());
        self.epic5_rate_limits = service.epic5_rate_limits();
        self.auth_service = Some(service);
        self
    }

    pub(crate) fn member_service(&self) -> crate::member::MemberService {
        crate::member::MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        )
    }

    pub(crate) fn set_artifact_stream_invalidation(
        &self,
        invalidation: Arc<dyn ArtifactStreamInvalidation>,
    ) {
        *self
            .artifact_stream_invalidation
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::downgrade(&invalidation));
    }

    pub(crate) fn set_view_grant_service(
        &self,
        service: Arc<crate::view_grants::ViewGrantService>,
    ) {
        *self
            .view_grant_service
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(&service));
    }

    pub(in crate::message) fn view_grant_service(
        &self,
    ) -> Option<Arc<crate::view_grants::ViewGrantService>> {
        self.view_grant_service
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    pub(crate) fn cancel_artifact_streams(&self, workspace_id: &str, artifact_id: &str) -> usize {
        self.artifact_stream_invalidation()
            .map(|invalidation| invalidation.cancel_artifact(workspace_id, artifact_id))
            .unwrap_or(0)
    }

    pub(crate) fn cancel_artifact_streams_for_access_change(
        &self,
        signal: &AccessChangeSignal,
    ) -> usize {
        self.artifact_stream_invalidation()
            .map(|invalidation| invalidation.cancel_authorization_signal(signal))
            .unwrap_or(0)
    }

    fn artifact_stream_invalidation(&self) -> Option<Arc<dyn ArtifactStreamInvalidation>> {
        self.artifact_stream_invalidation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    pub(crate) fn with_invitation_gateway_base_url(
        mut self,
        gateway_base_url: pioneer_protocol::GatewayBaseUrl,
    ) -> Self {
        self.invitation_gateway_base_url = Arc::new(gateway_base_url);
        self
    }

    pub(crate) fn cli_turn_mcp_invoker(&self) -> Arc<dyn crate::turn_mcp::invoker::TurnMcpInvoker> {
        Arc::new(self.mcp_service.turn_mcp_invoker())
    }

    pub(crate) fn with_cli_mcp_limits(
        self,
        limits: crate::cli_runtime::mcp::limits::CliMcpRuntimeLimits,
    ) -> Self {
        self.mcp_service
            .configure_projection_limits(limits.max_tools(), limits.max_total_schema_bytes());
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cli_runtime_manager_for_tests(
        self,
        manager: Arc<CLIAgentRuntimeManager>,
    ) -> Self {
        self.with_cli_runtime_manager(manager)
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_home_for_tests(mut self, runtime_home: PathBuf) -> Self {
        self.artifact_runtime_home = runtime_home;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cli_mcp_readiness_override_for_tests(
        mut self,
        readiness: pioneer_protocol::CliMcpAdapterReadiness,
    ) -> Self {
        let slot = match readiness.injection {
            pioneer_protocol::CliMcpInjectionKind::CodexManagedStdioMcp => 0,
            pioneer_protocol::CliMcpInjectionKind::ClaudeStrictStdioMcp => 1,
            pioneer_protocol::CliMcpInjectionKind::OpenCodeDynamicStdioMcp => 2,
        };
        self.cli_mcp_readiness_overrides[slot] = Some(readiness);
        self
    }

    #[cfg(test)]
    pub(crate) fn cli_mcp_readiness_override_for_tests(
        &self,
        runtime_kind: pioneer_protocol::CLIAgentRuntimeKind,
    ) -> Option<pioneer_protocol::CliMcpAdapterReadiness> {
        let slot = match runtime_kind {
            pioneer_protocol::CLIAgentRuntimeKind::Codex => 0,
            pioneer_protocol::CLIAgentRuntimeKind::Claude => 1,
            pioneer_protocol::CLIAgentRuntimeKind::OpenCode => 2,
        };
        self.cli_mcp_readiness_overrides[slot].clone()
    }

    #[cfg(test)]
    pub(crate) fn cli_runtime_skill_preflight_test_events(&self) -> Arc<Mutex<Vec<String>>> {
        self.cli_runtime_skill_preflight_test_events.clone()
    }

    #[cfg(test)]
    pub(crate) async fn set_thread_episodic_ingestor_for_test(
        &self,
        ingestor: Arc<dyn ThreadEpisodicIngestor>,
    ) {
        *self.thread_episodic_ingestor.write().await = ingestor;
    }

    pub async fn ensure_hook_runtime_with_run_store(&self) {
        let existing_runtime = self.hook_runtime.read().await.clone();

        if let Some(runtime) = existing_runtime {
            match GatewayHookRuntimeBuilder::from_runtime(self.crud_store.clone(), runtime.as_ref())
                .with_crud_run_store()
                .install(agents_doc_prompt_hook_package(self.crud_store.clone()))
            {
                Ok(builder) => {
                    let runtime = builder.build();
                    *self.hook_runtime.write().await = Some(runtime.clone());
                    self.crud_store
                        .background_work("hooks.recovery")
                        .invalidate();
                    self.agent_manager.set_hook_runtime(Some(runtime)).await;
                }
                Err(error) => {
                    error!(error = %error, "failed to install AGENTS.md prompt hook package");
                }
            }
            return;
        }

        match GatewayHookRuntimeBuilder::new(self.crud_store.clone())
            .with_crud_run_store()
            .install(agents_doc_prompt_hook_package(self.crud_store.clone()))
        {
            Ok(builder) => {
                let runtime = builder.build();
                *self.hook_runtime.write().await = Some(runtime.clone());
                self.crud_store
                    .background_work("hooks.recovery")
                    .invalidate();
                self.agent_manager.set_hook_runtime(Some(runtime)).await;
            }
            Err(error) => {
                error!(error = %error, "failed to build hook runtime");
            }
        }
    }

    pub(crate) async fn cli_task_mutation_cache(
        &self,
        turn_id: &str,
    ) -> Arc<crate::task_tools::TaskToolMutationCache> {
        self.mcp_service.cli_task_mutation_cache(turn_id).await
    }

    pub async fn bind_task_bridge(self: &Arc<Self>) {
        self.task_agent_executor.bind(Arc::downgrade(self));
        self.task_runtime
            .register_executor(self.task_agent_executor.clone())
            .await;
        self.agent_manager
            .set_task_tool_provider(Some(Arc::new(
                crate::task_tools::GatewayTaskToolProvider::new(Arc::downgrade(self)),
            )))
            .await;
    }

    pub async fn bind_agent_tool_bridges(self: &Arc<Self>) {
        self.tool_loop_config
            .computer_use
            .image_admission
            .set(Arc::new(artifact_tools::GatewayCuaImageSink(
                Arc::downgrade(self),
            )));
        self.bind_permission_approval_bridge().await;
        self.bind_task_bridge().await;
        self.bind_artifact_tool_bridge().await;
    }

    pub(crate) async fn bind_context_compaction(self: &Arc<Self>) {
        self.agent_manager
            .set_context_controller(Some(Arc::new(
                crate::compaction::GatewayNativeContextController::new(Arc::downgrade(self)),
            )))
            .await;
        self.crud_store.invalidate_compaction_history_dependencies();
    }

    /// Resolve the database-backed launch catalog on an isolated Tokio task.
    /// Task scheduling and turn admission compose large futures, so catalog
    /// projection must not inherit their worker stack.
    pub(crate) async fn prepare_agent_action_binding(
        self: &Arc<Self>,
        turn_id: String,
        mut binding: agent_action_tools::AgentActionRuntimeBinding,
    ) -> anyhow::Result<agent_action_tools::AgentActionRuntimeBinding> {
        let processor = Arc::clone(self);
        message_fresh_task(async move {
            binding
                .refresh_start_options_catalog(processor.as_ref(), turn_id.as_str())
                .await?;
            Ok(binding)
        })
        .await
        .map_err(|error| anyhow::anyhow!("agent action binding preparation task failed: {error}"))?
    }

    /// Publish a fully prepared runtime binding without database or projection work.
    pub(crate) async fn register_agent_action_binding(
        &self,
        turn_id: impl Into<String>,
        binding: agent_action_tools::AgentActionRuntimeBinding,
    ) {
        self.agent_action_bindings
            .lock()
            .await
            .insert(turn_id.into(), binding);
    }

    pub(crate) async fn unregister_agent_action_binding(&self, turn_id: &str) {
        self.agent_action_bindings.lock().await.remove(turn_id);
    }

    pub(crate) async fn agent_action_binding(
        &self,
        turn_id: &str,
    ) -> Option<agent_action_tools::AgentActionRuntimeBinding> {
        self.agent_action_bindings
            .lock()
            .await
            .get(turn_id)
            .cloned()
    }

    pub(crate) async fn materialize_agent_action_tools(
        self: &Arc<Self>,
        context: pioneer_agent::TurnToolContext,
    ) -> Result<pioneer_agent::TurnToolMaterialization, String> {
        agent_action_tools::materialize(self.clone(), context).await
    }

    async fn bind_permission_approval_bridge(self: &Arc<Self>) {
        let mut worker = self.native_permission_approval_worker.lock().await;
        if worker.is_some() {
            return;
        }

        let (broker, mut request_rx) =
            crate::permissions::GatewayPermissionApprovalBroker::channel();
        let broker = Arc::new(broker);
        self.agent_manager
            .set_permission_approval_broker(broker.clone())
            .await;
        self.mcp_service
            .set_permission_approval_broker(broker)
            .await;

        let processor = Arc::downgrade(self);
        *worker = Some(tokio::spawn(async move {
            while let Some(event) = request_rx.recv().await {
                let Some(processor) = processor.upgrade() else {
                    match event {
                        crate::permissions::GatewayPermissionApprovalEvent::Revalidate(request) => {
                            let _ = request.respond_to.send(Err(
                                "tool permission authority processor is unavailable".to_owned(),
                            ));
                        }
                        crate::permissions::GatewayPermissionApprovalEvent::Open(request) => {
                            let _ = request
                                .respond_to
                                .send(pioneer_tools::PermissionApprovalResolution::Cancelled);
                        }
                        crate::permissions::GatewayPermissionApprovalEvent::Cancelled {
                            ..
                        }
                        | crate::permissions::GatewayPermissionApprovalEvent::Expired { .. } => {}
                    }
                    break;
                };
                let processor = processor.with_database_class(SqliteWriteClass::Critical);
                match event {
                    crate::permissions::GatewayPermissionApprovalEvent::Revalidate(request) => {
                        let result = processor
                            .revalidate_native_tool_permission_context(&request.context)
                            .await
                            .map_err(|error| format!("{error:#}"));
                        let _ = request.respond_to.send(result);
                    }
                    crate::permissions::GatewayPermissionApprovalEvent::Open(request) => {
                        processor.open_native_permission_request(request).await;
                    }
                    crate::permissions::GatewayPermissionApprovalEvent::Cancelled {
                        request_id,
                    } => {
                        processor
                            .cancel_native_permission_request(request_id.as_str())
                            .await;
                    }
                    crate::permissions::GatewayPermissionApprovalEvent::Expired { request_id } => {
                        processor
                            .expire_native_permission_request(request_id.as_str())
                            .await;
                    }
                }
            }
        }));
    }

    pub async fn bind_memory_bridge(self: &Arc<Self>) {
        let memory_provider = Arc::new(crate::memory_tools::GatewayMemoryProvider::new(
            Arc::downgrade(self),
        ));
        self.agent_manager
            .set_memory_provider(Some(memory_provider.clone()))
            .await;
        self.agent_manager
            .set_memory_write_provider(Some(memory_provider.clone()))
            .await;
        self.agent_manager
            .set_memory_post_turn_extractor_provider(Some(memory_provider.clone()))
            .await;

        let bridge = MemoryBridgeProviders { memory_provider };
        *self.memory_bridge_providers.write().await = Some(bridge.clone());
        self.install_memory_hook_runtime(bridge).await;
    }

    async fn install_memory_hook_runtime(&self, bridge: MemoryBridgeProviders) {
        match GatewayHookRuntimeBuilder::new(self.crud_store.clone())
            .with_crud_run_store()
            .install(pioneer_memory::hooks::package(
                bridge.memory_provider.clone(),
                Some(bridge.memory_provider.clone()),
                Some(bridge.memory_provider.clone()),
                self.agent_manager.memory_tool_bundle_artifact_store(),
                self.memory_loop_config(),
            ))
            .and_then(|builder| {
                builder.install(agents_doc_prompt_hook_package(self.crud_store.clone()))
            }) {
            Ok(builder) => {
                let runtime = builder.build();
                *self.hook_runtime.write().await = Some(runtime.clone());
                self.crud_store
                    .background_work("hooks.recovery")
                    .invalidate();
                self.agent_manager.set_hook_runtime(Some(runtime)).await;
            }
            Err(error) => {
                error!(error = %error, "failed to install memory hook package");
            }
        }
    }

    pub(crate) async fn reinstall_memory_hook_runtime_if_bound(&self) {
        let Some(bridge) = self.memory_bridge_providers.read().await.clone() else {
            self.ensure_hook_runtime_with_run_store().await;
            return;
        };
        self.install_memory_hook_runtime(bridge).await;
    }

    pub async fn bind_memory_bridge_if_enabled(self: &Arc<Self>) {
        if self.memory_runtime.is_enabled() {
            self.bind_memory_bridge().await;
        }
    }

    pub(crate) fn memory_runtime(&self) -> Arc<GatewayMemoryRuntime> {
        self.memory_runtime.clone()
    }

    pub(crate) fn provider_registry(&self) -> Arc<ProviderRegistry> {
        self.provider_registry.clone()
    }

    pub(crate) fn observation_governor(
        &self,
    ) -> Arc<crate::authorization::ObservationAdmissionGovernor> {
        self.observation_governor.clone()
    }

    pub(crate) async fn current_authorization_revision(&self) -> anyhow::Result<u64> {
        self.authorization_invalidation_hub.current_revision().await
    }

    pub(crate) async fn initialize_authorization_generation(&self) -> anyhow::Result<u64> {
        Ok(self
            .authorization_invalidation_hub
            .current_generation_for_maintenance()
            .await?
            .get())
    }

    pub(crate) async fn register_execution_lease(
        &self,
        turn_id: &str,
    ) -> anyhow::Result<crate::authorization::ExecutionLeaseGuard> {
        let context = self
            .load_turn_execution_authorization_context(turn_id)
            .await?;
        let generation = self.current_authorization_revision().await?;
        self.execution_leases
            .register(self.crud_store.as_ref(), turn_id, &context, generation)
            .await
    }

    pub(crate) async fn guard_execution_side_effect(
        &self,
        turn_id: &str,
        action: crate::authorization::ResourceAction,
    ) -> anyhow::Result<crate::authorization::ExecutionLeaseGuard> {
        let generation = self.current_authorization_revision().await?;
        self.execution_leases
            .guard(self.crud_store.as_ref(), turn_id, action, generation)
            .await
    }

    pub(crate) async fn guard_execution_commit(
        &self,
        turn_id: &str,
    ) -> anyhow::Result<crate::authorization::ExecutionLeaseGuard> {
        let context = self
            .load_turn_execution_authorization_context(turn_id)
            .await?;
        self.guard_execution_side_effect(turn_id, context.continuation_action())
            .await
    }

    async fn execution_progress_is_current_cached(&self, turn_id: &str) -> anyhow::Result<bool> {
        let generation = self.current_authorization_revision().await?;
        self.execution_leases
            .guard_progress_cached(turn_id, generation)
            .await
    }

    async fn guard_execution_progress(&self, turn_id: &str) -> anyhow::Result<()> {
        if self.execution_progress_is_current_cached(turn_id).await? {
            return Ok(());
        }
        self.guard_execution_commit(turn_id).await.map(drop)
    }

    async fn retire_execution_progress_state(&self, turn_id: &str) {
        // Cache writers hold `progress_items` while rechecking this lease.
        // Retiring the lease first therefore makes the subsequent cache clear
        // a barrier: no in-flight progress can reinsert state behind it.
        self.execution_leases.finish(turn_id).await;
        self.progress_items.lock().await.forget_turn(turn_id);
        let removed = {
            let mut buffers = self.agent_message_buffers.lock().await;
            let mut removed = Vec::new();
            buffers.retain(|_, buffer| {
                if buffer.turn_id == turn_id {
                    removed.push(std::mem::take(buffer));
                    false
                } else {
                    true
                }
            });
            removed
        };
        for buffer in removed {
            Self::record_agent_markdown_buffer(
                &buffer,
                buffer.text.len(),
                pioneer_observability::GatewayMarkdownMessageOutcome::Abandoned,
            );
        }
    }

    pub(crate) async fn validate_task_execution_admission_seed(
        &self,
        seed: &pioneer_tasks::TaskExecutionAdmissionSeed,
    ) -> anyhow::Result<()> {
        let context = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
            seed.authorization_context_json.as_str(),
        )?;
        if context.workspace_id() != seed.workspace_id
            || context.root_thread_id() != seed.root_thread_id
            || context.initiating_principal_id().as_str() != seed.initiating_principal_id
        {
            anyhow::bail!("Task execution admission seed does not match its context");
        }
        context
            .verify_task_admission_boundary(
                self.crud_store.as_ref(),
                seed.workspace_id.as_str(),
                seed.root_thread_id.as_str(),
                seed.initiating_principal_id.as_str(),
            )
            .await?;
        let revision = self.current_authorization_revision().await?;
        self.execution_leases
            .revalidate_context(
                self.crud_store.as_ref(),
                &context,
                crate::authorization::ResourceAction::TaskCreate,
                revision,
            )
            .await?;
        context.verify_current_provider_authority(self.provider_registry.as_ref())
    }

    pub(crate) fn memory_loop_config(&self) -> MemoryLoopConfig {
        self.memory_loop_config
            .read()
            .map(|config| config.clone())
            .unwrap_or_else(|_| self.tool_loop_config.memory.clone())
            .normalized()
    }

    pub(crate) fn apply_memory_loop_config(&self, config: MemoryLoopConfig) {
        if let Ok(mut current) = self.memory_loop_config.write() {
            *current = config.normalized();
        }
    }

    pub(crate) async fn apply_thread_episodic_runtime_config(
        &self,
        config: ThreadEpisodicRuntimeConfig,
    ) {
        let thread_episodic_ingestion_enabled = config.enabled && config.indexing_enabled;
        if let Ok(mut current) = self.thread_episodic_runtime_config.write() {
            *current = config.clone();
        }
        self.thread_episodic_index_executor
            .set_indexing_enabled(thread_episodic_ingestion_enabled);
        self.thread_episodic_index_executor
            .apply_config(config.index_executor);
        self.thread_episodic_recall_service
            .apply_config(config.recall_service);
        let mut vector_search_config = config.vector_search.clone();
        if !config.enabled || !config.vector_search_enabled {
            vector_search_config.enabled = false;
        }
        self.thread_episodic_embedding_provider_resolver
            .apply_config(vector_search_config);
        *self.thread_episodic_ingestor.write().await =
            Arc::new(StoreThreadEpisodicIngestor::with_config(
                self.crud_store.clone(),
                thread_episodic_ingestion_enabled,
            ));
    }

    pub(crate) fn apply_thread_episodic_workspace_vector_search_configs(
        &self,
        configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    ) {
        self.thread_episodic_embedding_provider_resolver
            .apply_workspace_configs(configs.clone());
        self.thread_episodic_recall_service
            .apply_workspace_vector_search_configs(configs);
        self.thread_episodic_index_executor.wake();
    }

    pub(crate) fn compaction_settings(
        &self,
    ) -> anyhow::Result<pioneer_compaction::CompactionSettings> {
        self.compaction_settings
            .read()
            .map(|value| value.clone())
            .map_err(|_| anyhow::anyhow!("compaction settings unavailable"))
    }
    pub(crate) fn compaction_settings_for_workspace(
        &self,
        workspace: &str,
    ) -> anyhow::Result<pioneer_compaction::CompactionSettings> {
        let legacy = self.compaction_settings()?;
        let settings = self
            .workspace_compaction_settings
            .read()
            .map_err(|_| anyhow::anyhow!("workspace compaction settings unavailable"))?;
        Ok(settings
            .get(workspace)
            .map_or(legacy.clone(), |value| value.compaction(&legacy)))
    }
    pub(crate) fn apply_workspace_compaction_settings(
        &self,
        settings: &crate::settings::GatewaySettings,
    ) -> anyhow::Result<()> {
        let prepared = settings.workspace_compaction_settings();
        *self
            .workspace_compaction_settings
            .write()
            .map_err(|_| anyhow::anyhow!("workspace compaction settings unavailable"))? = prepared;
        self.crud_store.invalidate_compaction_history_dependencies();
        Ok(())
    }

    pub(crate) fn apply_keepawake_setting(&self, enabled: bool) -> anyhow::Result<()> {
        self.keepawake.set_enabled(enabled)
    }

    /// Clone the processor with one explicit class for its core persistence
    /// services while preserving all shared runtime state. Long-lived workers
    /// use this at their task boundary instead of inheriting request priority.
    fn scoped_with_database_class(&self, class: SqliteWriteClass) -> Self {
        let crud_store = Arc::new(match class {
            SqliteWriteClass::Critical => self.crud_store.with_critical_writes(),
            SqliteWriteClass::Interactive => self.crud_store.with_interactive_writes(),
            SqliteWriteClass::Maintenance => self.crud_store.with_maintenance_access(),
        });
        self.scoped_with_crud_store(crud_store)
    }

    /// Background reconciliation performs bounded discovery reads but owns
    /// correctness-critical mutations. Keep those two resource classes
    /// independent instead of allowing critical writer priority to bypass the
    /// maintenance read limiter.
    fn scoped_for_background_reconciliation(&self) -> Self {
        self.scoped_with_crud_store(Arc::new(
            self.crud_store.with_maintenance_reads_and_critical_writes(),
        ))
    }

    fn scoped_with_crud_store(&self, crud_store: Arc<CrudStore>) -> Self {
        let mut scoped = self.clone();
        scoped.crud_store = crud_store.clone();
        scoped.workspace_manager = Arc::new(
            self.workspace_manager
                .with_database(crud_store.database_connection()),
        );
        scoped.timeout_supervisor =
            Arc::new(self.timeout_supervisor.with_crud_store(crud_store.clone()));
        scoped.recovery_coordinator = Arc::new(
            self.recovery_coordinator
                .with_crud_store(crud_store.clone()),
        );
        scoped
    }

    fn with_database_class(self: &Arc<Self>, class: SqliteWriteClass) -> Arc<Self> {
        Arc::new(self.scoped_with_database_class(class))
    }

    pub(crate) fn for_background_reconciliation(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(self.scoped_for_background_reconciliation())
    }

    /// Selects the correctness-critical Task service without losing the read
    /// class already carried by this scoped processor. This is the sole bridge
    /// for nested TaskService calls made from MessageProcessor work.
    fn critical_task_service(&self) -> Arc<pioneer_tasks::TaskService> {
        match self.crud_store.database_connection().read_class() {
            SqliteReadClass::Interactive => self.task_runtime.critical_service(),
            SqliteReadClass::Maintenance => self.task_runtime.background_control_service(),
        }
    }

    async fn run_turn_execution_owner_heartbeat_worker(processor: Weak<Self>) {
        loop {
            let Some(this) = processor.upgrade() else {
                break;
            };
            let store = this.crud_store.with_maintenance_reads_and_critical_writes();
            let owner = this.turn_execution_owner_id.clone();
            let notification = store.turn_execution_heartbeat_notification(owner.as_ref());
            #[cfg(test)]
            let barrier = this.turn_execution_heartbeat_barrier.clone();
            // Keep only the DB scope and owner identity during I/O, retry and
            // sleep. The heartbeat must not keep MessageProcessor alive.
            drop(this);
            let cycle = AssertUnwindSafe(async {
                match crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                    pioneer_observability::DatabaseOperationSite::ExecutionOwnerHeartbeat,
                    retry_transient_storage_access(|| {
                        store.supervise_turn_execution_heartbeat(
                            owner.as_ref(),
                            TURN_EXECUTION_OWNER_LEASE_SECONDS,
                        )
                    }),
                )
                .await
                {
                    Ok(parked) => parked,
                    Err(error) => {
                        record_resilience_worker_error(
                            "Turn execution owner heartbeat",
                            &error,
                            ResilienceWorkerFailureImpact::CriticalPoll,
                        );
                        false
                    }
                }
            })
            .catch_unwind()
            .await;
            if cycle.is_err() {
                error!("contained panic in Turn execution owner heartbeat worker");
            }
            #[cfg(test)]
            barrier
                .wait_if_armed(
                    "__execution_owner_heartbeat_written__",
                    &tokio_util::sync::CancellationToken::new(),
                )
                .await;
            // Retries are bounded by the existing storage policy; subsequent
            // polls remain serial and use the existing short service interval.
            // Drop the scoped DB handle before waiting; neither branch retains
            // MessageProcessor. notify_one retains a permit across the final
            // empty check and waiter registration (including admission races).
            drop(store);
            if matches!(cycle, Ok(true)) {
                notification.notified().await;
            } else {
                tokio::select! {
                    _ = notification.notified() => {},
                    _ = sleep(Duration::from_secs(RESILIENCE_WORKER_POLL_INTERVAL_SECONDS)) => {},
                }
            }
        }
    }

    async fn run_recovery_due_worker(processor: Weak<Self>) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let Some(mut owner) = initial.crud_store.background_work("recovery.due").acquire() else {
            return;
        };
        let cancellation = initial.cli_history_shutdown.clone();
        drop(initial);
        let mut clocks = pioneer_crud::RecoveryClockPass::default();
        let mut admission_error = None;
        let mut clock_observation = None;
        let mut clock_rescan = false;
        loop {
            let ready = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                ready = owner.ready() => ready,
            };
            if ready == Ready::Closed {
                return;
            }
            let Some(original) = processor.upgrade() else {
                return;
            };
            let this = original.for_background_reconciliation();
            drop(original);
            #[cfg(test)]
            let wait_barrier = this.recovery_due_wait_barrier.clone();
            let mut transient_storage_poll_failed = false;
            if ready == Ready::SettlementRequired {
                clock_rescan = true;
                let settle_observation = owner.observe();
                if let Err(error) = this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                {
                    record_resilience_worker_poll_error(
                        "recovery due settlement",
                        &error.into(),
                        &mut transient_storage_poll_failed,
                    );
                    owner.retry_after(
                        settle_observation,
                        Duration::from_secs(if transient_storage_poll_failed {
                            RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS
                        } else {
                            RESILIENCE_WORKER_POLL_INTERVAL_SECONDS
                        }),
                    );
                }
                continue;
            }
            let observation = owner.observe();
            if clock_observation.is_some_and(|at| owner.producer_changed_since(at)) {
                clock_rescan = true;
            }
            clock_observation = Some(observation);
            let cycle = AssertUnwindSafe(owner.scope(async {
                let now = chrono::DateTime::<chrono::Utc>::from(
                    this.crud_store.background_work("recovery.due").wall_now(),
                )
                .timestamp();
                // Reuse this quantum's unconditional PK frontier for execution
                // AND clocks. No filtered population scan precedes this page.
                let complete = match this
                    .crud_store
                    .recovery_clock_quantum(&mut clocks, false)
                    .await
                {
                    Ok(complete) => complete,
                    Err(error) => {
                        admission_error.get_or_insert(error);
                        true
                    }
                };
                for id in &clocks.job_candidates {
                    let result = crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                        pioneer_observability::DatabaseOperationSite::RecoveryJobs,
                        retry_transient_storage_access(|| {
                            this.recovery_coordinator.run_due_job_candidate(now, id)
                        }),
                    )
                    .await;
                    match result {
                        Ok(events) => {
                            if !events.is_empty() {
                                clock_rescan = true;
                            }
                            for event in events {
                                this.handle_recovery_event(event, now).await;
                            }
                        }
                        Err(error) => {
                            admission_error.get_or_insert(error);
                        }
                    }
                }
                if !complete {
                    return Ok(Plan::Runnable);
                }
                if let Some(error) = admission_error.take() {
                    return Err(error);
                }
                let next = clocks.job_next;
                clocks = Default::default();
                if std::mem::take(&mut clock_rescan) {
                    return Ok(Plan::Runnable);
                }
                Ok(next.map_or(Plan::Dormant, |at| Plan::WallDeadline(at.into())))
            }))
            .catch_unwind()
            .await;
            let mut _parked = false;
            match cycle {
                Ok(Ok(plan)) if !owner.requires_settlement() => {
                    _parked = owner.publish(observation, plan) && !matches!(plan, Plan::Runnable);
                }
                Ok(Ok(_)) => {
                    // A swallowed lower mutation failure still carries Unknown.
                    owner.retry_after(
                        observation,
                        Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS),
                    );
                }
                Ok(Err(error)) => {
                    admission_error = None;
                    clocks = Default::default();
                    clock_rescan = false;
                    record_resilience_worker_poll_error(
                        "recovery due jobs",
                        &error,
                        &mut transient_storage_poll_failed,
                    );
                    owner.retry_after(
                        observation,
                        Duration::from_secs(if transient_storage_poll_failed {
                            RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS
                        } else {
                            RESILIENCE_WORKER_POLL_INTERVAL_SECONDS
                        }),
                    );
                }
                Err(_) => {
                    admission_error = None;
                    clocks = Default::default();
                    clock_rescan = false;
                    error!("contained panic in recovery due worker");
                    owner.retry_after(
                        observation,
                        Duration::from_secs(RESILIENCE_WORKER_POLL_INTERVAL_SECONDS),
                    );
                }
            }
            #[cfg(test)]
            if _parked {
                this.completed_history_preparation_barrier
                    .wait_if_armed("__recovery_clock_settled__", &cancellation)
                    .await;
            }
            drop(this);
            #[cfg(test)]
            wait_barrier
                .wait_if_armed("__recovery_due_before_wait__", &cancellation)
                .await;
        }
    }

    async fn run_projection_delivery_resilience_worker(processor: Weak<Self>) {
        // Independent owner leases and phases. Delivery retains its existing
        // bounded/causal drain; the adapter below only forwards coalesced input
        // invalidations and never claims, discovers or dispatches business rows.
        tokio::join!(
            Self::run_native_projection_owner(processor.clone(), false),
            Self::run_native_projection_owner(processor.clone(), true),
            Self::run_native_delivery_inputs(
                processor.clone(),
                pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER
            ),
            Self::run_native_delivery_inputs(
                processor,
                pioneer_crud::NATIVE_TURN_EVENT_EPISODIC_CONSUMER
            ),
        );
    }

    async fn run_native_delivery_inputs(processor: Weak<Self>, consumer: &'static str) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let name = if consumer == pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER {
            "turn.delivery.live"
        } else {
            "turn.delivery.episodic"
        };
        let work = initial.crud_store.background_work(name);
        let cancel = initial.cli_history_shutdown.clone();
        let Some(mut owner) = work.acquire() else {
            return;
        };
        drop(initial);
        loop {
            let ready = tokio::select! {
                _ = cancel.cancelled() => return,
                ready = owner.ready() => ready,
            };
            if ready == Ready::Closed {
                return;
            }
            let Some(this) = processor.upgrade() else {
                return;
            };
            if ready == Ready::SettlementRequired {
                let settle_observation = owner.observe();
                if this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    owner.retry_after(settle_observation, Duration::from_secs(5));
                }
                continue;
            }
            let observed = owner.observe();
            owner
                .scope(async {
                    if consumer == pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER {
                        this.kick_native_live_turn_event_deliveries();
                    } else {
                        this.kick_native_episodic_turn_event_deliveries();
                    }
                })
                .await;
            // The existing native owner owns discovery, deadlines and Empty.
            // This memory-only input adapter settles just the forwarded hint.
            owner.publish(observed, Plan::Dormant);
        }
    }

    async fn run_native_projection_owner(processor: Weak<Self>, finalization: bool) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let work = initial.crud_store.background_work(if finalization {
            "turn.finalization"
        } else {
            "turn.projection"
        });
        let cancellation = initial.cli_history_shutdown.clone();
        let Some(mut owner) = work.acquire() else {
            return;
        };
        drop(initial);
        let mut reporting = reconciliation_diagnostics::Reporter::new(
            reconciliation_diagnostics::Operation::NativeFinalization,
        );
        loop {
            let ready = tokio::select! {
                _ = cancellation.cancelled() => return,
                ready = owner.ready() => ready,
            };
            if ready == Ready::Closed {
                return;
            }
            let Some(original) = processor.upgrade() else {
                return;
            };
            let this = original.for_background_reconciliation();
            drop(original);
            if ready == Ready::SettlementRequired {
                let settle_observation = owner.observe();
                if this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    owner.retry_after(settle_observation, Duration::from_secs(2));
                }
                continue;
            }
            let observed = owner.observe();
            let cycle = owner.scope(AssertUnwindSafe(async {
                let now = now_timestamp_secs();
                if finalization {
                    let result = crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::FinalizationRecovery,
                        pioneer_observability::DatabaseOperationSite::PreparedFinalizationReconcile,
                        retry_transient_storage_access(|| this.reconcile_prepared_native_turn_finalizations(now, 64)),
                    ).await;
                    reporting.observe_finalizations(&result, Instant::now());
                    let summary = result?;
                    if summary.committed_count() > 0 {
                        info!(scanned = summary.scanned, committed = summary.committed_count(), "reconciled prepared native Turn finalizations");
                        this.kick_native_turn_event_deliveries();
                    }
                    // Finish the bounded pass before retrying poison, keeping
                    // independent healthy intents reachable beyond its prefix.
                    if !summary.pass_complete { return Ok(Plan::Runnable); }
                    anyhow::ensure!(!summary.pass_failed, "native finalization pass has failures");
                    Ok(Plan::Dormant)
                } else {
                    let summary = crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::ProjectionRecovery,
                        pioneer_observability::DatabaseOperationSite::ProjectionReplay,
                        retry_transient_storage_access(|| this.crud_store.replay_due_turn_event_projections(now, 64)),
                    ).await?;
                    if summary.quarantined > 0 || summary.missing_events > 0 {
                        warn!(claimed = summary.claimed, projected = summary.projected, failed = summary.failed,
                            quarantined = summary.quarantined, missing_events = summary.missing_events,
                            "turn event projection replay found quarantined streams");
                    }
                    if summary.claimed >= 64 { return Ok(Plan::Runnable); }
                    Ok(summary.next_eligible_at_unix.map_or(Plan::Dormant, |at| {
                        Plan::WallDeadline(pioneer_crud::unix_to_datetime(at).into())
                    }))
                }
            }).catch_unwind()).await;
            match cycle {
                Ok(Ok(plan)) if !owner.requires_settlement() => {
                    owner.publish(observed, plan);
                }
                Ok(Err(error)) => {
                    error!(finalization, error = %format!("{error:#}"), "native projection/finalization recovery deferred");
                    owner.retry_after(observed, Duration::from_secs(2));
                }
                _ => {
                    owner.retry_after(observed, Duration::from_secs(2));
                }
            }
            #[cfg(test)]
            this.completed_history_preparation_barrier
                .wait_if_armed(
                    if finalization {
                        "__finalization_before_wait__"
                    } else {
                        "__event_delivery_recovery_before_kick__"
                    },
                    &cancellation,
                )
                .await;
            drop(this);
        }
    }

    async fn run_task_lifecycle_resilience_worker(processor: Weak<Self>) {
        use pioneer_sqlite::background::{Plan, Ready};
        let mut occurrence_reporting = reconciliation_diagnostics::Reporter::new(
            reconciliation_diagnostics::Operation::TaskRunOccurrence,
        );
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let work = initial.crud_store.background_work("tasks.lifecycle");
        let cancellation = initial.cli_history_shutdown.clone();
        let Some(mut owner) = work.acquire() else {
            return;
        };
        drop(initial);
        let mut delivery_cursor: Option<(String, String)> = None;
        let mut delivery_pending = None;
        let mut delivery_recovery = None;
        let mut delivery_pass = None;
        let mut delivery_rescan = false;
        let mut delivery_failed = false;
        loop {
            let ready = tokio::select! {
                _ = cancellation.cancelled() => return,
                ready = owner.ready() => ready,
            };
            if ready == Ready::Closed {
                return;
            }
            let Some(original) = processor.upgrade() else {
                return;
            };
            let this = original.for_background_reconciliation();
            drop(original);
            if ready == Ready::SettlementRequired {
                delivery_cursor = None;
                delivery_pass = None;
                let settle_observation = owner.observe();
                if this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    error!("Task lifecycle change settlement failed");
                    owner.retry_after(settle_observation, Duration::from_secs(2));
                }
                continue;
            }
            let observed = owner.observe();
            if delivery_cursor.is_none() {
                delivery_pending = None;
                delivery_recovery = None;
                delivery_pass = Some(observed);
                delivery_rescan = false;
                delivery_failed = false;
            }
            delivery_rescan |= delivery_pass.is_some_and(|at| owner.producer_changed_since(at));
            let cycle = owner.scope(AssertUnwindSafe(async {
                let now = work.wall_now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs().min(i64::MAX as u64) as i64;
                let mut failed = false;
                {
                    if let Err(error) = this.crud_store.seed_child_work_with_clock(&|| now).await {
                        failed = true;
                        error!(error = %format!("{error:#}"), "Task child seed quantum failed");
                    }
                    match crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::TaskReconcile,
                        pioneer_observability::DatabaseOperationSite::TaskChildReconcile,
                        this.reconcile_terminal_task_child_turns_with_retry(64),
                    )
                    .await
                    {
                        Ok(reconciled) if reconciled > 0 => info!(
                            reconciled,
                            "reconciled terminal child Turns with their TaskRun aggregates"
                        ),
                        Ok(_) => {}
                        Err(error) => { failed = true; tasks::report_task_child_reconciliation_error(error); },
                    }
                }
                {
                    let result = crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::TaskReconcile,
                        pioneer_observability::DatabaseOperationSite::TaskOccurrenceReconcile,
                        this.reconcile_terminal_task_run_occurrence_turns(64),
                    )
                    .await;
                    failed |= result.as_ref().map_or(true, |summary| summary.first_error.is_some()
                        || summary.notification_errors > 0 || summary.pending.is_none());
                    occurrence_reporting.observe_occurrences(&result, Instant::now());
                    if let Ok(summary) = result
                        && summary.selected > 0
                    {
                        info!(
                            selected = summary.selected,
                            claimed = summary.claimed,
                            reconciled = summary.changed,
                            lost_claims = summary.lost_claims,
                            stale = summary.stale,
                            unresolved = summary.unresolved,
                            no_change = summary.no_change,
                            claim_errors = summary.claim_errors,
                            claim_deferrals = summary.claim_deferrals,
                            claim_deferral_conflicts = summary.claim_deferral_conflicts,
                            claim_deferral_errors = summary.claim_deferral_errors,
                            claim_without_snapshot = summary.claim_without_snapshot,
                            claim_commit_unknown = summary.claim_commit_unknown,
                            repair_errors = summary.repair_errors,
                            storage_errors = summary.storage_errors,
                            notification_errors = summary.notification_errors,
                            queue_state_errors = summary.queue_state_errors,
                            "TaskRun parent occurrence reconciliation pass"
                        );
                    }
                }
                {
                    if let Err(error) =
                        crate::database::attribution::scope_database_workload_result(
                            pioneer_observability::DatabaseWorkload::TaskReconcile,
                            pioneer_observability::DatabaseOperationSite::TaskDelivery,
                            this.process_due_task_deliveries(now, 64),
                        )
                        .await
                    {
                        failed = true;
                        error!(error = %format!("{error:#}"), "task delivery worker failed");
                    }
                }
                let now = work.wall_now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs().min(i64::MAX as u64) as i64;
                let mut deadlines = this.crud_store.task_lifecycle_non_delivery_deadlines(now).await?;
                let (pending, recovery, next, point_failed) = this.crud_store.task_delivery_deadline_page(
                    now, delivery_cursor.as_ref().map(|(a, _)| a.as_str()),
                    delivery_cursor.as_ref().map(|(_, u)| u.as_str()),
                ).await?;
                delivery_pending = delivery_pending.into_iter().chain(pending).min();
                delivery_recovery = delivery_recovery.into_iter().chain(recovery).min();
                delivery_cursor = next;
                delivery_failed |= point_failed;
                failed |= delivery_cursor.is_none() && delivery_failed;
                deadlines.delivery_pending = delivery_pending;
                deadlines.delivery_recovery = delivery_recovery;
                let cooldown = this.task_runtime.background_control_service().delivery_recovery_retry_deadline();
                let mut earliest = deadlines.child_point.into_iter()
                    .chain(deadlines.child_scope).chain(deadlines.occurrence)
                    .map(|at| std::time::UNIX_EPOCH + Duration::from_secs(at.max(0) as u64))
                    .chain(deadlines.delivery_pending.map(|at| std::time::UNIX_EPOCH
                        + Duration::from_secs(at.timestamp().saturating_add(i64::from(at.timestamp_subsec_nanos() > 0)).max(0) as u64)))
                    .min();
                // A cooling delivery source is the same lane as its retry:
                // suppress that source, retain the retry, and independently
                // retain every other lane's wall-clock obligation.
                let recovery_cooldown = cooldown.filter(|until| *until > tokio::time::Instant::now());
                if recovery_cooldown.is_none() {
                    earliest = earliest.into_iter().chain(deadlines.delivery_recovery.map(|at|
                        std::time::UNIX_EPOCH + Duration::from_secs(at.max(0) as u64))).min();
                }
                let plan = earliest.map_or(Plan::Dormant, |at| {
                    if at <= work.wall_now() { Plan::Runnable }
                    else { Plan::WallDeadline(at) }
                });
                let plan = if delivery_cursor.is_some() || delivery_rescan { Plan::Runnable } else { plan };
                let plan = recovery_cooldown.map_or(plan, |until| plan.with_monotonic_deadline(until));
                Ok::<_, anyhow::Error>((plan, failed))
            }).catch_unwind()).await;
            match cycle {
                Ok(Ok((plan, false))) => {
                    let settled = owner.publish(observed, plan);
                    #[cfg(test)]
                    if settled
                        && delivery_cursor.is_none()
                        && !delivery_rescan
                        && !matches!(plan, Plan::Runnable)
                    {
                        this.completed_history_preparation_barrier
                            .wait_if_armed("__task_lifecycle_delivery_settled__", &cancellation)
                            .await;
                    }
                    let _ = settled;
                }
                Ok(Ok((_, true))) => {
                    owner.retry_after(observed, Duration::from_secs(2));
                }
                Ok(Err(error)) => {
                    error!(error = %format!("{error:#}"), "Task lifecycle deadline observation failed");
                    owner.retry_after(observed, Duration::from_secs(2));
                }
                Err(_) => {
                    error!("contained panic in task lifecycle resilience worker");
                    owner.retry_after(observed, Duration::from_secs(2));
                }
            }
            #[cfg(test)]
            this.completed_history_preparation_barrier
                .wait_if_armed("__task_lifecycle_before_wait__", &this.cli_history_shutdown)
                .await;
            // The weak processor and every database resource are released
            // before common waiting. Dormant has neither a timer nor SQL.
            drop(this);
        }
    }

    async fn initialize_resilience_workers(self: &Arc<Self>) -> anyhow::Result<()> {
        self.thread_episodic_index_executor.wake();
        let background = self.for_background_reconciliation();
        let trace = pioneer_observability::GatewayOperationTrace::start(
            pioneer_observability::GatewayOperation::ResilienceInitialize,
        );
        let repair_stage =
            trace.stage(pioneer_observability::GatewayOperationStage::ResilienceReadModelRepair);
        match background
            .crud_store
            .repair_deterministic_read_model_violations_with_timings()
            .await
        {
            Ok(report) => {
                for timing in report.stage_timings {
                    trace.record_stage_timing(
                        gateway_stage_for_read_model_repair(timing.stage),
                        timing.started_at,
                        timing.duration,
                        timing.operations,
                    );
                }
                let summary = report.summary;
                trace.record_items(
                    pioneer_observability::GatewayOperationItemKind::ReadModelRepairDetected,
                    u64::try_from(summary.detected).unwrap_or(u64::MAX),
                );
                trace.record_items(
                    pioneer_observability::GatewayOperationItemKind::ReadModelRepairRepaired,
                    u64::try_from(summary.repaired).unwrap_or(u64::MAX),
                );
                trace.record_items(
                    pioneer_observability::GatewayOperationItemKind::ReadModelRepairRemaining,
                    u64::try_from(summary.remaining).unwrap_or(u64::MAX),
                );
                repair_stage.succeed();
                if summary.remaining > 0 {
                    warn!(
                        detected = summary.detected,
                        repaired = summary.repaired,
                        remaining = summary.remaining,
                        "read-model invariant verification found unresolved violations"
                    );
                } else if summary.detected > 0 {
                    info!(
                        detected = summary.detected,
                        repaired = summary.repaired,
                        "read-model invariant verification repaired deterministic violations"
                    );
                }
            }
            Err(error) => {
                drop(repair_stage);
                warn!(
                    error = %format!("{error:#}"),
                    "read-model invariant verification failed at startup"
                );
                trace.finish_failure();
                return Err(error).context("read-model invariant verification failed at startup");
            }
        }

        let deadline_stage =
            trace.stage(pioneer_observability::GatewayOperationStage::ResilienceDeadlineBackfill);
        match background
            .timeout_supervisor
            .backfill_missing_deadlines(1024)
            .await
        {
            Ok(backfilled) => {
                deadline_stage.succeed();
                if backfilled > 0 {
                    warn!(
                        backfilled,
                        "backfilled missing running item deadlines during startup"
                    );
                }
            }
            Err(error) => {
                drop(deadline_stage);
                warn!(
                    error = %format!("{error:#}"),
                    "failed to backfill missing running item deadlines during startup"
                );
                trace.finish_failure();
                return Err(error).context("deadline backfill failed at startup");
            }
        }

        let admission_stage = trace
            .stage(pioneer_observability::GatewayOperationStage::ResilienceAdmissionLeaseReconcile);
        match background
            .crud_store
            .reconcile_execution_admission_leases()
            .await
        {
            Ok(reconciled) => {
                admission_stage.succeed();
                if reconciled > 0 {
                    warn!(
                        reconciled,
                        "released stale execution admission quota leases during startup"
                    );
                }
            }
            Err(error) => {
                drop(admission_stage);
                warn!(
                    error = %format!("{error:#}"),
                    "failed to reconcile execution admission quota leases at startup"
                );
                trace.finish_failure();
                return Err(error).context("execution admission lease reconciliation failed");
            }
        }

        let task_runtime_stage =
            trace.stage(pioneer_observability::GatewayOperationStage::ResilienceTaskRuntimeStart);
        let partial_startup = match self.task_runtime.start().await {
            Ok(()) => {
                task_runtime_stage.succeed();
                None
            }
            Err(error) => {
                drop(task_runtime_stage);
                error!(error = %format!("{error:#}"), "failed to start task runtime");
                if error
                    .downcast_ref::<pioneer_tasks::PartialTaskStartup>()
                    .is_some()
                {
                    Some(error)
                } else {
                    trace.finish_failure();
                    return Err(error).context("failed to start task runtime");
                }
            }
        };

        let task_events_stage = trace
            .stage(pioneer_observability::GatewayOperationStage::ResilienceTaskEventListenerStart);
        self.start_task_event_listener().await;
        task_events_stage.succeed();

        let hook_recovery_stage =
            trace.stage(pioneer_observability::GatewayOperationStage::ResilienceHookRecoveryStart);
        self.start_hook_recovery_worker().await;
        hook_recovery_stage.succeed();

        if partial_startup.is_some() {
            // Keep explicit failure diagnostics/trace, but do not trap Gateway
            // initialization in a retry loop on classified row-local poison.
            trace.finish_failure();
        } else {
            trace.finish_success();
        }
        Ok(())
    }

    /// The queue phase of the control fallback. Admission is advisory; the
    /// writer-owned scheduler rechecks capacity and keeps all fairness/claims.
    async fn promote_admitted_agent_queue(
        &self,
        admission: Option<&pioneer_crud::RecoveryAdmission>,
        clock: impl Fn() -> sea_orm::prelude::DateTimeWithTimeZone + Sync,
    ) -> anyhow::Result<Vec<pioneer_crud::PromotedAgentExecution>> {
        if admission
            .is_some_and(|ready| !ready.queue || ready.queue_next.is_some_and(|at| at > clock()))
        {
            // The advisory F1 gate may admit <1s early. Its exact deadline
            // avoids an empty writer/retry loop during that fractional gap.
            return Ok(Vec::new());
        }
        let policy = crate::authorization::AgentWorkResourcePolicy::default();
        crate::database::attribution::scope_database_workload_result(
            pioneer_observability::DatabaseWorkload::ExecutionSupervision,
            pioneer_observability::DatabaseOperationSite::QueuedExecutionPromotion,
            retry_transient_storage_access(|| {
                self.crud_store.promote_queued_agent_executions(
                    clock(),
                    64,
                    i64::try_from(policy.idle_timeout_secs).unwrap_or(i64::MAX),
                    i64::try_from(policy.hard_timeout_secs).unwrap_or(i64::MAX),
                )
            }),
        )
        .await
    }

    #[cfg(test)]
    pub async fn start_resilience_workers(self: &Arc<Self>) -> anyhow::Result<()> {
        let cancellation = tokio_util::sync::CancellationToken::new();
        self.start_resilience_workers_with_cancellation(&cancellation, |_| {})
            .await
    }

    pub(crate) async fn start_resilience_workers_with_cancellation<F>(
        self: &Arc<Self>,
        cancellation: &tokio_util::sync::CancellationToken,
        on_degraded: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(bool) + Send + Sync,
    {
        let mut guard = self.resilience_worker.lock().await;
        if guard.is_some() {
            on_degraded(false);
            return Ok(());
        }

        let processor = Arc::downgrade(self);
        self.recovery_coordinator
            .set_listener_starter(Arc::new(move |thread_id| {
                let processor = processor.clone();
                Box::pin(async move {
                    let processor = processor.upgrade().ok_or_else(|| {
                        "message processor stopped before recovery listener startup".to_owned()
                    })?;
                    let processor = processor.with_database_class(SqliteWriteClass::Critical);
                    processor
                        .ensure_agent_listener_task(thread_id.as_str())
                        .await
                        .map_err(|error| format!("{error:#}"))
                })
            }))
            .await;

        let mut retry_delay = Duration::from_millis(250);
        loop {
            if cancellation.is_cancelled() {
                return Err(anyhow::anyhow!("resilience initialization cancelled"));
            }
            let initialization = async {
                self.authorization_invalidation_hub
                    .current_generation_for_maintenance()
                    .await
                    .context("Gateway startup requires durable authorization policy generation")?;
                self.initialize_resilience_workers().await
            }
            .await;
            match initialization {
                Ok(()) => {
                    on_degraded(false);
                    break;
                }
                Err(error) => {
                    on_degraded(true);
                    warn!(
                        error = %format!("{error:#}"),
                        retry_delay_ms = retry_delay.as_millis() as u64,
                        "resilience initialization failed; retrying independently"
                    );
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            return Err(anyhow::anyhow!("resilience initialization cancelled"));
                        }
                        _ = sleep(retry_delay) => {}
                    }
                    retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(30));
                }
            }
        }
        if cancellation.is_cancelled() {
            return Err(anyhow::anyhow!("resilience initialization cancelled"));
        }

        let processor = Arc::downgrade(self);
        let cancellation = cancellation.clone();
        let worker_cancellation = cancellation.clone();
        let worker = async move {
            struct IsolatedWorkerGuard(Vec<JoinHandle<()>>);

            impl Drop for IsolatedWorkerGuard {
                fn drop(&mut self) {
                    for worker in &self.0 {
                        worker.abort();
                    }
                }
            }

            let _isolated_workers = IsolatedWorkerGuard(vec![
                tokio::spawn(Self::run_turn_execution_owner_heartbeat_worker(
                    processor.clone(),
                )),
                tokio::spawn(Self::run_recovery_due_worker(processor.clone())),
                tokio::spawn(Self::run_execution_timeout_worker(processor.clone())),
                tokio::spawn(Self::run_native_terminal_effect_worker(processor.clone())),
                tokio::spawn(Self::run_agent_action_ledger_worker(processor.clone())),
                tokio::spawn(Self::run_skill_upload_cleanup_worker(processor.clone())),
                tokio::spawn(Self::run_native_terminal_effect_archive_worker(
                    processor.clone(),
                    false,
                )),
                tokio::spawn(Self::run_native_terminal_effect_archive_worker(
                    processor.clone(),
                    true,
                )),
                tokio::spawn(Self::run_cli_command_heartbeat_worker(processor.clone())),
                tokio::spawn(Self::run_cli_supervision_worker(processor.clone())),
                tokio::spawn(Self::run_projection_delivery_resilience_worker(
                    processor.clone(),
                )),
                tokio::spawn(Self::run_task_lifecycle_resilience_worker(
                    processor.clone(),
                )),
                tokio::spawn(Self::run_compaction_lifecycle_worker(processor.clone())),
                tokio::spawn(Self::run_compaction_history_worker(processor.clone())),
            ]);
            Self::run_recovery_control_worker(processor, worker_cancellation).await;
        };
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {},
                _ = worker => {},
            }
        });

        *guard = Some(handle);
        Ok(())
    }

    /// One retained control owner, independent of the sibling owner tasks.
    /// Startup owns its cancellation together with those siblings.
    async fn run_recovery_control_worker(
        processor: Weak<Self>,
        worker_cancellation: tokio_util::sync::CancellationToken,
    ) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let Some(mut owner) = initial
            .crud_store
            .background_work("recovery.control")
            .acquire()
        else {
            return;
        };
        drop(initial);
        let mut clocks = pioneer_crud::RecoveryClockPass::default();
        let mut control_admission = pioneer_crud::ControlAdmissionPass::default();
        let mut admission_complete = false;
        let mut clock_observation = None;
        let mut clock_rescan = false;
        loop {
            let ready = tokio::select! { biased; _ = worker_cancellation.cancelled() => return, ready = owner.ready() => ready };
            if ready == Ready::Closed {
                return;
            }
            let Some(this) = processor.upgrade() else {
                break;
            };
            let this = this.for_background_reconciliation();
            if ready == Ready::SettlementRequired {
                clock_rescan = true;
                let settle_observation = owner.observe();
                if this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    owner.retry_after(settle_observation, Duration::from_secs(60));
                }
                continue;
            }
            let observed = owner.observe();
            if clock_observation.is_some_and(|at| owner.producer_changed_since(at)) {
                clock_rescan = true;
            }
            clock_observation = Some(observed);
            let mut clock_complete = false;
            let mut clock_observed = false;
            let mut clock_failed = false;
            let plan = owner.scope(async {
                let now = this.crud_store.background_work("recovery.control").wall_now().duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_secs().min(i64::MAX as u64) as i64;
                let mut transient_storage_poll_failed = false;
                let mut phase_failed = false;
                let admission = match crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                    pioneer_observability::DatabaseOperationSite::RecoveryJobs,
                    async {
                        #[cfg(test)]
                        {
                            let failure = this.recovery_admission_failure.lock().unwrap().take();
                            if let Some(error) = failure {
                                return Err(error);
                            }
                        }
                        clock_observed = true;
                        clock_complete = match this.crud_store.recovery_clock_quantum(&mut clocks, true).await {
                            Ok(complete) => complete,
                            Err(error) => { clock_failed=true; clock_rescan=true; return Err(error); }
                        };
                        admission_complete = match this.crud_store.control_admission_quantum(&mut control_admission).await {
                            Ok(done) => done,
                            Err(error) => {control_admission = Default::default(); return Err(error);}
                        };
                        #[cfg(test)]
                        if !admission_complete {
                            this.completed_history_preparation_barrier.wait_if_armed(
                                "__control_admission_raw_page__", &worker_cancellation).await;
                        }
                        let mut admission = this.crud_store.recovery_admission_core(this.turn_execution_owner_id.as_ref(), now).await?;
                        control_admission.apply_at(&mut admission, pioneer_crud::utc_now());
                        admission.orphan = this.recovery_coordinator.orphan_supervision_more();
                        admission.orphan_next = this.recovery_coordinator.orphan_supervision_deadline().map(pioneer_crud::unix_to_datetime);
                        admission.terminal_audit = clocks.terminal_audit;
                        admission.backfill = clocks.backfill;
                        admission.terminal_repair = clocks.repair_next.is_some_and(|at| at <= pioneer_crud::unix_to_datetime(now));
                        admission.terminal_delivery = clocks.terminal_next.is_some_and(|at| at <= pioneer_crud::unix_to_datetime(now));
                        admission.terminal_repair_next = clocks.repair_next;
                        admission.terminal_delivery_next = clocks.terminal_next;
                        Ok(admission)
                    },
                )
                .await
                {
                    Ok(admission) => Some(admission),
                    Err(error) => {
                        // Observation failure is advisory: preserve its diagnostic
                        // without deferring healthy authoritative phases below.
                        record_resilience_worker_error(
                            "recovery admission",
                            &error,
                            ResilienceWorkerFailureImpact::Maintenance,
                        );
                        // Unknown is never Empty: allow every independent phase
                        // to attempt its existing authoritative discovery.
                        None
                    }
                };
                #[cfg(test)]
                this.completed_history_preparation_barrier
                    .wait_if_armed(
                        "__resilience_before_history_poll__",
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await;
                // Command heartbeats have their own retained deadline owner.
                // Timeout authority still checks liveness before destructive CAS.
                let mut action_outbox_changed = false;
                let mut terminal_effects_changed = false;
                match this
                    .promote_admitted_agent_queue(admission.as_ref(), pioneer_crud::utc_now)
                    .await
                {
                    Ok(promoted) => {
                        action_outbox_changed = !promoted.is_empty();
                        for execution in &promoted {
                            if let Err(error) =
                                crate::database::attribution::scope_database_workload_result(
                                    pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                                    pioneer_observability::DatabaseOperationSite::AgentOutboxWake,
                                    this.crud_store.wake_agent_action_outbox_for_execution(
                                        execution.execution_id.as_str(),
                                        pioneer_crud::utc_now(),
                                    ),
                                )
                                .await
                            {
                                phase_failed = true;
                    record_resilience_worker_poll_error(
                                    "agent domain promoted agent outbox wake",
                                    &error,
                                    &mut transient_storage_poll_failed,
                                );
                            }
                        }
                        let roots = promoted
                            .into_iter()
                            .map(|execution| execution.root_execution_id)
                            .collect::<std::collections::BTreeSet<_>>();
                        for root_execution_id in roots {
                            crate::database::attribution::scope_database_workload(
                                pioneer_observability::DatabaseWorkload::ExecutionSupervision, pioneer_observability::DatabaseOperationSite::AgentGraphNotification,
                                this.notify_agent_work_graph_state_changed(
                                    root_execution_id.as_str(),
                                ),
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        phase_failed = true;
                    record_resilience_worker_poll_error(
                            "agent domain durable agent queue",
                            &error,
                            &mut transient_storage_poll_failed,
                        );
                    }
                }
                if transient_storage_poll_failed {
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS));
                }

                if action_outbox_changed
                    || admission.as_ref().is_none_or(|ready| ready.action_outbox)
                {
                    match crate::database::attribution::scope_database_workload_result(
                        pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                        pioneer_observability::DatabaseOperationSite::AgentActionOutbox,
                        // One input quantum. Report errors/back off only after all
                        // independent known-committed rows have had their dispatch.
                        agent_action_tools::process_due_agent_action_outbox(&this, 64),
                    )
                    .await
                    {
                        Ok(batch) => {
                            terminal_effects_changed |=
                                batch.delivered > 0 || !batch.errors.is_empty();
                            if !batch.errors.is_empty() {
                                tracing::warn!(
                                    delivered = batch.delivered,
                                    candidate_failures = batch.errors.len(),
                                    "agent action outbox quantum made partial progress"
                                );
                                transient_storage_poll_failed = true;
                                for error in batch.errors {
                                    phase_failed = true;
                    record_resilience_worker_poll_error(
                                        "agent domain agent action outbox",
                                        &error,
                                        &mut transient_storage_poll_failed,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            transient_storage_poll_failed = true;
                            phase_failed = true;
                    record_resilience_worker_poll_error(
                                "agent domain agent action outbox",
                                &error,
                                &mut transient_storage_poll_failed,
                            );
                        }
                    }
                    if transient_storage_poll_failed {
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS));
                }
                }

                if terminal_effects_changed {
                    this.crud_store.background_work("recovery.effects").dependency_released();
                }

                if admission.is_none() && !clock_observed {
                    // A failure before clock discovery still permits this late
                    // independent bounded phase (including F7's late commit).
                    clock_rescan = true;
                    match this.crud_store.recovery_clock_quantum(&mut clocks, true).await {
                        Ok(done) => clock_complete = done,
                        Err(error) => {
                            phase_failed = true;
                            clock_failed = true;
                            record_resilience_worker_poll_error("recovery terminalization observation", &error, &mut transient_storage_poll_failed);
                        }
                    }
                }
                if admission
                    .as_ref()
                    .is_none_or(|ready| ready.terminal_audit)
                {
                    if let Err(error) = crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::RecoveryTerminalizationAudit,
                    pioneer_observability::DatabaseOperationSite::RecoveryTerminalizationAudit,
                    retry_transient_storage_access(|| {
                        this.crud_store
                            .enqueue_missing_recovery_terminalization_candidates(&clocks.terminal_audit_candidates, now)
                    }),
                )
                .await
                {
                    phase_failed = true;
                    record_resilience_worker_poll_error(
                        "recovery terminalization auditor",
                        &error,
                        &mut transient_storage_poll_failed,
                    );
                }
                    if transient_storage_poll_failed {
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS));
                }
                }

                if admission.as_ref().is_none_or(|ready| {
                    ready.terminal_delivery || ready.terminal_audit
                }) {
                    if let Err(error) = crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::RecoveryTerminalizationApply,
                    pioneer_observability::DatabaseOperationSite::RecoveryTerminalizationApply,
                    retry_transient_storage_access(|| {
                        this.process_due_recovery_terminalizations(now, 64)
                    }),
                )
                .await
                {
                    phase_failed = true;
                    record_resilience_worker_poll_error(
                        "recovery terminalization outbox",
                        &error,
                        &mut transient_storage_poll_failed,
                    );
                }
                    if transient_storage_poll_failed {
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS));
                }
                }

                match crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                    pioneer_observability::DatabaseOperationSite::RecoveryJobs,
                    retry_transient_storage_access(|| async {
                        match admission.as_ref() {
                            Some(ready) => {
                                this.recovery_coordinator
                                    .run_admitted_supervision_audits(now, 64, ready, Some(&clocks.backfill_candidates))
                                    .await
                            }
                            None => {
                                this.recovery_coordinator
                                    .run_admitted_supervision_audits(now, 64, &pioneer_crud::RecoveryAdmission::unknown(), Some(&clocks.backfill_candidates))
                                    .await
                            }
                        }
                    }),
                )
                .await
                {
                    Ok((events, backfill_error)) => {
                        for event in events {
                            crate::database::attribution::scope_database_workload(
                                pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                                pioneer_observability::DatabaseOperationSite::RecoveryEventApply,
                                this.handle_recovery_event(event, now),
                            )
                            .await;
                        }
                        if let Some(error) = backfill_error {
                            phase_failed = true;
                            record_resilience_worker_poll_error("recovery timeout backfill point", &error, &mut transient_storage_poll_failed);
                        }
                    }
                    Err(error) => {
                        phase_failed = true;
                    record_resilience_worker_poll_error(
                            "recovery coordinator",
                            &error,
                            &mut transient_storage_poll_failed,
                        );
                    }
                }
                if transient_storage_poll_failed {
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_TRANSIENT_STORAGE_BACKOFF_SECONDS));
                }

                // An observation error remains Unknown through ALL independent
                // phases. Only an authoritative phase's own error selects the
                // early60s backoff above (F7).
                if phase_failed || admission.is_none() {
                    // Keep a partial healthy tail on grouped-read errors; restart
                    // completed/failed reconstruction after its point phase ran.
                    if clock_complete || clock_failed { clocks=Default::default(); }
                    return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(RESILIENCE_WORKER_POLL_INTERVAL_SECONDS));
                }
                let ready = admission.unwrap();
                let (more_active, active_next) = this.recovery_coordinator.active_supervision_schedule();
                if more_active || this.recovery_coordinator.orphan_supervision_more() { return Plan::Runnable; }
                // Supervision can finish an execution/release root capacity
                // after the pre-queue snapshot. Own ACK suppression must not
                // park newly enabled queue/outbox work in that same quantum.
                let had_clock_work = ready.terminal_repair || ready.terminal_delivery;
                let had_immediate = ready.control_runnable() || ready.orphan || ready.active;
                if had_clock_work || ready.control_runnable() { clock_rescan=true; }
                let mut ready = if had_immediate {
                    match this.crud_store.recovery_admission_core(this.turn_execution_owner_id.as_ref(), now).await {
                        Ok(mut ready) => {
                            control_admission.apply_at(&mut ready, pioneer_crud::utc_now());
                            ready.orphan = this.recovery_coordinator.orphan_supervision_more();
                            ready.terminal_audit = clocks.terminal_audit;
                            ready.backfill = clocks.backfill;
                            ready
                        },
                        Err(error) => {
                            record_resilience_worker_error("recovery admission", &error, ResilienceWorkerFailureImpact::Maintenance);
                            return this.recovery_coordinator.supervision_retry_plan(tokio::time::Instant::now() + Duration::from_secs(2));
                        }
                    }
                } else { ready };
                // The finite orphan pass owns this clock and its error budget;
                // grouped advisory clocks cannot bypass the same lane cooldown.
                ready.orphan_next = this.recovery_coordinator.orphan_supervision_deadline()
                    .map(pioneer_crud::unix_to_datetime);
                if !clock_complete || !admission_complete { return Plan::Runnable; }
                control_admission = Default::default();
                admission_complete = false;
                let clock_next = clocks.repair_next.into_iter().chain(clocks.terminal_next).min();
                clocks=Default::default();
                if std::mem::take(&mut clock_rescan) || ready.control_runnable() { return Plan::Runnable; }
                let next = ready.control_deadline().into_iter().chain(clock_next).chain(active_next.map(pioneer_crud::unix_to_datetime))
                    .chain(this.recovery_coordinator.orphan_supervision_deadline().map(pioneer_crud::unix_to_datetime)).min();
                let plan = next.map_or(Plan::Dormant, |at| Plan::WallDeadline(at.into()));
                this.recovery_coordinator.supervision_retry_deadline()
                    .map_or(plan, |at| plan.with_monotonic_deadline(at))
                }).await;
            let settled = owner.publish(observed, plan);
            if !settled && let Plan::MonotonicDeadline(at) = plan {
                // Own ambiguous commit advances the generation; the common
                // retry fence accepts it only without an intervening producer.
                owner.retry_after(
                    observed,
                    at.saturating_duration_since(tokio::time::Instant::now()),
                );
            }
            #[cfg(test)]
            if settled && !matches!(plan, Plan::Runnable) {
                this.completed_history_preparation_barrier
                    .wait_if_armed("__control_owner_settled__", &worker_cancellation)
                    .await;
            }
            if matches!(plan, Plan::Runnable) {
                tokio::select! { _ = worker_cancellation.cancelled() => return, _ = sleep(Duration::from_millis(10)) => {} }
            }
        }
    }

    pub(crate) async fn shutdown_resilience_workers(&self) {
        self.cli_history_shutdown.cancel();
        {
            let workers = &self.native_turn_event_delivery_workers;
            let _admission = workers
                .admission
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            workers.shutdown.cancel();
            workers.tasks.close();
        }
        self.native_turn_event_delivery_workers.tasks.wait().await;
        for worker in [
            &self.resilience_worker,
            &self.task_event_listener_worker,
            &self.hook_recovery_worker,
        ] {
            if let Some(handle) = worker.lock().await.take() {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.suspend_completed_history_checks().await;
        self.task_runtime.shutdown().await;
    }

    async fn run_cli_command_heartbeat_worker(processor: Weak<Self>) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(initial) = processor.upgrade() else {
            return;
        };
        let work = initial.crud_store.background_work("cli.commands");
        let Some(mut owner) = work.acquire() else {
            return;
        };
        let cancellation = initial.cli_history_shutdown.clone();
        let tracker = initial.cli_runtime_command_heartbeats.clone();
        drop(initial);
        let mut previous = None;
        let mut reconstruct = true;
        let mut upper: Option<String> = None;
        let mut after: Option<String> = None;
        let mut seen = std::collections::HashSet::new();
        let mut rescan = false;
        let mut failed = false;
        let mut retry_at: Option<tokio::time::Instant> = None;
        loop {
            let ready = tokio::select! {
                biased; _ = cancellation.cancelled() => return,
                ready = owner.ready() => ready,
                _ = tracker.dependency_released() => Ready::Work,
            };
            if ready == Ready::Closed {
                return;
            }
            let Some(this) = processor.upgrade() else {
                return;
            };
            let this = this.for_background_reconciliation();
            if ready == Ready::SettlementRequired {
                let settle_observation = owner.observe();
                reconstruct = true;
                upper = None;
                after = None;
                seen.clear();
                failed = false;
                retry_at = None;
                if this
                    .crud_store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    owner.retry_after(settle_observation, Duration::from_secs(60));
                }
                continue;
            }
            let observed = owner.observe();
            if previous.is_some_and(|old| old != observed) {
                if upper.is_some() {
                    rescan = true;
                } else {
                    reconstruct = true;
                    after = None;
                    seen.clear();
                    failed = false;
                }
                retry_at = None;
            }
            previous = Some(observed);
            let now = now_timestamp_secs();
            let (more, next) = tokio::select! { biased; _ = cancellation.cancelled() => return,
                result = owner.scope(crate::database::attribution::scope_database_workload(
                    pioneer_observability::DatabaseWorkload::ExecutionSupervision,
                    pioneer_observability::DatabaseOperationSite::CliCommandHeartbeat,
                    async {
                        if reconstruct && retry_at.is_none_or(|at| at <= tokio::time::Instant::now()) {
                            retry_at = None;
                            let result = async {
                                if upper.is_none() { upper = this.crud_store.command_supervision_upper().await?; after = None; seen.clear(); }
                                let complete = if let Some(last) = upper.as_deref() {
                                    let (next, rows) = this.crud_store.command_supervision_page(after.as_deref(), last).await?;
                                    let complete = next.is_none() || next.as_deref() == Some(last);
                                    after = next;
                                    let mut attempts = Vec::with_capacity(rows.len());
                                    for row in rows {
                                        match row { Ok(row) => attempts.push(row), Err(error) => { failed = true; record_resilience_worker_error("CLI command source", &error, ResilienceWorkerFailureImpact::Maintenance); } }
                                    }
                                    match this.restore_cli_runtime_command_items(None, attempts).await {
                                        Ok(keys) => seen.extend(keys),
                                        Err(error) => { failed = true; record_resilience_worker_error("CLI command reconstruction", &error, ResilienceWorkerFailureImpact::Maintenance); }
                                    }
                                    complete
                                } else { true };
                                if complete {
                                    upper = None; after = None;
                                    if std::mem::take(&mut rescan) { seen.clear(); }
                                    else if failed {
                                        retry_at = Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                        failed = false; seen.clear();
                                    } else {
                                        // Fence the complete durable reconstruction before
                                        // pruning old memory entries. A newer commit/unknown
                                        // generation retains another pass, never false Empty.
                                        if owner.publish(observed, Plan::Runnable) {
                                            this.cli_runtime_command_heartbeats.retain_discovered_commands(&seen).await;
                                            reconstruct = false;
                                            retry_at = None; seen.clear();
                                        }
                                    }
                                }
                                Ok::<_,anyhow::Error>(())
                            }.await;
                            if let Err(error) = result {
                                record_resilience_worker_error("CLI command discovery", &error, ResilienceWorkerFailureImpact::Maintenance);
                                retry_at = Some(tokio::time::Instant::now() + Duration::from_secs(30));
                                reconstruct = true;
                            }
                        }
                        // Reconstruction failure cannot suppress the existing
                        // bounded heartbeat quota of already known commands.
                        this.heartbeat_registered_cli_runtime_command_items(now).await;
                        let plan = this.cli_runtime_command_heartbeats.next_wait(now).await;
                        let next = retry_at.map_or(plan, |at|plan.with_monotonic_deadline(at));
                        (reconstruct && retry_at.is_none(), next)
                    },
                )) => result,
            };
            if owner.requires_settlement() {
                continue;
            }
            let plan = if more { Plan::Runnable } else { next };
            let settled = owner.publish(observed, plan);
            #[cfg(test)]
            if settled && !matches!(plan, Plan::Runnable) {
                this.completed_history_preparation_barrier
                    .wait_if_armed("__cli_commands_settled__", &cancellation)
                    .await;
            }
            #[cfg(not(test))]
            let _ = settled;
            drop(this);
            if more {
                tokio::select! { _ = cancellation.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
            }
        }
    }

    async fn update_cli_runtime_command_item_registry(
        &self,
        instance: &crate::cli_runtime::session_instance::CliSessionInstanceId,
        turn_binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        event: &RuntimeEvent,
    ) {
        self.cli_runtime_command_heartbeats
            .update_from_runtime_event(instance, turn_binding, event, now_timestamp_secs())
            .await;
        if matches!(event, RuntimeEvent::ItemStarted(started) if crate::cli_runtime::command_heartbeat::is_command_execution_kind(&started.item_kind))
            || matches!(event, RuntimeEvent::ItemCompleted(completed) if crate::cli_runtime::command_heartbeat::is_command_execution_kind(&completed.item_kind))
            || event.turn_terminal_kind().is_some()
        {
            self.crud_store
                .background_work("cli.commands")
                .dependency_released();
        }
    }

    async fn detach_cli_runtime_command_observer(
        &self,
        instance: &crate::cli_runtime::session_instance::CliSessionInstanceId,
    ) {
        self.cli_runtime_command_heartbeats
            .detach_observer(instance)
            .await;
        self.crud_store.background_work("cli.commands").invalidate();
    }

    async fn rehydrate_cli_runtime_command_items(
        &self,
        observer: Option<&crate::cli_runtime::session_instance::CliSessionInstanceId>,
        limit: u64,
    ) -> usize {
        let attempts = match self
            .crud_store
            .list_running_turn_item_attempts_by_type(TurnItemType::CommandExecution, limit)
            .await
        {
            Ok(attempts) => attempts,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to load durable CLI runtime command supervision candidates"
                );
                return 0;
            }
        };
        match self
            .restore_cli_runtime_command_items(observer, attempts)
            .await
        {
            Ok(keys) => {
                if !keys.is_empty() {
                    self.crud_store
                        .background_work("cli.commands")
                        .dependency_released();
                }
                keys.len()
            }
            Err(error) => {
                warn!(error = %format!("{error:#}"), "CLI command reconstruction incomplete");
                0
            }
        }
    }

    async fn restore_cli_runtime_command_items(
        &self,
        observer: Option<&crate::cli_runtime::session_instance::CliSessionInstanceId>,
        attempts: Vec<pioneer_crud::RunningTurnItemAttempt>,
    ) -> anyhow::Result<Vec<crate::cli_runtime::command_heartbeat::CliRuntimeCommandHeartbeatKey>>
    {
        let mut restored = Vec::new();
        let mut failed = false;
        for attempt in attempts {
            let binding = match self
                .crud_store
                .get_cli_runtime_turn_binding(attempt.turn_id.as_str())
                .await
            {
                Ok(Some(binding)) => binding,
                Ok(None) => continue,
                Err(error) => {
                    failed = true;
                    warn!(
                        turn_id = attempt.turn_id.as_str(),
                        item_id = attempt.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to load CLI runtime command owner binding"
                    );
                    continue;
                }
            };
            if !matches!(
                binding.status.as_str(),
                crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING
                    | crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
            ) {
                continue;
            }
            if let Some(observer) = observer
                && (observer.key().workspace_id != binding.workspace_id
                    || observer.key().runtime_id != binding.runtime_id
                    || observer.key().thread_id != binding.continuation_thread_id)
            {
                continue;
            }
            let owner_attempt = match self
                .crud_store
                .latest_cli_runtime_turn_attempt(binding.turn_id.as_str())
                .await
            {
                Ok(Some(owner_attempt)) if owner_attempt.status.is_active() => owner_attempt,
                Ok(_) => continue,
                Err(error) => {
                    failed = true;
                    warn!(
                        turn_id = binding.turn_id.as_str(),
                        item_id = attempt.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to load durable CLI runtime command execution attempt"
                    );
                    continue;
                }
            };
            if owner_attempt.runtime_id != binding.runtime_id
                || owner_attempt.native_thread_id != binding.native_thread_id
            {
                continue;
            }
            let running_segment = match self
                .crud_store
                .latest_running_cli_runtime_execution_segment_for_turn(binding.turn_id.as_str())
                .await
            {
                Ok(segment) => segment,
                Err(error) => {
                    failed = true;
                    warn!(
                        turn_id = binding.turn_id.as_str(),
                        item_id = attempt.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to load durable CLI runtime command execution segment"
                    );
                    continue;
                }
            };
            let Some(native_turn_id) = running_segment
                .as_ref()
                .map(|segment| segment.native_turn_id.clone())
                .or_else(|| owner_attempt.native_turn_id.clone())
            else {
                continue;
            };
            let owner = match self
                .crud_store
                .resolve_cli_runtime_native_turn_owner(
                    binding.runtime_id.as_str(),
                    native_turn_id.as_str(),
                )
                .await
            {
                Ok(Some(owner)) => owner,
                Ok(None) => continue,
                Err(error) => {
                    failed = true;
                    warn!(
                        turn_id = binding.turn_id.as_str(),
                        item_id = attempt.item_id.as_str(),
                        native_turn_id = native_turn_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to resolve durable CLI runtime command native owner"
                    );
                    continue;
                }
            };
            if owner.attempt.id != owner_attempt.id
                || owner.binding.turn_id != binding.turn_id
                || !owner.attempt.status.is_active()
                || owner.segment.as_ref().is_some_and(|segment| {
                    segment.status != pioneer_crud::CliRuntimeExecutionSegmentStatus::Running
                })
            {
                continue;
            }

            let attached_observer = if let Some(observer) = observer {
                Some(observer.clone())
            } else if let Some(manager) = self.cli_runtime_manager.as_ref() {
                let key = match crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                    binding.workspace_id.clone(),
                    binding.runtime_id.clone(),
                    binding.continuation_thread_id.clone(),
                ) {
                    Ok(key) => key,
                    Err(_) => {
                        failed = true;
                        continue;
                    }
                };
                manager
                    .existing_session(&key)
                    .await
                    .map(|handle| handle.instance().clone())
            } else {
                None
            };
            let key = self
                .cli_runtime_command_heartbeats
                .restore_from_durable(
                    attached_observer.as_ref(),
                    &binding,
                    &attempt,
                    native_turn_id,
                )
                .await;
            restored.push(key);
        }
        anyhow::ensure!(
            !failed,
            "CLI command reconstruction retained uncertain records"
        );
        Ok(restored)
    }

    #[cfg(test)]
    async fn heartbeat_due_cli_runtime_command_items(&self, now_unix: i64) -> usize {
        if self
            .cli_runtime_command_heartbeats
            .should_rehydrate(now_unix, CLI_RUNTIME_COMMAND_REHYDRATION_INTERVAL_SECONDS)
            .await
        {
            self.rehydrate_cli_runtime_command_items(None, 1_024).await;
        }
        self.heartbeat_registered_cli_runtime_command_items(now_unix)
            .await
    }

    async fn heartbeat_registered_cli_runtime_command_items(&self, now_unix: i64) -> usize {
        let due = self
            .cli_runtime_command_heartbeats
            .due_items_bounded(now_unix, 64)
            .await;
        let mut heartbeated = 0usize;
        for item in due {
            let key = item.key.clone();
            let owner = match self
                .crud_store
                .resolve_cli_runtime_native_turn_owner(
                    key.runtime_id.as_str(),
                    item.native_turn_id(),
                )
                .await
            {
                Ok(Some(owner)) if item.matches_active_native_turn_owner(&owner) => owner,
                Ok(_) => {
                    self.cli_runtime_command_heartbeats.remove_key(&item).await;
                    continue;
                }
                Err(error) => {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        turn_id = key.turn_id.as_str(),
                        item_id = key.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to validate CLI runtime command heartbeat binding"
                    );
                    self.cli_runtime_command_heartbeats
                        .mark_attempt_failed(&item, now_unix)
                        .await;
                    continue;
                }
            };

            let open_command = match self
                .crud_store
                .list_running_turn_item_attempts_for_turn(key.turn_id.as_str())
                .await
            {
                Ok(attempts) => attempts.into_iter().any(|attempt| {
                    attempt.item_id == key.item_id
                        && attempt.item_type == TurnItemType::CommandExecution
                }),
                Err(error) => {
                    warn!(
                        turn_id = key.turn_id.as_str(),
                        item_id = key.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to validate durable CLI runtime open command item"
                    );
                    self.cli_runtime_command_heartbeats
                        .mark_attempt_failed(&item, now_unix)
                        .await;
                    continue;
                }
            };
            if !open_command {
                self.cli_runtime_command_heartbeats.remove_key(&item).await;
                continue;
            }

            let Some((workspace_id, turn)) = (match self
                .crud_store
                .get_turn(
                    owner.binding.thread_id.as_str(),
                    owner.binding.turn_id.as_str(),
                )
                .await
            {
                Ok(turn) => turn,
                Err(error) => {
                    warn!(
                        turn_id = key.turn_id.as_str(),
                        item_id = key.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to load Pioneer turn for CLI runtime command supervision"
                    );
                    self.cli_runtime_command_heartbeats
                        .mark_attempt_failed(&item, now_unix)
                        .await;
                    continue;
                }
            }) else {
                self.cli_runtime_command_heartbeats.remove_key(&item).await;
                continue;
            };
            if turn.status != TurnStatus::InProgress {
                self.cli_runtime_command_heartbeats.remove_key(&item).await;
                continue;
            }
            match self
                .reconcile_cli_runtime_turn_from_runtime(
                    &owner.binding,
                    workspace_id.as_str(),
                    &turn,
                )
                .await
            {
                Ok(cli_runtime::CLIRuntimeAuthoritativeTurnState::Active(_)) => {}
                Ok(cli_runtime::CLIRuntimeAuthoritativeTurnState::Terminal) => {
                    self.cli_runtime_command_heartbeats.remove_key(&item).await;
                    continue;
                }
                Ok(cli_runtime::CLIRuntimeAuthoritativeTurnState::Superseded) => {
                    self.cli_runtime_command_heartbeats.remove_key(&item).await;
                    continue;
                }
                Ok(cli_runtime::CLIRuntimeAuthoritativeTurnState::Deferred) => {
                    let dependency = if let Ok(session_key) =
                        crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                            owner.binding.workspace_id.clone(),
                            owner.binding.runtime_id.clone(),
                            owner.binding.continuation_thread_id.clone(),
                        ) {
                        let gate = self
                            .cli_runtime_session_transition_mutex(&session_key)
                            .await;
                        let busy = gate.try_lock().is_err();
                        busy.then_some(gate)
                    } else {
                        None
                    };
                    self.cli_runtime_command_heartbeats
                        .mark_deferred(&item, now_unix, dependency)
                        .await;
                    continue;
                }
                Ok(cli_runtime::CLIRuntimeAuthoritativeTurnState::Unavailable) => {
                    self.cli_runtime_command_heartbeats
                        .mark_attempt_failed(&item, now_unix)
                        .await;
                    continue;
                }
                Err(error) => {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        turn_id = key.turn_id.as_str(),
                        item_id = key.item_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to confirm CLI runtime command activity"
                    );
                    self.cli_runtime_command_heartbeats
                        .mark_attempt_failed(&item, now_unix)
                        .await;
                    continue;
                }
            }

            let renewal = self
                .timeout_supervisor
                .heartbeat_cli_command_item_attempt(&owner, &key.item_id, now_unix)
                .await;
            if matches!(renewal, Ok(false)) {
                self.cli_runtime_command_heartbeats.remove_key(&item).await;
                continue;
            }
            if let Err(error) = renewal {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = key.turn_id.as_str(),
                    item_id = key.item_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to heartbeat active CLI runtime command item"
                );
                self.cli_runtime_command_heartbeats
                    .mark_attempt_failed(&item, now_unix)
                    .await;
                continue;
            }
            self.cli_runtime_command_heartbeats
                .mark_heartbeat_succeeded(&item, now_unix)
                .await;
            heartbeated = heartbeated.saturating_add(1);
        }

        heartbeated
    }

    pub async fn start_hook_recovery_worker(self: &Arc<Self>) {
        {
            let config = self.hook_recovery_config.read().await.clone();
            if !config.enabled {
                return;
            }
        }

        let mut guard = self.hook_recovery_worker.lock().await;
        if guard.is_some() {
            return;
        }

        if self.cli_history_shutdown.is_cancelled() {
            return;
        }
        let Some(mut owner) = self.crud_store.background_work("hooks.recovery").acquire() else {
            return;
        };
        let cancellation = self.cli_history_shutdown.clone();
        let processor = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            use pioneer_sqlite::background::{Plan, Ready};
            let mut first_pass = true;
            let mut clocks = pioneer_crud::HookClockPass::default();
            let mut admission_error = None;
            let mut clock_observation = None;
            let mut clock_rescan = false;
            loop {
                let ready = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return,
                    ready = owner.ready() => ready,
                };
                if ready == Ready::Closed {
                    return;
                }
                let Some(this) = processor.upgrade() else {
                    return;
                };
                let this = this.for_background_reconciliation();
                let observation = owner.observe();
                if clock_observation.is_some_and(|at| owner.producer_changed_since(at)) {
                    clock_rescan = true;
                }
                clock_observation = Some(observation);
                let config = this.hook_recovery_config.read().await.clone();
                let retry = Duration::from_millis(config.poll_interval_ms.max(250));
                if ready == Ready::SettlementRequired {
                    clock_rescan = true;
                    if let Err(error) = this
                        .crud_store
                        .database_connection()
                        .settle_background_change(&owner)
                        .await
                    {
                        error!(error = %error, "hook recovery change settlement failed");
                        owner.retry_after(observation, retry);
                    }
                    continue;
                }
                if !config.enabled {
                    owner.publish(observation, Plan::Dormant);
                    continue;
                }
                if first_pass && !config.startup_scan {
                    first_pass = false;
                    owner.publish(
                        observation,
                        Plan::MonotonicDeadline(tokio::time::Instant::now() + retry),
                    );
                    continue;
                }
                first_pass = false;
                let result = owner
                    .scope(
                        crate::database::attribution::scope_database_workload_result(
                            pioneer_observability::DatabaseWorkload::HookRecovery,
                            pioneer_observability::DatabaseOperationSite::HookRecovery,
                            async {
                                if this
                                    .hook_runtime
                                    .read()
                                    .await
                                    .as_ref()
                                    .is_none_or(|runtime| !runtime.has_run_store())
                                {
                                    clocks = Default::default();
                                    admission_error = None;
                                    return Ok::<_, anyhow::Error>(Plan::Dormant);
                                }
                                clocks.raw_limit = config.batch_size.max(1).min(64);
                                let complete = match this
                                    .crud_store
                                    .hook_recovery_clock_quantum(
                                        &mut clocks,
                                        config.stale_running_after_ms,
                                    )
                                    .await
                                {
                                    Ok(complete) => complete,
                                    Err(error) => {
                                        admission_error.get_or_insert(error);
                                        true
                                    }
                                };
                                let now = chrono::DateTime::<chrono::Utc>::from(
                                    this.crud_store.background_work("hooks.recovery").wall_now(),
                                )
                                .timestamp_millis();
                                // Singleton locator bounds discovery and isolates poison
                                // hydration/handler errors while keeping the existing runner.
                                for id in &clocks.candidates {
                                    match this.run_hook_recovery_pass(config.clone(), now, id).await
                                    {
                                        Ok(Some(summary)) => {
                                            if summary.scanned_count > 0 {
                                                clock_rescan = true;
                                            }
                                        }
                                        Ok(None) => return Ok::<_, anyhow::Error>(Plan::Dormant),
                                        Err(error) => {
                                            admission_error.get_or_insert(error);
                                        }
                                    }
                                }
                                if !complete {
                                    return Ok(Plan::Runnable);
                                }
                                if let Some(error) = admission_error.take() {
                                    return Err(error);
                                }
                                let next = clocks.next;
                                clocks = Default::default();
                                if std::mem::take(&mut clock_rescan) {
                                    return Ok(Plan::Runnable);
                                }
                                Ok(next.map_or(Plan::Dormant, |at| Plan::WallDeadline(at.into())))
                            },
                        ),
                    )
                    .await;
                match result {
                    Ok(plan) if !owner.requires_settlement() => {
                        let _published = owner.publish(observation, plan);
                        #[cfg(test)]
                        if _published && !matches!(plan, Plan::Runnable) {
                            this.completed_history_preparation_barrier
                                .wait_if_armed("__hook_clock_settled__", &cancellation)
                                .await;
                        }
                    }
                    Ok(_) => {
                        owner.retry_after(observation, retry);
                    }
                    Err(error) => {
                        admission_error = None;
                        clocks = Default::default();
                        clock_rescan = false;
                        error!(error = %error, "hook recovery discovery failed");
                        owner.retry_after(observation, retry);
                    }
                }
                // DB scopes drop before waiting. Due backlog yields through the
                // shared owner; dormant Empty has no domain-discovery timer.
            }
        });

        *guard = Some(handle);
    }

    async fn run_hook_recovery_pass(
        &self,
        config: GatewayHookRecoveryConfig,
        now_unix_ms: i64,
        candidate: &str,
    ) -> anyhow::Result<Option<pioneer_hooks::HookRecoverySummary>> {
        let Some(runtime) = self.hook_runtime.read().await.clone() else {
            return Ok(None);
        };
        if !runtime.has_run_store() {
            return Ok(None);
        }
        let runtime = GatewayHookRuntimeBuilder::from_runtime(self.crud_store.clone(), &runtime)
            .with_recovery_candidate(candidate.to_owned())
            .build();
        let options = HookRecoveryOptions {
            now_unix_ms,
            batch_size: config.batch_size,
            max_concurrent: config.max_concurrent,
            stale_running_after_ms: config.stale_running_after_ms,
            strict_debug: config.strict_debug,
        };
        match runtime.recover_background_runs_once(options).await {
            Ok(summary) => {
                if summary.scanned_count > 0
                    || summary.retried_count > 0
                    || summary.unrecoverable_count > 0
                    || summary.timed_out_count > 0
                {
                    info!(
                        scanned = summary.scanned_count,
                        recovered = summary.recovered_count,
                        executed = summary.executed_count,
                        retried = summary.retried_count,
                        timed_out = summary.timed_out_count,
                        unrecoverable = summary.unrecoverable_count,
                        skipped = summary.skipped_count,
                        "hook recovery pass completed"
                    );
                } else {
                    debug!("hook recovery pass completed with no recoverable runs");
                }
                Ok(Some(summary))
            }
            Err(error) => {
                error!(error = %format!("{error:#}"), "hook recovery pass failed");
                Err(anyhow::anyhow!("{error}"))
            }
        }
    }

    async fn start_task_event_listener(self: &Arc<Self>) {
        use pioneer_sqlite::background::{Plan, Ready};
        let mut guard = self.task_event_listener_worker.lock().await;
        if guard.is_some() || self.cli_history_shutdown.is_cancelled() {
            return;
        }
        let wake = self.crud_store.background_work("tasks.fanout");
        let Some(mut owner) = wake.acquire() else {
            return;
        };
        let processor = Arc::downgrade(self);
        let shutdown = self.cli_history_shutdown.clone();
        #[cfg(test)]
        let wait_barrier = self.recovery_due_wait_barrier.clone();
        *guard = Some(tokio::spawn(async move {
            loop {
                let ready = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    ready = owner.ready() => ready,
                };
                if ready == Ready::Closed {
                    return;
                }
                let Some(this) = processor.upgrade() else {
                    return;
                };
                let this = this.for_background_reconciliation();
                if ready == Ready::SettlementRequired {
                    let settle_observation = owner.observe();
                    if this
                        .crud_store
                        .database_connection()
                        .settle_background_change(&owner)
                        .await
                        .is_err()
                    {
                        warn!(
                            failure_class = "task_fanout_settlement_failure",
                            "task fanout change settlement deferred"
                        );
                        owner.retry_after(settle_observation, Duration::from_secs(5));
                    }
                    continue;
                }
                let observation = owner.observe();
                let result = owner
                    .scope(
                        crate::database::attribution::scope_database_workload_result(
                            pioneer_observability::DatabaseWorkload::TaskEventFanout,
                            pioneer_observability::DatabaseOperationSite::TaskEventFanout,
                            this.task_event_fanout_quantum(),
                        ),
                    )
                    .await;
                match result {
                    Ok(summary) if summary.errors == 0 && !owner.requires_settlement() => {
                        debug!(selected=summary.selected,event_inputs=summary.event_inputs,emitted=summary.emitted,errors=summary.errors,pending=?summary.pending,"task fanout quantum completed");
                        let plan = match summary.deadline {
                            Some(at) => std::time::UNIX_EPOCH
                                .checked_add(Duration::from_secs(u64::try_from(at).unwrap_or(0)))
                                .map(Plan::WallDeadline),
                            None => Some(Plan::Dormant),
                        };
                        if let Some(plan) = plan {
                            owner.publish(observation, plan);
                            #[cfg(test)]
                            if summary.deadline.is_none() {
                                wait_barrier
                                    .wait_if_armed("__task_fanout_before_wait__", &shutdown)
                                    .await;
                            }
                        } else {
                            owner.retry_after(observation, Duration::from_secs(5));
                        }
                    }
                    Ok(_) | Err(_) => {
                        warn!(
                            failure_class = "task_fanout_pass_failed",
                            "task fanout quantum deferred"
                        );
                        owner.retry_after(observation, Duration::from_secs(5));
                    }
                }
            }
        }));
    }

    async fn task_event_fanout_quantum(&self) -> anyhow::Result<TaskEventFanoutSummary> {
        self.task_event_fanout_quantum_with_clock(&now_timestamp_secs)
            .await
    }

    async fn task_event_fanout_quantum_with_clock(
        &self,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> anyhow::Result<TaskEventFanoutSummary> {
        use pioneer_crud::{
            TASK_EVENT_FANOUT_BYTE_BUDGET, TASK_EVENT_FANOUT_EVENT_BUDGET,
            TASK_EVENT_FANOUT_TASK_BUDGET,
        };
        let maintenance = self.crud_store.with_maintenance_access();
        let candidates = maintenance
            .due_task_event_fanout(clock(), TASK_EVENT_FANOUT_TASK_BUDGET)
            .await?;
        let mut summary = TaskEventFanoutSummary {
            selected: candidates.len(),
            ..Default::default()
        };
        let mut events_left = TASK_EVENT_FANOUT_EVENT_BUDGET;
        let mut bytes_left = TASK_EVENT_FANOUT_BYTE_BUDGET;
        let mut tasks_left = candidates.len();
        for candidate in candidates {
            if events_left == 0 || bytes_left == 0 {
                break;
            }
            let allowance = (events_left / tasks_left.max(1)).max(1);
            tasks_left -= 1;
            // One allocation even if claim/preparation/release panics. No
            // compensating rediscovery or fresh budget after an unknown commit.
            events_left = events_left.saturating_sub(allowance);
            let result = std::panic::AssertUnwindSafe(async {
                let Some(claim) = maintenance
                    .claim_task_event_fanout(&candidate, clock)
                    .await?
                else {
                    return Ok::<(), anyhow::Error>(());
                };
                let outcome = match self
                    .emit_task_event_fanout_page(
                        &claim,
                        allowance,
                        &mut bytes_left,
                        &mut summary.emitted,
                        clock,
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        summary.errors += 1;
                        warn!(
                            failure_class =
                                if reconciliation_diagnostics::is_storage_failure(&error) {
                                    "storage"
                                } else {
                                    "candidate"
                                },
                            "task fanout delivery failed; prefix ACKs retained"
                        );
                        pioneer_crud::TaskEventFanoutOutcome::Failed
                    }
                };
                maintenance
                    .release_task_event_fanout(&claim, outcome, clock)
                    .await?;
                Ok(())
            })
            .catch_unwind()
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    summary.errors += 1;
                    warn!(
                        failure_class = if reconciliation_diagnostics::is_storage_failure(&error) {
                            "storage"
                        } else {
                            "candidate"
                        },
                        "task fanout candidate bookkeeping failed"
                    );
                }
                Err(_) => {
                    // Panic says nothing about commit/rollback. Leave durable
                    // state intact, including confirmed ACKs; never compensate.
                    summary.errors += 1;
                    warn!(failure_class = "panic", "task fanout candidate panicked");
                }
            }
        }
        summary.event_inputs = TASK_EVENT_FANOUT_EVENT_BUDGET - events_left;
        summary.pending = match maintenance.next_task_event_fanout_deadline().await {
            Ok(deadline) => {
                summary.deadline = deadline;
                Some(deadline.is_some())
            }
            Err(error) => {
                summary.errors += 1;
                warn!(
                    failure_class = if reconciliation_diagnostics::is_storage_failure(&error) {
                        "storage"
                    } else {
                        "candidate"
                    },
                    "task fanout queue state unavailable"
                );
                None
            }
        };
        Ok(summary)
    }

    async fn emit_task_event_fanout_page(
        &self,
        claim: &pioneer_crud::TaskEventFanoutClaim,
        limit: usize,
        bytes_left: &mut usize,
        emitted: &mut usize,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> anyhow::Result<pioneer_crud::TaskEventFanoutOutcome> {
        let maintenance = self.crud_store.with_maintenance_access();
        let after = maintenance
            .get_task_event_fanout_cursor(&claim.task_id)
            .await?
            .with_context(|| {
                format!(
                    "missing durable task event fanout cursor for `{}`",
                    claim.task_id
                )
            })?;
        // Pending owns the durable post-install interval. A legacy or reset
        // cursor may lag behind it, but must never reopen pre-install history.
        let after = after.max(claim.first_sequence - 1);
        let allow_oversized = *bytes_left == pioneer_crud::TASK_EVENT_FANOUT_BYTE_BUDGET;
        let page = maintenance
            .task_event_fanout_page(&claim.task_id, after, limit, bytes_left, allow_oversized)
            .await;
        let events = match page {
            Ok(pioneer_crud::TaskEventFanoutPage::Prefix { events, .. }) => events,
            Ok(pioneer_crud::TaskEventFanoutPage::BudgetDeferred) => {
                return Ok(pioneer_crud::TaskEventFanoutOutcome::BudgetDeferred);
            }
            Err(error) => return Err(error),
        };
        // Repository charged the admissible prefix before reading full rows,
        // so decode/read/emit panics cannot issue the same byte budget again.
        for event in events {
            let event = event?;
            if !maintenance.renew_task_event_fanout(claim, clock).await? {
                anyhow::bail!("task fanout holder changed before emit");
            }
            let sequence = event.sequence;
            // Poll emit and renewal together. Awaiting renewal inside a timer
            // branch could deadlock if emit currently owns a short write tx.
            // Both futures belong to this dispatcher and drop on cancellation.
            let result: anyhow::Result<()> = tokio::select! {
                result=self.emit_task_event(event)=>result,
                result=async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        if !maintenance.renew_task_event_fanout(claim, clock).await? {
                            anyhow::bail!("task fanout holder changed during emit");
                        }
                    }
                }=>result,
            };
            result?;
            self.crud_store
                .ack_task_event_fanout(claim, sequence, clock)
                .await?;
            *emitted += 1;
        }
        Ok(pioneer_crud::TaskEventFanoutOutcome::Delivered)
    }

    pub(crate) fn current_skills_snapshot_version(&self) -> u64 {
        self.skills_snapshot_version.load(Ordering::SeqCst)
    }

    pub(crate) fn next_skills_snapshot_version(&self) -> u64 {
        self.skills_snapshot_version
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    pub(crate) fn current_mcp_snapshot_version(&self) -> u64 {
        self.mcp_snapshot_version.load(Ordering::SeqCst)
    }

    pub(crate) fn next_mcp_snapshot_version(&self) -> u64 {
        self.mcp_snapshot_version
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    pub(crate) async fn acquire_skills_write_lock(&self) -> OwnedMutexGuard<()> {
        self.skills_write_lock.clone().lock_owned().await
    }

    pub(crate) async fn install_one_cli_runtime_skill(
        &self,
        plan: &crate::cli_runtime::skills::CliRuntimeSkillInstallPlan,
    ) -> anyhow::Result<crate::cli_runtime::skills::CliRuntimeSkillInstallResult> {
        let candidates = self
            .external_runtime_receipt_conversion_candidates()
            .await?;
        crate::cli_runtime::skills::install_one_cli_runtime_skill(
            &self.cli_runtime_skill_destination_locks,
            &self.skills_write_lock,
            candidates.as_slice(),
            plan,
        )
        .await
    }

    pub(crate) async fn external_runtime_receipt_conversion_candidates(
        &self,
    ) -> anyhow::Result<Vec<pioneer_skills::ExternalRuntimeReceiptConversionCandidate>> {
        let mut candidates = self
            .crud_store
            .list_skill_installations()
            .await?
            .into_iter()
            .map(
                |row| pioneer_skills::ExternalRuntimeReceiptConversionCandidate {
                    skill_id: row.skill_id,
                    owner: row.owner,
                    slug: row.slug,
                    source_kind: row.source_kind,
                },
            )
            .collect::<Vec<_>>();
        for bundled in
            crate::system_skills::bundled_system_skill_catalog_entries(std::path::Path::new(""))?
        {
            if candidates
                .iter()
                .any(|candidate| candidate.skill_id == bundled.skill_id)
            {
                continue;
            }
            candidates.push(pioneer_skills::ExternalRuntimeReceiptConversionCandidate {
                skill_id: bundled.skill_id,
                owner: bundled.owner,
                slug: bundled.slug,
                source_kind: "system".to_owned(),
            });
        }
        candidates.sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
        Ok(candidates)
    }

    pub(crate) async fn acquire_skill_upload_lock(&self, upload_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut guard = self.skill_upload_locks.lock().await;
            guard
                .entry(upload_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }
}

fn voice_input_semantic_status_changed(
    previous: &GatewayVoiceInputSettings,
    current: &GatewayVoiceInputSettings,
) -> bool {
    previous.enabled != current.enabled
        || previous.provider != current.provider
        || previous.model != current.model
        || previous.runtime.phase != current.runtime.phase
        || previous.runtime.effective_enabled != current.runtime.effective_enabled
        || previous.runtime.model != current.runtime.model
        || previous.runtime.error != current.runtime.error
}

fn empty_object_value() -> JsonValue {
    JsonValue::Object(serde_json::Map::new())
}

pub(crate) fn now_timestamp_secs() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

pub(crate) fn now_timestamp_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn parse_request_id(value: &JsonValue) -> Option<RequestId> {
    value
        .as_object()
        .and_then(|object| object.get("id"))
        .and_then(|value| serde_json::from_value::<RequestId>(value.clone()).ok())
}

fn first_user_text(input: &[pioneer_protocol::UserInput]) -> Option<String> {
    input.iter().find_map(|item| match item {
        pioneer_protocol::UserInput::Text { text, .. } => {
            let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
            (!normalized.is_empty()).then_some(normalized)
        }
        _ => None,
    })
}

impl MessageProcessor {
    /// Validates user-authored Turn attachments as exact, immutable artifact
    /// versions scoped to the already-authorized target thread. The persisted
    /// reference is metadata only; content remains behind the existing Axum
    /// HTTP authorization path.
    async fn validate_turn_artifact_user_inputs(
        &self,
        workspace_id: &str,
        thread_id: &str,
        input: &[pioneer_protocol::UserInput],
    ) -> anyhow::Result<()> {
        let database = self.crud_store.database_connection();
        if !input
            .iter()
            .any(|value| matches!(value, pioneer_protocol::UserInput::Artifact { .. }))
        {
            return Ok(());
        }
        let target_authorization_root = pioneer_crud::resolve_artifact_binding_authorization_root(
            &database,
            workspace_id,
            Some(thread_id),
            None,
            None,
            None,
        )
        .await
        .map_err(|_| anyhow::anyhow!("artifact reference is unavailable"))?;

        for value in input {
            let pioneer_protocol::UserInput::Artifact {
                artifact_id,
                version_id,
            } = value
            else {
                continue;
            };
            let requested_version_id = version_id.as_deref();
            let Some(version_id) = requested_version_id
                .as_deref()
                .map(str::trim)
                .filter(|version_id| !version_id.is_empty())
            else {
                anyhow::bail!("artifact reference requires an exact version");
            };
            if artifact_id.trim() != artifact_id || requested_version_id != Some(version_id) {
                anyhow::bail!("artifact reference is unavailable");
            }
            let summary = self
                .artifact_service
                .get_artifact(workspace_id, artifact_id, Some(version_id))
                .await
                .map_err(|_| anyhow::anyhow!("artifact reference is unavailable"))?;
            if summary.workspace_id != workspace_id
                || summary.artifact.artifact_id != *artifact_id
                || summary.artifact.version_id.as_deref() != Some(version_id)
                || summary.artifact.status != pioneer_protocol::ArtifactStatus::Ready
            {
                anyhow::bail!("artifact reference is unavailable");
            }

            let durable_scope_matches = pioneer_crud::resolve_artifact_authorization_scope(
                &database,
                artifact_id,
                Some(workspace_id),
                None,
            )
            .await
            .map_err(|_| anyhow::anyhow!("artifact reference is unavailable"))?
            .is_some_and(|scope| {
                scope
                    .thread_id
                    .as_deref()
                    .is_some_and(|artifact_root_thread_id| {
                        target_authorization_root.as_deref() == Some(artifact_root_thread_id)
                    })
            });
            // Planned-turn uploads can refer to the not-yet-created Turn, so
            // the generic resolver may not materialize that binding yet. Its
            // existing direct thread scope is still authoritative only when
            // every stored binding agrees with the exact target thread.
            let direct_thread_scope_matches = summary.primary_thread_id.as_deref()
                == Some(thread_id)
                && summary.bindings.iter().all(|binding| {
                    binding.workspace_id == workspace_id
                        && binding.thread_id.as_deref() == Some(thread_id)
                });
            if !durable_scope_matches && !direct_thread_scope_matches {
                anyhow::bail!("artifact reference is unavailable");
            }
        }

        Ok(())
    }

    async fn resolve_provider_artifact_inputs(
        &self,
        workspace_id: &str,
        input: &[pioneer_protocol::UserInput],
    ) -> anyhow::Result<Vec<ResolvedArtifactInput>> {
        let mut resolved = Vec::new();
        for value in input {
            let pioneer_protocol::UserInput::Artifact {
                artifact_id,
                version_id,
            } = value
            else {
                continue;
            };

            let provider_artifact = self
                .artifact_service
                .resolve_provider_attachment(workspace_id, artifact_id, version_id.as_deref())
                .await
                .with_context(|| {
                    format!("failed to resolve artifact `{artifact_id}` for provider input")
                })?;
            resolved.push(ResolvedArtifactInput {
                artifact_id: provider_artifact.artifact_id,
                version_id: provider_artifact.version_id,
                content_type: provider_artifact.content_type,
                attachment: provider_artifact.attachment,
            });
        }

        Ok(resolved)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn persist_turn_runtime_snapshot(
        &self,
        thread_id: &str,
        workspace_id: &str,
        turn_id: &str,
        mode: pioneer_protocol::ThreadMode,
        hook_runtime_context: &pioneer_agent::AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        reasoning_effort: Option<&str>,
        workspace_skill_policies: &HashMap<
            pioneer_skills::SkillPolicyKey,
            pioneer_agent::WorkspaceSkillPolicy,
        >,
        input: &[pioneer_protocol::UserInput],
        capabilities: &[pioneer_protocol::TurnCapability],
        resolved_artifacts: &[ResolvedArtifactInput],
        runtime_environment: &HashMap<String, String>,
        history: &[ChatMessage],
        agent_skill_overlay: &[pioneer_skills::AgentSkillRuntimeEntry],
        accepted_history: Option<&crate::compaction::frozen::PreparedHistory>,
    ) -> anyhow::Result<()> {
        let canonical = history.iter().all(|message| message.provenance.is_some());
        let mut snapshot = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
            thread_id,
            workspace_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            reasoning_effort,
            workspace_skill_policies,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            if canonical { &[] } else { history },
            agent_skill_overlay,
        )?;
        let mut frozen_prepared = None;
        if let Some(prepared) = accepted_history {
            anyhow::ensure!(
                canonical && history == prepared.messages.as_slice(),
                "runtime history does not match its accepted preparation"
            );
            anyhow::ensure!(
                prepared.accepted_scopes.contains(thread_id),
                "runtime preparation does not include its destination"
            );
            snapshot.history_json = serde_json::to_string(&prepared.descriptor)?;
        } else if canonical {
            let allowed =
                if let Some(accepted) = self.crud_store.get_turn_runtime_snapshot(turn_id).await? {
                    anyhow::ensure!(
                        accepted.workspace_id == workspace_id && accepted.thread_id == thread_id,
                        "runtime history scope identity mismatch"
                    );
                    crate::turn_runtime_snapshot::accepted_history_scopes_from_snapshot(
                        self.crud_store.as_ref(),
                        &accepted,
                    )
                    .await?
                } else {
                    crate::compaction::frozen::execution_history_scopes(
                        self.crud_store.as_ref(),
                        workspace_id,
                        thread_id,
                        turn_id,
                        hook_runtime_context.conversation_thread_id.as_deref(),
                    )
                    .await?
                };
            let prepared = crate::compaction::frozen::capture_prepared(
                self.crud_store.as_ref(),
                workspace_id,
                thread_id,
                &allowed,
                history,
            )
            .await
            .context("failed to freeze canonical runtime history")?;
            snapshot.history_json = serde_json::to_string(&prepared.descriptor)?;
            frozen_prepared = Some(prepared);
        }
        // The borrowed preparation retains its hold through this atomic root
        // handoff; the writer rechecks the epochs read during preparation.
        self.crud_store
            .upsert_turn_runtime_snapshot_with_source_epochs(
                snapshot,
                accepted_history
                    .map(|prepared| &prepared.source_epochs)
                    .unwrap_or(&Default::default()),
            )
            .await
            .with_context(|| format!("failed to persist runtime snapshot for turn `{turn_id}`"))?;
        drop(frozen_prepared);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn persist_turn_runtime_snapshot_with_optional_agent_overlay(
        &self,
        thread_id: &str,
        workspace_id: &str,
        turn_id: &str,
        mode: pioneer_protocol::ThreadMode,
        hook_runtime_context: &pioneer_agent::AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        reasoning_effort: Option<&str>,
        workspace_skill_policies: &HashMap<
            pioneer_skills::SkillPolicyKey,
            pioneer_agent::WorkspaceSkillPolicy,
        >,
        input: &[pioneer_protocol::UserInput],
        capabilities: &[pioneer_protocol::TurnCapability],
        resolved_artifacts: &[ResolvedArtifactInput],
        runtime_environment: &HashMap<String, String>,
        history: &[ChatMessage],
        agent_skill_overlay: &mut Vec<pioneer_skills::AgentSkillRuntimeEntry>,
        accepted_history: Option<&crate::compaction::frozen::PreparedHistory>,
    ) -> anyhow::Result<()> {
        match self
            .persist_turn_runtime_snapshot(
                thread_id,
                workspace_id,
                turn_id,
                mode,
                hook_runtime_context,
                model,
                provider_name,
                reasoning_effort,
                workspace_skill_policies,
                input,
                capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                agent_skill_overlay.as_slice(),
                accepted_history,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if !agent_skill_overlay.is_empty() => {
                warn!(
                    workspace_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist Agent skill pins; retrying authoritative base-only snapshot"
                );
                agent_skill_overlay.clear();
                self.persist_turn_runtime_snapshot(
                    thread_id,
                    workspace_id,
                    turn_id,
                    mode,
                    hook_runtime_context,
                    model,
                    provider_name,
                    reasoning_effort,
                    workspace_skill_policies,
                    input,
                    capabilities,
                    resolved_artifacts,
                    runtime_environment,
                    history,
                    &[],
                    accepted_history,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn native_api_provider_supports_agent_skill_overlay(
        &self,
        workspace_id: &str,
        provider_name: &str,
    ) -> bool {
        match self
            .provider_registry
            .get_or_create_for_workspace(workspace_id, provider_name)
        {
            Ok(provider) => provider.capabilities().tool_calling,
            Err(error) => {
                warn!(
                    workspace_id,
                    provider = provider_name,
                    error = %format!("{error:#}"),
                    "failed to resolve native provider capabilities; Agent skill overlay disabled"
                );
                false
            }
        }
    }

    pub(crate) async fn load_agent_skill_overlay_for_new_native_turn(
        &self,
        workspace_id: &str,
        turn_id: &str,
    ) -> Vec<pioneer_skills::AgentSkillRuntimeEntry> {
        let Some(supervisor) = self.self_improvement_supervisor.as_ref() else {
            return Vec::new();
        };
        let overlay = supervisor.load_overlay_for_new_turn(workspace_id).await;
        match overlay {
            Ok(entries) => entries,
            Err(error) => {
                warn!(
                    workspace_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to materialize Agent skill overlay; continuing without it"
                );
                Vec::new()
            }
        }
    }

    pub(crate) async fn load_agent_skill_overlay_for_scoped_turn(
        &self,
        principal_id: &pioneer_protocol::PrincipalId,
        workspace_id: &str,
        turn_id: &str,
    ) -> anyhow::Result<Vec<pioneer_skills::AgentSkillRuntimeEntry>> {
        if self.self_improvement_supervisor.is_none() {
            return Ok(Vec::new());
        }
        crate::self_improvement::overlay::load_scoped_agent_skill_overlay(
            self.crud_store.as_ref(),
            principal_id,
            workspace_id,
        )
        .await
        .with_context(|| {
            format!("failed to materialize Member Agent skill overlay for turn `{turn_id}`")
        })
    }

    async fn user_message_payload_from_input_resolved(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        input: &[pioneer_protocol::UserInput],
    ) -> anyhow::Result<Option<(String, Vec<pioneer_protocol::UserMessageAttachment>)>> {
        let mut text_parts = Vec::new();
        let mut attachments = Vec::new();
        let mut bound_artifact_ids = Vec::new();

        for (index, value) in input.iter().enumerate() {
            match value {
                pioneer_protocol::UserInput::Text { text, .. } => {
                    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    if !normalized.is_empty() {
                        text_parts.push(normalized);
                    }
                }
                pioneer_protocol::UserInput::Image { url } => {
                    attachments
                        .push(pioneer_protocol::UserMessageAttachment::Image { url: url.clone() });
                }
                pioneer_protocol::UserInput::LocalImage { path } => {
                    attachments.push(pioneer_protocol::UserMessageAttachment::LocalImage {
                        path: path.clone(),
                    });
                }
                pioneer_protocol::UserInput::File { url } => {
                    attachments
                        .push(pioneer_protocol::UserMessageAttachment::File { url: url.clone() });
                }
                pioneer_protocol::UserInput::LocalFile { path } => {
                    attachments.push(pioneer_protocol::UserMessageAttachment::LocalFile {
                        path: path.clone(),
                    });
                }
                pioneer_protocol::UserInput::Audio { url } => {
                    attachments
                        .push(pioneer_protocol::UserMessageAttachment::Audio { url: url.clone() });
                }
                pioneer_protocol::UserInput::LocalAudio { path } => {
                    attachments.push(pioneer_protocol::UserMessageAttachment::LocalAudio {
                        path: path.clone(),
                    });
                }
                pioneer_protocol::UserInput::Video { url } => {
                    attachments
                        .push(pioneer_protocol::UserMessageAttachment::Video { url: url.clone() });
                }
                pioneer_protocol::UserInput::LocalVideo { path } => {
                    attachments.push(pioneer_protocol::UserMessageAttachment::LocalVideo {
                        path: path.clone(),
                    });
                }
                pioneer_protocol::UserInput::Mention { name, .. } => {
                    text_parts.push(format!("mention: {name}"));
                }
                pioneer_protocol::UserInput::Artifact {
                    artifact_id,
                    version_id,
                } => {
                    let summary = self
                        .artifact_service
                        .get_artifact(workspace_id, artifact_id, version_id.as_deref())
                        .await
                        .with_context(|| {
                            format!("failed to resolve artifact `{artifact_id}` for user message")
                        })?;
                    let resolved_version_id = summary.artifact.version_id.clone();
                    attachments.push(pioneer_protocol::UserMessageAttachment::Artifact {
                        artifact: summary.artifact,
                    });
                    self.artifact_service
                        .bind_artifact(BindArtifactRequest {
                            workspace_id: workspace_id.to_owned(),
                            artifact_id: artifact_id.clone(),
                            version_id: resolved_version_id,
                            target: ArtifactBindingTarget {
                                thread_id: Some(thread_id.to_owned()),
                                turn_id: Some(turn_id.to_owned()),
                                message_id: Some(item_id.to_owned()),
                                turn_item_id: Some(item_id.to_owned()),
                                tool_call_id: None,
                                task_id: None,
                                task_run_id: None,
                                binding_kind: ArtifactBindingKind::UserInput,
                                direction: ArtifactBindingDirection::Input,
                                role: Some(ArtifactRole::User),
                                item_index: Some(index as i64),
                            },
                            metadata: Default::default(),
                        })
                        .await
                        .with_context(|| {
                            format!("failed to bind artifact `{artifact_id}` to user message")
                        })?;
                    bound_artifact_ids.push(artifact_id.clone());
                }
            }
        }

        if !bound_artifact_ids.is_empty() {
            self.send_thread_artifacts_changed_to_thread_and_ancestors(
                workspace_id,
                thread_id,
                bound_artifact_ids,
                "user_input_binding",
                now_timestamp_secs(),
            )
            .await;
        }

        let text = text_parts.join("\n");
        if text.is_empty() && attachments.is_empty() {
            Ok(None)
        } else {
            Ok(Some((text, attachments)))
        }
    }
}

fn user_message_item_id(turn_id: &str) -> String {
    format!("user_{turn_id}")
}

fn fallback_title_from_first_user_text(user_text: &str) -> Option<String> {
    let words = user_text.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() {
        return None;
    }

    if words.len() > 6 {
        return Some(format!("{}...", words[..6].join(" ")));
    }

    Some(words.join(" "))
}

fn artifact_quota_policy_from_config(config: &GatewayArtifactsConfig) -> ArtifactQuotaPolicy {
    ArtifactQuotaPolicy {
        max_file_bytes: config.max_file_bytes,
        max_workspace_bytes: config.max_workspace_bytes,
        max_files_per_workspace: config.max_files_per_workspace,
        warn_at_percent: config.quota_warn_at_percent,
    }
}

#[cfg(test)]
impl MessageProcessor {
    pub fn new(
        thread_manager: Arc<ThreadManager>,
        provider_registry: Arc<ProviderRegistry>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
        gateway_secrets: Arc<GatewaySecrets>,
        summary_config: summary::SummaryConfig,
        tool_loop_config: ToolLoopConfig,
    ) -> Self {
        let memory_runtime = Arc::new(GatewayMemoryRuntime::disabled(crud_store.clone()));
        let runtime_home = std::env::temp_dir()
            .join("pioneer-message-tests")
            .join(generate_id(21));
        std::fs::create_dir_all(&runtime_home)
            .expect("message processor test runtime home should be created");
        Self::new_with_memory_runtime(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_secrets,
            summary_config,
            tool_loop_config,
            memory_runtime,
            runtime_home,
            GatewayArtifactsConfig::default(),
        )
    }

    pub(crate) fn set_mcp_runtime_connector_for_tests(
        &self,
        connector: Arc<dyn pioneer_mcp::McpRuntimeConnector>,
    ) {
        self.mcp_service.set_connector_for_tests(connector);
    }

    pub(crate) fn with_agent_manager(
        thread_manager: Arc<ThreadManager>,
        agent_manager: Arc<AgentManager>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
    ) -> Self {
        let maintenance_crud_store = Arc::new(crud_store.with_maintenance_access());
        let timeout_supervisor = Arc::new(TimeoutSupervisor::new(
            crud_store.clone(),
            TimeoutPolicyRegistry::default(),
        ));
        let provider_registry = Arc::new(ProviderRegistry::with_provider(
            "openai",
            Arc::new(pioneer_provider::providers::EchoProvider::new()),
        ));
        let now_snapshot = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        };
        let web = pioneer_config::GatewayWebToolsConfig::default();
        let gateway_secrets = Arc::new(GatewaySecrets::new(Arc::new(
            pioneer_keystore::MemorySecretStore::new(),
        )));
        let mcp_snapshot_version = Arc::new(AtomicU64::new(0));
        let authorization_invalidation_hub = Arc::new(AuthorizationInvalidationHub::default());
        let execution_leases = Arc::new(crate::authorization::ExecutionLeaseRegistry::default());
        let observation_governor =
            Arc::new(crate::authorization::ObservationAdmissionGovernor::default());
        let turn_execution_owner_id: Arc<str> =
            format!("gateway-turn-owner-{}", generate_id(21)).into();
        let mcp_service = Arc::new(McpService::new(
            crud_store.clone(),
            session_manager.clone(),
            gateway_secrets.clone(),
            mcp_snapshot_version.clone(),
            authorization_invalidation_hub.clone(),
            execution_leases.clone(),
        ));
        let task_agent_executor = Arc::new(task_agent_executor::TaskAgentExecutor::new());
        let task_runtime = Arc::new(TaskRuntime::new(crud_store.clone()));
        let memory_runtime = Arc::new(GatewayMemoryRuntime::disabled(crud_store.clone()));
        let artifact_runtime_home = std::env::temp_dir().join("pioneer-message-tests");
        let artifact_service = Arc::new(ArtifactService::new(
            crud_store.clone(),
            Arc::new(LocalArtifactBlobStore::new(artifact_runtime_home.clone())),
        ));
        let artifact_uploads = Arc::new(artifacts::upload::ArtifactUploadSessionManager::new(
            artifact_runtime_home
                .join("artifacts")
                .join("upload_sessions"),
        ));
        let normalized_tool_loop_config = ToolLoopConfig {
            provider: pioneer_provider::ProviderTimeoutPolicy::default(),
            web: pioneer_tools::WebToolsConfig {
                default_timeout_ms: web.default_timeout_ms,
                hard_max_timeout_ms: web.hard_max_timeout_ms,
                default_fetch_max_bytes: web.default_fetch_max_bytes,
                hard_fetch_max_bytes: web.hard_fetch_max_bytes,
                default_download_max_bytes: web.default_download_max_bytes,
                hard_download_max_bytes: web.hard_download_max_bytes,
                default_max_results: web.default_max_results,
                hard_max_results: web.hard_max_results,
                default_snippet_chars: web.default_snippet_chars,
                hard_max_snippet_chars: web.hard_max_snippet_chars,
                default_link_count: web.default_link_count,
                hard_link_count: web.hard_link_count,
                default_render_max_chars: web.default_render_max_chars,
                ddg_html_search_url: web.ddg_html_search_url,
                ddg_instant_api_url: web.ddg_instant_api_url,
                default_user_agent: web.default_user_agent,
            },
            computer_use: pioneer_tools::ComputerUseToolsConfig {
                runtime_home_dir: std::env::temp_dir().join("pioneer-message-tests"),
                ..pioneer_tools::ComputerUseToolsConfig::default()
            },
            skills: pioneer_agent::SkillsLoopConfig {
                enabled: true,
                max_skills_per_source: 256,
                max_skill_file_bytes: 1024 * 1024,
                prompt_max_chars: 24_000,
                allow_implicit_invocation: false,
                system_roots: Vec::new(),
                user_roots: vec!["{homeDirectory}/skills/workspace/{workspaceId}/user".to_owned()],
                registry_roots: vec![
                    "{homeDirectory}/skills/workspace/{workspaceId}/registry".to_owned(),
                ],
                system_import_roots: Vec::new(),
                user_import_roots: Vec::new(),
                registry_import_roots: Vec::new(),
                validation: pioneer_agent::SkillsValidationLoopConfig {
                    strict_agentskills: true,
                    accept_openclaw_profile: true,
                },
                security: pioneer_agent::SkillsSecurityLoopConfig {
                    allow_untrusted_install: false,
                    min_trust_for_shell_tools: pioneer_skills::SkillTrustLevel::Verified,
                    min_trust_for_http_tools: pioneer_skills::SkillTrustLevel::Community,
                    min_trust_for_function_proxy_tools: pioneer_skills::SkillTrustLevel::Community,
                    max_install_archive_bytes: 10 * 1024 * 1024,
                    max_install_archive_compressed_bytes: 10 * 1024 * 1024,
                    max_install_archive_uncompressed_bytes: 50 * 1024 * 1024,
                    max_install_archive_entries: 2048,
                    max_install_file_bytes: 1024 * 1024,
                    upload_ttl_secs: 3600,
                    upload_recommended_chunk_size_bytes: 256 * 1024,
                    upload_max_chunk_size_bytes: 1024 * 1024,
                },
                dependencies: pioneer_agent::SkillsDependenciesLoopConfig {
                    preflight_on_resolve: true,
                    runtime_recheck_on_tool_call: true,
                },
                runtime: pioneer_agent::SkillsRuntimeLoopConfig {
                    enable_dynamic_tools: true,
                    enable_read_skill: true,
                    max_dynamic_tools_per_skill: 64,
                    read_skill_max_chars: 72_000,
                    compact_mode_threshold: 6,
                    allow_shell_tools: true,
                    allow_http_tools: true,
                    allow_function_proxy_tools: true,
                },
            },
            memory: pioneer_memory::hooks::MemoryLoopConfig::default(),
            budget: pioneer_tools::ToolLoopBudgetConfig::default(),
            execution_windows: pioneer_tools::ExecutionWindowsConfig::default(),
            retry: pioneer_tools::ToolRetryBudgetConfig::default(),
        }
        .normalized();
        let recovery_coordinator = Arc::new(
            RecoveryCoordinator::new(
                crud_store.clone(),
                agent_manager.clone(),
                provider_registry.clone(),
                RecoveryPolicyRegistry::default(),
                normalized_tool_loop_config.clone(),
            )
            .with_authorization_invalidation_hub(authorization_invalidation_hub.clone())
            .with_execution_leases(execution_leases.clone())
            .with_turn_execution_owner_id(turn_execution_owner_id.clone()),
        );
        let memory_loop_config =
            Arc::new(StdRwLock::new(normalized_tool_loop_config.memory.clone()));
        let thread_episodic_backend: Arc<dyn ThreadEpisodicMemvidBackend> =
            Arc::new(MemvidThreadEpisodicBackend::new());
        let thread_episodic_storage_root =
            isolated_test_thread_episodic_storage_root(&artifact_runtime_home);
        let thread_episodic_embedding_provider_resolver = Arc::new(
            ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver::new(
                provider_registry.clone(),
                artifact_runtime_home.clone(),
                ThreadEpisodicRuntimeConfig::default().vector_search,
            ),
        );
        let thread_episodic_workspace_refill_supervisor =
            Arc::new(crate::database::startup::ThreadEpisodicWorkspaceRefillSupervisor::default());
        let thread_episodic_index_executor = Arc::new(
            ThreadEpisodicIndexExecutor::new(
                maintenance_crud_store.clone(),
                thread_episodic_backend.clone(),
                Arc::new(RuntimeVectorThreadEpisodicIndexPayloadProvider::new(
                    Arc::new(StoreThreadEpisodicIndexPayloadProvider::new(
                        maintenance_crud_store.clone(),
                        thread_episodic_storage_uri_from_path(
                            thread_episodic_storage_root.as_path(),
                        ),
                    )),
                    thread_episodic_embedding_provider_resolver.clone(),
                    maintenance_crud_store,
                )),
            )
            .with_projection_runtime(
                thread_episodic_storage_root.clone(),
                thread_episodic_embedding_provider_resolver.clone(),
                thread_episodic_workspace_refill_supervisor.clone(),
            ),
        );
        let thread_episodic_recall_embedding_provider_resolver: Arc<
            dyn ThreadEpisodicIndexEmbeddingProviderResolver,
        > = thread_episodic_embedding_provider_resolver.clone();
        let thread_episodic_recall_service = Arc::new(
            ThreadEpisodicRecallService::with_embedding_provider_resolver(
                crud_store.clone(),
                thread_episodic_backend,
                Some(thread_episodic_recall_embedding_provider_resolver),
            ),
        );
        let (thread_episodic_vector_refill_status_tx, _) = broadcast::channel(64);
        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            push_scopes: Arc::new(scoped_push::PushScopes::default()),
            authorization_invalidation_hub,
            execution_leases,
            observation_governor,
            turn_execution_owner_id,
            artifact_stream_invalidation: Arc::new(StdRwLock::new(None)),
            view_grant_service: Arc::new(StdRwLock::new(None)),
            auth_service: None,
            epic5_rate_limits: Arc::new(crate::epic5_observability::Epic5RateLimits::default()),
            cli_runtime_manager: None,
            remote_access_supervisor: None,
            thread_episodic_vector_refill_status_tx,
            thread_episodic_workspace_refill_supervisor,
            workspace_manager,
            crud_store: crud_store.clone(),
            gateway_secrets,
            gateway_listen_addr: Arc::new(std::sync::RwLock::new(None)),
            invitation_gateway_base_url: Arc::new(
                pioneer_protocol::GatewayBaseUrl::parse_presentation("http://127.0.0.1:17878")
                    .expect("static Gateway base URL is valid"),
            ),
            task_child_point_cursor: Arc::new(Mutex::new(Default::default())),
            task_child_scope_cursor: Arc::new(Mutex::new(Default::default())),
            completed_history_checks: compaction_background::shared_history_jobs(&crud_store),
            native_turn_event_delivery_workers: agent_runtime::shared_delivery_workers(&crud_store),
            compaction_lifecycle_not_before: Arc::new(StdRwLock::new(None)),
            #[cfg(test)]
            completed_history_preparation_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            turn_execution_heartbeat_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            recovery_due_wait_barrier: Arc::new(Default::default()),
            #[cfg(test)]
            recovery_admission_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_history_preparation_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_terminal_preparation_finished: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            task_cli_admission_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_preparation_attempts: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            task_cli_history_revalidation_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_cli_readiness_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            task_output_capture_failures: Arc::new(std::sync::Mutex::new(HashMap::new())),
            context_compaction_config: GatewayContextCompactionConfig::default(),
            compaction_clock: Arc::new(crate::compaction::SystemCompactionClock::default()),
            compaction_coordinator: crate::compaction::ContextCompactionCoordinator::for_store(
                &crud_store,
            ),
            cli_history_shutdown: tokio_util::sync::CancellationToken::new(),
            #[cfg(test)]
            cli_transfer_capture_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cli_transfer_test_deadline: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cli_history_monitor_drops: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cli_history_monitor_cleanup_drops: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            predispatch_cli_turn_read_failures: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(test)]
            claude_boundary_write_failures: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(test)]
            native_cancellation_finish_barrier: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            native_cancellation_materialization_failure: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            native_cancellation_materialization_barrier: Arc::new(Mutex::new(None)),
            workspace_compaction_settings: Arc::new(StdRwLock::new(
                std::collections::BTreeMap::new(),
            )),
            compaction_settings: Arc::new(StdRwLock::new(
                pioneer_compaction::CompactionSettings::default(),
            )),
            summary_config: Arc::new(summary::SummaryConfig {
                summary_model: Some("test-model".to_owned()),
                summary_model_provider: Some("echo".to_owned()),
                title_model: Some("test-model".to_owned()),
                title_model_provider: Some("echo".to_owned()),
            }),
            agent_listener_tasks: Arc::new(Mutex::new(HashMap::new())),
            agent_listener_generation: Arc::new(AtomicU64::new(1)),
            agent_message_buffers: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            markdown_delta_parse_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            progress_item_type_lookup_count: Arc::new(AtomicU64::new(0)),
            progress_items: Arc::new(Mutex::new(ProgressItemRegistry::default())),
            agent_action_bindings: Arc::new(Mutex::new(HashMap::new())),
            parent_timeline_targets: Arc::new(Mutex::new(HashMap::new())),
            user_turn_cancel_intents: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_pending_turn_events: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_pending_turn_server_requests: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_machine_requests: Arc::new(Mutex::new(
                CLIRuntimeMachineRequestRegistry::default(),
            )),
            cli_runtime_proxy_cache: Arc::new(Mutex::new(HashMap::new())),
            provider_readiness: Arc::new(provider_readiness::ProviderReadinessSupervisor::default()),
            cli_runtime_pending_turn_activity_sequence: Arc::new(AtomicU64::new(0)),
            cli_runtime_event_hubs: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_turn_binding_cache: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_session_turn_mutexes: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_session_turn_leases: Arc::new(Mutex::new(HashMap::new())),
            cli_runtime_command_heartbeats: CliRuntimeCommandHeartbeatTracker::new(
                MessageProcessorResilienceConfig::default()
                    .cli_runtime_command_heartbeat
                    .interval_secs,
            ),
            artifact_tool_states: Arc::new(Mutex::new(HashMap::new())),
            artifact_output_dirs: Arc::new(Mutex::new(HashMap::new())),
            turn_final_assistant_texts: Arc::new(Mutex::new(HashMap::new())),
            native_permission_pending_requests: Arc::new(Mutex::new(HashMap::new())),
            native_permission_approval_worker: Arc::new(Mutex::new(None)),
            artifact_finalization_retry_turns: Arc::new(Mutex::new(HashSet::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            thread_title_publication_mutexes: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            thread_title_publication_barrier: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            thread_title_publication_attempted: Arc::new(tokio::sync::Notify::new()),
            timeout_supervisor,
            recovery_coordinator,
            native_terminal_effect_kick_running: Arc::new(AtomicBool::new(false)),
            native_terminal_effect_kick_pending: Arc::new(AtomicBool::new(false)),
            native_lifecycle_readiness: Arc::new(
                native_health::NativeLifecycleReadinessState::default(),
            ),
            resilience_worker: Arc::new(Mutex::new(None)),
            hook_recovery_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config: normalized_tool_loop_config,
            memory_loop_config,
            thread_episodic_runtime_config: Arc::new(StdRwLock::new(
                ThreadEpisodicRuntimeConfig::default(),
            )),
            administration_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            cli_runtime_skill_destination_locks: new_cli_runtime_skill_destination_locks(),
            #[cfg(test)]
            cli_runtime_skill_preflight_test_events: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            cli_mcp_readiness_overrides: [None, None, None],
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            skill_upload_owners: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
            memory_runtime,
            memory_bridge_providers: Arc::new(RwLock::new(None)),
            hook_runtime: Arc::new(RwLock::new(None)),
            hook_recovery_config: Arc::new(RwLock::new(GatewayHookRecoveryConfig::default())),
            keepawake: Arc::new(GatewayKeepAwake::default()),
            artifact_runtime_home,
            thread_episodic_storage_root,
            artifact_service,
            artifact_uploads,
            thread_episodic_ingestor: Arc::new(RwLock::new(Arc::new(
                StoreThreadEpisodicIngestor::new(crud_store),
            ))),
            thread_episodic_index_executor,
            thread_episodic_embedding_provider_resolver,
            thread_episodic_recall_service,
            voice_input_supervisor: None,
            self_improvement_supervisor: None,
            gateway_settings_update_lock: Arc::new(Mutex::new(())),
            voice_finalizations: Default::default(),
            #[cfg(test)]
            voice_test_transcriber: None,
            voice_sessions: GatewayVoiceSessionStore::default(),
            voice_session_buffers: GatewayVoiceSessionBufferStore::default(),
        }
    }
}
