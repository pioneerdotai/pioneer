mod agent_runtime;
mod artifact_finalization_diagnostics;
mod artifact_registration;
mod artifact_tools;
mod artifacts;
mod binary;
mod dispatch;
mod hooks;
mod markdown;
mod mcp;
mod memory_handlers;
mod notifications;
mod provider_handlers;
mod skills;
mod summary;
mod task_agent_executor;
mod task_artifacts;
mod task_delivery;
mod task_handlers;
mod tasks;
#[cfg(test)]
mod tests;
mod thread_agents_doc_handlers;
mod thread_handlers;
mod turn_handlers;
mod workspace_handlers;

pub use summary::SummaryConfig;

use crate::hook_runtime::GatewayHookRuntimeBuilder;
use crate::keep_awake::GatewayKeepAwake;
use crate::prompt_hooks::agents_doc_prompt_hook_package;
use crate::tokenizer::count_tokens;
use anyhow::Context as AnyhowContext;
use pioneer_agent::MemoryLoopConfig;
use pioneer_agent::{AgentManager, ResolvedArtifactInput, ToolLoopConfig};
use pioneer_artifacts::{
    ArtifactBindingTarget, ArtifactGcPolicy, ArtifactQuotaPolicy, ArtifactRegistrationCandidate,
    ArtifactRegistrationContext, ArtifactRegistrationSource, ArtifactService, ArtifactToolState,
    BindArtifactRequest, LocalArtifactBlobStore,
};
use pioneer_config::{GatewayArtifactsConfig, GatewayHookRecoveryConfig};
use pioneer_crud::{ConversationEntry, CrudStore, TimeoutCandidate};
use pioneer_hooks::{HookRecoveryOptions, HookRuntime};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ArtifactBindingDirection, ArtifactBindingKind,
    ArtifactCreatedByKind, ArtifactCreatedNotification, ArtifactKind, ArtifactRole,
    ContextCompressedNotification, ContextCompressingNotification, INVALID_PARAMS_CODE,
    INVALID_REQUEST_CODE, ItemDeltaStream, ItemTimeoutDetectedNotification, JSONRPC_VERSION,
    JsonRpcErrorResponse, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    MARKDOWN_AST_VERSION, METHOD_NOT_FOUND_CODE, McpAuditEventSummary, McpChangedAction,
    McpChangedItem, McpChangedNotification, McpDiagnosticLevel, McpInstallParams,
    McpInstallResponse, McpInstallResult, McpInstallResultStatus, McpInstallStatus,
    McpLifecycleAuditSummary, McpListItem, McpListParams, McpListResponse, McpPolicySetParams,
    McpPolicySetResponse, McpPolicyState, McpPromptCatalogItem, McpResourceCatalogItem,
    McpResourceTemplateCatalogItem, McpRuntimeState, McpRuntimeStatus, McpServerCatalogDetails,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerHealthDetails, McpServerPolicy,
    McpServerRestartParams, McpServerRestartResponse, McpServerStatus, McpSourceKind,
    McpToolAnnotationSummary, McpToolCatalogItem, McpTransportSummary, McpTurnBindingSummary,
    McpUninstallParams, McpUninstallResponse, McpValidationDiagnostic, PARSE_ERROR_CODE,
    ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse, ProviderListModelsParams,
    ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits, ProviderModelPricing,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, ProviderSummary, RequestId,
    SkillsUploadAbortParams, SkillsUploadFinishParams, SkillsUploadStartParams, SystemEventLevel,
    TaskAgendaParams, TaskCancelParams, TaskCreateParams, TaskDeliveriesParams, TaskDelivery,
    TaskDeliveryAttempt, TaskDeliveryMode, TaskDetachParams, TaskEventsParams, TaskGetParams,
    TaskListParams, TaskPauseParams, TaskRescheduleParams, TaskResumeParams,
    TaskTreeParams as TaskTreeTaskParams, TaskWaitParams, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocArchiveResponse, ThreadAgentsDocChangedNotification, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocPayload, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocResolvedPayload,
    ThreadAgentsDocSaveParams, ThreadAgentsDocSaveReason, ThreadAgentsDocSaveResponse,
    ThreadAgentsDocStatus, ThreadAgentsDocSummary, ThreadArtifactsChangedNotification,
    ThreadFolderCreateParams, ThreadFolderCreateResponse, ThreadFolderDeleteParams,
    ThreadFolderDeleteResponse, ThreadFolderMoveParams, ThreadFolderMoveResponse, ThreadGetParams,
    ThreadGetResponse, ThreadHistoryParams, ThreadHistoryResponse, ThreadMoveParams,
    ThreadMoveResponse, ThreadStartParams, ThreadTreeChangedNotification, ThreadTreeParams,
    ThreadTreeResponse, ThreadUnsubscribeParams, ThreadUpdateParams, ThreadUpdateResponse,
    ThreadUpdatedNotification, TimelineItem, TimelineLane, TimelineOrigin, TimelineOriginKind,
    TimelinePayload, ToolCallStatus, ToolStoragePayload, TurnCancelParams, TurnCancelResponse,
    TurnCompletedNotification, TurnFailedNotification, TurnGetParams, TurnGetResponse, TurnItem,
    TurnItemEvent, TurnItemEventPayload, TurnItemType, TurnItemsParams, TurnStartParams,
    TurnStatus, TurnTimelineChangedNotification, TurnTimelineChangedReason, TurnTimelineParams,
    TurnTimelineResponse, Workspace, WorkspaceChangeKind, WorkspaceChangedNotification,
    WorkspaceCreateParams, WorkspaceCreateResponse, WorkspaceDefaultParams,
    WorkspaceDefaultResponse, WorkspaceListParams, WorkspaceListResponse, WorkspaceSelectParams,
    WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
    constants::{events, methods},
};
use pioneer_provider::{ChatMessage, ProviderRegistry};
use pioneer_tasks::TaskRuntime;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::mcp_service::McpService;
use crate::memory_runtime::GatewayMemoryRuntime;
use crate::resilience::{
    RecoveryCoordinator, RecoveryPolicyRegistry, RecoveryTerminalOutcome, TimeoutPolicyRegistry,
    TimeoutSupervisor,
};
use crate::secrets::GatewaySecrets;
use crate::session::{ConnectionId, SessionManager};
use crate::thread::ThreadManager;
use crate::workspace::{WorkspaceError, WorkspaceManager};

pub(crate) type MessageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Gateway handlers compose large protocol and CRUD futures; this is the explicit heap boundary.
pub(crate) fn message_future<'a, F, T>(future: F) -> MessageFuture<'a, T>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(future)
}

#[derive(Clone, Copy)]
pub struct ContextBudget {
    pub max_context_tokens: usize,
    pub response_reserve_tokens: usize,
}

impl ContextBudget {
    fn history_budget(&self) -> usize {
        self.max_context_tokens
            .saturating_sub(self.response_reserve_tokens)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThreadTitleJobState {
    Pending,
    Running,
    Succeeded,
    FailedRetriable,
    FailedNonRetriable,
}

impl ThreadTitleJobState {
    const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::FailedRetriable)
    }
}

#[derive(Clone)]
pub struct MessageProcessor {
    thread_manager: Arc<ThreadManager>,
    agent_manager: Arc<AgentManager>,
    provider_registry: Arc<ProviderRegistry>,
    session_manager: Arc<SessionManager>,
    workspace_manager: Arc<WorkspaceManager>,
    pub(crate) crud_store: Arc<CrudStore>,
    gateway_secrets: Arc<GatewaySecrets>,
    summary_config: Arc<summary::SummaryConfig>,
    context_budget: ContextBudget,
    agent_listener_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    agent_message_buffers: Arc<Mutex<HashMap<String, String>>>,
    parent_timeline_targets: Arc<Mutex<HashMap<String, agent_runtime::ParentTimelineTarget>>>,
    turn_llm_context_sequences: Arc<Mutex<HashMap<String, i64>>>,
    artifact_tool_states: Arc<Mutex<HashMap<String, Arc<ArtifactToolState>>>>,
    artifact_output_dirs: Arc<Mutex<HashMap<String, String>>>,
    turn_final_assistant_texts: Arc<Mutex<HashMap<String, String>>>,
    artifact_finalization_retry_turns: Arc<Mutex<HashSet<String>>>,
    title_job_runtime: Arc<Mutex<HashMap<String, ThreadTitleJobState>>>,
    timeout_supervisor: Arc<TimeoutSupervisor>,
    recovery_coordinator: Arc<RecoveryCoordinator>,
    resilience_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    hook_recovery_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    task_event_listener_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    skills_watcher_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    tool_loop_config: ToolLoopConfig,
    memory_loop_config: Arc<StdRwLock<MemoryLoopConfig>>,
    skills_snapshot_version: Arc<AtomicU64>,
    mcp_snapshot_version: Arc<AtomicU64>,
    mcp_service: Arc<McpService>,
    skills_write_lock: Arc<tokio::sync::Mutex<()>>,
    skill_upload_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    task_agent_executor: Arc<task_agent_executor::TaskAgentExecutor>,
    pub(crate) task_runtime: Arc<TaskRuntime>,
    memory_runtime: Arc<GatewayMemoryRuntime>,
    memory_bridge_providers: Arc<RwLock<Option<MemoryBridgeProviders>>>,
    hook_runtime: Arc<RwLock<Option<Arc<HookRuntime>>>>,
    hook_recovery_config: Arc<RwLock<GatewayHookRecoveryConfig>>,
    keepawake: Arc<GatewayKeepAwake>,
    artifact_runtime_home: PathBuf,
    pub(crate) artifact_service: Arc<ArtifactService>,
    artifact_uploads: Arc<artifacts::upload::ArtifactUploadSessionManager>,
    artifact_downloads: Arc<artifacts::download::ArtifactDownloadSessionManager>,
}

#[derive(Clone)]
struct MemoryBridgeProviders {
    memory_provider: Arc<crate::memory_tools::GatewayMemoryProvider>,
    memory_policy_provider: Arc<crate::memory_policy::GatewayMemoryTurnPolicyProvider>,
}

impl MessageProcessor {
    pub fn new_with_memory_runtime(
        thread_manager: Arc<ThreadManager>,
        provider_registry: Arc<ProviderRegistry>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
        gateway_secrets: Arc<GatewaySecrets>,
        summary_config: summary::SummaryConfig,
        context_budget: ContextBudget,
        tool_loop_config: ToolLoopConfig,
        memory_runtime: Arc<GatewayMemoryRuntime>,
        runtime_home: PathBuf,
        artifacts_config: GatewayArtifactsConfig,
    ) -> Self {
        let now_snapshot = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        };
        let mcp_snapshot_version = Arc::new(AtomicU64::new(0));
        let task_agent_executor = Arc::new(task_agent_executor::TaskAgentExecutor::new());
        let task_runtime = Arc::new(TaskRuntime::new(crud_store.clone()));
        let mcp_service = Arc::new(McpService::new(
            crud_store.clone(),
            session_manager.clone(),
            gateway_secrets.clone(),
            mcp_snapshot_version.clone(),
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
            TimeoutPolicyRegistry::default(),
        ));
        let recovery_coordinator = Arc::new(RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager.clone(),
            provider_registry.clone(),
            RecoveryPolicyRegistry::default(),
        ));
        let artifact_service = Arc::new(ArtifactService::new_with_policies(
            crud_store.clone(),
            Arc::new(LocalArtifactBlobStore::new(runtime_home.clone())),
            artifact_quota_policy_from_config(&artifacts_config),
            ArtifactGcPolicy {
                grace_secs: artifacts_config.gc_grace_secs,
                output_dir_ttl_secs: artifacts_config.output_dir_ttl_secs,
            },
        ));
        let artifact_uploads = Arc::new(artifacts::upload::ArtifactUploadSessionManager::new(
            runtime_home.join("artifacts").join("upload_sessions"),
        ));
        let artifact_downloads =
            Arc::new(artifacts::download::ArtifactDownloadSessionManager::new());

        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store: crud_store.clone(),
            gateway_secrets,
            summary_config: Arc::new(summary_config),
            context_budget,
            agent_listener_tasks: Arc::new(Mutex::new(HashMap::new())),
            agent_message_buffers: Arc::new(Mutex::new(HashMap::new())),
            parent_timeline_targets: Arc::new(Mutex::new(HashMap::new())),
            turn_llm_context_sequences: Arc::new(Mutex::new(HashMap::new())),
            artifact_tool_states: Arc::new(Mutex::new(HashMap::new())),
            artifact_output_dirs: Arc::new(Mutex::new(HashMap::new())),
            turn_final_assistant_texts: Arc::new(Mutex::new(HashMap::new())),
            artifact_finalization_retry_turns: Arc::new(Mutex::new(HashSet::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            timeout_supervisor,
            recovery_coordinator,
            resilience_worker: Arc::new(Mutex::new(None)),
            hook_recovery_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config: normalized_tool_loop_config,
            memory_loop_config,
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
            memory_runtime,
            memory_bridge_providers: Arc::new(RwLock::new(None)),
            hook_runtime: Arc::new(RwLock::new(None)),
            hook_recovery_config: Arc::new(RwLock::new(GatewayHookRecoveryConfig::default())),
            keepawake: Arc::new(GatewayKeepAwake::default()),
            artifact_runtime_home: runtime_home,
            artifact_service,
            artifact_uploads,
            artifact_downloads,
        }
    }

    pub async fn set_hook_recovery_config(&self, config: GatewayHookRecoveryConfig) {
        *self.hook_recovery_config.write().await = config;
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
                    self.agent_manager.set_hook_runtime(Some(runtime)).await;
                }
                Err(error) => {
                    warn!(error = %error, "failed to install AGENTS.md prompt hook package");
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
                self.agent_manager.set_hook_runtime(Some(runtime)).await;
            }
            Err(error) => {
                warn!(error = %error, "failed to build hook runtime");
            }
        }
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
        self.bind_task_bridge().await;
        self.bind_artifact_tool_bridge().await;
    }

    pub async fn bind_memory_bridge(self: &Arc<Self>) {
        let memory_provider = Arc::new(crate::memory_tools::GatewayMemoryProvider::new(
            Arc::downgrade(self),
        ));
        let memory_policy_provider =
            Arc::new(crate::memory_policy::GatewayMemoryTurnPolicyProvider::new(
                self.provider_registry.clone(),
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
        self.agent_manager
            .set_memory_turn_policy_provider(Some(memory_policy_provider.clone()))
            .await;

        let bridge = MemoryBridgeProviders {
            memory_provider,
            memory_policy_provider,
        };
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
                Some(bridge.memory_policy_provider),
                None,
                self.agent_manager.memory_tool_bundle_artifact_store(),
                self.memory_loop_config(),
            ))
            .and_then(|builder| {
                builder.install(agents_doc_prompt_hook_package(self.crud_store.clone()))
            }) {
            Ok(builder) => {
                let runtime = builder.build();
                *self.hook_runtime.write().await = Some(runtime.clone());
                self.agent_manager.set_hook_runtime(Some(runtime)).await;
            }
            Err(error) => {
                warn!(error = %error, "failed to install memory hook package");
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

    pub(crate) fn apply_keepawake_setting(&self, enabled: bool) -> anyhow::Result<()> {
        self.keepawake.set_enabled(enabled)
    }

    pub async fn start_resilience_workers(self: &Arc<Self>) {
        self.bind_agent_tool_bridges().await;
        match self
            .crud_store
            .repair_deterministic_read_model_violations()
            .await
        {
            Ok(summary) => {
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
                warn!(
                    error = %format!("{error:#}"),
                    "read-model invariant verification failed at startup"
                );
            }
        }
        match self
            .timeout_supervisor
            .backfill_missing_deadlines(1024)
            .await
        {
            Ok(backfilled) => {
                if backfilled > 0 {
                    warn!(
                        backfilled,
                        "backfilled missing running item deadlines during startup"
                    );
                }
            }
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to backfill missing running item deadlines during startup"
                );
            }
        }
        if let Err(error) = self.task_runtime.start().await {
            warn!(error = %format!("{error:#}"), "failed to start task runtime");
        }
        self.start_task_event_listener().await;
        self.start_hook_recovery_worker().await;
        let mut guard = self.resilience_worker.lock().await;
        if guard.is_some() {
            return;
        }

        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut next_skill_upload_cleanup = 0;
            loop {
                let now = now_timestamp_secs();
                if now >= next_skill_upload_cleanup {
                    this.cleanup_stale_skill_uploads(now).await;
                    next_skill_upload_cleanup = now.saturating_add(60);
                }

                match this.timeout_supervisor.poll_timeouts(now, 64).await {
                    Ok(candidates) => {
                        for candidate in candidates {
                            this.handle_timeout_candidate(candidate, now).await;
                        }
                    }
                    Err(error) => {
                        warn!(error = %format!("{error:#}"), "timeout supervisor poll failed");
                    }
                }

                match this.recovery_coordinator.run_ready_jobs(now, 64).await {
                    Ok(events) => {
                        for event in events {
                            this.handle_recovery_event(event, now).await;
                        }
                    }
                    Err(error) => {
                        warn!(error = %format!("{error:#}"), "recovery coordinator poll failed");
                    }
                }

                if let Err(error) = this.process_due_task_deliveries(now, 64).await {
                    warn!(error = %format!("{error:#}"), "task delivery worker poll failed");
                }

                sleep(Duration::from_secs(2)).await;
            }
        });

        *guard = Some(handle);
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

        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut first_pass = true;
            loop {
                let config = this.hook_recovery_config.read().await.clone();
                if config.enabled && (!first_pass || config.startup_scan) {
                    this.run_hook_recovery_pass(config.clone()).await;
                }
                first_pass = false;
                sleep(Duration::from_millis(config.poll_interval_ms.max(250))).await;
            }
        });

        *guard = Some(handle);
    }

    async fn run_hook_recovery_pass(&self, config: GatewayHookRecoveryConfig) {
        let Some(runtime) = self.hook_runtime.read().await.clone() else {
            return;
        };
        if !runtime.has_run_store() {
            return;
        }
        let options = HookRecoveryOptions {
            now_unix_ms: now_timestamp_millis(),
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
            }
            Err(error) => {
                warn!(error = %format!("{error:#}"), "hook recovery pass failed");
            }
        }
    }

    async fn start_task_event_listener(self: &Arc<Self>) {
        let mut guard = self.task_event_listener_worker.lock().await;
        if guard.is_some() {
            return;
        }
        let this = self.clone();
        let mut subscription = self
            .task_runtime
            .event_bus()
            .subscribe(pioneer_tasks::TaskEventFilter::default());
        *guard = Some(tokio::spawn(async move {
            let mut cursors_by_task: HashMap<String, i64> = HashMap::new();
            loop {
                match subscription.recv().await {
                    pioneer_tasks::TaskEventWakeDelivery::Wake(wake) => {
                        if let Err(error) = this
                            .emit_committed_task_events_after_cursor(
                                wake.task_id.as_str(),
                                &mut cursors_by_task,
                            )
                            .await
                        {
                            warn!(
                                task_id = %wake.task_id,
                                event_id = %wake.event_id,
                                sequence = wake.sequence,
                                error = %format!("{error:#}"),
                                "failed to fan out committed task events after wake"
                            );
                        }
                    }
                    pioneer_tasks::TaskEventWakeDelivery::Lagged(count) => {
                        let task_ids =
                            match this.task_runtime.service().list_task_event_task_ids().await {
                                Ok(task_ids) => task_ids,
                                Err(error) => {
                                    warn!(
                                        missed_wakes = count,
                                        error = %format!("{error:#}"),
                                        "failed to enumerate task event log after wake bus lag"
                                    );
                                    cursors_by_task.keys().cloned().collect::<Vec<_>>()
                                }
                            };
                        warn!(
                            missed_wakes = count,
                            task_count = task_ids.len(),
                            "task wake bus lagged; rescanning committed task event log"
                        );
                        for task_id in task_ids {
                            if let Err(error) = this
                                .emit_committed_task_events_after_cursor(
                                    task_id.as_str(),
                                    &mut cursors_by_task,
                                )
                                .await
                            {
                                warn!(
                                    task_id = %task_id,
                                    error = %format!("{error:#}"),
                                    "failed to rescan committed task events after lag"
                                );
                            }
                        }
                    }
                    pioneer_tasks::TaskEventWakeDelivery::Closed => break,
                }
            }
        }));
    }

    async fn emit_committed_task_events_after_cursor(
        &self,
        task_id: &str,
        cursors_by_task: &mut HashMap<String, i64>,
    ) -> anyhow::Result<()> {
        let after_sequence = cursors_by_task.get(task_id).copied().unwrap_or(0);
        let events = self
            .task_runtime
            .service()
            .list_task_events_after(task_id, after_sequence)
            .await?;

        for event in events {
            let sequence = event.sequence;
            self.emit_task_event(event).await?;
            cursors_by_task.insert(task_id.to_owned(), sequence);
        }

        Ok(())
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

#[cfg(test)]
fn user_message_payload_from_input(
    input: &[pioneer_protocol::UserInput],
) -> Option<(String, Vec<pioneer_protocol::UserMessageAttachment>)> {
    let mut text_parts = Vec::new();
    let mut attachments = Vec::new();

    for value in input {
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
            pioneer_protocol::UserInput::Artifact { artifact_id, .. } => {
                text_parts.push(format!("artifact: {artifact_id}"));
            }
        }
    }

    let text = text_parts.join("\n");
    if text.is_empty() && attachments.is_empty() {
        None
    } else {
        Some((text, attachments))
    }
}

impl MessageProcessor {
    async fn validate_artifact_user_inputs(
        &self,
        workspace_id: &str,
        input: &[pioneer_protocol::UserInput],
    ) -> anyhow::Result<()> {
        for value in input {
            if let pioneer_protocol::UserInput::Artifact {
                artifact_id,
                version_id,
            } = value
            {
                self.artifact_service
                    .get_artifact(workspace_id, artifact_id, version_id.as_deref())
                    .await
                    .with_context(|| {
                        format!(
                            "artifact `{artifact_id}` is not available in workspace `{workspace_id}`"
                        )
                    })?;
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
            self.send_notification_to_thread_subscribers(
                thread_id,
                events::THREAD_ARTIFACTS_CHANGED,
                &ThreadArtifactsChangedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    artifact_ids: bound_artifact_ids,
                    reason: "user_input_binding".to_owned(),
                    generated_at: now_timestamp_secs(),
                },
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
        context_budget: ContextBudget,
        tool_loop_config: ToolLoopConfig,
    ) -> Self {
        let memory_runtime = Arc::new(GatewayMemoryRuntime::disabled(crud_store.clone()));
        Self::new_with_memory_runtime(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_secrets,
            summary_config,
            context_budget,
            tool_loop_config,
            memory_runtime,
            std::env::temp_dir().join("pioneer-message-tests"),
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
        let timeout_supervisor = Arc::new(TimeoutSupervisor::new(
            crud_store.clone(),
            TimeoutPolicyRegistry::default(),
        ));
        let provider_registry = Arc::new(ProviderRegistry::with_provider(
            "openai",
            Arc::new(pioneer_provider::providers::EchoProvider::new()),
        ));
        let recovery_coordinator = Arc::new(RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager.clone(),
            provider_registry.clone(),
            RecoveryPolicyRegistry::default(),
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
        let mcp_service = Arc::new(McpService::new(
            crud_store.clone(),
            session_manager.clone(),
            gateway_secrets.clone(),
            mcp_snapshot_version.clone(),
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
        let artifact_downloads =
            Arc::new(artifacts::download::ArtifactDownloadSessionManager::new());
        let normalized_tool_loop_config = ToolLoopConfig {
            preflight: pioneer_agent::PreflightLoopConfig::default(),
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
                artifacts_subdir: "tools/computer_use".to_owned(),
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
                    read_skill_max_chars: 24_000,
                    compact_mode_threshold: 6,
                    allow_shell_tools: true,
                    allow_http_tools: true,
                    allow_function_proxy_tools: true,
                },
            },
            memory: pioneer_memory::hooks::MemoryLoopConfig {
                active_recall: pioneer_memory::hooks::MemoryActiveRecallConfig {
                    mode: pioneer_memory::hooks::MemoryActiveRecallMode::DeterministicOnly,
                    ..pioneer_memory::hooks::MemoryActiveRecallConfig::default()
                },
                ..pioneer_memory::hooks::MemoryLoopConfig::default()
            },
            budget: pioneer_tools::ToolLoopBudgetConfig::default(),
            retry: pioneer_tools::ToolRetryBudgetConfig::default(),
        }
        .normalized();
        let memory_loop_config =
            Arc::new(StdRwLock::new(normalized_tool_loop_config.memory.clone()));
        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_secrets,
            summary_config: Arc::new(summary::SummaryConfig {
                summary_model: Some("test-model".to_owned()),
                summary_model_provider: Some("echo".to_owned()),
                title_model: Some("test-model".to_owned()),
                title_model_provider: Some("echo".to_owned()),
            }),
            context_budget: ContextBudget {
                max_context_tokens: 128_000,
                response_reserve_tokens: 16_000,
            },
            agent_listener_tasks: Arc::new(Mutex::new(HashMap::new())),
            agent_message_buffers: Arc::new(Mutex::new(HashMap::new())),
            parent_timeline_targets: Arc::new(Mutex::new(HashMap::new())),
            turn_llm_context_sequences: Arc::new(Mutex::new(HashMap::new())),
            artifact_tool_states: Arc::new(Mutex::new(HashMap::new())),
            artifact_output_dirs: Arc::new(Mutex::new(HashMap::new())),
            turn_final_assistant_texts: Arc::new(Mutex::new(HashMap::new())),
            artifact_finalization_retry_turns: Arc::new(Mutex::new(HashSet::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            timeout_supervisor,
            recovery_coordinator,
            resilience_worker: Arc::new(Mutex::new(None)),
            hook_recovery_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config: normalized_tool_loop_config,
            memory_loop_config,
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
            memory_runtime,
            memory_bridge_providers: Arc::new(RwLock::new(None)),
            hook_runtime: Arc::new(RwLock::new(None)),
            hook_recovery_config: Arc::new(RwLock::new(GatewayHookRecoveryConfig::default())),
            keepawake: Arc::new(GatewayKeepAwake::default()),
            artifact_runtime_home,
            artifact_service,
            artifact_uploads,
            artifact_downloads,
        }
    }
}
