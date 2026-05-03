mod agent_runtime;
mod binary;
mod dispatch;
mod markdown;
mod mcp;
mod notifications;
mod provider_handlers;
mod skills;
mod summary;
mod task_agent_executor;
mod task_delivery;
mod task_handlers;
mod tasks;
#[cfg(test)]
mod tests;
mod thread_handlers;
mod turn_handlers;
mod workspace_handlers;

pub use summary::SummaryConfig;

use crate::tokenizer::count_tokens;
use pioneer_agent::{AgentManager, ToolLoopConfig};
use pioneer_crud::{ConversationEntry, CrudStore, TimeoutCandidate};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ContextCompressedNotification,
    ContextCompressingNotification, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, ItemDeltaStream,
    ItemTimeoutDetectedNotification, JSONRPC_VERSION, JsonRpcErrorResponse, JsonRpcNotification,
    JsonRpcRequest, JsonRpcResponse, MARKDOWN_AST_VERSION, METHOD_NOT_FOUND_CODE,
    McpAuditEventSummary, McpChangedAction, McpChangedItem, McpChangedNotification,
    McpDiagnosticLevel, McpInstallParams, McpInstallResponse, McpInstallResult,
    McpInstallResultStatus, McpInstallStatus, McpLifecycleAuditSummary, McpListItem, McpListParams,
    McpListResponse, McpPolicySetParams, McpPolicySetResponse, McpPolicyState,
    McpPromptCatalogItem, McpResourceCatalogItem, McpResourceTemplateCatalogItem, McpRuntimeState,
    McpRuntimeStatus, McpServerCatalogDetails, McpServerDetailsParams, McpServerDetailsResponse,
    McpServerHealthDetails, McpServerPolicy, McpServerRestartParams, McpServerRestartResponse,
    McpServerStatus, McpSourceKind, McpToolAnnotationSummary, McpToolCatalogItem,
    McpTransportSummary, McpTurnBindingSummary, McpUninstallParams, McpUninstallResponse,
    McpValidationDiagnostic, PARSE_ERROR_CODE, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListModelsParams, ProviderListModelsResponse,
    ProviderListResponse, ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits,
    ProviderModelPricing, ProviderSetApiKeyParams, ProviderSetApiKeyResponse, ProviderSummary,
    RequestId, SkillsUploadAbortParams, SkillsUploadFinishParams, SkillsUploadStartParams,
    SystemEventLevel, Task, TaskAgendaParams, TaskCancelParams, TaskCancelResponse,
    TaskCreateParams, TaskCreateResponse, TaskDeliveriesParams, TaskDelivery, TaskDeliveryAttempt,
    TaskDeliveryMode, TaskDetachParams, TaskDetachResponse, TaskEventsParams, TaskGetParams,
    TaskListParams, TaskPauseParams, TaskRescheduleParams, TaskResumeParams,
    TaskTreeParams as TaskTreeTaskParams, TaskTrigger, TaskTurnItem, TaskWaitParams,
    TaskWaitResponse, ThreadFolderCreateParams, ThreadFolderCreateResponse,
    ThreadFolderDeleteParams, ThreadFolderDeleteResponse, ThreadFolderMoveParams,
    ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse, ThreadHistoryParams,
    ThreadHistoryResponse, ThreadMoveParams, ThreadMoveResponse, ThreadStartParams,
    ThreadTreeChangedNotification, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeParams,
    ThreadUpdatedNotification, TimelineItem, TimelineLane, TimelineOrigin, TimelineOriginKind,
    TimelinePayload, ToolCallStatus, TurnCancelParams, TurnCancelResponse,
    TurnCompletedNotification, TurnFailedNotification, TurnGetParams, TurnGetResponse, TurnItem,
    TurnItemEvent, TurnItemEventPayload, TurnItemType, TurnItemsParams, TurnStartParams,
    TurnStatus, TurnTimelineChangedNotification, TurnTimelineChangedReason, TurnTimelineParams,
    TurnTimelineResponse, WorkspaceCreateParams, WorkspaceCreateResponse, WorkspaceDefaultParams,
    WorkspaceDefaultResponse, WorkspaceListParams, WorkspaceListResponse,
    constants::{events, methods},
};
use pioneer_provider::{ChatMessage, ProviderRegistry};
use pioneer_tasks::TaskRuntime;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::mcp_service::McpService;
use crate::resilience::{
    RecoveryCoordinator, RecoveryPolicyRegistry, RecoveryTerminalOutcome, TimeoutPolicyRegistry,
    TimeoutSupervisor,
};
use crate::session::{ConnectionId, SessionManager};
use crate::settings::{GatewaySettings, save_gateway_settings};
use crate::thread::ThreadManager;
use crate::workspace::{WorkspaceError, WorkspaceManager};

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
    gateway_settings: Arc<RwLock<GatewaySettings>>,
    settings_path: PathBuf,
    summary_config: Arc<summary::SummaryConfig>,
    context_budget: ContextBudget,
    agent_listener_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    agent_message_buffers: Arc<Mutex<HashMap<String, String>>>,
    turn_llm_context_sequences: Arc<Mutex<HashMap<String, i64>>>,
    title_job_runtime: Arc<Mutex<HashMap<String, ThreadTitleJobState>>>,
    timeout_supervisor: Arc<TimeoutSupervisor>,
    recovery_coordinator: Arc<RecoveryCoordinator>,
    resilience_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    task_event_listener_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    skills_watcher_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    tool_loop_config: ToolLoopConfig,
    skills_snapshot_version: Arc<AtomicU64>,
    mcp_snapshot_version: Arc<AtomicU64>,
    mcp_service: Arc<McpService>,
    skills_write_lock: Arc<tokio::sync::Mutex<()>>,
    skill_upload_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    task_agent_executor: Arc<task_agent_executor::TaskAgentExecutor>,
    pub(crate) task_runtime: Arc<TaskRuntime>,
}

impl MessageProcessor {
    pub fn new(
        thread_manager: Arc<ThreadManager>,
        provider_registry: Arc<ProviderRegistry>,
        session_manager: Arc<SessionManager>,
        workspace_manager: Arc<WorkspaceManager>,
        crud_store: Arc<CrudStore>,
        gateway_settings: Arc<RwLock<GatewaySettings>>,
        settings_path: PathBuf,
        summary_config: summary::SummaryConfig,
        context_budget: ContextBudget,
        tool_loop_config: ToolLoopConfig,
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
            gateway_settings.clone(),
            mcp_snapshot_version.clone(),
        ));
        let agent_manager = Arc::new(AgentManager::new_with_mcp(
            provider_registry.clone(),
            tool_loop_config.clone(),
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

        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_settings,
            settings_path,
            summary_config: Arc::new(summary_config),
            context_budget,
            agent_listener_tasks: Arc::new(Mutex::new(HashMap::new())),
            agent_message_buffers: Arc::new(Mutex::new(HashMap::new())),
            turn_llm_context_sequences: Arc::new(Mutex::new(HashMap::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            timeout_supervisor,
            recovery_coordinator,
            resilience_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config,
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
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

    pub async fn start_resilience_workers(self: &Arc<Self>) {
        self.bind_task_bridge().await;
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
        if let Err(error) = self.task_runtime.start().await {
            warn!(error = %format!("{error:#}"), "failed to start task runtime");
        }
        self.start_task_event_listener().await;
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
            while let Some(event) = subscription.recv().await {
                if let Err(error) = this.emit_task_event(event).await {
                    warn!(
                        error = %format!("{error:#}"),
                        "failed to fan out committed task event"
                    );
                }
            }
        }));
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
            pioneer_protocol::UserInput::Skill { name, .. } => {
                text_parts.push(format!("skill: {name}"));
            }
            pioneer_protocol::UserInput::Mention { name, .. } => {
                text_parts.push(format!("mention: {name}"));
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

#[cfg(test)]
impl MessageProcessor {
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
        let gateway_settings = Arc::new(RwLock::new(
            crate::settings::GatewaySettings::default_for_tests(),
        ));
        let mcp_snapshot_version = Arc::new(AtomicU64::new(0));
        let mcp_service = Arc::new(McpService::new(
            crud_store.clone(),
            session_manager.clone(),
            gateway_settings.clone(),
            mcp_snapshot_version.clone(),
        ));
        let task_agent_executor = Arc::new(task_agent_executor::TaskAgentExecutor::new());
        let task_runtime = Arc::new(TaskRuntime::new(crud_store.clone()));
        Self {
            thread_manager,
            agent_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store,
            gateway_settings,
            settings_path: PathBuf::from("/tmp/pioneer-test-settings.toml"),
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
            turn_llm_context_sequences: Arc::new(Mutex::new(HashMap::new())),
            title_job_runtime: Arc::new(Mutex::new(HashMap::new())),
            timeout_supervisor,
            recovery_coordinator,
            resilience_worker: Arc::new(Mutex::new(None)),
            task_event_listener_worker: Arc::new(Mutex::new(None)),
            skills_watcher_worker: Arc::new(Mutex::new(None)),
            tool_loop_config: ToolLoopConfig {
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
                    system_roots: vec!["{homeDirectory}/skills/system".to_owned()],
                    user_roots: vec!["{homeDirectory}/skills/user".to_owned()],
                    workspace_roots: vec![
                        "{homeDirectory}/skills/workspace/{workspaceId}".to_owned(),
                    ],
                    registry_roots: vec!["{homeDirectory}/skills/registry".to_owned()],
                    validation: pioneer_agent::SkillsValidationLoopConfig {
                        strict_agentskills: true,
                        accept_openclaw_profile: true,
                    },
                    security: pioneer_agent::SkillsSecurityLoopConfig {
                        allow_untrusted_install: false,
                        min_trust_for_shell_tools: pioneer_skills::SkillTrustLevel::Verified,
                        min_trust_for_http_tools: pioneer_skills::SkillTrustLevel::Community,
                        min_trust_for_function_proxy_tools:
                            pioneer_skills::SkillTrustLevel::Community,
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
                budget: pioneer_tools::ToolLoopBudgetConfig::default(),
                retry: pioneer_tools::ToolRetryBudgetConfig::default(),
            }
            .normalized(),
            skills_snapshot_version: Arc::new(AtomicU64::new(now_snapshot)),
            mcp_snapshot_version,
            mcp_service,
            skills_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            skill_upload_locks: Arc::new(Mutex::new(HashMap::new())),
            task_agent_executor,
            task_runtime,
        }
    }
}
