use super::{MessageProcessor, message_future};
use crate::bootstrap::bootstrap;
use crate::memory_runtime::GatewayMemoryRuntime;
use crate::secrets::GatewaySecrets;
use crate::session::SessionManager;
use crate::thread::ThreadManager;
use crate::workspace::WorkspaceManager;
use async_trait::async_trait;
use futures_util::StreamExt;
use migration::{Migrator, MigratorTrait};
use pioneer_agent::{AgentManager, AgentMcpToolProvider, SkillsLoopConfig, ToolLoopConfig};
use pioneer_config::{GatewayHookRecoveryConfig, GatewayMemoryConfig, GatewayWebToolsConfig};
use pioneer_crud::{
    AgentMemoryListFilter, CrudStore, MemoryActorRecord, NewAgentMemoryCandidate,
    ThreadAgentsDocSaveReason, global_agent_memory_scope_key,
};
use pioneer_entity::{thread, thread_sandox_policy, turn, turn_input, turn_status_history};
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookDiagnosticCode,
    HookDiagnosticMessage, HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy,
    HookHandler, HookHandlerRequest, HookHandlerResponse, HookId, HookInputPayload, HookKind,
    HookPhase, HookPromptContent, HookRegistry, HookRuntime, HookRuntimeOptions, HookSectionId,
    HookSubscription, HookSubscriptionId, HookSubscriptionRegistry, PromptSectionContribution,
    TurnPreCompactionRawTurnRetention, TurnPreCompactionSummaryStorage,
    TurnPreCompactionSummaryStrategy, TurnPreCompactionTrigger,
};
use pioneer_keystore::{MemorySecretStore, SecretFilter, SecretId, SecretKind, SecretStore};
use pioneer_memory::hooks::{AgentMemoryProvider, MemoryRecallRequest, MemoryTurnContext};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, INVALID_REQUEST_CODE, ItemCompletedNotification,
    ItemDeltaNotification, ItemDeltaStream, ItemStartedNotification,
    ItemToolRetryScheduledNotification, JsonRpcErrorResponse, JsonRpcNotification, JsonRpcResponse,
    McpChangedAction, McpChangedNotification, McpInstallResponse, McpInstallResultStatus,
    McpInstallStatus, McpListResponse, McpPolicySetResponse, McpRuntimeState,
    McpServerDetailsResponse, McpServerStatus, McpSourceKind, McpTransportSummary,
    McpUninstallResponse, MemoryActor, MemoryActorKind, MemoryCandidateDecision,
    MemoryCandidateStatus, MemoryCandidatesDecideParams, MemoryCandidatesDecideResponse,
    MemoryCandidatesListParams, MemoryCandidatesListResponse, MemoryCategory, MemoryChangeKind,
    MemoryChangedNotification, MemoryForgetParams, MemoryForgetResponse, MemoryForgetTarget,
    MemoryForgottenNotification, MemoryGetParams, MemoryGetResponse, MemoryListParams,
    MemoryListResponse, MemoryRememberParams, MemoryRememberResponse, MemoryScope, MemoryScopeKind,
    MemorySearchParams, MemorySearchResponse, MemorySensitivity, PromptManifest,
    PromptManifestDiagnostic, PromptManifestDiagnosticCode, PromptManifestHookContributionKind,
    PromptManifestHookPhase, PromptManifestHookSource, PromptManifestHookSourceEntry,
    PromptManifestHookTruncation, PromptManifestProfile, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListParams, ProviderListResponse,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, RecoveryAction, RecoveryTrigger,
    SandboxMode, SkillArchiveFormat, SkillAuditEvent as ProtocolSkillAuditEvent, SkillListResponse,
    SkillsChangedNotification, SkillsHealthResponse, SkillsInstallResponse,
    SkillsPolicySetResponse, SkillsUninstallResponse, SkillsUpdateResponse,
    SkillsUploadAbortResponse, SkillsUploadChunkHeader, SkillsUploadFinishResponse,
    SkillsUploadStartResponse, TaskAgendaResponse, TaskAgentPrompt, TaskAgentSpecInput,
    TaskAttachmentMode, TaskCompletionBehavior, TaskCreateParams, TaskDeliveriesParams,
    TaskDeliveriesResponse, TaskDeliveryFormat, TaskDeliveryMode, TaskDeliveryPolicy,
    TaskDeliveryStatus, TaskEventPayload, TaskExecutorKind, TaskLifecyclePolicy, TaskOwnerKind,
    TaskParentTerminalAction, TaskPauseResponse, TaskResult, TaskResumeResponse,
    TaskRetryBackoffKind, TaskRetryPolicy, TaskRun, TaskTriggerInput, TaskTriggerSpec,
    TaskTriggerStatus, TaskValue, TaskWaitParams, Thread, ThreadAgentsDocArchiveResponse,
    ThreadAgentsDocGetResponse, ThreadAgentsDocResolveForThreadResponse,
    ThreadAgentsDocSaveResponse, ThreadAgentsDocStatus, ThreadClosedNotification,
    ThreadFolderCreateResponse, ThreadFolderDeleteResponse, ThreadFolderMoveResponse,
    ThreadHistoryEventPayload, ThreadHistoryResponse, ThreadMode, ThreadMoveResponse,
    ThreadOriginKind, ThreadSidebarVisibility, ThreadStartParams, ThreadStartResponse,
    ThreadStatus, ThreadTreeResponse, ThreadUnsubscribeResponse, ThreadUnsubscribeStatus,
    TimelineOriginKind, ToolCallStatus, ToolDisplayPayload, ToolOutputPolicySnapshot,
    ToolResultView, ToolStoragePayload, Turn, TurnCancelResponse, TurnCompletedNotification,
    TurnFailedNotification, TurnGetResponse, TurnItem, TurnItemEventPayload, TurnItemType,
    TurnKind, TurnOrigin, TurnSkillBinding, TurnStartResponse, TurnStatus, TurnTimelineParams,
    TurnTimelineResponse, UserInput, UserMessageAttachment, WorkspaceChangeKind,
    WorkspaceChangedNotification, WorkspaceCreateResponse, WorkspaceDefaultResponse,
    WorkspaceListResponse, WorkspaceSelectResponse, WorkspaceUpdateResponse, constants::events,
};
use pioneer_provider::providers::EchoProvider;
use pioneer_provider::{
    ChatRequest, ChatResponse, Provider, ProviderCapabilities, ProviderInputCapabilities,
    ProviderToolCall, StreamChunk,
};
use pioneer_skills::SkillTrustLevel;
use pioneer_tools::{
    BuiltinTools, ComputerUseToolsConfig, RawToolCall, ToolError, ToolLoopBudgetConfig,
    ToolRetryBudgetConfig, WebToolsConfig, build_tools,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, Database, EntityTrait, QueryFilter};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

struct CompletingSystemExecutor;

#[async_trait]
impl pioneer_tasks::TaskExecutor for CompletingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: pioneer_tasks::TaskExecutionContext,
        run: TaskRun,
        handle: pioneer_tasks::TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<pioneer_tasks::TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .complete_run(
                Some(TaskResult {
                    summary: Some("delivered scheduled result".to_owned()),
                    data: Some(TaskValue::Object(BTreeMap::from([(
                        "rawText".to_owned(),
                        TaskValue::String("delivered scheduled result\nfull detail".to_owned()),
                    )]))),
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(pioneer_tasks::TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: pioneer_tasks::TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: pioneer_tasks::TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<()> {
        Ok(())
    }
}

fn test_provider() -> Arc<pioneer_provider::ProviderRegistry> {
    Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(EchoProvider::new()),
    ))
}

struct SequencedToolProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    first_tool_calls: Vec<ProviderToolCall>,
    second_text: String,
    next_index: AtomicUsize,
}

#[derive(Clone, Copy)]
enum MemoryAgentE2eScript {
    ChatCapture,
}

struct MemoryAgentE2eProvider {
    name: &'static str,
    script: MemoryAgentE2eScript,
    policy_requests: std::sync::Mutex<Vec<ChatRequest>>,
    main_requests: std::sync::Mutex<Vec<ChatRequest>>,
    next_main_index: AtomicUsize,
}

struct DelayedProvider {
    delay: Duration,
    text: String,
}

struct CountingDelayedProvider {
    delay: Duration,
    text: String,
    calls: AtomicUsize,
}

struct CaptureSummaryProvider {
    text: String,
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    calls: AtomicUsize,
}

struct PreflightCaptureProvider {
    text: String,
    requests: std::sync::Mutex<Vec<ChatRequest>>,
}

struct HangingChildProvider {
    child_main_calls: AtomicUsize,
}

struct FlakyTitleProvider {
    failures_before_success: usize,
    text: String,
    calls: AtomicUsize,
}

struct GuardAwareProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    next_index: AtomicUsize,
}

struct CreateThenHangProvider {
    next_index: AtomicUsize,
}

#[async_trait::async_trait]
impl Provider for DelayedProvider {
    fn name(&self) -> &str {
        "delayed"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, _request: ChatRequest) -> anyhow::Result<ChatResponse> {
        sleep(self.delay).await;
        Ok(ChatResponse {
            text: self.text.clone(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

#[async_trait::async_trait]
impl Provider for CountingDelayedProvider {
    fn name(&self) -> &str {
        "counting-delayed"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, _request: ChatRequest) -> anyhow::Result<ChatResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        sleep(self.delay).await;
        Ok(ChatResponse {
            text: self.text.clone(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

impl CountingDelayedProvider {
    fn new(delay: Duration, text: impl Into<String>) -> Self {
        Self {
            delay,
            text: text.into(),
            calls: AtomicUsize::new(0),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl CaptureSummaryProvider {
    fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            requests: std::sync::Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("capture summary requests lock")
            .clone()
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl PreflightCaptureProvider {
    fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("preflight capture requests lock")
            .clone()
    }
}

impl HangingChildProvider {
    fn new() -> Self {
        Self {
            child_main_calls: AtomicUsize::new(0),
        }
    }

    fn child_main_call_count(&self) -> usize {
        self.child_main_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Provider for CaptureSummaryProvider {
    fn name(&self) -> &str {
        "capture-summary"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .expect("capture summary requests lock")
            .push(request);
        Ok(ChatResponse {
            text: self.text.clone(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

#[async_trait::async_trait]
impl Provider for PreflightCaptureProvider {
    fn name(&self) -> &str {
        "preflight-capture"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        let preflight = is_turn_preflight_request(&request);
        self.requests
            .lock()
            .expect("preflight capture requests lock")
            .push(request);
        if preflight {
            return Ok(test_turn_preflight_response());
        }
        Ok(text_response(self.text.clone()))
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

#[async_trait::async_trait]
impl Provider for HangingChildProvider {
    fn name(&self) -> &str {
        "hanging-child"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        let preflight = is_turn_preflight_request(&request);
        let child_main = is_child_task_main_request(&request);
        if preflight {
            return Ok(test_turn_preflight_response());
        }
        if child_main {
            self.child_main_calls.fetch_add(1, Ordering::SeqCst);
            return futures_util::future::pending::<anyhow::Result<ChatResponse>>().await;
        }
        Ok(text_response("parent done"))
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

#[async_trait::async_trait]
impl Provider for FlakyTitleProvider {
    fn name(&self) -> &str {
        "flaky-title"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, _request: ChatRequest) -> anyhow::Result<ChatResponse> {
        let call_number = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call_number <= self.failures_before_success {
            return Err(anyhow::anyhow!("transient title provider failure"));
        }

        Ok(ChatResponse {
            text: self.text.clone(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

impl FlakyTitleProvider {
    fn new(failures_before_success: usize, text: impl Into<String>) -> Self {
        Self {
            failures_before_success,
            text: text.into(),
            calls: AtomicUsize::new(0),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Provider for GuardAwareProvider {
    fn name(&self) -> &str {
        "guard-aware"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_turn_preflight_request(&request) {
            return Ok(test_task_turn_preflight_response());
        }
        if is_child_task_request(&request) {
            sleep(Duration::from_secs(10)).await;
            return Ok(ChatResponse {
                text: "slow child".to_owned(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }

        self.requests
            .lock()
            .expect("guard provider lock poisoned")
            .push(request.clone());
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let response = match index {
            0 => ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![ProviderToolCall {
                    id: "call_guard_create".to_owned(),
                    name: "task_create".to_owned(),
                    arguments: json!({
                        "title": "Long child for guard",
                        "goal": "Return slowly"
                    })
                    .to_string(),
                }],
            },
            1 => ChatResponse {
                text: "premature final".to_owned(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            },
            2 => {
                let task_id = extract_task_id_from_messages(&request.messages)
                    .unwrap_or_else(|| "missing_task_id".to_owned());
                ChatResponse {
                    text: String::new(),
                    usage: None,
                    reasoning_content: None,
                    tool_calls: vec![ProviderToolCall {
                        id: "call_guard_detach".to_owned(),
                        name: "task_detach".to_owned(),
                        arguments: json!({ "taskId": task_id }).to_string(),
                    }],
                }
            }
            _ => ChatResponse {
                text: "detached and done".to_owned(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            },
        };
        Ok(response)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

impl GuardAwareProvider {
    fn new() -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("guard provider lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl Provider for CreateThenHangProvider {
    fn name(&self) -> &str {
        "create-then-hang"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_turn_preflight_request(&request) {
            return Ok(test_task_turn_preflight_response());
        }
        if is_child_task_request(&request) {
            sleep(Duration::from_secs(10)).await;
            return Ok(ChatResponse {
                text: "slow child".to_owned(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }

        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            return Ok(ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![ProviderToolCall {
                    id: "call_cancel_create".to_owned(),
                    name: "task_create".to_owned(),
                    arguments: json!({
                        "title": "Child cancelled with parent",
                        "goal": "Keep running until parent cancels"
                    })
                    .to_string(),
                }],
            });
        }
        sleep(Duration::from_secs(10)).await;
        Ok(ChatResponse {
            text: "should not complete".to_owned(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

impl CreateThenHangProvider {
    fn new() -> Self {
        Self {
            next_index: AtomicUsize::new(0),
        }
    }
}

#[derive(Clone)]
enum Phase13HookBehavior {
    Succeed {
        contributions: Vec<HookContribution>,
    },
    Fail,
    Pending,
}

struct Phase13RecordingHookHandler {
    hook_id: HookId,
    calls: Arc<std::sync::Mutex<Vec<HookHandlerRequest>>>,
    behavior: Phase13HookBehavior,
}

#[async_trait::async_trait]
impl HookHandler for Phase13RecordingHookHandler {
    fn id(&self) -> HookId {
        self.hook_id.clone()
    }

    fn kind(&self) -> HookKind {
        HookKind::new("test.phase13").expect("valid hook kind")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPreCompaction]
    }

    fn capabilities(&self) -> HookCapabilities {
        HookCapabilities::new([
            HookCapability::new("contribute_prompt_section").expect("valid capability")
        ])
    }

    async fn execute(
        &self,
        request: HookHandlerRequest,
    ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
        self.calls
            .lock()
            .expect("phase 13 hook calls lock")
            .push(request);

        match &self.behavior {
            Phase13HookBehavior::Succeed { contributions } => Ok(HookHandlerResponse {
                contributions: contributions.clone(),
                ..HookHandlerResponse::default()
            }),
            Phase13HookBehavior::Fail => Err(HookError::new(
                HookDiagnosticCode::new("test.phase13_failed").expect("valid diagnostic code"),
                HookDiagnosticMessage::new("phase 13 hook failed").expect("valid diagnostic"),
            )),
            Phase13HookBehavior::Pending => futures_util::future::pending().await,
        }
    }
}

fn phase_13_empty_hook_runtime() -> Arc<HookRuntime> {
    Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ))
}

fn phase_13_hook_runtime(
    calls: Arc<std::sync::Mutex<Vec<HookHandlerRequest>>>,
    behavior: Phase13HookBehavior,
    await_policy: HookAwaitPolicy,
    timeout_ms: Option<u64>,
    failure_policy: HookFailurePolicy,
) -> Arc<HookRuntime> {
    phase_13_hook_runtime_with_fallback(
        calls,
        behavior,
        await_policy,
        timeout_ms,
        failure_policy,
        Vec::new(),
    )
}

fn phase_13_hook_runtime_with_fallback(
    calls: Arc<std::sync::Mutex<Vec<HookHandlerRequest>>>,
    behavior: Phase13HookBehavior,
    await_policy: HookAwaitPolicy,
    timeout_ms: Option<u64>,
    failure_policy: HookFailurePolicy,
    fallback_contributions: Vec<HookContribution>,
) -> Arc<HookRuntime> {
    let handlers = Arc::new(HookRegistry::new());
    let subscriptions = Arc::new(HookSubscriptionRegistry::new());
    let hook_id = HookId::new("test.phase13_recorder").expect("valid hook id");
    handlers
        .register_handler(Arc::new(Phase13RecordingHookHandler {
            hook_id: hook_id.clone(),
            calls,
            behavior,
        }))
        .expect("phase 13 hook registers");
    subscriptions
        .register_subscription(
            handlers.as_ref(),
            HookSubscription::new(
                HookSubscriptionId::new("test.phase13_subscription")
                    .expect("valid subscription id"),
                hook_id,
                HookPhase::TurnPreCompaction,
            )
            .with_execution_policy(HookExecutionPolicy {
                await_policy,
                timeout_ms,
                max_parallelism: None,
            })
            .with_failure_policy(failure_policy)
            .with_fallback_contributions(fallback_contributions),
        )
        .expect("phase 13 hook subscription registers");

    Arc::new(HookRuntime::with_options(
        handlers,
        subscriptions,
        HookRuntimeOptions {
            default_deadline_timeout_ms: 25,
            ..HookRuntimeOptions::default()
        },
    ))
}

async fn install_test_hook_runtime(processor: &Arc<MessageProcessor>, runtime: Arc<HookRuntime>) {
    *processor.hook_runtime.write().await = Some(runtime.clone());
    processor
        .agent_manager
        .set_hook_runtime(Some(runtime))
        .await;
}

async fn install_recoverable_test_hook_runtime(
    processor: &Arc<MessageProcessor>,
    runtime: Arc<HookRuntime>,
) {
    *processor.hook_runtime.write().await = Some(runtime);
    processor.ensure_hook_runtime_with_run_store().await;
}

#[test]
fn phase_15_message_processor_ensure_hook_runtime_attaches_crud_store() {
    run_gateway_message_test("phase15-hook-runtime", || async {
        let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
        let harness = setup_phase_13_compaction_harness(phase_13_provider_registry(provider)).await;
        let runtime = phase_13_empty_hook_runtime();
        assert!(!runtime.has_run_store());

        install_recoverable_test_hook_runtime(&harness.processor, runtime.clone()).await;

        let stored = harness
            .processor
            .hook_runtime
            .read()
            .await
            .clone()
            .expect("message processor should store hook runtime");
        assert!(stored.has_run_store());
        assert!(!Arc::ptr_eq(&stored, &runtime));
        assert!(harness.processor.agent_manager.has_hook_runtime().await);
    });
}

#[test]
fn phase_21_message_processor_starts_generic_hook_recovery_worker() {
    run_gateway_message_test("phase21-hook-recovery-worker", || async {
        let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
        let harness = setup_phase_13_compaction_harness(phase_13_provider_registry(provider)).await;
        harness
            .processor
            .set_hook_recovery_config(GatewayHookRecoveryConfig {
                enabled: true,
                startup_scan: false,
                poll_interval_ms: 60_000,
                batch_size: 4,
                max_concurrent: 1,
                stale_running_after_ms: 1_000,
                strict_debug: true,
            })
            .await;
        install_recoverable_test_hook_runtime(&harness.processor, phase_13_empty_hook_runtime())
            .await;

        harness.processor.start_hook_recovery_worker().await;

        let mut guard = harness.processor.hook_recovery_worker.lock().await;
        let handle = guard
            .take()
            .expect("hook recovery worker should be started");
        handle.abort();
    });
}

#[tokio::test]
async fn phase_21_memory_bridge_installs_recoverable_hook_runtime() {
    let harness = setup_memory_gateway_harness("phase21_recoverable_runtime", true).await;
    harness.processor.bind_memory_bridge_if_enabled().await;

    let runtime = harness
        .processor
        .hook_runtime
        .read()
        .await
        .clone()
        .expect("memory bridge should install hook runtime");
    assert!(runtime.has_run_store());
    let post_turn_subscription = runtime
        .subscriptions()
        .get_subscription(
            &HookSubscriptionId::new("memory.post_turn_extractor.default")
                .expect("static subscription id is valid"),
        )
        .expect("subscription lookup succeeds");
    assert!(post_turn_subscription.is_some());
}

#[tokio::test]
async fn memory_settings_update_reinstalls_memory_hook_runtime() {
    let mut harness = setup_memory_gateway_harness("settings_reinstall_memory_hooks", true).await;
    harness.processor.bind_memory_bridge_if_enabled().await;

    assert_memory_hook_subscription(&harness.processor, "memory.active_recall.default", true).await;
    assert_memory_hook_subscription(
        &harness.processor,
        "memory.post_turn_extractor.default",
        true,
    )
    .await;

    let disable_request_id = generate_test_request_id("settings", "disable_memory_hooks");
    let disable_request = json!({
        "jsonrpc": "2.0",
        "id": disable_request_id,
        "method": "settings/update",
        "params": {
            "update": {
                "memory": {
                    "enabled": true,
                    "deterministic_recall_enabled": true,
                    "active_recall_enabled": false,
                    "tools_enabled": true,
                    "proactive_writes_enabled": false,
                    "background_extraction_enabled": true,
                    "proactive_writes_model": {
                        "source": "thread"
                    },
                    "debug_trace_enabled": false,
                    "strict_diagnostics_enabled": false
                }
            }
        }
    });
    harness
        .processor
        .process_request(harness.connection_id, &disable_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut harness.rx, disable_request_id.as_str()).await;

    assert_memory_hook_subscription(&harness.processor, "memory.active_recall.default", false)
        .await;
    assert_memory_hook_subscription(
        &harness.processor,
        "memory.post_turn_extractor.default",
        false,
    )
    .await;

    let enable_request_id = generate_test_request_id("settings", "enable_memory_hooks");
    let enable_request = json!({
        "jsonrpc": "2.0",
        "id": enable_request_id,
        "method": "settings/update",
        "params": {
            "update": {
                "memory": {
                    "enabled": true,
                    "deterministic_recall_enabled": true,
                    "active_recall_enabled": true,
                    "tools_enabled": true,
                    "proactive_writes_enabled": true,
                    "background_extraction_enabled": true,
                    "proactive_writes_model": {
                        "source": "custom",
                        "model_provider": "extractor-provider",
                        "model": "extractor-model"
                    },
                    "debug_trace_enabled": false,
                    "strict_diagnostics_enabled": false
                }
            }
        }
    });
    harness
        .processor
        .process_request(harness.connection_id, &enable_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut harness.rx, enable_request_id.as_str()).await;

    assert_memory_hook_subscription(&harness.processor, "memory.active_recall.default", true).await;
    assert_memory_hook_subscription(
        &harness.processor,
        "memory.post_turn_extractor.default",
        true,
    )
    .await;
    let memory_config = harness.processor.memory_loop_config();
    assert_eq!(
        memory_config.post_turn_extractor.provider_name.as_deref(),
        Some("extractor-provider")
    );
    assert_eq!(
        memory_config.post_turn_extractor.model.as_deref(),
        Some("extractor-model")
    );
}

async fn assert_memory_hook_subscription(
    processor: &MessageProcessor,
    subscription_id: &str,
    expected_present: bool,
) {
    let runtime = processor
        .hook_runtime
        .read()
        .await
        .clone()
        .expect("memory hook runtime should be installed");
    let subscription_id =
        HookSubscriptionId::new(subscription_id).expect("test subscription id should be valid");
    let found = runtime
        .subscriptions()
        .get_subscription(&subscription_id)
        .expect("subscription lookup should succeed")
        .is_some();
    assert_eq!(
        found, expected_present,
        "unexpected presence for hook subscription `{subscription_id}`"
    );
}

impl SequencedToolProvider {
    fn new(first_tool_calls: Vec<ProviderToolCall>, second_text: impl Into<String>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            first_tool_calls,
            second_text: second_text.into(),
            next_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("sequenced tool provider lock poisoned")
            .clone()
    }
}

impl MemoryAgentE2eProvider {
    fn new(name: &'static str, script: MemoryAgentE2eScript) -> Self {
        Self {
            name,
            script,
            policy_requests: std::sync::Mutex::new(Vec::new()),
            main_requests: std::sync::Mutex::new(Vec::new()),
            next_main_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_main_requests(&self) -> Vec<ChatRequest> {
        self.main_requests
            .lock()
            .expect("memory e2e provider main requests lock poisoned")
            .clone()
    }

    fn snapshot_policy_requests(&self) -> Vec<ChatRequest> {
        self.policy_requests
            .lock()
            .expect("memory e2e provider policy requests lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl Provider for MemoryAgentE2eProvider {
    fn name(&self) -> &str {
        self.name
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_memory_policy_classifier_request(&request) {
            self.policy_requests
                .lock()
                .expect("memory e2e provider policy requests lock poisoned")
                .push(request);
            return Ok(ChatResponse {
                text: memory_policy_json_for_script(self.script),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }

        self.main_requests
            .lock()
            .expect("memory e2e provider main requests lock poisoned")
            .push(request.clone());
        let round = self.next_main_index.fetch_add(1, Ordering::SeqCst);
        Ok(memory_agent_e2e_response(self.script, round, &request))
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        let mut chunks = Vec::new();
        if !response.tool_calls.is_empty() {
            chunks.push(Ok(StreamChunk::tool_calls(response.tool_calls)));
        }
        if !response.text.is_empty() {
            chunks.push(Ok(StreamChunk::delta(response.text)));
        }
        chunks.push(Ok(StreamChunk::final_chunk()));
        Ok(futures_util::stream::iter(chunks).boxed())
    }
}

fn is_memory_policy_classifier_request(request: &ChatRequest) -> bool {
    request.messages.iter().any(|message| {
        message
            .content
            .contains("Pioneer memory turn policy classifier")
    })
}

fn memory_policy_json_for_script(script: MemoryAgentE2eScript) -> String {
    let value = match script {
        MemoryAgentE2eScript::ChatCapture => json!({
            "intent": "normal",
            "recall": "allow",
            "prompt": "full",
            "readTools": "allow",
            "rememberTool": "allow",
            "forgetTool": "allow",
            "postTurnExtraction": "disabled",
            "activeMemory": "disabled",
            "explicitRemember": false,
            "explicitForget": false,
            "forgetTargetHint": null,
            "language": "ru",
            "confidence": 0.9,
            "reasonCode": "default_allow_read"
        }),
    };
    serde_json::to_string(&value).expect("policy json should serialize")
}

fn memory_agent_e2e_response(
    script: MemoryAgentE2eScript,
    _round: usize,
    _request: &ChatRequest,
) -> ChatResponse {
    match script {
        MemoryAgentE2eScript::ChatCapture => text_response("Ок."),
    }
}

fn text_response(text: impl Into<String>) -> ChatResponse {
    ChatResponse {
        text: text.into(),
        usage: None,
        reasoning_content: None,
        tool_calls: Vec::new(),
    }
}

fn test_turn_preflight_response() -> ChatResponse {
    test_turn_preflight_response_with_visible_tools(&[])
}

fn test_task_turn_preflight_response() -> ChatResponse {
    test_turn_preflight_response_with_visible_tools(&[
        "task_create",
        "task_wait",
        "task_cancel",
        "task_update",
        "task_detach",
        "task_list",
        "task_get",
        "task_reschedule",
        "task_pause",
        "task_resume",
    ])
}

fn test_turn_preflight_response_with_visible_tools(tool_names: &[&str]) -> ChatResponse {
    text_response(json!({ "tools": { "visibleTools": tool_names } }).to_string())
}

fn sequenced_tool_provider_preflight_response(
    request: &ChatRequest,
    first_tool_calls: &[ProviderToolCall],
) -> ChatResponse {
    let task_needed = first_tool_calls
        .iter()
        .any(|tool_call| tool_call.name.starts_with("task_"))
        || request
            .messages
            .iter()
            .any(|message| message.content.contains("delegate a task"));

    if task_needed {
        test_task_turn_preflight_response()
    } else {
        test_turn_preflight_response()
    }
}

fn is_turn_preflight_request(request: &ChatRequest) -> bool {
    request.compiled_prompt.is_none()
        && request.tools.is_none()
        && request.tool_choice.is_none()
        && request.messages.len() == 1
        && request.messages[0]
            .content
            .contains("internal turn preflight planner")
        && request.messages[0]
            .content
            .contains("Structured input JSON")
}

fn extract_task_id_from_messages(messages: &[pioneer_provider::ChatMessage]) -> Option<String> {
    for message in messages.iter().rev() {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(message.content.as_str())
            && let Some(task_id) = value.get("taskId").and_then(serde_json::Value::as_str)
        {
            return Some(task_id.to_owned());
        }
        if let Some(index) = message.content.find("\"taskId\"") {
            let suffix = &message.content[index..];
            if let Some(start) = suffix.find(':') {
                let after_colon = suffix[start + 1..].trim_start();
                if let Some(rest) = after_colon.strip_prefix('"')
                    && let Some(end) = rest.find('"')
                {
                    return Some(rest[..end].to_owned());
                }
            }
        }
    }
    None
}

fn is_child_task_request(request: &ChatRequest) -> bool {
    request.messages.iter().any(|message| {
        message
            .content
            .contains("You are executing a delegated task.")
            || message
                .content
                .contains("You are executing this durable task run now.")
    })
}

fn is_child_task_main_request(request: &ChatRequest) -> bool {
    request.compiled_prompt.is_some() && is_child_task_request(request)
}

#[async_trait::async_trait]
impl Provider for SequencedToolProvider {
    fn name(&self) -> &str {
        "sequenced-tools"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_turn_preflight_request(&request) {
            return Ok(sequenced_tool_provider_preflight_response(
                &request,
                &self.first_tool_calls,
            ));
        }

        self.requests
            .lock()
            .expect("sequenced tool provider lock poisoned")
            .push(request);

        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            return Ok(ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: self.first_tool_calls.clone(),
            });
        }

        Ok(ChatResponse {
            text: self.second_text.clone(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk()),
        ])
        .boxed())
    }
}

fn test_gateway_secrets() -> Arc<GatewaySecrets> {
    Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())))
}

fn test_gateway_secrets_with_store() -> (Arc<GatewaySecrets>, Arc<MemorySecretStore>) {
    let secret_store = Arc::new(MemorySecretStore::new());
    (
        Arc::new(GatewaySecrets::new(secret_store.clone())),
        secret_store,
    )
}

async fn setup_provider_api_key_processor(
    case_id: &str,
) -> (
    MessageProcessor,
    Arc<MemorySecretStore>,
    mpsc::Receiver<Message>,
    u64,
    Arc<WorkspaceManager>,
    String,
    std::path::PathBuf,
) {
    let session_manager = Arc::new(SessionManager::new());
    let (tx, rx) = mpsc::channel(16);
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let secret_store = Arc::new(MemorySecretStore::new());
    let gateway_secrets = Arc::new(GatewaySecrets::new(secret_store.clone()));
    let base_dir = unique_temp_dir(case_id);
    std::fs::create_dir_all(&base_dir).expect("create settings dir");
    let settings_path = base_dir.join("gateway-settings.toml");
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager.clone(),
        crud_store,
        gateway_secrets,
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    (
        processor,
        secret_store,
        rx,
        connection_id,
        workspace_manager,
        workspace_id,
        settings_path,
    )
}

#[tokio::test]
async fn provider_api_key_handlers_use_keystore_without_settings_write() {
    let (
        processor,
        secret_store,
        mut rx,
        connection_id,
        workspace_manager,
        workspace_id,
        settings_path,
    ) = setup_provider_api_key_processor("provider_api_key_handlers").await;
    let other_workspace_id = workspace_manager
        .create_workspace("provider_scope_other", Some("Provider Scope Other"))
        .await
        .expect("create other workspace")
        .id;
    let set_request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "set"))
            .expect("valid set request id");
    let list_request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "list"))
            .expect("valid list request id");
    let other_list_request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "list_other"))
            .expect("valid other list request id");
    let delete_request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "delete"))
            .expect("valid delete request id");
    let delete_missing_request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "missing"))
            .expect("valid missing delete request id");

    processor
        .provider_set_api_key(
            connection_id,
            set_request_id.clone(),
            ProviderSetApiKeyParams {
                workspace_id: workspace_id.clone(),
                provider: "  OpenRouter  ".to_owned(),
                api_key: "sk-secret-provider-key".to_owned(),
            },
        )
        .await;
    let set_response = recv_response_by_id(&mut rx, set_request_id.as_str()).await;
    let set_payload: ProviderSetApiKeyResponse =
        serde_json::from_value(set_response.result).expect("provider/set_api_key payload");
    assert_eq!(set_payload.provider, "openrouter");
    assert!(set_payload.updated);
    assert_eq!(
        secret_store
            .get_string(
                &SecretId::workspace_provider_api_key(workspace_id.as_str(), "openrouter")
                    .expect("provider id"),
            )
            .expect("read provider key"),
        Some("sk-secret-provider-key".to_owned())
    );
    assert!(
        !settings_path.exists(),
        "provider key handler must not write gateway settings"
    );

    processor
        .provider_list(
            connection_id,
            list_request_id.clone(),
            ProviderListParams {
                workspace_id: workspace_id.clone(),
            },
        )
        .await;
    let list_response = recv_response_by_id(&mut rx, list_request_id.as_str()).await;
    let list_payload: ProviderListResponse =
        serde_json::from_value(list_response.result).expect("provider/list payload");
    assert_eq!(list_payload.providers.len(), 1);
    assert_eq!(list_payload.providers[0].name, "openrouter");

    processor
        .provider_list(
            connection_id,
            other_list_request_id.clone(),
            ProviderListParams {
                workspace_id: other_workspace_id,
            },
        )
        .await;
    let other_list_response = recv_response_by_id(&mut rx, other_list_request_id.as_str()).await;
    let other_list_payload: ProviderListResponse =
        serde_json::from_value(other_list_response.result).expect("provider/list other payload");
    assert!(other_list_payload.providers.is_empty());

    processor
        .provider_delete_api_key(
            connection_id,
            delete_request_id.clone(),
            ProviderDeleteApiKeyParams {
                workspace_id: workspace_id.clone(),
                provider: "OpenRouter".to_owned(),
            },
        )
        .await;
    let delete_response = recv_response_by_id(&mut rx, delete_request_id.as_str()).await;
    let delete_payload: ProviderDeleteApiKeyResponse =
        serde_json::from_value(delete_response.result).expect("provider/delete_api_key payload");
    assert_eq!(delete_payload.provider, "openrouter");
    assert!(delete_payload.deleted);
    assert_eq!(
        secret_store
            .get_string(
                &SecretId::workspace_provider_api_key(workspace_id.as_str(), "openrouter")
                    .expect("provider id"),
            )
            .expect("read deleted provider key"),
        None
    );

    processor
        .provider_delete_api_key(
            connection_id,
            delete_missing_request_id.clone(),
            ProviderDeleteApiKeyParams {
                workspace_id,
                provider: "openrouter".to_owned(),
            },
        )
        .await;
    let missing_delete_response =
        recv_response_by_id(&mut rx, delete_missing_request_id.as_str()).await;
    let missing_delete_payload: ProviderDeleteApiKeyResponse =
        serde_json::from_value(missing_delete_response.result)
            .expect("provider/delete_api_key missing payload");
    assert_eq!(missing_delete_payload.provider, "openrouter");
    assert!(!missing_delete_payload.deleted);
}

#[tokio::test]
async fn provider_set_api_key_rejects_empty_key_without_store_write() {
    let (
        processor,
        secret_store,
        mut rx,
        connection_id,
        _workspace_manager,
        workspace_id,
        _settings_path,
    ) = setup_provider_api_key_processor("provider_api_key_empty").await;
    let request_id =
        pioneer_protocol::RequestId::new(generate_test_request_id("provider", "empty"))
            .expect("valid request id");

    processor
        .provider_set_api_key(
            connection_id,
            request_id.clone(),
            ProviderSetApiKeyParams {
                workspace_id,
                provider: "openrouter".to_owned(),
                api_key: "   ".to_owned(),
            },
        )
        .await;

    let error = recv_error_by_id(&mut rx, request_id.as_str()).await;
    assert_eq!(error.error.code, pioneer_protocol::INVALID_PARAMS_CODE);
    let entries = secret_store
        .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
        .expect("list provider keys");
    assert!(entries.is_empty());
}

fn test_task_create_params(
    workspace_id: &str,
    parent_thread_id: &str,
    parent_turn_id: &str,
    goal: &str,
    max_depth: i64,
) -> TaskCreateParams {
    TaskCreateParams {
        workspace_id: workspace_id.to_owned(),
        owner_kind: TaskOwnerKind::Thread,
        owner_id: Some(parent_thread_id.to_owned()),
        created_by_thread_id: Some(parent_thread_id.to_owned()),
        created_by_turn_id: Some(parent_turn_id.to_owned()),
        parent_task_id: None,
        executor_kind: TaskExecutorKind::Agent,
        title: goal.to_owned(),
        goal: goal.to_owned(),
        priority: 0,
        trigger: TaskTriggerInput {
            spec: TaskTriggerSpec::Immediate,
        },
        agent_spec: Some(TaskAgentSpecInput {
            agent_role: Some("worker".to_owned()),
            agent_nickname: Some("Worker".to_owned()),
            model: Some("test-model".to_owned()),
            model_provider: Some("openai".to_owned()),
            prompt: TaskAgentPrompt {
                goal: goal.to_owned(),
                instructions: vec!["Return a concise final result.".to_owned()],
                input: None,
                output_instructions: None,
            },
            context_policy: None,
            tool_policy: None,
            result_contract: None,
            depth: 0,
            max_depth,
        }),
        lifecycle_policy: Some(TaskLifecyclePolicy {
            attachment: TaskAttachmentMode::Attached,
            on_parent_cancel: TaskParentTerminalAction::Cancel,
            on_parent_failure: TaskParentTerminalAction::Cancel,
            completion: TaskCompletionBehavior::CompleteOnTerminalRun,
        }),
        delivery_policy: Some(TaskDeliveryPolicy {
            mode: TaskDeliveryMode::None,
            thread_id: None,
            webhook_url: None,
            include_result: true,
            format: TaskDeliveryFormat::Summary,
        }),
        retry_policy: Some(TaskRetryPolicy {
            max_attempts: 1,
            backoff: TaskRetryBackoffKind::None,
            initial_delay_seconds: None,
            max_delay_seconds: None,
            retry_on: Vec::new(),
        }),
        timeout_policy: None,
        concurrency_policy: None,
        metadata: None,
    }
}

async fn create_task_for_test(
    processor: &Arc<MessageProcessor>,
    params: TaskCreateParams,
) -> anyhow::Result<pioneer_protocol::TaskCreateResponse> {
    message_future(
        processor
            .task_runtime
            .service()
            .create_task(pioneer_tasks::TaskCreateContext::default(), params),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:#}"))
}

async fn wait_tasks_for_test(
    processor: &Arc<MessageProcessor>,
    params: TaskWaitParams,
) -> anyhow::Result<pioneer_protocol::TaskWaitResponse> {
    processor
        .task_runtime
        .service()
        .wait_tasks(pioneer_tasks::TaskWaitContext::default(), params)
        .await
        .map_err(|error| anyhow::anyhow!("{error:#}"))
}

async fn cancel_task_for_test(
    processor: &Arc<MessageProcessor>,
    params: pioneer_protocol::TaskCancelParams,
) -> anyhow::Result<pioneer_protocol::TaskCancelResponse> {
    message_future(
        processor
            .task_runtime
            .service()
            .cancel_task(pioneer_tasks::TaskMutationContext::default(), params),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:#}"))
}

async fn detach_task_for_test(
    processor: &Arc<MessageProcessor>,
    params: pioneer_protocol::TaskDetachParams,
) -> anyhow::Result<pioneer_protocol::TaskDetachResponse> {
    message_future(
        processor
            .task_runtime
            .service()
            .detach_task(pioneer_tasks::TaskMutationContext::default(), params),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error:#}"))
}

fn test_summary_config() -> super::summary::SummaryConfig {
    super::summary::SummaryConfig {
        summary_model: Some("test-model".to_owned()),
        summary_model_provider: Some("echo".to_owned()),
        title_model: Some("test-model".to_owned()),
        title_model_provider: Some("echo".to_owned()),
    }
}

fn test_context_budget() -> super::ContextBudget {
    super::ContextBudget {
        max_context_tokens: 128_000,
        response_reserve_tokens: 16_000,
    }
}

fn test_tool_loop_config() -> ToolLoopConfig {
    let web = GatewayWebToolsConfig::default();
    ToolLoopConfig {
        preflight: pioneer_agent::PreflightLoopConfig::default(),
        web: WebToolsConfig {
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
        computer_use: ComputerUseToolsConfig {
            runtime_home_dir: std::env::temp_dir().join("pioneer-gateway-message-tests"),
            artifacts_subdir: "tools/computer_use".to_owned(),
            ..ComputerUseToolsConfig::default()
        },
        skills: SkillsLoopConfig {
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
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
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
        budget: ToolLoopBudgetConfig::default(),
        retry: ToolRetryBudgetConfig::default(),
    }
}

fn test_tool_loop_config_with_roots(
    system_root: &std::path::Path,
    user_root: &std::path::Path,
    _workspace_root: &std::path::Path,
    registry_root: &std::path::Path,
) -> ToolLoopConfig {
    let mut config = test_tool_loop_config();
    config.skills.system_roots = vec![system_root.display().to_string()];
    config.skills.user_roots = vec![user_root.display().to_string()];
    config.skills.registry_roots = vec![registry_root.display().to_string()];
    config
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "pioneer_gateway_{prefix}_{}_{}",
        std::process::id(),
        now_nanos
    ))
}

async fn spawn_one_shot_http_server(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test http server should bind");
    let addr = listener
        .local_addr()
        .expect("test http server should expose local addr");
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request_buf = [0_u8; 1024];
        let _ = stream.read(&mut request_buf).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    format!("http://{addr}/dynamic-secret")
}

fn write_test_skill(
    root: &std::path::Path,
    slug: &str,
    extra_frontmatter: &str,
    body: &str,
) -> std::path::PathBuf {
    let skill_dir = root.join("tests").join(slug);
    std::fs::create_dir_all(&skill_dir).expect("must create skill directory");
    let frontmatter = if extra_frontmatter.trim().is_empty() {
        String::new()
    } else {
        format!("{extra_frontmatter}\n")
    };
    std::fs::write(
        skill_dir.join("SKILL.md"),
        format!(
            r#"---
name: {slug}
slug: {slug}
description: {slug} description
{frontmatter}---
{body}"#
        ),
    )
    .expect("must write skill file");
    skill_dir
}

async fn create_finalized_skill_upload(
    processor: &MessageProcessor,
    rx: &mut mpsc::Receiver<Message>,
    connection_id: u64,
    workspace_id: &str,
    source_path: &std::path::Path,
    request_prefix: &str,
) -> String {
    let archive = build_test_skill_archive(source_path);
    let sha256 = hex::encode(Sha256::digest(archive.as_slice()));
    let start_request_id = generate_test_request_id(request_prefix, "start");
    let start_request = json!({
        "jsonrpc": "2.0",
        "id": start_request_id,
        "method": "skills/upload/start",
        "params": {
            "workspace_id": workspace_id,
            "file_name": format!(
                "{}.tar.gz",
                source_path.file_name().unwrap().to_string_lossy()
            ),
            "archive_format": SkillArchiveFormat::TarGz,
            "compressed_size_bytes": archive.len(),
            "uncompressed_size_hint_bytes": archive.len(),
            "sha256": sha256
        }
    });
    processor
        .process_request(connection_id, &start_request.to_string())
        .await;

    let start_response = recv_response_by_id(rx, start_request_id.as_str()).await;
    let start_payload: SkillsUploadStartResponse =
        serde_json::from_value(start_response.result).expect("skills/upload/start decode");

    let header = SkillsUploadChunkHeader {
        workspace_id: workspace_id.to_owned(),
        upload_id: start_payload.upload_id.clone(),
        offset: 0,
        len: u64::try_from(archive.len()).expect("archive length should fit u64"),
        chunk_sha256: Some(hex::encode(Sha256::digest(archive.as_slice()))),
    };
    let header_bytes = serde_json::to_vec(&header).expect("chunk header should encode");
    let mut frame = Vec::with_capacity(8 + header_bytes.len() + archive.len());
    frame.extend_from_slice(b"PSU1");
    frame.extend_from_slice(
        &u32::try_from(header_bytes.len())
            .expect("chunk header length should fit u32")
            .to_be_bytes(),
    );
    frame.extend_from_slice(header_bytes.as_slice());
    frame.extend_from_slice(archive.as_slice());

    processor
        .process_binary_frame(connection_id, frame.as_slice())
        .await;
    let _ack = recv_notification_by_method(rx, events::SKILLS_UPLOAD_CHUNK_ACK).await;

    let finish_request_id = generate_test_request_id(request_prefix, "finish");
    let finish_request = json!({
        "jsonrpc": "2.0",
        "id": finish_request_id,
        "method": "skills/upload/finish",
        "params": {
            "workspace_id": workspace_id,
            "upload_id": start_payload.upload_id
        }
    });
    processor
        .process_request(connection_id, &finish_request.to_string())
        .await;
    let finish_response = recv_response_by_id(rx, finish_request_id.as_str()).await;
    let finish_payload: SkillsUploadFinishResponse =
        serde_json::from_value(finish_response.result).expect("skills/upload/finish decode");
    assert_eq!(finish_payload.status, "finalized");
    finish_payload.upload_id
}

fn build_test_skill_archive(source_path: &std::path::Path) -> Vec<u8> {
    use flate2::{Compression, GzBuilder};
    use std::io;
    use tar::{Builder, Header};

    let root_name = source_path
        .file_name()
        .expect("source path should have file name")
        .to_string_lossy()
        .to_string();
    let encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);

    let mut root_header = Header::new_gnu();
    root_header.set_entry_type(tar::EntryType::Directory);
    root_header.set_size(0);
    root_header.set_mode(0o755);
    root_header.set_mtime(0);
    root_header.set_uid(0);
    root_header.set_gid(0);
    root_header.set_cksum();
    builder
        .append_data(
            &mut root_header,
            std::path::Path::new(root_name.as_str()),
            io::empty(),
        )
        .expect("append root dir");

    let skill_path = source_path.join("SKILL.md");
    let mut skill_file = std::fs::File::open(skill_path.as_path()).expect("open SKILL.md");
    let skill_size = skill_file.metadata().expect("stat SKILL.md").len();
    let archive_path = format!("{root_name}/SKILL.md");
    let mut skill_header = Header::new_gnu();
    skill_header.set_entry_type(tar::EntryType::Regular);
    skill_header.set_size(skill_size);
    skill_header.set_mode(0o644);
    skill_header.set_mtime(0);
    skill_header.set_uid(0);
    skill_header.set_gid(0);
    skill_header.set_cksum();
    builder
        .append_data(
            &mut skill_header,
            std::path::Path::new(archive_path.as_str()),
            &mut skill_file,
        )
        .expect("append SKILL.md");

    builder
        .into_inner()
        .expect("finalize tar")
        .finish()
        .expect("finalize gzip")
}

fn generate_test_request_id(prefix: &str, suffix: &str) -> String {
    let mut id = format!("{prefix}{suffix}")
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    if id.len() > 21 {
        id.truncate(21);
    }
    while id.len() < 21 {
        id.push('0');
    }
    id
}

#[test]
fn item_delta_event_method_maps_generic_to_agent_message_delta() {
    assert_eq!(
        MessageProcessor::item_delta_event_method(Some(ItemDeltaStream::Generic)),
        events::ITEM_AGENT_MESSAGE_DELTA
    );
}

struct ProgressDeltaHarness {
    processor: MessageProcessor,
    crud_store: Arc<CrudStore>,
    rx: Option<mpsc::Receiver<Message>>,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    item_id: String,
}

async fn setup_progress_delta_harness(
    case_id: &str,
    item: TurnItem,
    subscribe: bool,
) -> ProgressDeltaHarness {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager.clone(),
        test_provider(),
        session_manager.clone(),
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_id = generate_test_request_id("thr", case_id);
    let turn_id = generate_test_request_id("turn", case_id);
    let item_id = item.item_id().to_owned();

    let rx = if subscribe {
        let (tx, rx) = mpsc::channel(16);
        let connection_id = session_manager.register_connection(tx).await;
        thread_manager
            .thread_start(
                connection_id,
                workspace_id.clone(),
                ThreadStartParams {
                    thread_id: thread_id.clone(),
                    workspace_id: workspace_id.clone(),
                    name: None,
                    model: Some("test-model".to_owned()),
                    model_provider: Some("openai".to_owned()),
                    sandbox: Some(SandboxMode::FullAccess),
                    mode: Some(pioneer_protocol::ThreadMode::Agent),
                    origin_kind: None,
                    sidebar_visibility: None,
                    agent_nickname: None,
                    agent_role: None,
                },
            )
            .await
            .expect("thread/start should seed subscribed thread");
        Some(rx)
    } else {
        None
    };

    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.clone(),
        id: thread_id.clone(),
        name: None,
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let turn = Turn {
        id: turn_id.clone(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
        .await
        .expect("turn/start should materialize");
    crud_store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item,
            },
            2,
        )
        .await
        .expect("item/started should materialize");

    ProgressDeltaHarness {
        processor,
        crud_store,
        rx,
        workspace_id,
        thread_id,
        turn_id,
        item_id,
    }
}

fn progress_delta_notification(
    harness: &ProgressDeltaHarness,
    delta: &str,
    stream: ItemDeltaStream,
) -> ItemDeltaNotification {
    ItemDeltaNotification {
        workspace_id: harness.workspace_id.clone(),
        thread_id: harness.thread_id.clone(),
        turn_id: harness.turn_id.clone(),
        item_id: harness.item_id.clone(),
        delta: delta.to_owned(),
        stream: Some(stream),
        payload: None,
        markdown: None,
        markdown_version: None,
    }
}

async fn turn_item_payloads(harness: &ProgressDeltaHarness) -> Vec<TurnItemEventPayload> {
    harness
        .crud_store
        .get_turn_item_events(harness.thread_id.as_str(), harness.turn_id.as_str())
        .await
        .expect("turn item events query should succeed")
        .expect("turn item events should exist")
        .events
        .into_iter()
        .map(|event| event.payload)
        .collect()
}

fn assert_no_persisted_item_delta(payloads: &[TurnItemEventPayload]) {
    assert!(
        payloads
            .iter()
            .all(|payload| !matches!(payload, TurnItemEventPayload::ItemDelta { .. })),
        "progress delta should not be persisted as a turn item event"
    );
}

fn command_execution_item(item_id: &str) -> TurnItem {
    TurnItem::CommandExecution {
        id: item_id.to_owned(),
        tool_name: "exec_command".to_owned(),
        arguments: json!({ "command": ["echo", "hello"] }),
        status: ToolCallStatus::InProgress,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
        display: ToolDisplayPayload::Shell {
            stdout: None,
            stderr: None,
            aggregated_output: None,
            exit_code: None,
            duration_ms: None,
            timed_out: None,
            truncated: false,
        },
        storage: ToolStoragePayload::Shell {
            stdout: None,
            stderr: None,
            aggregated_output: None,
            exit_code: None,
            duration_ms: None,
            timed_out: None,
            truncated: false,
        },
        recovery: None,
        command: vec!["echo".to_owned(), "hello".to_owned()],
        cwd: None,
        success: None,
        outcome: None,
        observation: None,
    }
}

fn web_fetch_item(item_id: &str) -> TurnItem {
    TurnItem::WebFetch {
        id: item_id.to_owned(),
        tool_name: "web_fetch".to_owned(),
        arguments: json!({ "url": "https://example.com" }),
        status: ToolCallStatus::InProgress,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::None,
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
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assistant_progress_delta_is_live_only_and_completed_item_is_durable() {
    let mut harness = setup_progress_delta_harness(
        "agent_msg",
        TurnItem::AgentMessage {
            id: "item_progress_agent_msg".to_owned(),
            text: String::new(),
            markdown: None,
            markdown_version: None,
        },
        true,
    )
    .await;

    harness
        .processor
        .handle_progress_agent_event(AgentProgressEvent::ItemDelta {
            notification: progress_delta_notification(
                &harness,
                "hel",
                ItemDeltaStream::AgentMessage,
            ),
        })
        .await;

    let live = recv_notification_by_method(
        harness.rx.as_mut().expect("live receiver should exist"),
        events::ITEM_AGENT_MESSAGE_DELTA,
    )
    .await;
    let live_payload: ItemDeltaNotification =
        serde_json::from_value(live.params.expect("delta params expected"))
            .expect("live item delta payload should decode");
    assert_eq!(live_payload.delta, "hel");

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());

    harness
        .processor
        .handle_durable_agent_event(AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: harness.thread_id.clone(),
                turn_id: harness.turn_id.clone(),
                item: TurnItem::AgentMessage {
                    id: harness.item_id.clone(),
                    text: "hello".to_owned(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        })
        .await;

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());
    assert!(payloads.iter().any(|payload| matches!(
        payload,
        TurnItemEventPayload::ItemCompleted {
            item: TurnItem::AgentMessage { id, text, .. },
            ..
        } if id == &harness.item_id && text == "hello"
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_assistant_progress_deltas_do_not_create_persisted_delta_rows() {
    let harness = setup_progress_delta_harness(
        "manymsg",
        TurnItem::AgentMessage {
            id: "item_progress_many_msg".to_owned(),
            text: String::new(),
            markdown: None,
            markdown_version: None,
        },
        false,
    )
    .await;

    for _ in 0..1000 {
        harness
            .processor
            .handle_progress_agent_event(AgentProgressEvent::ItemDelta {
                notification: progress_delta_notification(
                    &harness,
                    "x",
                    ItemDeltaStream::AgentMessage,
                ),
            })
            .await;
    }

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reasoning_progress_delta_is_live_only_by_default() {
    let harness = setup_progress_delta_harness(
        "reasoning",
        TurnItem::Reasoning {
            id: "item_progress_reasoning".to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        },
        false,
    )
    .await;

    harness
        .processor
        .handle_progress_agent_event(AgentProgressEvent::ItemDelta {
            notification: progress_delta_notification(
                &harness,
                "thinking",
                ItemDeltaStream::Generic,
            ),
        })
        .await;

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdout_progress_delta_is_live_only_for_now() {
    let harness = setup_progress_delta_harness(
        "stdout",
        command_execution_item("item_progress_stdout"),
        false,
    )
    .await;

    harness
        .processor
        .handle_progress_agent_event(AgentProgressEvent::ItemDelta {
            notification: progress_delta_notification(&harness, "chunk\n", ItemDeltaStream::Stdout),
        })
        .await;

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_non_shell_tool_progress_delta_is_live_only() {
    let harness = setup_progress_delta_harness(
        "web_fetch",
        web_fetch_item("item_progress_web_fetch"),
        false,
    )
    .await;

    harness
        .processor
        .handle_progress_agent_event(AgentProgressEvent::ItemDelta {
            notification: progress_delta_notification(
                &harness,
                "RAW_FETCH_CONTENT_SHOULD_NOT_PERSIST",
                ItemDeltaStream::Generic,
            ),
        })
        .await;

    let payloads = turn_item_payloads(&harness).await;
    assert_no_persisted_item_delta(payloads.as_slice());
}

#[test]
fn fallback_title_from_first_user_text_truncates_to_six_words() {
    let title = super::fallback_title_from_first_user_text("один два три четыре пять шесть семь")
        .expect("fallback title should be generated");
    assert_eq!(title, "один два три четыре пять шесть...");
}

#[test]
fn fallback_title_from_first_user_text_keeps_short_message() {
    let title = super::fallback_title_from_first_user_text("какая сегодня погода")
        .expect("fallback title should be generated");
    assert_eq!(title, "какая сегодня погода");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_russian_first_message_generates_parent_title_successfully() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let counting_provider = Arc::new(CountingDelayedProvider::new(
        Duration::from_millis(10),
        "Русский заголовок",
    ));
    let mut summary_config = test_summary_config();
    summary_config.title_model_provider = Some("openai".to_owned());
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        counting_provider.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        summary_config,
        test_context_budget(),
        test_tool_loop_config(),
    ));

    let thread_id = "thr_title_russian_0001";
    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.clone(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let seed_turn = Turn {
        id: "turn_seed_russian_0000001".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    let long_russian = "Очень длинное русскоязычное сообщение ".repeat(120);
    crud_store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &seed_turn,
            &[UserInput::Text {
                text: long_russian.clone(),
                text_elements: Vec::new(),
            }],
        )
        .await
        .expect("seed turn should materialize");
    processor.spawn_initial_thread_title_task(thread_id.to_owned(), Some(long_russian));
    let thread =
        wait_for_thread_name_equals(crud_store.clone(), thread_id, "Русский заголовок").await;
    assert_eq!(thread.name.as_deref(), Some("Русский заголовок"));
    assert!(
        counting_provider.call_count() >= 1,
        "title provider should be called for long UTF-8 message"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_title_triggers_are_singleflight_per_thread() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let counting_provider = Arc::new(CountingDelayedProvider::new(
        Duration::from_millis(200),
        "Единый заголовок",
    ));
    let mut summary_config = test_summary_config();
    summary_config.title_model_provider = Some("openai".to_owned());
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        counting_provider.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        summary_config,
        test_context_budget(),
        test_tool_loop_config(),
    ));

    let thread_id = "thr_title_singleflight_0001";
    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.clone(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let seed_turn = Turn {
        id: "turn_seed_singleflight_01".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &seed_turn,
            &[UserInput::Text {
                text: "первый текст для заголовка".to_owned(),
                text_elements: Vec::new(),
            }],
        )
        .await
        .expect("seed turn should materialize");

    processor.spawn_initial_thread_title_task(
        thread_id.to_owned(),
        Some("первый текст для заголовка".to_owned()),
    );
    processor.spawn_initial_thread_title_task(
        thread_id.to_owned(),
        Some("первый текст для заголовка".to_owned()),
    );

    let thread =
        wait_for_thread_name_equals(crud_store.clone(), thread_id, "Единый заголовок").await;
    assert_eq!(
        counting_provider.call_count(),
        1,
        "singleflight should keep one active title generation job"
    );
    assert_eq!(thread.name.as_deref(), Some("Единый заголовок"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn title_generation_retries_after_transient_failure() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let flaky_provider = Arc::new(FlakyTitleProvider::new(1, "Надежный заголовок"));
    let mut summary_config = test_summary_config();
    summary_config.title_model_provider = Some("openai".to_owned());
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        flaky_provider.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        summary_config,
        test_context_budget(),
        test_tool_loop_config(),
    ));

    let thread_id = "thr_title_retry_0001";
    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.clone(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let seed_turn = Turn {
        id: "turn_seed_retry_000000001".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &seed_turn,
            &[UserInput::Text {
                text: "текст для ретрая заголовка".to_owned(),
                text_elements: Vec::new(),
            }],
        )
        .await
        .expect("seed turn should materialize");

    processor.spawn_initial_thread_title_task(
        thread_id.to_owned(),
        Some("текст для ретрая заголовка".to_owned()),
    );

    let thread =
        wait_for_thread_name_equals(crud_store.clone(), thread_id, "Надежный заголовок").await;
    assert_eq!(
        flaky_provider.call_count(),
        2,
        "title job should retry after one transient failure"
    );
    assert_eq!(thread.name.as_deref(), Some("Надежный заголовок"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_thread_scope_skips_auto_title_generation() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let counting_provider = Arc::new(CountingDelayedProvider::new(
        Duration::from_millis(10),
        "Не должен вызываться",
    ));
    let mut summary_config = test_summary_config();
    summary_config.title_model_provider = Some("openai".to_owned());
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        counting_provider.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        summary_config,
        test_context_budget(),
        test_tool_loop_config(),
    ));

    let child_title = "Child keeps parent name";
    let thread_id = "thr_title_child_scope_0001";
    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.clone(),
        id: thread_id.to_owned(),
        name: Some(child_title.to_owned()),
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::TaskRun,
        sidebar_visibility: ThreadSidebarVisibility::Hidden,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let seed_turn = Turn {
        id: "turn_seed_child_scope_00001".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(&thread, SandboxMode::FullAccess, &seed_turn, &[])
        .await
        .expect("child thread should be materialized");

    processor.spawn_initial_thread_title_task(
        thread_id.to_owned(),
        Some("входной текст дочернего треда".to_owned()),
    );
    sleep(Duration::from_millis(200)).await;
    let thread = crud_store
        .get_thread_by_id(thread_id)
        .await
        .expect("thread query should succeed")
        .expect("thread should exist");

    assert_eq!(
        counting_provider.call_count(),
        0,
        "child thread should never trigger auto-title provider call"
    );
    assert_eq!(thread.name.as_deref(), Some(child_title));
}

#[test]
fn user_message_payload_from_input_extracts_text_and_attachments() {
    let (text, attachments) = super::user_message_payload_from_input(&[
        UserInput::Text {
            text: "analyze this".to_owned(),
            text_elements: Vec::new(),
        },
        UserInput::File {
            url: "https://example.com/report.pdf".to_owned(),
        },
        UserInput::LocalFile {
            path: "/tmp/report.pdf".to_owned(),
        },
        UserInput::Audio {
            url: "https://example.com/note.mp3".to_owned(),
        },
        UserInput::LocalAudio {
            path: "/tmp/note.wav".to_owned(),
        },
        UserInput::Video {
            url: "https://example.com/demo.mp4".to_owned(),
        },
        UserInput::LocalVideo {
            path: "/tmp/demo.mov".to_owned(),
        },
    ])
    .expect("rendered user message payload");

    assert_eq!(text, "analyze this");
    assert_eq!(
        attachments,
        vec![
            UserMessageAttachment::File {
                url: "https://example.com/report.pdf".to_owned()
            },
            UserMessageAttachment::LocalFile {
                path: "/tmp/report.pdf".to_owned()
            },
            UserMessageAttachment::Audio {
                url: "https://example.com/note.mp3".to_owned()
            },
            UserMessageAttachment::LocalAudio {
                path: "/tmp/note.wav".to_owned()
            },
            UserMessageAttachment::Video {
                url: "https://example.com/demo.mp4".to_owned()
            },
            UserMessageAttachment::LocalVideo {
                path: "/tmp/demo.mov".to_owned()
            }
        ]
    );
}

#[test]
fn user_message_payload_from_input_keeps_attachment_only_message() {
    let (text, attachments) = super::user_message_payload_from_input(&[UserInput::LocalFile {
        path: "/tmp/report.pdf".to_owned(),
    }])
    .expect("attachment-only user message should still materialize");

    assert!(text.is_empty());
    assert_eq!(
        attachments,
        vec![UserMessageAttachment::LocalFile {
            path: "/tmp/report.pdf".to_owned()
        }]
    );
}

async fn start_thread_for_artifact_test(
    processor: &MessageProcessor,
    connection_id: u64,
    _rx: &mut mpsc::Receiver<Message>,
    workspace_id: &str,
    thread_id: &str,
) -> ThreadStartResponse {
    processor
        .thread_manager
        .thread_start(
            connection_id,
            workspace_id.to_owned(),
            ThreadStartParams {
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                name: None,
                model: None,
                model_provider: None,
                sandbox: None,
                mode: None,
                origin_kind: None,
                sidebar_visibility: None,
                agent_nickname: None,
                agent_role: None,
            },
        )
        .await
        .expect("thread/start should succeed")
        .response
}

async fn ingest_user_test_artifact(
    processor: &MessageProcessor,
    workspace_id: &str,
    display_name: &str,
) -> pioneer_protocol::ArtifactRef {
    processor
        .artifact_service
        .ingest_bytes(pioneer_artifacts::IngestArtifactBytesRequest {
            workspace_id: workspace_id.to_owned(),
            primary_thread_id: None,
            bytes: b"hello artifact".to_vec(),
            display_name: display_name.to_owned(),
            kind: pioneer_protocol::ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            created_by_kind: pioneer_protocol::ArtifactCreatedByKind::User,
            created_by_actor_id: Some("test-user".to_owned()),
            binding: None,
            metadata: Default::default(),
        })
        .await
        .expect("artifact ingest should succeed")
        .artifact
}

async fn materialize_artifact_api_thread(
    crud_store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) {
    let thread = pioneer_protocol::Thread {
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        mode: pioneer_protocol::ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at: 1,
        updated_at: 1,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
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
    crud_store
        .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[])
        .await
        .expect("artifact API test thread should materialize");
}

async fn ingest_bound_test_artifact(
    processor: &MessageProcessor,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    message_id: &str,
    display_name: &str,
    bytes: Vec<u8>,
) -> pioneer_protocol::ArtifactRef {
    processor
        .artifact_service
        .ingest_bytes(pioneer_artifacts::IngestArtifactBytesRequest {
            workspace_id: workspace_id.to_owned(),
            primary_thread_id: Some(thread_id.to_owned()),
            bytes,
            display_name: display_name.to_owned(),
            kind: pioneer_protocol::ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            created_by_kind: pioneer_protocol::ArtifactCreatedByKind::User,
            created_by_actor_id: Some("test-user".to_owned()),
            binding: Some(pioneer_artifacts::ArtifactBindingTarget {
                thread_id: Some(thread_id.to_owned()),
                turn_id: Some(turn_id.to_owned()),
                message_id: Some(message_id.to_owned()),
                turn_item_id: None,
                tool_call_id: None,
                task_id: None,
                task_run_id: None,
                binding_kind: pioneer_protocol::ArtifactBindingKind::UserInput,
                direction: pioneer_protocol::ArtifactBindingDirection::Input,
                role: Some(pioneer_protocol::ArtifactRole::User),
                item_index: Some(0),
            }),
            metadata: Default::default(),
        })
        .await
        .expect("bound artifact ingest should succeed")
        .artifact
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_with_artifact_input_materializes_user_message_attachment_and_binding() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let capture_provider = Arc::new(CaptureSummaryProvider::new("artifact answer"));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        capture_provider.clone(),
    ));
    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let artifact = ingest_user_test_artifact(&processor, workspace_id.as_str(), "report.txt").await;
    let thread = start_thread_for_artifact_test(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_artifact_user_msg",
    )
    .await;

    let turn_id = "turn_artifact_user_01";
    let request_id = generate_test_request_id("turnartifact", "input");
    let request = json!({
        "jsonrpc": "2.0",
        "id": request_id.clone(),
        "method": "turn/start",
        "params": {
            "thread_id": thread.thread.id,
            "turn_id": turn_id,
            "input": [
                { "type": "text", "text": "summarize" },
                {
                    "type": "artifact",
                    "artifactId": artifact.artifact_id.clone(),
                    "versionId": artifact.version_id.clone()
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let _response = recv_response_by_id(&mut rx, request_id.as_str()).await;
    let _turn_started = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;
    let mut user_message = None;
    for _ in 0..10 {
        let completed = recv_notification_by_method(&mut rx, events::ITEM_COMPLETED).await;
        let completed_payload: pioneer_protocol::ItemCompletedNotification =
            serde_json::from_value(completed.params.expect("item/completed params"))
                .expect("item/completed payload should decode");
        if let TurnItem::UserMessage {
            text, attachments, ..
        } = completed_payload.item
        {
            user_message = Some((text, attachments));
            break;
        }
    }
    let (text, attachments) = user_message.expect("expected user message item/completed");
    let _turn_completed = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;
    assert_eq!(text, "summarize");
    assert!(matches!(
        attachments.as_slice(),
        [UserMessageAttachment::Artifact { artifact: resolved }]
            if resolved.artifact_id == artifact.artifact_id
                && resolved.display_name == "report.txt"
    ));

    let summary = crud_store
        .get_artifact_summary(&workspace_id, &artifact.artifact_id, None)
        .await
        .expect("artifact summary query should succeed");
    let bindings = summary
        .bindings
        .iter()
        .filter(|binding| binding.binding_kind == pioneer_protocol::ArtifactBindingKind::UserInput)
        .collect::<Vec<_>>();
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].thread_id.as_deref(),
        Some(thread.thread.id.as_str())
    );
    assert_eq!(bindings[0].turn_id.as_deref(), Some(turn_id));
    assert_eq!(
        bindings[0].message_id.as_deref(),
        Some(format!("user_{turn_id}").as_str())
    );
    assert_eq!(bindings[0].item_index, Some(1));

    let requests = capture_provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let user_message = requests[0]
        .messages
        .last()
        .expect("provider request should include user message");
    assert_eq!(user_message.content, "summarize");
    assert!(matches!(
        user_message.content_parts.as_slice(),
        [pioneer_provider::MessageContentPart::File { file }]
            if file.name.as_deref() == Some("report.txt")
                && file.mime_type == "text/plain"
                && file.size_bytes == Some(14)
                && matches!(file.source, pioneer_provider::AttachmentDataSource::Path { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_rejects_artifact_from_another_workspace() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let other_workspace = workspace_manager
        .create_workspace("ws_artifact_other", Some("Artifact Other"))
        .await
        .expect("other workspace should be created");
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let artifact =
        ingest_user_test_artifact(&processor, other_workspace.id.as_str(), "foreign.txt").await;
    let thread = start_thread_for_artifact_test(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_artifact_foreign",
    )
    .await;

    let request_id = generate_test_request_id("turnartifact", "foreign");
    let request = json!({
        "jsonrpc": "2.0",
        "id": request_id.clone(),
        "method": "turn/start",
        "params": {
            "thread_id": thread.thread.id,
            "turn_id": "turn_artifact_foreign",
            "input": [
                {
                    "type": "artifact",
                    "artifactId": artifact.artifact_id.clone(),
                    "versionId": artifact.version_id.clone()
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let error = recv_error_by_id(&mut rx, request_id.as_str()).await;
    assert!(
        error
            .error
            .message
            .contains("failed to validate artifact input"),
        "unexpected error: {}",
        error.error.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_list_get_delete_restore_bind_api_roundtrip() {
    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let other_workspace = workspace_manager
        .create_workspace("ws_artifact_api_other", Some("Artifact API Other"))
        .await
        .expect("other workspace should be created");
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    materialize_artifact_api_thread(
        crud_store.as_ref(),
        workspace_id.as_str(),
        "thr_artifact_api",
        "turn_artifact_api_01",
    )
    .await;
    let artifact = ingest_bound_test_artifact(
        &processor,
        workspace_id.as_str(),
        "thr_artifact_api",
        "turn_artifact_api_01",
        "msg_artifact_api_01",
        "api.txt",
        b"hello artifact".to_vec(),
    )
    .await;
    ingest_bound_test_artifact(
        &processor,
        other_workspace.id.as_str(),
        "thr_artifact_api",
        "turn_artifact_api_other",
        "msg_artifact_api_other",
        "other.txt",
        b"other artifact".to_vec(),
    )
    .await;

    let list_id = generate_test_request_id("artifactlist", "thread");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": list_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_LIST_FOR_THREAD,
                "params": {
                    "workspace_id": workspace_id,
                    "thread_id": "thr_artifact_api"
                }
            })
            .to_string(),
        )
        .await;
    let list_response = recv_response_by_id(&mut rx, list_id.as_str()).await;
    let list: pioneer_protocol::ArtifactListResponse =
        serde_json::from_value(list_response.result).expect("artifact/list response should decode");
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].artifact.artifact_id, artifact.artifact_id);
    assert_eq!(list.items[0].bindings.len(), 1);
    assert_eq!(list.next_cursor, None);

    let get_id = generate_test_request_id("artifactget", "summary");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": get_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_GET,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id
                }
            })
            .to_string(),
        )
        .await;
    let get_response = recv_response_by_id(&mut rx, get_id.as_str()).await;
    let get: pioneer_protocol::ArtifactGetResponse =
        serde_json::from_value(get_response.result).expect("artifact/get response should decode");
    assert_eq!(get.artifact.bindings.len(), 1);

    let read_id = generate_test_request_id("artifactread", "range");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": read_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_READ,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id,
                    "offset": 6,
                    "max_bytes": 4
                }
            })
            .to_string(),
        )
        .await;
    let read_response = recv_response_by_id(&mut rx, read_id.as_str()).await;
    let read: pioneer_protocol::ArtifactReadResponse =
        serde_json::from_value(read_response.result).expect("artifact/read response should decode");
    assert_eq!(read.offset, 6);
    assert_eq!(read.len, 4);
    assert_eq!(read.content_base64, "YXJ0aQ==");
    assert!(read.truncated);

    let bind_id = generate_test_request_id("artifactbind", "second");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": bind_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_BIND,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id,
                    "thread_id": "thr_artifact_api",
                    "turn_id": "turn_artifact_api_02",
                    "message_id": "msg_artifact_api_02",
                    "binding_kind": "manual_attach",
                    "direction": "context",
                    "role": "user",
                    "item_index": 1
                }
            })
            .to_string(),
        )
        .await;
    let _bind_response = recv_response_by_id(&mut rx, bind_id.as_str()).await;
    let summary = crud_store
        .get_artifact_summary(&workspace_id, &artifact.artifact_id, None)
        .await
        .expect("artifact summary query should succeed");
    assert_eq!(summary.bindings.len(), 2);
    assert_eq!(
        crud_store
            .count_artifact_blobs_by_workspace(&workspace_id)
            .await
            .expect("artifact blob count should succeed"),
        1
    );

    let delete_id = generate_test_request_id("artifactdelete", "soft");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": delete_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_DELETE,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id
                }
            })
            .to_string(),
        )
        .await;
    let delete_response = recv_response_by_id(&mut rx, delete_id.as_str()).await;
    let delete: pioneer_protocol::ArtifactDeleteResponse =
        serde_json::from_value(delete_response.result)
            .expect("artifact/delete response should decode");
    assert_eq!(delete.status, pioneer_protocol::ArtifactStatus::Deleted);

    let list_deleted_id = generate_test_request_id("artifactlist", "deleted");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": list_deleted_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_LIST_FOR_THREAD,
                "params": {
                    "workspace_id": workspace_id,
                    "thread_id": "thr_artifact_api"
                }
            })
            .to_string(),
        )
        .await;
    let list_after_delete_response = recv_response_by_id(&mut rx, list_deleted_id.as_str()).await;
    let list_after_delete: pioneer_protocol::ArtifactListResponse =
        serde_json::from_value(list_after_delete_response.result)
            .expect("artifact/list response should decode");
    assert!(list_after_delete.items.is_empty());

    let restore_id = generate_test_request_id("artifactrestore", "ready");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": restore_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_RESTORE,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id
                }
            })
            .to_string(),
        )
        .await;
    let restore_response = recv_response_by_id(&mut rx, restore_id.as_str()).await;
    let restore: pioneer_protocol::ArtifactRestoreResponse =
        serde_json::from_value(restore_response.result)
            .expect("artifact/restore response should decode");
    assert_eq!(restore.status, pioneer_protocol::ArtifactStatus::Ready);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_read_caps_oversized_json_request() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let artifact = ingest_bound_test_artifact(
        &processor,
        workspace_id.as_str(),
        "thr_artifact_read_cap",
        "turn_artifact_read_cap",
        "msg_artifact_read_cap",
        "large.txt",
        vec![b'a'; 1024 * 1024 + 16],
    )
    .await;

    let read_id = generate_test_request_id("artifactread", "capped");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": read_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_READ,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id,
                    "max_bytes": 2 * 1024 * 1024
                }
            })
            .to_string(),
        )
        .await;
    let response = recv_response_by_id(&mut rx, read_id.as_str()).await;
    let read: pioneer_protocol::ArtifactReadResponse =
        serde_json::from_value(response.result).expect("artifact/read response should decode");
    assert_eq!(read.len, 1024 * 1024);
    assert!(read.truncated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_get_and_read_reject_cross_workspace_artifact() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let other_workspace = workspace_manager
        .create_workspace("ws_artifact_api_foreign", Some("Artifact API Foreign"))
        .await
        .expect("other workspace should be created");
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let artifact = ingest_bound_test_artifact(
        &processor,
        other_workspace.id.as_str(),
        "thr_foreign_artifact_api",
        "turn_foreign_artifact_api",
        "msg_foreign_artifact_api",
        "foreign.txt",
        b"foreign".to_vec(),
    )
    .await;

    let get_id = generate_test_request_id("artifactget", "foreign");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": get_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_GET,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id
                }
            })
            .to_string(),
        )
        .await;
    let get_error = recv_error_by_id(&mut rx, get_id.as_str()).await;
    assert!(get_error.error.message.contains("failed to get artifact"));

    let read_id = generate_test_request_id("artifactread", "foreign");
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": read_id,
                "method": pioneer_protocol::constants::methods::ARTIFACT_READ,
                "params": {
                    "workspace_id": workspace_id,
                    "artifact_id": artifact.artifact_id
                }
            })
            .to_string(),
        )
        .await;
    let read_error = recv_error_by_id(&mut rx, read_id.as_str()).await;
    assert!(read_error.error.message.contains("failed to read artifact"));
}

#[test]
fn force_fail_tool_item_marks_in_progress_tool_as_failed() {
    let item = pioneer_protocol::TurnItem::CommandExecution {
        id: "item_1".to_owned(),
        tool_name: "exec_command".to_owned(),
        arguments: json!({}),
        status: pioneer_protocol::ToolCallStatus::InProgress,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
        display: ToolDisplayPayload::Shell {
            stdout: None,
            stderr: None,
            aggregated_output: Some("partial output".to_owned()),
            exit_code: None,
            duration_ms: None,
            timed_out: None,
            truncated: false,
        },
        storage: ToolStoragePayload::Shell {
            stdout: None,
            stderr: None,
            aggregated_output: Some("partial output".to_owned()),
            exit_code: None,
            duration_ms: None,
            timed_out: None,
            truncated: false,
        },
        recovery: None,
        command: vec!["echo".to_owned(), "hello".to_owned()],
        cwd: None,
        success: None,
        outcome: None,
        observation: None,
    };
    let failed = MessageProcessor::force_fail_tool_item(item, "recovery attempts exhausted")
        .expect("tool item should be force-failed");
    match failed {
        pioneer_protocol::TurnItem::CommandExecution {
            status,
            success,
            display,
            ..
        } => {
            assert_eq!(status, pioneer_protocol::ToolCallStatus::Failed);
            assert_eq!(success, Some(false));
            let ToolDisplayPayload::Summary(summary) = display else {
                panic!("failed tool should contain summary display");
            };
            let text = summary.lines.join("\n");
            assert!(text.contains("partial output"));
            assert!(text.contains("recovery failed: recovery attempts exhausted"));
        }
        _ => panic!("unexpected turn item variant"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_task_agent_run_creates_child_thread_and_wait_returns_result() {
    let session_manager = Arc::new(SessionManager::new());
    let (tx, _rx) = mpsc::channel(8);
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let child_title = "Summarize child result";
    let response = create_task_for_test(
        &processor,
        test_task_create_params(
            workspace_id.as_str(),
            "thr_parent_task_test",
            "turn_parent_task_test",
            child_title,
            3,
        ),
    )
    .await
    .expect("task_create should start immediate child task");
    let run = response
        .run
        .clone()
        .expect("immediate task should create run");
    let lineage = wait_for_child_lineage_for_run(crud_store.clone(), run.id.as_str()).await;
    assert!(!lineage.child_turn_id.is_empty());
    assert_eq!(
        lineage.parent_turn_id.as_deref(),
        Some("turn_parent_task_test"),
        "immediate attached subagent should stay under the live parent turn"
    );

    let child_thread = crud_store
        .get_thread_model(lineage.child_thread_id.as_str())
        .await
        .expect("child thread query should succeed")
        .expect("child thread should be persisted");
    assert_eq!(
        child_thread.sidebar_visibility,
        ThreadSidebarVisibility::Hidden
    );
    assert_eq!(child_thread.name.as_deref(), Some(child_title));

    let tree_threads = processor
        .list_threads_snapshot_for_connection(workspace_id.as_str(), 100, connection_id)
        .await
        .expect("thread tree snapshot should load");
    assert!(
        tree_threads
            .iter()
            .all(|thread| thread.id != lineage.child_thread_id),
        "hidden child thread must not appear in sidebar thread tree"
    );

    let wait_response = wait_tasks_for_test(
        &processor,
        TaskWaitParams {
            task_ids: vec![response.task.id.clone()],
            run_ids: Vec::new(),
            timeout_ms: Some(5_000),
            return_completed: true,
            return_pending: true,
            ..Default::default()
        },
    )
    .await
    .expect("task_wait should succeed");
    assert!(
        !wait_response.completed.is_empty(),
        "child echo turn should complete the task"
    );
    let completed = &wait_response.completed[0];
    assert_eq!(
        completed.child_turn_id.as_deref(),
        Some(lineage.child_turn_id.as_str())
    );
    assert!(
        completed
            .run
            .as_ref()
            .and_then(|run| run.result.as_ref())
            .and_then(|result| result.summary.as_deref())
            .is_some(),
        "task_wait should return normalized run result"
    );

    let task_events = crud_store
        .get_task_events(response.task.id.as_str(), None)
        .await
        .expect("task events should load");
    let event_types = task_events
        .events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&events::TASK_CREATED));
    assert!(event_types.contains(&events::TASK_RUN_CREATED));
    assert!(event_types.contains(&events::TASK_RUN_STARTED));
    assert!(event_types.contains(&events::TASK_RUN_COMPLETED));
    assert!(event_types.contains(&events::TASK_COMPLETED));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hidden_task_agent_run_uses_preflight_before_child_main_prompt_compile() {
    let provider = Arc::new(PreflightCaptureProvider::new("hidden child completed"));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let response = create_task_for_test(
        &processor,
        test_task_create_params(
            workspace_id.as_str(),
            "thr_parent_preflight_task",
            "turn_parent_preflight_task",
            "Hidden preflight child",
            3,
        ),
    )
    .await
    .expect("task_create should start hidden child task");
    let run = response
        .run
        .clone()
        .expect("immediate task should create run");
    let lineage = wait_for_child_lineage_for_run(crud_store.clone(), run.id.as_str()).await;
    let child_thread = crud_store
        .get_thread_model(lineage.child_thread_id.as_str())
        .await
        .expect("child thread query should succeed")
        .expect("child thread should be persisted");
    assert_eq!(
        child_thread.sidebar_visibility,
        ThreadSidebarVisibility::Hidden
    );

    let wait_response = wait_tasks_for_test(
        &processor,
        TaskWaitParams {
            task_ids: vec![response.task.id.clone()],
            run_ids: Vec::new(),
            timeout_ms: Some(5_000),
            return_completed: true,
            return_pending: true,
            ..Default::default()
        },
    )
    .await
    .expect("task_wait should succeed");
    assert!(
        !wait_response.completed.is_empty(),
        "hidden child task should complete"
    );

    let requests = provider.snapshot_requests();
    let preflight_pos = requests
        .iter()
        .position(is_turn_preflight_request)
        .expect("hidden child task should call preflight");
    let child_main_pos = requests
        .iter()
        .position(is_child_task_main_request)
        .expect("hidden child task should call main provider after prompt compile");
    assert!(
        preflight_pos < child_main_pos,
        "hidden child task preflight must run before the main child provider request"
    );
    assert!(requests[preflight_pos].tools.is_none());
    assert!(requests[preflight_pos].tool_choice.is_none());
    assert!(requests[preflight_pos].compiled_prompt.is_none());
    assert!(requests[child_main_pos].compiled_prompt.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_hidden_task_run_uses_preflight_before_restored_child_main_prompt_compile() {
    let initial_provider = Arc::new(HangingChildProvider::new());
    let initial_provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        initial_provider.clone(),
    ));
    let initial_session_manager = Arc::new(SessionManager::new());
    let initial_thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let initial_processor = Arc::new(MessageProcessor::new(
        initial_thread_manager,
        initial_provider_registry,
        initial_session_manager,
        workspace_manager.clone(),
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    initial_processor.bind_task_bridge().await;

    let response = create_task_for_test(
        &initial_processor,
        test_task_create_params(
            workspace_id.as_str(),
            "thr_parent_recovered_preflight_task",
            "turn_parent_recovered_preflight_task",
            "Recovered hidden preflight child",
            3,
        ),
    )
    .await
    .expect("task_create should start hidden child task");
    let run = response
        .run
        .clone()
        .expect("immediate task should create run");
    let lineage = wait_for_child_lineage_for_run(crud_store.clone(), run.id.as_str()).await;
    for _ in 0..100 {
        if initial_provider.child_main_call_count() > 0 {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        initial_provider.child_main_call_count() > 0,
        "initial child task provider should be hanging in the main child request"
    );

    let execution = crud_store
        .load_execution_for_run(run.id.as_str())
        .await
        .expect("execution query should succeed")
        .expect("task run execution should exist");
    let stale_at = super::now_timestamp_secs().saturating_sub(120);
    crud_store
        .mark_execution_running(execution.id.as_str(), stale_at, Some(stale_at))
        .await
        .expect("execution lease should be made stale for startup recovery");

    let recovery_provider = Arc::new(PreflightCaptureProvider::new(
        "recovered hidden child completed",
    ));
    let recovery_provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        recovery_provider.clone(),
    ));
    let recovery_processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        recovery_provider_registry,
        Arc::new(SessionManager::new()),
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    recovery_processor.bind_task_bridge().await;
    recovery_processor
        .task_runtime
        .start()
        .await
        .expect("startup recovery should run");

    wait_for_task_status(
        crud_store.clone(),
        response.task.id.as_str(),
        pioneer_protocol::TaskStatus::Completed,
    )
    .await;
    let (_, child_turn) = crud_store
        .get_turn(
            lineage.child_thread_id.as_str(),
            lineage.child_turn_id.as_str(),
        )
        .await
        .expect("child turn lookup should succeed")
        .expect("child turn should exist");
    assert_eq!(child_turn.status, TurnStatus::Completed);

    let requests = recovery_provider.snapshot_requests();
    let preflight_pos = requests
        .iter()
        .position(is_turn_preflight_request)
        .expect("recovered hidden child task should call preflight");
    let child_main_pos = requests
        .iter()
        .position(is_child_task_main_request)
        .expect("recovered hidden child task should call main provider after prompt compile");
    assert!(
        preflight_pos < child_main_pos,
        "recovered hidden child task preflight must run before the restored main child provider request"
    );
    assert!(requests[preflight_pos].compiled_prompt.is_none());
    assert!(requests[child_main_pos].compiled_prompt.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_task_agent_run_creates_parent_visible_occurrence_turn() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let parent_thread_id = "thr_parent_scheduled_task";
    let creation_turn_id = "turn_parent_scheduled_task";
    let parent_thread = Thread {
        workspace_id: workspace_id.clone(),
        id: parent_thread_id.to_owned(),
        name: Some("Scheduled parent".to_owned()),
        preview: "schedule task".to_owned(),
        mode: ThreadMode::Agent,
        model: "test-model".to_owned(),
        model_provider: "openai".to_owned(),
        created_at,
        updated_at: created_at,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        turns: Vec::new(),
    };
    let creation_turn = Turn {
        id: creation_turn_id.to_owned(),
        status: TurnStatus::Completed,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &Turn {
                status: TurnStatus::InProgress,
                ..creation_turn.clone()
            },
            &[],
        )
        .await
        .expect("creation turn start should persist");
    crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: workspace_id.clone(),
                thread_id: parent_thread_id.to_owned(),
                turn: creation_turn,
            },
            created_at,
        )
        .await
        .expect("creation turn completion should persist");

    let due_at = created_at.saturating_add(1);
    let mut params = test_task_create_params(
        workspace_id.as_str(),
        parent_thread_id,
        creation_turn_id,
        "Scheduled child result",
        3,
    );
    params.trigger = TaskTriggerInput {
        spec: TaskTriggerSpec::ScheduledAt {
            scheduled_at: due_at,
            timezone: Some("UTC".to_owned()),
            catch_up_policy: None,
        },
    };
    params.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::KeepRunning,
        on_parent_failure: TaskParentTerminalAction::KeepRunning,
        completion: TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    if let Some(agent_spec) = params.agent_spec.as_mut() {
        agent_spec.prompt.instructions =
            vec!["Execute this scheduled test task and return a short result.".to_owned()];
        agent_spec.prompt.output_instructions =
            Some("Return one sentence confirming scheduled execution.".to_owned());
    }
    let response = create_task_for_test(&processor, params)
        .await
        .expect("scheduled task_create should succeed");

    processor
        .task_runtime
        .process_due_once(due_at)
        .await
        .expect("scheduled run should dispatch");

    wait_for_task_status(
        crud_store.clone(),
        response.task.id.as_str(),
        pioneer_protocol::TaskStatus::Completed,
    )
    .await;
    let task_response = crud_store
        .get_task(response.task.id.as_str())
        .await
        .expect("task query should succeed")
        .expect("task should exist");
    let run = task_response
        .runs
        .iter()
        .find(|run| run.trigger_id.is_some())
        .expect("scheduled run should exist");
    let lineage = wait_for_child_lineage_for_run(crud_store.clone(), run.id.as_str()).await;
    assert_eq!(lineage.parent_thread_id, parent_thread_id);
    assert_eq!(lineage.parent_turn_id.as_deref(), Some(run.id.as_str()));
    assert_ne!(lineage.parent_turn_id.as_deref(), Some(creation_turn_id));

    let (_, occurrence_turn) = crud_store
        .get_turn(parent_thread_id, run.id.as_str())
        .await
        .expect("occurrence turn lookup should succeed")
        .expect("occurrence turn should exist");
    assert_eq!(occurrence_turn.turn_kind, TurnKind::TaskRun);
    assert_eq!(occurrence_turn.origin, TurnOrigin::ScheduledTask);
    assert_eq!(occurrence_turn.status, TurnStatus::Completed);
    let occurrence_items = crud_store
        .get_turn_item_events(parent_thread_id, run.id.as_str())
        .await
        .expect("occurrence turn items should load")
        .expect("occurrence turn item stream should exist");
    assert!(
        occurrence_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemCompleted {
                item: TurnItem::Task { item },
                ..
            } if item.id == crate::task_tools::task_run_anchor_id(run.id.as_str())
                && item.task_id == response.task.id
                && item.run_id.as_deref() == Some(run.id.as_str())
        )),
        "occurrence turn should persist a parent-visible task anchor for desktop reload"
    );
    let progress_target = processor
        .task_progress_parent_target_for_test(
            &task_response,
            &TaskEventPayload::Progress {
                task_id: response.task.id.clone(),
                run_id: Some(run.id.clone()),
                message: "working".to_owned(),
                details: None,
            },
        )
        .await
        .expect("scheduled run progress should have a parent timeline target");
    assert_eq!(
        progress_target,
        (parent_thread_id.to_owned(), run.id.clone()),
        "live scheduled run progress must patch the occurrence turn, not the creation turn"
    );

    let (_, old_creation_turn) = crud_store
        .get_turn(parent_thread_id, creation_turn_id)
        .await
        .expect("creation turn lookup should succeed")
        .expect("creation turn should still exist");
    assert_eq!(old_creation_turn.turn_kind, TurnKind::Conversation);
    assert_eq!(old_creation_turn.status, TurnStatus::Completed);

    let creation_timeline = processor
        .compose_turn_timeline_for_test(TurnTimelineParams {
            thread_id: parent_thread_id.to_owned(),
            turn_id: creation_turn_id.to_owned(),
            compose_tasks: true,
            include_collapsed_task_events: true,
            max_child_items_per_task: None,
        })
        .await
        .expect("creation turn/timeline should compose")
        .expect("creation turn/timeline should exist");
    assert!(
        creation_timeline.items.iter().all(|item| {
            item.origin.kind != TimelineOriginKind::ChildTurn
                && item.origin.run_id.as_deref() != Some(run.id.as_str())
        }),
        "creation turn timeline must not compose future scheduled run activity"
    );

    let occurrence_timeline = processor
        .compose_turn_timeline_for_test(TurnTimelineParams {
            thread_id: parent_thread_id.to_owned(),
            turn_id: run.id.clone(),
            compose_tasks: true,
            include_collapsed_task_events: true,
            max_child_items_per_task: None,
        })
        .await
        .expect("occurrence turn/timeline should compose")
        .expect("occurrence turn/timeline should exist");
    assert!(
        occurrence_timeline
            .items
            .iter()
            .any(|item| item.origin.kind == TimelineOriginKind::TaskEvent
                && item.origin.run_id.as_deref() == Some(run.id.as_str())),
        "occurrence turn timeline should include the scheduled run lifecycle"
    );
    assert!(
        occurrence_timeline
            .items
            .iter()
            .any(|item| item.origin.kind == TimelineOriginKind::ChildTurn),
        "occurrence turn timeline should include scheduled child turn activity"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_event_listener_fans_out_notifications_from_committed_event_log() {
    let session_manager = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(8);
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    session_manager
        .set_connection_workspace(connection_id, Some(workspace_id.clone()))
        .await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.start_task_event_listener().await;

    let response = create_task_for_test(
        &processor,
        TaskCreateParams {
            workspace_id: workspace_id.clone(),
            owner_kind: TaskOwnerKind::Workspace,
            owner_id: Some(workspace_id.clone()),
            created_by_thread_id: None,
            created_by_turn_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::System,
            title: "Notify task".to_owned(),
            goal: "Emit committed notification".to_owned(),
            priority: 0,
            trigger: TaskTriggerInput {
                spec: TaskTriggerSpec::Manual {
                    allowed_actor: None,
                },
            },
            agent_spec: None,
            lifecycle_policy: None,
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: None,
        },
    )
    .await
    .expect("task should create");

    let notification = recv_notification_by_method(&mut rx, events::TASK_CREATED).await;
    let params = notification
        .params
        .expect("task created notification should have params");
    assert_eq!(params["task"]["id"], response.task.id);
    assert_eq!(params["context"]["taskId"], response.task.id);
    assert_eq!(params["context"]["sequence"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_agent_without_explicit_model_or_provider_is_rejected() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("global-default-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(EchoProvider::new()),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let mut missing_model = test_task_create_params(
        workspace_id.as_str(),
        "thr_parent_model_inherit",
        "turn_parent_model_inherit",
        "Reject missing model",
        3,
    );
    let agent_spec = missing_model
        .agent_spec
        .as_mut()
        .expect("test helper should create agent spec");
    agent_spec.model = None;

    let error = create_task_for_test(&processor, missing_model)
        .await
        .expect_err("task_create should reject empty agent model");
    assert!(
        format!("{error:#}").contains("agent executor requires agent_spec.model"),
        "unexpected error: {error:#}"
    );

    let mut missing_provider = test_task_create_params(
        workspace_id.as_str(),
        "thr_parent_model_inherit",
        "turn_parent_model_inherit",
        "Reject missing provider",
        3,
    );
    let agent_spec = missing_provider
        .agent_spec
        .as_mut()
        .expect("test helper should create agent spec");
    agent_spec.model_provider = None;

    let error = create_task_for_test(&processor, missing_provider)
        .await
        .expect_err("task_create should reject empty agent model provider");
    assert!(
        format!("{error:#}").contains("agent executor requires agent_spec.model_provider"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_depth_limit_rejects_subtask_creation() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let root = create_task_for_test(
        &processor,
        test_task_create_params(
            workspace_id.as_str(),
            "thr_depth_parent",
            "turn_depth_parent",
            "Root at max depth one",
            1,
        ),
    )
    .await
    .expect("root task at max depth one should be allowed");

    let mut child_params = test_task_create_params(
        workspace_id.as_str(),
        "thr_depth_parent",
        "turn_depth_child",
        "Forbidden child",
        1,
    );
    child_params.parent_task_id = Some(root.task.id.clone());
    let error = create_task_for_test(&processor, child_params)
        .await
        .expect_err("child task beyond max depth should fail");
    assert!(format!("{error:#}").contains("exceeds max depth"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_detach_updates_lifecycle_policy() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(DelayedProvider {
            delay: Duration::from_secs(10),
            text: "delayed result".to_owned(),
        }),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    let response = create_task_for_test(
        &processor,
        test_task_create_params(
            workspace_id.as_str(),
            "thr_guard_parent",
            "turn_guard_parent",
            "Long running child",
            3,
        ),
    )
    .await
    .expect("long-running child task should start");

    detach_task_for_test(
        &processor,
        pioneer_protocol::TaskDetachParams {
            task_id: response.task.id.clone(),
        },
    )
    .await
    .expect("task_detach should succeed");

    let detached = processor
        .crud_store
        .get_task(response.task.id.as_str())
        .await
        .expect("task query should succeed")
        .expect("task should exist");
    assert_eq!(
        detached
            .task
            .lifecycle_policy
            .as_ref()
            .expect("task should keep lifecycle policy")
            .attachment,
        TaskAttachmentMode::Detached
    );

    cancel_task_for_test(
        &processor,
        pioneer_protocol::TaskCancelParams {
            task_id: response.task.id,
            reason: Some("test cleanup".to_owned()),
            scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
        },
    )
    .await
    .expect("cleanup cancellation should succeed");
}

#[test]
fn agent_mode_materializes_task_tools_and_chat_mode_does_not() {
    run_gateway_message_test(
        "agent_mode_materializes_task_tools_and_chat_mode_does_not",
        || async {
            agent_mode_materializes_task_tools_and_chat_mode_does_not_impl().await;
        },
    );
}

async fn agent_mode_materializes_task_tools_and_chat_mode_does_not_impl() {
    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider = Arc::new(SequencedToolProvider::new(Vec::new(), ""));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "parent",
        provider.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    start_thread_and_turn(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_task_tools_agent",
        "turn_task_tools_agent",
        "Agent",
        "parent",
    )
    .await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;

    let agent_requests = provider.snapshot_requests();
    let first_agent_request = agent_requests
        .first()
        .expect("agent request should be captured");
    let agent_prompt = first_agent_request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(
        agent_prompt
            .full_system_text
            .contains("## Task Orchestration"),
        "agent mode should explain task orchestration when task tools are available"
    );
    let tool_names = first_agent_request
        .tools
        .as_ref()
        .expect("agent mode should expose tools")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    for expected in [
        "task_create",
        "task_wait",
        "task_cancel",
        "task_detach",
        "task_list",
        "task_get",
        "task_reschedule",
        "task_pause",
        "task_resume",
    ] {
        assert!(
            tool_names.contains(&expected),
            "agent mode should expose {expected}"
        );
    }

    let chat_provider = Arc::new(SequencedToolProvider::new(Vec::new(), ""));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "parent",
        chat_provider.clone(),
    ));
    let (tx_chat, mut rx_chat) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx_chat).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;
    start_thread_and_turn(
        &processor,
        connection_id,
        &mut rx_chat,
        workspace_id.as_str(),
        "thr_task_tools_chat",
        "turn_task_tools_chat",
        "Chat",
        "parent",
    )
    .await;
    let _ = recv_notification_by_method(&mut rx_chat, events::TURN_COMPLETED).await;
    let chat_requests = chat_provider.snapshot_requests();
    let first_chat_request = chat_requests
        .first()
        .expect("chat request should be captured");
    assert!(
        first_chat_request.tools.is_none(),
        "chat mode must not expose task tools by default"
    );
    if let Some(prompt) = first_chat_request.compiled_prompt.as_ref() {
        assert!(
            !prompt.full_system_text.contains("## Task Orchestration"),
            "chat mode without task tools should not advertise task orchestration"
        );
    }
}

#[test]
fn task_create_tool_persists_anchor_and_composed_timeline() {
    run_gateway_message_test(
        "task_create_tool_persists_anchor_and_composed_timeline",
        || async {
            task_create_tool_persists_anchor_and_composed_timeline_impl().await;
        },
    );
}

#[test]
fn task_create_tool_idempotency_key_deduplicates_parallel_mutations() {
    run_gateway_message_test(
        "task_create_tool_idempotency_key_deduplicates_parallel_mutations",
        || async {
            task_create_tool_idempotency_key_deduplicates_parallel_mutations_impl().await;
        },
    );
}

async fn task_create_tool_idempotency_key_deduplicates_parallel_mutations_impl() {
    let (tx, mut rx) = mpsc::channel(128);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "parent"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let child_title = "Idempotent delegated task";
    let task_arguments = json!({
        "title": child_title,
        "goal": "Return a short delegated result",
        "instructions": ["Return a concise final answer."],
        "idempotencyKey": "same-task-create-key"
    })
    .to_string();
    let provider = Arc::new(SequencedToolProvider::new(
        vec![
            ProviderToolCall {
                id: "call_task_create_idem_1".to_owned(),
                name: "task_create".to_owned(),
                arguments: task_arguments.clone(),
            },
            ProviderToolCall {
                id: "call_task_create_idem_2".to_owned(),
                name: "task_create".to_owned(),
                arguments: task_arguments,
            },
        ],
        "parent observed child result",
    ));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "parent",
        provider.clone(),
    ));
    provider_registry.insert("echo", Arc::new(EchoProvider::new()));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.start_resilience_workers().await;

    start_thread_and_turn(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_task_tool_idempotency",
        "turn_task_tool_idempotency",
        "Agent",
        "parent",
    )
    .await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;

    let turn_items = crud_store
        .get_turn_item_events("thr_task_tool_idempotency", "turn_task_tool_idempotency")
        .await
        .expect("turn items should load")
        .expect("turn should exist");
    let task_anchor_count = turn_items
        .events
        .iter()
        .filter(|event| {
            matches!(
                &event.payload,
                TurnItemEventPayload::ItemCompleted {
                    item: TurnItem::Task { item },
                    ..
                } if item.title == child_title
            )
        })
        .count();
    assert_eq!(
        task_anchor_count, 1,
        "same idempotency key in one provider round must create one durable task anchor"
    );
}

async fn task_create_tool_persists_anchor_and_composed_timeline_impl() {
    let (tx, mut rx) = mpsc::channel(128);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "parent"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let child_title = "Summarize from task tool";
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_task_create_1".to_owned(),
            name: "task_create".to_owned(),
            arguments: json!({
                "title": child_title,
                "goal": "Return a short delegated result",
                "instructions": ["Return a concise final answer."]
            })
            .to_string(),
        }],
        "parent observed child result",
    ));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "parent",
        provider.clone(),
    ));
    provider_registry.insert("echo", Arc::new(EchoProvider::new()));
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.start_resilience_workers().await;

    start_thread_and_turn(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_task_tool_anchor",
        "turn_task_tool_anchor",
        "Agent",
        "parent",
    )
    .await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;
    let requests = provider.snapshot_requests();
    assert!(
        requests.iter().any(|request| {
            request.messages.iter().any(|message| {
                message
                    .content
                    .contains("Attached task results are available")
            })
        }),
        "completed attached task result should be delivered to the next parent model round"
    );

    let turn_items = crud_store
        .get_turn_item_events("thr_task_tool_anchor", "turn_task_tool_anchor")
        .await
        .expect("turn items should load")
        .expect("turn should exist");
    let anchor_task_id = turn_items
        .events
        .iter()
        .find_map(|event| match &event.payload {
            TurnItemEventPayload::ItemCompleted {
                item: TurnItem::Task { item },
                ..
            } => Some(item.task_id.clone()),
            _ => None,
        });
    let anchor_task_id = anchor_task_id.expect("task_create tool should persist task anchor");
    wait_for_task_status(
        crud_store.clone(),
        anchor_task_id.as_str(),
        pioneer_protocol::TaskStatus::Completed,
    )
    .await;
    let anchor_status = wait_for_task_anchor_status(
        crud_store.clone(),
        "turn_task_tool_anchor",
        anchor_task_id.as_str(),
        pioneer_protocol::TaskStatus::Completed,
    )
    .await;
    assert_eq!(
        anchor_status,
        pioneer_protocol::TaskStatus::Completed,
        "task anchor read model should be refreshed from task lifecycle events"
    );
    let lineage = crud_store
        .list_thread_lineage_for_task(anchor_task_id.as_str())
        .await
        .expect("lineage query should succeed");
    assert!(
        !lineage.is_empty(),
        "agent task should create hidden child lineage"
    );
    let child_thread = crud_store
        .get_thread_model(lineage[0].child_thread_id.as_str())
        .await
        .expect("child thread lookup should succeed")
        .expect("child thread should exist");
    assert_eq!(
        child_thread.sidebar_visibility,
        ThreadSidebarVisibility::Hidden
    );
    assert_eq!(child_thread.name.as_deref(), Some(child_title));

    let timeline_request = json!({
        "jsonrpc": "2.0",
        "id": "turntimelinephase4001",
        "method": "turn/timeline",
        "params": {
            "threadId": "thr_task_tool_anchor",
            "turnId": "turn_task_tool_anchor",
            "composeTasks": true,
            "includeCollapsedTaskEvents": true
        }
    });
    processor
        .process_request(connection_id, &timeline_request.to_string())
        .await;
    let timeline_response = recv_response_by_id(&mut rx, "turntimelinephase4001").await;
    let timeline: TurnTimelineResponse = serde_json::from_value(timeline_response.result)
        .expect("turn/timeline response should decode");
    assert!(timeline.items.iter().any(|item| {
        item.origin.kind == TimelineOriginKind::ParentTurn
            && matches!(
                item.payload,
                pioneer_protocol::TimelinePayload::TurnItemEvent { .. }
            )
    }));
    assert!(
        timeline
            .items
            .iter()
            .any(|item| item.origin.kind == TimelineOriginKind::TaskEvent),
        "composed timeline should include task lifecycle events"
    );
    assert!(
        timeline
            .items
            .iter()
            .any(|item| item.origin.kind == TimelineOriginKind::ChildTurn),
        "composed timeline should include child turn events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_delivery_worker_uses_lineage_parent_turn_for_owner_thread() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("must connect to sqlite memory");
    Migrator::up(&connection, None)
        .await
        .expect("migrations must succeed");
    bootstrap(&connection)
        .await
        .expect("gateway bootstrap should create default workspace");

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let workspace_manager = Arc::new(WorkspaceManager::new(connection.clone()));
    let workspace_id = workspace_manager
        .list_workspaces()
        .await
        .expect("workspace/list should succeed")
        .into_iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .expect("default workspace should exist")
        .id;
    let crud_store = Arc::new(CrudStore::new(connection.clone()));
    let processor = MessageProcessor::with_agent_manager(
        thread_manager,
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        session_manager,
        workspace_manager,
        crud_store.clone(),
    );
    processor
        .task_runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;

    let owner_thread_id = "thr_delivery_owner";
    processor
        .process_request(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": "taskdeliverythread001",
                "method": "thread/start",
                "params": {
                    "thread_id": owner_thread_id,
                    "workspace_id": workspace_id,
                    "mode": "Agent"
                }
            })
            .to_string(),
        )
        .await;
    let _ = recv_response_by_id(&mut rx, "taskdeliverythread001").await;

    let response = processor
        .task_runtime
        .service()
        .create_task(
            pioneer_tasks::TaskCreateContext::default(),
            TaskCreateParams {
                workspace_id: workspace_id.clone(),
                owner_kind: TaskOwnerKind::Thread,
                owner_id: Some(owner_thread_id.to_owned()),
                created_by_thread_id: Some(owner_thread_id.to_owned()),
                created_by_turn_id: None,
                parent_task_id: None,
                executor_kind: TaskExecutorKind::System,
                title: "Scheduled delivery".to_owned(),
                goal: "Deliver a scheduled result".to_owned(),
                priority: 0,
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::ScheduledAt {
                        scheduled_at: 4_000_000_000,
                        timezone: Some("UTC".to_owned()),
                        catch_up_policy: None,
                    },
                },
                agent_spec: None,
                lifecycle_policy: None,
                delivery_policy: Some(TaskDeliveryPolicy {
                    mode: TaskDeliveryMode::OwnerThread,
                    thread_id: None,
                    webhook_url: None,
                    include_result: true,
                    format: TaskDeliveryFormat::Summary,
                }),
                retry_policy: None,
                timeout_policy: None,
                concurrency_policy: None,
                metadata: None,
            },
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();

    processor
        .task_runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("scheduled task should fire");

    let task_after_run = processor
        .crud_store
        .get_task(task_id.as_str())
        .await
        .expect("task should read")
        .expect("task should exist");
    let run = task_after_run
        .runs
        .last()
        .expect("scheduled run should exist")
        .clone();
    let parent_thread = processor
        .thread_manager
        .thread_get(owner_thread_id)
        .await
        .expect("owner thread should be loaded");
    let occurrence_turn = Turn {
        id: run.id.clone(),
        status: TurnStatus::Completed,
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        error: None,
        prompt_manifest: None,
    };
    processor
        .crud_store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &Turn {
                status: TurnStatus::InProgress,
                ..occurrence_turn.clone()
            },
            &[],
        )
        .await
        .expect("occurrence turn start should persist");
    processor
        .crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: workspace_id.clone(),
                thread_id: owner_thread_id.to_owned(),
                turn: occurrence_turn,
            },
            4_000_000_000,
        )
        .await
        .expect("occurrence turn completion should persist");
    connection
        .execute_unprepared(&format!(
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at) values ('child_delivery_thread_1', 'child_delivery_turn_1', '{owner_thread_id}', '{}', '{task_id}', '{}', '{owner_thread_id}', 0, '2096-10-02T07:06:40+00:00')",
            run.id, run.id
        ))
        .await
        .expect("lineage should persist");

    processor
        .process_due_task_deliveries(4_000_000_000, 10)
        .await
        .expect("delivery worker should run");

    let deliveries = processor
        .task_runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: workspace_id.clone(),
            task_id: Some(task_id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    let delivery = deliveries
        .deliveries
        .first()
        .expect("delivery should exist");
    assert_eq!(delivery.status, TaskDeliveryStatus::Delivered);
    let delivered_turn_id = delivery
        .delivered_turn_id
        .as_deref()
        .expect("owner thread delivery should record turn id");
    assert_eq!(
        delivered_turn_id,
        run.id.as_str(),
        "owner thread delivery must use thread_lineage.parent_turn_id instead of creating a delivery turn"
    );
    let items = crud_store
        .get_turn_item_events(owner_thread_id, delivered_turn_id)
        .await
        .expect("items should read")
        .expect("occurrence turn should exist");
    assert!(
        !items.events.iter().any(|event| {
            matches!(
                &event.payload,
                TurnItemEventPayload::ItemStarted {
                    item: TurnItem::Task { .. },
                    ..
                } | TurnItemEventPayload::ItemCompleted {
                    item: TurnItem::Task { .. },
                    ..
                }
            )
        }),
        "delivery turn should not duplicate the scheduled run task anchor"
    );
    assert!(items.events.iter().any(|event| {
        matches!(
            &event.payload,
            TurnItemEventPayload::ItemStarted {
                item: TurnItem::AgentMessage { text, .. },
                ..
            } if text == "delivered scheduled result\nfull detail"
        )
    }));
    assert!(items.events.iter().any(|event| {
        matches!(
            &event.payload,
            TurnItemEventPayload::ItemCompleted {
                item: TurnItem::AgentMessage { text, .. },
                ..
            } if text == "delivered scheduled result\nfull detail"
        )
    }));
    let owner_thread = crud_store
        .get_thread_model(owner_thread_id)
        .await
        .expect("owner thread should read")
        .expect("owner thread should still exist");
    assert_eq!(owner_thread.mode, ThreadMode::Agent);

    let deliveries_request = json!({
        "jsonrpc": "2.0",
        "id": "taskdeliveriesrpc0001",
        "method": "task/deliveries",
        "params": {
            "workspaceId": workspace_id,
            "taskId": task_id,
            "statuses": ["delivered"],
            "limit": 10
        }
    });
    processor
        .process_request(connection_id, &deliveries_request.to_string())
        .await;
    let deliveries_response = recv_response_by_id(&mut rx, "taskdeliveriesrpc0001").await;
    let deliveries_payload: TaskDeliveriesResponse =
        serde_json::from_value(deliveries_response.result)
            .expect("task/deliveries response should decode");
    assert_eq!(deliveries_payload.deliveries.len(), 1);
    assert_eq!(
        deliveries_payload.deliveries[0].status,
        TaskDeliveryStatus::Delivered
    );

    let agenda_request = json!({
        "jsonrpc": "2.0",
        "id": "taskagendarpc00000001",
        "method": "task/agenda",
        "params": {
            "workspaceId": workspace_id,
            "ownerKind": "thread",
            "ownerId": owner_thread_id,
            "includeCompleted": true,
            "limit": 10
        }
    });
    processor
        .process_request(connection_id, &agenda_request.to_string())
        .await;
    let agenda_response = recv_response_by_id(&mut rx, "taskagendarpc00000001").await;
    let agenda_payload: TaskAgendaResponse =
        serde_json::from_value(agenda_response.result).expect("task/agenda response should decode");
    let agenda_item = agenda_payload
        .items
        .iter()
        .find(|item| item.task.id == deliveries_payload.deliveries[0].task_id)
        .expect("agenda should include completed scheduled delivery when requested");
    assert_eq!(
        agenda_item
            .latest_delivery
            .as_ref()
            .map(|delivery| delivery.status),
        Some(TaskDeliveryStatus::Delivered)
    );
    assert_eq!(
        agenda_item.result_preview.as_deref(),
        Some("delivered scheduled result")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_agenda_pause_resume_json_rpc_contracts() {
    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));

    let task_id = processor
        .task_runtime
        .service()
        .create_task(
            pioneer_tasks::TaskCreateContext::default(),
            TaskCreateParams {
                workspace_id: workspace_id.clone(),
                owner_kind: TaskOwnerKind::Thread,
                owner_id: Some("thr_agenda_owner".to_owned()),
                created_by_thread_id: Some("thr_agenda_owner".to_owned()),
                created_by_turn_id: None,
                parent_task_id: None,
                executor_kind: TaskExecutorKind::System,
                title: "Agenda contract".to_owned(),
                goal: "Prove scheduled task read APIs".to_owned(),
                priority: 0,
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::ScheduledAt {
                        scheduled_at: 4_000_000_000,
                        timezone: Some("UTC".to_owned()),
                        catch_up_policy: None,
                    },
                },
                agent_spec: None,
                lifecycle_policy: None,
                delivery_policy: None,
                retry_policy: None,
                timeout_policy: None,
                concurrency_policy: None,
                metadata: None,
            },
        )
        .await
        .expect("scheduled task should create")
        .task
        .id;

    {
        let agenda_request = json!({
            "jsonrpc": "2.0",
            "id": "taskagendarpc00000002",
            "method": "task/agenda",
            "params": {
                "workspaceId": workspace_id.as_str(),
                "ownerKind": "thread",
                "ownerId": "thr_agenda_owner",
                "limit": 10
            }
        });
        processor
            .process_request(connection_id, &agenda_request.to_string())
            .await;
        let agenda_response = recv_response_by_id(&mut rx, "taskagendarpc00000002").await;
        let agenda: TaskAgendaResponse = serde_json::from_value(agenda_response.result)
            .expect("task/agenda response should decode");
        let agenda_item = agenda
            .items
            .iter()
            .find(|item| item.task.id == task_id)
            .expect("agenda should include active scheduled task");
        assert_eq!(agenda_item.next_fire_at, Some(4_000_000_000));
        assert_eq!(agenda_item.trigger_status, Some(TaskTriggerStatus::Active));
    }

    {
        let pause_request = json!({
            "jsonrpc": "2.0",
            "id": "taskpauserpc000000001",
            "method": "task/pause",
            "params": {
                "taskId": task_id.as_str(),
                "reason": "contract test"
            }
        });
        processor
            .process_request(connection_id, &pause_request.to_string())
            .await;
        let pause_response = recv_response_by_id(&mut rx, "taskpauserpc000000001").await;
        let paused: TaskPauseResponse = serde_json::from_value(pause_response.result)
            .expect("task/pause response should decode");
        assert_eq!(paused.task.id, task_id);
        assert!(
            paused
                .triggers
                .iter()
                .all(|trigger| trigger.status == TaskTriggerStatus::Paused)
        );
    }

    {
        let agenda_without_paused_request = json!({
            "jsonrpc": "2.0",
            "id": "taskagendarpc00000003",
            "method": "task/agenda",
            "params": {
                "workspaceId": workspace_id.as_str(),
                "ownerKind": "thread",
                "ownerId": "thr_agenda_owner",
                "limit": 10
            }
        });
        processor
            .process_request(connection_id, &agenda_without_paused_request.to_string())
            .await;
        let agenda_without_paused_response =
            recv_response_by_id(&mut rx, "taskagendarpc00000003").await;
        let agenda_without_paused: TaskAgendaResponse =
            serde_json::from_value(agenda_without_paused_response.result)
                .expect("task/agenda response should decode");
        assert!(
            agenda_without_paused
                .items
                .iter()
                .all(|item| item.task.id != task_id),
            "paused task should be hidden unless includePaused is requested"
        );
    }

    {
        let agenda_with_paused_request = json!({
            "jsonrpc": "2.0",
            "id": "taskagendarpc00000004",
            "method": "task/agenda",
            "params": {
                "workspaceId": workspace_id.as_str(),
                "ownerKind": "thread",
                "ownerId": "thr_agenda_owner",
                "includePaused": true,
                "limit": 10
            }
        });
        processor
            .process_request(connection_id, &agenda_with_paused_request.to_string())
            .await;
        let agenda_with_paused_response =
            recv_response_by_id(&mut rx, "taskagendarpc00000004").await;
        let agenda_with_paused: TaskAgendaResponse =
            serde_json::from_value(agenda_with_paused_response.result)
                .expect("task/agenda response should decode");
        assert!(agenda_with_paused.items.iter().any(|item| {
            item.task.id == task_id && item.trigger_status == Some(TaskTriggerStatus::Paused)
        }));
    }

    {
        let resume_request = json!({
            "jsonrpc": "2.0",
            "id": "taskresumerpc00000001",
            "method": "task/resume",
            "params": {
                "taskId": task_id.as_str(),
                "reason": "contract test"
            }
        });
        processor
            .process_request(connection_id, &resume_request.to_string())
            .await;
        let resume_response = recv_response_by_id(&mut rx, "taskresumerpc00000001").await;
        let resumed: TaskResumeResponse = serde_json::from_value(resume_response.result)
            .expect("task/resume response should decode");
        assert_eq!(resumed.task.id, task_id);
        assert!(
            resumed
                .triggers
                .iter()
                .all(|trigger| trigger.status == TaskTriggerStatus::Active)
        );
        assert!(
            resumed
                .triggers
                .iter()
                .all(|trigger| trigger.next_fire_at == Some(4_000_000_000))
        );
    }
}

#[test]
fn task_parent_turn_guard_forces_wait_cancel_or_detach_before_completion() {
    run_gateway_message_test("task-parent-turn-guard", || async {
        let (tx, mut rx) = mpsc::channel(128);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("test-model", "parent"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let provider = Arc::new(GuardAwareProvider::new());
        let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "parent",
            provider.clone(),
        ));
        provider_registry.insert(
            "delayed",
            Arc::new(DelayedProvider {
                delay: Duration::from_secs(10),
                text: "slow child".to_owned(),
            }),
        );
        let processor = Arc::new(MessageProcessor::new(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        ));
        processor.bind_task_bridge().await;

        start_thread_and_turn(
            &processor,
            connection_id,
            &mut rx,
            workspace_id.as_str(),
            "thr_task_guard_parent",
            "turn_task_guard_parent",
            "Agent",
            "parent",
        )
        .await;
        let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;

        let requests = provider.snapshot_requests();
        assert!(
            requests.len() >= 4,
            "parent guard should keep the same turn active for another model round"
        );
        assert!(
            requests.iter().any(|request| {
                request.messages.iter().any(|message| {
                    message
                        .content
                        .contains("Attached tasks created by this turn")
                })
            }),
            "guard observation should be model-visible"
        );

        let turn_items = crud_store
            .get_turn_item_events("thr_task_guard_parent", "turn_task_guard_parent")
            .await
            .expect("turn items should load")
            .expect("turn should exist");
        let task_id = turn_items
            .events
            .iter()
            .find_map(|event| match &event.payload {
                TurnItemEventPayload::ItemCompleted {
                    item: TurnItem::Task { item },
                    ..
                } => Some(item.task_id.clone()),
                _ => None,
            });
        let task_id = task_id.expect("task anchor should exist");
        let task = crud_store
            .get_task(task_id.as_str())
            .await
            .expect("task should load")
            .expect("task should exist");
        assert_eq!(
            task.task
                .lifecycle_policy
                .as_ref()
                .map(|policy| policy.attachment),
            Some(TaskAttachmentMode::Detached),
            "task_detach should unblock parent completion"
        );
        cancel_task_for_test(
            &processor,
            pioneer_protocol::TaskCancelParams {
                task_id,
                reason: Some("test cleanup".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("detached child cleanup should succeed");
    });
}

#[test]
fn parent_turn_cancel_cancels_attached_child_tasks_through_service() {
    run_gateway_message_test(
        "parent_turn_cancel_cancels_attached_child_tasks_through_service",
        || async {
            parent_turn_cancel_cancels_attached_child_tasks_through_service_impl().await;
        },
    );
}

async fn parent_turn_cancel_cancels_attached_child_tasks_through_service_impl() {
    let (tx, mut rx) = mpsc::channel(128);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "parent"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider = Arc::new(CreateThenHangProvider::new());
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "parent", provider,
    ));
    provider_registry.insert(
        "delayed",
        Arc::new(DelayedProvider {
            delay: Duration::from_secs(10),
            text: "slow child".to_owned(),
        }),
    );
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    ));
    processor.bind_task_bridge().await;

    start_thread_and_turn(
        &processor,
        connection_id,
        &mut rx,
        workspace_id.as_str(),
        "thr_task_cancel_parent",
        "turn_task_cancel_parent",
        "Agent",
        "parent",
    )
    .await;

    let task_id = wait_for_task_anchor(
        crud_store.clone(),
        "thr_task_cancel_parent",
        "turn_task_cancel_parent",
    )
    .await;
    processor
        .agent_manager
        .cancel_turn(
            "thr_task_cancel_parent",
            "turn_task_cancel_parent",
            "test parent cancellation",
        )
        .await
        .expect("parent cancel should dispatch");
    let _ = recv_notification_by_method(&mut rx, events::TURN_FAILED).await;

    let task = crud_store
        .get_task(task_id.as_str())
        .await
        .expect("task should load")
        .expect("task should exist");
    assert_eq!(task.task.status, pioneer_protocol::TaskStatus::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_start_returns_response_and_started_notification() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000001",
            "workspace_id": workspace_id,
            "model": "o3"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = rx.recv().await.expect("expected thread/start response");
    let notification = rx
        .recv()
        .await
        .expect("expected thread/started notification");

    assert!(matches!(notification, Message::Text(_)));

    let response_payload = match response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse = serde_json::from_value(rpc_response.result)
        .expect("thread/start response payload should decode");
    assert!(!thread_response.thread.workspace_id.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_started_notification_is_not_broadcast_to_foreign_connections() {
    let (tx_a, mut rx_a) = mpsc::channel(8);
    let (tx_b, mut rx_b) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_a = session_manager.register_connection(tx_a).await;
    let _connection_b = session_manager.register_connection(tx_b).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000002",
            "workspace_id": workspace_id
        }
    });

    processor
        .process_request(connection_a, &request.to_string())
        .await;

    let _response = rx_a.recv().await.expect("expected thread/start response");
    let _notification = rx_a
        .recv()
        .await
        .expect("expected thread/started notification");

    let foreign_message = timeout(Duration::from_millis(150), rx_b.recv()).await;
    assert!(
        foreign_message.is_err(),
        "foreign connection must not receive unrelated thread/started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_tree_hides_foreign_empty_draft_threads() {
    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_a = session_manager.register_connection(tx_a).await;
    let connection_b = session_manager.register_connection(tx_b).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let thread_id = "thr_000000000000000122";
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "startdraftthread00001",
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id
        }
    });

    processor
        .process_request(connection_a, &thread_start_request.to_string())
        .await;
    let _response = recv_response_by_id(&mut rx_a, "startdraftthread00001").await;
    let _notification = recv_notification_by_method(&mut rx_a, events::THREAD_STARTED).await;

    let tree_request_b = json!({
        "jsonrpc": "2.0",
        "id": "treerequestclientb001",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_b, &tree_request_b.to_string())
        .await;
    let tree_response_b = recv_response_by_id(&mut rx_b, "treerequestclientb001").await;
    let tree_b: ThreadTreeResponse =
        serde_json::from_value(tree_response_b.result).expect("thread/tree B result decode");
    assert!(
        tree_b.threads.iter().all(|thread| thread.id != thread_id),
        "foreign client must not see another connection's empty draft thread"
    );

    let tree_request_a = json!({
        "jsonrpc": "2.0",
        "id": "treerequestclienta001",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_a, &tree_request_a.to_string())
        .await;
    let tree_response_a = recv_response_by_id(&mut rx_a, "treerequestclienta001").await;
    let tree_a: ThreadTreeResponse =
        serde_json::from_value(tree_response_a.result).expect("thread/tree A result decode");
    assert!(
        tree_a.threads.iter().any(|thread| thread.id == thread_id),
        "own client should still see its local empty draft thread"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_closed_removes_empty_thread_started_by_connection() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let agent_manager = Arc::new(AgentManager::new(test_provider(), test_tool_loop_config()));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        thread_manager.clone(),
        agent_manager.clone(),
        session_manager,
        workspace_manager,
        crud_store,
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000003",
            "workspace_id": workspace_id
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = rx.recv().await.expect("expected thread/start response");
    let response_payload = match response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse = serde_json::from_value(rpc_response.result)
        .expect("thread/start response payload should decode");
    let thread_id = thread_response.thread.id;

    let _started_notification = rx.recv().await.expect("expected started notification");

    assert!(
        thread_manager.has_thread(&thread_id).await,
        "thread should be in memory before disconnect"
    );

    processor.connection_closed(connection_id).await;

    assert!(
        !thread_manager.has_thread(&thread_id).await,
        "empty thread should be removed when owner disconnects"
    );
    assert!(
        !agent_manager.has_thread(&thread_id).await,
        "empty thread should be removed from agent manager when owner disconnects"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn internal_llm_context_event_persists_without_websocket_notification() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let _connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let agent_manager = Arc::new(AgentManager::new(test_provider(), test_tool_loop_config()));
    let (workspace_manager, crud_store, _workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        thread_manager,
        agent_manager,
        session_manager,
        workspace_manager,
        crud_store.clone(),
    );

    processor
        .handle_durable_agent_event(AgentDurableEvent::TurnLlmContextAppended {
            thread_id: "thread_llm_ctx".to_owned(),
            turn_id: "turn_llm_ctx".to_owned(),
            item_id: "item_llm_ctx".to_owned(),
            attempt_id: Some("1".to_owned()),
            sequence: 7,
            source: "tool_result".to_owned(),
            tool_name: "read_file".to_owned(),
            payload: ToolResultView::Json {
                value: json!({
                    "output": "SECRET_LLM_CONTEXT_SENTINEL"
                }),
                truncated: false,
            },
            output_policy_snapshot: ToolOutputPolicySnapshot::for_tool_name("read_file"),
        })
        .await;

    let rows = crud_store
        .list_turn_llm_context("turn_llm_ctx")
        .await
        .expect("llm context rows should be readable");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].payload.contains("SECRET_LLM_CONTEXT_SENTINEL"));

    let unexpected = timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(
        unexpected.is_err(),
        "internal llm context event must not be forwarded to websocket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_durable_item_completed_persists_before_committed_notification() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let agent_manager = Arc::new(AgentManager::new(test_provider(), test_tool_loop_config()));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        thread_manager.clone(),
        agent_manager.clone(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
    );

    let thread_id = "thr_direct_durable_01";
    let turn_id = "turn_direct_durable_01";
    let item_id = "item_direct_durable01";

    let thread_start = thread_manager
        .system_thread_start_seeded(
            workspace_id.clone(),
            pioneer_protocol::ThreadStartParams {
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.clone(),
                name: None,
                model: None,
                model_provider: None,
                sandbox: Some(SandboxMode::FullAccess),
                mode: None,
                origin_kind: None,
                sidebar_visibility: None,
                agent_nickname: None,
                agent_role: None,
            },
            None,
            None,
        )
        .await
        .expect("thread should start");

    crud_store
        .materialize_turn_start(
            &thread_start.started_notification.thread,
            SandboxMode::FullAccess,
            &Turn {
                id: turn_id.to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: Default::default(),
                origin: Default::default(),
                error: None,
                prompt_manifest: None,
            },
            &[],
        )
        .await
        .expect("turn start should persist");
    agent_manager
        .ensure_thread(thread_id, workspace_id.as_str())
        .await
        .expect("agent thread should be registered for committed subscription");
    let mut committed_rx = agent_manager
        .subscribe_committed(thread_id)
        .await
        .expect("committed subscription should exist");

    let notification = ItemCompletedNotification {
        workspace_id,
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: "durably completed".to_owned(),
            markdown: None,
            markdown_version: None,
        },
    };

    processor
        .handle_durable_agent_event(AgentDurableEvent::ItemCompleted {
            notification: notification.clone(),
        })
        .await;

    let committed = timeout(Duration::from_secs(1), committed_rx.recv())
        .await
        .expect("committed notification should arrive")
        .expect("committed lane should stay open");
    assert!(matches!(
        committed,
        AgentDurableEvent::ItemCompleted { notification: committed_notification }
            if committed_notification.turn_id == turn_id
                && committed_notification.item.item_id() == item_id
    ));

    let item_events = crud_store
        .get_turn_item_events(thread_id, turn_id)
        .await
        .expect("turn item events should be readable")
        .expect("turn item events should exist");
    assert!(
        item_events.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemCompleted {
                item: TurnItem::AgentMessage { id, text, .. },
                ..
            } if id == item_id && text == "durably completed"
        )),
        "committed durable event must already be persisted in the read model"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_durable_item_completed_does_not_commit_when_persistence_fails() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let agent_manager = Arc::new(AgentManager::new(test_provider(), test_tool_loop_config()));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        thread_manager,
        agent_manager.clone(),
        session_manager,
        workspace_manager,
        crud_store,
    );

    let thread_id = "thr_missing_durable_1";
    agent_manager
        .ensure_thread(thread_id, workspace_id.as_str())
        .await
        .expect("agent thread should be registered for committed subscription");
    let mut committed_rx = agent_manager
        .subscribe_committed(thread_id)
        .await
        .expect("committed subscription should exist");

    processor
        .handle_durable_agent_event(AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id,
                thread_id: thread_id.to_owned(),
                turn_id: "turn_missing_durable1".to_owned(),
                item: TurnItem::CommandExecution {
                    id: "item_missing_durable1".to_owned(),
                    tool_name: "grep_files".to_owned(),
                    arguments: json!({}),
                    status: pioneer_protocol::ToolCallStatus::InProgress,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
                    display: ToolDisplayPayload::Progress {
                        stage: "running".to_owned(),
                        metadata: pioneer_protocol::ToolMetadata::default(),
                    },
                    storage: ToolStoragePayload::Metadata {
                        metadata: pioneer_protocol::ToolMetadata::default(),
                    },
                    recovery: None,
                    command: Vec::new(),
                    cwd: None,
                    success: None,
                    outcome: None,
                    observation: None,
                },
            },
        })
        .await;

    assert!(
        timeout(Duration::from_millis(100), committed_rx.recv())
            .await
            .is_err(),
        "failed durable persistence must not publish a committed notification"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_closed_keeps_active_turn_running_without_subscribers() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let agent_manager = Arc::new(AgentManager::new(test_provider(), test_tool_loop_config()));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        thread_manager.clone(),
        agent_manager.clone(),
        session_manager,
        workspace_manager,
        crud_store,
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000013",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;

    let _thread_start_response = rx.recv().await.expect("expected thread/start response");
    let _thread_started_notification = rx
        .recv()
        .await
        .expect("expected thread/started notification");

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000013",
            "turn_id": "turn_000000000000000013",
            "input": [
                {
                    "type": "text",
                    "text": "continue after disconnect"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let _turn_start_response = rx.recv().await.expect("expected turn/start response");
    let _turn_started_notification = rx.recv().await.expect("expected turn/started notification");

    processor.connection_closed(connection_id).await;

    assert!(
        thread_manager.has_thread("thr_000000000000000013").await,
        "thread with active turn must stay loaded after disconnect"
    );
    assert!(
        agent_manager.has_thread("thr_000000000000000013").await,
        "agent runtime must keep running active turn after disconnect"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_unsubscribe_returns_status_and_closed_notification() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager.clone(),
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000004",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &start_request.to_string())
        .await;

    let start_response = rx.recv().await.expect("expected thread/start response");
    let start_response_payload = match start_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let start_rpc_response: JsonRpcResponse =
        serde_json::from_str(&start_response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse = serde_json::from_value(start_rpc_response.result)
        .expect("thread/start response payload should decode");
    let thread_id = thread_response.thread.id;
    let workspace_id = thread_response.thread.workspace_id;
    let _started_notification = rx.recv().await.expect("expected started notification");

    let unsubscribe_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "thread/unsubscribe",
        "params": {
            "threadId": thread_id
        }
    });
    processor
        .process_request(connection_id, &unsubscribe_request.to_string())
        .await;

    let unsubscribe_response = rx.recv().await.expect("expected unsubscribe response");
    let closed_notification = rx
        .recv()
        .await
        .expect("expected thread/closed notification");

    let unsubscribe_response_payload = match unsubscribe_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let unsubscribe_rpc_response: JsonRpcResponse =
        serde_json::from_str(&unsubscribe_response_payload).expect("response should decode");
    let unsubscribe_result: ThreadUnsubscribeResponse =
        serde_json::from_value(unsubscribe_rpc_response.result)
            .expect("unsubscribe response payload should decode");
    assert_eq!(
        unsubscribe_result.status,
        ThreadUnsubscribeStatus::Unsubscribed
    );

    let closed_notification_payload = match closed_notification {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text notification, got {other:?}"),
    };
    let notification: JsonRpcNotification =
        serde_json::from_str(&closed_notification_payload).expect("notification should decode");
    assert_eq!(notification.method, "thread/closed");
    let params = notification
        .params
        .expect("thread/closed notification params must be present");
    let closed: ThreadClosedNotification =
        serde_json::from_value(params).expect("thread/closed params should decode");

    assert_eq!(closed.workspace_id, workspace_id);
    assert!(!thread_manager.has_thread(&closed.thread_id).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_start_rejects_unknown_workspace_id() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000005",
            "workspace_id": "ws_missing_0000000001"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = rx.recv().await.expect("expected error response");
    let payload = match response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let value: serde_json::Value = serde_json::from_str(&payload).expect("json must decode");
    assert!(
        value.get("error").is_some(),
        "response must contain json-rpc error object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_start_rejects_missing_thread_id() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "workspace_id": workspace_id
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = rx.recv().await.expect("expected error response");
    let payload = match response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let value: serde_json::Value = serde_json::from_str(&payload).expect("json must decode");
    let message = value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();

    assert!(
        message.contains("thread_id"),
        "error should mention missing thread_id, got: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_start_rejects_missing_workspace_id() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000099"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = rx.recv().await.expect("expected error response");
    let payload = match response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let value: serde_json::Value = serde_json::from_str(&payload).expect("json must decode");
    let message = value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();

    assert!(
        message.contains("workspace_id"),
        "error should mention missing workspace_id, got: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_list_returns_existing_workspaces() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/list",
        "params": {}
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response).expect("workspace/list response should decode");
    let list: WorkspaceListResponse =
        serde_json::from_value(rpc_response.result).expect("workspace/list result decode");
    assert!(!list.workspaces.is_empty());
    assert!(list.workspaces.iter().any(|workspace| workspace.is_active));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_default_returns_single_active_workspace() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/default",
        "params": {}
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response).expect("workspace/default response should decode");
    let ensured: WorkspaceDefaultResponse =
        serde_json::from_value(rpc_response.result).expect("workspace/default result decode");
    assert!(!ensured.workspace.id.trim().is_empty());
    assert!(ensured.workspace.is_active);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_create_creates_new_workspace() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/create",
        "params": {
            "workspace_id": "ws_000000000000000111",
            "name": "Integration Workspace"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response).expect("workspace/create response should decode");
    let created: WorkspaceCreateResponse =
        serde_json::from_value(rpc_response.result).expect("workspace/create result decode");
    assert_eq!(created.workspace.id, "ws_000000000000000111");
    assert_eq!(created.workspace.name, "Integration Workspace");
    assert!(created.workspace.is_active);
    assert_eq!(created.workspace.id.len(), 21);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_create_rejects_missing_workspace_id() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/create",
        "params": {
            "name": "Missing Id"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let payload: serde_json::Value =
        serde_json::from_str(&response).expect("workspace/create error should decode");
    let message = payload
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    assert!(
        message.contains("workspace_id"),
        "error should mention missing workspace_id, got: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_select_returns_workspace_and_sets_session_scope() {
    let (processor, session_manager, connection_id, mut rx, workspace_manager, _) =
        setup_workspace_message_processor().await;
    let selected = workspace_manager
        .create_workspace("ws_000000000000000112", Some("Selected Workspace"))
        .await
        .expect("workspace create should succeed");

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/select",
        "params": {
            "workspace_id": selected.id,
            "make_current": true
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response).expect("workspace/select response should decode");
    let selected_response: WorkspaceSelectResponse =
        serde_json::from_value(rpc_response.result).expect("workspace/select result decode");
    assert_eq!(selected_response.workspace.id, "ws_000000000000000112");
    assert!(selected_response.workspace.is_current);
    assert_eq!(
        session_manager.connection_workspace_id(connection_id).await,
        Some("ws_000000000000000112".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_update_returns_updated_workspace_without_switching_session() {
    let (
        processor,
        session_manager,
        connection_id,
        mut rx,
        workspace_manager,
        current_workspace_id,
    ) = setup_workspace_message_processor().await;
    session_manager
        .set_connection_workspace(connection_id, Some(current_workspace_id.clone()))
        .await;
    let updated = workspace_manager
        .create_workspace("ws_000000000000000113", Some("Before Rename"))
        .await
        .expect("workspace create should succeed");

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/update",
        "params": {
            "workspace_id": updated.id,
            "name": "  After Rename  "
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let rpc_response: JsonRpcResponse =
        serde_json::from_str(&response).expect("workspace/update response should decode");
    let updated_response: WorkspaceUpdateResponse =
        serde_json::from_value(rpc_response.result).expect("workspace/update result decode");
    assert_eq!(updated_response.workspace.id, "ws_000000000000000113");
    assert_eq!(updated_response.workspace.name, "After Rename");
    assert_eq!(
        session_manager.connection_workspace_id(connection_id).await,
        Some(current_workspace_id)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_select_rejects_malformed_params_without_mutating_session_scope() {
    let (processor, session_manager, connection_id, mut rx, _, current_workspace_id) =
        setup_workspace_message_processor().await;
    session_manager
        .set_connection_workspace(connection_id, Some(current_workspace_id.clone()))
        .await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "workspace/select",
        "params": {
            "make_current": true
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_text(&mut rx).await;
    let payload: serde_json::Value =
        serde_json::from_str(&response).expect("workspace/select error should decode");
    assert_eq!(
        payload
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(serde_json::Value::as_i64),
        Some(pioneer_protocol::INVALID_PARAMS_CODE as i64)
    );
    assert_eq!(
        session_manager.connection_workspace_id(connection_id).await,
        Some(current_workspace_id)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_create_broadcasts_changed_to_other_workspace_connections() {
    let (tx_a, mut rx_a) = mpsc::channel(8);
    let (tx_b, mut rx_b) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_a = session_manager.register_connection(tx_a).await;
    let connection_b = session_manager.register_connection(tx_b).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, current_workspace_id) = setup_workspace_manager().await;
    session_manager
        .set_connection_workspace(connection_b, Some(current_workspace_id))
        .await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "workspacecreate000001",
        "method": "workspace/create",
        "params": {
            "workspace_id": "ws_000000000000000114",
            "name": "Broadcast Workspace"
        }
    });

    processor
        .process_request(connection_a, &request.to_string())
        .await;

    let (_response, notification_a) = recv_response_and_notification_by_id_method(
        &mut rx_a,
        "workspacecreate000001",
        events::WORKSPACE_CHANGED,
    )
    .await;
    let notification_b = recv_notification_by_method(&mut rx_b, events::WORKSPACE_CHANGED).await;

    for notification in [notification_a, notification_b] {
        let changed: WorkspaceChangedNotification =
            serde_json::from_value(notification.params.expect("workspace/changed params"))
                .expect("workspace/changed payload decodes");
        assert_eq!(changed.kind, WorkspaceChangeKind::Created);
        assert_eq!(changed.workspace.id, "ws_000000000000000114");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_update_broadcasts_updated_notification() {
    let (processor, _session_manager, connection_id, mut rx, workspace_manager, _) =
        setup_workspace_message_processor().await;
    let workspace = workspace_manager
        .create_workspace("ws_000000000000000115", Some("Before"))
        .await
        .expect("workspace create should succeed");

    let request = json!({
        "jsonrpc": "2.0",
        "id": "workspaceupdate000001",
        "method": "workspace/update",
        "params": {
            "workspace_id": workspace.id,
            "name": "After"
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let (_response, notification) = recv_response_and_notification_by_id_method(
        &mut rx,
        "workspaceupdate000001",
        events::WORKSPACE_CHANGED,
    )
    .await;
    let changed: WorkspaceChangedNotification =
        serde_json::from_value(notification.params.expect("workspace/changed params"))
            .expect("workspace/changed payload decodes");
    assert_eq!(changed.kind, WorkspaceChangeKind::Updated);
    assert_eq!(changed.workspace.name, "After");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_select_broadcasts_current_change_only_when_current_changes() {
    let (processor, _session_manager, connection_id, mut rx, workspace_manager, _) =
        setup_workspace_message_processor().await;
    let workspace = workspace_manager
        .create_workspace("ws_000000000000000116", Some("Selectable"))
        .await
        .expect("workspace create should succeed");

    let request = json!({
        "jsonrpc": "2.0",
        "id": "workspaceselect000001",
        "method": "workspace/select",
        "params": {
            "workspace_id": workspace.id,
            "make_current": true
        }
    });

    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let (_response, notification) = recv_response_and_notification_by_id_method(
        &mut rx,
        "workspaceselect000001",
        events::WORKSPACE_CHANGED,
    )
    .await;
    let changed: WorkspaceChangedNotification =
        serde_json::from_value(notification.params.expect("workspace/changed params"))
            .expect("workspace/changed payload decodes");
    assert_eq!(changed.kind, WorkspaceChangeKind::CurrentChanged);
    assert_eq!(changed.workspace.id, "ws_000000000000000116");

    let repeat = json!({
        "jsonrpc": "2.0",
        "id": "workspaceselect000002",
        "method": "workspace/select",
        "params": {
            "workspace_id": "ws_000000000000000116",
            "make_current": true
        }
    });

    processor
        .process_request(connection_id, &repeat.to_string())
        .await;

    let _response = recv_response_by_id(&mut rx, "workspaceselect000002").await;
    assert!(
        timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
        "repeat selection of already-current workspace must not emit workspace/changed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_returns_response_and_started_notification() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager.clone(),
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000010",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;

    let thread_start_response = rx.recv().await.expect("expected thread/start response");
    let thread_start_response_payload = match thread_start_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let thread_start_rpc_response: JsonRpcResponse =
        serde_json::from_str(&thread_start_response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse =
        serde_json::from_value(thread_start_rpc_response.result)
            .expect("thread/start response payload should decode");
    let _thread_started_notification = rx
        .recv()
        .await
        .expect("expected thread/started notification");

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": thread_response.thread.id,
            "turn_id": "turn_000000000000000003",
            "input": [
                {
                    "type": "text",
                    "text": "Hello"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let turn_response = rx.recv().await.expect("expected turn/start response");
    let turn_started_notification = rx.recv().await.expect("expected turn/started notification");

    let turn_response_payload = match turn_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let turn_rpc_response: JsonRpcResponse =
        serde_json::from_str(&turn_response_payload).expect("response should decode");
    let turn_result: TurnStartResponse = serde_json::from_value(turn_rpc_response.result)
        .expect("turn/start response payload should decode");
    assert_eq!(
        turn_result.turn.status,
        pioneer_protocol::TurnStatus::InProgress
    );

    let turn_notification_payload = match turn_started_notification {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text notification, got {other:?}"),
    };
    let notification: JsonRpcNotification =
        serde_json::from_str(&turn_notification_payload).expect("notification should decode");
    assert_eq!(notification.method, "turn/started");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_succeeds_when_skill_roots_are_missing() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = vec!["/tmp/pioneer-skills-missing-system".to_owned()];
    tool_loop_config.skills.user_roots = vec!["/tmp/pioneer-skills-missing-user".to_owned()];
    tool_loop_config.skills.registry_roots =
        vec!["/tmp/pioneer-skills-missing-registry".to_owned()];

    let processor = MessageProcessor::new(
        thread_manager.clone(),
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000012",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;

    let thread_start_response = rx.recv().await.expect("expected thread/start response");
    let thread_start_response_payload = match thread_start_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let thread_start_rpc_response: JsonRpcResponse =
        serde_json::from_str(&thread_start_response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse =
        serde_json::from_value(thread_start_rpc_response.result)
            .expect("thread/start response payload should decode");
    let _thread_started_notification = rx
        .recv()
        .await
        .expect("expected thread/started notification");

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": thread_response.thread.id,
            "turn_id": "turn_000000000000000006",
            "input": [
                {
                    "type": "text",
                    "text": "No skills should still work"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let turn_response = rx.recv().await.expect("expected turn/start response");
    let _turn_started_notification = rx.recv().await.expect("expected turn/started notification");

    let turn_response_payload = match turn_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let turn_rpc_response: JsonRpcResponse =
        serde_json::from_str(&turn_response_payload).expect("response should decode");
    let turn_result: TurnStartResponse = serde_json::from_value(turn_rpc_response.result)
        .expect("turn/start response payload should decode");
    assert_eq!(turn_result.turn.status, TurnStatus::InProgress);
}

async fn start_agents_doc_prompt_test_thread(
    processor: &MessageProcessor,
    connection_id: crate::session::ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    workspace_id: &str,
    thread_id: &str,
) {
    let request_id = generate_test_request_id("agdocpthrd", thread_id);
    let request = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id,
            "name": "AGENTS.md prompt test"
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;
    let _ = recv_response_by_id(rx, request_id.as_str()).await;
    let _ = recv_notification_by_method(rx, events::THREAD_STARTED).await;
}

async fn start_agents_doc_prompt_test_turn(
    processor: &MessageProcessor,
    connection_id: crate::session::ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    thread_id: &str,
    turn_id: &str,
) {
    let request_id = generate_test_request_id("agdocpturn", turn_id);
    let request = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "turn/start",
        "params": {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "mode": "Agent",
            "model": "test-model",
            "model_provider": "capture-summary",
            "input": [
                {
                    "type": "text",
                    "text": "Use the prompt context"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;
    let _ = recv_response_by_id(rx, request_id.as_str()).await;
    let _ = wait_for_thread_manager_turn_status(
        &processor.thread_manager,
        thread_id,
        turn_id,
        TurnStatus::Completed,
    )
    .await;
}

#[test]
fn agents_doc_turn_without_doc_omits_agents_md_prompt_section() {
    run_thread_agents_doc_rpc_test(|| async {
        let (tx, mut rx) = mpsc::channel(64);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let capture_provider = Arc::new(CaptureSummaryProvider::new("ok"));
        let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "capture-summary",
            capture_provider.clone(),
        ));
        let processor = MessageProcessor::new(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        );

        let thread_id = "thr_agents_doc_prompt0";
        let turn_id = "turn_agents_doc_prompt0";
        start_agents_doc_prompt_test_thread(
            &processor,
            connection_id,
            &mut rx,
            workspace_id.as_str(),
            thread_id,
        )
        .await;
        start_agents_doc_prompt_test_turn(&processor, connection_id, &mut rx, thread_id, turn_id)
            .await;

        let requests = capture_provider.snapshot_requests();
        let prompt = requests
            .last()
            .and_then(|request| request.compiled_prompt.as_ref())
            .expect("provider request should include compiled prompt");
        assert!(!prompt.dynamic_system_text.contains("## AGENTS.md"));

        let (_, turn) = crud_store
            .get_turn(thread_id, turn_id)
            .await
            .expect("turn should load")
            .expect("turn should exist");
        let manifest = turn
            .prompt_manifest
            .expect("prompt manifest should be persisted");
        assert!(
            !manifest
                .section_ids
                .iter()
                .any(|section_id| section_id == "agents_md"),
            "prompt manifest should not include agents_md without an effective doc"
        );
        assert!(
            !manifest
                .hook_sources
                .iter()
                .any(|source| source.section_id.as_deref() == Some("agents_md")),
            "prompt manifest should not include AGENTS.md hook source without an effective doc"
        );
    });
}

fn agents_doc_prompt_hook_source<'a>(
    manifest: &'a PromptManifest,
    doc: &pioneer_crud::ThreadAgentsDocRecord,
) -> &'a PromptManifestHookSourceEntry {
    let contribution_id = format!("thread_agents_doc.{}.v{}", doc.id, doc.version);
    manifest
        .hook_sources
        .iter()
        .find(|source| {
            source.section_id.as_deref() == Some("agents_md")
                && source.source.hook_id == "pioneer.thread_agents_doc_prompt"
                && source.source.subscription_id
                    == "pioneer.thread_agents_doc_prompt.turn_pre_prompt_compile"
                && source.source.phase == PromptManifestHookPhase::TurnPrePromptCompile
                && source.source.contribution_id.as_deref() == Some(contribution_id.as_str())
                && source.contribution_kind == PromptManifestHookContributionKind::PromptSection
        })
        .expect("AGENTS.md prompt hook source should be persisted")
}

#[test]
fn agents_doc_turn_prompt_uses_root_inheritance_and_folder_override() {
    run_thread_agents_doc_rpc_test(|| async {
        let (tx, mut rx) = mpsc::channel(96);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let capture_provider = Arc::new(CaptureSummaryProvider::new("ok"));
        let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "capture-summary",
            capture_provider.clone(),
        ));
        let processor = MessageProcessor::new(
            thread_manager,
            provider_registry,
            session_manager,
            workspace_manager,
            crud_store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        );

        let thread_id = "thr_agents_doc_prompt1";
        start_agents_doc_prompt_test_thread(
            &processor,
            connection_id,
            &mut rx,
            workspace_id.as_str(),
            thread_id,
        )
        .await;

        let root_doc = crud_store
            .save_thread_agents_doc(
                workspace_id.as_str(),
                None,
                "Root only instruction",
                None,
                None,
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("root AGENTS.md should save");

        start_agents_doc_prompt_test_turn(
            &processor,
            connection_id,
            &mut rx,
            thread_id,
            "turn_agents_doc_prompt1",
        )
        .await;
        let root_prompt = capture_provider
            .snapshot_requests()
            .last()
            .and_then(|request| request.compiled_prompt.as_ref())
            .expect("root provider request should include compiled prompt")
            .dynamic_system_text
            .clone();
        assert!(root_prompt.contains("## AGENTS.md"));
        assert!(root_prompt.contains("effective AGENTS.md for this thread tree scope"));
        assert!(root_prompt.contains("Root only instruction"));

        let (_, root_turn) = crud_store
            .get_turn(thread_id, "turn_agents_doc_prompt1")
            .await
            .expect("root turn should load")
            .expect("root turn should exist");
        let root_manifest = root_turn
            .prompt_manifest
            .expect("root prompt manifest should be persisted");
        let root_source = agents_doc_prompt_hook_source(&root_manifest, &root_doc);
        assert_eq!(root_source.source_count, Some(1));
        assert_eq!(root_source.truncation, PromptManifestHookTruncation::None);
        assert!(root_source.source.contribution_hash.is_some());

        let child_folder = crud_store
            .create_thread_folder(workspace_id.as_str(), None, "Child")
            .await
            .expect("child folder should be created");
        crud_store
            .move_thread_to_folder(
                workspace_id.as_str(),
                thread_id,
                Some(child_folder.id.as_str()),
            )
            .await
            .expect("thread should move to child folder");

        start_agents_doc_prompt_test_turn(
            &processor,
            connection_id,
            &mut rx,
            thread_id,
            "turn_agents_doc_prompt2",
        )
        .await;
        let inherited_prompt = capture_provider
            .snapshot_requests()
            .last()
            .and_then(|request| request.compiled_prompt.as_ref())
            .expect("inherited provider request should include compiled prompt")
            .dynamic_system_text
            .clone();
        assert!(inherited_prompt.contains("Root only instruction"));
        assert!(inherited_prompt.contains("explicit user messages take precedence"));

        let folder_doc = crud_store
            .save_thread_agents_doc(
                workspace_id.as_str(),
                Some(child_folder.id.as_str()),
                "Folder only instruction",
                None,
                None,
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("folder AGENTS.md should save");

        start_agents_doc_prompt_test_turn(
            &processor,
            connection_id,
            &mut rx,
            thread_id,
            "turn_agents_doc_prompt3",
        )
        .await;
        let folder_prompt = capture_provider
            .snapshot_requests()
            .last()
            .and_then(|request| request.compiled_prompt.as_ref())
            .expect("folder provider request should include compiled prompt")
            .dynamic_system_text
            .clone();
        assert!(folder_prompt.contains("effective AGENTS.md for this thread tree scope"));
        assert!(folder_prompt.contains("Folder only instruction"));
        assert!(!folder_prompt.contains("Root only instruction"));

        let (_, folder_turn) = crud_store
            .get_turn(thread_id, "turn_agents_doc_prompt3")
            .await
            .expect("turn should load")
            .expect("turn should exist");
        let manifest = folder_turn
            .prompt_manifest
            .expect("prompt manifest should be persisted");
        assert!(
            manifest
                .section_ids
                .iter()
                .any(|section_id| section_id == "agents_md"),
            "prompt manifest should include agents_md when an effective doc exists"
        );
        let source = agents_doc_prompt_hook_source(&manifest, &folder_doc);
        assert_eq!(source.source_count, Some(1));
        assert_eq!(source.truncation, PromptManifestHookTruncation::None);

        let turn_get_request = json!({
            "jsonrpc": "2.0",
            "id": "getagentsdocprompt003",
            "method": "turn/get",
            "params": {
                "thread_id": thread_id,
                "turn_id": "turn_agents_doc_prompt3"
            }
        });
        processor
            .process_request(connection_id, &turn_get_request.to_string())
            .await;
        let response = recv_response_by_id(&mut rx, "getagentsdocprompt003").await;
        let turn_get: TurnGetResponse =
            serde_json::from_value(response.result).expect("turn/get result should decode");
        let returned_manifest = turn_get
            .turn
            .prompt_manifest
            .expect("turn/get should return prompt manifest");
        assert_eq!(returned_manifest.hook_sources, manifest.hook_sources);

        let updated_folder_doc = crud_store
            .save_thread_agents_doc(
                workspace_id.as_str(),
                Some(child_folder.id.as_str()),
                "Folder instruction after turn",
                Some(folder_doc.version),
                None,
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("folder AGENTS.md should update");
        assert_ne!(updated_folder_doc.version, folder_doc.version);

        let (_, folder_turn_after_doc_change) = crud_store
            .get_turn(thread_id, "turn_agents_doc_prompt3")
            .await
            .expect("turn should load after doc change")
            .expect("turn should exist after doc change");
        let manifest_after_doc_change = folder_turn_after_doc_change
            .prompt_manifest
            .expect("prompt manifest should remain persisted after doc change");
        let source_after_doc_change =
            agents_doc_prompt_hook_source(&manifest_after_doc_change, &folder_doc);
        assert_eq!(source_after_doc_change.source_count, Some(1));
        let updated_contribution_id = format!(
            "thread_agents_doc.{}.v{}",
            updated_folder_doc.id, updated_folder_doc.version
        );
        assert_ne!(
            source_after_doc_change.source.contribution_id.as_deref(),
            Some(updated_contribution_id.as_str())
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_skill_resolution_event_persists_turn_skill_bindings() {
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _workspace_id) = setup_workspace_manager().await;
    let session_manager = Arc::new(SessionManager::new());

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let turn_id = "turn_000000000000000020";
    processor
        .handle_durable_agent_event(AgentDurableEvent::TurnSkillsResolved {
            thread_id: "thr_000000000000000020".to_owned(),
            turn_id: turn_id.to_owned(),
            bindings: vec![TurnSkillBinding {
                skill_slug: "pioneer/my-skill".to_owned(),
                skill_version: Some("1.2.3".to_owned()),
                fingerprint: "fp-my-skill".to_owned(),
                source_kind: "user".to_owned(),
                resolved_reason: "explicit_mention".to_owned(),
            }],
        })
        .await;

    let bindings = crud_store
        .find_turn_skill_bindings(turn_id)
        .await
        .expect("must read persisted turn skill bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].skill_slug, "pioneer/my-skill");
    assert_eq!(bindings[0].resolved_reason, "explicit_mention");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_skill_audit_event_persists_audit_rows() {
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, _workspace_id) = setup_workspace_manager().await;
    let session_manager = Arc::new(SessionManager::new());

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let turn_id = "turn_000000000000000021";
    processor
        .handle_durable_agent_event(AgentDurableEvent::SkillAuditEvents {
            thread_id: "thr_000000000000000021".to_owned(),
            turn_id: turn_id.to_owned(),
            events: vec![
                ProtocolSkillAuditEvent {
                    skill_slug: "pioneer/my-skill".to_owned(),
                    source_kind: "user".to_owned(),
                    action: "resolve_allowed".to_owned(),
                    decision: "allowed".to_owned(),
                    reason_code: None,
                    details: json!({"resolved_reason":"explicit_mention"}),
                    created_at_unix: 1_700_000_000,
                },
                ProtocolSkillAuditEvent {
                    skill_slug: "pioneer/my-skill".to_owned(),
                    source_kind: "user".to_owned(),
                    action: "runtime_blocked".to_owned(),
                    decision: "blocked".to_owned(),
                    reason_code: Some("runtime.dependency_missing".to_owned()),
                    details: json!({
                        "dependency_diagnostics": [
                            {
                                "kind": "bin",
                                "name": "node",
                                "status": "missing",
                                "hint": "Install node and ensure it is available in PATH."
                            }
                        ]
                    }),
                    created_at_unix: 1_700_000_100,
                },
            ],
        })
        .await;

    let events = crud_store
        .list_turn_skill_audit_event_records(turn_id)
        .await
        .expect("must load persisted audit rows");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].skill_slug, "pioneer/my-skill");
    assert_eq!(events[1].action, "runtime_blocked");

    let snapshots = crud_store
        .list_turn_skill_dependency_snapshot_records(turn_id)
        .await
        .expect("must load dependency snapshots");
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].diagnostics_json.contains("\"name\":\"node\""));
}

#[test]
fn phase_11_prompt_manifest_hook_sources_roundtrip_existing_event() {
    run_gateway_message_test("prompt-manifest-hook-sources", || async {
        let (tx, mut rx) = mpsc::channel(8);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let processor = MessageProcessor::new(
            thread_manager.clone(),
            test_provider(),
            session_manager,
            workspace_manager,
            crud_store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        );

        let thread_id = "thr_000000000000000090";
        let turn_id = "turn_000000000000000090";

        let thread_start_request = json!({
            "jsonrpc": "2.0",
            "id": "aaaaaaaaaaaaaaaaaaaaa",
            "method": "thread/start",
            "params": {
                "thread_id": thread_id,
                "workspace_id": workspace_id
            }
        });
        processor
            .process_request(connection_id, &thread_start_request.to_string())
            .await;
        let _ = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
        let _ = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

        let turn_start_request = json!({
            "jsonrpc": "2.0",
            "id": "bbbbbbbbbbbbbbbbbbbbb",
            "method": "turn/start",
            "params": {
                "thread_id": thread_id,
                "turn_id": turn_id,
                "input": [{"type": "text", "text": "manifest me"}]
            }
        });
        processor
            .process_request(connection_id, &turn_start_request.to_string())
            .await;
        let _ = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
        let _ = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;

        let manifest = PromptManifest {
            compiler_version: "0.1.0-test".to_owned(),
            profile: PromptManifestProfile::AssistantFull,
            section_ids: vec![
                "identity_base".to_owned(),
                "assistant_safety".to_owned(),
                "soul_core".to_owned(),
                "identity_core".to_owned(),
                "user_persona".to_owned(),
            ],
            fingerprint_stable: "stable-fp".to_owned(),
            fingerprint_dynamic: "dynamic-fp".to_owned(),
            fingerprint_full: "full-fp".to_owned(),
            diagnostics: vec![PromptManifestDiagnostic {
                code: PromptManifestDiagnosticCode::MissingFile,
                message: "bootstrap file `SOUL.md` is missing".to_owned(),
                file: Some("/tmp/SOUL.md".to_owned()),
                section_id: None,
                hook_source: Some(PromptManifestHookSource {
                    hook_id: "test.prompt_manifest_hook".to_owned(),
                    subscription_id: "test.prompt_manifest_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:gatewaydiagnostic".to_owned()),
                }),
            }],
            hook_sources: vec![PromptManifestHookSourceEntry {
                source: PromptManifestHookSource {
                    hook_id: "test.prompt_manifest_hook".to_owned(),
                    subscription_id: "test.prompt_manifest_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:gatewaysource".to_owned()),
                },
                section_id: Some("identity_base".to_owned()),
                contribution_kind: PromptManifestHookContributionKind::PromptSection,
                priority: Some(10),
                source_count: Some(1),
                truncation: PromptManifestHookTruncation::None,
            }],
        };

        processor
            .handle_durable_agent_event(AgentDurableEvent::PromptManifestCompiled {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                manifest: manifest.clone(),
            })
            .await;

        let (_, in_memory_turn) = thread_manager
            .turn_get(thread_id, turn_id)
            .await
            .expect("turn should exist in thread manager");
        assert_eq!(in_memory_turn.prompt_manifest, Some(manifest.clone()));

        let (_, persisted_turn) = crud_store
            .get_turn(thread_id, turn_id)
            .await
            .expect("turn/get from crud should succeed")
            .expect("turn must be persisted");
        assert_eq!(persisted_turn.prompt_manifest, Some(manifest.clone()));

        let turn_get_request = json!({
            "jsonrpc": "2.0",
            "id": "ccccccccccccccccccccc",
            "method": "turn/get",
            "params": {
                "thread_id": thread_id,
                "turn_id": turn_id
            }
        });
        processor
            .process_request(connection_id, &turn_get_request.to_string())
            .await;
        let response = recv_response_by_id(&mut rx, "ccccccccccccccccccccc").await;
        let turn_get: TurnGetResponse =
            serde_json::from_value(response.result).expect("turn/get result should decode");

        assert_eq!(turn_get.turn.prompt_manifest, Some(manifest));
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_materializes_thread_and_turn_state() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("must connect to sqlite memory");
    Migrator::up(&connection, None)
        .await
        .expect("migrations must succeed");
    bootstrap(&connection)
        .await
        .expect("gateway bootstrap should create default workspace");

    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let workspace_manager = Arc::new(WorkspaceManager::new(connection.clone()));
    let workspace_id = workspace_manager
        .list_workspaces()
        .await
        .expect("workspace/list should succeed")
        .into_iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .expect("default workspace should exist")
        .id;
    let crud_store = Arc::new(CrudStore::new(connection.clone()));
    let processor = MessageProcessor::with_agent_manager(
        thread_manager,
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        session_manager,
        workspace_manager,
        crud_store,
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000011",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;

    let thread_start_response = rx.recv().await.expect("expected thread/start response");
    let thread_start_response_payload = match thread_start_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let thread_start_rpc_response: JsonRpcResponse =
        serde_json::from_str(&thread_start_response_payload).expect("response should decode");
    let thread_response: ThreadStartResponse =
        serde_json::from_value(thread_start_rpc_response.result)
            .expect("thread/start response payload should decode");
    let _thread_started_notification = rx
        .recv()
        .await
        .expect("expected thread/started notification");

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": thread_response.thread.id,
            "turn_id": "turn_000000000000000004",
            "input": [
                {
                    "type": "text",
                    "text": "Persist me"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let turn_response = rx.recv().await.expect("expected turn/start response");
    let _turn_started_notification = rx.recv().await.expect("expected turn/started notification");

    let turn_response_payload = match turn_response {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text response, got {other:?}"),
    };
    let turn_rpc_response: JsonRpcResponse =
        serde_json::from_str(&turn_response_payload).expect("response should decode");
    let turn_result: TurnStartResponse = serde_json::from_value(turn_rpc_response.result)
        .expect("turn/start response payload should decode");

    let stored_thread = thread::Entity::find_by_id(thread_response.thread.id.clone())
        .one(&connection)
        .await
        .expect("thread query should succeed")
        .expect("thread row must exist");
    assert_eq!(stored_thread.id, thread_response.thread.id);
    assert_eq!(stored_thread.preview, "Persist me");

    let stored_turn = turn::Entity::find_by_id(turn_result.turn.id.clone())
        .one(&connection)
        .await
        .expect("turn query should succeed")
        .expect("turn row must exist");
    assert_eq!(stored_turn.thread_id, thread_response.thread.id);
    assert!(matches!(
        stored_turn.status.as_str(),
        "in_progress" | "completed" | "failed" | "interrupted"
    ));

    let stored_sandbox_policy =
        thread_sandox_policy::Entity::find_by_id(thread_response.thread.id.clone())
            .one(&connection)
            .await
            .expect("thread_sandox_policy query should succeed")
            .expect("thread_sandox_policy row must exist");
    assert_eq!(stored_sandbox_policy.mode, "full_access");

    let stored_input = turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.eq(turn_result.turn.id.clone()))
        .one(&connection)
        .await
        .expect("turn_input query should succeed")
        .expect("turn_input row must exist");
    assert_eq!(stored_input.id.len(), 21);
    assert!(
        stored_input
            .id
            .chars()
            .all(|value| value.is_ascii_alphanumeric())
    );
    assert_eq!(stored_input.input_type, "text");
    assert_eq!(stored_input.text.as_deref(), Some("Persist me"));

    let turn_id = turn_result.turn.id.clone();

    let status_history = turn_status_history::Entity::find()
        .filter(turn_status_history::Column::TurnId.eq(turn_id.clone()))
        .one(&connection)
        .await
        .expect("turn_status_history query should succeed")
        .expect("turn_status_history row must exist");
    assert_eq!(status_history.id.len(), 21);
    assert!(
        status_history
            .id
            .chars()
            .all(|value| value.is_ascii_alphanumeric())
    );
    assert!(matches!(
        status_history.status.as_str(),
        "in_progress" | "completed" | "failed" | "interrupted"
    ));

    let event_row = connection
        .query_one(
            &Query::select()
                .columns([Alias::new("event_type"), Alias::new("sequence")])
                .from(Alias::new("turn_event"))
                .and_where(Expr::col(Alias::new("turn_id")).eq(turn_id))
                .to_owned(),
        )
        .await
        .expect("turn_event query should succeed")
        .expect("turn_event row must exist");
    let event_type = event_row
        .try_get::<String>("", "event_type")
        .expect("event_type must be present");
    let sequence = event_row
        .try_get::<i64>("", "sequence")
        .expect("sequence must be present");
    assert_eq!(event_type, "turn/started");
    assert_eq!(sequence, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_get_returns_turn_snapshot() {
    let (tx, mut rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000021",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
    let _ = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000021",
            "turn_id": "turn_000000000000000021",
            "input": [{"type": "text", "text": "hello"}]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;

    let turn_get_request = json!({
        "jsonrpc": "2.0",
        "id": "ccccccccccccccccccccc",
        "method": "turn/get",
        "params": {
            "thread_id": "thr_000000000000000021",
            "turn_id": "turn_000000000000000021"
        }
    });
    processor
        .process_request(connection_id, &turn_get_request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, "ccccccccccccccccccccc").await;
    let turn_get: TurnGetResponse =
        serde_json::from_value(response.result).expect("turn/get result should decode");
    assert_eq!(turn_get.thread_id, "thr_000000000000000021");
    assert!(!turn_get.workspace_id.is_empty());
    assert_eq!(turn_get.turn.id, "turn_000000000000000021");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_cancel_interrupts_running_turn_and_is_idempotent() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "delayed"));
    let thread_manager_for_assert = thread_manager.clone();
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "delayed",
        Arc::new(DelayedProvider {
            delay: Duration::from_secs(30),
            text: "too late".to_owned(),
        }),
    ));
    let crud_store_for_assert = crud_store.clone();
    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_request_id = generate_test_request_id("turncancel", "thread");
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": thread_request_id,
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000022",
            "workspace_id": workspace_id,
            "model": "test-model",
            "model_provider": "delayed"
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, thread_request_id.as_str()).await;

    let turn_start_request_id = generate_test_request_id("turncancel", "start");
    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": turn_start_request_id,
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000022",
            "turn_id": "turn_000000000000000022",
            "mode": "Chat",
            "model": "test-model",
            "model_provider": "delayed",
            "input": [{"type": "text", "text": "hello"}]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, turn_start_request_id.as_str()).await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;
    wait_for_thread_manager_turn_status(
        thread_manager_for_assert.as_ref(),
        "thr_000000000000000022",
        "turn_000000000000000022",
        TurnStatus::InProgress,
    )
    .await;

    let turn_cancel_request_id = generate_test_request_id("turncancel", "stop1");
    let turn_cancel_request = json!({
        "jsonrpc": "2.0",
        "id": turn_cancel_request_id,
        "method": "turn/cancel",
        "params": {
            "thread_id": "thr_000000000000000022",
            "turn_id": "turn_000000000000000022",
            "reason": "user clicked stop"
        }
    });
    processor
        .process_request(connection_id, &turn_cancel_request.to_string())
        .await;

    let (cancel_response, failed_notification) = recv_response_and_notification_by_id_method(
        &mut rx,
        turn_cancel_request_id.as_str(),
        events::TURN_FAILED,
    )
    .await;
    let failed_params = failed_notification
        .params
        .expect("turn/failed params should exist");
    let failed: TurnFailedNotification =
        serde_json::from_value(failed_params).expect("turn/failed params should decode");
    assert_eq!(failed.turn.id, "turn_000000000000000022");
    assert_eq!(failed.turn.status, TurnStatus::Interrupted);
    assert_eq!(failed.turn.error.as_deref(), Some("user clicked stop"));

    let cancel_result: TurnCancelResponse =
        serde_json::from_value(cancel_response.result).expect("turn/cancel response should decode");
    assert_eq!(cancel_result.thread_id, "thr_000000000000000022");
    assert_eq!(cancel_result.turn.status, TurnStatus::Interrupted);
    assert_eq!(
        cancel_result.turn.error.as_deref(),
        Some("user clicked stop")
    );

    let second_cancel_request_id = generate_test_request_id("turncancel", "stop2");
    let second_cancel_request = json!({
        "jsonrpc": "2.0",
        "id": second_cancel_request_id,
        "method": "turn/cancel",
        "params": {
            "thread_id": "thr_000000000000000022",
            "turn_id": "turn_000000000000000022",
            "reason": "second click"
        }
    });
    processor
        .process_request(connection_id, &second_cancel_request.to_string())
        .await;

    let second_response = recv_response_by_id(&mut rx, second_cancel_request_id.as_str()).await;
    let second_result: TurnCancelResponse = serde_json::from_value(second_response.result)
        .expect("second turn/cancel response should decode");
    assert_eq!(second_result.turn.status, TurnStatus::Interrupted);
    assert_eq!(
        second_result.turn.error.as_deref(),
        Some("user clicked stop")
    );

    let (_, persisted_turn) = crud_store_for_assert
        .get_turn("thr_000000000000000022", "turn_000000000000000022")
        .await
        .expect("persisted turn should load")
        .expect("interrupted turn should be persisted");
    assert_eq!(persisted_turn.status, TurnStatus::Interrupted);
    assert_eq!(persisted_turn.error.as_deref(), Some("user clicked stop"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_cancel_rejects_non_subscribed_connection() {
    let (tx_owner, mut rx_owner) = mpsc::channel(16);
    let (tx_foreign, mut rx_foreign) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let owner_connection_id = session_manager.register_connection(tx_owner).await;
    let foreign_connection_id = session_manager.register_connection(tx_foreign).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "delayed"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "delayed",
        Arc::new(DelayedProvider {
            delay: Duration::from_secs(30),
            text: "too late".to_owned(),
        }),
    ));
    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_request_id = generate_test_request_id("turncancelforeign", "thread");
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": thread_request_id,
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000023",
            "workspace_id": workspace_id,
            "model": "test-model",
            "model_provider": "delayed"
        }
    });
    processor
        .process_request(owner_connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx_owner, thread_request_id.as_str()).await;

    let turn_start_request_id = generate_test_request_id("turncancelforeign", "start");
    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": turn_start_request_id,
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000023",
            "turn_id": "turn_000000000000000023",
            "mode": "Chat",
            "model": "test-model",
            "model_provider": "delayed",
            "input": [{"type": "text", "text": "hello"}]
        }
    });
    processor
        .process_request(owner_connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx_owner, turn_start_request_id.as_str()).await;

    let cancel_request_id = generate_test_request_id("turncancelforeign", "stop");
    let cancel_request = json!({
        "jsonrpc": "2.0",
        "id": cancel_request_id,
        "method": "turn/cancel",
        "params": {
            "thread_id": "thr_000000000000000023",
            "turn_id": "turn_000000000000000023",
            "reason": "foreign stop"
        }
    });
    processor
        .process_request(foreign_connection_id, &cancel_request.to_string())
        .await;

    let error = recv_error_by_id(&mut rx_foreign, cancel_request_id.as_str()).await;
    assert_eq!(error.error.code, INVALID_REQUEST_CODE);
    assert!(error.error.message.contains("is not subscribed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_cancel_completed_turn_returns_completed_snapshot() {
    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_request_id = generate_test_request_id("turncancelcompleted", "thread");
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": thread_request_id,
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000024",
            "workspace_id": workspace_id,
            "model": "test-model",
            "model_provider": "openai"
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, thread_request_id.as_str()).await;

    let turn_start_request_id = generate_test_request_id("turncancelcompleted", "start");
    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": turn_start_request_id,
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000024",
            "turn_id": "turn_000000000000000024",
            "mode": "Chat",
            "model": "test-model",
            "model_provider": "openai",
            "input": [{"type": "text", "text": "hello"}]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, turn_start_request_id.as_str()).await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;

    let cancel_request_id = generate_test_request_id("turncancelcompleted", "stop");
    let cancel_request = json!({
        "jsonrpc": "2.0",
        "id": cancel_request_id,
        "method": "turn/cancel",
        "params": {
            "thread_id": "thr_000000000000000024",
            "turn_id": "turn_000000000000000024",
            "reason": "too late"
        }
    });
    processor
        .process_request(connection_id, &cancel_request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, cancel_request_id.as_str()).await;
    let result: TurnCancelResponse =
        serde_json::from_value(response.result).expect("turn/cancel response should decode");
    assert_eq!(result.turn.status, TurnStatus::Completed);
    assert!(result.turn.error.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_tree_returns_folders_and_placements_after_moves() {
    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_id = "thr_000000000000000041";

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_start_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
    let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let folder_create_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "name": "Product"
        }
    });
    processor
        .process_request(connection_id, &folder_create_request.to_string())
        .await;
    let folder_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
    let folder_create: ThreadFolderCreateResponse =
        serde_json::from_value(folder_response.result).expect("folderCreate result decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let thread_move_request = json!({
        "jsonrpc": "2.0",
        "id": "ccccccccccccccccccccc",
        "method": "thread/move",
        "params": {
            "workspace_id": workspace_id,
            "thread_id": thread_id,
            "folder_id": folder_create.folder.id
        }
    });
    processor
        .process_request(connection_id, &thread_move_request.to_string())
        .await;
    let thread_move_response = recv_response_by_id(&mut rx, "ccccccccccccccccccccc").await;
    let moved: ThreadMoveResponse =
        serde_json::from_value(thread_move_response.result).expect("thread/move decode");
    assert!(moved.moved);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let tree_request = json!({
        "jsonrpc": "2.0",
        "id": "ddddddddddddddddddddd",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &tree_request.to_string())
        .await;

    let tree_response = recv_response_by_id(&mut rx, "ddddddddddddddddddddd").await;
    let tree: ThreadTreeResponse =
        serde_json::from_value(tree_response.result).expect("thread/tree result decode");

    assert!(
        tree.threads.iter().any(|thread| thread.id == thread_id),
        "thread/tree should include thread"
    );
    assert!(
        tree.folders
            .iter()
            .any(|folder| folder.id == folder_create.folder.id),
        "thread/tree should include created folder"
    );
    assert!(
        tree.placements.iter().any(|placement| {
            placement.thread_id == thread_id
                && placement.folder_id == Some(folder_create.folder.id.clone())
        }),
        "thread/tree should include placement into folder"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn folder_delete_promotes_nested_contents_to_parent() {
    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let parent_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaab",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "name": "Parent"
        }
    });
    processor
        .process_request(connection_id, &parent_request.to_string())
        .await;
    let parent_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaab").await;
    let parent: ThreadFolderCreateResponse =
        serde_json::from_value(parent_response.result).expect("parent create decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let child_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbc",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "parent_folder_id": parent.folder.id,
            "name": "Child"
        }
    });
    processor
        .process_request(connection_id, &child_request.to_string())
        .await;
    let child_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbc").await;
    let child: ThreadFolderCreateResponse =
        serde_json::from_value(child_response.result).expect("child create decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let grandchild_request = json!({
        "jsonrpc": "2.0",
        "id": "ccccccccccccccccccccd",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "parent_folder_id": child.folder.id,
            "name": "Grandchild"
        }
    });
    processor
        .process_request(connection_id, &grandchild_request.to_string())
        .await;
    let grandchild_response = recv_response_by_id(&mut rx, "ccccccccccccccccccccd").await;
    let grandchild: ThreadFolderCreateResponse =
        serde_json::from_value(grandchild_response.result).expect("grandchild create decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let thread_id = "thr_000000000000000042";
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "dddddddddddddddddddde",
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_start = recv_response_by_id(&mut rx, "dddddddddddddddddddde").await;
    let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let move_to_child_request = json!({
        "jsonrpc": "2.0",
        "id": "eeeeeeeeeeeeeeeeeeeef",
        "method": "thread/move",
        "params": {
            "workspace_id": workspace_id,
            "thread_id": thread_id,
            "folder_id": child.folder.id
        }
    });
    processor
        .process_request(connection_id, &move_to_child_request.to_string())
        .await;
    let move_response = recv_response_by_id(&mut rx, "eeeeeeeeeeeeeeeeeeeef").await;
    let move_result: ThreadMoveResponse =
        serde_json::from_value(move_response.result).expect("thread move decode");
    assert!(move_result.moved);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let delete_child_request = json!({
        "jsonrpc": "2.0",
        "id": "ffffffffffffffffffffg",
        "method": "thread/folder/delete",
        "params": {
            "workspace_id": workspace_id,
            "folder_id": child.folder.id
        }
    });
    processor
        .process_request(connection_id, &delete_child_request.to_string())
        .await;
    let delete_response = recv_response_by_id(&mut rx, "ffffffffffffffffffffg").await;
    let delete_result: ThreadFolderDeleteResponse =
        serde_json::from_value(delete_response.result).expect("folder delete decode");
    assert!(delete_result.deleted);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let tree_request = json!({
        "jsonrpc": "2.0",
        "id": "ggggggggggggggggggggh",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &tree_request.to_string())
        .await;
    let tree_response = recv_response_by_id(&mut rx, "ggggggggggggggggggggh").await;
    let tree: ThreadTreeResponse =
        serde_json::from_value(tree_response.result).expect("thread tree decode");

    assert!(
        !tree
            .folders
            .iter()
            .any(|folder| folder.id == child.folder.id),
        "deleted child folder should be removed from tree"
    );
    let promoted_grandchild = tree
        .folders
        .iter()
        .find(|folder| folder.id == grandchild.folder.id)
        .expect("grandchild folder should still exist");
    assert_eq!(
        promoted_grandchild.parent_folder_id,
        Some(parent.folder.id.clone()),
        "grandchild should be reparented one level up"
    );
    let promoted_thread = tree
        .placements
        .iter()
        .find(|placement| placement.thread_id == thread_id)
        .expect("thread placement should exist after move");
    assert_eq!(
        promoted_thread.folder_id,
        Some(parent.folder.id),
        "thread should be moved one level up after folder delete"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_and_folder_move_support_root_target() {
    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let parent_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaac",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "name": "Parent"
        }
    });
    processor
        .process_request(connection_id, &parent_request.to_string())
        .await;
    let parent_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaac").await;
    let parent: ThreadFolderCreateResponse =
        serde_json::from_value(parent_response.result).expect("parent create decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let child_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbd",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "parent_folder_id": parent.folder.id,
            "name": "Child"
        }
    });
    processor
        .process_request(connection_id, &child_request.to_string())
        .await;
    let child_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbd").await;
    let child: ThreadFolderCreateResponse =
        serde_json::from_value(child_response.result).expect("child create decode");
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let thread_id = "thr_000000000000000043";
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "cccccccccccccccccccce",
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_start = recv_response_by_id(&mut rx, "cccccccccccccccccccce").await;
    let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let move_to_child_request = json!({
        "jsonrpc": "2.0",
        "id": "ddddddddddddddddddddf",
        "method": "thread/move",
        "params": {
            "workspace_id": workspace_id,
            "thread_id": thread_id,
            "folder_id": child.folder.id
        }
    });
    processor
        .process_request(connection_id, &move_to_child_request.to_string())
        .await;
    let move_response = recv_response_by_id(&mut rx, "ddddddddddddddddddddf").await;
    let move_result: ThreadMoveResponse =
        serde_json::from_value(move_response.result).expect("thread move decode");
    assert!(move_result.moved);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let move_folder_to_root_request = json!({
        "jsonrpc": "2.0",
        "id": "eeeeeeeeeeeeeeeeeeeeg",
        "method": "thread/folder/move",
        "params": {
            "workspace_id": workspace_id,
            "folder_id": child.folder.id,
            "parent_folder_id": null
        }
    });
    processor
        .process_request(connection_id, &move_folder_to_root_request.to_string())
        .await;
    let move_folder_response = recv_response_by_id(&mut rx, "eeeeeeeeeeeeeeeeeeeeg").await;
    let move_folder_result: ThreadFolderMoveResponse =
        serde_json::from_value(move_folder_response.result).expect("folder move decode");
    assert!(move_folder_result.moved);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let move_thread_to_root_request = json!({
        "jsonrpc": "2.0",
        "id": "ffffffffffffffffffffh",
        "method": "thread/move",
        "params": {
            "workspace_id": workspace_id,
            "thread_id": thread_id,
            "folder_id": null
        }
    });
    processor
        .process_request(connection_id, &move_thread_to_root_request.to_string())
        .await;
    let move_root_response = recv_response_by_id(&mut rx, "ffffffffffffffffffffh").await;
    let move_root_result: ThreadMoveResponse =
        serde_json::from_value(move_root_response.result).expect("thread root move decode");
    assert!(move_root_result.moved);
    let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

    let tree_request = json!({
        "jsonrpc": "2.0",
        "id": "ggggggggggggggggggggi",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &tree_request.to_string())
        .await;
    let tree_response = recv_response_by_id(&mut rx, "ggggggggggggggggggggi").await;
    let tree: ThreadTreeResponse =
        serde_json::from_value(tree_response.result).expect("thread tree decode");

    let moved_folder = tree
        .folders
        .iter()
        .find(|folder| folder.id == child.folder.id)
        .expect("moved folder should exist");
    assert!(
        moved_folder.parent_folder_id.is_none(),
        "folder move to root should clear parent"
    );

    let thread_placement = tree
        .placements
        .iter()
        .find(|placement| placement.thread_id == thread_id)
        .expect("thread placement should exist");
    assert!(
        thread_placement.folder_id.is_none(),
        "thread move to root should clear folder id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_tree_changed_is_broadcast_to_other_connections() {
    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_a = session_manager.register_connection(tx_a).await;
    let connection_b = session_manager.register_connection(tx_b).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager.clone(),
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    session_manager
        .set_connection_workspace(connection_b, Some(workspace_id.clone()))
        .await;

    let folder_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaad",
        "method": "thread/folder/create",
        "params": {
            "workspace_id": workspace_id,
            "name": "Broadcast Folder"
        }
    });
    processor
        .process_request(connection_a, &folder_request.to_string())
        .await;

    let _response = recv_response_by_id(&mut rx_a, "aaaaaaaaaaaaaaaaaaaad").await;
    let sender_notification =
        recv_notification_by_method(&mut rx_a, events::THREAD_TREE_CHANGED).await;
    let receiver_notification =
        recv_notification_by_method(&mut rx_b, events::THREAD_TREE_CHANGED).await;

    let sender_params = sender_notification
        .params
        .expect("sender tree/changed params expected");
    let sender_workspace = sender_params
        .get("workspace_id")
        .and_then(serde_json::Value::as_str)
        .expect("workspace_id should exist");
    assert_eq!(sender_workspace, workspace_id);

    let receiver_params = receiver_notification
        .params
        .expect("receiver tree/changed params expected");
    let receiver_workspace = receiver_params
        .get("workspace_id")
        .and_then(serde_json::Value::as_str)
        .expect("workspace_id should exist");
    assert_eq!(receiver_workspace, workspace_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_tree_includes_agents_doc_summaries_without_content() {
    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let empty_tree_request = json!({
        "jsonrpc": "2.0",
            "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &empty_tree_request.to_string())
        .await;
    let empty_tree_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
    let empty_tree: ThreadTreeResponse =
        serde_json::from_value(empty_tree_response.result).expect("empty tree should decode");
    assert!(empty_tree.agents_docs.is_empty());
    assert!(empty_tree.threads.is_empty());
    assert!(empty_tree.placements.is_empty());

    let active_folder = crud_store
        .create_thread_folder(workspace_id.as_str(), None, "Active")
        .await
        .expect("active folder should be created");
    let draft_folder = crud_store
        .create_thread_folder(workspace_id.as_str(), None, "Draft")
        .await
        .expect("draft folder should be created");
    let archived_folder = crud_store
        .create_thread_folder(workspace_id.as_str(), None, "Archived")
        .await
        .expect("archived folder should be created");

    let root_content = "# Root AGENTS.md\n";
    let active_content = "# Folder AGENTS.md\n";
    let root_doc = crud_store
        .save_thread_agents_doc(
            workspace_id.as_str(),
            None,
            root_content,
            None,
            None,
            ThreadAgentsDocSaveReason::Manual,
        )
        .await
        .expect("root doc should save");
    let active_doc = crud_store
        .save_thread_agents_doc(
            workspace_id.as_str(),
            Some(active_folder.id.as_str()),
            active_content,
            None,
            None,
            ThreadAgentsDocSaveReason::Manual,
        )
        .await
        .expect("active folder doc should save");
    let draft_doc = crud_store
        .create_thread_agents_doc_draft(workspace_id.as_str(), Some(draft_folder.id.as_str()), None)
        .await
        .expect("draft folder doc should be created");
    let archived_doc = crud_store
        .save_thread_agents_doc(
            workspace_id.as_str(),
            Some(archived_folder.id.as_str()),
            "# Archived AGENTS.md\n",
            None,
            None,
            ThreadAgentsDocSaveReason::Manual,
        )
        .await
        .expect("archived folder doc should save");
    crud_store
        .archive_thread_agents_doc(
            workspace_id.as_str(),
            Some(archived_folder.id.as_str()),
            Some(archived_doc.version),
            None,
        )
        .await
        .expect("archived folder doc should archive");

    let tree_request = json!({
        "jsonrpc": "2.0",
            "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "thread/tree",
        "params": {
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &tree_request.to_string())
        .await;
    let tree_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
    let tree_result = tree_response.result.clone();
    let raw_summaries = tree_result
        .get("agents_docs")
        .and_then(serde_json::Value::as_array)
        .expect("agents_docs should be an array");
    assert!(
        raw_summaries
            .iter()
            .all(|summary| summary.get("content").is_none()),
        "thread/tree must not include full AGENTS.md content"
    );

    let tree: ThreadTreeResponse =
        serde_json::from_value(tree_response.result).expect("tree should decode");
    assert_eq!(tree.folders.len(), 3);
    assert_eq!(tree.agents_docs.len(), 3);
    assert!(tree.threads.is_empty());
    assert!(tree.placements.is_empty());

    let root_summary = tree
        .agents_docs
        .iter()
        .find(|summary| summary.folder_id.is_none())
        .expect("root summary should be present");
    assert_eq!(root_summary.id, root_doc.id);
    assert_eq!(root_summary.status, ThreadAgentsDocStatus::Active);
    assert_eq!(root_summary.char_count, root_content.chars().count());

    let folder_summary = tree
        .agents_docs
        .iter()
        .find(|summary| summary.folder_id.as_deref() == Some(active_folder.id.as_str()))
        .expect("active folder summary should be present");
    assert_eq!(folder_summary.id, active_doc.id);
    assert_eq!(folder_summary.status, ThreadAgentsDocStatus::Active);
    assert_eq!(folder_summary.char_count, active_content.chars().count());

    let draft_summary = tree
        .agents_docs
        .iter()
        .find(|summary| summary.folder_id.as_deref() == Some(draft_folder.id.as_str()))
        .expect("draft folder summary should be present");
    assert_eq!(draft_summary.id, draft_doc.id);
    assert_eq!(draft_summary.status, ThreadAgentsDocStatus::Draft);
    assert_eq!(draft_summary.char_count, 0);

    assert!(
        tree.agents_docs
            .iter()
            .all(|summary| { summary.folder_id.as_deref() != Some(archived_folder.id.as_str()) }),
        "archived AGENTS.md summaries should be hidden from thread/tree"
    );
}

fn run_gateway_message_test<F, Fut>(name: &str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(name)
        .enable_all()
        .build()
        .expect("test runtime should build")
        .block_on(async move {
            tokio::spawn(test()).await.expect("test task should finish");
        });
}

fn run_thread_agents_doc_rpc_test<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    run_gateway_message_test("thread-agents-doc-rpc-test", test);
}

#[test]
fn thread_agents_doc_rpc_saves_inherits_archives_and_resolves_for_thread() {
    run_thread_agents_doc_rpc_test(|| async {
        let (tx, mut rx) = mpsc::channel(128);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let processor = MessageProcessor::new(
            thread_manager,
            test_provider(),
            session_manager,
            workspace_manager,
            crud_store,
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        );

        let empty_get = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocgetempty0001",
            "method": "thread/agents_doc/get",
            "params": {
                "workspace_id": workspace_id
            }
        });
        processor
            .process_request(connection_id, &empty_get.to_string())
            .await;
        let empty_get_response = recv_response_by_id(&mut rx, "agentsdocgetempty0001").await;
        let empty_get: ThreadAgentsDocGetResponse =
            serde_json::from_value(empty_get_response.result).expect("empty get should decode");
        assert!(empty_get.explicit.is_none());
        assert!(empty_get.effective.is_none());

        let save_root = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocsaveroot0001",
            "method": "thread/agents_doc/save",
            "params": {
                "workspace_id": workspace_id,
                "content": "# Root instructions\nUse root context.\n",
                "save_reason": "manual"
            }
        });
        processor
            .process_request(connection_id, &save_root.to_string())
            .await;
        let (save_root_response, save_root_changed) = recv_response_and_notification_by_id_method(
            &mut rx,
            "agentsdocsaveroot0001",
            events::THREAD_AGENTS_DOC_CHANGED,
        )
        .await;
        let save_root: ThreadAgentsDocSaveResponse =
            serde_json::from_value(save_root_response.result).expect("root save should decode");
        assert_eq!(save_root.doc.status, ThreadAgentsDocStatus::Active);
        assert!(save_root.doc.folder_id.is_none());
        assert_eq!(save_root.doc.version, 2);
        let save_root_changed_params = save_root_changed
            .params
            .expect("agents doc changed params expected");
        assert_eq!(
            save_root_changed_params
                .get("effective_changed")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

        let folder_create = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocfolder000001",
            "method": "thread/folder/create",
            "params": {
                "workspace_id": workspace_id,
                "name": "Child"
            }
        });
        processor
            .process_request(connection_id, &folder_create.to_string())
            .await;
        let folder_response = recv_response_by_id(&mut rx, "agentsdocfolder000001").await;
        let folder: ThreadFolderCreateResponse =
            serde_json::from_value(folder_response.result).expect("folder create should decode");
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

        let get_child = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocgetchild0001",
            "method": "thread/agents_doc/get",
            "params": {
                "workspace_id": workspace_id,
                "folder_id": folder.folder.id
            }
        });
        processor
            .process_request(connection_id, &get_child.to_string())
            .await;
        let get_child_response = recv_response_by_id(&mut rx, "agentsdocgetchild0001").await;
        let child_context: ThreadAgentsDocGetResponse =
            serde_json::from_value(get_child_response.result).expect("child get should decode");
        assert!(child_context.explicit.is_none());
        let child_effective = child_context
            .effective
            .expect("child should inherit root AGENTS.md");
        assert!(child_effective.inherited);
        assert!(child_effective.source_folder_id.is_none());
        assert_eq!(child_effective.doc.content, save_root.doc.content);

        let save_child = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocsavechild001",
            "method": "thread/agents_doc/save",
            "params": {
                "workspace_id": workspace_id,
                "folder_id": folder.folder.id,
                "content": "# Child instructions\nUse child context.\n"
            }
        });
        processor
            .process_request(connection_id, &save_child.to_string())
            .await;
        let (save_child_response, _) = recv_response_and_notification_by_id_method(
            &mut rx,
            "agentsdocsavechild001",
            events::THREAD_AGENTS_DOC_CHANGED,
        )
        .await;
        let save_child: ThreadAgentsDocSaveResponse =
            serde_json::from_value(save_child_response.result).expect("child save should decode");
        assert_eq!(save_child.doc.status, ThreadAgentsDocStatus::Active);
        assert_eq!(save_child.doc.folder_id, Some(folder.folder.id.clone()));
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

        let thread_id = "thr_agents_doc_rpc_001";
        let start_thread = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocthread000001",
            "method": "thread/start",
            "params": {
                "workspace_id": workspace_id,
                "thread_id": thread_id
            }
        });
        processor
            .process_request(connection_id, &start_thread.to_string())
            .await;
        let _thread_start = recv_response_by_id(&mut rx, "agentsdocthread000001").await;
        let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

        let move_thread = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocmove00000001",
            "method": "thread/move",
            "params": {
                "workspace_id": workspace_id,
                "thread_id": thread_id,
                "folder_id": folder.folder.id
            }
        });
        processor
            .process_request(connection_id, &move_thread.to_string())
            .await;
        let move_response = recv_response_by_id(&mut rx, "agentsdocmove00000001").await;
        let move_result: ThreadMoveResponse =
            serde_json::from_value(move_response.result).expect("thread move should decode");
        assert!(move_result.moved);
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

        let resolve_thread = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocresolve00001",
            "method": "thread/agents_doc/resolve_for_thread",
            "params": {
                "workspace_id": workspace_id,
                "thread_id": thread_id
            }
        });
        processor
            .process_request(connection_id, &resolve_thread.to_string())
            .await;
        let resolve_response = recv_response_by_id(&mut rx, "agentsdocresolve00001").await;
        let resolved: ThreadAgentsDocResolveForThreadResponse =
            serde_json::from_value(resolve_response.result).expect("thread resolve should decode");
        let resolved = resolved.effective.expect("thread should resolve child doc");
        assert!(!resolved.inherited);
        assert_eq!(resolved.source_folder_id, Some(folder.folder.id.clone()));
        assert_eq!(resolved.doc.content, save_child.doc.content);

        let archive_child = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocarchive00001",
            "method": "thread/agents_doc/archive",
            "params": {
                "workspace_id": workspace_id,
                "folder_id": folder.folder.id,
                "expected_version": save_child.doc.version
            }
        });
        processor
            .process_request(connection_id, &archive_child.to_string())
            .await;
        let (archive_response, _) = recv_response_and_notification_by_id_method(
            &mut rx,
            "agentsdocarchive00001",
            events::THREAD_AGENTS_DOC_CHANGED,
        )
        .await;
        let archive: ThreadAgentsDocArchiveResponse =
            serde_json::from_value(archive_response.result).expect("archive should decode");
        assert!(archive.archived);
        let fallback = archive.effective.expect("child should fall back to root");
        assert!(fallback.inherited);
        assert!(fallback.source_folder_id.is_none());
        assert_eq!(fallback.doc.content, save_root.doc.content);
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;
    });
}

#[test]
fn thread_agents_doc_rpc_rejects_large_content_and_version_conflicts() {
    run_thread_agents_doc_rpc_test(|| async {
        let (tx, mut rx) = mpsc::channel(32);
        let session_manager = Arc::new(SessionManager::new());
        let connection_id = session_manager.register_connection(tx).await;
        let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
        let processor = MessageProcessor::new(
            thread_manager,
            test_provider(),
            session_manager,
            workspace_manager,
            crud_store,
            test_gateway_secrets(),
            test_summary_config(),
            test_context_budget(),
            test_tool_loop_config(),
        );

        let oversized_content = "x".repeat(64 * 1024 + 1);
        let oversized_save = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocoversize0001",
            "method": "thread/agents_doc/save",
            "params": {
                "workspace_id": workspace_id,
                "content": oversized_content
            }
        });
        processor
            .process_request(connection_id, &oversized_save.to_string())
            .await;
        let oversized_error = recv_error_by_id(&mut rx, "agentsdocoversize0001").await;
        assert_eq!(
            oversized_error.error.code,
            pioneer_protocol::INVALID_PARAMS_CODE
        );

        let save_root = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocversion00001",
            "method": "thread/agents_doc/save",
            "params": {
                "workspace_id": workspace_id,
                "content": "# Root\n"
            }
        });
        processor
            .process_request(connection_id, &save_root.to_string())
            .await;
        let (save_response, _) = recv_response_and_notification_by_id_method(
            &mut rx,
            "agentsdocversion00001",
            events::THREAD_AGENTS_DOC_CHANGED,
        )
        .await;
        let save: ThreadAgentsDocSaveResponse =
            serde_json::from_value(save_response.result).expect("save should decode");
        let _tree_changed = recv_notification_by_method(&mut rx, events::THREAD_TREE_CHANGED).await;

        let conflict_save = json!({
            "jsonrpc": "2.0",
            "id": "agentsdocconflict0001",
            "method": "thread/agents_doc/save",
            "params": {
                "workspace_id": workspace_id,
                "content": "# Changed\n",
                "expected_version": save.doc.version + 1
            }
        });
        processor
            .process_request(connection_id, &conflict_save.to_string())
            .await;
        let conflict_error = recv_error_by_id(&mut rx, "agentsdocconflict0001").await;
        assert_eq!(
            conflict_error.error.code,
            pioneer_protocol::INVALID_REQUEST_CODE
        );
        assert!(
            conflict_error.error.message.contains("version conflict"),
            "version conflict should be explicit in error message"
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_history_returns_materialized_history() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000032",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
    let _ = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000032",
            "turn_id": "turn_000000000000000032",
            "input": [{"type": "text", "text": "history message"}]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;
    let _ = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;

    let thread_history_request = json!({
        "jsonrpc": "2.0",
        "id": "ccccccccccccccccccccc",
        "method": "thread/history",
        "params": {
            "thread_id": "thr_000000000000000032",
            "limit": 20
        }
    });
    processor
        .process_request(connection_id, &thread_history_request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, "ccccccccccccccccccccc").await;
    let history: ThreadHistoryResponse =
        serde_json::from_value(response.result).expect("thread/history result should decode");

    let has_turn_started = history.events.iter().any(|event| {
        matches!(
            &event.payload,
            ThreadHistoryEventPayload::TurnStarted { turn, input, .. }
                if turn.id == "turn_000000000000000032"
                    && input.iter().any(|value| matches!(
                        value,
                        UserInput::Text { text, .. } if text == "history message"
                    ))
        )
    });
    let has_agent_item_completed = history.events.iter().any(|event| {
        matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemCompleted { item, .. }
                if matches!(item, pioneer_protocol::TurnItem::AgentMessage { text, .. } if text == "history message")
        )
    });
    let has_reasoning_item = history.events.iter().any(|event| {
        matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemStarted { item, .. }
                if matches!(item, pioneer_protocol::TurnItem::Reasoning { .. })
        )
    });
    let has_user_item = history.events.iter().any(|event| {
        matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemCompleted { item, .. }
                if matches!(item, pioneer_protocol::TurnItem::UserMessage { text, .. } if text == "history message")
        )
    });

    assert!(
        has_turn_started,
        "thread/history must include turn/started with original user input"
    );
    assert!(
        has_agent_item_completed,
        "thread/history must include completed assistant item"
    );
    assert!(
        has_reasoning_item,
        "thread/history must include reasoning item events"
    );
    assert!(
        has_user_item,
        "thread/history must include user message item events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_items_returns_stream_events_for_resume() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000022",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_text(&mut rx).await;
    let _ = recv_text(&mut rx).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000022",
            "turn_id": "turn_000000000000000022",
            "input": [{"type": "text", "text": "resume me"}]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    // Drain turn/start response, turn/started, and agent lifecycle events.
    // The echo provider completes instantly so all events arrive quickly.
    let mut drained = 0;
    while drained < 10 {
        let payload = recv_text_timeout(&mut rx, Duration::from_secs(2)).await;
        drained += 1;
        // Wait until we see the turn/completed notification, meaning all events are persisted.
        if payload.contains("turn/completed") {
            break;
        }
    }

    let turn_items_request = json!({
        "jsonrpc": "2.0",
        "id": "ccccccccccccccccccccc",
        "method": "turn/items",
        "params": {
            "thread_id": "thr_000000000000000022",
            "turn_id": "turn_000000000000000022"
        }
    });
    processor
        .process_request(connection_id, &turn_items_request.to_string())
        .await;

    // The response may be preceded by late notifications; find the RPC response.
    let mut payload = recv_text(&mut rx).await;
    for _ in 0..5 {
        if payload.contains("\"id\":\"ccccccccccccccccccccc\"") {
            break;
        }
        payload = recv_text(&mut rx).await;
    }
    let response: JsonRpcResponse =
        serde_json::from_str(&payload).expect("turn/items response should decode");
    let turn_items: pioneer_protocol::TurnItemsResponse =
        serde_json::from_value(response.result).expect("turn/items result should decode");
    assert_eq!(turn_items.thread_id, "thr_000000000000000022");
    assert!(!turn_items.workspace_id.is_empty());
    assert_eq!(turn_items.turn_id, "turn_000000000000000022");
    assert!(
        !turn_items.events.is_empty(),
        "turn/items should return at least one item stream event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_lifecycle_notification_is_persisted_for_history_replay() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaa99",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000099",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaa99").await;
    let thread_start: ThreadStartResponse =
        serde_json::from_value(response.result).expect("thread/start result should decode");

    let turn = Turn {
        id: "turn_000000000000000099".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    };
    crud_store
        .materialize_turn_start(
            &thread_start.thread,
            SandboxMode::FullAccess,
            &turn,
            &[UserInput::Text {
                text: "recover me".to_owned(),
                text_elements: Vec::new(),
            }],
        )
        .await
        .expect("turn start should persist");

    processor
        .handle_recovery_event(
            crate::resilience::RecoveryCoordinatorEvent::RecoveryOpened {
                job_id: "recovery_job_history".to_owned(),
                turn_id: turn.id.clone(),
                item_id: "reasoning_history".to_owned(),
                item_type: TurnItemType::Reasoning,
                trigger: RecoveryTrigger::ProviderError,
                action: RecoveryAction::RetryWithBackoff,
                attempt_number: 1,
            },
            1_700_000_001,
        )
        .await;

    let opened = recv_notification_by_method(&mut rx, events::ITEM_RECOVERY_OPENED).await;
    assert_eq!(
        opened
            .params
            .and_then(|params| params.get("recovery_job_id").cloned()),
        Some(json!("recovery_job_history"))
    );

    let history = crud_store
        .get_thread_history(thread_start.thread.id.as_str(), Some(16))
        .await
        .expect("thread history should load")
        .expect("thread history should exist");
    assert!(history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::ItemRecoveryOpened {
            recovery_job_id,
            trigger,
            action,
            attempt_number,
            ..
        } if recovery_job_id == "recovery_job_history"
            && *trigger == RecoveryTrigger::ProviderError
            && *action == RecoveryAction::RetryWithBackoff
            && *attempt_number == 1
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_retry_notification_is_persisted_for_history_replay_before_live() {
    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_tool_retry_gateway".to_owned(),
            name: "missing_gateway_tool".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done after retry event",
    ));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai", provider,
    ));
    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "toolretrythreadstart1",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000199",
            "workspace_id": workspace_id,
            "mode": "Agent"
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_start_response = recv_response_by_id(&mut rx, "toolretrythreadstart1").await;
    let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "toolretryturnstart001",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000199",
            "turn_id": "turn_000000000000000199",
            "input": [
                {
                    "type": "text",
                    "text": "trigger a missing tool"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _turn_start_response = recv_response_by_id(&mut rx, "toolretryturnstart001").await;
    let _turn_started = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;

    let scheduled_notification =
        recv_notification_by_method(&mut rx, events::ITEM_TOOL_RETRY_SCHEDULED).await;
    assert_eq!(
        scheduled_notification.method,
        events::ITEM_TOOL_RETRY_SCHEDULED
    );
    let scheduled: ItemToolRetryScheduledNotification = serde_json::from_value(
        scheduled_notification
            .params
            .clone()
            .expect("tool retry scheduled params should exist"),
    )
    .expect("tool retry scheduled notification should decode");
    assert_eq!(scheduled.tool_name, "missing_gateway_tool");

    let history = crud_store
        .get_thread_history("thr_000000000000000199", Some(32))
        .await
        .expect("thread history should load")
        .expect("thread history should exist");
    assert!(history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::ItemToolRetryScheduled {
            tool_name,
            tool_retry_episode_id,
            ..
        } if tool_name == "missing_gateway_tool"
            && tool_retry_episode_id == &scheduled.tool_retry_episode_id
    )));

    let _turn_completed = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_emits_full_lifecycle_notifications_and_echoes_text() {
    let (tx, mut rx) = mpsc::channel(16);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaaaaaaaa",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000012",
            "workspace_id": workspace_id
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;

    let _thread_start_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaaaaaaaa").await;
    let _thread_started_notification =
        recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbbbbbbb",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000012",
            "turn_id": "turn_000000000000000005",
            "input": [
                {
                    "type": "text",
                    "text": "Echo this back"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let turn_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbbbbbbb").await;
    let turn_started_notification =
        recv_notification_by_method(&mut rx, events::TURN_STARTED).await;
    let turn_start_result: TurnStartResponse =
        serde_json::from_value(turn_response.result).expect("turn/start result should decode");
    assert_eq!(turn_start_result.turn.status, TurnStatus::InProgress);

    assert_eq!(turn_started_notification.method, events::TURN_STARTED);

    let (
        thinking_started_notification,
        thinking_completed_notification,
        message_started_notification,
        item_delta_notification,
        item_completed_notification,
        turn_completed_notification,
    ) = recv_echo_turn_lifecycle_notifications(&mut rx).await;

    let thinking_started_params = thinking_started_notification
        .params
        .expect("thinking item/started params should exist");
    let thinking_started: ItemStartedNotification = serde_json::from_value(thinking_started_params)
        .expect("thinking item/started params must decode");
    let thinking_item_id = match thinking_started.item {
        pioneer_protocol::TurnItem::Reasoning {
            id,
            summary,
            content,
        } => {
            assert!(summary.is_empty());
            assert!(content.is_empty());
            id
        }
        other => panic!("expected reasoning item, got {other:?}"),
    };

    assert_eq!(
        thinking_completed_notification.method,
        events::ITEM_COMPLETED
    );
    let thinking_completed_params = thinking_completed_notification
        .params
        .expect("thinking item/completed params should exist");
    let thinking_completed: ItemCompletedNotification =
        serde_json::from_value(thinking_completed_params)
            .expect("thinking item/completed params must decode");
    match thinking_completed.item {
        pioneer_protocol::TurnItem::Reasoning {
            id,
            summary,
            content,
        } => {
            assert_eq!(id, thinking_item_id);
            assert!(summary.is_empty());
            assert!(content.is_empty());
        }
        other => panic!("expected reasoning item, got {other:?}"),
    }

    assert_eq!(message_started_notification.method, events::ITEM_STARTED);
    let message_started_params = message_started_notification
        .params
        .expect("message item/started params should exist");
    let message_started: ItemStartedNotification = serde_json::from_value(message_started_params)
        .expect("message item/started params must decode");
    let agent_item_id = match message_started.item {
        pioneer_protocol::TurnItem::AgentMessage { id, text, .. } => {
            assert!(text.is_empty());
            id
        }
        other => panic!("expected agent message item, got {other:?}"),
    };

    assert_eq!(
        item_delta_notification.method,
        events::ITEM_AGENT_MESSAGE_DELTA
    );
    let item_delta_params = item_delta_notification
        .params
        .expect("item delta params should exist");
    let item_delta: ItemDeltaNotification =
        serde_json::from_value(item_delta_params).expect("item delta params must decode");
    assert_eq!(item_delta.item_id, agent_item_id);
    assert_eq!(item_delta.delta, "Echo this back");

    assert_eq!(item_completed_notification.method, events::ITEM_COMPLETED);
    let item_completed_params = item_completed_notification
        .params
        .expect("item/completed params should exist");
    let item_completed: ItemCompletedNotification =
        serde_json::from_value(item_completed_params).expect("item/completed params must decode");
    match item_completed.item {
        pioneer_protocol::TurnItem::AgentMessage { id, text, .. } => {
            assert_eq!(id, agent_item_id);
            assert_eq!(text, "Echo this back");
        }
        other => panic!("expected agent message item, got {other:?}"),
    }

    assert_eq!(turn_completed_notification.method, events::TURN_COMPLETED);
    let turn_completed_params = turn_completed_notification
        .params
        .expect("turn/completed params should exist");
    let turn_completed: TurnCompletedNotification =
        serde_json::from_value(turn_completed_params).expect("turn/completed params must decode");
    assert_eq!(turn_completed.turn.id, "turn_000000000000000005");
    assert_eq!(turn_completed.turn.status, TurnStatus::Completed);
    assert!(turn_completed.turn.error.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_executes_dynamic_skill_tool_end_to_end() {
    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix time")
        .as_nanos();
    let skill_root = std::env::temp_dir().join(format!("pioneer-gateway-skill-{nanos}"));
    let skill_dir = skill_root.join("tests").join("my-skill");
    std::fs::create_dir_all(&skill_dir).expect("create skill dir");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: my-skill
slug: my-skill
description: gateway skill
runtime:
  tools:
    - tool_slug: echo_shell
      description: Echo shell
      kind: shell
      parameters:
        type: object
      execution_class: session_scoped
      config:
        command: ["/bin/sh", "-c", "printf gw-shell-ok"]
      output_policy:
        timeline:
          mode: full
          max_bytes: 1048576
        storage:
          mode: full
          max_bytes: 1048576
        deltas:
          mode: persist_and_display
          max_chunk_bytes: 65536
          max_total_bytes: 1048576
---
Gateway skill body"#,
    )
    .expect("write SKILL.md");

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_gw_dynamic_1".to_owned(),
            name: "skill.tests-my-skill.echo-shell".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done",
    ));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.runtime.allow_shell_tools = true;
    tool_loop_config.skills.security.min_trust_for_shell_tools = SkillTrustLevel::Community;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaadyn111",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000130",
            "workspace_id": workspace_id,
            "mode": "Agent"
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaadyn111").await;
    let _thread_started = recv_notification_by_method(&mut rx, events::THREAD_STARTED).await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbdyn111",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000130",
            "turn_id": "turn_000000000000000130",
            "input": [
                { "type": "skill", "name": "my-skill", "path": "" },
                { "type": "text", "text": "run dynamic skill" }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let _turn_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbdyn111").await;
    let _turn_started = recv_notification_by_method(&mut rx, events::TURN_STARTED).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while provider.snapshot_requests().len() < 2 {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "provider should receive second round with dynamic tool result"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let requests = provider.snapshot_requests();
    assert!(requests.len() >= 2);
    let second_round = &requests[1];
    let dynamic_result = second_round
        .messages
        .iter()
        .find(|message| message.role == pioneer_provider::Role::Tool)
        .expect("second round must include dynamic tool result");
    assert_eq!(
        dynamic_result.name.as_deref(),
        Some("skill.tests-my-skill.echo-shell")
    );
    assert!(dynamic_result.content.contains("gw-shell-ok"));

    let _turn_completed = recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await;
    let history = crud_store_for_assert
        .get_thread_history("thr_000000000000000130", Some(64))
        .await
        .expect("thread history should load")
        .expect("thread history should exist");
    let dynamic_history_item = history
        .events
        .iter()
        .find_map(|event| {
            if let ThreadHistoryEventPayload::ItemCompleted { item, .. } = &event.payload
                && let pioneer_protocol::TurnItem::DynamicToolCall {
                    tool_name, storage, ..
                } = item
                && tool_name == "skill.tests-my-skill.echo-shell"
            {
                return Some(storage);
            }
            None
        })
        .expect("dynamic shell item should be present in replay history");
    assert!(
        matches!(dynamic_history_item, ToolStoragePayload::Shell { stdout: Some(stdout), .. } if stdout.contains("gw-shell-ok")),
        "replayed thread history should contain bounded dynamic shell stdout when policy allows it"
    );

    let _ = std::fs::remove_dir_all(skill_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_materializes_mcp_tool_bindings_and_executes_tool() {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();
    let tool_loop_config = test_tool_loop_config();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config.clone(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let transport = pioneer_mcp::McpTransportConfig::Stdio {
        command: "npx".to_owned(),
        args: vec!["-y".to_owned(), "resend-mcp".to_owned()],
        cwd: None,
        env: std::collections::BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    };
    let install_record = pioneer_crud::McpServerInstallationRecord {
        id: None,
        scope_kind: "workspace".to_owned(),
        scope_key: workspace_id.clone(),
        name: "resend".to_owned(),
        display_name: None,
        source_kind: "config".to_owned(),
        source_ref: json!({"kind":"test"}).to_string(),
        transport_kind: "stdio".to_owned(),
        transport_json: serde_json::to_string(&transport).expect("transport should serialize"),
        auth_json: serde_json::to_string(&pioneer_mcp::McpAuthConfig::default())
            .expect("auth should serialize"),
        secret_refs_json: "[]".to_owned(),
        enabled: true,
        allow_implicit_invocation: true,
        required: false,
        fingerprint: "phase3-mcp-fingerprint".to_owned(),
        updated_at_unix: 1_700_000_000,
    };
    crud_store_for_assert
        .upsert_mcp_server_installation(&install_record, 1_700_000_000)
        .await
        .expect("MCP install seed should persist");
    processor
        .mcp_service
        .reload_workspace(workspace_id.as_str())
        .await
        .expect("MCP runtime should reload");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = crud_store_for_assert
            .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
            .await
            .expect("MCP installation lookup should succeed")
            .expect("resend MCP installation should exist");
        if let Some(server_id) = row.id.as_deref()
            && crud_store_for_assert
                .find_mcp_server_catalog_snapshot(server_id)
                .await
                .expect("MCP catalog lookup should succeed")
                .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "MCP fake server catalog should be persisted"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let materialization = processor
        .mcp_service
        .materialize_mcp_tools(workspace_id.as_str(), "turn_000000000000000131")
        .await
        .expect("MCP tools should materialize");
    assert!(
        materialization
            .bundles
            .iter()
            .flat_map(|bundle| bundle.specs.iter())
            .any(|configured| configured.spec.name == "mcp_resend_send"),
        "MCP callable should be materialized as a dynamic tool"
    );

    let built_tools = pioneer_tools::build_tools(
        std::env::temp_dir(),
        "turn_000000000000000131",
        tool_loop_config.web,
        tool_loop_config.computer_use,
        materialization.bundles,
    )
    .expect("tool runtime should build with MCP extension");
    let call = built_tools
        .router
        .build_tool_call(pioneer_tools::RawToolCall {
            call_id: "call_gw_mcp_1".to_owned(),
            tool_name: "mcp_resend_send".to_owned(),
            arguments: json!({"message":"hello"}).to_string(),
        })
        .expect("MCP router call should build from binding");
    let result = built_tools
        .runtime
        .execute_tool_call(call)
        .await
        .expect("MCP tool should execute through runtime");
    assert!(result.success());
    assert!(result.model_visible_text().contains("\"tool\": \"send\""));

    let bindings = crud_store_for_assert
        .list_turn_mcp_bindings("turn_000000000000000131")
        .await
        .expect("turn MCP bindings should load");
    assert!(bindings.iter().any(|binding| {
        binding.server_name == "resend"
            && binding.raw_tool_name == "send"
            && binding.callable_name == "mcp_resend_send"
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_http_body_is_model_visible_but_not_persisted_or_broadcast() {
    const SECRET: &str = "SECRET_DYNAMIC_HTTP_BODY_SENTINEL";

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let url = spawn_one_shot_http_server(SECRET).await;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix time")
        .as_nanos();
    let skill_root = std::env::temp_dir().join(format!("pioneer-gateway-http-skill-{nanos}"));
    let skill_dir = skill_root.join("tests").join("http-skill");
    std::fs::create_dir_all(&skill_dir).expect("create skill dir");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        format!(
            r#"---
name: http-skill
slug: http-skill
description: gateway http skill
runtime:
  tools:
    - tool_slug: fetch_secret
      description: Fetch secret
      kind: http
      parameters:
        type: object
      execution_class: shared
      config:
        method: GET
        url: "{url}"
      output_policy:
        timeline:
          mode: full
          max_bytes: 1048576
        storage:
          mode: full
          max_bytes: 1048576
        recovery:
          mode: evidence
          diagnostic_excerpt:
            mode: output
            max_chars: 4000
        deltas:
          mode: persist_and_display
          max_chunk_bytes: 65536
          max_total_bytes: 1048576
---
Gateway HTTP skill body"#
        ),
    )
    .expect("write SKILL.md");

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_gw_dynamic_http_1".to_owned(),
            name: "skill.tests-http-skill.fetch-secret".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done",
    ));
    let provider_registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.runtime.allow_http_tools = true;
    tool_loop_config.skills.security.min_trust_for_http_tools = SkillTrustLevel::Community;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": "aaaaaaaaaaaaaaadyn112",
        "method": "thread/start",
        "params": {
            "thread_id": "thr_000000000000000131",
            "workspace_id": workspace_id,
            "mode": "Agent"
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _thread_response = recv_response_by_id(&mut rx, "aaaaaaaaaaaaaaadyn112").await;

    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": "bbbbbbbbbbbbbbbdyn112",
        "method": "turn/start",
        "params": {
            "thread_id": "thr_000000000000000131",
            "turn_id": "turn_000000000000000131",
            "input": [
                { "type": "skill", "name": "http-skill", "path": "" },
                { "type": "text", "text": "run dynamic http skill" }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;

    let _turn_response = recv_response_by_id(&mut rx, "bbbbbbbbbbbbbbbdyn112").await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while provider.snapshot_requests().len() < 2 {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "provider should receive second round with dynamic http result"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let requests = provider.snapshot_requests();
    let dynamic_result = requests[1]
        .messages
        .iter()
        .find(|message| message.role == pioneer_provider::Role::Tool)
        .expect("second round must include dynamic http tool result");
    assert_eq!(
        dynamic_result.name.as_deref(),
        Some("skill.tests-http-skill.fetch-secret")
    );
    assert!(dynamic_result.content.contains(SECRET));

    let mut saw_turn_completed = false;
    for _ in 0..200 {
        let message = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("websocket notification should arrive")
            .expect("websocket channel should stay open");
        let payload = match message {
            Message::Text(payload) => payload.to_string(),
            other => panic!("expected text websocket message, got {other:?}"),
        };
        assert!(
            !payload.contains(SECRET),
            "dynamic HTTP body leaked into websocket payload: {payload}"
        );
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        if value.get("id").is_none()
            && value.get("method").and_then(serde_json::Value::as_str)
                == Some(events::TURN_COMPLETED)
        {
            saw_turn_completed = true;
            break;
        }
    }
    assert!(
        saw_turn_completed,
        "turn completed notification should arrive"
    );

    let retained_context = crud_store_for_assert
        .list_turn_llm_context("turn_000000000000000131")
        .await
        .expect("turn llm context should load");
    assert!(
        retained_context.is_empty(),
        "terminal turn cleanup should remove dynamic HTTP llm_view"
    );

    let turn_items = crud_store_for_assert
        .get_turn_item_events("thr_000000000000000131", "turn_000000000000000131")
        .await
        .expect("turn item events should load")
        .expect("turn item events should exist");
    assert!(
        !serde_json::to_string(&turn_items).unwrap().contains(SECRET),
        "dynamic HTTP body leaked into turn item events"
    );

    let history = crud_store_for_assert
        .get_thread_history("thr_000000000000000131", Some(64))
        .await
        .expect("thread history should load")
        .expect("thread history should exist");
    let history_json = serde_json::to_string(&history.events).unwrap();
    assert!(
        !history_json.contains(SECRET),
        "dynamic HTTP body leaked into thread history"
    );
    assert!(
        history_json.contains("bodyHash") || history_json.contains("bodyBytes"),
        "dynamic HTTP history should retain body hash/size metadata"
    );

    let _ = std::fs::remove_dir_all(skill_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_list_returns_sorted_catalog_snapshot() {
    let base_dir = unique_temp_dir("skills_list_snapshot");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");

    write_test_skill(&system_root, "sys-b", "", "system skill body");
    write_test_skill(&user_root, "user-a", "", "user skill body");
    write_test_skill(&registry_root, "registry-c", "", "registry skill body");

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "skillslist00000000001",
        "method": "skills/list",
        "params": {
            "workspace_id": workspace_id,
            "include_health": true,
            "include_policy": true
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, "skillslist00000000001").await;
    let payload: SkillListResponse =
        serde_json::from_value(response.result).expect("skills/list payload should decode");
    assert!(payload.snapshot_version > 0);
    assert!(payload.generated_at > 0);
    assert!(
        payload.skills.len() >= 3,
        "skills/list should include all discovered sources"
    );

    for pair in payload.skills.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        assert!(
            (left.source_kind.clone(), left.slug.clone())
                <= (right.source_kind.clone(), right.slug.clone()),
            "skills/list must be sorted by source_kind then slug"
        );
    }

    assert!(
        payload
            .skills
            .iter()
            .any(|skill| skill.source_kind == "system" && skill.slug == "tests/sys-b")
    );
    assert!(
        payload
            .skills
            .iter()
            .any(|skill| skill.source_kind == "user" && skill.slug == "tests/user-a")
    );
    assert!(
        payload
            .skills
            .iter()
            .any(|skill| skill.source_kind == "registry" && skill.slug == "tests/registry-c")
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_policy_set_mutates_policy_and_emits_changed() {
    let base_dir = unique_temp_dir("skills_policy_set");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");

    write_test_skill(&user_root, "policy-skill", "", "policy skill body");

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let set_request = json!({
        "jsonrpc": "2.0",
        "id": "skillspolicy000000001",
        "method": "skills/policy/set",
        "params": {
            "workspace_id": workspace_id,
            "skill_slug": "tests/policy-skill",
            "source_kind": "user",
            "enabled": false,
            "allow_implicit_invocation": false
        }
    });
    processor
        .process_request(connection_id, &set_request.to_string())
        .await;

    let set_response = recv_response_by_id(&mut rx, "skillspolicy000000001").await;
    let set_payload: SkillsPolicySetResponse =
        serde_json::from_value(set_response.result).expect("skills/policy/set decode");
    assert_eq!(set_payload.policy.skill_slug, "tests/policy-skill");
    assert_eq!(set_payload.policy.source_kind, "user");
    assert_eq!(set_payload.policy.enabled, Some(false));
    assert_eq!(set_payload.policy.allow_implicit_invocation, Some(false));

    let changed = recv_notification_by_method(&mut rx, events::SKILLS_CHANGED).await;
    let changed_params = changed
        .params
        .expect("skills/changed params should be present");
    let changed_payload: SkillsChangedNotification =
        serde_json::from_value(changed_params).expect("skills/changed payload should decode");
    assert_eq!(changed_payload.reason, "policy_updated");
    assert_eq!(changed_payload.workspace_id, workspace_id);
    assert!(
        changed_payload
            .changes
            .iter()
            .any(|change| change.slug == "tests/policy-skill" && change.change_type == "policy")
    );

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": "skillspolicy000000002",
        "method": "skills/list",
        "params": {
            "workspace_id": workspace_id,
            "include_health": true,
            "include_policy": true
        }
    });
    processor
        .process_request(connection_id, &list_request.to_string())
        .await;

    let list_response = recv_response_by_id(&mut rx, "skillspolicy000000002").await;
    let list_payload: SkillListResponse =
        serde_json::from_value(list_response.result).expect("skills/list payload should decode");
    let policy_skill = list_payload
        .skills
        .iter()
        .find(|skill| skill.slug == "tests/policy-skill" && skill.source_kind == "user")
        .expect("policy-skill should exist in skills/list");
    assert!(!policy_skill.policy.enabled);
    assert_eq!(policy_skill.status, "disabled");

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_install_update_uninstall_round_trip_persists_and_notifies() {
    let base_dir = unique_temp_dir("skills_lifecycle");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_v1_root = base_dir.join("source-v1");
    let source_v2_root = base_dir.join("source-v2");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_v1_root).expect("must create source v1 root");
    std::fs::create_dir_all(&source_v2_root).expect("must create source v2 root");

    let source_v1 = write_test_skill(
        &source_v1_root,
        "registry-skill",
        "version: \"1.0.0\"",
        "registry skill body v1",
    );
    let source_v2 = write_test_skill(
        &source_v2_root,
        "registry-skill",
        "version: \"2.0.0\"",
        "registry skill body v2",
    );

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let mut tool_loop_config =
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root);
    tool_loop_config.skills.user_roots =
        vec![format!("{}/{{workspaceId}}/user", workspace_root.display())];
    tool_loop_config.skills.registry_roots = vec![format!(
        "{}/{{workspaceId}}/registry",
        workspace_root.display()
    )];

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let install_upload_id = create_finalized_skill_upload(
        &processor,
        &mut rx,
        connection_id,
        workspace_id.as_str(),
        source_v1.as_path(),
        "skilllifev1",
    )
    .await;
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "skillslifecycle000001",
        "method": "skills/install",
        "params": {
            "workspace_id": workspace_id,
            "source": {
                "type": "uploaded_archive",
                "upload_id": install_upload_id
            },
            "target_source_kind": "registry"
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;

    let install_response = recv_response_by_id(&mut rx, "skillslifecycle000001").await;
    let install_payload: SkillsInstallResponse =
        serde_json::from_value(install_response.result).expect("skills/install payload decode");
    assert_eq!(install_payload.status, "installed");
    assert_eq!(install_payload.skill.slug, "pioneer/registry-skill");

    let install_changed = recv_notification_by_method(&mut rx, events::SKILLS_CHANGED).await;
    let install_changed_payload: SkillsChangedNotification = serde_json::from_value(
        install_changed
            .params
            .expect("skills/changed params expected"),
    )
    .expect("skills/changed decode");
    assert_eq!(install_changed_payload.reason, "installed");

    let installed_row = crud_store_for_assert
        .find_skill_installation("pioneer/registry-skill", "registry", workspace_id.as_str())
        .await
        .expect("read installed row should succeed");
    assert!(
        installed_row.is_some(),
        "install should persist skill_installation"
    );
    let initial_fingerprint = installed_row
        .as_ref()
        .map(|row| row.fingerprint.clone())
        .expect("installed row should be present");
    let installed_policy = crud_store_for_assert
        .list_workspace_skill_policies(workspace_id.as_str())
        .await
        .expect("read workspace skill policies should succeed")
        .into_iter()
        .find(|policy| {
            policy.skill_slug == "pioneer/registry-skill" && policy.source_kind == "registry"
        })
        .expect("default policy should be persisted on install");
    assert_eq!(installed_policy.enabled, Some(true));
    assert_eq!(installed_policy.allow_implicit_invocation, Some(true));

    let update_upload_id = create_finalized_skill_upload(
        &processor,
        &mut rx,
        connection_id,
        workspace_id.as_str(),
        source_v2.as_path(),
        "skilllifev2",
    )
    .await;
    let update_request = json!({
        "jsonrpc": "2.0",
        "id": "skillslifecycle000002",
        "method": "skills/update",
        "params": {
            "workspace_id": workspace_id,
            "slug": "pioneer/registry-skill",
            "source_kind": "registry",
            "source": {
                "type": "uploaded_archive",
                "upload_id": update_upload_id
            }
        }
    });
    processor
        .process_request(connection_id, &update_request.to_string())
        .await;

    let update_response = recv_response_by_id(&mut rx, "skillslifecycle000002").await;
    let update_payload: SkillsUpdateResponse =
        serde_json::from_value(update_response.result).expect("skills/update payload decode");
    assert_eq!(update_payload.status, "updated");
    assert_eq!(update_payload.skill.slug, "pioneer/registry-skill");
    assert_ne!(
        update_payload.skill.fingerprint, initial_fingerprint,
        "update should change fingerprint for modified source"
    );

    let update_changed = recv_notification_by_method(&mut rx, events::SKILLS_CHANGED).await;
    let update_changed_payload: SkillsChangedNotification = serde_json::from_value(
        update_changed
            .params
            .expect("skills/changed params expected"),
    )
    .expect("skills/changed decode");
    assert_eq!(update_changed_payload.reason, "updated");
    assert!(
        update_changed_payload
            .changes
            .iter()
            .any(|change| change.change_type == "update")
    );

    let uninstall_request = json!({
        "jsonrpc": "2.0",
        "id": "skillslifecycle000003",
        "method": "skills/uninstall",
        "params": {
            "workspace_id": workspace_id,
            "slug": "pioneer/registry-skill",
            "source_kind": "registry"
        }
    });
    processor
        .process_request(connection_id, &uninstall_request.to_string())
        .await;

    let uninstall_response = recv_response_by_id(&mut rx, "skillslifecycle000003").await;
    let uninstall_payload: SkillsUninstallResponse =
        serde_json::from_value(uninstall_response.result).expect("skills/uninstall payload decode");
    assert_eq!(uninstall_payload.status, "uninstalled");
    assert_eq!(uninstall_payload.slug, "pioneer/registry-skill");
    assert!(uninstall_payload.removed_install_path.is_some());

    let uninstall_changed = recv_notification_by_method(&mut rx, events::SKILLS_CHANGED).await;
    let uninstall_changed_payload: SkillsChangedNotification = serde_json::from_value(
        uninstall_changed
            .params
            .expect("skills/changed params expected"),
    )
    .expect("skills/changed decode");
    assert_eq!(uninstall_changed_payload.reason, "uninstalled");
    assert!(
        uninstall_changed_payload
            .changes
            .iter()
            .any(|change| change.change_type == "uninstall")
    );

    let removed_row = crud_store_for_assert
        .find_skill_installation("pioneer/registry-skill", "registry", workspace_id.as_str())
        .await
        .expect("read removed row should succeed");
    assert!(
        removed_row.is_none(),
        "uninstall should remove skill_installation row"
    );

    let audit_rows = crud_store_for_assert
        .list_skill_audit_event_records_for_source("pioneer/registry-skill", "registry", 64)
        .await
        .expect("audit rows read should succeed");
    assert!(
        !audit_rows.is_empty(),
        "install/update/uninstall must persist audit rows"
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_health_returns_dependency_diagnostics() {
    let base_dir = unique_temp_dir("skills_health");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");

    write_test_skill(
        &user_root,
        "dep-skill",
        "dependencies:\n  bins:\n    - pioneer_missing_bin_for_health_test",
        "dep skill body",
    );

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let health_request = json!({
        "jsonrpc": "2.0",
        "id": "skillshealth000000001",
        "method": "skills/health",
        "params": {
            "workspace_id": workspace_id,
            "skills": [],
            "audit_limit": 16
        }
    });
    processor
        .process_request(connection_id, &health_request.to_string())
        .await;

    let health_response = recv_response_by_id(&mut rx, "skillshealth000000001").await;
    let health_payload: SkillsHealthResponse =
        serde_json::from_value(health_response.result).expect("skills/health payload decode");
    let dep_skill = health_payload
        .skills
        .iter()
        .find(|skill| skill.slug == "tests/dep-skill" && skill.source_kind == "user")
        .expect("dep-skill should exist in health payload");
    assert!(
        dep_skill
            .dependency_diagnostics
            .iter()
            .any(|diagnostic| diagnostic.name == "pioneer_missing_bin_for_health_test"),
        "skills/health should expose dependency diagnostics"
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_install_rejects_relative_path_with_structured_error_code() {
    let base_dir = unique_temp_dir("skills_invalid_install");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": "skillsinvalid00000001",
        "method": "skills/install",
        "params": {
            "workspace_id": workspace_id,
            "source": {
                "type": "path",
                "path": "./relative/path"
            },
            "target_source_kind": "registry"
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_error_by_id(&mut rx, "skillsinvalid00000001").await;
    assert!(
        response.error.message.contains("invalid params")
            && response.error.message.contains("uploaded_archive"),
        "path lifecycle source should be absent from protocol: {}",
        response.error.message
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_upload_abort_is_connection_bound() {
    let base_dir = unique_temp_dir("skills_upload_abort_bound");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_root = base_dir.join("source");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_root).expect("must create source root");

    let source_path = write_test_skill(
        &source_root,
        "abort-bound-skill",
        "version: \"1.0.0\"",
        "abort bound body",
    );
    let archive = build_test_skill_archive(source_path.as_path());
    let archive_sha = hex::encode(Sha256::digest(archive.as_slice()));

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let owner_connection_id = session_manager.register_connection(tx.clone()).await;
    let foreign_connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let mut tool_loop_config =
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root);
    tool_loop_config.skills.user_roots =
        vec![format!("{}/{{workspaceId}}/user", workspace_root.display())];
    tool_loop_config.skills.registry_roots = vec![format!(
        "{}/{{workspaceId}}/registry",
        workspace_root.display()
    )];

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let start_request_id = generate_test_request_id("skillabort", "start");
    let foreign_abort_request_id = generate_test_request_id("skillabort", "foreign");
    let owner_abort_request_id = generate_test_request_id("skillabort", "owner");
    let start_request = json!({
        "jsonrpc": "2.0",
        "id": start_request_id,
        "method": "skills/upload/start",
        "params": {
            "workspace_id": workspace_id,
            "file_name": "abort-bound-skill.tar.gz",
            "archive_format": SkillArchiveFormat::TarGz,
            "compressed_size_bytes": archive.len(),
            "uncompressed_size_hint_bytes": archive.len(),
            "sha256": archive_sha
        }
    });
    processor
        .process_request(owner_connection_id, &start_request.to_string())
        .await;
    let start_response = recv_response_by_id(&mut rx, start_request_id.as_str()).await;
    let start_payload: SkillsUploadStartResponse =
        serde_json::from_value(start_response.result).expect("skills/upload/start decode");

    let foreign_abort_request = json!({
        "jsonrpc": "2.0",
        "id": foreign_abort_request_id,
        "method": "skills/upload/abort",
        "params": {
            "workspace_id": workspace_id,
            "upload_id": start_payload.upload_id
        }
    });
    processor
        .process_request(foreign_connection_id, &foreign_abort_request.to_string())
        .await;
    let foreign_error = recv_error_by_id(&mut rx, foreign_abort_request_id.as_str()).await;
    let foreign_code = foreign_error
        .error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert_eq!(foreign_code, "skills.upload.not_found");

    let row_after_foreign_abort = crud_store_for_assert
        .find_skill_upload_session(start_payload.upload_id.as_str())
        .await
        .expect("read upload should succeed")
        .expect("upload should remain");
    assert_eq!(row_after_foreign_abort.status, "receiving");

    let owner_abort_request = json!({
        "jsonrpc": "2.0",
        "id": owner_abort_request_id,
        "method": "skills/upload/abort",
        "params": {
            "workspace_id": workspace_id,
            "upload_id": start_payload.upload_id
        }
    });
    processor
        .process_request(owner_connection_id, &owner_abort_request.to_string())
        .await;
    let owner_response = recv_response_by_id(&mut rx, owner_abort_request_id.as_str()).await;
    let abort_payload: SkillsUploadAbortResponse =
        serde_json::from_value(owner_response.result).expect("skills/upload/abort decode");
    assert_eq!(abort_payload.status, "aborted");

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_upload_chunk_digest_mismatch_aborts_session() {
    let base_dir = unique_temp_dir("skills_upload_bad_chunk_digest");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_root = base_dir.join("source");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_root).expect("must create source root");

    let source_path = write_test_skill(
        &source_root,
        "bad-chunk-digest-skill",
        "version: \"1.0.0\"",
        "bad chunk digest body",
    );
    let archive = build_test_skill_archive(source_path.as_path());
    let archive_sha = hex::encode(Sha256::digest(archive.as_slice()));

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let start_request_id = generate_test_request_id("skillbadchunk", "start");
    let start_request = json!({
        "jsonrpc": "2.0",
        "id": start_request_id,
        "method": "skills/upload/start",
        "params": {
            "workspace_id": workspace_id,
            "file_name": "bad-chunk-digest-skill.tar.gz",
            "archive_format": SkillArchiveFormat::TarGz,
            "compressed_size_bytes": archive.len(),
            "uncompressed_size_hint_bytes": archive.len(),
            "sha256": archive_sha
        }
    });
    processor
        .process_request(connection_id, &start_request.to_string())
        .await;
    let start_response = recv_response_by_id(&mut rx, start_request_id.as_str()).await;
    let start_payload: SkillsUploadStartResponse =
        serde_json::from_value(start_response.result).expect("skills/upload/start decode");

    let header = SkillsUploadChunkHeader {
        workspace_id: workspace_id.to_owned(),
        upload_id: start_payload.upload_id.clone(),
        offset: 0,
        len: u64::try_from(archive.len()).expect("archive length should fit u64"),
        chunk_sha256: Some("0".repeat(64)),
    };
    let header_bytes = serde_json::to_vec(&header).expect("chunk header should encode");
    let mut frame = Vec::with_capacity(8 + header_bytes.len() + archive.len());
    frame.extend_from_slice(b"PSU1");
    frame.extend_from_slice(
        &u32::try_from(header_bytes.len())
            .expect("chunk header length should fit u32")
            .to_be_bytes(),
    );
    frame.extend_from_slice(header_bytes.as_slice());
    frame.extend_from_slice(archive.as_slice());

    processor
        .process_binary_frame(connection_id, frame.as_slice())
        .await;

    let row = crud_store_for_assert
        .find_skill_upload_session(start_payload.upload_id.as_str())
        .await
        .expect("read upload should succeed")
        .expect("upload should remain");
    assert_eq!(row.status, "aborted");
    assert_eq!(row.received_bytes, 0);
    let payload_parent = std::path::Path::new(row.payload_path.as_str())
        .parent()
        .expect("payload should have parent")
        .to_path_buf();
    assert!(
        !payload_parent.exists(),
        "invalid upload payload should be removed"
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_install_with_user_target_persists_user_source_kind() {
    let base_dir = unique_temp_dir("skills_install_user_target");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_root = base_dir.join("source");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_root).expect("must create source root");

    let source_path = write_test_skill(
        &source_root,
        "user-target-skill",
        "version: \"1.0.0\"",
        "user target skill body",
    );

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let mut tool_loop_config =
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root);
    tool_loop_config.skills.user_roots =
        vec![format!("{}/{{workspaceId}}/user", workspace_root.display())];
    tool_loop_config.skills.registry_roots = vec![format!(
        "{}/{{workspaceId}}/registry",
        workspace_root.display()
    )];

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let request_id = "skillsinstalluser0001";
    let upload_id = create_finalized_skill_upload(
        &processor,
        &mut rx,
        connection_id,
        workspace_id.as_str(),
        source_path.as_path(),
        "skilluserupl",
    )
    .await;
    let request = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "skills/install",
        "params": {
            "workspace_id": workspace_id,
            "source": {
                "type": "uploaded_archive",
                "upload_id": upload_id
            },
            "target_source_kind": "user"
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, request_id).await;
    let payload: SkillsInstallResponse =
        serde_json::from_value(response.result).expect("skills/install payload decode");
    assert_eq!(payload.status, "installed");
    assert_eq!(payload.skill.source_kind, "user");
    assert!(
        payload.skill.install_path.starts_with(
            workspace_root
                .join(&workspace_id)
                .join("user")
                .display()
                .to_string()
                .as_str()
        ),
        "user-target install path must be under workspace user root"
    );

    let changed = recv_notification_by_method(&mut rx, events::SKILLS_CHANGED).await;
    let changed_payload: SkillsChangedNotification =
        serde_json::from_value(changed.params.expect("skills/changed params expected"))
            .expect("skills/changed decode");
    assert!(changed_payload.changes.iter().any(|change| {
        change.change_type == "install"
            && change.slug == "pioneer/user-target-skill"
            && change.source_kind == "user"
    }));

    let user_row = crud_store_for_assert
        .find_skill_installation("pioneer/user-target-skill", "user", workspace_id.as_str())
        .await
        .expect("read installed user row should succeed");
    assert!(
        user_row.is_some(),
        "install should persist skill_installation for user source kind"
    );
    let registry_row = crud_store_for_assert
        .find_skill_installation(
            "pioneer/user-target-skill",
            "registry",
            workspace_id.as_str(),
        )
        .await
        .expect("read installed registry row should succeed");
    assert!(
        registry_row.is_none(),
        "install should not persist registry row for user target"
    );
    let user_policy = crud_store_for_assert
        .list_workspace_skill_policies(workspace_id.as_str())
        .await
        .expect("read workspace skill policies should succeed")
        .into_iter()
        .find(|policy| {
            policy.skill_slug == "pioneer/user-target-skill" && policy.source_kind == "user"
        })
        .expect("default user policy should be persisted on install");
    assert_eq!(user_policy.enabled, Some(true));
    assert_eq!(user_policy.allow_implicit_invocation, Some(true));

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_install_defaults_to_user_source_and_isolates_workspaces() {
    let base_dir = unique_temp_dir("skills_install_user_default");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_base = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_root = base_dir.join("source");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_base).expect("must create workspace base");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_root).expect("must create source root");

    let source_path = write_test_skill(
        &source_root,
        "user-default-skill",
        "owner: pioneer\nversion: \"1.0.0\"",
        "user default skill body",
    );

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_a) = setup_workspace_manager().await;
    let workspace_b = "ws_skills_scope_b".to_owned();
    workspace_manager
        .create_workspace(workspace_b.as_str(), Some("Skills Scope B"))
        .await
        .expect("workspace B should be created");
    let crud_store_for_assert = crud_store.clone();

    let mut tool_loop_config =
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_base, &registry_root);
    tool_loop_config.skills.user_roots =
        vec![format!("{}/{{workspaceId}}/user", workspace_base.display())];
    tool_loop_config.skills.registry_roots = vec![format!(
        "{}/{{workspaceId}}/registry",
        workspace_base.display()
    )];

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        tool_loop_config,
    );

    let upload_id = create_finalized_skill_upload(
        &processor,
        &mut rx,
        connection_id,
        workspace_a.as_str(),
        source_path.as_path(),
        "skillwsdefupl",
    )
    .await;
    let install_request_id = generate_test_request_id("skillwsdef", "install");
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": install_request_id,
        "method": "skills/install",
        "params": {
            "workspace_id": workspace_a.clone(),
            "source": {
                "type": "uploaded_archive",
                "upload_id": upload_id
            }
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;

    let install_response = recv_response_by_id(&mut rx, install_request_id.as_str()).await;
    let install_payload: SkillsInstallResponse =
        serde_json::from_value(install_response.result).expect("skills/install payload decode");
    assert_eq!(install_payload.status, "installed");
    assert_eq!(install_payload.skill.source_kind, "user");
    assert!(
        install_payload.skill.install_path.starts_with(
            workspace_base
                .join(&workspace_a)
                .join("user")
                .display()
                .to_string()
                .as_str()
        ),
        "user install path must be under the current workspace user root"
    );

    let workspace_row = crud_store_for_assert
        .find_skill_installation("pioneer/user-default-skill", "user", workspace_a.as_str())
        .await
        .expect("read installed workspace row should succeed");
    assert!(
        workspace_row.is_some(),
        "install should persist skill_installation for the installing workspace"
    );
    let other_workspace_row = crud_store_for_assert
        .find_skill_installation("pioneer/user-default-skill", "user", workspace_b.as_str())
        .await
        .expect("read other workspace row should succeed");
    assert!(
        other_workspace_row.is_none(),
        "install should not persist a row for another workspace"
    );

    write_test_skill(
        workspace_base.join(&workspace_b).join("user").as_path(),
        "user-default-skill",
        "owner: pioneer\nversion: \"1.0.0\"",
        "same slug in another workspace",
    );

    let list_b_request_id = generate_test_request_id("skillwsdef", "listb");
    let list_b_request = json!({
        "jsonrpc": "2.0",
        "id": list_b_request_id,
        "method": "skills/list",
        "params": {
            "workspace_id": workspace_b
        }
    });
    processor
        .process_request(connection_id, &list_b_request.to_string())
        .await;
    let list_b_response = recv_response_by_id(&mut rx, list_b_request_id.as_str()).await;
    let list_b_payload: SkillListResponse =
        serde_json::from_value(list_b_response.result).expect("skills/list payload decode");
    let workspace_b_skill = list_b_payload
        .skills
        .iter()
        .find(|skill| skill.slug == "pioneer/user-default-skill" && skill.source_kind == "user")
        .expect("workspace B should discover its local skill file");
    assert!(
        !workspace_b_skill.install.installed,
        "workspace A installation must not mark workspace B skill as installed"
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_install_blocks_dependency_failure_under_default_policy() {
    let base_dir = unique_temp_dir("skills_untrusted_block");
    let system_root = base_dir.join("system");
    let user_root = base_dir.join("user");
    let workspace_root = base_dir.join("workspace");
    let registry_root = base_dir.join("registry");
    let source_root = base_dir.join("source");
    std::fs::create_dir_all(&system_root).expect("must create system root");
    std::fs::create_dir_all(&user_root).expect("must create user root");
    std::fs::create_dir_all(&workspace_root).expect("must create workspace root");
    std::fs::create_dir_all(&registry_root).expect("must create registry root");
    std::fs::create_dir_all(&source_root).expect("must create source root");

    let source_path = write_test_skill(
        &source_root,
        "dependency-blocked-skill",
        "metadata:\n  clawdbot:\n    requires:\n      commands:\n        - definitely-missing-pioneer-command-987654321",
        "dependency blocked body",
    );

    let (tx, mut rx) = mpsc::channel(32);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config_with_roots(&system_root, &user_root, &workspace_root, &registry_root),
    );

    let upload_id = create_finalized_skill_upload(
        &processor,
        &mut rx,
        connection_id,
        workspace_id.as_str(),
        source_path.as_path(),
        "skilldepblk",
    )
    .await;
    let request = json!({
        "jsonrpc": "2.0",
        "id": "skillsuntrusted000001",
        "method": "skills/install",
        "params": {
            "workspace_id": workspace_id,
            "source": {
                "type": "uploaded_archive",
                "upload_id": upload_id
            },
            "target_source_kind": "registry"
        }
    });
    processor
        .process_request(connection_id, &request.to_string())
        .await;

    let response = recv_error_by_id(&mut rx, "skillsuntrusted000001").await;
    let code = response
        .error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert_eq!(code, "skills.install_blocked.dependency");

    let _ = std::fs::remove_dir_all(base_dir);
}

struct FakeMcpRuntimeConnector;

#[async_trait::async_trait]
impl pioneer_mcp::McpRuntimeConnector for FakeMcpRuntimeConnector {
    async fn connect(
        &self,
        installation: pioneer_mcp::McpServerInstallation,
        installation_id: String,
        resolver: Arc<dyn pioneer_mcp::McpSecretResolver>,
        now_unix: i64,
    ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, pioneer_mcp::McpRuntimeError> {
        for secret_ref in &installation.secret_refs {
            if resolver
                .resolve_mcp_secret(secret_ref.ref_id.as_str())
                .is_none()
            {
                return Err(pioneer_mcp::McpRuntimeError::auth_required(format!(
                    "missing fake MCP secret `{}`",
                    secret_ref.ref_id
                )));
            }
        }

        let catalog = pioneer_mcp::McpCatalogSnapshot::from_json_values(
            installation_id,
            json!({"name":"fake-mcp","version":"test"}),
            None,
            json!([{"name":"send"},{"name":"domains"}]),
            json!([{"uri":"resend://account"}]),
            json!([{"uriTemplate":"resend://{id}"}]),
            json!([{"name":"draft"}]),
            now_unix,
        )
        .expect("fake MCP catalog should build");
        Ok(Box::new(FakeMcpRuntimeSession { catalog }))
    }
}

struct FakeMcpRuntimeSession {
    catalog: pioneer_mcp::McpCatalogSnapshot,
}

#[async_trait::async_trait]
impl pioneer_mcp::McpRuntimeSession for FakeMcpRuntimeSession {
    fn initial_catalog(&self) -> &pioneer_mcp::McpCatalogSnapshot {
        &self.catalog
    }

    async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
        std::future::pending::<pioneer_mcp::McpSessionEvent>().await
    }

    async fn refresh_catalog(
        &mut self,
    ) -> Result<pioneer_mcp::McpCatalogSnapshot, pioneer_mcp::McpRuntimeError> {
        Ok(self.catalog.clone())
    }

    async fn call_tool(
        &mut self,
        raw_tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<pioneer_mcp::McpToolCallResult, pioneer_mcp::McpRuntimeError> {
        Ok(pioneer_mcp::McpToolCallResult {
            content: json!([{
                "type": "text",
                "text": format!("called {raw_tool_name}")
            }]),
            structured_content: Some(json!({
                "tool": raw_tool_name,
                "arguments": arguments,
            })),
            is_error: false,
            duration_ms: 1,
            meta: None,
        })
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_list_empty_then_install_stdio_persists_redacts_and_notifies() {
    let base_dir = unique_temp_dir("mcp_stdio_install");
    std::fs::create_dir_all(&base_dir).expect("must create test settings dir");
    let settings_path = base_dir.join("gateway-settings.toml");
    let (gateway_secrets, secret_store) = test_gateway_secrets_with_store();

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        gateway_secrets.clone(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_list_empty_000001",
        "method": "mcp/list",
        "params": {"workspace_id": workspace_id}
    });
    processor
        .process_request(connection_id, &list_request.to_string())
        .await;
    let list_response = recv_response_by_id(&mut rx, "mcp_list_empty_000001").await;
    let list_payload: McpListResponse =
        serde_json::from_value(list_response.result).expect("mcp/list payload decode");
    assert_eq!(list_payload.snapshot_version, 0);
    assert!(list_payload.servers.is_empty());

    let secret = "re_xxxxxxxxx";
    let install_config = json!({
        "mcpServers": {
            "resend": {
                "command": "npx",
                "args": ["-y", "resend-mcp"],
                "env": {"RESEND_API_KEY": secret}
            }
        }
    });
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_install_stdio0001",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "scope_kind": "workspace",
            "enabled": true,
            "config_json": install_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;

    let install_response = recv_response_by_id(&mut rx, "mcp_install_stdio0001").await;
    let install_response_json =
        serde_json::to_string(&install_response).expect("mcp/install response serialize");
    assert!(!install_response_json.contains(secret));
    let install_payload: McpInstallResponse =
        serde_json::from_value(install_response.result).expect("mcp/install payload decode");
    assert_eq!(install_payload.status, McpInstallStatus::Ok);
    assert_eq!(install_payload.audit.events_written, 1);
    assert_eq!(install_payload.servers.len(), 1);
    let installed = &install_payload.servers[0];
    assert_eq!(installed.name, "resend");
    assert_eq!(installed.status, McpInstallResultStatus::Installed);
    let installed_server = installed.server.as_ref().expect("installed server item");
    match &installed_server.transport {
        McpTransportSummary::Stdio { command } => assert_eq!(command, "npx"),
        other => panic!("expected stdio transport, got {other:?}"),
    }
    assert_eq!(installed_server.runtime.state, McpRuntimeState::NotStarted);
    assert!(!installed_server.runtime.live);
    assert!(installed_server.policy.enabled);
    assert!(installed_server.policy.allow_implicit_invocation);

    let changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;
    let changed_payload: McpChangedNotification =
        serde_json::from_value(changed.params.expect("mcp/changed params expected"))
            .expect("mcp/changed payload decode");
    assert_eq!(changed_payload.snapshot_version, 1);
    assert!(changed_payload.changed.iter().any(|change| {
        change.name == "resend"
            && change.source_kind == McpSourceKind::Config
            && change.action == McpChangedAction::Install
    }));

    let status_changed =
        recv_notification_by_method(&mut rx, events::MCP_SERVER_STATUS_CHANGED).await;
    let status_payload: pioneer_protocol::McpServerStatusChangedNotification =
        serde_json::from_value(status_changed.params.expect("status params expected"))
            .expect("status payload decode");
    assert_eq!(status_payload.server.name, "resend");
    assert_eq!(
        status_payload.server.runtime.state,
        McpRuntimeState::Starting
    );

    let catalog_changed =
        recv_notification_by_method(&mut rx, events::MCP_SERVER_CATALOG_CHANGED).await;
    let catalog_payload: pioneer_protocol::McpServerCatalogChangedNotification =
        serde_json::from_value(catalog_changed.params.expect("catalog params expected"))
            .expect("catalog payload decode");
    assert_eq!(catalog_payload.name, "resend");
    assert_eq!(catalog_payload.tools_count, 2);
    assert_eq!(catalog_payload.resources_count, 1);
    assert_eq!(catalog_payload.resource_templates_count, 1);
    assert_eq!(catalog_payload.prompts_count, 1);

    let status_changed =
        recv_notification_by_method(&mut rx, events::MCP_SERVER_STATUS_CHANGED).await;
    let status_payload: pioneer_protocol::McpServerStatusChangedNotification =
        serde_json::from_value(status_changed.params.expect("ready status params expected"))
            .expect("ready status payload decode");
    assert_eq!(status_payload.server.runtime.state, McpRuntimeState::Ready);
    assert!(status_payload.server.runtime.live);

    let row = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find should succeed")
        .expect("resend MCP installation should exist");
    let db_json = format!(
        "{}{}{}",
        row.transport_json, row.source_ref, row.secret_refs_json
    );
    assert!(!db_json.contains(secret));
    let secret_refs =
        serde_json::from_str::<Vec<pioneer_mcp::McpSecretRef>>(row.secret_refs_json.as_str())
            .expect("secret refs should decode");
    assert_eq!(secret_refs.len(), 1);
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(secret_refs[0].ref_id.as_str())
            .expect("MCP secret should read from keystore"),
        Some(secret.to_owned())
    );
    assert_eq!(
        secret_store
            .list(SecretFilter::Kind(SecretKind::McpSecret))
            .expect("MCP secret metadata should list")
            .len(),
        1
    );
    if settings_path.exists() {
        let settings_content =
            std::fs::read_to_string(&settings_path).expect("read gateway settings");
        assert!(!settings_content.contains(secret));
        assert!(!settings_content.contains("[mcp]"));
        assert!(!settings_content.contains("[mcp.secrets]"));
    }

    let audit_rows = crud_store_for_assert
        .list_recent_mcp_audit_event_records("resend", 16)
        .await
        .expect("MCP audit list should succeed");
    assert!(audit_rows.iter().any(|row| row.action == "install"));
    assert!(audit_rows.iter().any(|row| row.action == "start"));
    assert!(audit_rows.iter().any(|row| row.action == "started"));
    assert!(
        audit_rows
            .iter()
            .any(|row| row.action == "catalog_refreshed")
    );
    assert!(
        audit_rows
            .iter()
            .all(|row| !row.details_json.contains(secret))
    );

    crud_store_for_assert
        .upsert_mcp_server_catalog_snapshot(
            &pioneer_crud::McpServerCatalogSnapshotRecord {
                server_installation_id: row.id.clone().expect("MCP installation id expected"),
                catalog_version: "catalog-v1".to_owned(),
                server_info_json: "{}".to_owned(),
                server_instructions_hash: None,
                tools_json: r#"[{"name":"send"},{"name":"domains"}]"#.to_owned(),
                resources_json: r#"[{"uri":"resend://account"}]"#.to_owned(),
                resource_templates_json: r#"[{"uriTemplate":"resend://{id}"}]"#.to_owned(),
                prompts_json: r#"[{"name":"draft"}]"#.to_owned(),
                generated_at_unix: 1_700_000_000,
            },
            1_700_000_000,
        )
        .await
        .expect("MCP catalog snapshot upsert should succeed");

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_list_after_000001",
        "method": "mcp/list",
        "params": {"workspace_id": workspace_id}
    });
    processor
        .process_request(connection_id, &list_request.to_string())
        .await;
    let list_response = recv_response_by_id(&mut rx, "mcp_list_after_000001").await;
    let list_payload: McpListResponse =
        serde_json::from_value(list_response.result).expect("mcp/list payload decode");
    assert_eq!(list_payload.servers.len(), 1);
    assert_eq!(list_payload.servers[0].name, "resend");
    assert_eq!(
        list_payload.servers[0].runtime.state,
        McpRuntimeState::Ready
    );
    assert!(list_payload.servers[0].runtime.live);
    assert_eq!(list_payload.servers[0].tools_count, 2);
    assert_eq!(list_payload.servers[0].resources_count, 1);
    assert_eq!(list_payload.servers[0].resource_templates_count, 1);
    assert_eq!(list_payload.servers[0].prompts_count, 1);

    let policy_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_policy_set_000001",
        "method": "mcp/policy/set",
        "params": {
            "workspace_id": workspace_id,
            "name": "resend",
            "enabled": false,
            "allow_implicit_invocation": false
        }
    });
    processor
        .process_request(connection_id, &policy_request.to_string())
        .await;
    let policy_response = recv_response_by_id(&mut rx, "mcp_policy_set_000001").await;
    let policy_payload: McpPolicySetResponse =
        serde_json::from_value(policy_response.result).expect("mcp/policy/set payload decode");
    assert_eq!(policy_payload.policy.name, "resend");
    assert!(!policy_payload.policy.enabled);
    assert!(!policy_payload.policy.allow_implicit_invocation);
    assert!(!policy_payload.server.policy.enabled);
    assert!(!policy_payload.server.policy.allow_implicit_invocation);
    assert_eq!(
        policy_payload.server.runtime.state,
        McpRuntimeState::Disabled
    );

    let changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;
    let changed_payload: McpChangedNotification =
        serde_json::from_value(changed.params.expect("mcp/changed params expected"))
            .expect("mcp/changed policy payload decode");
    assert!(changed_payload.changed.iter().any(|change| {
        change.name == "resend"
            && change.source_kind == McpSourceKind::Config
            && change.action == McpChangedAction::Policy
    }));

    let row = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find after policy should succeed")
        .expect("resend MCP installation should exist after policy");
    assert!(!row.enabled);
    assert!(!row.allow_implicit_invocation);

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_install_http_disabled_persists_and_lists_disabled_state() {
    let base_dir = unique_temp_dir("mcp_http_disabled");
    std::fs::create_dir_all(&base_dir).expect("must create test settings dir");
    let settings_path = base_dir.join("gateway-settings.toml");
    let (gateway_secrets, _secret_store) = test_gateway_secrets_with_store();

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        gateway_secrets.clone(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let secret = "Bearer re_xxxxxxxxx";
    let install_config = json!({
        "mcpServers": {
            "resend": {
                "url": "http://127.0.0.1:3000/mcp",
                "headers": {"Authorization": secret},
                "disabled": true
            }
        }
    });
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_install_http_0001",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "config_json": install_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, "mcp_install_http_0001").await;
    let response_json = serde_json::to_string(&response).expect("mcp/install response serialize");
    assert!(!response_json.contains(secret));
    let payload: McpInstallResponse =
        serde_json::from_value(response.result).expect("mcp/install payload decode");
    assert_eq!(payload.status, McpInstallStatus::Ok);
    let server = payload.servers[0]
        .server
        .as_ref()
        .expect("installed HTTP server");
    match &server.transport {
        McpTransportSummary::StreamableHttp { url } => {
            assert_eq!(url, "http://127.0.0.1:3000/mcp")
        }
        other => panic!("expected streamable_http transport, got {other:?}"),
    }
    assert_eq!(server.runtime.state, McpRuntimeState::Disabled);
    assert_eq!(server.status, McpServerStatus::Disabled);
    assert!(!server.policy.enabled);
    assert!(server.policy.allow_implicit_invocation);

    let _changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;

    let row = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find should succeed")
        .expect("resend MCP installation should exist");
    assert!(!row.transport_json.contains(secret));
    assert!(!row.source_ref.contains(secret));
    assert!(!row.secret_refs_json.contains(secret));
    assert!(!row.enabled);
    assert!(row.allow_implicit_invocation);
    let secret_refs =
        serde_json::from_str::<Vec<pioneer_mcp::McpSecretRef>>(row.secret_refs_json.as_str())
            .expect("secret refs should decode");
    assert_eq!(secret_refs.len(), 1);
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(secret_refs[0].ref_id.as_str())
            .expect("MCP header secret should read from keystore"),
        Some(secret.to_owned())
    );
    if settings_path.exists() {
        let settings_content =
            std::fs::read_to_string(&settings_path).expect("read gateway settings");
        assert!(!settings_content.contains(secret));
        assert!(!settings_content.contains("[mcp]"));
        assert!(!settings_content.contains("[mcp.secrets]"));
    }

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_list_http_0000001",
        "method": "mcp/list",
        "params": {"workspace_id": workspace_id}
    });
    processor
        .process_request(connection_id, &list_request.to_string())
        .await;
    let list_response = recv_response_by_id(&mut rx, "mcp_list_http_0000001").await;
    let list_payload: McpListResponse =
        serde_json::from_value(list_response.result).expect("mcp/list payload decode");
    assert_eq!(list_payload.servers.len(), 1);
    assert_eq!(
        list_payload.servers[0].runtime.state,
        McpRuntimeState::Disabled
    );

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_update_deletes_stale_keystore_refs_after_success() {
    let base_dir = unique_temp_dir("mcp_update_secret_cleanup");
    std::fs::create_dir_all(&base_dir).expect("must create test settings dir");
    let settings_path = base_dir.join("gateway-settings.toml");
    let (gateway_secrets, _secret_store) = test_gateway_secrets_with_store();

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        gateway_secrets.clone(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let old_secret = "old_resend_secret";
    let install_config = json!({
        "mcpServers": {
            "resend": {
                "command": "npx",
                "env": {"RESEND_API_KEY": old_secret},
                "disabled": true
            }
        }
    });
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_update_install001",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "config_json": install_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;
    let install_response = recv_response_by_id(&mut rx, "mcp_update_install001").await;
    let install_response_json =
        serde_json::to_string(&install_response).expect("install response serialize");
    assert!(!install_response_json.contains(old_secret));
    let _changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;

    let row = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find should succeed")
        .expect("resend MCP installation should exist");
    let old_refs =
        serde_json::from_str::<Vec<pioneer_mcp::McpSecretRef>>(row.secret_refs_json.as_str())
            .expect("old refs should decode");
    assert_eq!(old_refs.len(), 1);
    let old_ref_id = old_refs[0].ref_id.clone();
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(old_ref_id.as_str())
            .expect("old secret should read"),
        Some(old_secret.to_owned())
    );

    let new_secret = "new_resend_secret";
    let update_config = json!({
        "mcpServers": {
            "resend": {
                "command": "npx",
                "env": {"RESEND_TOKEN": new_secret}
            }
        }
    });
    let update_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_update_update0001",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "config_json": update_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &update_request.to_string())
        .await;
    let update_response = recv_response_by_id(&mut rx, "mcp_update_update0001").await;
    let update_response_json =
        serde_json::to_string(&update_response).expect("update response serialize");
    assert!(!update_response_json.contains(old_secret));
    assert!(!update_response_json.contains(new_secret));
    let update_payload: McpInstallResponse =
        serde_json::from_value(update_response.result).expect("update payload decode");
    assert_eq!(update_payload.status, McpInstallStatus::Ok);
    assert_eq!(
        update_payload.servers[0].status,
        McpInstallResultStatus::Updated
    );

    let row = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find should succeed")
        .expect("resend MCP installation should exist");
    let new_refs =
        serde_json::from_str::<Vec<pioneer_mcp::McpSecretRef>>(row.secret_refs_json.as_str())
            .expect("new refs should decode");
    assert_eq!(new_refs.len(), 1);
    let new_ref_id = new_refs[0].ref_id.clone();
    assert_ne!(old_ref_id, new_ref_id);
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(old_ref_id.as_str())
            .expect("old secret should be deleted"),
        None
    );
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(new_ref_id.as_str())
            .expect("new secret should read"),
        Some(new_secret.to_owned())
    );
    assert!(!row.transport_json.contains(old_secret));
    assert!(!row.transport_json.contains(new_secret));
    assert!(!row.source_ref.contains(old_secret));
    assert!(!row.source_ref.contains(new_secret));
    assert!(!row.secret_refs_json.contains(old_secret));
    assert!(!row.secret_refs_json.contains(new_secret));
    if settings_path.exists() {
        let settings_content =
            std::fs::read_to_string(&settings_path).expect("read gateway settings");
        assert!(!settings_content.contains(old_secret));
        assert!(!settings_content.contains(new_secret));
        assert!(!settings_content.contains("[mcp]"));
        assert!(!settings_content.contains("[mcp.secrets]"));
    }

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_details_and_uninstall_return_full_ui_state_and_remove_server() {
    let base_dir = unique_temp_dir("mcp_details_uninstall");
    std::fs::create_dir_all(&base_dir).expect("must create test settings dir");
    let (gateway_secrets, _secret_store) = test_gateway_secrets_with_store();

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        gateway_secrets.clone(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let secret = "re_xxxxxxxxx";
    let install_config = json!({
        "mcpServers": {
            "resend": {
                "command": "npx",
                "args": ["-y", "resend-mcp"],
                "env": {"RESEND_API_KEY": secret}
            }
        }
    });
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_details_install01",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "config_json": install_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;
    let install_response = recv_response_by_id(&mut rx, "mcp_details_install01").await;
    let install_payload: McpInstallResponse =
        serde_json::from_value(install_response.result).expect("mcp/install payload decode");
    let installed_server = install_payload.servers[0]
        .server
        .as_ref()
        .expect("installed server item");
    let server_id = installed_server.id.clone();

    let changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;
    let changed_payload: McpChangedNotification =
        serde_json::from_value(changed.params.expect("mcp/changed params expected"))
            .expect("mcp/changed install payload decode");
    assert_eq!(changed_payload.workspace_id, workspace_id);

    for attempt in 0..20 {
        let request_id = format!("mcp_details_wait_{attempt:04}");
        let list_request = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "mcp/list",
            "params": {"workspace_id": workspace_id}
        });
        processor
            .process_request(connection_id, &list_request.to_string())
            .await;
        let list_response = recv_response_by_id(&mut rx, request_id.as_str()).await;
        let list_payload: McpListResponse =
            serde_json::from_value(list_response.result).expect("mcp/list payload decode");
        if list_payload
            .servers
            .iter()
            .any(|server| server.name == "resend" && server.status == McpServerStatus::Ready)
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    crud_store_for_assert
        .replace_turn_mcp_bindings(
            "turn_mcp_details",
            &[pioneer_crud::TurnMcpBindingRecord {
                server_installation_id: server_id.clone(),
                server_name: "resend".to_owned(),
                raw_tool_name: "send".to_owned(),
                callable_name: "mcp_resend_send".to_owned(),
                catalog_version: "catalog-v1".to_owned(),
                fingerprint: installed_server.fingerprint.clone(),
            }],
            1_700_000_010,
        )
        .await
        .expect("MCP binding insert should succeed");

    let details_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_details_000000001",
        "method": "mcp/server/details",
        "params": {
            "workspace_id": workspace_id,
            "server_id": server_id
        }
    });
    processor
        .process_request(connection_id, &details_request.to_string())
        .await;
    let details_response = recv_response_by_id(&mut rx, "mcp_details_000000001").await;
    let details_payload: McpServerDetailsResponse =
        serde_json::from_value(details_response.result).expect("mcp/details payload decode");
    assert_eq!(details_payload.server.name, "resend");
    assert_eq!(details_payload.server.status, McpServerStatus::Ready);
    assert!(details_payload.health.runtime.live);
    assert_eq!(details_payload.catalog.tools.len(), 2);
    assert_eq!(details_payload.catalog.resources.len(), 1);
    assert_eq!(details_payload.catalog.resource_templates.len(), 1);
    assert_eq!(details_payload.catalog.prompts.len(), 1);
    assert!(
        details_payload
            .audit
            .iter()
            .any(|event| event.action == "install")
    );
    assert!(
        details_payload
            .audit
            .iter()
            .all(|event| !event.details.to_string().contains(secret))
    );
    assert!(
        details_payload
            .recent_bindings
            .iter()
            .any(|binding| { binding.server_name == "resend" && binding.raw_tool_name == "send" })
    );

    let row_before_uninstall = crud_store_for_assert
        .find_mcp_server_installation("workspace", workspace_id.as_str(), "resend")
        .await
        .expect("MCP installation find before uninstall should succeed")
        .expect("resend MCP installation should exist before uninstall");
    let secret_refs_before_uninstall = serde_json::from_str::<Vec<pioneer_mcp::McpSecretRef>>(
        row_before_uninstall.secret_refs_json.as_str(),
    )
    .expect("secret refs should decode before uninstall");
    assert_eq!(secret_refs_before_uninstall.len(), 1);
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(secret_refs_before_uninstall[0].ref_id.as_str())
            .expect("secret should exist before uninstall"),
        Some(secret.to_owned())
    );

    let uninstall_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_uninstall_0000001",
        "method": "mcp/uninstall",
        "params": {
            "workspace_id": workspace_id,
            "name": "resend"
        }
    });
    processor
        .process_request(connection_id, &uninstall_request.to_string())
        .await;
    let uninstall_response = recv_response_by_id(&mut rx, "mcp_uninstall_0000001").await;
    let uninstall_payload: McpUninstallResponse =
        serde_json::from_value(uninstall_response.result).expect("mcp/uninstall payload decode");
    assert!(uninstall_payload.removed);
    assert_eq!(uninstall_payload.name, "resend");
    assert_eq!(uninstall_payload.audit.events_written, 1);

    let changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;
    let changed_payload: McpChangedNotification =
        serde_json::from_value(changed.params.expect("mcp/changed params expected"))
            .expect("mcp/changed uninstall payload decode");
    assert_eq!(changed_payload.workspace_id, workspace_id);
    assert!(changed_payload.changed.iter().any(|change| {
        change.name == "resend"
            && change.source_kind == McpSourceKind::Config
            && change.action == McpChangedAction::Uninstall
    }));

    let rows = crud_store_for_assert
        .list_mcp_server_installations("workspace", workspace_id.as_str())
        .await
        .expect("MCP installations list should succeed");
    assert!(rows.is_empty());
    let catalog = crud_store_for_assert
        .find_mcp_server_catalog_snapshot(uninstall_payload.server_id.as_str())
        .await
        .expect("MCP catalog lookup after uninstall should succeed");
    assert!(catalog.is_none());
    assert_eq!(
        gateway_secrets
            .get_mcp_secret(secret_refs_before_uninstall[0].ref_id.as_str())
            .expect("secret should be deleted after uninstall"),
        None
    );
    let uninstall_response_json =
        serde_json::to_string(&uninstall_payload).expect("uninstall payload serialize");
    assert!(!uninstall_response_json.contains(secret));
    let audit_rows = crud_store_for_assert
        .list_recent_mcp_audit_event_records("resend", 16)
        .await
        .expect("MCP audit list after uninstall should succeed");
    assert!(
        audit_rows
            .iter()
            .all(|row| !row.details_json.contains(secret))
    );

    let missing_details_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_details_missing01",
        "method": "mcp/server/details",
        "params": {
            "workspace_id": workspace_id,
            "server_id": uninstall_payload.server_id
        }
    });
    processor
        .process_request(connection_id, &missing_details_request.to_string())
        .await;
    let missing_details = recv_error_by_id(&mut rx, "mcp_details_missing01").await;
    let code = missing_details
        .error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert_eq!(code, "mcp.not_found");

    let _ = std::fs::remove_dir_all(base_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_install_itemizes_valid_and_invalid_servers() {
    let base_dir = unique_temp_dir("mcp_itemized_install");
    std::fs::create_dir_all(&base_dir).expect("must create test settings dir");

    let (tx, mut rx) = mpsc::channel(64);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let crud_store_for_assert = crud_store.clone();

    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager,
        workspace_manager,
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );
    processor.set_mcp_runtime_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));

    let install_config = json!({
        "mcpServers": {
            "alpha": {"command": "npx"},
            "bad name": {}
        }
    });
    let install_request = json!({
        "jsonrpc": "2.0",
        "id": "mcp_install_mix_00001",
        "method": "mcp/install",
        "params": {
            "workspace_id": workspace_id,
            "config_json": install_config.to_string()
        }
    });
    processor
        .process_request(connection_id, &install_request.to_string())
        .await;

    let response = recv_response_by_id(&mut rx, "mcp_install_mix_00001").await;
    let payload: McpInstallResponse =
        serde_json::from_value(response.result).expect("mcp/install payload decode");
    assert_eq!(payload.status, McpInstallStatus::Partial);
    assert_eq!(payload.audit.events_written, 1);
    assert_eq!(payload.servers.len(), 2);
    assert!(payload.servers.iter().any(|item| {
        item.name == "alpha"
            && item.status == McpInstallResultStatus::Installed
            && item.server.is_some()
    }));
    assert!(payload.servers.iter().any(|item| {
        item.name == "bad name"
            && item.status == McpInstallResultStatus::ValidationError
            && item.server.is_none()
            && item
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_server_name")
    }));

    let changed = recv_notification_by_method(&mut rx, events::MCP_CHANGED).await;
    let changed_payload: McpChangedNotification =
        serde_json::from_value(changed.params.expect("mcp/changed params expected"))
            .expect("mcp/changed payload decode");
    assert_eq!(changed_payload.changed.len(), 1);
    assert_eq!(changed_payload.changed[0].name, "alpha");

    let rows = crud_store_for_assert
        .list_mcp_server_installations("workspace", workspace_id.as_str())
        .await
        .expect("MCP installations list should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "alpha");

    let _ = std::fs::remove_dir_all(base_dir);
}

async fn setup_workspace_manager() -> (Arc<WorkspaceManager>, Arc<CrudStore>, String) {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("must connect to sqlite memory");
    Migrator::up(&connection, None)
        .await
        .expect("migrations must succeed");
    bootstrap(&connection)
        .await
        .expect("gateway bootstrap should create default workspace");
    let workspace_manager = Arc::new(WorkspaceManager::new(connection.clone()));
    let workspaces = workspace_manager
        .list_workspaces()
        .await
        .expect("workspace/list should succeed in tests");
    let workspace_id = workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
        .or_else(|| workspaces.first())
        .expect("default workspace should exist after bootstrap")
        .id
        .clone();
    (
        workspace_manager,
        Arc::new(CrudStore::new(connection)),
        workspace_id,
    )
}

async fn setup_workspace_message_processor() -> (
    MessageProcessor,
    Arc<SessionManager>,
    crate::session::ConnectionId,
    mpsc::Receiver<Message>,
    Arc<WorkspaceManager>,
    String,
) {
    let (tx, rx) = mpsc::channel(8);
    let session_manager = Arc::new(SessionManager::new());
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = MessageProcessor::new(
        thread_manager,
        test_provider(),
        session_manager.clone(),
        workspace_manager.clone(),
        crud_store,
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    (
        processor,
        session_manager,
        connection_id,
        rx,
        workspace_manager,
        workspace_id,
    )
}

struct Phase13CompactionHarness {
    processor: Arc<MessageProcessor>,
    crud_store: Arc<CrudStore>,
    workspace_id: String,
}

async fn setup_phase_13_compaction_harness(
    provider_registry: Arc<pioneer_provider::ProviderRegistry>,
) -> Phase13CompactionHarness {
    let session_manager = Arc::new(SessionManager::new());
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        phase_13_summary_config(),
        phase_13_context_budget(),
        test_tool_loop_config(),
    ));

    Phase13CompactionHarness {
        processor,
        crud_store,
        workspace_id,
    }
}

fn phase_13_summary_config() -> super::summary::SummaryConfig {
    super::summary::SummaryConfig {
        summary_model: Some("test-model".to_owned()),
        summary_model_provider: Some("summary-capture".to_owned()),
        title_model: Some("test-model".to_owned()),
        title_model_provider: Some("summary-capture".to_owned()),
    }
}

fn phase_13_context_budget() -> super::ContextBudget {
    super::ContextBudget {
        max_context_tokens: 1_000,
        response_reserve_tokens: 200,
    }
}

fn phase_13_provider_registry(
    provider: Arc<CaptureSummaryProvider>,
) -> Arc<pioneer_provider::ProviderRegistry> {
    Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "summary-capture",
        provider,
    ))
}

fn phase_13_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn phase_13_test_thread(
    workspace_id: &str,
    thread_id: &str,
    timestamp: i64,
) -> pioneer_protocol::Thread {
    pioneer_protocol::Thread {
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: Some("Phase 13 Thread".to_owned()),
        preview: "phase 13 compaction".to_owned(),
        mode: ThreadMode::Agent,
        model: "test-model".to_owned(),
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

fn phase_13_turn(turn_id: &str, status: TurnStatus) -> Turn {
    Turn {
        id: turn_id.to_owned(),
        status,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    }
}

fn phase_13_long_text(marker: &str) -> String {
    format!("{marker} {}", "alpha beta gamma delta epsilon ".repeat(60))
}

async fn seed_phase_13_compaction_thread(
    crud_store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    raw_marker: &str,
) {
    let base_timestamp = phase_13_now_secs();
    for index in 0..2 {
        let timestamp = base_timestamp + i64::from(index) * 3;
        let thread = phase_13_test_thread(workspace_id, thread_id, timestamp);
        let turn_id = format!("{thread_id}turn{index}");
        let started_turn = phase_13_turn(turn_id.as_str(), TurnStatus::InProgress);
        let user_text = phase_13_long_text(&format!("{raw_marker}_user_{index}"));
        let assistant_text = phase_13_long_text(&format!("{raw_marker}_assistant_{index}"));

        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &started_turn,
                &[UserInput::Text {
                    text: user_text,
                    text_elements: Vec::new(),
                }],
            )
            .await
            .expect("turn start should materialize");

        crud_store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.clone(),
                    item: TurnItem::AgentMessage {
                        id: format!("agent_message_{index}"),
                        text: assistant_text,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("assistant item should materialize");

        crud_store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: phase_13_turn(turn_id.as_str(), TurnStatus::Completed),
                },
                timestamp + 2,
            )
            .await
            .expect("turn completion should materialize");
    }
}

fn phase_13_prompt_section_contribution() -> HookContribution {
    HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: pioneer_hooks::HookContributionId::new("phase13.prompt_section")
            .expect("valid contribution id"),
        section_id: HookSectionId::new("phase13.prompt_section").expect("valid section id"),
        title: None,
        domain: HookDomain::new("test.phase13").expect("valid hook domain"),
        priority: 100,
        content: HookPromptContent::new("this must not enter the summary prompt")
            .expect("valid prompt content"),
        max_chars: None,
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    })
}

fn phase_13_history_value(messages: &[pioneer_provider::ChatMessage]) -> serde_json::Value {
    serde_json::to_value(messages).expect("chat history should serialize")
}

#[tokio::test]
async fn phase_13_pre_compaction_hook_is_dispatched() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness = setup_phase_13_compaction_harness(phase_13_provider_registry(provider)).await;
    let thread_id = generate_test_request_id("p13", "dispatch");
    let turn_id = generate_test_request_id("turn", "dispatch");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_DISPATCH_RAW",
    )
    .await;

    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Succeed {
                contributions: Vec::new(),
            },
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::BestEffort,
        ),
    )
    .await;
    assert!(harness.processor.agent_manager.has_hook_runtime().await);

    let _history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), turn_id.as_str())
        .await;

    let calls = calls.lock().expect("phase 13 calls lock");
    assert_eq!(calls.len(), 1);
    let request = &calls[0];
    assert_eq!(request.phase, HookPhase::TurnPreCompaction);
    assert_eq!(request.input.kind.as_str(), "turn.pre_compaction");
    assert_eq!(
        request
            .context
            .workspace_id
            .as_ref()
            .expect("context workspace")
            .as_str(),
        harness.workspace_id.as_str()
    );
    assert_eq!(
        request
            .context
            .thread_id
            .as_ref()
            .expect("context thread")
            .as_str(),
        thread_id.as_str()
    );
    assert_eq!(
        request
            .context
            .turn_id
            .as_ref()
            .expect("context turn")
            .as_str(),
        turn_id.as_str()
    );
    assert_eq!(
        request.context.mode,
        Some(pioneer_hooks::HookContextMode::System)
    );
    assert_eq!(
        request.context.actor.as_ref().expect("context actor").kind,
        pioneer_hooks::HookActorKind::Service
    );

    let HookInputPayload::TurnPreCompaction(payload) = &request.input.payload else {
        panic!("pre-compaction payload should be typed");
    };
    assert_eq!(payload.workspace_id.as_str(), harness.workspace_id.as_str());
    assert_eq!(payload.thread_id.as_str(), thread_id.as_str());
    assert_eq!(
        payload.turn_id.as_ref().expect("payload turn").as_str(),
        turn_id.as_str()
    );
    assert!(payload.compaction_id.as_str().starts_with("cmp_"));
    assert_eq!(
        payload.trigger,
        TurnPreCompactionTrigger::ContextBudgetThreshold
    );
    assert_eq!(
        payload.source_range.source_kind,
        pioneer_hooks::TurnPreCompactionSourceKind::ConversationHistory
    );
    assert_eq!(payload.source_range.loaded_completed_turn_count, 2);
    assert_eq!(payload.source_range.source_entry_count, 2);
    assert_eq!(
        payload.summary_policy.strategy,
        TurnPreCompactionSummaryStrategy::ProgressiveFullHistorySummary
    );
    assert_eq!(
        payload.retention_policy.raw_turn_retention,
        TurnPreCompactionRawTurnRetention::RetainOriginalTurns
    );
    assert_eq!(
        payload.retention_policy.summary_storage,
        TurnPreCompactionSummaryStorage::ThreadSummary
    );
}

#[tokio::test]
async fn phase_13_empty_hook_runtime_preserves_current_compaction_behavior() {
    let no_runtime_provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let no_runtime_harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(no_runtime_provider.clone()))
            .await;
    let no_runtime_thread_id = generate_test_request_id("p13", "noruntime");
    seed_phase_13_compaction_thread(
        no_runtime_harness.crud_store.as_ref(),
        no_runtime_harness.workspace_id.as_str(),
        no_runtime_thread_id.as_str(),
        "PHASE13_EMPTY_RAW",
    )
    .await;
    let no_runtime_history = no_runtime_harness
        .processor
        .load_conversation_history(no_runtime_thread_id.as_str(), "turn_no_runtime")
        .await;
    let no_runtime_summary = no_runtime_harness
        .crud_store
        .get_thread_summary(no_runtime_thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    let empty_runtime_provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let empty_runtime_harness = setup_phase_13_compaction_harness(phase_13_provider_registry(
        empty_runtime_provider.clone(),
    ))
    .await;
    let empty_runtime_thread_id = generate_test_request_id("p13", "emptyruntime");
    seed_phase_13_compaction_thread(
        empty_runtime_harness.crud_store.as_ref(),
        empty_runtime_harness.workspace_id.as_str(),
        empty_runtime_thread_id.as_str(),
        "PHASE13_EMPTY_RAW",
    )
    .await;
    install_test_hook_runtime(
        &empty_runtime_harness.processor,
        phase_13_empty_hook_runtime(),
    )
    .await;
    let empty_runtime_history = empty_runtime_harness
        .processor
        .load_conversation_history(empty_runtime_thread_id.as_str(), "turn_empty_runtime")
        .await;
    let empty_runtime_summary = empty_runtime_harness
        .crud_store
        .get_thread_summary(empty_runtime_thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    assert_eq!(
        phase_13_history_value(no_runtime_history.as_slice()),
        phase_13_history_value(empty_runtime_history.as_slice())
    );
    assert_eq!(no_runtime_summary, empty_runtime_summary);
    assert_eq!(no_runtime_provider.call_count(), 1);
    assert_eq!(empty_runtime_provider.call_count(), 1);
}

#[tokio::test]
async fn phase_13_best_effort_hook_failure_keeps_compaction_behavior() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    let thread_id = generate_test_request_id("p13", "besteffort");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_BESTEFFORT_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Fail,
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::BestEffort,
        ),
    )
    .await;

    let history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_best_effort")
        .await;
    let summary = harness
        .crud_store
        .get_thread_summary(thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    assert_eq!(calls.lock().expect("phase 13 calls lock").len(), 1);
    assert_eq!(provider.call_count(), 1);
    assert_eq!(
        summary.as_ref().map(|(summary, _)| summary.as_str()),
        Some("compressed summary")
    );
    assert_eq!(history.len(), 1);
    assert!(history[0].content.contains("compressed summary"));
}

#[tokio::test]
async fn phase_13_fallback_hook_failure_keeps_compaction_and_ignores_fallback_contribution() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    let thread_id = generate_test_request_id("p13", "fallback");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_FALLBACK_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime_with_fallback(
            calls.clone(),
            Phase13HookBehavior::Fail,
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::Fallback,
            vec![phase_13_prompt_section_contribution()],
        ),
    )
    .await;

    let history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_fallback")
        .await;
    let summary = harness
        .crud_store
        .get_thread_summary(thread_id.as_str())
        .await
        .expect("summary lookup succeeds");
    let summary_prompt = provider.snapshot_requests()[0].messages[0].content.clone();

    assert_eq!(calls.lock().expect("phase 13 calls lock").len(), 1);
    assert_eq!(provider.call_count(), 1);
    assert_eq!(
        summary.as_ref().map(|(summary, _)| summary.as_str()),
        Some("compressed summary")
    );
    assert_eq!(history.len(), 1);
    assert!(!summary_prompt.contains("this must not enter the summary prompt"));
}

#[tokio::test]
async fn phase_13_required_hook_failure_skips_summary_update_and_uses_fallback() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    let thread_id = generate_test_request_id("p13", "requiredfail");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_REQUIRED_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Fail,
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::Required,
        ),
    )
    .await;

    let history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_required_failure")
        .await;
    let summary = harness
        .crud_store
        .get_thread_summary(thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    assert_eq!(calls.lock().expect("phase 13 calls lock").len(), 1);
    assert_eq!(provider.call_count(), 0);
    assert!(summary.is_none());
    assert!(!history.is_empty());
    assert!(
        history
            .iter()
            .any(|message| message.content.contains("PHASE13_REQUIRED_RAW"))
    );
}

#[tokio::test]
async fn phase_13_fail_closed_hook_failure_skips_summary_update() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    let thread_id = generate_test_request_id("p13", "failclosed");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_FAILCLOSED_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Fail,
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::FailClosed,
        ),
    )
    .await;

    let history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_fail_closed")
        .await;
    let summary = harness
        .crud_store
        .get_thread_summary(thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    assert_eq!(calls.lock().expect("phase 13 calls lock").len(), 1);
    assert_eq!(provider.call_count(), 0);
    assert!(summary.is_none());
    assert!(!history.is_empty());
}

#[tokio::test]
async fn phase_13_deadline_required_timeout_skips_summary_update() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    let thread_id = generate_test_request_id("p13", "deadline");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "PHASE13_DEADLINE_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Pending,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::Required,
        ),
    )
    .await;

    let history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_deadline")
        .await;
    let summary = harness
        .crud_store
        .get_thread_summary(thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    assert_eq!(calls.lock().expect("phase 13 calls lock").len(), 1);
    assert_eq!(provider.call_count(), 0);
    assert!(summary.is_none());
    assert!(!history.is_empty());
}

#[tokio::test]
async fn phase_13_hook_contributions_do_not_mutate_summary() {
    let baseline_provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let baseline_harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(baseline_provider.clone()))
            .await;
    let baseline_thread_id = generate_test_request_id("p13", "baseline");
    seed_phase_13_compaction_thread(
        baseline_harness.crud_store.as_ref(),
        baseline_harness.workspace_id.as_str(),
        baseline_thread_id.as_str(),
        "PHASE13_NONMUTATION_RAW",
    )
    .await;
    let baseline_history = baseline_harness
        .processor
        .load_conversation_history(baseline_thread_id.as_str(), "turn_baseline")
        .await;
    let baseline_summary = baseline_harness
        .crud_store
        .get_thread_summary(baseline_thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    let hook_provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let hook_harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(hook_provider.clone())).await;
    let hook_thread_id = generate_test_request_id("p13", "withcontrib");
    seed_phase_13_compaction_thread(
        hook_harness.crud_store.as_ref(),
        hook_harness.workspace_id.as_str(),
        hook_thread_id.as_str(),
        "PHASE13_NONMUTATION_RAW",
    )
    .await;
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &hook_harness.processor,
        phase_13_hook_runtime(
            calls,
            Phase13HookBehavior::Succeed {
                contributions: vec![phase_13_prompt_section_contribution()],
            },
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::BestEffort,
        ),
    )
    .await;
    let hook_history = hook_harness
        .processor
        .load_conversation_history(hook_thread_id.as_str(), "turn_hook_contrib")
        .await;
    let hook_summary = hook_harness
        .crud_store
        .get_thread_summary(hook_thread_id.as_str())
        .await
        .expect("summary lookup succeeds");

    let baseline_prompt = baseline_provider.snapshot_requests()[0].messages[0]
        .content
        .clone();
    let hook_prompt = hook_provider.snapshot_requests()[0].messages[0]
        .content
        .clone();

    assert_eq!(baseline_prompt, hook_prompt);
    assert_eq!(baseline_summary, hook_summary);
    assert_eq!(
        phase_13_history_value(baseline_history.as_slice()),
        phase_13_history_value(hook_history.as_slice())
    );
    assert!(!hook_prompt.contains("this must not enter the summary prompt"));
}

#[tokio::test]
async fn phase_13_pre_compaction_input_is_bounded() {
    let provider = Arc::new(CaptureSummaryProvider::new("compressed summary"));
    let harness = setup_phase_13_compaction_harness(phase_13_provider_registry(provider)).await;
    let thread_id = generate_test_request_id("p13", "bounded");
    seed_phase_13_compaction_thread(
        harness.crud_store.as_ref(),
        harness.workspace_id.as_str(),
        thread_id.as_str(),
        "VERY_RAW_USER_TEXT_PHASE13",
    )
    .await;
    let omitted_summary_tail = "OMITTED_SUMMARY_TAIL_PHASE13";
    let long_summary = format!("{}{}", "s".repeat(2_100), omitted_summary_tail);
    harness
        .crud_store
        .update_thread_summary(thread_id.as_str(), long_summary.as_str(), 1)
        .await
        .expect("existing summary should update");

    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_test_hook_runtime(
        &harness.processor,
        phase_13_hook_runtime(
            calls.clone(),
            Phase13HookBehavior::Succeed {
                contributions: Vec::new(),
            },
            HookAwaitPolicy::Blocking,
            None,
            HookFailurePolicy::BestEffort,
        ),
    )
    .await;
    let _history = harness
        .processor
        .load_conversation_history(thread_id.as_str(), "turn_bounded")
        .await;

    let calls = calls.lock().expect("phase 13 calls lock");
    let request = calls.first().expect("pre-compaction hook should run");
    let serialized_input =
        serde_json::to_string(&request.input).expect("input should serialize safely");
    assert!(!serialized_input.contains("VERY_RAW_USER_TEXT_PHASE13"));
    assert!(!serialized_input.contains(omitted_summary_tail));

    let HookInputPayload::TurnPreCompaction(payload) = &request.input.payload else {
        panic!("pre-compaction payload should be typed");
    };
    let preview = payload
        .existing_summary_preview
        .as_ref()
        .expect("existing summary preview should be present");
    assert!(preview.truncated);
    assert_eq!(preview.max_chars, 2_000);
    assert_eq!(payload.source_range.existing_summary_turn_count, Some(1));
    assert_eq!(payload.source_range.max_loaded_turns, 200);
    assert_eq!(payload.source_range.source_entry_count, 2);
    assert_eq!(payload.token_budget.max_context_tokens, 1_000);
    assert_eq!(payload.token_budget.response_reserve_tokens, 200);
    assert_eq!(payload.summary_policy.compression_threshold_bps, 8_000);
    assert_eq!(payload.summary_policy.compression_target_bps, 1_000);
}

struct MemoryGatewayHarness {
    processor: Arc<MessageProcessor>,
    crud_store: Arc<CrudStore>,
    workspace_manager: Arc<WorkspaceManager>,
    session_manager: Arc<SessionManager>,
    rx: mpsc::Receiver<Message>,
    connection_id: u64,
    workspace_id: String,
    runtime_home: std::path::PathBuf,
}

struct MemoryAgentE2eHarness {
    processor: Arc<MessageProcessor>,
    crud_store: Arc<CrudStore>,
    workspace_id: String,
    connection_id: u64,
    rx: mpsc::Receiver<Message>,
    runtime_home: std::path::PathBuf,
    _provider_registry: Arc<pioneer_provider::ProviderRegistry>,
}

async fn setup_memory_gateway_harness(case_id: &str, enabled: bool) -> MemoryGatewayHarness {
    let session_manager = Arc::new(SessionManager::new());
    let (tx, rx) = mpsc::channel(32);
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    session_manager
        .set_connection_workspace(connection_id, Some(workspace_id.clone()))
        .await;
    let runtime_home = unique_temp_dir(&format!("memory_{case_id}"));
    std::fs::create_dir_all(runtime_home.as_path()).expect("create memory runtime home");
    let memory_runtime = Arc::new(
        GatewayMemoryRuntime::from_config(
            crud_store.clone(),
            runtime_home.as_path(),
            &GatewayMemoryConfig {
                enabled,
                capsules_dir: "memory/capsules".to_owned(),
                allow_global_user_by_default: true,
                allow_global_agent_by_default: false,
                ..GatewayMemoryConfig::default()
            },
        )
        .expect("memory runtime should initialize"),
    );

    if enabled {
        assert!(memory_runtime.is_enabled());
        assert!(
            memory_runtime
                .capsules_root()
                .expect("enabled runtime should expose capsules root")
                .starts_with(runtime_home.as_path())
        );
    }

    let processor = Arc::new(MessageProcessor::new_with_memory_runtime(
        thread_manager,
        test_provider(),
        session_manager.clone(),
        workspace_manager.clone(),
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
        memory_runtime,
        runtime_home.clone(),
        pioneer_config::GatewayArtifactsConfig::default(),
    ));

    MemoryGatewayHarness {
        processor,
        crud_store,
        workspace_manager,
        session_manager,
        rx,
        connection_id,
        workspace_id,
        runtime_home,
    }
}

async fn setup_memory_agent_e2e_harness(
    case_id: &str,
    provider_registry: Arc<pioneer_provider::ProviderRegistry>,
) -> MemoryAgentE2eHarness {
    let session_manager = Arc::new(SessionManager::new());
    let (tx, rx) = mpsc::channel(128);
    let connection_id = session_manager.register_connection(tx).await;
    let thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    session_manager
        .set_connection_workspace(connection_id, Some(workspace_id.clone()))
        .await;

    let runtime_home = unique_temp_dir(&format!("memory_agent_e2e_{case_id}"));
    std::fs::create_dir_all(runtime_home.as_path()).expect("create memory e2e runtime home");
    let memory_runtime = Arc::new(
        GatewayMemoryRuntime::from_config(
            crud_store.clone(),
            runtime_home.as_path(),
            &GatewayMemoryConfig {
                enabled: true,
                capsules_dir: "memory/capsules".to_owned(),
                allow_global_user_by_default: true,
                allow_global_agent_by_default: false,
                ..GatewayMemoryConfig::default()
            },
        )
        .expect("memory runtime should initialize"),
    );

    let processor = Arc::new(MessageProcessor::new_with_memory_runtime(
        thread_manager,
        provider_registry.clone(),
        session_manager,
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
        memory_runtime,
        runtime_home.clone(),
        pioneer_config::GatewayArtifactsConfig::default(),
    ));
    processor.bind_memory_bridge_if_enabled().await;

    MemoryAgentE2eHarness {
        processor,
        crud_store,
        workspace_id,
        connection_id,
        rx,
        runtime_home,
        _provider_registry: provider_registry,
    }
}

fn workspace_memory_scope(workspace_id: &str) -> MemoryScope {
    MemoryScope {
        kind: MemoryScopeKind::Workspace,
        key: workspace_id.to_owned(),
    }
}

fn user_memory_scope() -> MemoryScope {
    MemoryScope {
        kind: MemoryScopeKind::User,
        key: "default".to_owned(),
    }
}

fn agent_global_memory_scope(agent_id: &str) -> MemoryScope {
    MemoryScope {
        kind: MemoryScopeKind::Agent,
        key: global_agent_memory_scope_key(agent_id),
    }
}

fn memory_remember_params(
    scope: MemoryScope,
    category: MemoryCategory,
    key: Option<&str>,
    content: &str,
) -> MemoryRememberParams {
    MemoryRememberParams {
        scope,
        category,
        namespace: None,
        key: key.map(str::to_owned),
        content: content.to_owned(),
        sensitivity: Some(MemorySensitivity::Normal),
        confidence: Some(0.9),
        importance: Some(0.7),
        provenance: None,
        source_context_kind: None,
        idempotency_key: None,
        supersedes: None,
        metadata: Default::default(),
    }
}

fn memory_request_id(prefix: &str) -> pioneer_protocol::RequestId {
    pioneer_protocol::RequestId::new(generate_test_request_id("mem", prefix))
        .expect("valid memory request id")
}

async fn start_memory_e2e_thread(
    harness: &mut MemoryAgentE2eHarness,
    thread_id: &str,
    mode: &str,
    model_provider: &str,
) {
    let request_id = generate_test_request_id("thstart", thread_id);
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": harness.workspace_id,
            "mode": mode,
            "model": "test-model",
            "model_provider": model_provider
        }
    });
    harness
        .processor
        .process_request(harness.connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut harness.rx, request_id.as_str()).await;
    let _ = recv_notification_by_method(&mut harness.rx, events::THREAD_STARTED).await;
}

async fn start_memory_e2e_turn(
    harness: &mut MemoryAgentE2eHarness,
    thread_id: &str,
    turn_id: &str,
    mode: &str,
    model_provider: &str,
    text: &str,
) {
    let request_id = generate_test_request_id("tustart", turn_id);
    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "turn/start",
        "params": {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "mode": mode,
            "model": "test-model",
            "model_provider": model_provider,
            "input": [
                {
                    "type": "text",
                    "text": text
                }
            ]
        }
    });
    harness
        .processor
        .process_request(harness.connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(&mut harness.rx, request_id.as_str()).await;
    let _ = recv_notification_by_method(&mut harness.rx, events::TURN_STARTED).await;
}

async fn run_memory_e2e_turn(
    harness: &mut MemoryAgentE2eHarness,
    thread_id: &str,
    turn_id: &str,
    mode: &str,
    model_provider: &str,
    text: &str,
) {
    start_memory_e2e_turn(harness, thread_id, turn_id, mode, model_provider, text).await;
    let _ = recv_notification_by_method(&mut harness.rx, events::TURN_COMPLETED).await;
}

async fn run_memory_e2e_thread_turn(
    harness: &mut MemoryAgentE2eHarness,
    thread_id: &str,
    turn_id: &str,
    mode: &str,
    model_provider: &str,
    text: &str,
) {
    start_memory_e2e_thread(harness, thread_id, mode, model_provider).await;
    run_memory_e2e_turn(harness, thread_id, turn_id, mode, model_provider, text).await;
}

fn memory_e2e_provider_registry() -> Arc<pioneer_provider::ProviderRegistry> {
    Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "echo",
        Arc::new(EchoProvider::new()),
    ))
}

fn memory_tool_context(harness: &MemoryGatewayHarness, turn_suffix: &str) -> MemoryTurnContext {
    MemoryTurnContext {
        workspace_id: harness.workspace_id.clone(),
        thread_id: generate_test_request_id("thread", turn_suffix),
        turn_id: generate_test_request_id("turn", turn_suffix),
        mode: pioneer_protocol::ThreadMode::Agent,
        input_text: "phase09 memory tool test".to_owned(),
        task_id: None,
        agent_id: Some("phase09-agent".to_owned()),
    }
}

async fn materialize_memory_tools_for_context(
    harness: &MemoryGatewayHarness,
    context: MemoryTurnContext,
) -> pioneer_memory::hooks::MemoryToolMaterialization {
    let provider =
        crate::memory_tools::GatewayMemoryProvider::new(Arc::downgrade(&harness.processor));
    provider
        .materialize_memory_tools(context)
        .await
        .expect("memory provider should materialize")
}

async fn built_memory_tools(harness: &MemoryGatewayHarness, turn_suffix: &str) -> BuiltinTools {
    let context = memory_tool_context(harness, turn_suffix);
    let materialization = materialize_memory_tools_for_context(harness, context.clone()).await;
    let tool_loop_config = test_tool_loop_config();
    build_tools(
        harness.runtime_home.clone(),
        context.turn_id,
        tool_loop_config.web,
        tool_loop_config.computer_use,
        materialization.bundles,
    )
    .expect("memory tools should build")
}

async fn execute_memory_tool_payload(
    tools: &BuiltinTools,
    tool_name: &str,
    call_id: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let call = tools
        .router
        .build_tool_call(RawToolCall {
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            arguments: arguments.to_string(),
        })
        .expect("memory tool call should parse");
    let result = tools
        .runtime
        .execute_tool_call(call)
        .await
        .expect("memory tool should execute");
    result.output.raw_json()
}

async fn execute_memory_tool_error(
    tools: &BuiltinTools,
    tool_name: &str,
    call_id: &str,
    arguments: serde_json::Value,
) -> ToolError {
    let call = tools
        .router
        .build_tool_call(RawToolCall {
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            arguments: arguments.to_string(),
        })
        .expect("memory tool call should parse");
    match tools.runtime.execute_tool_call(call).await {
        Ok(_) => panic!("memory tool should reject invalid args"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn memory_remember_get_list_roundtrip() {
    let mut harness = setup_memory_gateway_harness("roundtrip", true).await;
    let remember_id = memory_request_id("rememberroundtrip");
    let scope = workspace_memory_scope(harness.workspace_id.as_str());

    harness
        .processor
        .memory_remember(
            harness.connection_id,
            remember_id.clone(),
            memory_remember_params(
                scope.clone(),
                MemoryCategory::Identity,
                Some("favorite_city"),
                "The user likes Porto for quiet working trips.",
            ),
        )
        .await;

    let (remember_response, changed_notification) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        remember_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;
    let remember_payload: MemoryRememberResponse =
        serde_json::from_value(remember_response.result).expect("memory remember response");
    assert!(remember_payload.created);
    assert_eq!(remember_payload.record.scope, scope);
    assert_eq!(
        remember_payload.record.content,
        "The user likes Porto for quiet working trips."
    );
    let changed_payload: MemoryChangedNotification = serde_json::from_value(
        changed_notification
            .params
            .expect("memory changed params should be present"),
    )
    .expect("memory changed notification");
    assert_eq!(changed_payload.memory_id, remember_payload.record.id);
    assert_eq!(changed_payload.change_kind, MemoryChangeKind::Created);

    let get_id = memory_request_id("getroundtrip");
    harness
        .processor
        .memory_get(
            harness.connection_id,
            get_id.clone(),
            MemoryGetParams {
                memory_id: remember_payload.record.id.clone(),
                include_deleted: false,
            },
        )
        .await;
    let get_response = recv_response_by_id(&mut harness.rx, get_id.as_str()).await;
    let get_payload: MemoryGetResponse =
        serde_json::from_value(get_response.result).expect("memory get response");
    assert_eq!(
        get_payload
            .record
            .expect("memory get should return record")
            .id,
        remember_payload.record.id
    );

    let list_id = memory_request_id("listroundtrip");
    harness
        .processor
        .memory_list(
            harness.connection_id,
            list_id.clone(),
            MemoryListParams {
                scopes: vec![workspace_memory_scope(harness.workspace_id.as_str())],
                categories: Vec::new(),
                statuses: Vec::new(),
                query: None,
                limit: Some(10),
                cursor: None,
            },
        )
        .await;
    let list_response = recv_response_by_id(&mut harness.rx, list_id.as_str()).await;
    let list_payload: MemoryListResponse =
        serde_json::from_value(list_response.result).expect("memory list response");
    assert!(
        list_payload
            .records
            .iter()
            .any(|record| record.id == remember_payload.record.id)
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_search_returns_memvid_backed_hits_filtered_by_control_plane() {
    let mut harness = setup_memory_gateway_harness("search_filter", true).await;
    let scope = workspace_memory_scope(harness.workspace_id.as_str());
    let unique_phrase = "phase06 violet mango recall target";

    let target_id = memory_request_id("searchtarget");
    harness
        .processor
        .memory_remember(
            harness.connection_id,
            target_id.clone(),
            memory_remember_params(
                scope.clone(),
                MemoryCategory::Preference,
                Some("search_target"),
                unique_phrase,
            ),
        )
        .await;
    let (target_response, _) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        target_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;
    let target_payload: MemoryRememberResponse =
        serde_json::from_value(target_response.result).expect("target remember response");

    let other_id = memory_request_id("searchother");
    harness
        .processor
        .memory_remember(
            harness.connection_id,
            other_id.clone(),
            memory_remember_params(
                scope.clone(),
                MemoryCategory::Preference,
                Some("search_other"),
                "The user prefers compact keyboard layouts.",
            ),
        )
        .await;
    let _ = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        other_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;

    let search_id = memory_request_id("searchbefore");
    harness
        .processor
        .memory_search(
            harness.connection_id,
            search_id.clone(),
            MemorySearchParams {
                query: "violet mango".to_owned(),
                scopes: vec![scope.clone()],
                categories: Vec::new(),
                statuses: Vec::new(),
                limit: Some(10),
                cursor: None,
                include_provenance: true,
            },
        )
        .await;
    let search_response = recv_response_by_id(&mut harness.rx, search_id.as_str()).await;
    let search_payload: MemorySearchResponse =
        serde_json::from_value(search_response.result).expect("memory search response");
    assert!(
        search_payload
            .hits
            .iter()
            .any(|hit| hit.record.id == target_payload.record.id)
    );

    let forget_id = memory_request_id("forgettarget");
    harness
        .processor
        .memory_forget(
            harness.connection_id,
            forget_id.clone(),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: target_payload.record.id.clone(),
                },
                reason: Some("test cleanup".to_owned()),
                actor: None,
                dry_run: false,
            },
        )
        .await;
    let (forget_response, forgotten_notification) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        forget_id.as_str(),
        events::MEMORY_FORGOTTEN,
    )
    .await;
    let forget_payload: MemoryForgetResponse =
        serde_json::from_value(forget_response.result).expect("memory forget response");
    assert_eq!(
        forget_payload.forgotten_memory_ids,
        vec![target_payload.record.id.clone()]
    );
    let forgotten_payload: MemoryForgottenNotification = serde_json::from_value(
        forgotten_notification
            .params
            .expect("memory forgotten params should be present"),
    )
    .expect("memory forgotten notification");
    assert_eq!(
        forgotten_payload.memory_ids,
        vec![target_payload.record.id.clone()]
    );

    let search_after_id = memory_request_id("searchafter");
    harness
        .processor
        .memory_search(
            harness.connection_id,
            search_after_id.clone(),
            MemorySearchParams {
                query: "violet mango".to_owned(),
                scopes: vec![scope],
                categories: Vec::new(),
                statuses: Vec::new(),
                limit: Some(10),
                cursor: None,
                include_provenance: true,
            },
        )
        .await;
    let search_after_response =
        recv_response_by_id(&mut harness.rx, search_after_id.as_str()).await;
    let search_after_payload: MemorySearchResponse =
        serde_json::from_value(search_after_response.result).expect("memory search response");
    assert!(
        !search_after_payload
            .hits
            .iter()
            .any(|hit| hit.record.id == target_payload.record.id),
        "deleted memory must not leak through memvid search"
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_search_respects_connection_workspace() {
    let mut harness = setup_memory_gateway_harness("workspace_isolation", true).await;
    let workspace_a = harness.workspace_id.clone();
    let scope_a = workspace_memory_scope(workspace_a.as_str());
    let remember_id = memory_request_id("isoremember");

    harness
        .processor
        .memory_remember(
            harness.connection_id,
            remember_id.clone(),
            memory_remember_params(
                scope_a.clone(),
                MemoryCategory::ProjectFact,
                Some("workspace_secret"),
                "workspace alpha owns the phase06 isolation phrase",
            ),
        )
        .await;
    let _ = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        remember_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;

    let workspace_b = harness
        .workspace_manager
        .create_workspace("phase06_workspace_b", Some("Phase 06 Workspace B"))
        .await
        .expect("create workspace B");
    harness
        .session_manager
        .set_connection_workspace(harness.connection_id, Some(workspace_b.id.clone()))
        .await;

    let empty_scope_search_id = memory_request_id("isoempty");
    harness
        .processor
        .memory_search(
            harness.connection_id,
            empty_scope_search_id.clone(),
            MemorySearchParams {
                query: "phase06 isolation phrase".to_owned(),
                scopes: Vec::new(),
                categories: Vec::new(),
                statuses: Vec::new(),
                limit: Some(10),
                cursor: None,
                include_provenance: false,
            },
        )
        .await;
    let empty_scope_response =
        recv_response_by_id(&mut harness.rx, empty_scope_search_id.as_str()).await;
    let empty_scope_payload: MemorySearchResponse =
        serde_json::from_value(empty_scope_response.result).expect("memory search response");
    assert!(empty_scope_payload.hits.is_empty());

    let explicit_scope_search_id = memory_request_id("isoexplicit");
    harness
        .processor
        .memory_search(
            harness.connection_id,
            explicit_scope_search_id.clone(),
            MemorySearchParams {
                query: "phase06 isolation phrase".to_owned(),
                scopes: vec![scope_a],
                categories: Vec::new(),
                statuses: Vec::new(),
                limit: Some(10),
                cursor: None,
                include_provenance: false,
            },
        )
        .await;
    let explicit_scope_response =
        recv_response_by_id(&mut harness.rx, explicit_scope_search_id.as_str()).await;
    let explicit_scope_payload: MemorySearchResponse =
        serde_json::from_value(explicit_scope_response.result).expect("memory search response");
    assert!(explicit_scope_payload.hits.is_empty());

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_empty_scopes_use_user_default_and_connection_workspace() {
    let mut harness = setup_memory_gateway_harness("default_scopes", true).await;
    let workspace_scope = workspace_memory_scope(harness.workspace_id.as_str());
    let user_scope = user_memory_scope();
    let agent_scope = agent_global_memory_scope("phase06-agent");

    for (request_suffix, scope, key, content) in [
        (
            "defaultuser",
            user_scope.clone(),
            "default_user",
            "phase06 default scope user memory",
        ),
        (
            "defaultworkspace",
            workspace_scope.clone(),
            "default_workspace",
            "phase06 default scope workspace memory",
        ),
        (
            "defaultagent",
            agent_scope.clone(),
            "default_agent",
            "phase06 default scope global agent memory",
        ),
    ] {
        let request_id = memory_request_id(request_suffix);
        harness
            .processor
            .memory_remember(
                harness.connection_id,
                request_id.clone(),
                memory_remember_params(scope, MemoryCategory::Preference, Some(key), content),
            )
            .await;
        let _ = recv_response_and_notification_by_id_method(
            &mut harness.rx,
            request_id.as_str(),
            events::MEMORY_CHANGED,
        )
        .await;
    }

    let list_id = memory_request_id("defaultlist");
    harness
        .processor
        .memory_list(
            harness.connection_id,
            list_id.clone(),
            MemoryListParams {
                scopes: Vec::new(),
                categories: Vec::new(),
                statuses: Vec::new(),
                query: None,
                limit: Some(20),
                cursor: None,
            },
        )
        .await;
    let list_response = recv_response_by_id(&mut harness.rx, list_id.as_str()).await;
    let list_payload: MemoryListResponse =
        serde_json::from_value(list_response.result).expect("memory list response");
    let contents = list_payload
        .records
        .iter()
        .map(|record| record.content.as_str())
        .collect::<Vec<_>>();
    assert!(contents.contains(&"phase06 default scope user memory"));
    assert!(contents.contains(&"phase06 default scope workspace memory"));
    assert!(!contents.contains(&"phase06 default scope global agent memory"));

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_forget_dry_run_does_not_emit_forgotten_notification() {
    let mut harness = setup_memory_gateway_harness("dry_run", true).await;
    let remember_id = memory_request_id("dryremember");
    harness
        .processor
        .memory_remember(
            harness.connection_id,
            remember_id.clone(),
            memory_remember_params(
                workspace_memory_scope(harness.workspace_id.as_str()),
                MemoryCategory::Preference,
                Some("dry_run"),
                "phase06 dry-run forget memory",
            ),
        )
        .await;
    let (remember_response, _) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        remember_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;
    let remember_payload: MemoryRememberResponse =
        serde_json::from_value(remember_response.result).expect("memory remember response");

    let forget_id = memory_request_id("dryforget");
    harness
        .processor
        .memory_forget(
            harness.connection_id,
            forget_id.clone(),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: remember_payload.record.id,
                },
                reason: Some("dry run".to_owned()),
                actor: None,
                dry_run: true,
            },
        )
        .await;
    let forget_response = recv_response_by_id(&mut harness.rx, forget_id.as_str()).await;
    let forget_payload: MemoryForgetResponse =
        serde_json::from_value(forget_response.result).expect("memory forget response");
    assert!(forget_payload.dry_run);
    assert!(
        timeout(Duration::from_millis(100), harness.rx.recv())
            .await
            .is_err(),
        "dry-run forget must not emit memory/forgotten"
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_methods_fail_when_runtime_disabled() {
    let mut harness = setup_memory_gateway_harness("disabled", false).await;
    let remember_id = memory_request_id("disabledremember");
    harness
        .processor
        .memory_remember(
            harness.connection_id,
            remember_id.clone(),
            memory_remember_params(
                workspace_memory_scope(harness.workspace_id.as_str()),
                MemoryCategory::Preference,
                Some("disabled"),
                "this should not be stored",
            ),
        )
        .await;

    let error = recv_error_by_id(&mut harness.rx, remember_id.as_str()).await;
    assert_eq!(error.error.code, INVALID_REQUEST_CODE);
    assert!(error.error.message.contains("memory runtime is disabled"));
    let rows = harness
        .crud_store
        .list_agent_memory_records(AgentMemoryListFilter::default())
        .await
        .expect("list memory records");
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_provider_materializes_five_memory_tools_when_runtime_enabled() {
    let harness = setup_memory_gateway_harness("tools_materialize", true).await;
    let materialization =
        materialize_memory_tools_for_context(&harness, memory_tool_context(&harness, "mat")).await;

    assert!(materialization.diagnostics.is_empty());
    assert_eq!(materialization.bundles.len(), 1);
    let bundle = materialization.bundles.first().expect("memory tool bundle");
    let names = bundle
        .specs
        .iter()
        .map(|configured| configured.spec.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        names,
        std::collections::BTreeSet::from([
            "memory_forget",
            "memory_get",
            "memory_list",
            "memory_remember",
            "memory_search",
        ])
    );
    for configured in &bundle.specs {
        assert_eq!(
            configured.spec.payload_kind,
            pioneer_tools::PayloadKind::Function
        );
        assert_eq!(
            configured.output_projection,
            pioneer_tools::ToolOutputProjectionKind::DynamicGeneric
        );
        assert!(configured.spec.parameters.is_object());
        assert!(!configured.spec.description.trim().is_empty());
    }
    let search = bundle
        .specs
        .iter()
        .find(|configured| configured.spec.name == "memory_search")
        .expect("search spec");
    assert_eq!(
        search.spec.recovery.idempotency_mode,
        pioneer_tools::ToolIdempotencyMode::Safe
    );
    assert!(search.spec.recovery.can_resume);
    let list = bundle
        .specs
        .iter()
        .find(|configured| configured.spec.name == "memory_list")
        .expect("list spec");
    assert_eq!(
        list.spec.recovery.idempotency_mode,
        pioneer_tools::ToolIdempotencyMode::Safe
    );
    assert!(list.spec.recovery.can_resume);
    let remember = bundle
        .specs
        .iter()
        .find(|configured| configured.spec.name == "memory_remember")
        .expect("remember spec");
    assert_eq!(
        remember.spec.recovery.idempotency_mode,
        pioneer_tools::ToolIdempotencyMode::RequiresKey
    );
    assert_eq!(remember.spec.recovery.max_attempts, 1);

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_list_tool_returns_inventory_without_semantic_query() {
    let harness = setup_memory_gateway_harness("tool_list_inventory", true).await;
    let tools = built_memory_tools(&harness, "tool_list_inventory").await;

    let name_result = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_list_seed_name",
        json!({
            "content": "Имя пользователя — Александр.",
            "category": "identity",
            "key": "user_name"
        }),
    )
    .await;
    let language_result = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_list_seed_lang",
        json!({
            "content": "Пользователь предпочитает русский язык для общения.",
            "category": "preference",
            "key": "preferred_language"
        }),
    )
    .await;

    let list_result = execute_memory_tool_payload(
        &tools,
        "memory_list",
        "call_memory_list_inventory",
        json!({
            "scopes": ["user"],
            "limit": 20
        }),
    )
    .await;

    let name_id = name_result
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember should return name memory id");
    let language_id = language_result
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember should return language memory id");
    let records = list_result
        .get("records")
        .and_then(serde_json::Value::as_array)
        .expect("list should return records");
    let listed_ids = records
        .iter()
        .filter_map(|record| record.get("memoryId").and_then(serde_json::Value::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(listed_ids.contains(name_id));
    assert!(listed_ids.contains(language_id));

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_provider_disabled_runtime_materializes_no_tools() {
    let harness = setup_memory_gateway_harness("tools_disabled", false).await;
    let materialization =
        materialize_memory_tools_for_context(&harness, memory_tool_context(&harness, "disabled"))
            .await;

    assert!(materialization.bundles.is_empty());
    assert!(
        materialization
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("memory runtime is disabled"))
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_provider_recall_calls_memory_service() {
    let mut harness = setup_memory_gateway_harness("provider_recall", true).await;
    let remember_id = memory_request_id("providerrecall");
    harness
        .processor
        .memory_remember(
            harness.connection_id,
            remember_id.clone(),
            memory_remember_params(
                user_memory_scope(),
                MemoryCategory::Preference,
                Some("phase09_provider_recall"),
                "phase09 provider recall should find the green papaya preference",
            ),
        )
        .await;
    let (remember_response, _) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        remember_id.as_str(),
        events::MEMORY_CHANGED,
    )
    .await;
    let remember_payload: MemoryRememberResponse =
        serde_json::from_value(remember_response.result).expect("memory remember response");

    let provider =
        crate::memory_tools::GatewayMemoryProvider::new(Arc::downgrade(&harness.processor));
    let recall_context = memory_tool_context(&harness, "provider_recall");
    let thread_start_id = memory_request_id("providerthread");
    harness
        .processor
        .thread_start(
            harness.connection_id,
            thread_start_id.clone(),
            ThreadStartParams {
                thread_id: recall_context.thread_id.clone(),
                workspace_id: harness.workspace_id.clone(),
                name: None,
                model: Some("test-model".to_owned()),
                model_provider: Some("openai".to_owned()),
                sandbox: None,
                mode: Some(pioneer_protocol::ThreadMode::Agent),
                origin_kind: None,
                sidebar_visibility: None,
                agent_nickname: None,
                agent_role: None,
            },
        )
        .await;
    let (thread_response, _) = recv_response_and_notification_by_id_method(
        &mut harness.rx,
        thread_start_id.as_str(),
        events::THREAD_STARTED,
    )
    .await;
    let thread_payload: ThreadStartResponse =
        serde_json::from_value(thread_response.result).expect("thread start response");
    harness
        .crud_store
        .materialize_turn_start(
            &thread_payload.thread,
            SandboxMode::FullAccess,
            &Turn {
                id: recall_context.turn_id.clone(),
                status: TurnStatus::InProgress,
                turn_kind: Default::default(),
                origin: Default::default(),
                error: None,
                prompt_manifest: None,
            },
            &[],
        )
        .await
        .expect("turn start should persist for recall scope");
    let snapshot = provider
        .recall_memory(
            recall_context,
            MemoryRecallRequest {
                query: "green papaya".to_owned(),
                categories: Vec::new(),
                top_k: Some(5),
                max_chars: Some(500),
            },
        )
        .await
        .expect("provider recall should succeed");

    assert!(snapshot.diagnostics.is_empty());
    assert!(
        snapshot
            .items
            .iter()
            .any(|item| item.memory_id == remember_payload.record.id)
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_bridge_binds_when_runtime_enabled_for_phase_10() {
    let harness = setup_memory_gateway_harness("bridge_explicit", true).await;

    harness.processor.bind_memory_bridge_if_enabled().await;

    assert!(harness.processor.agent_manager.has_memory_provider().await);

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_bridge_not_bound_when_runtime_disabled() {
    let harness = setup_memory_gateway_harness("bridge_disabled", false).await;

    harness.processor.bind_memory_bridge_if_enabled().await;

    assert!(!harness.processor.agent_manager.has_memory_provider().await);

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[test]
fn chat_mode_russian_remember_does_not_mutate_agent_memory() {
    run_gateway_message_test("chat_mode_russian_remember", || async {
        chat_mode_russian_remember_does_not_mutate_agent_memory_impl().await;
    });
}

async fn chat_mode_russian_remember_does_not_mutate_agent_memory_impl() {
    let chat_provider = Arc::new(MemoryAgentE2eProvider::new(
        "memory-chat",
        MemoryAgentE2eScript::ChatCapture,
    ));
    let provider_registry = memory_e2e_provider_registry();
    provider_registry.insert("memory-chat", chat_provider.clone());
    let mut harness = setup_memory_agent_e2e_harness("chat_no_memory", provider_registry).await;

    let chat_thread_id = generate_test_request_id("thr", "memchatnomutate");
    let chat_turn_id = generate_test_request_id("turn", "memchatnomutate");
    run_memory_e2e_thread_turn(
        &mut harness,
        chat_thread_id.as_str(),
        chat_turn_id.as_str(),
        "Chat",
        "memory-chat",
        "Запомни: меня зовут Александр",
    )
    .await;

    assert!(
        chat_provider.snapshot_policy_requests().is_empty(),
        "chat mode should not run memory policy classifier"
    );
    let chat_requests = chat_provider.snapshot_main_requests();
    assert!(
        chat_requests.iter().any(|request| request
            .messages
            .iter()
            .any(|message| message.content.contains("Запомни: меня зовут Александр"))),
        "chat provider should receive the user chat request"
    );
    for request in &chat_requests {
        assert!(
            request
                .tools
                .as_ref()
                .map(|tools| !tools.iter().any(|tool| tool.name.starts_with("memory_")))
                .unwrap_or(true),
            "chat request should not expose memory tools"
        );
        assert!(
            request
                .compiled_prompt
                .as_ref()
                .map(|prompt| !prompt.full_system_text.contains("## Memory Recall"))
                .unwrap_or(true),
            "chat request should not include memory recall prompt"
        );
    }

    let records = harness
        .crud_store
        .list_agent_memory_records(AgentMemoryListFilter::default())
        .await
        .expect("memory records should list");
    assert!(records.is_empty());
    let history = harness
        .crud_store
        .get_thread_history(chat_thread_id.as_str(), Some(64))
        .await
        .expect("thread history should load")
        .expect("thread history should exist");
    assert!(
        !history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemCompleted {
                item: TurnItem::DynamicToolCall { tool_name, .. },
                ..
            } if tool_name.starts_with("memory_")
        )),
        "chat mode must not persist memory dynamic tool calls"
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_remember_writes_memory_with_turn_provenance() {
    let mut harness = setup_memory_gateway_harness("tool_remember", true).await;
    let tools = built_memory_tools(&harness, "remember").await;
    let output = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_remember",
        json!({
            "content": "phase09 remember tool stores a durable user preference",
            "category": "preference",
            "scope": "user",
            "key": "phase09_remember_preference",
            "source_context": "direct_user_conversation"
        }),
    )
    .await;

    let memory_id = output
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember output should include memoryId")
        .to_owned();
    assert_eq!(
        output
            .get("scope")
            .and_then(|scope| scope.get("kind"))
            .and_then(serde_json::Value::as_str),
        Some("user")
    );
    assert_eq!(
        output.get("category").and_then(serde_json::Value::as_str),
        Some("preference")
    );
    assert!(output.get("provenance").is_some());
    assert_eq!(
        output
            .get("provenance")
            .and_then(|provenance| provenance.get("source_turn_id"))
            .and_then(serde_json::Value::as_str),
        Some(generate_test_request_id("turn", "remember").as_str())
    );

    let notification = recv_notification_by_method(&mut harness.rx, events::MEMORY_CHANGED).await;
    let changed_payload: MemoryChangedNotification = serde_json::from_value(
        notification
            .params
            .expect("memory changed params should be present"),
    )
    .expect("memory changed notification");
    assert_eq!(changed_payload.memory_id, memory_id);
    assert_eq!(changed_payload.change_kind, MemoryChangeKind::Created);

    let get = harness
        .processor
        .memory_runtime()
        .service()
        .get(
            harness
                .processor
                .memory_runtime()
                .operation_context(Some(harness.workspace_id.clone()), None),
            MemoryGetParams {
                memory_id,
                include_deleted: false,
            },
        )
        .await
        .expect("service get should succeed");
    assert!(get.record.is_some());

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_search_calls_memory_service_and_returns_filtered_hits() {
    let harness = setup_memory_gateway_harness("tool_search", true).await;
    let tools = built_memory_tools(&harness, "search").await;
    let remember = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_search_seed",
        json!({
            "content": "phase09 search tool finds the silver apricot preference",
            "category": "preference",
            "scope": "user",
            "key": "phase09_search_preference"
        }),
    )
    .await;
    let memory_id = remember
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember output should include id")
        .to_owned();

    let search = execute_memory_tool_payload(
        &tools,
        "memory_search",
        "call_memory_search",
        json!({
            "query": "silver apricot",
            "scopes": ["user"],
            "categories": ["preference"],
            "limit": 5,
            "includeProvenance": true
        }),
    )
    .await;
    let hits = search
        .get("hits")
        .and_then(serde_json::Value::as_array)
        .expect("search output should include hits");
    let hit = hits
        .iter()
        .find(|hit| hit.get("memoryId").and_then(serde_json::Value::as_str) == Some(&memory_id))
        .expect("search should return seeded memory");
    assert_eq!(
        hit.get("scope")
            .and_then(|scope| scope.get("kind"))
            .and_then(serde_json::Value::as_str),
        Some("user")
    );
    assert_eq!(
        hit.get("category").and_then(serde_json::Value::as_str),
        Some("preference")
    );
    assert!(hit.get("provenance").is_some());

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_get_returns_exact_record_with_provenance() {
    let harness = setup_memory_gateway_harness("tool_get", true).await;
    let tools = built_memory_tools(&harness, "get").await;
    let remember = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_get_seed",
        json!({
            "content": "phase09 get tool exact detail target",
            "category": "project_fact",
            "scope": "workspace",
            "key": "phase09_get_fact"
        }),
    )
    .await;
    let memory_id = remember
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember output should include id")
        .to_owned();

    let get = execute_memory_tool_payload(
        &tools,
        "memory_get",
        "call_memory_get",
        json!({
            "memoryId": memory_id
        }),
    )
    .await;
    let record = get.get("record").expect("get output should include record");
    assert_eq!(
        record.get("memoryId").and_then(serde_json::Value::as_str),
        remember.get("memoryId").and_then(serde_json::Value::as_str)
    );
    assert_eq!(
        record.get("key").and_then(serde_json::Value::as_str),
        Some("phase09_get_fact")
    );
    assert!(record.get("provenance").is_some());

    let get_by_key = execute_memory_tool_payload(
        &tools,
        "memory_get",
        "call_memory_get_key",
        json!({
            "key": "phase09_get_fact",
            "scope": "workspace"
        }),
    )
    .await;
    assert_eq!(
        get_by_key
            .get("record")
            .and_then(|record| record.get("memoryId"))
            .and_then(serde_json::Value::as_str),
        remember.get("memoryId").and_then(serde_json::Value::as_str)
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_forget_tombstones_memory_and_suppresses_future_search() {
    let mut harness = setup_memory_gateway_harness("tool_forget", true).await;
    let tools = built_memory_tools(&harness, "forget").await;
    let remember = execute_memory_tool_payload(
        &tools,
        "memory_remember",
        "call_memory_forget_seed",
        json!({
            "content": "phase09 forget tool removes the amber pear target",
            "category": "preference",
            "scope": "user",
            "key": "phase09_forget_target"
        }),
    )
    .await;
    let memory_id = remember
        .get("memoryId")
        .and_then(serde_json::Value::as_str)
        .expect("remember output should include id")
        .to_owned();

    let forget = execute_memory_tool_payload(
        &tools,
        "memory_forget",
        "call_memory_forget",
        json!({
            "memoryId": memory_id,
            "reason": "phase09 forget test"
        }),
    )
    .await;
    assert_eq!(
        forget
            .get("forgottenMemoryIds")
            .and_then(serde_json::Value::as_array)
            .expect("forget output ids")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>(),
        vec![memory_id.as_str()]
    );

    let forgotten = recv_notification_by_method(&mut harness.rx, events::MEMORY_FORGOTTEN).await;
    let forgotten_payload: MemoryForgottenNotification = serde_json::from_value(
        forgotten
            .params
            .expect("memory forgotten params should be present"),
    )
    .expect("memory forgotten notification");
    assert_eq!(forgotten_payload.memory_ids, vec![memory_id.clone()]);

    let search = execute_memory_tool_payload(
        &tools,
        "memory_search",
        "call_memory_forget_search",
        json!({
            "query": "amber pear",
            "scopes": ["user"],
            "limit": 5
        }),
    )
    .await;
    let hits = search
        .get("hits")
        .and_then(serde_json::Value::as_array)
        .expect("search output should include hits");
    assert!(
        !hits
            .iter()
            .any(|hit| hit.get("memoryId").and_then(serde_json::Value::as_str) == Some(&memory_id))
    );

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_invalid_args_return_tool_error() {
    let harness = setup_memory_gateway_harness("tool_invalid", true).await;
    let tools = built_memory_tools(&harness, "invalid").await;

    for (tool_name, call_id, args) in [
        (
            "memory_search",
            "call_invalid_search",
            json!({ "query": " " }),
        ),
        ("memory_get", "call_invalid_get", json!({})),
        (
            "memory_get",
            "call_invalid_get_key",
            json!({ "key": "phase09_missing_scope" }),
        ),
        (
            "memory_remember",
            "call_invalid_remember",
            json!({ "content": "missing category" }),
        ),
        (
            "memory_forget",
            "call_invalid_forget",
            json!({ "key": "phase09_missing_scope" }),
        ),
        (
            "memory_search",
            "call_invalid_unknown",
            json!({ "query": "x", "unknown": true }),
        ),
        (
            "memory_list",
            "call_invalid_list_unknown",
            json!({ "unknown": true }),
        ),
    ] {
        let error = execute_memory_tool_error(&tools, tool_name, call_id, args).await;
        assert!(
            matches!(error, ToolError::InvalidArguments(_)),
            "expected invalid arguments for {tool_name}, got {error:?}"
        );
    }

    let rows = harness
        .crud_store
        .list_agent_memory_records(AgentMemoryListFilter::default())
        .await
        .expect("list memory records");
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_tool_duplicate_remember_retry_is_idempotent() {
    let harness = setup_memory_gateway_harness("tool_retry", true).await;
    let tools = built_memory_tools(&harness, "retry").await;
    let args = json!({
        "content": "phase09 duplicate remember retry stores one logical memory",
        "category": "preference",
        "scope": "user"
    });

    let first =
        execute_memory_tool_payload(&tools, "memory_remember", "call_memory_retry", args.clone())
            .await;
    let second =
        execute_memory_tool_payload(&tools, "memory_remember", "call_memory_retry", args).await;

    assert_eq!(
        first.get("memoryId").and_then(serde_json::Value::as_str),
        second.get("memoryId").and_then(serde_json::Value::as_str)
    );
    assert_eq!(
        first.get("created").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        second.get("created").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(
        first
            .get("key")
            .and_then(serde_json::Value::as_str)
            .is_some()
    );

    let rows = harness
        .crud_store
        .list_agent_memory_records(AgentMemoryListFilter::default())
        .await
        .expect("list memory records");
    assert_eq!(rows.len(), 1);

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

#[tokio::test]
async fn memory_candidates_list_and_decide_use_service_boundary() {
    let mut harness = setup_memory_gateway_harness("candidates", true).await;
    let now = super::now_timestamp_secs();
    let candidate_ids = [
        ("candidate_reject", MemoryCandidateDecision::Reject),
        ("candidate_expire", MemoryCandidateDecision::Expire),
        ("candidate_approve", MemoryCandidateDecision::Approve),
    ];

    for (candidate_id_suffix, _) in candidate_ids {
        harness
            .crud_store
            .insert_agent_memory_candidate(
                NewAgentMemoryCandidate {
                    id: Some(generate_test_request_id("cand", candidate_id_suffix)),
                    scope: workspace_memory_scope(harness.workspace_id.as_str()),
                    namespace: None,
                    category: MemoryCategory::ProjectDecision,
                    key: Some(candidate_id_suffix.to_owned()),
                    status: None,
                    candidate_text: format!("phase06 candidate text {candidate_id_suffix}"),
                    confidence: 0.82,
                    reason: "test candidate".to_owned(),
                    source_context_kind: None,
                    source_thread_id: None,
                    source_turn_id: None,
                    source_item_id: None,
                    created_by: Some(MemoryActorRecord {
                        kind: MemoryActorKind::User,
                        id: Some("tester".to_owned()),
                    }),
                    dedupe_key: None,
                    metadata_json: (candidate_id_suffix == "candidate_approve").then(|| {
                        serde_json::json!({
                            "semantic": {
                                "fields": {
                                    "intent": "explicit_store",
                                    "explicitness": "explicit",
                                    "category": "project_decision",
                                    "subject": "project",
                                    "subject_key": "phase06-test-project",
                                    "attribute": "custom",
                                    "custom_attribute": "phase06_decision",
                                    "scope_hint": "project_workspace",
                                    "durability": "project_lifetime",
                                    "sensitivity": "none",
                                    "certainty": "high"
                                },
                                "normalized_value": format!("phase06 candidate text {candidate_id_suffix}")
                            }
                        })
                        .to_string()
                    }),
                },
                now,
            )
            .await
            .expect("insert candidate");
    }

    let list_id = memory_request_id("candidatelist");
    harness
        .processor
        .memory_candidates_list(
            harness.connection_id,
            list_id.clone(),
            MemoryCandidatesListParams {
                scopes: Vec::new(),
                categories: Vec::new(),
                statuses: Vec::new(),
                limit: Some(10),
                cursor: None,
            },
        )
        .await;
    let list_response = recv_response_by_id(&mut harness.rx, list_id.as_str()).await;
    let list_payload: MemoryCandidatesListResponse =
        serde_json::from_value(list_response.result).expect("candidate list response");
    assert_eq!(list_payload.candidates.len(), 3);

    for (candidate_id_suffix, decision) in candidate_ids {
        let candidate_id = generate_test_request_id("cand", candidate_id_suffix);
        let decide_id = memory_request_id(candidate_id_suffix);
        harness
            .processor
            .memory_candidates_decide(
                harness.connection_id,
                decide_id.clone(),
                MemoryCandidatesDecideParams {
                    candidate_id: candidate_id.clone(),
                    decision,
                    reason: Some(format!("test {decision:?}")),
                    actor: Some(MemoryActor {
                        kind: MemoryActorKind::User,
                        id: Some("tester".to_owned()),
                    }),
                },
            )
            .await;
        let decide_response = recv_response_by_id(&mut harness.rx, decide_id.as_str()).await;
        let decide_payload: MemoryCandidatesDecideResponse =
            serde_json::from_value(decide_response.result).expect("candidate decide response");
        assert_eq!(decide_payload.candidate.id, candidate_id);
        assert_eq!(
            decide_payload.candidate.status,
            match decision {
                MemoryCandidateDecision::Approve => MemoryCandidateStatus::Approved,
                MemoryCandidateDecision::Reject => MemoryCandidateStatus::Rejected,
                MemoryCandidateDecision::Expire => MemoryCandidateStatus::Expired,
            }
        );
        if decision == MemoryCandidateDecision::Approve {
            assert!(decide_payload.record.is_some());
        } else {
            assert!(decide_payload.record.is_none());
        }
    }

    let _ = std::fs::remove_dir_all(harness.runtime_home);
}

async fn start_thread_and_turn(
    processor: &Arc<MessageProcessor>,
    connection_id: u64,
    rx: &mut mpsc::Receiver<Message>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    mode: &str,
    model_provider: &str,
) {
    let thread_request_id = "req_thread_start_0001";
    let thread_start_request = json!({
        "jsonrpc": "2.0",
        "id": thread_request_id,
        "method": "thread/start",
        "params": {
            "thread_id": thread_id,
            "workspace_id": workspace_id,
            "mode": mode,
            "model": "test-model",
            "model_provider": model_provider
        }
    });
    processor
        .process_request(connection_id, &thread_start_request.to_string())
        .await;
    let _ = recv_response_by_id(rx, thread_request_id).await;
    let _ = recv_notification_by_method(rx, events::THREAD_STARTED).await;

    let turn_request_id = "req_turn_start_000001";
    let turn_start_request = json!({
        "jsonrpc": "2.0",
        "id": turn_request_id,
        "method": "turn/start",
        "params": {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "mode": mode,
            "model": "test-model",
            "model_provider": model_provider,
            "input": [
                {
                    "type": "text",
                    "text": "delegate a task"
                }
            ]
        }
    });
    processor
        .process_request(connection_id, &turn_start_request.to_string())
        .await;
    let _ = recv_response_by_id(rx, turn_request_id).await;
    let _ = recv_notification_by_method(rx, events::TURN_STARTED).await;
}

async fn wait_for_task_anchor(
    crud_store: Arc<CrudStore>,
    thread_id: &str,
    turn_id: &str,
) -> String {
    for _ in 0..100 {
        if let Some(turn_items) = crud_store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn item query should succeed")
            && let Some(task_id) = turn_items
                .events
                .iter()
                .find_map(|event| match &event.payload {
                    TurnItemEventPayload::ItemCompleted {
                        item: TurnItem::Task { item },
                        ..
                    }
                    | TurnItemEventPayload::ItemUpdated {
                        item: TurnItem::Task { item },
                        ..
                    } => Some(item.task_id.clone()),
                    _ => None,
                })
        {
            return task_id;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for task anchor");
}

async fn wait_for_child_lineage_for_run(
    crud_store: Arc<CrudStore>,
    run_id: &str,
) -> pioneer_protocol::ThreadLineage {
    for _ in 0..100 {
        if let Some(lineage) = crud_store
            .list_thread_lineage_for_run(run_id)
            .await
            .expect("lineage query should succeed")
            .pop()
        {
            return lineage;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for child lineage for run `{run_id}`");
}

async fn wait_for_task_anchor_status(
    crud_store: Arc<CrudStore>,
    turn_id: &str,
    task_id: &str,
    expected_status: pioneer_protocol::TaskStatus,
) -> pioneer_protocol::TaskStatus {
    let item_id = format!("task_{task_id}");
    for _ in 0..100 {
        if let Some(TurnItem::Task { item }) = crud_store
            .get_turn_item(turn_id, item_id.as_str())
            .await
            .expect("turn item query should succeed")
        {
            if item.status == expected_status {
                return item.status;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }
    let Some(TurnItem::Task { item }) = crud_store
        .get_turn_item(turn_id, item_id.as_str())
        .await
        .expect("turn item query should succeed")
    else {
        panic!("task anchor read model was not found");
    };
    item.status
}

async fn wait_for_task_status(
    crud_store: Arc<CrudStore>,
    task_id: &str,
    expected_status: pioneer_protocol::TaskStatus,
) -> pioneer_protocol::TaskStatus {
    for _ in 0..100 {
        let response = crud_store
            .get_task(task_id)
            .await
            .expect("task query should succeed")
            .expect("task should exist");
        if response.task.status == expected_status {
            return response.task.status;
        }
        sleep(Duration::from_millis(25)).await;
    }
    crud_store
        .get_task(task_id)
        .await
        .expect("task query should succeed")
        .expect("task should exist")
        .task
        .status
}

async fn wait_for_thread_name_equals(
    crud_store: Arc<CrudStore>,
    thread_id: &str,
    expected_name: &str,
) -> pioneer_entity::thread::Model {
    for _ in 0..200 {
        if let Some(model) = crud_store
            .get_thread_by_id(thread_id)
            .await
            .expect("thread query should succeed")
            && model.name.as_deref() == Some(expected_name)
        {
            return model;
        }
        sleep(Duration::from_millis(25)).await;
    }

    let model = crud_store
        .get_thread_by_id(thread_id)
        .await
        .expect("thread query should succeed")
        .expect("thread should exist");
    panic!(
        "timed out waiting for thread `{thread_id}` name `{expected_name}`, last={:?}",
        model.name
    );
}

async fn wait_for_thread_manager_turn_status(
    thread_manager: &ThreadManager,
    thread_id: &str,
    turn_id: &str,
    expected_status: TurnStatus,
) -> Turn {
    for _ in 0..100 {
        if let Some((_workspace_id, turn)) = thread_manager.turn_get(thread_id, turn_id).await
            && turn.status == expected_status
        {
            return turn;
        }
        sleep(Duration::from_millis(10)).await;
    }

    let last = thread_manager
        .turn_get(thread_id, turn_id)
        .await
        .map(|(_workspace_id, turn)| (turn.status, turn.error));
    panic!(
        "timed out waiting for turn `{turn_id}` in thread `{thread_id}` status `{expected_status:?}`, last={last:?}"
    );
}

async fn recv_text(rx: &mut mpsc::Receiver<Message>) -> String {
    recv_text_timeout(rx, Duration::from_secs(1)).await
}

async fn recv_text_timeout(rx: &mut mpsc::Receiver<Message>, wait_for: Duration) -> String {
    let message = timeout(wait_for, rx.recv())
        .await
        .expect("timed out waiting for websocket message")
        .expect("websocket channel closed unexpectedly");

    match message {
        Message::Text(payload) => payload.to_string(),
        other => panic!("expected text websocket message, got {other:?}"),
    }
}

async fn recv_response_by_id(
    rx: &mut mpsc::Receiver<Message>,
    request_id: &str,
) -> JsonRpcResponse {
    for _ in 0..200 {
        let payload = recv_text_timeout(rx, Duration::from_secs(2)).await;
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        if value.get("id").and_then(serde_json::Value::as_str) == Some(request_id) {
            return serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                panic!("json-rpc response should decode: {error}; payload: {value}")
            });
        }
    }

    panic!("timed out waiting for response id `{request_id}`");
}

async fn recv_response_and_notification_by_id_method(
    rx: &mut mpsc::Receiver<Message>,
    request_id: &str,
    method: &str,
) -> (JsonRpcResponse, JsonRpcNotification) {
    let mut response = None;
    let mut notification = None;

    for _ in 0..200 {
        let payload = recv_text_timeout(rx, Duration::from_secs(2)).await;
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");

        if response.is_none()
            && value.get("id").and_then(serde_json::Value::as_str) == Some(request_id)
        {
            response = Some(
                serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                    panic!("json-rpc response should decode: {error}; payload: {value}")
                }),
            );
        } else if notification.is_none()
            && value.get("id").is_none()
            && value.get("method").and_then(serde_json::Value::as_str) == Some(method)
        {
            notification = Some(
                serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                    panic!("json-rpc notification should decode: {error}; payload: {value}")
                }),
            );
        }

        if response.is_some() && notification.is_some() {
            return (
                response.expect("response should be present"),
                notification.expect("notification should be present"),
            );
        }
    }

    panic!("timed out waiting for response id `{request_id}` and notification `{method}`");
}

async fn recv_error_by_id(
    rx: &mut mpsc::Receiver<Message>,
    request_id: &str,
) -> JsonRpcErrorResponse {
    for _ in 0..200 {
        let payload = recv_text_timeout(rx, Duration::from_secs(2)).await;
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        if value.get("id").and_then(serde_json::Value::as_str) == Some(request_id)
            && value.get("error").is_some()
        {
            return serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                panic!("json-rpc error response should decode: {error}; payload: {value}")
            });
        }
    }

    panic!("timed out waiting for error response id `{request_id}`");
}

async fn recv_notification_by_method(
    rx: &mut mpsc::Receiver<Message>,
    method: &str,
) -> JsonRpcNotification {
    for _ in 0..200 {
        let payload = recv_text_timeout(rx, Duration::from_secs(2)).await;
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        if value.get("id").is_none()
            && value.get("method").and_then(serde_json::Value::as_str) == Some(method)
        {
            return serde_json::from_value(value).expect("json-rpc notification should decode");
        }
    }

    panic!("timed out waiting for notification method `{method}`");
}

async fn recv_echo_turn_lifecycle_notifications(
    rx: &mut mpsc::Receiver<Message>,
) -> (
    JsonRpcNotification,
    JsonRpcNotification,
    JsonRpcNotification,
    JsonRpcNotification,
    JsonRpcNotification,
    JsonRpcNotification,
) {
    let mut thinking_started_notification = None;
    let mut thinking_completed_notification = None;
    let mut message_started_notification = None;
    let mut item_delta_notification = None;
    let mut item_completed_notification = None;
    let mut turn_completed_notification = None;
    let mut seen_methods = Vec::new();

    for _ in 0..200 {
        let message = match timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(message)) => message,
            Ok(None) => panic!("websocket channel closed unexpectedly"),
            Err(_) => panic!(
                "timed out waiting for turn lifecycle notifications; \
                saw_thinking_started={}, saw_thinking_completed={}, saw_message_started={}, \
                saw_delta={}, saw_item_completed={}, saw_turn_completed={}, seen_methods={seen_methods:?}",
                thinking_started_notification.is_some(),
                thinking_completed_notification.is_some(),
                message_started_notification.is_some(),
                item_delta_notification.is_some(),
                item_completed_notification.is_some(),
                turn_completed_notification.is_some(),
            ),
        };
        let payload = match message {
            Message::Text(payload) => payload.to_string(),
            other => panic!("expected text websocket message, got {other:?}"),
        };
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        if value.get("id").is_some() {
            continue;
        }

        let Some(method) = value.get("method").and_then(serde_json::Value::as_str) else {
            continue;
        };
        seen_methods.push(method.to_owned());

        match method {
            events::ITEM_STARTED => {
                let notification: JsonRpcNotification =
                    serde_json::from_value(value).expect("json-rpc notification should decode");
                let params = notification
                    .params
                    .clone()
                    .expect("item/started params should exist");
                let started: ItemStartedNotification =
                    serde_json::from_value(params).expect("item/started payload should decode");
                match started.item {
                    pioneer_protocol::TurnItem::Reasoning { .. } => {
                        thinking_started_notification = Some(notification);
                    }
                    pioneer_protocol::TurnItem::AgentMessage { .. } => {
                        message_started_notification = Some(notification);
                    }
                    _ => {}
                }
            }
            events::ITEM_AGENT_MESSAGE_DELTA => {
                let notification: JsonRpcNotification =
                    serde_json::from_value(value).expect("json-rpc notification should decode");
                item_delta_notification = Some(notification);
            }
            events::ITEM_COMPLETED => {
                let notification: JsonRpcNotification =
                    serde_json::from_value(value).expect("json-rpc notification should decode");
                let params = notification
                    .params
                    .clone()
                    .expect("item/completed params should exist");
                let completed: ItemCompletedNotification =
                    serde_json::from_value(params).expect("item/completed payload should decode");
                match completed.item {
                    pioneer_protocol::TurnItem::Reasoning { .. } => {
                        thinking_completed_notification = Some(notification);
                    }
                    pioneer_protocol::TurnItem::AgentMessage { .. } => {
                        item_completed_notification = Some(notification);
                    }
                    _ => {}
                }
            }
            events::TURN_COMPLETED => {
                turn_completed_notification = Some(
                    serde_json::from_value(value).expect("json-rpc notification should decode"),
                );
            }
            _ => {}
        }

        if let (
            Some(thinking_started),
            Some(thinking_completed),
            Some(message_started),
            Some(item_delta),
            Some(item_completed),
            Some(turn_completed),
        ) = (
            thinking_started_notification.clone(),
            thinking_completed_notification.clone(),
            message_started_notification.clone(),
            item_delta_notification.clone(),
            item_completed_notification.clone(),
            turn_completed_notification.clone(),
        ) {
            return (
                thinking_started,
                thinking_completed,
                message_started,
                item_delta,
                item_completed,
                turn_completed,
            );
        }
    }

    panic!("timed out waiting for full turn lifecycle notifications");
}
