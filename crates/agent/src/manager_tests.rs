use super::{
    AgentCommand, AgentEvent, AgentManager, AgentMemoryProvider, AgentMemoryTurnPolicyProvider,
    AgentStartError, MemoryExtractionPolicy, MemoryRecallItem, MemoryRecallRequest,
    MemoryRecallSnapshot, MemoryToolMaterialization, MemoryTurnContext, MemoryTurnPolicy,
    MemoryTurnPolicyContext, MemoryTurnPolicyRequest, RecoveryAttemptRequest, ToolLoopConfig,
    TurnExecutionControl,
};
use futures_util::StreamExt;
use pioneer_hooks::{
    AuditContribution, HookActorKind, HookAuditEventKind, HookAwaitPolicy, HookContextMode,
    HookContribution, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticSeverity,
    HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy, HookHandler, HookHandlerRequest,
    HookHandlerResponse, HookId, HookInputKind, HookKind, HookPhase, HookPolicyKey, HookPolicySet,
    HookPromptContent, HookPromptSectionTitle, HookRegistry, HookRuntime, HookSectionId,
    HookSubscription, HookSubscriptionId, HookSubscriptionRegistry, HookValue, PolicyContribution,
    PromptManifestDiagnosticContribution, PromptSectionContribution,
};
use pioneer_protocol::{
    AgentDurableEvent, MemoryCategory, MemoryScope, MemoryScopeKind, RecoveryAction,
    RecoveryAttemptContext, StorageOutputPolicy, ThreadMode, ToolLoopBudgetAction,
    ToolLoopBudgetLimitKind, ToolRecoveryIdempotencyMode, ToolRecoveryRetryClass,
    ToolRetryResolution, ToolStoragePayload, TurnItem, TurnItemType, UserInput,
};
use pioneer_provider::providers::EchoProvider;
use pioneer_provider::{
    ChatRequest, ChatResponse, Provider, ProviderCapabilities, ProviderInputCapabilities,
    ProviderRegistry, ProviderToolCall, Role, StreamChunk,
};
use pioneer_skills::{SkillAuditAction, SkillAuditDecision, SkillTrustLevel};
use pioneer_tools::{
    ComputerUseToolsConfig, ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind,
    ToolEventTrace, ToolExtensionBundle, ToolHandler, ToolInvocation, ToolLoopBudgetConfig,
    ToolPayload, ToolRetryBudgetConfig, ToolSpec, WebToolsConfig, dynamic_unknown_output_policy,
};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, advance, sleep, timeout};

fn test_tool_loop_config() -> ToolLoopConfig {
    ToolLoopConfig {
        web: WebToolsConfig {
            default_timeout_ms: 20_000,
            hard_max_timeout_ms: 120_000,
            default_fetch_max_bytes: 2 * 1024 * 1024,
            hard_fetch_max_bytes: 8 * 1024 * 1024,
            default_download_max_bytes: 128 * 1024 * 1024,
            hard_download_max_bytes: 1024 * 1024 * 1024,
            default_max_results: 8,
            hard_max_results: 20,
            default_snippet_chars: 420,
            hard_max_snippet_chars: 4_096,
            default_link_count: 40,
            hard_link_count: 200,
            default_render_max_chars: 40_000,
            ddg_html_search_url: "https://duckduckgo.com/html/".to_owned(),
            ddg_instant_api_url: "https://api.duckduckgo.com/".to_owned(),
            default_user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36".to_owned(),
        },
        computer_use: ComputerUseToolsConfig {
            runtime_home_dir: std::env::temp_dir().join("pioneer-agent-tests"),
            artifacts_subdir: "tools/computer_use".to_owned(),
            ..ComputerUseToolsConfig::default()
        },
        skills: super::SkillsLoopConfig {
            enabled: true,
            max_skills_per_source: 256,
            max_skill_file_bytes: 1024 * 1024,
            prompt_max_chars: 24_000,
            allow_implicit_invocation: false,
            system_roots: vec!["{homeDirectory}/skills/system".to_owned()],
            user_roots: vec!["{homeDirectory}/skills/user".to_owned()],
            workspace_roots: vec!["{homeDirectory}/skills/workspace/{workspaceId}".to_owned()],
            registry_roots: vec!["{homeDirectory}/skills/registry".to_owned()],
            validation: super::SkillsValidationLoopConfig {
                strict_agentskills: true,
                accept_openclaw_profile: true,
            },
            security: super::SkillsSecurityLoopConfig {
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
            dependencies: super::SkillsDependenciesLoopConfig {
                preflight_on_resolve: true,
                runtime_recheck_on_tool_call: true,
            },
            runtime: super::SkillsRuntimeLoopConfig {
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
        budget: ToolLoopBudgetConfig::default(),
        retry: ToolRetryBudgetConfig::default(),
    }
}

fn test_manager() -> AgentManager {
    let registry = Arc::new(ProviderRegistry::with_provider(
        "echo",
        Arc::new(EchoProvider::new()),
    ));
    AgentManager::new(registry, test_tool_loop_config())
}

/// A provider that never completes — useful for testing concurrent turn rejection.
struct PendingProvider;

#[async_trait::async_trait]
impl Provider for PendingProvider {
    fn name(&self) -> &str {
        "pending"
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
        // Block forever
        futures_util::future::pending::<()>().await;
        unreachable!()
    }

    async fn stream_chat(
        &self,
        _request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        futures_util::future::pending::<()>().await;
        unreachable!()
    }
}

#[derive(Default)]
struct CaptureAgentProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
}

impl CaptureAgentProvider {
    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .clone()
    }
}

const PHASE_07_HOOK_PHASES: [HookPhase; 5] = [
    HookPhase::TurnPrePolicy,
    HookPhase::TurnPrePromptContext,
    HookPhase::TurnPrePromptCompile,
    HookPhase::TurnPostPromptCompile,
    HookPhase::TurnPostTurn,
];

#[derive(Debug, Clone, PartialEq)]
struct RecordedHookCall {
    phase: HookPhase,
    input_kind: HookInputKind,
    payload: HookValue,
    workspace_id: Option<String>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    mode: Option<HookContextMode>,
    actor_kind: Option<HookActorKind>,
    policy_set: HookPolicySet,
}

struct RecordingHookHandler {
    hook_id: HookId,
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    fail: bool,
}

#[async_trait::async_trait]
impl HookHandler for RecordingHookHandler {
    fn id(&self) -> HookId {
        self.hook_id.clone()
    }

    fn kind(&self) -> HookKind {
        HookKind::new("test").expect("valid hook kind")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        PHASE_07_HOOK_PHASES.to_vec()
    }

    async fn execute(
        &self,
        request: HookHandlerRequest,
    ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
        self.calls
            .lock()
            .expect("recording hook calls lock poisoned")
            .push(RecordedHookCall {
                phase: request.phase,
                input_kind: request.input.kind.clone(),
                payload: request.input.payload.clone(),
                workspace_id: request
                    .context
                    .workspace_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned()),
                thread_id: request
                    .context
                    .thread_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned()),
                turn_id: request
                    .context
                    .turn_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned()),
                mode: request.context.mode.clone(),
                actor_kind: request
                    .context
                    .actor
                    .as_ref()
                    .map(|actor| actor.kind.clone()),
                policy_set: request.policy_set.clone(),
            });

        if self.fail {
            return Err(HookError::new(
                HookDiagnosticCode::new("test.phase07_failed").expect("valid diagnostic code"),
                HookDiagnosticMessage::new("phase 07 hook failed").expect("valid diagnostic"),
            ));
        }

        Ok(HookHandlerResponse {
            contributions: self.contributions.clone(),
            ..HookHandlerResponse::default()
        })
    }
}

fn empty_hook_runtime() -> Arc<HookRuntime> {
    Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ))
}

fn recording_hook_runtime(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail: bool,
) -> Arc<HookRuntime> {
    recording_hook_runtime_with_fallback(calls, contributions, failure_policy, fail, Vec::new())
}

fn recording_hook_runtime_with_fallback(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail: bool,
    fallback_contributions: Vec<HookContribution>,
) -> Arc<HookRuntime> {
    let handlers = Arc::new(HookRegistry::new());
    let subscriptions = Arc::new(HookSubscriptionRegistry::new());
    let hook_id = HookId::new("test.phase07_recorder").expect("valid hook id");
    handlers
        .register_handler(Arc::new(RecordingHookHandler {
            hook_id: hook_id.clone(),
            calls,
            contributions,
            fail,
        }))
        .expect("recording hook registers");

    for phase in PHASE_07_HOOK_PHASES {
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(phase_07_subscription_id(phase), hook_id.clone(), phase)
                    .with_execution_policy(HookExecutionPolicy {
                        await_policy: HookAwaitPolicy::Blocking,
                        timeout_ms: None,
                        max_parallelism: None,
                    })
                    .with_failure_policy(failure_policy)
                    .with_fallback_contributions(fallback_contributions.clone()),
            )
            .expect("recording hook subscription registers");
    }

    Arc::new(HookRuntime::new(handlers, subscriptions))
}

fn policy_contribution(
    domain: &str,
    key: &str,
    value: HookValue,
    priority: i32,
) -> HookContribution {
    HookContribution::Policy(PolicyContribution {
        domain: HookDomain::new(domain).expect("valid domain"),
        key: HookPolicyKey::new(key).expect("valid policy key"),
        value,
        priority,
        diagnostics: Vec::new(),
    })
}

fn policy_value(policy_set: &HookPolicySet, domain: &str, key: &str) -> Option<HookValue> {
    policy_set
        .get(
            &HookDomain::new(domain).expect("valid domain"),
            &HookPolicyKey::new(key).expect("valid policy key"),
        )
        .map(|entry| entry.value.clone())
}

fn phase_07_subscription_id(phase: HookPhase) -> HookSubscriptionId {
    let value = match phase {
        HookPhase::TurnPrePolicy => "test.phase07.pre_policy",
        HookPhase::TurnPrePromptContext => "test.phase07.pre_prompt_context",
        HookPhase::TurnPrePromptCompile => "test.phase07.pre_prompt_compile",
        HookPhase::TurnPostPromptCompile => "test.phase07.post_prompt_compile",
        HookPhase::TurnPostTurn => "test.phase07.post_turn",
        HookPhase::TurnPreCompaction => "test.phase07.pre_compaction",
    };
    HookSubscriptionId::new(value).expect("valid subscription id")
}

fn phase_07_ignored_contributions() -> Vec<HookContribution> {
    vec![
        HookContribution::Policy(PolicyContribution {
            domain: HookDomain::new("test").expect("valid domain"),
            key: HookPolicyKey::new("ignored_policy").expect("valid policy key"),
            value: HookValue::Bool(true),
            priority: 10_000,
            diagnostics: Vec::new(),
        }),
        HookContribution::PromptSection(PromptSectionContribution {
            section_id: HookSectionId::new("test.phase07_ignored_section")
                .expect("valid section id"),
            title: Some(
                HookPromptSectionTitle::new("Ignored Phase 07 Hook Section")
                    .expect("valid section title"),
            ),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 10_000,
            content: HookPromptContent::new("HOOK OUTPUT MUST NOT APPEAR")
                .expect("valid prompt content"),
            max_chars: None,
            diagnostics: Vec::new(),
            truncated: false,
        }),
        HookContribution::PromptManifestDiagnostic(PromptManifestDiagnosticContribution {
            code: HookDiagnosticCode::new("test.phase07_manifest_ignored")
                .expect("valid diagnostic code"),
            message: HookDiagnosticMessage::new("HOOK MANIFEST DIAGNOSTIC MUST NOT APPEAR")
                .expect("valid diagnostic message"),
            severity: HookDiagnosticSeverity::Warning,
            hook_id: None,
            subscription_id: None,
        }),
        HookContribution::Audit(AuditContribution {
            event_kind: HookAuditEventKind::new("test.phase07_audit_ignored")
                .expect("valid audit event kind"),
            details: HookValue::Text("HOOK AUDIT MUST NOT APPEAR".to_owned()),
            safe_for_user: true,
        }),
    ]
}

fn snapshot_hook_calls(
    calls: &Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
) -> Vec<RecordedHookCall> {
    calls
        .lock()
        .expect("recording hook calls lock poisoned")
        .clone()
}

fn stable_request_projection(request: &ChatRequest) -> serde_json::Value {
    serde_json::json!({
        "model": request.model.clone(),
        "messages": serde_json::to_value(&request.messages).expect("messages serialize"),
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
        "tools": serde_json::to_value(&request.tools).expect("tools serialize"),
        "tool_choice": serde_json::to_value(&request.tool_choice).expect("tool choice serializes"),
        "parallel_tool_calls": request.parallel_tool_calls,
        "compiled_prompt": request.compiled_prompt.as_ref().map(|prompt| {
            serde_json::json!({
                "stable_system_text": prompt.stable_system_text.clone(),
                "boundary_marker": prompt.boundary_marker.clone(),
            })
        }),
    })
}

fn assert_stable_requests_eq(left: &ChatRequest, right: &ChatRequest) {
    assert_eq!(
        stable_request_projection(left),
        stable_request_projection(right)
    );
}

#[derive(Default)]
struct CaptureStandardProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
}

impl CaptureStandardProvider {
    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .clone()
    }
}

struct SequencedToolProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    first_tool_calls: Vec<pioneer_provider::ProviderToolCall>,
    second_text: String,
    next_index: AtomicUsize,
}

impl SequencedToolProvider {
    fn new(
        first_tool_calls: Vec<pioneer_provider::ProviderToolCall>,
        second_text: impl Into<String>,
    ) -> Self {
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
            .expect("capture provider lock poisoned")
            .clone()
    }
}

#[derive(Clone, Copy)]
enum LoopBudgetProviderMode {
    ToolWhileAvailableThenFinal,
    AlwaysTools,
    TooManyToolsThenFinal,
    RepeatedMissingToolThenFinal,
    RetryEpisodeResetThenFinal,
}

struct LoopBudgetProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    next_index: AtomicUsize,
    mode: LoopBudgetProviderMode,
    tool_calls_per_round: usize,
}

impl LoopBudgetProvider {
    fn new(mode: LoopBudgetProviderMode, tool_calls_per_round: usize) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
            mode,
            tool_calls_per_round,
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("loop budget provider lock poisoned")
            .clone()
    }

    fn tools_available(request: &ChatRequest) -> bool {
        request
            .tools
            .as_ref()
            .map(|tools| !tools.is_empty())
            .unwrap_or(false)
    }

    fn missing_tool_call(id: usize) -> ProviderToolCall {
        ProviderToolCall {
            id: format!("call_loop_budget_{id}"),
            name: "missing_loop_budget_tool".to_owned(),
            arguments: r#"{"same":true}"#.to_owned(),
        }
    }

    fn tool_suggest_call(id: usize) -> ProviderToolCall {
        ProviderToolCall {
            id: format!("call_loop_budget_success_{id}"),
            name: "tool_suggest".to_owned(),
            arguments: r#"{"query":"read files","limit":1}"#.to_owned(),
        }
    }

    fn missing_tool_calls(&self, round_index: usize) -> Vec<ProviderToolCall> {
        (0..self.tool_calls_per_round)
            .map(|offset| Self::missing_tool_call(round_index * 100 + offset))
            .collect()
    }
}

#[derive(Default)]
struct ProviderRecoveryBoundaryProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    next_index: AtomicUsize,
}

impl ProviderRecoveryBoundaryProvider {
    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("provider boundary lock poisoned")
            .clone()
    }
}

struct RecordingMemoryProvider {
    recall_contexts: std::sync::Mutex<Vec<MemoryTurnContext>>,
    recall_requests: std::sync::Mutex<Vec<MemoryRecallRequest>>,
    tool_contexts: std::sync::Mutex<Vec<MemoryTurnContext>>,
    recall_result: Result<MemoryRecallSnapshot, String>,
    tool_result: Result<MemoryToolMaterialization, String>,
}

impl RecordingMemoryProvider {
    fn new(
        recall_result: Result<MemoryRecallSnapshot, String>,
        tool_result: Result<MemoryToolMaterialization, String>,
    ) -> Self {
        Self {
            recall_contexts: std::sync::Mutex::new(Vec::new()),
            recall_requests: std::sync::Mutex::new(Vec::new()),
            tool_contexts: std::sync::Mutex::new(Vec::new()),
            recall_result,
            tool_result,
        }
    }

    fn ok() -> Self {
        Self::new(
            Ok(MemoryRecallSnapshot::empty()),
            Ok(MemoryToolMaterialization::default()),
        )
    }

    fn recall_contexts(&self) -> Vec<MemoryTurnContext> {
        self.recall_contexts
            .lock()
            .expect("memory recall contexts lock poisoned")
            .clone()
    }

    fn recall_requests(&self) -> Vec<MemoryRecallRequest> {
        self.recall_requests
            .lock()
            .expect("memory recall requests lock poisoned")
            .clone()
    }

    fn tool_contexts(&self) -> Vec<MemoryTurnContext> {
        self.tool_contexts
            .lock()
            .expect("memory tool contexts lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentMemoryProvider for RecordingMemoryProvider {
    async fn recall_memory(
        &self,
        context: MemoryTurnContext,
        request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        self.recall_contexts
            .lock()
            .expect("memory recall contexts lock poisoned")
            .push(context);
        self.recall_requests
            .lock()
            .expect("memory recall requests lock poisoned")
            .push(request);
        self.recall_result.clone()
    }

    async fn materialize_memory_tools(
        &self,
        context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        self.tool_contexts
            .lock()
            .expect("memory tool contexts lock poisoned")
            .push(context);
        self.tool_result.clone()
    }
}

struct FakeMemoryTurnPolicyProvider {
    contexts: std::sync::Mutex<Vec<MemoryTurnPolicyContext>>,
    requests: std::sync::Mutex<Vec<MemoryTurnPolicyRequest>>,
    result: Result<MemoryTurnPolicy, String>,
}

impl FakeMemoryTurnPolicyProvider {
    fn new(policy: MemoryTurnPolicy) -> Self {
        Self {
            contexts: std::sync::Mutex::new(Vec::new()),
            requests: std::sync::Mutex::new(Vec::new()),
            result: Ok(policy),
        }
    }

    fn failing(error: impl Into<String>) -> Self {
        Self {
            contexts: std::sync::Mutex::new(Vec::new()),
            requests: std::sync::Mutex::new(Vec::new()),
            result: Err(error.into()),
        }
    }

    fn contexts(&self) -> Vec<MemoryTurnPolicyContext> {
        self.contexts
            .lock()
            .expect("memory policy contexts lock poisoned")
            .clone()
    }

    fn requests(&self) -> Vec<MemoryTurnPolicyRequest> {
        self.requests
            .lock()
            .expect("memory policy requests lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentMemoryTurnPolicyProvider for FakeMemoryTurnPolicyProvider {
    async fn resolve_memory_turn_policy(
        &self,
        context: MemoryTurnPolicyContext,
        request: MemoryTurnPolicyRequest,
    ) -> Result<MemoryTurnPolicy, String> {
        self.contexts
            .lock()
            .expect("memory policy contexts lock poisoned")
            .push(context);
        self.requests
            .lock()
            .expect("memory policy requests lock poisoned")
            .push(request);
        self.result.clone()
    }
}

struct MemoryFakeHandler;

#[async_trait::async_trait]
impl ToolHandler for MemoryFakeHandler {
    async fn handle(
        &self,
        _invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        Ok(Box::new(FunctionToolOutput::new("memory-ok", true)))
    }
}

struct RecordingMemoryToolHandler {
    calls: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl ToolHandler for RecordingMemoryToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        self.calls
            .lock()
            .expect("memory tool calls lock poisoned")
            .push(invocation.tool_name);
        Ok(Box::new(FunctionToolOutput::new("memory-ok", true)))
    }
}

fn fake_memory_tool_spec(name: &str) -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            name,
            "test-only memory tool",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
    )
}

fn fake_memory_tool_bundle_for_names(
    names: &[&str],
    handler: Arc<dyn ToolHandler>,
) -> ToolExtensionBundle {
    ToolExtensionBundle {
        specs: names
            .iter()
            .map(|name| fake_memory_tool_spec(name))
            .collect(),
        handlers: names
            .iter()
            .map(|name| ((*name).to_owned(), handler.clone()))
            .collect(),
    }
}

fn fake_standard_memory_tool_bundle() -> ToolExtensionBundle {
    let handler: Arc<dyn ToolHandler> = Arc::new(MemoryFakeHandler);
    fake_memory_tool_bundle_for_names(
        &[
            "memory_search",
            "memory_get",
            "memory_remember",
            "memory_forget",
        ],
        handler,
    )
}

fn recording_standard_memory_tool_bundle()
-> (ToolExtensionBundle, Arc<std::sync::Mutex<Vec<String>>>) {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler: Arc<dyn ToolHandler> = Arc::new(RecordingMemoryToolHandler {
        calls: calls.clone(),
    });
    (
        fake_memory_tool_bundle_for_names(
            &[
                "memory_search",
                "memory_get",
                "memory_remember",
                "memory_forget",
            ],
            handler,
        ),
        calls,
    )
}

fn memory_recall_item(
    memory_id: &str,
    category: MemoryCategory,
    key: Option<&str>,
    content: &str,
) -> MemoryRecallItem {
    MemoryRecallItem {
        memory_id: memory_id.to_owned(),
        scope: MemoryScope {
            kind: MemoryScopeKind::User,
            key: "global".to_owned(),
        },
        category,
        key: key.map(str::to_owned),
        content: content.to_owned(),
        score: Some(1.0),
        updated_at: 1_714_867_200,
    }
}

#[derive(Default)]
struct StatefulMemoryProvider {
    stored_content: Arc<std::sync::Mutex<Option<String>>>,
}

#[async_trait::async_trait]
impl AgentMemoryProvider for StatefulMemoryProvider {
    async fn recall_memory(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        let Some(content) = self
            .stored_content
            .lock()
            .expect("stateful memory lock poisoned")
            .clone()
        else {
            return Ok(MemoryRecallSnapshot::empty());
        };

        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_stateful_name",
                MemoryCategory::Identity,
                Some("name"),
                content.as_str(),
            )],
            diagnostics: Vec::new(),
            truncated: false,
        })
    }

    async fn materialize_memory_tools(
        &self,
        _context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        let handler: Arc<dyn ToolHandler> = Arc::new(StatefulMemoryToolHandler {
            stored_content: self.stored_content.clone(),
        });
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_memory_tool_bundle_for_names(
                &[
                    "memory_search",
                    "memory_get",
                    "memory_remember",
                    "memory_forget",
                ],
                handler,
            )],
            diagnostics: Vec::new(),
        })
    }
}

struct StatefulMemoryToolHandler {
    stored_content: Arc<std::sync::Mutex<Option<String>>>,
}

#[async_trait::async_trait]
impl ToolHandler for StatefulMemoryToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        match invocation.tool_name.as_str() {
            "memory_remember" => {
                let content = match &invocation.payload {
                    ToolPayload::Function { arguments } => arguments
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("User's name is Alexander."),
                    _ => "User's name is Alexander.",
                };
                *self
                    .stored_content
                    .lock()
                    .expect("stateful memory lock poisoned") = Some(content.to_owned());
            }
            "memory_forget" => {
                *self
                    .stored_content
                    .lock()
                    .expect("stateful memory lock poisoned") = None;
            }
            _ => {}
        }
        Ok(Box::new(FunctionToolOutput::new("memory-ok", true)))
    }
}

#[async_trait::async_trait]
impl Provider for CaptureStandardProvider {
    fn name(&self) -> &str {
        "capture-standard"
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
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .push(request);
        Ok(ChatResponse {
            text: "done".to_owned(),
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
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
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

#[async_trait::async_trait]
impl Provider for LoopBudgetProvider {
    fn name(&self) -> &str {
        "loop-budget"
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
        let tools_available = Self::tools_available(&request);
        self.requests
            .lock()
            .expect("loop budget provider lock poisoned")
            .push(request);

        let round_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let tool_calls = match self.mode {
            LoopBudgetProviderMode::ToolWhileAvailableThenFinal if tools_available => {
                vec![Self::missing_tool_call(round_index)]
            }
            LoopBudgetProviderMode::AlwaysTools => vec![Self::missing_tool_call(round_index)],
            LoopBudgetProviderMode::TooManyToolsThenFinal if tools_available => {
                self.missing_tool_calls(round_index)
            }
            LoopBudgetProviderMode::RepeatedMissingToolThenFinal if tools_available => {
                vec![Self::missing_tool_call(round_index)]
            }
            LoopBudgetProviderMode::RetryEpisodeResetThenFinal if tools_available => {
                match round_index {
                    0 | 2 => vec![Self::missing_tool_call(round_index)],
                    1 => vec![Self::tool_suggest_call(round_index)],
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        };

        Ok(ChatResponse {
            text: if tool_calls.is_empty() {
                "final without tools".to_owned()
            } else {
                String::new()
            },
            usage: None,
            reasoning_content: None,
            tool_calls,
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
impl Provider for ProviderRecoveryBoundaryProvider {
    fn name(&self) -> &str {
        "provider-recovery-boundary"
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
        self.requests
            .lock()
            .expect("provider boundary lock poisoned")
            .push(request);

        match self.next_index.fetch_add(1, Ordering::SeqCst) {
            0 => Err(anyhow::anyhow!("initial provider failure 500")),
            1 => Ok(ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![ProviderToolCall {
                    id: "call_provider_boundary_1".to_owned(),
                    name: "missing_tool_for_boundary".to_owned(),
                    arguments: "{}".to_owned(),
                }],
            }),
            _ => Err(anyhow::anyhow!(
                "late provider failure after recovery success"
            )),
        }
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
impl Provider for CaptureAgentProvider {
    fn name(&self) -> &str {
        "capture"
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
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .push(request);
        Ok(ChatResponse {
            text: "done".to_owned(),
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

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("pioneer-agent-{prefix}-{nanos}-{id}"))
}

fn test_agent_event_from_durable(event: AgentDurableEvent) -> Option<AgentEvent> {
    match event {
        AgentDurableEvent::PromptManifestCompiled {
            thread_id,
            turn_id,
            manifest,
        } => Some(AgentEvent::PromptManifestCompiled {
            thread_id,
            turn_id,
            manifest,
        }),
        AgentDurableEvent::TurnSkillsResolved {
            thread_id,
            turn_id,
            bindings,
        } => Some(AgentEvent::TurnSkillsResolved {
            thread_id,
            turn_id,
            bindings,
        }),
        AgentDurableEvent::SkillAuditEvents {
            thread_id,
            turn_id,
            events,
        } => Some(AgentEvent::SkillAuditEvents {
            thread_id,
            turn_id,
            events: events
                .into_iter()
                .map(|event| pioneer_skills::SkillAuditEvent {
                    skill_slug: event.skill_slug,
                    source_kind: event.source_kind,
                    action: match event.action.as_str() {
                        "install" => SkillAuditAction::Install,
                        "update" => SkillAuditAction::Update,
                        "uninstall" => SkillAuditAction::Uninstall,
                        "resolve_allowed" => SkillAuditAction::ResolveAllowed,
                        "resolve_blocked" => SkillAuditAction::ResolveBlocked,
                        "runtime_allowed" => SkillAuditAction::RuntimeAllowed,
                        "runtime_blocked" => SkillAuditAction::RuntimeBlocked,
                        "security_warn" => SkillAuditAction::SecurityWarn,
                        _ => SkillAuditAction::SecurityWarn,
                    },
                    decision: match event.decision.as_str() {
                        "allowed" => SkillAuditDecision::Allowed,
                        "blocked" => SkillAuditDecision::Blocked,
                        "warning" => SkillAuditDecision::Warning,
                        _ => SkillAuditDecision::Warning,
                    },
                    reason_code: event.reason_code,
                    details: event.details,
                    created_at_unix: event.created_at_unix,
                })
                .collect(),
        }),
        AgentDurableEvent::TurnLlmContextAppended {
            thread_id,
            turn_id,
            item_id,
            attempt_id,
            sequence,
            source,
            tool_name,
            payload,
            output_policy_snapshot,
        } => Some(AgentEvent::TurnLlmContextAppended {
            thread_id,
            turn_id,
            item_id,
            attempt_id,
            sequence,
            source,
            tool_name,
            payload: match payload {
                pioneer_protocol::ToolResultView::Text { text, truncated } => {
                    pioneer_tools::ToolResultView::Text { text, truncated }
                }
                pioneer_protocol::ToolResultView::Json { value, truncated } => {
                    pioneer_tools::ToolResultView::Json { value, truncated }
                }
                pioneer_protocol::ToolResultView::Empty => pioneer_tools::ToolResultView::Empty,
            },
            output_policy_snapshot,
        }),
        AgentDurableEvent::ItemStarted { notification } => {
            Some(AgentEvent::ItemStarted(notification))
        }
        AgentDurableEvent::ItemCompleted { notification } => {
            Some(AgentEvent::ItemCompleted(notification))
        }
        AgentDurableEvent::ItemToolRetryScheduled { notification } => {
            Some(AgentEvent::ItemToolRetryScheduled(notification))
        }
        AgentDurableEvent::ItemToolRetryResolved { notification } => {
            Some(AgentEvent::ItemToolRetryResolved(notification))
        }
        AgentDurableEvent::ItemToolRetryExhausted { notification } => {
            Some(AgentEvent::ItemToolRetryExhausted(notification))
        }
        AgentDurableEvent::TurnToolLoopBudgetExceeded { notification } => {
            Some(AgentEvent::TurnToolLoopBudgetExceeded(notification))
        }
        AgentDurableEvent::ProviderFailureDetected {
            thread_id,
            turn_id,
            item_id,
            item_type,
            failure,
            recovery,
        } => Some(AgentEvent::ProviderFailureDetected {
            thread_id,
            turn_id,
            item_id,
            item_type,
            failure,
            recovery,
        }),
        AgentDurableEvent::RecoveryAttemptSucceeded {
            thread_id,
            turn_id,
            recovery,
        } => Some(AgentEvent::RecoveryAttemptSucceeded {
            thread_id,
            turn_id,
            recovery,
        }),
        AgentDurableEvent::TurnCompleted {
            thread_id,
            turn_id,
            recovery,
        } => Some(AgentEvent::TurnCompleted {
            thread_id,
            turn_id,
            recovery,
        }),
        AgentDurableEvent::TurnFailed {
            thread_id,
            turn_id,
            error,
            recovery,
        } => Some(AgentEvent::TurnFailed {
            thread_id,
            turn_id,
            error,
            recovery,
        }),
        AgentDurableEvent::TurnInterrupted {
            thread_id,
            turn_id,
            reason,
            recovery,
        } => Some(AgentEvent::TurnFailed {
            thread_id,
            turn_id,
            error: reason,
            recovery,
        }),
        AgentDurableEvent::TaskEvent { .. } | AgentDurableEvent::ThreadLineageCreated { .. } => {
            None
        }
    }
}

async fn subscribe_agent_events(
    manager: &AgentManager,
    thread_id: &str,
) -> tokio::sync::mpsc::Receiver<AgentEvent> {
    let mut durable_rx = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    tokio::spawn(async move {
        while let Some(event) = durable_rx.recv().await {
            let Some(event) = test_agent_event_from_durable(event) else {
                continue;
            };
            if tx.send(event).await.is_err() {
                break;
            }
        }
    });
    rx
}

async fn recv_events_until_terminal(
    events: &mut tokio::sync::mpsc::Receiver<AgentEvent>,
) -> Vec<AgentEvent> {
    let mut observed = Vec::new();

    for _ in 0..160 {
        let event = match timeout(Duration::from_secs(2), events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                panic!("agent event channel should stay open")
            }
            Err(_) => panic!("timed out waiting for terminal agent event"),
        };
        let terminal = matches!(
            event,
            AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
        );
        observed.push(event);
        if terminal {
            return observed;
        }
    }

    panic!("terminal agent event not received")
}

fn assert_turn_completed(observed: &[AgentEvent]) {
    assert!(
        matches!(observed.last(), Some(AgentEvent::TurnCompleted { .. })),
        "expected terminal turn completion, observed {observed:?}"
    );
}

fn assert_turn_failed(observed: &[AgentEvent], expected_error: &str) {
    let Some(AgentEvent::TurnFailed { error, .. }) = observed.last() else {
        panic!("expected terminal turn failure, observed {observed:?}");
    };
    assert_eq!(error, expected_error);
}

async fn start_simple_turn(
    manager: &AgentManager,
    thread_id: &str,
    workspace_id: &str,
    turn_id: &str,
    mode: ThreadMode,
    provider_name: &str,
    text: &str,
) -> Vec<AgentEvent> {
    manager
        .ensure_thread(thread_id, workspace_id)
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(manager, thread_id).await;
    manager
        .start_turn(
            thread_id,
            turn_id,
            mode,
            "test-model",
            provider_name,
            HashMap::new(),
            vec![UserInput::Text {
                text: text.to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    recv_events_until_terminal(&mut events).await
}

#[tokio::test]
async fn phase_07_no_and_empty_hook_runtime_preserve_agent_request() {
    let no_runtime_provider = Arc::new(CaptureAgentProvider::default());
    let no_runtime_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        no_runtime_provider.clone(),
    ));
    let no_runtime_manager = AgentManager::new(no_runtime_registry, test_tool_loop_config());
    assert!(!no_runtime_manager.has_hook_runtime().await);

    let no_runtime_observed = start_simple_turn(
        &no_runtime_manager,
        "thr_phase07_no_runtime",
        "ws_phase07_request",
        "turn_phase07_no_runtime",
        ThreadMode::Agent,
        "capture",
        "phase 07 request baseline",
    )
    .await;
    assert_turn_completed(&no_runtime_observed);

    let empty_runtime_provider = Arc::new(CaptureAgentProvider::default());
    let empty_runtime_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        empty_runtime_provider.clone(),
    ));
    let empty_runtime_manager = AgentManager::new(empty_runtime_registry, test_tool_loop_config());
    empty_runtime_manager
        .set_hook_runtime(Some(empty_hook_runtime()))
        .await;
    assert!(empty_runtime_manager.has_hook_runtime().await);

    let empty_runtime_observed = start_simple_turn(
        &empty_runtime_manager,
        "thr_phase07_empty_runtime",
        "ws_phase07_request",
        "turn_phase07_empty_runtime",
        ThreadMode::Agent,
        "capture",
        "phase 07 request baseline",
    )
    .await;
    assert_turn_completed(&empty_runtime_observed);

    let no_runtime_requests = no_runtime_provider.snapshot_requests();
    let empty_runtime_requests = empty_runtime_provider.snapshot_requests();
    assert_eq!(no_runtime_requests.len(), 1);
    assert_eq!(empty_runtime_requests.len(), 1);
    assert_stable_requests_eq(&no_runtime_requests[0], &empty_runtime_requests[0]);
}

#[tokio::test]
async fn phase_07_agent_mode_calls_each_hook_phase_once() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase07_agent_hooks",
        "ws_phase07_agent_hooks",
        "turn_phase07_agent_hooks",
        ThreadMode::Agent,
        "capture",
        "phase 07 agent hook phases",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    assert_eq!(calls.len(), PHASE_07_HOOK_PHASES.len());
    assert_eq!(
        calls.iter().map(|call| call.phase).collect::<Vec<_>>(),
        PHASE_07_HOOK_PHASES
    );
    for call in calls {
        assert_eq!(call.input_kind, HookInputKind::from(call.phase));
        assert_eq!(call.payload, HookValue::Null);
        assert_eq!(call.workspace_id.as_deref(), Some("ws_phase07_agent_hooks"));
        assert_eq!(call.thread_id.as_deref(), Some("thr_phase07_agent_hooks"));
        assert_eq!(call.turn_id.as_deref(), Some("turn_phase07_agent_hooks"));
        assert_eq!(call.mode, Some(HookContextMode::Agent));
        assert_eq!(call.actor_kind, Some(HookActorKind::Agent));
        assert!(call.policy_set.is_empty());
    }
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_07_agent_mode_without_tool_calling_calls_each_hook_phase_once() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase07_agent_no_tools",
        "ws_phase07_agent_no_tools",
        "turn_phase07_agent_no_tools",
        ThreadMode::Agent,
        "capture-standard",
        "phase 07 agent hook phases without provider tool calling",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    assert_eq!(calls.len(), PHASE_07_HOOK_PHASES.len());
    assert_eq!(
        calls.iter().map(|call| call.phase).collect::<Vec<_>>(),
        PHASE_07_HOOK_PHASES
    );
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_07_chat_mode_does_not_call_turn_hooks() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase07_chat_hooks",
        "ws_phase07_chat_hooks",
        "turn_phase07_chat_hooks",
        ThreadMode::Chat,
        "capture-standard",
        "phase 07 chat hook phases",
    )
    .await;
    assert_turn_completed(&observed);

    assert!(snapshot_hook_calls(&calls).is_empty());
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].compiled_prompt.is_none());
}

#[tokio::test]
async fn phase_07_hook_contributions_do_not_affect_agent_request() {
    let baseline_provider = Arc::new(CaptureAgentProvider::default());
    let baseline_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        baseline_provider.clone(),
    ));
    let baseline_manager = AgentManager::new(baseline_registry, test_tool_loop_config());
    baseline_manager
        .set_hook_runtime(Some(empty_hook_runtime()))
        .await;

    let baseline_observed = start_simple_turn(
        &baseline_manager,
        "thr_phase07_contrib_baseline",
        "ws_phase07_contrib",
        "turn_phase07_contrib_baseline",
        ThreadMode::Agent,
        "capture",
        "phase 07 ignored contributions",
    )
    .await;
    assert_turn_completed(&baseline_observed);

    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook_provider = Arc::new(CaptureAgentProvider::default());
    let hook_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        hook_provider.clone(),
    ));
    let hook_manager = AgentManager::new(hook_registry, test_tool_loop_config());
    hook_manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls,
            phase_07_ignored_contributions(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let hook_observed = start_simple_turn(
        &hook_manager,
        "thr_phase07_contrib_hook",
        "ws_phase07_contrib",
        "turn_phase07_contrib_hook",
        ThreadMode::Agent,
        "capture",
        "phase 07 ignored contributions",
    )
    .await;
    assert_turn_completed(&hook_observed);

    let baseline_requests = baseline_provider.snapshot_requests();
    let hook_requests = hook_provider.snapshot_requests();
    assert_eq!(baseline_requests.len(), 1);
    assert_eq!(hook_requests.len(), 1);
    assert_stable_requests_eq(&baseline_requests[0], &hook_requests[0]);
    let prompt = hook_requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(
        !prompt
            .full_system_text
            .contains("HOOK OUTPUT MUST NOT APPEAR")
    );

    let hook_manifest = hook_observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    assert!(
        !hook_manifest
            .section_ids
            .iter()
            .any(|section_id| section_id == "test.phase07_ignored_section")
    );
    assert!(!hook_manifest.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .contains("HOOK MANIFEST DIAGNOSTIC MUST NOT APPEAR")
    }));
    assert!(
        !hook_observed
            .iter()
            .any(|event| matches!(event, AgentEvent::SkillAuditEvents { .. }))
    );
}

#[tokio::test]
async fn phase_08_required_policy_hook_failure_fails_turn() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::Required,
            true,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase07_required_failure",
        "ws_phase07_required_failure",
        "turn_phase07_required_failure",
        ThreadMode::Agent,
        "capture",
        "phase 07 required hook failure",
    )
    .await;
    assert_turn_failed(&observed, "turn policy hook failed");

    assert!(provider.snapshot_requests().is_empty());
    let calls = snapshot_hook_calls(&calls);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].phase, HookPhase::TurnPrePolicy);
}

#[tokio::test]
async fn phase_08_policy_hook_contribution_reaches_later_phase_policy_set() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![policy_contribution(
                "test",
                "mode",
                HookValue::Text("strict".to_owned()),
                10,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_policy_reaches_later",
        "ws_phase08_policy_reaches_later",
        "turn_phase08_policy_reaches_later",
        ThreadMode::Agent,
        "capture",
        "phase 08 policy contribution",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_policy = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePolicy)
        .expect("pre-policy hook called");
    assert!(pre_policy.policy_set.is_empty());
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert_eq!(
        policy_value(&pre_prompt_context.policy_set, "test", "mode"),
        Some(HookValue::Text("strict".to_owned()))
    );
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_08_multiple_policy_contributions_merge_deterministically() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![
                policy_contribution("test", "mode", HookValue::Text("weak".to_owned()), 0),
                policy_contribution("test", "mode", HookValue::Text("strong".to_owned()), 10),
                policy_contribution("test", "allowed", HookValue::Bool(true), 5),
            ],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_policy_merge",
        "ws_phase08_policy_merge",
        "turn_phase08_policy_merge",
        ThreadMode::Agent,
        "capture",
        "phase 08 policy merge",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert_eq!(
        policy_value(&pre_prompt_context.policy_set, "test", "mode"),
        Some(HookValue::Text("strong".to_owned()))
    );
    assert_eq!(
        policy_value(&pre_prompt_context.policy_set, "test", "allowed"),
        Some(HookValue::Bool(true))
    );
    let ordered_keys = pre_prompt_context
        .policy_set
        .entries
        .keys()
        .map(|key| (key.domain.as_str().to_owned(), key.key.as_str().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_keys,
        vec![
            ("test".to_owned(), "allowed".to_owned()),
            ("test".to_owned(), "mode".to_owned())
        ]
    );
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_08_best_effort_policy_hook_failure_does_not_fail_turn() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            true,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_best_effort_policy_failure",
        "ws_phase08_best_effort_policy_failure",
        "turn_phase08_best_effort_policy_failure",
        ThreadMode::Agent,
        "capture",
        "phase 08 best effort policy failure",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(provider.snapshot_requests().len(), 1);
    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert!(pre_prompt_context.policy_set.is_empty());
}

#[tokio::test]
async fn phase_08_fallback_policy_hook_failure_returns_fallback_policy_and_continues() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_fallback(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::Fallback,
            true,
            vec![policy_contribution(
                "test",
                "mode",
                HookValue::Text("fallback".to_owned()),
                1,
            )],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_fallback_policy_failure",
        "ws_phase08_fallback_policy_failure",
        "turn_phase08_fallback_policy_failure",
        ThreadMode::Agent,
        "capture",
        "phase 08 fallback policy failure",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert_eq!(
        policy_value(&pre_prompt_context.policy_set, "test", "mode"),
        Some(HookValue::Text("fallback".to_owned()))
    );
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_08_fail_closed_policy_hook_failure_fails_turn() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::FailClosed,
            true,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_fail_closed_policy_failure",
        "ws_phase08_fail_closed_policy_failure",
        "turn_phase08_fail_closed_policy_failure",
        ThreadMode::Agent,
        "capture",
        "phase 08 fail closed policy failure",
    )
    .await;
    assert_turn_failed(&observed, "turn policy hook failed");

    assert!(provider.snapshot_requests().is_empty());
    let calls = snapshot_hook_calls(&calls);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].phase, HookPhase::TurnPrePolicy);
}

#[tokio::test]
async fn phase_08_policy_contributions_do_not_change_memory_policy_yet() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_phase08_policy_isolation",
                MemoryCategory::Preference,
                Some("city"),
                "User likes Porto.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::explicit_remember(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![policy_contribution(
                "test",
                "memory_disabled",
                HookValue::Bool(true),
                100,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase08_memory_not_migrated",
        "ws_phase08_memory_not_migrated",
        "turn_phase08_memory_not_migrated",
        ThreadMode::Agent,
        "capture",
        "phase 08 memory remains on existing path",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(policy_provider.contexts().len(), 1);
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    assert_eq!(memory_provider.recall_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(prompt.full_system_text.contains("User likes Porto."));
    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert_eq!(
        policy_value(&pre_prompt_context.policy_set, "test", "memory_disabled"),
        Some(HookValue::Bool(true))
    );
}

#[tokio::test]
async fn no_memory_provider_keeps_agent_request_unchanged() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "thr_memory_no_provider",
        "ws_memory_no_provider",
        "turn_memory_no_provider",
        ThreadMode::Agent,
        "capture",
        "hello without memory",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.messages.len(), 1);
    assert_eq!(request.messages[0].role, Role::User);
    assert_eq!(request.messages[0].content, "hello without memory");
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("Memory Recall"));
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include built-in tools");
    assert!(!tools.iter().any(|tool| tool.name == "memory_fake"));
    assert!(!tools.iter().any(|tool| tool.name.starts_with("memory_")));
}

#[tokio::test]
async fn memory_provider_is_optional_and_agent_turn_completes() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager =
        AgentManager::new_with_mcp_and_memory(registry, test_tool_loop_config(), None, None);

    let observed = start_simple_turn(
        &manager,
        "thr_memory_optional",
        "ws_memory_optional",
        "turn_memory_optional",
        ThreadMode::Agent,
        "capture",
        "run without a memory provider",
    )
    .await;
    assert_turn_completed(&observed);
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn memory_provider_recall_error_degrades_gracefully() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Err("recall unavailable".to_owned()),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_recall_error",
        "ws_memory_recall_error",
        "turn_memory_recall_error",
        ThreadMode::Agent,
        "capture",
        "continue even if recall fails",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.recall_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(!prompt.full_system_text.contains("recall unavailable"));
}

#[tokio::test]
async fn memory_provider_tool_materialization_error_degrades_gracefully() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Err("memory tools unavailable".to_owned()),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_tool_error",
        "ws_memory_tool_error",
        "turn_memory_tool_error",
        ThreadMode::Agent,
        "capture",
        "continue even if memory tools fail",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.tool_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("agent request should include built-in tools");
    assert!(!tools.iter().any(|tool| tool.name == "memory_fake"));
    assert!(!tools.iter().any(|tool| tool.name.starts_with("memory_")));
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("## Memory Recall"));
}

#[tokio::test]
async fn memory_provider_receives_turn_context_for_agent_mode() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_context",
        "ws_memory_context",
        "turn_memory_context",
        ThreadMode::Agent,
        "capture",
        "my birthday is May 5",
    )
    .await;
    assert_turn_completed(&observed);

    let recall_contexts = memory_provider.recall_contexts();
    assert_eq!(recall_contexts.len(), 1);
    let context = &recall_contexts[0];
    assert_eq!(context.workspace_id, "ws_memory_context");
    assert_eq!(context.thread_id, "thr_memory_context");
    assert_eq!(context.turn_id, "turn_memory_context");
    assert_eq!(context.mode, ThreadMode::Agent);
    assert_eq!(context.input_text, "my birthday is May 5");
    assert_eq!(context.task_id, None);
    assert_eq!(context.agent_id, None);

    let recall_requests = memory_provider.recall_requests();
    assert_eq!(recall_requests.len(), 1);
    assert_eq!(recall_requests[0].query, "my birthday is May 5");

    let tool_contexts = memory_provider.tool_contexts();
    assert_eq!(tool_contexts, recall_contexts);
}

#[tokio::test]
async fn chat_mode_does_not_call_memory_provider_by_default() {
    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::ok());
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::explicit_remember(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_chat",
        "ws_memory_chat",
        "turn_memory_chat",
        ThreadMode::Chat,
        "capture-standard",
        "plain chat should not touch memory",
    )
    .await;
    assert_turn_completed(&observed);

    assert!(memory_provider.recall_contexts().is_empty());
    assert!(memory_provider.recall_requests().is_empty());
    assert!(memory_provider.tool_contexts().is_empty());
    assert!(policy_provider.contexts().is_empty());
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].tools.is_none());
    assert!(requests[0].compiled_prompt.is_none());
}

#[tokio::test]
async fn memory_tool_materialization_bundles_are_merged_when_provider_returns_them() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: vec!["test memory tool diagnostic".to_owned()],
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_bundle",
        "ws_memory_bundle",
        "turn_memory_bundle",
        ThreadMode::Agent,
        "capture",
        "show memory test tool",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.recall_contexts().len(), 1);
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("agent request should include tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_remember"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(prompt.full_system_text.contains(
        "Available memory tools: memory_search, memory_get, memory_remember, memory_forget."
    ));
    assert!(
        !prompt
            .full_system_text
            .contains("test memory tool diagnostic")
    );
}

#[tokio::test]
async fn identity_recall_prompt_contains_relevant_memory() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_identity_name",
                MemoryCategory::Identity,
                Some("name"),
                "User's name is Alexander.",
            )],
            diagnostics: vec!["internal recall diagnostic".to_owned()],
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_identity_recall",
        "ws_memory_identity_recall",
        "turn_memory_identity_recall",
        ThreadMode::Agent,
        "capture",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include memory tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(
        prompt
            .full_system_text
            .contains("User's name is Alexander.")
    );
    assert!(prompt.full_system_text.contains("mem_identity_name"));
    assert!(
        !prompt
            .full_system_text
            .contains("internal recall diagnostic")
    );

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    assert!(
        manifest
            .section_ids
            .iter()
            .any(|section| section == "memory_recall")
    );
}

#[tokio::test]
async fn memory_provider_without_tools_has_no_policy_or_recall() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_should_not_render",
                MemoryCategory::Identity,
                Some("name"),
                "User's name is Alexander.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization::default()),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_no_tools",
        "ws_memory_no_tools",
        "turn_memory_no_tools",
        ThreadMode::Agent,
        "capture",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.tool_contexts().len(), 1);
    assert!(memory_provider.recall_contexts().is_empty());
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include built-in tools");
    assert!(!tools.iter().any(|tool| tool.name.starts_with("memory_")));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("## Memory Recall"));
    assert!(
        !prompt
            .full_system_text
            .contains("User's name is Alexander.")
    );
}

#[tokio::test]
async fn memory_policy_classifier_no_use_disables_recall_without_phrase_matching() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_ignore",
                MemoryCategory::Preference,
                Some("city"),
                "User likes Porto.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(MemoryTurnPolicy::no_use()));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_ignore",
        "ws_memory_ignore",
        "turn_memory_ignore",
        ThreadMode::Agent,
        "capture",
        "No uses la memoria para esta respuesta; responde directamente.",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(policy_provider.contexts().len(), 1);
    assert!(memory_provider.tool_contexts().is_empty());
    assert!(memory_provider.recall_contexts().is_empty());
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include built-in tools");
    assert!(!tools.iter().any(|tool| tool.name.starts_with("memory_")));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("## Memory Recall"));
    assert!(!prompt.full_system_text.contains("User likes Porto."));
}

#[tokio::test]
async fn memory_policy_no_save_keeps_read_recall_but_blocks_remember() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_russian_ignore",
                MemoryCategory::Preference,
                Some("city"),
                "User likes Porto.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::no_save(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_no_save",
        "ws_memory_no_save",
        "turn_memory_no_save",
        ThreadMode::Agent,
        "capture",
        "Speichere das nicht, aber nutze vorhandenen Kontext falls hilfreich.",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(policy_provider.contexts().len(), 1);
    let policy_requests = policy_provider.requests();
    assert_eq!(policy_requests.len(), 1);
    assert_eq!(
        policy_requests[0].default_policy.post_turn_extraction,
        MemoryExtractionPolicy::Disabled
    );
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    assert_eq!(memory_provider.recall_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    assert!(!tools.iter().any(|tool| tool.name == "memory_remember"));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(
        prompt
            .full_system_text
            .contains("Memory writes are disabled for this turn")
    );
    assert!(prompt.full_system_text.contains("User likes Porto."));
    assert!(
        prompt
            .full_system_text
            .contains("Do not store, update, infer, or extract new memories")
    );
}

#[tokio::test]
async fn memory_policy_invalid_classifier_json_uses_default_allow_fallback() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_fallback",
                MemoryCategory::Preference,
                Some("city"),
                "User likes Porto.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::failing(
        "classifier_invalid_json",
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_policy_fallback",
        "ws_memory_policy_fallback",
        "turn_memory_policy_fallback",
        ThreadMode::Agent,
        "capture",
        "Ayudame con el proyecto.",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(policy_provider.contexts().len(), 1);
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    assert_eq!(memory_provider.recall_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include memory tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_remember"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(prompt.full_system_text.contains("User likes Porto."));
    assert!(
        prompt
            .full_system_text
            .contains("Call memory_remember proactively")
    );
}

#[tokio::test]
async fn recall_sensitive_policy_mentions_memory_search_when_recall_is_insufficient() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_search_policy",
        "ws_memory_search_policy",
        "turn_memory_search_policy",
        ThreadMode::Agent,
        "capture",
        "do you remember my birthday?",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include memory tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("If unsure on a non-trivial task, do one lightweight memory_search")
    );
    assert!(!prompt.full_system_text.contains("Relevant memories:"));
}

#[tokio::test]
async fn memory_policy_default_allows_proactive_remember_tool() {
    let (memory_bundle, calls) = recording_standard_memory_tool_bundle();
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_memory_remember_proactive".to_owned(),
            name: "memory_remember".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "remembered",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![memory_bundle],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::proactive_write_allowed(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_proactive_remember_tool",
        "ws_memory_proactive_remember_tool",
        "turn_memory_proactive_remember_tool",
        ThreadMode::Agent,
        "sequenced-tools",
        "I prefer concise answers for architecture proposals.",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(
        calls
            .lock()
            .expect("memory tool calls lock poisoned")
            .as_slice(),
        &["memory_remember".to_owned()]
    );
    assert_eq!(policy_provider.contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert!(requests.len() >= 2);
    let first_request = &requests[0];
    let tools = first_request
        .tools
        .as_ref()
        .expect("first request should include memory tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_remember"));
    let first_prompt = first_request
        .compiled_prompt
        .as_ref()
        .expect("first request should include prompt");
    assert!(
        first_prompt
            .full_system_text
            .contains("Call memory_remember proactively")
    );
}

#[tokio::test]
async fn explicit_remember_request_can_trigger_memory_remember() {
    let (memory_bundle, calls) = recording_standard_memory_tool_bundle();
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_memory_remember".to_owned(),
            name: "memory_remember".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "remembered",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![memory_bundle],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::explicit_remember(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_remember_tool",
        "ws_memory_remember_tool",
        "turn_memory_remember_tool",
        ThreadMode::Agent,
        "sequenced-tools",
        "remember that my name is Alexander",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(
        calls
            .lock()
            .expect("memory tool calls lock poisoned")
            .as_slice(),
        &["memory_remember".to_owned()]
    );
    assert_eq!(policy_provider.contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert!(requests.len() >= 2);
    let first_prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("first request should include prompt");
    assert!(
        first_prompt
            .full_system_text
            .contains("Call memory_remember proactively")
    );
}

#[tokio::test]
async fn explicit_forget_request_can_trigger_memory_forget() {
    let (memory_bundle, calls) = recording_standard_memory_tool_bundle();
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_memory_forget".to_owned(),
            name: "memory_forget".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "forgotten",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot::empty()),
        Ok(MemoryToolMaterialization {
            bundles: vec![memory_bundle],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned())),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_forget_tool",
        "ws_memory_forget_tool",
        "turn_memory_forget_tool",
        ThreadMode::Agent,
        "sequenced-tools",
        "forget my birthday",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(
        calls
            .lock()
            .expect("memory tool calls lock poisoned")
            .as_slice(),
        &["memory_forget".to_owned()]
    );
    assert_eq!(policy_provider.contexts().len(), 1);
    assert!(memory_provider.recall_contexts().is_empty());
    let requests = provider.snapshot_requests();
    assert!(requests.len() >= 2);
    let first_request = &requests[0];
    let tools = first_request
        .tools
        .as_ref()
        .expect("first request should include memory tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    assert!(!tools.iter().any(|tool| tool.name == "memory_remember"));
    let first_prompt = first_request
        .compiled_prompt
        .as_ref()
        .expect("first request should include prompt");
    assert!(
        first_prompt
            .full_system_text
            .contains("If the user asks you to forget something, call memory_forget.")
    );
    assert!(
        first_prompt
            .full_system_text
            .contains("only to identify and forget")
    );
    assert!(!first_prompt.full_system_text.contains("Relevant memories:"));
}

#[tokio::test]
async fn remembered_memory_is_recalled_in_new_thread_and_forget_suppresses_it() {
    let remember_provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_stateful_memory_remember".to_owned(),
            name: "memory_remember".to_owned(),
            arguments: serde_json::json!({
                "content": "User's name is Alexander.",
                "category": "identity",
                "scope": "user",
                "key": "name",
                "source": "explicit_user_request"
            })
            .to_string(),
        }],
        "remembered",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-remember",
        remember_provider.clone(),
    ));
    let manager = AgentManager::new(registry.clone(), test_tool_loop_config());
    let memory_provider = Arc::new(StatefulMemoryProvider::default());
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider;
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_stateful_memory_remember",
        "ws_stateful_memory",
        "turn_stateful_memory_remember",
        ThreadMode::Agent,
        "sequenced-remember",
        "remember that my name is Alexander",
    )
    .await;
    assert_turn_completed(&observed);

    let recall_provider = Arc::new(CaptureAgentProvider::default());
    registry.insert("capture-recall", recall_provider.clone());
    let observed = start_simple_turn(
        &manager,
        "thr_stateful_memory_recall_new",
        "ws_stateful_memory",
        "turn_stateful_memory_recall_new",
        ThreadMode::Agent,
        "capture-recall",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);
    let recall_requests = recall_provider.snapshot_requests();
    assert_eq!(recall_requests.len(), 1);
    let recall_prompt = recall_requests[0]
        .compiled_prompt
        .as_ref()
        .expect("recall turn should include prompt");
    assert!(recall_prompt.full_system_text.contains("## Memory Recall"));
    assert!(
        recall_prompt
            .full_system_text
            .contains("User's name is Alexander.")
    );

    let forget_provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_stateful_memory_forget".to_owned(),
            name: "memory_forget".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "forgotten",
    ));
    registry.insert("sequenced-forget", forget_provider);
    let observed = start_simple_turn(
        &manager,
        "thr_stateful_memory_forget",
        "ws_stateful_memory",
        "turn_stateful_memory_forget",
        ThreadMode::Agent,
        "sequenced-forget",
        "forget my name",
    )
    .await;
    assert_turn_completed(&observed);

    let after_forget_provider = Arc::new(CaptureAgentProvider::default());
    registry.insert("capture-after-forget", after_forget_provider.clone());
    let observed = start_simple_turn(
        &manager,
        "thr_stateful_memory_after_forget",
        "ws_stateful_memory",
        "turn_stateful_memory_after_forget",
        ThreadMode::Agent,
        "capture-after-forget",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);
    let after_forget_requests = after_forget_provider.snapshot_requests();
    assert_eq!(after_forget_requests.len(), 1);
    let after_forget_prompt = after_forget_requests[0]
        .compiled_prompt
        .as_ref()
        .expect("after-forget turn should include prompt");
    assert!(
        after_forget_prompt
            .full_system_text
            .contains("## Memory Recall")
    );
    assert!(
        !after_forget_prompt
            .full_system_text
            .contains("User's name is Alexander.")
    );
    assert!(
        !after_forget_prompt
            .full_system_text
            .contains("Relevant memories:")
    );
}

fn loop_budget_manager(
    provider: Arc<LoopBudgetProvider>,
    tool_loop_config: ToolLoopConfig,
) -> AgentManager {
    let registry = Arc::new(ProviderRegistry::with_provider("loop-budget", provider));
    AgentManager::new(registry, tool_loop_config)
}

fn tool_result_message_count(request: &ChatRequest) -> usize {
    request
        .messages
        .iter()
        .filter(|message| message.role == Role::Tool)
        .count()
}

async fn start_loop_budget_turn(
    manager: &AgentManager,
    thread_id: &str,
    turn_id: &str,
) -> tokio::sync::mpsc::Receiver<AgentEvent> {
    manager
        .ensure_thread(thread_id, "ws_loop_budget")
        .await
        .expect("thread should be created");
    let events = subscribe_agent_events(manager, thread_id).await;
    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "loop-budget",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run loop budget test".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");
    events
}

#[tokio::test]
async fn tool_loop_provider_round_budget_requests_final_no_tools_round() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 2;
    config.budget.max_tool_calls_per_turn = 16;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_rounds",
        "turn_loop_budget_rounds",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    assert!(matches!(terminal, AgentEvent::TurnCompleted { .. }));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnToolLoopBudgetExceeded(notification)
            if notification.limit_kind == ToolLoopBudgetLimitKind::AgentRounds
                && notification.action == ToolLoopBudgetAction::RequestFinalNoToolsRound
                && notification.turn_id == "turn_loop_budget_rounds"
    )));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .tools
            .as_ref()
            .map(|tools| !tools.is_empty())
            .unwrap_or(false)
    );
    assert!(
        requests[1]
            .tools
            .as_ref()
            .map(|tools| !tools.is_empty())
            .unwrap_or(false)
    );
    assert!(
        requests[2].tools.is_none(),
        "final budget round must send no tool definitions"
    );
    assert_eq!(
        tool_result_message_count(&requests[2]),
        2,
        "only tool calls from the two tool-capable rounds should reach model history"
    );
}

#[tokio::test]
async fn memory_recall_policy_is_omitted_when_tool_loop_disables_tools() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 2;
    config.budget.max_tool_calls_per_turn = 16;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_loop_budget",
                MemoryCategory::Identity,
                Some("name"),
                "User's name is Alexander.",
            )],
            diagnostics: Vec::new(),
            truncated: false,
        }),
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;

    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_memory_policy",
        "turn_loop_budget_memory_policy",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    assert!(matches!(
        observed_events.last(),
        Some(AgentEvent::TurnCompleted { .. })
    ));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .tools
            .as_ref()
            .expect("first round should include tools")
            .iter()
            .any(|tool| tool.name == "memory_search")
    );
    assert!(
        requests[0]
            .compiled_prompt
            .as_ref()
            .expect("first round should include prompt")
            .full_system_text
            .contains("## Memory Recall")
    );
    assert!(requests[2].tools.is_none());
    assert!(
        !requests[2]
            .compiled_prompt
            .as_ref()
            .expect("final no-tools round should include prompt")
            .full_system_text
            .contains("## Memory Recall")
    );
}

#[tokio::test]
async fn tool_loop_fails_when_provider_requests_tools_after_tools_disabled() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::AlwaysTools,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 1;
    config.budget.max_tool_calls_per_turn = 16;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_disabled_tools",
        "turn_loop_budget_disabled_tools",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    match terminal {
        AgentEvent::TurnFailed { error, .. } => {
            assert!(error.contains("tool_loop_budget_exceeded"));
            assert!(error.contains("provider_returned_tools_after_tools_disabled"));
        }
        other => panic!("turn should fail deterministically: {other:?}"),
    }
    let budget_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::TurnToolLoopBudgetExceeded(notification)
                    if notification.limit_kind
                        == ToolLoopBudgetLimitKind::ProviderReturnedToolsAfterToolsDisabled
                        && notification.action == ToolLoopBudgetAction::FailTurn
            )
        })
        .expect("hard loop-budget event should be emitted");
    let failed_index = observed_events
        .iter()
        .position(|event| matches!(event, AgentEvent::TurnFailed { .. }))
        .expect("turn failed event should be emitted");
    assert!(budget_index < failed_index);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].tools.is_none());
    assert_eq!(
        tool_result_message_count(&requests[1]),
        1,
        "only the first tool-capable round should execute a tool"
    );
}

#[tokio::test]
async fn tool_loop_total_tool_call_budget_prevents_execution() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::TooManyToolsThenFinal,
        3,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 8;
    config.budget.max_tool_calls_per_turn = 2;
    config.retry.max_recoverable_retry_rounds_per_episode = 8;
    config.retry.max_same_tool_error_retries_per_episode = 8;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_tool_calls",
        "turn_loop_budget_tool_calls",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    assert!(matches!(terminal, AgentEvent::TurnCompleted { .. }));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnToolLoopBudgetExceeded(notification)
            if notification.limit_kind == ToolLoopBudgetLimitKind::ToolCalls
                && notification.action == ToolLoopBudgetAction::RequestFinalNoToolsRound
    )));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].tools.is_none());
    assert_eq!(
        tool_result_message_count(&requests[1]),
        0,
        "tool calls that exceed total budget must not execute"
    );
    assert!(
        requests[1]
            .messages
            .iter()
            .all(|message| message.tool_calls.as_ref().is_none_or(Vec::is_empty)),
        "excess assistant tool calls must not be appended as unanswered history"
    );
}

#[tokio::test]
async fn tool_loop_recoverable_retry_rounds_are_bounded() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::RepeatedMissingToolThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 8;
    config.budget.max_tool_calls_per_turn = 8;
    config.retry.max_recoverable_retry_rounds_per_episode = 1;
    config.retry.max_same_tool_error_retries_per_episode = 8;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_retries",
        "turn_loop_budget_retries",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    assert!(matches!(terminal, AgentEvent::TurnCompleted { .. }));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::ItemToolRetryScheduled(notification)
            if notification.tool_name == "missing_loop_budget_tool"
    )));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::ItemToolRetryExhausted(notification)
            if notification.tool_name == "missing_loop_budget_tool"
    )));
    let missing_tool_snapshot = observed_events
        .iter()
        .find_map(|event| {
            if let AgentEvent::ItemCompleted(notification) = event
                && let TurnItem::DynamicToolCall {
                    tool_name,
                    recovery_policy,
                    ..
                } = &notification.item
                && tool_name == "missing_loop_budget_tool"
            {
                return recovery_policy.clone();
            }
            None
        })
        .expect("unknown tool item should have conservative recovery policy");
    assert_eq!(
        missing_tool_snapshot.retry_class,
        ToolRecoveryRetryClass::Never
    );
    assert_eq!(
        missing_tool_snapshot.idempotency_mode,
        ToolRecoveryIdempotencyMode::None
    );
    assert_eq!(missing_tool_snapshot.max_attempts, 1);
    assert_eq!(
        missing_tool_snapshot.resolved_action,
        RecoveryAction::MarkFailed
    );

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].tools.is_none());
    assert_eq!(tool_result_message_count(&requests[2]), 2);
}

#[tokio::test]
async fn tool_loop_same_failure_signature_is_bounded() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::RepeatedMissingToolThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 8;
    config.budget.max_tool_calls_per_turn = 8;
    config.retry.max_recoverable_retry_rounds_per_episode = 8;
    config.retry.max_same_tool_error_retries_per_episode = 1;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_same_failure",
        "turn_loop_budget_same_failure",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    assert!(matches!(terminal, AgentEvent::TurnCompleted { .. }));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::ItemToolRetryScheduled(notification)
            if notification.tool_name == "missing_loop_budget_tool"
    )));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::ItemToolRetryExhausted(notification)
            if notification.tool_name == "missing_loop_budget_tool"
    )));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].tools.is_none());
    assert_eq!(tool_result_message_count(&requests[2]), 2);
}

#[tokio::test]
async fn tool_retry_budget_resets_after_successful_episode() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::RetryEpisodeResetThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    config.budget.max_agent_rounds_per_turn = 8;
    config.budget.max_tool_calls_per_turn = 8;
    config.retry.max_recoverable_retry_rounds_per_episode = 1;
    config.retry.max_same_tool_error_retries_per_episode = 1;
    config.retry.max_retries_per_tool_name_per_episode = 1;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_retry_episode_reset",
        "turn_loop_budget_retry_episode_reset",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    let terminal = observed_events
        .last()
        .expect("terminal event should be observed");
    assert!(matches!(terminal, AgentEvent::TurnCompleted { .. }));
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::ItemToolRetryResolved(notification)
            if notification.resolution == ToolRetryResolution::Succeeded
    )));

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 4,
        "successful retry reset should allow another provider round with tools"
    );
    assert!(
        requests[3]
            .tools
            .as_ref()
            .map(|tools| !tools.is_empty())
            .unwrap_or(false),
        "second same-tool failure must schedule a normal retry round, not final no-tools exhaustion"
    );
}

#[tokio::test]
async fn resolved_tool_items_emit_matching_recovery_policy_snapshots() {
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_policy_snapshot".to_owned(),
            name: "tool_suggest".to_owned(),
            arguments: serde_json::json!({"intent": "find a tool"}).to_string(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "thr_tool_policy_snapshot";
    let turn_id = "turn_tool_policy_snapshot";
    manager
        .ensure_thread(thread_id, "ws_tool_policy_snapshot")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "suggest a tool".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed_events = recv_events_until_terminal(&mut events).await;
    let started_snapshot = observed_events.iter().find_map(|event| {
        if let AgentEvent::ItemStarted(notification) = event
            && let TurnItem::DynamicToolCall {
                tool_name,
                recovery_policy,
                ..
            } = &notification.item
            && tool_name == "tool_suggest"
        {
            return recovery_policy.clone();
        }
        None
    });
    let completed_snapshot = observed_events.iter().find_map(|event| {
        if let AgentEvent::ItemCompleted(notification) = event
            && let TurnItem::DynamicToolCall {
                tool_name,
                recovery_policy,
                ..
            } = &notification.item
            && tool_name == "tool_suggest"
        {
            return recovery_policy.clone();
        }
        None
    });

    let started_snapshot = started_snapshot.expect("started tool item should have policy");
    let completed_snapshot = completed_snapshot.expect("completed tool item should have policy");
    assert_eq!(started_snapshot, completed_snapshot);
    assert_eq!(
        started_snapshot.retry_class,
        ToolRecoveryRetryClass::Arguments
    );
    assert_eq!(
        started_snapshot.idempotency_mode,
        ToolRecoveryIdempotencyMode::Safe
    );
    assert_eq!(started_snapshot.max_attempts, 2);
    assert!(!started_snapshot.can_resume);
    assert_eq!(
        started_snapshot.resolved_action,
        RecoveryAction::RetryAttempt
    );
    assert_eq!(started_snapshot.base_backoff_secs, 1);
    assert_eq!(started_snapshot.max_wall_clock_secs, 300);
    assert_eq!(started_snapshot.no_progress_limit, 2);
}

#[tokio::test(start_paused = true)]
async fn start_turn_emits_lifecycle_events() {
    let manager = test_manager();
    manager
        .ensure_thread("thr_000000000000000001", "ws_000000000000000001")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000001").await;

    manager
        .start_turn(
            "thr_000000000000000001",
            "turn_000000000000000001",
            ThreadMode::Chat,
            "openai/gpt-4o",
            "echo",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let thinking_started = timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("expected thinking item_started")
        .expect("broadcast should be open");
    advance(Duration::from_secs(30)).await;
    yield_now().await;

    // Collect remaining events — the provider call will fail in tests
    // (no real HTTP), so we expect a TurnFailed event. This is expected
    // behavior for unit tests; integration tests would use a mock server.
    let mut collected = vec![thinking_started];
    while let Ok(Some(event)) = timeout(Duration::from_secs(1), events.recv()).await {
        let is_terminal = matches!(
            event,
            AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
        );
        collected.push(event);
        if is_terminal {
            break;
        }
    }

    // First event should be ItemStarted (Reasoning)
    match &collected[0] {
        AgentEvent::ItemStarted(notification) => match &notification.item {
            TurnItem::Reasoning {
                summary, content, ..
            } => {
                assert_eq!(notification.workspace_id, "ws_000000000000000001");
                assert!(summary.is_empty());
                assert!(content.is_empty());
            }
            other => panic!("unexpected item: {other:?}"),
        },
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn start_turn_rejects_second_running_turn() {
    let registry = Arc::new(ProviderRegistry::with_provider(
        "pending",
        Arc::new(PendingProvider),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .ensure_thread("thr_000000000000000002", "ws_000000000000000002")
        .await
        .expect("thread should be created");

    manager
        .start_turn(
            "thr_000000000000000002",
            "turn_000000000000000002",
            ThreadMode::Chat,
            "openai/gpt-4o",
            "pending",
            HashMap::new(),
            vec![UserInput::Text {
                text: "first".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("first turn should start");

    let error = manager
        .start_turn(
            "thr_000000000000000002",
            "turn_000000000000000003",
            ThreadMode::Chat,
            "openai/gpt-4o",
            "pending",
            HashMap::new(),
            vec![UserInput::Text {
                text: "second".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect_err("second turn should be rejected while first is running");
    assert_eq!(error, AgentStartError::TurnAlreadyRunning);
}

#[tokio::test(start_paused = true)]
async fn cancel_turn_emits_interrupted_durable_event() {
    let registry = Arc::new(ProviderRegistry::with_provider(
        "pending",
        Arc::new(PendingProvider),
    ));
    let manager = Arc::new(AgentManager::new(registry, test_tool_loop_config()));
    let thread_id = "thr_000000000000000003";
    let turn_id = "turn_000000000000000003";

    manager
        .ensure_thread(thread_id, "ws_000000000000000003")
        .await
        .expect("thread should be created");
    let mut durable_events = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Chat,
            "openai/gpt-4o",
            "pending",
            HashMap::new(),
            vec![UserInput::Text {
                text: "first".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let cancel_manager = manager.clone();
    let cancel_handle = tokio::spawn(async move {
        cancel_manager
            .cancel_turn(thread_id, turn_id, "user clicked stop")
            .await
    });

    yield_now().await;
    advance(Duration::from_millis(800)).await;
    yield_now().await;

    cancel_handle
        .await
        .expect("cancel task should not panic")
        .expect("turn cancellation should succeed");

    for _ in 0..16 {
        let event = timeout(Duration::from_secs(1), durable_events.recv())
            .await
            .expect("durable event should be emitted")
            .expect("durable receiver should stay open");
        if let AgentDurableEvent::TurnInterrupted {
            thread_id: event_thread_id,
            turn_id: event_turn_id,
            reason,
            recovery,
        } = event
        {
            assert_eq!(event_thread_id, thread_id);
            assert_eq!(event_turn_id, turn_id);
            assert_eq!(reason, "user clicked stop");
            assert!(recovery.is_none());
            return;
        }
    }

    panic!("turn cancellation should emit TurnInterrupted");
}

#[tokio::test(start_paused = true)]
async fn non_tool_recovery_request_restarts_turn_without_failing() {
    let registry = Arc::new(ProviderRegistry::with_provider(
        "pending",
        Arc::new(PendingProvider),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "thr_000000000000000004";
    let turn_id = "turn_000000000000000004";

    manager
        .ensure_thread(thread_id, "ws_000000000000000004")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Chat,
            "openai/gpt-4o",
            "pending",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let _ = timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("expected at least one lifecycle event")
        .expect("broadcast should be open");

    loop {
        match events.try_recv() {
            Ok(_) => continue,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    manager
        .start_recovery_attempt(
            thread_id,
            RecoveryAttemptRequest {
                recovery_job_id: "recovery_job_1".to_owned(),
                recovery_attempt_id: "recovery_attempt_1".to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: "reasoning_item".to_owned(),
                item_type: TurnItemType::Reasoning,
                force_non_stream: false,
                refresh_provider_auth: false,
                compact_history: false,
                continue_generation: false,
                model_override: None,
                retained_llm_context: Vec::new(),
            },
        )
        .await
        .expect("non-tool recovery request should restart active turn");

    yield_now().await;

    let mut saw_restart_item_started = false;
    for _ in 0..8 {
        let event = timeout(Duration::from_millis(250), events.recv()).await;
        let Ok(Some(event)) = event else {
            continue;
        };

        match event {
            AgentEvent::ItemStarted(notification) if notification.turn_id == turn_id => {
                saw_restart_item_started = true;
                break;
            }
            AgentEvent::TurnFailed { .. } => {
                panic!("non-tool recovery must not fail the turn");
            }
            _ => {}
        }
    }

    assert!(
        saw_restart_item_started,
        "non-tool recovery should restart turn flow and emit item lifecycle again"
    );
}

#[tokio::test]
async fn continue_generation_recovery_is_compiled_into_system_prompt() {
    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "thr_000000000000000004a";
    let turn_id = "turn_000000000000000004a";

    manager
        .ensure_thread(thread_id, "ws_000000000000000004a")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture-standard",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");

        if matches!(event, AgentEvent::TurnCompleted { .. }) {
            break;
        }
    }

    let recovery_job_id = "recovery_job_2";
    let recovery_attempt_id = "recovery_attempt_2";
    manager
        .start_recovery_attempt(
            thread_id,
            RecoveryAttemptRequest {
                recovery_job_id: recovery_job_id.to_owned(),
                recovery_attempt_id: recovery_attempt_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: "reasoning_item".to_owned(),
                item_type: TurnItemType::Reasoning,
                force_non_stream: false,
                refresh_provider_auth: false,
                compact_history: false,
                continue_generation: true,
                model_override: None,
                retained_llm_context: Vec::new(),
            },
        )
        .await
        .expect("recovery request should restart completed turn");

    let mut saw_recovery_success = false;
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");

        if matches!(
            event,
            AgentEvent::RecoveryAttemptSucceeded {
                ref recovery,
                ..
            } if recovery.job_id == recovery_job_id && recovery.attempt_id == recovery_attempt_id
        ) {
            saw_recovery_success = true;
            break;
        }
    }
    assert!(
        saw_recovery_success,
        "provider recovery should succeed at the provider response boundary"
    );

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "expected initial turn plus recovery restart request"
    );
    let recovery_request = &requests[1];
    let continuation_prompt = recovery_request
        .compiled_prompt
        .as_ref()
        .expect("recovery request should include compiled prompt payload");
    assert!(
        continuation_prompt
            .full_system_text
            .contains("Continue from where it stopped without repeating prior text.")
    );
    assert!(
        continuation_prompt
            .full_system_text
            .contains("Recovery Continuation")
    );
    assert!(
        continuation_prompt
            .full_system_text
            .contains("Current date/time:")
    );
    assert!(continuation_prompt.full_system_text.contains("OS:"));
}

#[tokio::test]
async fn provider_recovery_success_boundary_clears_recovery_before_later_provider_failure() {
    let provider = Arc::new(ProviderRecoveryBoundaryProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "provider-recovery-boundary",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "thr_000000000000000004b";
    let turn_id = "turn_000000000000000004b";
    let recovery_job_id = "recovery_job_provider_boundary";
    let recovery_attempt_id = "recovery_attempt_provider_boundary";

    manager
        .ensure_thread(thread_id, "ws_000000000000000004b")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "provider-recovery-boundary",
            HashMap::new(),
            vec![UserInput::Text {
                text: "trigger provider failure".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut saw_initial_failure = false;
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive initial provider failure")
            .expect("broadcast should remain open");
        if let AgentEvent::ProviderFailureDetected { recovery, .. } = event {
            assert!(
                recovery.is_none(),
                "initial provider failure must not be attributed to recovery"
            );
            saw_initial_failure = true;
            break;
        }
    }
    assert!(
        saw_initial_failure,
        "initial provider failure should be emitted before recovery starts"
    );

    manager
        .start_recovery_attempt(
            thread_id,
            RecoveryAttemptRequest {
                recovery_job_id: recovery_job_id.to_owned(),
                recovery_attempt_id: recovery_attempt_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: "reasoning_item".to_owned(),
                item_type: TurnItemType::Reasoning,
                force_non_stream: false,
                refresh_provider_auth: false,
                compact_history: false,
                continue_generation: false,
                model_override: None,
                retained_llm_context: Vec::new(),
            },
        )
        .await
        .expect("provider recovery should restart completed provider attempt");

    let mut saw_recovery_success = false;
    let mut saw_late_failure_without_recovery = false;
    for _ in 0..80 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive recovery boundary events")
            .expect("broadcast should remain open");

        match event {
            AgentEvent::RecoveryAttemptSucceeded { recovery, .. } => {
                assert_eq!(recovery.job_id, recovery_job_id);
                assert_eq!(recovery.attempt_id, recovery_attempt_id);
                saw_recovery_success = true;
            }
            AgentEvent::ProviderFailureDetected {
                failure, recovery, ..
            } if failure
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("late provider failure") =>
            {
                assert!(
                    saw_recovery_success,
                    "recovery success must be emitted before the later provider failure"
                );
                assert!(
                    recovery.is_none(),
                    "later provider failure must start a fresh recovery budget"
                );
                saw_late_failure_without_recovery = true;
                break;
            }
            AgentEvent::TurnFailed { error, .. } => {
                panic!(
                    "agent manager should surface provider failure, not terminal failure: {error}"
                );
            }
            _ => {}
        }
    }

    assert!(
        saw_recovery_success,
        "successful provider round should close active provider recovery"
    );
    assert!(
        saw_late_failure_without_recovery,
        "later provider failure should not be attached to the old recovery"
    );
    assert!(
        provider.snapshot_requests().len() >= 3,
        "provider should receive initial request, recovery request, and later request"
    );
}

#[tokio::test]
async fn explicit_skill_input_injects_skill_prompt_and_binding() {
    let skill_root = unique_temp_dir("skills");
    let skill_dir = skill_root.join("tests").join("my-skill");
    fs::create_dir_all(&skill_dir).expect("failed to create skill dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: My Skill\nslug: my-skill\ndescription: Test skill description\n---\nFollow the skill.",
    )
    .expect("failed to write SKILL.md");

    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000010";
    let turn_id = "turn_000000000000000010";
    manager
        .ensure_thread(thread_id, "ws_000000000000000010")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "run with skill".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut saw_binding_event = false;
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");

        match event {
            AgentEvent::TurnSkillsResolved { bindings, .. } => {
                assert_eq!(bindings.len(), 1);
                assert_eq!(bindings[0].skill_slug, "tests/my-skill");
                assert_eq!(bindings[0].resolved_reason, "explicit_mention");
                saw_binding_event = true;
            }
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => {
                panic!("turn should not fail: {error}");
            }
            _ => {}
        }
    }

    assert!(
        saw_binding_event,
        "expected TurnSkillsResolved event with one active skill"
    );

    let requests = provider.snapshot_requests();
    assert!(
        !requests.is_empty(),
        "provider should receive at least one request"
    );

    let compiled_prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("compiled prompt should be attached to provider request");
    assert!(compiled_prompt.full_system_text.contains("[Skills]"));
    assert!(compiled_prompt.full_system_text.contains("$tests/my-skill"));
    assert!(compiled_prompt.full_system_text.contains("My Skill"));

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn explicit_skill_input_injects_prompt_for_non_tool_calling_provider() {
    let skill_root = unique_temp_dir("skills-standard");
    let skill_dir = skill_root.join("tests").join("my-skill");
    fs::create_dir_all(&skill_dir).expect("failed to create skill dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: My Skill\nslug: my-skill\ndescription: Test skill description\n---\nFollow the skill.",
    )
    .expect("failed to write SKILL.md");

    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000011";
    let turn_id = "turn_000000000000000011";
    manager
        .ensure_thread(thread_id, "ws_000000000000000011")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture-standard",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "run with skill".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");

        match event {
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => {
                panic!("turn should not fail: {error}");
            }
            _ => {}
        }
    }

    let requests = provider.snapshot_requests();
    assert!(
        !requests.is_empty(),
        "provider should receive at least one request"
    );

    let compiled_prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("compiled prompt should be attached to provider request");
    assert!(compiled_prompt.full_system_text.contains("[Skills]"));
    assert!(compiled_prompt.full_system_text.contains("$tests/my-skill"));

    let _ = fs::remove_dir_all(skill_root);
}

fn write_skill(
    root: &std::path::Path,
    slug: &str,
    body: &str,
    runtime_yaml: Option<&str>,
) -> PathBuf {
    let skill_dir = root.join("tests").join(slug);
    fs::create_dir_all(&skill_dir).expect("create skill directory");
    let runtime_block = runtime_yaml
        .map(|runtime| format!("{runtime}\n"))
        .unwrap_or_default();
    fs::write(
        skill_dir.join("SKILL.md"),
        format!(
            "---\nname: {}\nslug: {}\ndescription: {}\n{}---\n{}",
            slug, slug, slug, runtime_block, body
        ),
    )
    .expect("write skill markdown");

    skill_dir
}

#[tokio::test]
async fn turn_execution_control_emits_recovery_success_on_attempt_completion() {
    let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(4);
    let control = TurnExecutionControl::new(command_tx, 7);
    let recovery = RecoveryAttemptContext {
        job_id: "recovery_job_control".to_owned(),
        attempt_id: "recovery_attempt_control".to_owned(),
    };

    let _token = control.register_attempt("tool_item_1".to_owned()).await;
    assert!(
        control
            .cancel_attempt_for_recovery("tool_item_1", recovery.clone())
            .await
    );
    control
        .complete_attempt("turn_control_recovery", "tool_item_1")
        .await;

    let command = timeout(Duration::from_secs(1), command_rx.recv())
        .await
        .expect("recovery success command should be emitted")
        .expect("command channel should remain open");

    match command {
        AgentCommand::RecoveryAttemptSucceeded {
            turn_id,
            run_id,
            recovery: emitted,
        } => {
            assert_eq!(turn_id, "turn_control_recovery");
            assert_eq!(run_id, 7);
            assert_eq!(emitted, recovery);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[tokio::test]
async fn active_skill_contributes_dynamic_tool_definition_to_model_request() {
    let skill_root = unique_temp_dir("dynamic-tool-def");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Skill body",
        Some(
            r#"runtime:
  tools:
    - tool_slug: fetch_data
      description: Fetch data
      kind: http
      parameters:
        type: object
      execution_class: shared
      config:
        method: GET
        url: https://example.com"#,
        ),
    );

    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000120", "ws_000000000000000120")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000120").await;

    manager
        .start_turn(
            "thr_000000000000000120",
            "turn_000000000000000120",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "use the skill".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event")
            .expect("broadcast should remain open");
        if matches!(event, AgentEvent::TurnCompleted { .. }) {
            break;
        }
    }

    let requests = provider.snapshot_requests();
    assert!(!requests.is_empty());
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("agent request should include tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"skill.tests-my-skill.fetch-data"));
    assert!(tool_names.contains(&"read_skill"));

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn dynamic_skill_tool_executes_and_emits_dynamic_tool_call() {
    let skill_root = unique_temp_dir("dynamic-shell");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Skill body",
        Some(
            r#"runtime:
  tools:
    - tool_slug: echo_shell
      description: Echo shell
      kind: shell
      parameters:
        type: object
      execution_class: session_scoped
      config:
        command: ["sh", "-lc", "printf shell-ok"]
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
          max_total_bytes: 1048576"#,
        ),
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_dynamic_1".to_owned(),
            name: "skill.tests-my-skill.echo-shell".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.runtime.allow_shell_tools = true;
    tool_loop_config.skills.security.min_trust_for_shell_tools = SkillTrustLevel::Community;

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000121";
    let turn_id = "turn_000000000000000121";
    manager
        .ensure_thread(thread_id, "ws_000000000000000121")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "trigger tool".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut observed_events = Vec::new();
    let event_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() <= event_deadline,
            "timed out waiting for dynamic shell terminal event; requests={}, observed_events={observed_events:?}",
            provider.snapshot_requests().len()
        );
        match timeout(Duration::from_millis(250), events.recv()).await {
            Ok(Some(event)) => {
                let terminal = matches!(
                    event,
                    AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
                );
                observed_events.push(event);
                if terminal {
                    break;
                }
            }
            Ok(None) => {
                panic!("agent event channel should stay open")
            }
            Err(_) => continue,
        }
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while provider.snapshot_requests().len() < 2 {
        assert!(
            Instant::now() <= deadline,
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
    assert!(dynamic_result.content.contains("shell-ok"));

    let completed_dynamic_item = observed_events
        .iter()
        .find_map(|event| {
            if let AgentEvent::ItemCompleted(notification) = event
                && let TurnItem::DynamicToolCall {
                    tool_name,
                    storage,
                    output_policy,
                    ..
                } = &notification.item
                && tool_name == "skill.tests-my-skill.echo-shell"
            {
                return Some((storage, output_policy));
            }
            None
        })
        .expect("dynamic shell item should complete");
    assert!(matches!(
        completed_dynamic_item.1.storage,
        StorageOutputPolicy::Full { .. }
    ));
    assert!(
        matches!(completed_dynamic_item.0, ToolStoragePayload::Shell { stdout: Some(stdout), .. } if stdout.contains("shell-ok")),
        "dynamic shell storage should contain bounded shell stdout when policy allows it; observed storage={:?}, policy={:?}",
        completed_dynamic_item.0,
        completed_dynamic_item.1
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn tool_recovery_succeeds_at_tool_attempt_boundary() {
    let skill_root = unique_temp_dir("dynamic-shell-recovery");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Skill body",
        Some(
            r#"runtime:
  tools:
    - tool_slug: slow_shell
      description: Slow shell
      kind: shell
      parameters:
        type: object
      execution_class: session_scoped
      config:
        command: ["sh", "-lc", "sleep 3"]"#,
        ),
    );

    let tool_call_id = "call_dynamic_recovery_1";
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: tool_call_id.to_owned(),
            name: "skill.tests-my-skill.slow-shell".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.runtime.allow_shell_tools = true;
    tool_loop_config.skills.security.min_trust_for_shell_tools = SkillTrustLevel::Community;

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000122";
    let turn_id = "turn_000000000000000122";
    let recovery_job_id = "recovery_job_tool_boundary";
    let recovery_attempt_id = "recovery_attempt_tool_boundary";

    manager
        .ensure_thread(thread_id, "ws_000000000000000122")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "trigger slow tool".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut recovery_requested = false;
    let mut saw_recovery_succeeded = false;

    for _ in 0..200 {
        if !recovery_requested && !provider.snapshot_requests().is_empty() {
            if manager
                .start_recovery_attempt(
                    thread_id,
                    RecoveryAttemptRequest {
                        recovery_job_id: recovery_job_id.to_owned(),
                        recovery_attempt_id: recovery_attempt_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item_id: tool_call_id.to_owned(),
                        item_type: TurnItemType::DynamicToolCall,
                        force_non_stream: false,
                        refresh_provider_auth: false,
                        compact_history: false,
                        continue_generation: false,
                        model_override: None,
                        retained_llm_context: Vec::new(),
                    },
                )
                .await
                .is_ok()
            {
                recovery_requested = true;
            }
        }

        let event = match timeout(Duration::from_millis(100), events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                panic!("broadcast should remain open")
            }
            Err(_) => continue,
        };

        match event {
            AgentEvent::RecoveryAttemptSucceeded { recovery, .. } => {
                assert_eq!(recovery.job_id, recovery_job_id);
                assert_eq!(recovery.attempt_id, recovery_attempt_id);
                saw_recovery_succeeded = true;
                break;
            }
            AgentEvent::TurnFailed { error, .. } => {
                panic!("turn should not fail: {error}");
            }
            _ => {}
        }
    }

    assert!(recovery_requested, "test must request tool recovery");
    assert!(
        saw_recovery_succeeded,
        "tool recovery should succeed when the cancelled attempt finishes"
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn runtime_recheck_blocks_dynamic_tool_when_dependency_missing() {
    let skill_root = unique_temp_dir("dynamic-shell-recheck");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Skill body",
        Some(
            r#"dependencies:
  commands:
    - definitely-missing-phase3-bin
runtime:
  tools:
    - tool_slug: echo_shell
      description: Echo shell
      kind: shell
      parameters:
        type: object
      execution_class: session_scoped
      config:
        command: ["sh", "-lc", "printf shell-ok"]"#,
        ),
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_dynamic_2".to_owned(),
            name: "skill.tests-my-skill.echo-shell".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.runtime.allow_shell_tools = true;
    tool_loop_config.skills.security.min_trust_for_shell_tools = SkillTrustLevel::Community;
    tool_loop_config.skills.dependencies.preflight_on_resolve = false;
    tool_loop_config
        .skills
        .dependencies
        .runtime_recheck_on_tool_call = true;

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000121a";
    let turn_id = "turn_000000000000000121a";
    manager
        .ensure_thread(thread_id, "ws_000000000000000121a")
        .await
        .expect("thread should be created");

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "trigger tool".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let deadline = Instant::now() + Duration::from_secs(10);
    while provider.snapshot_requests().len() < 2 {
        assert!(
            Instant::now() <= deadline,
            "provider should receive second round with blocked tool result"
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
    assert!(
        dynamic_result
            .content
            .contains("runtime.dependency_missing")
    );
    assert!(!dynamic_result.content.contains("shell-ok"));

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn skill_resolution_emits_allowed_and_blocked_audit_events() {
    let skill_root = unique_temp_dir("skills-audit-events");
    write_skill(skill_root.as_path(), "good-skill", "Good body", None);
    write_skill(
        skill_root.as_path(),
        "bad-skill",
        "Bad body",
        Some(
            r#"dependencies:
  commands:
    - definitely-missing-phase3-bin"#,
        ),
    );

    let provider = Arc::new(CaptureStandardProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "capture-standard",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.dependencies.preflight_on_resolve = true;

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000121b";
    let turn_id = "turn_000000000000000121b";
    manager
        .ensure_thread(thread_id, "ws_000000000000000121b")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture-standard",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "good-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Skill {
                    name: "bad-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "resolve both skills".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut saw_allowed = false;
    let mut saw_blocked = false;
    for _ in 0..60 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event in time")
            .expect("broadcast should remain open");

        match event {
            AgentEvent::SkillAuditEvents { events, .. } => {
                for item in events {
                    if item.skill_slug == "tests/good-skill"
                        && item.action == SkillAuditAction::ResolveAllowed
                    {
                        saw_allowed = true;
                    }
                    if item.skill_slug == "tests/bad-skill"
                        && item.action == SkillAuditAction::ResolveBlocked
                        && item.reason_code.as_deref() == Some("resolve.dependency_missing")
                    {
                        saw_blocked = true;
                    }
                }
            }
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => {
                panic!("turn should not fail: {error}");
            }
            _ => {}
        }
    }

    assert!(saw_allowed, "expected resolve_allowed audit event");
    assert!(
        saw_blocked,
        "expected resolve_blocked dependency audit event"
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn read_skill_returns_active_skill_body() {
    let skill_root = unique_temp_dir("read-skill-ok");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Full body text for read_skill.",
        None,
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_read_1".to_owned(),
            name: "read_skill".to_owned(),
            arguments: r#"{"slug":"tests/my-skill"}"#.to_owned(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000122", "ws_000000000000000122")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000122").await;

    manager
        .start_turn(
            "thr_000000000000000122",
            "turn_000000000000000122",
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "read skill".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut read_skill_started_policy = None;
    let mut read_skill_completed_policy = None;
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event")
            .expect("broadcast should remain open");
        match &event {
            AgentEvent::ItemStarted(notification) => {
                if let TurnItem::DynamicToolCall {
                    tool_name,
                    recovery_policy,
                    ..
                } = &notification.item
                    && tool_name == "read_skill"
                {
                    read_skill_started_policy = recovery_policy.clone();
                }
            }
            AgentEvent::ItemCompleted(notification) => {
                if let TurnItem::DynamicToolCall {
                    tool_name,
                    recovery_policy,
                    ..
                } = &notification.item
                    && tool_name == "read_skill"
                {
                    read_skill_completed_policy = recovery_policy.clone();
                }
            }
            _ => {}
        }
        if matches!(event, AgentEvent::TurnCompleted { .. }) {
            break;
        }
    }

    let read_skill_started_policy =
        read_skill_started_policy.expect("dynamic tool start should include policy");
    let read_skill_completed_policy =
        read_skill_completed_policy.expect("dynamic tool completion should include policy");
    assert_eq!(read_skill_started_policy, read_skill_completed_policy);

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "provider should receive second round with tool result"
    );
    let second_round = &requests[1];
    let read_skill_result = second_round
        .messages
        .iter()
        .find(|message| message.role == pioneer_provider::Role::Tool)
        .expect("second round must include tool result message");
    assert_eq!(read_skill_result.name.as_deref(), Some("read_skill"));
    assert!(
        read_skill_result
            .content
            .contains("\"slug\":\"tests/my-skill\"")
    );
    assert!(
        read_skill_result
            .content
            .contains("Full body text for read_skill.")
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn read_skill_rejects_non_active_slug() {
    let skill_root = unique_temp_dir("read-skill-miss");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "Full body text for read_skill.",
        None,
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_read_miss".to_owned(),
            name: "read_skill".to_owned(),
            arguments: r#"{"slug":"unknown/skill"}"#.to_owned(),
        }],
        "done",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000123", "ws_000000000000000123")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000123").await;

    manager
        .start_turn(
            "thr_000000000000000123",
            "turn_000000000000000123",
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "read skill".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut saw_failed_dynamic_tool = false;
    for _ in 0..50 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event")
            .expect("broadcast should remain open");
        match event {
            AgentEvent::ItemCompleted(notification) => {
                if let TurnItem::DynamicToolCall {
                    tool_name,
                    recovery_policy,
                    success,
                    ..
                } = notification.item
                    && tool_name == "read_skill"
                {
                    assert_eq!(success, Some(false));
                    let recovery_policy = recovery_policy
                        .expect("runtime-blocked dynamic tool should have policy snapshot");
                    assert_eq!(
                        recovery_policy.retry_class,
                        ToolRecoveryRetryClass::Transient
                    );
                    assert_eq!(
                        recovery_policy.idempotency_mode,
                        ToolRecoveryIdempotencyMode::None
                    );
                    assert_eq!(recovery_policy.max_attempts, 1);
                    assert_eq!(
                        recovery_policy.resolved_action,
                        RecoveryAction::RetryAttempt
                    );
                    saw_failed_dynamic_tool = true;
                }
            }
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => {
                panic!("turn should not fail: {error}");
            }
            _ => {}
        }
    }

    assert!(
        saw_failed_dynamic_tool,
        "expected failed read_skill dynamic tool call"
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn invalid_skill_runtime_config_fails_open_to_builtin_tools() {
    let skill_root = unique_temp_dir("invalid-runtime-config");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "body",
        Some(
            r#"runtime:
  tools:
    - tool_slug: bad_proxy
      description: Missing target tool
      kind: function_proxy
      parameters:
        type: object
      execution_class: shared
      config: {}"#,
        ),
    );

    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000124", "ws_000000000000000124")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000124").await;

    manager
        .start_turn(
            "thr_000000000000000124",
            "turn_000000000000000124",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "run".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event")
            .expect("broadcast should remain open");
        match event {
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => panic!("turn should not fail: {error}"),
            _ => {}
        }
    }

    let requests = provider.snapshot_requests();
    assert!(!requests.is_empty());
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("request should still include builtin tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"read_file"));
    assert!(!tool_names.contains(&"skill.tests-my-skill.bad-proxy"));

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn invalid_skill_runtime_tool_is_excluded_per_tool() {
    let skill_root = unique_temp_dir("invalid-runtime-per-tool");
    write_skill(
        skill_root.as_path(),
        "my-skill",
        "body",
        Some(
            r#"runtime:
  tools:
    - tool_slug: bad_proxy
      description: Missing target tool
      kind: function_proxy
      parameters:
        type: object
      execution_class: shared
      config: {}
    - tool_slug: fetch_data
      description: Fetch data
      kind: http
      parameters:
        type: object
      execution_class: shared
      config:
        method: GET
        url: https://example.com"#,
        ),
    );

    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.workspace_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000125", "ws_000000000000000125")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000125").await;

    manager
        .start_turn(
            "thr_000000000000000125",
            "turn_000000000000000125",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![
                UserInput::Skill {
                    name: "my-skill".to_owned(),
                    path: String::new(),
                },
                UserInput::Text {
                    text: "run".to_owned(),
                    text_elements: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive event")
            .expect("broadcast should remain open");
        match event {
            AgentEvent::TurnCompleted { .. } => break,
            AgentEvent::TurnFailed { error, .. } => panic!("turn should not fail: {error}"),
            _ => {}
        }
    }

    let requests = provider.snapshot_requests();
    assert!(!requests.is_empty());
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("request should include tool definitions");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"skill.tests-my-skill.fetch-data"));
    assert!(!tool_names.contains(&"skill.tests-my-skill.bad-proxy"));

    let _ = fs::remove_dir_all(skill_root);
}
