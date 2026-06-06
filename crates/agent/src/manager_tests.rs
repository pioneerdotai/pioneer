use super::{
    AgentCommand, AgentEvent, AgentManager, AgentMcpAvailability, AgentMcpMaterialization,
    AgentMcpMaterializationRequest, AgentMcpToolProvider, AgentMemoryProvider,
    AgentMemoryTurnPolicyProvider, AgentPostTurnHookDispatchPolicy, AgentStartError,
    AgentTurnHookRuntimeContext, MemoryExtractionPolicy, MemoryRecallItem, MemoryRecallRequest,
    MemoryRecallSnapshot, MemoryToolMaterialization, MemoryTurnContext, MemoryTurnPolicy,
    MemoryTurnPolicyContext, MemoryTurnPolicyRequest, PendingAttachedTask, RecoveryAttemptRequest,
    ReviewRequiredTaskObservation, TaskToolMaterialization, TaskToolProvider, TaskTurnContext,
    ToolLoopConfig, TurnExecutionControl, TurnFinalizationContext, TurnFinalizationDecision,
    TurnFinalizationProvider,
};
use futures_util::StreamExt;
use pioneer_hooks::{
    AuditContribution, HookActorKind, HookAuditEventKind, HookAwaitPolicy, HookCapabilities,
    HookCapability, HookContextMode, HookContribution, HookDiagnosticCode, HookDiagnosticMessage,
    HookDiagnosticSeverity, HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy,
    HookHandler, HookHandlerRequest, HookHandlerResponse, HookId, HookInputKind, HookInputPayload,
    HookKind, HookPhase, HookPolicyKey, HookPolicySet, HookPromptContent, HookPromptContextSet,
    HookPromptSectionTitle, HookRegistry, HookRuntime, HookRuntimeBuilder, HookSectionId,
    HookSubscription, HookSubscriptionId, HookSubscriptionRegistry, HookValue, PolicyContribution,
    PromptContextContribution, PromptManifestDiagnosticContribution, PromptSectionContribution,
    TurnPostTurnStatus, TurnPostTurnToolStatus,
};
use pioneer_memory::hooks::memory_turn_policy_from_hook_policy_set;
use pioneer_memory::{MemoryModeRecallParams, MemoryRecallMode};
use pioneer_protocol::{
    AgentDurableEvent, ItemCompletedNotification, ItemStartedNotification, McpScopeKind,
    McpTurnBindingSummary, MemoryCategory, MemoryScope, MemoryScopeKind,
    PromptManifestDiagnosticCode, PromptManifestHookContributionKind, PromptManifestHookPhase,
    PromptManifestHookTruncation, ProviderFailureClass, RecoveryAction, RecoveryAttemptContext,
    StorageOutputPolicy, SystemEventLevel, ThreadMode, ToolCallStatus, ToolLoopBudgetAction,
    ToolLoopBudgetLimitKind, ToolRecoveryIdempotencyMode, ToolRecoveryRetryClass,
    ToolRetryResolution, ToolStoragePayload, TurnCapability, TurnCapabilityAcceptedReason,
    TurnCapabilityKind, TurnCapabilityRejectedReason, TurnItem, TurnItemType, UserInput,
};
use pioneer_provider::providers::EchoProvider;
use pioneer_provider::{
    AttachmentDataSource, ChatRequest, ChatResponse, InputTypeSupport, MessageContentPart,
    Provider, ProviderCapabilities, ProviderInputCapabilities, ProviderRegistry, ProviderToolCall,
    Role, StreamChunk,
};
use pioneer_skills::{SkillAuditAction, SkillAuditDecision, SkillTrustLevel};
use pioneer_tools::{
    ComputerUseToolsConfig, ConfiguredToolSpec, ExecutionClass, ExecutionWindowsConfig,
    FunctionToolOutput, PayloadKind, ToolError, ToolEventTrace, ToolExtensionBundle, ToolHandler,
    ToolIdempotencyMode, ToolInvocation, ToolLoopBudgetConfig, ToolPayload, ToolRecoveryMetadata,
    ToolRetryBudgetConfig, ToolRetryClass, ToolSpec, WebToolsConfig, dynamic_unknown_output_policy,
};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, advance, sleep, timeout};

fn test_tool_loop_config() -> ToolLoopConfig {
    ToolLoopConfig {
        provider: pioneer_provider::ProviderTimeoutPolicy::default(),
        preflight: super::PreflightLoopConfig::default(),
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
            system_roots: Vec::new(),
            user_roots: vec![
                "{homeDirectory}/skills/workspace/{workspaceId}/user".to_owned(),
            ],
            registry_roots: vec![
                "{homeDirectory}/skills/workspace/{workspaceId}/registry".to_owned(),
            ],
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
        memory: super::MemoryLoopConfig {
            active_recall: super::MemoryActiveRecallConfig {
                mode: super::MemoryActiveRecallMode::DeterministicOnly,
                ..super::MemoryActiveRecallConfig::default()
            },
            ..super::MemoryLoopConfig::default()
        },
        budget: ToolLoopBudgetConfig::default(),
        execution_windows: ExecutionWindowsConfig::default(),
        retry: ToolRetryBudgetConfig::default(),
    }
}

fn set_execution_window_budget(
    config: &mut ToolLoopConfig,
    max_agent_rounds_per_window: u32,
    max_tool_calls_per_window: u32,
) {
    config.budget.max_agent_rounds_per_turn = max_agent_rounds_per_window;
    config.budget.max_tool_calls_per_turn = max_tool_calls_per_window;
    config.execution_windows.window.max_agent_rounds_per_window = max_agent_rounds_per_window;
    config.execution_windows.window.max_tool_calls_per_window = max_tool_calls_per_window;
}

fn test_manager() -> AgentManager {
    let registry = Arc::new(ProviderRegistry::with_provider(
        "echo",
        Arc::new(EchoProvider::new()),
    ));
    AgentManager::new(registry, test_tool_loop_config())
}

fn skill_capability(slug: &str) -> TurnCapability {
    TurnCapability {
        id: format!("skill:user:{slug}"),
        kind: TurnCapabilityKind::Skill {
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
        },
        label: Some(slug.to_owned()),
    }
}

fn mcp_tool_capability(server_name: &str, raw_tool_name: &str) -> TurnCapability {
    TurnCapability {
        id: format!("mcp-tool:workspace:{server_name}:{raw_tool_name}"),
        kind: TurnCapabilityKind::McpTool {
            server_name: server_name.to_owned(),
            raw_tool_name: raw_tool_name.to_owned(),
            scope_kind: McpScopeKind::Workspace,
        },
        label: Some(format!("{server_name}/{raw_tool_name}")),
    }
}

fn assert_compact_skill_prompt(
    system_text: &str,
    skill_slug: &str,
    display_name: &str,
    body: &str,
) {
    assert!(system_text.contains("[Skills]"));
    assert!(system_text.contains(display_name));
    assert!(system_text.contains(&format!("Skill slug for read_skill: `{skill_slug}`")));
    assert!(!system_text.contains(&format!("[Skill Body: ${skill_slug}]")));
    assert!(!system_text.contains(body));
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

struct CaptureAgentProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    response_text: String,
    preflight_response_text: String,
}

struct EmptyNoToolRoundProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    first_tool_call: Option<ProviderToolCall>,
    empty_rounds_before_final: usize,
    next_index: AtomicUsize,
}

struct SequencedTextProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    responses: Vec<String>,
    next_index: AtomicUsize,
}

const TOOL_SCHEMA_DUMP_RESPONSE: &str = r#"{"name":"write_file","description":"Write a file","parameters":{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"],"additionalProperties":false}}"#;
const RAW_TOOL_CALL_MARKUP_RESPONSE: &str = concat!(
    "][transport][<tool_call>\n",
    "][transport][<invoke name=\"exec_command\">][transport][<command>]",
    "<item>bash && -lc && grep -nP '[\\xe2\\x80\\x94]' /tmp/article.md</item>",
    "</command></invoke>\n",
    "</tool_call>"
);

#[derive(Default)]
struct RetryOnceFinalizationProvider {
    contexts: std::sync::Mutex<Vec<TurnFinalizationContext>>,
    next_index: AtomicUsize,
}

impl Default for CaptureAgentProvider {
    fn default() -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            response_text: "done".to_owned(),
            preflight_response_text: TEST_PREFLIGHT_RESPONSE.to_owned(),
        }
    }
}

impl CaptureAgentProvider {
    fn with_response_text(response_text: impl Into<String>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            response_text: response_text.into(),
            preflight_response_text: TEST_PREFLIGHT_RESPONSE.to_owned(),
        }
    }

    fn with_preflight_response(preflight_response_text: impl Into<String>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            response_text: "done".to_owned(),
            preflight_response_text: preflight_response_text.into(),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("capture provider lock poisoned")
                .clone(),
        )
    }

    fn snapshot_all_requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .clone()
    }
}

impl EmptyNoToolRoundProvider {
    fn new(empty_rounds_before_final: usize) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            first_tool_call: None,
            empty_rounds_before_final,
            next_index: AtomicUsize::new(0),
        }
    }

    fn with_first_tool_call(
        first_tool_call: ProviderToolCall,
        empty_rounds_before_final: usize,
    ) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            first_tool_call: Some(first_tool_call),
            empty_rounds_before_final,
            next_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("empty no-tool provider lock poisoned")
                .clone(),
        )
    }
}

impl SequencedTextProvider {
    fn new(responses: Vec<&str>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            responses: responses.into_iter().map(str::to_owned).collect(),
            next_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("sequenced text provider lock poisoned")
                .clone(),
        )
    }
}

impl RetryOnceFinalizationProvider {
    fn snapshot_contexts(&self) -> Vec<TurnFinalizationContext> {
        self.contexts
            .lock()
            .expect("retry finalization provider lock poisoned")
            .clone()
    }
}

struct TextOnlyAgentProvider {
    inner: CaptureAgentProvider,
}

impl TextOnlyAgentProvider {
    fn with_preflight_response(preflight_response_text: impl Into<String>) -> Self {
        Self {
            inner: CaptureAgentProvider::with_preflight_response(preflight_response_text),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        self.inner.snapshot_requests()
    }
}

struct TestMcpToolProvider {
    materialization: AgentMcpMaterialization,
    requests: std::sync::Mutex<Vec<AgentMcpMaterializationRequest>>,
}

impl TestMcpToolProvider {
    fn new(materialization: AgentMcpMaterialization) -> Self {
        Self {
            materialization,
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn snapshot_requests(&self) -> Vec<AgentMcpMaterializationRequest> {
        self.requests
            .lock()
            .expect("MCP provider requests lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentMcpToolProvider for TestMcpToolProvider {
    async fn mcp_availability(&self, _workspace_id: &str) -> Result<AgentMcpAvailability, String> {
        Ok(AgentMcpAvailability {
            available_mcp: vec!["resend".to_owned(), "resend/send".to_owned()],
            blocked_mcp: Vec::new(),
        })
    }

    async fn materialize_mcp_tools(
        &self,
        request: AgentMcpMaterializationRequest,
    ) -> Result<AgentMcpMaterialization, String> {
        self.requests
            .lock()
            .expect("MCP provider requests lock poisoned")
            .push(request);
        Ok(self.materialization.clone())
    }
}

struct NoopMcpToolExecutor;

#[async_trait::async_trait]
impl pioneer_tools::McpToolExecutor for NoopMcpToolExecutor {
    async fn call_mcp_tool(
        &self,
        _request: pioneer_tools::McpToolCallRequest,
        _trace: ToolEventTrace,
    ) -> Result<pioneer_tools::McpToolCallOutput, ToolError> {
        Ok(pioneer_tools::McpToolCallOutput {
            content: json!([{"type":"text","text":"ok"}]),
            structured_content: None,
            is_error: false,
            duration_ms: 1,
            meta: None,
        })
    }
}

fn explicit_mcp_tool_materialization(
    capability_id: &str,
    workspace_id: &str,
) -> AgentMcpMaterialization {
    let descriptor = pioneer_tools::McpDynamicToolDescriptor {
        callable_name: "mcp_resend_send".to_owned(),
        workspace_id: workspace_id.to_owned(),
        server_id: "mcp_server_resend_001".to_owned(),
        server_name: "resend".to_owned(),
        raw_tool_name: "send".to_owned(),
        catalog_version: "catalog-v1".to_owned(),
        fingerprint: "fingerprint-resend".to_owned(),
        snapshot_version: 1,
        description: "Send an email through Resend.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "to": {"type": "string"}
            },
            "required": ["to"],
            "additionalProperties": false
        }),
        annotations: pioneer_tools::McpDynamicToolAnnotations::default(),
        timeout_ms: Some(5_000),
        selection_reason: "explicit_composer_capability".to_owned(),
        capability_id: Some(capability_id.to_owned()),
    };
    let materialized =
        pioneer_tools::materialize_mcp_runtime_tools(&[descriptor], Arc::new(NoopMcpToolExecutor));

    AgentMcpMaterialization {
        bundles: materialized.bundles,
        available_mcp: vec!["resend".to_owned(), "resend/send".to_owned()],
        blocked_mcp: Vec::new(),
        diagnostics: Vec::new(),
        accepted_capabilities: vec![pioneer_protocol::TurnAcceptedCapability {
            id: capability_id.to_owned(),
            label: Some("resend/send".to_owned()),
            kind: TurnCapabilityKind::McpTool {
                server_name: "resend".to_owned(),
                raw_tool_name: "send".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
            reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
        }],
        rejected_capabilities: Vec::new(),
        mcp_bindings: materialized
            .bindings
            .into_iter()
            .map(|binding| McpTurnBindingSummary {
                server_installation_id: binding.server_installation_id,
                server_name: binding.server_name,
                raw_tool_name: binding.raw_tool_name,
                callable_name: binding.callable_name,
                catalog_version: binding.catalog_version,
                fingerprint: binding.fingerprint,
                selection_reason: binding.selection_reason,
                capability_id: binding.capability_id,
            })
            .collect(),
    }
}

const PHASE_07_HOOK_PHASES: [HookPhase; 7] = [
    HookPhase::TurnPrePolicy,
    HookPhase::TurnPrePromptContext,
    HookPhase::TurnPreToolMaterialization,
    HookPhase::TurnPostPreflightPromptContext,
    HookPhase::TurnPrePromptCompile,
    HookPhase::TurnPostPromptCompile,
    HookPhase::TurnPostTurn,
];

#[derive(Debug, Clone, PartialEq)]
struct RecordedHookCall {
    phase: HookPhase,
    input_kind: HookInputKind,
    payload: HookInputPayload,
    workspace_id: Option<String>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    task_id: Option<String>,
    mode: Option<HookContextMode>,
    actor_kind: Option<HookActorKind>,
    policy_set: HookPolicySet,
    prompt_context_set: HookPromptContextSet,
}

struct RecordingHookHandler {
    hook_id: HookId,
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    fail_phases: Vec<HookPhase>,
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

    fn capabilities(&self) -> HookCapabilities {
        test_hook_capabilities()
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
                task_id: request
                    .context
                    .task_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned()),
                mode: request.context.mode.clone(),
                actor_kind: request
                    .context
                    .actor
                    .as_ref()
                    .map(|actor| actor.kind.clone()),
                policy_set: request.policy_set.clone(),
                prompt_context_set: request.prompt_context_set.clone(),
            });

        if self.fail_phases.contains(&request.phase) {
            return Err(HookError::new(
                HookDiagnosticCode::new("test.phase07_failed").expect("valid diagnostic code"),
                HookDiagnosticMessage::new("phase 07 hook failed").expect("valid diagnostic"),
            ));
        }

        Ok(HookHandlerResponse {
            contributions: recording_contributions_for_phase(request.phase, &self.contributions),
            ..HookHandlerResponse::default()
        })
    }
}

fn recording_contributions_for_phase(
    phase: HookPhase,
    contributions: &[HookContribution],
) -> Vec<HookContribution> {
    contributions
        .iter()
        .filter(|contribution| match contribution {
            HookContribution::Policy(_) => phase == HookPhase::TurnPrePolicy,
            HookContribution::PromptContext(_) => phase == HookPhase::TurnPrePromptContext,
            HookContribution::ToolBundle(_) => phase == HookPhase::TurnPreToolMaterialization,
            HookContribution::PromptSection(_) | HookContribution::PromptManifestDiagnostic(_) => {
                phase == HookPhase::TurnPrePromptCompile
            }
            HookContribution::Noop => true,
            HookContribution::Audit(_) | HookContribution::BackgroundJob(_) => false,
        })
        .cloned()
        .collect()
}

fn test_hook_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("contribute_policy").expect("valid capability"),
        HookCapability::new("contribute_prompt_context").expect("valid capability"),
        HookCapability::new("contribute_prompt_section").expect("valid capability"),
        HookCapability::new("contribute_tool_bundle").expect("valid capability"),
        HookCapability::new("contribute_prompt_manifest_diagnostic").expect("valid capability"),
        HookCapability::new("emit_audit").expect("valid capability"),
        HookCapability::new("schedule_background_job").expect("valid capability"),
    ])
}

fn empty_hook_runtime() -> Arc<HookRuntime> {
    Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ))
}

async fn install_configured_memory_hooks_for_test(manager: &AgentManager) {
    let memory_provider = manager
        .memory_provider
        .read()
        .await
        .clone()
        .expect("memory provider must be configured before installing test memory hooks");
    let memory_write_provider = manager.memory_write_provider.read().await.clone();
    let post_turn_extractor_provider = manager
        .memory_post_turn_extractor_provider
        .read()
        .await
        .clone();
    let policy_provider = manager.memory_turn_policy_provider.read().await.clone();
    let episodic_recall_provider = manager.memory_episodic_recall_provider.read().await.clone();
    let current_runtime = manager.hook_runtime.read().await.clone();
    let builder = current_runtime
        .as_ref()
        .map(|runtime| HookRuntimeBuilder::from_runtime(runtime.as_ref()))
        .unwrap_or_else(HookRuntimeBuilder::new);
    let runtime = builder
        .install(pioneer_memory::hooks::package(
            memory_provider,
            memory_write_provider,
            post_turn_extractor_provider,
            policy_provider,
            episodic_recall_provider,
            manager.memory_tool_bundle_artifact_store(),
            manager.tool_loop_config.memory.clone(),
        ))
        .expect("test memory hook package installs")
        .build();
    manager.set_hook_runtime(Some(runtime)).await;
}

fn recording_hook_runtime(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail: bool,
) -> Arc<HookRuntime> {
    recording_hook_runtime_with_fallback(calls, contributions, failure_policy, fail, Vec::new())
}

fn recording_hook_runtime_with_phase_failures(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail_phases: Vec<HookPhase>,
) -> Arc<HookRuntime> {
    recording_hook_runtime_with_phase_failures_and_fallback(
        calls,
        contributions,
        failure_policy,
        fail_phases,
        Vec::new(),
    )
}

fn recording_hook_runtime_with_fallback(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail: bool,
    fallback_contributions: Vec<HookContribution>,
) -> Arc<HookRuntime> {
    let fail_phases = if fail {
        PHASE_07_HOOK_PHASES.to_vec()
    } else {
        Vec::new()
    };
    recording_hook_runtime_with_phase_failures_and_fallback(
        calls,
        contributions,
        failure_policy,
        fail_phases,
        fallback_contributions,
    )
}

fn recording_hook_runtime_with_phase_failures_and_fallback(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    contributions: Vec<HookContribution>,
    failure_policy: HookFailurePolicy,
    fail_phases: Vec<HookPhase>,
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
            fail_phases,
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

fn recording_hook_runtime_for_phase(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    phase: HookPhase,
    await_policy: HookAwaitPolicy,
    failure_policy: HookFailurePolicy,
) -> Arc<HookRuntime> {
    recording_hook_runtime_for_phase_with_failures(
        calls,
        phase,
        await_policy,
        failure_policy,
        Vec::new(),
    )
}

fn recording_hook_runtime_for_phase_with_failures(
    calls: Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    phase: HookPhase,
    await_policy: HookAwaitPolicy,
    failure_policy: HookFailurePolicy,
    fail_phases: Vec<HookPhase>,
) -> Arc<HookRuntime> {
    let handlers = Arc::new(HookRegistry::new());
    let subscriptions = Arc::new(HookSubscriptionRegistry::new());
    let hook_id = HookId::new("test.phase12_recorder").expect("valid hook id");
    handlers
        .register_handler(Arc::new(RecordingHookHandler {
            hook_id: hook_id.clone(),
            calls,
            contributions: Vec::new(),
            fail_phases,
        }))
        .expect("recording hook registers");

    subscriptions
        .register_subscription(
            handlers.as_ref(),
            HookSubscription::new(phase_07_subscription_id(phase), hook_id, phase)
                .with_execution_policy(HookExecutionPolicy {
                    await_policy,
                    timeout_ms: None,
                    max_parallelism: None,
                })
                .with_failure_policy(failure_policy),
        )
        .expect("recording hook subscription registers");

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

fn prompt_context_contribution(
    contribution_id: &str,
    domain: &str,
    priority: i32,
    content: &str,
) -> HookContribution {
    HookContribution::PromptContext(PromptContextContribution {
        contribution_id: pioneer_hooks::HookContributionId::new(contribution_id)
            .expect("valid contribution id"),
        domain: HookDomain::new(domain).expect("valid domain"),
        priority,
        content: HookPromptContent::new(content).expect("valid prompt content"),
        max_chars: None,
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    })
}

fn prompt_context_contribution_with_max_chars(
    contribution_id: &str,
    domain: &str,
    priority: i32,
    content: &str,
    max_chars: usize,
) -> HookContribution {
    let HookContribution::PromptContext(mut contribution) =
        prompt_context_contribution(contribution_id, domain, priority, content)
    else {
        unreachable!("helper returns prompt context contribution");
    };
    contribution.max_chars = Some(max_chars);
    HookContribution::PromptContext(contribution)
}

fn prompt_section_contribution(
    section_id: &str,
    title: &str,
    domain: &str,
    priority: i32,
    content: &str,
) -> HookContribution {
    HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: pioneer_hooks::HookContributionId::new(section_id)
            .expect("valid contribution id"),
        section_id: HookSectionId::new(section_id).expect("valid section id"),
        title: Some(HookPromptSectionTitle::new(title).expect("valid section title")),
        domain: HookDomain::new(domain).expect("valid domain"),
        priority,
        content: HookPromptContent::new(content).expect("valid prompt content"),
        max_chars: None,
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    })
}

fn prompt_section_contribution_with_max_chars(
    section_id: &str,
    title: &str,
    domain: &str,
    priority: i32,
    content: &str,
    max_chars: usize,
) -> HookContribution {
    let HookContribution::PromptSection(mut contribution) =
        prompt_section_contribution(section_id, title, domain, priority, content)
    else {
        unreachable!("helper returns prompt section contribution");
    };
    contribution.max_chars = Some(max_chars);
    HookContribution::PromptSection(contribution)
}

fn prompt_manifest_diagnostic_contribution(
    code: &str,
    message: &str,
    safe_for_user: bool,
) -> HookContribution {
    HookContribution::PromptManifestDiagnostic(PromptManifestDiagnosticContribution {
        code: HookDiagnosticCode::new(code).expect("valid diagnostic code"),
        message: HookDiagnosticMessage::new(message).expect("valid diagnostic message"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user,
        hook_id: None,
        subscription_id: None,
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
        HookPhase::TurnPostPreflightPromptContext => "test.phase07.post_preflight_prompt_context",
        HookPhase::TurnPreToolMaterialization => "test.phase07.pre_tool_materialization",
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

async fn wait_for_hook_calls(
    calls: &Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    expected_len: usize,
) -> Vec<RecordedHookCall> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let snapshot = snapshot_hook_calls(calls);
        if snapshot.len() >= expected_len {
            return snapshot;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {expected_len} hook calls; observed {snapshot:?}");
        }
        yield_now().await;
    }
}

async fn wait_for_hook_phase(
    calls: &Arc<std::sync::Mutex<Vec<RecordedHookCall>>>,
    phase: HookPhase,
) -> RecordedHookCall {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(call) = snapshot_hook_calls(calls)
            .into_iter()
            .find(|call| call.phase == phase)
        {
            return call;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for hook phase {phase}");
        }
        yield_now().await;
    }
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

const TEST_PREFLIGHT_RESPONSE: &str = r#"{"tools":{"visibleTools":[]}}"#;

fn test_native_image_input_capabilities() -> ProviderInputCapabilities {
    ProviderInputCapabilities {
        text: true,
        file: InputTypeSupport::fallback_only(),
        image: InputTypeSupport::data_url_inline_only(),
        audio: InputTypeSupport::fallback_only(),
        video: InputTypeSupport::fallback_only(),
    }
}

fn preflight_response_with_visible_tools(tool_names: &[&str]) -> String {
    json!({
        "tools": {
            "visibleTools": tool_names,
        }
    })
    .to_string()
}

fn memory_read_preflight_response() -> String {
    preflight_response_with_visible_tools(&["memory_search", "memory_get"])
}

fn memory_all_preflight_response() -> String {
    preflight_response_with_visible_tools(&[
        "memory_search",
        "memory_list",
        "memory_get",
        "memory_remember",
        "memory_forget",
    ])
}

fn memory_remember_preflight_response() -> String {
    preflight_response_with_visible_tools(&["memory_remember"])
}

fn memory_forget_preflight_response() -> String {
    preflight_response_with_visible_tools(&["memory_search", "memory_get", "memory_forget"])
}

fn optional_domain_preflight_response() -> String {
    preflight_response_with_visible_tools(&[
        "memory_search",
        "task_create",
        "artifact_prepare",
        "artifact_register",
        "computer_use",
    ])
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

fn test_preflight_response() -> ChatResponse {
    ChatResponse {
        text: TEST_PREFLIGHT_RESPONSE.to_owned(),
        usage: None,
        reasoning_content: None,
        tool_calls: Vec::new(),
    }
}

fn visible_test_requests(requests: Vec<ChatRequest>) -> Vec<ChatRequest> {
    requests
        .into_iter()
        .filter(|request| !is_turn_preflight_request(request))
        .collect()
}

#[derive(Default)]
struct CaptureStandardProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
}

impl CaptureStandardProvider {
    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("capture provider lock poisoned")
                .clone(),
        )
    }
}

struct SequencedToolProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    first_tool_calls: Vec<pioneer_provider::ProviderToolCall>,
    second_text: String,
    preflight_response_text: String,
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
            preflight_response_text: TEST_PREFLIGHT_RESPONSE.to_owned(),
            next_index: AtomicUsize::new(0),
        }
    }

    fn with_preflight_response(
        first_tool_calls: Vec<pioneer_provider::ProviderToolCall>,
        second_text: impl Into<String>,
        preflight_response_text: impl Into<String>,
    ) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            first_tool_calls,
            second_text: second_text.into(),
            preflight_response_text: preflight_response_text.into(),
            next_index: AtomicUsize::new(0),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("capture provider lock poisoned")
                .clone(),
        )
    }
}

#[derive(Default)]
struct AlwaysTaskCreateProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    next_index: AtomicUsize,
}

impl AlwaysTaskCreateProvider {
    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("always task_create provider lock poisoned")
                .clone(),
        )
    }
}

#[derive(Default)]
struct FailingTaskMutationToolProvider {
    handler: Arc<FailingTaskCreateHandler>,
}

#[derive(Clone)]
struct StaticTaskToolProvider {
    bundle: ToolExtensionBundle,
}

struct ReviewGuardProvider {
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    steps: std::sync::Mutex<VecDeque<ReviewGuardProviderStep>>,
}

enum ReviewGuardProviderStep {
    Text(String),
    Tool {
        name: String,
        arguments: serde_json::Value,
    },
}

#[derive(Clone)]
struct ReviewGuardTaskToolProvider {
    state: Arc<std::sync::Mutex<ReviewGuardTaskState>>,
    bundle: ToolExtensionBundle,
}

struct ReviewGuardTaskState {
    review_query_skip_remaining: usize,
    observations: Vec<ReviewRequiredTaskObservation>,
    pending: Vec<PendingAttachedTask>,
    revision_observation: ReviewRequiredTaskObservation,
    accept_calls: usize,
    revise_calls: usize,
    wait_calls: usize,
}

struct ReviewGuardTaskHandler {
    state: Arc<std::sync::Mutex<ReviewGuardTaskState>>,
}

#[derive(Default)]
struct FailingTaskCreateHandler {
    calls: AtomicUsize,
}

impl FailingTaskCreateHandler {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ReviewGuardProvider {
    fn new(steps: Vec<ReviewGuardProviderStep>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            steps: std::sync::Mutex::new(VecDeque::from(steps)),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("review guard provider lock poisoned")
                .clone(),
        )
    }
}

impl ReviewGuardProviderStep {
    fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    fn tool(name: impl Into<String>, arguments: serde_json::Value) -> Self {
        Self::Tool {
            name: name.into(),
            arguments,
        }
    }
}

impl ReviewGuardTaskToolProvider {
    fn new(initial_observation: ReviewRequiredTaskObservation) -> Self {
        let state = Arc::new(std::sync::Mutex::new(ReviewGuardTaskState {
            review_query_skip_remaining: 0,
            observations: vec![initial_observation],
            pending: Vec::new(),
            revision_observation: review_guard_observation(
                "candidate_review_guard_revision",
                1,
                1,
                &["task_accept", "task_cancel"],
                Some("fixed after revision"),
            ),
            accept_calls: 0,
            revise_calls: 0,
            wait_calls: 0,
        }));
        let handler: Arc<dyn ToolHandler> = Arc::new(ReviewGuardTaskHandler {
            state: state.clone(),
        });
        Self {
            state,
            bundle: fake_task_tool_bundle_for_names(
                &[
                    "task_wait",
                    "task_get",
                    "task_accept",
                    "task_revise",
                    "task_cancel",
                ],
                handler,
            ),
        }
    }

    fn with_review_query_skip(self, skip: usize) -> Self {
        self.state
            .lock()
            .expect("review guard task state lock poisoned")
            .review_query_skip_remaining = skip;
        self
    }

    fn call_counts(&self) -> (usize, usize, usize) {
        let state = self
            .state
            .lock()
            .expect("review guard task state lock poisoned");
        (state.accept_calls, state.revise_calls, state.wait_calls)
    }
}

#[async_trait::async_trait]
impl ToolHandler for FailingTaskCreateHandler {
    async fn handle(
        &self,
        _invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ToolError::invalid_arguments(
            "`trigger` must be a JSON object with shape {\"kind\":\"cron\",\"cronExpr\":\"0 7 * * *\",\"timezone\":\"Europe/Moscow\"}",
        ))
    }
}

#[async_trait::async_trait]
impl ToolHandler for ReviewGuardTaskHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, ToolError> {
        let mut state = self
            .state
            .lock()
            .expect("review guard task state lock poisoned");
        let output = match invocation.tool_name.as_str() {
            "task_accept" => {
                state.accept_calls += 1;
                state.observations.clear();
                state.pending.clear();
                json!({
                    "taskId": "task_review_guard",
                    "runId": "run_review_guard",
                    "candidateId": "candidate_review_guard",
                    "accepted": true,
                    "taskTerminal": true
                })
            }
            "task_revise" => {
                state.revise_calls += 1;
                state.observations.clear();
                state.pending = vec![PendingAttachedTask {
                    task_id: "task_review_guard".to_owned(),
                    run_id: Some("run_review_guard".to_owned()),
                    title: "Review guard task".to_owned(),
                    status: "running".to_owned(),
                }];
                json!({
                    "taskId": "task_review_guard",
                    "runId": "run_review_guard",
                    "candidateId": "candidate_review_guard",
                    "requested": true,
                    "nextAction": "task_wait"
                })
            }
            "task_wait" => {
                state.wait_calls += 1;
                state.pending.clear();
                let revision_observation = state.revision_observation.clone();
                state.observations = vec![revision_observation.clone()];
                json!({
                    "reviewRequired": [{
                        "taskId": "task_review_guard",
                        "runId": "run_review_guard",
                        "candidateId": revision_observation.candidate_id,
                        "allowedActions": revision_observation.allowed_actions
                    }]
                })
            }
            "task_cancel" => {
                state.observations.clear();
                state.pending.clear();
                json!({ "cancelled": true })
            }
            "task_get" => json!({
                "task": {
                    "id": "task_review_guard",
                    "status": if state.observations.is_empty() { "completed" } else { "waiting_review" }
                }
            }),
            _ => json!({ "ok": true }),
        };
        Ok(Box::new(FunctionToolOutput::new(output.to_string(), true)))
    }
}

#[async_trait::async_trait]
impl TaskToolProvider for FailingTaskMutationToolProvider {
    async fn materialize_task_tools(
        &self,
        _context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String> {
        let spec = ToolSpec::new(
            "task_create",
            "Create a durable task.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "goal": { "type": "string" },
                    "trigger": {
                        "type": "object",
                        "properties": {
                            "kind": { "type": "string" }
                        }
                    }
                }
            }),
            PayloadKind::Function,
        )
        .with_recovery(ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Arguments,
            idempotency_mode: ToolIdempotencyMode::RequiresKey,
            max_attempts: 1,
            can_resume: false,
            max_wall_clock_secs: None,
        });
        let configured = ConfiguredToolSpec::new(
            spec,
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );
        Ok(TaskToolMaterialization {
            bundles: vec![ToolExtensionBundle {
                specs: vec![configured],
                handlers: vec![("task_create".to_owned(), self.handler.clone())],
            }],
            diagnostics: Vec::new(),
        })
    }

    async fn pending_attached_tasks(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<super::PendingAttachedTask>, String> {
        Ok(Vec::new())
    }

    async fn review_required_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String> {
        Ok(Vec::new())
    }

    async fn terminal_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<super::TerminalTaskObservation>, String> {
        Ok(Vec::new())
    }

    async fn cleanup_attached_tasks(
        &self,
        _context: TaskTurnContext,
        _reason: String,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl TaskToolProvider for ReviewGuardTaskToolProvider {
    async fn materialize_task_tools(
        &self,
        _context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String> {
        Ok(TaskToolMaterialization {
            bundles: vec![self.bundle.clone()],
            diagnostics: Vec::new(),
        })
    }

    async fn pending_attached_tasks(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<PendingAttachedTask>, String> {
        Ok(self
            .state
            .lock()
            .expect("review guard task state lock poisoned")
            .pending
            .clone())
    }

    async fn review_required_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String> {
        let mut state = self
            .state
            .lock()
            .expect("review guard task state lock poisoned");
        if state.review_query_skip_remaining > 0 {
            state.review_query_skip_remaining -= 1;
            return Ok(Vec::new());
        }
        Ok(state.observations.clone())
    }

    async fn terminal_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<super::TerminalTaskObservation>, String> {
        Ok(Vec::new())
    }

    async fn cleanup_attached_tasks(
        &self,
        _context: TaskTurnContext,
        _reason: String,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .expect("review guard task state lock poisoned");
        state.observations.clear();
        state.pending.clear();
        Ok(())
    }
}

#[async_trait::async_trait]
impl TaskToolProvider for StaticTaskToolProvider {
    async fn materialize_task_tools(
        &self,
        _context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String> {
        Ok(TaskToolMaterialization {
            bundles: vec![self.bundle.clone()],
            diagnostics: Vec::new(),
        })
    }

    async fn pending_attached_tasks(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<super::PendingAttachedTask>, String> {
        Ok(Vec::new())
    }

    async fn review_required_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String> {
        Ok(Vec::new())
    }

    async fn terminal_attached_task_observations(
        &self,
        _context: TaskTurnContext,
    ) -> Result<Vec<super::TerminalTaskObservation>, String> {
        Ok(Vec::new())
    }

    async fn cleanup_attached_tasks(
        &self,
        _context: TaskTurnContext,
        _reason: String,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum LoopBudgetProviderMode {
    ToolWhileAvailableThenFinal,
    TwoToolRoundsThenFinal,
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
    preflight_response_text: String,
}

impl LoopBudgetProvider {
    fn new(mode: LoopBudgetProviderMode, tool_calls_per_round: usize) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
            mode,
            tool_calls_per_round,
            preflight_response_text: TEST_PREFLIGHT_RESPONSE.to_owned(),
        }
    }

    fn with_preflight_response(
        mode: LoopBudgetProviderMode,
        tool_calls_per_round: usize,
        preflight_response_text: impl Into<String>,
    ) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
            mode,
            tool_calls_per_round,
            preflight_response_text: preflight_response_text.into(),
        }
    }

    fn snapshot_requests(&self) -> Vec<ChatRequest> {
        visible_test_requests(
            self.requests
                .lock()
                .expect("loop budget provider lock poisoned")
                .clone(),
        )
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

    fn successful_tool_call(id: usize) -> ProviderToolCall {
        ProviderToolCall {
            id: format!("call_loop_budget_success_{id}"),
            name: "list_dir".to_owned(),
            arguments: r#"{"path":".","depth":0,"limit":1}"#.to_owned(),
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
        visible_test_requests(
            self.requests
                .lock()
                .expect("provider boundary lock poisoned")
                .clone(),
        )
    }
}

struct RecordingMemoryProvider {
    recall_contexts: std::sync::Mutex<Vec<MemoryTurnContext>>,
    recall_requests: std::sync::Mutex<Vec<MemoryRecallRequest>>,
    mode_recall_requests: std::sync::Mutex<Vec<MemoryModeRecallParams>>,
    tool_contexts: std::sync::Mutex<Vec<MemoryTurnContext>>,
    recall_results: std::sync::Mutex<VecDeque<Result<MemoryRecallSnapshot, String>>>,
    tool_result: Result<MemoryToolMaterialization, String>,
}

impl RecordingMemoryProvider {
    fn new(
        recall_result: Result<MemoryRecallSnapshot, String>,
        tool_result: Result<MemoryToolMaterialization, String>,
    ) -> Self {
        Self::with_recall_sequence(vec![recall_result], tool_result)
    }

    fn with_recall_sequence(
        recall_results: Vec<Result<MemoryRecallSnapshot, String>>,
        tool_result: Result<MemoryToolMaterialization, String>,
    ) -> Self {
        Self {
            recall_contexts: std::sync::Mutex::new(Vec::new()),
            recall_requests: std::sync::Mutex::new(Vec::new()),
            mode_recall_requests: std::sync::Mutex::new(Vec::new()),
            tool_contexts: std::sync::Mutex::new(Vec::new()),
            recall_results: std::sync::Mutex::new(VecDeque::from(recall_results)),
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

    fn mode_recall_requests(&self) -> Vec<MemoryModeRecallParams> {
        self.mode_recall_requests
            .lock()
            .expect("memory mode recall requests lock poisoned")
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
        let mut results = self
            .recall_results
            .lock()
            .expect("memory recall results lock poisoned");
        if results.len() > 1 {
            results.pop_front().expect("result exists")
        } else {
            results
                .front()
                .cloned()
                .unwrap_or_else(|| Ok(MemoryRecallSnapshot::empty()))
        }
    }

    async fn recall_memory_mode(
        &self,
        context: MemoryTurnContext,
        request: MemoryModeRecallParams,
    ) -> Result<MemoryRecallSnapshot, String> {
        self.recall_contexts
            .lock()
            .expect("memory recall contexts lock poisoned")
            .push(context);
        self.mode_recall_requests
            .lock()
            .expect("memory mode recall requests lock poisoned")
            .push(request);
        let mut results = self
            .recall_results
            .lock()
            .expect("memory recall results lock poisoned");
        if results.len() > 1 {
            results.pop_front().expect("result exists")
        } else {
            results
                .front()
                .cloned()
                .unwrap_or_else(|| Ok(MemoryRecallSnapshot::empty()))
        }
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

fn fake_task_tool_spec(name: &str) -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            name,
            "test-only task tool",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true
            }),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
    )
}

fn fake_task_tool_bundle_for_names(
    names: &[&str],
    handler: Arc<dyn ToolHandler>,
) -> ToolExtensionBundle {
    ToolExtensionBundle {
        specs: names.iter().map(|name| fake_task_tool_spec(name)).collect(),
        handlers: names
            .iter()
            .map(|name| ((*name).to_owned(), handler.clone()))
            .collect(),
    }
}

fn review_guard_observation(
    candidate_id: &str,
    round: u32,
    remaining_revision_rounds: u32,
    allowed_actions: &[&str],
    summary: Option<&str>,
) -> ReviewRequiredTaskObservation {
    ReviewRequiredTaskObservation {
        task_id: "task_review_guard".to_owned(),
        run_id: "run_review_guard".to_owned(),
        candidate_id: candidate_id.to_owned(),
        title: "Review guard task".to_owned(),
        status: "waiting_review".to_owned(),
        candidate_status: "pending_review".to_owned(),
        round,
        summary: summary.map(str::to_owned),
        result_preview: Some("candidate output preview".to_owned()),
        extraction_error_preview: None,
        diagnostics: vec!["candidate diagnostic".to_owned()],
        child_thread_id: Some("child_thread_review_guard".to_owned()),
        child_turn_id: Some(format!("child_turn_review_guard_{round}")),
        max_revision_rounds: 2,
        remaining_revision_rounds,
        allowed_actions: allowed_actions
            .iter()
            .map(|action| (*action).to_owned())
            .collect(),
        revision_blocked_reason: if allowed_actions.contains(&"task_revise") {
            None
        } else {
            Some("max_revision_rounds_reached".to_owned())
        },
    }
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
            "memory_list",
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
                "memory_list",
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

    async fn recall_memory_mode(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryModeRecallParams,
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
        let preflight = is_turn_preflight_request(&request);
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }
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
            vision: true,
            tool_calling: true,
            input_types: test_native_image_input_capabilities(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        let preflight = is_turn_preflight_request(&request);
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(ChatResponse {
                text: self.preflight_response_text.clone(),
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
            vision: true,
            tool_calling: true,
            input_types: test_native_image_input_capabilities(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        let preflight = is_turn_preflight_request(&request);
        let tools_available = Self::tools_available(&request);
        self.requests
            .lock()
            .expect("loop budget provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(ChatResponse {
                text: self.preflight_response_text.clone(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }

        let round_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let tool_calls = match self.mode {
            LoopBudgetProviderMode::ToolWhileAvailableThenFinal if tools_available => {
                vec![Self::missing_tool_call(round_index)]
            }
            LoopBudgetProviderMode::TwoToolRoundsThenFinal
                if tools_available && round_index < 2 =>
            {
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
                    1 => vec![Self::successful_tool_call(round_index)],
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
impl Provider for AlwaysTaskCreateProvider {
    fn name(&self) -> &str {
        "always-task-create"
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
            .expect("always task_create provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }

        let round_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            text: String::new(),
            usage: None,
            reasoning_content: None,
            tool_calls: vec![ProviderToolCall {
                id: format!("call_task_create_loop_{round_index}"),
                name: "task_create".to_owned(),
                arguments: serde_json::json!({
                    "title": "Daily weather",
                    "goal": "Create the scheduled task",
                    "trigger": "every day at 07:00"
                })
                .to_string(),
            }],
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
        let preflight = is_turn_preflight_request(&request);
        self.requests
            .lock()
            .expect("provider boundary lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }

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
        let preflight = is_turn_preflight_request(&request);
        self.requests
            .lock()
            .expect("capture provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(ChatResponse {
                text: self.preflight_response_text.clone(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }
        Ok(ChatResponse {
            text: self.response_text.clone(),
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
impl Provider for ReviewGuardProvider {
    fn name(&self) -> &str {
        "review-guard"
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
            .expect("review guard provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }

        let step = self
            .steps
            .lock()
            .expect("review guard steps lock poisoned")
            .pop_front()
            .unwrap_or_else(|| ReviewGuardProviderStep::text("done"));
        let response = match step {
            ReviewGuardProviderStep::Text(text) => ChatResponse {
                text,
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            },
            ReviewGuardProviderStep::Tool { name, arguments } => ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![ProviderToolCall {
                    id: format!("call_review_guard_{name}"),
                    name,
                    arguments: arguments.to_string(),
                }],
            },
        };
        Ok(response)
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

#[async_trait::async_trait]
impl Provider for EmptyNoToolRoundProvider {
    fn name(&self) -> &str {
        "empty-no-tool-round"
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
            .expect("empty no-tool provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }

        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        if index == 0
            && let Some(first_tool_call) = self.first_tool_call.clone()
        {
            return Ok(ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![first_tool_call],
            });
        }

        let empty_index = index.saturating_sub(usize::from(self.first_tool_call.is_some()));
        if empty_index < self.empty_rounds_before_final {
            return Ok(ChatResponse {
                text: String::new(),
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            });
        }

        Ok(ChatResponse {
            text: "done after empty response recovery".to_owned(),
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
impl Provider for SequencedTextProvider {
    fn name(&self) -> &str {
        "sequenced-text"
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
            .expect("sequenced text provider lock poisoned")
            .push(request);
        if preflight {
            return Ok(test_preflight_response());
        }

        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let text = self
            .responses
            .get(index)
            .or_else(|| self.responses.last())
            .cloned()
            .unwrap_or_else(|| "done".to_owned());
        Ok(ChatResponse {
            text,
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
impl TurnFinalizationProvider for RetryOnceFinalizationProvider {
    async fn check_turn_finalization(
        &self,
        context: TurnFinalizationContext,
    ) -> Result<TurnFinalizationDecision, String> {
        self.contexts
            .lock()
            .expect("retry finalization provider lock poisoned")
            .push(context);
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            Ok(TurnFinalizationDecision::Retry {
                instruction: "finalization retry instruction".to_owned(),
            })
        } else {
            Ok(TurnFinalizationDecision::Allow)
        }
    }
}

#[async_trait::async_trait]
impl Provider for TextOnlyAgentProvider {
    fn name(&self) -> &str {
        "text-only"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        self.inner.chat(request).await
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        self.inner.stream_chat(request).await
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
        AgentDurableEvent::TurnCapabilitiesResolved {
            thread_id,
            turn_id,
            accepted,
            rejected,
            mcp_bindings,
        } => Some(AgentEvent::TurnCapabilitiesResolved {
            thread_id,
            turn_id,
            accepted,
            rejected,
            mcp_bindings,
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
        AgentDurableEvent::TurnExecutionWindowStarted { .. }
        | AgentDurableEvent::TurnExecutionWindowExhausted { .. }
        | AgentDurableEvent::TurnExecutionWindowCheckpointed { .. }
        | AgentDurableEvent::TurnExecutionWindowContinued { .. }
        | AgentDurableEvent::TurnExecutionWindowBlocked { .. } => None,
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
        AgentDurableEvent::TurnBlocked {
            thread_id,
            turn_id,
            reason,
            recovery,
        } => Some(AgentEvent::TurnBlocked {
            thread_id,
            turn_id,
            reason,
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
            AgentEvent::TurnCompleted { .. }
                | AgentEvent::TurnFailed { .. }
                | AgentEvent::TurnBlocked { .. }
        );
        observed.push(event);
        if terminal {
            return observed;
        }
    }

    panic!("terminal agent event not received")
}

async fn recv_events_until_loop_budget_action(
    events: &mut tokio::sync::mpsc::Receiver<AgentEvent>,
    limit_kind: ToolLoopBudgetLimitKind,
    action: ToolLoopBudgetAction,
) -> Vec<AgentEvent> {
    let mut observed = Vec::new();

    for _ in 0..160 {
        let event = match timeout(Duration::from_secs(2), events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                panic!("agent event channel should stay open")
            }
            Err(_) => panic!("timed out waiting for loop budget event"),
        };
        let matched = matches!(
            &event,
            AgentEvent::TurnToolLoopBudgetExceeded(notification)
                if notification.limit_kind == limit_kind && notification.action == action
        );
        assert!(
            !matches!(
                &event,
                AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
            ),
            "window budget exhaustion must not be terminal before continuation: {event:?}"
        );
        observed.push(event);
        if matched {
            return observed;
        }
    }

    panic!("loop budget event not received")
}

async fn recv_durable_events_until_turn_blocked(
    events: &mut tokio::sync::mpsc::Receiver<AgentDurableEvent>,
) -> Vec<AgentDurableEvent> {
    let mut observed = Vec::new();

    for _ in 0..160 {
        let event = match timeout(Duration::from_secs(2), events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                panic!("agent durable event channel should stay open")
            }
            Err(_) => panic!("timed out waiting for turn blocked event"),
        };
        assert!(
            !matches!(&event, AgentDurableEvent::TurnFailed { .. }),
            "controlled budget stop must not emit crash-style turn failure: {event:?}"
        );
        let blocked = matches!(&event, AgentDurableEvent::TurnBlocked { .. });
        observed.push(event);
        if blocked {
            return observed;
        }
    }

    panic!("turn blocked event not received")
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

fn assert_turn_blocked(observed: &[AgentEvent], expected_reason: &str) {
    let Some(AgentEvent::TurnBlocked { reason, .. }) = observed.last() else {
        panic!("expected terminal turn block, observed {observed:?}");
    };
    assert_eq!(reason, expected_reason);
}

fn completed_agent_message_text(observed: &[AgentEvent]) -> Option<String> {
    observed.iter().rev().find_map(|event| {
        let AgentEvent::ItemCompleted(notification) = event else {
            return None;
        };
        let TurnItem::AgentMessage { text, .. } = &notification.item else {
            return None;
        };
        Some(text.clone())
    })
}

fn completed_system_event_codes(observed: &[AgentEvent]) -> Vec<String> {
    observed
        .iter()
        .filter_map(|event| {
            let AgentEvent::ItemCompleted(ItemCompletedNotification { item, .. }) = event else {
                return None;
            };
            let TurnItem::SystemEvent { code, .. } = item else {
                return None;
            };
            code.clone()
        })
        .collect()
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
        .start_turn_with_capabilities(
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
            Vec::new(),
        )
        .await
        .expect("turn should start");

    recv_events_until_terminal(&mut events).await
}

#[tokio::test]
async fn preflight_agent_loop_runs_before_first_main_prompt_compile() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "preflight_agent_loop_thread",
        "workspace",
        "preflight_agent_loop_turn",
        ThreadMode::Agent,
        "capture",
        "hello",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_all_requests();
    assert_eq!(
        requests.len(),
        2,
        "agent turn should make one preflight request before the main provider request"
    );
    let preflight_request = &requests[0];
    assert!(preflight_request.tools.is_none());
    assert!(preflight_request.tool_choice.is_none());
    assert_eq!(preflight_request.compiled_prompt, None);
    assert!(
        preflight_request.messages[0]
            .content
            .contains("Structured input JSON")
    );
    assert!(preflight_request.messages[0].content.contains("\"tools\""));

    let main_request = &requests[1];
    assert!(main_request.compiled_prompt.is_some());
    assert!(main_request.tools.is_some());
}

#[tokio::test]
async fn empty_no_tool_round_retries_without_empty_agent_message() {
    let provider = Arc::new(EmptyNoToolRoundProvider::new(2));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "empty-no-tool-round",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "empty_no_tool_retry_thread",
        "workspace",
        "empty_no_tool_retry_turn",
        ThreadMode::Agent,
        "empty-no-tool-round",
        "hello",
    )
    .await;
    assert_turn_completed(&observed);

    let completed_messages = observed
        .iter()
        .filter_map(|event| {
            let AgentEvent::ItemCompleted(notification) = event else {
                return None;
            };
            let TurnItem::AgentMessage { text, .. } = &notification.item else {
                return None;
            };
            Some(text.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_messages,
        vec!["done after empty response recovery".to_owned()],
        "empty no-tool rounds must not create completed AgentMessage items"
    );

    let requests = provider.snapshot_requests();
    assert_eq!(
        requests.len(),
        3,
        "two empty model rounds should be retried inside the same agent loop before final answer"
    );
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .filter(|message| message
                .content
                .contains("Your previous response was empty and was not accepted"))
            .count(),
        1
    );
    assert_eq!(
        requests[2]
            .messages
            .iter()
            .filter(|message| message
                .content
                .contains("Your previous response was empty and was not accepted"))
            .count(),
        2
    );
}

#[tokio::test]
async fn empty_no_tool_round_after_tool_result_preserves_context() {
    let provider = Arc::new(EmptyNoToolRoundProvider::with_first_tool_call(
        ProviderToolCall {
            id: "call_empty_after_tool_list_dir".to_owned(),
            name: "list_dir".to_owned(),
            arguments: serde_json::json!({"path": ".", "depth": 0, "limit": 1}).to_string(),
        },
        1,
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "empty-no-tool-round",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "empty_after_tool_retry_thread",
        "workspace",
        "empty_after_tool_retry_turn",
        ThreadMode::Agent,
        "empty-no-tool-round",
        "list files",
    )
    .await;
    assert_turn_completed(&observed);
    assert_eq!(
        completed_agent_message_text(&observed).as_deref(),
        Some("done after empty response recovery")
    );

    let requests = provider.snapshot_requests();
    assert_eq!(
        requests.len(),
        3,
        "tool round, empty retry round, and final retry round should stay in one loop"
    );
    assert_eq!(
        tool_result_message_count(&requests[1]),
        1,
        "empty model round should see the completed tool result"
    );
    assert_eq!(
        tool_result_message_count(&requests[2]),
        1,
        "retry after empty model round must keep prior tool result context"
    );
    assert!(requests[2].messages.iter().any(|message| {
        message
            .content
            .contains("Your previous response was empty and was not accepted")
    }));
}

#[tokio::test]
async fn tool_schema_dump_no_tool_round_retries_without_schema_agent_message() {
    let provider = Arc::new(SequencedTextProvider::new(vec![
        TOOL_SCHEMA_DUMP_RESPONSE,
        "done after schema dump recovery",
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-text",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "schema_dump_retry_thread",
        "workspace",
        "schema_dump_retry_turn",
        ThreadMode::Agent,
        "sequenced-text",
        "hello",
    )
    .await;
    assert_turn_completed(&observed);

    let completed_messages = observed
        .iter()
        .filter_map(|event| {
            let AgentEvent::ItemCompleted(notification) = event else {
                return None;
            };
            let TurnItem::AgentMessage { text, .. } = &notification.item else {
                return None;
            };
            Some(text.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_messages,
        vec!["done after schema dump recovery".to_owned()],
        "tool schema dumps must not create completed AgentMessage items"
    );

    let requests = provider.snapshot_requests();
    assert_eq!(
        requests.len(),
        2,
        "schema dump should be retried inside the same agent loop before final answer"
    );
    assert!(
        requests[1].messages.iter().any(|message| {
            message
                .content
                .contains("reproduced tool schemas or tool definitions")
        }),
        "retry request should include the schema-dump recovery instruction"
    );
}

#[tokio::test]
async fn raw_tool_call_markup_no_tool_round_retries_without_raw_agent_message() {
    let provider = Arc::new(SequencedTextProvider::new(vec![
        RAW_TOOL_CALL_MARKUP_RESPONSE,
        "done after raw tool-call recovery",
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-text",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "raw_tool_markup_retry_thread",
        "workspace",
        "raw_tool_markup_retry_turn",
        ThreadMode::Agent,
        "sequenced-text",
        "hello",
    )
    .await;
    assert_turn_completed(&observed);

    let completed_messages = observed
        .iter()
        .filter_map(|event| {
            let AgentEvent::ItemCompleted(notification) = event else {
                return None;
            };
            let TurnItem::AgentMessage { text, .. } = &notification.item else {
                return None;
            };
            Some(text.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_messages,
        vec!["done after raw tool-call recovery".to_owned()],
        "raw tool-call markup must not create completed AgentMessage items"
    );

    let requests = provider.snapshot_requests();
    assert_eq!(
        requests.len(),
        2,
        "raw tool-call markup should be retried inside the same agent loop before final answer"
    );
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| message.content.contains("raw tool-call markup")),
        "retry request should include the raw tool-call markup recovery instruction"
    );
}

#[tokio::test]
async fn finalization_retry_runs_before_agent_message_in_same_loop() {
    let provider = Arc::new(SequencedTextProvider::new(vec![
        "premature final answer",
        "accepted final answer",
    ]));
    let finalization = Arc::new(RetryOnceFinalizationProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-text",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_turn_finalization_provider(Some(finalization.clone()))
        .await;

    let observed = start_simple_turn(
        &manager,
        "finalization_retry_thread",
        "workspace",
        "finalization_retry_turn",
        ThreadMode::Agent,
        "sequenced-text",
        "finish with artifact",
    )
    .await;
    assert_turn_completed(&observed);

    let completed_messages = observed
        .iter()
        .filter_map(|event| {
            let AgentEvent::ItemCompleted(notification) = event else {
                return None;
            };
            let TurnItem::AgentMessage { text, .. } = &notification.item else {
                return None;
            };
            Some(text.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_messages,
        vec!["accepted final answer".to_owned()],
        "rejected finalization attempt must not be emitted as AgentMessage"
    );

    let contexts = finalization.snapshot_contexts();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].final_text, "premature final answer");
    assert_eq!(contexts[1].final_text, "accepted final answer");

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "provider should receive the initial window requests before continuation"
    );
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| { message.content.contains("finalization retry instruction") })
    );
}

#[tokio::test]
async fn third_consecutive_empty_no_tool_round_surfaces_provider_failure() {
    let provider = Arc::new(EmptyNoToolRoundProvider::new(usize::MAX));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "empty-no-tool-round",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "empty_no_tool_failure_thread";
    let turn_id = "empty_no_tool_failure_turn";

    manager
        .ensure_thread(thread_id, "workspace")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;
    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "empty-no-tool-round",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut completed_agent_messages = Vec::new();
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");
        match event {
            AgentEvent::ItemCompleted(notification) => {
                if let TurnItem::AgentMessage { text, .. } = notification.item {
                    completed_agent_messages.push(text);
                }
            }
            AgentEvent::ProviderFailureDetected { failure, .. } => {
                assert_eq!(failure.class, ProviderFailureClass::EmptyResponse);
                assert_eq!(
                    failure.provider_code.as_deref(),
                    Some("empty_model_response")
                );
                assert!(failure.is_recoverable_hint);
                assert_eq!(
                    failure.message.as_deref(),
                    Some("model returned an empty response without tool calls")
                );
                assert!(
                    completed_agent_messages.is_empty(),
                    "empty no-tool provider failure must not create AgentMessage items"
                );
                assert_eq!(
                    provider.snapshot_requests().len(),
                    3,
                    "provider failure should happen on the third consecutive empty no-tool round"
                );
                return;
            }
            AgentEvent::TurnCompleted { .. } => {
                panic!("empty no-tool rounds must not complete the turn")
            }
            AgentEvent::TurnFailed { error, .. } => {
                panic!(
                    "empty no-tool rounds should surface provider failure, not TurnFailed: {error}"
                )
            }
            _ => {}
        }
    }

    panic!("provider failure was not emitted for repeated empty no-tool rounds")
}

#[tokio::test]
async fn third_consecutive_tool_schema_dump_no_tool_round_surfaces_provider_failure() {
    let provider = Arc::new(SequencedTextProvider::new(vec![
        TOOL_SCHEMA_DUMP_RESPONSE,
        TOOL_SCHEMA_DUMP_RESPONSE,
        TOOL_SCHEMA_DUMP_RESPONSE,
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-text",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "schema_dump_failure_thread";
    let turn_id = "schema_dump_failure_turn";

    manager
        .ensure_thread(thread_id, "workspace")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;
    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-text",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let mut completed_agent_messages = Vec::new();
    for _ in 0..40 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("must receive agent event in time")
            .expect("broadcast should remain open");
        match event {
            AgentEvent::ItemCompleted(notification) => {
                if let TurnItem::AgentMessage { text, .. } = notification.item {
                    completed_agent_messages.push(text);
                }
            }
            AgentEvent::ProviderFailureDetected { failure, .. } => {
                assert_eq!(failure.class, ProviderFailureClass::Unknown);
                assert_eq!(
                    failure.provider_code.as_deref(),
                    Some("tool_schema_dump_response")
                );
                assert!(failure.is_recoverable_hint);
                assert_eq!(
                    failure.message.as_deref(),
                    Some("model returned tool schema definitions instead of a final answer")
                );
                assert!(
                    completed_agent_messages.is_empty(),
                    "tool schema dump provider failure must not create AgentMessage items"
                );
                assert_eq!(
                    provider.snapshot_requests().len(),
                    3,
                    "provider failure should happen on the third consecutive schema dump no-tool round"
                );
                return;
            }
            AgentEvent::TurnCompleted { .. } => {
                panic!("tool schema dump no-tool rounds must not complete the turn")
            }
            AgentEvent::TurnFailed { error, .. } => {
                panic!(
                    "tool schema dump no-tool rounds should surface provider failure, not TurnFailed: {error}"
                )
            }
            _ => {}
        }
    }

    panic!("provider failure was not emitted for repeated tool schema dump no-tool rounds")
}

#[tokio::test]
async fn context_isolation_old_task_local_constraint_stays_historical() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let thread_id = "context_isolation_thread";
    let workspace_id = "context_isolation_workspace";

    manager
        .ensure_thread(thread_id, workspace_id)
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            "context_isolation_turn_1",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "For this one-off check, do not click the red button.".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("first turn should start");
    let first_observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&first_observed);

    manager
        .start_turn_with_capabilities(
            thread_id,
            "context_isolation_turn_2",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "Open the requested desktop location.".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("second turn should start");
    let second_observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&second_observed);

    let requests = provider.snapshot_requests();
    assert_eq!(
        requests.len(),
        2,
        "two completed agent turns should produce two visible provider requests"
    );
    let second_request = &requests[1];
    let second_prompt = second_request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");

    assert!(
        !second_prompt
            .full_system_text
            .contains("For this one-off check, do not click the red button."),
        "old task-local text must not be promoted into the active system prompt"
    );

    let second_messages_text = second_request
        .messages
        .iter()
        .map(|message| message.text_content_lossy())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        second_messages_text.contains("Open the requested desktop location."),
        "current turn instruction must be present"
    );
    assert!(
        !second_request.messages.iter().any(|message| {
            message.role == pioneer_provider::Role::System
                && message
                    .text_content_lossy()
                    .contains("For this one-off check, do not click the red button.")
        }),
        "old task-local text must not be reintroduced as an active system instruction"
    );
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

    let calls = wait_for_hook_calls(&calls, PHASE_07_HOOK_PHASES.len()).await;
    assert_eq!(calls.len(), PHASE_07_HOOK_PHASES.len());
    assert_eq!(
        calls.iter().map(|call| call.phase).collect::<Vec<_>>(),
        PHASE_07_HOOK_PHASES
    );
    for call in calls {
        assert_eq!(call.input_kind, HookInputKind::from(call.phase));
        if call.phase == HookPhase::TurnPostTurn {
            let HookInputPayload::TurnPostTurn(payload) = &call.payload else {
                panic!("post-turn call should receive typed post-turn payload");
            };
            assert_eq!(payload.status, TurnPostTurnStatus::Succeeded);
            assert_eq!(
                payload
                    .user_text
                    .as_ref()
                    .map(|preview| preview.text.as_str()),
                Some("phase 07 agent hook phases")
            );
            assert_eq!(
                payload
                    .assistant_text
                    .as_ref()
                    .map(|preview| preview.text.as_str()),
                Some("done")
            );
        } else if call.phase == HookPhase::TurnPreToolMaterialization {
            let HookInputPayload::TurnPreToolMaterialization(payload) = &call.payload else {
                panic!("pre-tool-materialization call should receive typed payload");
            };
            assert!(payload.provider_tool_calling);
        } else if call.phase == HookPhase::TurnPrePolicy {
            let HookInputPayload::TurnPrePolicy(payload) = &call.payload else {
                panic!("pre-policy call should receive typed payload");
            };
            assert_eq!(payload.input_text, "phase 07 agent hook phases");
            assert_eq!(payload.model.as_deref(), Some("test-model"));
            assert_eq!(payload.model_provider.as_deref(), Some("capture"));
        } else if call.phase == HookPhase::TurnPrePromptContext {
            let HookInputPayload::TurnPrePromptContext(payload) = &call.payload else {
                panic!("pre-prompt-context call should receive typed payload");
            };
            assert_eq!(payload.input_text, "phase 07 agent hook phases");
            assert_eq!(payload.model.as_deref(), Some("test-model"));
            assert_eq!(payload.model_provider.as_deref(), Some("capture"));
        } else if call.phase == HookPhase::TurnPostPreflightPromptContext {
            let HookInputPayload::TurnPostPreflightPromptContext(payload) = &call.payload else {
                panic!("post-preflight prompt-context call should receive typed payload");
            };
            assert_eq!(payload.input_text, "phase 07 agent hook phases");
            assert_eq!(payload.model.as_deref(), Some("test-model"));
            assert_eq!(payload.model_provider.as_deref(), Some("capture"));
        } else if call.phase == HookPhase::TurnPrePromptCompile {
            let HookInputPayload::TurnPrePromptCompile(payload) = &call.payload else {
                panic!("pre-prompt-compile call should receive typed payload");
            };
            assert!(payload.provider_tool_calling);
        } else {
            assert_eq!(call.payload, HookInputPayload::Empty);
        }
        assert_eq!(call.workspace_id.as_deref(), Some("ws_phase07_agent_hooks"));
        assert_eq!(call.thread_id.as_deref(), Some("thr_phase07_agent_hooks"));
        assert_eq!(call.turn_id.as_deref(), Some("turn_phase07_agent_hooks"));
        assert_eq!(call.mode, Some(HookContextMode::Agent));
        assert_eq!(call.actor_kind, Some(HookActorKind::Agent));
        assert!(call.policy_set.is_empty());
        assert!(call.prompt_context_set.is_empty());
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

    let calls = wait_for_hook_calls(&calls, PHASE_07_HOOK_PHASES.len()).await;
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
async fn phase_07_non_prompt_section_contributions_do_not_affect_agent_request() {
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
    assert!(!prompt.full_system_text.contains("ignored_policy"));

    let hook_manifest = hook_observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
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
async fn phase_12_post_turn_hook_runs_after_success_with_summary_input() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_success",
        "ws_phase12_success",
        "turn_phase12_success",
        ThreadMode::Agent,
        "capture",
        "phase 12 user text",
    )
    .await;
    assert_turn_completed(&observed);

    let call = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    let HookInputPayload::TurnPostTurn(payload) = call.payload else {
        panic!("post-turn hook should receive typed post-turn payload");
    };
    assert_eq!(payload.status, TurnPostTurnStatus::Succeeded);
    assert_eq!(
        payload
            .user_text
            .as_ref()
            .map(|preview| preview.text.as_str()),
        Some("phase 12 user text")
    );
    assert_eq!(
        payload
            .assistant_text
            .as_ref()
            .map(|preview| preview.text.as_str()),
        Some("done")
    );
    assert!(payload.error.is_none());
    assert!(payload.tool_events.is_empty());
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn task_runtime_turn_hook_context_marks_post_turn_as_task_owned() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;
    manager
        .ensure_thread("thr_task_runtime_hook", "ws_task_runtime_hook")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, "thr_task_runtime_hook").await;

    manager
        .start_turn_with_hook_context(
            "thr_task_runtime_hook",
            "turn_task_runtime_hook",
            ThreadMode::Agent,
            AgentTurnHookRuntimeContext::task("task-runtime-1"),
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "TASK RUN EXECUTION\nRUN OBJECTIVE\nDo the scheduled work.".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            Vec::new(),
        )
        .await
        .expect("task runtime turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let call = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    assert_eq!(call.mode, Some(HookContextMode::Task));
    assert_eq!(call.actor_kind, Some(HookActorKind::Task));
    assert_eq!(call.task_id.as_deref(), Some("task-runtime-1"));
    let HookInputPayload::TurnPostTurn(payload) = call.payload else {
        panic!("post-turn hook should receive typed post-turn payload");
    };
    assert_eq!(payload.status, TurnPostTurnStatus::Succeeded);
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_12_post_turn_hook_receives_tool_event_summaries() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_phase12_tool_summary".to_owned(),
            name: "list_dir".to_owned(),
            arguments: serde_json::json!({"path": ".", "depth": 0, "limit": 1}).to_string(),
        }],
        "final after tool",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_tool_summary",
        "ws_phase12_tool_summary",
        "turn_phase12_tool_summary",
        ThreadMode::Agent,
        "sequenced-tools",
        "phase 12 tool summary",
    )
    .await;
    assert_turn_completed(&observed);

    let call = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    let HookInputPayload::TurnPostTurn(payload) = call.payload else {
        panic!("post-turn hook should receive typed post-turn payload");
    };
    assert_eq!(
        payload
            .assistant_text
            .as_ref()
            .map(|preview| preview.text.as_str()),
        Some("final after tool")
    );
    let tool_event = payload
        .tool_events
        .iter()
        .find(|event| event.tool_name == "list_dir")
        .expect("list_dir event should be summarized");
    assert_eq!(tool_event.item_id, "call_phase12_tool_summary");
    assert_eq!(tool_event.status, TurnPostTurnToolStatus::Succeeded);
    assert!(!payload.tool_events_truncated);
    assert!(payload.domain_events.iter().any(|event| {
        event.item_id.as_deref() == Some("call_phase12_tool_summary")
            && event.code.as_deref() == Some("tool.succeeded")
    }));
    assert_eq!(provider.snapshot_requests().len(), 2);
}

#[tokio::test]
async fn phase_12_post_turn_hook_receives_bounded_summary_input() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let long_user_text = "u".repeat(4_200);
    let long_assistant_text = "a".repeat(4_200);
    let provider = Arc::new(CaptureAgentProvider::with_response_text(
        long_assistant_text.clone(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_bounded",
        "ws_phase12_bounded",
        "turn_phase12_bounded",
        ThreadMode::Agent,
        "capture",
        long_user_text.as_str(),
    )
    .await;
    assert_turn_completed(&observed);

    let call = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    let HookInputPayload::TurnPostTurn(payload) = call.payload else {
        panic!("post-turn hook should receive typed post-turn payload");
    };
    let user_preview = payload.user_text.as_ref().expect("user preview");
    assert!(user_preview.truncated);
    assert_eq!(user_preview.original_chars, 4_200);
    assert_eq!(user_preview.text.chars().count(), user_preview.max_chars);
    assert_ne!(user_preview.text, long_user_text);

    let assistant_preview = payload.assistant_text.as_ref().expect("assistant preview");
    assert!(assistant_preview.truncated);
    assert_eq!(assistant_preview.original_chars, 4_200);
    assert_eq!(
        assistant_preview.text.chars().count(),
        assistant_preview.max_chars
    );
    assert_ne!(assistant_preview.text, long_assistant_text);
}

#[tokio::test]
async fn phase_12_post_turn_hook_failure_does_not_change_completed_turn() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase_with_failures(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::Required,
            vec![HookPhase::TurnPostTurn],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_hook_failure",
        "ws_phase12_hook_failure",
        "turn_phase12_hook_failure",
        ThreadMode::Agent,
        "capture",
        "phase 12 hook failure",
    )
    .await;
    assert_turn_completed(&observed);
    let _ = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    assert!(matches!(
        observed.last(),
        Some(AgentEvent::TurnCompleted { .. })
    ));
}

#[tokio::test]
async fn phase_12_post_turn_hook_runs_after_failure_when_configured() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_post_turn_hook_dispatch_policy(
            AgentPostTurnHookDispatchPolicy::default().include_failures(),
        )
        .await;
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_phase_failures(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::Required,
            vec![HookPhase::TurnPrePolicy],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_failure",
        "ws_phase12_failure",
        "turn_phase12_failure",
        ThreadMode::Agent,
        "capture",
        "phase 12 failure user text",
    )
    .await;
    assert_turn_failed(&observed, "turn policy hook failed");

    let post_turn = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    let HookInputPayload::TurnPostTurn(payload) = post_turn.payload else {
        panic!("post-turn hook should receive typed post-turn payload");
    };
    assert_eq!(payload.status, TurnPostTurnStatus::Failed);
    assert_eq!(
        payload
            .user_text
            .as_ref()
            .map(|preview| preview.text.as_str()),
        Some("phase 12 failure user text")
    );
    assert_eq!(
        payload.error.as_ref().map(|preview| preview.text.as_str()),
        Some("turn policy hook failed")
    );
    assert!(provider.snapshot_requests().is_empty());
}

#[tokio::test]
async fn phase_12_background_post_turn_hook_does_not_delay_completion_notification() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let runtime = recording_hook_runtime_for_phase(
        calls.clone(),
        HookPhase::TurnPostTurn,
        HookAwaitPolicy::Background,
        HookFailurePolicy::BestEffort,
    );
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager.set_hook_runtime(Some(runtime.clone())).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_background",
        "ws_phase12_background",
        "turn_phase12_background",
        ThreadMode::Agent,
        "capture",
        "phase 12 background",
    )
    .await;
    assert_turn_completed(&observed);
    assert!(matches!(
        observed.last(),
        Some(AgentEvent::TurnCompleted { .. })
    ));

    let _post_turn = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;
    assert_eq!(
        runtime
            .queued_background_len()
            .expect("background queue len"),
        0
    );
}

#[tokio::test]
async fn phase_12_post_turn_hook_does_not_create_task_anchor() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPostTurn,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_no_anchor",
        "ws_phase12_no_anchor",
        "turn_phase12_no_anchor",
        ThreadMode::Agent,
        "capture",
        "phase 12 no task anchor",
    )
    .await;
    assert_turn_completed(&observed);
    let _ = wait_for_hook_phase(&calls, HookPhase::TurnPostTurn).await;

    assert!(!observed.iter().any(|event| {
        matches!(
            event,
            AgentEvent::ItemStarted(ItemStartedNotification {
                item: TurnItem::Task { .. },
                ..
            }) | AgentEvent::ItemCompleted(ItemCompletedNotification {
                item: TurnItem::Task { .. },
                ..
            })
        )
    }));
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
async fn phase_08_best_effort_policy_hook_failure_does_not_mark_turn_failed() {
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
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
async fn phase_09_one_context_contribution_reaches_pre_prompt_compile() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![prompt_context_contribution(
                "test.context.one",
                "test",
                10,
                "context one",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;
    let observed = start_simple_turn(
        &manager,
        "thr_phase09_context_one",
        "ws_phase09_context_one",
        "turn_phase09_context_one",
        ThreadMode::Agent,
        "capture",
        "phase 09 context aggregation",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    assert!(pre_prompt_context.prompt_context_set.is_empty());
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    assert_eq!(pre_prompt_compile.prompt_context_set.entries.len(), 1);
    let entry = &pre_prompt_compile.prompt_context_set.entries[0];
    assert_eq!(entry.contribution_id.as_str(), "test.context.one");
    assert_eq!(entry.domain.as_str(), "test");
    assert_eq!(entry.content.as_str(), "context one");

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("context one"));
}

#[tokio::test]
async fn phase_09_multiple_context_contributions_order_deterministically() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![
                prompt_context_contribution("test.context.low", "test", 0, "low"),
                prompt_context_contribution("test.context.same_b", "test.b", 5, "same b"),
                prompt_context_contribution("test.context.high", "test", 10, "high"),
                prompt_context_contribution("test.context.same_a", "test.a", 5, "same a"),
            ],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;
    let observed = start_simple_turn(
        &manager,
        "thr_phase09_context_order",
        "ws_phase09_context_order",
        "turn_phase09_context_order",
        ThreadMode::Agent,
        "capture",
        "phase 09 context ordering",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    let ordered_ids = pre_prompt_compile
        .prompt_context_set
        .entries
        .iter()
        .map(|entry| entry.contribution_id.as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_ids,
        vec![
            "test.context.high".to_owned(),
            "test.context.same_a".to_owned(),
            "test.context.same_b".to_owned(),
            "test.context.low".to_owned(),
        ]
    );
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_09_context_budget_truncates_predictably() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![prompt_context_contribution_with_max_chars(
                "test.context.long",
                "test",
                10,
                "0123456789abcdef",
                8,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase09_context_truncate",
        "ws_phase09_context_truncate",
        "turn_phase09_context_truncate",
        ThreadMode::Agent,
        "capture",
        "phase 09 context truncation",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    let set = &pre_prompt_compile.prompt_context_set;
    assert_eq!(set.entries.len(), 1);
    assert_eq!(set.entries[0].content.as_str(), "01234567");
    assert!(set.entries[0].truncated);
    assert!(set.truncated);
    assert!(set.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "prompt_context.truncated"
            && diagnostic.safe_for_user
            && !diagnostic.message.as_str().contains("0123456789abcdef")
    }));
    assert_eq!(provider.snapshot_requests().len(), 1);
}

#[tokio::test]
async fn phase_09_failed_context_hook_yields_diagnostic_and_empty_context() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_phase_failures(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::Required,
            vec![HookPhase::TurnPrePromptContext],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase09_context_failure",
        "ws_phase09_context_failure",
        "turn_phase09_context_failure",
        ThreadMode::Agent,
        "capture",
        "phase 09 context failure",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    let set = &pre_prompt_compile.prompt_context_set;
    assert!(set.entries.is_empty());
    assert!(set.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "prompt_context.runtime_failed" && diagnostic.safe_for_user
    }));
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("phase 07 hook failed"));
}

#[tokio::test]
async fn phase_09_context_contribution_does_not_change_provider_prompt_yet() {
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
        "thr_phase09_prompt_baseline",
        "ws_phase09_prompt",
        "turn_phase09_prompt_baseline",
        ThreadMode::Agent,
        "capture",
        "phase 09 prompt stability",
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
            calls.clone(),
            vec![prompt_context_contribution(
                "test.context.hidden",
                "test",
                10,
                "HOOK PROMPT CONTEXT MUST NOT APPEAR",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let hook_observed = start_simple_turn(
        &hook_manager,
        "thr_phase09_prompt_hook",
        "ws_phase09_prompt",
        "turn_phase09_prompt_hook",
        ThreadMode::Agent,
        "capture",
        "phase 09 prompt stability",
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
            .contains("HOOK PROMPT CONTEXT MUST NOT APPEAR")
    );
}

#[tokio::test]
async fn phase_09_no_context_hooks_preserve_agent_request() {
    let baseline_provider = Arc::new(CaptureAgentProvider::default());
    let baseline_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        baseline_provider.clone(),
    ));
    let baseline_manager = AgentManager::new(baseline_registry, test_tool_loop_config());

    let baseline_observed = start_simple_turn(
        &baseline_manager,
        "thr_phase09_no_context_baseline",
        "ws_phase09_no_context",
        "turn_phase09_no_context_baseline",
        ThreadMode::Agent,
        "capture",
        "phase 09 no context hooks",
    )
    .await;
    assert_turn_completed(&baseline_observed);

    let empty_provider = Arc::new(CaptureAgentProvider::default());
    let empty_registry = Arc::new(ProviderRegistry::with_provider(
        "capture",
        empty_provider.clone(),
    ));
    let empty_manager = AgentManager::new(empty_registry, test_tool_loop_config());
    empty_manager
        .set_hook_runtime(Some(empty_hook_runtime()))
        .await;

    let empty_observed = start_simple_turn(
        &empty_manager,
        "thr_phase09_no_context_empty",
        "ws_phase09_no_context",
        "turn_phase09_no_context_empty",
        ThreadMode::Agent,
        "capture",
        "phase 09 no context hooks",
    )
    .await;
    assert_turn_completed(&empty_observed);

    let baseline_requests = baseline_provider.snapshot_requests();
    let empty_requests = empty_provider.snapshot_requests();
    assert_eq!(baseline_requests.len(), 1);
    assert_eq!(empty_requests.len(), 1);
    assert_stable_requests_eq(&baseline_requests[0], &empty_requests[0]);
}

#[tokio::test]
async fn phase_09_chat_mode_still_does_not_call_turn_hooks() {
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
            vec![prompt_context_contribution(
                "test.context.chat",
                "test",
                10,
                "CHAT HOOK CONTEXT MUST NOT RUN",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase09_chat_context",
        "ws_phase09_chat_context",
        "turn_phase09_chat_context",
        ThreadMode::Chat,
        "capture-standard",
        "phase 09 chat mode should not call hooks",
    )
    .await;
    assert_turn_completed(&observed);

    assert!(snapshot_hook_calls(&calls).is_empty());
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].compiled_prompt.is_none());
}

#[tokio::test]
async fn phase_09_memory_path_remains_unchanged() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_phase09_context_isolation",
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
            vec![prompt_context_contribution(
                "test.context.memory",
                "test",
                10,
                "HOOK MEMORY CONTEXT MUST NOT APPEAR",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase09_memory_not_migrated",
        "ws_phase09_memory_not_migrated",
        "turn_phase09_memory_not_migrated",
        ThreadMode::Agent,
        "capture",
        "phase 09 memory remains on existing path",
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
    assert!(
        !prompt
            .full_system_text
            .contains("HOOK MEMORY CONTEXT MUST NOT APPEAR")
    );
}

#[tokio::test]
async fn phase_10_hook_prompt_section_appears_in_compiled_prompt() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_section_contribution(
                "test.phase10.alpha",
                "Alpha Hook Section",
                "test",
                10,
                "alpha hook section content",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_prompt_section",
        "ws_phase10_prompt_section",
        "turn_phase10_prompt_section",
        ThreadMode::Agent,
        "capture",
        "phase 10 prompt section",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Alpha Hook Section"));
    assert!(
        prompt
            .full_system_text
            .contains("alpha hook section content")
    );
}

#[tokio::test]
async fn phase_10_prompt_compile_receives_final_visible_tools_not_registered_memory_tools() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
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
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_visible_tools_core",
        "ws_phase10_visible_tools_core",
        "turn_phase10_visible_tools_core",
        ThreadMode::Agent,
        "capture",
        "ordinary question",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    let HookInputPayload::TurnPrePromptCompile(payload) = &pre_prompt_compile.payload else {
        panic!("pre-prompt-compile call should receive typed payload");
    };
    let hook_tool_names = payload
        .available_tool_names
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect::<Vec<_>>();
    assert!(hook_tool_names.contains(&"request_tools".to_owned()));
    assert!(
        !hook_tool_names
            .iter()
            .any(|name| name.starts_with("memory_")),
        "prompt compile must not see hidden registered memory tools: {hook_tool_names:?}"
    );

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request_tool_names = requests[0]
        .tools
        .as_ref()
        .expect("main request should include provider tools")
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    for core_tool in [
        "exec_command",
        "write_stdin",
        "apply_patch",
        "write_file",
        "edit_file",
        "request_tools",
    ] {
        assert!(
            hook_tool_names.contains(&core_tool.to_owned()),
            "ordinary turns should expose core tool `{core_tool}` before prompt compile: {hook_tool_names:?}"
        );
        assert!(
            request_tool_names.contains(&core_tool.to_owned()),
            "ordinary turns should expose core tool `{core_tool}` to the provider without lazy activation: {request_tool_names:?}"
        );
    }
    assert!(
        !request_tool_names
            .iter()
            .any(|name| name.starts_with("memory_")),
        "provider tools must not include hidden registered memory tools: {request_tool_names:?}"
    );
    let mut sorted_hook_tool_names = hook_tool_names.clone();
    sorted_hook_tool_names.sort();
    let mut sorted_request_tool_names = request_tool_names.clone();
    sorted_request_tool_names.sort();
    assert_eq!(sorted_hook_tool_names, sorted_request_tool_names);

    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("main request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("Some tool domains and their tools are hidden until requested.")
    );
    assert!(
        !prompt.full_system_text.contains("Available memory tools:"),
        "hidden memory tools must not render memory prompt contract"
    );
}

#[tokio::test]
async fn phase_10_prompt_manifest_and_tool_schemas_exclude_discovery_tools() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_no_discovery_tools",
        "ws_phase10_no_discovery_tools",
        "turn_phase10_no_discovery_tools",
        ThreadMode::Agent,
        "capture",
        "ordinary question",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("main request should include provider tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(!tool_names.contains(&"tool_search"));
    assert!(!tool_names.contains(&"tool_suggest"));

    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("main request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("tool_search"));
    assert!(!prompt.full_system_text.contains("tool_suggest"));

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let manifest_json = serde_json::to_string(manifest).expect("manifest should serialize");
    assert!(!manifest_json.contains("tool_search"));
    assert!(!manifest_json.contains("tool_suggest"));
}

#[tokio::test]
async fn phase_10_preflight_selected_optional_domain_tool_schemas_are_serialized() {
    let registered_optional_tools = [
        "memory_search",
        "memory_get",
        "task_create",
        "task_wait",
        "artifact_prepare",
        "artifact_register",
    ];
    let handler: Arc<dyn ToolHandler> = Arc::new(MemoryFakeHandler);
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        optional_domain_preflight_response(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(Arc::new(StaticTaskToolProvider {
            bundle: fake_memory_tool_bundle_for_names(&registered_optional_tools, handler),
        })))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_optional_domain_tools",
        "ws_phase10_optional_domain_tools",
        "turn_phase10_optional_domain_tools",
        ThreadMode::Agent,
        "capture",
        "ordinary question",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request_tool_names = requests[0]
        .tools
        .as_ref()
        .expect("main request should include provider tools")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    let probe_tool_loop_config = test_tool_loop_config();
    let computer_use_available = pioneer_tools::build_builtin_tools(
        ".",
        "turn_phase10_computer_use_probe",
        probe_tool_loop_config.web,
        probe_tool_loop_config.computer_use,
    )
    .router
    .has_handler("computer_use");

    for selected in [
        "memory_search",
        "task_create",
        "artifact_prepare",
        "artifact_register",
    ] {
        assert!(
            request_tool_names.contains(&selected),
            "selected optional tool `{selected}` should be serialized"
        );
    }
    if computer_use_available {
        assert!(
            request_tool_names.contains(&"computer_use"),
            "selected optional tool `computer_use` should be serialized when registered"
        );
    } else {
        assert!(
            !request_tool_names.contains(&"computer_use"),
            "unregistered optional tool `computer_use` must stay hidden"
        );
    }
    for hidden in ["memory_get", "task_wait"] {
        assert!(
            !request_tool_names.contains(&hidden),
            "registered but unselected optional tool `{hidden}` must stay hidden"
        );
    }
    assert!(!request_tool_names.contains(&"tool_search"));
    assert!(!request_tool_names.contains(&"tool_suggest"));
}

#[tokio::test]
async fn phase_11_review_guard_injects_after_final_answer_attempt_and_allows_accept() {
    let provider = Arc::new(ReviewGuardProvider::new(vec![
        ReviewGuardProviderStep::text("premature final answer"),
        ReviewGuardProviderStep::tool(
            "task_accept",
            json!({
                "taskId": "task_review_guard",
                "runId": "run_review_guard",
                "candidateId": "candidate_review_guard"
            }),
        ),
        ReviewGuardProviderStep::text("accepted child result"),
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "review-guard",
        provider.clone(),
    ));
    let task_provider = ReviewGuardTaskToolProvider::new(review_guard_observation(
        "candidate_review_guard",
        0,
        2,
        &["task_accept", "task_revise", "task_cancel"],
        Some("initial candidate"),
    ))
    .with_review_query_skip(1);
    let task_provider_for_assert = task_provider.clone();
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(Arc::new(task_provider)))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_review_accept_after_guard",
        "ws_phase11_review_accept_after_guard",
        "turn_phase11_review_accept_after_guard",
        ThreadMode::Agent,
        "review-guard",
        "Review the attached child result.",
    )
    .await;
    assert_turn_completed(&observed);
    assert_eq!(
        completed_agent_message_text(&observed).as_deref(),
        Some("accepted child result")
    );
    assert_eq!(task_provider_for_assert.call_counts(), (1, 0, 0));

    let codes = completed_system_event_codes(&observed);
    assert_eq!(
        codes
            .iter()
            .filter(|code| code.as_str() == "task.review_required.observed")
            .count(),
        1
    );
    assert!(
        !codes
            .iter()
            .any(|code| code.as_str() == "task.terminal.observed"),
        "review-required observation must not be recorded as a terminal task observation"
    );
    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 3,
        "guard should loop after injecting review observation before accept"
    );
}

#[tokio::test]
async fn phase_11_review_guard_revise_waits_for_revision_candidate_then_accepts() {
    let provider = Arc::new(ReviewGuardProvider::new(vec![
        ReviewGuardProviderStep::tool(
            "task_revise",
            json!({
                "taskId": "task_review_guard",
                "runId": "run_review_guard",
                "candidateId": "candidate_review_guard",
                "feedback": "Fix the missing detail."
            }),
        ),
        ReviewGuardProviderStep::tool("task_wait", json!({ "taskIds": ["task_review_guard"] })),
        ReviewGuardProviderStep::tool(
            "task_accept",
            json!({
                "taskId": "task_review_guard",
                "runId": "run_review_guard",
                "candidateId": "candidate_review_guard_revision"
            }),
        ),
        ReviewGuardProviderStep::text("accepted revised child result"),
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "review-guard",
        provider.clone(),
    ));
    let task_provider = ReviewGuardTaskToolProvider::new(review_guard_observation(
        "candidate_review_guard",
        0,
        2,
        &["task_accept", "task_revise", "task_cancel"],
        Some("initial candidate"),
    ));
    let task_provider_for_assert = task_provider.clone();
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(Arc::new(task_provider)))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_review_revise_wait_accept",
        "ws_phase11_review_revise_wait_accept",
        "turn_phase11_review_revise_wait_accept",
        ThreadMode::Agent,
        "review-guard",
        "Review and revise the attached child result if needed.",
    )
    .await;
    assert_turn_completed(&observed);
    assert_eq!(
        completed_agent_message_text(&observed).as_deref(),
        Some("accepted revised child result")
    );
    assert_eq!(task_provider_for_assert.call_counts(), (1, 1, 1));

    let codes = completed_system_event_codes(&observed);
    assert_eq!(
        codes
            .iter()
            .filter(|code| code.as_str() == "task.review_required.observed")
            .count(),
        2
    );
    assert!(
        !codes
            .iter()
            .any(|code| code.as_str() == "task.terminal.observed"),
        "review-required observation must not be recorded as a terminal task observation"
    );
    let requests = provider.snapshot_requests();
    let revision_review_request = requests
        .iter()
        .find(|request| {
            request.messages.iter().any(|message| {
                message.content.contains("candidate_review_guard_revision")
                    && message.content.contains("max_revision_rounds_reached")
            })
        })
        .expect("revision candidate observation should be sent to provider");
    let visible_tool_names = revision_review_request
        .tools
        .as_ref()
        .expect("review request should include tool schemas")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(visible_tool_names.contains(&"task_accept"));
    assert!(visible_tool_names.contains(&"task_cancel"));
    assert!(visible_tool_names.contains(&"task_wait"));
    assert!(
        !visible_tool_names.contains(&"task_revise"),
        "task_revise must stay hidden when the candidate no longer allows revisions"
    );
}

#[tokio::test]
async fn phase_11_repeated_review_observation_does_not_spam_or_complete() {
    let provider = Arc::new(ReviewGuardProvider::new(vec![
        ReviewGuardProviderStep::text("ignoring review and answering anyway"),
    ]));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "review-guard",
        provider.clone(),
    ));
    let task_provider = ReviewGuardTaskToolProvider::new(review_guard_observation(
        "candidate_review_guard",
        0,
        2,
        &["task_accept", "task_revise", "task_cancel"],
        Some("initial candidate"),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(Arc::new(task_provider)))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_review_no_spam",
        "ws_phase11_review_no_spam",
        "turn_phase11_review_no_spam",
        ThreadMode::Agent,
        "review-guard",
        "Review the attached child result.",
    )
    .await;
    assert_turn_blocked(
        &observed,
        "Attached task result review is still required. Call task_accept, task_revise, or task_cancel for each pending review candidate before providing the final answer.",
    );

    let codes = completed_system_event_codes(&observed);
    assert_eq!(
        codes
            .iter()
            .filter(|code| code.as_str() == "task.review_required.observed")
            .count(),
        1
    );
    assert!(
        !codes
            .iter()
            .any(|code| code.as_str() == "task.terminal.observed"),
        "review-required observation must not be recorded as a terminal task observation"
    );
    assert!(completed_agent_message_text(&observed).is_none());
}

#[tokio::test]
async fn computer_use_text_only_model_does_not_gate_computer_use() {
    let provider = Arc::new(TextOnlyAgentProvider::with_preflight_response(
        preflight_response_with_visible_tools(&["computer_use"]),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "text-only",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "thr_computer_use_text_only_no_gate",
        "ws_computer_use_text_only_no_gate",
        "turn_computer_use_text_only_no_gate",
        ThreadMode::Agent,
        "text-only",
        "Open the requested desktop location.",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request_tool_names = requests[0]
        .tools
        .as_ref()
        .expect("main request should still include available provider tools")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    let probe_tool_loop_config = test_tool_loop_config();
    let computer_use_available = pioneer_tools::build_builtin_tools(
        ".",
        "turn_computer_use_text_only_no_gate_probe",
        probe_tool_loop_config.web,
        probe_tool_loop_config.computer_use,
    )
    .router
    .has_handler("computer_use");
    if computer_use_available {
        assert!(
            request_tool_names.contains(&"computer_use"),
            "computer_use must not be provider-gated by image capability"
        );
    } else {
        assert!(
            !request_tool_names.contains(&"computer_use"),
            "unregistered computer_use must stay hidden"
        );
    }
}

#[tokio::test]
async fn computer_use_registered_tool_exposes_when_preflight_selects_it() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        preflight_response_with_visible_tools(&["computer_use"]),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());

    let observed = start_simple_turn(
        &manager,
        "thr_computer_use_registered_tool",
        "ws_computer_use_registered_tool",
        "turn_computer_use_registered_tool",
        ThreadMode::Agent,
        "capture",
        "Open the requested desktop location.",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request_tool_names = requests[0]
        .tools
        .as_ref()
        .expect("main request should include provider tools")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    let probe_tool_loop_config = test_tool_loop_config();
    let computer_use_available = pioneer_tools::build_builtin_tools(
        ".",
        "turn_computer_use_registered_tool_probe",
        probe_tool_loop_config.web,
        probe_tool_loop_config.computer_use,
    )
    .router
    .has_handler("computer_use");
    if computer_use_available {
        assert!(
            request_tool_names.contains(&"computer_use"),
            "image-capable providers may expose computer_use when preflight selects it"
        );
    } else {
        assert!(
            !request_tool_names.contains(&"computer_use"),
            "unregistered computer_use must stay hidden even for image-capable providers"
        );
    }
}

#[tokio::test]
async fn phase_10_prompt_compile_receives_preflight_selected_memory_tools() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let preflight_response = json!({
        "tools": {
            "visibleTools": ["memory_search", "memory_get"]
        }
    });
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        preflight_response.to_string(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_phase10_visible_name",
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
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_visible_tools_memory",
        "ws_phase10_visible_tools_memory",
        "turn_phase10_visible_tools_memory",
        ThreadMode::Agent,
        "capture",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    let HookInputPayload::TurnPrePromptCompile(payload) = &pre_prompt_compile.payload else {
        panic!("pre-prompt-compile call should receive typed payload");
    };
    let hook_tool_names = payload
        .available_tool_names
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect::<Vec<_>>();
    assert!(hook_tool_names.contains(&"memory_search".to_owned()));
    assert!(hook_tool_names.contains(&"memory_get".to_owned()));
    assert!(!hook_tool_names.contains(&"memory_remember".to_owned()));
    assert!(!hook_tool_names.contains(&"memory_forget".to_owned()));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let request_tool_names = requests[0]
        .tools
        .as_ref()
        .expect("main request should include provider tools")
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    assert!(request_tool_names.contains(&"memory_search".to_owned()));
    assert!(request_tool_names.contains(&"memory_get".to_owned()));
    assert!(!request_tool_names.contains(&"memory_remember".to_owned()));
    assert!(!request_tool_names.contains(&"memory_forget".to_owned()));

    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("main request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("Available memory tools: memory_search, memory_get.")
    );
}

#[tokio::test]
async fn phase_10_hook_prompt_section_ordering_is_deterministic() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![
                prompt_section_contribution("test.phase10.low", "Low Hook", "test", 0, "low hook"),
                prompt_section_contribution(
                    "test.phase10.same_b",
                    "Same B Hook",
                    "test.b",
                    5,
                    "same b hook",
                ),
                prompt_section_contribution(
                    "test.phase10.high",
                    "High Hook",
                    "test",
                    10,
                    "high hook",
                ),
                prompt_section_contribution(
                    "test.phase10.same_a",
                    "Same A Hook",
                    "test.a",
                    5,
                    "same a hook",
                ),
            ],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_prompt_order",
        "ws_phase10_prompt_order",
        "turn_phase10_prompt_order",
        ThreadMode::Agent,
        "capture",
        "phase 10 prompt section order",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    let high = prompt
        .full_system_text
        .find("high hook")
        .expect("high hook appears");
    let same_a = prompt
        .full_system_text
        .find("same a hook")
        .expect("same a hook appears");
    let same_b = prompt
        .full_system_text
        .find("same b hook")
        .expect("same b hook appears");
    let low = prompt
        .full_system_text
        .find("low hook")
        .expect("low hook appears");
    assert!(high < same_a);
    assert!(same_a < same_b);
    assert!(same_b < low);
}

#[tokio::test]
async fn phase_10_hook_prompt_section_id_appears_in_prompt_manifest() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_section_contribution(
                "test.phase10.manifest",
                "Manifest Hook Section",
                "test",
                10,
                "manifest hook section content",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_manifest_id",
        "ws_phase10_manifest_id",
        "turn_phase10_manifest_id",
        ThreadMode::Agent,
        "capture",
        "phase 10 manifest id",
    )
    .await;
    assert_turn_completed(&observed);

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
            .any(|section_id| section_id == "test.phase10.manifest")
    );
}

#[tokio::test]
async fn phase_10_hook_prompt_section_truncation_is_recorded_in_prompt_manifest() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_section_contribution_with_max_chars(
                "test.phase10.truncated",
                "Truncated Hook Section",
                "test",
                10,
                "0123456789abcdef",
                8,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_manifest_truncation",
        "ws_phase10_manifest_truncation",
        "turn_phase10_manifest_truncation",
        ThreadMode::Agent,
        "capture",
        "phase 10 manifest truncation",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("01234567"));
    assert!(!prompt.full_system_text.contains("89abcdef"));

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    assert!(manifest.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == PromptManifestDiagnosticCode::DynamicSectionTruncated
            && diagnostic.section_id.as_deref() == Some("test.phase10.truncated")
            && !diagnostic.message.contains("0123456789abcdef")
    }));
}

#[tokio::test]
async fn phase_11_hook_prompt_section_is_manifest_observable() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_section_contribution(
                "test.phase11.manifest_source",
                "Manifest Source Hook",
                "test",
                10,
                "phase 11 manifest source content",
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_manifest_source",
        "ws_phase11_manifest_source",
        "turn_phase11_manifest_source",
        ThreadMode::Agent,
        "capture",
        "phase 11 manifest source",
    )
    .await;
    assert_turn_completed(&observed);

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let source = manifest
        .hook_sources
        .iter()
        .find(|source| source.section_id.as_deref() == Some("test.phase11.manifest_source"))
        .expect("hook source should be recorded for hook prompt section");

    assert_eq!(
        source.contribution_kind,
        PromptManifestHookContributionKind::PromptSection
    );
    assert_eq!(source.truncation, PromptManifestHookTruncation::None);
    assert_eq!(source.source.hook_id, "test.phase07_recorder");
    assert_eq!(
        source.source.subscription_id,
        "test.phase07.pre_prompt_compile"
    );
    assert_eq!(
        source.source.phase,
        PromptManifestHookPhase::TurnPrePromptCompile
    );
    assert!(
        source
            .source
            .contribution_hash
            .as_deref()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
    assert_eq!(
        source.source.contribution_id.as_deref(),
        Some("test.phase11.manifest_source")
    );
    assert_eq!(source.priority, Some(10));
    assert_eq!(source.source_count, Some(0));
}

#[tokio::test]
async fn phase_11_failed_best_effort_hook_diagnostic_is_manifest_observable() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_phase_failures(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            Vec::new(),
            HookFailurePolicy::BestEffort,
            vec![HookPhase::TurnPrePromptCompile],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_best_effort_failed",
        "ws_phase11_best_effort_failed",
        "turn_phase11_best_effort_failed",
        ThreadMode::Agent,
        "capture",
        "phase 11 best effort failed",
    )
    .await;
    assert_turn_completed(&observed);

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let diagnostic = manifest
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == PromptManifestDiagnosticCode::HookBestEffortFailed)
        .expect("best-effort hook failure should be recorded in prompt manifest");
    let hook_source = diagnostic
        .hook_source
        .as_ref()
        .expect("best-effort failure diagnostic should include hook source");

    assert_eq!(hook_source.hook_id, "test.phase07_recorder");
    assert_eq!(
        hook_source.subscription_id,
        "test.phase07.pre_prompt_compile"
    );
    assert_eq!(
        hook_source.phase,
        PromptManifestHookPhase::TurnPrePromptCompile
    );
    assert_eq!(diagnostic.message, "diagnostic redacted");
}

#[tokio::test]
async fn phase_11_sensitive_diagnostic_content_is_redacted() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_manifest_diagnostic_contribution(
                "test.phase11_sensitive",
                "sk-test-secret-123 password=hunter2 Authorization: Bearer token",
                false,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_sensitive",
        "ws_phase11_sensitive",
        "turn_phase11_sensitive",
        ThreadMode::Agent,
        "capture",
        "phase 11 sensitive diagnostic",
    )
    .await;
    assert_turn_completed(&observed);

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let serialized = serde_json::to_string(manifest).expect("manifest should serialize");

    assert!(manifest.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == PromptManifestDiagnosticCode::HookDiagnostic
            && diagnostic.message == "Hook diagnostic redacted."
            && diagnostic.hook_source.is_some()
    }));
    for secret in [
        "sk-test-secret-123",
        "password=hunter2",
        "Authorization: Bearer token",
    ] {
        assert!(
            !serialized.contains(secret),
            "manifest must not contain sensitive diagnostic fragment `{secret}`"
        );
    }
}

#[tokio::test]
async fn phase_11_hook_prompt_section_truncation_status_is_manifest_observable() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            vec![prompt_section_contribution_with_max_chars(
                "test.phase11.truncated_source",
                "Truncated Source Hook",
                "test",
                10,
                "0123456789abcdef",
                8,
            )],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase11_truncated_source",
        "ws_phase11_truncated_source",
        "turn_phase11_truncated_source",
        ThreadMode::Agent,
        "capture",
        "phase 11 truncated source",
    )
    .await;
    assert_turn_completed(&observed);

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let source = manifest
        .hook_sources
        .iter()
        .find(|source| source.section_id.as_deref() == Some("test.phase11.truncated_source"))
        .expect("hook source should be recorded for truncated section");

    assert_eq!(source.truncation, PromptManifestHookTruncation::Hook);
}

#[tokio::test]
async fn phase_10_pre_prompt_compile_receives_policy_and_prompt_context_sets() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime(
            calls.clone(),
            vec![
                policy_contribution("test", "prompt_section_allowed", HookValue::Bool(true), 10),
                prompt_context_contribution("test.context.phase10", "test", 10, "context one"),
                prompt_section_contribution(
                    "test.phase10.with_context",
                    "Context-Aware Hook",
                    "test",
                    10,
                    "context-aware hook section",
                ),
            ],
            HookFailurePolicy::BestEffort,
            false,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_context_sets",
        "ws_phase10_context_sets",
        "turn_phase10_context_sets",
        ThreadMode::Agent,
        "capture",
        "phase 10 context sets",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_compile = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptCompile)
        .expect("pre-prompt-compile hook called");
    assert_eq!(
        policy_value(
            &pre_prompt_compile.policy_set,
            "test",
            "prompt_section_allowed"
        ),
        Some(HookValue::Bool(true))
    );
    assert!(
        pre_prompt_compile
            .prompt_context_set
            .entries
            .iter()
            .any(|entry| entry.contribution_id.as_str() == "test.context.phase10")
    );

    let requests = provider.snapshot_requests();
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("context-aware hook section")
    );
}

#[tokio::test]
async fn phase_10_best_effort_hook_failure_continues_without_section() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_phase_failures(
            calls,
            Vec::new(),
            HookFailurePolicy::BestEffort,
            vec![HookPhase::TurnPrePromptCompile],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_best_effort_failure",
        "ws_phase10_best_effort_failure",
        "turn_phase10_best_effort_failure",
        ThreadMode::Agent,
        "capture",
        "phase 10 best effort failure",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("phase 07 hook failed"));
}

#[tokio::test]
async fn phase_10_fallback_prompt_section_is_rendered() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(
            recording_hook_runtime_with_phase_failures_and_fallback(
                Arc::new(std::sync::Mutex::new(Vec::new())),
                Vec::new(),
                HookFailurePolicy::Fallback,
                vec![HookPhase::TurnPrePromptCompile],
                vec![prompt_section_contribution(
                    "test.phase10.fallback",
                    "Fallback Hook Section",
                    "test",
                    10,
                    "fallback hook section content",
                )],
            ),
        ))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_fallback",
        "ws_phase10_fallback",
        "turn_phase10_fallback",
        ThreadMode::Agent,
        "capture",
        "phase 10 fallback",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Fallback Hook Section"));
    assert!(
        prompt
            .full_system_text
            .contains("fallback hook section content")
    );
}

#[tokio::test]
async fn phase_10_required_prompt_section_failure_fails_turn() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_with_phase_failures(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            Vec::new(),
            HookFailurePolicy::Required,
            vec![HookPhase::TurnPrePromptCompile],
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase10_required_failure",
        "ws_phase10_required_failure",
        "turn_phase10_required_failure",
        ThreadMode::Agent,
        "capture",
        "phase 10 required failure",
    )
    .await;

    assert_turn_failed(&observed, "turn prompt section hook failed");
    assert!(provider.snapshot_requests().is_empty());
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
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    install_configured_memory_hooks_for_test(&manager).await;

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
    install_configured_memory_hooks_for_test(&manager).await;

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
async fn memory_provider_receives_task_runtime_context_for_task_turn() {
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
    install_configured_memory_hooks_for_test(&manager).await;
    manager
        .ensure_thread("thr_memory_task_context", "ws_memory_task_context")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, "thr_memory_task_context").await;

    manager
        .start_turn_with_hook_context(
            "thr_memory_task_context",
            "turn_memory_task_context",
            ThreadMode::Agent,
            AgentTurnHookRuntimeContext::task("task-runtime-memory-context"),
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "TASK RUN EXECUTION\nRUN OBJECTIVE\nSummarize the scheduled work.".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            Vec::new(),
        )
        .await
        .expect("task runtime turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let recall_contexts = memory_provider.recall_contexts();
    assert!(!recall_contexts.is_empty());
    assert!(
        recall_contexts
            .iter()
            .all(|context| { context.task_id.as_deref() == Some("task-runtime-memory-context") })
    );

    let tool_contexts = memory_provider.tool_contexts();
    assert!(!tool_contexts.is_empty());
    assert!(
        tool_contexts
            .iter()
            .all(|context| { context.task_id.as_deref() == Some("task-runtime-memory-context") })
    );
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
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    assert!(tools.iter().any(|tool| tool.name == "memory_list"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_remember"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(prompt.full_system_text.contains(
        "Available memory tools: memory_search, memory_list, memory_get, memory_remember, memory_forget."
    ));
    assert!(
        !prompt
            .full_system_text
            .contains("test memory tool diagnostic")
    );
}

#[tokio::test]
async fn pre_tool_materialization_hook_receives_local_non_memory_tool_bundle_names() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPreToolMaterialization,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_pre_tool_bundle_names",
        "ws_pre_tool_bundle_names",
        "turn_pre_tool_bundle_names",
        ThreadMode::Agent,
        "capture",
        "pre-tool should see memory tools",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = wait_for_hook_calls(&calls, 1).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].phase, HookPhase::TurnPreToolMaterialization);
    let HookInputPayload::TurnPreToolMaterialization(payload) = &calls[0].payload else {
        panic!("pre-tool hook should receive typed tool materialization input");
    };
    let tool_names = payload
        .existing_tool_names
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>();
    assert!(
        tool_names.contains(&"read_skill"),
        "pre-tool materialization input should include local skill tools, got {tool_names:?}"
    );
    assert!(
        !tool_names.iter().any(|name| name.starts_with("memory_")),
        "memory tools should be contributed by memory hook, not pre-existing local input: {tool_names:?}"
    );

    let request = provider
        .snapshot_requests()
        .into_iter()
        .next()
        .expect("provider request");
    let tools = request.tools.expect("agent request should include tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
}

#[tokio::test]
async fn phase_13_memory_tool_bundle_visibility_is_driven_by_hook_policy_set() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
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
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::no_save(),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase13_tool_policy",
        "ws_phase13_tool_policy",
        "turn_phase13_tool_policy",
        ThreadMode::Agent,
        "capture",
        "Speichere das nicht, aber nutze vorhandene Erinnerung.",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_policy_index = calls
        .iter()
        .position(|call| call.phase == HookPhase::TurnPrePolicy)
        .expect("pre-policy hook call exists");
    let pre_tool_index = calls
        .iter()
        .position(|call| call.phase == HookPhase::TurnPreToolMaterialization)
        .expect("pre-tool hook call exists");
    assert!(
        pre_policy_index < pre_tool_index,
        "policy must run before tool materialization"
    );
    let policy = memory_turn_policy_from_hook_policy_set(&calls[pre_tool_index].policy_set)
        .expect("memory policy exists at tool materialization")
        .expect("memory policy decodes at tool materialization");
    assert_eq!(
        policy.reason_code,
        super::MemoryPolicyReasonCode::MemoryNoSave
    );
    assert_eq!(
        policy.remember_tool,
        super::MemoryMutationToolPolicy::Disabled
    );

    assert_eq!(memory_provider.tool_contexts().len(), 1);
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("agent request should include tools");
    assert!(tools.iter().any(|tool| tool.name == "memory_search"));
    assert!(tools.iter().any(|tool| tool.name == "memory_get"));
    assert!(tools.iter().any(|tool| tool.name == "memory_forget"));
    assert!(!tools.iter().any(|tool| tool.name == "memory_remember"));
}

#[tokio::test]
async fn phase_13_without_memory_hook_subscription_exposes_no_memory_tools() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_hook_runtime(Some(recording_hook_runtime_for_phase(
            calls.clone(),
            HookPhase::TurnPreToolMaterialization,
            HookAwaitPolicy::Blocking,
            HookFailurePolicy::BestEffort,
        )))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase13_no_memory_hook",
        "ws_phase13_no_memory_hook",
        "turn_phase13_no_memory_hook",
        ThreadMode::Agent,
        "capture",
        "no memory provider is installed",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = wait_for_hook_calls(&calls, 1).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].phase, HookPhase::TurnPreToolMaterialization);
    assert!(
        memory_turn_policy_from_hook_policy_set(&calls[0].policy_set).is_none(),
        "memory policy should not exist without memory hooks"
    );
    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let tools = requests[0]
        .tools
        .as_ref()
        .expect("agent request should include non-memory built-in tools");
    assert!(!tools.iter().any(|tool| tool.name.starts_with("memory_")));
}

#[tokio::test]
async fn identity_recall_prompt_contains_relevant_memory() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    assert!(!prompt.full_system_text.contains("mem_identity_name"));
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
async fn phase_14_memory_recall_and_prompt_contract_are_manifest_observable() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    let memory_provider = Arc::new(RecordingMemoryProvider::new(
        Ok(MemoryRecallSnapshot {
            items: vec![memory_recall_item(
                "mem_phase14_identity",
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
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase14_memory_manifest",
        "ws_phase14_memory_manifest",
        "turn_phase14_memory_manifest",
        ThreadMode::Agent,
        "capture",
        "what is my name?",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.recall_contexts().len(), 1);
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");

    let prompt_contract_source = manifest
        .hook_sources
        .iter()
        .find(|source| source.section_id.as_deref() == Some("memory_recall"))
        .expect("memory prompt contract hook source should be recorded");
    assert_eq!(
        prompt_contract_source.contribution_kind,
        PromptManifestHookContributionKind::PromptSection
    );
    assert_eq!(
        prompt_contract_source.source.phase,
        PromptManifestHookPhase::TurnPrePromptCompile
    );
    assert_eq!(
        prompt_contract_source.source.hook_id,
        "memory.prompt_contract"
    );
    assert_eq!(
        prompt_contract_source.source.subscription_id,
        "memory.prompt_contract.default"
    );
    assert_eq!(
        prompt_contract_source.source.contribution_id.as_deref(),
        Some("memory.prompt_contract.section")
    );

    let deterministic_recall_source = manifest
        .hook_sources
        .iter()
        .find(|source| {
            source.section_id.is_none()
                && source.contribution_kind == PromptManifestHookContributionKind::PromptContext
                && source.source.hook_id == "memory.deterministic_recall"
        })
        .expect("deterministic recall prompt-context hook source should be recorded");
    assert_eq!(
        deterministic_recall_source.source.phase,
        PromptManifestHookPhase::TurnPrePromptContext
    );
    assert_eq!(
        deterministic_recall_source.source.subscription_id,
        "memory.deterministic_recall.default"
    );
    assert_eq!(
        deterministic_recall_source
            .source
            .contribution_id
            .as_deref(),
        Some("memory.deterministic_recall.context")
    );
    assert_eq!(deterministic_recall_source.source_count, Some(1));
}

#[tokio::test]
async fn phase_15_active_memory_recall_contributes_prompt_context_and_manifest() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let mut config = test_tool_loop_config();
    config.memory.active_recall.mode = super::MemoryActiveRecallMode::StrictDebug;
    config.memory.active_recall.max_queries = 1;
    config
        .memory
        .active_recall
        .deterministic_sufficient_min_items = 99;
    let manager = AgentManager::new(registry, config);
    let memory_provider = Arc::new(RecordingMemoryProvider::with_recall_sequence(
        vec![
            Ok(MemoryRecallSnapshot::empty()),
            Ok(MemoryRecallSnapshot {
                items: vec![memory_recall_item(
                    "mem_phase15_active",
                    MemoryCategory::ProjectDecision,
                    Some("hook_runtime"),
                    "Use the hook runtime for proactive memory recall.",
                )],
                diagnostics: Vec::new(),
                truncated: false,
            }),
        ],
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase15_active_memory",
        "ws_phase15_active_memory",
        "turn_phase15_active_memory",
        ThreadMode::Agent,
        "capture",
        "continue the previous architecture implementation with the same memory constraints",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.recall_contexts().len(), 2);
    assert_eq!(memory_provider.tool_contexts().len(), 1);
    assert!(memory_provider.mode_recall_requests().is_empty());
    let recall_requests = memory_provider.recall_requests();
    assert_eq!(recall_requests.len(), 2);
    assert_eq!(recall_requests[1].top_k, Some(5));
    assert_eq!(recall_requests[1].max_chars, Some(1_500));

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(prompt.full_system_text.contains("## Memory Recall"));
    assert!(
        prompt
            .full_system_text
            .contains("Additional active memory context for this turn:")
    );
    assert!(
        prompt
            .full_system_text
            .contains("Use the hook runtime for proactive memory recall.")
    );

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    let active_recall_source = manifest
        .hook_sources
        .iter()
        .find(|source| {
            source.section_id.is_none()
                && source.contribution_kind == PromptManifestHookContributionKind::PromptContext
                && source.source.hook_id == "memory.active_recall"
        })
        .expect("active recall prompt-context hook source should be recorded");
    assert_eq!(
        active_recall_source.source.phase,
        PromptManifestHookPhase::TurnPostPreflightPromptContext
    );
    assert_eq!(
        active_recall_source.source.subscription_id,
        "memory.active_recall.default"
    );
    assert_eq!(
        active_recall_source.source.contribution_id.as_deref(),
        Some("memory.active_recall.context")
    );
    assert_eq!(active_recall_source.source_count, Some(1));
}

#[tokio::test]
async fn memory_identity_flow_preflight_provider_owned_active_recall_contributes_prompt_context() {
    let preflight_response = json!({
        "tools": {
            "visibleTools": ["memory_search", "memory_get"]
        },
        "memory": {
            "activeRecall": {
                "status": "run",
                "reasonCode": "provider_run",
                "confidence": 0.92,
                "modes": ["profile"],
                "targets": [{
                    "scopeKind": "user",
                    "factClass": "user_identity",
                    "category": "identity",
                    "subject": "current_user",
                    "attribute": "name"
                }],
                "diagnostics": ["preflight provider ok"]
            }
        }
    });
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        preflight_response.to_string(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let mut config = test_tool_loop_config();
    config.memory.active_recall.mode = super::MemoryActiveRecallMode::Hybrid;
    config
        .memory
        .active_recall
        .deterministic_sufficient_min_items = 99;
    config.memory.active_recall.max_queries = 1;
    let manager = AgentManager::new(registry, config);
    let memory_provider = Arc::new(RecordingMemoryProvider::with_recall_sequence(
        vec![
            Ok(MemoryRecallSnapshot::empty()),
            Ok(MemoryRecallSnapshot {
                items: vec![memory_recall_item(
                    "mem_preflight_memory_name",
                    MemoryCategory::Identity,
                    Some("name"),
                    "User's name is Alexander.",
                )],
                diagnostics: Vec::new(),
                truncated: false,
            }),
        ],
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_preflight_memory_provider_owned",
        "ws_preflight_memory_provider_owned",
        "turn_preflight_memory_provider_owned",
        ThreadMode::Agent,
        "capture",
        "как меня зовут?",
    )
    .await;
    assert_turn_completed(&observed);

    let mode_requests = memory_provider.mode_recall_requests();
    assert_eq!(mode_requests.len(), 1);
    assert_eq!(mode_requests[0].mode, MemoryRecallMode::Profile);
    assert!(!memory_provider.recall_contexts().is_empty());

    let requests = provider.snapshot_all_requests();
    assert_eq!(requests.len(), 2);
    assert!(is_turn_preflight_request(&requests[0]));
    assert!(requests[0].compiled_prompt.is_none());
    assert!(requests[0].tools.is_none());
    assert!(requests[1].compiled_prompt.is_some());
    let tool_names = requests[1]
        .tools
        .as_ref()
        .expect("main identity request should include final visible tools")
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    for core_tool in pioneer_tools::PREFLIGHT_CORE_TOOL_NAMES {
        assert!(
            tool_names.contains(core_tool),
            "core tool `{core_tool}` should stay visible in identity flow"
        );
    }
    assert!(tool_names.contains(&"memory_search"));
    assert!(tool_names.contains(&"memory_get"));
    assert!(!tool_names.contains(&"memory_list"));
    assert!(!tool_names.contains(&"memory_remember"));
    assert!(!tool_names.contains(&"memory_forget"));
    assert!(!tool_names.contains(&"task_create"));
    assert!(!tool_names.contains(&"artifact_prepare"));
    let prompt = requests[1]
        .compiled_prompt
        .as_ref()
        .expect("main request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("Additional active memory context for this turn:")
    );
    assert!(
        prompt
            .full_system_text
            .contains("User's name is Alexander.")
    );
}

#[tokio::test]
async fn preflight_memory_active_recall_disabled_suppresses_provider_plan() {
    let preflight_response = json!({
        "tools": {
            "visibleTools": []
        },
        "memory": {
            "activeRecall": {
                "status": "run",
                "reasonCode": "provider_run",
                "confidence": 0.92,
                "modes": ["profile"],
                "targets": [{
                    "scopeKind": "user",
                    "factClass": "user_identity",
                    "category": "identity",
                    "subject": "current_user",
                    "attribute": "name"
                }],
                "diagnostics": ["must be ignored"]
            }
        }
    });
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        preflight_response.to_string(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let mut config = test_tool_loop_config();
    config.memory.active_recall.mode = super::MemoryActiveRecallMode::Disabled;
    let manager = AgentManager::new(registry, config);
    let memory_provider = Arc::new(RecordingMemoryProvider::with_recall_sequence(
        vec![Ok(MemoryRecallSnapshot::empty())],
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_preflight_memory_disabled",
        "ws_preflight_memory_disabled",
        "turn_preflight_memory_disabled",
        ThreadMode::Agent,
        "capture",
        "как меня зовут?",
    )
    .await;
    assert_turn_completed(&observed);

    assert_eq!(memory_provider.recall_contexts().len(), 1);
    assert!(memory_provider.mode_recall_requests().is_empty());
    let requests = provider.snapshot_all_requests();
    assert_eq!(requests.len(), 2);
    let prompt = requests[1]
        .compiled_prompt
        .as_ref()
        .expect("main request should include compiled prompt");
    assert!(
        !prompt
            .full_system_text
            .contains("Additional active memory context for this turn:")
    );
    assert!(!prompt.full_system_text.contains("must be ignored"));
}

#[tokio::test]
async fn phase_16_active_memory_duplicate_suppression_is_manifest_observable() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let mut config = test_tool_loop_config();
    config.memory.active_recall.mode = super::MemoryActiveRecallMode::StrictDebug;
    config.memory.active_recall.max_queries = 1;
    config
        .memory
        .active_recall
        .deterministic_sufficient_min_items = 99;
    let manager = AgentManager::new(registry, config);
    let duplicate_memory = memory_recall_item(
        "mem_phase16_duplicate",
        MemoryCategory::Identity,
        Some("name"),
        "User's name is Alexander.",
    );
    let memory_provider = Arc::new(RecordingMemoryProvider::with_recall_sequence(
        vec![
            Ok(MemoryRecallSnapshot {
                items: vec![duplicate_memory.clone()],
                diagnostics: Vec::new(),
                truncated: false,
            }),
            Ok(MemoryRecallSnapshot {
                items: vec![duplicate_memory],
                diagnostics: Vec::new(),
                truncated: false,
            }),
        ],
        Ok(MemoryToolMaterialization {
            bundles: vec![fake_standard_memory_tool_bundle()],
            diagnostics: Vec::new(),
        }),
    ));
    let memory_trait_provider: Arc<dyn AgentMemoryProvider> = memory_provider.clone();
    manager
        .set_memory_provider(Some(memory_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase16_active_memory_dup",
        "ws_phase16_active_memory_dup",
        "turn_phase16_active_memory_dup",
        ThreadMode::Agent,
        "capture",
        "continue the previous architecture implementation with the same memory constraints",
    )
    .await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(
        prompt
            .full_system_text
            .contains("Relevant memory context for this turn:")
    );
    assert_eq!(
        prompt
            .full_system_text
            .matches("User's name is Alexander.")
            .count(),
        1
    );
    assert!(
        !prompt
            .full_system_text
            .contains("Additional active memory context for this turn:")
    );

    let manifest = observed
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .next()
        .expect("prompt manifest should be emitted");
    assert!(manifest.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == PromptManifestDiagnosticCode::HookDiagnostic
            && diagnostic.message.contains("memory active recall dedup")
            && diagnostic.message.contains("active_duplicate_count=1")
            && diagnostic.message.contains("active_rendered_count=0")
            && diagnostic.message.contains("duplicate_only=true")
            && diagnostic
                .hook_source
                .as_ref()
                .is_some_and(|source| source.hook_id == "memory.active_recall")
    }));
}

#[tokio::test]
async fn memory_provider_without_tools_recalls_but_omits_prompt_contract() {
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    assert_eq!(memory_provider.recall_contexts().len(), 1);
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
        MemoryExtractionPolicy::Allow
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
async fn phase_12_memory_policy_classifier_contributes_full_hook_policy() {
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
    let policy_provider = Arc::new(FakeMemoryTurnPolicyProvider::new(
        MemoryTurnPolicy::no_save().with_detected_language(Some("de".to_owned())),
    ));
    let policy_trait_provider: Arc<dyn AgentMemoryTurnPolicyProvider> = policy_provider.clone();
    manager
        .set_memory_turn_policy_provider(Some(policy_trait_provider))
        .await;
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_phase12_memory_policy_payload",
        "ws_phase12_memory_policy_payload",
        "turn_phase12_memory_policy_payload",
        ThreadMode::Agent,
        "capture",
        "Speichere das nicht.",
    )
    .await;
    assert_turn_completed(&observed);

    let calls = snapshot_hook_calls(&calls);
    let pre_prompt_context = calls
        .iter()
        .find(|call| call.phase == HookPhase::TurnPrePromptContext)
        .expect("pre-prompt-context hook called");
    let policy = memory_turn_policy_from_hook_policy_set(&pre_prompt_context.policy_set)
        .expect("memory policy exists")
        .expect("memory policy decodes from hook policy set");

    assert_eq!(
        policy.reason_code,
        super::MemoryPolicyReasonCode::MemoryNoSave
    );
    assert_eq!(policy.detected_language.as_deref(), Some("de"));
    assert_eq!(
        policy.remember_tool,
        super::MemoryMutationToolPolicy::Disabled
    );
    assert_eq!(policy.read_tools, super::MemoryReadToolPolicy::Allow);
}

#[tokio::test]
async fn memory_policy_invalid_classifier_json_uses_default_allow_fallback() {
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_all_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    let provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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
    install_configured_memory_hooks_for_test(&manager).await;

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
            .contains("If unsure whether injected memory is enough for a non-trivial task")
    );
    assert!(!prompt.full_system_text.contains("Relevant memories:"));
}

#[tokio::test]
async fn memory_policy_default_allows_proactive_remember_tool() {
    let (memory_bundle, calls) = recording_standard_memory_tool_bundle();
    let provider = Arc::new(SequencedToolProvider::with_preflight_response(
        vec![ProviderToolCall {
            id: "call_memory_remember_proactive".to_owned(),
            name: "memory_remember".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "remembered",
        memory_remember_preflight_response(),
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    let provider = Arc::new(SequencedToolProvider::with_preflight_response(
        vec![ProviderToolCall {
            id: "call_memory_remember".to_owned(),
            name: "memory_remember".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "remembered",
        memory_remember_preflight_response(),
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
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_memory_remember_tool",
        "ws_memory_remember_tool",
        "turn_memory_remember_tool",
        ThreadMode::Agent,
        "sequenced-tools",
        concat!("remember ", "that my name is Alexander"),
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
    let provider = Arc::new(SequencedToolProvider::with_preflight_response(
        vec![ProviderToolCall {
            id: "call_memory_forget".to_owned(),
            name: "memory_forget".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "forgotten",
        memory_forget_preflight_response(),
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
    install_configured_memory_hooks_for_test(&manager).await;

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
    let remember_provider = Arc::new(SequencedToolProvider::with_preflight_response(
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
        memory_remember_preflight_response(),
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
    install_configured_memory_hooks_for_test(&manager).await;

    let observed = start_simple_turn(
        &manager,
        "thr_stateful_memory_remember",
        "ws_stateful_memory",
        "turn_stateful_memory_remember",
        ThreadMode::Agent,
        "sequenced-remember",
        concat!("remember ", "that my name is Alexander"),
    )
    .await;
    assert_turn_completed(&observed);

    let recall_provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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

    let forget_provider = Arc::new(SequencedToolProvider::with_preflight_response(
        vec![ProviderToolCall {
            id: "call_stateful_memory_forget".to_owned(),
            name: "memory_forget".to_owned(),
            arguments: "{}".to_owned(),
        }],
        "forgotten",
        memory_forget_preflight_response(),
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

    let after_forget_provider = Arc::new(CaptureAgentProvider::with_preflight_response(
        memory_read_preflight_response(),
    ));
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
        .start_turn_with_capabilities(
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
            Vec::new(),
        )
        .await
        .expect("turn should start");
    events
}

#[tokio::test]
async fn tool_loop_provider_round_budget_requests_continuation_without_final_no_tools_round() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_rounds",
        "turn_loop_budget_rounds",
    )
    .await;

    let observed_events = recv_events_until_loop_budget_action(
        &mut events,
        ToolLoopBudgetLimitKind::AgentRounds,
        ToolLoopBudgetAction::ContinueInNextWindow,
    )
    .await;
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnToolLoopBudgetExceeded(notification)
            if notification.limit_kind == ToolLoopBudgetLimitKind::AgentRounds
                && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
                && notification.turn_id == "turn_loop_budget_rounds"
    )));
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "agent-round window exhaustion must not fail the turn"
    );

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "provider should receive the initial window requests before continuation"
    );
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
        requests.iter().all(|request| request.tools.is_some()),
        "window budget exhaustion must not send a fake final no-tools provider round"
    );
}

#[tokio::test]
async fn production_regression_repeated_failed_tools_continue_without_final_no_tools_failure() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_production_failed_tools_window_regression",
        "turn_production_failed_tools_window_regression",
    )
    .await;

    let observed_events = recv_events_until_loop_budget_action(
        &mut events,
        ToolLoopBudgetLimitKind::AgentRounds,
        ToolLoopBudgetAction::ContinueInNextWindow,
    )
    .await;

    let failed_tool_completions = observed_events
        .iter()
        .filter(|event| {
            matches!(
                event,
                AgentEvent::ItemCompleted(ItemCompletedNotification {
                    item: TurnItem::DynamicToolCall {
                        tool_name,
                        status,
                        ..
                    },
                    ..
                }) if tool_name == "missing_loop_budget_tool"
                    && matches!(status, ToolCallStatus::Failed)
            )
        })
        .count();
    assert!(
        failed_tool_completions >= 1,
        "fixture must simulate repeated failed tool calls before window exhaustion: {observed_events:?}"
    );
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnToolLoopBudgetExceeded(notification)
            if notification.limit_kind == ToolLoopBudgetLimitKind::AgentRounds
                && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
                && notification.turn_id == "turn_production_failed_tools_window_regression"
    )));
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnCompleted { .. })),
        "first window exhaustion must not produce an empty/completed final answer"
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "first window exhaustion must not fail the turn"
    );
    let observed_debug = format!("{observed_events:#?}");
    assert!(
        !observed_debug.contains("final_no_tools_round_already_used"),
        "production regression: final no-tools pressure must not reappear after window exhaustion"
    );

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "fixture should reach repeated provider rounds before continuation: {requests:?}"
    );
    assert!(
        requests.iter().all(|request| request.tools.is_some()),
        "budget continuation must not switch into final no-tools mode"
    );
}

#[tokio::test]
async fn execution_window_continuation_restarts_same_turn_and_completes_next_window() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::TwoToolRoundsThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_multi_window",
        "turn_loop_budget_multi_window",
    )
    .await;

    let observed_events = recv_events_until_terminal(&mut events).await;
    assert!(matches!(
        observed_events.last(),
        Some(AgentEvent::TurnCompleted { turn_id, .. })
            if turn_id == "turn_loop_budget_multi_window"
    ));
    let budget_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::TurnToolLoopBudgetExceeded(notification)
                    if notification.turn_id == "turn_loop_budget_multi_window"
                        && notification.limit_kind == ToolLoopBudgetLimitKind::AgentRounds
                        && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
            )
        })
        .expect("first window should exhaust and request continuation");
    let completed_index = observed_events
        .iter()
        .position(|event| matches!(event, AgentEvent::TurnCompleted { .. }))
        .expect("turn should complete after continuation");
    assert!(budget_index < completed_index);
    let observed_turn_ids = observed_events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::PromptManifestCompiled { turn_id, .. }
            | AgentEvent::TurnSkillsResolved { turn_id, .. }
            | AgentEvent::TurnCapabilitiesResolved { turn_id, .. }
            | AgentEvent::SkillAuditEvents { turn_id, .. }
            | AgentEvent::TurnLlmContextAppended { turn_id, .. }
            | AgentEvent::ProviderFailureDetected { turn_id, .. }
            | AgentEvent::RecoveryAttemptSucceeded { turn_id, .. }
            | AgentEvent::TurnCompleted { turn_id, .. }
            | AgentEvent::TurnFailed { turn_id, .. }
            | AgentEvent::TurnBlocked { turn_id, .. } => Some(turn_id.as_str()),
            AgentEvent::ItemStarted(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::ItemDelta(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::ItemCompleted(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::ItemToolRetryScheduled(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::ItemToolRetryResolved(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::ItemToolRetryExhausted(notification) => Some(notification.turn_id.as_str()),
            AgentEvent::TurnToolLoopBudgetExceeded(notification) => {
                Some(notification.turn_id.as_str())
            }
            AgentEvent::ItemHeartbeat { turn_id, .. } => Some(turn_id.as_str()),
        })
        .collect::<Vec<_>>();
    assert!(
        observed_turn_ids
            .iter()
            .all(|turn_id| *turn_id == "turn_loop_budget_multi_window"),
        "continuation must stay inside one user turn id: {observed_turn_ids:?}"
    );
    assert!(
        !observed_events[..budget_index].iter().any(|event| matches!(
            event,
            AgentEvent::TurnCompleted { .. } | AgentEvent::TurnFailed { .. }
        )),
        "window boundary must not be terminal"
    );
    assert!(
        !observed_events[..=budget_index]
            .iter()
            .any(|event| matches!(
                event,
                AgentEvent::ItemCompleted(ItemCompletedNotification {
                    item: TurnItem::AgentMessage { .. },
                    ..
                })
            )),
        "first exhausted window must not emit a final assistant message"
    );
    let final_agent_message_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::ItemCompleted(ItemCompletedNotification {
                    item: TurnItem::AgentMessage { text, .. },
                    ..
                }) if text == "final without tools"
            )
        })
        .expect("final assistant message should appear after continuation");
    assert!(budget_index < final_agent_message_index);
    assert!(final_agent_message_index < completed_index);
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "multi-window continuation should not fail the turn"
    );

    let requests = provider.snapshot_requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[2]
            .tools
            .as_ref()
            .map(|tools| !tools.is_empty())
            .unwrap_or(false),
        "continuation window should keep tools enabled"
    );
    assert_eq!(
        tool_result_message_count(&requests[2]),
        0,
        "same-turn continuation should not synthesize unanswered excess tool history"
    );
}

#[tokio::test]
async fn provider_recovery_success_boundary_survives_execution_window_continuation() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::TwoToolRoundsThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let thread_id = "thr_loop_budget_recovery_window";
    let turn_id = "turn_loop_budget_recovery_window";
    let recovery_job_id = "recovery_job_window_continuation";
    let recovery_attempt_id = "recovery_attempt_window_continuation";

    let mut events = start_loop_budget_turn(&manager, thread_id, turn_id).await;
    let initial_events = recv_events_until_terminal(&mut events).await;
    assert!(matches!(
        initial_events.last(),
        Some(AgentEvent::TurnCompleted { .. })
    ));

    provider.next_index.store(0, Ordering::SeqCst);
    provider
        .requests
        .lock()
        .expect("loop budget provider lock poisoned")
        .clear();
    while events.try_recv().is_ok() {}

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
                execution_checkpoint_context: None,
            },
        )
        .await
        .expect("recovery request should restart completed turn");

    let mut recovery_events = Vec::new();
    let mut saw_recovery_success = false;
    let mut saw_recovery_budget_continuation = false;
    for _ in 0..160 {
        let event = match timeout(Duration::from_secs(2), events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => panic!("agent event channel should stay open"),
            Err(_) => panic!("timed out waiting for recovery continuation terminal event"),
        };
        if matches!(
            event,
            AgentEvent::RecoveryAttemptSucceeded {
                ref recovery,
                ..
            } if recovery.job_id == recovery_job_id
                && recovery.attempt_id == recovery_attempt_id
        ) {
            saw_recovery_success = true;
        }
        if matches!(
            event,
            AgentEvent::TurnToolLoopBudgetExceeded(ref notification)
                if notification.limit_kind == ToolLoopBudgetLimitKind::AgentRounds
                    && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
        ) {
            assert!(
                saw_recovery_success,
                "provider recovery should close at the provider boundary before later window continuation"
            );
            saw_recovery_budget_continuation = true;
        }
        let terminal_after_recovery_continuation = saw_recovery_budget_continuation
            && matches!(
                event,
                AgentEvent::TurnCompleted { .. }
                    | AgentEvent::TurnFailed { .. }
                    | AgentEvent::TurnBlocked { .. }
            );
        recovery_events.push(event);
        if terminal_after_recovery_continuation {
            break;
        }
    }
    assert!(
        saw_recovery_budget_continuation,
        "recovery attempt should cross an execution-window boundary"
    );
    assert!(
        saw_recovery_success,
        "recovery success should be emitted before recovery continuation terminal event"
    );
    assert!(
        matches!(
            recovery_events.last(),
            Some(AgentEvent::TurnCompleted { recovery: None, .. })
        ),
        "completed turn should not carry recovery after the recovery job was already closed: {recovery_events:#?}"
    );
    assert!(
        !recovery_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "recovery continuation must not fail the turn"
    );
}

#[tokio::test]
async fn max_windows_cap_blocks_continuation_without_turn_failed() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.execution_windows.total.max_windows_per_turn = 1;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let thread_id = "thr_loop_budget_max_windows";
    let turn_id = "turn_loop_budget_max_windows";
    manager
        .ensure_thread(thread_id, "ws_loop_budget")
        .await
        .expect("thread should be created");
    let mut durable_events = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "loop-budget",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run max windows cap test".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed_events = recv_durable_events_until_turn_blocked(&mut durable_events).await;
    let blocked = observed_events
        .iter()
        .find_map(|event| {
            if let AgentDurableEvent::TurnExecutionWindowBlocked { notification } = event {
                Some(notification)
            } else {
                None
            }
        })
        .expect("blocked event should be observed");
    assert_eq!(blocked.thread_id, thread_id);
    assert_eq!(blocked.turn_id, turn_id);
    assert_eq!(blocked.window_index, 1);
    assert_eq!(blocked.total_windows, 1);
    assert_eq!(
        blocked.status,
        pioneer_protocol::ExecutionWindowStatus::Blocked
    );
    assert!(
        blocked
            .reason
            .contains("max execution windows per turn reached")
    );
    let checkpoint_id = blocked
        .checkpoint_id
        .as_deref()
        .expect("blocked event should reference checkpoint");
    assert!(
        observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowCheckpointed { notification, .. }
                if notification.checkpoint_id == checkpoint_id
        )),
        "max-window block should be preceded by checkpointed event"
    );
    assert!(
        !observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowContinued { .. }
        )),
        "max-window cap must not schedule another execution window"
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentDurableEvent::TurnFailed { .. })),
        "max-window cap should be controlled blocked state, not failed"
    );
}

#[tokio::test]
async fn total_tool_call_cap_blocks_continuation_without_turn_failed() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config.execution_windows.total.max_tool_calls_per_turn = 1;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let thread_id = "thr_loop_budget_total_tools";
    let turn_id = "turn_loop_budget_total_tools";
    manager
        .ensure_thread(thread_id, "ws_loop_budget")
        .await
        .expect("thread should be created");
    let mut durable_events = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "loop-budget",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run total tool-call cap test".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed_events = recv_durable_events_until_turn_blocked(&mut durable_events).await;
    let blocked = observed_events
        .iter()
        .find_map(|event| {
            if let AgentDurableEvent::TurnExecutionWindowBlocked { notification } = event {
                Some(notification)
            } else {
                None
            }
        })
        .expect("blocked event should be observed");
    assert_eq!(blocked.thread_id, thread_id);
    assert_eq!(blocked.turn_id, turn_id);
    assert!(blocked.reason.contains("max_total_tool_calls_per_turn"));
    let checkpoint_id = blocked
        .checkpoint_id
        .as_deref()
        .expect("blocked event should reference checkpoint");
    assert!(
        observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowCheckpointed { notification, .. }
                if notification.checkpoint_id == checkpoint_id
        )),
        "total tool-call block should be preceded by checkpointed event"
    );
    assert!(
        !observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowContinued { .. }
        )),
        "total tool-call cap must not schedule another execution window"
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentDurableEvent::TurnFailed { .. })),
        "total tool-call cap should be controlled blocked state, not failed"
    );
}

#[tokio::test]
async fn consecutive_failed_window_cap_blocks_continuation_without_turn_failed() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    config
        .execution_windows
        .total
        .max_consecutive_failed_windows = 1;
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let thread_id = "thr_loop_budget_failed_windows";
    let turn_id = "turn_loop_budget_failed_windows";
    manager
        .ensure_thread(thread_id, "ws_loop_budget")
        .await
        .expect("thread should be created");
    let mut durable_events = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "loop-budget",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run consecutive failed windows cap test".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed_events = recv_durable_events_until_turn_blocked(&mut durable_events).await;
    let blocked = observed_events
        .iter()
        .find_map(|event| {
            if let AgentDurableEvent::TurnExecutionWindowBlocked { notification } = event {
                Some(notification)
            } else {
                None
            }
        })
        .expect("blocked event should be observed");
    assert_eq!(blocked.thread_id, thread_id);
    assert_eq!(blocked.turn_id, turn_id);
    assert!(blocked.reason.contains("max_consecutive_failed_windows"));
    let checkpoint_id = blocked
        .checkpoint_id
        .as_deref()
        .expect("blocked event should reference checkpoint");
    assert!(
        observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowCheckpointed { notification, .. }
                if notification.checkpoint_id == checkpoint_id
        )),
        "consecutive failed-window block should be preceded by checkpointed event"
    );
    assert!(
        !observed_events.iter().any(|event| matches!(
            event,
            AgentDurableEvent::TurnExecutionWindowContinued { .. }
        )),
        "consecutive failed-window cap must not schedule another execution window"
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentDurableEvent::TurnFailed { .. })),
        "consecutive failed-window cap should be controlled blocked state, not failed"
    );
}

#[tokio::test]
async fn memory_recall_tools_remain_available_for_budget_continuation() {
    let provider = Arc::new(LoopBudgetProvider::with_preflight_response(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
        memory_read_preflight_response(),
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
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
    install_configured_memory_hooks_for_test(&manager).await;

    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_memory_policy",
        "turn_loop_budget_memory_policy",
    )
    .await;

    let _observed_events = recv_events_until_loop_budget_action(
        &mut events,
        ToolLoopBudgetLimitKind::AgentRounds,
        ToolLoopBudgetAction::ContinueInNextWindow,
    )
    .await;

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 2,
        "provider should receive initial window requests before continuation"
    );
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
    assert!(
        requests.iter().all(|request| request.tools.is_some()),
        "budget continuation must not compile a final no-tools prompt"
    );
}

#[tokio::test]
async fn provider_tools_after_tools_disabled_requests_continuation_without_turn_failed() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::AlwaysTools,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 8, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 1;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_disabled_tools",
        "turn_loop_budget_disabled_tools",
    )
    .await;

    let observed_events = recv_events_until_loop_budget_action(
        &mut events,
        ToolLoopBudgetLimitKind::ProviderReturnedToolsAfterToolsDisabled,
        ToolLoopBudgetAction::ContinueInNextWindow,
    )
    .await;
    let budget_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::TurnToolLoopBudgetExceeded(notification)
                    if notification.limit_kind
                        == ToolLoopBudgetLimitKind::ProviderReturnedToolsAfterToolsDisabled
                        && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
            )
        })
        .expect("provider tool-call leak should request continuation");
    assert!(
        !observed_events[..=budget_index]
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "provider tools after tools-disabled must not fail the turn"
    );

    let requests = provider.snapshot_requests();
    assert!(
        requests.len() >= 4,
        "continuation should schedule another provider request after the no-tools violation"
    );
    let no_tools_index = requests
        .iter()
        .position(|request| request.tools.is_none())
        .expect("fixture should enter one no-tools finalization round before continuation");
    assert_eq!(
        tool_result_message_count(&requests[no_tools_index]),
        2,
        "only tool-capable retry rounds should execute tools before no-tools violation"
    );
    assert!(
        requests
            .iter()
            .skip(no_tools_index + 1)
            .any(|request| request.tools.is_some()),
        "next execution window should restore tools after provider tools-disabled violation"
    );
}

#[tokio::test]
async fn failed_task_create_cannot_finalize_as_success_claim() {
    let task_tools = Arc::new(FailingTaskMutationToolProvider::default());
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_task_create_failed_then_claim_success".to_owned(),
            name: "task_create".to_owned(),
            arguments: serde_json::json!({
                "title": "Daily weather",
                "goal": "Create the scheduled task",
                "trigger": "every day at 07:00"
            })
            .to_string(),
        }],
        "задача настроена",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(task_tools.clone()))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_task_mutation_guard",
        "ws_task_mutation_guard",
        "turn_task_mutation_guard",
        ThreadMode::Agent,
        "sequenced-tools",
        "create a scheduled task",
    )
    .await;

    assert_turn_completed(&observed);
    let final_text =
        completed_agent_message_text(&observed).expect("final assistant text should be emitted");
    assert!(
        final_text.contains("Task mutation failed"),
        "final text should report the primary failed mutation, got: {final_text}"
    );
    assert!(
        !final_text.contains("задача настроена"),
        "failed mutation guard must not pass through a success claim: {final_text}"
    );
    assert_eq!(
        task_tools.handler.calls(),
        0,
        "invalid task_create arguments must fail before mutation side effects"
    );
}

#[tokio::test]
async fn task_mutation_failure_preserves_root_cause_when_provider_returns_tools_after_disabled() {
    let task_tools = Arc::new(FailingTaskMutationToolProvider::default());
    let provider = Arc::new(AlwaysTaskCreateProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider(
        "always-task-create",
        provider.clone(),
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 8, 16);
    config.retry.max_recoverable_retry_rounds_per_episode = 16;
    config.retry.max_same_tool_error_retries_per_episode = 16;
    let manager = AgentManager::new(registry, config);
    manager
        .set_task_tool_provider(Some(task_tools.clone()))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_task_mutation_provider_tools_disabled",
        "ws_task_mutation_provider_tools_disabled",
        "turn_task_mutation_provider_tools_disabled",
        ThreadMode::Agent,
        "always-task-create",
        "create a scheduled task",
    )
    .await;

    assert_turn_completed(&observed);
    assert!(
        !observed.iter().any(|event| match event {
            AgentEvent::TurnFailed { .. } => true,
            _ => false,
        }),
        "deterministic task mutation finalization must not surface a turn failure"
    );
    let final_text =
        completed_agent_message_text(&observed).expect("final assistant text should be emitted");
    assert!(final_text.contains("Task mutation failed"), "{final_text}");
    assert!(final_text.contains("task_create"), "{final_text}");
    assert!(
        !final_text.contains("provider_returned_tools_after_tools_disabled"),
        "root cause must stay the task mutation failure, got: {final_text}"
    );
    assert_eq!(
        task_tools.handler.calls(),
        0,
        "invalid task_create arguments and final no-tools tool calls must not execute mutations"
    );
    let requests = provider.snapshot_requests();
    assert!(
        requests
            .iter()
            .skip(1)
            .any(|request| request.tools.is_none()),
        "task mutation finalization should still include a no-tools provider request"
    );
}

#[tokio::test]
async fn partial_task_tool_name_does_not_emit_visible_truncated_tool_row() {
    let task_tools = Arc::new(FailingTaskMutationToolProvider::default());
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_partial_task_tool_name".to_owned(),
            name: "tas".to_owned(),
            arguments: serde_json::json!({}).to_string(),
        }],
        "final after partial tool name",
    ));
    let registry = Arc::new(ProviderRegistry::with_provider(
        "sequenced-tools",
        provider.clone(),
    ));
    let manager = AgentManager::new(registry, test_tool_loop_config());
    manager
        .set_task_tool_provider(Some(task_tools.clone()))
        .await;

    let observed = start_simple_turn(
        &manager,
        "thr_partial_task_tool_name",
        "ws_partial_task_tool_name",
        "turn_partial_task_tool_name",
        ThreadMode::Agent,
        "sequenced-tools",
        "create a scheduled task",
    )
    .await;

    assert_turn_completed(&observed);
    assert_eq!(
        task_tools.handler.calls(),
        0,
        "partial prefix must not dispatch as task_create"
    );
    assert!(
        !observed.iter().any(|event| matches!(
            event,
            AgentEvent::ItemStarted(ItemStartedNotification {
                item: TurnItem::DynamicToolCall { tool_name, .. },
                ..
            }) | AgentEvent::ItemCompleted(ItemCompletedNotification {
                item: TurnItem::DynamicToolCall { tool_name, .. },
                ..
            }) if tool_name == "tas"
        )),
        "partial tool name prefixes must not create visible `tas` rows: {observed:?}"
    );
}

#[tokio::test]
async fn tool_loop_total_tool_call_budget_prevents_execution() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::TooManyToolsThenFinal,
        3,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 8, 2);
    config.retry.max_recoverable_retry_rounds_per_episode = 8;
    config.retry.max_same_tool_error_retries_per_episode = 8;
    let manager = loop_budget_manager(provider.clone(), config);
    let mut events = start_loop_budget_turn(
        &manager,
        "thr_loop_budget_tool_calls",
        "turn_loop_budget_tool_calls",
    )
    .await;

    let observed_events = recv_events_until_loop_budget_action(
        &mut events,
        ToolLoopBudgetLimitKind::ToolCalls,
        ToolLoopBudgetAction::ContinueInNextWindow,
    )
    .await;
    assert!(observed_events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnToolLoopBudgetExceeded(notification)
            if notification.limit_kind == ToolLoopBudgetLimitKind::ToolCalls
                && notification.action == ToolLoopBudgetAction::ContinueInNextWindow
    )));
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnFailed { .. })),
        "tool-call window exhaustion must not fail the turn"
    );

    let requests = provider.snapshot_requests();
    assert!(
        !requests.is_empty(),
        "provider should receive the window request that exceeded the tool-call budget"
    );
    assert!(
        requests[0]
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
    set_execution_window_budget(&mut config, 8, 8);
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
    set_execution_window_budget(&mut config, 8, 8);
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
    set_execution_window_budget(&mut config, 8, 8);
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
            name: "list_dir".to_owned(),
            arguments: serde_json::json!({"path": ".", "depth": 0, "limit": 1}).to_string(),
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
        .start_turn_with_capabilities(
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
            && tool_name == "list_dir"
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
            && tool_name == "list_dir"
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
        .start_turn_with_capabilities(
            "thr_000000000000000001",
            "turn_000000000000000001",
            ThreadMode::Agent,
            "openai/gpt-4o",
            "echo",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
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

#[tokio::test]
async fn start_turn_initializes_first_execution_window_index() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 2, 16);
    let manager = loop_budget_manager(provider, config);
    let thread_id = "thr_initial_window_index";
    let workspace_id = "ws_initial_window_index";
    let turn_id = "turn_initial_window_index";
    manager
        .ensure_thread(thread_id, workspace_id)
        .await
        .expect("thread should be created");
    let mut durable_events = manager
        .take_durable_receiver(thread_id)
        .await
        .expect("thread should expose one durable receiver");

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "loop-budget",
            HashMap::new(),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("turn should start");

    for _ in 0..20 {
        let event = match timeout(Duration::from_secs(1), durable_events.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => panic!("durable receiver should stay open"),
            Err(_) => continue,
        };
        if let AgentDurableEvent::TurnExecutionWindowStarted { notification } = event {
            assert_eq!(notification.workspace_id, workspace_id);
            assert_eq!(notification.thread_id, thread_id);
            assert_eq!(notification.turn_id, turn_id);
            assert_eq!(notification.window_index, 1);
            assert_eq!(
                notification.status,
                pioneer_protocol::ExecutionWindowStatus::Running
            );
            return;
        }
    }

    panic!("initial execution-window started event was not emitted");
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
        .start_turn_with_capabilities(
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
                execution_checkpoint_context: None,
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
        .start_turn_with_capabilities(
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
                execution_checkpoint_context: None,
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
        .start_turn_with_capabilities(
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
                execution_checkpoint_context: None,
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
async fn explicit_skill_input_injects_compact_skill_prompt_and_binding() {
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000010";
    let turn_id = "turn_000000000000000010";
    manager
        .ensure_thread(thread_id, "ws_000000000000000010")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run with skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let saw_binding_event = observed.iter().any(|event| match event {
        AgentEvent::TurnSkillsResolved { bindings, .. } => {
            assert_eq!(bindings.len(), 1);
            assert_eq!(bindings[0].skill_slug, "tests/my-skill");
            assert_eq!(bindings[0].resolved_reason, "explicit_composer_capability");
            true
        }
        _ => false,
    });
    let saw_capability_event = observed.iter().any(|event| match event {
        AgentEvent::TurnCapabilitiesResolved {
            accepted, rejected, ..
        } => {
            assert!(rejected.is_empty());
            assert_eq!(accepted.len(), 1);
            assert_eq!(accepted[0].id, "skill:user:my-skill");
            assert_eq!(
                accepted[0].reason,
                TurnCapabilityAcceptedReason::ExplicitComposerCapability
            );
            true
        }
        _ => false,
    });

    assert!(
        saw_binding_event,
        "expected TurnSkillsResolved event with one active skill"
    );
    assert!(
        saw_capability_event,
        "expected TurnCapabilitiesResolved event with accepted skill capability"
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
    assert_compact_skill_prompt(
        compiled_prompt.full_system_text.as_str(),
        "user:tests/my-skill",
        "My Skill",
        "Follow the skill.",
    );

    let manifest = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .expect("prompt manifest should be emitted");
    assert!(
        manifest
            .section_ids
            .iter()
            .any(|section_id| section_id == "skills_runtime_prompt"),
        "accepted skill prompt must remain represented by skills_runtime_prompt"
    );
    assert!(
        !manifest
            .section_ids
            .iter()
            .any(|section_id| section_id == "skills_prompt"),
        "legacy skills_prompt manifest section must not be introduced"
    );

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn rejected_capability_emits_event_warning_and_manifest_diagnostic() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = Vec::new();
    tool_loop_config.skills.registry_roots = Vec::new();
    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_capability_rejected_diagnostics";
    let turn_id = "turn_capability_rejected_diagnostics";

    manager
        .ensure_thread(thread_id, "ws_capability_rejected_diagnostics")
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run with missing skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("missing-skill")],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let (accepted, rejected) = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::TurnCapabilitiesResolved {
                accepted, rejected, ..
            } => Some((accepted, rejected)),
            _ => None,
        })
        .expect("capability resolution event should be emitted");
    assert!(accepted.is_empty());
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].id, "skill:user:missing-skill");
    assert_eq!(rejected[0].reason, TurnCapabilityRejectedReason::NotFound);
    assert!(
        rejected[0]
            .message
            .contains("not installed or not available")
    );

    let serialized = serde_json::to_string(&AgentDurableEvent::TurnCapabilitiesResolved {
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        accepted: accepted.clone(),
        rejected: rejected.clone(),
        mcp_bindings: Vec::new(),
    })
    .expect("capability event should serialize");
    assert!(serialized.contains("turn_capabilities_resolved"));
    assert!(serialized.contains("not installed or not available"));
    assert!(!serialized.contains("SKILL.md"));

    let warning = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::ItemCompleted(ItemCompletedNotification { item, .. }) => match item {
                TurnItem::SystemEvent {
                    level,
                    message,
                    code,
                    details,
                    ..
                } if code.as_deref() == Some("capability.rejected") => {
                    Some((level, message, details))
                }
                _ => None,
            },
            _ => None,
        })
        .expect("rejected capability should emit a visible warning item");
    assert_eq!(*warning.0, SystemEventLevel::Warning);
    assert!(
        warning
            .1
            .contains("Capability `missing-skill` was not attached")
    );
    assert_eq!(
        warning
            .2
            .as_ref()
            .and_then(|details| details.get("rejected"))
            .and_then(|value| value.as_array())
            .map(Vec::len),
        Some(1)
    );

    let manifest = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .expect("prompt manifest should be emitted");
    let diagnostic = manifest
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == PromptManifestDiagnosticCode::CapabilityRejected)
        .expect("rejected capability should be included in prompt manifest diagnostics");
    assert!(diagnostic.message.contains("missing-skill"));
    assert!(
        diagnostic
            .message
            .contains("not installed or not available")
    );
    assert_eq!(diagnostic.section_id, None);
    assert_eq!(diagnostic.hook_source, None);
}

#[tokio::test]
async fn explicit_skill_input_injects_compact_prompt_for_non_tool_calling_provider() {
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    let thread_id = "thr_000000000000000011";
    let turn_id = "turn_000000000000000011";
    manager
        .ensure_thread(thread_id, "ws_000000000000000011")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture-standard",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run with skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
    assert_compact_skill_prompt(
        compiled_prompt.full_system_text.as_str(),
        "user:tests/my-skill",
        "My Skill",
        "Follow the skill.",
    );

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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000120", "ws_000000000000000120")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000120").await;

    manager
        .start_turn_with_capabilities(
            "thr_000000000000000120",
            "turn_000000000000000120",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "use the skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
    assert!(tool_names.contains(&"skill.user-tests-my-skill.fetch-data"));
    assert!(tool_names.contains(&"read_skill"));

    let _ = fs::remove_dir_all(skill_root);
}

#[tokio::test]
async fn explicit_mcp_tool_contributes_dynamic_tool_definition_without_prompt_section() {
    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let workspace_id = "ws_000000000000000122";
    let capability_id = "mcp-tool:workspace:resend:send";
    let mcp_provider = Arc::new(TestMcpToolProvider::new(explicit_mcp_tool_materialization(
        capability_id,
        workspace_id,
    )));

    let manager = AgentManager::new_with_mcp(
        registry,
        test_tool_loop_config(),
        Some(mcp_provider.clone()),
    );
    let thread_id = "thr_000000000000000122";
    let turn_id = "turn_000000000000000122";
    manager
        .ensure_thread(thread_id, workspace_id)
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "use resend".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![mcp_tool_capability("resend", "send")],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let materialization_requests = mcp_provider.snapshot_requests();
    assert_eq!(materialization_requests.len(), 1);
    assert_eq!(materialization_requests[0].workspace_id, workspace_id);
    assert_eq!(materialization_requests[0].turn_id, turn_id);
    assert!(materialization_requests[0].explicit_servers.is_empty());
    assert_eq!(materialization_requests[0].explicit_tools.len(), 1);
    assert_eq!(
        materialization_requests[0].explicit_tools[0].capability_id,
        capability_id
    );

    let requests = provider.snapshot_requests();
    assert!(!requests.is_empty());
    let first_turn_request = &requests[0];
    let tools = first_turn_request
        .tools
        .as_ref()
        .expect("agent request should include provider tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"mcp_resend_send"));
    assert!(
        !tool_names.contains(&"mcp_resend_domains"),
        "unselected MCP tools must not appear when materialization did not include them"
    );

    let prompt = first_turn_request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert!(!prompt.full_system_text.contains("mcp_resend_send"));
    assert!(!prompt.full_system_text.contains("resend/send"));
    assert!(!prompt.full_system_text.contains("MCP server"));
    assert!(!prompt.dynamic_system_text.contains("mcp_"));
    assert!(!prompt.dynamic_system_text.contains("resend"));

    let mcp_event_bindings = observed.iter().find_map(|event| match event {
        AgentEvent::TurnCapabilitiesResolved { mcp_bindings, .. } => Some(mcp_bindings),
        _ => None,
    });
    let mcp_event_bindings =
        mcp_event_bindings.expect("capability event should include MCP bindings");
    assert_eq!(mcp_event_bindings.len(), 1);
    assert_eq!(
        mcp_event_bindings[0].selection_reason,
        "explicit_composer_capability"
    );
    assert_eq!(
        mcp_event_bindings[0].capability_id.as_deref(),
        Some(capability_id)
    );

    let manifest = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .expect("prompt manifest should be emitted");
    assert!(
        !manifest
            .section_ids
            .iter()
            .any(|section_id| section_id.contains("mcp")),
        "accepted MCP tools must be represented by bindings/tool schemas, not prompt sections: {:?}",
        manifest.section_ids
    );
}

#[tokio::test]
async fn text_file_skill_and_mcp_capabilities_survive_single_turn_prompt_gate() {
    let skill_root = unique_temp_dir("phase-6-combined-skill");
    let skill_dir = skill_root.join("tests").join("my-skill");
    fs::create_dir_all(&skill_dir).expect("failed to create skill dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: My Skill\nslug: my-skill\ndescription: Test skill description\n---\nFollow the skill.",
    )
    .expect("failed to write SKILL.md");

    let file_root = unique_temp_dir("phase-6-combined-file");
    fs::create_dir_all(&file_root).expect("failed to create file root");
    let attachment_path = file_root.join("note.txt");
    fs::write(&attachment_path, "file body").expect("failed to write attachment");

    let provider = Arc::new(CaptureAgentProvider::default());
    let registry = Arc::new(ProviderRegistry::with_provider("capture", provider.clone()));
    let workspace_id = "ws_000000000000000126";
    let capability_id = "mcp-tool:workspace:resend:send";
    let mcp_provider = Arc::new(TestMcpToolProvider::new(explicit_mcp_tool_materialization(
        capability_id,
        workspace_id,
    )));

    let mut tool_loop_config = test_tool_loop_config();
    tool_loop_config.skills.system_roots = Vec::new();
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
    tool_loop_config.skills.registry_roots = Vec::new();
    let manager =
        AgentManager::new_with_mcp(registry, tool_loop_config, Some(mcp_provider.clone()));
    let thread_id = "thr_000000000000000126";
    let turn_id = "turn_000000000000000126";
    manager
        .ensure_thread(thread_id, workspace_id)
        .await
        .expect("thread should be created");
    let mut events = subscribe_agent_events(&manager, thread_id).await;

    manager
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![
                UserInput::Text {
                    text: "use the file, skill, and resend".to_owned(),
                    text_elements: Vec::new(),
                },
                UserInput::LocalFile {
                    path: attachment_path.display().to_string(),
                },
            ],
            vec![
                skill_capability("my-skill"),
                mcp_tool_capability("resend", "send"),
            ],
            Vec::new(),
        )
        .await
        .expect("turn should start");

    let observed = recv_events_until_terminal(&mut events).await;
    assert_turn_completed(&observed);

    let requests = provider.snapshot_requests();
    assert!(!requests.is_empty());
    let request = &requests[0];
    let user_message = request
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .expect("request should include user message");
    assert_eq!(user_message.content, "use the file, skill, and resend");
    assert_eq!(user_message.content_parts.len(), 1);
    match &user_message.content_parts[0] {
        MessageContentPart::File { file } => {
            assert_eq!(file.mime_type, "text/plain");
            assert_eq!(
                file.source,
                AttachmentDataSource::Path {
                    path: attachment_path.display().to_string()
                }
            );
        }
        other => panic!("expected file attachment, got {other:?}"),
    }

    let prompt = request
        .compiled_prompt
        .as_ref()
        .expect("agent request should include compiled prompt");
    assert_compact_skill_prompt(
        prompt.full_system_text.as_str(),
        "user:tests/my-skill",
        "My Skill",
        "Follow the skill.",
    );
    assert!(!prompt.full_system_text.contains("mcp_resend_send"));
    assert!(!prompt.full_system_text.contains("resend/send"));

    let tools = request
        .tools
        .as_ref()
        .expect("agent request should include provider tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"mcp_resend_send"));

    let (accepted, rejected, mcp_bindings) = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::TurnCapabilitiesResolved {
                accepted,
                rejected,
                mcp_bindings,
                ..
            } => Some((accepted, rejected, mcp_bindings)),
            _ => None,
        })
        .expect("capability resolution event should be emitted");
    assert!(rejected.is_empty());
    assert_eq!(accepted.len(), 2);
    assert!(
        accepted
            .iter()
            .any(|capability| capability.id == "skill:user:my-skill")
    );
    assert!(
        accepted
            .iter()
            .any(|capability| capability.id == capability_id)
    );
    assert_eq!(mcp_bindings.len(), 1);
    assert_eq!(
        mcp_bindings[0].selection_reason,
        "explicit_composer_capability"
    );

    let manifest = observed
        .iter()
        .find_map(|event| match event {
            AgentEvent::PromptManifestCompiled { manifest, .. } => Some(manifest),
            _ => None,
        })
        .expect("prompt manifest should be emitted");
    assert!(
        manifest
            .section_ids
            .iter()
            .any(|section_id| section_id == "skills_runtime_prompt")
    );
    assert!(
        !manifest
            .section_ids
            .iter()
            .any(|section_id| section_id.contains("mcp")),
        "MCP capabilities must not create prompt sections: {:?}",
        manifest.section_ids
    );

    let _ = fs::remove_dir_all(skill_root);
    let _ = fs::remove_dir_all(file_root);
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
        command: ["/bin/sh", "-c", "printf shell-ok"]
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
            name: "skill.user-tests-my-skill.echo-shell".to_owned(),
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
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
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "trigger tool".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
        Some("skill.user-tests-my-skill.echo-shell")
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
                && tool_name == "skill.user-tests-my-skill.echo-shell"
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
        command: ["/bin/sh", "-c", "sleep 3"]"#,
        ),
    );

    let tool_call_id = "call_dynamic_recovery_1";
    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: tool_call_id.to_owned(),
            name: "skill.user-tests-my-skill.slow-shell".to_owned(),
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
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
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "trigger slow tool".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
                        execution_checkpoint_context: None,
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
        command: ["/bin/sh", "-c", "printf shell-ok"]"#,
        ),
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_dynamic_2".to_owned(),
            name: "skill.user-tests-my-skill.echo-shell".to_owned(),
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
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
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "trigger tool".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
        Some("skill.user-tests-my-skill.echo-shell")
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];
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
        .start_turn_with_capabilities(
            thread_id,
            turn_id,
            ThreadMode::Agent,
            "test-model",
            "capture-standard",
            HashMap::new(),
            vec![UserInput::Text {
                text: "resolve both skills".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![
                skill_capability("good-skill"),
                skill_capability("bad-skill"),
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
                        assert_eq!(
                            item.details
                                .get("resolved_reason")
                                .and_then(|value| value.as_str()),
                            Some("explicit_composer_capability")
                        );
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
    let skill_dir = write_skill(
        skill_root.as_path(),
        "my-skill",
        "Full body text for read_skill.",
        None,
    );

    let provider = Arc::new(SequencedToolProvider::new(
        vec![ProviderToolCall {
            id: "call_read_1".to_owned(),
            name: "read_skill".to_owned(),
            arguments: r#"{"slug":"user:tests/my-skill"}"#.to_owned(),
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000122", "ws_000000000000000122")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000122").await;

    manager
        .start_turn_with_capabilities(
            "thr_000000000000000122",
            "turn_000000000000000122",
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "read skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
            .contains("\"slug\":\"user:tests/my-skill\"")
    );
    assert!(
        read_skill_result
            .content
            .contains("Full body text for read_skill.")
    );
    assert!(
        read_skill_result.content.contains(
            format!(
                "\"skill_asset_root\":\"{}\"",
                fs::canonicalize(skill_dir.as_path())
                    .expect("skill dir canonicalizes")
                    .display()
            )
            .as_str()
        )
    );
    assert!(
        read_skill_result
            .content
            .contains("relative_path_resolution")
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000123", "ws_000000000000000123")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000123").await;

    manager
        .start_turn_with_capabilities(
            "thr_000000000000000123",
            "turn_000000000000000123",
            ThreadMode::Agent,
            "test-model",
            "sequenced-tools",
            HashMap::new(),
            vec![UserInput::Text {
                text: "read skill".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000124", "ws_000000000000000124")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000124").await;

    manager
        .start_turn_with_capabilities(
            "thr_000000000000000124",
            "turn_000000000000000124",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
    assert!(!tool_names.contains(&"skill.user-tests-my-skill.bad-proxy"));

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
    tool_loop_config.skills.user_roots = vec![skill_root.display().to_string()];

    let manager = AgentManager::new(registry, tool_loop_config);
    manager
        .ensure_thread("thr_000000000000000125", "ws_000000000000000125")
        .await
        .expect("thread should be created");

    let mut events = subscribe_agent_events(&manager, "thr_000000000000000125").await;

    manager
        .start_turn_with_capabilities(
            "thr_000000000000000125",
            "turn_000000000000000125",
            ThreadMode::Agent,
            "test-model",
            "capture",
            HashMap::new(),
            vec![UserInput::Text {
                text: "run".to_owned(),
                text_elements: Vec::new(),
            }],
            vec![skill_capability("my-skill")],
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
    assert!(tool_names.contains(&"skill.user-tests-my-skill.fetch-data"));
    assert!(!tool_names.contains(&"skill.user-tests-my-skill.bad-proxy"));

    let _ = fs::remove_dir_all(skill_root);
}
