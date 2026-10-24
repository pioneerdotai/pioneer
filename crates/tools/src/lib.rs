pub mod apply_patch;
mod argument_normalizer;
mod builtin_argument_validator;
mod classifier;
mod context;
mod domain;
mod error;
mod events;
mod file_policy;
mod loop_guard;
mod mcp_policy;
mod network_policy;
mod no_progress_guard;
mod orchestrator;
mod output_dynamic_policy;
mod output_policy;
mod output_projection;
mod permissions;
mod process_policy;
mod registry;
mod retry_controller;
mod router;
mod runtime;
mod sandbox_backend;
mod shell_format;
mod spec;
mod tool_index;
mod visibility;
mod web;
mod windows_restricted_token_backend;

pub mod handlers;

pub use argument_normalizer::{
    ToolArgumentCoercion, ToolArgumentNormalization, normalize_tool_arguments_for_tool,
    normalize_tool_arguments_from_schema,
};
pub use classifier::{DefaultErrorClassifier, ErrorClassifier, classify_tool_error};
pub use context::{
    AnyToolResult, CallToolResult, ExecCommandArgs, FunctionToolOutput, LocalShellPayload,
    ToolCallSource, ToolDeliveryReceipt, ToolErrorClass, ToolInvocation, ToolOutcome,
    ToolOutcomeStatus, ToolOutput, ToolPayload, ToolSearchOutput, ToolSearchResultTool,
    ToolSuggestOutput, WriteStdinArgs,
};
pub use domain::{
    ARTIFACT_DOMAIN_TOOL_NAMES, BUILTIN_TOOL_DOMAIN_MAP, BuiltinToolDomain,
    COMPUTER_USE_DOMAIN_TOOL_NAMES, MEMORY_DOMAIN_TOOL_NAMES, REQUEST_TOOLS_DOMAIN_VALUES,
    TASK_DOMAIN_TOOL_NAMES, THREADS_DOMAIN_TOOL_NAMES, builtin_tool_domain_map,
    builtin_tool_domain_names, registered_domain_tool_names,
};
pub use error::ToolError;
pub use events::{
    DurableToolEventEnvelope, ObservationContext, ToolCallCompletedEvent, ToolCallFailedEvent,
    ToolCallStartedEvent, ToolDeltaPayload, ToolEvent, ToolEventBus, ToolEventKind,
    ToolEventPayload, ToolEventTrace, ToolOutputDeltaEvent, native_patch_history_record,
    register_native_patch_observer, unregister_native_patch_observer,
    unregister_native_patch_observers_for_turn,
};
pub use file_policy::{
    FilePolicyChecker, FilePolicyDecision, FilePolicyDeny, FilePolicyDenyReason, FilePolicyGrant,
    FilePolicyOperation,
};
pub use handlers::{
    ExcludedMcpRuntimeTool, ExcludedSkillRuntimeTool, McpDynamicToolAnnotations,
    McpDynamicToolBinding, McpDynamicToolDescriptor, McpRuntimeToolMaterialization,
    McpToolCallOutput, McpToolCallRequest, McpToolExecutor, RequestToolsDomainDiagnostic,
    RequestToolsHandler, RequestToolsResult, SkillDynamicToolDescriptor, SkillDynamicToolKind,
    SkillReadToolConfig, SkillReadToolEntry, SkillRuntimeToolMaterialization,
    SkillRuntimeToolPolicyDiagnostic, materialize_mcp_runtime_tools,
    materialize_skill_runtime_tools,
};
pub use loop_guard::{
    ExecutionWindowAdmissionDecision, ExecutionWindowBudgetConfig,
    ExecutionWindowTotalBudgetConfig, ExecutionWindowsConfig, ToolLoopBudgetAction,
    ToolLoopBudgetConfig, ToolLoopBudgetExceeded, ToolLoopBudgetReason, ToolLoopGuard,
    ToolLoopGuardDecision, ToolLoopRoundAction, ToolLoopRoundPlan,
    decide_execution_window_admission,
};
pub use mcp_policy::{enforce_mcp_network_policy, mcp_policy_classification_metadata};
pub use network_policy::{
    NetworkPolicyChecker, NetworkPolicyDecision, NetworkPolicyDeny, NetworkPolicyDenyReason,
    NetworkPolicyGrant, enforce_network_url,
};
pub use no_progress_guard::{
    ToolNoProgressFeedback, ToolNoProgressGuard, ToolNoProgressGuardConfig,
    ToolNoProgressPreflightDecision,
};
pub use orchestrator::{ApprovalState, OrchestratorPolicy, SandboxTarget, ToolOrchestrator};
pub use output_dynamic_policy::{
    DynamicToolKind, DynamicToolOutputPolicyCaps, DynamicToolPolicyContext,
    DynamicToolPolicyDiagnostic, DynamicToolPolicyDiagnosticCode, DynamicToolPolicyResolution,
    resolve_dynamic_tool_output_policy,
};
pub use output_policy::{
    DeltaOutputPolicy, DiagnosticExcerptPolicy, LlmOutputPolicy, LlmRetentionPolicy,
    RecoveryOutputPolicy, StorageOutputPolicy, TimelineOutputPolicy, ToolDisplayPayload,
    ToolMetadata, ToolMetadataRawKind, ToolMetadataValue, ToolOutputPolicy,
    ToolOutputPolicySnapshot, ToolOutputProjectionKind, ToolOutputSummary, ToolRecoveryView,
    ToolResultEnvelope, ToolResultView, ToolStoragePayload, builtin_output_policy,
    computer_use_output_policy, download_output_policy, dynamic_unknown_output_policy,
    is_thread_paged_tool, mcp_output_policy, model_only_metadata_policy, shell_output_policy,
    thread_read_page_bytes, thread_read_page_fits, web_fetch_output_policy,
    web_search_output_policy,
};
pub use output_projection::{ToolProjectionInput, bounded_summary_for_chars, project_tool_result};
pub use permissions::{
    PermissionActionKind, PermissionApprovalBroker, PermissionApprovalResolution,
    PermissionDecision, PermissionDecisionReason, PermissionEvaluationContext, PermissionIntent,
    PermissionRequestKey, PermissionRequestScope, ProfileToolPermissionEvaluator,
    StaticPermissionApprovalBroker, ToolPermissionEvaluator, extract_permission_intent,
};
pub use process_policy::{ProcessSpawnPlan, build_process_spawn_plan, resolve_process_cwd};
pub use registry::{ToolHandler, ToolRegistry, ToolRegistryBuilder};
pub use retry_controller::{
    ToolFailureSignature, ToolRetryBudgetConfig, ToolRetryBudgetKind, ToolRetryBudgetSnapshot,
    ToolRetryBudgetUsage, ToolRetryClassBudget, ToolRetryController, ToolRetryDecision,
    ToolRetryEpisodeState, ToolRetryEventDraft, ToolRetryExhaustionReason, ToolRetryObservation,
    ToolRetryPrompt, ToolRetryPromptEntry, ToolRetryResolution, ToolRetryResolvedEntry,
    default_tool_retry_class_budgets,
};
pub use router::{RawToolCall, ToolCall, ToolRouter};
pub use runtime::ToolCallRuntime;
pub use sandbox_backend::{
    NativeSandboxBackend, NativeSandboxPrepareOutcome, NativeSandboxPreparedSpawn,
    NativeSandboxRequest, NonoBackendSupport, NonoCapabilityPlan, NonoSandboxBackend,
    build_nono_capability_plan, configure_nono_command, prepare_native_sandbox_backend,
};
pub use shell_format::{ExecModelPayload, ExecPayloadInput, ExecTruncation, render_exec_ui_text};
pub use spec::{
    ConfiguredToolSpec, DynamicSkillPermissionKind, DynamicSkillPermissionMetadata, ExecutionClass,
    PayloadKind, REQUEST_TOOLS_TOOL_NAME, ToolIdempotencyMode, ToolPayloadBinding,
    ToolPermissionMetadata, ToolRecoveryMetadata, ToolRetryClass, ToolSpec,
    builtin_tool_recovery_metadata, builtin_tool_specs,
};
pub use tool_index::{
    PREFLIGHT_CORE_FILE_TOOL_NAMES, PREFLIGHT_CORE_TOOL_NAMES, PreflightCandidateToolDescriptor,
    PreflightToolIndex, build_preflight_tool_index, filesystem_catalog_snapshot,
};
pub use visibility::{
    FinalToolVisibility, FinalToolVisibilityInput, ToolVisibilityDiagnostic,
    ToolVisibilityDiagnosticCode, ToolVisibilitySnapshot, ToolVisibilitySource,
    compute_final_tool_visibility, materialized_dynamic_extension_tool_names,
};
pub use web::{
    DownloadModelPayload, WebFetchLink, WebFetchModelPayload, WebFetchTruncation,
    WebSearchModelPayload, WebSearchResultItem, default_favicon_url, render_download_ui_text,
    render_web_fetch_ui_text, render_web_search_ui_text,
};
pub use windows_restricted_token_backend::{
    WindowsRestrictedTokenBackend, WindowsRestrictedTokenPlan, WindowsRestrictedTokenSupport,
    WindowsWorkspaceGrant, WindowsWorkspaceGrantAccess, build_windows_restricted_token_plan,
    configure_windows_restricted_token_command,
};

pub use handlers::computer_use::{
    ComputerUseBackend, computer_use_tool_specs, is_computer_use_tool,
};
#[cfg(feature = "computer-use")]
use handlers::materialize_computer_use_domain_bundle;

use handlers::{
    ApplyPatchHandler, DownloadUrlHandler, GrepHandler, ListDirHandler, ReadFileHandler,
    UnifiedExecHandler, WebFetchHandler, WebSearchHandler,
};
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct WebToolsConfig {
    pub default_timeout_ms: u64,
    pub hard_max_timeout_ms: u64,
    pub default_fetch_max_bytes: usize,
    pub hard_fetch_max_bytes: usize,
    pub default_download_max_bytes: usize,
    pub hard_download_max_bytes: usize,
    pub default_max_results: usize,
    pub hard_max_results: usize,
    pub default_snippet_chars: usize,
    pub hard_max_snippet_chars: usize,
    pub default_link_count: usize,
    pub hard_link_count: usize,
    pub default_render_max_chars: usize,
    pub ddg_html_search_url: String,
    pub ddg_instant_api_url: String,
    pub default_user_agent: String,
}

impl Default for WebToolsConfig {
    fn default() -> Self {
        Self {
            default_timeout_ms: 20_000,
            hard_max_timeout_ms: 120_000,
            default_fetch_max_bytes: 2 * 1024 * 1024,
            hard_fetch_max_bytes: 8 * 1024 * 1024,
            default_download_max_bytes: 128 * 1024 * 1024,
            hard_download_max_bytes: 1024 * 1024 * 1024,
            default_max_results: 8,
            hard_max_results: 20,
            default_snippet_chars: 420,
            hard_max_snippet_chars: 4096,
            default_link_count: 40,
            hard_link_count: 200,
            default_render_max_chars: 40_000,
            ddg_html_search_url: "https://duckduckgo.com/html/".to_owned(),
            ddg_instant_api_url: "https://api.duckduckgo.com/".to_owned(),
            default_user_agent: "Mozilla/5.0".to_owned(),
        }
    }
}

impl WebToolsConfig {
    pub fn normalized(&self) -> Self {
        let ddg_html_search_url = if self.ddg_html_search_url.trim().is_empty() {
            "https://duckduckgo.com/html/".to_owned()
        } else {
            self.ddg_html_search_url.clone()
        };
        let ddg_instant_api_url = if self.ddg_instant_api_url.trim().is_empty() {
            "https://api.duckduckgo.com/".to_owned()
        } else {
            self.ddg_instant_api_url.clone()
        };
        let default_user_agent = if self.default_user_agent.trim().is_empty() {
            "Mozilla/5.0".to_owned()
        } else {
            self.default_user_agent.clone()
        };

        Self {
            default_timeout_ms: self.default_timeout_ms.max(1),
            hard_max_timeout_ms: self.hard_max_timeout_ms.max(1),
            default_fetch_max_bytes: self.default_fetch_max_bytes.max(1),
            hard_fetch_max_bytes: self.hard_fetch_max_bytes.max(1),
            default_download_max_bytes: self.default_download_max_bytes.max(1),
            hard_download_max_bytes: self.hard_download_max_bytes.max(1),
            default_max_results: self.default_max_results.max(1),
            hard_max_results: self.hard_max_results.max(1),
            default_snippet_chars: self.default_snippet_chars.max(1),
            hard_max_snippet_chars: self.hard_max_snippet_chars.max(1),
            default_link_count: self.default_link_count.max(1),
            hard_link_count: self.hard_link_count.max(1),
            default_render_max_chars: self.default_render_max_chars.max(1),
            ddg_html_search_url,
            ddg_instant_api_url,
            default_user_agent,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComputerUseToolsConfig {
    pub image_admission: handlers::computer_use::ComputerUseImageAdmission,
    pub runtime_home_dir: PathBuf,
    pub max_total_bytes: u64,
    pub backend: Option<Arc<dyn ComputerUseBackend>>,
}
impl ComputerUseToolsConfig {
    pub fn normalized(&self) -> Self {
        Self {
            image_admission: self.image_admission.clone(),
            runtime_home_dir: self.runtime_home_dir.clone(),
            max_total_bytes: self.max_total_bytes.max(1),
            backend: self.backend.clone(),
        }
    }
}
impl Default for ComputerUseToolsConfig {
    fn default() -> Self {
        Self {
            image_admission: Default::default(),
            runtime_home_dir: PathBuf::new(),
            max_total_bytes: 1024 * 1024 * 1024,
            backend: None,
        }
    }
}

pub struct BuiltinTools {
    pub router: Arc<ToolRouter>,
    pub runtime: ToolCallRuntime,
    pub visibility: ToolVisibilitySnapshot,
    pub event_bus: ToolEventBus,
}

impl BuiltinTools {
    pub fn with_permission_approval_broker(
        mut self,
        approval_broker: Arc<dyn PermissionApprovalBroker>,
    ) -> Self {
        self.runtime =
            self.runtime
                .with_orchestrator(Arc::new(ToolOrchestrator::with_approval_broker(
                    OrchestratorPolicy::default(),
                    approval_broker,
                )));
        self
    }
}

#[derive(Clone, Default)]
pub struct ToolExtensionBundle {
    pub specs: Vec<ConfiguredToolSpec>,
    pub handlers: Vec<(String, Arc<dyn ToolHandler>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildToolsError {
    DuplicateToolName(String),
    DuplicateHandlerName(String),
    MissingHandlerForTool(String),
    HandlerWithoutSpec(String),
    InvalidPermissionContext(String),
}

impl Display for BuildToolsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateToolName(name) => write!(f, "duplicate tool name `{name}`"),
            Self::DuplicateHandlerName(name) => write!(f, "duplicate handler name `{name}`"),
            Self::MissingHandlerForTool(name) => {
                write!(f, "missing handler for extension tool `{name}`")
            }
            Self::HandlerWithoutSpec(name) => {
                write!(f, "extension handler `{name}` has no matching tool spec")
            }
            Self::InvalidPermissionContext(message) => {
                write!(f, "invalid permission context: {message}")
            }
        }
    }
}

impl std::error::Error for BuildToolsError {}

pub fn build_tools(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    permission_context: PermissionEvaluationContext,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    extensions: Vec<ToolExtensionBundle>,
) -> Result<BuiltinTools, BuildToolsError> {
    build_tools_with_environment(
        workdir,
        turn_id,
        permission_context,
        web_tools_config,
        computer_use_tools_config,
        extensions,
        BTreeMap::new(),
    )
}

pub fn build_tools_with_environment(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    permission_context: PermissionEvaluationContext,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    extensions: Vec<ToolExtensionBundle>,
    environment: BTreeMap<String, String>,
) -> Result<BuiltinTools, BuildToolsError> {
    build_tools_with_environment_and_security_snapshot(
        workdir,
        turn_id,
        permission_context,
        web_tools_config,
        computer_use_tools_config,
        extensions,
        environment,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_tools_with_environment_and_security_snapshot(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    permission_context: PermissionEvaluationContext,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    extensions: Vec<ToolExtensionBundle>,
    environment: BTreeMap<String, String>,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
) -> Result<BuiltinTools, BuildToolsError> {
    let turn_id = turn_id.into();
    validate_permission_context(&turn_id, &permission_context)?;
    let web_tools_config = web_tools_config.normalized();

    #[cfg(feature = "computer-use")]
    let extensions = {
        let mut extensions = extensions;
        extensions.push(materialize_computer_use_domain_bundle(
            computer_use_tools_config.normalized(),
            turn_id.clone(),
        ));
        extensions
    };

    #[cfg(not(feature = "computer-use"))]
    let _ = computer_use_tools_config;

    let mut configured_specs = builtin_tool_specs();
    if let Some(search) = configured_specs
        .iter_mut()
        .find(|configured| configured.spec.name == "web_search")
    {
        search.spec.permission_metadata.network_targets = vec![
            web_tools_config.ddg_html_search_url.clone(),
            web_tools_config.ddg_instant_api_url.clone(),
        ];
    }
    let builtin_tool_names = configured_specs
        .iter()
        .map(|configured| configured.spec.name.clone())
        .collect::<HashSet<_>>();
    let mut seen_tool_names = configured_specs
        .iter()
        .map(|configured| configured.spec.name.clone())
        .collect::<HashSet<_>>();
    let mut extension_tool_names = HashSet::new();
    let mut extension_handler_names = HashSet::new();

    for extension in &extensions {
        for spec in &extension.specs {
            let name = spec.spec.name.clone();
            if !seen_tool_names.insert(name.clone()) {
                return Err(BuildToolsError::DuplicateToolName(name));
            }
            extension_tool_names.insert(name.clone());
            configured_specs.push(spec.clone());
        }

        for (name, _) in &extension.handlers {
            if builtin_tool_names.contains(name.as_str()) {
                return Err(BuildToolsError::DuplicateToolName(name.clone()));
            }
            if !extension_tool_names.contains(name.as_str()) {
                return Err(BuildToolsError::HandlerWithoutSpec(name.clone()));
            }
            if !extension_handler_names.insert(name.clone()) {
                return Err(BuildToolsError::DuplicateHandlerName(name.clone()));
            }
        }
    }

    for name in &extension_tool_names {
        if !extension_handler_names.contains(name) {
            return Err(BuildToolsError::MissingHandlerForTool(name.clone()));
        }
    }

    let visibility = ToolVisibilitySnapshot::new(
        configured_specs
            .iter()
            .map(|configured| configured.spec.clone())
            .collect(),
    );

    let mut builder = ToolRegistryBuilder::new();

    let registered_tool_names = configured_specs
        .iter()
        .map(|configured| configured.spec.name.clone())
        .collect::<Vec<_>>();

    for configured_spec in configured_specs {
        builder.push_configured_spec(configured_spec);
    }

    let unified_exec_handler = Arc::new(UnifiedExecHandler::default());
    builder.register_handler("exec_command", unified_exec_handler.clone());
    builder.register_handler("write_stdin", unified_exec_handler);
    builder.register_handler("read_file", Arc::new(ReadFileHandler));
    builder.register_handler("list_dir", Arc::new(ListDirHandler));
    builder.register_handler("grep_files", Arc::new(GrepHandler));
    builder.register_handler("apply_patch", Arc::new(ApplyPatchHandler));
    builder.register_handler(
        "web_search",
        Arc::new(WebSearchHandler::new(web_tools_config.clone())),
    );
    builder.register_handler(
        "web_fetch",
        Arc::new(WebFetchHandler::new(web_tools_config.clone())),
    );
    builder.register_handler(
        "download_url",
        Arc::new(DownloadUrlHandler::new(web_tools_config)),
    );
    let blocked_tool_names = Arc::new(RwLock::new(BTreeMap::new()));

    builder.register_handler(
        REQUEST_TOOLS_TOOL_NAME,
        Arc::new(
            RequestToolsHandler::new(visibility.clone(), registered_tool_names)
                .with_shared_blocked_tool_names(blocked_tool_names.clone()),
        ),
    );

    for extension in extensions {
        for (name, handler) in extension.handlers {
            builder.register_dyn_handler(name, handler);
        }
    }

    let (configured_specs, registry) = builder.build();

    let event_bus = ToolEventBus::with_thread_id(
        512,
        permission_context
            .thread_id
            .clone()
            .unwrap_or_else(|| "unbound-thread".to_owned()),
    );

    let router = Arc::new(ToolRouter::new_with_blocked_tool_names(
        configured_specs,
        registry,
        visibility.clone(),
        event_bus.clone(),
        turn_id.clone(),
        blocked_tool_names,
    ));

    let orchestrator = Arc::new(ToolOrchestrator::new(OrchestratorPolicy::default()));

    let runtime = ToolCallRuntime::new(
        router.clone(),
        orchestrator,
        event_bus.clone(),
        turn_id,
        permission_context,
        workdir.into(),
    )
    .with_environment(environment)
    .with_execution_security_snapshot(execution_security_snapshot);

    Ok(BuiltinTools {
        router,
        runtime,
        visibility,
        event_bus,
    })
}

pub fn build_builtin_tools(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    permission_context: PermissionEvaluationContext,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
) -> BuiltinTools {
    build_builtin_tools_with_security_snapshot(
        workdir,
        turn_id,
        permission_context,
        web_tools_config,
        computer_use_tools_config,
        None,
    )
}

pub fn build_builtin_tools_with_security_snapshot(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    permission_context: PermissionEvaluationContext,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
) -> BuiltinTools {
    build_tools_with_environment_and_security_snapshot(
        workdir,
        turn_id,
        permission_context,
        web_tools_config,
        computer_use_tools_config,
        Vec::new(),
        BTreeMap::new(),
        execution_security_snapshot,
    )
    .expect("build builtin tools")
}

fn validate_permission_context(
    turn_id: &str,
    permission_context: &PermissionEvaluationContext,
) -> Result<(), BuildToolsError> {
    if permission_context.workspace_id.is_none() {
        return Err(BuildToolsError::InvalidPermissionContext(
            "workspace_id is required".to_owned(),
        ));
    }
    if permission_context.thread_id.is_none() {
        return Err(BuildToolsError::InvalidPermissionContext(
            "thread_id is required".to_owned(),
        ));
    }
    if permission_context.turn_id.as_deref() != Some(turn_id) {
        return Err(BuildToolsError::InvalidPermissionContext(
            "turn_id must match tool runtime turn".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BuildToolsError, BuiltinTools, ComputerUseToolsConfig, DynamicToolOutputPolicyCaps,
        McpDynamicToolAnnotations, McpDynamicToolDescriptor, McpToolCallOutput, McpToolCallRequest,
        McpToolExecutor, SkillDynamicToolDescriptor, SkillDynamicToolKind, ToolExtensionBundle,
        WebToolsConfig, materialize_mcp_runtime_tools, materialize_skill_runtime_tools,
    };
    use crate::context::{FunctionToolOutput, ToolInvocation};
    use crate::events::ToolEventTrace;
    use crate::output_policy::dynamic_unknown_output_policy;
    use crate::permissions::{
        PermissionActionKind, PermissionApprovalBroker, PermissionApprovalResolution,
        PermissionDecisionReason, PermissionEvaluationContext, PermissionIntent,
        PermissionRequestKey,
    };
    use crate::registry::ToolHandler;
    use crate::router::RawToolCall;
    use crate::spec::{
        ConfiguredToolSpec, ExecutionClass, PayloadKind, REQUEST_TOOLS_TOOL_NAME, ToolSpec,
    };
    use async_trait::async_trait;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct EchoHandler;

    #[async_trait]
    impl ToolHandler for EchoHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, crate::error::ToolError> {
            Ok(Box::new(FunctionToolOutput::new("ok", true)))
        }
    }

    struct CountingHandler {
        calls: Arc<AtomicUsize>,
    }

    struct RecordingApprovalBroker {
        actions: Arc<Mutex<Vec<PermissionActionKind>>>,
    }

    struct RecordingMcpExecutor {
        calls: Arc<Mutex<Vec<McpToolCallRequest>>>,
    }

    struct RecordingEffectHandler {
        calls: Arc<Mutex<Vec<String>>>,
    }

    struct CancellationObservingHandler {
        started: Arc<Notify>,
        cancellation_observed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl PermissionApprovalBroker for RecordingApprovalBroker {
        async fn request_approval(
            &self,
            _context: &PermissionEvaluationContext,
            _invocation: &ToolInvocation,
            intent: &PermissionIntent,
            _key: &PermissionRequestKey,
            _reason: PermissionDecisionReason,
        ) -> PermissionApprovalResolution {
            self.actions
                .lock()
                .expect("approval actions")
                .push(intent.action);
            PermissionApprovalResolution::AllowOnce
        }
    }

    #[async_trait]
    impl McpToolExecutor for RecordingMcpExecutor {
        async fn call_mcp_tool(
            &self,
            request: McpToolCallRequest,
            _trace: ToolEventTrace,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<McpToolCallOutput, crate::error::ToolError> {
            self.calls.lock().expect("MCP calls").push(request);
            Ok(McpToolCallOutput {
                content: serde_json::json!([{
                    "type": "text",
                    "text": "nested-mcp-ok"
                }]),
                structured_content: Some(serde_json::json!({"status": "nested-mcp-ok"})),
                is_error: false,
                duration_ms: 1,
                meta: None,
            })
        }
    }

    #[async_trait]
    impl ToolHandler for CountingHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, crate::error::ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FunctionToolOutput::new("ok", true)))
        }
    }

    #[async_trait]
    impl ToolHandler for RecordingEffectHandler {
        async fn handle(
            &self,
            invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, crate::error::ToolError> {
            self.calls
                .lock()
                .expect("effect calls")
                .push(invocation.tool_name);
            Ok(Box::new(FunctionToolOutput::with_payload(
                "effect-ok",
                true,
                serde_json::json!({"status": "effect-ok"}),
            )))
        }
    }

    #[async_trait]
    impl ToolHandler for CancellationObservingHandler {
        async fn handle(
            &self,
            invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, crate::error::ToolError> {
            let cancellation = invocation.cancellation.clone();
            let observed = self.cancellation_observed.clone();
            tokio::spawn(async move {
                cancellation.cancelled().await;
                observed.store(true, Ordering::SeqCst);
            });
            self.started.notify_one();
            std::future::pending::<
                Result<Box<dyn crate::context::ToolOutput>, crate::error::ToolError>,
            >()
            .await
        }
    }

    fn test_web_config() -> WebToolsConfig {
        WebToolsConfig {
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
            default_user_agent: "Mozilla/5.0".to_owned(),
        }
    }

    fn test_computer_use_config() -> ComputerUseToolsConfig {
        ComputerUseToolsConfig {
            runtime_home_dir: std::env::temp_dir().join("pioneer-tools-tests"),
            max_total_bytes: 1024 * 1024 * 1024,
            ..ComputerUseToolsConfig::default()
        }
    }

    fn test_permission_context(turn_id: &str) -> crate::PermissionEvaluationContext {
        crate::PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            turn_id,
            pioneer_protocol::default_turn_permission_profile_snapshot(),
        )
    }

    fn test_full_access_snapshot(
        workdir: &Path,
    ) -> pioneer_protocol::TurnExecutionSecuritySnapshot {
        pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
            workdir.to_string_lossy(),
            1,
        )
    }

    fn build_builtin_tools(
        workdir: impl Into<PathBuf>,
        turn_id: impl Into<String>,
        permission_context: crate::PermissionEvaluationContext,
        web_tools_config: WebToolsConfig,
        computer_use_tools_config: ComputerUseToolsConfig,
    ) -> BuiltinTools {
        let workdir = workdir.into();
        let security_snapshot = test_full_access_snapshot(workdir.as_path());
        super::build_builtin_tools_with_security_snapshot(
            workdir,
            turn_id,
            permission_context,
            web_tools_config,
            computer_use_tools_config,
            Some(security_snapshot),
        )
    }

    fn build_tools(
        workdir: impl Into<PathBuf>,
        turn_id: impl Into<String>,
        permission_context: crate::PermissionEvaluationContext,
        web_tools_config: WebToolsConfig,
        computer_use_tools_config: ComputerUseToolsConfig,
        extensions: Vec<ToolExtensionBundle>,
    ) -> Result<BuiltinTools, BuildToolsError> {
        let workdir = workdir.into();
        let security_snapshot = test_full_access_snapshot(workdir.as_path());
        super::build_tools_with_environment_and_security_snapshot(
            workdir,
            turn_id,
            permission_context,
            web_tools_config,
            computer_use_tools_config,
            extensions,
            std::collections::BTreeMap::new(),
            Some(security_snapshot),
        )
    }

    fn dynamic_skill_descriptor(
        canonical_tool_name: &str,
        skill_asset_root: &Path,
        kind: SkillDynamicToolKind,
        config: serde_json::Value,
    ) -> SkillDynamicToolDescriptor {
        SkillDynamicToolDescriptor {
            canonical_tool_name: canonical_tool_name.to_owned(),
            skill_id: pioneer_protocol::SkillId::new("S".repeat(21))
                .expect("valid dynamic skill id"),
            skill_owner: Some("workspace_test".to_owned()),
            skill_slug: "runtime-consent".to_owned(),
            skill_asset_root: skill_asset_root.to_string_lossy().into_owned(),
            skill_fingerprint: "f".repeat(64),
            source_kind: pioneer_skills::SkillSourceKind::User,
            trust_level: pioneer_skills::SkillTrustLevel::Internal,
            description: "Dynamic consent test".to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "additionalProperties": true
            }),
            execution_class: ExecutionClass::Shared,
            kind,
            config,
            requested_output_policy: None,
        }
    }

    fn supervised_test_context(turn_id: &str) -> PermissionEvaluationContext {
        PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            turn_id,
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        )
    }

    fn supervised_native_snapshot(
        workspace: &Path,
        app_read_root: Option<&Path>,
    ) -> pioneer_protocol::TurnExecutionSecuritySnapshot {
        let mut entries = vec![
            pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                pioneer_protocol::TurnFilesystemAccess::Read,
                workspace.to_string_lossy(),
            ),
        ];
        if let Some(root) = app_read_root {
            entries.push(pioneer_protocol::TurnFilesystemSandboxEntry {
                path: pioneer_protocol::TurnFilesystemSandboxPath::ExplicitPath {
                    path: root.to_string_lossy().into_owned(),
                },
                access: pioneer_protocol::TurnFilesystemAccess::Read,
                provenance: pioneer_protocol::TurnSecurityRuleProvenance::Runtime,
                resolved_path: Some(root.to_string_lossy().into_owned()),
            });
        }
        pioneer_protocol::TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            workspace.to_string_lossy(),
            entries,
            1,
        )
    }

    async fn one_response_http_server(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("dynamic-tool test listener");
        let address = listener.local_addr().expect("dynamic-tool test address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("dynamic-tool request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        (
            format!("http://localhost:{}/nested", address.port()),
            server,
        )
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn dynamic_shell_uses_one_semantic_prompt_and_real_native_sandbox() {
        if !nono::Sandbox::is_supported() {
            return;
        }

        let current = std::env::current_dir().expect("test cwd");
        let fixture = tempfile::tempdir_in(current).expect("dynamic shell fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skills/workspace_test/runtime-consent");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");
        let script = skill_root.join("run.sh");
        std::fs::write(
            script.as_path(),
            "#!/bin/sh\nprintf 'dynamic-ok' > dynamic-output.txt\n",
        )
        .expect("dynamic skill script");

        let tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.shell";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::Shell,
                serde_json::json!({
                    "command": ["/bin/sh", "${skill_asset_root}/run.sh"]
                }),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut snapshot =
            supervised_native_snapshot(workspace.as_path(), Some(skill_root.as_path()));
        snapshot.authority_cap.filesystem = pioneer_protocol::TurnFilesystemSandboxPolicy {
            kind: pioneer_protocol::TurnFilesystemSandboxKind::Restricted,
            entries: vec![
                pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                    pioneer_protocol::TurnFilesystemAccess::Write,
                    workspace.to_string_lossy(),
                ),
            ],
        };
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_shell_consent",
            supervised_test_context("turn_dynamic_shell_consent"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(snapshot),
        )
        .expect("dynamic shell tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_shell_consent".to_owned(),
                tool_name: tool_name.to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect("dynamic shell call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved dynamic shell should execute");

        assert!(result.success(), "{}", result.raw_output_text());
        assert_eq!(
            std::fs::read_to_string(workspace.join("dynamic-output.txt"))
                .expect("dynamic shell output"),
            "dynamic-ok"
        );
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::ShellCommand],
            "the non-effectful skill wrapper must not create a second prompt"
        );
    }

    #[tokio::test]
    async fn dynamic_function_proxy_prompts_once_for_target_file_read_and_applies_grant() {
        let fixture = tempfile::tempdir().expect("dynamic proxy fixture");
        let workspace = fixture.path().join("workspace");
        let outside = fixture.path().join("outside");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(outside.as_path()).expect("outside");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");
        let outside_file = outside.join("approved.txt");
        std::fs::write(outside_file.as_path(), "proxy-approved\n").expect("outside file");

        let tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.proxy";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::FunctionProxy,
                serde_json::json!({"target_tool": "read_file"}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_proxy_consent",
            supervised_test_context("turn_dynamic_proxy_consent"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_proxy_consent".to_owned(),
                tool_name: tool_name.to_owned(),
                arguments: serde_json::json!({
                    "arguments": {"path": outside_file}
                })
                .to_string(),
            })
            .expect("dynamic proxy call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved proxied read should execute");

        assert!(result.raw_output_text().contains("proxy-approved"));
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::FileRead],
            "the proxy wrapper must defer to exactly one target-specific prompt"
        );
    }

    #[tokio::test]
    async fn dynamic_function_proxy_apply_patch_creates_an_approved_missing_root() {
        let fixture = tempfile::tempdir().expect("dynamic patch proxy fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        let target = fixture
            .path()
            .join("downloads")
            .join("weekend-trip-plan")
            .join("README.md");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");

        let tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.patch_proxy";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::FunctionProxy,
                serde_json::json!({"target_tool": "apply_patch"}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let turn_id = "turn_dynamic_patch_proxy_consent";
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            turn_id,
            supervised_test_context(turn_id),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic patch proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;

        let nested_call_id = "call_dynamic_patch_proxy::apply_patch";
        let identity = crate::apply_patch::history::InvocationIdentity::new(
            "thread_test",
            turn_id,
            nested_call_id,
        )
        .expect("nested patch identity");
        assert!(crate::events::register_native_patch_observer(
            &identity,
            Arc::new(crate::apply_patch::DurableCommitObserver::default()),
        ));
        let patch = format!(
            "*** Begin Patch\n*** Add File: {}\n+approved through proxy\n*** End Patch",
            target.display()
        );
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_patch_proxy".to_owned(),
                tool_name: tool_name.to_owned(),
                arguments: serde_json::json!({
                    "arguments": {"patch": patch}
                })
                .to_string(),
            })
            .expect("dynamic patch proxy call");

        let result = tools.runtime.execute_tool_call(call).await;
        crate::events::unregister_native_patch_observer(&identity);
        let result = result.expect("approved proxied patch should execute");

        assert!(result.success(), "{}", result.raw_output_text());
        assert_eq!(
            std::fs::read_to_string(target).expect("proxied patch output"),
            "approved through proxy"
        );
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::FileWrite],
            "the proxy wrapper must defer to exactly one target-specific prompt"
        );
    }

    #[tokio::test]
    async fn two_dynamic_function_proxies_prompt_only_for_the_final_target() {
        let fixture = tempfile::tempdir().expect("two-level dynamic proxy fixture");
        let workspace = fixture.path().join("workspace");
        let outside = fixture.path().join("outside");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(outside.as_path()).expect("outside");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");
        let outside_file = outside.join("approved.txt");
        std::fs::write(outside_file.as_path(), "two-proxy-approved\n").expect("outside file");

        let outer_proxy = "skill.SSSSSSSSSSSSSSSSSSSSS.outer_proxy";
        let inner_proxy = "skill.SSSSSSSSSSSSSSSSSSSSS.inner_proxy";
        let materialization = materialize_skill_runtime_tools(
            &[
                dynamic_skill_descriptor(
                    outer_proxy,
                    skill_root.as_path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool": inner_proxy}),
                ),
                dynamic_skill_descriptor(
                    inner_proxy,
                    skill_root.as_path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool": "read_file"}),
                ),
            ],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_two_dynamic_proxies",
            supervised_test_context("turn_two_dynamic_proxies"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("two-level dynamic proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_two_dynamic_proxies".to_owned(),
                tool_name: outer_proxy.to_owned(),
                arguments: serde_json::json!({
                    "arguments": {"path": outside_file}
                })
                .to_string(),
            })
            .expect("two-level dynamic proxy call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved final target should execute");
        assert!(result.raw_output_text().contains("two-proxy-approved"));
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::FileRead],
            "non-effectful intermediate proxies must not create approvals"
        );
    }

    #[tokio::test]
    async fn dynamic_function_proxy_prompts_once_for_network_and_applies_exact_origin_grant() {
        let fixture = tempfile::tempdir().expect("dynamic network proxy fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");
        let (url, server) = one_response_http_server("nested-network-ok").await;

        let tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.network_proxy";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::FunctionProxy,
                serde_json::json!({"target_tool": "web_fetch"}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_network_proxy_consent",
            supervised_test_context("turn_dynamic_network_proxy_consent"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic network proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_network_proxy_consent".to_owned(),
                tool_name: tool_name.to_owned(),
                arguments: serde_json::json!({
                    "arguments": {"url": url, "max_bytes": 4096}
                })
                .to_string(),
            })
            .expect("dynamic network proxy call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved proxied network request should execute");

        assert!(
            result.raw_output_text().contains("nested-network-ok"),
            "{}",
            result.raw_output_text()
        );
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::Network],
            "the proxy wrapper must defer to exactly one target-specific network prompt"
        );
        server.await.expect("dynamic network test server");
    }

    #[tokio::test]
    async fn dynamic_function_proxy_prompts_once_for_mcp_and_preserves_target_binding() {
        let fixture = tempfile::tempdir().expect("dynamic MCP proxy fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");

        let mcp_tool_name = "mcp__mail__send";
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mcp_materialization = materialize_mcp_runtime_tools(
            &[McpDynamicToolDescriptor {
                callable_name: mcp_tool_name.to_owned(),
                workspace_id: "workspace_test".to_owned(),
                server_id: "mail-installation".to_owned(),
                server_name: "mail".to_owned(),
                raw_tool_name: "send".to_owned(),
                catalog_version: "catalog-v1".to_owned(),
                fingerprint: "m".repeat(64),
                snapshot_version: 7,
                description: "Send a test message".to_owned(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {"recipient": {"type": "string"}},
                    "required": ["recipient"],
                    "additionalProperties": false
                }),
                // Server-authored read-only claims must not suppress consent.
                annotations: McpDynamicToolAnnotations {
                    title: Some("Send".to_owned()),
                    read_only_hint: Some(true),
                    destructive_hint: Some(false),
                    idempotent_hint: Some(true),
                    open_world_hint: Some(false),
                },
                timeout_ms: Some(2_000),
                max_arguments_bytes: 16 * 1024,
                selection_reason: "test-selection".to_owned(),
                capability_id: Some("mcp-tool:mail:send".to_owned()),
            }],
            Arc::new(RecordingMcpExecutor {
                calls: calls.clone(),
            }),
        );
        assert!(mcp_materialization.excluded_tools.is_empty());

        let proxy_tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.mcp_proxy";
        let skill_materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                proxy_tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::FunctionProxy,
                serde_json::json!({"target_tool": mcp_tool_name}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(skill_materialization.excluded_tools.is_empty());
        let mut extensions = skill_materialization.bundles.clone();
        extensions.extend(mcp_materialization.bundles.clone());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_mcp_proxy_consent",
            supervised_test_context("turn_dynamic_mcp_proxy_consent"),
            test_web_config(),
            test_computer_use_config(),
            extensions,
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic MCP proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        skill_materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_mcp_proxy_consent".to_owned(),
                tool_name: proxy_tool_name.to_owned(),
                arguments: serde_json::json!({
                    "arguments": {"recipient": "approved@example.test"}
                })
                .to_string(),
            })
            .expect("dynamic MCP proxy call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved proxied MCP call should execute");

        assert!(result.raw_output_text().contains("nested-mcp-ok"));
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::McpWriteOrUnknown],
            "the proxy wrapper must defer to exactly one MCP target prompt"
        );
        let calls = calls.lock().expect("MCP calls");
        assert_eq!(calls.len(), 1, "MCP target must execute exactly once");
        assert_eq!(calls[0].server_id, "mail-installation");
        assert_eq!(calls[0].raw_tool_name, "send");
        assert_eq!(
            calls[0].arguments,
            serde_json::json!({"recipient": "approved@example.test"})
        );
    }

    #[tokio::test]
    async fn dynamic_function_proxies_defer_once_to_task_and_agent_target_permissions() {
        let fixture = tempfile::tempdir().expect("dynamic task/agent proxy fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");

        let effect_calls = Arc::new(Mutex::new(Vec::new()));
        let mut effect_bundle = ToolExtensionBundle::default();
        for target in ["task_create", "threads_turn_start"] {
            effect_bundle.specs.push(ConfiguredToolSpec::new(
                ToolSpec::new(
                    target,
                    format!("Test target {target}"),
                    serde_json::json!({"type": "object", "additionalProperties": true}),
                    PayloadKind::Function,
                ),
                ExecutionClass::Shared,
                dynamic_unknown_output_policy(),
            ));
            effect_bundle.handlers.push((
                target.to_owned(),
                Arc::new(RecordingEffectHandler {
                    calls: effect_calls.clone(),
                }) as Arc<dyn ToolHandler>,
            ));
        }

        let task_proxy = "skill.SSSSSSSSSSSSSSSSSSSSS.task_proxy";
        let agent_proxy = "skill.SSSSSSSSSSSSSSSSSSSSS.agent_proxy";
        let skill_materialization = materialize_skill_runtime_tools(
            &[
                dynamic_skill_descriptor(
                    task_proxy,
                    skill_root.as_path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool": "task_create"}),
                ),
                dynamic_skill_descriptor(
                    agent_proxy,
                    skill_root.as_path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool": "threads_turn_start"}),
                ),
            ],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(skill_materialization.excluded_tools.is_empty());
        let mut extensions = skill_materialization.bundles.clone();
        extensions.push(effect_bundle);
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_task_agent_proxy_consent",
            supervised_test_context("turn_dynamic_task_agent_proxy_consent"),
            test_web_config(),
            test_computer_use_config(),
            extensions,
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic task/agent proxy tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        skill_materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;

        for (call_id, proxy, arguments) in [
            (
                "call_dynamic_task_proxy_consent",
                task_proxy,
                serde_json::json!({"title": "Nested task", "goal": "Run safely"}),
            ),
            (
                "call_dynamic_agent_proxy_consent",
                agent_proxy,
                serde_json::json!({
                    "targetOptionId": "native-agent",
                    "input": {"prompt": "Run safely"}
                }),
            ),
        ] {
            let call = tools
                .router
                .build_tool_call(RawToolCall {
                    call_id: call_id.to_owned(),
                    tool_name: proxy.to_owned(),
                    arguments: serde_json::json!({"arguments": arguments}).to_string(),
                })
                .expect("dynamic task/agent proxy call");
            let result = tools
                .runtime
                .execute_tool_call(call)
                .await
                .expect("approved proxied task/agent target should execute");
            assert!(result.raw_output_text().contains("effect-ok"));
        }

        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![
                PermissionActionKind::TaskSubagent,
                PermissionActionKind::AgentAction,
            ],
            "each proxy must defer to one semantic target prompt"
        );
        assert_eq!(
            *effect_calls.lock().expect("effect calls"),
            vec!["task_create".to_owned(), "threads_turn_start".to_owned()],
            "each target side effect must execute exactly once"
        );
    }

    #[tokio::test]
    async fn dynamic_http_tool_prompts_once_and_executes_with_exact_origin_grant() {
        let fixture = tempfile::tempdir().expect("dynamic HTTP fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");
        let (url, server) = one_response_http_server("dynamic-http-ok").await;

        let tool_name = "skill.SSSSSSSSSSSSSSSSSSSSS.http";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                tool_name,
                skill_root.as_path(),
                SkillDynamicToolKind::Http,
                serde_json::json!({"method": "POST", "url": url}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_http_consent",
            supervised_test_context("turn_dynamic_http_consent"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic HTTP tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_http_consent".to_owned(),
                tool_name: tool_name.to_owned(),
                arguments: serde_json::json!({
                    "body": {"value": "approved-body"}
                })
                .to_string(),
            })
            .expect("dynamic HTTP call");

        let result = tools
            .runtime
            .execute_tool_call(call)
            .await
            .expect("approved dynamic HTTP request should execute");

        assert!(result.raw_output_text().contains("dynamic-http-ok"));
        assert_eq!(
            *actions.lock().expect("approval actions"),
            vec![PermissionActionKind::Network],
            "dynamic HTTP must use exactly one network prompt"
        );
        server.await.expect("dynamic HTTP test server");
    }

    #[tokio::test]
    async fn dynamic_function_proxy_propagates_outer_cancellation_to_nested_target_token() {
        let fixture = tempfile::tempdir().expect("dynamic cancellation fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");

        let started = Arc::new(Notify::new());
        let cancellation_observed = Arc::new(AtomicBool::new(false));
        let mut target_bundle = ToolExtensionBundle::default();
        target_bundle.specs.push(ConfiguredToolSpec::new(
            ToolSpec::new(
                "blocking_effect",
                "Wait for cancellation",
                serde_json::json!({"type": "object", "additionalProperties": true}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        ));
        target_bundle.handlers.push((
            "blocking_effect".to_owned(),
            Arc::new(CancellationObservingHandler {
                started: started.clone(),
                cancellation_observed: cancellation_observed.clone(),
            }) as Arc<dyn ToolHandler>,
        ));

        let proxy_tool = "skill.SSSSSSSSSSSSSSSSSSSSS.cancel_proxy";
        let materialization = materialize_skill_runtime_tools(
            &[dynamic_skill_descriptor(
                proxy_tool,
                skill_root.as_path(),
                SkillDynamicToolKind::FunctionProxy,
                serde_json::json!({"target_tool": "blocking_effect"}),
            )],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        let mut extensions = materialization.bundles.clone();
        extensions.push(target_bundle);
        let tools = super::build_tools_with_environment_and_security_snapshot(
            workspace.clone(),
            "turn_dynamic_proxy_cancellation",
            supervised_test_context("turn_dynamic_proxy_cancellation"),
            test_web_config(),
            test_computer_use_config(),
            extensions,
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(workspace.as_path(), None)),
        )
        .expect("dynamic cancellation tools should build")
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: Arc::new(Mutex::new(Vec::new())),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_proxy_cancellation".to_owned(),
                tool_name: proxy_tool.to_owned(),
                arguments: serde_json::json!({"arguments": {}}).to_string(),
            })
            .expect("dynamic cancellation call");
        let cancellation = tokio_util::sync::CancellationToken::new();
        let runtime = tools.runtime.clone();
        let run_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            runtime
                .execute_tool_call_with_cancellation(call, run_cancellation)
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
            .await
            .expect("nested target should start");
        cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("cancelled proxy should finish")
            .expect("proxy task should not panic");
        assert!(matches!(result, Err(crate::error::ToolError::Cancelled(_))));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !cancellation_observed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("nested target must observe the exact outer cancellation token");
    }

    #[tokio::test]
    async fn dynamic_function_proxy_cycle_is_rejected_at_bounded_depth() {
        let fixture = tempfile::tempdir().expect("dynamic proxy cycle fixture");
        let workspace = fixture.path().join("workspace");
        let skill_root = fixture.path().join("skill");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(skill_root.as_path()).expect("skill root");

        let proxy_names = (0..9)
            .map(|index| format!("skill.SSSSSSSSSSSSSSSSSSSSS.cycle_{index}"))
            .collect::<Vec<_>>();
        let descriptors = proxy_names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let target_tool = proxy_names[(index + 1) % proxy_names.len()].clone();
                dynamic_skill_descriptor(
                    name,
                    skill_root.as_path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool": target_tool}),
                )
            })
            .collect::<Vec<_>>();
        let materialization = materialize_skill_runtime_tools(
            descriptors.as_slice(),
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        assert!(materialization.excluded_tools.is_empty());
        let tools = build_tools(
            workspace.clone(),
            "turn_dynamic_proxy_cycle",
            test_permission_context("turn_dynamic_proxy_cycle"),
            test_web_config(),
            test_computer_use_config(),
            materialization.bundles.clone(),
        )
        .expect("dynamic cycle tools should build");
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        let call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_dynamic_proxy_cycle".to_owned(),
                tool_name: proxy_names[0].clone(),
                arguments: serde_json::json!({"arguments": {}}).to_string(),
            })
            .expect("dynamic cycle call");

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tools.runtime.execute_tool_call(call),
        )
        .await
        .expect("cycle must terminate at the configured bound");
        match result {
            Err(crate::error::ToolError::Rejected(message)) => assert!(
                message.contains("maximum depth"),
                "unexpected bounded rejection: {message}"
            ),
            Err(error) => panic!("cycle should preserve a typed rejection: {error}"),
            Ok(output) => panic!(
                "cycle should not execute successfully: {}",
                output.raw_output_text()
            ),
        }
    }

    #[tokio::test]
    async fn build_tools_merges_builtin_and_extension_specs() {
        let extension_spec = ConfiguredToolSpec::new(
            ToolSpec::new(
                "skill.test.echo",
                "Echo tool",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );

        let built = build_tools(
            ".",
            "turn_merge",
            test_permission_context("turn_merge"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: vec![extension_spec],
                handlers: vec![("skill.test.echo".to_owned(), Arc::new(EchoHandler))],
            }],
        )
        .expect("tools must build");

        assert!(built.router.find_spec("read_file").is_some());
        assert!(built.router.find_spec("skill.test.echo").is_some());
        assert!(
            built
                .visibility
                .all_specs()
                .iter()
                .any(|spec| spec.name == "skill.test.echo")
        );
        let visible = built.visibility.get().await;
        assert!(visible.iter().any(|spec| spec.name == "skill.test.echo"));

        let call = built
            .router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "skill.test.echo".to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect("tool call must parse");

        let result = built
            .runtime
            .execute_tool_call(call)
            .await
            .expect("tool execution should succeed");
        assert_eq!(result.raw_output_text(), "ok");
    }

    #[test]
    fn web_search_permission_metadata_uses_normalized_configured_endpoints() {
        let mut config = test_web_config();
        config.ddg_html_search_url = "https://search.internal.example/html/".to_owned();
        config.ddg_instant_api_url = "https://instant.internal.example/api/".to_owned();
        let built = build_builtin_tools(
            ".",
            "turn_search_targets",
            test_permission_context("turn_search_targets"),
            config,
            test_computer_use_config(),
        );

        let spec = built
            .router
            .find_spec("web_search")
            .expect("web_search spec");
        assert_eq!(
            spec.spec.permission_metadata.network_targets,
            vec![
                "https://search.internal.example/html/".to_owned(),
                "https://instant.internal.example/api/".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn request_tools_is_in_default_model_visible_specs() {
        let built = build_builtin_tools(
            ".",
            "turn_request_tools_visible",
            test_permission_context("turn_request_tools_visible"),
            test_web_config(),
            test_computer_use_config(),
        );

        let visible = built.router.model_visible_specs().await;
        assert!(
            visible.iter().any(|spec| spec.name == "request_tools"),
            "request_tools must be visible in the default tool-enabled provider catalog"
        );
        assert!(
            built.router.has_handler("request_tools"),
            "request_tools must have a runtime handler"
        );
    }

    #[test]
    fn removed_file_mutators_are_absent_from_catalog_and_runtime() {
        let built = build_builtin_tools(
            ".",
            "turn_removed_file_mutators",
            test_permission_context("turn_removed_file_mutators"),
            test_web_config(),
            test_computer_use_config(),
        );

        for tool_name in ["write_file", "edit_file"] {
            assert!(
                built.router.find_spec(tool_name).is_none(),
                "removed tool {tool_name} must not have a visible schema"
            );
            assert!(
                !built.router.has_handler(tool_name),
                "removed tool {tool_name} must not have a runtime handler"
            );
            let error = built
                .router
                .build_tool_call(RawToolCall {
                    call_id: format!("call_{tool_name}"),
                    tool_name: tool_name.to_owned(),
                    arguments: "{}".to_owned(),
                })
                .expect_err("removed file mutators must be rejected as unknown tools");
            assert!(
                matches!(error, crate::ToolError::NotFound(_)),
                "removed tool {tool_name} should fail before permission or handler dispatch"
            );
        }
    }

    #[tokio::test]
    async fn computer_use_registration_matches_build_and_backend_and_starts_hidden() {
        for backend_available in [false, true] {
            let mut config = test_computer_use_config();
            if backend_available {
                config.backend = Some(Arc::new(UnconfirmedCuaBackend::default()));
            }
            let built = build_builtin_tools(
                ".",
                "turn_computer_use_hidden_default",
                test_permission_context("turn_computer_use_hidden_default"),
                test_web_config(),
                config,
            );

            // A unified tools feature alone cannot make backend-less execution
            // available. The real registry owns this contract, before visibility.
            assert_eq!(
                built.router.find_spec("cua_type_text").is_some(),
                cfg!(feature = "computer-use") && backend_available
            );
            let visible = built.router.model_visible_specs().await;
            assert!(
                !visible
                    .iter()
                    .any(|spec| crate::is_computer_use_tool(&spec.name)),
                "computer_use schema must not be visible in the default provider catalog"
            );
        }
    }

    #[tokio::test]
    async fn request_tools_runtime_returns_compact_domain_result() {
        let extension_specs = ["artifact_prepare", "artifact_register", "artifact_read"]
            .into_iter()
            .map(|name| {
                ConfiguredToolSpec::new(
                    ToolSpec::new(
                        name,
                        "Artifact domain test tool",
                        serde_json::json!({"type":"object"}),
                        PayloadKind::Function,
                    ),
                    ExecutionClass::Shared,
                    dynamic_unknown_output_policy(),
                )
            })
            .collect::<Vec<_>>();

        let built = build_tools(
            ".",
            "turn_request_tools_result",
            test_permission_context("turn_request_tools_result"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: extension_specs,
                handlers: vec![
                    ("artifact_prepare".to_owned(), Arc::new(EchoHandler)),
                    ("artifact_register".to_owned(), Arc::new(EchoHandler)),
                    ("artifact_read".to_owned(), Arc::new(EchoHandler)),
                ],
            }],
        )
        .expect("tools must build");

        built
            .router
            .set_model_visible_tools(&["request_tools".to_owned(), "read_file".to_owned()])
            .await;

        let call = built
            .router
            .build_model_tool_call(RawToolCall {
                call_id: "call_request_tools_result".to_owned(),
                tool_name: "request_tools".to_owned(),
                arguments: serde_json::json!({
                    "domains": ["artifact", "memory", "artifact"],
                    "reason": "Need artifact and memory tools."
                })
                .to_string(),
            })
            .await
            .expect("request_tools call should parse");

        let result = built
            .runtime
            .execute_tool_call(call)
            .await
            .expect("request_tools should execute");
        let output = serde_json::from_value::<crate::RequestToolsResult>(result.raw_output_json())
            .expect("request_tools output should match compact result contract");

        assert_eq!(
            output.added.get("artifact"),
            Some(&vec![
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned()
            ])
        );
        assert_eq!(output.unknown_or_unavailable.len(), 1);
        assert_eq!(output.unknown_or_unavailable[0].domain, "memory");
        assert!(output.already_visible.is_empty());
        assert!(output.blocked.is_empty());

        let model_text = result.model_visible_text();
        assert!(model_text.contains("\"added\""));
        assert!(!model_text.contains("\"parameters\""));
        assert!(!model_text.contains("\"properties\""));
        assert!(!model_text.contains("\"additionalProperties\""));
    }

    #[tokio::test]
    async fn materialized_dynamic_tool_is_not_hidden_by_model_gate() {
        let extension_spec = ConfiguredToolSpec::new(
            ToolSpec::new(
                "skill.test.visible-dynamic",
                "Visible dynamic tool",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );

        let built = build_tools(
            ".",
            "turn_dynamic_visible",
            test_permission_context("turn_dynamic_visible"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: vec![extension_spec],
                handlers: vec![(
                    "skill.test.visible-dynamic".to_owned(),
                    Arc::new(EchoHandler),
                )],
            }],
        )
        .expect("tools must build");

        let call = built
            .router
            .build_model_tool_call(RawToolCall {
                call_id: "call_dynamic_visible".to_owned(),
                tool_name: "skill.test.visible-dynamic".to_owned(),
                arguments: "{}".to_owned(),
            })
            .await
            .expect("materialized dynamic tool should be visible by existing tool pipeline");

        let result = built
            .runtime
            .execute_tool_call(call)
            .await
            .expect("dynamic tool execution should succeed");

        assert_eq!(result.raw_output_text(), "ok");
    }

    #[tokio::test]
    async fn hidden_registered_tool_is_not_dispatched_to_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let extension_spec = ConfiguredToolSpec::new(
            ToolSpec::new(
                "skill.test.hidden-dynamic",
                "Hidden dynamic tool",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );

        let built = build_tools(
            ".",
            "turn_hidden_dispatch",
            test_permission_context("turn_hidden_dispatch"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: vec![extension_spec],
                handlers: vec![(
                    "skill.test.hidden-dynamic".to_owned(),
                    Arc::new(CountingHandler {
                        calls: calls.clone(),
                    }),
                )],
            }],
        )
        .expect("tools must build");

        built
            .router
            .set_model_visible_tools(&["read_file".to_owned()])
            .await;

        let error = built
            .router
            .build_model_tool_call(RawToolCall {
                call_id: "call_hidden_dynamic".to_owned(),
                tool_name: "skill.test.hidden-dynamic".to_owned(),
                arguments: "{}".to_owned(),
            })
            .await
            .expect_err("hidden registered tool should be rejected before dispatch");

        assert!(matches!(error, crate::ToolError::NotVisible(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn build_tools_rejects_duplicate_tool_names() {
        let extension_spec = ConfiguredToolSpec::new(
            ToolSpec::new(
                "read_file",
                "Conflicts with builtin",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );

        let error = match build_tools(
            ".",
            "turn_dup",
            test_permission_context("turn_dup"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: vec![extension_spec],
                handlers: vec![("read_file".to_owned(), Arc::new(EchoHandler))],
            }],
        ) {
            Ok(_) => panic!("duplicate name should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, BuildToolsError::DuplicateToolName(_)));
    }

    #[test]
    fn build_tools_rejects_missing_handler_for_extension_spec() {
        let extension_spec = ConfiguredToolSpec::new(
            ToolSpec::new(
                "skill.test.nohandler",
                "No handler",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        );

        let error = match build_tools(
            ".",
            "turn_missing_handler",
            test_permission_context("turn_missing_handler"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: vec![extension_spec],
                handlers: Vec::new(),
            }],
        ) {
            Ok(_) => panic!("missing handler should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, BuildToolsError::MissingHandlerForTool(_)));
    }

    #[test]
    fn build_tools_rejects_permission_context_for_another_turn() {
        let error = match build_tools(
            ".",
            "turn_expected",
            test_permission_context("turn_other"),
            test_web_config(),
            test_computer_use_config(),
            Vec::new(),
        ) {
            Ok(_) => panic!("mismatched permission context should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            BuildToolsError::InvalidPermissionContext(_)
        ));
    }
    #[derive(Debug, Default)]
    struct UnconfirmedCuaBackend {
        failure: std::sync::atomic::AtomicUsize,
        calls: std::sync::atomic::AtomicUsize,
        releases: std::sync::atomic::AtomicUsize,
        observations_delivered: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl crate::ComputerUseBackend for UnconfirmedCuaBackend {
        async fn execute(
            &self,
            _owner: &str,
            name: &str,
            _arguments: serde_json::Value,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<serde_json::Value, crate::ToolError> {
            if name == "get_window_state" {
                return Ok(
                    serde_json::json!({"content":[{"type":"image","mimeType":"image/png","data":"AAECAw=="}],"structuredContent":{"fixture_observation":true},"isError":false}),
                );
            }
            assert_eq!(name, "type_text");
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.failure.load(std::sync::atomic::Ordering::SeqCst) {
                1 => return Err(crate::ToolError::execution_failed("mutation response lost")),
                2 => return Err(crate::ToolError::cancelled("mutation response cancelled")),
                _ => {}
            }
            // Transport fixture: no OS capability or successful GUI effect is claimed.
            Ok(
                serde_json::json!({"content":[{"type":"text","text":"input delivery unverified"},{"type":"image","mimeType":"image/png","data":"AAECAw=="}],"structuredContent":{"action":{"effect":"unverifiable"},"capture_origin":{"x":-8,"y":3}},"isError":false}),
            )
        }
        fn observation_delivered(&self, _owner: &str, _context: &serde_json::Value) -> bool {
            self.observations_delivered.fetch_add(1, Ordering::SeqCst);
            true // Transport fixture only; no OS readiness claim.
        }
        fn release_owner(&self, _owner: &str) {
            self.releases
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[derive(Debug, Default)]
    struct ProxyImageSink {
        block_next: AtomicBool,
        reject: AtomicBool,
        entered: Notify,
        resume: Notify,
    }
    #[async_trait]
    impl crate::handlers::computer_use::ComputerUseImageSink for ProxyImageSink {
        async fn admit(
            &self,
            _owner: &str,
            _call_id: &str,
            _index: usize,
            _bytes: Vec<u8>,
            mime: &str,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<pioneer_provider::MessageAttachment, crate::ToolError> {
            if self.block_next.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if self.reject.load(Ordering::SeqCst) {
                return Err(crate::ToolError::execution_failed(
                    "artifact admission denied",
                ));
            }
            let mut image = pioneer_provider::MessageAttachment::from_path(
                "/artifacts/proxy-capture.png",
                mime,
            );
            image.artifact = Some(pioneer_provider::AttachmentArtifactContext {
                workspace_id: "workspace".into(),
                artifact_id: "capture".into(),
                artifact_version_id: Some("v1".into()),
            });
            Ok(image)
        }
    }
    // The existing transport owner fixture exercises the real CUA handler,
    // artifact admission and runtime publication; it claims no GUI/OS success.
    #[cfg(feature = "computer-use")]
    #[tokio::test]
    async fn cua_observation_delivery_guards_admission_and_publication() {
        use std::future::Future;
        use tokio_util::sync::CancellationToken;

        fn fixture_tools(
            root: &Path,
            backend: Arc<UnconfirmedCuaBackend>,
            sink: Arc<ProxyImageSink>,
        ) -> BuiltinTools {
            let image_admission =
                crate::handlers::computer_use::ComputerUseImageAdmission::default();
            image_admission.set(sink);
            super::build_tools_with_environment_and_security_snapshot(
                root,
                "turn_delivery",
                supervised_test_context("turn_delivery"),
                test_web_config(),
                ComputerUseToolsConfig {
                    backend: Some(backend),
                    image_admission,
                    ..test_computer_use_config()
                },
                Vec::new(),
                std::collections::BTreeMap::new(),
                Some(supervised_native_snapshot(root, None)),
            )
            .unwrap()
            .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
                actions: Arc::new(Mutex::new(Vec::new())),
            }))
        }
        fn call(tools: &BuiltinTools, capture: bool) -> crate::ToolCall {
            tools.router.build_tool_call(RawToolCall {
                call_id: if capture { "observation" } else { "input" }.into(),
                tool_name: if capture { "cua_get_window_state" } else { "cua_type_text" }.into(),
                arguments: if capture {
                    serde_json::json!({"pid":7,"window_id":9,"include_screenshot":true})
                } else {
                    serde_json::json!({"text":"private input","target":{"kind":"window","pid":7,"window_id":9}})
                }.to_string(),
            }).unwrap()
        }

        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(UnconfirmedCuaBackend::default());
        let sink = Arc::new(ProxyImageSink::default());
        sink.block_next.store(true, Ordering::SeqCst);
        sink.reject.store(true, Ordering::SeqCst);
        let tools = fixture_tools(root.path(), backend.clone(), sink.clone());
        let mut observation = Box::pin(tools.runtime.execute_tool_call(call(&tools, true)));
        tokio::select! {
            _ = sink.entered.notified() => {},
            _ = &mut observation => panic!("capture must wait for artifact admission"),
        }
        let mut input = Box::pin(tools.runtime.execute_tool_call(call(&tools, false)));
        std::future::poll_fn(|cx| {
            assert!(input.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        assert_eq!(backend.observations_delivered.load(Ordering::SeqCst), 0);
        sink.resume.notify_one();
        assert!(observation.await.is_err());
        assert!(matches!(input.await, Err(crate::ToolError::Rejected(_))));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        assert!(backend.releases.load(Ordering::SeqCst) > 0);

        // Admission succeeded, but the real durable completion boundary can
        // still reject, lose its acknowledgement, cancel, or lose its caller.
        for fault in ["rejected", "ack_dropped", "cancelled", "caller_dropped"] {
            let backend = Arc::new(UnconfirmedCuaBackend::default());
            let tools = fixture_tools(
                root.path(),
                backend.clone(),
                Arc::new(ProxyImageSink::default()),
            );
            let mut durable = tools.runtime.event_bus().take_durable_receiver().unwrap();
            let cancellation = CancellationToken::new();
            let mut observation = Box::pin(
                tools
                    .runtime
                    .execute_tool_call_with_cancellation(call(&tools, true), cancellation.clone()),
            );
            let completion = loop {
                tokio::select! {
                    envelope = durable.recv() => {
                        let envelope = envelope.unwrap();
                        if envelope.event.kind() == crate::ToolEventKind::CallCompleted {
                            break envelope;
                        }
                        envelope.acknowledge(Ok(()));
                    },
                    _ = &mut observation => panic!("observation must wait for durable completion"),
                }
            };
            assert_eq!(backend.observations_delivered.load(Ordering::SeqCst), 0);
            match fault {
                "rejected" => {
                    completion.acknowledge(Err("storage unavailable".into()));
                    assert!(observation.await.is_err());
                }
                "ack_dropped" => {
                    drop(completion);
                    assert!(observation.await.is_err());
                }
                "cancelled" => {
                    cancellation.cancel();
                    completion.acknowledge(Ok(()));
                    assert!(observation.await.is_err());
                }
                _ => {
                    drop(observation);
                    drop(completion);
                }
            }
            assert_eq!(backend.observations_delivered.load(Ordering::SeqCst), 0);
            assert!(backend.releases.load(Ordering::SeqCst) > 0);
            let mut input = Box::pin(tools.runtime.execute_tool_call(call(&tools, false)));
            let result = loop {
                tokio::select! {
                    envelope = durable.recv() => envelope.unwrap().acknowledge(Ok(())),
                    result = &mut input => break result,
                }
            };
            assert!(
                matches!(result, Err(crate::ToolError::Rejected(_))),
                "{fault}"
            );
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0, "{fault}");
        }
    }

    #[cfg(feature = "computer-use")]
    #[tokio::test]
    async fn two_skill_proxies_preserve_cua_permission_unknown_outcome_and_owner_cleanup() {
        let fixture = tempfile::tempdir().unwrap();
        let outer = "skill.SSSSSSSSSSSSSSSSSSSSS.outer_cua";
        let inner = "skill.SSSSSSSSSSSSSSSSSSSSS.inner_cua";
        let materialization = materialize_skill_runtime_tools(
            &[
                dynamic_skill_descriptor(
                    outer,
                    fixture.path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool":inner}),
                ),
                dynamic_skill_descriptor(
                    inner,
                    fixture.path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool":"cua_type_text"}),
                ),
                dynamic_skill_descriptor(
                    "skill.SSSSSSSSSSSSSSSSSSSSS.unrelated",
                    fixture.path(),
                    SkillDynamicToolKind::FunctionProxy,
                    serde_json::json!({"target_tool":"read_file"}),
                ),
            ],
            None,
            DynamicToolOutputPolicyCaps::default(),
        );
        let backend = Arc::new(UnconfirmedCuaBackend::default());
        let actions = Arc::new(Mutex::new(Vec::new()));
        let image_admission = crate::handlers::computer_use::ComputerUseImageAdmission::default();
        image_admission.set(Arc::new(ProxyImageSink::default()));
        let tools = super::build_tools_with_environment_and_security_snapshot(
            fixture.path(),
            "turn_proxy_cua",
            supervised_test_context("turn_proxy_cua"),
            test_web_config(),
            ComputerUseToolsConfig {
                backend: Some(backend.clone()),
                image_admission,
                ..test_computer_use_config()
            },
            materialization.bundles.clone(),
            std::collections::BTreeMap::new(),
            Some(supervised_native_snapshot(fixture.path(), None)),
        )
        .unwrap()
        .with_permission_approval_broker(Arc::new(RecordingApprovalBroker {
            actions: actions.clone(),
        }));
        materialization
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        async fn observe(tools: &super::BuiltinTools) {
            let call = tools
                .router
                .build_tool_call(RawToolCall {
                    call_id: "fresh_observation".into(),
                    tool_name: "cua_get_window_state".into(),
                    arguments: serde_json::json!({"pid":7,"window_id":9,"include_screenshot":true})
                        .to_string(),
                })
                .unwrap();
            tools
                .runtime
                .execute_tool_call(call)
                .await
                .expect("transport observation delivered");
        }
        observe(&tools).await;
        actions.lock().unwrap().clear();
        let call = tools.router.build_tool_call(RawToolCall { call_id:"proxy_cua".to_owned(), tool_name:outer.to_owned(), arguments:serde_json::json!({"arguments":{"text":"private input", "target":{"kind":"window","pid":7,"window_id":9}}}).to_string() }).unwrap();
        let result = tools.runtime.execute_tool_call(call).await.unwrap();
        assert_eq!(
            result.outcome.status,
            crate::ToolOutcomeStatus::PartialSuccess
        );
        assert!(!result.outcome.should_retry);
        assert!(result.outcome.incomplete);
        let message = result.to_model_input_item().into_chat_message();
        assert_eq!(message.tool_call_id.as_deref(), Some("proxy_cua"));
        assert_eq!(message.name.as_deref(), Some(outer));
        assert!(message.content_parts.iter().any(|part| matches!(part,
            pioneer_provider::MessageContentPart::Image { image }
            if image.artifact.as_ref().is_some_and(|artifact| artifact.artifact_version_id.as_deref() == Some("v1")))));
        assert!(message.content_parts.iter().any(|part| matches!(part,
            pioneer_provider::MessageContentPart::Text { text } if text.contains("capture_origin"))));
        let technical = serde_json::to_string(&result.projection().unwrap().storage).unwrap();
        assert!(!technical.contains("capture_origin"));
        assert!(!technical.contains("private input"));
        assert_eq!(
            *actions.lock().unwrap(),
            vec![PermissionActionKind::ComputerUse]
        );
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // Real backend Err must retain no-replay through both configured depths,
        // including the classifier used by the agent's model/history boundary.
        for failure in [1, 2] {
            backend
                .failure
                .store(failure, std::sync::atomic::Ordering::SeqCst);
            for name in [inner, outer] {
                observe(&tools).await;
                let call = tools.router.build_tool_call(RawToolCall {
                    call_id: format!("proxy_err_{failure}_{name}"),
                    tool_name: name.to_owned(),
                    arguments: serde_json::json!({"arguments":{"text":"private input", "target":{"kind":"window","pid":7,"window_id":9}}}).to_string(),
                }).unwrap();
                assert_eq!(call.recovery.retry_class, crate::ToolRetryClass::Never);
                let mut events = tools.runtime.event_bus().subscribe();
                let error = tools
                    .runtime
                    .execute_tool_call(call)
                    .await
                    .err()
                    .expect("backend Err");
                assert!(matches!(
                    error,
                    crate::ToolError::ExecutionFailed(_) | crate::ToolError::Cancelled(_)
                ));
                let outcome = tools.router.classify_tool_error(name, &error);
                assert!(!outcome.should_retry);
                assert_eq!(outcome.status, crate::ToolOutcomeStatus::FatalError);
                let mut failed = 0;
                while let Ok(event) = events.try_recv() {
                    if let crate::ToolEventPayload::CallFailed(event) = event.payload {
                        failed += 1;
                        assert!(!event.outcome.should_retry);
                        assert_eq!(event.outcome.status, crate::ToolOutcomeStatus::FatalError);
                    }
                }
                assert_eq!(failed, if name == inner { 2 } else { 3 });
            }
        }
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 5);
        let unrelated = tools.router.classify_tool_error(
            "skill.SSSSSSSSSSSSSSSSSSSSS.unrelated",
            &crate::ToolError::execution_failed("mutation response lost"),
        );
        assert!(unrelated.should_retry);
        let unrelated_call = tools
            .router
            .build_tool_call(RawToolCall {
                call_id: "unrelated_control".into(),
                tool_name: "skill.SSSSSSSSSSSSSSSSSSSSS.unrelated".into(),
                arguments: "{}".into(),
            })
            .unwrap();
        assert_eq!(
            unrelated_call.recovery,
            tools
                .router
                .find_spec(&unrelated_call.tool_name)
                .unwrap()
                .spec
                .recovery
        );
        let releases = backend.releases.load(std::sync::atomic::Ordering::SeqCst);
        materialization.clear_function_proxy_runtime().await;
        drop(tools);
        assert_eq!(
            backend.releases.load(std::sync::atomic::Ordering::SeqCst),
            releases + 1
        );
    }
}

pub mod html;
