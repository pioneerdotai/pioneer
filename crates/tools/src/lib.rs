mod argument_normalizer;
mod classifier;
mod context;
mod domain;
mod error;
mod events;
mod loop_guard;
mod orchestrator;
mod output_dynamic_policy;
mod output_policy;
mod output_projection;
mod registry;
mod retry_controller;
mod router;
mod runtime;
mod shell_format;
mod spec;
mod visibility;
mod web;

pub mod handlers;

pub use argument_normalizer::{
    ToolArgumentCoercion, ToolArgumentNormalization, normalize_tool_arguments_from_schema,
};
pub use classifier::{DefaultErrorClassifier, ErrorClassifier, classify_tool_error};
pub use context::{
    AnyToolResult, CallToolResult, ExecCommandArgs, FunctionToolOutput, LocalShellPayload,
    ToolCallSource, ToolErrorClass, ToolInvocation, ToolOutcome, ToolOutcomeStatus, ToolOutput,
    ToolPayload, ToolSearchOutput, ToolSearchResultTool, ToolSuggestOutput, WriteStdinArgs,
};
pub use domain::{
    ARTIFACT_DOMAIN_TOOL_NAMES, BUILTIN_TOOL_DOMAIN_MAP, BuiltinToolDomain,
    COMPUTER_USE_DOMAIN_TOOL_NAMES, MEMORY_DOMAIN_TOOL_NAMES, REQUEST_TOOLS_DOMAIN_VALUES,
    TASK_DOMAIN_TOOL_NAMES, builtin_tool_domain_map, builtin_tool_domain_names,
    registered_domain_tool_names,
};
pub use error::ToolError;
pub use events::{
    ObservationContext, ToolCallCompletedEvent, ToolCallFailedEvent, ToolCallStartedEvent,
    ToolDeltaPayload, ToolEvent, ToolEventBus, ToolEventKind, ToolEventPayload, ToolEventTrace,
    ToolOutputDeltaEvent,
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
    ToolLoopBudgetAction, ToolLoopBudgetConfig, ToolLoopBudgetExceeded, ToolLoopBudgetReason,
    ToolLoopGuard, ToolLoopGuardDecision, ToolLoopRoundPlan,
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
    mcp_output_policy, model_only_metadata_policy, shell_output_policy, web_fetch_output_policy,
    web_search_output_policy,
};
pub use output_projection::{ToolProjectionInput, project_tool_result};
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
pub use shell_format::{ExecModelPayload, ExecPayloadInput, ExecTruncation, render_exec_ui_text};
pub use spec::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, REQUEST_TOOLS_TOOL_NAME, ToolIdempotencyMode,
    ToolPayloadBinding, ToolRecoveryMetadata, ToolRetryClass, ToolSpec,
    builtin_tool_recovery_metadata, builtin_tool_specs,
};
pub use visibility::ToolVisibilitySnapshot;
pub use web::{
    DownloadModelPayload, WebFetchLink, WebFetchModelPayload, WebFetchTruncation,
    WebSearchModelPayload, WebSearchResultItem, default_favicon_url, render_download_ui_text,
    render_web_fetch_ui_text, render_web_search_ui_text,
};

#[cfg(feature = "computer-use")]
use handlers::ComputerUseHandler;

use handlers::{
    ApplyPatchHandler, DownloadUrlHandler, GrepHandler, ListDirHandler, ReadFileHandler,
    UnifiedExecHandler, WebFetchHandler, WebSearchHandler,
};
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Formatter};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

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
    pub runtime_home_dir: PathBuf,
    pub artifacts_subdir: String,
    pub retention_hours: u64,
    pub max_total_bytes: u64,
    pub run_max_steps_default: u32,
    pub snapshot_transport_max_bytes: usize,
    pub snapshot_transport_max_side_px: u32,
    pub snapshot_transport_min_side_px: u32,
    pub snapshot_downscale_factor: f64,
    pub max_consecutive_same_snapshot_hash: u32,
    pub max_consecutive_same_action_signature: u32,
    pub max_consecutive_no_progress_steps: u32,
    pub max_recovery_attempts_per_step: u32,
    pub max_recovery_attempts_per_run: u32,
}

impl ComputerUseToolsConfig {
    pub fn normalized(&self) -> Self {
        let artifacts_subdir = normalize_artifacts_subdir(self.artifacts_subdir.as_str());
        let runtime_home_dir = if self.runtime_home_dir.as_os_str().is_empty() {
            PathBuf::from(".pioneer")
        } else {
            self.runtime_home_dir.clone()
        };

        Self {
            runtime_home_dir,
            artifacts_subdir,
            retention_hours: self.retention_hours.max(1),
            max_total_bytes: self.max_total_bytes.max(1),
            run_max_steps_default: self.run_max_steps_default.max(1),
            snapshot_transport_max_bytes: self.snapshot_transport_max_bytes.max(256 * 1024),
            snapshot_transport_max_side_px: self.snapshot_transport_max_side_px.clamp(320, 4096),
            snapshot_transport_min_side_px: self.snapshot_transport_min_side_px.clamp(160, 2048),
            snapshot_downscale_factor: self.snapshot_downscale_factor.clamp(0.5, 0.95),
            max_consecutive_same_snapshot_hash: self.max_consecutive_same_snapshot_hash.max(1),
            max_consecutive_same_action_signature: self
                .max_consecutive_same_action_signature
                .max(1),
            max_consecutive_no_progress_steps: self.max_consecutive_no_progress_steps.max(1),
            max_recovery_attempts_per_step: self.max_recovery_attempts_per_step.max(1),
            max_recovery_attempts_per_run: self.max_recovery_attempts_per_run.max(1),
        }
    }
}

impl Default for ComputerUseToolsConfig {
    fn default() -> Self {
        Self {
            runtime_home_dir: PathBuf::from(".pioneer"),
            artifacts_subdir: "tools/computer_use".to_owned(),
            retention_hours: 24,
            max_total_bytes: 1024 * 1024 * 1024,
            run_max_steps_default: 300,
            snapshot_transport_max_bytes: 8 * 1024 * 1024,
            snapshot_transport_max_side_px: 1280,
            snapshot_transport_min_side_px: 320,
            snapshot_downscale_factor: 0.85,
            max_consecutive_same_snapshot_hash: 6,
            max_consecutive_same_action_signature: 8,
            max_consecutive_no_progress_steps: 4,
            max_recovery_attempts_per_step: 2,
            max_recovery_attempts_per_run: 12,
        }
    }
}

fn normalize_artifacts_subdir(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "tools/computer_use".to_owned();
    }

    let candidate = Path::new(trimmed);
    if candidate.is_absolute() || candidate.components().any(is_disallowed_component) {
        return "tools/computer_use".to_owned();
    }

    trimmed.to_owned()
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

pub struct BuiltinTools {
    pub router: Arc<ToolRouter>,
    pub runtime: ToolCallRuntime,
    pub visibility: ToolVisibilitySnapshot,
    pub event_bus: ToolEventBus,
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
        }
    }
}

impl std::error::Error for BuildToolsError {}

pub fn build_tools(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    extensions: Vec<ToolExtensionBundle>,
) -> Result<BuiltinTools, BuildToolsError> {
    build_tools_with_environment(
        workdir,
        turn_id,
        web_tools_config,
        computer_use_tools_config,
        extensions,
        BTreeMap::new(),
    )
}

pub fn build_tools_with_environment(
    workdir: impl Into<PathBuf>,
    turn_id: impl Into<String>,
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
    extensions: Vec<ToolExtensionBundle>,
    environment: BTreeMap<String, String>,
) -> Result<BuiltinTools, BuildToolsError> {
    let turn_id = turn_id.into();
    let web_tools_config = web_tools_config.normalized();

    #[cfg(feature = "computer-use")]
    let computer_use_tools_config = computer_use_tools_config.normalized();

    #[cfg(not(feature = "computer-use"))]
    let _ = computer_use_tools_config;

    let mut configured_specs = builtin_tool_specs();
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
    builder.register_handler(
        REQUEST_TOOLS_TOOL_NAME,
        Arc::new(RequestToolsHandler::new(
            visibility.clone(),
            registered_tool_names,
        )),
    );

    #[cfg(feature = "computer-use")]
    builder.register_handler(
        "computer_use",
        Arc::new(ComputerUseHandler::new(computer_use_tools_config)),
    );

    for extension in extensions {
        for (name, handler) in extension.handlers {
            builder.register_dyn_handler(name, handler);
        }
    }

    let (configured_specs, registry) = builder.build();

    let event_bus = ToolEventBus::default();

    let router = Arc::new(ToolRouter::new(
        configured_specs,
        registry,
        visibility.clone(),
        event_bus.clone(),
        turn_id.clone(),
    ));

    let orchestrator = Arc::new(ToolOrchestrator::new(OrchestratorPolicy::default()));

    let runtime = ToolCallRuntime::new(
        router.clone(),
        orchestrator,
        event_bus.clone(),
        turn_id,
        workdir.into(),
    )
    .with_environment(environment);

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
    web_tools_config: WebToolsConfig,
    computer_use_tools_config: ComputerUseToolsConfig,
) -> BuiltinTools {
    build_tools_with_environment(
        workdir,
        turn_id,
        web_tools_config,
        computer_use_tools_config,
        Vec::new(),
        BTreeMap::new(),
    )
    .expect("build builtin tools")
}

#[cfg(test)]
mod tests {
    use super::{
        BuildToolsError, ComputerUseToolsConfig, ToolExtensionBundle, WebToolsConfig,
        build_builtin_tools, build_tools,
    };
    use crate::context::{FunctionToolOutput, ToolInvocation};
    use crate::events::ToolEventTrace;
    use crate::output_policy::dynamic_unknown_output_policy;
    use crate::registry::ToolHandler;
    use crate::router::RawToolCall;
    use crate::spec::{ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolSpec};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            artifacts_subdir: "tools/computer_use".to_owned(),
            retention_hours: 24,
            max_total_bytes: 1024 * 1024 * 1024,
            run_max_steps_default: 30,
            ..ComputerUseToolsConfig::default()
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

    #[tokio::test]
    async fn request_tools_is_in_default_model_visible_specs() {
        let built = build_builtin_tools(
            ".",
            "turn_request_tools_visible",
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

    #[tokio::test]
    async fn request_tools_runtime_returns_compact_domain_result() {
        let extension_specs = ["artifact_prepare", "artifact_register"]
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
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: extension_specs,
                handlers: vec![
                    ("artifact_prepare".to_owned(), Arc::new(EchoHandler)),
                    ("artifact_register".to_owned(), Arc::new(EchoHandler)),
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
                "artifact_register".to_owned()
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
}
