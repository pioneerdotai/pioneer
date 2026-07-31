//! API-provider agent loop.
//!
//! This crate owns prompt compilation, provider streaming, and Pioneer tool
//! execution for API-backed providers. CLI-backed agent runtimes bypass this
//! loop after gateway turn materialization and are selected by the gateway.

mod agent_loop;
mod chat;
mod hooks;
mod manager_recovery;
#[cfg(test)]
mod manager_tests;

use pioneer_hooks::HookRuntime;
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ExecutionCheckpointPayload,
    ExecutionWindowExhaustionReason, McpScopeKind, ProviderFailureClass, ProviderFailureDetails,
    ProviderFailureStage, ThreadMode, TurnCapability, TurnExecutionSecuritySnapshot, TurnItemType,
    TurnPermissionProfileSnapshot, UserInput,
};
#[cfg(test)]
use pioneer_protocol::{
    ItemCompletedNotification, ItemDeltaNotification, ItemStartedNotification,
    ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
    ItemToolRetryScheduledNotification, PromptManifest, ToolOutputPolicySnapshot,
    TurnToolLoopBudgetExceededNotification,
};
use pioneer_provider::{
    ChatMessage, InputContentType, MessageAttachment, ProviderRegistry, ProviderTimeoutPolicy,
    ReasoningConfig, ReasoningEffort,
};
#[cfg(test)]
use pioneer_skills::SkillAuditEvent;
use pioneer_skills::{
    AgentSkillRuntimeEntry, SkillCatalogSnapshot, SkillId, SkillPolicyKey, SkillTrustLevel,
};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use hooks::{
    AgentPostTurnHookDispatchPolicy, AgentTurnHookRuntimeContext, AgentTurnPostTurnDispatchMode,
};
use hooks::{AgentToolBundleArtifactStore, DeferredTaskPostTurnDispatchStore};
use manager_recovery::apply_recovery_adjustments;
pub use pioneer_memory::hooks::{
    AgentEpisodicRecallProvider, AgentMemoryPostTurnExtractorProvider, AgentMemoryProvider,
    AgentMemoryTurnPolicyProvider, AgentMemoryWriteProvider, MemoryActiveContextPolicy,
    MemoryActiveRecallConfig, MemoryActiveRecallDecisionContext, MemoryActiveRecallDecisionRequest,
    MemoryActiveRecallMode, MemoryActiveRecallPlannerConfig,
    MemoryActiveRecallPlannerFallbackPolicy, MemoryClassifierFallbackPolicy,
    MemoryExtractionPolicy, MemoryLoopConfig, MemoryManifest, MemoryManifestActiveItem,
    MemoryManifestCandidateItem, MemoryManifestRequest, MemoryMutationToolPolicy,
    MemoryPolicyReasonCode, MemoryPolicySource, MemoryPostTurnExtractorConfig,
    MemoryPostTurnExtractorContext, MemoryPostTurnExtractorRequest, MemoryPromptPolicy,
    MemoryReadToolPolicy, MemoryRecallItem, MemoryRecallPolicy, MemoryRecallRequest,
    MemoryRecallSnapshot, MemoryToolMaterialization, MemoryTurnContext, MemoryTurnPolicy,
    MemoryTurnPolicyContext, MemoryTurnPolicyOverride, MemoryTurnPolicyRequest,
};
use pioneer_tools::{
    ComputerUseToolsConfig, ExecutionWindowsConfig, PermissionApprovalBroker,
    StaticPermissionApprovalBroker, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
};

/// Classifies an API-provider transport failure without exposing turn-specific
/// recovery machinery.
pub fn classify_provider_failure_message(
    error_message: &str,
    stage: ProviderFailureStage,
) -> ProviderFailureClass {
    chat::classify_provider_failure_message(error_message, stage)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedArtifactInput {
    pub artifact_id: String,
    pub version_id: Option<String>,
    pub content_type: InputContentType,
    pub attachment: MessageAttachment,
}

const COMMAND_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct ToolLoopConfig {
    pub provider: ProviderTimeoutPolicy,
    pub preflight: PreflightLoopConfig,
    pub web: WebToolsConfig,
    pub computer_use: ComputerUseToolsConfig,
    pub skills: SkillsLoopConfig,
    pub memory: MemoryLoopConfig,
    pub budget: ToolLoopBudgetConfig,
    pub execution_windows: ExecutionWindowsConfig,
    pub retry: ToolRetryBudgetConfig,
}

#[derive(Debug, Clone, Default)]
pub struct PreflightLoopConfig {
    pub provider_name: Option<String>,
    pub model: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_output_chars: Option<usize>,
}

impl PreflightLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            provider_name: normalized_optional_config_text(self.provider_name.as_deref()),
            model: normalized_optional_config_text(self.model.as_deref()),
            timeout_ms: self.timeout_ms,
            max_output_chars: self.max_output_chars,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SkillsLoopConfig {
    pub enabled: bool,
    pub max_skills_per_source: usize,
    pub max_skill_file_bytes: usize,
    pub prompt_max_chars: usize,
    pub allow_implicit_invocation: bool,
    pub system_roots: Vec<String>,
    pub user_roots: Vec<String>,
    pub registry_roots: Vec<String>,
    pub system_import_roots: Vec<String>,
    pub user_import_roots: Vec<String>,
    pub registry_import_roots: Vec<String>,
    pub validation: SkillsValidationLoopConfig,
    pub security: SkillsSecurityLoopConfig,
    pub dependencies: SkillsDependenciesLoopConfig,
    pub runtime: SkillsRuntimeLoopConfig,
}

#[derive(Debug, Clone)]
pub struct SkillsRuntimeLoopConfig {
    pub enable_dynamic_tools: bool,
    pub enable_read_skill: bool,
    pub max_dynamic_tools_per_skill: usize,
    pub read_skill_max_chars: usize,
    pub compact_mode_threshold: usize,
    pub allow_shell_tools: bool,
    pub allow_http_tools: bool,
    pub allow_function_proxy_tools: bool,
}

#[derive(Debug, Clone)]
pub struct SkillsValidationLoopConfig {
    pub strict_agentskills: bool,
    pub accept_openclaw_profile: bool,
}

#[derive(Debug, Clone)]
pub struct SkillsSecurityLoopConfig {
    pub allow_untrusted_install: bool,
    pub min_trust_for_shell_tools: SkillTrustLevel,
    pub min_trust_for_http_tools: SkillTrustLevel,
    pub min_trust_for_function_proxy_tools: SkillTrustLevel,
    pub max_install_archive_bytes: usize,
    pub max_install_archive_compressed_bytes: usize,
    pub max_install_archive_uncompressed_bytes: usize,
    pub max_install_archive_entries: usize,
    pub max_install_file_bytes: usize,
    pub upload_ttl_secs: u64,
    pub upload_recommended_chunk_size_bytes: usize,
    pub upload_max_chunk_size_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SkillsDependenciesLoopConfig {
    pub preflight_on_resolve: bool,
    pub runtime_recheck_on_tool_call: bool,
}

impl SkillsLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            enabled: self.enabled,
            max_skills_per_source: self.max_skills_per_source.max(1),
            max_skill_file_bytes: self.max_skill_file_bytes.max(1),
            prompt_max_chars: self.prompt_max_chars.max(1),
            allow_implicit_invocation: self.allow_implicit_invocation,
            system_roots: self.system_roots.clone(),
            user_roots: self.user_roots.clone(),
            registry_roots: self.registry_roots.clone(),
            system_import_roots: self.system_import_roots.clone(),
            user_import_roots: self.user_import_roots.clone(),
            registry_import_roots: self.registry_import_roots.clone(),
            validation: self.validation.normalized(),
            security: self.security.normalized(),
            dependencies: self.dependencies.normalized(),
            runtime: self.runtime.normalized(),
        }
    }
}

impl SkillsRuntimeLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            enable_dynamic_tools: self.enable_dynamic_tools,
            enable_read_skill: self.enable_read_skill,
            max_dynamic_tools_per_skill: self.max_dynamic_tools_per_skill.max(1),
            read_skill_max_chars: self.read_skill_max_chars.max(1),
            compact_mode_threshold: self.compact_mode_threshold,
            allow_shell_tools: self.allow_shell_tools,
            allow_http_tools: self.allow_http_tools,
            allow_function_proxy_tools: self.allow_function_proxy_tools,
        }
    }
}

impl SkillsValidationLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            strict_agentskills: self.strict_agentskills,
            accept_openclaw_profile: self.accept_openclaw_profile,
        }
    }
}

impl SkillsSecurityLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            allow_untrusted_install: self.allow_untrusted_install,
            min_trust_for_shell_tools: self.min_trust_for_shell_tools.clone(),
            min_trust_for_http_tools: self.min_trust_for_http_tools.clone(),
            min_trust_for_function_proxy_tools: self.min_trust_for_function_proxy_tools.clone(),
            max_install_archive_bytes: self.max_install_archive_bytes.max(1),
            max_install_archive_compressed_bytes: self.max_install_archive_compressed_bytes.max(1),
            max_install_archive_uncompressed_bytes: self
                .max_install_archive_uncompressed_bytes
                .max(1),
            max_install_archive_entries: self.max_install_archive_entries.max(1),
            max_install_file_bytes: self.max_install_file_bytes.max(1),
            upload_ttl_secs: self.upload_ttl_secs.max(60),
            upload_recommended_chunk_size_bytes: self.upload_recommended_chunk_size_bytes.max(1),
            upload_max_chunk_size_bytes: self.upload_max_chunk_size_bytes.max(1),
        }
    }
}

impl SkillsDependenciesLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            preflight_on_resolve: self.preflight_on_resolve,
            runtime_recheck_on_tool_call: self.runtime_recheck_on_tool_call,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceSkillPolicy {
    pub enabled: Option<bool>,
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMcpAvailability {
    pub available_mcp: Vec<String>,
    pub blocked_mcp: Vec<String>,
}

#[derive(Clone, Default)]
pub struct AgentMcpMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub available_mcp: Vec<String>,
    pub blocked_mcp: Vec<String>,
    pub diagnostics: Vec<AgentMcpResolutionDiagnostic>,
    pub accepted_capabilities: Vec<pioneer_protocol::TurnAcceptedCapability>,
    pub rejected_capabilities: Vec<pioneer_protocol::TurnRejectedCapability>,
    pub mcp_bindings: Vec<pioneer_protocol::McpTurnBindingSummary>,
    pub persistence: Option<AgentMcpProjectionPersistenceRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpProjectionPersistenceRequest {
    pub workspace_id: String,
    pub turn_id: String,
    pub projection_version: u32,
    pub manifest_hash: String,
    pub resolution_status: String,
    pub bindings: Vec<AgentMcpProjectionBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpProjectionBinding {
    pub server_installation_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub callable_name: String,
    pub canonical_callable_name: String,
    pub provider_callable_name: String,
    pub catalog_version: String,
    pub installation_fingerprint: String,
    pub canonical_schema_fingerprint: String,
    pub provider_schema_fingerprint: String,
    pub annotations_json: String,
    pub annotations_digest: String,
    pub effective_timeout_ms: u64,
    pub runtime_generation: u64,
    pub projection_activation_generation: u64,
    pub selection_reason: String,
    pub capability_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpPersistedProjection {
    pub turn_id: String,
    pub manifest_hash: String,
    pub tool_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpProjectionPersistenceError {
    pub message: String,
}

impl std::fmt::Display for AgentMcpProjectionPersistenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for AgentMcpProjectionPersistenceError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpResolutionDiagnostic {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMcpMaterializationFailureReason {
    ExplicitCapabilityRejected,
    RequiredInstallationUnavailable,
    ResolutionUncertain,
    ProjectionInvalid,
    ProviderUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpMaterializationError {
    pub reason: AgentMcpMaterializationFailureReason,
    pub message: String,
    pub diagnostics: Vec<AgentMcpResolutionDiagnostic>,
    pub accepted_capabilities: Vec<pioneer_protocol::TurnAcceptedCapability>,
    pub rejected_capabilities: Vec<pioneer_protocol::TurnRejectedCapability>,
}

impl std::fmt::Display for AgentMcpMaterializationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for AgentMcpMaterializationError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMcpMaterializationRequest {
    pub workspace_id: String,
    pub turn_id: String,
    pub explicit_servers: Vec<AgentMcpServerRef>,
    pub explicit_tools: Vec<AgentMcpToolRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpServerRef {
    pub capability_id: String,
    pub label: Option<String>,
    pub name: String,
    pub scope_kind: McpScopeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMcpToolRef {
    pub capability_id: String,
    pub label: Option<String>,
    pub server_name: String,
    pub raw_tool_name: String,
    pub scope_kind: McpScopeKind,
}

#[async_trait::async_trait]
pub trait AgentMcpToolProvider: Send + Sync {
    async fn mcp_availability(&self, workspace_id: &str) -> Result<AgentMcpAvailability, String>;

    async fn materialize_mcp_tools(
        &self,
        request: AgentMcpMaterializationRequest,
    ) -> Result<AgentMcpMaterialization, AgentMcpMaterializationError>;

    async fn persist_mcp_projection(
        &self,
        request: AgentMcpProjectionPersistenceRequest,
    ) -> Result<AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTurnContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAttachedTask {
    pub task_id: String,
    pub run_id: Option<String>,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRequiredTaskObservation {
    pub task_id: String,
    pub run_id: String,
    pub candidate_id: String,
    pub title: String,
    pub status: String,
    pub candidate_status: String,
    pub round: u32,
    pub summary: Option<String>,
    pub result_preview: Option<String>,
    pub extraction_error_preview: Option<String>,
    pub diagnostics: Vec<String>,
    pub child_thread_id: Option<String>,
    pub child_turn_id: Option<String>,
    pub max_revision_rounds: u32,
    pub remaining_revision_rounds: u32,
    pub allowed_actions: Vec<String>,
    pub revision_blocked_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTaskObservation {
    pub task_id: String,
    pub run_id: Option<String>,
    pub title: String,
    pub status: String,
    pub summary: Option<String>,
    pub error_message: Option<String>,
    pub child_thread_id: Option<String>,
    pub child_turn_id: Option<String>,
}

#[derive(Clone, Default)]
pub struct TaskToolMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnToolContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Clone, Default)]
pub struct TurnToolMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillContinuationAuthorizationContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub skill_id: SkillId,
    pub fingerprint: String,
}

#[async_trait::async_trait]
pub trait TurnToolProvider: Send + Sync {
    async fn materialize_turn_tools(
        &self,
        context: TurnToolContext,
    ) -> Result<TurnToolMaterialization, String>;

    async fn authorize_skill_continuation(
        &self,
        _context: SkillContinuationAuthorizationContext,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnFinalizationContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub final_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnFinalizationDecision {
    Allow,
    Retry { instruction: String },
    Fail { message: String },
}

#[async_trait::async_trait]
pub trait TurnFinalizationProvider: Send + Sync {
    async fn check_turn_finalization(
        &self,
        context: TurnFinalizationContext,
    ) -> Result<TurnFinalizationDecision, String>;
}

#[async_trait::async_trait]
pub trait TaskToolProvider: Send + Sync {
    async fn materialize_task_tools(
        &self,
        context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String>;

    async fn pending_attached_tasks(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<PendingAttachedTask>, String>;

    async fn review_required_attached_task_observations(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String>;

    async fn terminal_attached_task_observations(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<TerminalTaskObservation>, String>;

    async fn cleanup_attached_tasks(
        &self,
        context: TaskTurnContext,
        reason: String,
    ) -> Result<(), String>;
}

impl ToolLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            provider: self.provider,
            preflight: self.preflight.normalized(),
            web: self.web.normalized(),
            computer_use: self.computer_use.normalized(),
            skills: self.skills.normalized(),
            memory: self.memory.normalized(),
            budget: self.budget.normalized(),
            execution_windows: self.execution_windows.normalized(),
            retry: self.retry.normalized(),
        }
    }
}

fn normalized_optional_config_text(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_owned())
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub enum AgentEvent {
    PromptManifestCompiled {
        thread_id: String,
        turn_id: String,
        manifest: PromptManifest,
    },
    TurnSkillsResolved {
        thread_id: String,
        turn_id: String,
        bindings: Vec<pioneer_protocol::TurnSkillBinding>,
    },
    TurnCapabilitiesResolved {
        thread_id: String,
        turn_id: String,
        accepted: Vec<pioneer_protocol::TurnAcceptedCapability>,
        rejected: Vec<pioneer_protocol::TurnRejectedCapability>,
        mcp_bindings: Vec<pioneer_protocol::McpTurnBindingSummary>,
    },
    SkillAuditEvents {
        thread_id: String,
        turn_id: String,
        events: Vec<SkillAuditEvent>,
    },
    TurnLlmContextAppended {
        thread_id: String,
        turn_id: String,
        item_id: String,
        attempt_id: Option<String>,
        sequence: i64,
        source: String,
        tool_name: String,
        payload: pioneer_tools::ToolResultView,
        output_policy_snapshot: ToolOutputPolicySnapshot,
    },
    ItemStarted(ItemStartedNotification),
    ItemDelta(ItemDeltaNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    ItemHeartbeat {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    },
    ProviderFailureDetected {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
    RecoveryAttemptSucceeded {
        thread_id: String,
        turn_id: String,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    },
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
    TurnFailed {
        thread_id: String,
        turn_id: String,
        error: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
    TurnBlocked {
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
}

pub use pioneer_runtime_events::{
    AgentEventHub, AgentEventHubError, DurableEventReceiver, ExecutionEventHub,
    ExecutionEventHubError, ExecutionTurnObservation, ExecutionTurnStatus, ProgressCoalescer,
    ProgressCoalescerConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStartError {
    ThreadNotFound,
    TurnAlreadyRunning,
    ThreadWorkspaceMismatch {
        expected_workspace_id: String,
        actual_workspace_id: String,
    },
    Internal(String),
}

impl Display for AgentStartError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ThreadNotFound => write!(f, "thread is not registered in agent manager"),
            Self::TurnAlreadyRunning => write!(f, "thread already has a running turn"),
            Self::ThreadWorkspaceMismatch {
                expected_workspace_id,
                actual_workspace_id,
            } => write!(
                f,
                "thread workspace mismatch: expected `{expected_workspace_id}`, got `{actual_workspace_id}`"
            ),
            Self::Internal(error) => write!(f, "internal agent error: {error}"),
        }
    }
}

impl Error for AgentStartError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentControlError {
    ThreadNotFound,
    NoActiveTurn,
    TurnMismatch,
    TurnAlreadyRunning,
    AttemptNotRunning,
    ExecutionWindowContinuationBlocked { reason: String },
    Internal(String),
}

impl Display for AgentControlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ThreadNotFound => write!(f, "thread is not registered in agent manager"),
            Self::NoActiveTurn => write!(f, "thread has no active turn"),
            Self::TurnMismatch => write!(f, "active turn does not match the requested turn"),
            Self::TurnAlreadyRunning => write!(f, "thread already has an active turn"),
            Self::AttemptNotRunning => write!(f, "turn item attempt is not running"),
            Self::ExecutionWindowContinuationBlocked { reason } => write!(f, "{reason}"),
            Self::Internal(error) => write!(f, "internal agent control error: {error}"),
        }
    }
}

impl Error for AgentControlError {}

#[derive(Debug, Clone, PartialEq)]
pub struct RetainedProviderHistoryMessage {
    pub sequence: i64,
    pub message: ChatMessage,
}

#[derive(Debug, Clone)]
pub struct RecoveryAttemptRequest {
    pub recovery_job_id: String,
    pub recovery_attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub force_non_stream: bool,
    pub disable_tool_calling: bool,
    pub disable_image_input: bool,
    pub refresh_provider_auth: bool,
    pub compact_history: bool,
    pub continue_generation: bool,
    pub model_override: Option<String>,
    pub retained_provider_history: Vec<RetainedProviderHistoryMessage>,
    pub execution_checkpoint_context: Option<ExecutionCheckpointContext>,
}

#[derive(Debug, Clone)]
pub struct ExecutionCheckpointContext {
    pub window_id: String,
    pub window_index: u32,
    pub checkpoint_id: String,
    pub checkpoint_kind: String,
    pub payload: ExecutionCheckpointPayload,
    pub usage: ExecutionWindowUsageSnapshot,
}

impl ExecutionCheckpointContext {
    pub fn next_window_index(&self) -> u32 {
        self.window_index.saturating_add(1).max(2)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionWindowUsageSnapshot {
    pub total_windows: u32,
    pub total_tool_calls: u64,
    pub total_wall_clock_ms: u64,
    pub total_provider_tokens: u64,
    pub provider_token_usage_unknown: bool,
    pub consecutive_no_progress_windows: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TurnExecutionUsageCounters {
    total_windows: u32,
    total_tool_calls: u64,
    total_wall_clock_ms: u64,
    total_provider_tokens: u64,
    provider_token_usage_unknown: bool,
    consecutive_no_progress_windows: u32,
}

impl TurnExecutionUsageCounters {
    fn from_snapshot(snapshot: ExecutionWindowUsageSnapshot) -> Self {
        Self {
            total_windows: snapshot.total_windows,
            total_tool_calls: snapshot.total_tool_calls,
            total_wall_clock_ms: snapshot.total_wall_clock_ms,
            total_provider_tokens: snapshot.total_provider_tokens,
            provider_token_usage_unknown: snapshot.provider_token_usage_unknown,
            consecutive_no_progress_windows: snapshot.consecutive_no_progress_windows,
        }
    }

    fn snapshot(self) -> ExecutionWindowUsageSnapshot {
        ExecutionWindowUsageSnapshot {
            total_windows: self.total_windows,
            total_tool_calls: self.total_tool_calls,
            total_wall_clock_ms: self.total_wall_clock_ms,
            total_provider_tokens: self.total_provider_tokens,
            provider_token_usage_unknown: self.provider_token_usage_unknown,
            consecutive_no_progress_windows: self.consecutive_no_progress_windows,
        }
    }

    fn observe_checkpoint_payload(&mut self, payload: &ExecutionCheckpointPayload) {
        if payload.window.window_index <= self.total_windows {
            return;
        }

        self.total_windows = payload.window.window_index;
        self.total_tool_calls = self
            .total_tool_calls
            .saturating_add(u64::from(payload.window.tool_call_count));

        if let (Some(started_at), Some(completed_at)) = (
            payload.window.started_at_unix_ms,
            payload.window.completed_at_unix_ms,
        ) {
            let duration_ms = completed_at.saturating_sub(started_at);
            if duration_ms > 0 {
                self.total_wall_clock_ms = self
                    .total_wall_clock_ms
                    .saturating_add(u64::try_from(duration_ms).unwrap_or(u64::MAX));
            }
        }

        match payload
            .window
            .provider_token_count
            .or(payload.provider_budget.provider_token_count)
        {
            Some(tokens) => {
                self.total_provider_tokens = self.total_provider_tokens.saturating_add(tokens);
            }
            None => {
                self.provider_token_usage_unknown = true;
            }
        }

        if execution_checkpoint_payload_is_no_progress_recovery(payload) {
            self.consecutive_no_progress_windows =
                self.consecutive_no_progress_windows.saturating_add(1);
        } else {
            self.consecutive_no_progress_windows = 0;
        }
    }
}

fn execution_checkpoint_payload_is_no_progress_recovery(
    payload: &ExecutionCheckpointPayload,
) -> bool {
    payload.window.agent_round_count == 0
        && payload.tools.executed_count == 0
        && matches!(
            payload.window.exhaustion_reason,
            Some(
                ExecutionWindowExhaustionReason::ProviderFailureContinuation
                    | ExecutionWindowExhaustionReason::RuntimeShutdownContinuation
            )
        )
}

#[derive(Debug, Clone)]
pub struct RestoredRecoveryTurnRequest {
    pub turn_id: String,
    pub execution_window_index: u32,
    pub mode: ThreadMode,
    pub hook_runtime_context: AgentTurnHookRuntimeContext,
    pub model: String,
    pub provider_name: String,
    pub reasoning: Option<ReasoningConfig>,
    pub workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
    pub skill_catalog: SkillCatalogSnapshot,
    pub agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
    pub input: Vec<UserInput>,
    pub capabilities: Vec<TurnCapability>,
    pub resolved_artifacts: Vec<ResolvedArtifactInput>,
    pub runtime_environment: HashMap<String, String>,
    pub history: Vec<ChatMessage>,
    pub permission_profile: TurnPermissionProfileSnapshot,
    pub execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
}

#[derive(Debug, Clone, Default)]
struct TurnExecutionOptions {
    force_non_stream: bool,
    disable_tool_calling: bool,
    continue_generation_hint: bool,
}

fn reasoning_config_from_effort(
    effort: Option<&str>,
) -> Result<Option<ReasoningConfig>, AgentStartError> {
    let Some(effort) = effort else {
        return Ok(None);
    };
    ReasoningEffort::from_str(effort)
        .map(ReasoningConfig::effort)
        .map(Some)
        .ok_or_else(|| {
            AgentStartError::Internal(format!("unsupported reasoning effort `{effort}`"))
        })
}

#[derive(Debug, Clone)]
struct ActiveTurnRequest {
    turn_id: String,
    execution_window_index: u32,
    mode: ThreadMode,
    hook_runtime_context: AgentTurnHookRuntimeContext,
    model: String,
    provider_name: String,
    reasoning: Option<ReasoningConfig>,
    workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
    skill_catalog: SkillCatalogSnapshot,
    agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
    input: Vec<UserInput>,
    capabilities: Vec<TurnCapability>,
    resolved_artifacts: Vec<ResolvedArtifactInput>,
    runtime_environment: HashMap<String, String>,
    history: Vec<ChatMessage>,
    retained_provider_history: Vec<RetainedProviderHistoryMessage>,
    execution_checkpoint_context: Option<ExecutionCheckpointContext>,
    execution_usage: TurnExecutionUsageCounters,
    execution_options: TurnExecutionOptions,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
}

#[derive(Debug, Clone)]
enum TurnTaskFailure {
    Terminal(String),
    Blocked(String),
    ProviderFailure {
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
    },
}

#[derive(Debug, Clone)]
struct ExecutionWindowContinuation {
    reason: ExecutionWindowExhaustionReason,
    exhausted_window_id: String,
    checkpoint_id: String,
    checkpoint_payload: ExecutionCheckpointPayload,
    provider_history: Vec<RetainedProviderHistoryMessage>,
    exhausted_limit: Option<u64>,
    exhausted_observed: Option<u64>,
    reason_code: String,
}

#[derive(Debug, Clone)]
enum TurnTaskSuccess {
    Completed,
    NeedsContinuation(ExecutionWindowContinuation),
}

#[derive(Debug)]
struct TurnTaskCompletion {
    result: Result<TurnTaskSuccess, TurnTaskFailure>,
    post_turn_dispatch: Option<hooks::AgentTurnPostTurnHookDispatch>,
}

#[derive(Debug)]
enum AgentCommand {
    StartTurn {
        turn_id: String,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: String,
        provider_name: String,
        reasoning: Option<ReasoningConfig>,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        execution_checkpoint_context: Option<ExecutionCheckpointContext>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
        ack: oneshot::Sender<Result<(), AgentStartError>>,
    },
    TurnTaskFinished {
        turn_id: String,
        run_id: u64,
        completion: TurnTaskCompletion,
    },
    CancelAttempt {
        turn_id: String,
        item_id: String,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    CancelTurn {
        turn_id: String,
        reason: String,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    ObserveTurn {
        turn_id: String,
        ack: oneshot::Sender<Option<ExecutionTurnObservation>>,
    },
    StartRecoveryAttempt {
        request: RecoveryAttemptRequest,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    StartRestoredRecoveryTurn {
        turn_request: RestoredRecoveryTurnRequest,
        recovery_request: RecoveryAttemptRequest,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    RecoveryAttemptSucceeded {
        turn_id: String,
        run_id: u64,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    },
    Shutdown,
}

#[derive(Clone)]
struct TurnExecutionControl {
    attempt_controls: Arc<tokio::sync::Mutex<HashMap<String, AttemptControl>>>,
    command_tx: mpsc::Sender<AgentCommand>,
    run_id: u64,
}

#[derive(Clone)]
struct AttemptControl {
    cancellation_token: CancellationToken,
    recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
}

impl TurnExecutionControl {
    fn new(command_tx: mpsc::Sender<AgentCommand>, run_id: u64) -> Self {
        Self {
            attempt_controls: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            command_tx,
            run_id,
        }
    }

    async fn register_attempt(&self, item_id: String) -> CancellationToken {
        let token = CancellationToken::new();
        self.attempt_controls.lock().await.insert(
            item_id,
            AttemptControl {
                cancellation_token: token.clone(),
                recovery: None,
            },
        );
        token
    }

    async fn complete_attempt(&self, turn_id: &str, item_id: &str) {
        let recovery = self
            .attempt_controls
            .lock()
            .await
            .remove(item_id)
            .and_then(|control| control.recovery);

        self.succeed_recovery_attempt(turn_id, recovery).await;
    }

    async fn succeed_recovery_attempt(
        &self,
        turn_id: &str,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) {
        let Some(recovery) = recovery else {
            return;
        };
        let _ = self
            .command_tx
            .send(AgentCommand::RecoveryAttemptSucceeded {
                turn_id: turn_id.to_owned(),
                run_id: self.run_id,
                recovery,
            })
            .await;
    }

    async fn cancel_attempt(&self, item_id: &str) -> bool {
        let token = self
            .attempt_controls
            .lock()
            .await
            .get(item_id)
            .map(|control| control.cancellation_token.clone());
        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    async fn cancel_attempt_for_recovery(
        &self,
        item_id: &str,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    ) -> bool {
        let token = {
            let mut controls = self.attempt_controls.lock().await;
            let Some(control) = controls.get_mut(item_id) else {
                return false;
            };
            control.recovery = Some(recovery);
            control.cancellation_token.clone()
        };

        token.cancel();
        true
    }

    async fn cancel_all_attempts(&self) {
        let tokens = self
            .attempt_controls
            .lock()
            .await
            .values()
            .map(|control| control.cancellation_token.clone())
            .collect::<Vec<_>>();
        for token in tokens {
            token.cancel();
        }
    }
}

struct AgentThreadHandle {
    workspace_id: String,
    command_tx: mpsc::Sender<AgentCommand>,
    event_hub: Arc<AgentEventHub>,
    loop_handle: JoinHandle<()>,
}

#[derive(Default)]
struct AgentManagerState {
    threads: HashMap<String, AgentThreadHandle>,
}

pub struct AgentManager {
    state: RwLock<AgentManagerState>,
    provider_registry: Arc<ProviderRegistry>,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: RwLock<Option<Arc<dyn TurnToolProvider>>>,
    turn_finalization_provider: RwLock<Option<Arc<dyn TurnFinalizationProvider>>>,
    task_tool_provider: RwLock<Option<Arc<dyn TaskToolProvider>>>,
    memory_provider: RwLock<Option<Arc<dyn AgentMemoryProvider>>>,
    memory_write_provider: RwLock<Option<Arc<dyn AgentMemoryWriteProvider>>>,
    memory_post_turn_extractor_provider:
        RwLock<Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>>,
    memory_turn_policy_provider: RwLock<Option<Arc<dyn AgentMemoryTurnPolicyProvider>>>,
    memory_episodic_recall_provider: RwLock<Option<Arc<dyn AgentEpisodicRecallProvider>>>,
    hook_runtime: RwLock<Option<Arc<HookRuntime>>>,
    tool_bundle_artifacts: Arc<AgentToolBundleArtifactStore>,
    post_turn_hook_dispatch_policy: RwLock<AgentPostTurnHookDispatchPolicy>,
    deferred_task_post_turn_dispatches: Arc<DeferredTaskPostTurnDispatchStore>,
    permission_approval_broker: Arc<RwLock<Arc<dyn PermissionApprovalBroker>>>,
}

impl AgentManager {
    pub fn new(provider_registry: Arc<ProviderRegistry>, tool_loop_config: ToolLoopConfig) -> Self {
        Self::new_with_mcp(provider_registry, tool_loop_config, None)
    }

    pub fn new_with_mcp(
        provider_registry: Arc<ProviderRegistry>,
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    ) -> Self {
        Self::new_with_mcp_and_memory(provider_registry, tool_loop_config, mcp_tool_provider, None)
    }

    pub fn new_with_mcp_and_memory(
        provider_registry: Arc<ProviderRegistry>,
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
        memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    ) -> Self {
        Self {
            state: RwLock::new(AgentManagerState::default()),
            provider_registry,
            tool_loop_config: tool_loop_config.normalized(),
            mcp_tool_provider,
            turn_tool_provider: RwLock::new(None),
            turn_finalization_provider: RwLock::new(None),
            task_tool_provider: RwLock::new(None),
            memory_provider: RwLock::new(memory_provider),
            memory_write_provider: RwLock::new(None),
            memory_post_turn_extractor_provider: RwLock::new(None),
            memory_turn_policy_provider: RwLock::new(None),
            memory_episodic_recall_provider: RwLock::new(None),
            hook_runtime: RwLock::new(None),
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
            post_turn_hook_dispatch_policy: RwLock::new(AgentPostTurnHookDispatchPolicy::default()),
            deferred_task_post_turn_dispatches: Arc::new(
                DeferredTaskPostTurnDispatchStore::default(),
            ),
            permission_approval_broker: Arc::new(RwLock::new(Arc::new(
                StaticPermissionApprovalBroker::default(),
            ))),
        }
    }

    pub async fn set_permission_approval_broker(&self, broker: Arc<dyn PermissionApprovalBroker>) {
        *self.permission_approval_broker.write().await = broker;
    }

    pub async fn set_task_tool_provider(&self, provider: Option<Arc<dyn TaskToolProvider>>) {
        *self.task_tool_provider.write().await = provider;
    }

    pub async fn set_turn_tool_provider(&self, provider: Option<Arc<dyn TurnToolProvider>>) {
        *self.turn_tool_provider.write().await = provider;
    }

    pub async fn set_turn_finalization_provider(
        &self,
        provider: Option<Arc<dyn TurnFinalizationProvider>>,
    ) {
        *self.turn_finalization_provider.write().await = provider;
    }

    pub async fn set_memory_provider(&self, provider: Option<Arc<dyn AgentMemoryProvider>>) {
        *self.memory_provider.write().await = provider;
    }

    pub async fn set_memory_write_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    ) {
        *self.memory_write_provider.write().await = provider;
    }

    pub async fn set_memory_post_turn_extractor_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    ) {
        *self.memory_post_turn_extractor_provider.write().await = provider;
    }

    pub async fn set_memory_turn_policy_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    ) {
        *self.memory_turn_policy_provider.write().await = provider;
    }

    pub async fn set_memory_episodic_recall_provider(
        &self,
        provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    ) {
        *self.memory_episodic_recall_provider.write().await = provider;
    }

    pub async fn set_hook_runtime(&self, runtime: Option<Arc<HookRuntime>>) {
        // Existing loops keep the runtime snapshot captured by ensure_thread.
        *self.hook_runtime.write().await = runtime;
    }

    pub fn memory_tool_bundle_artifact_store(
        &self,
    ) -> Arc<dyn pioneer_memory::hooks::MemoryToolBundleArtifactStore> {
        self.tool_bundle_artifacts.clone()
    }

    pub async fn ensure_hook_runtime_with_current_providers(
        &self,
    ) -> Result<Option<Arc<HookRuntime>>, AgentStartError> {
        Ok(self.hook_runtime.read().await.clone())
    }

    pub async fn set_post_turn_hook_dispatch_policy(
        &self,
        policy: AgentPostTurnHookDispatchPolicy,
    ) {
        // Existing loops keep the policy snapshot captured by ensure_thread.
        *self.post_turn_hook_dispatch_policy.write().await = policy;
    }

    pub async fn accept_deferred_task_result_post_turn(&self, thread_id: &str, turn_id: &str) {
        self.deferred_task_post_turn_dispatches
            .accept(thread_id, turn_id)
            .await;
    }

    pub async fn discard_deferred_task_result_post_turn(&self, thread_id: &str, turn_id: &str) {
        self.deferred_task_post_turn_dispatches
            .discard(thread_id, turn_id)
            .await;
    }

    pub async fn has_memory_provider(&self) -> bool {
        self.memory_provider.read().await.is_some()
    }

    pub async fn has_hook_runtime(&self) -> bool {
        self.hook_runtime.read().await.is_some()
    }

    pub async fn ensure_thread(
        &self,
        thread_id: &str,
        workspace_id: &str,
    ) -> Result<(), AgentStartError> {
        if let Some(existing_workspace_id) = self
            .state
            .read()
            .await
            .threads
            .get(thread_id)
            .map(|thread| thread.workspace_id.clone())
        {
            if existing_workspace_id != workspace_id {
                return Err(AgentStartError::ThreadWorkspaceMismatch {
                    expected_workspace_id: existing_workspace_id,
                    actual_workspace_id: workspace_id.to_owned(),
                });
            }
            return Ok(());
        }

        let thread_id_owned = thread_id.to_owned();
        let workspace_id_owned = workspace_id.to_owned();

        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let event_hub = Arc::new(AgentEventHub::new());

        let hook_runtime = self.hook_runtime.read().await.clone();
        let tool_bundle_artifacts = hook_runtime
            .as_ref()
            .map(|_| self.tool_bundle_artifacts.clone());
        let loop_handle = tokio::spawn(Box::pin(agent_loop::run_agent_loop(
            thread_id_owned,
            workspace_id_owned.clone(),
            self.provider_registry.clone(),
            self.tool_loop_config.clone(),
            self.mcp_tool_provider.clone(),
            self.turn_tool_provider.read().await.clone(),
            self.turn_finalization_provider.read().await.clone(),
            self.task_tool_provider.read().await.clone(),
            hook_runtime,
            tool_bundle_artifacts,
            self.permission_approval_broker.clone(),
            *self.post_turn_hook_dispatch_policy.read().await,
            self.deferred_task_post_turn_dispatches.clone(),
            command_tx.clone(),
            command_rx,
            event_hub.clone(),
        )));

        self.state.write().await.threads.insert(
            thread_id.to_owned(),
            AgentThreadHandle {
                workspace_id: workspace_id_owned,
                command_tx,
                event_hub,
                loop_handle,
            },
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_and_security_snapshot(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        reasoning_effort: Option<&str>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
            thread_id,
            turn_id,
            mode,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            Vec::new(),
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            reasoning_effort,
            permission_profile,
            execution_security_snapshot,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        reasoning_effort: Option<&str>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        let reasoning = reasoning_config_from_effort(reasoning_effort)?;
        self.start_turn_with_hook_context_and_execution_checkpoint_and_reasoning(
            thread_id,
            turn_id,
            mode,
            AgentTurnHookRuntimeContext::default(),
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            None,
            reasoning,
            permission_profile,
            Some(execution_security_snapshot),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_permission_profile_and_security_snapshot(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_hook_context_permission_profile_security_snapshot_and_agent_skill_overlay(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            Vec::new(),
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            permission_profile,
            execution_security_snapshot,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_permission_profile_security_snapshot_and_agent_skill_overlay(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_hook_context_and_execution_checkpoint_permission_profile_security_snapshot_and_agent_skill_overlay(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            None,
            permission_profile,
            execution_security_snapshot,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_reasoning_permission_profile_and_security_snapshot(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        reasoning_effort: Option<&str>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_hook_context_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            Vec::new(),
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            reasoning_effort,
            permission_profile,
            execution_security_snapshot,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        reasoning_effort: Option<&str>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        let reasoning = reasoning_config_from_effort(reasoning_effort)?;
        self.start_turn_with_hook_context_and_execution_checkpoint_and_reasoning(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            None,
            reasoning,
            permission_profile,
            Some(execution_security_snapshot),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_and_execution_checkpoint_permission_profile_and_security_snapshot(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        execution_checkpoint_context: Option<ExecutionCheckpointContext>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_hook_context_and_execution_checkpoint_permission_profile_security_snapshot_and_agent_skill_overlay(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            Vec::new(),
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            execution_checkpoint_context,
            permission_profile,
            execution_security_snapshot,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_turn_with_hook_context_and_execution_checkpoint_permission_profile_security_snapshot_and_agent_skill_overlay(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        execution_checkpoint_context: Option<ExecutionCheckpointContext>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: TurnExecutionSecuritySnapshot,
    ) -> Result<(), AgentStartError> {
        self.start_turn_with_hook_context_and_execution_checkpoint_and_reasoning(
            thread_id,
            turn_id,
            mode,
            hook_runtime_context,
            model,
            provider_name,
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            execution_checkpoint_context,
            None,
            permission_profile,
            Some(execution_security_snapshot),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_turn_with_hook_context_and_execution_checkpoint_and_reasoning(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        hook_runtime_context: AgentTurnHookRuntimeContext,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        skill_catalog: SkillCatalogSnapshot,
        agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
        input: Vec<UserInput>,
        capabilities: Vec<TurnCapability>,
        resolved_artifacts: Vec<ResolvedArtifactInput>,
        runtime_environment: HashMap<String, String>,
        history: Vec<ChatMessage>,
        execution_checkpoint_context: Option<ExecutionCheckpointContext>,
        reasoning: Option<ReasoningConfig>,
        permission_profile: TurnPermissionProfileSnapshot,
        execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    ) -> Result<(), AgentStartError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentStartError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::StartTurn {
                turn_id: turn_id.to_owned(),
                mode,
                hook_runtime_context,
                model: model.to_owned(),
                provider_name: provider_name.to_owned(),
                reasoning,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                input,
                capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                execution_checkpoint_context,
                permission_profile,
                execution_security_snapshot,
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentStartError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentStartError::Internal(
                "agent loop dropped ack".to_owned(),
            ))
        })
    }

    pub async fn subscribe_progress(
        &self,
        thread_id: &str,
    ) -> Option<broadcast::Receiver<AgentProgressEvent>> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|thread| thread.event_hub.subscribe_live())
    }

    pub async fn subscribe_committed(
        &self,
        thread_id: &str,
    ) -> Option<broadcast::Receiver<AgentDurableEvent>> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|thread| thread.event_hub.subscribe_committed())
    }

    pub async fn take_durable_receiver(&self, thread_id: &str) -> Option<DurableEventReceiver> {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        }?;
        hub.take_durable_receiver().await
    }

    pub async fn publish_committed(&self, thread_id: &str, event: AgentDurableEvent) {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.publish_committed(event);
        }
    }

    pub async fn publish_progress(&self, thread_id: &str, event: AgentProgressEvent) -> bool {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.publish_progress(event);
            true
        } else {
            false
        }
    }

    pub async fn flush_progress_for_item(
        &self,
        thread_id: &str,
        workspace_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> bool {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.flush_progress_for_item(workspace_id, thread_id, turn_id, item_id)
                .await;
            true
        } else {
            false
        }
    }

    pub async fn cancel_attempt(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::CancelAttempt {
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped cancel ack".to_owned(),
            ))
        })
    }

    pub async fn cancel_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::CancelTurn {
                turn_id: turn_id.to_owned(),
                reason: reason.to_owned(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped cancel turn ack".to_owned(),
            ))
        })
    }

    pub async fn observe_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Option<ExecutionTurnObservation> {
        let command_tx = {
            let state = self.state.read().await;
            state.threads.get(thread_id)?.command_tx.clone()
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        command_tx
            .send(AgentCommand::ObserveTurn {
                turn_id: turn_id.to_owned(),
                ack: ack_tx,
            })
            .await
            .ok()?;
        ack_rx.await.ok().flatten()
    }

    pub async fn start_recovery_attempt(
        &self,
        thread_id: &str,
        request: RecoveryAttemptRequest,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::StartRecoveryAttempt {
                request,
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped recovery ack".to_owned(),
            ))
        })
    }

    pub async fn start_restored_recovery_turn(
        &self,
        thread_id: &str,
        workspace_id: &str,
        turn_request: RestoredRecoveryTurnRequest,
        recovery_request: RecoveryAttemptRequest,
    ) -> Result<(), AgentControlError> {
        self.ensure_thread(thread_id, workspace_id)
            .await
            .map_err(|error| AgentControlError::Internal(error.to_string()))?;

        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::StartRestoredRecoveryTurn {
                turn_request,
                recovery_request,
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped restored recovery ack".to_owned(),
            ))
        })
    }

    pub async fn remove_thread(&self, thread_id: &str) {
        let thread = self.state.write().await.threads.remove(thread_id);
        let Some(thread) = thread else {
            return;
        };

        let _ = thread.command_tx.send(AgentCommand::Shutdown).await;
        thread.loop_handle.abort();
    }

    pub async fn has_thread(&self, thread_id: &str) -> bool {
        self.state.read().await.threads.contains_key(thread_id)
    }
}

#[cfg(test)]
mod event_class_tests {
    use super::*;
    use pioneer_protocol::{ItemDeltaStream, TurnItem};
    use tokio::time::{Duration, sleep, timeout};

    fn reasoning_item(id: &str) -> TurnItem {
        TurnItem::Reasoning {
            id: id.to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        }
    }

    fn durable_turn_completed(turn_id: &str) -> AgentDurableEvent {
        AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: turn_id.to_owned(),
            recovery: None,
        }
    }

    fn test_progress_config() -> ProgressCoalescerConfig {
        ProgressCoalescerConfig {
            flush_interval: Duration::from_secs(60),
            max_pending_keys: 16,
            max_append_bytes_per_key: 128,
            max_snapshot_bytes_per_key: 64,
            max_flush_batch_size: 16,
        }
    }

    fn delta_notification(
        item_id: &str,
        stream: ItemDeltaStream,
        delta: impl Into<String>,
    ) -> ItemDeltaNotification {
        ItemDeltaNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: item_id.to_owned(),
            delta: delta.into(),
            stream: Some(stream),
            payload: None,
            markdown: None,
            markdown_version: None,
        }
    }

    #[tokio::test]
    async fn durable_lane_applies_backpressure_when_full() {
        let hub = Arc::new(AgentEventHub::with_capacity(1, 1));
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");

        hub.publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect("first event should enqueue");

        let pending_hub = hub.clone();
        let pending = tokio::spawn(async move {
            pending_hub
                .publish_durable(durable_turn_completed("turn_2"))
                .await
        });

        sleep(Duration::from_millis(25)).await;
        assert!(
            !pending.is_finished(),
            "full durable queue must apply backpressure instead of dropping"
        );

        assert!(durable_rx.recv().await.is_some());
        pending
            .await
            .expect("publish task should complete")
            .expect("second event should enqueue after capacity is freed");
        assert!(matches!(
            durable_rx.recv().await,
            Some(AgentDurableEvent::TurnCompleted { turn_id, .. }) if turn_id == "turn_2"
        ));
    }

    #[tokio::test]
    async fn closed_durable_lane_is_reported_to_publisher() {
        let hub = AgentEventHub::with_capacity(1, 1);
        let durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");
        drop(durable_rx);

        let error = hub
            .publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect_err("closed durable lane must fail publishing");

        assert_eq!(error, AgentEventHubError::DurableLaneClosed);
    }

    #[tokio::test]
    async fn lagged_live_lane_does_not_drop_durable_events() {
        let hub = AgentEventHub::with_progress_config(8, 1, test_progress_config());
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");
        let _lagged_live_rx = hub.subscribe_live();

        for index in 0..16 {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: ItemDeltaNotification {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "thread_1".to_owned(),
                    turn_id: "turn_1".to_owned(),
                    item_id: "item_1".to_owned(),
                    delta: format!("delta {index}"),
                    stream: Some(ItemDeltaStream::AgentMessage),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            });
        }

        let mut live_rx_after_progress = hub.subscribe_live();
        hub.publish_durable(AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            recovery: None,
        })
        .await
        .expect("durable publish must not depend on live receiver health");

        let event = timeout(Duration::from_secs(1), durable_rx.recv())
            .await
            .expect("durable event should arrive")
            .expect("durable lane should remain open");
        assert!(matches!(event, AgentDurableEvent::TurnCompleted { .. }));

        let event = timeout(Duration::from_secs(1), live_rx_after_progress.recv())
            .await
            .expect("pending progress should flush before durable completion")
            .expect("live lane should remain open");
        assert!(
            matches!(event, AgentProgressEvent::ItemDelta { .. }),
            "durable events must not be mirrored into the lossy live lane"
        );
        assert!(
            timeout(Duration::from_millis(25), live_rx_after_progress.recv())
                .await
                .is_err(),
            "progress should be coalesced into a bounded live update"
        );
    }

    #[tokio::test]
    async fn raw_progress_waits_for_coalescer_flush() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        hub.publish_progress(AgentProgressEvent::ItemDelta {
            notification: delta_notification(
                "item_1",
                ItemDeltaStream::AgentMessage,
                "first delta",
            ),
        });

        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "raw progress must not bypass the coalescer"
        );

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "first delta"
        ));
    }

    #[tokio::test]
    async fn append_progress_is_coalesced_and_bounded() {
        let hub = AgentEventHub::with_progress_config(
            8,
            8,
            ProgressCoalescerConfig {
                max_append_bytes_per_key: 14,
                ..test_progress_config()
            },
        );
        let mut live_rx = hub.subscribe_live();

        for delta in ["aaa", "bbb", "ccc", "ddd", "eee", "fff"] {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: delta_notification("item_1", ItemDeltaStream::AgentMessage, delta),
            });
        }

        hub.shutdown_progress().await;
        let notification = match timeout(Duration::from_secs(1), live_rx.recv()).await {
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) => notification,
            other => panic!("expected coalesced item delta, got {other:?}"),
        };
        assert!(notification.delta.len() <= 14);
        assert!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("truncated"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        );
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "append progress should flush as one bounded update for one key"
        );
    }

    #[tokio::test]
    async fn snapshot_progress_replaces_older_snapshots() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for stage in ["queued", "running", "almost done"] {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: delta_notification("tool_1", ItemDeltaStream::ToolProgress, stage),
            });
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "almost done"
        ));
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "snapshot progress should replace stale snapshots"
        );
    }

    #[tokio::test]
    async fn item_completed_flushes_progress_before_durable_event() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");

        hub.publish_progress(AgentProgressEvent::ItemDelta {
            notification: delta_notification("item_1", ItemDeltaStream::Generic, "thinking"),
        });
        hub.publish_durable(AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item: reasoning_item("item_1"),
            },
        })
        .await
        .expect("durable completion should publish");

        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "thinking"
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), durable_rx.recv()).await,
            Ok(Some(AgentDurableEvent::ItemCompleted { notification })) if notification.item.item_id() == "item_1"
        ));
    }

    #[tokio::test]
    async fn heartbeats_are_coalesced() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for _ in 0..4 {
            hub.publish_heartbeat(
                "ws_1".to_owned(),
                "thread_1".to_owned(),
                "turn_1".to_owned(),
                "item_1".to_owned(),
                TurnItemType::Reasoning,
            );
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemHeartbeat { item_id, .. })) if item_id == "item_1"
        ));
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "heartbeat progress should be rate-limited per item"
        );
    }

    #[tokio::test]
    async fn task_progress_uses_snapshot_semantics() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for summary in ["started", "halfway", "done soon"] {
            hub.publish_progress(AgentProgressEvent::TaskProgress {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item_id: "task_item_task_1".to_owned(),
                task_id: "task_1".to_owned(),
                run_id: Some("run_1".to_owned()),
                summary: summary.to_owned(),
            });
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification }))
                if notification.stream == Some(ItemDeltaStream::ToolProgress)
                    && notification.delta == "done soon"
        ));
    }

    #[tokio::test]
    async fn committed_lane_is_emitted_explicitly_after_durable_publish() {
        let hub = AgentEventHub::with_capacity(8, 8);
        let mut committed_rx = hub.subscribe_committed();

        hub.publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect("durable publish should succeed");
        assert!(
            timeout(Duration::from_millis(25), committed_rx.recv())
                .await
                .is_err(),
            "raw durable ingress must not notify committed subscribers"
        );

        hub.publish_committed(AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            recovery: None,
        });
        assert!(matches!(
            timeout(Duration::from_secs(1), committed_rx.recv()).await,
            Ok(Ok(AgentDurableEvent::TurnCompleted { turn_id, .. })) if turn_id == "turn_1"
        ));
    }
}
