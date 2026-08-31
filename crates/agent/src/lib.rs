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

use pioneer_hooks::{HookMetadataKey, HookPhase, HookPhaseRequest, HookRuntime, HookValue};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ExecutionCheckpointPayload,
    ExecutionWindowExhaustionReason, McpScopeKind, NativeTerminalEffectPayload,
    ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage, ThreadMode, TurnCapability,
    TurnExecutionSecuritySnapshot, TurnItemType, TurnPermissionProfileSnapshot, UserInput,
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
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use hooks::{
    AgentPostTurnHookDispatchPolicy, AgentTurnHookRuntimeContext, AgentTurnPostTurnDispatchMode,
};
use hooks::{AgentToolBundleArtifactStore, DurablePostTurnHookRuntimeSnapshot};
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
use sha2::{Digest, Sha256};

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
const DEFAULT_CONTROL_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_CONTROL_ACK_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_CONTROL_OUTCOME_CAPACITY: usize = 512;
const MAX_CONTROL_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONTROL_ACK_TIMEOUT: Duration = Duration::from_secs(120);

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

/// One authoritative read of all task outcomes that can prevent a parent
/// Turn from reaching its final answer.  The revision is a durable read token
/// supplied by the task service; callers must not interpret a failed read as
/// an empty snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskFinalizationSnapshot {
    pub revision: String,
    pub pending: Vec<PendingAttachedTask>,
    pub review_required: Vec<ReviewRequiredTaskObservation>,
    pub terminal: Vec<TerminalTaskObservation>,
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
    /// Stable, versioned identifier for the durable attached-task cleanup
    /// semantics implemented by this provider. Change this value whenever a
    /// replacement provider would interpret an existing cleanup obligation
    /// differently.
    fn terminal_cleanup_runtime_contract(&self) -> &'static str;

    async fn materialize_task_tools(
        &self,
        context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String>;

    /// Read the parent task gate as one logical snapshot.  The default is
    /// retained for lightweight providers and test doubles; the Gateway
    /// implementation overrides it with a single Task Service wait snapshot.
    async fn attached_task_finalization_snapshot(
        &self,
        context: TaskTurnContext,
    ) -> Result<TaskFinalizationSnapshot, String> {
        let pending = self.pending_attached_tasks(context.clone()).await?;
        let review_required = self
            .review_required_attached_task_observations(context.clone())
            .await?;
        let terminal = self.terminal_attached_task_observations(context).await?;
        Ok(TaskFinalizationSnapshot {
            revision: format!(
                "pending:{};review:{};terminal:{}",
                pending.len(),
                review_required.len(),
                terminal.len()
            ),
            pending,
            review_required,
            terminal,
        })
    }

    /// Wait for a bounded interval while attached work is still pending, then
    /// return a fresh logical finalization snapshot. Runtime providers should
    /// override this with their durable Task event wait; the default keeps
    /// lightweight providers fair without requiring an event source.
    async fn wait_for_attached_task_finalization_snapshot(
        &self,
        context: TaskTurnContext,
        timeout_ms: u64,
    ) -> Result<TaskFinalizationSnapshot, String> {
        let snapshot = self
            .attached_task_finalization_snapshot(context.clone())
            .await?;
        if snapshot.pending.is_empty() || timeout_ms == 0 {
            return Ok(snapshot);
        }
        tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)).await;
        self.attached_task_finalization_snapshot(context).await
    }

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

    async fn cleanup_attached_tasks_idempotent(
        &self,
        effect_id: &str,
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

/// Execution bounds for the native thread control plane. An enqueue timeout
/// abandons only an unaccepted send. An ACK timeout identifies an unresponsive
/// actor generation, which the manager fences before durable recovery is
/// allowed to create a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentControlPlaneConfig {
    pub enqueue_timeout: Duration,
    pub acknowledgement_timeout: Duration,
    pub outcome_capacity_per_thread: usize,
}

const MAX_CONTROL_OUTCOME_CAPACITY_PER_THREAD: usize = 1_024;
const RETIRED_CONTROL_OUTCOME_CAPACITY: usize = 4_096;

impl Default for AgentControlPlaneConfig {
    fn default() -> Self {
        Self {
            enqueue_timeout: DEFAULT_CONTROL_ENQUEUE_TIMEOUT,
            acknowledgement_timeout: DEFAULT_CONTROL_ACK_TIMEOUT,
            outcome_capacity_per_thread: DEFAULT_CONTROL_OUTCOME_CAPACITY,
        }
    }
}

impl AgentControlPlaneConfig {
    fn normalized(self) -> Self {
        Self {
            enqueue_timeout: self
                .enqueue_timeout
                .clamp(Duration::from_millis(1), MAX_CONTROL_ENQUEUE_TIMEOUT),
            acknowledgement_timeout: self
                .acknowledgement_timeout
                .clamp(Duration::from_millis(1), MAX_CONTROL_ACK_TIMEOUT),
            outcome_capacity_per_thread: self
                .outcome_capacity_per_thread
                .clamp(1, MAX_CONTROL_OUTCOME_CAPACITY_PER_THREAD),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgentControlOperation {
    StartTurn,
    CancelAttempt,
    CancelTurn,
    ObserveTurn,
    StartRecoveryAttempt,
    StartRestoredRecoveryTurn,
}

impl Display for AgentControlOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StartTurn => "start_turn",
            Self::CancelAttempt => "cancel_attempt",
            Self::CancelTurn => "cancel_turn",
            Self::ObserveTurn => "observe_turn",
            Self::StartRecoveryAttempt => "start_recovery_attempt",
            Self::StartRestoredRecoveryTurn => "start_restored_recovery_turn",
        })
    }
}

/// Stable semantic identity used to reconcile a control request after its
/// caller stopped waiting. The identifiers contain durable object IDs only;
/// credentials and request payloads never enter this registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AgentControlOperationId {
    StartTurn { turn_id: String },
    CancelAttempt { turn_id: String, item_id: String },
    CancelTurn { turn_id: String },
    StartRecoveryAttempt { recovery_attempt_id: String },
    StartRestoredRecoveryTurn { recovery_attempt_id: String },
}

impl AgentControlOperationId {
    fn operation(&self) -> AgentControlOperation {
        match self {
            Self::StartTurn { .. } => AgentControlOperation::StartTurn,
            Self::CancelAttempt { .. } => AgentControlOperation::CancelAttempt,
            Self::CancelTurn { .. } => AgentControlOperation::CancelTurn,
            Self::StartRecoveryAttempt { .. } => AgentControlOperation::StartRecoveryAttempt,
            Self::StartRestoredRecoveryTurn { .. } => {
                AgentControlOperation::StartRestoredRecoveryTurn
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentControlOperationFailure {
    Start(AgentStartError),
    Control(AgentControlError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentControlOperationStatus {
    Pending {
        actor_generation: u64,
    },
    Applied {
        actor_generation: u64,
    },
    Rejected {
        actor_generation: u64,
        failure: AgentControlOperationFailure,
    },
    /// The actor generation was fenced before its accepted command produced
    /// a trustworthy ACK. Callers must inspect durable domain state before
    /// deciding whether to retry the semantic objective.
    ReconciliationRequired {
        actor_generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStartError {
    ThreadNotFound,
    TurnAlreadyRunning,
    ThreadWorkspaceMismatch {
        expected_workspace_id: String,
        actual_workspace_id: String,
    },
    MailboxEnqueueTimeout {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    AcknowledgementTimeout {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    AcknowledgementDropped {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    LoopUnavailable {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    OperationPending {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    ReconciliationCapacityExceeded {
        operation: AgentControlOperation,
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
            Self::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control mailbox enqueue timed out for {operation} on actor generation {actor_generation}"
                )
            }
            Self::AcknowledgementTimeout {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control acknowledgement timed out for {operation} on actor generation {actor_generation}"
                )
            }
            Self::AcknowledgementDropped {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control acknowledgement was dropped for {operation} on actor generation {actor_generation}"
                )
            }
            Self::LoopUnavailable {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent loop is unavailable for {operation} on actor generation {actor_generation}"
                )
            }
            Self::OperationPending {
                operation,
                actor_generation,
            } => write!(
                f,
                "agent control operation {operation} is already pending on actor generation {actor_generation}"
            ),
            Self::ReconciliationCapacityExceeded { operation } => write!(
                f,
                "agent control reconciliation capacity is exhausted for {operation}"
            ),
            Self::Internal(error) => write!(f, "internal agent error: {error}"),
        }
    }
}

impl Error for AgentStartError {}

impl AgentStartError {
    fn unresponsive_actor_generation(&self) -> Option<u64> {
        match self {
            Self::MailboxEnqueueTimeout {
                actor_generation, ..
            }
            | Self::AcknowledgementTimeout {
                actor_generation, ..
            }
            | Self::AcknowledgementDropped {
                actor_generation, ..
            }
            | Self::LoopUnavailable {
                actor_generation, ..
            } => Some(*actor_generation),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentControlError {
    ThreadNotFound,
    NoActiveTurn,
    TurnMismatch,
    TurnAlreadyRunning,
    AttemptNotRunning,
    ExecutionWindowContinuationBlocked {
        reason: String,
    },
    MailboxEnqueueTimeout {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    AcknowledgementTimeout {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    AcknowledgementDropped {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    LoopUnavailable {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    OperationPending {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    ReconciliationCapacityExceeded {
        operation: AgentControlOperation,
    },
    StaleGeneration {
        expected: u64,
        actual: u64,
    },
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
            Self::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control mailbox enqueue timed out for {operation} on actor generation {actor_generation}"
                )
            }
            Self::AcknowledgementTimeout {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control acknowledgement timed out for {operation} on actor generation {actor_generation}"
                )
            }
            Self::AcknowledgementDropped {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent control acknowledgement was dropped for {operation} on actor generation {actor_generation}"
                )
            }
            Self::LoopUnavailable {
                operation,
                actor_generation,
            } => {
                write!(
                    f,
                    "agent loop is unavailable for {operation} on actor generation {actor_generation}"
                )
            }
            Self::OperationPending {
                operation,
                actor_generation,
            } => write!(
                f,
                "agent control operation {operation} is already pending on actor generation {actor_generation}"
            ),
            Self::ReconciliationCapacityExceeded { operation } => write!(
                f,
                "agent control reconciliation capacity is exhausted for {operation}"
            ),
            Self::StaleGeneration { expected, actual } => write!(
                f,
                "stale agent actor generation: expected {expected}, current {actual}"
            ),
            Self::Internal(error) => write!(f, "internal agent control error: {error}"),
        }
    }
}

impl Error for AgentControlError {}

impl AgentControlError {
    fn unresponsive_actor_generation(&self) -> Option<u64> {
        match self {
            Self::MailboxEnqueueTimeout {
                actor_generation, ..
            }
            | Self::AcknowledgementTimeout {
                actor_generation, ..
            }
            | Self::AcknowledgementDropped {
                actor_generation, ..
            }
            | Self::LoopUnavailable {
                actor_generation, ..
            } => Some(*actor_generation),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTerminalEffectExecutionError {
    RuntimeUnavailable,
    ProviderUnavailable,
    InvalidPayload(String),
    HookFailed { message: String, retryable: bool },
    CleanupFailed(String),
}

impl AgentTerminalEffectExecutionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RuntimeUnavailable => "hook_runtime_unavailable",
            Self::ProviderUnavailable => "task_provider_unavailable",
            Self::InvalidPayload(_) => "invalid_payload",
            Self::HookFailed { .. } => "hook_failed",
            Self::CleanupFailed(_) => "cleanup_failed",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::RuntimeUnavailable
                | Self::ProviderUnavailable
                | Self::HookFailed {
                    retryable: true,
                    ..
                }
                | Self::CleanupFailed(_)
        )
    }
}

impl Display for AgentTerminalEffectExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeUnavailable => f.write_str("post-turn hook runtime is unavailable"),
            Self::ProviderUnavailable => f.write_str("attached-task provider is unavailable"),
            Self::InvalidPayload(error) => write!(f, "invalid terminal effect payload: {error}"),
            Self::HookFailed { message, .. } => {
                write!(f, "post-turn hook failed: {message}")
            }
            Self::CleanupFailed(error) => write!(f, "attached-task cleanup failed: {error}"),
        }
    }
}

impl Error for AgentTerminalEffectExecutionError {}

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

#[derive(Debug, Clone)]
enum StoredControlOutcome {
    Start(Result<(), AgentStartError>),
    Control(Result<(), AgentControlError>),
}

impl StoredControlOutcome {
    fn is_rejected(&self) -> bool {
        matches!(self, Self::Start(Err(_)) | Self::Control(Err(_)))
    }
}

#[derive(Debug, Clone)]
struct StoredControlOperation {
    actor_generation: u64,
    state: StoredControlOperationState,
}

#[derive(Debug, Clone)]
enum StoredControlOperationState {
    /// The caller owns an in-flight mailbox send. If that caller is dropped,
    /// Tokio drops the unsent message and this entry may be re-admitted after
    /// the enqueue deadline without fencing a healthy actor.
    Dispatching {
        deadline: Instant,
    },
    /// The mailbox accepted the command. Its ACK is now authoritative: once
    /// this deadline expires, the exact actor generation must be fenced before
    /// durable reconciliation creates a replacement.
    Enqueued {
        deadline: Instant,
    },
    Completed(StoredControlOutcome),
}

#[derive(Debug, Clone)]
enum ControlOperationAdmission {
    Fresh,
    Pending,
    Completed(StoredControlOutcome),
    EnqueuedDeadlineExceeded {
        operation: AgentControlOperation,
        actor_generation: u64,
    },
    Saturated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlOperationRetirementGate {
    Ready,
    WaitUntil(Instant),
    FenceExpiredEnqueued,
}

#[derive(Debug, Default)]
struct ControlOperationRegistryState {
    entries: HashMap<AgentControlOperationId, StoredControlOperation>,
    order: VecDeque<AgentControlOperationId>,
}

/// Per-thread bounded reconciliation index. It is intentionally independent
/// from the actor mailbox so an ACK timeout can be inspected while the actor
/// finishes applying the already accepted command.
#[derive(Debug)]
struct ControlOperationRegistry {
    capacity: usize,
    enqueue_timeout: Duration,
    acknowledgement_timeout: Duration,
    state: StdMutex<ControlOperationRegistryState>,
    retirement_notify: Arc<Notify>,
}

impl ControlOperationRegistry {
    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_config(AgentControlPlaneConfig {
            outcome_capacity_per_thread: capacity,
            ..AgentControlPlaneConfig::default()
        })
    }

    fn with_config(config: AgentControlPlaneConfig) -> Self {
        let config = config.normalized();
        Self {
            capacity: config.outcome_capacity_per_thread,
            enqueue_timeout: config.enqueue_timeout,
            acknowledgement_timeout: config.acknowledgement_timeout,
            state: StdMutex::new(ControlOperationRegistryState::default()),
            retirement_notify: Arc::new(Notify::new()),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ControlOperationRegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn begin(
        &self,
        operation_id: AgentControlOperationId,
        actor_generation: u64,
    ) -> ControlOperationAdmission {
        self.begin_at(operation_id, actor_generation, Instant::now())
    }

    fn begin_at(
        &self,
        operation_id: AgentControlOperationId,
        actor_generation: u64,
        now: Instant,
    ) -> ControlOperationAdmission {
        let mut state = self.lock_state();
        if let Some(existing) = state.entries.get(&operation_id)
            && existing.actor_generation == actor_generation
        {
            match existing.state.clone() {
                StoredControlOperationState::Completed(outcome) if !outcome.is_rejected() => {
                    return ControlOperationAdmission::Completed(outcome);
                }
                StoredControlOperationState::Dispatching { deadline } if now < deadline => {
                    return ControlOperationAdmission::Pending;
                }
                StoredControlOperationState::Enqueued { deadline } if now < deadline => {
                    return ControlOperationAdmission::Pending;
                }
                StoredControlOperationState::Enqueued { .. } => {
                    return ControlOperationAdmission::EnqueuedDeadlineExceeded {
                        operation: operation_id.operation(),
                        actor_generation,
                    };
                }
                StoredControlOperationState::Dispatching { .. } => {
                    // A canceled sender cannot have transferred ownership of
                    // the command to the mailbox. Its expired reservation is
                    // safe to discard and re-admit below.
                }
                StoredControlOperationState::Completed(_) => {
                    // A typed rejection proves that the objective was not
                    // applied. It remains queryable until the next semantic
                    // retry, but must not poison that stable operation ID
                    // after actor state or provider authority changes.
                }
            }
        }

        // A replacement actor owns a disjoint reconciliation generation. A
        // late completion from the old actor is fenced in `complete` below.
        state.entries.remove(&operation_id);
        state.order.retain(|candidate| candidate != &operation_id);
        while state.entries.len() >= self.capacity {
            if let Some((expired_operation, expired_generation)) = state
                .order
                .iter()
                .filter_map(|candidate| {
                    state.entries.get(candidate).map(|entry| (candidate, entry))
                })
                .find_map(|(candidate, entry)| match &entry.state {
                    StoredControlOperationState::Enqueued { deadline } if now >= *deadline => {
                        Some((candidate.operation(), entry.actor_generation))
                    }
                    _ => None,
                })
            {
                return ControlOperationAdmission::EnqueuedDeadlineExceeded {
                    operation: expired_operation,
                    actor_generation: expired_generation,
                };
            }
            let Some(completed_index) = state.order.iter().position(|candidate| {
                state.entries.get(candidate).is_some_and(|entry| {
                    matches!(&entry.state, StoredControlOperationState::Completed(_))
                        || matches!(
                            &entry.state,
                            StoredControlOperationState::Dispatching { deadline }
                                if now >= *deadline
                        )
                })
            }) else {
                // Accepted Pending commands are reconciliation authority. Do
                // not evict them merely to admit more mailbox traffic.
                return ControlOperationAdmission::Saturated;
            };
            if let Some(evicted) = state.order.remove(completed_index) {
                state.entries.remove(&evicted);
            }
        }
        state.order.push_back(operation_id.clone());
        state.entries.insert(
            operation_id,
            StoredControlOperation {
                actor_generation,
                state: StoredControlOperationState::Dispatching {
                    deadline: deadline_after(now, self.enqueue_timeout),
                },
            },
        );
        self.retirement_notify.notify_one();
        ControlOperationAdmission::Fresh
    }

    fn mark_enqueued(&self, operation_id: &AgentControlOperationId, actor_generation: u64) {
        self.mark_enqueued_at(operation_id, actor_generation, Instant::now());
    }

    fn mark_enqueued_at(
        &self,
        operation_id: &AgentControlOperationId,
        actor_generation: u64,
        now: Instant,
    ) {
        let mut state = self.lock_state();
        let Some(entry) = state.entries.get_mut(operation_id) else {
            return;
        };
        if entry.actor_generation == actor_generation
            && matches!(
                &entry.state,
                StoredControlOperationState::Dispatching { .. }
            )
        {
            entry.state = StoredControlOperationState::Enqueued {
                deadline: deadline_after(now, self.acknowledgement_timeout),
            };
            self.retirement_notify.notify_one();
        }
    }

    fn abandon_pending(&self, operation_id: &AgentControlOperationId, actor_generation: u64) {
        let mut state = self.lock_state();
        let remove = state.entries.get(operation_id).is_some_and(|entry| {
            entry.actor_generation == actor_generation
                && !matches!(&entry.state, StoredControlOperationState::Completed(_))
        });
        if remove {
            state.entries.remove(operation_id);
            state.order.retain(|candidate| candidate != operation_id);
            self.retirement_notify.notify_one();
        }
    }

    fn retirement_gate(&self, now: Instant) -> ControlOperationRetirementGate {
        let mut state = self.lock_state();
        let expired_dispatches = state
            .order
            .iter()
            .filter(|operation_id| {
                state.entries.get(*operation_id).is_some_and(|entry| {
                    matches!(
                        &entry.state,
                        StoredControlOperationState::Dispatching { deadline } if now >= *deadline
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for operation_id in &expired_dispatches {
            state.entries.remove(operation_id);
        }
        if !expired_dispatches.is_empty() {
            state
                .order
                .retain(|operation_id| !expired_dispatches.contains(operation_id));
        }

        let mut wait_until = None;
        for entry in state.entries.values() {
            match &entry.state {
                StoredControlOperationState::Completed(_) => {}
                StoredControlOperationState::Enqueued { deadline } if now >= *deadline => {
                    return ControlOperationRetirementGate::FenceExpiredEnqueued;
                }
                StoredControlOperationState::Dispatching { deadline }
                | StoredControlOperationState::Enqueued { deadline } => {
                    wait_until = Some(
                        wait_until.map_or(*deadline, |current: Instant| current.min(*deadline)),
                    );
                }
            }
        }
        wait_until.map_or(ControlOperationRetirementGate::Ready, |deadline| {
            ControlOperationRetirementGate::WaitUntil(deadline)
        })
    }

    fn complete(
        &self,
        operation_id: &AgentControlOperationId,
        actor_generation: u64,
        attempted: StoredControlOutcome,
    ) -> StoredControlOutcome {
        let mut state = self.lock_state();
        let Some(existing) = state.entries.get(operation_id) else {
            return attempted;
        };
        if existing.actor_generation != actor_generation {
            return attempted;
        }
        if let StoredControlOperationState::Completed(outcome) = &existing.state {
            return outcome.clone();
        }
        if let Some(existing) = state.entries.get_mut(operation_id) {
            existing.state = StoredControlOperationState::Completed(attempted.clone());
        }
        state.order.retain(|candidate| candidate != operation_id);
        state.order.push_back(operation_id.clone());
        self.retirement_notify.notify_one();
        attempted
    }

    fn completed(
        &self,
        operation_id: &AgentControlOperationId,
        actor_generation: u64,
    ) -> Option<StoredControlOutcome> {
        self.lock_state()
            .entries
            .get(operation_id)
            .filter(|entry| entry.actor_generation == actor_generation)
            .and_then(|entry| match &entry.state {
                StoredControlOperationState::Completed(outcome) => Some(outcome.clone()),
                StoredControlOperationState::Dispatching { .. }
                | StoredControlOperationState::Enqueued { .. } => None,
            })
    }

    fn status(
        &self,
        operation_id: &AgentControlOperationId,
    ) -> Option<AgentControlOperationStatus> {
        let state = self.lock_state();
        let entry = state.entries.get(operation_id)?;
        Some(Self::status_for_entry(entry, false))
    }

    fn retirement_snapshot(&self) -> Vec<(AgentControlOperationId, AgentControlOperationStatus)> {
        // Expired Dispatching reservations were never transferred to the
        // mailbox and therefore are not reconciliation authority.
        let _ = self.retirement_gate(Instant::now());
        let state = self.lock_state();
        state
            .order
            .iter()
            .filter_map(|operation_id| {
                state
                    .entries
                    .get(operation_id)
                    .map(|entry| (operation_id.clone(), Self::status_for_entry(entry, true)))
            })
            .collect()
    }

    fn status_for_entry(
        entry: &StoredControlOperation,
        retiring: bool,
    ) -> AgentControlOperationStatus {
        match &entry.state {
            StoredControlOperationState::Dispatching { .. }
            | StoredControlOperationState::Enqueued { .. } => {
                if retiring {
                    AgentControlOperationStatus::ReconciliationRequired {
                        actor_generation: entry.actor_generation,
                    }
                } else {
                    AgentControlOperationStatus::Pending {
                        actor_generation: entry.actor_generation,
                    }
                }
            }
            StoredControlOperationState::Completed(StoredControlOutcome::Start(Ok(())))
            | StoredControlOperationState::Completed(StoredControlOutcome::Control(Ok(()))) => {
                AgentControlOperationStatus::Applied {
                    actor_generation: entry.actor_generation,
                }
            }
            StoredControlOperationState::Completed(StoredControlOutcome::Start(Err(error))) => {
                AgentControlOperationStatus::Rejected {
                    actor_generation: entry.actor_generation,
                    failure: AgentControlOperationFailure::Start(error.clone()),
                }
            }
            StoredControlOperationState::Completed(StoredControlOutcome::Control(Err(error))) => {
                AgentControlOperationStatus::Rejected {
                    actor_generation: entry.actor_generation,
                    failure: AgentControlOperationFailure::Control(error.clone()),
                }
            }
        }
    }
}

fn deadline_after(now: Instant, timeout: Duration) -> Instant {
    // An unrepresentable configured duration must fail closed instead of
    // turning a reconciliation entry into an immortal registry occupant.
    now.checked_add(timeout).unwrap_or(now)
}

#[derive(Debug)]
struct AgentStartAck {
    sender: oneshot::Sender<Result<(), AgentStartError>>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    outcomes: Arc<ControlOperationRegistry>,
}

impl AgentStartAck {
    fn completed(&self) -> Option<Result<(), AgentStartError>> {
        match self
            .outcomes
            .completed(&self.operation_id, self.actor_generation)
        {
            Some(StoredControlOutcome::Start(outcome)) => Some(outcome),
            _ => None,
        }
    }

    fn send(
        self,
        attempted: Result<(), AgentStartError>,
    ) -> Result<(), Result<(), AgentStartError>> {
        let canonical = match self.outcomes.complete(
            &self.operation_id,
            self.actor_generation,
            StoredControlOutcome::Start(attempted),
        ) {
            StoredControlOutcome::Start(outcome) => outcome,
            StoredControlOutcome::Control(_) => Err(AgentStartError::Internal(
                "control reconciliation outcome type changed".to_owned(),
            )),
        };
        self.sender.send(canonical)
    }
}

#[derive(Debug)]
struct AgentControlAck {
    sender: oneshot::Sender<Result<(), AgentControlError>>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    outcomes: Arc<ControlOperationRegistry>,
}

impl AgentControlAck {
    fn completed(&self) -> Option<Result<(), AgentControlError>> {
        match self
            .outcomes
            .completed(&self.operation_id, self.actor_generation)
        {
            Some(StoredControlOutcome::Control(outcome)) => Some(outcome),
            _ => None,
        }
    }

    fn send(
        self,
        attempted: Result<(), AgentControlError>,
    ) -> Result<(), Result<(), AgentControlError>> {
        let canonical = match self.outcomes.complete(
            &self.operation_id,
            self.actor_generation,
            StoredControlOutcome::Control(attempted),
        ) {
            StoredControlOutcome::Control(outcome) => outcome,
            StoredControlOutcome::Start(_) => Err(AgentControlError::Internal(
                "control reconciliation outcome type changed".to_owned(),
            )),
        };
        self.sender.send(canonical)
    }
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
        ack: AgentStartAck,
    },
    TurnTaskFinished {
        turn_id: String,
        run_id: u64,
        completion: TurnTaskCompletion,
    },
    CancelAttempt {
        turn_id: String,
        item_id: String,
        ack: AgentControlAck,
    },
    CancelTurn {
        turn_id: String,
        reason: String,
        ack: AgentControlAck,
    },
    ObserveTurn {
        turn_id: String,
        ack: oneshot::Sender<Result<Option<ExecutionTurnObservation>, AgentControlError>>,
    },
    StartRecoveryAttempt {
        request: RecoveryAttemptRequest,
        ack: AgentControlAck,
    },
    StartRestoredRecoveryTurn {
        turn_request: RestoredRecoveryTurnRequest,
        recovery_request: RecoveryAttemptRequest,
        ack: AgentControlAck,
    },
    RecoveryAttemptSucceeded {
        turn_id: String,
        run_id: u64,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    },
    Shutdown,
}

fn cached_start_outcome(
    outcomes: &ControlOperationRegistry,
    operation_id: &AgentControlOperationId,
    actor_generation: u64,
) -> Result<Option<Result<(), AgentStartError>>, AgentStartError> {
    let operation = operation_id.operation();
    match outcomes.begin(operation_id.clone(), actor_generation) {
        ControlOperationAdmission::Fresh => Ok(None),
        ControlOperationAdmission::Pending => Err(AgentStartError::OperationPending {
            operation,
            actor_generation,
        }),
        ControlOperationAdmission::Completed(StoredControlOutcome::Start(outcome)) => {
            Ok(Some(outcome))
        }
        ControlOperationAdmission::Completed(StoredControlOutcome::Control(_)) => {
            Err(AgentStartError::Internal(
                "control operation ID was reconciled with a non-start outcome".to_owned(),
            ))
        }
        ControlOperationAdmission::EnqueuedDeadlineExceeded {
            operation,
            actor_generation,
        } => Err(AgentStartError::AcknowledgementTimeout {
            operation,
            actor_generation,
        }),
        ControlOperationAdmission::Saturated => {
            Err(AgentStartError::ReconciliationCapacityExceeded { operation })
        }
    }
}

fn cached_control_outcome(
    outcomes: &ControlOperationRegistry,
    operation_id: &AgentControlOperationId,
    actor_generation: u64,
) -> Result<Option<Result<(), AgentControlError>>, AgentControlError> {
    let operation = operation_id.operation();
    match outcomes.begin(operation_id.clone(), actor_generation) {
        ControlOperationAdmission::Fresh => Ok(None),
        ControlOperationAdmission::Pending => Err(AgentControlError::OperationPending {
            operation,
            actor_generation,
        }),
        ControlOperationAdmission::Completed(StoredControlOutcome::Control(outcome)) => {
            Ok(Some(outcome))
        }
        ControlOperationAdmission::Completed(StoredControlOutcome::Start(_)) => {
            Err(AgentControlError::Internal(
                "control operation ID was reconciled with a start outcome".to_owned(),
            ))
        }
        ControlOperationAdmission::EnqueuedDeadlineExceeded {
            operation,
            actor_generation,
        } => Err(AgentControlError::AcknowledgementTimeout {
            operation,
            actor_generation,
        }),
        ControlOperationAdmission::Saturated => {
            Err(AgentControlError::ReconciliationCapacityExceeded { operation })
        }
    }
}

async fn dispatch_start_command(
    command_tx: mpsc::Sender<AgentCommand>,
    command: AgentCommand,
    ack_rx: oneshot::Receiver<Result<(), AgentStartError>>,
    outcomes: Arc<ControlOperationRegistry>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    config: AgentControlPlaneConfig,
) -> Result<(), AgentStartError> {
    use pioneer_observability::{NativeLifecycleOutcome as Outcome, NativeLifecycleStage as Stage};

    let started = Instant::now();
    let operation = operation_id.operation();
    let permit = match tokio::time::timeout(config.enqueue_timeout, command_tx.reserve()).await {
        Err(_) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            pioneer_observability::record_native_lifecycle_event(
                pioneer_observability::NativeLifecycleEventMetric {
                    stage: Stage::Turn,
                    outcome: Outcome::TimedOut,
                    provider_class: pioneer_observability::NativeProviderClass::Unknown,
                    elapsed: Some(started.elapsed()),
                },
            );
            return Err(AgentStartError::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            });
        }
        Ok(Err(_)) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            pioneer_observability::record_native_lifecycle_event(
                pioneer_observability::NativeLifecycleEventMetric {
                    stage: Stage::Turn,
                    outcome: Outcome::Closed,
                    provider_class: pioneer_observability::NativeProviderClass::Unknown,
                    elapsed: Some(started.elapsed()),
                },
            );
            return Err(AgentStartError::LoopUnavailable {
                operation,
                actor_generation,
            });
        }
        Ok(Ok(permit)) => permit,
    };
    // No await is allowed between marking mailbox ownership and sending via
    // the reserved slot. Dropping the caller can therefore leave either an
    // unaccepted Dispatching reservation or an accepted Enqueued command,
    // never an ambiguous state between the two.
    outcomes.mark_enqueued(&operation_id, actor_generation);
    permit.send(command);

    let result = match tokio::time::timeout(config.acknowledgement_timeout, ack_rx).await {
        Err(_) => Err(AgentStartError::AcknowledgementTimeout {
            operation,
            actor_generation,
        }),
        Ok(Err(_)) => Err(AgentStartError::AcknowledgementDropped {
            operation,
            actor_generation,
        }),
        Ok(Ok(outcome)) => outcome,
    };
    let outcome = match &result {
        Ok(()) => Outcome::Started,
        Err(AgentStartError::AcknowledgementTimeout { .. }) => Outcome::TimedOut,
        Err(AgentStartError::AcknowledgementDropped { .. }) => Outcome::Closed,
        Err(_) => Outcome::Rejected,
    };
    pioneer_observability::record_native_lifecycle_event(
        pioneer_observability::NativeLifecycleEventMetric {
            stage: Stage::Turn,
            outcome,
            provider_class: pioneer_observability::NativeProviderClass::Unknown,
            elapsed: Some(started.elapsed()),
        },
    );
    result
}

async fn dispatch_control_command(
    command_tx: mpsc::Sender<AgentCommand>,
    command: AgentCommand,
    ack_rx: oneshot::Receiver<Result<(), AgentControlError>>,
    outcomes: Arc<ControlOperationRegistry>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    config: AgentControlPlaneConfig,
) -> Result<(), AgentControlError> {
    let operation = operation_id.operation();
    let permit = match tokio::time::timeout(config.enqueue_timeout, command_tx.reserve()).await {
        Err(_) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            return Err(AgentControlError::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            });
        }
        Ok(Err(_)) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            return Err(AgentControlError::LoopUnavailable {
                operation,
                actor_generation,
            });
        }
        Ok(Ok(permit)) => permit,
    };
    outcomes.mark_enqueued(&operation_id, actor_generation);
    permit.send(command);

    await_control_ack(
        ack_rx,
        operation_id,
        actor_generation,
        config.acknowledgement_timeout,
    )
    .await
}

async fn dispatch_cancel_turn_command(
    command_tx: mpsc::Sender<AgentCommand>,
    command: AgentCommand,
    ack_rx: oneshot::Receiver<Result<(), AgentControlError>>,
    control: Option<TurnExecutionControl>,
    outcomes: Arc<ControlOperationRegistry>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    config: AgentControlPlaneConfig,
) -> Result<(), AgentControlError> {
    let operation = operation_id.operation();
    let permit = match tokio::time::timeout(config.enqueue_timeout, command_tx.reserve()).await {
        Err(_) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            return Err(AgentControlError::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            });
        }
        Ok(Err(_)) => {
            outcomes.abandon_pending(&operation_id, actor_generation);
            return Err(AgentControlError::LoopUnavailable {
                operation,
                actor_generation,
            });
        }
        Ok(Ok(permit)) => permit,
    };

    // Apply the out-of-band cancellation only after admission is guaranteed.
    // A full/closed mailbox therefore cannot leave a partially cancelled Turn
    // behind an error which claims that the command was never accepted.
    outcomes.mark_enqueued(&operation_id, actor_generation);
    if let Some(control) = control {
        control.cancel_all_attempts();
    }
    permit.send(command);

    await_control_ack(
        ack_rx,
        operation_id,
        actor_generation,
        config.acknowledgement_timeout,
    )
    .await
}

async fn await_control_ack(
    ack_rx: oneshot::Receiver<Result<(), AgentControlError>>,
    operation_id: AgentControlOperationId,
    actor_generation: u64,
    acknowledgement_timeout: Duration,
) -> Result<(), AgentControlError> {
    let operation = operation_id.operation();
    match tokio::time::timeout(acknowledgement_timeout, ack_rx).await {
        Err(_) => Err(AgentControlError::AcknowledgementTimeout {
            operation,
            actor_generation,
        }),
        Ok(Err(_)) => Err(AgentControlError::AcknowledgementDropped {
            operation,
            actor_generation,
        }),
        Ok(Ok(outcome)) => outcome,
    }
}

#[derive(Clone)]
struct TurnExecutionControl {
    attempt_controls: Arc<tokio::sync::Mutex<HashMap<String, AttemptControl>>>,
    turn_cancellation_token: CancellationToken,
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
            turn_cancellation_token: CancellationToken::new(),
            command_tx,
            run_id,
        }
    }

    async fn register_attempt(&self, item_id: String) -> CancellationToken {
        let token = self.turn_cancellation_token.child_token();
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
        // Completing a tool attempt is not the recovery commit point.  The
        // surrounding provider round may still need to persist its assistant
        // envelope, subsequent results, checkpoint, or terminal outcome.  The
        // actor owns recovery success after that durable boundary.
        let _ = turn_id;
        self.attempt_controls.lock().await.remove(item_id);
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

    fn cancel_all_attempts(&self) {
        // Attempt tokens are children of the Turn token. Cancelling the
        // parent is immediate and needs no potentially contended registry lock.
        self.turn_cancellation_token.cancel();
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.turn_cancellation_token.clone()
    }
}

#[derive(Clone)]
struct ActiveTurnControl {
    turn_id: String,
    run_id: u64,
    execution: TurnExecutionControl,
    observation: ExecutionTurnObservation,
    started_at: Instant,
    first_durable_event_observed: bool,
}

/// Control-plane state is intentionally independent from the per-thread actor
/// mailbox. Cancellation and liveness observation must remain available while
/// the actor is committing data-plane events or waiting on a provider/tool.
#[derive(Clone, Default)]
struct AgentThreadControlPlane {
    active: Arc<StdRwLock<Option<ActiveTurnControl>>>,
}

impl AgentThreadControlPlane {
    fn activate(&self, turn_id: String, run_id: u64, execution: TurnExecutionControl) {
        *self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ActiveTurnControl {
            turn_id,
            run_id,
            execution,
            observation: ExecutionTurnObservation {
                status: ExecutionTurnStatus::InProgress,
                message: None,
            },
            started_at: Instant::now(),
            first_durable_event_observed: false,
        });
    }

    fn clear(&self, turn_id: &str, run_id: u64) {
        let mut active = self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active
            .as_ref()
            .is_some_and(|active| active.turn_id == turn_id && active.run_id == run_id)
        {
            *active = None;
        }
    }

    fn clear_all(&self) {
        *self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    fn execution_for(&self, turn_id: &str) -> Option<TurnExecutionControl> {
        self.active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|active| active.turn_id == turn_id)
            .map(|active| active.execution.clone())
    }

    fn active_turn_id(&self) -> Option<String> {
        self.active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|active| active.turn_id.clone())
    }

    fn observe_first_durable_event_latency(&self, turn_id: &str) -> Option<Duration> {
        let mut active = self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = active
            .as_mut()
            .filter(|active| active.turn_id == turn_id && !active.first_durable_event_observed)?;
        active.first_durable_event_observed = true;
        Some(active.started_at.elapsed())
    }

    fn set_observation(&self, turn_id: &str, run_id: u64, observation: ExecutionTurnObservation) {
        let mut active = self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = active
            .as_mut()
            .filter(|active| active.turn_id == turn_id && active.run_id == run_id)
        {
            active.observation = observation;
        }
    }

    fn observation_for(&self, turn_id: &str) -> Option<ExecutionTurnObservation> {
        self.active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|active| active.turn_id == turn_id)
            .map(|active| active.observation.clone())
    }
}

struct AgentThreadHandle {
    workspace_id: String,
    generation: u64,
    command_tx: mpsc::Sender<AgentCommand>,
    control_outcomes: Arc<ControlOperationRegistry>,
    control_plane: AgentThreadControlPlane,
    event_hub: Arc<AgentEventHub>,
    loop_handle: JoinHandle<()>,
}

#[derive(Default)]
struct AgentManagerState {
    threads: HashMap<String, AgentThreadHandle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetiredControlOperationKey {
    thread_id: String,
    operation_id: AgentControlOperationId,
}

/// Process-local reconciliation tombstones for actor generations that had to
/// be fenced. Durable Turn state remains the cross-restart authority; this
/// bounded ledger only prevents an ACK timeout from erasing the caller's
/// immediate typed reconciliation result when the live handle is removed.
#[derive(Debug)]
struct RetiredControlOperationRegistry {
    capacity: usize,
    entries: HashMap<RetiredControlOperationKey, AgentControlOperationStatus>,
    order: VecDeque<RetiredControlOperationKey>,
}

impl RetiredControlOperationRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn record(
        &mut self,
        thread_id: &str,
        snapshot: Vec<(AgentControlOperationId, AgentControlOperationStatus)>,
    ) {
        for (operation_id, status) in snapshot {
            let key = RetiredControlOperationKey {
                thread_id: thread_id.to_owned(),
                operation_id,
            };
            if self.entries.contains_key(&key) {
                self.order.retain(|candidate| candidate != &key);
            }
            while self.entries.len() >= self.capacity {
                let Some(evicted) = self.order.pop_front() else {
                    break;
                };
                self.entries.remove(&evicted);
            }
            self.order.push_back(key.clone());
            self.entries.insert(key, status);
        }
    }

    fn status(
        &self,
        thread_id: &str,
        operation_id: &AgentControlOperationId,
    ) -> Option<AgentControlOperationStatus> {
        self.entries
            .get(&RetiredControlOperationKey {
                thread_id: thread_id.to_owned(),
                operation_id: operation_id.clone(),
            })
            .cloned()
    }
}

/// Immutable dependency view used by one native Turn execution generation.
///
/// The thread actor owns only command serialization and event transport.  It
/// takes this snapshot when it accepts a new Turn, so later configuration
/// updates affect the next Turn without changing an already running one.
#[derive(Clone)]
pub(crate) struct NativeTurnRuntimeSnapshot {
    pub(crate) generation: u64,
    pub(crate) tool_loop_config: ToolLoopConfig,
    pub(crate) mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    pub(crate) turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    pub(crate) turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    pub(crate) task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    pub(crate) task_cleanup_runtime_contract: Option<String>,
    pub(crate) hook_runtime: Option<Arc<HookRuntime>>,
    pub(crate) tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    pub(crate) post_turn_hook_dispatch_policy: AgentPostTurnHookDispatchPolicy,
    pub(crate) permission_approval_broker: Arc<dyn PermissionApprovalBroker>,
}

const NATIVE_TERMINAL_RUNTIME_HISTORY_LIMIT: usize = 128;
const NATIVE_RUNTIME_GENERATION_ENTROPY_LEN: usize = 32;

fn initial_native_runtime_generation() -> u64 {
    // Runtime generations are persisted in terminal-effect rows. Starting at
    // `1` on every process allowed an old row to alias an unrelated in-memory
    // handler generation after restart. A secret-free random boot epoch keeps
    // the existing compact u64 wire/schema while making cross-process aliasing
    // cryptographically negligible. The high bit is reserved so ordinary
    // configuration increments retain ample headroom.
    let entropy = pioneer_protocol::generate_id(NATIVE_RUNTIME_GENERATION_ENTROPY_LEN);
    let mut digest = Sha256::new();
    digest.update(b"pioneer-native-runtime-generation-v1");
    digest.update([0]);
    digest.update(entropy.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(bytes) & (u64::MAX >> 1)).max(1)
}

#[derive(Clone)]
struct NativeTerminalRuntimeSnapshot {
    generation: u64,
    hook_runtime: Option<Arc<HookRuntime>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    task_cleanup_runtime_contract: Option<String>,
}

struct NativeRuntimeDependencyState {
    generation: u64,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    memory_write_provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    memory_post_turn_extractor_provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    memory_turn_policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    memory_episodic_recall_provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    post_turn_hook_dispatch_policy: AgentPostTurnHookDispatchPolicy,
    permission_approval_broker: Arc<dyn PermissionApprovalBroker>,
    terminal_runtime_history: VecDeque<NativeTerminalRuntimeSnapshot>,
}

impl NativeRuntimeDependencyState {
    fn terminal_runtime_snapshot(&self) -> NativeTerminalRuntimeSnapshot {
        NativeTerminalRuntimeSnapshot {
            generation: self.generation,
            hook_runtime: self.hook_runtime.clone(),
            task_tool_provider: self.task_tool_provider.clone(),
            task_cleanup_runtime_contract: self
                .task_tool_provider
                .as_ref()
                .map(|provider| provider.terminal_cleanup_runtime_contract().to_owned()),
        }
    }
}

pub(crate) struct NativeRuntimeDependencies {
    state: RwLock<NativeRuntimeDependencyState>,
    tool_bundle_artifacts: Arc<AgentToolBundleArtifactStore>,
}

impl NativeRuntimeDependencies {
    fn new(
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
        memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    ) -> Self {
        Self {
            state: RwLock::new(NativeRuntimeDependencyState {
                generation: initial_native_runtime_generation(),
                tool_loop_config,
                mcp_tool_provider,
                turn_tool_provider: None,
                turn_finalization_provider: None,
                task_tool_provider: None,
                memory_provider,
                memory_write_provider: None,
                memory_post_turn_extractor_provider: None,
                memory_turn_policy_provider: None,
                memory_episodic_recall_provider: None,
                hook_runtime: None,
                post_turn_hook_dispatch_policy: AgentPostTurnHookDispatchPolicy::default(),
                permission_approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
                terminal_runtime_history: VecDeque::with_capacity(
                    NATIVE_TERMINAL_RUNTIME_HISTORY_LIMIT,
                ),
            }),
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
        }
    }

    async fn update(&self, update: impl FnOnce(&mut NativeRuntimeDependencyState)) {
        let mut state = self.state.write().await;
        let previous = state.terminal_runtime_snapshot();
        if state
            .terminal_runtime_history
            .back()
            .is_none_or(|snapshot| snapshot.generation != previous.generation)
        {
            state.terminal_runtime_history.push_back(previous);
            while state.terminal_runtime_history.len() > NATIVE_TERMINAL_RUNTIME_HISTORY_LIMIT {
                state.terminal_runtime_history.pop_front();
            }
        }
        update(&mut state);
        state.generation = state.generation.saturating_add(1);
    }

    /// Update provider bindings that are only inputs to the next assembled
    /// hook runtime. These trait objects are not read from a Turn snapshot and
    /// therefore must not consume an executable runtime generation or evict a
    /// terminal-effect adapter from the bounded generation history. The
    /// corresponding `set_hook_runtime` publication remains the single atomic
    /// generation boundary for the assembled memory hook package.
    async fn update_hook_builder_input(
        &self,
        update: impl FnOnce(&mut NativeRuntimeDependencyState),
    ) {
        let mut state = self.state.write().await;
        update(&mut state);
    }

    async fn terminal_runtime_snapshot_for(
        &self,
        generation: u64,
    ) -> NativeTerminalRuntimeSnapshot {
        let state = self.state.read().await;
        if state.generation == generation {
            return state.terminal_runtime_snapshot();
        }
        state
            .terminal_runtime_history
            .iter()
            .find(|snapshot| snapshot.generation == generation)
            .cloned()
            // A process restart or bounded-history eviction cannot retain
            // executable trait objects. The durable hook plan still fences
            // subscription semantics; current handlers/providers are only
            // rebound as execution adapters at that boundary.
            .unwrap_or_else(|| state.terminal_runtime_snapshot())
    }

    pub(crate) async fn snapshot(&self) -> NativeTurnRuntimeSnapshot {
        let state = self.state.read().await;
        NativeTurnRuntimeSnapshot {
            generation: state.generation,
            tool_loop_config: state.tool_loop_config.clone(),
            mcp_tool_provider: state.mcp_tool_provider.clone(),
            turn_tool_provider: state.turn_tool_provider.clone(),
            turn_finalization_provider: state.turn_finalization_provider.clone(),
            task_tool_provider: state.task_tool_provider.clone(),
            task_cleanup_runtime_contract: state
                .task_tool_provider
                .as_ref()
                .map(|provider| provider.terminal_cleanup_runtime_contract().to_owned()),
            hook_runtime: state.hook_runtime.clone(),
            tool_bundle_artifacts: state
                .hook_runtime
                .as_ref()
                .map(|_| self.tool_bundle_artifacts.clone()),
            post_turn_hook_dispatch_policy: state.post_turn_hook_dispatch_policy,
            permission_approval_broker: state.permission_approval_broker.clone(),
        }
    }
}

pub struct AgentManager {
    state: RwLock<AgentManagerState>,
    retired_control_outcomes: StdMutex<RetiredControlOperationRegistry>,
    // Serializes stale-thread replacement.  Without this gate two concurrent
    // callers can both observe a finished actor, both spawn a replacement, and
    // the later registry write silently strands the first owner.
    thread_creation_lock: tokio::sync::Mutex<()>,
    next_thread_generation: AtomicU64,
    provider_registry: Arc<ProviderRegistry>,
    runtime_dependencies: Arc<NativeRuntimeDependencies>,
    control_plane_config: AgentControlPlaneConfig,
}

/// Bounded, identifier-free supervisor view used by process readiness.
/// Counts are authoritative for the current registry read and generations let
/// operators distinguish a repaired replacement actor from a stale handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentManagerHealthSnapshot {
    pub runtime_generation: u64,
    pub highest_actor_generation: u64,
    pub registered_actors: u64,
    pub active_turns: u64,
    pub dead_actors: u64,
    pub durable_listener_gaps: u64,
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
        Self::new_with_mcp_memory_and_control_config(
            provider_registry,
            tool_loop_config,
            mcp_tool_provider,
            memory_provider,
            AgentControlPlaneConfig::default(),
        )
    }

    pub fn new_with_mcp_memory_and_control_config(
        provider_registry: Arc<ProviderRegistry>,
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
        memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
        control_plane_config: AgentControlPlaneConfig,
    ) -> Self {
        let runtime_dependencies = Arc::new(NativeRuntimeDependencies::new(
            tool_loop_config.normalized(),
            mcp_tool_provider,
            memory_provider,
        ));
        Self {
            state: RwLock::new(AgentManagerState::default()),
            retired_control_outcomes: StdMutex::new(RetiredControlOperationRegistry::new(
                RETIRED_CONTROL_OUTCOME_CAPACITY,
            )),
            thread_creation_lock: tokio::sync::Mutex::new(()),
            next_thread_generation: AtomicU64::new(1),
            provider_registry,
            runtime_dependencies,
            control_plane_config: control_plane_config.normalized(),
        }
    }

    pub async fn set_permission_approval_broker(&self, broker: Arc<dyn PermissionApprovalBroker>) {
        self.runtime_dependencies
            .update(move |state| state.permission_approval_broker = broker)
            .await;
    }

    pub async fn set_task_tool_provider(&self, provider: Option<Arc<dyn TaskToolProvider>>) {
        self.runtime_dependencies
            .update(move |state| state.task_tool_provider = provider)
            .await;
    }

    /// Return the currently installed task-tool bridge for server-owned
    /// execution adapters.  The provider remains behind the trait boundary;
    /// callers cannot inspect or replace its internal authorization state.
    pub async fn task_tool_provider(&self) -> Option<Arc<dyn TaskToolProvider>> {
        self.runtime_dependencies
            .state
            .read()
            .await
            .task_tool_provider
            .clone()
    }

    /// Return a generation only when recovery terminalization can bind the
    /// durable attached-task cleanup adapter. The generation and provider are
    /// read under one lock so a concurrent runtime update cannot publish a
    /// mismatched cleanup plan. Missing cleanup authority is a retryable typed
    /// failure, never permission to terminalize without the obligation.
    pub async fn terminal_cleanup_runtime_binding(
        &self,
    ) -> Result<(u64, String), AgentTerminalEffectExecutionError> {
        let state = self.runtime_dependencies.state.read().await;
        state
            .task_tool_provider
            .as_ref()
            .map(|provider| {
                (
                    state.generation,
                    provider.terminal_cleanup_runtime_contract().to_owned(),
                )
            })
            .ok_or(AgentTerminalEffectExecutionError::ProviderUnavailable)
    }

    pub async fn execute_terminal_effect(
        &self,
        effect_id: &str,
        claim_token: &str,
        runtime_generation: u64,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        payload: NativeTerminalEffectPayload,
    ) -> Result<(), AgentTerminalEffectExecutionError> {
        let terminal_runtime = self
            .runtime_dependencies
            .terminal_runtime_snapshot_for(runtime_generation)
            .await;
        match payload {
            NativeTerminalEffectPayload::PostTurnHook {
                request,
                runtime_snapshot,
            } => {
                let mut request: HookPhaseRequest =
                    serde_json::from_value(request).map_err(|error| {
                        AgentTerminalEffectExecutionError::InvalidPayload(error.to_string())
                    })?;
                if request.phase != HookPhase::TurnPostTurn
                    || request.context.workspace_id.as_ref().map(|id| id.as_str())
                        != Some(workspace_id)
                    || request.context.thread_id.as_ref().map(|id| id.as_str()) != Some(thread_id)
                    || request.context.turn_id.as_ref().map(|id| id.as_str()) != Some(turn_id)
                {
                    return Err(AgentTerminalEffectExecutionError::InvalidPayload(
                        "hook request scope does not match its durable outbox row".to_owned(),
                    ));
                }
                request.context.metadata.insert(
                    HookMetadataKey::new("native_terminal_effect_id")
                        .expect("static hook metadata key is valid"),
                    HookValue::Text(effect_id.to_owned()),
                );
                request.context.metadata.insert(
                    HookMetadataKey::new("native_terminal_effect_claim_token")
                        .expect("static hook metadata key is valid"),
                    HookValue::Text(claim_token.to_owned()),
                );
                let runtime = terminal_runtime
                    .hook_runtime
                    .clone()
                    .ok_or(AgentTerminalEffectExecutionError::RuntimeUnavailable)?;
                let runtime_snapshot: DurablePostTurnHookRuntimeSnapshot =
                    serde_json::from_value(runtime_snapshot).map_err(|error| {
                        AgentTerminalEffectExecutionError::InvalidPayload(error.to_string())
                    })?;
                let subscriptions = runtime_snapshot
                    .validate_and_take_subscriptions(runtime.as_ref())
                    .map_err(AgentTerminalEffectExecutionError::InvalidPayload)?;
                let phase_result = runtime
                    .run_phase_with_snapshot_to_completion(request, subscriptions)
                    .await;
                phase_result.map_err(|error| AgentTerminalEffectExecutionError::HookFailed {
                    message: error.to_string(),
                    retryable: error.retryable(),
                })?;
                Ok(())
            }
            NativeTerminalEffectPayload::PostTurnHookPreparationFailed { failure } => {
                Err(AgentTerminalEffectExecutionError::InvalidPayload(format!(
                    "post-turn hook preparation failed: {failure:?}"
                )))
            }
            NativeTerminalEffectPayload::AttachedTaskCleanup {
                reason,
                runtime_contract,
            } => {
                let provider = terminal_runtime
                    .task_tool_provider
                    .clone()
                    .ok_or(AgentTerminalEffectExecutionError::ProviderUnavailable)?;
                let rebound_contract = terminal_runtime
                    .task_cleanup_runtime_contract
                    .as_deref()
                    .ok_or(AgentTerminalEffectExecutionError::ProviderUnavailable)?;
                if runtime_contract != rebound_contract {
                    return Err(AgentTerminalEffectExecutionError::InvalidPayload(format!(
                        "attached-task cleanup runtime contract mismatch: durable `{runtime_contract}`, rebound `{rebound_contract}`"
                    )));
                }
                provider
                    .cleanup_attached_tasks_idempotent(
                        effect_id,
                        TaskTurnContext {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                        },
                        reason,
                    )
                    .await
                    .map_err(AgentTerminalEffectExecutionError::CleanupFailed)
            }
        }
    }

    pub async fn set_turn_tool_provider(&self, provider: Option<Arc<dyn TurnToolProvider>>) {
        self.runtime_dependencies
            .update(move |state| state.turn_tool_provider = provider)
            .await;
    }

    /// Publish the two cooperating Turn-tool providers as one runtime
    /// generation. A Turn snapshot must never observe a newly installed tool
    /// materializer paired with the previous finalization policy (or vice
    /// versa).
    pub async fn set_turn_execution_providers(
        &self,
        tool_provider: Option<Arc<dyn TurnToolProvider>>,
        finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    ) {
        self.runtime_dependencies
            .update(move |state| {
                state.turn_tool_provider = tool_provider;
                state.turn_finalization_provider = finalization_provider;
            })
            .await;
    }

    pub async fn set_turn_finalization_provider(
        &self,
        provider: Option<Arc<dyn TurnFinalizationProvider>>,
    ) {
        self.runtime_dependencies
            .update(move |state| state.turn_finalization_provider = provider)
            .await;
    }

    pub async fn set_memory_provider(&self, provider: Option<Arc<dyn AgentMemoryProvider>>) {
        self.runtime_dependencies
            .update_hook_builder_input(move |state| state.memory_provider = provider)
            .await;
    }

    pub async fn set_memory_write_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    ) {
        self.runtime_dependencies
            .update_hook_builder_input(move |state| state.memory_write_provider = provider)
            .await;
    }

    pub async fn set_memory_post_turn_extractor_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    ) {
        self.runtime_dependencies
            .update_hook_builder_input(move |state| {
                state.memory_post_turn_extractor_provider = provider
            })
            .await;
    }

    pub async fn set_memory_turn_policy_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    ) {
        self.runtime_dependencies
            .update_hook_builder_input(move |state| state.memory_turn_policy_provider = provider)
            .await;
    }

    pub async fn set_memory_episodic_recall_provider(
        &self,
        provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    ) {
        self.runtime_dependencies
            .update_hook_builder_input(move |state| {
                state.memory_episodic_recall_provider = provider
            })
            .await;
    }

    pub async fn set_hook_runtime(&self, runtime: Option<Arc<HookRuntime>>) {
        self.runtime_dependencies
            .update(move |state| state.hook_runtime = runtime)
            .await;
    }

    pub fn memory_tool_bundle_artifact_store(
        &self,
    ) -> Arc<dyn pioneer_memory::hooks::MemoryToolBundleArtifactStore> {
        self.runtime_dependencies.tool_bundle_artifacts.clone()
    }

    pub async fn ensure_hook_runtime_with_current_providers(
        &self,
    ) -> Result<Option<Arc<HookRuntime>>, AgentStartError> {
        Ok(self
            .runtime_dependencies
            .state
            .read()
            .await
            .hook_runtime
            .clone())
    }

    pub async fn set_post_turn_hook_dispatch_policy(
        &self,
        policy: AgentPostTurnHookDispatchPolicy,
    ) {
        self.runtime_dependencies
            .update(move |state| state.post_turn_hook_dispatch_policy = policy)
            .await;
    }

    pub async fn has_memory_provider(&self) -> bool {
        self.runtime_dependencies
            .state
            .read()
            .await
            .memory_provider
            .is_some()
    }

    pub async fn has_hook_runtime(&self) -> bool {
        self.runtime_dependencies
            .state
            .read()
            .await
            .hook_runtime
            .is_some()
    }

    pub async fn ensure_thread(
        &self,
        thread_id: &str,
        workspace_id: &str,
    ) -> Result<(), AgentStartError> {
        let _creation_guard = self.thread_creation_lock.lock().await;
        let existing = {
            let state = self.state.read().await;
            state.threads.get(thread_id).map(|thread| {
                (
                    thread.workspace_id.clone(),
                    thread.loop_handle.is_finished(),
                )
            })
        };
        if let Some((existing_workspace_id, loop_finished)) = existing {
            if existing_workspace_id != workspace_id {
                return Err(AgentStartError::ThreadWorkspaceMismatch {
                    expected_workspace_id: existing_workspace_id,
                    actual_workspace_id: workspace_id.to_owned(),
                });
            }
            if !loop_finished {
                return Ok(());
            }
            // A failed loop must not leave a permanently poisoned handle. The
            // durable event receiver remains in its hub, and uncommitted work
            // is recovered from storage by the Gateway coordinator.
            self.remove_thread_while_creation_locked(thread_id).await;
        }

        let thread_id_owned = thread_id.to_owned();
        let workspace_id_owned = workspace_id.to_owned();

        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let event_hub = Arc::new(AgentEventHub::new());
        let control_plane = AgentThreadControlPlane::default();
        let generation = self.next_thread_generation.fetch_add(1, Ordering::Relaxed);
        let control_outcomes = Arc::new(ControlOperationRegistry::with_config(
            self.control_plane_config,
        ));

        let loop_handle = tokio::spawn(Box::pin(agent_loop::run_agent_loop(
            thread_id_owned,
            workspace_id_owned.clone(),
            self.provider_registry.clone(),
            self.runtime_dependencies.clone(),
            command_tx.clone(),
            command_rx,
            event_hub.clone(),
            control_plane.clone(),
        )));

        self.state.write().await.threads.insert(
            thread_id.to_owned(),
            AgentThreadHandle {
                workspace_id: workspace_id_owned,
                generation,
                command_tx,
                control_outcomes,
                control_plane,
                event_hub,
                loop_handle,
            },
        );

        Ok(())
    }

    async fn finish_start_dispatch<T>(
        &self,
        thread_id: &str,
        result: Result<T, AgentStartError>,
    ) -> Result<T, AgentStartError> {
        if let Some(actor_generation) = result
            .as_ref()
            .err()
            .and_then(AgentStartError::unresponsive_actor_generation)
        {
            self.fence_unresponsive_thread_generation(thread_id, actor_generation)
                .await;
        }
        result
    }

    async fn finish_control_dispatch<T>(
        &self,
        thread_id: &str,
        result: Result<T, AgentControlError>,
    ) -> Result<T, AgentControlError> {
        if let Some(actor_generation) = result
            .as_ref()
            .err()
            .and_then(AgentControlError::unresponsive_actor_generation)
        {
            self.fence_unresponsive_thread_generation(thread_id, actor_generation)
                .await;
        }
        result
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
        let (command_tx, actor_generation, outcomes) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentStartError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.generation,
                thread.control_outcomes.clone(),
            )
        };

        let operation_id = AgentControlOperationId::StartTurn {
            turn_id: turn_id.to_owned(),
        };
        if let Some(outcome) = self
            .finish_start_dispatch(
                thread_id,
                cached_start_outcome(outcomes.as_ref(), &operation_id, actor_generation),
            )
            .await?
        {
            return outcome;
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::StartTurn {
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
            ack: AgentStartAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_start_command(
            command_tx,
            command,
            ack_rx,
            outcomes,
            operation_id,
            actor_generation,
            self.control_plane_config,
        )
        .await;
        self.finish_start_dispatch(thread_id, result).await
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
        self.take_durable_receiver_with_generation(thread_id)
            .await
            .map(|(_, receiver)| receiver)
    }

    /// Leases the durable receiver together with the exact actor generation
    /// whose event hub owns it. Gateway listener registries use this atomic
    /// identity to prevent a stale listener completion from deleting or
    /// masquerading as a replacement generation.
    pub async fn take_durable_receiver_with_generation(
        &self,
        thread_id: &str,
    ) -> Option<(u64, DurableEventReceiver)> {
        let (generation, hub) = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| (thread.generation, thread.event_hub.clone()))
        }?;
        let receiver = hub.take_durable_receiver().await?;
        Some((generation, receiver))
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
        let (command_tx, actor_generation, outcomes) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.generation,
                thread.control_outcomes.clone(),
            )
        };

        let operation_id = AgentControlOperationId::CancelAttempt {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        if let Some(outcome) = self
            .finish_control_dispatch(
                thread_id,
                cached_control_outcome(outcomes.as_ref(), &operation_id, actor_generation),
            )
            .await?
        {
            return outcome;
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::CancelAttempt {
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_control_command(
            command_tx,
            command,
            ack_rx,
            outcomes,
            operation_id,
            actor_generation,
            self.control_plane_config,
        )
        .await;
        self.finish_control_dispatch(thread_id, result).await
    }

    pub async fn cancel_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> Result<(), AgentControlError> {
        let (command_tx, control_plane, actor_generation, outcomes) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.control_plane.clone(),
                thread.generation,
                thread.control_outcomes.clone(),
            )
        };

        let operation_id = AgentControlOperationId::CancelTurn {
            turn_id: turn_id.to_owned(),
        };
        if let Some(outcome) = self
            .finish_control_dispatch(
                thread_id,
                cached_control_outcome(outcomes.as_ref(), &operation_id, actor_generation),
            )
            .await?
        {
            return outcome;
        }
        let (ack_tx, ack_rx) = oneshot::channel();

        let control = control_plane.execution_for(turn_id);

        let command = AgentCommand::CancelTurn {
            turn_id: turn_id.to_owned(),
            reason: reason.to_owned(),
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_cancel_turn_command(
            command_tx,
            command,
            ack_rx,
            control,
            outcomes,
            operation_id,
            actor_generation,
            self.control_plane_config,
        )
        .await;
        self.finish_control_dispatch(thread_id, result).await
    }

    pub async fn observe_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<ExecutionTurnObservation>, AgentControlError> {
        let (command_tx, control_plane, actor_generation) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.control_plane.clone(),
                thread.generation,
            )
        };
        if let Some(observation) = control_plane.observation_for(turn_id) {
            return Ok(Some(observation));
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let operation = AgentControlOperation::ObserveTurn;
        let result = match tokio::time::timeout(
            self.control_plane_config.enqueue_timeout,
            command_tx.send(AgentCommand::ObserveTurn {
                turn_id: turn_id.to_owned(),
                ack: ack_tx,
            }),
        )
        .await
        {
            Err(_) => Err(AgentControlError::MailboxEnqueueTimeout {
                operation,
                actor_generation,
            }),
            Ok(Err(_)) => Err(AgentControlError::LoopUnavailable {
                operation,
                actor_generation,
            }),
            Ok(Ok(())) => {
                match tokio::time::timeout(
                    self.control_plane_config.acknowledgement_timeout,
                    ack_rx,
                )
                .await
                {
                    Err(_) => Err(AgentControlError::AcknowledgementTimeout {
                        operation,
                        actor_generation,
                    }),
                    Ok(Err(_)) => Err(AgentControlError::AcknowledgementDropped {
                        operation,
                        actor_generation,
                    }),
                    Ok(Ok(outcome)) => outcome,
                }
            }
        };
        self.finish_control_dispatch(thread_id, result).await
    }

    /// Returns the exact native Turn currently occupying a thread runtime.
    /// Durable callers use this only as a retry fence; it grants no authority
    /// and does not expose provider state.
    pub async fn active_turn_id(&self, thread_id: &str) -> Option<String> {
        self.state
            .read()
            .await
            .threads
            .get(thread_id)
            .and_then(|thread| thread.control_plane.active_turn_id())
    }

    /// Resolve the exact actor generation which currently owns `turn_id`.
    /// Terminal callers capture this before mutating in-memory Turn state and
    /// carry it through the durable commit as retirement authority.
    pub async fn turn_owner_generation(&self, thread_id: &str, turn_id: &str) -> Option<u64> {
        let state = self.state.read().await;
        let thread = state.threads.get(thread_id)?;
        (thread.control_plane.active_turn_id().as_deref() == Some(turn_id))
            .then_some(thread.generation)
    }

    /// Records the first committed durable event for an active Turn exactly
    /// once and returns admission-to-commit latency for bounded lifecycle metrics.
    /// The Turn identifier is only a lookup key and never leaves this control
    /// plane as a metric attribute.
    pub async fn observe_first_durable_event_latency(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Option<Duration> {
        let control_plane = self
            .state
            .read()
            .await
            .threads
            .get(thread_id)
            .map(|thread| thread.control_plane.clone())?;
        control_plane.observe_first_durable_event_latency(turn_id)
    }

    /// Read the bounded outcome registry without using the actor mailbox. A
    /// `Pending` result means a dispatch or accepted command is still inside
    /// its configured deadline. Retrying the semantic operation after an
    /// accepted command's deadline fences that exact actor generation.
    pub async fn control_operation_status(
        &self,
        thread_id: &str,
        operation_id: &AgentControlOperationId,
    ) -> Result<Option<AgentControlOperationStatus>, AgentControlError> {
        let outcomes = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.control_outcomes.clone())
        };
        if let Some(status) = outcomes
            .as_ref()
            .and_then(|outcomes| outcomes.status(operation_id))
        {
            return Ok(Some(status));
        }
        let retired = self
            .retired_control_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status(thread_id, operation_id);
        if retired.is_some() || outcomes.is_some() {
            Ok(retired)
        } else {
            Err(AgentControlError::ThreadNotFound)
        }
    }

    pub async fn thread_generation(&self, thread_id: &str) -> Option<u64> {
        self.state
            .read()
            .await
            .threads
            .get(thread_id)
            .map(|thread| thread.generation)
    }

    pub async fn health_snapshot(&self) -> AgentManagerHealthSnapshot {
        let runtime_generation = self.runtime_dependencies.state.read().await.generation;
        let state = self.state.read().await;
        let mut snapshot = AgentManagerHealthSnapshot {
            runtime_generation,
            highest_actor_generation: 0,
            registered_actors: 0,
            active_turns: 0,
            dead_actors: 0,
            durable_listener_gaps: 0,
        };
        for thread in state.threads.values() {
            snapshot.registered_actors = snapshot.registered_actors.saturating_add(1);
            snapshot.highest_actor_generation =
                snapshot.highest_actor_generation.max(thread.generation);
            if thread.loop_handle.is_finished() {
                snapshot.dead_actors = snapshot.dead_actors.saturating_add(1);
            }
            if thread.control_plane.active_turn_id().is_some() {
                snapshot.active_turns = snapshot.active_turns.saturating_add(1);
            }
            if !thread.event_hub.durable_receiver_is_claimed() {
                snapshot.durable_listener_gaps = snapshot.durable_listener_gaps.saturating_add(1);
            }
        }
        snapshot
    }

    pub async fn start_recovery_attempt(
        &self,
        thread_id: &str,
        request: RecoveryAttemptRequest,
    ) -> Result<(), AgentControlError> {
        let (command_tx, actor_generation, outcomes) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.generation,
                thread.control_outcomes.clone(),
            )
        };

        let operation_id = AgentControlOperationId::StartRecoveryAttempt {
            recovery_attempt_id: request.recovery_attempt_id.clone(),
        };
        if let Some(outcome) = self
            .finish_control_dispatch(
                thread_id,
                cached_control_outcome(outcomes.as_ref(), &operation_id, actor_generation),
            )
            .await?
        {
            return outcome;
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::StartRecoveryAttempt {
            request,
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_control_command(
            command_tx,
            command,
            ack_rx,
            outcomes,
            operation_id,
            actor_generation,
            self.control_plane_config,
        )
        .await;
        self.finish_control_dispatch(thread_id, result).await
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

        let (command_tx, actor_generation, outcomes) = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            (
                thread.command_tx.clone(),
                thread.generation,
                thread.control_outcomes.clone(),
            )
        };

        let operation_id = AgentControlOperationId::StartRestoredRecoveryTurn {
            recovery_attempt_id: recovery_request.recovery_attempt_id.clone(),
        };
        if let Some(outcome) = self
            .finish_control_dispatch(
                thread_id,
                cached_control_outcome(outcomes.as_ref(), &operation_id, actor_generation),
            )
            .await?
        {
            return outcome;
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::StartRestoredRecoveryTurn {
            turn_request,
            recovery_request,
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_control_command(
            command_tx,
            command,
            ack_rx,
            outcomes,
            operation_id,
            actor_generation,
            self.control_plane_config,
        )
        .await;
        self.finish_control_dispatch(thread_id, result).await
    }

    /// Fence exactly the actor generation which failed the bounded control
    /// protocol. The generation check is performed while holding registry
    /// ownership, so a delayed timeout from an old caller cannot remove a
    /// replacement actor. Durable Gateway recovery remains the authority for
    /// reconstructing any Turn that generation may have owned.
    async fn fence_unresponsive_thread_generation(
        &self,
        thread_id: &str,
        actor_generation: u64,
    ) -> bool {
        // Keep actor retirement and replacement mutually exclusive until the
        // fenced task has actually stopped. Merely removing its registry entry
        // before `abort()` is observed would allow a replacement generation to
        // overlap provider/tool work still unwinding in the old task.
        let _creation_guard = self.thread_creation_lock.lock().await;
        let thread = {
            let mut state = self.state.write().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return false;
            };
            if thread.generation != actor_generation {
                return false;
            }
            if let Some(turn_id) = thread.control_plane.active_turn_id()
                && let Some(control) = thread.control_plane.execution_for(turn_id.as_str())
            {
                control.cancel_all_attempts();
            }
            thread.loop_handle.abort();
            state.threads.remove(thread_id)
        };
        if let Some(thread) = thread {
            let outcomes = thread.control_outcomes.clone();
            self.retired_control_outcomes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .record(thread_id, outcomes.retirement_snapshot());
            let _ = thread.loop_handle.await;
            // The actor can finish an already-running command between abort
            // request and cancellation. Refresh the tombstones after join so
            // a persisted ACK outcome wins over the conservative
            // reconciliation-required snapshot.
            self.retired_control_outcomes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .record(thread_id, outcomes.retirement_snapshot());
            true
        } else {
            false
        }
    }

    pub async fn remove_thread(&self, thread_id: &str) {
        let _creation_guard = self.thread_creation_lock.lock().await;
        self.remove_thread_while_creation_locked(thread_id).await;
    }

    /// Remove an actor while the caller owns `thread_creation_lock`.
    ///
    /// Keeping the lock through cooperative shutdown (and the abort fallback)
    /// ensures the registry never exposes a replacement generation while the
    /// previous actor can still execute queued commands. The bounded retired
    /// snapshot preserves reconciliation evidence for any command that was
    /// accepted before teardown.
    async fn remove_thread_while_creation_locked(&self, thread_id: &str) {
        let thread = self.state.write().await.threads.remove(thread_id);
        let Some(thread) = thread else {
            return;
        };

        let outcomes = thread.control_outcomes.clone();
        self.retired_control_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(thread_id, outcomes.retirement_snapshot());

        if let Some(turn_id) = thread.control_plane.active_turn_id()
            && let Some(control) = thread.control_plane.execution_for(turn_id.as_str())
        {
            control.cancel_all_attempts();
        }
        let shutdown_accepted = matches!(
            tokio::time::timeout(
                self.control_plane_config.enqueue_timeout,
                thread.command_tx.send(AgentCommand::Shutdown),
            )
            .await,
            Ok(Ok(()))
        );
        let mut loop_handle = thread.loop_handle;
        if !shutdown_accepted
            || tokio::time::timeout(tokio::time::Duration::from_millis(2_500), &mut loop_handle)
                .await
                .is_err()
        {
            // The actor has had a cooperative cancellation window.  Fence it
            // only after that window, so tool cleanup is not dropped at the
            // same instant as the registry entry.
            loop_handle.abort();
            let _ = loop_handle.await;
        }

        // A graceful actor may have completed an already accepted command
        // while draining to Shutdown. Refresh the tombstones so that its
        // canonical ACK wins over the conservative pre-shutdown snapshot.
        self.retired_control_outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(thread_id, outcomes.retirement_snapshot());
    }

    /// Retire a thread only after its authoritative terminal transaction.
    /// Terminal side effects are already owned by the durable outbox, so the
    /// actor generation can be fenced without waiting on its mailbox.
    pub async fn retire_thread_after_terminal_commit(
        &self,
        thread_id: &str,
        terminal_turn_id: &str,
        terminal_actor_generation: u64,
    ) {
        let retirement_deadline = deadline_after(
            Instant::now(),
            self.control_plane_config
                .enqueue_timeout
                .max(self.control_plane_config.acknowledgement_timeout),
        );
        loop {
            // Retirement and replacement must remain mutually exclusive until
            // the retired actor has actually stopped. Removing the registry
            // entry before joining is not sufficient: an aborted task can
            // still be unwinding provider/tool work while a replacement is
            // admitted.
            let creation_guard = self.thread_creation_lock.lock().await;
            let (thread, wait_gate) = {
                let mut state = self.state.write().await;
                let Some(thread) = state.threads.get(thread_id) else {
                    return;
                };
                if thread.generation != terminal_actor_generation {
                    // A terminal commit from an older listener must never
                    // retire a replacement actor.
                    return;
                }
                let active_turn_id = thread.control_plane.active_turn_id();
                if active_turn_id
                    .as_deref()
                    .is_some_and(|active_turn_id| active_turn_id != terminal_turn_id)
                {
                    // A new Turn has acquired this generation while the
                    // terminal projection was delayed. It now owns retirement.
                    return;
                }

                let now = Instant::now();
                match thread.control_outcomes.retirement_gate(now) {
                    ControlOperationRetirementGate::WaitUntil(deadline)
                        if now < retirement_deadline =>
                    {
                        // Re-check after the earliest admission/ACK deadline.
                        // The creation lock is deliberately released while
                        // sleeping, so a legitimate next Turn can take over.
                        (
                            None,
                            Some((
                                deadline.min(retirement_deadline),
                                thread.control_outcomes.retirement_notify.clone(),
                            )),
                        )
                    }
                    ControlOperationRetirementGate::Ready
                    | ControlOperationRetirementGate::FenceExpiredEnqueued
                    | ControlOperationRetirementGate::WaitUntil(_) => {
                        // The terminal transaction and outbox are durable.
                        // Expired accepted operations remain visible as
                        // ReconciliationRequired in the retired registry.
                        thread.loop_handle.abort();
                        (state.threads.remove(thread_id), None)
                    }
                }
            };

            if let Some((wait_until, notify)) = wait_gate {
                drop(creation_guard);
                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wait_until)) => {}
                    _ = notify.notified() => {}
                }
                continue;
            }
            if let Some(thread) = thread {
                let outcomes = thread.control_outcomes.clone();
                self.retired_control_outcomes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .record(thread_id, outcomes.retirement_snapshot());
                let _ = thread.loop_handle.await;
                self.retired_control_outcomes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .record(thread_id, outcomes.retirement_snapshot());
            }
            return;
        }
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

    #[test]
    fn native_runtime_generations_use_distinct_process_epochs() {
        let first = initial_native_runtime_generation();
        let second = initial_native_runtime_generation();

        assert_ne!(first, 0);
        assert_ne!(second, 0);
        assert_ne!(
            first, second,
            "separately constructed process owners must not reuse persisted runtime generation 1"
        );
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
    async fn dropped_durable_listener_can_be_replaced_without_losing_the_lane() {
        let hub = AgentEventHub::with_capacity(1, 1);
        let durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");
        drop(durable_rx);

        hub.publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect("hub must retain queued events while no listener owns the receiver");

        let mut replacement = hub
            .take_durable_receiver()
            .await
            .expect("replacement listener should lease the retained receiver");
        assert!(matches!(
            replacement.recv().await,
            Some(AgentDurableEvent::TurnCompleted { turn_id, .. }) if turn_id == "turn_1"
        ));
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

    #[tokio::test]
    async fn control_protocol_bounds_full_mailbox_enqueue_and_abandons_unaccepted_operation() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        command_tx
            .try_send(AgentCommand::Shutdown)
            .expect("test mailbox should fill");
        let operation_id = AgentControlOperationId::CancelTurn {
            turn_id: "turn_control_full".to_owned(),
        };
        let outcomes = Arc::new(ControlOperationRegistry::new(4));
        let (completion_tx, _completion_rx) = mpsc::channel(1);
        let control = TurnExecutionControl::new(completion_tx, 1);
        let attempt = control
            .register_attempt("attempt-not-admitted".to_owned())
            .await;
        assert!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 7)
                .expect("fresh operation should be admitted")
                .is_none()
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::CancelTurn {
            turn_id: "turn_control_full".to_owned(),
            reason: "cancel".to_owned(),
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation: 7,
                outcomes: outcomes.clone(),
            },
        };

        let result = dispatch_cancel_turn_command(
            command_tx,
            command,
            ack_rx,
            Some(control),
            outcomes.clone(),
            operation_id.clone(),
            7,
            AgentControlPlaneConfig {
                enqueue_timeout: Duration::from_millis(5),
                acknowledgement_timeout: Duration::from_millis(5),
                outcome_capacity_per_thread: 4,
            },
        )
        .await;
        assert_eq!(
            result,
            Err(AgentControlError::MailboxEnqueueTimeout {
                operation: AgentControlOperation::CancelTurn,
                actor_generation: 7,
            })
        );
        assert_eq!(
            outcomes.status(&operation_id),
            None,
            "an operation which never entered the mailbox must be safe to retry"
        );
        assert!(
            !attempt.is_cancelled(),
            "an operation without mailbox admission must not partially cancel the Turn"
        );
    }

    #[tokio::test]
    async fn control_protocol_reconciles_applied_operation_after_caller_ack_timeout() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let operation_id = AgentControlOperationId::CancelTurn {
            turn_id: "turn_control_late_ack".to_owned(),
        };
        let outcomes = Arc::new(ControlOperationRegistry::new(4));
        assert!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 11)
                .expect("fresh operation should be admitted")
                .is_none()
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::CancelTurn {
            turn_id: "turn_control_late_ack".to_owned(),
            reason: "cancel".to_owned(),
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation: 11,
                outcomes: outcomes.clone(),
            },
        };
        let actor = tokio::spawn(async move {
            let command = command_rx.recv().await.expect("command should enqueue");
            sleep(Duration::from_millis(25)).await;
            let AgentCommand::CancelTurn { ack, .. } = command else {
                panic!("unexpected control command")
            };
            let _ = ack.send(Ok(()));
        });

        let result = dispatch_control_command(
            command_tx,
            command,
            ack_rx,
            outcomes.clone(),
            operation_id.clone(),
            11,
            AgentControlPlaneConfig {
                enqueue_timeout: Duration::from_millis(10),
                acknowledgement_timeout: Duration::from_millis(5),
                outcome_capacity_per_thread: 4,
            },
        )
        .await;
        assert_eq!(
            result,
            Err(AgentControlError::AcknowledgementTimeout {
                operation: AgentControlOperation::CancelTurn,
                actor_generation: 11,
            })
        );
        assert_eq!(
            outcomes.status(&operation_id),
            Some(AgentControlOperationStatus::Pending {
                actor_generation: 11
            })
        );
        actor.await.expect("test actor should finish");
        assert_eq!(
            outcomes.status(&operation_id),
            Some(AgentControlOperationStatus::Applied {
                actor_generation: 11
            })
        );
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 11),
            Ok(Some(Ok(()))),
            "a semantic retry must replay the canonical late outcome"
        );
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 12),
            Ok(None),
            "a replacement actor generation must never inherit a stale outcome"
        );
    }

    #[tokio::test]
    async fn control_protocol_retries_observed_rejection_after_actor_state_changes() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let operation_id = AgentControlOperationId::CancelTurn {
            turn_id: "turn_control_rejected".to_owned(),
        };
        let outcomes = Arc::new(ControlOperationRegistry::new(4));
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 19),
            Ok(None)
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = AgentCommand::CancelTurn {
            turn_id: "turn_control_rejected".to_owned(),
            reason: "cancel".to_owned(),
            ack: AgentControlAck {
                sender: ack_tx,
                operation_id: operation_id.clone(),
                actor_generation: 19,
                outcomes: outcomes.clone(),
            },
        };
        let actor = tokio::spawn(async move {
            let command = command_rx.recv().await.expect("command should enqueue");
            let AgentCommand::CancelTurn { ack, .. } = command else {
                panic!("unexpected control command")
            };
            let _ = ack.send(Err(AgentControlError::NoActiveTurn));
        });

        assert_eq!(
            dispatch_control_command(
                command_tx,
                command,
                ack_rx,
                outcomes.clone(),
                operation_id.clone(),
                19,
                AgentControlPlaneConfig {
                    enqueue_timeout: Duration::from_millis(10),
                    acknowledgement_timeout: Duration::from_millis(10),
                    outcome_capacity_per_thread: 4,
                },
            )
            .await,
            Err(AgentControlError::NoActiveTurn)
        );
        actor.await.expect("test actor should finish");
        assert!(matches!(
            outcomes.status(&operation_id),
            Some(AgentControlOperationStatus::Rejected {
                actor_generation: 19,
                failure: AgentControlOperationFailure::Control(AgentControlError::NoActiveTurn),
            })
        ));
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 19),
            Ok(None),
            "the typed rejection remains queryable until the same semantic objective is re-admitted"
        );
        outcomes.complete(&operation_id, 19, StoredControlOutcome::Control(Ok(())));
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &operation_id, 19),
            Ok(Some(Ok(()))),
            "an applied objective remains replayable for idempotent reconciliation"
        );

        let unknown_rejection = AgentControlOperationId::CancelTurn {
            turn_id: "turn_control_unknown_rejection".to_owned(),
        };
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &unknown_rejection, 19),
            Ok(None)
        );
        outcomes.complete(
            &unknown_rejection,
            19,
            StoredControlOutcome::Control(Err(AgentControlError::NoActiveTurn)),
        );
        assert!(matches!(
            outcomes.status(&unknown_rejection),
            Some(AgentControlOperationStatus::Rejected {
                actor_generation: 19,
                ..
            })
        ));
        assert_eq!(
            cached_control_outcome(outcomes.as_ref(), &unknown_rejection, 19),
            Ok(None),
            "a rejection retained after an unknown ACK outcome is queryable, then safely re-admitted"
        );
    }

    #[test]
    fn first_durable_event_latency_is_observed_once_per_active_turn() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let control_plane = AgentThreadControlPlane::default();
        control_plane.activate(
            "turn_first_event".to_owned(),
            1,
            TurnExecutionControl::new(command_tx, 1),
        );

        assert!(
            control_plane
                .observe_first_durable_event_latency("turn_first_event")
                .is_some()
        );
        assert_eq!(
            control_plane.observe_first_durable_event_latency("turn_first_event"),
            None,
            "replayed durable commits must not duplicate the first-event sample"
        );
        assert_eq!(
            control_plane.observe_first_durable_event_latency("unrelated_turn"),
            None
        );
    }

    #[test]
    fn thread_control_plane_recovers_poisoned_lock_without_losing_cancellation_access() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let control_plane = AgentThreadControlPlane::default();
        let poisoned = control_plane.active.clone();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = poisoned.write().expect("lock is initially available");
            panic!("poison control-plane lock");
        }));
        assert!(panic.is_err());

        control_plane.activate(
            "turn_after_poison".to_owned(),
            3,
            TurnExecutionControl::new(command_tx, 3),
        );
        assert!(control_plane.execution_for("turn_after_poison").is_some());
        control_plane.clear("turn_after_poison", 3);
        assert!(control_plane.active_turn_id().is_none());
    }

    #[test]
    fn control_reconciliation_type_corruption_fails_typed_without_panicking() {
        let outcomes = ControlOperationRegistry::new(1);
        let operation_id = AgentControlOperationId::StartTurn {
            turn_id: "turn_wrong_outcome_type".to_owned(),
        };
        assert!(matches!(
            outcomes.begin(operation_id.clone(), 7),
            ControlOperationAdmission::Fresh
        ));
        outcomes.complete(&operation_id, 7, StoredControlOutcome::Control(Ok(())));

        assert!(matches!(
            cached_start_outcome(&outcomes, &operation_id, 7),
            Err(AgentStartError::Internal(message))
                if message.contains("non-start outcome")
        ));
    }

    #[test]
    fn control_operation_registry_is_count_bounded_and_generation_fenced() {
        let outcomes = ControlOperationRegistry::new(2);
        for index in 1..=3 {
            let operation_id = AgentControlOperationId::CancelTurn {
                turn_id: format!("turn_bounded_{index}"),
            };
            assert!(
                cached_control_outcome(&outcomes, &operation_id, 3)
                    .expect("completed entries may be evicted for fresh work")
                    .is_none()
            );
            outcomes.complete(&operation_id, 3, StoredControlOutcome::Control(Ok(())));
        }
        assert_eq!(
            outcomes.status(&AgentControlOperationId::CancelTurn {
                turn_id: "turn_bounded_1".to_owned()
            }),
            None
        );
        assert!(matches!(
            outcomes.status(&AgentControlOperationId::CancelTurn {
                turn_id: "turn_bounded_3".to_owned()
            }),
            Some(AgentControlOperationStatus::Applied {
                actor_generation: 3
            })
        ));
    }

    #[test]
    fn retired_control_operation_registry_is_globally_bounded() {
        let mut retired = RetiredControlOperationRegistry::new(2);
        for generation in 1..=3 {
            retired.record(
                format!("thread_{generation}").as_str(),
                vec![(
                    AgentControlOperationId::CancelTurn {
                        turn_id: format!("turn_{generation}"),
                    },
                    AgentControlOperationStatus::ReconciliationRequired {
                        actor_generation: generation,
                    },
                )],
            );
        }

        assert_eq!(
            retired.status(
                "thread_1",
                &AgentControlOperationId::CancelTurn {
                    turn_id: "turn_1".to_owned(),
                },
            ),
            None,
        );
        assert!(matches!(
            retired.status(
                "thread_3",
                &AgentControlOperationId::CancelTurn {
                    turn_id: "turn_3".to_owned(),
                },
            ),
            Some(AgentControlOperationStatus::ReconciliationRequired {
                actor_generation: 3,
            })
        ));
    }

    #[test]
    fn control_operation_registry_clamps_untrusted_capacity() {
        let registry = ControlOperationRegistry::with_config(AgentControlPlaneConfig {
            enqueue_timeout: Duration::MAX,
            acknowledgement_timeout: Duration::MAX,
            outcome_capacity_per_thread: usize::MAX,
        });

        assert_eq!(registry.capacity, MAX_CONTROL_OUTCOME_CAPACITY_PER_THREAD);
        assert_eq!(registry.enqueue_timeout, MAX_CONTROL_ENQUEUE_TIMEOUT);
        assert_eq!(registry.acknowledgement_timeout, MAX_CONTROL_ACK_TIMEOUT);
    }

    #[test]
    fn control_operation_registry_preserves_pending_reconciliation_and_rejects_saturation() {
        let outcomes = ControlOperationRegistry::new(2);
        let first = AgentControlOperationId::CancelTurn {
            turn_id: "turn_pending_1".to_owned(),
        };
        let second = AgentControlOperationId::CancelTurn {
            turn_id: "turn_pending_2".to_owned(),
        };
        let third = AgentControlOperationId::CancelTurn {
            turn_id: "turn_pending_3".to_owned(),
        };

        assert_eq!(cached_control_outcome(&outcomes, &first, 41), Ok(None));
        assert_eq!(
            cached_control_outcome(&outcomes, &first, 41),
            Err(AgentControlError::OperationPending {
                operation: AgentControlOperation::CancelTurn,
                actor_generation: 41,
            }),
            "a duplicate pending request must not enqueue a second command"
        );
        assert_eq!(cached_control_outcome(&outcomes, &second, 41), Ok(None));
        assert_eq!(
            cached_control_outcome(&outcomes, &third, 41),
            Err(AgentControlError::ReconciliationCapacityExceeded {
                operation: AgentControlOperation::CancelTurn,
            }),
            "an all-pending registry fails admission instead of losing reconciliation authority"
        );
        assert!(matches!(
            outcomes.status(&first),
            Some(AgentControlOperationStatus::Pending {
                actor_generation: 41
            })
        ));
        assert!(matches!(
            outcomes.status(&second),
            Some(AgentControlOperationStatus::Pending {
                actor_generation: 41
            })
        ));
        assert_eq!(outcomes.status(&third), None);

        outcomes.complete(&first, 40, StoredControlOutcome::Control(Ok(())));
        assert!(matches!(
            outcomes.status(&first),
            Some(AgentControlOperationStatus::Pending {
                actor_generation: 41
            })
        ));
        outcomes.complete(&first, 41, StoredControlOutcome::Control(Ok(())));
        assert_eq!(cached_control_outcome(&outcomes, &third, 41), Ok(None));
        assert_eq!(
            outcomes.status(&first),
            None,
            "the oldest completed outcome may be evicted after its pending objective resolves"
        );
        assert!(matches!(
            outcomes.status(&second),
            Some(AgentControlOperationStatus::Pending {
                actor_generation: 41
            })
        ));
    }

    #[test]
    fn control_operation_registry_expires_dispatch_and_enqueued_states_safely() {
        let config = AgentControlPlaneConfig {
            enqueue_timeout: Duration::from_millis(10),
            acknowledgement_timeout: Duration::from_millis(20),
            outcome_capacity_per_thread: 1,
        };
        let base = Instant::now();
        let operation_id = AgentControlOperationId::CancelTurn {
            turn_id: "turn_abandoned_dispatch".to_owned(),
        };
        let dispatching = ControlOperationRegistry::with_config(config);
        assert!(matches!(
            dispatching.begin_at(operation_id.clone(), 7, base),
            ControlOperationAdmission::Fresh
        ));
        assert!(matches!(
            dispatching.begin_at(operation_id.clone(), 7, base + Duration::from_millis(9)),
            ControlOperationAdmission::Pending
        ));
        assert!(
            matches!(
                dispatching.begin_at(operation_id.clone(), 7, base + Duration::from_millis(10)),
                ControlOperationAdmission::Fresh
            ),
            "an expired send reservation was never accepted by the mailbox and is safe to re-admit"
        );

        let abandoned = ControlOperationRegistry::with_config(config);
        assert!(matches!(
            abandoned.begin_at(operation_id.clone(), 9, base),
            ControlOperationAdmission::Fresh
        ));
        assert_eq!(
            abandoned.retirement_gate(base + Duration::from_millis(10)),
            ControlOperationRetirementGate::Ready
        );
        assert!(
            abandoned.retirement_snapshot().is_empty(),
            "an unaccepted expired reservation must not become a reconciliation tombstone"
        );

        let enqueued = ControlOperationRegistry::with_config(config);
        assert!(matches!(
            enqueued.begin_at(operation_id.clone(), 11, base),
            ControlOperationAdmission::Fresh
        ));
        enqueued.mark_enqueued_at(&operation_id, 11, base);
        let unrelated = AgentControlOperationId::CancelAttempt {
            turn_id: "turn_registry_capacity".to_owned(),
            item_id: "item_registry_capacity".to_owned(),
        };
        assert!(
            matches!(
                enqueued.begin_at(unrelated, 11, base + Duration::from_millis(20)),
                ControlOperationAdmission::EnqueuedDeadlineExceeded {
                    operation: AgentControlOperation::CancelTurn,
                    actor_generation: 11,
                }
            ),
            "an accepted command whose ACK deadline elapsed must identify the exact actor generation instead of saturating forever"
        );
    }
}
