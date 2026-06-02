pub(crate) mod preflight;
mod provider;
mod skill_tools;
mod skills;
mod tool_recovery_policy;
mod tool_retry_lifecycle;
mod tooling;

use self::preflight::{
    TurnPreflightOrchestratorInput, TurnPreflightOrchestratorResult, TurnPreflightTurnInput,
    build_turn_preflight_diagnostics_snapshot, run_turn_preflight_orchestrator,
    trace_turn_preflight_diagnostics,
};
use self::tool_retry_lifecycle::{
    ToolRetryLifecycleTracker, emit_tool_loop_budget_exceeded, emit_tool_retry_drafts,
    turn_item_type_code,
};
use crate::hooks::{
    AgentToolBundleArtifactStore, AgentTurnHookContext, AgentTurnPostTurnHookDispatch,
    AgentTurnPostTurnSummary, EffectiveTurnPolicySet, EffectiveTurnPromptContextSet,
    EffectiveTurnPromptManifestHookContributionKind, EffectiveTurnPromptManifestHookDiagnosticCode,
    EffectiveTurnPromptManifestHookMetadata, EffectiveTurnPromptManifestHookSource,
    EffectiveTurnPromptSectionSet, run_agent_turn_policy_hook_phase,
    run_agent_turn_post_preflight_prompt_context_hook_phase,
    run_agent_turn_prompt_compile_hook_phase, run_agent_turn_prompt_context_hook_phase,
    run_agent_turn_tool_materialization_hook_phase, run_noop_agent_turn_hook_phase,
    tool_bundle_contributions_from_bundles,
};
use crate::{
    AgentEventHub, AgentEventHubError, AgentMcpAvailability, AgentMcpMaterialization,
    AgentMcpMaterializationRequest, AgentMcpServerRef, AgentMcpToolProvider, AgentMcpToolRef,
    AgentTurnHookRuntimeContext, ResolvedArtifactInput, RetainedToolLlmContext,
    ReviewRequiredTaskObservation, TaskToolMaterialization, TaskToolProvider, TaskTurnContext,
    TerminalTaskObservation, ToolLoopConfig, TurnExecutionControl, TurnFinalizationContext,
    TurnFinalizationDecision, TurnFinalizationProvider, TurnToolContext, TurnToolMaterialization,
    TurnToolProvider,
};
use chrono::Local;
use futures_util::{StreamExt, stream};
use pioneer_config::AppConfig;
use pioneer_hooks::{
    HookPhase, HookRuntime, HookToolName, TurnPostPreflightPromptContextHookInput,
    TurnPostTurnDomain, TurnPostTurnDomainEventSummary, TurnPostTurnToolErrorClass,
    TurnPostTurnToolEventSummary, TurnPostTurnToolOutcomeStatus, TurnPostTurnToolStatus,
    TurnPrePolicyHookInput, TurnPrePromptCompileHookInput, TurnPrePromptContextHookInput,
};
use pioneer_memory::hooks::{
    MemoryEpisodicRecallCapabilities, MemoryTurnContext, MemoryTurnPolicy,
    build_active_recall_local_preflight_plan, deterministic_recall_context_summary,
    memory_turn_policy_from_hook_policy_set,
};
use pioneer_promt::{
    CompiledPromptBundle, PromptCompileInput, PromptDiagnosticCode, PromptDynamicSectionId,
    PromptLimits, PromptProfile, PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId,
    PromptRuntimeSectionInput, ToolRetryInstructionKind, compile_prompt,
    render_tool_retry_instruction, runtime_sections_with_request_tools_catalog,
    tool_loop_final_answer_instruction,
};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, PromptManifest, PromptManifestDiagnostic,
    PromptManifestDiagnosticCode, PromptManifestHookContributionKind, PromptManifestHookPhase,
    PromptManifestHookSource, PromptManifestHookSourceEntry, PromptManifestHookTruncation,
    PromptManifestProfile, ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage,
    ProviderTransportKind, RecoveryAttemptContext, ThreadMode, ToolRecoveryPolicySnapshot,
    TurnAcceptedCapability, TurnCapability, TurnCapabilityAcceptedReason, TurnCapabilityKind,
    TurnCapabilityRejectedReason, TurnItem, TurnItemType, TurnRejectedCapability, UserInput,
    generate_id,
};
use pioneer_provider::{
    AttachmentDataSource, ChatMessage, ChatRequest, CompiledPromptPayload, InputContentType,
    MessageAttachment, MessageContentPart, ModelInputItem, Provider, ProviderRegistry,
    ProviderTimeoutPolicy, ProviderToolCall, ToolDefinition, infer_mime_from_reference,
};
use pioneer_skills::{
    ExcludedSkill, ResolvedSkill, SkillExcludedReason, SkillExplicitRef, SkillPolicyKey,
    SkillResolvedReason,
};
use pioneer_tools::{
    FinalToolVisibility, PreflightToolIndex, REQUEST_TOOLS_TOOL_NAME, RawToolCall,
    RequestToolsResult, ToolErrorClass, ToolLoopBudgetReason, ToolLoopGuard, ToolLoopGuardDecision,
    ToolOutcome, ToolOutcomeStatus, ToolRecoveryView, ToolResultEnvelope, ToolResultView,
    ToolRetryController, ToolRetryDecision, ToolRetryObservation, build_builtin_tools,
    build_tools_with_environment, classify_tool_error,
};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const TURN_ITEM_ID_LEN: usize = 21;
const MAX_REVIEW_REQUIRED_TASK_OBSERVATIONS: usize = 20;
const MAX_TERMINAL_TASK_OBSERVATIONS: usize = 20;
const SKILL_TOOL_BUNDLE_PRIORITY: i32 = 400;
const MCP_TOOL_BUNDLE_PRIORITY: i32 = 300;
const TURN_TOOL_BUNDLE_PRIORITY: i32 = 250;
const TASK_TOOL_BUNDLE_PRIORITY: i32 = 200;
const MAX_CONSECUTIVE_EMPTY_NO_TOOL_ROUNDS: usize = 3;
const EMPTY_NO_TOOL_ROUND_RECOVERY_INSTRUCTION: &str = concat!(
    "Your previous response was empty and was not accepted. ",
    "Continue the current turn from the existing tool results in context. ",
    "Do not restart completed work. ",
    "If work remains, call the next required tool. ",
    "If the task is complete, provide a non-empty final answer."
);

#[derive(Debug, Default, Clone)]
struct PendingToolUiState {
    tool_name: String,
    arguments: String,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    output_policy: Option<pioneer_protocol::ToolOutputPolicySnapshot>,
    latest_observation: Option<pioneer_protocol::ToolObservation>,
    started_sent: bool,
}

#[derive(Debug, Clone, Copy)]
struct TurnCapabilityResolutionInput<'a> {
    capabilities: &'a [TurnCapability],
}

#[derive(Debug, Default)]
struct TurnCapabilityResolutionOutput {
    skill_refs: Vec<SkillExplicitRef>,
    mcp_server_refs: Vec<AgentMcpServerRef>,
    mcp_tool_refs: Vec<AgentMcpToolRef>,
    rejected: Vec<TurnRejectedCapability>,
}

#[derive(Debug, Default, Clone)]
struct TurnCapabilityResolutionSummary {
    accepted: Vec<TurnAcceptedCapability>,
    rejected: Vec<TurnRejectedCapability>,
}

#[derive(Debug)]
struct AgentRoundResponse {
    text: String,
    reasoning: String,
    tool_calls: Vec<ProviderToolCall>,
}

#[derive(Debug)]
struct ExecutedToolResult {
    item_id: String,
    item_type: TurnItemType,
    attempt_number: u32,
    tool_name: String,
    arguments: String,
    model_visible_text: String,
    success: bool,
    outcome: ToolOutcome,
    recovery_view: Option<ToolRecoveryView>,
    request_tools_result: Option<RequestToolsResult>,
    message: ChatMessage,
}

#[derive(Debug, Clone)]
struct TaskMutationFailure {
    tool_name: String,
    error: String,
}

#[derive(Debug, Default)]
struct TaskMutationFinalizationGuard {
    failures_since_last_success: Vec<TaskMutationFailure>,
}

#[derive(Debug)]
struct RenderedTaskObservation {
    task_ids: Vec<String>,
    message: String,
    details: JsonValue,
}

#[derive(Debug)]
struct RenderedReviewRequiredObservation {
    signatures: Vec<String>,
    observations: Vec<ReviewRequiredTaskObservation>,
    message: String,
    details: JsonValue,
}

impl ExecutedToolResult {
    fn retry_observation(&self) -> ToolRetryObservation {
        ToolRetryObservation::from_tool_outcome(
            self.item_id.clone(),
            turn_item_type_code(self.item_type),
            self.attempt_number,
            self.tool_name.clone(),
            self.arguments.clone(),
            self.success,
            self.outcome.clone(),
        )
        .with_recovery_view(self.recovery_view.clone())
    }
}

fn extract_request_tools_result(
    tool_name: &str,
    success: bool,
    projection: Option<&ToolResultEnvelope>,
) -> Option<RequestToolsResult> {
    if tool_name != REQUEST_TOOLS_TOOL_NAME || !success {
        return None;
    }
    let value = match &projection?.llm_view {
        ToolResultView::Json { value, .. } => value.clone(),
        ToolResultView::Text { text, .. } => serde_json::from_str(text).ok()?,
        ToolResultView::Empty => return None,
    };
    serde_json::from_value(value).ok()
}

fn apply_request_tools_visibility_expansion(
    visible_tool_names: &mut Vec<String>,
    request_tools_result: &RequestToolsResult,
    router: &pioneer_tools::ToolRouter,
) -> Vec<String> {
    let mut visible = visible_tool_names.iter().cloned().collect::<BTreeSet<_>>();
    let mut added = Vec::new();

    for tool_name in request_tools_result
        .added
        .values()
        .flat_map(|names| names.iter())
    {
        if visible.contains(tool_name) || router.find_spec(tool_name.as_str()).is_none() {
            continue;
        }
        visible.insert(tool_name.clone());
        visible_tool_names.push(tool_name.clone());
        added.push(tool_name.clone());
    }

    added
}

fn apply_request_tools_results_to_visible_tools(
    visible_tool_names: &mut Vec<String>,
    executed_results: &[ExecutedToolResult],
    router: &pioneer_tools::ToolRouter,
) -> Vec<String> {
    let mut added = Vec::new();
    for result in executed_results {
        let Some(request_tools_result) = result.request_tools_result.as_ref() else {
            continue;
        };
        added.extend(apply_request_tools_visibility_expansion(
            visible_tool_names,
            request_tools_result,
            router,
        ));
    }
    added
}

fn apply_review_required_tools_to_visible_tools(
    visible_tool_names: &mut Vec<String>,
    observations: &[ReviewRequiredTaskObservation],
    router: &pioneer_tools::ToolRouter,
) -> Vec<String> {
    let mut wanted = BTreeSet::from(["task_get".to_owned(), "task_wait".to_owned()]);
    for observation in observations {
        wanted.extend(observation.allowed_actions.iter().cloned());
    }

    let mut visible = visible_tool_names.iter().cloned().collect::<BTreeSet<_>>();
    let mut added = Vec::new();
    for tool_name in wanted {
        if visible.contains(tool_name.as_str()) || router.find_spec(tool_name.as_str()).is_none() {
            continue;
        }
        visible.insert(tool_name.clone());
        visible_tool_names.push(tool_name.clone());
        added.push(tool_name);
    }
    added
}

fn sync_review_action_tools_to_observations(
    visible_tool_names: &mut Vec<String>,
    observations: &[ReviewRequiredTaskObservation],
) -> Vec<String> {
    let allowed_review_actions = observations
        .iter()
        .flat_map(|observation| observation.allowed_actions.iter().map(String::as_str))
        .filter(|tool_name| matches!(*tool_name, "task_accept" | "task_revise"))
        .collect::<BTreeSet<_>>();
    let mut removed = Vec::new();
    visible_tool_names.retain(|tool_name| {
        if matches!(tool_name.as_str(), "task_accept" | "task_revise")
            && !allowed_review_actions.contains(tool_name.as_str())
        {
            removed.push(tool_name.clone());
            false
        } else {
            true
        }
    });
    removed
}

impl TaskMutationFinalizationGuard {
    fn observe(&mut self, result: &ExecutedToolResult) {
        if !is_guarded_task_mutation_tool(result.tool_name.as_str()) {
            return;
        }
        if result.success {
            self.failures_since_last_success.clear();
            return;
        }
        self.failures_since_last_success.push(TaskMutationFailure {
            tool_name: result.tool_name.clone(),
            error: result.model_visible_text.clone(),
        });
    }

    fn deterministic_failure_message(&self) -> Option<String> {
        let failure = self.failures_since_last_success.last()?;
        Some(format!(
            "Task mutation failed and no later task mutation succeeded. Do not report the task as created, updated, or rescheduled. Failed tool: {}. Primary error: {}",
            failure.tool_name, failure.error
        ))
    }
}

fn is_guarded_task_mutation_tool(tool_name: &str) -> bool {
    matches!(tool_name, "task_create" | "task_update" | "task_reschedule")
}

fn append_text_fragment(target: &mut String, fragment: &str) {
    if fragment.is_empty() {
        return;
    }
    if !target.is_empty() {
        target.push('\n');
    }
    target.push_str(fragment);
}

fn post_turn_tool_event_summary(result: &ExecutedToolResult) -> TurnPostTurnToolEventSummary {
    TurnPostTurnToolEventSummary {
        item_id: result.item_id.clone(),
        item_type: format!("{:?}", result.item_type),
        tool_name: result.tool_name.clone(),
        attempt_number: result.attempt_number,
        status: if result.success {
            TurnPostTurnToolStatus::Succeeded
        } else {
            TurnPostTurnToolStatus::Failed
        },
        outcome_status: Some(post_turn_tool_outcome_status(result.outcome.status)),
        error_class: result.outcome.error_class.map(post_turn_tool_error_class),
    }
}

fn post_turn_domain_event_summary_from_tool(
    result: &ExecutedToolResult,
) -> TurnPostTurnDomainEventSummary {
    TurnPostTurnDomainEventSummary {
        domain: TurnPostTurnDomain::Tool,
        code: Some(if result.success {
            "tool.succeeded".to_owned()
        } else {
            "tool.failed".to_owned()
        }),
        item_id: Some(result.item_id.clone()),
        message: None,
    }
}

fn post_turn_tool_outcome_status(status: ToolOutcomeStatus) -> TurnPostTurnToolOutcomeStatus {
    match status {
        ToolOutcomeStatus::Ok => TurnPostTurnToolOutcomeStatus::Ok,
        ToolOutcomeStatus::RecoverableError => TurnPostTurnToolOutcomeStatus::RecoverableError,
        ToolOutcomeStatus::FatalError => TurnPostTurnToolOutcomeStatus::FatalError,
        ToolOutcomeStatus::PartialSuccess => TurnPostTurnToolOutcomeStatus::PartialSuccess,
    }
}

fn post_turn_tool_error_class(error_class: ToolErrorClass) -> TurnPostTurnToolErrorClass {
    match error_class {
        ToolErrorClass::InvalidArguments => TurnPostTurnToolErrorClass::InvalidArguments,
        ToolErrorClass::NotFound => TurnPostTurnToolErrorClass::NotFound,
        ToolErrorClass::ToolNotVisible => TurnPostTurnToolErrorClass::ToolNotVisible,
        ToolErrorClass::PermissionDenied => TurnPostTurnToolErrorClass::PermissionDenied,
        ToolErrorClass::CommandNotFound => TurnPostTurnToolErrorClass::CommandNotFound,
        ToolErrorClass::Timeout => TurnPostTurnToolErrorClass::Timeout,
        ToolErrorClass::Cancelled => TurnPostTurnToolErrorClass::Cancelled,
        ToolErrorClass::ExecutionFailed => TurnPostTurnToolErrorClass::ExecutionFailed,
        ToolErrorClass::NeedsNarrowing => TurnPostTurnToolErrorClass::NeedsNarrowing,
        ToolErrorClass::Internal => TurnPostTurnToolErrorClass::Internal,
        ToolErrorClass::OutputTruncated => TurnPostTurnToolErrorClass::OutputTruncated,
        ToolErrorClass::Unknown => TurnPostTurnToolErrorClass::Unknown,
    }
}

#[derive(Debug, Clone)]
pub(super) enum ChatTurnError {
    Terminal(String),
    ProviderFailure {
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
    },
    WithPostTurnDispatch {
        error: Box<ChatTurnError>,
        post_turn_dispatch: AgentTurnPostTurnHookDispatch,
    },
}

#[derive(Debug, Clone, Default)]
pub(super) struct ChatTurnOutcome {
    pub(super) post_turn_dispatch: Option<AgentTurnPostTurnHookDispatch>,
}

impl ChatTurnError {
    pub(super) fn into_parts(self) -> (Self, Option<AgentTurnPostTurnHookDispatch>) {
        match self {
            Self::WithPostTurnDispatch {
                error,
                post_turn_dispatch,
            } => (*error, Some(post_turn_dispatch)),
            other => (other, None),
        }
    }
}

fn agent_event_error(error: AgentEventHubError) -> ChatTurnError {
    ChatTurnError::Terminal(format!("failed to publish agent event: {error}"))
}

fn chat_error_post_turn_status(error: &ChatTurnError) -> pioneer_hooks::TurnPostTurnStatus {
    match error {
        ChatTurnError::ProviderFailure { .. } => pioneer_hooks::TurnPostTurnStatus::ProviderFailure,
        ChatTurnError::Terminal(_) | ChatTurnError::WithPostTurnDispatch { .. } => {
            pioneer_hooks::TurnPostTurnStatus::Failed
        }
    }
}

fn chat_error_preview(error: &ChatTurnError) -> String {
    match error {
        ChatTurnError::Terminal(message) => message.clone(),
        ChatTurnError::ProviderFailure { failure, .. } => {
            failure.message.clone().unwrap_or_else(|| {
                format!(
                    "provider failure: {:?} during {:?}",
                    failure.class, failure.stage
                )
            })
        }
        ChatTurnError::WithPostTurnDispatch { error, .. } => chat_error_preview(error),
    }
}

fn with_post_turn_failure_dispatch(
    error: ChatTurnError,
    hook_context: AgentTurnHookContext,
    effective_policy_set: EffectiveTurnPolicySet,
    effective_prompt_context_set: EffectiveTurnPromptContextSet,
    user_text: String,
    assistant_text: String,
    tool_events: Vec<TurnPostTurnToolEventSummary>,
    domain_events: Vec<TurnPostTurnDomainEventSummary>,
) -> ChatTurnError {
    let status = chat_error_post_turn_status(&error);
    let error_preview = chat_error_preview(&error);
    let summary = AgentTurnPostTurnSummary::failed_with_events(
        status,
        user_text,
        assistant_text,
        error_preview,
        tool_events,
        domain_events,
    );
    ChatTurnError::WithPostTurnDispatch {
        error: Box::new(error),
        post_turn_dispatch: AgentTurnPostTurnHookDispatch::new(
            hook_context,
            effective_policy_set,
            effective_prompt_context_set,
            summary,
        ),
    }
}

async fn emit_durable_event(
    event_tx: &AgentEventHub,
    event: AgentDurableEvent,
) -> Result<(), ChatTurnError> {
    event_tx
        .publish_durable(event)
        .await
        .map_err(agent_event_error)
}

async fn emit_progress_event(
    event_tx: &AgentEventHub,
    event: AgentProgressEvent,
) -> Result<(), ChatTurnError> {
    event_tx.publish_progress(event);
    Ok(())
}

fn protocol_skill_audit_event(
    event: pioneer_skills::SkillAuditEvent,
) -> pioneer_protocol::SkillAuditEvent {
    pioneer_protocol::SkillAuditEvent {
        skill_slug: event.skill_slug,
        source_kind: event.source_kind,
        action: event.action.as_db_value().to_owned(),
        decision: event.decision.as_db_value().to_owned(),
        reason_code: event.reason_code,
        details: event.details,
        created_at_unix: event.created_at_unix,
    }
}

fn normalize_optional_prompt(content: Option<String>) -> Option<String> {
    content.and_then(|content| {
        let trimmed = content.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn normalize_skill_capability_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn normalize_mcp_capability_token(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn rejected_capability(
    capability: &TurnCapability,
    reason: TurnCapabilityRejectedReason,
    message: impl Into<String>,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: capability.id.clone(),
        label: capability.label.clone(),
        kind: capability.kind.clone(),
        reason,
        message: message.into(),
    }
}

fn resolve_turn_capability_input(
    input: TurnCapabilityResolutionInput<'_>,
) -> TurnCapabilityResolutionOutput {
    let mut normalized = TurnCapabilityResolutionOutput::default();
    let mut seen = HashSet::<String>::new();

    for capability in input.capabilities {
        if capability.id.trim().is_empty() {
            normalized.rejected.push(rejected_capability(
                capability,
                TurnCapabilityRejectedReason::InvalidInput,
                "Capability is missing an id.",
            ));
            continue;
        }

        let canonical_key = match &capability.kind {
            TurnCapabilityKind::Skill { slug, source_kind } => {
                let slug = slug.trim();
                let source_kind = source_kind.trim();
                if slug.is_empty() || source_kind.is_empty() {
                    normalized.rejected.push(rejected_capability(
                        capability,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "Skill capability is missing a slug or source kind.",
                    ));
                    continue;
                }
                format!(
                    "skill:{}:{}",
                    source_kind.to_ascii_lowercase(),
                    normalize_skill_capability_token(slug)
                )
            }
            TurnCapabilityKind::McpServer { name, scope_kind } => {
                let name = name.trim();
                if name.is_empty() {
                    normalized.rejected.push(rejected_capability(
                        capability,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP server capability is missing a server name.",
                    ));
                    continue;
                }
                format!(
                    "mcp_server:{}:{}",
                    scope_kind.as_str(),
                    normalize_mcp_capability_token(name)
                )
            }
            TurnCapabilityKind::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => {
                let server_name = server_name.trim();
                let raw_tool_name = raw_tool_name.trim();
                if server_name.is_empty() || raw_tool_name.is_empty() {
                    normalized.rejected.push(rejected_capability(
                        capability,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP tool capability is missing a server name or tool name.",
                    ));
                    continue;
                }
                format!(
                    "mcp_tool:{}:{}:{}",
                    scope_kind.as_str(),
                    normalize_mcp_capability_token(server_name),
                    normalize_mcp_capability_token(raw_tool_name)
                )
            }
        };

        if !seen.insert(canonical_key) {
            normalized.rejected.push(rejected_capability(
                capability,
                TurnCapabilityRejectedReason::Duplicate,
                "Capability was selected more than once.",
            ));
            continue;
        }

        match &capability.kind {
            TurnCapabilityKind::Skill { slug, source_kind } => {
                normalized.skill_refs.push(SkillExplicitRef {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    slug: slug.trim().to_owned(),
                    source_kind: source_kind.trim().to_owned(),
                });
            }
            TurnCapabilityKind::McpServer { name, scope_kind } => {
                normalized.mcp_server_refs.push(AgentMcpServerRef {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    name: name.trim().to_owned(),
                    scope_kind: *scope_kind,
                });
            }
            TurnCapabilityKind::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => {
                normalized.mcp_tool_refs.push(AgentMcpToolRef {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    server_name: server_name.trim().to_owned(),
                    raw_tool_name: raw_tool_name.trim().to_owned(),
                    scope_kind: *scope_kind,
                });
            }
        }
    }

    normalized
}

fn normalize_turn_capabilities(capabilities: &[TurnCapability]) -> TurnCapabilityResolutionOutput {
    resolve_turn_capability_input(TurnCapabilityResolutionInput { capabilities })
}

fn skill_capability_kind(reference: &SkillExplicitRef) -> TurnCapabilityKind {
    TurnCapabilityKind::Skill {
        slug: reference.slug.clone(),
        source_kind: reference.source_kind.clone(),
    }
}

fn accepted_skill_capability(reference: &SkillExplicitRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: skill_capability_kind(reference),
        reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
    }
}

fn rejected_skill_capability(
    reference: &SkillExplicitRef,
    reason: TurnCapabilityRejectedReason,
    message: impl Into<String>,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: skill_capability_kind(reference),
        reason,
        message: message.into(),
    }
}

fn skill_ref_matches_resolved_skill(reference: &SkillExplicitRef, skill: &ResolvedSkill) -> bool {
    if !reference.source_kind.trim().is_empty()
        && reference.source_kind.as_str() != skill.definition.identity.source_kind.as_db_value()
    {
        return false;
    }

    let normalized_ref = normalize_skill_capability_token(reference.slug.as_str());
    if normalized_ref.is_empty() {
        return false;
    }

    [
        skill.slug.as_str(),
        skill.definition.identity.slug.as_str(),
        skill.definition.identity.name.as_str(),
        skill.definition.identity.display_name.as_str(),
    ]
    .into_iter()
    .any(|candidate| normalized_ref == normalize_skill_capability_token(candidate))
}

fn skill_ref_matches_excluded_skill(reference: &SkillExplicitRef, skill: &ExcludedSkill) -> bool {
    if !reference.source_kind.trim().is_empty() && reference.source_kind != skill.source_kind {
        return false;
    }

    let normalized_ref = normalize_skill_capability_token(reference.slug.as_str());
    let normalized_slug = normalize_skill_capability_token(skill.slug.as_str());
    normalized_ref == normalized_slug
        || skill
            .slug
            .rsplit('/')
            .next()
            .is_some_and(|slug| normalized_ref == normalize_skill_capability_token(slug))
}

fn skill_rejection_reason(reason: &SkillExcludedReason) -> TurnCapabilityRejectedReason {
    match reason {
        SkillExcludedReason::DisabledByPolicy => TurnCapabilityRejectedReason::DisabledByPolicy,
        SkillExcludedReason::DependencyMissing => TurnCapabilityRejectedReason::DependencyMissing,
        SkillExcludedReason::ValidationRejected | SkillExcludedReason::InvalidMetadata => {
            TurnCapabilityRejectedReason::ValidationRejected
        }
        SkillExcludedReason::TrustBlocked | SkillExcludedReason::SecurityBlocked => {
            TurnCapabilityRejectedReason::SecurityBlocked
        }
        SkillExcludedReason::DisabledModelInvocation => TurnCapabilityRejectedReason::Unavailable,
        SkillExcludedReason::NotMatched => TurnCapabilityRejectedReason::NotFound,
    }
}

fn skill_rejection_message(reference: &SkillExplicitRef, reason: &SkillExcludedReason) -> String {
    let label = reference
        .label
        .as_deref()
        .filter(|label| !label.trim().is_empty())
        .unwrap_or(reference.slug.as_str());
    match reason {
        SkillExcludedReason::DisabledByPolicy => {
            format!("Skill `{label}` is disabled by workspace policy.")
        }
        SkillExcludedReason::DependencyMissing => {
            format!("Skill `{label}` is missing required dependencies.")
        }
        SkillExcludedReason::ValidationRejected
        | SkillExcludedReason::InvalidMetadata
        | SkillExcludedReason::TrustBlocked
        | SkillExcludedReason::SecurityBlocked => {
            format!("Skill `{label}` did not pass validation.")
        }
        SkillExcludedReason::DisabledModelInvocation => {
            format!("Skill `{label}` is not available for model invocation.")
        }
        SkillExcludedReason::NotMatched => format!("Skill `{label}` is not available."),
    }
}

fn resolve_skill_capability_summary(
    explicit_refs: &[SkillExplicitRef],
    resolution: &skills::TurnSkillResolution,
) -> TurnCapabilityResolutionSummary {
    let mut summary = TurnCapabilityResolutionSummary::default();

    for reference in explicit_refs {
        if resolution.result.active.iter().any(|skill| {
            matches!(skill.reason, SkillResolvedReason::ExplicitCapability)
                && skill_ref_matches_resolved_skill(reference, skill)
        }) {
            summary.accepted.push(accepted_skill_capability(reference));
            continue;
        }

        if let Some(excluded) = resolution
            .result
            .excluded
            .iter()
            .find(|skill| skill_ref_matches_excluded_skill(reference, skill))
        {
            summary.rejected.push(rejected_skill_capability(
                reference,
                skill_rejection_reason(&excluded.reason),
                skill_rejection_message(reference, &excluded.reason),
            ));
            continue;
        }

        let label = reference
            .label
            .as_deref()
            .filter(|label| !label.trim().is_empty())
            .unwrap_or(reference.slug.as_str());
        summary.rejected.push(rejected_skill_capability(
            reference,
            TurnCapabilityRejectedReason::NotFound,
            format!("Skill `{label}` is not installed or not available in this workspace."),
        ));
    }

    summary
}

fn unsupported_mcp_capability_summary(
    server_refs: &[AgentMcpServerRef],
    tool_refs: &[AgentMcpToolRef],
) -> TurnCapabilityResolutionSummary {
    let mut summary = TurnCapabilityResolutionSummary::default();
    for reference in server_refs {
        summary.rejected.push(TurnRejectedCapability {
            id: reference.capability_id.clone(),
            label: reference.label.clone(),
            kind: TurnCapabilityKind::McpServer {
                name: reference.name.clone(),
                scope_kind: reference.scope_kind,
            },
            reason: TurnCapabilityRejectedReason::ProviderUnsupported,
            message: "MCP capabilities require a tool-calling model.".to_owned(),
        });
    }
    for reference in tool_refs {
        summary.rejected.push(TurnRejectedCapability {
            id: reference.capability_id.clone(),
            label: reference.label.clone(),
            kind: TurnCapabilityKind::McpTool {
                server_name: reference.server_name.clone(),
                raw_tool_name: reference.raw_tool_name.clone(),
                scope_kind: reference.scope_kind,
            },
            reason: TurnCapabilityRejectedReason::ProviderUnsupported,
            message: "MCP capabilities require a tool-calling model.".to_owned(),
        });
    }
    summary
}

fn capability_display_label(rejected: &TurnRejectedCapability) -> String {
    if let Some(label) = rejected.label.as_deref()
        && !label.trim().is_empty()
    {
        return label.to_owned();
    }

    match &rejected.kind {
        TurnCapabilityKind::Skill { slug, .. } => slug.clone(),
        TurnCapabilityKind::McpServer { name, .. } => name.clone(),
        TurnCapabilityKind::McpTool {
            server_name,
            raw_tool_name,
            ..
        } => format!("{server_name}/{raw_tool_name}"),
    }
}

fn capability_rejection_warning_message(rejected: &[TurnRejectedCapability]) -> String {
    match rejected {
        [] => String::new(),
        [single] => format!(
            "Capability `{}` was not attached: {}",
            capability_display_label(single),
            single.message
        ),
        many => {
            let details = many
                .iter()
                .take(3)
                .map(|item| format!("{}: {}", capability_display_label(item), item.message))
                .collect::<Vec<_>>()
                .join("; ");
            if many.len() > 3 {
                format!(
                    "{} selected capabilities were not attached: {}; and {} more.",
                    many.len(),
                    details,
                    many.len().saturating_sub(3)
                )
            } else {
                format!(
                    "{} selected capabilities were not attached: {}.",
                    many.len(),
                    details
                )
            }
        }
    }
}

fn capability_manifest_diagnostics(
    rejected: &[TurnRejectedCapability],
) -> Vec<PromptManifestDiagnostic> {
    rejected
        .iter()
        .map(|capability| PromptManifestDiagnostic {
            code: PromptManifestDiagnosticCode::CapabilityRejected,
            message: format!(
                "Capability `{}` was rejected: {}",
                capability_display_label(capability),
                capability.message
            ),
            file: None,
            section_id: None,
            hook_source: None,
        })
        .collect()
}

async fn emit_capability_resolution_events(
    event_tx: &AgentEventHub,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    accepted: &[TurnAcceptedCapability],
    rejected: &[TurnRejectedCapability],
    mcp_bindings: &[pioneer_protocol::McpTurnBindingSummary],
) -> Result<(), ChatTurnError> {
    if accepted.is_empty() && rejected.is_empty() && mcp_bindings.is_empty() {
        return Ok(());
    }

    emit_durable_event(
        event_tx,
        AgentDurableEvent::TurnCapabilitiesResolved {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            accepted: accepted.to_vec(),
            rejected: rejected.to_vec(),
            mcp_bindings: mcp_bindings.to_vec(),
        },
    )
    .await?;

    if !rejected.is_empty() {
        emit_durable_event(
            event_tx,
            AgentDurableEvent::ItemCompleted {
                notification: ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::SystemEvent {
                        id: generate_id(TURN_ITEM_ID_LEN),
                        level: pioneer_protocol::SystemEventLevel::Warning,
                        message: capability_rejection_warning_message(rejected),
                        code: Some("capability.rejected".to_owned()),
                        details: Some(json!({ "rejected": rejected })),
                    },
                },
            },
        )
        .await?;
    }

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct PromptSectionsForCompile {
    runtime_sections: Vec<PromptRuntimeSectionInput>,
}

fn prompt_sections_for_compile_from_hook_sections(
    section_set: &EffectiveTurnPromptSectionSet,
) -> Result<PromptSectionsForCompile, ChatTurnError> {
    let mut compiled_sections = PromptSectionsForCompile {
        runtime_sections: Vec::new(),
    };

    if section_set.is_empty() {
        return Ok(compiled_sections);
    }
    let sections = section_set.clone_hook_prompt_section_set();

    for entry in sections.entries() {
        compiled_sections
            .runtime_sections
            .push(PromptRuntimeSectionInput {
                id: prompt_runtime_section_id_from_hook_section(entry.section_id.as_str())?,
                title: entry.title.as_ref().map(|title| title.as_str().to_owned()),
                content: entry.content.as_str().to_owned(),
                max_chars: None,
                truncated: entry.truncated,
            });
    }

    Ok(compiled_sections)
}

fn prompt_runtime_section_id_from_hook_section(
    section_id: &str,
) -> Result<PromptRuntimeSectionId, ChatTurnError> {
    if let Some(id) = PromptRuntimeBuiltInSectionId::from_manifest_id(section_id) {
        return Ok(PromptRuntimeSectionId::BuiltIn(id));
    }
    let id = PromptDynamicSectionId::new(section_id).map_err(|error| {
        ChatTurnError::Terminal(format!(
            "failed to convert hook prompt section `{section_id}`: {error}"
        ))
    })?;
    Ok(PromptRuntimeSectionId::Dynamic(id))
}

fn hook_tool_names_from_strings(names: &[String]) -> Vec<HookToolName> {
    let mut names = names
        .iter()
        .filter_map(|name| HookToolName::new(name.clone()).ok())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

#[allow(clippy::too_many_arguments)]
async fn run_agent_turn_preflight_stage(
    provider_registry: Arc<ProviderRegistry>,
    provider: Arc<dyn Provider>,
    model: &str,
    provider_tool_calling: bool,
    tool_loop_config: &ToolLoopConfig,
    effective_policy_set: &EffectiveTurnPolicySet,
    effective_prompt_context_set: &EffectiveTurnPromptContextSet,
    tool_index: PreflightToolIndex,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    hook_runtime_context: &AgentTurnHookRuntimeContext,
    input_text: &str,
) -> TurnPreflightOrchestratorResult {
    let memory_config = tool_loop_config.memory.active_recall.normalized();
    let hook_policy_set = effective_policy_set.clone_hook_policy_set();
    let memory_policy = match memory_turn_policy_from_hook_policy_set(&hook_policy_set) {
        Some(Ok(policy)) => policy,
        Some(Err(error)) => {
            warn!(
                thread_id,
                turn_id,
                error = %error,
                "preflight memory policy decode failed; using memory no-use policy"
            );
            MemoryTurnPolicy::no_use()
        }
        None => MemoryTurnPolicy::no_use(),
    };
    let hook_prompt_context_set = effective_prompt_context_set.clone_hook_prompt_context_set();
    let deterministic_summary =
        deterministic_recall_context_summary(&hook_prompt_context_set, &memory_config);
    let prompt_context_input = TurnPrePromptContextHookInput::from_parts(
        input_text,
        Some(model.to_owned()),
        Some(provider.name().to_owned()),
    );
    let memory_context = MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input_text.to_owned(),
        task_id: hook_runtime_context.task_id.clone(),
        agent_id: hook_runtime_context.agent_id.clone(),
    };
    let active_recall = build_active_recall_local_preflight_plan(
        &memory_context,
        &prompt_context_input,
        &memory_policy,
        &memory_config,
        &deterministic_summary,
        MemoryEpisodicRecallCapabilities::default(),
        true,
    );
    let input_text_char_count = input_text.chars().count();
    let input_text_preview = active_recall.decision_context.input_text_preview.clone();
    let turn = TurnPreflightTurnInput {
        has_workspace_id: !workspace_id.trim().is_empty(),
        has_thread_id: !thread_id.trim().is_empty(),
        has_turn_id: !turn_id.trim().is_empty(),
        thread_mode: ThreadMode::Agent,
        provider_tool_calling,
        input_text_preview,
        input_text_char_count,
    };

    run_turn_preflight_orchestrator(TurnPreflightOrchestratorInput {
        provider_registry,
        workspace_id: workspace_id.to_owned(),
        thread_provider: provider,
        thread_provider_name: prompt_context_input
            .model_provider
            .clone()
            .unwrap_or_default(),
        thread_model: model.to_owned(),
        preflight_provider_name: tool_loop_config.preflight.provider_name.clone(),
        preflight_model: tool_loop_config.preflight.model.clone(),
        turn,
        tool_index,
        deterministic_summary,
        active_recall,
        timeout_ms: tool_loop_config.preflight.timeout_ms,
        max_output_chars: tool_loop_config.preflight.max_output_chars,
    })
    .await
}

fn trace_agent_turn_preflight(
    thread_id: &str,
    turn_id: &str,
    preflight: &TurnPreflightOrchestratorResult,
    final_visible_tools: &[String],
) {
    let snapshot = build_turn_preflight_diagnostics_snapshot(
        &preflight.local_modules,
        &preflight.plan,
        final_visible_tools,
    );
    trace_turn_preflight_diagnostics(thread_id, turn_id, &snapshot);
}

fn compute_agent_turn_final_visible_tools(
    router: &pioneer_tools::ToolRouter,
    preflight: &TurnPreflightOrchestratorResult,
    current_visible_tools: &[String],
) -> FinalToolVisibility {
    router.compute_final_visible_tools(
        &preflight.local_modules.tools.input.core_tools,
        &preflight.plan.tools.visible_tools,
        current_visible_tools,
    )
}

fn warn_final_visible_tool_diagnostics(
    thread_id: &str,
    turn_id: &str,
    diagnostics: &[pioneer_tools::ToolVisibilityDiagnostic],
) {
    for diagnostic in diagnostics {
        warn!(
            thread_id,
            turn_id,
            tool_name = diagnostic.tool_name.as_str(),
            source = diagnostic.source.as_str(),
            reason = diagnostic.reason.as_str(),
            code = ?diagnostic.code,
            "turn final visible tool was clamped"
        );
    }
}

fn preflight_active_recall_prompt_context_input(
    input_text: &str,
    model: &str,
    provider_name: &str,
    preflight: &TurnPreflightOrchestratorResult,
) -> Option<TurnPostPreflightPromptContextHookInput> {
    let plan = serde_json::to_value(&preflight.plan.memory.active_recall.decision).ok()?;
    Some(
        TurnPostPreflightPromptContextHookInput::from_parts(
            input_text.to_owned(),
            Some(model.to_owned()),
            Some(provider_name.to_owned()),
        )
        .with_active_memory_recall_preflight_plan(plan),
    )
}

fn compile_agent_prompt_bundle(
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    runtime_sections: &[PromptRuntimeSectionInput],
    include_task_orchestration_policy: bool,
    include_request_tools_catalog: bool,
    continue_generation_hint: bool,
    thread_id: &str,
    turn_id: &str,
) -> Result<CompiledPromptBundle, ChatTurnError> {
    let prompt_root = AppConfig::load()
        .map_err(|error| ChatTurnError::Terminal(format!("failed to load app config: {error}")))?
        .runtime_home_dir()
        .map_err(|error| {
            ChatTurnError::Terminal(format!("failed to resolve runtime home: {error:#}"))
        })?;

    compile_agent_prompt_bundle_with_prompt_root(
        prompt_root.as_path(),
        skills_prompt,
        retry_instruction,
        runtime_sections,
        include_task_orchestration_policy,
        include_request_tools_catalog,
        continue_generation_hint,
        thread_id,
        turn_id,
    )
}

fn compile_agent_prompt_bundle_with_prompt_root(
    prompt_root: &std::path::Path,
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    runtime_sections: &[PromptRuntimeSectionInput],
    include_task_orchestration_policy: bool,
    include_request_tools_catalog: bool,
    continue_generation_hint: bool,
    thread_id: &str,
    turn_id: &str,
) -> Result<CompiledPromptBundle, ChatTurnError> {
    let now = Local::now();

    let extra_system = format!(
        "## Runtime\nCurrent date/time: {} ({})\nOS: {}",
        now.format("%Y-%m-%d %H:%M:%S"),
        now.format("%Z"),
        std::env::consts::OS,
    );

    let runtime_sections = runtime_sections_with_request_tools_catalog(
        runtime_sections,
        include_request_tools_catalog,
    );

    let bundle = compile_prompt(PromptCompileInput {
        workspace_root: prompt_root.to_path_buf(),
        profile: PromptProfile::AssistantFull,
        skills_prompt,
        retry_instruction,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy,
        continue_generation_hint,
        runtime_sections,
        dynamic_sections: Vec::new(),
        dynamic_context: None,
        extra_system: Some(extra_system),
        limits: PromptLimits::default(),
    })
    .map_err(|error| ChatTurnError::Terminal(format!("failed to compile prompt: {error:#}")))?;

    if !bundle.diagnostics.is_empty() {
        warn!(
            thread_id,
            turn_id,
            diagnostics = bundle.diagnostics.len(),
            "prompt compiler emitted diagnostics"
        );
    }

    Ok(bundle)
}

fn compiled_prompt_payload_from_bundle(bundle: &CompiledPromptBundle) -> CompiledPromptPayload {
    CompiledPromptPayload {
        stable_system_text: bundle.stable_system_text.clone(),
        dynamic_system_text: bundle.dynamic_system_text.clone(),
        boundary_marker: bundle.boundary_marker.to_owned(),
        full_system_text: bundle.full_system_text.clone(),
    }
}

fn append_recovered_tool_llm_context(
    messages: &mut Vec<ChatMessage>,
    mut retained_context: Vec<RetainedToolLlmContext>,
) {
    retained_context.sort_by_key(|context| context.sequence);
    for context in retained_context {
        messages.push(ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: context.item_id.clone(),
                name: context.tool_name.clone(),
                arguments: context.arguments.clone(),
            }],
        ));

        if let Some(message) = recovered_tool_result_message(&context) {
            messages.push(message);
        }
    }
}

fn recovered_tool_result_message(context: &RetainedToolLlmContext) -> Option<ChatMessage> {
    let view =
        serde_json::from_value::<pioneer_tools::ToolResultView>(context.payload.clone()).ok()?;
    let (content, payload) = match view {
        pioneer_tools::ToolResultView::Text { text, truncated } => (
            text.clone(),
            serde_json::json!({
                "output": text,
                "truncated": truncated,
                "recovered_from_turn_llm_context": true,
            }),
        ),
        pioneer_tools::ToolResultView::Json {
            mut value,
            truncated,
        } => {
            if !value.is_object() {
                value = serde_json::json!({ "value": value });
            }
            if let Some(map) = value.as_object_mut() {
                map.entry("truncated".to_owned())
                    .or_insert(JsonValue::Bool(truncated));
                map.insert(
                    "recovered_from_turn_llm_context".to_owned(),
                    JsonValue::Bool(true),
                );
            }
            let content =
                serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            (content, value)
        }
        pioneer_tools::ToolResultView::Empty => (
            String::new(),
            serde_json::json!({
                "recovered_from_turn_llm_context": true
            }),
        ),
    };

    Some(
        ModelInputItem::tool_result(
            context.item_id.clone(),
            context.tool_name.clone(),
            content,
            Some(payload),
        )
        .into_chat_message(),
    )
}

fn prompt_manifest_profile(profile: PromptProfile) -> PromptManifestProfile {
    match profile {
        PromptProfile::AssistantFull => PromptManifestProfile::AssistantFull,
        PromptProfile::AssistantMinimal => PromptManifestProfile::AssistantMinimal,
        PromptProfile::AssistantNone => PromptManifestProfile::AssistantNone,
    }
}

fn prompt_diagnostic_code(code: PromptDiagnosticCode) -> PromptManifestDiagnosticCode {
    match code {
        PromptDiagnosticCode::MissingFile => PromptManifestDiagnosticCode::MissingFile,
        PromptDiagnosticCode::FileReadError => PromptManifestDiagnosticCode::FileReadError,
        PromptDiagnosticCode::FileTruncated => PromptManifestDiagnosticCode::FileTruncated,
        PromptDiagnosticCode::TotalBudgetTruncated => {
            PromptManifestDiagnosticCode::TotalBudgetTruncated
        }
        PromptDiagnosticCode::FileFilteredByProfile => {
            PromptManifestDiagnosticCode::FileFilteredByProfile
        }
        PromptDiagnosticCode::DynamicSectionTruncated => {
            PromptManifestDiagnosticCode::DynamicSectionTruncated
        }
        PromptDiagnosticCode::DynamicSectionOmitted => {
            PromptManifestDiagnosticCode::DynamicSectionOmitted
        }
    }
}

fn prompt_manifest_from_bundle(
    bundle: &CompiledPromptBundle,
    hook_metadata: &EffectiveTurnPromptManifestHookMetadata,
    capability_diagnostics: &[PromptManifestDiagnostic],
) -> PromptManifest {
    let mut diagnostics = bundle
        .diagnostics
        .iter()
        .map(|diagnostic| PromptManifestDiagnostic {
            code: prompt_diagnostic_code(diagnostic.code),
            message: diagnostic.message.clone(),
            file: diagnostic.file.clone(),
            section_id: diagnostic.section_id.clone(),
            hook_source: None,
        })
        .collect::<Vec<_>>();
    diagnostics.extend(prompt_manifest_hook_diagnostics(hook_metadata));
    diagnostics.extend(capability_diagnostics.iter().cloned());

    PromptManifest {
        compiler_version: bundle.compiler_version.to_owned(),
        profile: prompt_manifest_profile(bundle.profile),
        section_ids: bundle
            .sections
            .iter()
            .map(|section| section.id.manifest_id())
            .collect::<Vec<_>>(),
        fingerprint_stable: bundle.fingerprint_stable.clone(),
        fingerprint_dynamic: bundle.fingerprint_dynamic.clone(),
        fingerprint_full: bundle.fingerprint_full.clone(),
        diagnostics,
        hook_sources: prompt_manifest_hook_sources(bundle, hook_metadata),
    }
}

fn prompt_manifest_section_content_chars(bundle: &CompiledPromptBundle) -> BTreeMap<String, usize> {
    bundle
        .sections
        .iter()
        .map(|section| (section.id.manifest_id(), section.content.chars().count()))
        .collect()
}

fn prompt_manifest_hook_sources(
    bundle: &CompiledPromptBundle,
    hook_metadata: &EffectiveTurnPromptManifestHookMetadata,
) -> Vec<PromptManifestHookSourceEntry> {
    let section_content_chars = prompt_manifest_section_content_chars(bundle);

    hook_metadata
        .sources
        .iter()
        .filter_map(|entry| {
            let mut source = prompt_manifest_hook_source(&entry.source)?;
            source.contribution_id = entry
                .contribution_id
                .as_ref()
                .map(|contribution_id| contribution_id.as_str().to_owned());
            let section_id = entry
                .section_id
                .as_ref()
                .map(|section_id| section_id.as_str().to_owned());
            let prompt_truncated = prompt_manifest_source_prompt_truncated(
                section_id.as_deref(),
                entry.hook_content_chars,
                &section_content_chars,
            );
            Some(PromptManifestHookSourceEntry {
                source,
                section_id,
                contribution_kind: prompt_manifest_hook_contribution_kind(entry.contribution_kind),
                priority: entry.priority,
                source_count: entry.source_count,
                truncation: prompt_manifest_hook_truncation(entry.hook_truncated, prompt_truncated),
            })
        })
        .collect()
}

fn prompt_manifest_hook_diagnostics(
    hook_metadata: &EffectiveTurnPromptManifestHookMetadata,
) -> Vec<PromptManifestDiagnostic> {
    hook_metadata
        .diagnostics
        .iter()
        .map(|diagnostic| PromptManifestDiagnostic {
            code: match diagnostic.code {
                EffectiveTurnPromptManifestHookDiagnosticCode::HookDiagnostic => {
                    PromptManifestDiagnosticCode::HookDiagnostic
                }
                EffectiveTurnPromptManifestHookDiagnosticCode::HookBestEffortFailed => {
                    PromptManifestDiagnosticCode::HookBestEffortFailed
                }
            },
            message: diagnostic.message.clone(),
            file: None,
            section_id: None,
            hook_source: diagnostic
                .source
                .as_ref()
                .and_then(prompt_manifest_hook_source),
        })
        .collect()
}

fn prompt_manifest_hook_source(
    source: &EffectiveTurnPromptManifestHookSource,
) -> Option<PromptManifestHookSource> {
    Some(PromptManifestHookSource {
        hook_id: source.hook_id.as_str().to_owned(),
        subscription_id: source.subscription_id.as_str().to_owned(),
        phase: prompt_manifest_hook_phase(source.phase)?,
        contribution_id: None,
        contribution_hash: source
            .contribution_hash
            .as_ref()
            .map(|hash| hash.as_str().to_owned()),
    })
}

fn prompt_manifest_hook_phase(phase: HookPhase) -> Option<PromptManifestHookPhase> {
    match phase {
        HookPhase::TurnPrePromptContext => Some(PromptManifestHookPhase::TurnPrePromptContext),
        HookPhase::TurnPostPreflightPromptContext => {
            Some(PromptManifestHookPhase::TurnPostPreflightPromptContext)
        }
        HookPhase::TurnPrePromptCompile => Some(PromptManifestHookPhase::TurnPrePromptCompile),
        HookPhase::TurnPrePolicy
        | HookPhase::TurnPreToolMaterialization
        | HookPhase::TurnPostPromptCompile
        | HookPhase::TurnPostTurn
        | HookPhase::TurnPreCompaction => None,
    }
}

fn prompt_manifest_hook_contribution_kind(
    kind: EffectiveTurnPromptManifestHookContributionKind,
) -> PromptManifestHookContributionKind {
    match kind {
        EffectiveTurnPromptManifestHookContributionKind::PromptContext => {
            PromptManifestHookContributionKind::PromptContext
        }
        EffectiveTurnPromptManifestHookContributionKind::PromptSection => {
            PromptManifestHookContributionKind::PromptSection
        }
    }
}

fn prompt_manifest_source_prompt_truncated(
    section_id: Option<&str>,
    hook_content_chars: Option<usize>,
    section_content_chars: &BTreeMap<String, usize>,
) -> bool {
    let Some(section_id) = section_id else {
        return false;
    };
    match (hook_content_chars, section_content_chars.get(section_id)) {
        (Some(hook_chars), Some(compiled_chars)) => *compiled_chars < hook_chars,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

fn prompt_manifest_hook_truncation(
    hook_truncated: bool,
    prompt_truncated: bool,
) -> PromptManifestHookTruncation {
    match (hook_truncated, prompt_truncated) {
        (false, false) => PromptManifestHookTruncation::None,
        (true, false) => PromptManifestHookTruncation::Hook,
        (false, true) => PromptManifestHookTruncation::Prompt,
        (true, true) => PromptManifestHookTruncation::HookAndPrompt,
    }
}

async fn send_reasoning_completed(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    reasoning: &str,
    event_tx: &AgentEventHub,
) -> Result<(), ChatTurnError> {
    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::Reasoning {
                    id: thinking_item_id.to_owned(),
                    summary: Vec::new(),
                    content: if reasoning.is_empty() {
                        Vec::new()
                    } else {
                        vec![reasoning.to_owned()]
                    },
                },
            },
        },
    )
    .await
}

async fn start_reasoning_item(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    event_tx: &AgentEventHub,
) -> Result<String, ChatTurnError> {
    let thinking_item_id = generate_id(TURN_ITEM_ID_LEN);
    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::Reasoning {
                    id: thinking_item_id.clone(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
    )
    .await?;
    Ok(thinking_item_id)
}

pub(super) async fn execute_chat_turn_flow(
    thread_id: String,
    turn_id: String,
    workspace_id: String,
    mode: ThreadMode,
    provider_registry: Arc<ProviderRegistry>,
    provider: Arc<dyn Provider>,
    model: String,
    hook_runtime_context: AgentTurnHookRuntimeContext,
    workspace_skill_policies: HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    input: Vec<UserInput>,
    capabilities: Vec<TurnCapability>,
    resolved_artifacts: Vec<ResolvedArtifactInput>,
    runtime_environment: HashMap<String, String>,
    history: Vec<ChatMessage>,
    retained_llm_context: Vec<RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_tx: Arc<AgentEventHub>,
) -> Result<ChatTurnOutcome, ChatTurnError> {
    let user_message = build_user_message(input.as_slice(), resolved_artifacts.as_slice());

    let thinking_item_id = generate_id(TURN_ITEM_ID_LEN);
    let message_item_id = generate_id(TURN_ITEM_ID_LEN);

    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item: TurnItem::Reasoning {
                    id: thinking_item_id.clone(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
    )
    .await?;

    let result = match mode {
        ThreadMode::Agent => execute_agent_provider_response(
            provider_registry,
            &provider,
            model,
            hook_runtime_context,
            history,
            user_message.clone(),
            &input,
            &capabilities,
            &workspace_skill_policies,
            runtime_environment,
            retained_llm_context,
            force_non_stream,
            continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            turn_tool_provider,
            turn_finalization_provider,
            task_tool_provider,
            hook_runtime,
            tool_bundle_artifacts,
            turn_control.clone(),
            recovery,
            &workspace_id,
            &thread_id,
            &turn_id,
            thinking_item_id,
            &message_item_id,
            event_tx.clone(),
        )
        .await
        .map(|post_turn_dispatch| ChatTurnOutcome {
            post_turn_dispatch: Some(post_turn_dispatch),
        }),
        ThreadMode::Chat => {
            let result = execute_standard_provider_response(
                &provider,
                model,
                history,
                user_message,
                None,
                true,
                force_non_stream,
                &workspace_id,
                &thread_id,
                &turn_id,
                &thinking_item_id,
                &message_item_id,
                tool_loop_config.provider,
                event_tx.clone(),
            )
            .await;

            if result.is_ok() {
                turn_control
                    .succeed_recovery_attempt(turn_id.as_str(), recovery)
                    .await;
            }

            if result.is_err() {
                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::ItemCompleted {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.clone(),
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            item: TurnItem::Reasoning {
                                id: thinking_item_id,
                                summary: Vec::new(),
                                content: Vec::new(),
                            },
                        },
                    },
                )
                .await?;
            }

            result.map(|_| ChatTurnOutcome::default())
        }
    };

    match result {
        Ok(outcome) => Ok(outcome),
        Err(e) => Err(e),
    }
}

async fn execute_standard_provider_response(
    provider: &Arc<dyn Provider>,
    model: String,
    history: Vec<ChatMessage>,
    user_message: ChatMessage,
    compiled_prompt: Option<CompiledPromptPayload>,
    apply_chat_attachment_policy: bool,
    force_non_stream: bool,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: Arc<AgentEventHub>,
) -> Result<String, ChatTurnError> {
    let mut messages = history;

    messages.push(user_message);

    if apply_chat_attachment_policy {
        retain_chat_mode_attachment_messages(&mut messages);
    }

    let request = ChatRequest {
        model,
        messages,
        temperature: Some(0.7),
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt,
    };

    if provider.capabilities().streaming && !force_non_stream {
        provider::stream_provider_response(
            provider,
            request,
            workspace_id,
            thread_id,
            turn_id,
            thinking_item_id,
            message_item_id,
            provider_timeout_policy,
            event_tx.as_ref(),
        )
        .await
    } else {
        provider::non_stream_provider_response(
            provider,
            request,
            workspace_id,
            thread_id,
            turn_id,
            thinking_item_id,
            message_item_id,
            event_tx.as_ref(),
        )
        .await
    }
}

async fn load_mcp_availability(
    provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> AgentMcpAvailability {
    let Some(provider) = provider else {
        return AgentMcpAvailability::default();
    };
    match provider.mcp_availability(workspace_id).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to load MCP availability for skill dependencies"
            );
            AgentMcpAvailability::default()
        }
    }
}

async fn materialize_mcp_tooling(
    provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    workspace_id: &str,
    turn_id: &str,
    thread_id: &str,
    explicit_servers: &[AgentMcpServerRef],
    explicit_tools: &[AgentMcpToolRef],
) -> AgentMcpMaterialization {
    let Some(provider) = provider else {
        return AgentMcpMaterialization::default();
    };
    match provider
        .materialize_mcp_tools(AgentMcpMaterializationRequest {
            workspace_id: workspace_id.to_owned(),
            turn_id: turn_id.to_owned(),
            explicit_servers: explicit_servers.to_vec(),
            explicit_tools: explicit_tools.to_vec(),
        })
        .await
    {
        Ok(materialization) => materialization,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to materialize MCP dynamic tools"
            );
            AgentMcpMaterialization::default()
        }
    }
}

async fn materialize_task_tooling(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> TaskToolMaterialization {
    let Some(provider) = provider else {
        return TaskToolMaterialization::default();
    };
    match provider
        .materialize_task_tools(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
    {
        Ok(materialization) => materialization,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to materialize task orchestration tools"
            );
            TaskToolMaterialization::default()
        }
    }
}

async fn materialize_turn_tooling(
    provider: Option<&Arc<dyn TurnToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> TurnToolMaterialization {
    let Some(provider) = provider else {
        return TurnToolMaterialization::default();
    };
    match provider
        .materialize_turn_tools(TurnToolContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
    {
        Ok(materialization) => materialization,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to materialize turn tools"
            );
            TurnToolMaterialization::default()
        }
    }
}

async fn review_required_attached_task_observation(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Option<RenderedReviewRequiredObservation> {
    let provider = provider?;
    let observations = match provider
        .review_required_attached_task_observations(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
    {
        Ok(observations) => observations,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to query review-required attached tasks before parent turn completion"
            );
            return None;
        }
    };

    let mut observations = observations
        .into_iter()
        .take(MAX_REVIEW_REQUIRED_TASK_OBSERVATIONS)
        .collect::<Vec<_>>();
    if observations.is_empty() {
        return None;
    }
    observations.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.run_id.cmp(&right.run_id))
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });

    let signatures = observations
        .iter()
        .map(review_required_observation_signature)
        .collect::<Vec<_>>();
    let payload = observations
        .iter()
        .map(review_required_observation_payload)
        .collect::<Vec<_>>();
    let payload_text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "[]".to_owned());
    let message = format!(
        "Attached task result candidates require review before this turn can finish:\n{payload_text}\nFor each candidate, call one of its allowedActions (`task_accept`, `task_revise`, or `task_cancel`). Do not provide the final answer until every review-required candidate is accepted, revised, cancelled, or otherwise no longer waiting for review."
    );
    let details = json!({
        "taskIds": observations.iter().map(|observation| observation.task_id.clone()).collect::<Vec<_>>(),
        "observations": payload,
    });

    Some(RenderedReviewRequiredObservation {
        signatures,
        observations,
        message,
        details,
    })
}

fn review_required_observation_signature(observation: &ReviewRequiredTaskObservation) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        observation.task_id,
        observation.run_id,
        observation.candidate_id,
        observation.candidate_status,
        observation.round,
        observation.remaining_revision_rounds,
        observation.allowed_actions.join(","),
        observation.revision_blocked_reason.as_deref().unwrap_or("")
    )
}

fn review_required_observation_payload(observation: &ReviewRequiredTaskObservation) -> JsonValue {
    json!({
        "taskId": observation.task_id,
        "runId": observation.run_id,
        "candidateId": observation.candidate_id,
        "title": observation.title,
        "status": observation.status,
        "candidateStatus": observation.candidate_status,
        "round": observation.round,
        "summary": observation.summary.as_deref().map(|summary| bounded_task_text(summary, 800)),
        "resultPreview": observation.result_preview.as_deref().map(|preview| bounded_task_text(preview, 1200)),
        "extractionErrorPreview": observation.extraction_error_preview.as_deref().map(|preview| bounded_task_text(preview, 800)),
        "diagnostics": observation
            .diagnostics
            .iter()
            .map(|diagnostic| bounded_task_text(diagnostic, 400))
            .collect::<Vec<_>>(),
        "childThreadId": observation.child_thread_id,
        "childTurnId": observation.child_turn_id,
        "maxRevisionRounds": observation.max_revision_rounds,
        "remainingRevisionRounds": observation.remaining_revision_rounds,
        "allowedActions": observation.allowed_actions,
        "revisionBlockedReason": observation.revision_blocked_reason,
    })
}

fn review_required_final_answer_block_message() -> String {
    "Attached task result review is still required. Call task_accept, task_revise, or task_cancel for each pending review candidate before providing the final answer.".to_owned()
}

async fn pending_attached_task_observation(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Option<String> {
    let provider = provider?;
    let pending = match provider
        .pending_attached_tasks(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
    {
        Ok(pending) => pending,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to query pending attached tasks before parent turn completion"
            );
            return None;
        }
    };
    if pending.is_empty() {
        return None;
    }

    let task_lines = pending
        .iter()
        .map(|task| {
            let run = task
                .run_id
                .as_ref()
                .map(|run_id| format!(", run {run_id}"))
                .unwrap_or_default();
            format!(
                "- {} ({}, status {}{})",
                task.title, task.task_id, task.status, run
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Some(format!(
        "Attached tasks created by this turn are still active, so the turn cannot finish yet.\n{task_lines}\nCall task_wait for active runs, or task_cancel/task_detach when abandoning or backgrounding the work before giving the final answer."
    ))
}

async fn terminal_attached_task_observation(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    observed_task_ids: &BTreeSet<String>,
) -> Option<RenderedTaskObservation> {
    let provider = provider?;
    let observations = match provider
        .terminal_attached_task_observations(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
    {
        Ok(observations) => observations,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = error.as_str(),
                "failed to query terminal attached task observations"
            );
            return None;
        }
    };

    let mut observations = observations
        .into_iter()
        .filter(|observation| !observed_task_ids.contains(observation.task_id.as_str()))
        .take(MAX_TERMINAL_TASK_OBSERVATIONS)
        .collect::<Vec<_>>();
    if observations.is_empty() {
        return None;
    }
    observations.sort_by(|left, right| left.task_id.cmp(&right.task_id));

    let task_ids = observations
        .iter()
        .map(|observation| observation.task_id.clone())
        .collect::<Vec<_>>();
    let payload = observations
        .iter()
        .map(task_observation_payload)
        .collect::<Vec<_>>();
    let payload_text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "[]".to_owned());
    let message = format!(
        "Attached task results are available:\n{payload_text}\nUse these TaskRun results/errors in the next response. Full details are available with task_get."
    );

    Some(RenderedTaskObservation {
        task_ids,
        message,
        details: json!({
            "taskIds": observations.iter().map(|observation| observation.task_id.clone()).collect::<Vec<_>>(),
            "observations": payload,
        }),
    })
}

fn task_observation_payload(observation: &TerminalTaskObservation) -> JsonValue {
    json!({
        "taskId": observation.task_id,
        "runId": observation.run_id,
        "title": observation.title,
        "status": observation.status,
        "summary": observation.summary.as_deref().map(|summary| bounded_task_text(summary, 800)),
        "error": observation.error_message.as_deref().map(|error| bounded_task_text(error, 800)),
        "childThreadId": observation.child_thread_id,
        "childTurnId": observation.child_turn_id,
    })
}

fn bounded_task_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn record_observed_terminal_task_ids(
    observed_task_ids: &mut BTreeSet<String>,
    result: &ExecutedToolResult,
) {
    let Ok(value) = serde_json::from_str::<JsonValue>(result.model_visible_text.as_str()) else {
        return;
    };
    match result.tool_name.as_str() {
        "task_wait" => {
            for key in ["completed", "failed", "cancelled"] {
                if let Some(items) = value.get(key).and_then(JsonValue::as_array) {
                    for item in items {
                        if let Some(task_id) = item.get("taskId").and_then(JsonValue::as_str) {
                            observed_task_ids.insert(task_id.to_owned());
                        }
                    }
                }
            }
        }
        "task_get" => {
            if let Some(task) = value.get("task")
                && let Some(status) = task.get("status").and_then(JsonValue::as_str)
                && terminal_task_status_label(status)
                && let Some(task_id) = task.get("id").and_then(JsonValue::as_str)
            {
                observed_task_ids.insert(task_id.to_owned());
            }
        }
        "task_cancel" => {
            if let Some(task_id) = value
                .get("task")
                .and_then(|task| task.get("taskId"))
                .and_then(JsonValue::as_str)
            {
                observed_task_ids.insert(task_id.to_owned());
            }
        }
        "task_accept" => {
            if value
                .get("taskTerminal")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
                && let Some(task_id) = value.get("taskId").and_then(JsonValue::as_str)
            {
                observed_task_ids.insert(task_id.to_owned());
            }
        }
        _ => {}
    }
}

fn terminal_task_status_label(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

async fn execute_agent_provider_response(
    provider_registry: Arc<ProviderRegistry>,
    provider: &Arc<dyn Provider>,
    model: String,
    hook_runtime_context: AgentTurnHookRuntimeContext,
    history: Vec<ChatMessage>,
    user_message: ChatMessage,
    input: &[UserInput],
    capabilities: &[TurnCapability],
    workspace_skill_policies: &HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    runtime_environment: HashMap<String, String>,
    retained_llm_context: Vec<RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    turn_control: TurnExecutionControl,
    mut recovery: Option<RecoveryAttemptContext>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    initial_thinking_item_id: String,
    message_item_id: &str,
    event_tx: Arc<AgentEventHub>,
) -> Result<AgentTurnPostTurnHookDispatch, ChatTurnError> {
    let workdir = std::env::current_dir()
        .map_err(|error| ChatTurnError::Terminal(format!("failed to resolve cwd: {error}")))?;

    let tool_loop_config = tool_loop_config.normalized();
    let provider_tool_calling = provider.capabilities().tool_calling;
    let post_turn_model = model.clone();
    let post_turn_model_provider = provider.name().to_owned();
    let hook_context = AgentTurnHookContext::with_runtime_context(
        workspace_id,
        thread_id,
        turn_id,
        hook_runtime_context.clone(),
    );
    let user_input_text = user_message.text_content_lossy();

    let effective_policy_set = run_agent_turn_policy_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        TurnPrePolicyHookInput::from_parts(
            user_input_text.clone(),
            Some(model.clone()),
            Some(provider.name().to_owned()),
        ),
    )
    .await
    .map_err(|error| {
        warn!(
            thread_id,
            turn_id,
            error_kind = error.kind(),
            "turn policy hook failed before prompt construction"
        );
        ChatTurnError::Terminal(error.safe_message().to_owned())
    })?;

    let mut effective_prompt_context_set = run_agent_turn_prompt_context_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        &effective_policy_set,
        TurnPrePromptContextHookInput::from_parts(
            user_input_text.clone(),
            Some(model.clone()),
            Some(provider.name().to_owned()),
        ),
    )
    .await;

    let mcp_availability =
        load_mcp_availability(mcp_tool_provider.as_ref(), workspace_id, thread_id, turn_id).await;

    let normalized_capabilities = normalize_turn_capabilities(capabilities);

    let skills_resolution = match skills::resolve_turn_skills_with_explicit_refs(
        workdir.as_path(),
        workspace_id,
        input,
        normalized_capabilities.skill_refs.as_slice(),
        &tool_loop_config.skills,
        workspace_skill_policies,
        &mcp_availability,
    ) {
        Ok(resolution) => resolution,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to resolve turn skills"
            );
            skills::TurnSkillResolution {
                prompt: String::new(),
                result: pioneer_skills::SkillResolutionResult {
                    active: Vec::new(),
                    excluded: Vec::new(),
                },
                runtime_plan: pioneer_skills::SkillRuntimePlan {
                    tools: Vec::new(),
                    read_skill_index: HashMap::new(),
                    excluded_tools: Vec::new(),
                },
                audit_events: Vec::new(),
            }
        }
    };

    let skill_capability_summary = resolve_skill_capability_summary(
        normalized_capabilities.skill_refs.as_slice(),
        &skills_resolution,
    );

    let bindings = skills::to_turn_skill_bindings(skills_resolution.result.active.as_slice());

    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::TurnSkillsResolved {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            bindings,
        },
    )
    .await?;

    if !skills_resolution.audit_events.is_empty() {
        emit_durable_event(
            event_tx.as_ref(),
            AgentDurableEvent::SkillAuditEvents {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                events: skills_resolution
                    .audit_events
                    .iter()
                    .cloned()
                    .map(protocol_skill_audit_event)
                    .collect(),
            },
        )
        .await?;
    }

    let skills_prompt = normalize_optional_prompt(Some(skills_resolution.prompt.clone()));

    let task_materialization = if provider_tool_calling {
        materialize_task_tooling(
            task_tool_provider.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
        )
        .await
    } else {
        TaskToolMaterialization::default()
    };
    for diagnostic in &task_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "task tool materialization reported diagnostic"
        );
    }
    let include_task_orchestration_policy = !task_materialization.bundles.is_empty();

    let turn_tool_materialization = if provider_tool_calling {
        materialize_turn_tooling(
            turn_tool_provider.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
        )
        .await
    } else {
        TurnToolMaterialization::default()
    };
    for diagnostic in &turn_tool_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "turn tool materialization reported diagnostic"
        );
    }

    if !provider_tool_calling {
        let unsupported_mcp_summary = unsupported_mcp_capability_summary(
            normalized_capabilities.mcp_server_refs.as_slice(),
            normalized_capabilities.mcp_tool_refs.as_slice(),
        );
        let accepted_capabilities = skill_capability_summary.accepted.clone();
        let mut rejected_capabilities = normalized_capabilities.rejected.clone();
        rejected_capabilities.extend(skill_capability_summary.rejected.clone());
        rejected_capabilities.extend(unsupported_mcp_summary.rejected);
        emit_capability_resolution_events(
            event_tx.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            accepted_capabilities.as_slice(),
            rejected_capabilities.as_slice(),
            &[],
        )
        .await?;
        let capability_diagnostics =
            capability_manifest_diagnostics(rejected_capabilities.as_slice());

        let _effective_tool_bundle_set = run_agent_turn_tool_materialization_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
            Vec::new(),
            tool_bundle_artifacts.as_ref(),
            provider_tool_calling,
        )
        .await
        .map_err(|error| {
            warn!(
                thread_id,
                turn_id,
                error_kind = error.kind(),
                "turn tool materialization hook failed before prompt construction"
            );
            ChatTurnError::Terminal("turn tool materialization hook failed".to_owned())
        })?;

        let turn_preflight = run_agent_turn_preflight_stage(
            provider_registry.clone(),
            provider.clone(),
            model.as_str(),
            provider_tool_calling,
            &tool_loop_config,
            &effective_policy_set,
            &effective_prompt_context_set,
            PreflightToolIndex {
                core_tools: Vec::new(),
                candidate_tools: Vec::new(),
            },
            workspace_id,
            thread_id,
            turn_id,
            &hook_runtime_context,
            user_input_text.as_str(),
        )
        .await;

        trace_agent_turn_preflight(thread_id, turn_id, &turn_preflight, &[]);

        if let Some(active_recall_input) = preflight_active_recall_prompt_context_input(
            user_input_text.as_str(),
            model.as_str(),
            provider.name(),
            &turn_preflight,
        ) {
            effective_prompt_context_set = run_agent_turn_post_preflight_prompt_context_hook_phase(
                hook_runtime.as_ref(),
                &hook_context,
                &effective_policy_set,
                &effective_prompt_context_set,
                active_recall_input,
            )
            .await;
        }

        let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
            Vec::new(),
            TurnPrePromptCompileHookInput::from_parts(false, Vec::new()),
        )
        .await
        .map_err(|error| {
            warn!(
                thread_id,
                turn_id,
                error_kind = error.kind(),
                "turn prompt section hook failed before prompt construction"
            );
            ChatTurnError::Terminal("turn prompt section hook failed".to_owned())
        })?;
        let prompt_sections =
            prompt_sections_for_compile_from_hook_sections(&effective_prompt_section_set)?;

        let initial_prompt_bundle = compile_agent_prompt_bundle(
            skills_prompt.clone(),
            None,
            prompt_sections.runtime_sections.as_slice(),
            include_task_orchestration_policy,
            false,
            continue_generation_hint,
            thread_id,
            turn_id,
        )?;

        run_noop_agent_turn_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            HookPhase::TurnPostPromptCompile,
            &effective_policy_set,
            &effective_prompt_context_set,
        )
        .await;

        emit_durable_event(
            event_tx.as_ref(),
            AgentDurableEvent::PromptManifestCompiled {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                manifest: prompt_manifest_from_bundle(
                    &initial_prompt_bundle,
                    &EffectiveTurnPromptManifestHookMetadata::combined(
                        effective_prompt_context_set.manifest_metadata(),
                        effective_prompt_section_set.manifest_metadata(),
                    ),
                    capability_diagnostics.as_slice(),
                ),
            },
        )
        .await?;

        let result = execute_standard_provider_response(
            provider,
            model,
            history,
            user_message.clone(),
            Some(compiled_prompt_payload_from_bundle(&initial_prompt_bundle)),
            false,
            force_non_stream,
            workspace_id,
            thread_id,
            turn_id,
            initial_thinking_item_id.as_str(),
            message_item_id,
            tool_loop_config.provider,
            event_tx.clone(),
        )
        .await;

        match result {
            Ok(assistant_text) => {
                turn_control
                    .succeed_recovery_attempt(turn_id, recovery.take())
                    .await;
                let summary = AgentTurnPostTurnSummary::succeeded_with_model(
                    Some(post_turn_model.clone()),
                    Some(post_turn_model_provider.clone()),
                    extract_user_text(input),
                    assistant_text,
                    Vec::new(),
                    Vec::new(),
                );
                return Ok(AgentTurnPostTurnHookDispatch::new(
                    hook_context,
                    effective_policy_set,
                    effective_prompt_context_set,
                    summary,
                ));
            }
            Err(error) => {
                return Err(with_post_turn_failure_dispatch(
                    error,
                    hook_context,
                    effective_policy_set,
                    effective_prompt_context_set,
                    extract_user_text(input),
                    String::new(),
                    Vec::new(),
                    Vec::new(),
                ));
            }
        }
    }

    let mcp_materialization = materialize_mcp_tooling(
        mcp_tool_provider.as_ref(),
        workspace_id,
        turn_id,
        thread_id,
        normalized_capabilities.mcp_server_refs.as_slice(),
        normalized_capabilities.mcp_tool_refs.as_slice(),
    )
    .await;
    for diagnostic in &mcp_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "MCP dynamic tool materialization reported diagnostic"
        );
    }
    let mut accepted_capabilities = skill_capability_summary.accepted.clone();
    accepted_capabilities.extend(mcp_materialization.accepted_capabilities.clone());
    let mut rejected_capabilities = normalized_capabilities.rejected.clone();
    rejected_capabilities.extend(skill_capability_summary.rejected.clone());
    rejected_capabilities.extend(mcp_materialization.rejected_capabilities.clone());
    emit_capability_resolution_events(
        event_tx.as_ref(),
        workspace_id,
        thread_id,
        turn_id,
        accepted_capabilities.as_slice(),
        rejected_capabilities.as_slice(),
        mcp_materialization.mcp_bindings.as_slice(),
    )
    .await?;
    let capability_diagnostics = capability_manifest_diagnostics(rejected_capabilities.as_slice());

    let skill_tool_materialization = skill_tools::materialize_skill_tooling(
        &skills_resolution.runtime_plan,
        &tool_loop_config.skills,
    );

    for excluded in &skill_tool_materialization.excluded_tools {
        warn!(
            thread_id,
            turn_id,
            tool_name = excluded.canonical_tool_name.as_str(),
            reason = excluded.reason.as_str(),
            "excluded invalid skill runtime tool; continuing turn"
        );
    }
    for diagnostic in &skill_tool_materialization.policy_diagnostics {
        warn!(
            thread_id,
            turn_id,
            tool_name = diagnostic.canonical_tool_name.as_str(),
            diagnostics = ?diagnostic.diagnostics,
            "dynamic skill tool output policy was narrowed"
        );
    }

    let mut tool_bundle_contributions = Vec::new();
    tool_bundle_contributions.extend(tool_bundle_contributions_from_bundles(
        "skill",
        "skill.runtime",
        SKILL_TOOL_BUNDLE_PRIORITY,
        skill_tool_materialization.bundles.clone(),
    ));
    tool_bundle_contributions.extend(tool_bundle_contributions_from_bundles(
        "mcp",
        "mcp.runtime",
        MCP_TOOL_BUNDLE_PRIORITY,
        mcp_materialization.bundles.clone(),
    ));
    tool_bundle_contributions.extend(tool_bundle_contributions_from_bundles(
        "turn",
        "turn.runtime",
        TURN_TOOL_BUNDLE_PRIORITY,
        turn_tool_materialization.bundles.clone(),
    ));
    tool_bundle_contributions.extend(tool_bundle_contributions_from_bundles(
        "task",
        "task.runtime",
        TASK_TOOL_BUNDLE_PRIORITY,
        task_materialization.bundles.clone(),
    ));
    let effective_tool_bundle_set = run_agent_turn_tool_materialization_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        &effective_policy_set,
        &effective_prompt_context_set,
        tool_bundle_contributions,
        tool_bundle_artifacts.as_ref(),
        provider_tool_calling,
    )
    .await
    .map_err(|error| {
        warn!(
            thread_id,
            turn_id,
            error_kind = error.kind(),
            "turn tool materialization hook failed before tool runtime construction"
        );
        ChatTurnError::Terminal("turn tool materialization hook failed".to_owned())
    })?;

    let extension_bundles = effective_tool_bundle_set.bundles().to_vec();

    let runtime_environment = runtime_environment.into_iter().collect::<BTreeMap<_, _>>();
    let tools = match build_tools_with_environment(
        workdir.clone(),
        turn_id.to_owned(),
        tool_loop_config.web.clone(),
        tool_loop_config.computer_use.clone(),
        extension_bundles,
        runtime_environment.clone(),
    ) {
        Ok(tools) => tools,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = %error,
                "failed to build tool runtime with extensions; continuing with built-ins only"
            );
            build_tools_with_environment(
                workdir.clone(),
                turn_id.to_owned(),
                tool_loop_config.web.clone(),
                tool_loop_config.computer_use.clone(),
                Vec::new(),
                runtime_environment,
            )
            .unwrap_or_else(|_| {
                build_builtin_tools(
                    workdir.clone(),
                    turn_id.to_owned(),
                    tool_loop_config.web.clone(),
                    tool_loop_config.computer_use.clone(),
                )
            })
        }
    };

    skill_tool_materialization
        .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
        .await;

    let runtime = tools.runtime.clone();
    let router = tools.router.clone();

    let runtime_tool_index = Arc::new(
        skills_resolution
            .runtime_plan
            .tools
            .iter()
            .map(|descriptor| (descriptor.canonical_tool_name.clone(), descriptor.clone()))
            .collect::<HashMap<_, _>>(),
    );

    let runtime_recheck_policy = pioneer_skills::RuntimeExecutionRecheckPolicy {
        runtime_recheck_on_tool_call: tool_loop_config
            .skills
            .dependencies
            .runtime_recheck_on_tool_call,
        security: pioneer_skills::SkillSecurityPolicy {
            allow_untrusted_install: tool_loop_config.skills.security.allow_untrusted_install,
            min_trust_for_shell_tools: tool_loop_config
                .skills
                .security
                .min_trust_for_shell_tools
                .clone(),
            min_trust_for_http_tools: tool_loop_config
                .skills
                .security
                .min_trust_for_http_tools
                .clone(),
            min_trust_for_function_proxy_tools: tool_loop_config
                .skills
                .security
                .min_trust_for_function_proxy_tools
                .clone(),
            max_install_archive_bytes: tool_loop_config.skills.security.max_install_archive_bytes,
            max_install_file_bytes: tool_loop_config.skills.security.max_install_file_bytes,
        },
    };

    let turn_preflight = run_agent_turn_preflight_stage(
        provider_registry,
        provider.clone(),
        model.as_str(),
        provider_tool_calling,
        &tool_loop_config,
        &effective_policy_set,
        &effective_prompt_context_set,
        router.preflight_tool_index(),
        workspace_id,
        thread_id,
        turn_id,
        &hook_runtime_context,
        user_input_text.as_str(),
    )
    .await;

    let initial_visibility =
        compute_agent_turn_final_visible_tools(router.as_ref(), &turn_preflight, &[]);

    warn_final_visible_tool_diagnostics(
        thread_id,
        turn_id,
        initial_visibility.diagnostics.as_slice(),
    );

    trace_agent_turn_preflight(
        thread_id,
        turn_id,
        &turn_preflight,
        initial_visibility.visible_tools.as_slice(),
    );

    if let Some(active_recall_input) = preflight_active_recall_prompt_context_input(
        user_input_text.as_str(),
        model.as_str(),
        provider.name(),
        &turn_preflight,
    ) {
        effective_prompt_context_set = run_agent_turn_post_preflight_prompt_context_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
            active_recall_input,
        )
        .await;
    }

    let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        &effective_policy_set,
        &effective_prompt_context_set,
        Vec::new(),
        TurnPrePromptCompileHookInput::from_parts(
            provider_tool_calling,
            hook_tool_names_from_strings(initial_visibility.visible_tools.as_slice()),
        ),
    )
    .await
    .map_err(|error| {
        warn!(
            thread_id,
            turn_id,
            error_kind = error.kind(),
            "turn prompt section hook failed before prompt construction"
        );
        ChatTurnError::Terminal("turn prompt section hook failed".to_owned())
    })?;
    let prompt_sections =
        prompt_sections_for_compile_from_hook_sections(&effective_prompt_section_set)?;

    let initial_prompt_bundle = compile_agent_prompt_bundle(
        skills_prompt.clone(),
        None,
        prompt_sections.runtime_sections.as_slice(),
        include_task_orchestration_policy,
        true,
        continue_generation_hint,
        thread_id,
        turn_id,
    )?;

    run_noop_agent_turn_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        HookPhase::TurnPostPromptCompile,
        &effective_policy_set,
        &effective_prompt_context_set,
    )
    .await;

    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::PromptManifestCompiled {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            manifest: prompt_manifest_from_bundle(
                &initial_prompt_bundle,
                &EffectiveTurnPromptManifestHookMetadata::combined(
                    effective_prompt_context_set.manifest_metadata(),
                    effective_prompt_section_set.manifest_metadata(),
                ),
                capability_diagnostics.as_slice(),
            ),
        },
    )
    .await?;

    let mut visible_tool_names = initial_visibility.visible_tools;

    let pending_tool_ui = Arc::new(Mutex::new(HashMap::<String, PendingToolUiState>::new()));

    let mut tool_event_rx = tools.event_bus.subscribe();

    let event_tx_for_tools = event_tx.clone();
    let tool_ui_for_events = pending_tool_ui.clone();
    let workspace_id_for_tools = workspace_id.to_owned();
    let thread_id_for_tools = thread_id.to_owned();
    let turn_id_for_tools = turn_id.to_owned();

    let tool_event_forwarder = tokio::spawn(async move {
        loop {
            match tool_event_rx.recv().await {
                Ok(event) => {
                    if let Err(error) = tooling::forward_tool_event_to_agent(
                        event,
                        &event_tx_for_tools,
                        tool_ui_for_events.clone(),
                        workspace_id_for_tools.as_str(),
                        thread_id_for_tools.as_str(),
                        turn_id_for_tools.as_str(),
                    )
                    .await
                    {
                        warn!(
                            error = %error,
                            "failed to publish forwarded tool event"
                        );
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });

    let mut messages = history
        .into_iter()
        .filter(|message| message.role != pioneer_provider::Role::System)
        .collect::<Vec<_>>();
    let mut active_compiled_prompt =
        Some(compiled_prompt_payload_from_bundle(&initial_prompt_bundle));

    messages.push(user_message);
    append_recovered_tool_llm_context(&mut messages, retained_llm_context);

    let mut pending_retry_instruction: Option<String> = None;
    let mut applied_retry_instruction: Option<String> = None;
    let mut observed_review_required_signatures = BTreeSet::<String>::new();
    let mut observed_terminal_task_ids = BTreeSet::<String>::new();
    let mut tool_loop_guard = ToolLoopGuard::new(
        tool_loop_config.budget.clone(),
        tool_loop_final_answer_instruction(),
    );
    let mut tool_retry_controller = ToolRetryController::new(tool_loop_config.retry.clone());
    let mut tool_retry_lifecycle = ToolRetryLifecycleTracker::default();
    let mut task_mutation_finalization_guard = TaskMutationFinalizationGuard::default();
    let mut post_turn_assistant_text = String::new();
    let mut post_turn_tool_events = Vec::new();
    let mut post_turn_domain_events = Vec::new();

    let turn_result: Result<(), (ChatTurnError, String)> = async {
        let mut current_thinking_id = initial_thinking_item_id;
        let mut consecutive_empty_no_tool_rounds = 0usize;

        loop {
            retain_agent_attachment_messages(&mut messages);

            let review_observation = review_required_attached_task_observation(
                task_tool_provider.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
            )
            .await;
            if let Some(observation) = review_observation {
                sync_review_action_tools_to_observations(
                    &mut visible_tool_names,
                    observation.observations.as_slice(),
                );
                let has_new_signature = observation
                    .signatures
                    .iter()
                    .any(|signature| !observed_review_required_signatures.contains(signature));
                if has_new_signature {
                    observed_review_required_signatures.extend(observation.signatures.clone());
                    apply_review_required_tools_to_visible_tools(
                        &mut visible_tool_names,
                        observation.observations.as_slice(),
                        router.as_ref(),
                    );
                    let event_item_id = generate_id(TURN_ITEM_ID_LEN);
                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::ItemCompleted {
                            notification: ItemCompletedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item: TurnItem::SystemEvent {
                                    id: event_item_id,
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: observation.message.clone(),
                                    code: Some("task.review_required.observed".to_owned()),
                                    details: Some(observation.details),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    messages.push(ChatMessage::user(observation.message));
                }
            } else {
                sync_review_action_tools_to_observations(&mut visible_tool_names, &[]);
            }

            if let Some(observation) = terminal_attached_task_observation(
                task_tool_provider.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
                &observed_terminal_task_ids,
            )
            .await
            {
                for task_id in &observation.task_ids {
                    observed_terminal_task_ids.insert(task_id.clone());
                }
                let event_item_id = generate_id(TURN_ITEM_ID_LEN);
                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::ItemCompleted {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::SystemEvent {
                                id: event_item_id,
                                level: pioneer_protocol::SystemEventLevel::Info,
                                message: observation.message.clone(),
                                code: Some("task.terminal.observed".to_owned()),
                                details: Some(observation.details),
                            },
                        },
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                messages.push(ChatMessage::user(observation.message));
            }

            let round_plan = tool_loop_guard.begin_provider_round().map_err(|message| {
                (
                    ChatTurnError::Terminal(message),
                    current_thinking_id.clone(),
                )
            })?;

            if let Some(instruction) = round_plan.final_instruction.clone() {
                if let Some(budget_exceeded) = round_plan.budget_exceeded.as_ref() {
                    emit_tool_loop_budget_exceeded(
                        budget_exceeded,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                }
                let next_retry_instruction = normalize_optional_prompt(Some(instruction));
                if next_retry_instruction != applied_retry_instruction {
                    let refreshed_prompt_bundle = compile_agent_prompt_bundle(
                        skills_prompt.clone(),
                        next_retry_instruction.clone(),
                        prompt_sections.runtime_sections.as_slice(),
                        include_task_orchestration_policy,
                        round_plan.tools_enabled,
                        continue_generation_hint,
                        thread_id,
                        turn_id,
                    )
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    active_compiled_prompt = Some(compiled_prompt_payload_from_bundle(
                        &refreshed_prompt_bundle,
                    ));

                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::PromptManifestCompiled {
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            manifest: prompt_manifest_from_bundle(
                                &refreshed_prompt_bundle,
                                &EffectiveTurnPromptManifestHookMetadata::combined(
                                    effective_prompt_context_set.manifest_metadata(),
                                    effective_prompt_section_set.manifest_metadata(),
                                ),
                                capability_diagnostics.as_slice(),
                            ),
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;

                    applied_retry_instruction = next_retry_instruction.clone();
                }
                pending_retry_instruction = next_retry_instruction;
            }

            let tool_definitions = if round_plan.tools_enabled {
                router.set_model_visible_tools(&visible_tool_names).await;
                Some(
                    router
                        .model_visible_specs()
                        .await
                        .into_iter()
                        .map(|spec| ToolDefinition {
                            name: spec.name,
                            description: spec.description,
                            parameters: spec.parameters,
                        })
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };

            let round_compiled_prompt = if round_plan.tools_enabled {
                active_compiled_prompt.clone()
            } else {
                let no_tool_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
                    hook_runtime.as_ref(),
                    &hook_context,
                    &effective_policy_set,
                    &effective_prompt_context_set,
                    Vec::new(),
                    TurnPrePromptCompileHookInput::from_parts(false, Vec::new()),
                )
                .await
                .map_err(|error| {
                    (
                        ChatTurnError::Terminal(format!(
                            "turn prompt section hook failed before no-tool provider round: {}",
                            error.kind()
                        )),
                        current_thinking_id.clone(),
                    )
                })?;
                let no_tool_prompt_sections =
                    prompt_sections_for_compile_from_hook_sections(&no_tool_prompt_section_set)
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                let prompt_without_tools = compile_agent_prompt_bundle(
                    skills_prompt.clone(),
                    applied_retry_instruction.clone(),
                    no_tool_prompt_sections.runtime_sections.as_slice(),
                    include_task_orchestration_policy,
                    false,
                    continue_generation_hint,
                    thread_id,
                    turn_id,
                )
                .map_err(|error| (error, current_thinking_id.clone()))?;
                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::PromptManifestCompiled {
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        manifest: prompt_manifest_from_bundle(
                            &prompt_without_tools,
                            &EffectiveTurnPromptManifestHookMetadata::combined(
                                effective_prompt_context_set.manifest_metadata(),
                                no_tool_prompt_section_set.manifest_metadata(),
                            ),
                            capability_diagnostics.as_slice(),
                        ),
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                Some(compiled_prompt_payload_from_bundle(&prompt_without_tools))
            };

            let post_turn_assistant_text_len_before_round = post_turn_assistant_text.len();
            let round = provider::request_agent_round(
                provider,
                ChatRequest {
                    model: model.clone(),
                    messages: messages.clone(),
                    temperature: Some(0.7),
                    max_tokens: None,
                    tools: tool_definitions,
                    tool_choice: None,
                    parallel_tool_calls: round_plan.tools_enabled.then_some(true),
                    reasoning: None,
                    compiled_prompt: round_compiled_prompt,
                },
                workspace_id,
                thread_id,
                turn_id,
                current_thinking_id.as_str(),
                force_non_stream,
                tool_loop_config.provider,
                event_tx.as_ref(),
            )
            .await
            .map_err(|e| (e, current_thinking_id.clone()))?;

            if !round.text.trim().is_empty() {
                append_text_fragment(&mut post_turn_assistant_text, round.text.as_str());
            }

            turn_control
                .succeed_recovery_attempt(turn_id, recovery.take())
                .await;

            match tool_loop_guard
                .after_provider_round(round_plan.tools_enabled, round.tool_calls.len())
            {
                ToolLoopGuardDecision::Continue => {}
                ToolLoopGuardDecision::RequestFinalAnswer {
                    instruction,
                    budget_exceeded,
                } => {
                    emit_tool_loop_budget_exceeded(
                        &budget_exceeded,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                    send_reasoning_completed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        current_thinking_id.as_str(),
                        round.reasoning.as_str(),
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;

                    pending_retry_instruction =
                        normalize_optional_prompt(Some(instruction.clone()));
                    if pending_retry_instruction != applied_retry_instruction {
                        let refreshed_prompt_bundle = compile_agent_prompt_bundle(
                            skills_prompt.clone(),
                            pending_retry_instruction.clone(),
                            prompt_sections.runtime_sections.as_slice(),
                            include_task_orchestration_policy,
                            false,
                            continue_generation_hint,
                            thread_id,
                            turn_id,
                        )
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        active_compiled_prompt = Some(compiled_prompt_payload_from_bundle(
                            &refreshed_prompt_bundle,
                        ));

                        emit_durable_event(
                            event_tx.as_ref(),
                            AgentDurableEvent::PromptManifestCompiled {
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                manifest: prompt_manifest_from_bundle(
                                    &refreshed_prompt_bundle,
                                    &EffectiveTurnPromptManifestHookMetadata::combined(
                                        effective_prompt_context_set.manifest_metadata(),
                                        effective_prompt_section_set.manifest_metadata(),
                                    ),
                                    capability_diagnostics.as_slice(),
                                ),
                            },
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;

                        applied_retry_instruction = pending_retry_instruction.clone();
                    }

                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_empty_no_tool_rounds = 0;

                    continue;
                }
                ToolLoopGuardDecision::FailTurn {
                    message,
                    budget_exceeded,
                } => {
                    emit_tool_loop_budget_exceeded(
                        &budget_exceeded,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                    if budget_exceeded.reason
                        == ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled
                        && let Some(final_text) =
                            task_mutation_finalization_guard.deterministic_failure_message()
                    {
                        post_turn_assistant_text = final_text.clone();
                        send_reasoning_completed(
                            workspace_id,
                            thread_id,
                            turn_id,
                            current_thinking_id.as_str(),
                            round.reasoning.as_str(),
                            event_tx.as_ref(),
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        emit_durable_event(
                            event_tx.as_ref(),
                            AgentDurableEvent::ItemStarted {
                                notification: ItemStartedNotification {
                                    workspace_id: workspace_id.to_owned(),
                                    thread_id: thread_id.to_owned(),
                                    turn_id: turn_id.to_owned(),
                                    item: TurnItem::AgentMessage {
                                        id: message_item_id.to_owned(),
                                        text: String::new(),
                                        markdown: None,
                                        markdown_version: None,
                                    },
                                },
                            },
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        emit_progress_event(
                            event_tx.as_ref(),
                            AgentProgressEvent::ItemDelta {
                                notification: ItemDeltaNotification {
                                    workspace_id: workspace_id.to_owned(),
                                    thread_id: thread_id.to_owned(),
                                    turn_id: turn_id.to_owned(),
                                    item_id: message_item_id.to_owned(),
                                    delta: final_text.clone(),
                                    stream: Some(pioneer_protocol::ItemDeltaStream::AgentMessage),
                                    payload: None,
                                    markdown: None,
                                    markdown_version: None,
                                },
                            },
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        emit_durable_event(
                            event_tx.as_ref(),
                            AgentDurableEvent::ItemCompleted {
                                notification: ItemCompletedNotification {
                                    workspace_id: workspace_id.to_owned(),
                                    thread_id: thread_id.to_owned(),
                                    turn_id: turn_id.to_owned(),
                                    item: TurnItem::AgentMessage {
                                        id: message_item_id.to_owned(),
                                        text: final_text,
                                        markdown: None,
                                        markdown_version: None,
                                    },
                                },
                            },
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        return Ok(());
                    }
                    return Err((
                        ChatTurnError::Terminal(message),
                        current_thinking_id.clone(),
                    ));
                }
            }

            if round.tool_calls.is_empty() {
                if round_plan.tools_enabled && pending_retry_instruction.take().is_some() {
                    consecutive_empty_no_tool_rounds = 0;
                    continue;
                }

                if let Some(observation) = review_required_attached_task_observation(
                    task_tool_provider.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                )
                .await
                {
                    let has_new_signature = observation
                        .signatures
                        .iter()
                        .any(|signature| !observed_review_required_signatures.contains(signature));
                    if !has_new_signature {
                        return Err((
                            ChatTurnError::Terminal(review_required_final_answer_block_message()),
                            current_thinking_id.clone(),
                        ));
                    }

                    observed_review_required_signatures.extend(observation.signatures.clone());
                    sync_review_action_tools_to_observations(
                        &mut visible_tool_names,
                        observation.observations.as_slice(),
                    );
                    apply_review_required_tools_to_visible_tools(
                        &mut visible_tool_names,
                        observation.observations.as_slice(),
                        router.as_ref(),
                    );
                    send_reasoning_completed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        current_thinking_id.as_str(),
                        round.reasoning.as_str(),
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    let event_item_id = generate_id(TURN_ITEM_ID_LEN);
                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::ItemCompleted {
                            notification: ItemCompletedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item: TurnItem::SystemEvent {
                                    id: event_item_id,
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: observation.message.clone(),
                                    code: Some("task.review_required.observed".to_owned()),
                                    details: Some(observation.details),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    messages.push(ChatMessage::user(observation.message));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_empty_no_tool_rounds = 0;
                    continue;
                }

                if let Some(observation) = pending_attached_task_observation(
                    task_tool_provider.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                )
                .await
                {
                    send_reasoning_completed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        current_thinking_id.as_str(),
                        round.reasoning.as_str(),
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    let event_item_id = generate_id(TURN_ITEM_ID_LEN);
                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::ItemCompleted {
                            notification: ItemCompletedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item: TurnItem::SystemEvent {
                                    id: event_item_id,
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: observation.clone(),
                                    code: Some("task.attached.pending".to_owned()),
                                    details: None,
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    messages.push(ChatMessage::user(observation));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_empty_no_tool_rounds = 0;
                    continue;
                }

                if let Some(observation) = terminal_attached_task_observation(
                    task_tool_provider.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                    &observed_terminal_task_ids,
                )
                .await
                {
                    for task_id in &observation.task_ids {
                        observed_terminal_task_ids.insert(task_id.clone());
                    }
                    send_reasoning_completed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        current_thinking_id.as_str(),
                        round.reasoning.as_str(),
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    let event_item_id = generate_id(TURN_ITEM_ID_LEN);
                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::ItemCompleted {
                            notification: ItemCompletedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item: TurnItem::SystemEvent {
                                    id: event_item_id,
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: observation.message.clone(),
                                    code: Some("task.terminal.observed".to_owned()),
                                    details: Some(observation.details),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    messages.push(ChatMessage::user(observation.message));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_empty_no_tool_rounds = 0;
                    continue;
                }

                let deterministic_final_text =
                    task_mutation_finalization_guard.deterministic_failure_message();
                let final_text = deterministic_final_text
                    .clone()
                    .unwrap_or_else(|| round.text.clone());
                if deterministic_final_text.is_none() && final_text.trim().is_empty() {
                    consecutive_empty_no_tool_rounds += 1;

                    if consecutive_empty_no_tool_rounds
                        >= MAX_CONSECUTIVE_EMPTY_NO_TOOL_ROUNDS
                    {
                        return Err((
                            ChatTurnError::ProviderFailure {
                                item_id: current_thinking_id.clone(),
                                item_type: TurnItemType::Reasoning,
                                failure: ProviderFailureDetails {
                                    provider: provider.name().to_owned(),
                                    model: model.clone(),
                                    transport: if provider.capabilities().streaming
                                        && !force_non_stream
                                    {
                                        ProviderTransportKind::Stream
                                    } else {
                                        ProviderTransportKind::NonStream
                                    },
                                    class: ProviderFailureClass::EmptyResponse,
                                    stage: ProviderFailureStage::Finalize,
                                    http_status: None,
                                    provider_code: Some("empty_model_response".to_owned()),
                                    retry_after_ms: None,
                                    is_recoverable_hint: true,
                                    message: Some(
                                        "model returned an empty response without tool calls"
                                            .to_owned(),
                                    ),
                                },
                            },
                            current_thinking_id.clone(),
                        ));
                    }

                    if !round.reasoning.trim().is_empty() {
                        send_reasoning_completed(
                            workspace_id,
                            thread_id,
                            turn_id,
                            current_thinking_id.as_str(),
                            round.reasoning.as_str(),
                            event_tx.as_ref(),
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;

                        current_thinking_id =
                            start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                                .await
                                .map_err(|error| (error, current_thinking_id.clone()))?;
                    }

                    messages.push(ChatMessage::user(EMPTY_NO_TOOL_ROUND_RECOVERY_INSTRUCTION));
                    continue;
                }

                if let Some(finalization_provider) = turn_finalization_provider.as_ref() {
                    match finalization_provider
                        .check_turn_finalization(TurnFinalizationContext {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            final_text: final_text.clone(),
                        })
                        .await
                    {
                        Ok(TurnFinalizationDecision::Allow) => {}
                        Ok(TurnFinalizationDecision::Retry { instruction }) => {
                            post_turn_assistant_text
                                .truncate(post_turn_assistant_text_len_before_round);
                            if !round.reasoning.trim().is_empty() {
                                send_reasoning_completed(
                                    workspace_id,
                                    thread_id,
                                    turn_id,
                                    current_thinking_id.as_str(),
                                    round.reasoning.as_str(),
                                    event_tx.as_ref(),
                                )
                                .await
                                .map_err(|error| (error, current_thinking_id.clone()))?;

                                current_thinking_id = start_reasoning_item(
                                    workspace_id,
                                    thread_id,
                                    turn_id,
                                    event_tx.as_ref(),
                                )
                                .await
                                .map_err(|error| (error, current_thinking_id.clone()))?;
                            }
                            consecutive_empty_no_tool_rounds = 0;
                            messages.push(ChatMessage::user(instruction));
                            continue;
                        }
                        Ok(TurnFinalizationDecision::Fail { message }) => {
                            post_turn_assistant_text
                                .truncate(post_turn_assistant_text_len_before_round);
                            return Err((ChatTurnError::Terminal(message), current_thinking_id.clone()));
                        }
                        Err(error) => {
                            post_turn_assistant_text
                                .truncate(post_turn_assistant_text_len_before_round);
                            return Err((
                                ChatTurnError::Terminal(format!(
                                    "turn finalization check failed: {error}"
                                )),
                                current_thinking_id.clone(),
                            ));
                        }
                    }
                }
                if final_text != round.text {
                    post_turn_assistant_text = final_text.clone();
                }

                send_reasoning_completed(
                    workspace_id,
                    thread_id,
                    turn_id,
                    current_thinking_id.as_str(),
                    round.reasoning.as_str(),
                    event_tx.as_ref(),
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;

                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::ItemStarted {
                        notification: ItemStartedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::AgentMessage {
                                id: message_item_id.to_owned(),
                                text: String::new(),
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;

                if !final_text.is_empty() {
                    emit_progress_event(
                        event_tx.as_ref(),
                        AgentProgressEvent::ItemDelta {
                            notification: ItemDeltaNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item_id: message_item_id.to_owned(),
                                delta: final_text.clone(),
                                stream: Some(pioneer_protocol::ItemDeltaStream::AgentMessage),
                                payload: None,
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                }

                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::ItemCompleted {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::AgentMessage {
                                id: message_item_id.to_owned(),
                                text: final_text,
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;

                return Ok(());
            }

            consecutive_empty_no_tool_rounds = 0;

            send_reasoning_completed(
                workspace_id,
                thread_id,
                turn_id,
                current_thinking_id.as_str(),
                round.reasoning.as_str(),
                event_tx.as_ref(),
            )
            .await
            .map_err(|error| (error, current_thinking_id.clone()))?;

            messages.push(ChatMessage::assistant_tool_calls_with_reasoning(
                (!round.text.is_empty()).then_some(round.text.clone()),
                (!round.reasoning.is_empty()).then_some(round.reasoning.clone()),
                round.tool_calls.clone(),
            ));

            let tool_tasks = round
                .tool_calls
                .into_iter()
                .map(|model_tool_call| {
                    let router = router.clone();
                    let runtime = runtime.clone();
                    let turn_control = turn_control.clone();
                    let pending_tool_ui = pending_tool_ui.clone();
                    let event_tx = event_tx.clone();
                    let runtime_tool_index = runtime_tool_index.clone();
                    let runtime_recheck_policy = runtime_recheck_policy.clone();
                    let workspace_id = workspace_id.to_owned();
                    let thread_id = thread_id.to_owned();
                    let turn_id = turn_id.to_owned();

                    async move {
                        let item_id = model_tool_call.id.clone();
                        let arguments = model_tool_call.arguments.clone();
                        let tool_name = model_tool_call.name.clone();
                        let item_type = tooling::tool_item_type_from_name(tool_name.as_str());
                        let attempt_number = 1;
                        let recovery_policy = router
                            .find_spec(tool_name.as_str())
                            .map(|configured| {
                                tool_recovery_policy::snapshot_for_tool_metadata(
                                    item_type,
                                    configured.spec.recovery,
                                )
                            })
                            .unwrap_or_else(|| {
                                tool_recovery_policy::conservative_no_recovery_snapshot(item_type)
                            });

                        {
                            let mut pending = pending_tool_ui.lock().await;
                            let state = pending.entry(item_id.clone()).or_default();
                            state.tool_name = tool_name.clone();
                            state.arguments = arguments.clone();
                            state.recovery_policy = Some(recovery_policy.clone());
                            state.output_policy = router
                                .find_spec(tool_name.as_str())
                                .map(|configured| configured.output_policy.clone());
                        }

                        if let Some(descriptor) = runtime_tool_index.get(tool_name.as_str()) {
                            let check = pioneer_skills::recheck_runtime_tool_execution(
                                descriptor,
                                &runtime_recheck_policy,
                            );

                            if !check.allowed {
                                let error_payload = serde_json::json!({
                                    "error": check.message,
                                    "code": check.reason_code,
                                    "dependency_diagnostics": check.dependency_diagnostics,
                                    "skill_slug": descriptor.skill_slug,
                                    "source_kind": descriptor.source_kind.as_db_value(),
                                });

                                let error_text = serde_json::to_string(&error_payload)
                                    .unwrap_or_else(|_| error_payload.to_string());
                                {
                                    let mut pending = pending_tool_ui.lock().await;
                                    pending.remove(item_id.as_str());
                                }
                                let output_policy = router
                                    .find_spec(tool_name.as_str())
                                    .map(|configured| configured.output_policy.clone());
                                let outcome = classify_tool_error(
                                    tool_name.as_str(),
                                    &pioneer_tools::ToolError::Rejected(error_text.clone()),
                                );

                                let _ = event_tx
                                    .publish_durable(AgentDurableEvent::ItemStarted {
                                        notification: ItemStartedNotification {
                                            workspace_id: workspace_id.clone(),
                                            thread_id: thread_id.clone(),
                                            turn_id: turn_id.clone(),
                                            item: tooling::build_started_tool_turn_item(
                                                item_id.clone(),
                                                tool_name.clone(),
                                                arguments.clone(),
                                                Some(recovery_policy.clone()),
                                                output_policy.clone(),
                                                None,
                                            ),
                                        },
                                    })
                                    .await;

                                let _ = event_tx
                                    .publish_durable(AgentDurableEvent::ItemCompleted {
                                        notification: ItemCompletedNotification {
                                            workspace_id: workspace_id.clone(),
                                            thread_id: thread_id.clone(),
                                            turn_id: turn_id.clone(),
                                            item: tooling::build_failed_tool_turn_item(
                                                item_id.clone(),
                                                tool_name.clone(),
                                                arguments.clone(),
                                                error_text.clone(),
                                                outcome.clone(),
                                                Some(recovery_policy.clone()),
                                                output_policy,
                                                None,
                                            ),
                                        },
                                    })
                                    .await;

                                let _ = event_tx
                                    .publish_durable(AgentDurableEvent::SkillAuditEvents {
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        events: vec![protocol_skill_audit_event(
                                            pioneer_skills::SkillAuditEvent::runtime_blocked(
                                                descriptor.skill_slug.clone(),
                                                descriptor.source_kind.as_db_value().to_owned(),
                                                check
                                                    .reason_code
                                                    .clone()
                                                    .unwrap_or_else(|| "runtime.blocked".to_owned()),
                                                serde_json::json!({
                                                    "tool_name": descriptor.canonical_tool_name,
                                                    "message": check.message,
                                                    "dependency_diagnostics": check.dependency_diagnostics,
                                                }),
                                                match std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                {
                                                    Ok(duration) => i64::try_from(duration.as_secs())
                                                        .unwrap_or(i64::MAX),
                                                    Err(_) => 0,
                                                },
                                            ),
                                        )],
                                    })
                                    .await;

                                return ExecutedToolResult {
                                    item_id: item_id.clone(),
                                    item_type,
                                    attempt_number,
                                    tool_name: tool_name.clone(),
                                    arguments: arguments.clone(),
                                    model_visible_text: error_text.clone(),
                                    success: false,
                                    outcome: outcome.clone(),
                                    recovery_view: None,
                                    request_tools_result: None,
                                    message: tooling::build_tool_error_message(
                                        model_tool_call.id,
                                        tool_name,
                                        error_text,
                                        outcome,
                                    ),
                                };
                            }
                        }

                        let tool_call = match router.build_model_tool_call(RawToolCall {
                            call_id: model_tool_call.id.clone(),
                            tool_name: tool_name.clone(),
                            arguments: arguments.clone(),
                        }).await {
                            Ok(tool_call) => tool_call,
                            Err(error) => {
                                let suppress_partial_unknown_tool_ui =
                                    matches!(&error, pioneer_tools::ToolError::NotFound(_))
                                        && router.has_spec_name_with_prefix(tool_name.as_str());
                                let error_text = error.to_string();
                                {
                                    let mut pending = pending_tool_ui.lock().await;
                                    pending.remove(item_id.as_str());
                                }
                                let output_policy = router
                                    .find_spec(tool_name.as_str())
                                    .map(|configured| configured.output_policy.clone());
                                let outcome = classify_tool_error(tool_name.as_str(), &error);
                                if !suppress_partial_unknown_tool_ui {
                                    let _ = event_tx
                                        .publish_durable(AgentDurableEvent::ItemStarted {
                                            notification: ItemStartedNotification {
                                                workspace_id: workspace_id.clone(),
                                                thread_id: thread_id.clone(),
                                                turn_id: turn_id.clone(),
                                                item: tooling::build_started_tool_turn_item(
                                                    item_id.clone(),
                                                    tool_name.clone(),
                                                    arguments.clone(),
                                                    Some(recovery_policy.clone()),
                                                    output_policy.clone(),
                                                    None,
                                                ),
                                            },
                                        })
                                        .await;
                                    let _ = event_tx
                                        .publish_durable(AgentDurableEvent::ItemCompleted {
                                            notification: ItemCompletedNotification {
                                                workspace_id,
                                                thread_id,
                                                turn_id,
                                                item: tooling::build_failed_tool_turn_item(
                                                    item_id.clone(),
                                                    tool_name.clone(),
                                                    arguments.clone(),
                                                    error_text.clone(),
                                                    outcome.clone(),
                                                    Some(recovery_policy.clone()),
                                                    output_policy,
                                                    None,
                                                ),
                                            },
                                        })
                                        .await;
                                }
                                return ExecutedToolResult {
                                    item_id: item_id.clone(),
                                    item_type,
                                    attempt_number,
                                    tool_name: tool_name.clone(),
                                    arguments: arguments.clone(),
                                    model_visible_text: error_text.clone(),
                                    success: false,
                                    outcome: outcome.clone(),
                                    recovery_view: None,
                                    request_tools_result: None,
                                    message: tooling::build_tool_error_message(
                                        model_tool_call.id,
                                        tool_name,
                                        error_text,
                                        outcome,
                                    ),
                                };
                            }
                        };

                        let attempt_token = turn_control.register_attempt(item_id.clone()).await;
                        let heartbeat_stop = CancellationToken::new();
                        let heartbeat_cancel = heartbeat_stop.clone();
                        let heartbeat_event_tx = event_tx.clone();
                        let heartbeat_thread_id = thread_id.clone();
                        let heartbeat_turn_id = turn_id.clone();
                        let heartbeat_item_id = item_id.clone();
                        let heartbeat_item_type =
                            tooling::tool_item_type_from_name(tool_name.as_str());

                        let heartbeat_task = tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = heartbeat_cancel.cancelled() => break,
                                    _ = sleep(Duration::from_secs(5)) => {
                                        if heartbeat_cancel.is_cancelled() {
                                            break;
                                        }
                                        heartbeat_event_tx.publish_heartbeat(
                                            workspace_id.clone(),
                                            heartbeat_thread_id.clone(),
                                            heartbeat_turn_id.clone(),
                                            heartbeat_item_id.clone(),
                                            heartbeat_item_type,
                                        );
                                    }
                                }
                            }
                        });

                        let dispatch_result = runtime
                            .execute_tool_call_with_cancellation(tool_call, attempt_token.clone())
                            .await;

                        heartbeat_stop.cancel();
                        let _ = heartbeat_task.await;
                        turn_control
                            .complete_attempt(turn_id.as_str(), item_id.as_str())
                            .await;

                        let (
                            tool_output,
                            success,
                            outcome,
                            recovery_view,
                            request_tools_result,
                            message,
                        ) =
                            match dispatch_result {
                                Ok(result) => {
                                    let success = result.success();
                                    let projection = result.projection();
                                    let request_tools_result = extract_request_tools_result(
                                        tool_name.as_str(),
                                        success,
                                        projection,
                                    );
                                    (
                                        result.model_visible_text(),
                                        success,
                                        result.outcome.clone(),
                                        projection.and_then(|projection| projection.recovery.clone()),
                                        request_tools_result,
                                        result.to_model_input_item().into_chat_message(),
                                    )
                                }
                                Err(error) => {
                                    let output = error.to_string();
                                    let outcome = classify_tool_error(tool_name.as_str(), &error);
                                    let message = tooling::build_tool_error_message(
                                        model_tool_call.id.clone(),
                                        tool_name.clone(),
                                        output.clone(),
                                        outcome.clone(),
                                    );
                                    (output, false, outcome, None, None, message)
                                }
                            };

                        ExecutedToolResult {
                            item_id: item_id.clone(),
                            item_type,
                            attempt_number,
                            tool_name: tool_name.clone(),
                            arguments: arguments.clone(),
                            model_visible_text: tool_output,
                            success,
                            outcome,
                            recovery_view,
                            request_tools_result,
                            message,
                        }
                    }
                })
                .collect::<Vec<_>>();

            let parallel_tool_calls = tool_tasks.len().max(1);
            let executed_results = stream::iter(tool_tasks)
                .buffer_unordered(parallel_tool_calls)
                .collect::<Vec<_>>()
                .await;

            apply_request_tools_results_to_visible_tools(
                &mut visible_tool_names,
                &executed_results,
                router.as_ref(),
            );

            for result in &executed_results {
                record_observed_terminal_task_ids(&mut observed_terminal_task_ids, result);
                task_mutation_finalization_guard.observe(result);
            }
            post_turn_tool_events.extend(
                executed_results
                    .iter()
                    .map(post_turn_tool_event_summary),
            );
            post_turn_domain_events.extend(
                executed_results
                    .iter()
                    .map(post_turn_domain_event_summary_from_tool),
            );

            let retry_observations = executed_results
                .iter()
                .map(ExecutedToolResult::retry_observation)
                .collect::<Vec<_>>();
            let mut next_round_tools_enabled = true;
            pending_retry_instruction = match tool_retry_controller.decide(&retry_observations) {
                ToolRetryDecision::None { drafts } => {
                    emit_tool_retry_drafts(
                        drafts.as_slice(),
                        &mut tool_retry_lifecycle,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                    None
                }
                ToolRetryDecision::Retry { prompt, drafts } => {
                    emit_tool_retry_drafts(
                        drafts.as_slice(),
                        &mut tool_retry_lifecycle,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                    render_tool_retry_instruction(
                        ToolRetryInstructionKind::Retry,
                        &prompt.fact_lines(),
                    )
                }
                ToolRetryDecision::Exhausted { prompt, drafts, .. } => {
                    emit_tool_retry_drafts(
                        drafts.as_slice(),
                        &mut tool_retry_lifecycle,
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (agent_event_error(error), current_thinking_id.clone()))?;
                    let exhausted_instruction = render_tool_retry_instruction(
                        ToolRetryInstructionKind::Exhausted,
                        &prompt.fact_lines(),
                    );
                    if let Some(instruction) = exhausted_instruction.clone() {
                        next_round_tools_enabled = false;
                        match tool_loop_guard.request_final_answer_with_instruction(instruction) {
                            Ok(instruction) => Some(instruction),
                            Err(message) => {
                                return Err((
                                    ChatTurnError::Terminal(message),
                                    current_thinking_id.clone(),
                                ));
                            }
                        }
                    } else {
                        None
                    }
                }
            };

            let next_retry_instruction =
                normalize_optional_prompt(pending_retry_instruction.clone());

            if next_retry_instruction != applied_retry_instruction {
                let refreshed_prompt_bundle = compile_agent_prompt_bundle(
                    skills_prompt.clone(),
                    next_retry_instruction.clone(),
                    prompt_sections.runtime_sections.as_slice(),
                    include_task_orchestration_policy,
                    next_round_tools_enabled,
                    continue_generation_hint,
                    thread_id,
                    turn_id,
                )
                .map_err(|error| (error, current_thinking_id.clone()))?;
                active_compiled_prompt = Some(compiled_prompt_payload_from_bundle(
                    &refreshed_prompt_bundle,
                ));

                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::PromptManifestCompiled {
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        manifest: prompt_manifest_from_bundle(
                            &refreshed_prompt_bundle,
                            &EffectiveTurnPromptManifestHookMetadata::combined(
                                effective_prompt_context_set.manifest_metadata(),
                                effective_prompt_section_set.manifest_metadata(),
                            ),
                            capability_diagnostics.as_slice(),
                        ),
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;

                applied_retry_instruction = next_retry_instruction;
            }

            messages.extend(executed_results.into_iter().map(|result| result.message));

            current_thinking_id = start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
        }
    }
    .await;
    skill_tool_materialization
        .clear_function_proxy_runtime()
        .await;
    drop(runtime);
    drop(router);
    drop(tools);

    let _ = tool_event_forwarder.await;

    match turn_result {
        Ok(()) => {
            let summary = AgentTurnPostTurnSummary::succeeded_with_model(
                Some(post_turn_model.clone()),
                Some(post_turn_model_provider.clone()),
                extract_user_text(input),
                post_turn_assistant_text,
                post_turn_tool_events,
                post_turn_domain_events,
            );
            Ok(AgentTurnPostTurnHookDispatch::new(
                hook_context,
                effective_policy_set,
                effective_prompt_context_set,
                summary,
            ))
        }
        Err((error, thinking_id)) => {
            emit_durable_event(
                event_tx.as_ref(),
                AgentDurableEvent::ItemCompleted {
                    notification: ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: TurnItem::Reasoning {
                            id: thinking_id.clone(),
                            summary: Vec::new(),
                            content: Vec::new(),
                        },
                    },
                },
            )
            .await?;
            let error = match error {
                ChatTurnError::ProviderFailure {
                    item_id,
                    item_type,
                    failure,
                } => ChatTurnError::ProviderFailure {
                    item_id: if item_id.is_empty() {
                        thinking_id
                    } else {
                        item_id
                    },
                    item_type,
                    failure,
                },
                other => other,
            };
            Err(with_post_turn_failure_dispatch(
                error,
                hook_context,
                effective_policy_set,
                effective_prompt_context_set,
                extract_user_text(input),
                post_turn_assistant_text,
                post_turn_tool_events,
                post_turn_domain_events,
            ))
        }
    }
}

fn extract_user_text(input: &[UserInput]) -> String {
    let mut parts = Vec::new();

    for item in input {
        if let UserInput::Text { text, .. } = item {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed.to_owned());
            }
        }
    }

    parts.join("\n")
}

fn build_user_message(
    input: &[UserInput],
    resolved_artifacts: &[ResolvedArtifactInput],
) -> ChatMessage {
    let mut message = ChatMessage::user(extract_user_text(input));

    for item in input {
        match item {
            UserInput::Image { url } => {
                message
                    .content_parts
                    .push(MessageContentPart::image(MessageAttachment::from_url(
                        url.clone(),
                        infer_mime_from_reference(url.as_str(), InputContentType::Image),
                    )));
            }
            UserInput::LocalImage { path } => {
                message.content_parts.push(MessageContentPart::image(
                    MessageAttachment::from_path(
                        path.clone(),
                        infer_mime_from_reference(path.as_str(), InputContentType::Image),
                    ),
                ));
            }
            UserInput::File { url } => {
                message
                    .content_parts
                    .push(MessageContentPart::file(MessageAttachment::from_url(
                        url.clone(),
                        infer_mime_from_reference(url.as_str(), InputContentType::File),
                    )));
            }
            UserInput::LocalFile { path } => {
                message
                    .content_parts
                    .push(MessageContentPart::file(MessageAttachment::from_path(
                        path.clone(),
                        infer_mime_from_reference(path.as_str(), InputContentType::File),
                    )));
            }
            UserInput::Audio { url } => {
                message
                    .content_parts
                    .push(MessageContentPart::audio(MessageAttachment::from_url(
                        url.clone(),
                        infer_mime_from_reference(url.as_str(), InputContentType::Audio),
                    )));
            }
            UserInput::LocalAudio { path } => {
                message.content_parts.push(MessageContentPart::audio(
                    MessageAttachment::from_path(
                        path.clone(),
                        infer_mime_from_reference(path.as_str(), InputContentType::Audio),
                    ),
                ));
            }
            UserInput::Video { url } => {
                message
                    .content_parts
                    .push(MessageContentPart::video(MessageAttachment::from_url(
                        url.clone(),
                        infer_mime_from_reference(url.as_str(), InputContentType::Video),
                    )));
            }
            UserInput::LocalVideo { path } => {
                message.content_parts.push(MessageContentPart::video(
                    MessageAttachment::from_path(
                        path.clone(),
                        infer_mime_from_reference(path.as_str(), InputContentType::Video),
                    ),
                ));
            }
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => {
                if let Some(resolved) =
                    find_resolved_artifact(resolved_artifacts, artifact_id, version_id.as_deref())
                {
                    message
                        .content_parts
                        .push(content_part_for_resolved_artifact(resolved));
                }
            }
            UserInput::Text { .. } | UserInput::Mention { .. } => {}
        }
    }

    message
}

fn find_resolved_artifact<'a>(
    resolved_artifacts: &'a [ResolvedArtifactInput],
    artifact_id: &str,
    version_id: Option<&str>,
) -> Option<&'a ResolvedArtifactInput> {
    resolved_artifacts.iter().find(|artifact| {
        artifact.artifact_id == artifact_id && artifact.version_id.as_deref() == version_id
    })
}

fn content_part_for_resolved_artifact(resolved: &ResolvedArtifactInput) -> MessageContentPart {
    match resolved.content_type {
        InputContentType::Image => MessageContentPart::image(resolved.attachment.clone()),
        InputContentType::Audio => MessageContentPart::audio(resolved.attachment.clone()),
        InputContentType::Video => MessageContentPart::video(resolved.attachment.clone()),
        InputContentType::Text | InputContentType::File => {
            MessageContentPart::file(resolved.attachment.clone())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ComputerUseSnapshotScope {
    Session(u64),
    UnknownSnapshot,
}

fn computer_use_snapshot_scope(message: &ChatMessage) -> Option<ComputerUseSnapshotScope> {
    if message.role != pioneer_provider::Role::Tool
        || message.name.as_deref() != Some("computer_use")
        || !message.has_attachments()
    {
        return None;
    }

    let payload = match serde_json::from_str::<JsonValue>(message.content.as_str()) {
        Ok(payload) => payload,
        Err(_) => return Some(ComputerUseSnapshotScope::UnknownSnapshot),
    };

    match payload.get("action").and_then(JsonValue::as_str) {
        Some("snapshot") | None => {
            let session_id = payload.get("session_id").and_then(JsonValue::as_u64);
            Some(
                session_id
                    .map(ComputerUseSnapshotScope::Session)
                    .unwrap_or(ComputerUseSnapshotScope::UnknownSnapshot),
            )
        }
        _ => None,
    }
}

fn retain_agent_attachment_messages(messages: &mut [ChatMessage]) {
    let attachment_budget_bytes = pioneer_provider::default_attachment_pipeline_config()
        .max_total_bytes_per_request
        .max(1);
    retain_agent_attachment_messages_with_budget(messages, attachment_budget_bytes);
}

fn retain_agent_attachment_messages_with_budget(
    messages: &mut [ChatMessage],
    attachment_budget_bytes: usize,
) {
    let mut latest_snapshot_index: HashMap<ComputerUseSnapshotScope, usize> = HashMap::new();

    for (index, message) in messages.iter().enumerate().rev() {
        if let Some(scope) = computer_use_snapshot_scope(message) {
            latest_snapshot_index.entry(scope).or_insert(index);
        }
    }

    for (index, message) in messages.iter_mut().enumerate() {
        if !message.has_attachments() {
            continue;
        }

        if let Some(scope) = computer_use_snapshot_scope(message)
            && latest_snapshot_index.get(&scope).copied() != Some(index)
        {
            message.content_parts.clear();
        }
    }

    let mut total_attachment_bytes = estimated_total_attachment_bytes(messages);
    if total_attachment_bytes <= attachment_budget_bytes {
        return;
    }

    let snapshot_indices = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            (message.has_attachments() && computer_use_snapshot_scope(message).is_some())
                .then_some(index)
        })
        .collect::<Vec<_>>();

    for index in snapshot_indices {
        if total_attachment_bytes <= attachment_budget_bytes {
            break;
        }
        if messages[index].content_parts.is_empty() {
            continue;
        }
        let removed_bytes = estimated_message_attachment_bytes(&messages[index]);
        messages[index].content_parts.clear();
        if removed_bytes > 0 {
            total_attachment_bytes = total_attachment_bytes.saturating_sub(removed_bytes);
        } else {
            total_attachment_bytes = estimated_total_attachment_bytes(messages);
        }
    }
}

fn retain_chat_mode_attachment_messages(messages: &mut [ChatMessage]) {
    let keep_index = messages
        .iter()
        .enumerate()
        .rev()
        .find(|(_, message)| {
            message.role == pioneer_provider::Role::User && message.has_attachments()
        })
        .map(|(index, _)| index);

    for (index, message) in messages.iter_mut().enumerate() {
        if !message.has_attachments() {
            continue;
        }
        let keep = keep_index == Some(index) && message.role == pioneer_provider::Role::User;
        if !keep {
            message.content_parts.clear();
        }
    }
}

fn estimated_total_attachment_bytes(messages: &[ChatMessage]) -> usize {
    messages
        .iter()
        .map(estimated_message_attachment_bytes)
        .sum::<usize>()
}

fn estimated_message_attachment_bytes(message: &ChatMessage) -> usize {
    message
        .content_parts
        .iter()
        .map(estimated_attachment_part_bytes)
        .sum::<usize>()
}

fn estimated_attachment_part_bytes(part: &MessageContentPart) -> usize {
    let attachment = match part {
        MessageContentPart::Text { .. } => return 0,
        MessageContentPart::File { file } => file,
        MessageContentPart::Image { image } => image,
        MessageContentPart::Audio { audio } => audio,
        MessageContentPart::Video { video } => video,
    };

    if let Some(size_bytes) = attachment.size_bytes {
        return usize::try_from(size_bytes).unwrap_or(usize::MAX);
    }

    match &attachment.source {
        AttachmentDataSource::Path { path } => std::fs::metadata(path.as_str())
            .ok()
            .and_then(|meta| usize::try_from(meta.len()).ok())
            .unwrap_or(0),
        AttachmentDataSource::Bytes { base64_data } => base64_data.len().saturating_mul(3) / 4,
        AttachmentDataSource::Url { .. } | AttachmentDataSource::Reference { .. } => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutedToolResult, TaskMutationFinalizationGuard, append_recovered_tool_llm_context,
        apply_request_tools_results_to_visible_tools, apply_request_tools_visibility_expansion,
        apply_review_required_tools_to_visible_tools, build_user_message,
        compile_agent_prompt_bundle_with_prompt_root, normalize_turn_capabilities,
        resolve_skill_capability_summary, retain_agent_attachment_messages,
        retain_agent_attachment_messages_with_budget, retain_chat_mode_attachment_messages,
        review_required_observation_payload, review_required_observation_signature,
        sync_review_action_tools_to_observations,
    };
    use crate::{ResolvedArtifactInput, RetainedToolLlmContext, ReviewRequiredTaskObservation};
    use pioneer_promt::{
        PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput,
        PromptSectionId,
    };
    use pioneer_protocol::{
        McpScopeKind, TurnCapability, TurnCapabilityAcceptedReason, TurnCapabilityKind,
        TurnCapabilityRejectedReason, TurnItemType, UserInput,
    };
    use pioneer_provider::{
        AttachmentDataSource, ChatMessage, InputContentType, MessageAttachment, MessageContentPart,
        Role,
    };
    use pioneer_skills::compile::CompileSkillInput;
    use pioneer_skills::contract::default_skill_conformance;
    use pioneer_skills::{
        ExcludedSkill, ResolvedSkill, SkillDependencies, SkillExcludedReason, SkillExplicitRef,
        SkillResolutionResult, SkillResolvedReason, SkillRuntimePlan, SkillSourceKind,
        SkillTrustLevel, compile_skill_definition,
    };
    use pioneer_tools::{
        BuiltinToolDomain, ComputerUseToolsConfig, ConfiguredToolSpec, ExecutionClass,
        FunctionToolOutput, PayloadKind, RequestToolsResult, ToolError, ToolErrorClass,
        ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutcome, ToolOutput, ToolSpec,
        WebToolsConfig, dynamic_unknown_output_policy,
    };
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct NoopToolHandler;

    #[async_trait::async_trait]
    impl ToolHandler for NoopToolHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: pioneer_tools::ToolEventTrace,
        ) -> Result<Box<dyn ToolOutput>, ToolError> {
            Ok(Box::new(FunctionToolOutput::new("ok", true)))
        }
    }

    #[test]
    fn phase_11_review_required_observation_payload_is_bounded_and_actionable() {
        let observation = ReviewRequiredTaskObservation {
            task_id: "task_review_payload".to_owned(),
            run_id: "run_review_payload".to_owned(),
            candidate_id: "candidate_review_payload".to_owned(),
            title: "Review payload".to_owned(),
            status: "waiting_review".to_owned(),
            candidate_status: "pending_review".to_owned(),
            round: 2,
            summary: Some("candidate summary".to_owned()),
            result_preview: Some("r".repeat(1400)),
            extraction_error_preview: Some("e".repeat(900)),
            diagnostics: vec!["d".repeat(500), "diagnostic two".to_owned()],
            child_thread_id: Some("child_thread_review_payload".to_owned()),
            child_turn_id: Some("child_turn_review_payload".to_owned()),
            max_revision_rounds: 2,
            remaining_revision_rounds: 0,
            allowed_actions: vec!["task_accept".to_owned(), "task_cancel".to_owned()],
            revision_blocked_reason: Some("max_revision_rounds_reached".to_owned()),
        };

        let payload = review_required_observation_payload(&observation);
        assert_eq!(payload["taskId"], "task_review_payload");
        assert_eq!(payload["runId"], "run_review_payload");
        assert_eq!(payload["candidateId"], "candidate_review_payload");
        assert_eq!(payload["status"], "waiting_review");
        assert_eq!(payload["candidateStatus"], "pending_review");
        assert_eq!(payload["round"], 2);
        assert_eq!(payload["summary"], "candidate summary");
        assert_eq!(payload["childThreadId"], "child_thread_review_payload");
        assert_eq!(payload["childTurnId"], "child_turn_review_payload");
        assert_eq!(payload["maxRevisionRounds"], 2);
        assert_eq!(payload["remainingRevisionRounds"], 0);
        assert_eq!(
            payload["allowedActions"],
            serde_json::json!(["task_accept", "task_cancel"])
        );
        assert_eq!(
            payload["revisionBlockedReason"],
            "max_revision_rounds_reached"
        );
        assert!(
            payload["resultPreview"]
                .as_str()
                .expect("result preview should be present")
                .chars()
                .count()
                <= 1203
        );
        assert!(
            payload["extractionErrorPreview"]
                .as_str()
                .expect("error preview should be present")
                .chars()
                .count()
                <= 803
        );
        assert!(
            payload["diagnostics"][0]
                .as_str()
                .expect("diagnostic should be present")
                .chars()
                .count()
                <= 403
        );

        let signature = review_required_observation_signature(&observation);
        assert!(signature.contains("task_review_payload"));
        assert!(signature.contains("candidate_review_payload"));
        assert!(signature.contains("task_accept,task_cancel"));
        assert!(signature.contains("max_revision_rounds_reached"));
    }

    fn skill_capability(id: &str, slug: &str) -> TurnCapability {
        TurnCapability {
            id: id.to_owned(),
            label: Some(slug.to_owned()),
            kind: TurnCapabilityKind::Skill {
                slug: slug.to_owned(),
                source_kind: "user".to_owned(),
            },
        }
    }

    fn explicit_skill_ref(id: &str, slug: &str) -> SkillExplicitRef {
        SkillExplicitRef {
            capability_id: id.to_owned(),
            label: Some(slug.to_owned()),
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
        }
    }

    fn test_skill_definition(slug: &str) -> pioneer_skills::SkillDefinition {
        let conformance = default_skill_conformance();
        compile_skill_definition(CompileSkillInput {
            owner: "workspace".to_owned(),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: "desc".to_owned(),
            body: "body".to_owned(),
            source_kind: SkillSourceKind::User,
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: format!("fp-{slug}"),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({}),
            conformance,
        })
    }

    fn turn_skill_resolution(
        active: Vec<ResolvedSkill>,
        excluded: Vec<ExcludedSkill>,
    ) -> super::skills::TurnSkillResolution {
        super::skills::TurnSkillResolution {
            prompt: String::new(),
            result: SkillResolutionResult { active, excluded },
            runtime_plan: SkillRuntimePlan {
                tools: Vec::new(),
                read_skill_index: HashMap::new(),
                excluded_tools: Vec::new(),
            },
            audit_events: Vec::new(),
        }
    }

    fn mcp_server_capability(id: &str, name: &str) -> TurnCapability {
        TurnCapability {
            id: id.to_owned(),
            label: Some(name.trim().to_owned()),
            kind: TurnCapabilityKind::McpServer {
                name: name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    fn mcp_tool_capability(id: &str, server_name: &str, raw_tool_name: &str) -> TurnCapability {
        TurnCapability {
            id: id.to_owned(),
            label: Some(format!("{}/{}", server_name.trim(), raw_tool_name.trim())),
            kind: TurnCapabilityKind::McpTool {
                server_name: server_name.to_owned(),
                raw_tool_name: raw_tool_name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    #[test]
    fn normalize_turn_capabilities_rejects_malformed_inputs() {
        let normalized = normalize_turn_capabilities(&[TurnCapability {
            id: String::new(),
            label: Some("bad".to_owned()),
            kind: TurnCapabilityKind::McpTool {
                server_name: "browser".to_owned(),
                raw_tool_name: "open".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }]);

        assert!(normalized.skill_refs.is_empty());
        assert!(normalized.mcp_tool_refs.is_empty());
        assert_eq!(normalized.rejected.len(), 1);
        assert_eq!(
            normalized.rejected[0].reason,
            TurnCapabilityRejectedReason::InvalidInput
        );
        assert!(normalized.rejected[0].message.contains("missing an id"));
    }

    #[test]
    fn normalize_turn_capabilities_deduplicates_by_canonical_key() {
        let normalized = normalize_turn_capabilities(&[
            skill_capability("skill:user:docs-a", "docs"),
            skill_capability("skill:user:docs-b", "docs"),
        ]);

        assert_eq!(normalized.skill_refs.len(), 1);
        assert_eq!(normalized.skill_refs[0].capability_id, "skill:user:docs-a");
        assert_eq!(normalized.rejected.len(), 1);
        assert_eq!(normalized.rejected[0].id, "skill:user:docs-b");
        assert_eq!(
            normalized.rejected[0].reason,
            TurnCapabilityRejectedReason::Duplicate
        );
    }

    #[test]
    fn normalize_turn_capabilities_splits_skill_server_and_tool_refs() {
        let normalized = normalize_turn_capabilities(&[
            skill_capability("skill:user:docs", "docs"),
            mcp_server_capability("mcp-server:workspace:browser", " browser "),
            mcp_tool_capability("mcp-tool:workspace:browser:open", " browser ", " open "),
        ]);

        assert_eq!(normalized.rejected, Vec::new());

        assert_eq!(normalized.skill_refs.len(), 1);
        assert_eq!(normalized.skill_refs[0].capability_id, "skill:user:docs");
        assert_eq!(normalized.skill_refs[0].slug, "docs");
        assert_eq!(normalized.skill_refs[0].source_kind, "user");

        assert_eq!(normalized.mcp_server_refs.len(), 1);
        assert_eq!(
            normalized.mcp_server_refs[0].capability_id,
            "mcp-server:workspace:browser"
        );
        assert_eq!(normalized.mcp_server_refs[0].name, "browser");
        assert_eq!(
            normalized.mcp_server_refs[0].scope_kind,
            McpScopeKind::Workspace
        );

        assert_eq!(normalized.mcp_tool_refs.len(), 1);
        assert_eq!(
            normalized.mcp_tool_refs[0].capability_id,
            "mcp-tool:workspace:browser:open"
        );
        assert_eq!(normalized.mcp_tool_refs[0].server_name, "browser");
        assert_eq!(normalized.mcp_tool_refs[0].raw_tool_name, "open");
        assert_eq!(
            normalized.mcp_tool_refs[0].scope_kind,
            McpScopeKind::Workspace
        );
    }

    #[test]
    fn resolve_skill_capability_summary_accepts_explicit_skill_with_stable_reason() {
        let explicit_ref = explicit_skill_ref("skill:user:docs", "docs");
        let resolution = turn_skill_resolution(
            vec![ResolvedSkill {
                slug: "workspace/docs".to_owned(),
                reason: SkillResolvedReason::ExplicitCapability,
                definition: test_skill_definition("docs"),
            }],
            Vec::new(),
        );

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.rejected.is_empty());
        assert_eq!(summary.accepted.len(), 1);
        assert_eq!(summary.accepted[0].id, "skill:user:docs");
        assert_eq!(summary.accepted[0].label.as_deref(), Some("docs"));
        assert_eq!(
            summary.accepted[0].reason,
            TurnCapabilityAcceptedReason::ExplicitComposerCapability
        );
        assert_eq!(
            summary.accepted[0].kind,
            TurnCapabilityKind::Skill {
                slug: "docs".to_owned(),
                source_kind: "user".to_owned()
            }
        );
    }

    #[test]
    fn resolve_skill_capability_summary_rejects_missing_skill() {
        let explicit_ref = explicit_skill_ref("skill:user:missing", "missing");
        let resolution = turn_skill_resolution(Vec::new(), Vec::new());

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.accepted.is_empty());
        assert_eq!(summary.rejected.len(), 1);
        assert_eq!(summary.rejected[0].id, "skill:user:missing");
        assert_eq!(
            summary.rejected[0].reason,
            TurnCapabilityRejectedReason::NotFound
        );
        assert!(
            summary.rejected[0]
                .message
                .contains("not installed or not available")
        );
    }

    #[test]
    fn resolve_skill_capability_summary_rejects_disabled_skill() {
        let explicit_ref = explicit_skill_ref("skill:user:docs", "docs");
        let resolution = turn_skill_resolution(
            Vec::new(),
            vec![ExcludedSkill {
                slug: "workspace/docs".to_owned(),
                source_kind: "user".to_owned(),
                reason: SkillExcludedReason::DisabledByPolicy,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            }],
        );

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.accepted.is_empty());
        assert_eq!(summary.rejected.len(), 1);
        assert_eq!(summary.rejected[0].id, "skill:user:docs");
        assert_eq!(
            summary.rejected[0].reason,
            TurnCapabilityRejectedReason::DisabledByPolicy
        );
    }

    #[test]
    fn resolve_skill_capability_summary_rejects_security_blocked_skill() {
        let explicit_ref = explicit_skill_ref("skill:user:docs", "docs");
        let resolution = turn_skill_resolution(
            Vec::new(),
            vec![ExcludedSkill {
                slug: "workspace/docs".to_owned(),
                source_kind: "user".to_owned(),
                reason: SkillExcludedReason::SecurityBlocked,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            }],
        );

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.accepted.is_empty());
        assert_eq!(summary.rejected.len(), 1);
        assert_eq!(summary.rejected[0].id, "skill:user:docs");
        assert_eq!(
            summary.rejected[0].reason,
            TurnCapabilityRejectedReason::SecurityBlocked
        );
    }

    fn task_result(tool_name: &str, success: bool, text: &str) -> ExecutedToolResult {
        ExecutedToolResult {
            item_id: "item_1234567890123456".to_owned(),
            item_type: TurnItemType::DynamicToolCall,
            attempt_number: 1,
            tool_name: tool_name.to_owned(),
            arguments: "{}".to_owned(),
            model_visible_text: text.to_owned(),
            success,
            outcome: if success {
                ToolOutcome::ok()
            } else {
                ToolOutcome::fatal(ToolErrorClass::InvalidArguments, Some(text.to_owned()))
            },
            recovery_view: None,
            request_tools_result: None,
            message: ChatMessage::tool_result("item_1234567890123456", tool_name, text),
        }
    }

    fn request_tools_executed_result(result: RequestToolsResult) -> ExecutedToolResult {
        let text = serde_json::to_string(&result).expect("request_tools result serializes");
        let mut executed = task_result("request_tools", true, text.as_str());
        executed.request_tools_result = Some(result);
        executed
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
            runtime_home_dir: std::env::temp_dir().join("pioneer-agent-request-tools-tests"),
            artifacts_subdir: "tools/computer_use".to_owned(),
            retention_hours: 24,
            max_total_bytes: 1024 * 1024 * 1024,
            run_max_steps_default: 30,
            ..ComputerUseToolsConfig::default()
        }
    }

    fn configured_test_tool(name: &str) -> ConfiguredToolSpec {
        ConfiguredToolSpec::new(
            ToolSpec::new(
                name,
                "test domain tool",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        )
    }

    fn build_tools_with_extension_names(names: &[&str]) -> pioneer_tools::BuiltinTools {
        pioneer_tools::build_tools(
            ".",
            "turn_request_tools_visibility",
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle {
                specs: names
                    .iter()
                    .filter(|name| **name != "computer_use")
                    .map(|name| configured_test_tool(name))
                    .collect(),
                handlers: names
                    .iter()
                    .filter(|name| **name != "computer_use")
                    .map(|name| {
                        (
                            (*name).to_owned(),
                            std::sync::Arc::new(NoopToolHandler) as std::sync::Arc<dyn ToolHandler>,
                        )
                    })
                    .collect(),
            }],
        )
        .expect("test tools must build")
    }

    fn build_tools_with_extension_specs(
        specs: Vec<ConfiguredToolSpec>,
    ) -> pioneer_tools::BuiltinTools {
        let specs = specs
            .into_iter()
            .filter(|configured| configured.spec.name != "computer_use")
            .collect::<Vec<_>>();
        let handlers = specs
            .iter()
            .map(|configured| {
                (
                    configured.spec.name.clone(),
                    std::sync::Arc::new(NoopToolHandler) as std::sync::Arc<dyn ToolHandler>,
                )
            })
            .collect();
        pioneer_tools::build_tools(
            ".",
            "turn_schema_token_guard",
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle { specs, handlers }],
        )
        .expect("test tools must build")
    }

    fn all_lazy_domain_tool_names() -> Vec<&'static str> {
        pioneer_tools::builtin_tool_domain_map()
            .iter()
            .flat_map(|(_, tool_names)| tool_names.iter().copied())
            .collect()
    }

    fn expected_visible(core_tools: &[String], extra: &[&str]) -> Vec<String> {
        core_tools
            .iter()
            .cloned()
            .chain(extra.iter().map(|name| (*name).to_owned()))
            .collect()
    }

    fn assert_no_discovery_tools(visible_tools: &[String]) {
        assert!(!visible_tools.iter().any(|name| name == "tool_search"));
        assert!(!visible_tools.iter().any(|name| name == "tool_suggest"));
    }

    fn assert_lazy_domain_tools_hidden_except(visible_tools: &[String], allowed: &[&str]) {
        let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
        for name in all_lazy_domain_tool_names() {
            if !allowed.contains(name) {
                assert!(
                    !visible_tools.iter().any(|visible| visible == name),
                    "lazy-domain tool `{name}` should stay hidden"
                );
            }
        }
    }

    fn heavy_hidden_domain_tool(name: &str) -> ConfiguredToolSpec {
        let mut properties = serde_json::Map::new();
        for index in 0..48 {
            properties.insert(
                format!("field_{index}"),
                serde_json::json!({
                    "type": "string",
                    "description": format!(
                        "Heavy hidden-domain schema field {index} for `{name}`. This fixture must stay out of ordinary provider requests."
                    )
                }),
            );
        }

        ConfiguredToolSpec::new(
            ToolSpec::new(
                name,
                "heavy hidden-domain test tool",
                serde_json::json!({
                    "type": "object",
                    "properties": properties,
                    "required": ["field_0"],
                    "additionalProperties": false
                }),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        )
    }

    fn serialized_tool_schema_bytes(specs: &[ToolSpec]) -> usize {
        serde_json::to_vec(specs)
            .expect("tool specs serialize")
            .len()
    }

    fn request_tools_result_for_added(
        domain: &str,
        tool_names: impl IntoIterator<Item = String>,
    ) -> RequestToolsResult {
        RequestToolsResult {
            added: BTreeMap::from([(domain.to_owned(), tool_names.into_iter().collect())]),
            already_visible: BTreeMap::new(),
            blocked: Vec::new(),
            unknown_or_unavailable: Vec::new(),
        }
    }

    #[tokio::test]
    async fn request_tools_control_result_reports_unavailable_without_schema_leak() {
        let built = pioneer_tools::build_builtin_tools(
            ".",
            "turn_agent_request_tools_result",
            test_web_config(),
            test_computer_use_config(),
        );
        built
            .router
            .set_model_visible_tools(&["request_tools".to_owned()])
            .await;

        let call = built
            .router
            .build_model_tool_call(pioneer_tools::RawToolCall {
                call_id: "call_agent_request_tools".to_owned(),
                tool_name: "request_tools".to_owned(),
                arguments: serde_json::json!({
                    "domains": ["memory", "memory"],
                    "reason": "Need memory tools."
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
        let output = serde_json::from_value::<RequestToolsResult>(result.raw_output_json())
            .expect("request_tools output should match result contract");

        assert!(output.added.is_empty());
        assert_eq!(output.unknown_or_unavailable.len(), 1);
        assert_eq!(output.unknown_or_unavailable[0].domain, "memory");
        assert_eq!(
            output.unknown_or_unavailable[0].tools,
            BuiltinToolDomain::Memory
                .tool_names()
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        );

        let text = result.model_visible_text();
        assert!(text.contains("\"unknownOrUnavailable\""));
        assert!(!text.contains("\"parameters\""));
        assert!(!text.contains("\"properties\""));
        assert!(!text.contains("\"additionalProperties\""));
    }

    #[tokio::test]
    async fn request_tools_visibility_expansion_exposes_artifact_next_round() {
        let built = build_tools_with_extension_names(&[
            "artifact_prepare",
            "artifact_register",
            "skill.test.dynamic",
        ]);
        let mut visible_tool_names = vec![
            "request_tools".to_owned(),
            "read_file".to_owned(),
            "skill.test.dynamic".to_owned(),
        ];
        let before = visible_tool_names.clone();
        let result = request_tools_result_for_added(
            "artifact",
            ["artifact_prepare", "artifact_register"]
                .into_iter()
                .map(str::to_owned),
        );

        let added = apply_request_tools_visibility_expansion(
            &mut visible_tool_names,
            &result,
            built.router.as_ref(),
        );
        built
            .router
            .set_model_visible_tools(&visible_tool_names)
            .await;
        let visible = built
            .router
            .model_visible_specs()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();

        assert_eq!(added, vec!["artifact_prepare", "artifact_register"]);
        for name in before {
            assert!(
                visible.contains(&name),
                "previously visible tool `{name}` must remain visible"
            );
        }
        assert!(visible.contains(&"artifact_prepare".to_owned()));
        assert!(visible.contains(&"artifact_register".to_owned()));
        assert!(!visible.contains(&"web_search".to_owned()));
    }

    #[tokio::test]
    async fn request_tools_visibility_expansion_exposes_all_task_tools_next_round() {
        let task_tools = BuiltinToolDomain::Task.tool_names();
        let built = build_tools_with_extension_names(task_tools);
        let mut visible_tool_names = vec!["request_tools".to_owned(), "read_file".to_owned()];
        let result = request_tools_result_for_added(
            "task",
            task_tools.iter().map(|name| (*name).to_owned()),
        );

        apply_request_tools_visibility_expansion(
            &mut visible_tool_names,
            &result,
            built.router.as_ref(),
        );
        built
            .router
            .set_model_visible_tools(&visible_tool_names)
            .await;
        let visible = built
            .router
            .model_visible_specs()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();

        assert!(visible.contains(&"request_tools".to_owned()));
        assert!(visible.contains(&"read_file".to_owned()));
        for name in task_tools {
            assert!(
                visible.contains(&(*name).to_owned()),
                "task domain tool `{name}` must be visible next round"
            );
        }
    }

    #[tokio::test]
    async fn phase_11_review_required_visibility_exposes_only_allowed_review_tools() {
        let built = build_tools_with_extension_names(&[
            "task_get",
            "task_wait",
            "task_accept",
            "task_revise",
            "task_cancel",
        ]);
        let mut no_review_visible = vec![
            "request_tools".to_owned(),
            "task_accept".to_owned(),
            "task_revise".to_owned(),
            "task_cancel".to_owned(),
        ];
        let removed_without_review =
            sync_review_action_tools_to_observations(&mut no_review_visible, &[]);
        assert_eq!(
            removed_without_review,
            vec!["task_accept".to_owned(), "task_revise".to_owned()]
        );
        assert!(no_review_visible.contains(&"task_cancel".to_owned()));

        let mut visible_tool_names = vec![
            "request_tools".to_owned(),
            "read_file".to_owned(),
            "task_revise".to_owned(),
        ];
        let observations = vec![ReviewRequiredTaskObservation {
            task_id: "task_review_visibility".to_owned(),
            run_id: "run_review_visibility".to_owned(),
            candidate_id: "candidate_review_visibility".to_owned(),
            title: "Review visibility".to_owned(),
            status: "waiting_review".to_owned(),
            candidate_status: "pending_review".to_owned(),
            round: 2,
            summary: None,
            result_preview: None,
            extraction_error_preview: None,
            diagnostics: Vec::new(),
            child_thread_id: None,
            child_turn_id: None,
            max_revision_rounds: 2,
            remaining_revision_rounds: 0,
            allowed_actions: vec!["task_accept".to_owned(), "task_cancel".to_owned()],
            revision_blocked_reason: Some("max_revision_rounds_reached".to_owned()),
        }];

        let removed = sync_review_action_tools_to_observations(
            &mut visible_tool_names,
            observations.as_slice(),
        );
        assert_eq!(removed, vec!["task_revise".to_owned()]);
        let added = apply_review_required_tools_to_visible_tools(
            &mut visible_tool_names,
            observations.as_slice(),
            built.router.as_ref(),
        );
        built
            .router
            .set_model_visible_tools(&visible_tool_names)
            .await;
        let visible = built
            .router
            .model_visible_specs()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();

        assert_eq!(
            added,
            vec![
                "task_accept".to_owned(),
                "task_cancel".to_owned(),
                "task_get".to_owned(),
                "task_wait".to_owned()
            ]
        );
        assert!(visible.contains(&"request_tools".to_owned()));
        assert!(visible.contains(&"read_file".to_owned()));
        assert!(visible.contains(&"task_accept".to_owned()));
        assert!(visible.contains(&"task_cancel".to_owned()));
        assert!(visible.contains(&"task_get".to_owned()));
        assert!(visible.contains(&"task_wait".to_owned()));
        assert!(!visible.contains(&"task_revise".to_owned()));
    }

    #[tokio::test]
    async fn request_tools_visibility_expansion_exposes_all_memory_tools_next_round() {
        let memory_tools = BuiltinToolDomain::Memory.tool_names();
        let built = build_tools_with_extension_names(memory_tools);
        let mut visible_tool_names = vec!["request_tools".to_owned(), "read_file".to_owned()];
        let result = request_tools_result_for_added(
            "memory",
            memory_tools.iter().map(|name| (*name).to_owned()),
        );

        let added = apply_request_tools_visibility_expansion(
            &mut visible_tool_names,
            &result,
            built.router.as_ref(),
        );
        built
            .router
            .set_model_visible_tools(&visible_tool_names)
            .await;
        let visible = built
            .router
            .model_visible_specs()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();

        assert_eq!(
            added,
            memory_tools
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        );
        for name in memory_tools {
            assert!(
                visible.contains(&(*name).to_owned()),
                "memory domain tool `{name}` must be visible next round"
            );
        }
    }

    #[test]
    fn request_tools_visibility_expansion_exposes_computer_use_only_when_registered() {
        let built = pioneer_tools::build_builtin_tools(
            ".",
            "turn_request_tools_computer_use_visibility",
            test_web_config(),
            test_computer_use_config(),
        );
        let mut visible_tool_names = vec!["request_tools".to_owned(), "read_file".to_owned()];
        let result = request_tools_result_for_added(
            "computer_use",
            BuiltinToolDomain::ComputerUse
                .tool_names()
                .iter()
                .map(|name| (*name).to_owned()),
        );

        let added = apply_request_tools_visibility_expansion(
            &mut visible_tool_names,
            &result,
            built.router.as_ref(),
        );

        if built.router.find_spec("computer_use").is_some() {
            assert_eq!(added, vec!["computer_use".to_owned()]);
            assert!(visible_tool_names.contains(&"computer_use".to_owned()));
        } else {
            assert!(added.is_empty());
            assert!(!visible_tool_names.contains(&"computer_use".to_owned()));
        }
        assert!(visible_tool_names.contains(&"request_tools".to_owned()));
        assert!(visible_tool_names.contains(&"read_file".to_owned()));
    }

    #[test]
    fn tool_visibility_computer_use_is_hidden_by_default_and_visible_when_selected() {
        let built = pioneer_tools::build_builtin_tools(
            ".",
            "turn_tool_visibility_computer_use",
            test_web_config(),
            test_computer_use_config(),
        );
        let core_tools = vec!["request_tools".to_owned(), "read_file".to_owned()];

        let core_only = built
            .router
            .compute_final_visible_tools(&core_tools, &[], &[]);
        assert!(
            !core_only.visible_tools.contains(&"computer_use".to_owned()),
            "computer_use must not be visible by default"
        );

        let selected = built.router.compute_final_visible_tools(
            &core_tools,
            &["computer_use".to_owned()],
            &[],
        );
        if built.router.find_spec("computer_use").is_some() {
            assert!(
                selected.visible_tools.contains(&"computer_use".to_owned()),
                "registered computer_use must become visible when preflight selects it"
            );
        } else {
            assert!(
                !selected.visible_tools.contains(&"computer_use".to_owned()),
                "unregistered computer_use must not become visible"
            );
            assert!(selected.diagnostics.iter().any(|diagnostic| {
                diagnostic.tool_name == "computer_use"
                    && diagnostic.code
                        == pioneer_tools::ToolVisibilityDiagnosticCode::UnknownToolDropped
            }));
        }
    }

    #[test]
    fn request_tools_provider_loop_result_batch_expands_visible_tools_for_next_round() {
        let built = build_tools_with_extension_names(&["artifact_prepare", "artifact_register"]);
        let mut visible_tool_names = vec!["request_tools".to_owned(), "read_file".to_owned()];
        let request_tools_result = request_tools_result_for_added(
            "artifact",
            ["artifact_prepare", "artifact_register"]
                .into_iter()
                .map(str::to_owned),
        );
        let executed_results = vec![
            task_result("list_dir", true, "ok"),
            request_tools_executed_result(request_tools_result),
        ];

        let added = apply_request_tools_results_to_visible_tools(
            &mut visible_tool_names,
            &executed_results,
            built.router.as_ref(),
        );

        assert_eq!(added, vec!["artifact_prepare", "artifact_register"]);
        assert!(visible_tool_names.contains(&"request_tools".to_owned()));
        assert!(visible_tool_names.contains(&"read_file".to_owned()));
        assert!(visible_tool_names.contains(&"artifact_prepare".to_owned()));
        assert!(visible_tool_names.contains(&"artifact_register".to_owned()));

        let added_again = apply_request_tools_results_to_visible_tools(
            &mut visible_tool_names,
            &executed_results,
            built.router.as_ref(),
        );
        assert!(added_again.is_empty());
        assert_eq!(
            visible_tool_names
                .iter()
                .filter(|name| name.as_str() == "artifact_prepare")
                .count(),
            1
        );
    }

    #[test]
    fn request_tools_visibility_expansion_is_monotonic_and_registered_only() {
        let built = build_tools_with_extension_names(&["artifact_prepare"]);
        let mut visible_tool_names = vec!["request_tools".to_owned(), "read_file".to_owned()];
        let before = visible_tool_names.clone();
        let result = request_tools_result_for_added(
            "artifact",
            ["artifact_prepare", "artifact_register"]
                .into_iter()
                .map(str::to_owned),
        );

        let added = apply_request_tools_visibility_expansion(
            &mut visible_tool_names,
            &result,
            built.router.as_ref(),
        );

        assert_eq!(added, vec!["artifact_prepare"]);
        for name in before {
            assert!(visible_tool_names.contains(&name));
        }
        assert!(visible_tool_names.contains(&"artifact_prepare".to_owned()));
        assert!(!visible_tool_names.contains(&"artifact_register".to_owned()));
    }

    #[test]
    fn tool_visibility_core_only_hides_materialized_domain_tools() {
        let built = build_tools_with_extension_names(&[
            "memory_search",
            "memory_get",
            "task_create",
            "skill.test.dynamic",
        ]);

        let visibility = built.router.compute_final_visible_tools(
            &["exec_command".to_owned(), "request_tools".to_owned()],
            &[],
            &[],
        );

        assert_eq!(
            visibility.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "skill.test.dynamic".to_owned()
            ]
        );
        assert!(
            !visibility
                .visible_tools
                .contains(&"memory_search".to_owned())
        );
        assert!(!visibility.visible_tools.contains(&"memory_get".to_owned()));
        assert!(!visibility.visible_tools.contains(&"task_create".to_owned()));
    }

    #[test]
    fn tool_visibility_adds_requested_optional_tools_only_when_registered() {
        let built = build_tools_with_extension_names(&["memory_search", "memory_get"]);

        let visibility = built.router.compute_final_visible_tools(
            &["exec_command".to_owned(), "request_tools".to_owned()],
            &[
                "memory_search".to_owned(),
                "memory_get".to_owned(),
                "memory_forget".to_owned(),
            ],
            &[],
        );

        assert_eq!(
            visibility.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_search".to_owned(),
                "memory_get".to_owned()
            ]
        );
        assert!(visibility.diagnostics.iter().any(|diagnostic| {
            diagnostic.tool_name == "memory_forget"
                && diagnostic.code
                    == pioneer_tools::ToolVisibilityDiagnosticCode::UnknownToolDropped
        }));
    }

    #[test]
    fn tool_visibility_preserves_phase03_current_turn_expansion() {
        let built = build_tools_with_extension_names(&[
            "artifact_prepare",
            "artifact_register",
            "memory_search",
        ]);

        let visibility = built.router.compute_final_visible_tools(
            &["request_tools".to_owned(), "read_file".to_owned()],
            &["memory_search".to_owned()],
            &[
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
            ],
        );

        assert_eq!(
            visibility.visible_tools,
            vec![
                "request_tools".to_owned(),
                "read_file".to_owned(),
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "memory_search".to_owned()
            ]
        );
    }

    #[test]
    fn tool_visibility_hidden_task_turn_requires_preflight_or_current_state() {
        let task_tools = BuiltinToolDomain::Task.tool_names();
        let built = build_tools_with_extension_names(task_tools);

        let core_only = built.router.compute_final_visible_tools(
            &["request_tools".to_owned(), "read_file".to_owned()],
            &[],
            &[],
        );
        for name in task_tools {
            assert!(
                !core_only.visible_tools.contains(&(*name).to_owned()),
                "task tool `{name}` must not be visible by default"
            );
        }

        let selected = built.router.compute_final_visible_tools(
            &["request_tools".to_owned(), "read_file".to_owned()],
            &["task_create".to_owned()],
            &[],
        );
        assert!(selected.visible_tools.contains(&"task_create".to_owned()));
        assert!(!selected.visible_tools.contains(&"task_wait".to_owned()));

        let preserved = built.router.compute_final_visible_tools(
            &["request_tools".to_owned(), "read_file".to_owned()],
            &[],
            &["task_create".to_owned(), "task_wait".to_owned()],
        );
        assert!(preserved.visible_tools.contains(&"task_create".to_owned()));
        assert!(preserved.visible_tools.contains(&"task_wait".to_owned()));
    }

    #[test]
    fn tool_visibility_domain_map_excludes_dynamic_and_control_tools() {
        let mapped = pioneer_tools::builtin_tool_domain_map()
            .iter()
            .flat_map(|(_, names)| names.iter().copied())
            .collect::<Vec<_>>();

        assert!(!mapped.contains(&"request_tools"));
        assert!(!mapped.contains(&"read_skill"));
        assert!(!mapped.iter().any(|name| name.starts_with("skill.")));
        assert!(!mapped.iter().any(|name| name.starts_with("mcp_")));
        assert_eq!(
            BuiltinToolDomain::Task.tool_names(),
            [
                "task_create",
                "task_wait",
                "task_accept",
                "task_revise",
                "task_cancel",
                "task_update",
                "task_detach",
                "task_list",
                "task_get",
                "task_reschedule",
                "task_pause",
                "task_resume",
            ]
        );
    }

    #[tokio::test]
    async fn tool_visibility_matrix_canonical_turn_types_and_fallbacks() {
        let lazy_domain_tools = all_lazy_domain_tool_names();
        let built = build_tools_with_extension_names(&lazy_domain_tools);
        let core_tools = built.router.preflight_tool_index().core_tools;
        let computer_use_expected = if built.router.has_handler("computer_use") {
            vec!["computer_use"]
        } else {
            Vec::new()
        };

        let mut cases = vec![
            (
                "core only ordinary q-and-a",
                "что такое Rust ownership?",
                Vec::<&str>::new(),
                Vec::<&str>::new(),
            ),
            (
                "identity lookup uses memory read tools",
                "как меня зовут?",
                vec!["memory_search", "memory_get"],
                vec!["memory_search", "memory_get"],
            ),
            (
                "explicit remember uses memory mutation tool",
                "persist the current user name as durable memory",
                vec!["memory_remember"],
                vec!["memory_remember"],
            ),
            (
                "artifact creation uses artifact tools",
                "создай отчет в docx",
                vec!["artifact_prepare", "artifact_register"],
                vec!["artifact_prepare", "artifact_register"],
            ),
            (
                "task creation uses task_create",
                "запусти задачу завтра",
                vec!["task_create"],
                vec!["task_create"],
            ),
        ];
        cases.push((
            "computer operation uses computer_use",
            "открой браузер и проверь сайт",
            vec!["computer_use"],
            computer_use_expected,
        ));

        for (label, _user_text, selected, expected_extra) in cases {
            let selected = selected.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let visibility = built
                .router
                .compute_final_visible_tools(&core_tools, &selected, &[]);

            assert_eq!(
                visibility.visible_tools,
                expected_visible(&core_tools, &expected_extra),
                "{label}"
            );
            assert_lazy_domain_tools_hidden_except(&visibility.visible_tools, &expected_extra);
            assert_no_discovery_tools(&visibility.visible_tools);

            built
                .router
                .set_model_visible_tools(&visibility.visible_tools)
                .await;
            let provider_tool_names = built
                .router
                .model_visible_specs()
                .await
                .into_iter()
                .map(|spec| spec.name)
                .collect::<Vec<_>>();
            assert_eq!(provider_tool_names, visibility.visible_tools, "{label}");
        }

        let provider_final_failure =
            built
                .router
                .compute_final_visible_tools(&core_tools, &[], &[]);
        assert_eq!(provider_final_failure.visible_tools, core_tools);
        assert_lazy_domain_tools_hidden_except(&provider_final_failure.visible_tools, &[]);
        assert!(
            provider_final_failure
                .visible_tools
                .contains(&"request_tools".to_owned())
        );
        assert_no_discovery_tools(&provider_final_failure.visible_tools);
    }

    #[test]
    fn tool_visibility_matrix_dynamic_tools_hidden_tasks_and_phase03_expansion() {
        let mut extension_names = all_lazy_domain_tool_names();
        extension_names.extend([
            "skill.file_editor",
            "skill.subtask_planner",
            "mcp.filesystem.write",
            "mcp.workspace.read",
        ]);
        let built = build_tools_with_extension_names(&extension_names);
        let core_tools = built.router.preflight_tool_index().core_tools;

        let hidden_file_editing_task =
            built
                .router
                .compute_final_visible_tools(&core_tools, &[], &[]);
        for dynamic in [
            "skill.file_editor",
            "skill.subtask_planner",
            "mcp.filesystem.write",
            "mcp.workspace.read",
        ] {
            assert!(
                hidden_file_editing_task
                    .visible_tools
                    .contains(&dynamic.to_owned()),
                "materialized dynamic tool `{dynamic}` must not depend on preflight"
            );
        }
        assert_lazy_domain_tools_hidden_except(&hidden_file_editing_task.visible_tools, &[]);
        assert_no_discovery_tools(&hidden_file_editing_task.visible_tools);

        let hidden_subtask_creating_task = built.router.compute_final_visible_tools(
            &core_tools,
            &["task_create".to_owned(), "task_wait".to_owned()],
            &[],
        );
        assert!(
            hidden_subtask_creating_task
                .visible_tools
                .contains(&"task_create".to_owned())
        );
        assert!(
            hidden_subtask_creating_task
                .visible_tools
                .contains(&"task_wait".to_owned())
        );
        assert!(
            !hidden_subtask_creating_task
                .visible_tools
                .contains(&"task_cancel".to_owned())
        );
        assert_no_discovery_tools(&hidden_subtask_creating_task.visible_tools);

        let phase03_expanded_artifacts = built.router.compute_final_visible_tools(
            &core_tools,
            &[],
            &[
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
            ],
        );
        assert!(
            phase03_expanded_artifacts
                .visible_tools
                .contains(&"artifact_prepare".to_owned())
        );
        assert!(
            phase03_expanded_artifacts
                .visible_tools
                .contains(&"artifact_register".to_owned())
        );
        assert!(
            phase03_expanded_artifacts
                .visible_tools
                .contains(&"request_tools".to_owned())
        );
        assert_no_discovery_tools(&phase03_expanded_artifacts.visible_tools);
    }

    #[tokio::test]
    async fn token_schema_guard_core_only_request_excludes_heavy_lazy_domain_schemas() {
        let heavy_specs = all_lazy_domain_tool_names()
            .into_iter()
            .map(heavy_hidden_domain_tool)
            .collect::<Vec<_>>();
        let built = build_tools_with_extension_specs(heavy_specs);
        let core_tools = built.router.preflight_tool_index().core_tools;
        let visibility = built
            .router
            .compute_final_visible_tools(&core_tools, &[], &[]);
        built
            .router
            .set_model_visible_tools(&visibility.visible_tools)
            .await;
        let specs = built.router.model_visible_specs().await;
        let names = specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<Vec<_>>();

        assert!(names.contains(&"request_tools"));
        for name in all_lazy_domain_tool_names() {
            assert!(
                !names.contains(&name),
                "ordinary core-only turns must not include hidden `{name}` schema"
            );
        }

        let bytes = serialized_tool_schema_bytes(&specs);
        const CORE_ONLY_TOOL_SCHEMA_BYTES_LIMIT: usize = 45_000;
        assert!(
            bytes <= CORE_ONLY_TOOL_SCHEMA_BYTES_LIMIT,
            "core-only provider tool schemas are {bytes} bytes; limit {CORE_ONLY_TOOL_SCHEMA_BYTES_LIMIT} leaves margin for core tools while catching accidental all-domain schema leakage"
        );
    }

    #[tokio::test]
    async fn token_schema_guard_selected_phase03_and_dynamic_schemas_are_explicit() {
        let mut heavy_specs = all_lazy_domain_tool_names()
            .into_iter()
            .map(heavy_hidden_domain_tool)
            .collect::<Vec<_>>();
        heavy_specs.push(heavy_hidden_domain_tool("skill.report_builder"));
        heavy_specs.push(heavy_hidden_domain_tool("mcp.filesystem.read"));
        let built = build_tools_with_extension_specs(heavy_specs);
        let core_tools = built.router.preflight_tool_index().core_tools;

        let core_visibility = built
            .router
            .compute_final_visible_tools(&core_tools, &[], &[]);
        built
            .router
            .set_model_visible_tools(&core_visibility.visible_tools)
            .await;
        let core_specs = built.router.model_visible_specs().await;
        let core_bytes = serialized_tool_schema_bytes(&core_specs);
        let core_names = core_specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<Vec<_>>();
        assert!(core_names.contains(&"skill.report_builder"));
        assert!(core_names.contains(&"mcp.filesystem.read"));
        assert!(!core_names.contains(&"artifact_prepare"));

        let selected_artifact = built.router.compute_final_visible_tools(
            &core_tools,
            &[
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
            ],
            &[],
        );
        built
            .router
            .set_model_visible_tools(&selected_artifact.visible_tools)
            .await;
        let selected_specs = built.router.model_visible_specs().await;
        let selected_names = selected_specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<Vec<_>>();
        assert!(selected_names.contains(&"artifact_prepare"));
        assert!(selected_names.contains(&"artifact_register"));
        assert!(serialized_tool_schema_bytes(&selected_specs) > core_bytes);

        let phase03_expanded = built.router.compute_final_visible_tools(
            &core_tools,
            &[],
            &["task_create".to_owned(), "task_wait".to_owned()],
        );
        built
            .router
            .set_model_visible_tools(&phase03_expanded.visible_tools)
            .await;
        let phase03_names = built
            .router
            .model_visible_specs()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        assert!(phase03_names.contains(&"task_create".to_owned()));
        assert!(phase03_names.contains(&"task_wait".to_owned()));
        assert!(!phase03_names.contains(&"task_cancel".to_owned()));
    }

    #[test]
    fn task_mutation_finalization_guard_reports_failed_mutation_until_success() {
        let mut guard = TaskMutationFinalizationGuard::default();
        guard.observe(&task_result("task_create", false, "trigger must be object"));

        let message = guard
            .deterministic_failure_message()
            .expect("failed task mutation should block success finalization");
        assert!(message.contains("task_create"));
        assert!(message.contains("trigger must be object"));

        guard.observe(&task_result("task_create", true, "{\"taskId\":\"abc\"}"));
        assert!(guard.deterministic_failure_message().is_none());
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let root = std::env::temp_dir().join(format!(
            "pioneer_agent_chat_{name}_{}_{}",
            std::process::id(),
            now_nanos
        ));
        let _ = std::fs::remove_dir_all(root.as_path());
        std::fs::create_dir_all(root.as_path()).expect("create temp dir");
        root
    }

    fn image_part(path: &str, size_bytes: Option<u64>) -> MessageContentPart {
        MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: None,
            size_bytes,
            sha256: None,
            source: AttachmentDataSource::Path {
                path: path.to_owned(),
            },
            artifact: None,
        })
    }

    fn file_part(path: &str, size_bytes: Option<u64>) -> MessageContentPart {
        MessageContentPart::file(MessageAttachment {
            mime_type: "application/pdf".to_owned(),
            name: Some("file.pdf".to_owned()),
            size_bytes,
            sha256: None,
            source: AttachmentDataSource::Path {
                path: path.to_owned(),
            },
            artifact: None,
        })
    }

    fn computer_use_snapshot_message(call_id: &str, session_id: u64, path: &str) -> ChatMessage {
        ChatMessage {
            role: pioneer_provider::Role::Tool,
            content: serde_json::json!({
                "action": "snapshot",
                "session_id": session_id,
                "snapshot": { "path": path },
            })
            .to_string(),
            reasoning_content: None,
            content_parts: vec![image_part(path, Some(512 * 1024))],
            tool_call_id: Some(call_id.to_owned()),
            name: Some("computer_use".to_owned()),
            tool_calls: None,
        }
    }

    #[test]
    fn prompt_bundle_uses_runtime_home_root_not_workspace_root() {
        let workspace_root = temp_dir("workspace_root");
        let runtime_home = temp_dir("runtime_home");
        std::fs::write(workspace_root.join("SOUL.md"), "workspace soul")
            .expect("write workspace SOUL");
        std::fs::write(workspace_root.join("IDENTITY.md"), "workspace identity")
            .expect("write workspace IDENTITY");
        std::fs::write(runtime_home.join("SOUL.md"), "runtime soul").expect("write runtime SOUL");
        std::fs::write(runtime_home.join("IDENTITY.md"), "runtime identity")
            .expect("write runtime IDENTITY");

        let bundle = compile_agent_prompt_bundle_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            false,
            false,
            false,
            "thread_test",
            "turn_test",
        )
        .expect("compile prompt bundle");

        assert!(bundle.full_system_text.contains("runtime soul"));
        assert!(bundle.full_system_text.contains("runtime identity"));
        assert!(!bundle.full_system_text.contains("workspace soul"));
        assert!(!bundle.full_system_text.contains("workspace identity"));
    }

    #[test]
    fn agents_md_runtime_section_is_compiled_by_agent_prompt_bundle() {
        let runtime_home = temp_dir("agents_md_runtime_prompt");
        let agents_section = PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::AgentsMd),
            title: Some("AGENTS.md".to_owned()),
            content: "These instructions come from the effective AGENTS.md for this thread tree scope. System, developer, and explicit user messages take precedence.\n\n<AGENTS_MD>\nUse repo rules.\n</AGENTS_MD>".to_owned(),
            max_chars: Some(20_000),
            truncated: false,
        };

        let bundle = compile_agent_prompt_bundle_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[agents_section],
            false,
            false,
            false,
            "thread_agents_md",
            "turn_agents_md",
        )
        .expect("compile prompt bundle");

        let agents_section = bundle
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::AgentsMd)
            .expect("agents_md section should be compiled");
        assert_eq!(agents_section.title, "AGENTS.md");
        assert!(bundle.dynamic_system_text.contains("## AGENTS.md"));
        assert!(bundle.dynamic_system_text.contains("Use repo rules."));
    }

    #[test]
    fn request_tools_catalog_is_compiled_for_tool_enabled_prompt_bundle() {
        let runtime_home = temp_dir("request_tools_catalog_prompt");

        let bundle = compile_agent_prompt_bundle_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            false,
            true,
            false,
            "thread_request_tools_catalog",
            "turn_request_tools_catalog",
        )
        .expect("compile prompt bundle");

        let catalog_section = bundle
            .sections
            .iter()
            .find(|section| {
                section.id.manifest_id() == pioneer_promt::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID
            })
            .expect("request_tools catalog section should be compiled");
        assert_eq!(
            catalog_section.title,
            pioneer_promt::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE
        );
        assert!(bundle.dynamic_system_text.contains(
            "If you need a hidden domain and its tools are not currently visible, call request_tools"
        ));
        for (domain, tool_names) in pioneer_tools::builtin_tool_domain_map() {
            let expected = format!("- {}: {}.", domain.as_str(), tool_names.join(", "));
            assert!(
                bundle.dynamic_system_text.contains(expected.as_str()),
                "catalog missing domain line `{expected}`"
            );
        }
        assert!(!bundle.dynamic_system_text.contains("mcp_"));
        assert!(!bundle.dynamic_system_text.contains("mcp."));
        assert!(!bundle.dynamic_system_text.contains("skill."));
        assert!(!bundle.dynamic_system_text.contains("\"parameters\""));
        assert!(!bundle.dynamic_system_text.contains("\"properties\""));
        assert!(
            !bundle
                .dynamic_system_text
                .contains("\"additionalProperties\"")
        );
    }

    #[test]
    fn request_tools_catalog_is_omitted_from_no_tool_prompt_bundle() {
        let runtime_home = temp_dir("request_tools_catalog_no_tools");

        let bundle = compile_agent_prompt_bundle_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            false,
            false,
            false,
            "thread_no_request_tools_catalog",
            "turn_no_request_tools_catalog",
        )
        .expect("compile prompt bundle");

        assert!(
            !bundle
                .sections
                .iter()
                .any(|section| section.id.manifest_id()
                    == pioneer_promt::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
        );
        assert!(
            !bundle
                .dynamic_system_text
                .contains("Some tool domains and their tools are hidden until requested")
        );
    }

    #[test]
    fn recovery_context_reconstructs_tool_messages_in_sequence_order() {
        let mut messages = vec![ChatMessage::user("retry the turn")];

        append_recovered_tool_llm_context(
            &mut messages,
            vec![
                RetainedToolLlmContext {
                    item_id: "call_2".to_owned(),
                    tool_name: "read_file".to_owned(),
                    arguments: "{\"path\":\"/tmp/b\"}".to_owned(),
                    sequence: 2,
                    payload: serde_json::json!({
                        "kind": "json",
                        "value": {
                            "output": "SECOND_SENTINEL"
                        },
                        "truncated": false
                    }),
                },
                RetainedToolLlmContext {
                    item_id: "call_1".to_owned(),
                    tool_name: "read_file".to_owned(),
                    arguments: "{\"path\":\"/tmp/a\"}".to_owned(),
                    sequence: 1,
                    payload: serde_json::json!({
                        "kind": "json",
                        "value": {
                            "output": "FIRST_SENTINEL"
                        },
                        "truncated": false
                    }),
                },
            ],
        );

        assert_eq!(messages.len(), 5);
        assert_eq!(messages[1].tool_calls.as_ref().unwrap()[0].id, "call_1");
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("call_1"));
        assert!(messages[2].content.contains("FIRST_SENTINEL"));
        assert_eq!(messages[3].tool_calls.as_ref().unwrap()[0].id, "call_2");
        assert_eq!(messages[4].tool_call_id.as_deref(), Some("call_2"));
        assert!(messages[4].content.contains("SECOND_SENTINEL"));
    }

    #[test]
    fn agent_policy_keeps_latest_snapshot_per_session_and_preserves_user_files() {
        let mut messages = vec![
            ChatMessage {
                role: pioneer_provider::Role::User,
                content: "Analyze this file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/task.pdf", Some(1024 * 1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
            },
            computer_use_snapshot_message("call_1", 1, "/tmp/s1-1.png"),
            computer_use_snapshot_message("call_2", 2, "/tmp/s2-1.png"),
            computer_use_snapshot_message("call_3", 1, "/tmp/s1-2.png"),
        ];

        retain_agent_attachment_messages(&mut messages);

        assert_eq!(messages[0].content_parts.len(), 1);
        assert!(messages[1].content_parts.is_empty());
        assert_eq!(messages[2].content_parts.len(), 1);
        assert_eq!(messages[3].content_parts.len(), 1);
    }

    #[test]
    fn agent_policy_prunes_snapshots_before_pinned_files_when_budget_exceeded() {
        let mut messages = vec![
            ChatMessage {
                role: pioneer_provider::Role::User,
                content: "Keep this PDF pinned".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/pinned.pdf", Some(400))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
            },
            computer_use_snapshot_message("call_1", 1, "/tmp/s1-1.png"),
            computer_use_snapshot_message("call_2", 2, "/tmp/s2-1.png"),
            computer_use_snapshot_message("call_3", 1, "/tmp/s1-2.png"),
        ];

        retain_agent_attachment_messages_with_budget(&mut messages, 700_000);

        assert_eq!(messages[0].content_parts.len(), 1);
        assert!(messages[1].content_parts.is_empty());
        assert!(messages[2].content_parts.is_empty());
        assert_eq!(messages[3].content_parts.len(), 1);
    }

    #[test]
    fn chat_policy_keeps_only_latest_user_attachment_message() {
        let mut messages = vec![
            ChatMessage {
                role: pioneer_provider::Role::User,
                content: "old file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/old.pdf", Some(1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
            },
            ChatMessage {
                role: pioneer_provider::Role::Tool,
                content: "tool-1".to_owned(),
                reasoning_content: None,
                content_parts: vec![image_part("/tmp/snapshot.png", Some(1024))],
                tool_call_id: Some("call_1".to_owned()),
                name: Some("computer_use".to_owned()),
                tool_calls: None,
            },
            ChatMessage {
                role: pioneer_provider::Role::User,
                content: "new file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/new.pdf", Some(1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
            },
        ];

        retain_chat_mode_attachment_messages(&mut messages);

        assert!(messages[0].content_parts.is_empty());
        assert!(messages[1].content_parts.is_empty());
        assert_eq!(messages[2].content_parts.len(), 1);
    }

    #[test]
    fn chat_policy_no_attachment_messages_keeps_payload_untouched() {
        let mut messages = vec![
            ChatMessage::user("u"),
            ChatMessage::assistant("a"),
            ChatMessage::tool("t"),
        ];

        retain_chat_mode_attachment_messages(&mut messages);

        assert_eq!(messages.len(), 3);
        assert!(
            messages
                .iter()
                .all(|message| message.content_parts.is_empty())
        );
    }

    #[test]
    fn build_user_message_adds_content_parts_for_current_input_attachments() {
        let input = vec![
            UserInput::Text {
                text: "describe screenshot".to_owned(),
                text_elements: Vec::new(),
            },
            UserInput::Image {
                url: "https://example.com/ui.png?cache=1".to_owned(),
            },
            UserInput::LocalImage {
                path: "/tmp/local-shot.jpg".to_owned(),
            },
            UserInput::File {
                url: "https://example.com/doc.pdf".to_owned(),
            },
            UserInput::LocalFile {
                path: "/tmp/report.json".to_owned(),
            },
            UserInput::Audio {
                url: "https://example.com/voice.mp3".to_owned(),
            },
            UserInput::LocalAudio {
                path: "/tmp/voice.wav".to_owned(),
            },
            UserInput::Video {
                url: "https://example.com/demo.mp4".to_owned(),
            },
            UserInput::LocalVideo {
                path: "/tmp/demo.mov".to_owned(),
            },
        ];

        let message = build_user_message(input.as_slice(), &[]);

        assert_eq!(message.role, Role::User);
        assert_eq!(message.content, "describe screenshot");
        assert_eq!(message.content_parts.len(), 8);

        match &message.content_parts[0] {
            MessageContentPart::Image { image } => {
                assert_eq!(image.mime_type, "image/png");
                assert_eq!(
                    image.source,
                    AttachmentDataSource::Url {
                        url: "https://example.com/ui.png?cache=1".to_owned(),
                    }
                );
            }
            _ => panic!("expected url image content part"),
        }

        match &message.content_parts[1] {
            MessageContentPart::Image { image } => {
                assert_eq!(image.mime_type, "image/jpeg");
                assert_eq!(
                    image.source,
                    AttachmentDataSource::Path {
                        path: "/tmp/local-shot.jpg".to_owned(),
                    }
                );
            }
            _ => panic!("expected local image content part"),
        }

        match &message.content_parts[2] {
            MessageContentPart::File { file } => {
                assert_eq!(file.mime_type, "application/pdf");
                assert_eq!(
                    file.source,
                    AttachmentDataSource::Url {
                        url: "https://example.com/doc.pdf".to_owned(),
                    }
                );
            }
            _ => panic!("expected url file content part"),
        }

        match &message.content_parts[3] {
            MessageContentPart::File { file } => {
                assert_eq!(file.mime_type, "application/json");
                assert_eq!(
                    file.source,
                    AttachmentDataSource::Path {
                        path: "/tmp/report.json".to_owned(),
                    }
                );
            }
            _ => panic!("expected local file content part"),
        }

        match &message.content_parts[4] {
            MessageContentPart::Audio { audio } => {
                assert_eq!(audio.mime_type, "audio/mpeg");
                assert_eq!(
                    audio.source,
                    AttachmentDataSource::Url {
                        url: "https://example.com/voice.mp3".to_owned(),
                    }
                );
            }
            _ => panic!("expected url audio content part"),
        }

        match &message.content_parts[5] {
            MessageContentPart::Audio { audio } => {
                assert_eq!(audio.mime_type, "audio/wav");
                assert_eq!(
                    audio.source,
                    AttachmentDataSource::Path {
                        path: "/tmp/voice.wav".to_owned(),
                    }
                );
            }
            _ => panic!("expected local audio content part"),
        }

        match &message.content_parts[6] {
            MessageContentPart::Video { video } => {
                assert_eq!(video.mime_type, "video/mp4");
                assert_eq!(
                    video.source,
                    AttachmentDataSource::Url {
                        url: "https://example.com/demo.mp4".to_owned(),
                    }
                );
            }
            _ => panic!("expected url video content part"),
        }

        match &message.content_parts[7] {
            MessageContentPart::Video { video } => {
                assert_eq!(video.mime_type, "video/quicktime");
                assert_eq!(
                    video.source,
                    AttachmentDataSource::Path {
                        path: "/tmp/demo.mov".to_owned(),
                    }
                );
            }
            _ => panic!("expected local video content part"),
        }
    }

    #[test]
    fn build_user_message_adds_resolved_artifact_attachment() {
        let input = vec![
            UserInput::Text {
                text: "summarize".to_owned(),
                text_elements: Vec::new(),
            },
            UserInput::Artifact {
                artifact_id: "art_1".to_owned(),
                version_id: Some("av_1".to_owned()),
            },
        ];
        let resolved = vec![ResolvedArtifactInput {
            artifact_id: "art_1".to_owned(),
            version_id: Some("av_1".to_owned()),
            content_type: InputContentType::File,
            attachment: MessageAttachment {
                mime_type: "text/plain".to_owned(),
                name: Some("report.txt".to_owned()),
                size_bytes: Some(13),
                sha256: Some("a".repeat(64)),
                source: AttachmentDataSource::Path {
                    path: "/tmp/materialized/report.txt".to_owned(),
                },
                artifact: None,
            },
        }];

        let message = build_user_message(input.as_slice(), resolved.as_slice());

        assert_eq!(message.content, "summarize");
        assert_eq!(message.content_parts.len(), 1);
        match &message.content_parts[0] {
            MessageContentPart::File { file } => {
                assert_eq!(file.name.as_deref(), Some("report.txt"));
                assert_eq!(file.size_bytes, Some(13));
                assert!(matches!(file.source, AttachmentDataSource::Path { .. }));
            }
            other => panic!("expected file artifact part, got {other:?}"),
        }
    }

    #[test]
    fn build_user_message_text_only_remains_plain_user_message() {
        let input = vec![
            UserInput::Text {
                text: "first".to_owned(),
                text_elements: Vec::new(),
            },
            UserInput::Text {
                text: "second".to_owned(),
                text_elements: Vec::new(),
            },
        ];

        let message = build_user_message(input.as_slice(), &[]);

        assert_eq!(message.content, "first\nsecond");
        assert!(message.content_parts.is_empty());
    }
}
