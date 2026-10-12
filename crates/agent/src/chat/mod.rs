mod native_control;
pub(crate) mod preflight;
mod provider;
pub(crate) mod semantic_state;
mod skill_tools;
mod skills;
mod tool_recovery_policy;
mod tool_retry_lifecycle;
mod tooling;
mod validator;

pub(crate) use provider::classify_provider_failure_message;

use self::preflight::{
    TurnPreflightOrchestratorInput, TurnPreflightOrchestratorResult, TurnPreflightTurnInput,
    build_turn_preflight_diagnostics_snapshot, run_turn_preflight_orchestrator,
    trace_turn_preflight_diagnostics,
};
use self::tool_retry_lifecycle::{
    ToolRetryLifecycleTracker, emit_tool_loop_budget_exceeded, emit_tool_retry_drafts,
    turn_item_type_code,
};
use self::validator::no_tool_final_answer_rejection;
use crate::hooks::{
    AgentToolBundleArtifactStore, AgentTurnHookContext, AgentTurnPostTurnHookDispatch,
    AgentTurnPostTurnSummary, EffectiveTurnPolicySet, EffectiveTurnPromptContextSet,
    EffectiveTurnPromptManifestHookContributionKind, EffectiveTurnPromptManifestHookDiagnosticCode,
    EffectiveTurnPromptManifestHookMetadata, EffectiveTurnPromptManifestHookSource,
    EffectiveTurnPromptSectionSet, run_agent_turn_notification_hook_phase,
    run_agent_turn_policy_hook_phase, run_agent_turn_post_preflight_prompt_context_hook_phase,
    run_agent_turn_prompt_compile_hook_phase, run_agent_turn_prompt_context_hook_phase,
    run_agent_turn_tool_materialization_hook_phase, tool_bundle_contributions_from_bundles,
};
use crate::{
    AgentEventHub, AgentEventHubError, AgentMcpAvailability, AgentMcpMaterialization,
    AgentMcpMaterializationError, AgentMcpMaterializationFailureReason,
    AgentMcpMaterializationRequest, AgentMcpResolutionDiagnostic, AgentMcpServerRef,
    AgentMcpToolProvider, AgentMcpToolRef, AgentTurnHookRuntimeContext, ExecutionCheckpointContext,
    ExecutionWindowContinuation, PendingAttachedTask, ResolvedArtifactInput,
    RetainedProviderHistoryMessage, ReviewRequiredTaskObservation,
    SkillContinuationAuthorizationContext, TaskFinalizationSnapshot, TaskToolMaterialization,
    TaskToolProvider, TaskTurnContext, TerminalTaskObservation, ToolLoopConfig,
    TurnExecutionControl, TurnFinalizationContext, TurnFinalizationDecision,
    TurnFinalizationProvider, TurnToolContext, TurnToolMaterialization, TurnToolProvider,
};
use chrono::Local;
use futures_util::{StreamExt, stream};
use pioneer_hooks::{
    HookActorKind, HookPhase, HookRuntime, HookToolName, TurnPostPreflightPromptContextHookInput,
    TurnPostTurnDomain, TurnPostTurnDomainEventSummary, TurnPostTurnToolEventSummary,
    TurnPostTurnToolStatus, TurnPrePolicyHookInput, TurnPrePromptCompileHookInput,
    TurnPrePromptContextHookInput,
};
use pioneer_memory::hooks::{
    MemoryEpisodicRecallCapabilities, MemoryTurnContext, MemoryTurnPolicy,
    active_recall_thread_episodic_summary,
    build_active_recall_local_preflight_plan_with_thread_summary,
    deterministic_recall_context_summary, memory_turn_policy_from_hook_policy_set,
};
use pioneer_promt::{
    CompiledInstructionDeliveryPlan, CompiledPromptBundle, ExecutionContinuationRuntimeFactsInput,
    FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID, PromptCompileInput, PromptDiagnosticCode,
    PromptDynamicSectionId, PromptLimits, PromptProfile, PromptRuntimeBuiltInSectionId,
    PromptRuntimeSectionId, PromptRuntimeSectionInput, ToolRetryInstructionKind,
    compile_instruction_delivery_plan, compile_prompt, current_permission_guidance,
    execution_continuation_section_with_runtime_facts, render_tool_retry_instruction,
    runtime_sections_with_request_tools_catalog, tool_loop_final_answer_instruction,
};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, EXECUTION_CHECKPOINT_DEFAULT_TOOL_DETAIL_LIMIT,
    ExecutionCheckpointProviderBudgetInput, ExecutionCheckpointProviderBudgetSummary,
    ExecutionCheckpointToolNoProgressState, ExecutionCheckpointToolSummary,
    ExecutionCheckpointWindowSummary, ExecutionWindowExhaustionReason, ExecutionWindowStatus,
    ItemCompletedNotification, ItemDeltaNotification, ItemStartedNotification, PromptManifest,
    PromptManifestDiagnostic, PromptManifestDiagnosticCode, PromptManifestHookContributionKind,
    PromptManifestHookPhase, PromptManifestHookSource, PromptManifestHookSourceEntry,
    PromptManifestHookTruncation, PromptManifestProfile, ProviderFailureDetails,
    ProviderFailureStage, ProviderTransportKind, RecoveryAttemptContext, ThreadMode,
    ToolRecoveryPolicySnapshot, TurnAcceptedCapability, TurnCapability,
    TurnCapabilityAcceptedReason, TurnCapabilityKind, TurnCapabilityRejectedReason,
    TurnExecutionSecuritySnapshot, TurnExecutionWindowCheckpointedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification, TurnItem,
    TurnItemExecutionClass, TurnItemType, TurnPermissionProfileSnapshot, TurnRejectedCapability,
    UserInput, build_execution_checkpoint_original_request_summary,
    build_execution_checkpoint_payload, build_execution_checkpoint_provider_budget_summary,
    generate_id,
};
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, ChatRequest, CompiledPromptPayload,
    InputContentType, MessageAttachment, MessageContentPart, NativeFileToolCapability,
    NativePatchWireShape, Provider, ProviderCallIdentity, ProviderRegistry, ProviderTimeoutPolicy,
    ProviderToolCall, ReasoningConfig, ToolDefinition, infer_mime_from_reference,
};
use pioneer_skills::{
    ExcludedSkill, ResolvedSkill, SkillExcludedReason, SkillExplicitRef, SkillPolicyKey,
    SkillResolvedReason,
};
use pioneer_tools::{
    FilePolicyChecker, FilePolicyOperation, FinalToolVisibility, PermissionApprovalBroker,
    PermissionEvaluationContext, PreflightToolIndex, REQUEST_TOOLS_TOOL_NAME, RawToolCall,
    RequestToolsResult, ToolErrorClass, ToolEventPayload, ToolLoopBudgetExceeded,
    ToolLoopBudgetReason, ToolLoopGuard, ToolLoopGuardDecision, ToolLoopRoundAction,
    ToolNoProgressGuard, ToolNoProgressGuardConfig, ToolNoProgressPreflightDecision, ToolOutcome,
    ToolRecoveryView, ToolResultEnvelope, ToolResultView, ToolRetryController, ToolRetryDecision,
    ToolRetryObservation, build_tools_with_environment_and_security_snapshot, classify_tool_error,
};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

const TURN_ITEM_ID_LEN: usize = 21;
pub(super) const TURN_FINALIZATION_GENERATION: i64 = 1;

fn deterministic_final_message_item_id(turn_id: &str) -> String {
    let digest = Sha256::digest(
        format!("native-turn-finalization:{turn_id}:{TURN_FINALIZATION_GENERATION}").as_bytes(),
    );
    hex::encode(digest)[..TURN_ITEM_ID_LEN].to_owned()
}

fn deterministic_tool_item_id(
    turn_id: &str,
    round_id: &str,
    ordinal: u32,
    provider_call_id: &str,
) -> String {
    let digest = Sha256::digest(
        format!("native-provider-call:{turn_id}:{round_id}:{ordinal}:{provider_call_id}")
            .as_bytes(),
    );
    hex::encode(digest)[..TURN_ITEM_ID_LEN].to_owned()
}
const MAX_REVIEW_REQUIRED_TASK_OBSERVATIONS: usize = 20;
const MAX_TERMINAL_TASK_OBSERVATIONS: usize = 20;
const SKILL_TOOL_BUNDLE_PRIORITY: i32 = 400;
const MCP_TOOL_BUNDLE_PRIORITY: i32 = 300;
const TURN_TOOL_BUNDLE_PRIORITY: i32 = 250;
const TASK_TOOL_BUNDLE_PRIORITY: i32 = 200;
const MAX_CONSECUTIVE_REJECTED_NO_TOOL_ROUNDS: usize = 3;
const ATTACHED_TASK_FINALIZATION_WAIT_MS: u64 = 5_000;

fn readable_agent_skill_overlay(
    agent_skill_overlay: &[pioneer_skills::AgentSkillRuntimeEntry],
    provider_tool_calling: bool,
) -> &[pioneer_skills::AgentSkillRuntimeEntry] {
    if provider_tool_calling {
        agent_skill_overlay
    } else {
        &[]
    }
}

fn agent_skill_cards_have_read_path(
    agent_skill_overlay: &[pioneer_skills::AgentSkillRuntimeEntry],
    read_skill_materialized: bool,
) -> bool {
    agent_skill_overlay.is_empty() || read_skill_materialized
}

async fn finish_tool_event_forwarder(
    forwarder: JoinHandle<Result<(), AgentEventHubError>>,
) -> Result<(), ChatTurnError> {
    match forwarder.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(agent_event_error(error)),
        Err(error) => Err(ChatTurnError::Terminal(format!(
            "tool event forwarder failed before its durable lifecycle queue drained: {error}"
        ))),
    }
}

#[derive(Debug, Default, Clone)]
struct PendingToolUiState {
    tool_name: String,
    arguments: String,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    turn_item_execution_class: TurnItemExecutionClass,
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
    usage: Option<pioneer_provider::TokenUsage>,
    text: String,
    reasoning: String,
    tool_calls: Vec<ProviderToolCall>,
    provider_replay_state: Option<pioneer_provider::ProviderReplayState>,
    provider_token_count: Option<u64>,
    termination: pioneer_provider::ProviderTermination,
}

#[derive(Debug)]
struct ExecutedToolResult {
    progress: pioneer_tools::ToolProgressEvidence,
    item_id: String,
    provider_call_id: String,
    ordinal: u32,
    item_type: TurnItemType,
    attempt_number: u32,
    tool_name: String,
    arguments: String,
    model_visible_text: String,
    success: bool,
    outcome: ToolOutcome,
    recovery_view: Option<ToolRecoveryView>,
    retained_llm_context: Option<(ToolResultView, pioneer_protocol::ToolOutputPolicySnapshot)>,
    request_tools_result: Option<RequestToolsResult>,
    full_message: Option<ChatMessage>,
    full_source_in_item: bool,
    message: ChatMessage,
}

struct SpawnedToolTask {
    item_id: String,
    provider_call_id: String,
    ordinal: u32,
    tool_name: String,
    arguments: String,
    handle: AbortOnDropJoinHandle<Result<ExecutedToolResult, ChatTurnError>>,
}

const NATIVE_PARALLEL_READ_LIMIT: usize = 4;

fn native_tool_batch_segment_len<'a>(
    calls: impl Iterator<Item = (&'a str, &'a str)>,
    supports_parallel_reads: impl Fn(&str) -> bool,
) -> usize {
    let mut identities = HashSet::new();
    let mut count = 0;
    for (name, arguments) in calls.take(NATIVE_PARALLEL_READ_LIMIT) {
        if !supports_parallel_reads(name) {
            return count.max(1);
        }
        if !identities.insert(ToolNoProgressGuard::exact_strategy_identity(
            name, arguments,
        )) {
            break;
        }
        count += 1;
    }
    count
}

fn unknown_tool_task_result(
    item_id: String,
    provider_call_id: String,
    ordinal: u32,
    tool_name: String,
    arguments: String,
    message: String,
) -> ExecutedToolResult {
    let outcome = ToolOutcome::fatal(ToolErrorClass::Internal, Some(message.clone()));
    ExecutedToolResult {
        progress: Default::default(),
        item_id,
        provider_call_id: provider_call_id.clone(),
        ordinal,
        item_type: tooling::tool_item_type_from_name(tool_name.as_str()),
        attempt_number: 1,
        tool_name: tool_name.clone(),
        arguments,
        model_visible_text: message.clone(),
        success: false,
        outcome: outcome.clone(),
        recovery_view: None,
        retained_llm_context: None,
        full_message: None,
        full_source_in_item: false,
        request_tools_result: None,
        message: tooling::build_tool_error_message(provider_call_id, tool_name, message, outcome),
    }
}

struct AbortOnDropJoinHandle<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDropJoinHandle<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        // Keep ownership of the JoinHandle while awaiting it.  Taking it out
        // before the await makes Drop unable to abort a task when the join
        // future is cancelled (for example when the parent Turn is fenced).
        // Tokio then detaches the child and any external side effect can outlive
        // the Turn owner.
        let result = self
            .handle
            .as_mut()
            .expect("join handle should be present")
            .await;
        self.handle.take();
        result
    }
}

impl<T> Drop for AbortOnDropJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskMutationFailure {
    tool_name: String,
    identity: TaskMutationIdentity,
    expected_revision: Option<i64>,
    error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskMutationIdentity {
    task_id: Option<String>,
    operation: TaskMutationOperation,
    postcondition_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaskMutationOperation {
    Create,
    Update,
    Reschedule,
}

impl TaskMutationIdentity {
    fn from_result(result: &ExecutedToolResult) -> (Self, Option<i64>) {
        let operation = match result.tool_name.as_str() {
            "task_create" => TaskMutationOperation::Create,
            "task_update" => TaskMutationOperation::Update,
            _ => TaskMutationOperation::Reschedule,
        };
        // Canonical arguments, never the error/display text, identify the
        // intended postcondition. A concurrency revision is a precondition.
        let mut value =
            serde_json::from_str::<JsonValue>(&result.arguments).unwrap_or(JsonValue::Null);
        let task_id = value
            .get("taskId")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
        let expected_revision = value.get("expectedRevision").and_then(JsonValue::as_i64);
        if let Some(object) = value.as_object_mut() {
            object.remove("expectedRevision");
        }
        let fingerprint = if value.is_object() {
            let signature = pioneer_tools::ToolFailureSignature::from_arguments(
                &result.tool_name,
                &value.to_string(),
                ToolErrorClass::Unknown,
            );
            hex::encode(Sha256::digest(signature.arguments_fingerprint.as_bytes()))
        } else {
            // An invalid invocation has no proven shared mutation identity.
            hex::encode(Sha256::digest(result.item_id.as_bytes()))
        };
        (
            Self {
                task_id,
                operation,
                postcondition_sha256: fingerprint,
            },
            expected_revision,
        )
    }

    fn confirmed(&self, result: &ExecutedToolResult, expected_revision: Option<i64>) -> bool {
        if !result.success || result.outcome.status != pioneer_tools::ToolOutcomeStatus::Ok {
            return false;
        }
        let Ok(output) = serde_json::from_str::<JsonValue>(&result.model_visible_text) else {
            return false;
        };
        let output_task = output
            .get("taskId")
            .and_then(JsonValue::as_str)
            .or_else(|| output.pointer("/task/id").and_then(JsonValue::as_str));
        let Some(output_task) = output_task.filter(|id| !id.is_empty()) else {
            return false;
        };
        if let Some(expected_task) = &self.task_id {
            if expected_task != output_task {
                return false;
            }
            let revision = output
                .get("revision")
                .and_then(JsonValue::as_i64)
                .or_else(|| output.pointer("/task/revision").and_then(JsonValue::as_i64));
            return revision.is_some_and(|revision| {
                revision > 0 && expected_revision.is_none_or(|expected| revision > expected)
            });
        }
        self.operation == TaskMutationOperation::Create
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskMutationFinalizationGuard {
    failures_since_last_success: Vec<TaskMutationFailure>,
}

#[derive(Debug)]
struct RenderedTaskObservation {
    task_ids: Vec<String>,
    details: JsonValue,
}

#[derive(Debug)]
struct RenderedReviewRequiredObservation {
    signatures: Vec<String>,
    observations: Vec<ReviewRequiredTaskObservation>,
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
        .with_model_visible_text(self.model_visible_text.clone())
        .with_progress(self.progress)
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

fn record_requested_tool_names(names: &mut Vec<String>, results: &[ExecutedToolResult]) {
    for result in results {
        if let Some(request) = &result.request_tools_result {
            names.extend(
                request
                    .added
                    .values()
                    .chain(request.already_visible.values())
                    .flatten()
                    .cloned(),
            );
        }
    }
    names.sort();
    names.dedup();
}

fn restored_requested_tool_names(
    context: Option<&ExecutionCheckpointContext>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Vec<String> {
    context
        .filter(|context| {
            let payload = &context.payload;
            payload.workspace_id == workspace_id
                && payload.thread_id == thread_id
                && payload.turn_id == turn_id
        })
        .map(|context| context.payload.requested_tool_names.clone())
        .unwrap_or_default()
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
    fn checkpoint_valid(&self) -> bool {
        self.failures_since_last_success.len() <= 256
            && self
                .failures_since_last_success
                .iter()
                .enumerate()
                .all(|(index, failure)| {
                    let operation_name = match failure.identity.operation {
                        TaskMutationOperation::Create => "task_create",
                        TaskMutationOperation::Update => "task_update",
                        TaskMutationOperation::Reschedule => "task_reschedule",
                    };
                    failure.tool_name == operation_name
                        && failure
                            .identity
                            .task_id
                            .as_ref()
                            .is_none_or(|id| !id.is_empty() && id.len() <= 256)
                        && failure.identity.postcondition_sha256.len() == 64
                        && failure
                            .identity
                            .postcondition_sha256
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit())
                        && failure.error.len() <= 4096
                        && self.failures_since_last_success[..index]
                            .iter()
                            .all(|other| other.identity != failure.identity)
                })
    }

    fn observe(&mut self, result: &ExecutedToolResult) -> Result<(), ChatTurnError> {
        if !is_guarded_task_mutation_tool(result.tool_name.as_str()) {
            return Ok(());
        }
        let (identity, expected_revision) = TaskMutationIdentity::from_result(result);
        if result.success {
            self.failures_since_last_success.retain(|failure| {
                failure.identity != identity
                    || !identity.confirmed(result, failure.expected_revision)
            });
            return Ok(());
        }
        if let Some(failure) = self
            .failures_since_last_success
            .iter_mut()
            .find(|failure| failure.identity == identity)
        {
            failure.expected_revision = match (failure.expected_revision, expected_revision) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            return Ok(());
        }
        if self.failures_since_last_success.len() >= 256 {
            return Err(ChatTurnError::Blocked(
                "unresolved task mutation limit exceeded".to_owned(),
            ));
        }
        self.failures_since_last_success.push(TaskMutationFailure {
            tool_name: result.tool_name.clone(),
            identity,
            expected_revision,
            error: bounded_utf8_suffix(&result.model_visible_text, 4096).to_owned(),
        });
        Ok(())
    }

    fn deterministic_failure_message(&self) -> Option<String> {
        let failure = self.failures_since_last_success.last()?;
        Some(format!(
            "Task mutation has an unresolved postcondition. Do not report this task as created, updated, or rescheduled. Failed tool: {}. Primary error: {}",
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
    target.push_str(bounded_utf8_suffix(
        fragment,
        semantic_state::MAX_SUMMARY_TEXT_BYTES,
    ));
    if target.len() > semantic_state::MAX_SUMMARY_TEXT_BYTES {
        *target = bounded_utf8_suffix(target, semantic_state::MAX_SUMMARY_TEXT_BYTES).to_owned();
    }
}

fn bounded_utf8_suffix(text: &str, max_bytes: usize) -> &str {
    let mut start = text.len().saturating_sub(max_bytes);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn post_turn_tool_event_summary(result: &ExecutedToolResult) -> TurnPostTurnToolEventSummary {
    let outcome = tooling::protocol_outcome_from_tool_outcome(&result.outcome);
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
        outcome_status: Some(post_turn_tool_outcome_status(outcome.status)),
        error_class: outcome.error_class.map(post_turn_tool_error_class),
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

use crate::post_turn::{post_turn_tool_error_class, post_turn_tool_outcome_status};

#[derive(Debug, Clone)]
pub(super) enum ChatTurnError {
    Terminal(String),
    Transient(String),
    Blocked(String),
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

#[derive(Debug, Clone)]
pub(super) enum ChatTurnOutcome {
    Completed {
        post_turn_dispatch: Option<AgentTurnPostTurnHookDispatch>,
    },
    NeedsContinuation(ExecutionWindowContinuation),
}

impl Default for ChatTurnOutcome {
    fn default() -> Self {
        Self::Completed {
            post_turn_dispatch: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ChatExecutionWindowStats {
    window_index: u32,
    started_at_unix_ms: i64,
    agent_round_count: u32,
    requested_tool_call_count: u32,
    executed_tool_success_count: u32,
    executed_tool_failed_count: u32,
    provider_token_count: Option<u64>,
    provider_token_usage_unknown: bool,
}

impl ChatExecutionWindowStats {
    fn new(window_index: u32) -> Self {
        Self {
            window_index,
            started_at_unix_ms: Local::now().timestamp_millis(),
            agent_round_count: 0,
            requested_tool_call_count: 0,
            executed_tool_success_count: 0,
            executed_tool_failed_count: 0,
            provider_token_count: None,
            provider_token_usage_unknown: false,
        }
    }

    fn record_provider_round(
        &mut self,
        requested_tool_calls: usize,
        provider_token_count: Option<u64>,
    ) {
        self.agent_round_count = self.agent_round_count.saturating_add(1);
        self.requested_tool_call_count = self
            .requested_tool_call_count
            .saturating_add(u32::try_from(requested_tool_calls).unwrap_or(u32::MAX));
        if let Some(provider_token_count) = provider_token_count {
            self.provider_token_count = Some(
                self.provider_token_count
                    .unwrap_or_default()
                    .saturating_add(provider_token_count),
            );
        } else {
            self.provider_token_usage_unknown = true;
        }
    }

    fn record_preflight(&mut self, usage: preflight::PreflightBudgetUsage) {
        self.agent_round_count = self.agent_round_count.saturating_add(usage.attempts);
        if usage.attempts > 0 {
            self.provider_token_count = Some(
                self.provider_token_count
                    .unwrap_or_default()
                    .saturating_add(usage.known_tokens),
            );
        }
        self.provider_token_usage_unknown |= usage.unknown;
    }

    fn record_executed_tools(&mut self, results: &[ExecutedToolResult]) {
        for result in results {
            if result.success {
                self.executed_tool_success_count =
                    self.executed_tool_success_count.saturating_add(1);
            } else {
                self.executed_tool_failed_count = self.executed_tool_failed_count.saturating_add(1);
            }
        }
    }

    fn executed_tool_count(&self) -> u32 {
        self.executed_tool_success_count
            .saturating_add(self.executed_tool_failed_count)
    }

    fn checkpoint_window_summary(
        &self,
        reason: ExecutionWindowExhaustionReason,
    ) -> ExecutionCheckpointWindowSummary {
        ExecutionCheckpointWindowSummary {
            window_id: None,
            window_index: self.window_index,
            started_at_unix_ms: Some(self.started_at_unix_ms),
            completed_at_unix_ms: Some(Local::now().timestamp_millis()),
            agent_round_count: self.agent_round_count,
            tool_call_count: self.requested_tool_call_count,
            provider_token_count: self.provider_token_count,
            exhaustion_reason: Some(reason),
        }
    }

    fn checkpoint_provider_budget_summary(
        &self,
        model: &str,
        model_provider: &str,
        reason: ExecutionWindowExhaustionReason,
        exhausted_limit: Option<u64>,
        exhausted_observed: Option<u64>,
    ) -> ExecutionCheckpointProviderBudgetSummary {
        let mut summary = build_execution_checkpoint_provider_budget_summary(
            ExecutionCheckpointProviderBudgetInput {
                model: Some(model.to_owned()),
                model_provider: Some(model_provider.to_owned()),
                agent_round_count: self.agent_round_count,
                tool_call_count: self.requested_tool_call_count,
                provider_token_count: self.provider_token_count,
                exhaustion_reason: Some(reason),
                exhausted_limit,
                exhausted_observed,
            },
        );
        summary.provider_usage_available &= !self.provider_token_usage_unknown;
        summary
    }

    fn checkpoint_tool_summary(&self) -> ExecutionCheckpointToolSummary {
        let executed_count = self.executed_tool_count();
        let unexecuted_count = self
            .requested_tool_call_count
            .saturating_sub(executed_count);

        ExecutionCheckpointToolSummary {
            requested_count: self.requested_tool_call_count,
            executed_count,
            unexecuted_count,
            total_count: self.requested_tool_call_count,
            succeeded_count: self.executed_tool_success_count,
            failed_count: self.executed_tool_failed_count,
            in_progress_count: 0,
            detail_limit: u32::try_from(EXECUTION_CHECKPOINT_DEFAULT_TOOL_DETAIL_LIMIT)
                .unwrap_or(u32::MAX),
            details_truncated: false,
            details: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum AgentProviderLoopOutcome {
    Completed,
    NeedsContinuation(ExecutionWindowContinuation),
}

fn execution_window_exhaustion_reason(
    budget_reason: ToolLoopBudgetReason,
) -> ExecutionWindowExhaustionReason {
    match budget_reason {
        ToolLoopBudgetReason::AgentRoundsExceeded => {
            ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow
        }
        ToolLoopBudgetReason::ToolCallsExceeded => {
            ExecutionWindowExhaustionReason::MaxToolCallsPerWindow
        }
        ToolLoopBudgetReason::WallClockExceeded => {
            ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow
        }
        ToolLoopBudgetReason::ProviderTokensExceeded => {
            ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow
        }
        ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled => {
            ExecutionWindowExhaustionReason::ProviderFailureContinuation
        }
    }
}

fn build_execution_window_continuation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    input: &[UserInput],
    model: &str,
    model_provider: &str,
    stats: &ChatExecutionWindowStats,
    reason: ExecutionWindowExhaustionReason,
    exhausted_limit: Option<u64>,
    exhausted_observed: Option<u64>,
    reason_code: &str,
    tool_no_progress: ExecutionCheckpointToolNoProgressState,
    requested_tool_names: &[String],
) -> ExecutionWindowContinuation {
    let exhausted_window_id = execution_window_runtime_id(turn_id, stats.window_index);
    let checkpoint_id = execution_window_checkpoint_runtime_id(exhausted_window_id.as_str());
    let mut payload = build_execution_checkpoint_payload(
        workspace_id,
        thread_id,
        turn_id,
        build_execution_checkpoint_original_request_summary(input),
        stats.checkpoint_window_summary(reason),
        stats.checkpoint_provider_budget_summary(
            model,
            model_provider,
            reason,
            exhausted_limit,
            exhausted_observed,
        ),
        stats.checkpoint_tool_summary(),
        Vec::new(),
    );
    payload.tool_no_progress = tool_no_progress;
    payload.requested_tool_names = requested_tool_names.to_vec();

    ExecutionWindowContinuation {
        reason,
        exhausted_window_id,
        checkpoint_id,
        checkpoint_payload: payload,
        provider_history: Vec::new(),
        exhausted_limit,
        exhausted_observed,
        reason_code: reason_code.to_owned(),
    }
}

fn build_budget_execution_window_continuation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    input: &[UserInput],
    model: &str,
    model_provider: &str,
    stats: &ChatExecutionWindowStats,
    budget_exceeded: &ToolLoopBudgetExceeded,
    tool_no_progress: ExecutionCheckpointToolNoProgressState,
    requested_tool_names: &[String],
) -> ExecutionWindowContinuation {
    build_execution_window_continuation(
        workspace_id,
        thread_id,
        turn_id,
        input,
        model,
        model_provider,
        stats,
        execution_window_exhaustion_reason(budget_exceeded.reason),
        Some(u64::from(budget_exceeded.limit)),
        Some(u64::from(budget_exceeded.observed)),
        budget_exceeded.reason.code(),
        tool_no_progress,
        requested_tool_names,
    )
}

fn build_failure_execution_window_continuation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    input: &[UserInput],
    model: &str,
    model_provider: &str,
    stats: &ChatExecutionWindowStats,
    error: &ChatTurnError,
    tool_no_progress: ExecutionCheckpointToolNoProgressState,
    requested_tool_names: &[String],
) -> Option<ExecutionWindowContinuation> {
    let (reason, reason_code) = match error {
        ChatTurnError::ProviderFailure { .. } => (
            ExecutionWindowExhaustionReason::ProviderFailureContinuation,
            "provider_failure_continuation",
        ),
        ChatTurnError::Terminal(_) => return None,
        ChatTurnError::Transient(_) => (
            ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
            "runtime_shutdown_continuation",
        ),
        ChatTurnError::Blocked(_) => return None,
        ChatTurnError::WithPostTurnDispatch { error, .. } => {
            return build_failure_execution_window_continuation(
                workspace_id,
                thread_id,
                turn_id,
                input,
                model,
                model_provider,
                stats,
                error,
                tool_no_progress,
                requested_tool_names,
            );
        }
    };

    Some(build_execution_window_continuation(
        workspace_id,
        thread_id,
        turn_id,
        input,
        model,
        model_provider,
        stats,
        reason,
        None,
        None,
        reason_code,
        tool_no_progress,
        requested_tool_names,
    ))
}

fn execution_window_runtime_id(turn_id: &str, window_index: u32) -> String {
    format!("{turn_id}:window:{window_index}")
}

fn execution_window_checkpoint_runtime_id(window_id: &str) -> String {
    format!("{window_id}:checkpoint:window_exhausted")
}

async fn emit_execution_window_started(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    stats: &ChatExecutionWindowStats,
    event_tx: &AgentEventHub,
) -> Result<(), ChatTurnError> {
    emit_durable_event(
        event_tx,
        AgentDurableEvent::TurnExecutionWindowStarted {
            notification: TurnExecutionWindowStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                window_id: execution_window_runtime_id(turn_id, stats.window_index),
                window_index: stats.window_index,
                status: ExecutionWindowStatus::Running,
                started_at_unix_ms: stats.started_at_unix_ms,
            },
        },
    )
    .await
}

async fn emit_execution_window_exhausted_and_checkpointed(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    stats: &ChatExecutionWindowStats,
    continuation: &ExecutionWindowContinuation,
    event_tx: &AgentEventHub,
) -> Result<(), ChatTurnError> {
    let completed_at_unix_ms = continuation
        .checkpoint_payload
        .window
        .completed_at_unix_ms
        .unwrap_or_else(|| Local::now().timestamp_millis());
    emit_durable_event(
        event_tx,
        AgentDurableEvent::TurnExecutionWindowExhausted {
            notification: TurnExecutionWindowExhaustedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                window_id: continuation.exhausted_window_id.clone(),
                window_index: stats.window_index,
                status: ExecutionWindowStatus::Exhausted,
                exhaustion_reason: continuation.reason,
                limit: continuation.exhausted_limit.unwrap_or_default(),
                observed: continuation.exhausted_observed.unwrap_or_default(),
                agent_round_count: stats.agent_round_count,
                tool_call_count: stats.requested_tool_call_count,
                provider_token_count: stats.provider_token_count,
                started_at_unix_ms: stats.started_at_unix_ms,
                exhausted_at_unix_ms: completed_at_unix_ms,
                reason: continuation.reason_code.clone(),
            },
        },
    )
    .await?;

    let payload_bytes = serde_json::to_vec(&continuation.checkpoint_payload)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or(u64::MAX);
    emit_durable_event(
        event_tx,
        AgentDurableEvent::TurnExecutionWindowCheckpointed {
            notification: TurnExecutionWindowCheckpointedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                window_id: continuation.exhausted_window_id.clone(),
                window_index: stats.window_index,
                status: ExecutionWindowStatus::Checkpointed,
                checkpoint_id: continuation.checkpoint_id.clone(),
                checkpoint_kind: "window_exhausted".to_owned(),
                payload_bytes,
                created_at_unix_ms: completed_at_unix_ms,
            },
            payload: continuation.checkpoint_payload.clone(),
        },
    )
    .await
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
    let message = format!("failed to publish agent event: {error}");
    match error {
        AgentEventHubError::CommitRejected(rejection) if !rejection.is_retryable() => {
            ChatTurnError::Terminal(message)
        }
        _ => ChatTurnError::Transient(message),
    }
}

fn ensure_preflight_usage_committed(
    failure: Option<pioneer_protocol::TurnFailureDisposition>,
) -> Result<(), ChatTurnError> {
    use pioneer_protocol::TurnFailureDisposition;
    match failure {
        None => Ok(()),
        Some(TurnFailureDisposition::Permanent) => Err(ChatTurnError::Terminal(
            "preflight durable usage was rejected".to_owned(),
        )),
        Some(TurnFailureDisposition::Transient) => Err(ChatTurnError::Transient(
            "preflight durable usage is unavailable".to_owned(),
        )),
        Some(TurnFailureDisposition::Unknown) => Err(ChatTurnError::Blocked(
            "preflight durable usage outcome is unknown".to_owned(),
        )),
    }
}

fn chat_error_post_turn_status(error: &ChatTurnError) -> pioneer_hooks::TurnPostTurnStatus {
    match error {
        ChatTurnError::ProviderFailure { .. } => pioneer_hooks::TurnPostTurnStatus::ProviderFailure,
        ChatTurnError::Blocked(_) => pioneer_hooks::TurnPostTurnStatus::Blocked,
        ChatTurnError::Terminal(_)
        | ChatTurnError::Transient(_)
        | ChatTurnError::WithPostTurnDispatch { .. } => pioneer_hooks::TurnPostTurnStatus::Failed,
    }
}

fn chat_error_preview(error: &ChatTurnError) -> String {
    match error {
        ChatTurnError::Terminal(message) | ChatTurnError::Transient(message) => message.clone(),
        ChatTurnError::Blocked(message) => message.clone(),
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
        .publish_durable_and_wait(event)
        .await
        .map_err(agent_event_error)
}

#[allow(clippy::too_many_arguments)]
async fn persist_native_semantic_checkpoint(
    event_tx: &AgentEventHub,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    input: &[UserInput],
    model: &str,
    model_provider: &str,
    stats: &ChatExecutionWindowStats,
    no_progress: pioneer_protocol::ExecutionCheckpointToolNoProgressState,
    requested_tools: &[String],
    state: serde_json::Value,
    control: Option<pioneer_protocol::NativeControlRecord>,
    execution_owner_generation: Option<u64>,
    runtime_generation: u64,
) -> Result<(), ChatTurnError> {
    let mut payload = pioneer_protocol::build_execution_checkpoint_payload(
        workspace_id,
        thread_id,
        turn_id,
        build_execution_checkpoint_original_request_summary(input),
        ExecutionCheckpointWindowSummary {
            window_id: Some(execution_window_runtime_id(turn_id, stats.window_index)),
            window_index: stats.window_index,
            started_at_unix_ms: Some(stats.started_at_unix_ms),
            completed_at_unix_ms: None,
            agent_round_count: stats.agent_round_count,
            tool_call_count: stats.requested_tool_call_count,
            provider_token_count: stats.provider_token_count,
            exhaustion_reason: None,
        },
        build_execution_checkpoint_provider_budget_summary(
            ExecutionCheckpointProviderBudgetInput {
                model: Some(model.to_owned()),
                model_provider: Some(model_provider.to_owned()),
                agent_round_count: stats.agent_round_count,
                tool_call_count: stats.requested_tool_call_count,
                provider_token_count: stats.provider_token_count,
                exhaustion_reason: None,
                exhausted_limit: None,
                exhausted_observed: None,
            },
        ),
        stats.checkpoint_tool_summary(),
        Vec::new(),
    );
    payload.provider_budget.provider_usage_available &= !stats.provider_token_usage_unknown;
    payload.tool_no_progress = no_progress;
    payload.requested_tool_names = requested_tools.to_vec();
    payload.native_semantic_state = Some(state);
    emit_durable_event(
        event_tx,
        AgentDurableEvent::NativeSemanticCheckpoint {
            execution_owner_generation,
            runtime_generation,
            checkpoint: payload,
            control,
        },
    )
    .await
}

async fn persist_provider_history_message(
    event_tx: &AgentEventHub,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    envelope: &CanonicalProviderRoundEnvelope,
) -> Result<(), ChatTurnError> {
    let payload = serde_json::to_value(envelope).map_err(|error| {
        ChatTurnError::Terminal(format!(
            "failed to serialize canonical provider history: {error}"
        ))
    })?;
    event_tx
        .publish_durable_and_wait(AgentDurableEvent::TurnProviderHistoryAppended {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            sequence: 0,
            payload,
        })
        .await
        .map_err(agent_event_error)
}

pub(crate) fn bounded_tool_result_message(
    source: &ChatMessage,
    reference: &str,
) -> Result<ChatMessage, ChatTurnError> {
    bounded_tool_result_message_with_budget(source, reference, pioneer_compaction::RESULT_TOKENS)
}

pub(crate) fn bounded_tool_result_message_with_budget(
    source: &ChatMessage,
    reference: &str,
    available_tokens: u64,
) -> Result<ChatMessage, ChatTurnError> {
    let token_limit = available_tokens.min(pioneer_compaction::RESULT_TOKENS);
    let mut message = source.clone();
    let mut text = message.content.clone();
    message.content_parts = message.content_parts.into_iter().enumerate().filter_map(|(index, part)| {
        if let pioneer_provider::MessageContentPart::Text { text: part } = &part {
            if !text.is_empty() { text.push('\n'); }
            text.push_str(part);
            return None;
        }
        let attachment = match &part {
            pioneer_provider::MessageContentPart::File { file } => file,
            pioneer_provider::MessageContentPart::Image { image } => image,
            pioneer_provider::MessageContentPart::Audio { audio } => audio,
            pioneer_provider::MessageContentPart::Video { video } => video,
            pioneer_provider::MessageContentPart::Text { .. } => unreachable!(),
        };
        if matches!(attachment.source, pioneer_provider::AttachmentDataSource::Bytes { .. }) {
            // The source message is durably retained before this representation is published.
            // Inline binary data stays there; the working message carries its exact part reference.
            text.push('\n');
            text.push_str(&serde_json::json!({"attachment": {
                "mime_type": attachment.mime_type, "name": attachment.name, "size_bytes": attachment.size_bytes,
                "sha256": attachment.sha256, "artifact": attachment.artifact,
                "full_source": reference, "content_part": index,
                "read_tool": "threads_tools_result_read",
            }}).to_string());
            None
        } else { Some(part) }
    }).collect();
    message.content.clear();
    let framing =
        serde_json::to_string(&message).map_err(|e| ChatTurnError::Blocked(e.to_string()))?;
    if framing.len() > pioneer_compaction::RESULT_BYTES
        || pioneer_compaction::text_tokens(&framing) > token_limit
    {
        return Err(ChatTurnError::Blocked(
            "tool result attachment references exceed available context".into(),
        ));
    }
    let mut tokens = token_limit.saturating_sub(pioneer_compaction::text_tokens(&framing));
    let mut bytes = pioneer_compaction::RESULT_BYTES.saturating_sub(framing.len());
    loop {
        message.content =
            pioneer_compaction::results::result_excerpt(&text, reference, tokens, bytes)
                .map_err(|e| ChatTurnError::Blocked(e.to_string()))?;
        let encoded =
            serde_json::to_string(&message).map_err(|e| ChatTurnError::Blocked(e.to_string()))?;
        if encoded.len() <= pioneer_compaction::RESULT_BYTES
            && pioneer_compaction::text_tokens(&encoded) <= token_limit
        {
            return Ok(message);
        }
        tokens /= 2;
        bytes /= 2;
    }
}

async fn persist_retained_tool_result(
    event_tx: &AgentEventHub,
    thread_id: &str,
    turn_id: &str,
    result: &ExecutedToolResult,
) -> Result<(), ChatTurnError> {
    if result.message.role != pioneer_provider::Role::Tool
        || result.message.tool_call_id.as_deref() != Some(result.provider_call_id.as_str())
        || result.message.name.as_deref() != Some(result.tool_name.as_str())
    {
        return Err(ChatTurnError::Terminal(format!(
            "provider result envelope mismatch for Turn item `{}`",
            result.item_id
        )));
    }
    let payload = serde_json::to_value(result.full_message.as_ref().unwrap_or(&result.message))
        .map_err(|error| {
            ChatTurnError::Terminal(format!(
                "failed to serialize exact provider tool result: {error}"
            ))
        })?;
    let output_policy_snapshot = result
        .retained_llm_context
        .as_ref()
        .map(|(_, snapshot)| snapshot.clone())
        .unwrap_or_else(|| {
            pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(&result.tool_name)
        });
    event_tx
        .publish_durable_and_wait(AgentDurableEvent::TurnLlmContextAppended {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: result.item_id.clone(),
            attempt_id: None,
            sequence: 0,
            source: "tool_result_v2".to_owned(),
            tool_name: result.tool_name.clone(),
            payload: pioneer_protocol::ToolResultView::Json {
                value: payload,
                truncated: false,
            },
            output_policy_snapshot,
        })
        .await
        .map_err(agent_event_error)
}

async fn emit_turn_skills_resolved_event(
    event_tx: &AgentEventHub,
    thread_id: &str,
    turn_id: &str,
    bindings: Vec<pioneer_protocol::TurnSkillBinding>,
    require_gateway_confirmation: bool,
) -> Result<(), ChatTurnError> {
    let event = AgentDurableEvent::TurnSkillsResolved {
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        bindings,
    };
    if require_gateway_confirmation {
        event_tx
            .publish_durable_and_wait(event)
            .await
            .map_err(agent_event_error)
    } else {
        emit_durable_event(event_tx, event).await
    }
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
        skill_id: event.skill_id,
        skill_owner: event.skill_owner,
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
            TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            } => {
                let expected_id = format!("skill:{skill_id}");
                if capability.id != expected_id {
                    normalized.rejected.push(rejected_capability(
                        capability,
                        TurnCapabilityRejectedReason::InvalidInput,
                        format!("Skill capability must use exact ID `{expected_id}`."),
                    ));
                    continue;
                }
                expected_id
            }
            TurnCapabilityKind::Skill {
                pack_id: Some(_), ..
            }
            | TurnCapabilityKind::SkillPack { .. } => {
                normalized.rejected.push(rejected_capability(
                    capability,
                    TurnCapabilityRejectedReason::InvalidInput,
                    "Skill pack metadata reached the agent runtime boundary.",
                ));
                continue;
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
            TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            } => {
                normalized.skill_refs.push(SkillExplicitRef {
                    skill_id: skill_id.clone(),
                    label: capability.label.clone(),
                });
            }
            TurnCapabilityKind::Skill {
                pack_id: Some(_), ..
            }
            | TurnCapabilityKind::SkillPack { .. } => {
                unreachable!("pack metadata was rejected before runtime normalization")
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
        skill_id: reference.skill_id.clone(),
        pack_id: None,
    }
}

fn accepted_skill_capability(reference: &SkillExplicitRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id(),
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
        id: reference.capability_id(),
        label: reference.label.clone(),
        kind: skill_capability_kind(reference),
        reason,
        message: message.into(),
    }
}

fn skill_ref_matches_resolved_skill(reference: &SkillExplicitRef, skill: &ResolvedSkill) -> bool {
    reference.skill_id == skill.skill_id
}

fn skill_ref_matches_excluded_skill(reference: &SkillExplicitRef, skill: &ExcludedSkill) -> bool {
    reference.skill_id == skill.skill_id
}

fn skill_rejection_reason(reason: &SkillExcludedReason) -> TurnCapabilityRejectedReason {
    match reason {
        SkillExcludedReason::Unavailable => TurnCapabilityRejectedReason::Unavailable,
        SkillExcludedReason::NotFound => TurnCapabilityRejectedReason::NotFound,
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
        .map(str::to_owned)
        .unwrap_or_else(|| reference.skill_id.to_string());
    match reason {
        SkillExcludedReason::Unavailable => format!("Skill `{label}` is unavailable."),
        SkillExcludedReason::NotFound => format!("Skill `{label}` was not found."),
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
            .map(str::to_owned)
            .unwrap_or_else(|| reference.skill_id.to_string());
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

fn unavailable_mcp_provider_summary(
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
            reason: TurnCapabilityRejectedReason::Unavailable,
            message: "MCP resolution is unavailable for this turn.".to_owned(),
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
            reason: TurnCapabilityRejectedReason::Unavailable,
            message: "MCP resolution is unavailable for this turn.".to_owned(),
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
        TurnCapabilityKind::Skill { skill_id, .. } => skill_id.to_string(),
        TurnCapabilityKind::SkillPack { pack_id } => pack_id.to_string(),
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

async fn emit_mcp_resolution_diagnostics(
    event_tx: &AgentEventHub,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    diagnostics: &[AgentMcpResolutionDiagnostic],
) -> Result<(), ChatTurnError> {
    for diagnostic in diagnostics {
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
                        message: diagnostic.message.clone(),
                        code: Some(diagnostic.code.clone()),
                        details: None,
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

fn runtime_sections_with_execution_continuation_context(
    runtime_sections: &[PromptRuntimeSectionInput],
    execution_window_index: u32,
    execution_checkpoint_context: Option<&ExecutionCheckpointContext>,
    prior_visible_assistant_text: Option<&str>,
    initiating_thread_id: Option<&str>,
) -> Vec<PromptRuntimeSectionInput> {
    if execution_window_index <= 1 {
        return runtime_sections.to_vec();
    }
    let Some(context) = execution_checkpoint_context else {
        return runtime_sections.to_vec();
    };

    let mut sections = runtime_sections.to_vec();
    if sections.iter().any(|section| {
        matches!(
            &section.id,
            PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation)
        )
    }) {
        return sections;
    }

    let facts_input = ExecutionContinuationRuntimeFactsInput {
        checkpoint: &context.payload,
        initiating_thread_id,
        prior_visible_assistant_text,
    };
    let continuation_section = execution_continuation_section_with_runtime_facts(&facts_input);
    sections.push(PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation),
        title: Some(continuation_section.title),
        content: continuation_section.content,
        max_chars: None,
        truncated: false,
    });
    sections
}

fn runtime_sections_with_artifact_reference_policy(
    runtime_sections: Vec<PromptRuntimeSectionInput>,
    include_policy: bool,
) -> Result<Vec<PromptRuntimeSectionInput>, ChatTurnError> {
    if runtime_sections.iter().any(|section| {
        matches!(
            &section.id,
            PromptRuntimeSectionId::Dynamic(id) if id.as_str() == "artifact_references"
        )
    }) {
        return Ok(runtime_sections);
    }
    if !include_policy {
        return Ok(runtime_sections);
    }

    let mut sections = runtime_sections;
    sections.push(PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::Dynamic(
            PromptDynamicSectionId::new("artifact_references").map_err(|error| {
                ChatTurnError::Terminal(format!(
                    "failed to create artifact reference prompt section id: {error}"
                ))
            })?,
        ),
        title: Some("Artifact References".to_owned()),
        content: render_artifact_reference_policy_prompt(),
        max_chars: None,
        truncated: false,
    });
    Ok(sections)
}

fn native_file_tool_blocked_names(capability: &NativeFileToolCapability) -> Vec<(String, String)> {
    let mut blocked = Vec::new();
    if !capability.read_file {
        blocked.push((
            "read_file".to_owned(),
            capability
                .reason
                .clone()
                .unwrap_or_else(|| "native provider does not project read_file".to_owned()),
        ));
    }
    if !capability.apply_patch {
        blocked.push((
            "apply_patch".to_owned(),
            capability
                .reason
                .clone()
                .unwrap_or_else(|| "native provider does not project apply_patch".to_owned()),
        ));
    }
    blocked
}

fn append_native_filesystem_capability_section(
    runtime_sections: Vec<PromptRuntimeSectionInput>,
    capability: &NativeFileToolCapability,
    execution_security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<Vec<PromptRuntimeSectionInput>, ChatTurnError> {
    let id =
        PromptDynamicSectionId::new(FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID).map_err(|error| {
            ChatTurnError::Terminal(format!(
                "failed to create native filesystem capability prompt section: {error}"
            ))
        })?;
    let content = if capability.is_supported() {
        let apply_patch_input = match capability.patch_shape {
            NativePatchWireShape::Freeform => {
                "Call `apply_patch` with the complete patch document as raw text; do not wrap it in JSON."
            }
            NativePatchWireShape::JsonFunction => {
                "Call `apply_patch` with exactly one JSON field named `patch`; its string value is the complete patch document. Do not add other fields or nest another JSON object inside `patch`."
            }
            NativePatchWireShape::Unavailable => {
                "The `apply_patch` input format is unavailable for this turn."
            }
        };
        let filesystem_scope = native_filesystem_scope(execution_security_snapshot);
        format!(
            "The available filesystem tools for this turn are `read_file`, `list_dir`, `grep_files`, and `apply_patch`. Use `apply_patch` as the general UTF-8 text editor and follow its complete syntax and examples exactly. {apply_patch_input} {filesystem_scope} Read unfamiliar files before updating them. If a call fails, follow the returned error and next action instead of repeating the same call or switching to shell editing. Use only tools present in the current catalog."
        )
    } else {
        format!(
            "`read_file` and `apply_patch` are not available for this turn ({reason}). Do not invent unavailable filesystem tools or a replacement text editor; use only tools present in the current catalog.",
            reason = capability
                .reason
                .as_deref()
                .unwrap_or("the selected model connection does not support them")
        )
    };
    let mut sections = runtime_sections
        .into_iter()
        .filter(|section| section.id.manifest_id() != FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID)
        .collect::<Vec<_>>();
    sections.push(PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::Dynamic(id),
        title: Some("Filesystem Tools for This Turn".to_owned()),
        content,
        max_chars: None,
        truncated: false,
    });
    Ok(sections)
}

fn native_filesystem_scope(snapshot: Option<&TurnExecutionSecuritySnapshot>) -> String {
    let Some(snapshot) = snapshot else {
        return "The trusted turn filesystem scope is unavailable; do not attempt filesystem operations.".to_owned();
    };
    let cwd = snapshot.sandbox.cwd.as_str();
    if snapshot.sandbox.filesystem.kind == pioneer_protocol::TurnFilesystemSandboxKind::Unrestricted
    {
        return format!(
            "The current working directory is `{cwd}`; relative paths resolve from it. Filesystem access is unrestricted for this turn, so absolute paths are accepted."
        );
    }

    let writable = FilePolicyChecker::allowed_roots(snapshot, FilePolicyOperation::Write)
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<BTreeSet<_>>();
    let readable = FilePolicyChecker::allowed_roots(snapshot, FilePolicyOperation::Read)
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<BTreeSet<_>>();
    let read_only = readable.difference(&writable).cloned().collect::<Vec<_>>();
    let writable = writable.into_iter().collect::<Vec<_>>();
    format!(
        "The current working directory is `{cwd}`; relative paths resolve from it. Writable roots: {}. Additional read-only roots: {}. Absolute paths are accepted only under a root authorized for that operation.",
        display_prompt_roots(writable.as_slice()),
        display_prompt_roots(read_only.as_slice()),
    )
}

fn display_prompt_roots(roots: &[String]) -> String {
    if roots.is_empty() {
        "none".to_owned()
    } else {
        roots
            .iter()
            .map(|root| format!("`{root}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn native_provider_tool_definition(
    spec: pioneer_tools::ToolSpec,
    capability: &NativeFileToolCapability,
) -> ToolDefinition {
    if spec.name != "apply_patch" {
        return ToolDefinition {
            name: spec.name,
            description: spec.description,
            parameters: spec.parameters,
        };
    }
    let input_instruction = match capability.patch_shape {
        NativePatchWireShape::Freeform => {
            "Pass the complete patch document directly as raw text without a JSON wrapper."
        }
        NativePatchWireShape::JsonFunction => {
            "Pass exactly one JSON field named `patch`, containing the complete patch document as a string."
        }
        NativePatchWireShape::Unavailable => {
            "This tool has no supported input format for the selected model connection."
        }
    };
    ToolDefinition {
        name: spec.name,
        description: format!("{} {input_instruction}", spec.description),
        parameters: pioneer_provider::apply_patch_tool_schema(capability.patch_shape),
    }
}

fn history_or_prompt_context_has_artifact_refs(
    history: &[ChatMessage],
    effective_prompt_context_set: &EffectiveTurnPromptContextSet,
) -> bool {
    history.iter().any(|message| {
        prompt_text_has_inline_artifact_refs(message.content.as_str())
            || message
                .content_parts
                .iter()
                .filter_map(|part| part.text_value())
                .any(prompt_text_has_inline_artifact_refs)
    }) || effective_prompt_context_set
        .clone_hook_prompt_context_set()
        .entries()
        .any(|entry| prompt_text_has_inline_artifact_refs(entry.content.as_str()))
}

fn prompt_text_has_inline_artifact_refs(text: &str) -> bool {
    text.contains("Available artifacts from this user message:")
        || text.contains("Available artifacts from this assistant message:")
        || text.contains("Available artifacts for thread:")
}

fn render_artifact_reference_policy_prompt() -> String {
    [
        "Some prior messages or recalled thread snippets may include compact artifact references.",
        "Artifact references are metadata only; they do not contain the file, image, or binary content.",
        "If an artifact's content is needed to answer correctly, reveal the artifact tool domain when needed and call artifact_read with the referenced artifactId, and versionId when present.",
        "artifact_read may return text content for readable artifacts or an attachment for binary artifacts such as images, audio, video, PDFs, or other files.",
        "Do not call artifact_read for artifacts that are irrelevant to the current task.",
        "Do not guess unseen artifact content from names, mime types, or surrounding text.",
    ]
    .join("\n")
}

fn prior_visible_assistant_text_for_execution_continuation(
    history: &[ChatMessage],
) -> Option<String> {
    let text = history
        .iter()
        .filter(|message| message.role == pioneer_provider::Role::Assistant)
        .filter_map(|message| {
            let text = message.text_content_lossy();
            (!text.trim().is_empty()).then(|| text.trim().to_owned())
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    normalize_optional_prompt(Some(text))
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
    event_tx: Arc<AgentEventHub>,
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
    window_index: u32,
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
    let deterministic_summary = deterministic_recall_context_summary(&hook_prompt_context_set);
    let prompt_context_input = TurnPrePromptContextHookInput::from_parts(
        input_text,
        Some(model.to_owned()),
        Some(provider.name().to_owned()),
    );
    let memory_context = MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        conversation_thread_id: hook_runtime_context.conversation_thread_id.clone(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input_text.to_owned(),
        task_id: hook_runtime_context.task_id.clone(),
        agent_id: hook_runtime_context.agent_id.clone(),
        principal_id: (hook_runtime_context.actor_kind == HookActorKind::User)
            .then(|| hook_runtime_context.actor_id.clone())
            .flatten(),
    };
    let episodic_capabilities = MemoryEpisodicRecallCapabilities {
        current_thread_search: !memory_context
            .effective_conversation_thread_id()
            .trim()
            .is_empty(),
        related_thread_search: false,
        workspace_thread_search: false,
        full_input_query: false,
        current_task_context: hook_runtime_context
            .task_id
            .as_ref()
            .is_some_and(|task_id| !task_id.trim().is_empty()),
        completed_task_summary: false,
    };
    let thread_episodic_summary = active_recall_thread_episodic_summary(
        &hook_prompt_context_set,
        &memory_context,
        &episodic_capabilities,
    );
    let active_recall = build_active_recall_local_preflight_plan_with_thread_summary(
        &memory_context,
        &prompt_context_input,
        &memory_policy,
        &memory_config,
        &deterministic_summary,
        episodic_capabilities,
        thread_episodic_summary,
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
        usage_context: Some(preflight::PreflightUsageContext {
            events: event_tx,
            workspace: workspace_id.into(),
            thread: thread_id.into(),
            turn: turn_id.into(),
            window_index,
        }),
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

#[cfg(test)]
fn compile_agent_instruction_delivery_plan_with_prompt_root(
    prompt_root: &std::path::Path,
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    runtime_sections: &[PromptRuntimeSectionInput],
    permission_profile: &TurnPermissionProfileSnapshot,
    include_task_orchestration_policy: bool,
    include_request_tools_catalog: bool,
    continue_generation_hint: bool,
    initiating_thread_id: Option<&str>,
    thread_id: &str,
    turn_id: &str,
) -> Result<CompiledInstructionDeliveryPlan, ChatTurnError> {
    compile_agent_instruction_delivery_plan_with_captured_clock(
        prompt_root,
        skills_prompt,
        retry_instruction,
        runtime_sections,
        permission_profile,
        include_task_orchestration_policy,
        include_request_tools_catalog,
        continue_generation_hint,
        initiating_thread_id,
        thread_id,
        turn_id,
        &Local::now().fixed_offset(),
    )
}

fn compile_agent_instruction_delivery_plan_with_captured_clock(
    prompt_root: &std::path::Path,
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    runtime_sections: &[PromptRuntimeSectionInput],
    permission_profile: &TurnPermissionProfileSnapshot,
    include_task_orchestration_policy: bool,
    include_request_tools_catalog: bool,
    continue_generation_hint: bool,
    initiating_thread_id: Option<&str>,
    thread_id: &str,
    turn_id: &str,
    now: &chrono::DateTime<chrono::FixedOffset>,
) -> Result<CompiledInstructionDeliveryPlan, ChatTurnError> {
    let extra_system = format!(
        "## Runtime\n{}\nCurrent date/time: {} ({})\nOS: {}",
        pioneer_promt::render_thread_ids(initiating_thread_id, thread_id),
        now.format("%Y-%m-%d %H:%M:%S"),
        now.format("%Z"),
        std::env::consts::OS,
    );

    let runtime_sections =
        runtime_sections_with_current_permissions(runtime_sections, permission_profile);
    let runtime_sections = runtime_sections_with_request_tools_catalog(
        runtime_sections.as_slice(),
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

    compile_instruction_delivery_plan(bundle).map_err(|error| {
        ChatTurnError::Terminal(format!(
            "failed to compile native instruction delivery plan: {error:#}"
        ))
    })
}

fn runtime_sections_with_current_permissions(
    runtime_sections: &[PromptRuntimeSectionInput],
    permission_profile: &TurnPermissionProfileSnapshot,
) -> Vec<PromptRuntimeSectionInput> {
    let Some(content) = current_permission_guidance(permission_profile) else {
        return runtime_sections.to_vec();
    };

    let mut sections = runtime_sections.to_vec();
    sections.push(PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::CurrentPermissions),
        title: None,
        content,
        max_chars: Some(1_500),
        truncated: false,
    });
    sections
}

fn reproject_agent_instruction_plan(
    base: &CompiledInstructionDeliveryPlan,
    retry: Option<&str>,
    tools_enabled: bool,
) -> Result<CompiledInstructionDeliveryPlan, ChatTurnError> {
    pioneer_promt::reproject_native_instruction_plan(base, retry, tools_enabled, None)
        .map_err(|_| ChatTurnError::Blocked("accepted prompt reprojection failed".to_owned()))
}

fn compiled_prompt_payload_from_delivery_plan(
    plan: &CompiledInstructionDeliveryPlan,
) -> CompiledPromptPayload {
    let bundle = &plan.bundle;
    CompiledPromptPayload {
        stable_system_text: bundle.stable_system_text.clone(),
        dynamic_system_text: bundle.dynamic_system_text.clone(),
        boundary_marker: bundle.boundary_marker.to_owned(),
        full_system_text: plan.provider_instructions.text.clone(),
    }
}

fn append_recovered_provider_history(
    messages: &mut Vec<ChatMessage>,
    mut retained_context: Vec<RetainedProviderHistoryMessage>,
) {
    retained_context.sort_by_key(|context| context.sequence);
    messages.extend(retained_context.into_iter().map(|context| context.message));
}

fn next_retained_provider_history_sequence(
    retained_context: &[RetainedProviderHistoryMessage],
) -> i64 {
    retained_context
        .iter()
        .map(|context| context.sequence)
        .max()
        .map_or(0, |sequence| sequence.saturating_add(1))
}

const MAX_RETAINED_PROVIDER_MESSAGES: usize = 8192;
const MAX_RETAINED_PROVIDER_BYTES: usize = 256 * 1024 * 1024;

struct RetainedProviderByteCounter(usize);
impl std::io::Write for RetainedProviderByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|value| *value <= MAX_RETAINED_PROVIDER_BYTES)
            .ok_or_else(|| std::io::Error::other("retained native history byte limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Count actual message and provenance wire bytes without another transcript allocation.
fn retained_provider_message_bytes(message: &ChatMessage) -> Result<usize, ChatTurnError> {
    let mut counter = RetainedProviderByteCounter(0);
    serde_json::to_writer(&mut counter, &(message, &message.provenance)).map_err(|_| {
        ChatTurnError::Blocked("retained native history exceeds its byte bound".into())
    })?;
    Ok(counter.0)
}

/// Called only after successful context preparation. The exact acknowledged
/// checkpoint roots replace their old prefix; current native tool units stay whole.
fn synchronize_retained_provider_history(
    retained: &mut Vec<RetainedProviderHistoryMessage>,
    next_sequence: &mut i64,
    workspace: &str,
    thread: &str,
    turn: &str,
    accepted: &[ChatMessage],
) -> Result<usize, ChatTurnError> {
    let unavailable =
        || ChatTurnError::Blocked("retained native history projection unavailable".into());
    let mut units = std::collections::BTreeMap::new();
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for message in accepted {
        let Some(origin) = &message.provenance else {
            if matches!(
                message.role,
                pioneer_provider::Role::Assistant | pioneer_provider::Role::Tool
            ) {
                return Err(unavailable());
            }
            continue;
        };
        let root = origin
            .sources
            .iter()
            .any(|source| source.scope.starts_with("checkpoint:"));
        let current = origin.thread_id == thread
            && origin.sources.iter().any(|source| {
                source.scope.split_once(':').is_some_and(|(kind, owner)| {
                    owner == turn
                        && matches!(
                            kind,
                            "context"
                                | "event"
                                | "item"
                                | "pending-assistant"
                                | "pending-tool"
                                | "pending-item"
                        )
                })
            });
        let keep = root
            || (current
                && matches!(
                    message.role,
                    pioneer_provider::Role::Assistant | pioneer_provider::Role::Tool
                ));
        let key = (origin.thread_id.as_str(), origin.unit_id.as_str());
        if units
            .insert(key, keep)
            .is_some_and(|previous| previous != keep)
        {
            return Err(unavailable());
        }
        if !keep {
            continue;
        }
        if origin.workspace_id != workspace || origin.sources.is_empty() {
            return Err(unavailable());
        }
        if current
            && !root
            && !origin.sources.iter().all(|source| {
                source.scope.split_once(':').is_some_and(|(kind, owner)| {
                    owner == turn
                        && matches!(
                            kind,
                            "context"
                                | "event"
                                | "item"
                                | "pending-assistant"
                                | "pending-tool"
                                | "pending-item"
                        )
                })
            })
        {
            return Err(unavailable());
        }
        if selected.len() >= MAX_RETAINED_PROVIDER_MESSAGES {
            return Err(unavailable());
        }
        bytes = bytes
            .checked_add(retained_provider_message_bytes(message)?)
            .filter(|value| *value <= MAX_RETAINED_PROVIDER_BYTES)
            .ok_or_else(unavailable)?;
        selected.push(message);
    }
    // All validation precedes replacement. A failed preparation or bound leaves
    // the old agreed projection intact, including opaque native replay state.
    let next = selected
        .into_iter()
        .enumerate()
        .map(|(sequence, message)| RetainedProviderHistoryMessage {
            sequence: sequence as i64,
            message: message.clone(),
        })
        .collect::<Vec<_>>();
    *next_sequence = next.len() as i64;
    *retained = next;
    Ok(bytes)
}

fn retain_provider_history_message(
    retained_context: &mut Vec<RetainedProviderHistoryMessage>,
    next_sequence: &mut i64,
    retained_bytes: &mut Option<usize>,
    message: &ChatMessage,
) -> Result<(), ChatTurnError> {
    let unavailable = || ChatTurnError::Blocked("retained native history capacity exceeded".into());
    if retained_context.len() >= MAX_RETAINED_PROVIDER_MESSAGES {
        return Err(unavailable());
    }
    let previous_bytes = match *retained_bytes {
        Some(bytes) => bytes,
        None => retained_context.iter().try_fold(0usize, |bytes, entry| {
            bytes
                .checked_add(retained_provider_message_bytes(&entry.message)?)
                .filter(|bytes| *bytes <= MAX_RETAINED_PROVIDER_BYTES)
                .ok_or_else(unavailable)
        })?,
    };
    let bytes = previous_bytes
        .checked_add(retained_provider_message_bytes(message)?)
        .filter(|bytes| *bytes <= MAX_RETAINED_PROVIDER_BYTES)
        .ok_or_else(unavailable)?;
    let following_sequence = next_sequence.checked_add(1).ok_or_else(unavailable)?;
    retained_context.push(RetainedProviderHistoryMessage {
        sequence: *next_sequence,
        message: message.clone(),
    });
    *next_sequence = following_sequence;
    *retained_bytes = Some(bytes);
    Ok(())
}

fn prompt_manifest_profile(profile: PromptProfile) -> PromptManifestProfile {
    match profile {
        PromptProfile::AssistantFull => PromptManifestProfile::AssistantFull,
        PromptProfile::AssistantMinimal => PromptManifestProfile::AssistantMinimal,
        PromptProfile::AssistantNone => PromptManifestProfile::AssistantNone,
        PromptProfile::CliRuntime => PromptManifestProfile::CliRuntime,
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
        HookPhase::RuntimeTurnPreContext => Some(PromptManifestHookPhase::RuntimeTurnPreContext),
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
        EffectiveTurnPromptManifestHookContributionKind::ThreadContext => {
            PromptManifestHookContributionKind::ThreadContext
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
    context_session: Option<Arc<crate::compaction::controller::NativeContextSession>>,
    thread_id: String,
    turn_id: String,
    workspace_id: String,
    mode: ThreadMode,
    provider_registry: Arc<ProviderRegistry>,
    provider: Arc<dyn Provider>,
    model: String,
    hook_runtime_context: AgentTurnHookRuntimeContext,
    reasoning: Option<ReasoningConfig>,
    workspace_skill_policies: HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    skill_catalog: pioneer_skills::SkillCatalogSnapshot,
    agent_skill_overlay: Vec<pioneer_skills::AgentSkillRuntimeEntry>,
    input: Vec<UserInput>,
    capabilities: Vec<TurnCapability>,
    resolved_artifacts: Vec<ResolvedArtifactInput>,
    runtime_environment: HashMap<String, String>,
    history: Vec<ChatMessage>,
    retained_provider_history: Vec<RetainedProviderHistoryMessage>,
    execution_window_index: u32,
    execution_checkpoint_context: Option<ExecutionCheckpointContext>,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    force_non_stream: bool,
    disable_tool_calling: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    initial_preparation: Option<crate::NativeInitialPreparation>,
    runtime_generation: u64,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    memory_provider: Option<Arc<dyn crate::AgentMemoryProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<dyn PermissionApprovalBroker>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_tx: Arc<AgentEventHub>,
) -> Result<ChatTurnOutcome, ChatTurnError> {
    let preparation_started = Instant::now();
    let preparation_started_at_unix_ms = Local::now().timestamp_millis();
    if mode == ThreadMode::Message {
        return Err(ChatTurnError::Terminal(
            "message turns must complete in gateway admission without provider execution"
                .to_owned(),
        ));
    }

    let mut user_message = build_user_message(input.as_slice(), resolved_artifacts.as_slice());
    user_message.provenance = Some(crate::compaction::history::pending_origin(
        &workspace_id,
        &thread_id,
        &turn_id,
        "user-input",
        crate::compaction::history::PendingOriginKind::Input,
        &turn_id,
    ));

    if execution_security_snapshot.is_none() {
        return Err(ChatTurnError::Terminal(
            "missing turn execution security snapshot; refusing to execute turn without resolved sandbox policy".to_owned(),
        ));
    }

    let thinking_item_id = generate_id(TURN_ITEM_ID_LEN);
    let final_identity = if context_session
        .as_ref()
        .is_some_and(|s| s.context.overflow_recovery)
    {
        recovery
            .as_ref()
            .map(|r| format!("{turn_id}:context-recovery:{}", r.attempt_id))
            .unwrap_or_else(|| turn_id.clone())
    } else {
        turn_id.clone()
    };
    let message_item_id = deterministic_final_message_item_id(&final_identity);

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
        ThreadMode::Message => unreachable!("message mode is rejected before provider execution"),
        ThreadMode::Agent => {
            execute_agent_provider_response(
                context_session.as_deref(),
                provider_registry,
                &provider,
                model,
                hook_runtime_context,
                reasoning,
                history,
                user_message.clone(),
                &input,
                &capabilities,
                &workspace_skill_policies,
                &skill_catalog,
                &agent_skill_overlay,
                runtime_environment,
                retained_provider_history,
                execution_window_index,
                preparation_started,
                preparation_started_at_unix_ms,
                execution_checkpoint_context,
                permission_profile,
                execution_security_snapshot,
                force_non_stream,
                disable_tool_calling,
                continue_generation_hint,
                tool_loop_config,
                initial_preparation,
                runtime_generation,
                mcp_tool_provider,
                turn_tool_provider,
                turn_finalization_provider,
                task_tool_provider,
                memory_provider,
                hook_runtime,
                tool_bundle_artifacts,
                permission_approval_broker,
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
        }
        ThreadMode::Chat => {
            let result = execute_standard_provider_response(
                context_session.as_deref(),
                &provider,
                model,
                history,
                user_message,
                reasoning,
                None,
                true,
                force_non_stream,
                &workspace_id,
                &thread_id,
                &turn_id,
                &thinking_item_id,
                &message_item_id,
                tool_loop_config.provider,
                None,
                event_tx.clone(),
            )
            .await;

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

async fn await_context_request<T>(
    session: Option<&crate::compaction::controller::NativeContextSession>,
    future: impl std::future::Future<Output = Result<T, ChatTurnError>>,
) -> Result<T, ChatTurnError> {
    let Some(session) = session else {
        return future.await;
    };
    let work = async {
        if let Some(deadline) = session.provider_deadline() {
            tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| {
                    ChatTurnError::Blocked(
                        "context recovery deadline exceeded during provider retry".into(),
                    )
                })?
        } else {
            future.await
        }
    };
    tokio::select! { biased;
        _ = session.context.cancellation.cancelled() => Err(ChatTurnError::Blocked("native model request cancelled".into())),
        result = work => result,
    }
}

async fn execute_standard_provider_response(
    context_session: Option<&crate::compaction::controller::NativeContextSession>,
    provider: &Arc<dyn Provider>,
    model: String,
    history: Vec<ChatMessage>,
    user_message: ChatMessage,
    reasoning: Option<ReasoningConfig>,
    compiled_prompt: Option<CompiledPromptPayload>,
    apply_chat_attachment_policy: bool,
    force_non_stream: bool,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
    accounting: Option<&mut ChatExecutionWindowStats>,
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
        reasoning,
        compiled_prompt,
    };

    let (request, receipt) = if let Some(session) = context_session {
        let prepared = session
            .prepare(request, false)
            .await
            .map_err(|e| ChatTurnError::Blocked(format!("context preparation failed: {e}")))?;
        (prepared.request, Some(prepared.receipt))
    } else {
        (request, None)
    };
    let work = async {
        if provider.capabilities().streaming && !force_non_stream {
            provider::stream_provider_response_accounted(
                provider,
                request,
                workspace_id,
                thread_id,
                turn_id,
                thinking_item_id,
                message_item_id,
                provider_timeout_policy,
                event_tx.as_ref(),
                accounting,
            )
            .await
        } else {
            provider::non_stream_provider_response_with_policy(
                provider,
                request,
                workspace_id,
                thread_id,
                turn_id,
                thinking_item_id,
                message_item_id,
                provider_timeout_policy,
                event_tx.as_ref(),
                accounting,
            )
            .await
        }
    };
    let (text, usage) = await_context_request(context_session, work).await?;
    if let (Some(session), Some(receipt)) = (context_session, receipt) {
        session
            .record_usage(receipt, usage.as_ref())
            .map_err(|e| ChatTurnError::Blocked(format!("context usage unavailable: {e}")))?;
    }
    Ok(text)
}

async fn materialize_mcp_tooling(
    provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    workspace_id: &str,
    turn_id: &str,
    explicit_servers: &[AgentMcpServerRef],
    explicit_tools: &[AgentMcpToolRef],
) -> Result<AgentMcpMaterialization, AgentMcpMaterializationError> {
    let Some(provider) = provider else {
        if explicit_servers.is_empty() && explicit_tools.is_empty() {
            return Ok(AgentMcpMaterialization::default());
        }
        let summary = unavailable_mcp_provider_summary(explicit_servers, explicit_tools);
        return Err(AgentMcpMaterializationError {
            reason: AgentMcpMaterializationFailureReason::ProviderUnavailable,
            message: "explicit MCP capabilities cannot be resolved for this turn".to_owned(),
            diagnostics: vec![AgentMcpResolutionDiagnostic {
                code: "mcp.resolution.provider_unavailable".to_owned(),
                message: "MCP resolution is unavailable for this turn.".to_owned(),
            }],
            accepted_capabilities: Vec::new(),
            rejected_capabilities: summary.rejected,
        });
    };
    provider
        .materialize_mcp_tools(AgentMcpMaterializationRequest {
            workspace_id: workspace_id.to_owned(),
            turn_id: turn_id.to_owned(),
            explicit_servers: explicit_servers.to_vec(),
            explicit_tools: explicit_tools.to_vec(),
        })
        .await
}

async fn materialize_task_tooling(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<TaskToolMaterialization, String> {
    let Some(provider) = provider else {
        return Ok(TaskToolMaterialization::default());
    };
    provider
        .materialize_task_tools(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
}

async fn materialize_turn_tooling(
    provider: Option<&Arc<dyn TurnToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<TurnToolMaterialization, String> {
    let Some(provider) = provider else {
        return Ok(TurnToolMaterialization::default());
    };
    provider
        .materialize_turn_tools(TurnToolContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
}

#[allow(dead_code)]
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
    let details = json!({
        "taskIds": observations.iter().map(|observation| observation.task_id.clone()).collect::<Vec<_>>(),
        "observations": payload,
    });

    Some(RenderedReviewRequiredObservation {
        signatures,
        observations,
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

async fn attached_task_finalization_snapshot(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TaskFinalizationSnapshot>, String> {
    let Some(provider) = provider else {
        return Ok(None);
    };
    provider
        .attached_task_finalization_snapshot(TaskTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .await
        .map(Some)
}

async fn wait_for_attached_task_finalization_snapshot(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TaskFinalizationSnapshot>, String> {
    let Some(provider) = provider else {
        return Ok(None);
    };
    provider
        .wait_for_attached_task_finalization_snapshot(
            TaskTurnContext {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
            },
            ATTACHED_TASK_FINALIZATION_WAIT_MS,
        )
        .await
        .map(Some)
}

fn render_review_required_snapshot(
    observations: &[ReviewRequiredTaskObservation],
) -> Option<RenderedReviewRequiredObservation> {
    let mut observations = observations.to_vec();
    observations.truncate(MAX_REVIEW_REQUIRED_TASK_OBSERVATIONS);
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
    Some(RenderedReviewRequiredObservation {
        signatures,
        observations,
        details: json!({
            "taskIds": payload.iter().filter_map(|value| value.get("taskId").and_then(JsonValue::as_str)).collect::<Vec<_>>(),
            "observations": payload,
        }),
    })
}

fn render_pending_snapshot(pending: &[PendingAttachedTask]) -> Option<JsonValue> {
    if pending.is_empty() {
        return None;
    }
    Some(json!({
        "pendingCount": pending.len(),
        "tasks": pending.iter().take(20).map(|task| json!({
            "taskId": task.task_id, "runId": task.run_id,
            "title": bounded_task_text(&task.title, 400), "status": task.status,
        })).collect::<Vec<_>>(),
        "additionalTasksRequireAuthoritativeRead": pending.len() > 20,
    }))
}

fn render_terminal_snapshot(
    observations: &[TerminalTaskObservation],
    observed_task_ids: &BTreeSet<String>,
) -> Option<RenderedTaskObservation> {
    let mut observations = observations
        .iter()
        .filter(|observation| !observed_task_ids.contains(observation.task_id.as_str()))
        .take(MAX_TERMINAL_TASK_OBSERVATIONS)
        .cloned()
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
    Some(RenderedTaskObservation {
        task_ids,
        details: json!({
            "taskIds": observations.iter().map(|observation| observation.task_id.clone()).collect::<Vec<_>>(),
            "observations": payload,
        }),
    })
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

    Some(RenderedTaskObservation {
        task_ids,
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

fn workdir_from_execution_security_snapshot(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<PathBuf, ChatTurnError> {
    snapshot
        .map(|snapshot| PathBuf::from(snapshot.sandbox.cwd.as_str()))
        .ok_or_else(|| {
            ChatTurnError::Terminal("turn execution security snapshot is required".to_owned())
        })
}

/// Completed initial outputs are distinct from per-window runtime handles.
/// Initial phases end at this boundary before provider rounds begin; durable
/// restoration binds their outputs separately from the ephemeral event receiver.
#[derive(Clone)]
pub(crate) struct WarmNativeInitialTools {
    bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    skills: Option<Arc<skill_tools::SkillToolMaterialization>>,
}

struct PreparedNativeInitialTurn {
    outputs: crate::native_initial::AcceptedNativeInitialOutputs,
    extension_bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    skill_tool_materialization: Option<Arc<skill_tools::SkillToolMaterialization>>,
    tools: Option<pioneer_tools::BuiltinTools>,
}

async fn prepare_native_initial_turn(
    provider_registry: Arc<ProviderRegistry>,
    provider: &Arc<dyn Provider>,
    model: &str,
    hook_context: &AgentTurnHookContext,
    hook_runtime_context: &AgentTurnHookRuntimeContext,
    user_input_text: String,
    input: &[UserInput],
    capabilities: &[TurnCapability],
    workspace_skill_policies: &HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    skill_catalog: &pioneer_skills::SkillCatalogSnapshot,
    agent_skill_overlay: &[pioneer_skills::AgentSkillRuntimeEntry],
    runtime_environment: HashMap<String, String>,
    history: &[ChatMessage],
    execution_window_index: u32,
    execution_checkpoint_context: Option<&ExecutionCheckpointContext>,
    permission_profile: &TurnPermissionProfileSnapshot,
    execution_security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
    continue_generation_hint: bool,
    tool_loop_config: &ToolLoopConfig,
    provider_tool_calling: bool,
    native_file_tool_capability: NativeFileToolCapability,
    mcp_tool_provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<&Arc<dyn TurnToolProvider>>,
    task_tool_provider: Option<&Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<&Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<&Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<dyn PermissionApprovalBroker>,
    recovering: bool,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    event_tx: &Arc<AgentEventHub>,
    admission_clock: &chrono::DateTime<chrono::FixedOffset>,
) -> Result<PreparedNativeInitialTurn, ChatTurnError> {
    let workdir = workdir_from_execution_security_snapshot(execution_security_snapshot)?;
    let filesystem_prompt_snapshot = execution_security_snapshot.cloned();
    let effective_policy_set = run_agent_turn_policy_hook_phase(
        hook_runtime,
        hook_context,
        TurnPrePolicyHookInput::from_parts(
            user_input_text.clone(),
            Some(model.to_owned()),
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
        hook_runtime,
        hook_context,
        &effective_policy_set,
        TurnPrePromptContextHookInput::from_parts(
            user_input_text.clone(),
            Some(model.to_owned()),
            Some(provider.name().to_owned()),
        ),
    )
    .await
    .map_err(|_| {
        ChatTurnError::Terminal("required prompt-context preparation failed".to_owned())
    })?;

    let normalized_capabilities = normalize_turn_capabilities(capabilities);

    let mcp_materialization = if provider_tool_calling {
        match materialize_mcp_tooling(
            mcp_tool_provider,
            workspace_id,
            turn_id,
            normalized_capabilities.mcp_server_refs.as_slice(),
            normalized_capabilities.mcp_tool_refs.as_slice(),
        )
        .await
        {
            Ok(materialization) => materialization,
            Err(failure) => {
                for diagnostic in &failure.diagnostics {
                    warn!(
                        thread_id,
                        turn_id,
                        reason = ?failure.reason,
                        code = diagnostic.code.as_str(),
                        diagnostic = diagnostic.message.as_str(),
                        "MCP turn projection preflight failed"
                    );
                }
                let accepted_capabilities = failure.accepted_capabilities.clone();
                let mut rejected_capabilities = normalized_capabilities.rejected.clone();
                rejected_capabilities.extend(failure.rejected_capabilities.clone());
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
                emit_mcp_resolution_diagnostics(
                    event_tx.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                    failure.diagnostics.as_slice(),
                )
                .await?;
                return Err(ChatTurnError::Blocked(failure.message));
            }
        }
    } else {
        AgentMcpMaterialization::default()
    };
    for diagnostic in &mcp_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            code = diagnostic.code.as_str(),
            diagnostic = diagnostic.message.as_str(),
            "MCP dynamic tool materialization reported diagnostic"
        );
    }
    let projected_mcp_availability = AgentMcpAvailability {
        available_mcp: mcp_materialization.available_mcp.clone(),
        blocked_mcp: mcp_materialization.blocked_mcp.clone(),
    };

    let skills_resolution = match skills::resolve_turn_skills_with_explicit_refs(
        input,
        normalized_capabilities.skill_refs.as_slice(),
        skill_catalog,
        readable_agent_skill_overlay(agent_skill_overlay, provider_tool_calling),
        &tool_loop_config.skills,
        workspace_skill_policies,
        &projected_mcp_availability,
    ) {
        Ok(resolution) => resolution,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "combined MCP and skill preflight failed while resolving skills"
            );
            let accepted_capabilities = mcp_materialization.accepted_capabilities.clone();
            let mut rejected_capabilities = normalized_capabilities.rejected.clone();
            rejected_capabilities.extend(mcp_materialization.rejected_capabilities.clone());
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
            emit_mcp_resolution_diagnostics(
                event_tx.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
                mcp_materialization.diagnostics.as_slice(),
            )
            .await?;
            return Err(ChatTurnError::Blocked(format!(
                "combined MCP and skill preflight failed: {error:#}"
            )));
        }
    };
    if let Some(error) = skills_resolution.agent_overlay_error.as_deref() {
        warn!(
            workspace_id,
            thread_id,
            turn_id,
            error,
            "Agent skill overlay was rejected; continuing with ordinary skills only"
        );
    }

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

    let skill_capability_summary = resolve_skill_capability_summary(
        normalized_capabilities.skill_refs.as_slice(),
        &skills_resolution,
    );

    let unsupported_mcp_summary = if provider_tool_calling {
        TurnCapabilityResolutionSummary::default()
    } else {
        unsupported_mcp_capability_summary(
            normalized_capabilities.mcp_server_refs.as_slice(),
            normalized_capabilities.mcp_tool_refs.as_slice(),
        )
    };
    let mut accepted_capabilities = skill_capability_summary.accepted.clone();
    accepted_capabilities.extend(mcp_materialization.accepted_capabilities.clone());
    let mut rejected_capabilities = normalized_capabilities.rejected.clone();
    rejected_capabilities.extend(skill_capability_summary.rejected.clone());
    rejected_capabilities.extend(mcp_materialization.rejected_capabilities.clone());
    rejected_capabilities.extend(unsupported_mcp_summary.rejected);
    if !rejected_capabilities.is_empty() {
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
        emit_mcp_resolution_diagnostics(
            event_tx.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            mcp_materialization.diagnostics.as_slice(),
        )
        .await?;
        return Err(ChatTurnError::Blocked(
            "combined MCP and skill preflight rejected one or more explicit capabilities"
                .to_owned(),
        ));
    }

    if let Some(persistence_request) = mcp_materialization.persistence.clone() {
        let Some(mcp_tool_provider) = mcp_tool_provider else {
            return Err(ChatTurnError::Blocked(
                "resolved MCP projection has no persistence owner".to_owned(),
            ));
        };
        let expected_manifest_hash = persistence_request.manifest_hash.clone();
        let expected_tool_count = persistence_request.bindings.len();
        let persisted = mcp_tool_provider
            .persist_mcp_projection(persistence_request)
            .await
            .map_err(|error| {
                warn!(
                    thread_id,
                    turn_id,
                    error = %error,
                    "turn MCP projection persistence failed before provider start"
                );
                ChatTurnError::Blocked(format!(
                    "failed to persist resolved MCP projection: {error}"
                ))
            })?;
        if persisted.turn_id != turn_id
            || persisted.manifest_hash != expected_manifest_hash
            || persisted.tool_count != expected_tool_count
        {
            return Err(ChatTurnError::Blocked(
                "persisted MCP projection acknowledgement did not match the resolved projection"
                    .to_owned(),
            ));
        }
    }

    if !provider_tool_calling {
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
        emit_mcp_resolution_diagnostics(
            event_tx.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            mcp_materialization.diagnostics.as_slice(),
        )
        .await?;
    }
    let capability_diagnostics = capability_manifest_diagnostics(rejected_capabilities.as_slice());

    let mut skills_prompt = normalize_optional_prompt(Some(skills_resolution.prompt.clone()));

    let task_materialization = if provider_tool_calling {
        materialize_task_tooling(task_tool_provider, workspace_id, thread_id, turn_id)
            .await
            .map_err(|_| {
                ChatTurnError::Blocked("required task tooling could not be materialized".to_owned())
            })?
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

    // agent domain root execution admission is part of Agent response
    // admission, not an optional consequence of native tool calling. The
    // provider materializes the durable responding execution here even for a
    // model/backend that cannot receive tools, and any admission failure must
    // stop before the first external provider request.
    let turn_tool_materialization =
        materialize_turn_tooling(turn_tool_provider, workspace_id, thread_id, turn_id)
            .await
            .map_err(|error| {
                ChatTurnError::Terminal(format!(
                    "failed to admit durable Agent execution before provider start: {error}"
                ))
            })?;
    for diagnostic in &turn_tool_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "turn tool materialization reported diagnostic"
        );
    }

    if !provider_tool_calling {
        if !agent_skill_overlay.is_empty() {
            if recovering {
                return Err(ChatTurnError::Terminal(
                    "pinned Agent skills cannot be recovered without native tool calling"
                        .to_owned(),
                ));
            }
            emit_turn_skills_resolved_event(
                event_tx.as_ref(),
                thread_id,
                turn_id,
                skills::to_turn_skill_bindings(skills_resolution.result.active.as_slice(), &[]),
                true,
            )
            .await?;
        }

        let effective_tool_bundle_set = run_agent_turn_tool_materialization_hook_phase(
            hook_runtime,
            hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
            Vec::new(),
            tool_bundle_artifacts,
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

        if !effective_tool_bundle_set.bundles().is_empty() {
            return Err(ChatTurnError::Blocked(
                "accepted hook tools require native tool calling".to_owned(),
            ));
        }

        let turn_preflight = run_agent_turn_preflight_stage(
            event_tx.clone(),
            provider_registry.clone(),
            provider.clone(),
            model,
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
            execution_window_index,
            hook_runtime_context,
            user_input_text.as_str(),
        )
        .await;

        ensure_preflight_usage_committed(turn_preflight.publication_failure)?;
        trace_agent_turn_preflight(thread_id, turn_id, &turn_preflight, &[]);

        if let Some(active_recall_input) = preflight_active_recall_prompt_context_input(
            user_input_text.as_str(),
            model,
            provider.name(),
            &turn_preflight,
        ) {
            effective_prompt_context_set = run_agent_turn_post_preflight_prompt_context_hook_phase(
                hook_runtime,
                hook_context,
                &effective_policy_set,
                &effective_prompt_context_set,
                active_recall_input,
            )
            .await
            .map_err(|_| {
                ChatTurnError::Terminal(
                    "required post-preflight context preparation failed".to_owned(),
                )
            })?;
        }

        let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
            hook_runtime,
            hook_context,
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
        let prior_visible_assistant_text =
            prior_visible_assistant_text_for_execution_continuation(history);
        let prompt_runtime_sections = runtime_sections_with_execution_continuation_context(
            prompt_sections.runtime_sections.as_slice(),
            execution_window_index,
            execution_checkpoint_context,
            prior_visible_assistant_text.as_deref(),
            turn_tool_materialization.initiating_thread_id.as_deref(),
        );
        let prompt_runtime_sections = runtime_sections_with_artifact_reference_policy(
            prompt_runtime_sections,
            history_or_prompt_context_has_artifact_refs(history, &effective_prompt_context_set),
        )?;
        let prompt_runtime_sections = append_native_filesystem_capability_section(
            prompt_runtime_sections,
            &native_file_tool_capability,
            filesystem_prompt_snapshot.as_ref(),
        )?;

        let initial_instruction_plan = compile_agent_instruction_delivery_plan_with_captured_clock(
            tool_loop_config.computer_use.runtime_home_dir.as_path(),
            skills_prompt.clone(),
            None,
            prompt_runtime_sections.as_slice(),
            permission_profile,
            include_task_orchestration_policy,
            false,
            continue_generation_hint,
            turn_tool_materialization.initiating_thread_id.as_deref(),
            thread_id,
            turn_id,
            admission_clock,
        )?;

        run_agent_turn_notification_hook_phase(
            hook_runtime,
            hook_context,
            HookPhase::TurnPostPromptCompile,
            &effective_policy_set,
            &effective_prompt_context_set,
        )
        .await
        .map_err(|_| ChatTurnError::Terminal("required post-prompt hook failed".to_owned()))?;

        emit_durable_event(
            event_tx.as_ref(),
            AgentDurableEvent::PromptManifestCompiled {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                manifest: prompt_manifest_from_bundle(
                    &initial_instruction_plan.bundle,
                    &EffectiveTurnPromptManifestHookMetadata::combined(
                        effective_prompt_context_set.manifest_metadata(),
                        effective_prompt_section_set.manifest_metadata(),
                    ),
                    capability_diagnostics.as_slice(),
                ),
            },
        )
        .await?;

        return Ok(PreparedNativeInitialTurn {
            outputs: crate::native_initial::AcceptedNativeInitialOutputs {
                native_agent_action_contract: None,
                mcp_descriptors: Vec::new(),
                mcp_manifest_hash: None,
                policy: effective_policy_set,
                prompt_context: effective_prompt_context_set,
                prompt_sections: effective_prompt_section_set,
                instructions: initial_instruction_plan,
                capability_diagnostics,
                skill_plan: skills_resolution.runtime_plan,
                tool_metadata: effective_tool_bundle_set.metadata().clone(),
                extension_specs: Vec::new(),
                provider_tool_calling,
                initiating_thread_id: turn_tool_materialization.initiating_thread_id,
                visible_tools: Vec::new(),
                core_tools: Vec::new(),
                selected_optional_tools: Vec::new(),
                requested_tools: Vec::new(),
                preflight_usage: turn_preflight.usage,
                config: crate::native_initial::checked_config_clone(&tool_loop_config).map_err(
                    |_| {
                        ChatTurnError::Blocked(
                            "accepted initial configuration exceeds execution allowance".to_owned(),
                        )
                    },
                )?,
            },
            extension_bundles: Vec::new(),
            skill_tool_materialization: None,
            tools: None,
        });
    }

    let skill_tool_materialization = Arc::new(skill_tools::materialize_skill_tooling(
        &skills_resolution.runtime_plan,
        &tool_loop_config.skills,
    ));

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
        hook_runtime,
        hook_context,
        &effective_policy_set,
        &effective_prompt_context_set,
        tool_bundle_contributions,
        tool_bundle_artifacts,
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

    let extension_specs =
        crate::native_initial::checked_extension_specs(effective_tool_bundle_set.bundles())
            .map_err(|_| {
                ChatTurnError::Blocked(
                    "accepted initial tool catalog exceeds execution allowance".to_owned(),
                )
            })?;
    let extension_bundles = effective_tool_bundle_set.bundles().to_vec();

    let permission_context = PermissionEvaluationContext::for_turn(
        workspace_id,
        thread_id,
        turn_id,
        permission_profile.clone(),
    );
    let runtime_environment = runtime_environment.into_iter().collect::<BTreeMap<_, _>>();
    let tools = build_tools_with_environment_and_security_snapshot(
        workdir.clone(),
        turn_id.to_owned(),
        permission_context.clone(),
        tool_loop_config.web.clone(),
        tool_loop_config.computer_use.clone(),
        extension_bundles.clone(),
        runtime_environment,
        execution_security_snapshot.cloned(),
    )
    .map_err(|_| {
        ChatTurnError::Blocked("required tool runtime could not be constructed".to_owned())
    })?
    .with_permission_approval_broker(permission_approval_broker);

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
    emit_mcp_resolution_diagnostics(
        event_tx.as_ref(),
        workspace_id,
        thread_id,
        turn_id,
        mcp_materialization.diagnostics.as_slice(),
    )
    .await?;

    // One trusted provider capability decision owns native filesystem-tool
    // visibility for this turn.  Apply it before preflight so the core index,
    // request_tools catalog and final provider schema cannot diverge.
    tools
        .router
        .set_blocked_tool_names(native_file_tool_blocked_names(&native_file_tool_capability));
    tools
        .router
        .set_native_patch_wire_shape(native_file_tool_capability.patch_shape);

    skill_tool_materialization
        .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
        .await;

    let router = tools.router.clone();

    let turn_preflight = run_agent_turn_preflight_stage(
        event_tx.clone(),
        provider_registry,
        provider.clone(),
        model,
        provider_tool_calling,
        &tool_loop_config,
        &effective_policy_set,
        &effective_prompt_context_set,
        router.preflight_tool_index(),
        workspace_id,
        thread_id,
        turn_id,
        execution_window_index,
        hook_runtime_context,
        user_input_text.as_str(),
    )
    .await;

    ensure_preflight_usage_committed(turn_preflight.publication_failure)?;
    let mut requested_tool_names = restored_requested_tool_names(
        execution_checkpoint_context,
        workspace_id,
        thread_id,
        turn_id,
    );
    let initial_visibility = compute_agent_turn_final_visible_tools(
        router.as_ref(),
        &turn_preflight,
        &requested_tool_names,
    );
    requested_tool_names.retain(|name| initial_visibility.visible_tools.contains(name));

    warn_final_visible_tool_diagnostics(
        thread_id,
        turn_id,
        initial_visibility.diagnostics.as_slice(),
    );

    let read_skill_is_visible = initial_visibility
        .visible_tools
        .iter()
        .any(|tool_name| tool_name == "read_skill");
    let exposed_agent_overlay = if skills_resolution.agent_overlay_error.is_none()
        && agent_skill_cards_have_read_path(agent_skill_overlay, read_skill_is_visible)
    {
        readable_agent_skill_overlay(agent_skill_overlay, provider_tool_calling)
    } else {
        if skills_resolution.agent_overlay_error.is_none() && !agent_skill_overlay.is_empty() {
            let base_resolution = skills::resolve_turn_skills_with_explicit_refs(
                input,
                normalized_capabilities.skill_refs.as_slice(),
                skill_catalog,
                &[],
                &tool_loop_config.skills,
                workspace_skill_policies,
                &projected_mcp_availability,
            )
            .map_err(|error| {
                ChatTurnError::Terminal(format!(
                    "failed to rebuild ordinary skills after Agent read_skill was unavailable: \
                     {error:#}"
                ))
            })?;
            skills_prompt = normalize_optional_prompt(Some(base_resolution.prompt));
            warn!(
                workspace_id,
                thread_id,
                turn_id,
                "read_skill was absent from the final native tool plan; Agent cards were removed"
            );
        }
        &[]
    };

    if recovering && !agent_skill_overlay.is_empty() && exposed_agent_overlay.is_empty() {
        return Err(ChatTurnError::Terminal(
            "pinned Agent skills are unavailable in the recovered turn's final tool plan"
                .to_owned(),
        ));
    }

    emit_turn_skills_resolved_event(
        event_tx.as_ref(),
        thread_id,
        turn_id,
        skills::to_turn_skill_bindings(
            skills_resolution.result.active.as_slice(),
            exposed_agent_overlay,
        ),
        !agent_skill_overlay.is_empty(),
    )
    .await?;

    trace_agent_turn_preflight(
        thread_id,
        turn_id,
        &turn_preflight,
        initial_visibility.visible_tools.as_slice(),
    );

    if let Some(active_recall_input) = preflight_active_recall_prompt_context_input(
        user_input_text.as_str(),
        model,
        provider.name(),
        &turn_preflight,
    ) {
        effective_prompt_context_set = run_agent_turn_post_preflight_prompt_context_hook_phase(
            hook_runtime,
            hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
            active_recall_input,
        )
        .await
        .map_err(|_| {
            ChatTurnError::Terminal("required post-preflight context preparation failed".to_owned())
        })?;
    }

    let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
        hook_runtime,
        hook_context,
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
    let prior_visible_assistant_text =
        prior_visible_assistant_text_for_execution_continuation(history);
    let prompt_runtime_sections = runtime_sections_with_execution_continuation_context(
        prompt_sections.runtime_sections.as_slice(),
        execution_window_index,
        execution_checkpoint_context,
        prior_visible_assistant_text.as_deref(),
        turn_tool_materialization.initiating_thread_id.as_deref(),
    );
    let prompt_runtime_sections = runtime_sections_with_artifact_reference_policy(
        prompt_runtime_sections,
        history_or_prompt_context_has_artifact_refs(history, &effective_prompt_context_set),
    )?;
    let prompt_runtime_sections = append_native_filesystem_capability_section(
        prompt_runtime_sections,
        &native_file_tool_capability,
        filesystem_prompt_snapshot.as_ref(),
    )?;

    let initial_instruction_plan = compile_agent_instruction_delivery_plan_with_captured_clock(
        tool_loop_config.computer_use.runtime_home_dir.as_path(),
        skills_prompt.clone(),
        None,
        prompt_runtime_sections.as_slice(),
        permission_profile,
        include_task_orchestration_policy,
        true,
        continue_generation_hint,
        turn_tool_materialization.initiating_thread_id.as_deref(),
        thread_id,
        turn_id,
        admission_clock,
    )?;

    run_agent_turn_notification_hook_phase(
        hook_runtime,
        hook_context,
        HookPhase::TurnPostPromptCompile,
        &effective_policy_set,
        &effective_prompt_context_set,
    )
    .await
    .map_err(|_| ChatTurnError::Terminal("required post-prompt hook failed".to_owned()))?;

    emit_durable_event(
        event_tx.as_ref(),
        AgentDurableEvent::PromptManifestCompiled {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            manifest: prompt_manifest_from_bundle(
                &initial_instruction_plan.bundle,
                &EffectiveTurnPromptManifestHookMetadata::combined(
                    effective_prompt_context_set.manifest_metadata(),
                    effective_prompt_section_set.manifest_metadata(),
                ),
                capability_diagnostics.as_slice(),
            ),
        },
    )
    .await?;

    Ok(PreparedNativeInitialTurn {
        outputs: crate::native_initial::AcceptedNativeInitialOutputs {
            native_agent_action_contract: turn_tool_materialization.native_agent_action_contract,
            mcp_manifest_hash: mcp_materialization
                .persistence
                .as_ref()
                .map(|p| p.manifest_hash.clone()),
            mcp_descriptors: mcp_materialization.descriptors,
            policy: effective_policy_set,
            prompt_context: effective_prompt_context_set,
            prompt_sections: effective_prompt_section_set,
            instructions: initial_instruction_plan,
            capability_diagnostics,
            skill_plan: skills_resolution.runtime_plan,
            tool_metadata: effective_tool_bundle_set.metadata().clone(),
            extension_specs,
            provider_tool_calling,
            initiating_thread_id: turn_tool_materialization.initiating_thread_id,
            visible_tools: initial_visibility.visible_tools,
            core_tools: turn_preflight.local_modules.tools.input.core_tools.clone(),
            selected_optional_tools: turn_preflight.plan.tools.visible_tools.clone(),
            requested_tools: requested_tool_names,
            preflight_usage: turn_preflight.usage,
            config: crate::native_initial::checked_config_clone(&tool_loop_config).map_err(
                |_| {
                    ChatTurnError::Blocked(
                        "accepted initial configuration exceeds execution allowance".to_owned(),
                    )
                },
            )?,
        },
        extension_bundles,
        skill_tool_materialization: Some(skill_tool_materialization),
        tools: Some(tools),
    })
}

/// Fixed concrete owners only. Initial hooks, catalog discovery, preflight
/// and root admission never execute while restoring an accepted phase.
async fn restore_native_initial_tool_owners(
    outputs: &crate::native_initial::AcceptedNativeInitialOutputs,
    mcp_provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    turn_provider: Option<&Arc<dyn TurnToolProvider>>,
    task_provider: Option<&Arc<dyn TaskToolProvider>>,
    memory_provider: Option<&Arc<dyn crate::AgentMemoryProvider>>,
    hook_runtime_context: &AgentTurnHookRuntimeContext,
    original_input_text: &str,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<WarmNativeInitialTools, ChatTurnError> {
    let unavailable =
        || ChatTurnError::Blocked("accepted initial tool owner cannot be restored".to_owned());
    let mut mcp_specs = Vec::new();
    let mut memory_specs = Vec::new();
    let mut turn_specs = Vec::new();
    let mut task_specs = Vec::new();
    for (entry, specs) in outputs
        .tool_metadata
        .entries
        .iter()
        .zip(&outputs.extension_specs)
    {
        match (entry.domain.as_str(), entry.bundle_id.as_str()) {
            ("skill", id) if id.starts_with("skill.runtime.bundle.") => (),
            ("turn", id) if id.starts_with("turn.runtime.bundle.") => {
                turn_specs.push(specs.clone())
            }
            ("task", id) if id.starts_with("task.runtime.bundle.") => {
                task_specs.push(specs.clone())
            }
            ("memory", id) if id.starts_with("memory.tool_bundle.bundle.") => {
                memory_specs.push(specs.clone())
            }
            ("mcp", id) if id.starts_with("mcp.runtime.bundle.") => mcp_specs.push(specs.clone()),
            // Unsupported external hook owners never become builtins.
            _ => return Err(unavailable()),
        }
    }
    let skills = Some(Arc::new(skill_tools::materialize_skill_tooling(
        &outputs.skill_plan,
        &outputs.config.skills,
    )));
    let turns = if turn_specs.is_empty() {
        Vec::new()
    } else {
        turn_provider
            .ok_or_else(unavailable)?
            .restore_turn_tools(
                TurnToolContext {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                },
                outputs.native_agent_action_contract.as_deref(),
                &turn_specs,
            )
            .await
            .map_err(|_| unavailable())?
            .bundles
    };
    let tasks = if task_specs.is_empty() {
        Vec::new()
    } else {
        task_provider
            .ok_or_else(unavailable)?
            .restore_task_tools(
                TaskTurnContext {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                },
                &task_specs,
            )
            .await
            .map_err(|_| unavailable())?
            .bundles
    };
    let memory = if memory_specs.is_empty() {
        Vec::new()
    } else {
        memory_provider
            .ok_or_else(unavailable)?
            .restore_memory_tools(
                pioneer_memory::hooks::MemoryTurnContext {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    mode: ThreadMode::Agent,
                    conversation_thread_id: hook_runtime_context.conversation_thread_id.clone(),
                    input_text: original_input_text.to_owned(),
                    task_id: hook_runtime_context.task_id.clone(),
                    agent_id: hook_runtime_context.agent_id.clone(),
                    principal_id: (hook_runtime_context.actor_kind
                        == pioneer_hooks::HookActorKind::User)
                        .then(|| hook_runtime_context.actor_id.clone())
                        .flatten(),
                },
                &memory_specs,
            )
            .await
            .map_err(|_| unavailable())?
            .bundles
    };
    let mcp = if mcp_specs.is_empty() {
        Vec::new()
    } else {
        mcp_provider
            .ok_or_else(unavailable)?
            .restore_mcp_tools(
                workspace_id,
                turn_id,
                outputs
                    .mcp_manifest_hash
                    .as_deref()
                    .ok_or_else(unavailable)?,
                &outputs.mcp_descriptors,
            )
            .await
            .map_err(|_| unavailable())?
    };
    let mut bundles = Vec::new();
    for (entry, original) in outputs
        .tool_metadata
        .entries
        .iter()
        .zip(&outputs.extension_specs)
    {
        let (prefix, owned) = match entry.domain.as_str() {
            "skill" => (
                "skill.runtime.bundle.",
                skills.as_ref().ok_or_else(unavailable)?.bundles.as_slice(),
            ),
            "turn" => ("turn.runtime.bundle.", turns.as_slice()),
            "task" => ("task.runtime.bundle.", tasks.as_slice()),
            "memory" => ("memory.tool_bundle.bundle.", memory.as_slice()),
            "mcp" => ("mcp.runtime.bundle.", mcp.as_slice()),
            _ => return Err(unavailable()),
        };
        let index: usize = entry
            .bundle_id
            .as_str()
            .strip_prefix(prefix)
            .and_then(|n| n.parse().ok())
            .ok_or_else(unavailable)?;
        let bundle = owned.get(index).ok_or_else(unavailable)?;
        let actual = crate::native_initial::checked_extension_specs(std::slice::from_ref(bundle))
            .map_err(|_| unavailable())?;
        // Objects retain their semantic JSON key order independence; arrays
        // and every configured policy/binding remain exact.
        if serde_json::to_value(&actual[0]).map_err(|_| unavailable())?
            != serde_json::to_value(original).map_err(|_| unavailable())?
        {
            return Err(unavailable());
        }
        bundles.push(bundle.clone());
    }
    Ok(WarmNativeInitialTools { bundles, skills })
}

async fn restore_prepared_native_initial_turn(
    outputs: &crate::native_initial::AcceptedNativeInitialOutputs,
    warm: Option<&WarmNativeInitialTools>,
    mcp_provider: Option<&Arc<dyn AgentMcpToolProvider>>,
    turn_provider: Option<&Arc<dyn TurnToolProvider>>,
    task_provider: Option<&Arc<dyn TaskToolProvider>>,
    memory_provider: Option<&Arc<dyn crate::AgentMemoryProvider>>,
    hook_runtime_context: &AgentTurnHookRuntimeContext,
    original_input_text: &str,
    runtime_environment: &HashMap<String, String>,
    permission_profile: &TurnPermissionProfileSnapshot,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
    approval_broker: Arc<dyn PermissionApprovalBroker>,
    native_files: &NativeFileToolCapability,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<PreparedNativeInitialTurn, ChatTurnError> {
    if !outputs.provider_tool_calling {
        return Ok(PreparedNativeInitialTurn {
            outputs: outputs.clone(),
            extension_bundles: Vec::new(),
            skill_tool_materialization: None,
            tools: None,
        });
    }
    let cold;
    let warm = if let Some(warm) = warm {
        warm
    } else {
        cold = restore_native_initial_tool_owners(
            outputs,
            mcp_provider,
            turn_provider,
            task_provider,
            memory_provider,
            hook_runtime_context,
            original_input_text,
            workspace_id,
            thread_id,
            turn_id,
        )
        .await?;
        &cold
    };
    let workdir = workdir_from_execution_security_snapshot(security_snapshot)?;
    let tools = build_tools_with_environment_and_security_snapshot(
        workdir,
        turn_id.to_owned(),
        PermissionEvaluationContext::for_turn(
            workspace_id,
            thread_id,
            turn_id,
            permission_profile.clone(),
        ),
        outputs.config.web.clone(),
        outputs.config.computer_use.clone(),
        warm.bundles.clone(),
        runtime_environment
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
        security_snapshot.cloned(),
    )
    .map_err(|_| ChatTurnError::Blocked("accepted tool runtime cannot be restored".to_owned()))?
    .with_permission_approval_broker(approval_broker);
    tools
        .router
        .set_blocked_tool_names(native_file_tool_blocked_names(native_files));
    tools
        .router
        .set_native_patch_wire_shape(native_files.patch_shape);
    if let Some(skills) = &warm.skills {
        skills
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
    }
    Ok(PreparedNativeInitialTurn {
        outputs: outputs.clone(),
        extension_bundles: warm.bundles.clone(),
        skill_tool_materialization: warm.skills.clone(),
        tools: Some(tools),
    })
}

async fn execute_agent_provider_response(
    context_session: Option<&crate::compaction::controller::NativeContextSession>,
    provider_registry: Arc<ProviderRegistry>,
    provider: &Arc<dyn Provider>,
    model: String,
    hook_runtime_context: AgentTurnHookRuntimeContext,
    reasoning: Option<ReasoningConfig>,
    history: Vec<ChatMessage>,
    user_message: ChatMessage,
    input: &[UserInput],
    capabilities: &[TurnCapability],
    workspace_skill_policies: &HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    skill_catalog: &pioneer_skills::SkillCatalogSnapshot,
    agent_skill_overlay: &[pioneer_skills::AgentSkillRuntimeEntry],
    runtime_environment: HashMap<String, String>,
    retained_provider_history: Vec<RetainedProviderHistoryMessage>,
    execution_window_index: u32,
    preparation_started: Instant,
    preparation_started_at_unix_ms: i64,
    execution_checkpoint_context: Option<ExecutionCheckpointContext>,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    force_non_stream: bool,
    disable_tool_calling: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    initial_preparation: Option<crate::NativeInitialPreparation>,
    runtime_generation: u64,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    memory_provider: Option<Arc<dyn crate::AgentMemoryProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<dyn PermissionApprovalBroker>,
    turn_control: TurnExecutionControl,
    mut recovery: Option<RecoveryAttemptContext>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    initial_thinking_item_id: String,
    message_item_id: &str,
    event_tx: Arc<AgentEventHub>,
) -> Result<ChatTurnOutcome, ChatTurnError> {
    let mut window_stats = ChatExecutionWindowStats::new(execution_window_index);
    window_stats.started_at_unix_ms = preparation_started_at_unix_ms;
    emit_execution_window_started(
        workspace_id,
        thread_id,
        turn_id,
        &window_stats,
        event_tx.as_ref(),
    )
    .await?;
    let tool_loop_config = tool_loop_config.normalized();
    let model_tool_support = provider.model_tool_support(model.as_str());
    if !disable_tool_calling && model_tool_support == pioneer_provider::ModelToolSupport::Unknown {
        return Err(ChatTurnError::Blocked(
            "native Agent requires verified model tool support; capability evidence is unknown"
                .to_owned(),
        ));
    }
    let provider_tool_calling = model_tool_support == pioneer_provider::ModelToolSupport::Supported
        && !disable_tool_calling;
    let native_file_tool_capability = if provider_tool_calling {
        provider.native_file_tool_capability(model.as_str())
    } else {
        NativeFileToolCapability::unavailable(
            provider.name(),
            model.as_str(),
            "provider tool calling is disabled for this turn",
        )
    };
    let post_turn_model = model.clone();
    let post_turn_model_provider = provider.name().to_owned();
    let hook_context = AgentTurnHookContext::with_runtime_context(
        workspace_id,
        thread_id,
        turn_id,
        hook_runtime_context.clone(),
    );
    let user_input_text = user_message.text_content_lossy();

    use crate::native_initial::{AcceptedNativeInitialOutputs, InitialPhase};
    let initial_preparation = initial_preparation.ok_or_else(|| {
        ChatTurnError::Blocked("native initial preparation owner unavailable".to_owned())
    })?;
    let mut initial = initial_preparation.inner.lock().await;
    let reused_initial = matches!(initial.snapshot.state, InitialPhase::Completed { .. });
    let prepared = match &initial.snapshot.state {
        InitialPhase::Preparing => {
            return Err(ChatTurnError::Blocked(
                "initial preparation was interrupted before committed outputs; outcome is unknown"
                    .to_owned(),
            ));
        }
        InitialPhase::Completed { outputs } => {
            if outputs.provider_tool_calling != provider_tool_calling {
                return Err(ChatTurnError::Blocked(
                    "accepted initial tool contract differs".to_owned(),
                ));
            }
            restore_prepared_native_initial_turn(
                outputs,
                initial.warm_tools.as_ref(),
                mcp_tool_provider.as_ref(),
                turn_tool_provider.as_ref(),
                task_tool_provider.as_ref(),
                memory_provider.as_ref(),
                &hook_runtime_context,
                user_input_text.as_str(),
                &runtime_environment,
                &permission_profile,
                execution_security_snapshot.as_ref(),
                permission_approval_broker.clone(),
                &native_file_tool_capability,
                workspace_id,
                thread_id,
                turn_id,
            )
            .await?
        }
        InitialPhase::NotStarted => {
            if execution_window_index != 1 || execution_checkpoint_context.is_some() {
                return Err(ChatTurnError::Blocked(
                    "native continuation initial outputs were not committed".to_owned(),
                ));
            }
            let previous_digest = crate::native_initial::digest(&initial.encoded);
            initial.snapshot.state = InitialPhase::Preparing;
            let preparing = crate::native_initial::encode(&initial.snapshot, initial.byte_budget)
                .map_err(|_| {
                ChatTurnError::Blocked(
                    "native initial preparation exceeds snapshot allowance".to_owned(),
                )
            })?;
            initial.encoded = preparing.clone();
            emit_durable_event(
                event_tx.as_ref(),
                AgentDurableEvent::NativeInitialPhaseAdvanced {
                    preparation: pioneer_protocol::NativeInitialPhaseAdvance {
                        execution_owner_generation: initial_preparation
                            .execution_owner_generation(),
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        window_index: execution_window_index,
                        runtime_generation,
                        expected_sha256: previous_digest,
                        initial_phase_json: preparing,
                    },
                },
            )
            .await?;
            let prepared = prepare_native_initial_turn(
                provider_registry,
                provider,
                model.as_str(),
                &hook_context,
                &hook_runtime_context,
                user_input_text,
                input,
                capabilities,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                runtime_environment.clone(),
                history.as_slice(),
                execution_window_index,
                execution_checkpoint_context.as_ref(),
                &permission_profile,
                execution_security_snapshot.as_ref(),
                continue_generation_hint,
                &tool_loop_config,
                provider_tool_calling,
                native_file_tool_capability.clone(),
                mcp_tool_provider.as_ref(),
                turn_tool_provider.as_ref(),
                task_tool_provider.as_ref(),
                hook_runtime.as_ref(),
                tool_bundle_artifacts.as_ref(),
                permission_approval_broker.clone(),
                recovery.is_some(),
                workspace_id,
                thread_id,
                turn_id,
                &event_tx,
                &initial.snapshot.admission_clock,
            )
            .await?;

            let PreparedNativeInitialTurn {
                outputs,
                extension_bundles,
                skill_tool_materialization,
                tools,
            } = prepared;
            let mut completed = initial.snapshot.clone();
            completed.state = InitialPhase::Completed {
                outputs: Box::new(outputs),
            };
            let encoded =
                crate::native_initial::encode(&completed, initial.byte_budget).map_err(|_| {
                    ChatTurnError::Blocked(
                        "native initial outputs exceed snapshot allowance".to_owned(),
                    )
                })?;
            emit_durable_event(
                event_tx.as_ref(),
                AgentDurableEvent::NativeInitialPhaseAdvanced {
                    preparation: pioneer_protocol::NativeInitialPhaseAdvance {
                        execution_owner_generation: initial_preparation
                            .execution_owner_generation(),
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        window_index: execution_window_index,
                        runtime_generation,
                        expected_sha256: crate::native_initial::digest(&initial.encoded),
                        initial_phase_json: encoded.clone(),
                    },
                },
            )
            .await?;
            initial.snapshot = completed;
            initial.encoded = encoded;
            initial.warm_tools = Some(WarmNativeInitialTools {
                bundles: extension_bundles.clone(),
                skills: skill_tool_materialization.clone(),
            });
            let InitialPhase::Completed { outputs } = &initial.snapshot.state else {
                unreachable!()
            };
            PreparedNativeInitialTurn {
                outputs: outputs.as_ref().clone(),
                extension_bundles,
                skill_tool_materialization,
                tools,
            }
        }
    };
    if initial.warm_tools.is_none() {
        initial.warm_tools = Some(WarmNativeInitialTools {
            bundles: prepared.extension_bundles.clone(),
            skills: prepared.skill_tool_materialization.clone(),
        });
    }
    drop(initial);
    let PreparedNativeInitialTurn {
        outputs,
        tools,
        skill_tool_materialization,
        ..
    } = prepared;
    let AcceptedNativeInitialOutputs {
        policy: effective_policy_set,
        prompt_context: effective_prompt_context_set,
        prompt_sections: effective_prompt_section_set,
        instructions: initial_instruction_plan,
        capability_diagnostics,
        skill_plan: skill_runtime_plan,
        visible_tools: mut visible_tool_names,
        core_tools,
        selected_optional_tools,
        requested_tools: mut requested_tool_names,
        initiating_thread_id,
        preflight_usage,
        config: tool_loop_config,
        ..
    } = outputs;
    // Durable provider-usage receipts already own completed preflight. A new
    // window must not charge those attempts/tokens again.
    let preflight_usage = if reused_initial && execution_window_index > 1 {
        Default::default()
    } else {
        preflight_usage
    };
    let mut semantic = semantic_state::NativeSemanticState::restore(
        execution_checkpoint_context.as_ref(),
        workspace_id,
        thread_id,
        turn_id,
        execution_window_index,
        &tool_loop_config.retry,
        !reused_initial,
        tool_loop_config.no_progress_mode,
    )?;
    let mut native_controls =
        native_control::NativeControlRuntime::restore(semantic.controls.clone(), context_session)
            .await?;
    window_stats.record_preflight(preflight_usage);
    let continuation_sections = runtime_sections_with_execution_continuation_context(
        &[],
        execution_window_index,
        execution_checkpoint_context.as_ref(),
        prior_visible_assistant_text_for_execution_continuation(&history).as_deref(),
        initiating_thread_id.as_deref(),
    );
    let initial_instruction_plan = pioneer_promt::reproject_native_instruction_plan(
        &initial_instruction_plan,
        semantic.applied_retry_instruction.as_deref(),
        provider_tool_calling,
        continuation_sections.first(),
    )
    .map_err(|_| {
        ChatTurnError::Blocked("native continuation instruction projection unavailable".to_owned())
    })?;

    if !provider_tool_calling {
        if !native_controls.state.retained.is_empty() {
            return Err(ChatTurnError::Blocked(
                "non-tool native continuation has incompatible controls".to_owned(),
            ));
        }
        let mut guard = ToolLoopGuard::new_execution_window_with_started_at(
            tool_loop_config.execution_windows.window.clone(),
            "",
            preparation_started,
        );
        guard.record_preflight_provider_usage(
            preflight_usage.attempts,
            preflight_usage.known_tokens,
            preflight_usage.unknown,
        );
        let round_plan = guard
            .begin_provider_round()
            .map_err(ChatTurnError::Terminal)?;
        if let Some(budget_exceeded) = round_plan.budget_exceeded {
            let mut continuation = build_budget_execution_window_continuation(
                workspace_id,
                thread_id,
                turn_id,
                input,
                &post_turn_model,
                &post_turn_model_provider,
                &window_stats,
                &budget_exceeded,
                Default::default(),
                &[],
            );
            continuation.checkpoint_payload.native_semantic_state = Some(semantic.to_value()?);
            emit_execution_window_exhausted_and_checkpointed(
                workspace_id,
                thread_id,
                turn_id,
                &window_stats,
                &continuation,
                event_tx.as_ref(),
            )
            .await?;
            return Ok(ChatTurnOutcome::NeedsContinuation(continuation));
        }

        semantic.checkpoint_revision = semantic
            .checkpoint_revision
            .checked_add(1)
            .ok_or_else(|| ChatTurnError::Blocked("semantic revision exhausted".to_owned()))?;
        persist_native_semantic_checkpoint(
            event_tx.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            input,
            &post_turn_model,
            &post_turn_model_provider,
            &window_stats,
            Default::default(),
            &requested_tool_names,
            semantic.to_value()?,
            None,
            initial_preparation.execution_owner_generation(),
            runtime_generation,
        )
        .await?;
        let result = execute_standard_provider_response(
            context_session,
            provider,
            model,
            history,
            user_message.clone(),
            reasoning,
            Some(compiled_prompt_payload_from_delivery_plan(
                &initial_instruction_plan,
            )),
            false,
            force_non_stream,
            workspace_id,
            thread_id,
            turn_id,
            initial_thinking_item_id.as_str(),
            message_item_id,
            tool_loop_config.provider,
            Some(&mut window_stats),
            event_tx.clone(),
        )
        .await;

        match result {
            Ok(assistant_text) => {
                let summary = AgentTurnPostTurnSummary::succeeded_with_model(
                    Some(post_turn_model.clone()),
                    Some(post_turn_model_provider.clone()),
                    extract_user_text(input),
                    assistant_text,
                    Vec::new(),
                    Vec::new(),
                );
                return Ok(ChatTurnOutcome::Completed {
                    post_turn_dispatch: Some(AgentTurnPostTurnHookDispatch::new(
                        hook_context,
                        effective_policy_set,
                        effective_prompt_context_set,
                        summary,
                    )),
                });
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

    let tools = tools.ok_or_else(|| {
        ChatTurnError::Blocked("prepared native tool runtime is unavailable".to_owned())
    })?;
    let skill_tool_materialization = skill_tool_materialization.ok_or_else(|| {
        ChatTurnError::Blocked("prepared skill runtime is unavailable".to_owned())
    })?;
    let runtime = tools.runtime.clone();
    let router = tools.router.clone();
    if reused_initial {
        requested_tool_names = restored_requested_tool_names(
            execution_checkpoint_context.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
        );
        visible_tool_names = router
            .compute_final_visible_tools(
                &core_tools,
                &selected_optional_tools,
                &requested_tool_names,
            )
            .visible_tools;
    }
    let runtime_tool_index = Arc::new(
        skill_runtime_plan
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

    let pending_tool_ui = Arc::new(Mutex::new(HashMap::<String, PendingToolUiState>::new()));

    let mut tool_event_rx = tools
        .event_bus
        .take_durable_receiver()
        .expect("native chat owns the tool lifecycle durable receiver");

    let mut tool_progress_rx = tools.event_bus.subscribe();

    let event_tx_for_tools = event_tx.clone();
    let tool_ui_for_events = pending_tool_ui.clone();
    let workspace_id_for_tools = workspace_id.to_owned();
    let thread_id_for_tools = thread_id.to_owned();
    let turn_id_for_tools = turn_id.to_owned();

    let tool_event_forwarder = tokio::spawn(async move {
        let mut durable_open = true;
        while durable_open {
            tokio::select! {
                biased;
                event = tool_event_rx.recv(), if durable_open => {
                    match event {
                        Some(envelope) => {
                            let result = tooling::forward_tool_event_to_agent(
                                envelope.event.clone(),
                                &event_tx_for_tools,
                                tool_ui_for_events.clone(),
                                workspace_id_for_tools.as_str(),
                                thread_id_for_tools.as_str(),
                                turn_id_for_tools.as_str(),
                            )
                            .await;
                            envelope.acknowledge(
                                result
                                    .as_ref()
                                    .map(|()| ())
                                    .map_err(ToString::to_string),
                            );
                            result?;
                        }
                        None => durable_open = false,
                    }
                }
                event = tool_progress_rx.recv() => {
                    match event {
                        Ok(event)
                            if matches!(
                                &event.payload,
                                ToolEventPayload::Heartbeat
                            ) =>
                        {
                            tooling::forward_tool_event_to_agent(
                                event,
                                &event_tx_for_tools,
                                tool_ui_for_events.clone(),
                                workspace_id_for_tools.as_str(),
                                thread_id_for_tools.as_str(),
                                turn_id_for_tools.as_str(),
                            )
                            .await?;
                        }
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                    }
                }
            }
        }
        Ok::<(), AgentEventHubError>(())
    });

    let mut provider_history_roots = semantic.provider_history_roots;
    let restored_roots = if provider_history_roots.is_empty() {
        Vec::new()
    } else if let Some(session) = context_session {
        session
            .load_native_history_roots(&provider_history_roots)
            .await
            .map_err(|_| ChatTurnError::Blocked("native history roots unavailable".into()))?
    } else {
        return Err(ChatTurnError::Blocked(
            "native history root owner unavailable".into(),
        ));
    };
    let mut messages = history
        .into_iter()
        .filter(|message| message.role != pioneer_provider::Role::System)
        .collect::<Vec<_>>();

    messages.push(user_message);
    let mut continuation_provider_history = retained_provider_history;
    if !restored_roots.is_empty() {
        continuation_provider_history.retain(|entry| {
            !entry.message.provenance.as_ref().is_some_and(|origin| {
                origin
                    .sources
                    .iter()
                    .any(|source| source.scope.starts_with("checkpoint:"))
            })
        });
        continuation_provider_history.extend(restored_roots.into_iter().enumerate().map(
            |(index, message)| RetainedProviderHistoryMessage {
                sequence: i64::MIN + index as i64,
                message,
            },
        ));
    }
    let mut continuation_provider_history_bytes = None;
    let mut next_provider_history_sequence =
        next_retained_provider_history_sequence(&continuation_provider_history);
    append_recovered_provider_history(&mut messages, continuation_provider_history.clone());

    let mut semantic_checkpoint_revision = semantic.checkpoint_revision;
    let mut pending_retry_instruction = semantic.pending_retry_instruction;
    let mut applied_retry_instruction = semantic.applied_retry_instruction;
    let mut observed_review_required_signatures = semantic.observed_review_required_signatures;
    let mut observed_terminal_task_ids = semantic.observed_terminal_task_ids;
    let window_budget = &tool_loop_config.execution_windows.window;
    let mut tool_loop_guard = ToolLoopGuard::new_execution_window_with_started_at(
        window_budget.clone(),
        tool_loop_final_answer_instruction(),
        preparation_started,
    );
    tool_loop_guard.record_preflight_provider_usage(
        preflight_usage.attempts,
        preflight_usage.known_tokens,
        preflight_usage.unknown,
    );
    let mut tool_retry_controller =
        ToolRetryController::from_checkpoint(tool_loop_config.retry.clone(), semantic.retry)
            .map_err(|_| {
                ChatTurnError::Blocked("native retry checkpoint is incompatible".to_owned())
            })?;
    if tool_retry_controller.is_exhausted() {
        tool_loop_guard
            .request_final_answer_with_instruction(
                pioneer_protocol::NativeHostDirective::ExhaustedTools.instruction(),
            )
            .map_err(ChatTurnError::Blocked)?;
    }
    let restored_no_progress_state = execution_checkpoint_context
        .as_ref()
        .map(|context| context.payload.tool_no_progress.clone())
        .unwrap_or_default();
    let mut tool_no_progress_guard = ToolNoProgressGuard::from_checkpoint_with_mode(
        ToolNoProgressGuardConfig::default(),
        restored_no_progress_state,
        semantic.no_progress_mode,
    );
    tool_no_progress_guard
        .restore_progress_frontier(semantic.progress_frontier)
        .map_err(|message| ChatTurnError::Blocked(message.to_owned()))?;
    let mut tool_retry_lifecycle = ToolRetryLifecycleTracker::from_checkpoint(
        turn_id,
        semantic.retry_lifecycle,
        tool_retry_controller
            .state_snapshot()
            .active_episode
            .is_some(),
    )
    .map_err(|_| ChatTurnError::Blocked("native retry lifecycle is incompatible".to_owned()))?;
    let mut task_mutation_finalization_guard = semantic.task_mutation_guard;
    let mut post_turn_assistant_text = semantic.post_turn_assistant_text;
    let mut post_turn_tool_events = semantic.post_turn_tool_events;
    let mut post_turn_domain_events = semantic.post_turn_domain_events;

    macro_rules! capture_semantic_state {
        () => {
            (|| -> Result<serde_json::Value, ChatTurnError> {
                semantic_checkpoint_revision =
                    semantic_checkpoint_revision.checked_add(1).ok_or_else(|| {
                        ChatTurnError::Blocked("semantic revision exhausted".to_owned())
                    })?;
                semantic_state::capture(
                    semantic_checkpoint_revision,
                    turn_id,
                    &tool_loop_config.retry,
                    &native_controls.state,
                    &provider_history_roots,
                    tool_loop_config.no_progress_mode,
                    tool_no_progress_guard.progress_frontier(),
                    &tool_retry_controller,
                    &tool_retry_lifecycle,
                    &pending_retry_instruction,
                    &applied_retry_instruction,
                    &observed_review_required_signatures,
                    &observed_terminal_task_ids,
                    &task_mutation_finalization_guard,
                    &post_turn_assistant_text,
                    &post_turn_tool_events,
                    &post_turn_domain_events,
                )
            })()
        };
    }

    macro_rules! persist_control {
        ($thinking:expr, $directive:expr, $data:expr) => {{
            let control = native_controls
                .enqueue($directive, $data)
                .map_err(|error| (error, $thinking.clone()))?;
            persist_native_semantic_checkpoint(
                event_tx.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
                input,
                &post_turn_model,
                &post_turn_model_provider,
                &window_stats,
                tool_no_progress_guard.checkpoint_state(),
                &requested_tool_names,
                capture_semantic_state!().map_err(|error| (error, $thinking.clone()))?,
                control,
                initial_preparation.execution_owner_generation(),
                runtime_generation,
            )
            .await
            .map_err(|error| (error, $thinking.clone()))?;
        }};
    }

    let turn_result: Result<AgentProviderLoopOutcome, (ChatTurnError, String)> = async {
        let mut current_thinking_id = initial_thinking_item_id;
        let mut consecutive_rejected_no_tool_rounds = 0usize;
        let mut task_gate_query_failures = 0usize;

        loop {
            persist_native_semantic_checkpoint(
                event_tx.as_ref(), workspace_id, thread_id, turn_id, input,
                &post_turn_model, &post_turn_model_provider, &window_stats,
                tool_no_progress_guard.checkpoint_state(), &requested_tool_names,
                capture_semantic_state!().map_err(|e| (e, current_thinking_id.clone()))?,
                None, initial_preparation.execution_owner_generation(), runtime_generation,
            ).await.map_err(|e| (e, current_thinking_id.clone()))?;
            retain_agent_attachment_messages(&mut messages);

            let task_gate_snapshot = match attached_task_finalization_snapshot(
                task_tool_provider.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
            )
            .await
            {
                Ok(snapshot) => {
                    task_gate_query_failures = 0;
                    snapshot
                }
                Err(error) => {
                    task_gate_query_failures = task_gate_query_failures.saturating_add(1);
                    if task_gate_query_failures >= 3 {
                        return Err((
                            ChatTurnError::Blocked(format!(
                                "attached task finalization remains unavailable after bounded retries: {error}"
                            )),
                            current_thinking_id.clone(),
                        ));
                    }
                    send_reasoning_completed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        current_thinking_id.as_str(),
                        "",
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::RetryTaskStateRead,
                        Some((pioneer_protocol::NativeControlOrigin::RuntimeDiagnostic, json!({"state": "unavailable", "attempt": task_gate_query_failures}))));
                    current_thinking_id = start_reasoning_item(
                        workspace_id,
                        thread_id,
                        turn_id,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    continue;
                }
            };

            let review_observation = task_gate_snapshot
                .as_ref()
                .and_then(|snapshot| render_review_required_snapshot(snapshot.review_required.as_slice()));
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ReviewTaskResults,
                        Some((pioneer_protocol::NativeControlOrigin::TaskService, observation.details)));
                }
            } else {
                sync_review_action_tools_to_observations(&mut visible_tool_names, &[]);
            }

            if let Some(observation) = task_gate_snapshot.as_ref().and_then(|snapshot| {
                render_terminal_snapshot(snapshot.terminal.as_slice(), &observed_terminal_task_ids)
            })
            {
                for task_id in &observation.task_ids {
                    observed_terminal_task_ids.insert(task_id.clone());
                }
                persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ObserveTaskResults,
                    Some((pioneer_protocol::NativeControlOrigin::TaskService, observation.details)));
            }

            let round_plan = tool_loop_guard.begin_provider_round().map_err(|message| {
                (
                    ChatTurnError::Terminal(message),
                    current_thinking_id.clone(),
                )
            })?;

            if round_plan.action == ToolLoopRoundAction::RequestContinuation {
                let budget_exceeded = round_plan.budget_exceeded.clone().ok_or_else(|| {
                    (
                        ChatTurnError::Terminal(
                            "execution window continuation missing budget details".to_owned(),
                        ),
                        current_thinking_id.clone(),
                    )
                })?;
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
                    "",
                    event_tx.as_ref(),
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                let mut continuation = build_budget_execution_window_continuation(
                    workspace_id,
                    thread_id,
                    turn_id,
                    input,
                    post_turn_model.as_str(),
                    post_turn_model_provider.as_str(),
                    &window_stats,
                    &budget_exceeded,
                    tool_no_progress_guard.checkpoint_state(),
                    &requested_tool_names,
                );
                continuation.checkpoint_payload.native_semantic_state = Some(capture_semantic_state!()
                    .map_err(|error| (error, current_thinking_id.clone()))?);
                emit_execution_window_exhausted_and_checkpointed(
                    workspace_id,
                    thread_id,
                    turn_id,
                    &window_stats,
                    &continuation,
                    event_tx.as_ref(),
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                return Ok(AgentProviderLoopOutcome::NeedsContinuation(continuation));
            }

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
                pending_retry_instruction = next_retry_instruction;
            }

            let tool_definitions = if round_plan.tools_enabled {
                router.set_model_visible_tools(&visible_tool_names).await;
                Some(
                    router
                        .model_visible_specs()
                        .await
                        .into_iter()
                        .map(|spec| {
                            native_provider_tool_definition(spec, &native_file_tool_capability)
                        })
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };

            let control_projection = native_controls.projection(workspace_id, thread_id, turn_id)
                .map_err(|error| (error, current_thinking_id.clone()))?;
            control_projection.append_observations(&mut messages)
                .map_err(|error| (error, current_thinking_id.clone()))?;
            let round_plan_with_controls = reproject_agent_instruction_plan(
                &initial_instruction_plan,
                pending_retry_instruction.as_deref(),
                round_plan.tools_enabled,
            ).and_then(|plan| pioneer_promt::reproject_native_control_plan(&plan, &control_projection.instructions)
                .map_err(|_| ChatTurnError::Blocked("native control instruction projection unavailable".to_owned())))
                .map_err(|error| (error, current_thinking_id.clone()))?;
            let round_compiled_prompt = Some(compiled_prompt_payload_from_delivery_plan(&round_plan_with_controls));
            emit_durable_event(event_tx.as_ref(), AgentDurableEvent::PromptManifestCompiled {
                thread_id: thread_id.to_owned(), turn_id: turn_id.to_owned(),
                manifest: prompt_manifest_from_bundle(&round_plan_with_controls.bundle,
                    &EffectiveTurnPromptManifestHookMetadata::combined(
                        effective_prompt_context_set.manifest_metadata(), effective_prompt_section_set.manifest_metadata(),
                    ), capability_diagnostics.as_slice()),
            }).await.map_err(|error| (error, current_thinking_id.clone()))?;
            let post_turn_assistant_text_before_round = post_turn_assistant_text.clone();
            let request = ChatRequest {
                    model: model.clone(),
                    messages: messages.clone(),
                    temperature: Some(0.7),
                    max_tokens: None,
                    tools: tool_definitions,
                    tool_choice: None,
                    parallel_tool_calls: round_plan.tools_enabled.then_some(true),
                    reasoning,
                    compiled_prompt: round_compiled_prompt.clone(),
                            };
            let (request, receipt, covered_controls) = if let Some(session) = context_session {
                let prepared = session.prepare(request, false).await.map_err(|e| (
                    ChatTurnError::Blocked(format!("context preparation failed: {e}")),
                    current_thinking_id.clone(),
                ))?;
                (prepared.request, Some(prepared.receipt), prepared.covered_native_control_ids)
            } else { (request, None, Vec::new()) };
            control_projection.validate_inclusion(&request, &round_compiled_prompt)
                .map_err(|error| (error, current_thinking_id.clone()))?;
            native_controls.sync_after_compaction(&covered_controls)
                .map_err(|error| (error, current_thinking_id.clone()))?;
            if context_session.is_some() {
                provider_history_roots = semantic_state::history_roots_from_request(&request.messages)
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                continuation_provider_history_bytes = Some(synchronize_retained_provider_history(
                    &mut continuation_provider_history, &mut next_provider_history_sequence,
                    workspace_id, thread_id, turn_id, &request.messages,
                ).map_err(|error| (error, current_thinking_id.clone()))?);
            }
            messages = request.messages.clone();
            persist_native_semantic_checkpoint(
                event_tx.as_ref(), workspace_id, thread_id, turn_id, input,
                &post_turn_model, &post_turn_model_provider, &window_stats,
                tool_no_progress_guard.checkpoint_state(), &requested_tool_names,
                capture_semantic_state!().map_err(|e| (e, current_thinking_id.clone()))?,
                None, initial_preparation.execution_owner_generation(), runtime_generation,
            ).await.map_err(|e| (e, current_thinking_id.clone()))?;
            let tokens_before_round = window_stats.provider_token_count.unwrap_or(0);
            let round = await_context_request(context_session, provider::request_agent_round_accounted(
                provider,
                request,
                workspace_id,
                thread_id,
                turn_id,
                current_thinking_id.as_str(),
                force_non_stream,
                tool_loop_config.provider,
                event_tx.as_ref(),
                Some(&mut window_stats),
            ))
            .await
            .map_err(|e| (e, current_thinking_id.clone()))?;

            if let (Some(session), Some(receipt)) = (context_session, receipt) {
                session.record_usage(receipt, round.usage.as_ref()).map_err(|e| (
                    ChatTurnError::Blocked(format!("context usage unavailable: {e}")),
                    current_thinking_id.clone(),
                ))?;
            }

            applied_retry_instruction = pending_retry_instruction.clone();
            native_controls.mark_applied(&control_projection);
            persist_native_semantic_checkpoint(
                event_tx.as_ref(), workspace_id, thread_id, turn_id, input,
                &post_turn_model, &post_turn_model_provider, &window_stats,
                tool_no_progress_guard.checkpoint_state(), &requested_tool_names,
                capture_semantic_state!().map_err(|error| (error, current_thinking_id.clone()))?,
                None, initial_preparation.execution_owner_generation(), runtime_generation,
            ).await.map_err(|error| (error, current_thinking_id.clone()))?;
            if !round.text.trim().is_empty() {
                append_text_fragment(&mut post_turn_assistant_text, round.text.as_str());
            }


            if !round_plan.tools_enabled
                && !round.tool_calls.is_empty()
                && let Some(final_text) =
                    task_mutation_finalization_guard.deterministic_failure_message()
            {
                let final_task_gate_snapshot = match wait_for_attached_task_finalization_snapshot(
                    task_tool_provider.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        task_gate_query_failures = task_gate_query_failures.saturating_add(1);
                        if task_gate_query_failures >= 3 {
                            return Err((
                                ChatTurnError::Blocked(format!(
                                    "attached task finalization remains unavailable after bounded retries: {error}"
                                )),
                                current_thinking_id.clone(),
                            ));
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
                        persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::RetryTaskStateRead,
                            Some((pioneer_protocol::NativeControlOrigin::RuntimeDiagnostic, json!({"state": "unavailable", "attempt": task_gate_query_failures}))));
                        current_thinking_id = start_reasoning_item(
                            workspace_id,
                            thread_id,
                            turn_id,
                            event_tx.as_ref(),
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        consecutive_rejected_no_tool_rounds = 0;
                        continue;
                    }
                };

                if let Some(snapshot) = final_task_gate_snapshot.as_ref() {
                    let has_unobserved_terminal = snapshot.terminal.iter().any(|observation| {
                        !observed_terminal_task_ids.contains(observation.task_id.as_str())
                    });
                    if !snapshot.pending.is_empty()
                        || !snapshot.review_required.is_empty()
                        || has_unobserved_terminal
                    {
                        return Err((
                            ChatTurnError::Blocked(
                                "deterministic finalization was fenced because attached task state still requires observation or action"
                                    .to_owned(),
                            ),
                            current_thinking_id.clone(),
                        ));
                    }
                }

                post_turn_assistant_text = bounded_utf8_suffix(&final_text, semantic_state::MAX_SUMMARY_TEXT_BYTES).to_owned();
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
                if let Some(snapshot) = final_task_gate_snapshot.as_ref() {
                    emit_durable_event(
                        event_tx.as_ref(),
                        AgentDurableEvent::ItemCompleted {
                            notification: ItemCompletedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item: TurnItem::SystemEvent {
                                    id: generate_id(TURN_ITEM_ID_LEN),
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: "Attached task finalization gate snapshot committed before deterministic final answer."
                                        .to_owned(),
                                    code: Some("task.finalization.snapshot".to_owned()),
                                    details: Some(json!({
                                        "revision": snapshot.revision.as_str(),
                                        "pendingCount": snapshot.pending.len(),
                                        "reviewRequiredCount": snapshot.review_required.len(),
                                        "terminalCount": snapshot.terminal.len(),
                                    })),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                }
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
                                phase: Default::default(),
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
                    AgentDurableEvent::TurnFinalizationPrepared {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::AgentMessage {
                                id: message_item_id.to_owned(),
                                text: final_text,
                                phase: Default::default(),
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                        generation: TURN_FINALIZATION_GENERATION,
                        task_finalization_revision: final_task_gate_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.revision.clone()),
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                return Ok(AgentProviderLoopOutcome::Completed);
            }

            match tool_loop_guard.after_provider_round_with_usage(
                round_plan.tools_enabled,
                round.tool_calls.len(),
                round.provider_token_count.or(Some(window_stats.provider_token_count.unwrap_or(0).saturating_sub(tokens_before_round))),
            ) {
                ToolLoopGuardDecision::Continue => {}
                ToolLoopGuardDecision::RequestContinuation { budget_exceeded } => {
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
                    let mut continuation = build_budget_execution_window_continuation(
                        workspace_id,
                        thread_id,
                        turn_id,
                        input,
                        post_turn_model.as_str(),
                        post_turn_model_provider.as_str(),
                        &window_stats,
                        &budget_exceeded,
                        tool_no_progress_guard.checkpoint_state(),
                        &requested_tool_names,
                    );
                    continuation.checkpoint_payload.native_semantic_state = Some(capture_semantic_state!()
                    .map_err(|error| (error, current_thinking_id.clone()))?);
                emit_execution_window_exhausted_and_checkpointed(
                        workspace_id,
                        thread_id,
                        turn_id,
                        &window_stats,
                        &continuation,
                        event_tx.as_ref(),
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                    return Ok(AgentProviderLoopOutcome::NeedsContinuation(continuation));
                }
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

                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_rejected_no_tool_rounds = 0;

                    continue;
                }
            }

            if round.tool_calls.is_empty() {
                if round_plan.tools_enabled && pending_retry_instruction.take().is_some() {
                    consecutive_rejected_no_tool_rounds = 0;
                    continue;
                }

                // The snapshot used to render observations at the start of
                // the round is not sufficient as a terminal gate: a child
                // can transition while the provider is generating its final
                // text.  Take one fresh aggregate read immediately before
                // evaluating review/pending/terminal state and committing
                // the finalization snapshot event.
                let final_task_gate_snapshot = match wait_for_attached_task_finalization_snapshot(
                    task_tool_provider.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                )
                .await
                {
                    Ok(snapshot) => {
                        task_gate_query_failures = 0;
                        snapshot
                    }
                    Err(error) => {
                        task_gate_query_failures = task_gate_query_failures.saturating_add(1);
                        if task_gate_query_failures >= 3 {
                            return Err((
                                ChatTurnError::Blocked(format!(
                                    "attached task finalization remains unavailable after bounded retries: {error}"
                                )),
                                current_thinking_id.clone(),
                            ));
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
                        persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::RetryTaskStateRead,
                        Some((pioneer_protocol::NativeControlOrigin::RuntimeDiagnostic, json!({"state": "unavailable", "attempt": task_gate_query_failures}))));
                        current_thinking_id = start_reasoning_item(
                            workspace_id,
                            thread_id,
                            turn_id,
                            event_tx.as_ref(),
                        )
                        .await
                        .map_err(|error| (error, current_thinking_id.clone()))?;
                        consecutive_rejected_no_tool_rounds = 0;
                        continue;
                    }
                };

                if let Some(observation) = final_task_gate_snapshot
                    .as_ref()
                    .and_then(|snapshot| render_review_required_snapshot(snapshot.review_required.as_slice()))
                {
                    let has_new_signature = observation
                        .signatures
                        .iter()
                        .any(|signature| !observed_review_required_signatures.contains(signature));
                    if !has_new_signature {
                        return Err((
                            ChatTurnError::Blocked(review_required_final_answer_block_message()),
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ReviewTaskResults,
                        Some((pioneer_protocol::NativeControlOrigin::TaskService, observation.details)));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_rejected_no_tool_rounds = 0;
                    continue;
                }

                if let Some(observation) = final_task_gate_snapshot
                    .as_ref()
                    .and_then(|snapshot| render_pending_snapshot(snapshot.pending.as_slice()))
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::AwaitAttachedTasks,
                        Some((pioneer_protocol::NativeControlOrigin::TaskService, observation)));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_rejected_no_tool_rounds = 0;
                    continue;
                }

                if let Some(observation) = final_task_gate_snapshot.as_ref().and_then(|snapshot| {
                    render_terminal_snapshot(snapshot.terminal.as_slice(), &observed_terminal_task_ids)
                })
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ObserveTaskResults,
                        Some((pioneer_protocol::NativeControlOrigin::TaskService, observation.details)));
                    current_thinking_id =
                        start_reasoning_item(workspace_id, thread_id, turn_id, event_tx.as_ref())
                            .await
                            .map_err(|error| (error, current_thinking_id.clone()))?;
                    consecutive_rejected_no_tool_rounds = 0;
                    continue;
                }

                if let Some(snapshot) = final_task_gate_snapshot.as_ref() {
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
                                    message: "Attached task finalization gate snapshot committed before final answer.".to_owned(),
                                    code: Some("task.finalization.snapshot".to_owned()),
                                    details: Some(json!({
                                        "revision": snapshot.revision.as_str(),
                                        "pendingCount": snapshot.pending.len(),
                                        "reviewRequiredCount": snapshot.review_required.len(),
                                        "terminalCount": snapshot.terminal.len(),
                                    })),
                                },
                            },
                        },
                    )
                    .await
                    .map_err(|error| (error, current_thinking_id.clone()))?;
                }

                let deterministic_final_text =
                    task_mutation_finalization_guard.deterministic_failure_message();
                let final_text = deterministic_final_text
                    .clone()
                    .unwrap_or_else(|| round.text.clone());
                let final_answer_rejection = deterministic_final_text
                    .is_none()
                    .then(|| no_tool_final_answer_rejection(final_text.as_str()))
                    .flatten();
                if let Some(rejection) = final_answer_rejection {
                    post_turn_assistant_text = post_turn_assistant_text_before_round.clone();
                    consecutive_rejected_no_tool_rounds += 1;

                    if consecutive_rejected_no_tool_rounds
                        >= MAX_CONSECUTIVE_REJECTED_NO_TOOL_ROUNDS
                    {
                        let provider_failure_class = rejection.provider_failure_class();
                        return Err((
                            ChatTurnError::ProviderFailure {
                                item_id: current_thinking_id.clone(),
                                item_type: TurnItemType::Reasoning,
                                failure: ProviderFailureDetails {
                                    error_reason: None,
                                    request_id: None,
                                    provider: provider.name().to_owned(),
                                    model: model.clone(),
                                    transport: if provider.capabilities().streaming
                                        && !force_non_stream
                                    {
                                        ProviderTransportKind::Stream
                                    } else {
                                        ProviderTransportKind::NonStream
                                    },
                                    class: provider_failure_class,
                                    stage: ProviderFailureStage::Finalize,
                                    http_status: None,
                                    provider_code: Some(rejection.provider_code().to_owned()),
                                    retry_after_ms: None,
                                    is_recoverable_hint: !matches!(
                                        provider_failure_class,
                                        pioneer_protocol::ProviderFailureClass::Unknown
                                    ),
                                    message: Some(rejection.provider_message().to_owned()),
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

                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::RepairFinalAnswer,
                        Some((pioneer_protocol::NativeControlOrigin::RuntimeDiagnostic, json!({"requirement": rejection.recovery_instruction()}))));
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
                            post_turn_assistant_text = post_turn_assistant_text_before_round.clone();
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
                            consecutive_rejected_no_tool_rounds = 0;
                            persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::FinalizationRetry,
                                Some((pioneer_protocol::NativeControlOrigin::ArtifactService, json!({"requirement": instruction}))));
                            continue;
                        }
                        Ok(TurnFinalizationDecision::Fail { message }) => {
                            post_turn_assistant_text = post_turn_assistant_text_before_round.clone();
                            return Err((ChatTurnError::Terminal(message), current_thinking_id.clone()));
                        }
                        Err(error) => {
                            post_turn_assistant_text = post_turn_assistant_text_before_round.clone();
                            return Err((
                                ChatTurnError::Blocked(format!(
                                    "turn finalization outcome is unavailable: {error}"
                                )),
                                current_thinking_id.clone(),
                            ));
                        }
                    }
                }
                if final_text != round.text {
                    post_turn_assistant_text = bounded_utf8_suffix(&final_text, semantic_state::MAX_SUMMARY_TEXT_BYTES).to_owned();
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
                                phase: Default::default(),
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

                provider::persist_completed_response(
                    event_tx.as_ref(), thread_id, turn_id, message_item_id, current_thinking_id.as_str(),
                    &round.text, &round.reasoning, round.provider_replay_state.as_ref(),
                ).await.map_err(|error| (error, current_thinking_id.clone()))?;

                emit_durable_event(
                    event_tx.as_ref(),
                    AgentDurableEvent::TurnFinalizationPrepared {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::AgentMessage {
                                id: message_item_id.to_owned(),
                                text: final_text,
                                phase: Default::default(),
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                        generation: TURN_FINALIZATION_GENERATION,
                        task_finalization_revision: final_task_gate_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.revision.clone()),
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;

                return Ok(AgentProviderLoopOutcome::Completed);
            }

            consecutive_rejected_no_tool_rounds = 0;

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

            let mut assistant_round = ChatMessage::assistant_tool_calls_with_provider_state(
                (!round.text.is_empty()).then_some(round.text.clone()),
                (!round.reasoning.is_empty()).then_some(round.reasoning.clone()),
                round.tool_calls.clone(),
                round.provider_replay_state.clone(),
            );
            let round_id = current_thinking_id.clone();
            let resolved_tool_calls = round
                .tool_calls
                .into_iter()
                .enumerate()
                .map(|(ordinal, provider_call)| {
                    let ordinal = u32::try_from(ordinal).map_err(|_| {
                        ChatTurnError::Terminal(
                            "provider returned more tool calls than Pioneer can identify"
                                .to_owned(),
                        )
                    })?;
                    let item_id = deterministic_tool_item_id(
                        turn_id,
                        round_id.as_str(),
                        ordinal,
                        provider_call.id.as_str(),
                    );
                    Ok((provider_call, item_id, ordinal))
                })
                .collect::<Result<Vec<_>, ChatTurnError>>()
                .map_err(|error| (error, current_thinking_id.clone()))?;
            let provider_round = CanonicalProviderRoundEnvelope {
                version: 1,
                round_id: round_id.clone(),
                termination: round.termination,
                message: assistant_round.clone(),
                calls: resolved_tool_calls
                    .iter()
                    .map(|(call, item_id, ordinal)| ProviderCallIdentity {
                        provider_call_id: call.id.clone(),
                        turn_item_id: item_id.clone(),
                        ordinal: *ordinal,
                    })
                    .collect(),
            };
            persist_provider_history_message(
                event_tx.as_ref(),
                thread_id,
                turn_id,
                current_thinking_id.as_str(),
                &provider_round,
            )
            .await
            .map_err(|error| (error, current_thinking_id.clone()))?;
            assistant_round.provenance = Some(crate::compaction::history::pending_origin(
                workspace_id,thread_id,turn_id,&round_id,crate::compaction::history::PendingOriginKind::Assistant,&round_id,
            ));
            retain_provider_history_message(
                &mut continuation_provider_history,
                &mut next_provider_history_sequence,
                &mut continuation_provider_history_bytes,
                &assistant_round,
            ).map_err(|error| (error, current_thinking_id.clone()))?;
            messages.push(assistant_round);

            let mut pending_tool_calls = std::collections::VecDeque::from(resolved_tool_calls);
            let mut executed_results = Vec::new();
            let mut no_progress_feedback = pioneer_tools::ToolNoProgressFeedback::default();
            while !pending_tool_calls.is_empty() {
                let segment_len = native_tool_batch_segment_len(
                    pending_tool_calls.iter().map(|(call, _, _)| {
                        (call.name.as_str(), call.arguments.as_str())
                    }),
                    |name| router.supports_parallel_reads(name),
                );
                // Admission is computed by the parent after the preceding
                // segment has completed and updated the exact guard state.
                let guarded_tool_calls = pending_tool_calls
                .drain(..segment_len)
                .map(|(model_tool_call, item_id, ordinal)| {
                    let decision = tool_no_progress_guard.preflight(
                        model_tool_call.name.as_str(),
                        model_tool_call.arguments.as_str(),
                    );
                    (model_tool_call, item_id, ordinal, decision)
                })
                .collect::<Vec<_>>();

            let tool_tasks = guarded_tool_calls
                .into_iter()
                .map(|(model_tool_call, item_id, ordinal, no_progress_decision)| {
                    let router = router.clone();
                    let runtime = runtime.clone();
                    let turn_control = turn_control.clone();
                    let pending_tool_ui = pending_tool_ui.clone();
                    let event_tx = event_tx.clone();
                    let runtime_tool_index = runtime_tool_index.clone();
                    let runtime_recheck_policy = runtime_recheck_policy.clone();
                    let turn_tool_provider = turn_tool_provider.clone();
                    let workspace_id = workspace_id.to_owned();
                    let thread_id = thread_id.to_owned();
                    let turn_id = turn_id.to_owned();
                    let task_item_id = item_id.clone();
                    let task_tool_name = model_tool_call.name.clone();
                    let task_provider_call_id = model_tool_call.id.clone();
                    let task_arguments = model_tool_call.arguments.clone();
                    // Tool handlers can execute nested tools. Running each model tool call in its
                    // own task keeps nested dispatch from being polled under the full chat-loop
                    // future, whose execution-window state is intentionally large.
                    let handle = tokio::spawn(async move {
                        let provider_call_id = model_tool_call.id.clone();
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
                        let turn_item_execution_class = router
                            .find_spec(tool_name.as_str())
                            .map(|configured| configured.spec.turn_item_execution_class)
                            .unwrap_or_default();

                        {
                            let mut pending = pending_tool_ui.lock().await;
                            let state = pending.entry(item_id.clone()).or_default();
                            state.tool_name = tool_name.clone();
                            state.arguments = arguments.clone();
                            state.recovery_policy = Some(recovery_policy.clone());
                            state.turn_item_execution_class = turn_item_execution_class;
                            state.output_policy = router
                                .find_spec(tool_name.as_str())
                                .map(|configured| configured.output_policy.clone());
                        }

                        if let ToolNoProgressPreflightDecision::Block { message, .. } =
                            no_progress_decision
                        {
                            let output_policy = router
                                .find_spec(tool_name.as_str())
                                .map(|configured| configured.output_policy.clone());
                            let outcome = ToolOutcome::fatal(
                                ToolErrorClass::NeedsNarrowing,
                                Some(
                                    "This no-progress strategy is exhausted. Use a materially different approach; other tools and strategies remain available."
                                        .to_owned(),
                                ),
                            );
                            {
                                let mut pending = pending_tool_ui.lock().await;
                                pending.remove(item_id.as_str());
                            }
                            emit_durable_event(
                                event_tx.as_ref(),
                                AgentDurableEvent::ItemStarted {
                                    notification: ItemStartedNotification {
                                        workspace_id: workspace_id.clone(),
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        item: tooling::build_started_tool_turn_item(
                                            item_id.clone(),
                                            tool_name.clone(),
                                            arguments.clone(),
                                            Some(recovery_policy.clone()),
                                            turn_item_execution_class,
                                            output_policy.clone(),
                                            None,
                                        ),
                                    },
                                },
                            )
                            .await?;
                            emit_durable_event(
                                event_tx.as_ref(),
                                AgentDurableEvent::ItemCompleted {
                                    notification: ItemCompletedNotification {
                                        workspace_id: workspace_id.clone(),
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        item: tooling::build_failed_tool_turn_item(
                                            item_id.clone(),
                                            tool_name.clone(),
                                            arguments.clone(),
                                            message.clone(),
                                            outcome.clone(),
                                            Some(recovery_policy.clone()),
                                            turn_item_execution_class,
                                            output_policy,
                                            None,
                                        ),
                                    },
                                },
                            )
                            .await?;
                            return Ok(ExecutedToolResult {
                                progress: Default::default(),
                                item_id: item_id.clone(),
                                provider_call_id: provider_call_id.clone(),
                                ordinal,
                                item_type,
                                attempt_number,
                                tool_name: tool_name.clone(),
                                arguments: arguments.clone(),
                                model_visible_text: message.clone(),
                                success: false,
                                outcome: outcome.clone(),
                                recovery_view: None,
                                retained_llm_context: None,
            full_message: None,
        full_source_in_item: false,
                                request_tools_result: None,
                                message: tooling::build_tool_error_message(
                                    provider_call_id,
                                    tool_name,
                                    message,
                                    outcome,
                                ),
                            });
                        }

                        if let Some(descriptor) = runtime_tool_index.get(tool_name.as_str()) {
                            let check = match turn_tool_provider.as_ref() {
                                Some(provider) => match provider
                                    .authorize_skill_continuation(
                                        SkillContinuationAuthorizationContext {
                                            workspace_id: workspace_id.clone(),
                                            thread_id: thread_id.clone(),
                                            turn_id: turn_id.clone(),
                                            skill_id: descriptor.skill_id.clone(),
                                            fingerprint: descriptor.skill_fingerprint.clone(),
                                        },
                                    )
                                    .await
                                {
                                    Ok(()) => pioneer_skills::recheck_runtime_tool_execution(
                                        descriptor,
                                        &runtime_recheck_policy,
                                    ),
                                    Err(_) => pioneer_skills::RuntimeExecutionCheck {
                                        allowed: false,
                                        reason_code: Some(
                                            "runtime.authorization_revoked".to_owned(),
                                        ),
                                        message:
                                            "skill is no longer authorized for this turn"
                                                .to_owned(),
                                        dependency_diagnostics: Vec::new(),
                                    },
                                },
                                None => pioneer_skills::recheck_runtime_tool_execution(
                                    descriptor,
                                    &runtime_recheck_policy,
                                ),
                            };

                            if !check.allowed {
                                let error_payload = serde_json::json!({
                                    "error": check.message,
                                    "code": check.reason_code,
                                    "dependency_diagnostics": check.dependency_diagnostics,
                                    "skill_id": descriptor.skill_id,
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

                                emit_durable_event(
                                    event_tx.as_ref(),
                                    AgentDurableEvent::ItemStarted {
                                        notification: ItemStartedNotification {
                                            workspace_id: workspace_id.clone(),
                                            thread_id: thread_id.clone(),
                                            turn_id: turn_id.clone(),
                                            item: tooling::build_started_tool_turn_item(
                                                item_id.clone(),
                                                tool_name.clone(),
                                                arguments.clone(),
                                                Some(recovery_policy.clone()),
                                                turn_item_execution_class,
                                                output_policy.clone(),
                                                None,
                                            ),
                                        },
                                    },
                                )
                                .await?;

                                emit_durable_event(
                                    event_tx.as_ref(),
                                    AgentDurableEvent::ItemCompleted {
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
                                                turn_item_execution_class,
                                                output_policy,
                                                None,
                                            ),
                                        },
                                    },
                                )
                                .await?;

                                emit_durable_event(
                                    event_tx.as_ref(),
                                    AgentDurableEvent::SkillAuditEvents {
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        events: vec![protocol_skill_audit_event(
                                            pioneer_skills::SkillAuditEvent::runtime_blocked(
                                                descriptor.skill_id.clone(),
                                                descriptor.skill_owner.clone(),
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
                                    },
                                )
                                .await?;

                                return Ok(ExecutedToolResult {
                                    progress: Default::default(),
                                    item_id: item_id.clone(),
                                    provider_call_id: provider_call_id.clone(),
                                    ordinal,
                                    item_type,
                                    attempt_number,
                                    tool_name: tool_name.clone(),
                                    arguments: arguments.clone(),
                                    model_visible_text: error_text.clone(),
                                    success: false,
                                    outcome: outcome.clone(),
                                    recovery_view: None,
                                    retained_llm_context: None,
            full_message: None,
        full_source_in_item: false,
                                    request_tools_result: None,
                                    message: tooling::build_tool_error_message(
                                        provider_call_id,
                                        tool_name,
                                        error_text,
                                        outcome,
                                    ),
                                });
                            }
                        }

                        let tool_call = match router.build_model_tool_call(RawToolCall {
                            call_id: item_id.clone(),
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
                                    emit_durable_event(
                                        event_tx.as_ref(),
                                        AgentDurableEvent::ItemStarted {
                                            notification: ItemStartedNotification {
                                                workspace_id: workspace_id.clone(),
                                                thread_id: thread_id.clone(),
                                                turn_id: turn_id.clone(),
                                                item: tooling::build_started_tool_turn_item(
                                                    item_id.clone(),
                                                    tool_name.clone(),
                                                    arguments.clone(),
                                                    Some(recovery_policy.clone()),
                                                    turn_item_execution_class,
                                                    output_policy.clone(),
                                                    None,
                                                ),
                                            },
                                        },
                                    )
                                    .await?;
                                    emit_durable_event(
                                        event_tx.as_ref(),
                                        AgentDurableEvent::ItemCompleted {
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
                                                    turn_item_execution_class,
                                                    output_policy,
                                                    None,
                                                ),
                                            },
                                        },
                                    )
                                    .await?;
                                }
                                return Ok(ExecutedToolResult {
                                    progress: Default::default(),
                                    item_id: item_id.clone(),
                                    provider_call_id: provider_call_id.clone(),
                                    ordinal,
                                    item_type,
                                    attempt_number,
                                    tool_name: tool_name.clone(),
                                    arguments: arguments.clone(),
                                    model_visible_text: error_text.clone(),
                                    success: false,
                                    outcome: outcome.clone(),
                                    recovery_view: None,
                                    retained_llm_context: None,
            full_message: None,
        full_source_in_item: false,
                                    request_tools_result: None,
                                    message: tooling::build_tool_error_message(
                                        provider_call_id,
                                        tool_name,
                                        error_text,
                                        outcome,
                                    ),
                                });
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

                        let tool_attempt_started = Instant::now();
                        let dispatch_result = runtime
                            .execute_tool_call_with_cancellation(tool_call, attempt_token.clone())
                            .await;

                        heartbeat_stop.cancel();
                        if let Err(error) = heartbeat_task.await
                            && !error.is_cancelled()
                        {
                            error!(error = %error, "tool heartbeat task failed");
                        }
                        turn_control
                            .complete_attempt(turn_id.as_str(), item_id.as_str())
                            .await;

                        let progress = dispatch_result.as_ref().ok().map(|result| result.progress_evidence()).unwrap_or_default();
                        let full_source_in_item = dispatch_result.as_ref().ok().and_then(|r| r.projection()).is_some_and(|p| matches!(p.storage, pioneer_protocol::ToolStoragePayload::Shell { .. }) && matches!(p.output_policy.storage, pioneer_protocol::StorageOutputPolicy::Full { .. }));
                        let full_message = dispatch_result.as_ref().ok().map(|result| {
                            let mut message = result.to_history_message();
                            message.tool_call_id = Some(provider_call_id.clone());
                            message.name = Some(tool_name.clone());
                            message
                        });
                        let full_source_in_item = full_source_in_item && full_message.as_ref().is_none_or(|message| message.content_parts.is_empty());
                        let (
                            tool_output,
                            success,
                            outcome,
                            recovery_view,
                            retained_llm_context,
                            request_tools_result,
                            message,
                        ) =
                            match dispatch_result {
                                Ok(result) => {
                                    let success = result.success();
                                    let projection = result.projection();
                                    let retained_llm_context = projection.and_then(|projection| {
                                        tooling::retained_llm_context_view(
                                            &projection.output_policy,
                                            &projection.llm_view,
                                        )
                                        .map(|view| (view, projection.output_policy.clone()))
                                    });
                                    let request_tools_result = extract_request_tools_result(
                                        tool_name.as_str(),
                                        success,
                                        projection,
                                    );
                                    let mut message =
                                        result.to_model_input_item().into_chat_message();
                                    message.tool_call_id = Some(provider_call_id.clone());
                                    (
                                        result.model_visible_text(),
                                        success,
                                        result.outcome.clone(),
                                        projection.and_then(|projection| projection.recovery.clone()),
                                        retained_llm_context,
                                        request_tools_result,
                                        message,
                                    )
                                }
                                Err(error) => {
                                    let output = error.to_string();
                                    let outcome = classify_tool_error(tool_name.as_str(), &error);
                                    let message = tooling::build_tool_error_message(
                                        provider_call_id.clone(),
                                        tool_name.clone(),
                                        output.clone(),
                                        outcome.clone(),
                                    );
                                    (output, false, outcome, None, None, None, message)
                                }
                            };

                        pioneer_observability::record_native_lifecycle_event(
                            pioneer_observability::NativeLifecycleEventMetric {
                                stage: pioneer_observability::NativeLifecycleStage::ToolAttempt,
                                outcome: if success {
                                    pioneer_observability::NativeLifecycleOutcome::Succeeded
                                } else {
                                    pioneer_observability::NativeLifecycleOutcome::Failed
                                },
                                provider_class: pioneer_observability::NativeProviderClass::Api,
                                elapsed: Some(tool_attempt_started.elapsed()),
                            },
                        );

                        Ok(ExecutedToolResult {
                            progress,
                            item_id: item_id.clone(),
                            provider_call_id,
                            ordinal,
                            item_type,
                            attempt_number,
                            tool_name: tool_name.clone(),
                            arguments: arguments.clone(),
                            model_visible_text: tool_output,
                            success,
                            outcome,
                            recovery_view,
                            retained_llm_context,
                            request_tools_result,
                            full_message,
                            full_source_in_item,
                            message,
                        })
                    });

                    SpawnedToolTask {
                        item_id: task_item_id,
                        provider_call_id: task_provider_call_id,
                        ordinal,
                        tool_name: task_tool_name,
                        arguments: task_arguments,
                        handle: AbortOnDropJoinHandle::new(handle),
                    }
                })
                .collect::<Vec<_>>();

            let parallel_tool_calls = tool_tasks.len().max(1);
            let segment_results = stream::iter(tool_tasks)
                .map(|task| {
                    let event_tx = event_tx.clone();
                    let workspace_id = workspace_id.to_owned();
                    let thread_id = thread_id.to_owned();
                    let turn_id = turn_id.to_owned();
                    async move {
                    let SpawnedToolTask {
                        item_id,
                        provider_call_id,
                        ordinal,
                        tool_name,
                        arguments,
                        handle,
                    } = task;
                    let joined_result = match handle.join().await {
                        Ok(Ok(result)) => Ok(result),
                        Ok(Err(error)) => Err(format!(
                                "tool task failed before producing a verified result: {}; the side-effect outcome is unknown",
                                chat_error_preview(&error)
                            )),
                        Err(error) => Err(format!(
                                "tool task terminated before producing a verified result: {error}; the side-effect outcome is unknown"
                            )),
                    };
                    let (result, unknown_side_effect) = match joined_result {
                        Ok(result) => (result, None),
                        Err(text) => {
                            let outcome = ToolOutcome::fatal(
                                ToolErrorClass::Internal,
                                Some(text.clone()),
                            );
                            emit_durable_event(
                                event_tx.as_ref(),
                                AgentDurableEvent::ItemCompleted {
                                    notification: ItemCompletedNotification {
                                        workspace_id: workspace_id.clone(),
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        item: tooling::build_failed_tool_turn_item(
                                            item_id.clone(),
                                            tool_name.clone(),
                                            arguments.clone(),
                                            text.clone(),
                                            outcome.clone(),
                                            None,
                                            TurnItemExecutionClass::Standard,
                                            None,
                                            None,
                                        ),
                                    },
                                },
                            )
                            .await?;
                            let result = unknown_tool_task_result(
                                item_id,
                                provider_call_id,
                                ordinal,
                                tool_name,
                                arguments,
                                text.clone(),
                            );
                            (result, Some(text))
                        }
                    };
                    let mut result = result;
                    let typed_outcome = result.outcome.provider_outcome();
                    result.message.tool_outcome = Some(typed_outcome);
                    if let Some(full_message) = &mut result.full_message {
                        full_message.tool_outcome = Some(typed_outcome);
                    }
                    let reference = serde_json::json!({"workspace_id": workspace_id, "thread_id": thread_id,
                        "turn_id": turn_id, "item_id": result.item_id}).to_string();
                    if !result.full_source_in_item {
                        persist_retained_tool_result(event_tx.as_ref(), thread_id.as_str(), turn_id.as_str(), &result).await?;
                    }
                    result.message = bounded_tool_result_message(
                        result.full_message.as_ref().unwrap_or(&result.message), &reference,
                    )?;
                    // The durable terminal item already owns full shell output. Retain only
                    // its bounded provider representation in canonical replay history.
                    if result.full_source_in_item {
                        result.full_message = None;
                        persist_retained_tool_result(event_tx.as_ref(), thread_id.as_str(), turn_id.as_str(), &result).await?;
                    }
                    if let Some(error) = unknown_side_effect {
                        return Err(ChatTurnError::Terminal(error));
                    }
                    Ok::<ExecutedToolResult, ChatTurnError>(result)
                }
                })
                .buffer_unordered(parallel_tool_calls)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| (error, current_thinking_id.clone()))?;
                let observations = segment_results.iter()
                    .map(ExecutedToolResult::retry_observation).collect::<Vec<_>>();
                no_progress_feedback.fact_lines.extend(
                    tool_no_progress_guard.observe(&observations).fact_lines,
                );
                no_progress_feedback.fact_lines.truncate(semantic_state::MAX_SUMMARY_EVENTS);
                executed_results.extend(segment_results);
            }
            executed_results.sort_by_key(|result| result.ordinal);
            if tool_event_forwarder.is_finished() {
                return Err((
                    ChatTurnError::Terminal(
                        "tool event forwarder stopped before the durable lifecycle queue drained"
                            .to_owned(),
                    ),
                    current_thinking_id.clone(),
                ));
            }
            // The recovery attempt is closed only after the complete provider
            // round and every tool result have crossed the durable event lane.
            // A provider response alone is not enough: a later tool-result
            // persistence failure must keep the recovery context attached.
            if recovery.is_some() {
                turn_control
                    .succeed_recovery_attempt(turn_id, recovery.take())
                    .await;
            }
            window_stats.record_executed_tools(&executed_results);

            apply_request_tools_results_to_visible_tools(
                &mut visible_tool_names,
                &executed_results,
                router.as_ref(),
            );
            record_requested_tool_names(&mut requested_tool_names, &executed_results);

            for result in &executed_results {
                record_observed_terminal_task_ids(&mut observed_terminal_task_ids, result);
                task_mutation_finalization_guard.observe(result)
                    .map_err(|error| (error, current_thinking_id.clone()))?;
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

            // A bounded post-turn summary is independent of unresolved guards.
            if post_turn_tool_events.len() > semantic_state::MAX_SUMMARY_EVENTS {
                post_turn_tool_events.drain(..post_turn_tool_events.len() - semantic_state::MAX_SUMMARY_EVENTS);
            }
            if post_turn_domain_events.len() > semantic_state::MAX_SUMMARY_EVENTS {
                post_turn_domain_events.drain(..post_turn_domain_events.len() - semantic_state::MAX_SUMMARY_EVENTS);
            }
            let retry_observations = executed_results
                .iter()
                .map(ExecutedToolResult::retry_observation)
                .collect::<Vec<_>>();
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::RetryTools,
                        Some((pioneer_protocol::NativeControlOrigin::ToolService, json!({"facts": prompt.fact_lines()}))));
                    render_tool_retry_instruction(
                        ToolRetryInstructionKind::Retry,
                        &["Inspect the untrusted native tool retry observations.".to_owned()],
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
                    persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ExhaustedTools,
                        Some((pioneer_protocol::NativeControlOrigin::ToolService, json!({"facts": prompt.fact_lines()}))));
                    let exhausted_instruction = render_tool_retry_instruction(
                        ToolRetryInstructionKind::Exhausted,
                        &["Inspect the untrusted native tool retry observations.".to_owned()],
                    );
                    if let Some(instruction) = exhausted_instruction.clone() {
                        // Explicit retry exhaustion still uses a final no-tools round. Window
                        // budget exhaustion is handled earlier as durable continuation.
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

            if !no_progress_feedback.is_empty() {
                persist_control!(current_thinking_id, pioneer_protocol::NativeHostDirective::ChangeToolStrategy,
                    Some((pioneer_protocol::NativeControlOrigin::ToolService, json!({"facts": no_progress_feedback.fact_lines}))));
                let guard_instruction = render_tool_retry_instruction(
                    ToolRetryInstructionKind::Retry,
                    &["Inspect the untrusted native tool progress observations.".to_owned()],
                );
                pending_retry_instruction = match (
                    pending_retry_instruction.take(),
                    guard_instruction,
                ) {
                    (Some(current), Some(guard)) => Some(format!("{current}\n\n{guard}")),
                    (Some(current), None) => Some(current),
                    (None, Some(guard)) => Some(guard),
                    (None, None) => None,
                };
            }


            for result in &mut executed_results {
                result.message.provenance = Some(crate::compaction::history::pending_origin(
                    workspace_id,thread_id,turn_id,&round_id,
                    if result.full_source_in_item {crate::compaction::history::PendingOriginKind::ToolItem} else {crate::compaction::history::PendingOriginKind::ToolResult},
                    &result.item_id,
                ));
                retain_provider_history_message(
                    &mut continuation_provider_history,
                    &mut next_provider_history_sequence,
                    &mut continuation_provider_history_bytes,
                    &result.message,
                ).map_err(|error| (error, current_thinking_id.clone()))?;
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
    tools.event_bus.close_durable_lane();
    drop(runtime);
    drop(router);
    drop(tools);

    finish_tool_event_forwarder(tool_event_forwarder).await?;

    match turn_result {
        Ok(AgentProviderLoopOutcome::Completed) => {
            let summary = AgentTurnPostTurnSummary::succeeded_with_model(
                Some(post_turn_model.clone()),
                Some(post_turn_model_provider.clone()),
                extract_user_text(input),
                post_turn_assistant_text,
                post_turn_tool_events,
                post_turn_domain_events,
            );
            Ok(ChatTurnOutcome::Completed {
                post_turn_dispatch: Some(AgentTurnPostTurnHookDispatch::new(
                    hook_context,
                    effective_policy_set,
                    effective_prompt_context_set,
                    summary,
                )),
            })
        }
        Ok(AgentProviderLoopOutcome::NeedsContinuation(mut continuation)) => {
            continuation.provider_history = continuation_provider_history;
            Ok(ChatTurnOutcome::NeedsContinuation(continuation))
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
            if let Some(mut continuation) = build_failure_execution_window_continuation(
                workspace_id,
                thread_id,
                turn_id,
                input,
                post_turn_model.as_str(),
                post_turn_model_provider.as_str(),
                &window_stats,
                &error,
                tool_no_progress_guard.checkpoint_state(),
                &requested_tool_names,
            ) {
                continuation.checkpoint_payload.native_semantic_state =
                    Some(capture_semantic_state!()?);
                emit_execution_window_exhausted_and_checkpointed(
                    workspace_id,
                    thread_id,
                    turn_id,
                    &window_stats,
                    &continuation,
                    event_tx.as_ref(),
                )
                .await?;
            }
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

// A frame can supersede another only with an authoritative successful outcome
// and the same explicit session. Legacy/unknown results remain semantic input.
fn computer_use_snapshot_scope(message: &ChatMessage) -> Option<u64> {
    if message.role != pioneer_provider::Role::Tool
        || message.name.as_deref() != Some("computer_use")
        || !message.has_attachments()
        || message.tool_outcome.and_then(|outcome| outcome.is_error()) != Some(false)
    {
        return None;
    }
    let payload = serde_json::from_str::<JsonValue>(&message.content).ok()?;
    if payload.get("action").and_then(JsonValue::as_str) != Some("snapshot") {
        return None;
    }
    payload.get("session_id").and_then(JsonValue::as_u64)
}

fn retain_agent_attachment_messages(messages: &mut [ChatMessage]) {
    let mut latest_snapshot_index = HashMap::new();
    for (index, message) in messages.iter().enumerate().rev() {
        if let Some(session) = computer_use_snapshot_scope(message) {
            latest_snapshot_index.entry(session).or_insert(index);
        }
    }
    for (index, message) in messages.iter_mut().enumerate() {
        if let Some(session) = computer_use_snapshot_scope(message)
            && latest_snapshot_index.get(&session).copied() != Some(index)
        {
            // This mutates the request projection, not full tool_result_v2 history.
            message.content_parts.clear();
            message.content.push_str("\n[Earlier screenshot omitted: a later successful snapshot of this session follows.]");
        }
    }
    // Never drop current state to meet guessed bytes. The existing provider's
    // actual prepared-request admission must explicitly reject excess input.
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

#[cfg(test)]
mod tests {
    use super::{
        ChatExecutionWindowStats, ExecutedToolResult, TURN_ITEM_ID_LEN,
        TaskMutationFinalizationGuard, agent_skill_cards_have_read_path,
        append_native_filesystem_capability_section, append_recovered_provider_history,
        apply_request_tools_results_to_visible_tools, apply_request_tools_visibility_expansion,
        apply_review_required_tools_to_visible_tools, build_execution_window_continuation,
        build_user_message, compile_agent_instruction_delivery_plan_with_prompt_root,
        compiled_prompt_payload_from_delivery_plan, deterministic_final_message_item_id,
        deterministic_tool_item_id, materialize_mcp_tooling, native_filesystem_scope,
        native_provider_tool_definition, normalize_turn_capabilities, readable_agent_skill_overlay,
        resolve_skill_capability_summary, retain_agent_attachment_messages,
        retain_chat_mode_attachment_messages, review_required_observation_payload,
        review_required_observation_signature, runtime_sections_with_artifact_reference_policy,
        runtime_sections_with_execution_continuation_context,
        sync_review_action_tools_to_observations, workdir_from_execution_security_snapshot,
    };
    use super::{
        MAX_RETAINED_PROVIDER_BYTES, MAX_RETAINED_PROVIDER_MESSAGES, RetainedProviderByteCounter,
        retain_provider_history_message, retained_provider_message_bytes,
        synchronize_retained_provider_history,
    };
    use crate::{
        AgentMcpAvailability, AgentMcpMaterialization, AgentMcpMaterializationError,
        AgentMcpMaterializationFailureReason, AgentMcpMaterializationRequest,
        AgentMcpResolutionDiagnostic, AgentMcpServerRef, AgentMcpToolProvider,
        ExecutionCheckpointContext, ResolvedArtifactInput, RetainedProviderHistoryMessage,
        ReviewRequiredTaskObservation,
    };
    use pioneer_promt::{
        PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput,
        PromptSectionId,
    };

    #[test]
    fn provider_tool_definition_uses_only_the_selected_input_form() {
        let spec = pioneer_tools::ToolSpec::new(
            "apply_patch",
            "Apply patch.",
            serde_json::json!({"type": "object"}),
            pioneer_tools::PayloadKind::Custom,
        );
        let mut capability =
            pioneer_provider::select_native_file_tool_capability("openai", "gpt-5");
        let json = native_provider_tool_definition(spec.clone(), &capability);
        assert_eq!(json.parameters["type"], "object");
        assert_eq!(json.parameters["required"], serde_json::json!(["patch"]));
        assert!(
            json.description
                .contains("exactly one JSON field named `patch`")
        );
        assert!(json.description.contains("Apply patch."));

        capability.patch_shape = pioneer_provider::NativePatchWireShape::Freeform;
        let raw = native_provider_tool_definition(spec, &capability);
        assert_eq!(raw.parameters["type"], "string");
        assert!(raw.description.contains("directly as raw text"));
        assert!(!raw.description.contains("JSON field named `patch`"));
        assert!(!raw.description.to_ascii_lowercase().contains("codex"));
    }

    #[test]
    fn projected_apply_patch_definition_is_self_contained_for_each_input_form() {
        let spec = pioneer_tools::builtin_tool_specs()
            .into_iter()
            .find(|configured| configured.spec.name == "apply_patch")
            .expect("apply_patch builtin spec")
            .spec;
        let mut capability =
            pioneer_provider::select_native_file_tool_capability("openai", "gpt-5");

        for shape in [
            pioneer_provider::NativePatchWireShape::JsonFunction,
            pioneer_provider::NativePatchWireShape::Freeform,
        ] {
            capability.patch_shape = shape;
            let definition = native_provider_tool_definition(spec.clone(), &capability);
            for required in [
                "*** Begin Patch",
                "*** Add File:",
                "*** Update File:",
                "*** Move to:",
                "*** Delete File:",
                "*** End Patch",
                "current working directory",
            ] {
                assert!(
                    definition.description.contains(required),
                    "projected definition is missing {required} for {shape:?}"
                );
            }
            let lowered = definition.description.to_ascii_lowercase();
            assert!(!lowered.contains("codex"));
            assert!(!lowered.contains("proposal"));
            assert!(!lowered.contains("wire shape"));
        }
    }

    #[tokio::test]
    async fn glm_profiles_preserve_runtime_file_catalog_and_prompt_projection() {
        use pioneer_provider::{NativePatchWireShape, ProviderRegistry};
        let registry = ProviderRegistry::new(|_| "dummy-key".to_owned());
        let model = "glm-5.2";
        let file_tools = vec!["read_file".to_owned(), "apply_patch".to_owned()];
        for alias in [
            "glm",
            "zhipu",
            "bigmodel",
            "glm-cn",
            "zhipu-cn",
            "zai",
            "glm-global",
            "zhipu-global",
            "z.ai",
            "z-ai",
            "glm-coding",
            "glm-coding-cn",
            "zhipu-coding",
            "zai-coding-cn",
            "zai-coding",
            "glm-coding-global",
            "zai-coding-plan",
        ] {
            let provider = registry
                .get_or_create_for_workspace("fixture", alias)
                .unwrap();
            assert!(provider.capabilities().tool_calling);
            // Obtain the same provider-owned decision as the real turn, rather
            // than reproducing the capability family's allowlist in the test.
            let capability = provider.native_file_tool_capability(model);
            assert_eq!(capability.provider, provider.name());
            assert_eq!(capability.patch_shape, NativePatchWireShape::JsonFunction);
            assert!(super::native_file_tool_blocked_names(&capability).is_empty());
            let snapshot = test_full_access_security_snapshot();
            let built = pioneer_tools::build_builtin_tools_with_security_snapshot(
                ".",
                "turn_glm_file_tools",
                pioneer_tools::PermissionEvaluationContext::for_turn(
                    "fixture",
                    "thread",
                    "turn_glm_file_tools",
                    snapshot.permission_profile.clone(),
                ),
                test_web_config(),
                test_computer_use_config(),
                Some(snapshot.clone()),
            );
            built
                .router
                .set_blocked_tool_names(super::native_file_tool_blocked_names(&capability));
            built
                .router
                .set_native_patch_wire_shape(capability.patch_shape);
            let visibility = built
                .router
                .compute_final_visible_tools(&file_tools, &[], &[]);
            for name in &file_tools {
                assert!(visibility.visible_tools.contains(name), "{alias}/{name}");
                assert!(
                    built
                        .router
                        .preflight_tool_index()
                        .core_tools
                        .contains(name)
                );
            }
            built
                .router
                .set_model_visible_tools(&visibility.visible_tools)
                .await;
            let visible = built.router.model_visible_specs().await;
            for name in &file_tools {
                let spec = visible.iter().find(|spec| spec.name == *name).unwrap();
                let definition = native_provider_tool_definition(spec.clone(), &capability);
                assert_eq!(definition.name, *name);
                if name == "apply_patch" {
                    assert_eq!(definition.parameters["type"], "object");
                    assert_eq!(
                        definition.parameters["required"],
                        serde_json::json!(["patch"])
                    );
                    assert_eq!(
                        definition.parameters["properties"]["patch"]["type"],
                        "string"
                    );
                }
            }
            for (name, arguments) in [
                ("read_file", serde_json::json!({"path":"fixture.txt"})),
                (
                    "apply_patch",
                    serde_json::json!({"patch":"*** Begin Patch\n*** Add File: fixture.txt\n+dummy\n*** End Patch"}),
                ),
            ] {
                // Parsing/catalog acceptance only: no handler or filesystem
                // write is invoked, even when this test is eventually run.
                let call = built
                    .router
                    .build_model_tool_call(pioneer_tools::RawToolCall {
                        call_id: "dummy-call".into(),
                        tool_name: name.into(),
                        arguments: arguments.to_string(),
                    })
                    .await;
                let call = call.unwrap_or_else(|error| panic!("{alias}/{name}: {error}"));
                let invocation = pioneer_tools::ToolInvocation {
                    call_id: call.call_id,
                    tool_name: call.tool_name,
                    source: pioneer_tools::ToolCallSource::Model,
                    payload: call.payload,
                    workdir: std::path::PathBuf::from("."),
                    environment: BTreeMap::new(),
                    attempt_id: 1,
                    idempotency_key: call.idempotency_key,
                    recovery: call.recovery,
                    permission_metadata: call.permission_metadata,
                    execution_security_snapshot: Some(snapshot.clone()),
                    apply_patch_preflight: None,
                    cancellation: tokio_util::sync::CancellationToken::new(),
                };
                // Exercise the existing permission evaluator without executing
                // a file handler or constructing a second capability decision.
                let intent = pioneer_tools::PermissionIntent::new(
                    if name == "read_file" {
                        pioneer_tools::PermissionActionKind::FileRead
                    } else {
                        pioneer_tools::PermissionActionKind::FileWrite
                    },
                    pioneer_tools::PermissionRequestScope::empty(),
                );
                let mut context = pioneer_tools::PermissionEvaluationContext::for_turn(
                    "fixture",
                    "thread",
                    "turn_glm_file_tools",
                    snapshot.permission_profile.clone(),
                );
                use pioneer_tools::ToolPermissionEvaluator;
                assert!(matches!(
                    pioneer_tools::ProfileToolPermissionEvaluator.evaluate(
                        &context,
                        &invocation,
                        &intent
                    ),
                    pioneer_tools::PermissionDecision::Allow { .. }
                ));
                context.permission_profile.effective_policy.denied_tools = vec![name.to_owned()];
                assert!(matches!(
                    pioneer_tools::ProfileToolPermissionEvaluator.evaluate(
                        &context,
                        &invocation,
                        &intent
                    ),
                    pioneer_tools::PermissionDecision::Deny { .. }
                ));
            }
            let sections = append_native_filesystem_capability_section(
                Vec::new(),
                &capability,
                Some(&snapshot),
            )
            .unwrap();
            let content = &sections.last().unwrap().content;
            assert!(content.contains("`read_file`"));
            assert!(content.contains("`apply_patch`"));
            assert!(content.contains("exactly one JSON field named `patch`"));
            assert!(!content.contains("not available"));
            // Additional policy/catalog blocks still outrank provider support.
            built
                .router
                .set_blocked_tool_names([("apply_patch".to_owned(), "policy denied".to_owned())]);
            let blocked = built
                .router
                .compute_final_visible_tools(&file_tools, &[], &file_tools);
            assert!(blocked.visible_tools.contains(&"read_file".to_owned()));
            assert!(!blocked.visible_tools.contains(&"apply_patch".to_owned()));
            for missing in ["", "unknown", "unsupported"] {
                let unavailable = provider.native_file_tool_capability(missing);
                built
                    .router
                    .set_blocked_tool_names(super::native_file_tool_blocked_names(&unavailable));
                let blocked =
                    built
                        .router
                        .compute_final_visible_tools(&file_tools, &[], &file_tools);
                assert!(blocked.visible_tools.is_empty());
                let sections = append_native_filesystem_capability_section(
                    Vec::new(),
                    &unavailable,
                    Some(&snapshot),
                )
                .unwrap();
                assert!(sections.last().unwrap().content.contains("not available"));
            }
        }
    }

    #[test]
    fn native_final_message_identity_is_stable_per_turn_and_generation() {
        let first = deterministic_final_message_item_id("turn_stable_final_identity");
        let replay = deterministic_final_message_item_id("turn_stable_final_identity");
        let other = deterministic_final_message_item_id("turn_other_final_identity");
        assert_eq!(first, replay);
        assert_eq!(first.len(), TURN_ITEM_ID_LEN);
        assert_ne!(first, other);
    }

    #[test]
    fn provider_call_identity_is_round_scoped_even_when_provider_reuses_id() {
        let first = deterministic_tool_item_id("turn", "round_1", 0, "call_1");
        let replay = deterministic_tool_item_id("turn", "round_1", 0, "call_1");
        let next_round = deterministic_tool_item_id("turn", "round_2", 0, "call_1");

        assert_eq!(first, replay);
        assert_ne!(first, next_round);
        assert_eq!(first.len(), TURN_ITEM_ID_LEN);
    }
    use pioneer_protocol::{
        ExecutionCheckpointOriginalRequestSummary, ExecutionCheckpointPayload,
        ExecutionCheckpointProviderBudgetSummary, ExecutionCheckpointToolNoProgressExactVariant,
        ExecutionCheckpointToolNoProgressState, ExecutionCheckpointToolNoProgressStrategy,
        ExecutionCheckpointToolSummary, ExecutionCheckpointWindowSummary,
        ExecutionWindowExhaustionReason, McpScopeKind, TurnCapability,
        TurnCapabilityAcceptedReason, TurnCapabilityKind, TurnCapabilityRejectedReason,
        TurnExecutionSecuritySnapshot, TurnItemType, UserInput,
    };
    use pioneer_provider::{
        AttachmentDataSource, ChatMessage, InputContentType, MessageAttachment, MessageContentPart,
        ProviderToolCall, Role,
    };
    use pioneer_skills::compile::CompileSkillInput;
    use pioneer_skills::contract::default_skill_conformance;
    use pioneer_skills::{
        AgentSkillRuntimeEntry, ExcludedSkill, ResolvedSkill, SkillDependencies,
        SkillExcludedReason, SkillExplicitRef, SkillResolutionResult, SkillResolvedReason,
        SkillRuntimePlan, SkillSourceKind, SkillTrustLevel, compile_skill_definition,
    };
    use pioneer_tools::{
        BuiltinToolDomain, ComputerUseToolsConfig, ConfiguredToolSpec, ExecutionClass,
        FunctionToolOutput, PayloadKind, RequestToolsResult, ToolError, ToolErrorClass,
        ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutcome, ToolOutput, ToolSpec,
        WebToolsConfig, dynamic_unknown_output_policy,
    };
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn agent_skill_runtime_entry() -> AgentSkillRuntimeEntry {
        AgentSkillRuntimeEntry {
            skill_id: pioneer_protocol::SkillId::new("AAAAAAAAAAAAAAAAAAAAA")
                .expect("valid Agent skill ID"),
            slug: "agent-skill".to_owned(),
            version_id: "VVVVVVVVVVVVVVVVVVVVV".to_owned(),
            version_number: 1,
            display_name: "Agent skill".to_owned(),
            runtime_description: "Use for one learned procedure.".to_owned(),
            body: "Exact immutable instructions.".to_owned(),
            fingerprint: "fingerprint".to_owned(),
        }
    }

    #[test]
    fn agent_skill_overlay_requires_provider_tool_calling_support() {
        let entries = vec![agent_skill_runtime_entry()];

        assert!(
            readable_agent_skill_overlay(entries.as_slice(), false).is_empty(),
            "a provider without native tool calling must never receive Agent cards"
        );
        assert_eq!(
            readable_agent_skill_overlay(entries.as_slice(), true),
            entries.as_slice()
        );
    }

    #[test]
    fn agent_skill_cards_require_materialized_read_skill() {
        let entries = vec![agent_skill_runtime_entry()];

        assert!(!agent_skill_cards_have_read_path(entries.as_slice(), false));
        assert!(agent_skill_cards_have_read_path(entries.as_slice(), true));
        assert!(agent_skill_cards_have_read_path(&[], false));
    }

    #[derive(Default)]
    struct NoopToolHandler;

    #[test]
    fn native_batch_segments_bound_reads_and_separate_unknown_mutations_and_exact_duplicates() {
        let calls = [
            ("read", r#"{"path":"a"}"#),
            ("read", r#"{"path":"b"}"#),
            ("mutation", "{}"),
            ("read", r#"{"path":"c"}"#),
        ];
        let reads = |name: &str| name == "read";
        assert_eq!(
            super::native_tool_batch_segment_len(calls.iter().copied(), reads),
            2
        );
        assert_eq!(
            super::native_tool_batch_segment_len(calls[2..].iter().copied(), reads),
            1
        );
        assert_eq!(
            super::native_tool_batch_segment_len(calls[3..].iter().copied(), reads),
            1
        );
        assert_eq!(
            super::native_tool_batch_segment_len(calls.iter().copied(), |_| false),
            1
        );
        let duplicate = [
            ("read", r#"{"path":"a","start_line":1}"#),
            ("read", r#"{"start_line":1,"path":"a"}"#),
        ];
        assert_eq!(
            super::native_tool_batch_segment_len(duplicate.into_iter(), reads),
            1
        );
        let distinct = (0..9)
            .map(|index| ("read", format!(r#"{{"path":"{index}"}}"#)))
            .collect::<Vec<_>>();
        assert_eq!(
            super::native_tool_batch_segment_len(
                distinct.iter().map(|(name, args)| (*name, args.as_str())),
                reads
            ),
            4
        );
    }

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

    struct FailingMcpMaterializationProvider;

    #[async_trait::async_trait]
    impl AgentMcpToolProvider for FailingMcpMaterializationProvider {
        async fn mcp_availability(
            &self,
            _workspace_id: &str,
        ) -> Result<AgentMcpAvailability, String> {
            Ok(AgentMcpAvailability::default())
        }

        async fn materialize_mcp_tools(
            &self,
            _request: AgentMcpMaterializationRequest,
        ) -> Result<AgentMcpMaterialization, AgentMcpMaterializationError> {
            Err(AgentMcpMaterializationError {
                reason: AgentMcpMaterializationFailureReason::ResolutionUncertain,
                message: "storage unavailable".to_owned(),
                diagnostics: vec![AgentMcpResolutionDiagnostic {
                    code: "mcp.resolution.uncertain".to_owned(),
                    message: "storage unavailable".to_owned(),
                }],
                accepted_capabilities: Vec::new(),
                rejected_capabilities: Vec::new(),
            })
        }

        async fn persist_mcp_projection(
            &self,
            _request: crate::AgentMcpProjectionPersistenceRequest,
        ) -> Result<crate::AgentMcpPersistedProjection, crate::AgentMcpProjectionPersistenceError>
        {
            Err(crate::AgentMcpProjectionPersistenceError {
                message: "materialization failed before persistence".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn mcp_materialization_explicit_capability_without_provider_is_not_empty_fallback() {
        let failure = match materialize_mcp_tooling(
            None,
            "workspace",
            "turn",
            &[AgentMcpServerRef {
                capability_id: "mcp-server:workspace:resend".to_owned(),
                label: Some("resend".to_owned()),
                name: "resend".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            }],
            &[],
        )
        .await
        {
            Ok(_) => panic!("explicit MCP without a provider must fail"),
            Err(failure) => failure,
        };

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ProviderUnavailable
        );
        assert_eq!(failure.rejected_capabilities.len(), 1);
    }

    #[tokio::test]
    async fn mcp_materialization_provider_uncertainty_is_not_empty_fallback() {
        let provider: std::sync::Arc<dyn AgentMcpToolProvider> =
            std::sync::Arc::new(FailingMcpMaterializationProvider);
        let failure =
            match materialize_mcp_tooling(Some(&provider), "workspace", "turn", &[], &[]).await {
                Ok(_) => panic!("resolver uncertainty must fail before provider request"),
                Err(failure) => failure,
            };

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ResolutionUncertain
        );
        assert_eq!(failure.message, "storage unavailable");
    }

    #[tokio::test]
    async fn mcp_materialization_empty_valid_turn_without_provider_remains_valid() {
        let materialization = materialize_mcp_tooling(None, "workspace", "turn", &[], &[])
            .await
            .expect("empty valid MCP projection should remain successful");
        assert!(materialization.bundles.is_empty());
        assert!(materialization.rejected_capabilities.is_empty());
    }

    #[test]
    fn native_tool_workdir_comes_from_execution_security_snapshot() {
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/pioneer-snapshot-cwd", 1);

        let workdir = workdir_from_execution_security_snapshot(Some(&snapshot))
            .expect("snapshot cwd should resolve");

        assert_eq!(
            workdir,
            std::path::PathBuf::from("/tmp/pioneer-snapshot-cwd")
        );
    }

    #[test]
    fn native_filesystem_prompt_uses_dynamic_cwd_and_authorized_roots() {
        let cwd = temp_dir("filesystem_prompt_cwd");
        let additional = temp_dir("filesystem_prompt_additional");
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            cwd.to_string_lossy(),
            vec![
                pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                    pioneer_protocol::TurnFilesystemAccess::Write,
                    cwd.to_string_lossy(),
                ),
                pioneer_protocol::TurnFilesystemSandboxEntry {
                    path: pioneer_protocol::TurnFilesystemSandboxPath::ExplicitPath {
                        path: additional.to_string_lossy().into_owned(),
                    },
                    access: pioneer_protocol::TurnFilesystemAccess::Write,
                    provenance: pioneer_protocol::TurnSecurityRuleProvenance::ComposerSelection,
                    resolved_path: Some(additional.to_string_lossy().into_owned()),
                },
            ],
            1,
        );

        let scope = native_filesystem_scope(Some(&snapshot));

        assert!(scope.contains(cwd.to_string_lossy().as_ref()));
        assert!(
            scope.contains(
                additional
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert!(scope.contains("Writable roots"));
        assert!(scope.contains("relative paths resolve from it"));
    }

    #[test]
    fn native_filesystem_prompt_explains_the_selected_input_form_directly() {
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/tmp/pioneer-filesystem-prompt",
            1,
        );
        let capability = pioneer_provider::select_native_file_tool_capability("openai", "gpt-5");

        let sections =
            append_native_filesystem_capability_section(Vec::new(), &capability, Some(&snapshot))
                .expect("filesystem capability section should compile");
        let content = sections
            .last()
            .expect("filesystem capability section")
            .content
            .as_str();

        assert!(content.contains("exactly one JSON field named `patch`"));
        assert!(content.contains("current working directory is `/tmp/pioneer-filesystem-prompt`"));
        assert!(content.contains("returned error and next action"));
        let lowered = content.to_ascii_lowercase();
        assert!(!lowered.contains("codex"));
        assert!(!lowered.contains("wire shape"));
        assert!(!lowered.contains("proposal"));
        assert!(!lowered.contains("if-match"));
    }

    fn execution_checkpoint_context_fixture() -> ExecutionCheckpointContext {
        ExecutionCheckpointContext {
            window_id: "turn_1:window:1".to_owned(),
            window_index: 1,
            checkpoint_id: "checkpoint_1".to_owned(),
            checkpoint_kind: "execution_window_exhausted".to_owned(),
            usage: crate::ExecutionWindowUsageSnapshot::default(),
            payload: ExecutionCheckpointPayload {
                schema_version: pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
                workspace_id: "workspace_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                original_request: ExecutionCheckpointOriginalRequestSummary {
                    input_count: 1,
                    text_preview: Some("create a report".to_owned()),
                    text_truncated: false,
                    attachment_count: 0,
                    attachment_kinds: Vec::new(),
                },
                window: ExecutionCheckpointWindowSummary {
                    window_id: Some("turn_1:window:1".to_owned()),
                    window_index: 1,
                    started_at_unix_ms: Some(1_000),
                    completed_at_unix_ms: Some(2_000),
                    agent_round_count: 3,
                    tool_call_count: 7,
                    provider_token_count: Some(123),
                    exhaustion_reason: Some(
                        ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                    ),
                },
                provider_budget: ExecutionCheckpointProviderBudgetSummary {
                    model: Some("model-a".to_owned()),
                    model_provider: Some("provider-a".to_owned()),
                    agent_round_count: 3,
                    tool_call_count: 7,
                    provider_token_count: Some(123),
                    provider_usage_available: true,
                    exhaustion_reason: Some(
                        ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                    ),
                    exhausted_limit: Some(3),
                    exhausted_observed: Some(3),
                },
                tools: ExecutionCheckpointToolSummary {
                    requested_count: 7,
                    executed_count: 7,
                    unexecuted_count: 0,
                    total_count: 7,
                    succeeded_count: 7,
                    failed_count: 0,
                    in_progress_count: 0,
                    detail_limit: 32,
                    details_truncated: false,
                    details: Vec::new(),
                },
                tool_no_progress: Default::default(),
                requested_tool_names: Vec::new(),
                native_semantic_state: None,
                native_event_frontier: None,
                strict_obligations: Vec::new(),
            },
        }
    }

    #[test]
    fn execution_window_continuation_persists_tool_no_progress_state() {
        let state = ExecutionCheckpointToolNoProgressState {
            strategies: vec![ExecutionCheckpointToolNoProgressStrategy {
                strategy_id: "strategy_1".to_owned(),
                tool_name: "exec_command".to_owned(),
                executable: Some("runner".to_owned()),
                structural_fingerprint: "structural".to_owned(),
                structural_features: vec!["feature".to_owned()],
                exact_variants: vec![ExecutionCheckpointToolNoProgressExactVariant {
                    arguments_fingerprint: "exact".to_owned(),
                    timeout_count: 2,
                }],
                timeout_count: 2,
                cumulative_timeout_ms: 60_000,
                warning_emitted: true,
                exhausted: true,
            }],
            truncated: false,
        };
        let continuation = build_execution_window_continuation(
            "workspace",
            "thread",
            "turn",
            &[UserInput::Text {
                text: "continue".to_owned(),
                text_elements: Vec::new(),
            }],
            "model",
            "provider",
            &ChatExecutionWindowStats::new(1),
            ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
            Some(1),
            Some(1),
            "max_agent_rounds_per_window",
            state.clone(),
            &[],
        );

        assert_eq!(continuation.checkpoint_payload.tool_no_progress, state);
    }

    #[test]
    fn window_one_runtime_sections_do_not_add_execution_continuation_context() {
        let base_sections = vec![PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::MemoryRecall),
            title: None,
            content: "memory recall".to_owned(),
            max_chars: None,
            truncated: false,
        }];
        let checkpoint_context = execution_checkpoint_context_fixture();

        let sections = runtime_sections_with_execution_continuation_context(
            base_sections.as_slice(),
            1,
            Some(&checkpoint_context),
            Some("partial assistant text"),
            Some("initiating_thread_test"),
        );

        assert_eq!(sections, base_sections);
    }

    #[test]
    fn continuation_window_runtime_sections_add_execution_continuation_context() {
        let checkpoint_context = execution_checkpoint_context_fixture();

        let sections = runtime_sections_with_execution_continuation_context(
            &[],
            2,
            Some(&checkpoint_context),
            Some("partial assistant text"),
            Some("initiating_thread_test"),
        );

        assert_eq!(sections.len(), 1);
        assert!(
            sections[0]
                .content
                .contains("Initiating thread: initiating_thread_test")
        );
        assert!(sections[0].content.contains(&format!(
            "Execution thread: {} (internal child)",
            checkpoint_context.payload.thread_id
        )));
        assert_eq!(
            sections[0].id,
            PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation)
        );
        assert!(
            sections[0]
                .content
                .contains("Original request preview: create a report")
        );
        assert!(sections[0].content.contains("Completed window: index=1"));
        assert!(
            sections[0]
                .content
                .contains("Prior visible assistant text: partial assistant text")
        );
    }

    #[test]
    fn artifact_reference_policy_section_is_added_only_when_refs_exist() {
        let sections = runtime_sections_with_artifact_reference_policy(Vec::new(), true)
            .expect("section renders");

        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].title.as_deref(), Some("Artifact References"));
        assert!(sections[0].content.contains("artifact_read"));
        assert!(matches!(
            &sections[0].id,
            PromptRuntimeSectionId::Dynamic(id) if id.as_str() == "artifact_references"
        ));
    }

    #[test]
    fn artifact_reference_policy_section_is_not_added_without_refs() {
        let sections = runtime_sections_with_artifact_reference_policy(Vec::new(), false)
            .expect("section skipped");

        assert!(sections.is_empty());
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

    fn test_skill_id(seed: &str) -> pioneer_protocol::SkillId {
        let mut value = seed
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        value.truncate(21);
        while value.len() < 21 {
            value.push('T');
        }
        pioneer_protocol::SkillId::new(value).expect("valid test SkillId")
    }

    fn skill_capability(seed: &str, label: &str) -> TurnCapability {
        let skill_id = test_skill_id(seed);
        TurnCapability {
            id: format!("skill:{skill_id}"),
            label: Some(label.to_owned()),
            kind: TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            },
        }
    }

    fn explicit_skill_ref(seed: &str, label: &str) -> SkillExplicitRef {
        SkillExplicitRef {
            skill_id: test_skill_id(seed),
            label: Some(label.to_owned()),
        }
    }

    fn test_skill_definition(slug: &str) -> pioneer_skills::SkillDefinition {
        let conformance = default_skill_conformance();
        compile_skill_definition(CompileSkillInput {
            skill_id: test_skill_id(slug),
            owner: Some("workspace".to_owned()),
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
            agent_overlay_error: None,
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
    fn normalize_turn_capabilities_keeps_duplicate_labels_with_distinct_ids() {
        let normalized = normalize_turn_capabilities(&[
            skill_capability("docs-a", "docs"),
            skill_capability("docs-b", "docs"),
        ]);

        assert_eq!(normalized.skill_refs.len(), 2);
        assert_ne!(
            normalized.skill_refs[0].skill_id,
            normalized.skill_refs[1].skill_id
        );
        assert!(normalized.rejected.is_empty());
    }

    #[test]
    fn normalize_turn_capabilities_rejects_pack_metadata_at_runtime_boundary() {
        let pack_id = pioneer_protocol::SkillPackId::new("P".repeat(21)).expect("pack id");
        let skill_id = test_skill_id("packed");
        let normalized = normalize_turn_capabilities(&[
            TurnCapability {
                id: pioneer_protocol::skill_capability_key(&skill_id),
                kind: TurnCapabilityKind::Skill {
                    skill_id,
                    pack_id: Some(pack_id.clone()),
                },
                label: None,
            },
            TurnCapability {
                id: pioneer_protocol::skill_pack_capability_key(&pack_id),
                kind: TurnCapabilityKind::SkillPack { pack_id },
                label: None,
            },
        ]);

        assert!(normalized.skill_refs.is_empty());
        assert_eq!(normalized.rejected.len(), 2);
        assert!(
            normalized
                .rejected
                .iter()
                .all(|rejected| rejected.reason == TurnCapabilityRejectedReason::InvalidInput)
        );
    }

    #[test]
    fn normalize_turn_capabilities_splits_skill_server_and_tool_refs() {
        let normalized = normalize_turn_capabilities(&[
            skill_capability("docs", "docs"),
            mcp_server_capability("mcp-server:workspace:browser", " browser "),
            mcp_tool_capability("mcp-tool:workspace:browser:open", " browser ", " open "),
        ]);

        assert_eq!(normalized.rejected, Vec::new());

        assert_eq!(normalized.skill_refs.len(), 1);
        assert_eq!(normalized.skill_refs[0].skill_id, test_skill_id("docs"));

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
        let explicit_ref = explicit_skill_ref("docs", "docs");
        let resolution = turn_skill_resolution(
            vec![ResolvedSkill {
                skill_id: test_skill_id("docs"),
                slug: "workspace/docs".to_owned(),
                reason: SkillResolvedReason::ExplicitCapability,
                definition: test_skill_definition("docs"),
            }],
            Vec::new(),
        );

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.rejected.is_empty());
        assert_eq!(summary.accepted.len(), 1);
        assert_eq!(
            summary.accepted[0].id,
            format!("skill:{}", test_skill_id("docs"))
        );
        assert_eq!(summary.accepted[0].label.as_deref(), Some("docs"));
        assert_eq!(
            summary.accepted[0].reason,
            TurnCapabilityAcceptedReason::ExplicitComposerCapability
        );
        assert_eq!(
            summary.accepted[0].kind,
            TurnCapabilityKind::Skill {
                skill_id: test_skill_id("docs"),
                pack_id: None,
            }
        );
    }

    #[test]
    fn resolve_skill_capability_summary_rejects_missing_skill() {
        let explicit_ref = explicit_skill_ref("missing", "missing");
        let resolution = turn_skill_resolution(Vec::new(), Vec::new());

        let summary = resolve_skill_capability_summary(&[explicit_ref], &resolution);

        assert!(summary.accepted.is_empty());
        assert_eq!(summary.rejected.len(), 1);
        assert_eq!(
            summary.rejected[0].id,
            format!("skill:{}", test_skill_id("missing"))
        );
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
        let explicit_ref = explicit_skill_ref("docs", "docs");
        let resolution = turn_skill_resolution(
            Vec::new(),
            vec![ExcludedSkill {
                skill_id: test_skill_id("docs"),
                owner: Some("workspace".to_owned()),
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
        assert_eq!(
            summary.rejected[0].id,
            format!("skill:{}", test_skill_id("docs"))
        );
        assert_eq!(
            summary.rejected[0].reason,
            TurnCapabilityRejectedReason::DisabledByPolicy
        );
    }

    #[test]
    fn resolve_skill_capability_summary_rejects_security_blocked_skill() {
        let explicit_ref = explicit_skill_ref("docs", "docs");
        let resolution = turn_skill_resolution(
            Vec::new(),
            vec![ExcludedSkill {
                skill_id: test_skill_id("docs"),
                owner: Some("workspace".to_owned()),
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
        assert_eq!(
            summary.rejected[0].id,
            format!("skill:{}", test_skill_id("docs"))
        );
        assert_eq!(
            summary.rejected[0].reason,
            TurnCapabilityRejectedReason::SecurityBlocked
        );
    }

    fn task_result(tool_name: &str, success: bool, text: &str) -> ExecutedToolResult {
        ExecutedToolResult {
            progress: Default::default(),
            item_id: "item_1234567890123456".to_owned(),
            provider_call_id: "provider_call_1".to_owned(),
            ordinal: 0,
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
            retained_llm_context: None,
            full_message: None,
            full_source_in_item: false,
            request_tools_result: None,
            message: ChatMessage::tool_result("provider_call_1", tool_name, text),
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

    fn test_tools_permission_context(turn_id: &str) -> pioneer_tools::PermissionEvaluationContext {
        pioneer_tools::PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            turn_id,
            pioneer_protocol::default_turn_permission_profile_snapshot(),
        )
    }

    fn test_full_access_security_snapshot() -> pioneer_protocol::TurnExecutionSecuritySnapshot {
        pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(".", 1)
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
            test_tools_permission_context("turn_request_tools_visibility"),
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
            test_tools_permission_context("turn_schema_token_guard"),
            test_web_config(),
            test_computer_use_config(),
            vec![ToolExtensionBundle { specs, handlers }],
        )
        .expect("test tools must build")
    }

    fn all_lazy_domain_tool_names() -> Vec<&'static str> {
        let mut seen = BTreeSet::new();
        pioneer_tools::builtin_tool_domain_map()
            .iter()
            .flat_map(|(_, tool_names)| tool_names.iter().copied())
            .filter(|name| seen.insert(*name))
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
        let built = pioneer_tools::build_builtin_tools_with_security_snapshot(
            ".",
            "turn_agent_request_tools_result",
            test_tools_permission_context("turn_agent_request_tools_result"),
            test_web_config(),
            test_computer_use_config(),
            Some(test_full_access_security_snapshot()),
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
        let artifact_tools = BuiltinToolDomain::Artifact.tool_names();
        let mut extension_names = artifact_tools.to_vec();
        extension_names.push("skill.test.dynamic");
        let built = build_tools_with_extension_names(&extension_names);
        let mut visible_tool_names = vec![
            "request_tools".to_owned(),
            "read_file".to_owned(),
            "skill.test.dynamic".to_owned(),
        ];
        let before = visible_tool_names.clone();
        let result = request_tools_result_for_added(
            "artifact",
            artifact_tools.iter().map(|name| (*name).to_owned()),
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
            vec!["artifact_prepare", "artifact_register", "artifact_read"]
        );
        for name in before {
            assert!(
                visible.contains(&name),
                "previously visible tool `{name}` must remain visible"
            );
        }
        assert!(visible.contains(&"artifact_prepare".to_owned()));
        assert!(visible.contains(&"artifact_register".to_owned()));
        assert!(visible.contains(&"artifact_read".to_owned()));
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

    #[tokio::test]
    async fn requested_tools_survive_checkpoint_roundtrip_and_revalidate_on_resume() {
        let built = build_tools_with_extension_names(BuiltinToolDomain::Memory.tool_names());
        let mut request = request_tools_result_for_added(
            "memory",
            ["memory_remember".to_owned(), "memory_forget".to_owned()],
        );
        request
            .already_visible
            .insert("memory".to_owned(), vec!["memory_get".to_owned()]);
        let results = vec![request_tools_executed_result(request)];
        let mut names = Vec::new();
        super::record_requested_tool_names(&mut names, &results);
        super::record_requested_tool_names(&mut names, &results);
        assert_eq!(
            names,
            vec!["memory_forget", "memory_get", "memory_remember"]
        );

        for reason in [
            ExecutionWindowExhaustionReason::ProviderFailureContinuation,
            ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
            ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
        ] {
            let continuation = build_execution_window_continuation(
                "workspace",
                "thread",
                "turn",
                &[],
                "model",
                "provider",
                &ChatExecutionWindowStats::new(1),
                reason,
                None,
                None,
                "test",
                Default::default(),
                &names,
            );
            let bytes = serde_json::to_vec(&continuation.checkpoint_payload).unwrap();
            let mut context = execution_checkpoint_context_fixture();
            context.payload = serde_json::from_slice(&bytes).unwrap();
            let restored =
                super::restored_requested_tool_names(Some(&context), "workspace", "thread", "turn");
            assert_eq!(restored, names);
            for (workspace, thread, turn) in [
                ("other", "thread", "turn"),
                ("workspace", "other", "turn"),
                ("workspace", "thread", "other"),
            ] {
                assert!(
                    super::restored_requested_tool_names(Some(&context), workspace, thread, turn)
                        .is_empty()
                );
            }
            assert!(
                super::restored_requested_tool_names(None, "workspace", "thread", "turn")
                    .is_empty()
            );
            built
                .router
                .set_blocked_tool_names([("memory_forget".to_owned(), "revoked".to_owned())]);
            let mut restored = restored;
            restored.push("removed_tool".to_owned());
            let visible = built
                .router
                .compute_final_visible_tools(&[], &[], &restored);
            assert!(
                visible
                    .visible_tools
                    .contains(&"memory_remember".to_owned())
            );
            assert!(visible.visible_tools.contains(&"memory_get".to_owned()));
            assert!(!visible.visible_tools.contains(&"memory_forget".to_owned()));
            assert!(!visible.visible_tools.contains(&"removed_tool".to_owned()));
            built
                .router
                .set_model_visible_tools(&visible.visible_tools)
                .await;
            assert!(
                built
                    .router
                    .model_visible_specs()
                    .await
                    .iter()
                    .any(|spec| spec.name == "memory_remember")
            );

            for (name, allowed) in [("memory_remember", true), ("memory_forget", false)] {
                let call = built
                    .router
                    .build_model_tool_call(pioneer_tools::RawToolCall {
                        call_id: "restored_call".to_owned(),
                        tool_name: name.to_owned(),
                        arguments: "{}".to_owned(),
                    })
                    .await;
                assert_eq!(call.is_ok(), allowed);
            }
            let mut old = serde_json::to_value(&context.payload).unwrap();
            old.as_object_mut().unwrap().remove("requested_tool_names");
            let old: ExecutionCheckpointPayload = serde_json::from_value(old).unwrap();
            assert!(old.requested_tool_names.is_empty());
        }
    }

    #[test]
    fn request_tools_visibility_expansion_exposes_computer_use_only_when_registered() {
        let built = pioneer_tools::build_builtin_tools_with_security_snapshot(
            ".",
            "turn_request_tools_computer_use_visibility",
            test_tools_permission_context("turn_request_tools_computer_use_visibility"),
            test_web_config(),
            test_computer_use_config(),
            Some(test_full_access_security_snapshot()),
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
        let built = pioneer_tools::build_builtin_tools_with_security_snapshot(
            ".",
            "turn_tool_visibility_computer_use",
            test_tools_permission_context("turn_tool_visibility_computer_use"),
            test_web_config(),
            test_computer_use_config(),
            Some(test_full_access_security_snapshot()),
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
                "threads_start_options",
                "task_create",
                "task_wait",
                "task_result",
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
        guard
            .observe(&task_result("task_create", false, "trigger must be object"))
            .unwrap();

        let message = guard
            .deterministic_failure_message()
            .expect("failed task mutation should block success finalization");
        assert!(message.contains("task_create"));
        assert!(message.contains("trigger must be object"));

        guard
            .observe(&task_result("task_create", true, "{\"taskId\":\"abc\"}"))
            .unwrap();
        assert!(guard.deterministic_failure_message().is_none());
    }

    #[test]
    fn task_mutation_success_requires_same_task_postcondition_and_newer_revision() {
        let mut guard = TaskMutationFinalizationGuard::default();
        let mut failed = task_result("task_update", false, "revision conflict");
        failed.arguments =
            r#"{"taskId":"AAAAAAAAAAAAAAAAAAAAA","title":"accepted title","expectedRevision":7}"#
                .to_owned();
        guard.observe(&failed).unwrap();
        let mut unrelated = task_result(
            "task_update",
            true,
            r#"{"task":{"id":"BBBBBBBBBBBBBBBBBBBBB"},"revision":9}"#,
        );
        unrelated.arguments =
            r#"{"taskId":"BBBBBBBBBBBBBBBBBBBBB","title":"accepted title","expectedRevision":8}"#
                .to_owned();
        guard.observe(&unrelated).unwrap();
        assert!(guard.deterministic_failure_message().is_some());
        unrelated.arguments =
            r#"{"taskId":"AAAAAAAAAAAAAAAAAAAAA","title":"another title","expectedRevision":8}"#
                .to_owned();
        unrelated.model_visible_text =
            r#"{"task":{"id":"AAAAAAAAAAAAAAAAAAAAA"},"revision":9}"#.to_owned();
        guard.observe(&unrelated).unwrap();
        assert!(guard.deterministic_failure_message().is_some());
        let encoded = serde_json::to_vec(&guard).unwrap();
        let mut restored: TaskMutationFinalizationGuard = serde_json::from_slice(&encoded).unwrap();
        assert!(restored.checkpoint_valid());
        unrelated.arguments =
            r#"{"taskId":"AAAAAAAAAAAAAAAAAAAAA","title":"accepted title","expectedRevision":8}"#
                .to_owned();
        unrelated.model_visible_text =
            r#"{"task":{"id":"AAAAAAAAAAAAAAAAAAAAA"},"revision":7}"#.to_owned();
        restored.observe(&unrelated).unwrap();
        assert!(restored.deterministic_failure_message().is_some());
        unrelated.model_visible_text =
            r#"{"task":{"id":"AAAAAAAAAAAAAAAAAAAAA"},"revision":9}"#.to_owned();
        restored.observe(&unrelated).unwrap();
        assert!(restored.deterministic_failure_message().is_none());
    }

    #[test]
    fn restored_retry_exhaustion_keeps_native_next_round_without_tools() {
        let budget = pioneer_tools::ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 1,
            max_same_tool_error_retries_per_episode: 1,
            max_retries_per_tool_name_per_episode: 1,
        };
        let mut controller = pioneer_tools::ToolRetryController::new(budget.clone());
        let failure = pioneer_tools::ToolRetryObservation::from_tool_outcome(
            "item",
            "command_execution",
            1,
            "exec_command",
            "{}",
            false,
            pioneer_tools::ToolOutcome::recoverable(
                pioneer_tools::ToolErrorClass::NotFound,
                "correct the call",
                false,
                None,
            ),
        );
        assert!(matches!(
            controller.decide(&[failure.clone()]),
            pioneer_tools::ToolRetryDecision::Retry { .. }
        ));
        assert!(matches!(
            controller.decide(&[failure]),
            pioneer_tools::ToolRetryDecision::Exhausted { .. }
        ));
        let restored = pioneer_tools::ToolRetryController::from_checkpoint(
            budget,
            controller.checkpoint().unwrap(),
        )
        .unwrap();
        let mut guard = pioneer_tools::ToolLoopGuard::new_execution_window(
            Default::default(),
            "ordinary finalization",
        );
        if restored.is_exhausted() {
            guard
                .request_final_answer_with_instruction(
                    pioneer_protocol::NativeHostDirective::ExhaustedTools.instruction(),
                )
                .unwrap();
        }
        let round = guard.begin_provider_round().unwrap();
        assert!(
            !round.tools_enabled,
            "window/restart must not renew an exhausted tool allowance"
        );
        assert_eq!(
            round.final_instruction.as_deref(),
            Some(pioneer_protocol::NativeHostDirective::ExhaustedTools.instruction())
        );
    }

    #[test]
    fn semantic_checkpoint_restores_guidance_guards_and_rejects_legacy_foreign_or_oversized_state()
    {
        let budget = pioneer_tools::ToolRetryBudgetConfig::default();
        let controller = super::ToolRetryController::new(budget.clone());
        let lifecycle = super::ToolRetryLifecycleTracker::default();
        let mut guard = TaskMutationFinalizationGuard::default();
        guard
            .observe(&task_result("task_create", false, "invalid trigger"))
            .unwrap();
        let pending = Some("Retry only the unresolved mutation".to_owned());
        let applied = Some("Original accepted guidance".to_owned());
        let reviews = std::collections::BTreeSet::from(["review-original".to_owned()]);
        let terminal = std::collections::BTreeSet::from(["terminal-original".to_owned()]);
        let roots = vec![pioneer_compaction::SourceRef {
            scope: "checkpoint:native-owner".into(),
            id: "acknowledged-root".into(),
            version: "exact-version".into(),
        }];
        let mut progress_guard = super::ToolNoProgressGuard::default();
        progress_guard.observe(&[pioneer_tools::ToolRetryObservation::from_tool_outcome(
            "snapshot",
            "dynamic_tool_call",
            1,
            "read_file",
            "{}",
            true,
            pioneer_tools::ToolOutcome::ok(),
        )
        .with_progress(pioneer_tools::ToolProgressEvidence::resource_state(
            [1; 32], [2; 32],
        ))]);
        let mut context = execution_checkpoint_context_fixture();
        context.window_index = context.payload.window.window_index;
        context.payload.native_semantic_state = Some(
            super::semantic_state::capture(
                1,
                "turn_1",
                &budget,
                &super::native_control::NativeControlState::default(),
                &roots,
                pioneer_tools::ToolNoProgressMode::Enforce,
                progress_guard.progress_frontier(),
                &controller,
                &lifecycle,
                &pending,
                &applied,
                &reviews,
                &terminal,
                &guard,
                "Original post-turn summary",
                &[],
                &[],
            )
            .unwrap(),
        );
        let state = super::semantic_state::NativeSemanticState::restore(
            Some(&context),
            "workspace_1",
            "thread_1",
            "turn_1",
            context.window_index + 1,
            &budget,
            false,
            pioneer_tools::ToolNoProgressMode::Enforce,
        )
        .unwrap();
        assert_eq!(state.provider_history_roots, roots);
        assert_eq!(
            state.no_progress_mode,
            pioneer_tools::ToolNoProgressMode::Enforce
        );
        assert!(!crate::native_semantic_checkpoint_compatible(
            &context.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Observe
        ));
        assert!(
            super::semantic_state::NativeSemanticState::restore(
                Some(&context),
                "workspace_1",
                "thread_1",
                "turn_1",
                context.window_index + 1,
                &budget,
                false,
                pioneer_tools::ToolNoProgressMode::Observe
            )
            .is_err()
        );
        assert_eq!(state.progress_frontier.advanced_observations, 1);
        let mut cold_progress = super::ToolNoProgressGuard::default();
        cold_progress
            .restore_progress_frontier(state.progress_frontier.clone())
            .unwrap();
        cold_progress.observe(&[pioneer_tools::ToolRetryObservation::from_tool_outcome(
            "same-snapshot",
            "dynamic_tool_call",
            1,
            "read_file",
            "{}",
            true,
            pioneer_tools::ToolOutcome::ok(),
        )
        .with_progress(pioneer_tools::ToolProgressEvidence::resource_state(
            [1; 32], [2; 32],
        ))]);
        assert_eq!(cold_progress.progress_frontier().advanced_observations, 1);
        assert_eq!(cold_progress.progress_frontier().unchanged_observations, 1);
        let mut legacy_progress = context.clone();
        legacy_progress
            .payload
            .native_semantic_state
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("progress_frontier");
        assert!(!crate::native_semantic_checkpoint_compatible(
            &legacy_progress.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce
        ));
        let mut future_progress = context.clone();
        future_progress
            .payload
            .native_semantic_state
            .as_mut()
            .unwrap()["progress_frontier"]["schema_version"] = serde_json::json!(99);
        assert!(!crate::native_semantic_checkpoint_compatible(
            &future_progress.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce
        ));
        let mut inconsistent_progress = context.clone();
        inconsistent_progress
            .payload
            .native_semantic_state
            .as_mut()
            .unwrap()["progress_frontier"]["advanced_observations"] = serde_json::json!(0);
        assert!(!crate::native_semantic_checkpoint_compatible(
            &inconsistent_progress.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce,
        ));
        let mut legacy_mode = context.clone();
        legacy_mode
            .payload
            .native_semantic_state
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("no_progress_mode");
        assert!(!crate::native_semantic_checkpoint_compatible(
            &legacy_mode.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce
        ));

        let mut legacy_roots = context.clone();
        legacy_roots
            .payload
            .native_semantic_state
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("provider_history_roots");
        assert!(!crate::native_semantic_checkpoint_compatible(
            &legacy_roots.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce,
        ));
        let mut bad_roots = context.clone();
        bad_roots.payload.native_semantic_state.as_mut().unwrap()["provider_history_roots"][0]["version"] =
            serde_json::json!("v".repeat(257));
        assert!(!crate::native_semantic_checkpoint_compatible(
            &bad_roots.payload,
            &budget,
            pioneer_tools::ToolNoProgressMode::Enforce,
        ));
        assert_eq!(state.checkpoint_revision, 1);
        assert_eq!(state.pending_retry_instruction, pending);
        assert_eq!(state.applied_retry_instruction, applied);
        assert_eq!(state.observed_review_required_signatures, reviews);
        assert_eq!(state.observed_terminal_task_ids, terminal);
        assert!(
            state
                .task_mutation_guard
                .deterministic_failure_message()
                .is_some()
        );
        assert_eq!(state.post_turn_assistant_text, "Original post-turn summary");
        context.payload.workspace_id = "foreign".to_owned();
        assert!(
            super::semantic_state::NativeSemanticState::restore(
                Some(&context),
                "workspace_1",
                "thread_1",
                "turn_1",
                2,
                &budget,
                false,
                pioneer_tools::ToolNoProgressMode::Enforce,
            )
            .is_err()
        );
        context.payload.workspace_id = "workspace_1".to_owned();
        context.payload.native_semantic_state.as_mut().unwrap()["schema_version"] =
            serde_json::json!(crate::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION + 1);
        assert!(
            super::semantic_state::NativeSemanticState::restore(
                Some(&context),
                "workspace_1",
                "thread_1",
                "turn_1",
                2,
                &budget,
                false,
                pioneer_tools::ToolNoProgressMode::Enforce,
            )
            .is_err()
        );
        context.payload.native_semantic_state = None;
        assert!(
            super::semantic_state::NativeSemanticState::restore(
                Some(&context),
                "workspace_1",
                "thread_1",
                "turn_1",
                2,
                &budget,
                false,
                pioneer_tools::ToolNoProgressMode::Enforce,
            )
            .is_err()
        );
        assert!(
            super::semantic_state::NativeSemanticState::restore(
                None,
                "workspace_1",
                "thread_1",
                "turn_1",
                1,
                &budget,
                false,
                pioneer_tools::ToolNoProgressMode::Enforce,
            )
            .is_err()
        );
        let oversized = Some("界".repeat(32 * 1024));
        assert!(
            super::semantic_state::capture(
                1,
                "turn_1",
                &budget,
                &super::native_control::NativeControlState::default(),
                &[],
                pioneer_tools::ToolNoProgressMode::Enforce,
                &pioneer_tools::ToolProgressFrontier::default(),
                &controller,
                &lifecycle,
                &oversized,
                &None,
                &reviews,
                &terminal,
                &guard,
                "",
                &[],
                &[]
            )
            .is_err()
        );
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
            provenance: None,
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
            provider_replay_state: None,
            tool_outcome: Some(pioneer_provider::ToolResultOutcome::new(
                pioneer_provider::ToolResultStatus::Ok,
            )),
        }
    }

    #[test]
    fn native_delivery_plan_uses_runtime_home_root_not_workspace_root() {
        let workspace_root = temp_dir("workspace_root");
        let runtime_home = temp_dir("runtime_home");
        std::fs::write(workspace_root.join("SOUL.md"), "workspace soul")
            .expect("write workspace SOUL");
        std::fs::write(workspace_root.join("IDENTITY.md"), "workspace identity")
            .expect("write workspace IDENTITY");
        std::fs::write(runtime_home.join("SOUL.md"), "runtime soul").expect("write runtime SOUL");
        std::fs::write(runtime_home.join("IDENTITY.md"), "runtime identity")
            .expect("write runtime IDENTITY");

        let plan = compile_agent_instruction_delivery_plan_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            false,
            false,
            false,
            Some("thread_test"),
            "thread_test",
            "turn_test",
        )
        .expect("compile native delivery plan");

        assert!(plan.bundle.full_system_text.contains("runtime soul"));
        assert!(plan.bundle.full_system_text.contains("runtime identity"));
        assert!(!plan.bundle.full_system_text.contains("workspace soul"));
        assert!(!plan.bundle.full_system_text.contains("workspace identity"));
        assert_eq!(
            plan.provider_instructions.text,
            plan.bundle.full_system_text
        );
        let payload = compiled_prompt_payload_from_delivery_plan(&plan);
        assert_eq!(payload.full_system_text, plan.bundle.full_system_text);
        assert_eq!(payload.stable_system_text, plan.bundle.stable_system_text);
        assert_eq!(payload.dynamic_system_text, plan.bundle.dynamic_system_text);
        assert!(
            !plan
                .bundle
                .sections
                .iter()
                .any(|section| section.id == PromptSectionId::CurrentPermissions)
        );
    }

    #[test]
    fn agents_md_runtime_section_is_compiled_by_native_delivery_plan() {
        let runtime_home = temp_dir("agents_md_runtime_prompt");
        let agents_section = PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::AgentsMd),
            title: Some("AGENTS.md".to_owned()),
            content: "These instructions come from the effective AGENTS.md for this thread tree scope. System, developer, and explicit user messages take precedence.\n\n<AGENTS_MD>\nUse repo rules.\n</AGENTS_MD>".to_owned(),
            max_chars: Some(20_000),
            truncated: false,
        };

        let plan = compile_agent_instruction_delivery_plan_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[agents_section],
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            false,
            false,
            false,
            Some("thread_agents_md"),
            "thread_agents_md",
            "turn_agents_md",
        )
        .expect("compile native delivery plan");

        let agents_section = plan
            .bundle
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::AgentsMd)
            .expect("agents_md section should be compiled");
        assert_eq!(agents_section.title, "AGENTS.md");
        assert!(plan.bundle.dynamic_system_text.contains("## AGENTS.md"));
        assert!(plan.bundle.dynamic_system_text.contains("Use repo rules."));
    }

    #[test]
    fn current_permissions_section_is_compiled_for_restricted_native_delivery_plan() {
        let runtime_home = temp_dir("current_permissions_prompt");
        let profile = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );

        let plan = compile_agent_instruction_delivery_plan_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            &profile,
            false,
            true,
            false,
            Some("thread_permissions"),
            "thread_permissions",
            "turn_permissions",
        )
        .expect("compile native delivery plan");

        let permissions_section = plan
            .bundle
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::CurrentPermissions)
            .expect("current permissions section should be compiled");
        assert_eq!(permissions_section.title, "Current Permissions");
        assert!(
            plan.bundle
                .dynamic_system_text
                .contains("## Current Permissions")
        );
        assert!(
            plan.bundle
                .dynamic_system_text
                .contains("- mode: supervised")
        );
        assert!(
            plan.bundle
                .dynamic_system_text
                .contains("may require user approval")
        );
        assert!(
            !plan
                .bundle
                .dynamic_system_text
                .contains("ToolPermissionPolicySnapshot")
        );
    }

    #[test]
    fn request_tools_catalog_is_compiled_for_tool_enabled_native_delivery_plan() {
        let runtime_home = temp_dir("request_tools_catalog_prompt");

        let plan = compile_agent_instruction_delivery_plan_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            false,
            true,
            false,
            Some("thread_request_tools_catalog"),
            "thread_request_tools_catalog",
            "turn_request_tools_catalog",
        )
        .expect("compile native delivery plan");

        let catalog_section = plan
            .bundle
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
        assert!(plan.bundle.dynamic_system_text.contains(
            "If you need a hidden domain and its tools are not currently visible, call request_tools"
        ));
        for (domain, tool_names) in pioneer_tools::builtin_tool_domain_map() {
            let expected = format!("- {}: {}.", domain.as_str(), tool_names.join(", "));
            assert!(
                plan.bundle.dynamic_system_text.contains(expected.as_str()),
                "catalog missing domain line `{expected}`"
            );
        }
        assert!(!plan.bundle.dynamic_system_text.contains("mcp_"));
        assert!(!plan.bundle.dynamic_system_text.contains("mcp."));
        assert!(!plan.bundle.dynamic_system_text.contains("skill."));
        assert!(!plan.bundle.dynamic_system_text.contains("\"parameters\""));
        assert!(!plan.bundle.dynamic_system_text.contains("\"properties\""));
        assert!(
            !plan
                .bundle
                .dynamic_system_text
                .contains("\"additionalProperties\"")
        );
    }

    #[test]
    fn request_tools_catalog_is_omitted_from_no_tool_native_delivery_plan() {
        let runtime_home = temp_dir("request_tools_catalog_no_tools");

        let plan = compile_agent_instruction_delivery_plan_with_prompt_root(
            runtime_home.as_path(),
            None,
            None,
            &[],
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            false,
            false,
            false,
            Some("thread_no_request_tools_catalog"),
            "thread_no_request_tools_catalog",
            "turn_no_request_tools_catalog",
        )
        .expect("compile native delivery plan");

        assert!(
            !plan
                .bundle
                .sections
                .iter()
                .any(|section| section.id.manifest_id()
                    == pioneer_promt::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
        );
        assert!(
            !plan
                .bundle
                .dynamic_system_text
                .contains("Some tool domains and their tools are hidden until requested")
        );
    }

    #[test]
    fn retained_history_replaces_compacted_prefix_and_preserves_signed_atomic_tool_group() {
        use crate::compaction::history::{PendingOriginKind, pending_origin};
        let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            Some("working"),
            Some("opaque reasoning"),
            vec![ProviderToolCall {
                id: "native-call".into(),
                name: "read_file".into(),
                arguments: "{\"path\":\"/original\"}".into(),
            }],
            Some(pioneer_provider::ProviderReplayState::new(
                "test",
                serde_json::json!({"signature":"SIGNED_SENTINEL"}),
            )),
        );
        assistant.provenance = Some(pending_origin(
            "workspace",
            "thread",
            "turn",
            "round",
            PendingOriginKind::Assistant,
            "round",
        ));
        let mut result = ChatMessage::tool_result("native-call", "read_file", "result");
        result.provenance = Some(pending_origin(
            "workspace",
            "thread",
            "turn",
            "round",
            PendingOriginKind::ToolResult,
            "item",
        ));
        result.tool_outcome = Some(pioneer_provider::ToolResultOutcome::new(
            pioneer_provider::ToolResultStatus::Ok,
        ));
        // Pending metadata must remain pending, rather than becoming a reason
        // to discard native state or to declare a domain success.
        assistant.provenance.as_mut().unwrap().complete = false;
        result.provenance.as_mut().unwrap().complete = false;
        let mut retained = vec![];
        let mut sequence = 0;
        for generation in 0..256 {
            let mut root = ChatMessage::user(format!("acknowledged summary {generation}"));
            let mut origin = pending_origin(
                "workspace",
                "thread",
                "turn",
                "summary",
                PendingOriginKind::Assistant,
                "summary",
            );
            origin.unit_id = format!("checkpoint:owner:root-{generation}");
            origin.sources[0].scope = "checkpoint:owner".into();
            origin.sources[0].id = format!("root-{generation}");
            origin.sources[0].version = "exact-version".into();
            root.provenance = Some(origin);
            let accepted = vec![
                ChatMessage::system("instructions"),
                ChatMessage::user("original input"),
                root.clone(),
                assistant.clone(),
                result.clone(),
            ];
            let bytes = synchronize_retained_provider_history(
                &mut retained,
                &mut sequence,
                "workspace",
                "thread",
                "turn",
                &accepted,
            )
            .unwrap();
            assert!(bytes < 4096);
            assert_eq!(retained.len(), 3);
            assert_eq!(sequence, 3);
            assert_eq!(retained[0].message, root);
            assert_eq!(retained[1].message, assistant);
            assert_eq!(retained[2].message, result);
            let mut replay = vec![];
            append_recovered_provider_history(&mut replay, retained.clone());
            assert_eq!(
                replay,
                accepted[2..],
                "next window uses the accepted root and exact native suffix"
            );
        }
    }

    #[test]
    fn retained_history_rejects_split_units_missing_proof_and_capacity_without_replacement() {
        use crate::compaction::history::{PendingOriginKind, pending_origin};
        let mut assistant = ChatMessage::assistant("unchanged");
        assistant.provenance = Some(pending_origin(
            "workspace",
            "thread",
            "turn",
            "round",
            PendingOriginKind::Assistant,
            "round",
        ));
        let mut retained = vec![RetainedProviderHistoryMessage {
            sequence: 7,
            message: assistant.clone(),
        }];
        let original = retained.clone();
        let mut sequence = 8;
        let mut unrelated = ChatMessage::user("belongs to same indivisible unit");
        unrelated.provenance = assistant.provenance.clone();
        assert!(
            synchronize_retained_provider_history(
                &mut retained,
                &mut sequence,
                "workspace",
                "thread",
                "turn",
                &[assistant.clone(), unrelated]
            )
            .is_err()
        );
        assert_eq!(retained, original);
        assert_eq!(sequence, 8);
        assert!(
            synchronize_retained_provider_history(
                &mut retained,
                &mut sequence,
                "workspace",
                "thread",
                "turn",
                &[ChatMessage::assistant("unproven")]
            )
            .is_err()
        );
        assert_eq!(retained, original);
        let mut bytes = Some(MAX_RETAINED_PROVIDER_BYTES);
        assert!(
            retain_provider_history_message(&mut retained, &mut sequence, &mut bytes, &assistant)
                .is_err()
        );
        assert_eq!(retained, original);
        assert_eq!(sequence, 8);
        let mut bytes = None;
        sequence = i64::MAX;
        assert!(
            retain_provider_history_message(&mut retained, &mut sequence, &mut bytes, &assistant)
                .is_err()
        );
        assert_eq!(retained, original);
        let mut crowded = vec![assistant; MAX_RETAINED_PROVIDER_MESSAGES + 1];
        for (index, message) in crowded.iter_mut().enumerate() {
            message.provenance.as_mut().unwrap().unit_id = format!("turn:round-{index}");
        }
        assert!(
            synchronize_retained_provider_history(
                &mut retained,
                &mut sequence,
                "workspace",
                "thread",
                "turn",
                &crowded
            )
            .is_err()
        );
        assert_eq!(retained, original);
    }

    #[test]
    fn retained_history_counts_physical_wire_and_provenance_bytes_with_a_real_ceiling() {
        use std::io::Write;
        let mut message = ChatMessage::assistant("界\n\"immutable");
        message.provenance = Some(crate::compaction::history::pending_origin(
            "workspace",
            "thread",
            "turn",
            "round",
            crate::compaction::history::PendingOriginKind::Assistant,
            "round",
        ));
        assert_eq!(
            retained_provider_message_bytes(&message).unwrap(),
            serde_json::to_vec(&(&message, &message.provenance))
                .unwrap()
                .len()
        );
        let mut counter = RetainedProviderByteCounter(0);
        for _ in 0..MAX_RETAINED_PROVIDER_BYTES / 8192 {
            counter.write_all(&[b'a'; 8192]).unwrap();
        }
        assert_eq!(counter.0, MAX_RETAINED_PROVIDER_BYTES);
        assert!(counter.write_all(b"x").is_err());
        assert_eq!(counter.0, MAX_RETAINED_PROVIDER_BYTES);
    }

    #[test]
    fn recovery_context_replays_exact_provider_messages_in_sequence_order() {
        let mut messages = vec![ChatMessage::user("retry the turn")];
        let assistant_round = ChatMessage::assistant_tool_calls_with_provider_state(
            Some("working"),
            Some("opaque reasoning"),
            vec![
                ProviderToolCall {
                    id: "call_1".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{\"path\":\"/tmp/a\"}".to_owned(),
                },
                ProviderToolCall {
                    id: "call_2".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{\"path\":\"/tmp/b\"}".to_owned(),
                },
            ],
            Some(pioneer_provider::ProviderReplayState::new(
                "test",
                serde_json::json!({"signature": "SIGNED_SENTINEL"}),
            )),
        );

        append_recovered_provider_history(
            &mut messages,
            vec![
                RetainedProviderHistoryMessage {
                    sequence: 3,
                    message: ChatMessage::tool_result("call_2", "read_file", "SECOND_SENTINEL"),
                },
                RetainedProviderHistoryMessage {
                    sequence: 1,
                    message: assistant_round.clone(),
                },
                RetainedProviderHistoryMessage {
                    sequence: 2,
                    message: ChatMessage::tool_result("call_1", "read_file", "FIRST_SENTINEL"),
                },
            ],
        );

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1], assistant_round);
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("call_1"));
        assert!(messages[2].content.contains("FIRST_SENTINEL"));
        assert_eq!(messages[3].tool_call_id.as_deref(), Some("call_2"));
        assert!(messages[3].content.contains("SECOND_SENTINEL"));
    }

    #[test]
    fn agent_policy_keeps_latest_snapshot_per_session_and_preserves_user_files() {
        let mut messages = vec![
            ChatMessage {
                provenance: None,
                role: pioneer_provider::Role::User,
                content: "Analyze this file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/task.pdf", Some(1024 * 1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
                provider_replay_state: None,
                tool_outcome: None,
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
    fn agent_policy_preserves_current_snapshots_even_when_they_exceed_byte_budget() {
        let mut messages = vec![
            ChatMessage {
                provenance: None,
                role: pioneer_provider::Role::User,
                content: "Keep this PDF pinned".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/pinned.pdf", Some(400))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
                provider_replay_state: None,
                tool_outcome: None,
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
        assert!(messages[1].content.contains("Earlier screenshot omitted"));
    }

    #[test]
    fn current_visual_over_budget_is_explicitly_rejected_by_actual_admission() {
        let bytes = [1_u8; 64];
        let mut frame = computer_use_snapshot_message("current", 1, "/current.png");
        if let MessageContentPart::Image { image } = &mut frame.content_parts[0] {
            image.source = pioneer_provider::attachments::owned_bytes_source(&bytes);
            image.size_bytes = Some(bytes.len() as u64);
        }
        let mut messages = vec![frame];
        retain_agent_attachment_messages(&mut messages);
        assert!(messages[0].has_attachments());
        let config = pioneer_provider::attachments::AttachmentPipelineConfig {
            max_total_bytes_per_request: 32,
            ..Default::default()
        };
        let mut capabilities = pioneer_provider::ProviderCapabilities::default();
        capabilities.vision = true;
        capabilities.input_types.image = pioneer_provider::InputTypeSupport::data_url_inline_only();
        let error = pioneer_provider::attachments::prepare_messages_for_provider_with_config(
            "fixture",
            &capabilities,
            &messages,
            &config,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("ATTACHMENT_TOO_LARGE")
                || error
                    .to_string()
                    .contains("ATTACHMENT_TOTAL_BUDGET_EXCEEDED")
        );
        assert!(messages[0].has_attachments());
    }

    #[test]
    fn unknown_failed_and_unscoped_frames_cannot_prove_supersession() {
        for outcome in [
            None,
            Some(pioneer_provider::ToolResultOutcome::new(
                pioneer_provider::ToolResultStatus::RecoverableError,
            )),
            Some(pioneer_provider::ToolResultOutcome {
                schema_version: 2,
                status: pioneer_provider::ToolResultStatus::Ok,
            }),
        ] {
            let mut messages = vec![
                computer_use_snapshot_message("old", 1, "/old.png"),
                computer_use_snapshot_message("new", 1, "/new.png"),
            ];
            messages[1].tool_outcome = outcome;
            retain_agent_attachment_messages(&mut messages);
            assert!(messages.iter().all(ChatMessage::has_attachments));
        }
        let mut messages = vec![
            computer_use_snapshot_message("old", 1, "/old.png"),
            computer_use_snapshot_message("new", 1, "/new.png"),
        ];
        messages[1].content = r#"{"action":"snapshot"}"#.to_owned();
        retain_agent_attachment_messages(&mut messages);
        assert!(messages.iter().all(ChatMessage::has_attachments));
    }

    #[test]
    fn chat_policy_keeps_only_latest_user_attachment_message() {
        let mut messages = vec![
            ChatMessage {
                provenance: None,
                role: pioneer_provider::Role::User,
                content: "old file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/old.pdf", Some(1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
                provider_replay_state: None,
                tool_outcome: None,
            },
            ChatMessage {
                provenance: None,
                role: pioneer_provider::Role::Tool,
                content: "tool-1".to_owned(),
                reasoning_content: None,
                content_parts: vec![image_part("/tmp/snapshot.png", Some(1024))],
                tool_call_id: Some("call_1".to_owned()),
                name: Some("computer_use".to_owned()),
                tool_calls: None,
                provider_replay_state: None,
                tool_outcome: None,
            },
            ChatMessage {
                provenance: None,
                role: pioneer_provider::Role::User,
                content: "new file".to_owned(),
                reasoning_content: None,
                content_parts: vec![file_part("/tmp/new.pdf", Some(1024))],
                tool_call_id: None,
                name: None,
                tool_calls: None,
                provider_replay_state: None,
                tool_outcome: None,
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

#[cfg(test)]
mod compaction_result_budget_tests {
    use super::*;
    #[test]
    fn inline_binary_result_uses_a_reference_to_the_retained_part() {
        let mut source = ChatMessage::tool("binary result");
        let mut attachment = pioneer_provider::MessageAttachment::from_path("fixture", "image/png");
        attachment.source = pioneer_provider::AttachmentDataSource::Bytes {
            base64_data: "A".repeat(300_000),
        };
        source
            .content_parts
            .push(pioneer_provider::MessageContentPart::image(attachment));
        let bounded = bounded_tool_result_message(&source, "fixture-source").unwrap();
        assert!(bounded.content_parts.is_empty());
        assert!(
            bounded.content.contains("fixture-source") && bounded.content.contains("content_part")
        );
        assert!(bounded.content.len() < 1000);
        assert_eq!(source.content_parts.len(), 1);
    }
    #[test]
    fn result_budget_rejects_oversized_non_text_framing_without_looping() {
        let mut source = ChatMessage::tool("");
        source
            .content_parts
            .push(pioneer_provider::MessageContentPart::file(
                pioneer_provider::MessageAttachment::from_path(
                    "x".repeat(70_000),
                    "application/octet-stream",
                ),
            ));
        assert!(bounded_tool_result_message(&source, "fixture-ref").is_err());
    }
    #[test]
    fn result_budget_counts_serialized_framing_and_all_text_parts() {
        let mut source = ChatMessage::tool("\"\\\n".repeat(30_000));
        source.name = Some("fixture".into());
        source
            .content_parts
            .push(pioneer_provider::MessageContentPart::text(
                "🌍漢字".repeat(30_000),
            ));
        let page = bounded_tool_result_message(&source, "fixture-ref").unwrap();
        let wire = serde_json::to_string(&page).unwrap();
        assert!(wire.len() <= pioneer_compaction::RESULT_BYTES);
        assert!(pioneer_compaction::text_tokens(&wire) <= pioneer_compaction::RESULT_TOKENS);
        assert!(page.content_parts.is_empty());
        assert!(page.content.contains("fixture-ref"));
        assert!(source.content_parts.len() == 1);
    }
}

#[cfg(test)]
mod cold_initial_owner_tests {
    #[tokio::test]
    async fn cold_restore_keeps_skill_bridge_without_initial_hooks_and_rejects_unknown_owner() {
        let mut outputs = crate::native_initial::tests::outputs();
        outputs.provider_tool_calling = true;
        let restored = super::restore_native_initial_tool_owners(
            &outputs,
            None,
            None,
            None,
            None,
            &crate::AgentTurnHookRuntimeContext::default(),
            "original input",
            "workspace",
            "thread",
            "turn",
        )
        .await
        .unwrap();
        assert!(restored.skills.is_some());
        assert!(restored.bundles.is_empty());
        outputs
            .tool_metadata
            .entries
            .push(pioneer_hooks::HookToolBundleEntry {
                contribution_id: pioneer_hooks::HookContributionId::new("custom.contribution.0")
                    .unwrap(),
                bundle_id: pioneer_hooks::HookToolBundleId::new("custom.bundle.0").unwrap(),
                domain: pioneer_hooks::HookDomain::new("custom").unwrap(),
                priority: 1,
                tool_names: Vec::new(),
                diagnostics: Vec::new(),
            });
        outputs.extension_specs.push(Vec::new());
        assert!(
            super::restore_native_initial_tool_owners(
                &outputs,
                None,
                None,
                None,
                None,
                &crate::AgentTurnHookRuntimeContext::default(),
                "original input",
                "workspace",
                "thread",
                "turn"
            )
            .await
            .is_err()
        );
    }
}
