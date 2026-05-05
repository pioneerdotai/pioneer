mod provider;
mod skill_tools;
mod skills;
mod tool_recovery_policy;
mod tool_retry_lifecycle;
mod tooling;

use self::tool_retry_lifecycle::{
    ToolRetryLifecycleTracker, emit_tool_loop_budget_exceeded, emit_tool_retry_drafts,
    turn_item_type_code,
};
use crate::hooks::{
    AgentTurnHookContext, EffectiveTurnPromptSectionSet, run_agent_turn_policy_hook_phase,
    run_agent_turn_prompt_compile_hook_phase, run_agent_turn_prompt_context_hook_phase,
    run_noop_agent_turn_hook_phase,
};
use crate::memory::{
    filter_memory_tool_materialization, memory_recall_prompt_input, memory_tool_names,
    resolve_memory_turn_policy,
};
use crate::{
    AgentEventHub, AgentEventHubError, AgentMcpAvailability, AgentMcpMaterialization,
    AgentMcpToolProvider, AgentMemoryProvider, AgentMemoryTurnPolicyProvider, MemoryRecallRequest,
    MemoryRecallSnapshot, MemoryToolMaterialization, MemoryTurnContext, MemoryTurnPolicyContext,
    MemoryTurnPolicyRequest, RetainedToolLlmContext, TaskToolMaterialization, TaskToolProvider,
    TaskTurnContext, TerminalTaskObservation, ToolLoopConfig, TurnExecutionControl,
};
use chrono::Local;
use futures_util::{StreamExt, stream};
use pioneer_config::AppConfig;
use pioneer_hooks::{HookPhase, HookRuntime};
use pioneer_promt::{
    CompiledPromptBundle, DynamicPromptSectionInput, PromptCompileInput, PromptDiagnosticCode,
    PromptDynamicSectionId, PromptLimits, PromptProfile, ToolRetryInstructionKind, compile_prompt,
    render_memory_recall_prompt, render_tool_retry_instruction, tool_loop_final_answer_instruction,
};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, PromptManifest, PromptManifestDiagnostic,
    PromptManifestDiagnosticCode, PromptManifestProfile, ProviderFailureDetails,
    RecoveryAttemptContext, ThreadMode, ToolRecoveryPolicySnapshot, TurnItem, TurnItemType,
    UserInput, generate_id,
};
use pioneer_provider::{
    AttachmentDataSource, ChatMessage, ChatRequest, CompiledPromptPayload, InputContentType,
    MessageAttachment, MessageContentPart, ModelInputItem, Provider, ProviderToolCall,
    ToolDefinition, infer_mime_from_reference,
};
use pioneer_skills::SkillPolicyKey;
use pioneer_tools::{
    RawToolCall, ToolLoopGuard, ToolLoopGuardDecision, ToolOutcome, ToolRecoveryView,
    ToolRetryController, ToolRetryDecision, ToolRetryObservation, build_builtin_tools, build_tools,
    classify_tool_error,
};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const TURN_ITEM_ID_LEN: usize = 21;
const DISCOVERY_TOOL_SEARCH: &str = "tool_search";
const DISCOVERY_TOOL_SUGGEST: &str = "tool_suggest";
const PROVIDER_FIRST_CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
const PROVIDER_INTER_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_TERMINAL_TASK_OBSERVATIONS: usize = 20;

#[derive(Debug, Default, Clone)]
struct PendingToolUiState {
    tool_name: String,
    arguments: String,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    output_policy: Option<pioneer_protocol::ToolOutputPolicySnapshot>,
    latest_observation: Option<pioneer_protocol::ToolObservation>,
    started_sent: bool,
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
    message: ChatMessage,
}

#[derive(Debug)]
struct RenderedTaskObservation {
    task_ids: Vec<String>,
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

#[derive(Debug, Clone)]
pub(super) enum ChatTurnError {
    Terminal(String),
    ProviderFailure {
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
    },
}

fn agent_event_error(error: AgentEventHubError) -> ChatTurnError {
    ChatTurnError::Terminal(format!("failed to publish agent event: {error}"))
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

fn dynamic_prompt_sections_from_hook_sections(
    section_set: &EffectiveTurnPromptSectionSet,
) -> Result<Vec<DynamicPromptSectionInput>, ChatTurnError> {
    if section_set.is_empty() {
        return Ok(Vec::new());
    }
    let sections = section_set.clone_hook_prompt_section_set();

    sections
        .entries()
        .map(|entry| {
            let id = PromptDynamicSectionId::new(entry.section_id.as_str()).map_err(|error| {
                ChatTurnError::Terminal(format!(
                    "failed to convert hook prompt section `{}`: {error}",
                    entry.section_id
                ))
            })?;
            Ok(DynamicPromptSectionInput {
                id,
                title: entry.title.as_ref().map(|title| title.as_str().to_owned()),
                content: entry.content.as_str().to_owned(),
                max_chars: None,
                truncated: entry.truncated,
            })
        })
        .collect()
}

fn compile_agent_prompt_bundle(
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    memory_recall: Option<String>,
    dynamic_sections: &[DynamicPromptSectionInput],
    include_task_orchestration_policy: bool,
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
        memory_recall,
        dynamic_sections,
        include_task_orchestration_policy,
        continue_generation_hint,
        thread_id,
        turn_id,
    )
}

fn compile_agent_prompt_bundle_with_prompt_root(
    prompt_root: &std::path::Path,
    skills_prompt: Option<String>,
    retry_instruction: Option<String>,
    memory_recall: Option<String>,
    dynamic_sections: &[DynamicPromptSectionInput],
    include_task_orchestration_policy: bool,
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

    let bundle = compile_prompt(PromptCompileInput {
        workspace_root: prompt_root.to_path_buf(),
        profile: PromptProfile::AssistantFull,
        skills_prompt,
        retry_instruction,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy,
        continue_generation_hint,
        memory_recall,
        dynamic_sections: dynamic_sections.to_vec(),
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

fn prompt_manifest_from_bundle(bundle: &CompiledPromptBundle) -> PromptManifest {
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
        diagnostics: bundle
            .diagnostics
            .iter()
            .map(|diagnostic| PromptManifestDiagnostic {
                code: prompt_diagnostic_code(diagnostic.code),
                message: diagnostic.message.clone(),
                file: diagnostic.file.clone(),
                section_id: diagnostic.section_id.clone(),
            })
            .collect::<Vec<_>>(),
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
    provider: Arc<dyn Provider>,
    model: String,
    workspace_skill_policies: HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    input: Vec<UserInput>,
    history: Vec<ChatMessage>,
    retained_llm_context: Vec<RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    memory_turn_policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_tx: Arc<AgentEventHub>,
) -> Result<(), ChatTurnError> {
    let user_message = build_user_message(input.as_slice());

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
        ThreadMode::Agent => {
            execute_agent_provider_response(
                &provider,
                model,
                history,
                user_message.clone(),
                &input,
                &workspace_skill_policies,
                retained_llm_context,
                force_non_stream,
                continue_generation_hint,
                tool_loop_config,
                mcp_tool_provider,
                task_tool_provider,
                memory_provider,
                memory_turn_policy_provider,
                hook_runtime,
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

            result
        }
    };

    match result {
        Ok(()) => Ok(()),
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
    event_tx: Arc<AgentEventHub>,
) -> Result<(), ChatTurnError> {
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
) -> AgentMcpMaterialization {
    let Some(provider) = provider else {
        return AgentMcpMaterialization::default();
    };
    match provider.materialize_mcp_tools(workspace_id, turn_id).await {
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

fn memory_turn_context(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    mode: ThreadMode,
    input_text: String,
) -> MemoryTurnContext {
    MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode,
        input_text,
        task_id: None,
        agent_id: None,
    }
}

fn memory_recall_request(input_text: &str) -> MemoryRecallRequest {
    MemoryRecallRequest {
        query: input_text.to_owned(),
        categories: Vec::new(),
        top_k: Some(5),
        max_chars: Some(1_500),
    }
}

async fn load_memory_recall_snapshot(
    provider: Option<&Arc<dyn AgentMemoryProvider>>,
    context: MemoryTurnContext,
    request: MemoryRecallRequest,
) -> MemoryRecallSnapshot {
    let Some(provider) = provider else {
        return MemoryRecallSnapshot::empty();
    };
    match provider.recall_memory(context.clone(), request).await {
        Ok(snapshot) => {
            for diagnostic in &snapshot.diagnostics {
                warn!(
                    thread_id = context.thread_id.as_str(),
                    turn_id = context.turn_id.as_str(),
                    diagnostic = diagnostic.as_str(),
                    "memory recall provider reported diagnostic"
                );
            }
            snapshot
        }
        Err(error) => {
            warn!(
                thread_id = context.thread_id.as_str(),
                turn_id = context.turn_id.as_str(),
                error = error.as_str(),
                "memory recall provider failed; continuing without memory recall"
            );
            MemoryRecallSnapshot::empty()
        }
    }
}

async fn materialize_memory_tooling(
    provider: Option<&Arc<dyn AgentMemoryProvider>>,
    context: MemoryTurnContext,
) -> MemoryToolMaterialization {
    let Some(provider) = provider else {
        return MemoryToolMaterialization::default();
    };
    match provider.materialize_memory_tools(context.clone()).await {
        Ok(materialization) => {
            for diagnostic in &materialization.diagnostics {
                warn!(
                    thread_id = context.thread_id.as_str(),
                    turn_id = context.turn_id.as_str(),
                    diagnostic = diagnostic.as_str(),
                    "memory tool materialization reported diagnostic"
                );
            }
            materialization
        }
        Err(error) => {
            warn!(
                thread_id = context.thread_id.as_str(),
                turn_id = context.turn_id.as_str(),
                error = error.as_str(),
                "memory tool materialization failed; continuing without memory tools"
            );
            MemoryToolMaterialization::default()
        }
    }
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
        "Attached tasks created by this turn are still active, so the turn cannot finish yet.\n{task_lines}\nCall exactly one of task_wait, task_cancel, or task_detach for these task ids before giving the final answer."
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
        _ => {}
    }
}

fn terminal_task_status_label(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

async fn execute_agent_provider_response(
    provider: &Arc<dyn Provider>,
    model: String,
    history: Vec<ChatMessage>,
    user_message: ChatMessage,
    input: &[UserInput],
    workspace_skill_policies: &HashMap<SkillPolicyKey, crate::WorkspaceSkillPolicy>,
    retained_llm_context: Vec<RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    memory_turn_policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    turn_control: TurnExecutionControl,
    mut recovery: Option<RecoveryAttemptContext>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    initial_thinking_item_id: String,
    message_item_id: &str,
    event_tx: Arc<AgentEventHub>,
) -> Result<(), ChatTurnError> {
    let workdir = std::env::current_dir()
        .map_err(|error| ChatTurnError::Terminal(format!("failed to resolve cwd: {error}")))?;

    let tool_loop_config = tool_loop_config.normalized();
    let provider_tool_calling = provider.capabilities().tool_calling;
    let hook_context = AgentTurnHookContext::new(workspace_id, thread_id, turn_id);

    let effective_policy_set =
        run_agent_turn_policy_hook_phase(hook_runtime.as_ref(), &hook_context)
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

    let memory_context = memory_turn_context(
        workspace_id,
        thread_id,
        turn_id,
        ThreadMode::Agent,
        user_message.text_content_lossy(),
    );
    let memory_turn_policy = resolve_memory_turn_policy(
        memory_turn_policy_provider.as_ref(),
        MemoryTurnPolicyContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            mode: ThreadMode::Agent,
            input_text: memory_context.input_text.clone(),
            model: Some(model.clone()),
            model_provider: Some(provider.name().to_owned()),
        },
        MemoryTurnPolicyRequest::default(),
    )
    .await;
    for diagnostic in &memory_turn_policy.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "memory turn policy reported diagnostic"
        );
    }

    let effective_prompt_context_set = run_agent_turn_prompt_context_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        &effective_policy_set,
    )
    .await;

    let mcp_availability =
        load_mcp_availability(mcp_tool_provider.as_ref(), workspace_id, thread_id, turn_id).await;

    let skills_resolution = match skills::resolve_turn_skills(
        workdir.as_path(),
        workspace_id,
        input,
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

    if !provider_tool_calling {
        let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            &effective_policy_set,
            &effective_prompt_context_set,
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
        let dynamic_prompt_sections =
            dynamic_prompt_sections_from_hook_sections(&effective_prompt_section_set)?;

        let initial_prompt_bundle = compile_agent_prompt_bundle(
            skills_prompt.clone(),
            None,
            None,
            dynamic_prompt_sections.as_slice(),
            include_task_orchestration_policy,
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
                manifest: prompt_manifest_from_bundle(&initial_prompt_bundle),
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
            event_tx.clone(),
        )
        .await;

        if result.is_ok() {
            turn_control
                .succeed_recovery_attempt(turn_id, recovery.take())
                .await;
            run_noop_agent_turn_hook_phase(
                hook_runtime.as_ref(),
                &hook_context,
                HookPhase::TurnPostTurn,
                &effective_policy_set,
                &effective_prompt_context_set,
            )
            .await;
        }

        return result;
    }

    let memory_tool_materialization = if memory_turn_policy.allows_any_memory_tool() {
        let raw_materialization =
            materialize_memory_tooling(memory_provider.as_ref(), memory_context.clone()).await;
        filter_memory_tool_materialization(raw_materialization, &memory_turn_policy)
    } else {
        MemoryToolMaterialization::default()
    };
    for diagnostic in &memory_tool_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "memory tool materialization reported diagnostic"
        );
    }

    let mcp_materialization =
        materialize_mcp_tooling(mcp_tool_provider.as_ref(), workspace_id, turn_id, thread_id).await;
    for diagnostic in &mcp_materialization.diagnostics {
        warn!(
            thread_id,
            turn_id,
            diagnostic = diagnostic.as_str(),
            "MCP dynamic tool materialization reported diagnostic"
        );
    }
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

    let mut extension_bundles = skill_tool_materialization.bundles.clone();
    extension_bundles.extend(mcp_materialization.bundles.clone());
    extension_bundles.extend(task_materialization.bundles.clone());
    extension_bundles.extend(memory_tool_materialization.bundles.clone());

    let tools = match build_tools(
        workdir.clone(),
        turn_id.to_owned(),
        tool_loop_config.web.clone(),
        tool_loop_config.computer_use.clone(),
        extension_bundles,
    ) {
        Ok(tools) => tools,
        Err(error) => {
            warn!(
                thread_id,
                turn_id,
                error = %error,
                "failed to build tool runtime with extensions; continuing with built-ins only"
            );
            build_builtin_tools(
                workdir.clone(),
                turn_id.to_owned(),
                tool_loop_config.web.clone(),
                tool_loop_config.computer_use.clone(),
            )
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

    let all_tool_names = router
        .all_specs()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    let all_tool_name_set = all_tool_names.iter().cloned().collect::<BTreeSet<_>>();
    let available_memory_tool_names = memory_tool_names(&memory_tool_materialization)
        .into_iter()
        .filter(|name| all_tool_name_set.contains(name))
        .collect::<Vec<_>>();
    let memory_recall_snapshot =
        if memory_turn_policy.allow_pre_turn_recall() && !available_memory_tool_names.is_empty() {
            load_memory_recall_snapshot(
                memory_provider.as_ref(),
                memory_context.clone(),
                memory_recall_request(memory_context.input_text.as_str()),
            )
            .await
        } else {
            MemoryRecallSnapshot::empty()
        };
    let memory_recall_prompt =
        if memory_turn_policy.allow_memory_prompt() && !available_memory_tool_names.is_empty() {
            if let Some(prompt_policy) = memory_turn_policy.recall_prompt_policy() {
                let prompt_input = memory_recall_prompt_input(
                    available_memory_tool_names,
                    prompt_policy,
                    memory_recall_snapshot,
                );
                render_memory_recall_prompt(&prompt_input)
            } else {
                None
            }
        } else {
            None
        };

    let effective_prompt_section_set = run_agent_turn_prompt_compile_hook_phase(
        hook_runtime.as_ref(),
        &hook_context,
        &effective_policy_set,
        &effective_prompt_context_set,
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
    let dynamic_prompt_sections =
        dynamic_prompt_sections_from_hook_sections(&effective_prompt_section_set)?;

    let initial_prompt_bundle = compile_agent_prompt_bundle(
        skills_prompt.clone(),
        None,
        memory_recall_prompt.clone(),
        dynamic_prompt_sections.as_slice(),
        include_task_orchestration_policy,
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
            manifest: prompt_manifest_from_bundle(&initial_prompt_bundle),
        },
    )
    .await?;

    let mut visible_tool_names = all_tool_names.clone();

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
    let mut observed_terminal_task_ids = BTreeSet::<String>::new();
    let mut tool_loop_guard = ToolLoopGuard::new(
        tool_loop_config.budget.clone(),
        tool_loop_final_answer_instruction(),
    );
    let mut tool_retry_controller = ToolRetryController::new(tool_loop_config.retry.clone());
    let mut tool_retry_lifecycle = ToolRetryLifecycleTracker::default();

    let turn_result: Result<(), (ChatTurnError, String)> = async {
        let mut current_thinking_id = initial_thinking_item_id;

        loop {
            retain_agent_attachment_messages(&mut messages);

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
                        memory_recall_prompt.clone(),
                        dynamic_prompt_sections.as_slice(),
                        include_task_orchestration_policy,
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
                            manifest: prompt_manifest_from_bundle(&refreshed_prompt_bundle),
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

            let round_compiled_prompt = if round_plan.tools_enabled || memory_recall_prompt.is_none()
            {
                active_compiled_prompt.clone()
            } else {
                let prompt_without_memory = compile_agent_prompt_bundle(
                    skills_prompt.clone(),
                    applied_retry_instruction.clone(),
                    None,
                    dynamic_prompt_sections.as_slice(),
                    include_task_orchestration_policy,
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
                        manifest: prompt_manifest_from_bundle(&prompt_without_memory),
                    },
                )
                .await
                .map_err(|error| (error, current_thinking_id.clone()))?;
                Some(compiled_prompt_payload_from_bundle(&prompt_without_memory))
            };

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
                    compiled_prompt: round_compiled_prompt,
                },
                workspace_id,
                thread_id,
                turn_id,
                current_thinking_id.as_str(),
                force_non_stream,
                event_tx.as_ref(),
            )
            .await
            .map_err(|e| (e, current_thinking_id.clone()))?;

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
                            memory_recall_prompt.clone(),
                            dynamic_prompt_sections.as_slice(),
                            include_task_orchestration_policy,
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
                                manifest: prompt_manifest_from_bundle(&refreshed_prompt_bundle),
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
                    return Err((
                        ChatTurnError::Terminal(message),
                        current_thinking_id.clone(),
                    ));
                }
            }

            if round.tool_calls.is_empty() {
                if round_plan.tools_enabled && pending_retry_instruction.take().is_some() {
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
                    continue;
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

                if !round.text.is_empty() {
                    emit_progress_event(
                        event_tx.as_ref(),
                        AgentProgressEvent::ItemDelta {
                            notification: ItemDeltaNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item_id: message_item_id.to_owned(),
                                delta: round.text.clone(),
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
                                text: round.text,
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
                                    message: tooling::build_tool_error_message(
                                        model_tool_call.id,
                                        tool_name,
                                        error_text,
                                        outcome,
                                    ),
                                };
                            }
                        }

                        let tool_call = match router.build_tool_call(RawToolCall {
                            call_id: model_tool_call.id.clone(),
                            tool_name: tool_name.clone(),
                            arguments: arguments.clone(),
                        }) {
                            Ok(tool_call) => tool_call,
                            Err(error) => {
                                let error_text = error.to_string();
                                {
                                    let mut pending = pending_tool_ui.lock().await;
                                    pending.remove(item_id.as_str());
                                }
                                let output_policy = router
                                    .find_spec(tool_name.as_str())
                                    .map(|configured| configured.output_policy.clone());
                                let outcome = classify_tool_error(
                                    tool_name.as_str(),
                                    &pioneer_tools::ToolError::invalid_arguments(
                                        error_text.clone(),
                                    ),
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

                        let (tool_output, success, outcome, recovery_view, message) =
                            match dispatch_result {
                                Ok(result) => (
                                    result.model_visible_text(),
                                    result.success(),
                                    result.outcome.clone(),
                                    result
                                        .projection()
                                        .and_then(|projection| projection.recovery.clone()),
                                    result.to_model_input_item().into_chat_message(),
                                ),
                                Err(error) => {
                                    let output = error.to_string();
                                    let outcome = classify_tool_error(tool_name.as_str(), &error);
                                    let message = tooling::build_tool_error_message(
                                        model_tool_call.id.clone(),
                                        tool_name.clone(),
                                        output.clone(),
                                        outcome.clone(),
                                    );
                                    (output, false, outcome, None, message)
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

            for result in &executed_results {
                record_observed_terminal_task_ids(&mut observed_terminal_task_ids, result);
            }

            tooling::maybe_update_visible_tools_from_suggestions(
                executed_results.as_slice(),
                all_tool_names.as_slice(),
                &mut visible_tool_names,
            );

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
                    memory_recall_prompt.clone(),
                    dynamic_prompt_sections.as_slice(),
                    include_task_orchestration_policy,
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
                        manifest: prompt_manifest_from_bundle(&refreshed_prompt_bundle),
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
    let turn_succeeded = turn_result.is_ok();

    skill_tool_materialization
        .clear_function_proxy_runtime()
        .await;
    drop(runtime);
    drop(router);
    drop(tools);

    let _ = tool_event_forwarder.await;

    if turn_succeeded {
        run_noop_agent_turn_hook_phase(
            hook_runtime.as_ref(),
            &hook_context,
            HookPhase::TurnPostTurn,
            &effective_policy_set,
            &effective_prompt_context_set,
        )
        .await;
    }

    match turn_result {
        Ok(()) => Ok(()),
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
            Err(match error {
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
            })
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

fn build_user_message(input: &[UserInput]) -> ChatMessage {
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
            UserInput::Text { .. } | UserInput::Skill { .. } | UserInput::Mention { .. } => {}
        }
    }

    message
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
        append_recovered_tool_llm_context, build_user_message,
        compile_agent_prompt_bundle_with_prompt_root, retain_agent_attachment_messages,
        retain_agent_attachment_messages_with_budget, retain_chat_mode_attachment_messages,
    };
    use crate::RetainedToolLlmContext;
    use pioneer_protocol::UserInput;
    use pioneer_provider::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart, Role,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

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
            None,
            &[],
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

        let message = build_user_message(input.as_slice());

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

        let message = build_user_message(input.as_slice());

        assert_eq!(message.content, "first\nsecond");
        assert!(message.content_parts.is_empty());
    }
}
