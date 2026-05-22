#![allow(dead_code)]

use pioneer_memory::hooks::{
    ActiveRecallPlan, ActiveRecallPlanJson, DeterministicRecallContextSummary,
    MemoryActiveRecallDecisionContext, MemoryActiveRecallDecisionRequest,
    MemoryActiveRecallLocalPlan, normalize_active_recall_plan, parse_active_memory_decision_json,
};
use pioneer_promt::{
    TurnPreflightMemoryActiveRecallPromptInput, TurnPreflightPromptInput,
    render_turn_preflight_prompt,
};
use pioneer_protocol::ThreadMode;
use pioneer_provider::{ChatMessage, ChatRequest, Provider, ProviderRegistry, ReasoningConfig};
use pioneer_tools::{BuiltinToolDomain, PreflightToolIndex};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS: usize = 160;
const PREFLIGHT_DIAGNOSTIC_MAX_COUNT: usize = 16;
const PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS: usize = 512;
pub(crate) const TURN_PREFLIGHT_PROVIDER_DEFAULT_TIMEOUT_MS: u64 = 60_000;
pub(crate) const TURN_PREFLIGHT_PROVIDER_DEFAULT_MAX_OUTPUT_CHARS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightInput {
    pub turn: TurnPreflightTurnInput,
    pub tools: TurnPreflightToolsInput,
    pub memory: TurnPreflightMemoryInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightTurnInput {
    pub has_workspace_id: bool,
    pub has_thread_id: bool,
    pub has_turn_id: bool,
    pub thread_mode: ThreadMode,
    pub provider_tool_calling: bool,
    pub input_text_preview: String,
    pub input_text_char_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightToolsInput {
    pub core_tools: Vec<String>,
    pub candidate_tools: Vec<TurnPreflightCandidateTool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightCandidateTool {
    pub name: String,
    pub domain: BuiltinToolDomain,
    pub summary: String,
    pub mutation: bool,
}

pub(crate) fn turn_preflight_tools_input_from_index(
    index: PreflightToolIndex,
) -> TurnPreflightToolsInput {
    TurnPreflightToolsInput {
        core_tools: index.core_tools,
        candidate_tools: index
            .candidate_tools
            .into_iter()
            .map(|candidate| TurnPreflightCandidateTool {
                name: candidate.name,
                domain: candidate.domain,
                summary: candidate.summary,
                mutation: candidate.mutation,
            })
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryInput {
    pub deterministic_summary: DeterministicRecallContextSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_recall: Option<TurnPreflightMemoryActiveRecallInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryActiveRecallInput {
    pub decision_context: MemoryActiveRecallDecisionContext,
    pub decision_request: MemoryActiveRecallDecisionRequest,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TurnPreflightLocalModulePlans {
    pub tools: TurnPreflightLocalToolsState,
    pub memory: TurnPreflightLocalMemoryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightLocalToolsState {
    pub input: TurnPreflightToolsInput,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TurnPreflightLocalMemoryState {
    pub deterministic_summary: DeterministicRecallContextSummary,
    pub active_recall: TurnPreflightLocalActiveRecallState,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TurnPreflightLocalActiveRecallState {
    HostLocalFinal(TurnPreflightMemoryActiveRecallPlan),
    ProviderNeeded(TurnPreflightMemoryActiveRecallInput),
}

pub(crate) fn build_local_preflight_module_plans(
    tool_index: PreflightToolIndex,
    deterministic_summary: DeterministicRecallContextSummary,
    active_recall: MemoryActiveRecallLocalPlan,
) -> TurnPreflightLocalModulePlans {
    let active_recall = if active_recall.provider_planning_needed {
        TurnPreflightLocalActiveRecallState::ProviderNeeded(TurnPreflightMemoryActiveRecallInput {
            decision_context: active_recall.decision_context,
            decision_request: active_recall.decision_request,
        })
    } else {
        TurnPreflightLocalActiveRecallState::HostLocalFinal(wrap_memory_active_recall_plan(
            TurnPreflightPlanSource::HostLocal,
            None,
            active_recall.local_decision,
        ))
    };

    TurnPreflightLocalModulePlans {
        tools: TurnPreflightLocalToolsState {
            input: turn_preflight_tools_input_from_index(tool_index),
        },
        memory: TurnPreflightLocalMemoryState {
            deterministic_summary,
            active_recall,
        },
    }
}

impl TurnPreflightLocalModulePlans {
    pub(crate) fn provider_input(&self, turn: TurnPreflightTurnInput) -> TurnPreflightInput {
        TurnPreflightInput {
            turn,
            tools: self.tools.input.clone(),
            memory: self.memory.provider_input(),
        }
    }

    pub(crate) fn active_recall_provider_planning_needed(&self) -> bool {
        matches!(
            self.memory.active_recall,
            TurnPreflightLocalActiveRecallState::ProviderNeeded(_)
        )
    }

    pub(crate) fn host_local_active_recall_plan(
        &self,
    ) -> Option<&TurnPreflightMemoryActiveRecallPlan> {
        match &self.memory.active_recall {
            TurnPreflightLocalActiveRecallState::HostLocalFinal(plan) => Some(plan),
            TurnPreflightLocalActiveRecallState::ProviderNeeded(_) => None,
        }
    }
}

impl TurnPreflightLocalMemoryState {
    fn provider_input(&self) -> TurnPreflightMemoryInput {
        TurnPreflightMemoryInput {
            deterministic_summary: self.deterministic_summary.clone(),
            active_recall: match &self.active_recall {
                TurnPreflightLocalActiveRecallState::HostLocalFinal(_) => None,
                TurnPreflightLocalActiveRecallState::ProviderNeeded(input) => Some(input.clone()),
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct TurnPreflightProviderEndpoint {
    pub provider: Arc<dyn Provider>,
    pub provider_name: String,
    pub model: String,
}

#[derive(Clone)]
pub(crate) struct TurnPreflightProviderCallInput {
    pub local_modules: TurnPreflightLocalModulePlans,
    pub turn: TurnPreflightTurnInput,
    pub primary: TurnPreflightProviderEndpoint,
    pub thread: TurnPreflightProviderEndpoint,
    pub timeout_ms: u64,
    pub max_output_chars: usize,
}

pub(crate) fn turn_preflight_required_for_thread_mode(mode: ThreadMode) -> bool {
    matches!(mode, ThreadMode::Agent)
}

pub(crate) fn resolve_turn_preflight_provider_endpoints(
    provider_registry: &ProviderRegistry,
    workspace_id: &str,
    thread_provider: Arc<dyn Provider>,
    thread_provider_name: &str,
    thread_model: &str,
    preflight_provider_name: Option<&str>,
    preflight_model: Option<&str>,
) -> Result<(TurnPreflightProviderEndpoint, TurnPreflightProviderEndpoint), String> {
    let thread_endpoint = TurnPreflightProviderEndpoint {
        provider: thread_provider.clone(),
        provider_name: thread_provider_name.to_owned(),
        model: thread_model.to_owned(),
    };

    let preflight_provider_name = normalized_non_empty(preflight_provider_name);
    let preflight_model = normalized_non_empty(preflight_model);
    let Some((preflight_provider_name, preflight_model)) =
        preflight_provider_name.zip(preflight_model)
    else {
        return Ok((thread_endpoint.clone(), thread_endpoint));
    };

    let primary_provider = if preflight_provider_name == thread_provider_name {
        thread_provider
    } else {
        provider_registry
            .get_or_create_for_workspace(workspace_id, preflight_provider_name.as_str())
            .map_err(|error| {
                format!("failed to create preflight provider `{preflight_provider_name}`: {error}")
            })?
    };

    Ok((
        TurnPreflightProviderEndpoint {
            provider: primary_provider,
            provider_name: preflight_provider_name,
            model: preflight_model,
        },
        thread_endpoint,
    ))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TurnPreflightProviderCallResult {
    Success(TurnPreflightProviderSuccess),
    Failure(TurnPreflightProviderFailure),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TurnPreflightProviderSuccess {
    pub plan: ProviderTurnPreflightPlan,
    pub provider_call: TurnPreflightProviderCallMetadata,
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TurnPreflightProviderFailure {
    pub fallback_reason: TurnPreflightFallbackReason,
    pub attempts: Vec<TurnPreflightProviderAttemptFailure>,
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TurnPreflightProviderAttemptFailure {
    pub fallback_reason: TurnPreflightFallbackReason,
    pub diagnostic: TurnPreflightDiagnostic,
    pub provider_call: TurnPreflightProviderCallMetadata,
}

pub(crate) async fn call_turn_preflight_provider_with_retry(
    input: TurnPreflightProviderCallInput,
) -> TurnPreflightProviderCallResult {
    let prompt = match render_turn_preflight_prompt_from_local_modules(
        &input.local_modules,
        input.turn.clone(),
        input.max_output_chars,
    ) {
        Ok(prompt) => prompt,
        Err(error) => {
            return TurnPreflightProviderCallResult::Failure(local_preflight_provider_failure(
                TurnPreflightFallbackReason::ValidationError,
                "preflight.prompt.render_failed",
                format!("failed to render preflight prompt: {error}"),
            ));
        }
    };
    let input_chars = prompt.chars().count();
    let timeout_ms = input
        .timeout_ms
        .max(1)
        .min(TURN_PREFLIGHT_PROVIDER_DEFAULT_TIMEOUT_MS.saturating_mul(10));
    let max_output_chars = input.max_output_chars.max(1);

    let primary = call_turn_preflight_provider_once(
        &input.primary,
        prompt.as_str(),
        1,
        timeout_ms,
        max_output_chars,
        input_chars,
    )
    .await;
    match primary {
        Ok(success) => TurnPreflightProviderCallResult::Success(success),
        Err(primary_failure) => {
            if !turn_preflight_retry_endpoint_differs(&input.primary, &input.thread) {
                return TurnPreflightProviderCallResult::Failure(TurnPreflightProviderFailure {
                    fallback_reason: primary_failure.fallback_reason,
                    diagnostics: vec![primary_failure.diagnostic.clone()],
                    attempts: vec![primary_failure],
                });
            }

            let retry = call_turn_preflight_provider_once(
                &input.thread,
                prompt.as_str(),
                2,
                timeout_ms,
                max_output_chars,
                input_chars,
            )
            .await;

            match retry {
                Ok(mut success) => {
                    success.diagnostics.insert(
                        0,
                        diagnostic(
                            "preflight.provider.thread_model_retry_used",
                            Some("preflight retry used the current thread model"),
                        ),
                    );
                    success
                        .diagnostics
                        .insert(0, primary_failure.diagnostic.clone());
                    TurnPreflightProviderCallResult::Success(success)
                }
                Err(retry_failure) => {
                    let fallback_reason = retry_failure.fallback_reason;
                    let diagnostics = vec![
                        primary_failure.diagnostic.clone(),
                        diagnostic(
                            "preflight.provider.thread_model_retry_failed",
                            Some("preflight retry through the current thread model failed"),
                        ),
                        retry_failure.diagnostic.clone(),
                    ];
                    TurnPreflightProviderCallResult::Failure(TurnPreflightProviderFailure {
                        fallback_reason,
                        attempts: vec![primary_failure, retry_failure],
                        diagnostics,
                    })
                }
            }
        }
    }
}

pub(crate) fn render_turn_preflight_prompt_from_local_modules(
    local_modules: &TurnPreflightLocalModulePlans,
    turn: TurnPreflightTurnInput,
    max_output_chars: usize,
) -> Result<String, serde_json::Error> {
    let provider_input = local_modules.provider_input(turn);
    let structured_input_json = serde_json::to_string(&provider_input)?;
    Ok(render_turn_preflight_prompt(&TurnPreflightPromptInput {
        structured_input_json,
        memory_active_recall: TurnPreflightMemoryActiveRecallPromptInput {
            provider_planning_needed: local_modules.active_recall_provider_planning_needed(),
        },
        max_output_chars,
    }))
}

async fn call_turn_preflight_provider_once(
    endpoint: &TurnPreflightProviderEndpoint,
    prompt: &str,
    attempt: u32,
    timeout_ms: u64,
    max_output_chars: usize,
    input_chars: usize,
) -> Result<TurnPreflightProviderSuccess, TurnPreflightProviderAttemptFailure> {
    let started = Instant::now();
    let request = turn_preflight_chat_request(endpoint.model.as_str(), prompt.to_owned());
    let response = tokio::time::timeout(
        Duration::from_millis(timeout_ms.max(1)),
        request_turn_preflight_provider_json(endpoint.provider.as_ref(), request),
    )
    .await;

    let elapsed_ms = elapsed_ms(started);
    let raw = match response {
        Err(_) => {
            return Err(turn_preflight_attempt_failure(
                TurnPreflightFallbackReason::Timeout,
                "preflight.provider.timeout",
                "preflight provider request timed out".to_owned(),
                endpoint,
                attempt,
                input_chars,
                0,
                elapsed_ms,
            ));
        }
        Ok(Err(error)) => {
            return Err(turn_preflight_attempt_failure(
                TurnPreflightFallbackReason::ProviderError,
                "preflight.provider.error",
                format!("preflight provider request failed: {error:#}"),
                endpoint,
                attempt,
                input_chars,
                0,
                elapsed_ms,
            ));
        }
        Ok(Ok(raw)) => raw,
    };

    let output_chars = raw.chars().count();
    if output_chars > max_output_chars {
        return Err(turn_preflight_attempt_failure(
            TurnPreflightFallbackReason::ValidationError,
            "preflight.provider.output_too_large",
            format!("preflight provider response exceeded max_output_chars={max_output_chars}"),
            endpoint,
            attempt,
            input_chars,
            output_chars,
            elapsed_ms,
        ));
    }

    let plan = parse_provider_turn_preflight_plan_json_classified(raw.as_str()).map_err(
        |(fallback_reason, error)| {
            let (code, message) = match fallback_reason {
                TurnPreflightFallbackReason::InvalidJson => (
                    "preflight.provider.invalid_json",
                    format!("preflight provider returned invalid JSON: {error}"),
                ),
                TurnPreflightFallbackReason::ValidationError => (
                    "preflight.provider.validation_error",
                    format!("preflight provider returned invalid preflight plan: {error}"),
                ),
                TurnPreflightFallbackReason::Timeout
                | TurnPreflightFallbackReason::ProviderError => {
                    unreachable!(
                        "parse classification only returns invalid_json or validation_error"
                    )
                }
            };
            turn_preflight_attempt_failure(
                fallback_reason,
                code,
                message,
                endpoint,
                attempt,
                input_chars,
                output_chars,
                elapsed_ms,
            )
        },
    )?;

    Ok(TurnPreflightProviderSuccess {
        plan,
        provider_call: TurnPreflightProviderCallMetadata {
            provider: endpoint.provider_name.clone(),
            model: endpoint.model.clone(),
            attempt,
            input_chars,
            output_chars,
            elapsed_ms,
        },
        diagnostics: Vec::new(),
    })
}

fn turn_preflight_chat_request(model: &str, prompt: String) -> ChatRequest {
    ChatRequest {
        model: model.to_owned(),
        messages: vec![ChatMessage::user(prompt)],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: Some(ReasoningConfig::disabled()),
        compiled_prompt: None,
    }
}

async fn request_turn_preflight_provider_json(
    provider: &dyn Provider,
    request: ChatRequest,
) -> anyhow::Result<String> {
    if provider.capabilities().streaming {
        let mut stream = provider.stream_chat(request).await?;
        let mut text = String::new();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            let chunk = chunk?;
            text.push_str(chunk.delta.as_str());
            if chunk.is_final {
                break;
            }
        }
        return Ok(text);
    }

    provider.chat(request).await.map(|response| response.text)
}

fn parse_provider_turn_preflight_plan_json_classified(
    raw: &str,
) -> Result<ProviderTurnPreflightPlan, (TurnPreflightFallbackReason, serde_json::Error)> {
    let value = serde_json::from_str::<serde_json::Value>(raw.trim())
        .map_err(|error| (TurnPreflightFallbackReason::InvalidJson, error))?;
    let plan = serde_json::from_value::<ProviderTurnPreflightPlan>(value)
        .map_err(|error| (TurnPreflightFallbackReason::ValidationError, error))?;
    validate_provider_turn_preflight_plan(&plan)
        .map_err(|error| (TurnPreflightFallbackReason::ValidationError, error))?;
    Ok(normalize_provider_turn_preflight_plan(plan))
}

fn turn_preflight_retry_endpoint_differs(
    primary: &TurnPreflightProviderEndpoint,
    thread: &TurnPreflightProviderEndpoint,
) -> bool {
    primary.provider_name != thread.provider_name || primary.model != thread.model
}

fn normalized_non_empty(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_owned())
}

fn local_preflight_provider_failure(
    fallback_reason: TurnPreflightFallbackReason,
    code: &'static str,
    message: String,
) -> TurnPreflightProviderFailure {
    let diagnostic = diagnostic(code, Some(message.as_str()));
    TurnPreflightProviderFailure {
        fallback_reason,
        attempts: Vec::new(),
        diagnostics: vec![diagnostic],
    }
}

#[allow(clippy::too_many_arguments)]
fn turn_preflight_attempt_failure(
    fallback_reason: TurnPreflightFallbackReason,
    code: &'static str,
    message: String,
    endpoint: &TurnPreflightProviderEndpoint,
    attempt: u32,
    input_chars: usize,
    output_chars: usize,
    elapsed_ms: u64,
) -> TurnPreflightProviderAttemptFailure {
    TurnPreflightProviderAttemptFailure {
        fallback_reason,
        diagnostic: diagnostic(code, Some(message.as_str())),
        provider_call: TurnPreflightProviderCallMetadata {
            provider: endpoint.provider_name.clone(),
            model: endpoint.model.clone(),
            attempt,
            input_chars,
            output_chars,
            elapsed_ms,
        },
    }
}

fn diagnostic(code: &'static str, message: Option<&str>) -> TurnPreflightDiagnostic {
    TurnPreflightDiagnostic {
        code: TurnPreflightDiagnosticCode::new(code)
            .expect("static preflight diagnostic code must be valid"),
        message: message.map(|message| {
            TurnPreflightDiagnosticMessage::new(bounded_diagnostic_message(message))
                .expect("static preflight diagnostic message must be valid")
        }),
    }
}

fn bounded_diagnostic_message(message: &str) -> String {
    if message.chars().count() <= PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS {
        return message.to_owned();
    }

    let mut truncated = message
        .chars()
        .take(PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightPlan {
    pub tools: ProviderTurnPreflightToolsPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<ProviderTurnPreflightMemoryPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightToolsPlan {
    pub visible_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightMemoryPlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_recall: Option<ActiveRecallPlanJson>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightPlan {
    pub source: TurnPreflightPlanSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<TurnPreflightFallbackReason>,
    pub tools: TurnPreflightToolsPlan,
    pub memory: TurnPreflightMemoryPlan,
    pub diagnostics: TurnPreflightDiagnostics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_call: Option<TurnPreflightProviderCallMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightToolsPlan {
    pub visible_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryPlan {
    pub active_recall: TurnPreflightMemoryActiveRecallPlan,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryActiveRecallPlan {
    pub source: TurnPreflightPlanSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<TurnPreflightFallbackReason>,
    pub decision: ActiveRecallPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnPreflightPlanSource {
    Provider,
    HostLocal,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnPreflightFallbackReason {
    Timeout,
    ProviderError,
    InvalidJson,
    ValidationError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightDiagnostics {
    #[serde(default)]
    pub preflight_failed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightProviderCallMetadata {
    pub provider: String,
    pub model: String,
    pub attempt: u32,
    pub input_chars: usize,
    pub output_chars: usize,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightDiagnostic {
    pub code: TurnPreflightDiagnosticCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<TurnPreflightDiagnosticMessage>,
}

pub(crate) fn normalize_provider_turn_preflight_plan(
    mut plan: ProviderTurnPreflightPlan,
) -> ProviderTurnPreflightPlan {
    plan.tools.visible_tools = normalize_visible_tool_names(plan.tools.visible_tools);
    plan.diagnostics = normalize_preflight_diagnostics(plan.diagnostics);
    plan
}

pub(crate) fn parse_provider_turn_preflight_plan_json(
    raw: &str,
) -> Result<ProviderTurnPreflightPlan, serde_json::Error> {
    let plan = serde_json::from_str::<ProviderTurnPreflightPlan>(raw.trim())?;
    validate_provider_turn_preflight_plan(&plan)?;
    Ok(normalize_provider_turn_preflight_plan(plan))
}

pub(crate) fn validate_provider_turn_preflight_plan(
    plan: &ProviderTurnPreflightPlan,
) -> Result<(), serde_json::Error> {
    if let Some(active_recall) = plan
        .memory
        .as_ref()
        .and_then(|memory| memory.active_recall.as_ref())
    {
        parse_provider_memory_active_recall_plan(active_recall)?;
    }
    Ok(())
}

pub(crate) fn normalize_visible_tool_names(tool_names: Vec<String>) -> Vec<String> {
    let mut normalized = tool_names
        .into_iter()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

pub(crate) fn normalize_preflight_diagnostics(
    diagnostics: Vec<TurnPreflightDiagnostic>,
) -> Vec<TurnPreflightDiagnostic> {
    diagnostics
        .into_iter()
        .take(PREFLIGHT_DIAGNOSTIC_MAX_COUNT)
        .collect()
}

pub(crate) fn normalize_module_diagnostics(
    module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
) -> BTreeMap<String, Vec<TurnPreflightDiagnostic>> {
    module_diagnostics
        .into_iter()
        .map(|(module, diagnostics)| (module, normalize_preflight_diagnostics(diagnostics)))
        .collect()
}

pub(crate) fn parse_provider_memory_active_recall_plan(
    plan: &ActiveRecallPlanJson,
) -> Result<ActiveRecallPlan, serde_json::Error> {
    let raw = serde_json::to_string(plan)?;
    parse_active_memory_decision_json(raw.as_str())
}

pub(crate) fn wrap_memory_active_recall_plan(
    source: TurnPreflightPlanSource,
    fallback_reason: Option<TurnPreflightFallbackReason>,
    decision: ActiveRecallPlan,
) -> TurnPreflightMemoryActiveRecallPlan {
    TurnPreflightMemoryActiveRecallPlan {
        source,
        fallback_reason,
        decision: normalize_active_recall_plan(decision),
    }
}

pub(crate) fn fallback_turn_preflight_plan(
    fallback_reason: TurnPreflightFallbackReason,
    active_recall: TurnPreflightMemoryActiveRecallPlan,
    diagnostics: Vec<TurnPreflightDiagnostic>,
    module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
    provider_call: Option<TurnPreflightProviderCallMetadata>,
) -> TurnPreflightPlan {
    TurnPreflightPlan {
        source: TurnPreflightPlanSource::Fallback,
        fallback_reason: Some(fallback_reason),
        tools: TurnPreflightToolsPlan {
            visible_tools: Vec::new(),
        },
        memory: TurnPreflightMemoryPlan { active_recall },
        diagnostics: TurnPreflightDiagnostics {
            preflight_failed: true,
            diagnostics: normalize_preflight_diagnostics(diagnostics),
            module_diagnostics: normalize_module_diagnostics(module_diagnostics),
        },
        provider_call,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightDiagnosticCode(String);

impl TurnPreflightDiagnosticCode {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, TurnPreflightTextError> {
        let value = value.into();
        validate_structured_code(
            "TurnPreflightDiagnosticCode",
            value.as_str(),
            PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS,
        )?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for TurnPreflightDiagnosticCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TurnPreflightDiagnosticCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TurnPreflightDiagnosticCodeVisitor {
            type_name: "TurnPreflightDiagnosticCode",
            max_chars: PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightDiagnosticMessage(String);

impl TurnPreflightDiagnosticMessage {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, TurnPreflightTextError> {
        let value = value.into();
        validate_bounded_text(
            "TurnPreflightDiagnosticMessage",
            value.as_str(),
            PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS,
        )?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for TurnPreflightDiagnosticMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TurnPreflightDiagnosticMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TurnPreflightDiagnosticMessageVisitor {
            type_name: "TurnPreflightDiagnosticMessage",
            max_chars: PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightTextError {
    type_name: &'static str,
    reason: &'static str,
}

impl TurnPreflightTextError {
    fn new(type_name: &'static str, reason: &'static str) -> Self {
        Self { type_name, reason }
    }
}

impl fmt::Display for TurnPreflightTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.type_name, self.reason)
    }
}

impl std::error::Error for TurnPreflightTextError {}

struct TurnPreflightDiagnosticCodeVisitor {
    type_name: &'static str,
    max_chars: usize,
}

impl<'de> Visitor<'de> for TurnPreflightDiagnosticCodeVisitor {
    type Value = TurnPreflightDiagnosticCode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a non-empty structured {} string up to {} chars",
            self.type_name, self.max_chars
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        validate_structured_code(self.type_name, value, self.max_chars).map_err(E::custom)?;
        Ok(TurnPreflightDiagnosticCode(value.to_owned()))
    }
}

struct TurnPreflightDiagnosticMessageVisitor {
    type_name: &'static str,
    max_chars: usize,
}

impl<'de> Visitor<'de> for TurnPreflightDiagnosticMessageVisitor {
    type Value = TurnPreflightDiagnosticMessage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a non-empty {} string up to {} chars",
            self.type_name, self.max_chars
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        validate_bounded_text(self.type_name, value, self.max_chars).map_err(E::custom)?;
        Ok(TurnPreflightDiagnosticMessage(value.to_owned()))
    }
}

fn validate_structured_code(
    type_name: &'static str,
    value: &str,
    max_chars: usize,
) -> Result<(), TurnPreflightTextError> {
    validate_bounded_text(type_name, value, max_chars)?;
    if value.chars().any(char::is_whitespace) {
        return Err(TurnPreflightTextError::new(
            type_name,
            "cannot contain whitespace",
        ));
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(TurnPreflightTextError::new(
            type_name,
            "must contain only lowercase ascii letters, digits, dots, underscores or hyphens",
        ));
    }
    Ok(())
}

fn validate_bounded_text(
    type_name: &'static str,
    value: &str,
    max_chars: usize,
) -> Result<(), TurnPreflightTextError> {
    if value.trim().is_empty() {
        return Err(TurnPreflightTextError::new(type_name, "cannot be empty"));
    }
    if value.chars().count() > max_chars {
        return Err(TurnPreflightTextError::new(type_name, "is too long"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_memory::hooks::{
        ActiveMemoryDecisionReasonCode, ActiveMemoryDecisionReasonCodeJson,
        ActiveMemoryDecisionStatus, ActiveRecallMode, ActiveRecallPlanJsonStatus,
        ActiveRecallTarget, MemoryActiveRecallMode, MemoryActiveRecallPlannerFallbackPolicy,
        MemoryEpisodicRecallCapabilities, parse_active_memory_decision_json,
    };
    use pioneer_protocol::{
        MemoryAttribute, MemoryCategory, MemoryFactClass, MemoryScopeKind, MemorySubject,
    };
    use pioneer_provider::{ChatResponse, ProviderCapabilities, Role, StreamChunk};
    use serde_json::{Value as JsonValue, json};
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    enum FakePreflightResponse {
        Text(String),
        Error(String),
        DelayedText { delay_ms: u64, text: String },
    }

    struct FakePreflightProvider {
        name: String,
        streaming: bool,
        responses: Mutex<VecDeque<FakePreflightResponse>>,
        requests: Mutex<Vec<ChatRequest>>,
    }

    impl FakePreflightProvider {
        fn new(
            name: impl Into<String>,
            streaming: bool,
            responses: impl IntoIterator<Item = FakePreflightResponse>,
        ) -> Self {
            Self {
                name: name.into(),
                streaming,
                responses: Mutex::new(responses.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn text(name: impl Into<String>, text: impl Into<String>) -> Arc<Self> {
            Arc::new(Self::new(
                name,
                false,
                [FakePreflightResponse::Text(text.into())],
            ))
        }

        fn streaming_text(name: impl Into<String>, text: impl Into<String>) -> Arc<Self> {
            Arc::new(Self::new(
                name,
                true,
                [FakePreflightResponse::Text(text.into())],
            ))
        }

        fn failing(name: impl Into<String>, error: impl Into<String>) -> Arc<Self> {
            Arc::new(Self::new(
                name,
                false,
                [FakePreflightResponse::Error(error.into())],
            ))
        }

        fn delayed(name: impl Into<String>, delay_ms: u64, text: impl Into<String>) -> Arc<Self> {
            Arc::new(Self::new(
                name,
                false,
                [FakePreflightResponse::DelayedText {
                    delay_ms,
                    text: text.into(),
                }],
            ))
        }

        fn requests(&self) -> Vec<ChatRequest> {
            self.requests
                .lock()
                .expect("test request lock poisoned")
                .clone()
        }

        async fn next_response(&self) -> anyhow::Result<String> {
            let response = self
                .responses
                .lock()
                .expect("test response lock poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    FakePreflightResponse::Text(r#"{"tools":{"visibleTools":[]}}"#.to_owned())
                });

            match response {
                FakePreflightResponse::Text(text) => Ok(text),
                FakePreflightResponse::Error(error) => anyhow::bail!("{error}"),
                FakePreflightResponse::DelayedText { delay_ms, text } => {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    Ok(text)
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl Provider for FakePreflightProvider {
        fn name(&self) -> &str {
            self.name.as_str()
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                streaming: self.streaming,
                ..ProviderCapabilities::default()
            }
        }

        async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
            self.requests
                .lock()
                .expect("test request lock poisoned")
                .push(request);
            Ok(ChatResponse {
                text: self.next_response().await?,
                usage: None,
                reasoning_content: None,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            self.requests
                .lock()
                .expect("test request lock poisoned")
                .push(request);
            let text = self.next_response().await?;
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(StreamChunk::delta(text)),
                Ok(StreamChunk::final_chunk()),
            ])))
        }
    }

    fn diagnostic_code(value: impl Into<String>) -> TurnPreflightDiagnosticCode {
        TurnPreflightDiagnosticCode::new(value).expect("test diagnostic code must be valid")
    }

    fn diagnostic_message(value: &str) -> TurnPreflightDiagnosticMessage {
        TurnPreflightDiagnosticMessage::new(value).expect("test diagnostic message must be valid")
    }

    fn sample_input() -> TurnPreflightInput {
        TurnPreflightInput {
            turn: TurnPreflightTurnInput {
                has_workspace_id: true,
                has_thread_id: true,
                has_turn_id: true,
                thread_mode: ThreadMode::Agent,
                provider_tool_calling: true,
                input_text_preview: "как меня зовут?".to_owned(),
                input_text_char_count: 15,
            },
            tools: TurnPreflightToolsInput {
                core_tools: vec![
                    "exec_command".to_owned(),
                    "write_stdin".to_owned(),
                    "read_file".to_owned(),
                    "list_dir".to_owned(),
                    "grep_files".to_owned(),
                    "apply_patch".to_owned(),
                    "web_search".to_owned(),
                    "web_fetch".to_owned(),
                    "download_url".to_owned(),
                    "read_skill".to_owned(),
                    "request_tools".to_owned(),
                ],
                candidate_tools: vec![
                    TurnPreflightCandidateTool {
                        name: "memory_search".to_owned(),
                        domain: BuiltinToolDomain::Memory,
                        summary: "Search durable memory for relevant facts.".to_owned(),
                        mutation: false,
                    },
                    TurnPreflightCandidateTool {
                        name: "memory_get".to_owned(),
                        domain: BuiltinToolDomain::Memory,
                        summary: "Read one durable memory record by id.".to_owned(),
                        mutation: false,
                    },
                ],
            },
            memory: TurnPreflightMemoryInput {
                deterministic_summary: DeterministicRecallContextSummary {
                    memory_ids: BTreeSet::new(),
                    rendered_line_fingerprints: BTreeSet::new(),
                    context_count: 0,
                    context_chars: 0,
                    sufficient: false,
                },
                active_recall: Some(TurnPreflightMemoryActiveRecallInput {
                    decision_context: MemoryActiveRecallDecisionContext {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: "thr_1".to_owned(),
                        turn_id: "turn_1".to_owned(),
                        mode: ThreadMode::Agent,
                        input_text_preview: "как меня зовут?".to_owned(),
                        model: Some("thread-model".to_owned()),
                        model_provider: Some("thread-provider".to_owned()),
                    },
                    decision_request: MemoryActiveRecallDecisionRequest {
                        deterministic_context_count: 0,
                        deterministic_context_chars: 0,
                        deterministic_memory_ids: Vec::new(),
                        deterministic_sufficient: false,
                        deterministic_recall_empty: true,
                        has_workspace_context: true,
                        has_task_context: false,
                        input_length_bucket: "very_short".to_owned(),
                        config_mode: MemoryActiveRecallMode::Hybrid,
                        read_allowed: true,
                        active_memory_allowed: true,
                        explicit_no_memory: false,
                        input_text_char_count: 15,
                        available_modes: vec![
                            "profile".to_owned(),
                            "project".to_owned(),
                            "exact_canonical".to_owned(),
                        ],
                        available_scoped_contexts: vec!["thread".to_owned()],
                        episodic_capabilities: MemoryEpisodicRecallCapabilities {
                            current_thread_search: true,
                            related_thread_search: false,
                            current_task_context: false,
                            completed_task_summary: false,
                        },
                        max_queries: 3,
                        top_k_per_query: 5,
                        max_prompt_chars: 1_500,
                        max_input_chars: 4_000,
                        max_output_chars: 2_000,
                        fallback_policy: MemoryActiveRecallPlannerFallbackPolicy::Deterministic,
                    },
                }),
            },
        }
    }

    fn sample_provider_plan_json() -> JsonValue {
        json!({
            "tools": {
                "visibleTools": ["memory_search", "memory_get"]
            },
            "memory": {
                "activeRecall": {
                    "status": "run",
                    "reasonCode": "memory_likely",
                    "confidence": 0.92,
                    "modes": ["profile", "exact_canonical"],
                    "targets": [
                        {
                            "scopeKind": "user",
                            "factClass": "user_identity",
                            "category": "identity",
                            "subject": "current_user",
                            "attribute": "name",
                            "canonicalKey": "identity.current_user.name"
                        }
                    ],
                    "diagnostics": [
                        "memory.active_recall.identity_lookup"
                    ]
                }
            },
            "diagnostics": [
                {
                    "code": "preflight.tools.memory_selected",
                    "message": "Identity question needs memory read tools."
                }
            ]
        })
    }

    #[test]
    fn preflight_input_serializes_stable_json_without_tool_schemas() {
        let value = serde_json::to_value(sample_input()).expect("input serializes");

        assert_eq!(
            value["turn"]["inputTextPreview"],
            JsonValue::String("как меня зовут?".to_owned())
        );
        assert_eq!(value["turn"]["hasWorkspaceId"], json!(true));
        assert_eq!(value["turn"]["hasThreadId"], json!(true));
        assert_eq!(value["turn"]["hasTurnId"], json!(true));
        assert_eq!(value["turn"]["inputTextCharCount"], json!(15));
        assert_eq!(
            value["tools"]["coreTools"],
            json!([
                "exec_command",
                "write_stdin",
                "read_file",
                "list_dir",
                "grep_files",
                "apply_patch",
                "web_search",
                "web_fetch",
                "download_url",
                "read_skill",
                "request_tools"
            ])
        );
        assert_eq!(
            value["tools"]["candidateTools"],
            json!([
                {
                    "name": "memory_search",
                    "domain": "memory",
                    "summary": "Search durable memory for relevant facts.",
                    "mutation": false
                },
                {
                    "name": "memory_get",
                    "domain": "memory",
                    "summary": "Read one durable memory record by id.",
                    "mutation": false
                }
            ])
        );
        assert!(value.get("coreTools").is_none());
        assert!(value.get("candidateTools").is_none());
        assert_eq!(
            value["memory"]["deterministicSummary"],
            json!({
                "contextCount": 0,
                "contextChars": 0,
                "sufficient": false
            })
        );
        let mut deterministic_with_internal_fields = sample_input();
        deterministic_with_internal_fields
            .memory
            .deterministic_summary
            .rendered_line_fingerprints
            .insert("workspace project policy: private rendered line".to_owned());
        let deterministic_value =
            serde_json::to_value(deterministic_with_internal_fields).expect("input serializes");
        assert!(
            deterministic_value["memory"]["deterministicSummary"]
                .get("renderedLineFingerprints")
                .is_none()
        );
        let serialized = serde_json::to_string(&value).expect("value serializes");
        assert!(!serialized.contains("\"parameters\""));
        assert!(!serialized.contains("\"properties\""));
        assert!(!serialized.contains("\"jsonSchema\""));
        assert!(!serialized.contains("\"policy\""));
        assert!(!serialized.contains("recallAllowed"));
        assert!(!serialized.contains("readToolsAllowed"));
        assert!(!serialized.contains("rememberToolAllowed"));
        assert!(!serialized.contains("forgetToolAllowed"));
        assert!(!serialized.contains("activeRecallAllowed"));
        assert!(!serialized.contains("\"source\""));
        assert!(!serialized.contains("\"alwaysVisible\""));
        assert!(!serialized.contains("\"currentlyVisible\""));
    }

    #[test]
    fn preflight_input_reuses_active_recall_decision_request_contract() {
        let value = serde_json::to_value(sample_input()).expect("input serializes");

        assert_eq!(
            value["memory"]["activeRecall"]["decisionContext"]["inputTextPreview"],
            JsonValue::String("как меня зовут?".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decisionRequest"]["configMode"],
            JsonValue::String("hybrid".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decisionRequest"]["fallbackPolicy"],
            JsonValue::String("deterministic".to_owned())
        );

        let serialized = serde_json::to_string(&value).expect("value serializes");
        assert!(!serialized.contains("plannerEnabled"));
        assert!(!serialized.contains("plannerNeeded"));
    }

    #[test]
    fn preflight_input_candidate_tools_reject_core_dynamic_and_source_fields() {
        for domain in ["core", "dynamic"] {
            let mut value = serde_json::to_value(sample_input()).expect("input serializes");
            value["tools"]["candidateTools"][0]["domain"] = json!(domain);

            serde_json::from_value::<TurnPreflightInput>(value)
                .expect_err("candidate tool domains must be limited to lazy builtin domains");
        }

        let mut with_source = serde_json::to_value(sample_input()).expect("input serializes");
        with_source["tools"]["candidateTools"][0]["source"] = json!("builtin");

        serde_json::from_value::<TurnPreflightInput>(with_source)
            .expect_err("candidate tools must not carry provenance/source fields");
    }

    #[test]
    fn preflight_tools_input_adapts_compact_tool_index_without_schemas() {
        let input = turn_preflight_tools_input_from_index(pioneer_tools::PreflightToolIndex {
            core_tools: vec!["exec_command".to_owned(), "request_tools".to_owned()],
            candidate_tools: vec![pioneer_tools::PreflightCandidateToolDescriptor {
                name: "memory_search".to_owned(),
                domain: BuiltinToolDomain::Memory,
                summary: "Search durable memory.".to_owned(),
                mutation: false,
            }],
        });

        assert_eq!(
            input.core_tools,
            vec!["exec_command".to_owned(), "request_tools".to_owned()]
        );
        assert_eq!(input.candidate_tools.len(), 1);
        assert_eq!(input.candidate_tools[0].name, "memory_search");
        assert_eq!(input.candidate_tools[0].domain, BuiltinToolDomain::Memory);

        let serialized = serde_json::to_string(&input).expect("tools input serializes");
        assert!(!serialized.contains("parameters"));
        assert!(!serialized.contains("properties"));
        assert!(!serialized.contains("jsonSchema"));
    }

    fn sample_turn_input() -> TurnPreflightTurnInput {
        sample_input().turn
    }

    fn sample_active_recall_input() -> TurnPreflightMemoryActiveRecallInput {
        sample_input()
            .memory
            .active_recall
            .expect("sample input has active recall provider input")
    }

    fn sample_deterministic_summary() -> DeterministicRecallContextSummary {
        sample_input().memory.deterministic_summary
    }

    fn sample_tool_index() -> PreflightToolIndex {
        PreflightToolIndex {
            core_tools: vec!["exec_command".to_owned(), "request_tools".to_owned()],
            candidate_tools: vec![
                pioneer_tools::PreflightCandidateToolDescriptor {
                    name: "memory_search".to_owned(),
                    domain: BuiltinToolDomain::Memory,
                    summary: "Search durable memory.".to_owned(),
                    mutation: false,
                },
                pioneer_tools::PreflightCandidateToolDescriptor {
                    name: "task_create".to_owned(),
                    domain: BuiltinToolDomain::Task,
                    summary: "Create a subtask.".to_owned(),
                    mutation: true,
                },
            ],
        }
    }

    fn sample_memory_local_plan(
        provider_planning_needed: bool,
        local_decision: ActiveRecallPlan,
    ) -> MemoryActiveRecallLocalPlan {
        let active_recall = sample_active_recall_input();
        MemoryActiveRecallLocalPlan {
            decision_context: active_recall.decision_context,
            decision_request: active_recall.decision_request,
            local_decision,
            provider_planning_needed,
        }
    }

    fn sample_skip_decision(reason_code: ActiveMemoryDecisionReasonCode) -> ActiveRecallPlan {
        ActiveRecallPlan {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code,
            confidence: 1.0,
            modes: Vec::new(),
            targets: Vec::new(),
            debug_fallback: false,
            provider_used: false,
            provider_fallback_used: false,
            provider_input_chars: None,
            provider_output_chars: None,
            diagnostics: vec!["memory.active_recall.host_local".to_owned()],
        }
    }

    fn sample_low_confidence_run_decision() -> ActiveRecallPlan {
        ActiveRecallPlan {
            status: ActiveMemoryDecisionStatus::Run,
            reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
            confidence: 0.65,
            modes: vec![ActiveRecallMode::Profile],
            targets: Vec::new(),
            debug_fallback: false,
            provider_used: false,
            provider_fallback_used: false,
            provider_input_chars: None,
            provider_output_chars: None,
            diagnostics: vec!["memory.active_recall.local_candidate".to_owned()],
        }
    }

    fn sample_provider_needed_modules() -> TurnPreflightLocalModulePlans {
        build_local_preflight_module_plans(
            sample_tool_index(),
            sample_deterministic_summary(),
            sample_memory_local_plan(true, sample_low_confidence_run_decision()),
        )
    }

    fn provider_endpoint(
        provider: Arc<dyn Provider>,
        provider_name: &str,
        model: &str,
    ) -> TurnPreflightProviderEndpoint {
        TurnPreflightProviderEndpoint {
            provider,
            provider_name: provider_name.to_owned(),
            model: model.to_owned(),
        }
    }

    fn provider_call_input(
        primary: TurnPreflightProviderEndpoint,
        thread: TurnPreflightProviderEndpoint,
    ) -> TurnPreflightProviderCallInput {
        TurnPreflightProviderCallInput {
            local_modules: sample_provider_needed_modules(),
            turn: sample_turn_input(),
            primary,
            thread,
            timeout_ms: TURN_PREFLIGHT_PROVIDER_DEFAULT_TIMEOUT_MS,
            max_output_chars: TURN_PREFLIGHT_PROVIDER_DEFAULT_MAX_OUTPUT_CHARS,
        }
    }

    #[test]
    fn preflight_provider_entry_condition_depends_only_on_thread_mode() {
        assert!(turn_preflight_required_for_thread_mode(ThreadMode::Agent));
        assert!(!turn_preflight_required_for_thread_mode(ThreadMode::Chat));
    }

    #[test]
    fn preflight_provider_resolves_general_model_selection_or_thread_default() {
        let thread_provider =
            FakePreflightProvider::text("thread-provider", r#"{"tools":{"visibleTools":[]}}"#);
        let configured_provider =
            FakePreflightProvider::text("configured-provider", r#"{"tools":{"visibleTools":[]}}"#);
        let registry = ProviderRegistry::with_provider("configured-provider", configured_provider);

        let (primary, thread) = resolve_turn_preflight_provider_endpoints(
            &registry,
            "workspace_1",
            thread_provider.clone(),
            "thread-provider",
            "thread-model",
            None,
            None,
        )
        .expect("thread default resolves");

        assert_eq!(primary.provider_name, "thread-provider");
        assert_eq!(primary.model, "thread-model");
        assert_eq!(thread.provider_name, "thread-provider");
        assert_eq!(thread.model, "thread-model");
        assert!(Arc::ptr_eq(&primary.provider, &thread.provider));

        let (primary, thread) = resolve_turn_preflight_provider_endpoints(
            &registry,
            "workspace_1",
            thread_provider.clone(),
            "thread-provider",
            "thread-model",
            Some(" configured-provider "),
            Some(" configured-model "),
        )
        .expect("configured preflight model resolves");

        assert_eq!(primary.provider_name, "configured-provider");
        assert_eq!(primary.model, "configured-model");
        assert_eq!(thread.provider_name, "thread-provider");
        assert_eq!(thread.model, "thread-model");
        assert!(!Arc::ptr_eq(&primary.provider, &thread.provider));

        let (primary, thread) = resolve_turn_preflight_provider_endpoints(
            &registry,
            "workspace_1",
            thread_provider,
            "thread-provider",
            "thread-model",
            Some("configured-provider"),
            None,
        )
        .expect("incomplete configured selection falls back to thread");

        assert_eq!(primary.provider_name, "thread-provider");
        assert_eq!(primary.model, "thread-model");
        assert!(Arc::ptr_eq(&primary.provider, &thread.provider));
    }

    #[test]
    fn preflight_local_host_final_active_recall_is_omitted_from_provider_input() {
        let modules = build_local_preflight_module_plans(
            sample_tool_index(),
            sample_deterministic_summary(),
            sample_memory_local_plan(
                false,
                sample_skip_decision(ActiveMemoryDecisionReasonCode::DeterministicSufficient),
            ),
        );

        let provider_input = modules.provider_input(sample_turn_input());

        assert!(!modules.active_recall_provider_planning_needed());
        assert!(provider_input.memory.active_recall.is_none());
        let host_local = modules
            .host_local_active_recall_plan()
            .expect("host-local final active recall is retained for final composition");
        assert_eq!(host_local.source, TurnPreflightPlanSource::HostLocal);
        assert_eq!(host_local.fallback_reason, None);
        assert_eq!(
            host_local.decision.reason_code,
            ActiveMemoryDecisionReasonCode::DeterministicSufficient
        );
    }

    #[test]
    fn preflight_local_provider_needed_active_recall_enters_provider_input() {
        let modules = build_local_preflight_module_plans(
            sample_tool_index(),
            sample_deterministic_summary(),
            sample_memory_local_plan(true, sample_low_confidence_run_decision()),
        );

        let provider_input = modules.provider_input(sample_turn_input());
        let active_recall = provider_input
            .memory
            .active_recall
            .expect("provider-needed active recall enters provider input");

        assert!(modules.active_recall_provider_planning_needed());
        assert!(modules.host_local_active_recall_plan().is_none());
        assert_eq!(active_recall.decision_context.thread_id, "thr_1");
        assert_eq!(
            active_recall.decision_request.config_mode,
            MemoryActiveRecallMode::Hybrid
        );
        assert_eq!(
            active_recall.decision_request.available_modes,
            vec![
                "profile".to_owned(),
                "project".to_owned(),
                "exact_canonical".to_owned()
            ]
        );
    }

    #[test]
    fn preflight_local_tools_state_uses_compact_tool_index() {
        let modules = build_local_preflight_module_plans(
            sample_tool_index(),
            sample_deterministic_summary(),
            sample_memory_local_plan(
                false,
                sample_skip_decision(ActiveMemoryDecisionReasonCode::DeterministicSufficient),
            ),
        );

        let provider_input = modules.provider_input(sample_turn_input());

        assert_eq!(
            provider_input.tools.core_tools,
            vec!["exec_command".to_owned(), "request_tools".to_owned()]
        );
        assert_eq!(provider_input.tools.candidate_tools.len(), 2);
        assert_eq!(
            provider_input.tools.candidate_tools[0].name,
            "memory_search"
        );
        assert_eq!(
            provider_input.tools.candidate_tools[0].domain,
            BuiltinToolDomain::Memory
        );
        assert_eq!(provider_input.tools.candidate_tools[1].name, "task_create");
        assert_eq!(
            provider_input.tools.candidate_tools[1].domain,
            BuiltinToolDomain::Task
        );

        let serialized = serde_json::to_string(&provider_input).expect("input serializes");
        assert!(!serialized.contains("parameters"));
        assert!(!serialized.contains("properties"));
        assert!(!serialized.contains("jsonSchema"));
    }

    #[test]
    fn provider_preflight_plan_parses_concrete_visible_tools_and_memory_active_recall() {
        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(sample_provider_plan_json()).expect("provider plan parses");

        assert_eq!(
            parsed.tools.visible_tools,
            vec!["memory_search", "memory_get"]
        );
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");
        assert_eq!(active_recall.status, ActiveRecallPlanJsonStatus::Run);
        assert_eq!(
            active_recall.reason_code,
            ActiveMemoryDecisionReasonCodeJson::MemoryLikely
        );
        assert_eq!(
            active_recall.modes,
            vec![ActiveRecallMode::Profile, ActiveRecallMode::ExactCanonical]
        );
        assert_eq!(
            active_recall.targets[0].canonical_key.as_deref(),
            Some("identity.current_user.name")
        );
    }

    #[test]
    fn provider_preflight_plan_json_parse_helper_is_strict_and_normalizes() {
        let raw = json!({
            "tools": {
                "visibleTools": [" memory_get ", "", "memory_search", "memory_get"]
            },
            "memory": {
                "activeRecall": {
                    "status": "run",
                    "reasonCode": "memory_likely",
                    "confidence": 0.92,
                    "modes": ["exact_canonical"],
                    "targets": [
                        {
                            "canonicalKey": "identity.current_user.name"
                        }
                    ]
                }
            }
        })
        .to_string();

        let parsed = parse_provider_turn_preflight_plan_json(raw.as_str())
            .expect("provider plan should parse and normalize");

        assert_eq!(
            parsed.tools.visible_tools,
            vec!["memory_get".to_owned(), "memory_search".to_owned()]
        );

        let malformed = parse_provider_turn_preflight_plan_json("{");
        assert!(malformed.is_err());

        let host_owned = json!({
            "source": "provider",
            "tools": {
                "visibleTools": []
            }
        })
        .to_string();
        let error = parse_provider_turn_preflight_plan_json(host_owned.as_str())
            .expect_err("provider plan must reject top-level host-owned fields");
        assert!(error.to_string().contains("source"));
    }

    #[test]
    fn provider_preflight_plan_normalizes_visible_tools_and_diagnostics() {
        let diagnostics = (0..20)
            .map(|index| TurnPreflightDiagnostic {
                code: diagnostic_code(format!("preflight.test.{index}")),
                message: None,
            })
            .collect();
        let plan = ProviderTurnPreflightPlan {
            tools: ProviderTurnPreflightToolsPlan {
                visible_tools: vec![
                    " memory_get ".to_owned(),
                    String::new(),
                    "memory_search".to_owned(),
                    "memory_get".to_owned(),
                ],
            },
            memory: None,
            diagnostics,
        };

        let normalized = normalize_provider_turn_preflight_plan(plan);

        assert_eq!(
            normalized.tools.visible_tools,
            vec!["memory_get".to_owned(), "memory_search".to_owned()]
        );
        assert_eq!(normalized.diagnostics.len(), PREFLIGHT_DIAGNOSTIC_MAX_COUNT);
        assert_eq!(
            normalized
                .diagnostics
                .last()
                .map(|diagnostic| diagnostic.code.as_str()),
            Some("preflight.test.15")
        );
    }

    #[test]
    fn provider_preflight_plan_rejects_host_owned_top_level_fields() {
        for field in [
            "source",
            "fallbackReason",
            "providerCall",
            "provider",
            "model",
            "attempt",
            "inputChars",
            "outputChars",
            "elapsedMs",
            "visibleTools",
        ] {
            let mut value = sample_provider_plan_json();
            value[field] = json!("host_owned");
            let error = serde_json::from_value::<ProviderTurnPreflightPlan>(value)
                .expect_err("provider plan must reject host-owned top-level fields");
            assert!(
                error.to_string().contains(field),
                "error `{error}` did not mention `{field}`"
            );
        }
    }

    #[test]
    fn provider_preflight_plan_rejects_host_owned_diagnostics_shape() {
        let mut value = sample_provider_plan_json();
        value["diagnostics"] = json!({
            "preflightFailed": true,
            "diagnostics": [{ "code": "preflight.failed" }]
        });

        serde_json::from_value::<ProviderTurnPreflightPlan>(value)
            .expect_err("provider diagnostics must not accept final-plan diagnostics object");
    }

    #[test]
    fn provider_preflight_plan_rejects_invalid_or_overlong_diagnostics() {
        let mut invalid_code = sample_provider_plan_json();
        invalid_code["diagnostics"] = json!([{ "code": "Preflight.Timeout" }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(invalid_code)
            .expect_err("provider diagnostic codes must be structured");

        let mut overlong_code = sample_provider_plan_json();
        overlong_code["diagnostics"] =
            json!([{ "code": "x".repeat(PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS + 1) }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(overlong_code)
            .expect_err("provider diagnostic codes must be bounded");

        let mut overlong_message = sample_provider_plan_json();
        overlong_message["diagnostics"] = json!([{
            "code": "preflight.message_too_long",
            "message": "x".repeat(PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS + 1)
        }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(overlong_message)
            .expect_err("provider diagnostic messages must be bounded");
    }

    #[test]
    fn provider_memory_active_recall_uses_existing_memory_boundary() {
        let mut value = sample_provider_plan_json();
        value["memory"]["activeRecall"]["debugFallback"] = json!(true);
        value["memory"]["activeRecall"]["providerUsed"] = json!(true);
        value["memory"]["activeRecall"]["source"] = json!("provider");

        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(value).expect("provider plan uses memory active recall parser");
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");

        assert_eq!(active_recall.status, ActiveRecallPlanJsonStatus::Run);
        let serialized = serde_json::to_value(active_recall).expect("active recall serializes");
        assert!(serialized.get("debugFallback").is_none());
        assert!(serialized.get("providerUsed").is_none());
        assert!(serialized.get("source").is_none());
    }

    #[tokio::test]
    async fn preflight_provider_request_uses_internal_prompt_and_no_tools() {
        let provider =
            FakePreflightProvider::text("preflight-provider", r#"{"tools":{"visibleTools":[]}}"#);
        let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;

        let success = match result {
            TurnPreflightProviderCallResult::Success(success) => success,
            TurnPreflightProviderCallResult::Failure(failure) => {
                panic!("expected preflight provider success, got {failure:?}")
            }
        };
        assert_eq!(success.provider_call.provider, "preflight-provider");
        assert_eq!(success.provider_call.model, "preflight-model");
        assert_eq!(success.provider_call.attempt, 1);

        let requests = provider.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.model, "preflight-model");
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);
        assert!(
            request.messages[0]
                .content
                .contains("Structured input JSON")
        );
        assert!(request.messages[0].content.contains("\"candidateTools\""));
        assert!(!request.messages[0].content.contains("\"properties\""));
        assert!(!request.messages[0].content.contains("\"jsonSchema\""));
        assert!(request.tools.is_none());
        assert!(request.tool_choice.is_none());
        assert_eq!(request.parallel_tool_calls, None);
        assert_eq!(request.compiled_prompt, None);
        assert_eq!(request.reasoning, Some(ReasoningConfig::disabled()));
    }

    #[tokio::test]
    async fn preflight_provider_streaming_path_uses_same_internal_request_shape() {
        let provider = FakePreflightProvider::streaming_text(
            "preflight-provider",
            r#"{"tools":{"visibleTools":["task_create"]}}"#,
        );
        let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;

        let success = match result {
            TurnPreflightProviderCallResult::Success(success) => success,
            TurnPreflightProviderCallResult::Failure(failure) => {
                panic!("expected streaming preflight provider success, got {failure:?}")
            }
        };
        assert_eq!(success.plan.tools.visible_tools, vec!["task_create"]);
        let requests = provider.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].tools.is_none());
        assert!(requests[0].tool_choice.is_none());
        assert_eq!(requests[0].compiled_prompt, None);
    }

    #[tokio::test]
    async fn preflight_provider_success_parses_memory_active_recall_with_memory_contract() {
        let provider = FakePreflightProvider::text(
            "preflight-provider",
            sample_provider_plan_json().to_string(),
        );
        let endpoint = provider_endpoint(provider, "preflight-provider", "preflight-model");

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;

        let success = match result {
            TurnPreflightProviderCallResult::Success(success) => success,
            TurnPreflightProviderCallResult::Failure(failure) => {
                panic!("expected preflight provider success, got {failure:?}")
            }
        };
        assert_eq!(
            success.plan.tools.visible_tools,
            vec!["memory_get".to_owned(), "memory_search".to_owned()]
        );
        let active_recall = success
            .plan
            .memory
            .and_then(|memory| memory.active_recall)
            .expect("provider returned active recall");
        let parsed = parse_provider_memory_active_recall_plan(&active_recall)
            .expect("nested memory active recall must use existing memory parser");
        assert_eq!(parsed.status, ActiveMemoryDecisionStatus::Run);
        assert_eq!(
            parsed.reason_code,
            ActiveMemoryDecisionReasonCode::MemoryLikely
        );
    }

    #[tokio::test]
    async fn preflight_provider_rejects_host_owned_output_metadata() {
        let provider = FakePreflightProvider::text(
            "preflight-provider",
            json!({
                "source": "provider",
                "tools": {
                    "visibleTools": []
                }
            })
            .to_string(),
        );
        let endpoint = provider_endpoint(provider, "preflight-provider", "preflight-model");

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;

        let failure = match result {
            TurnPreflightProviderCallResult::Failure(failure) => failure,
            TurnPreflightProviderCallResult::Success(success) => {
                panic!("expected host-owned provider output to fail, got {success:?}")
            }
        };
        assert_eq!(
            failure.fallback_reason,
            TurnPreflightFallbackReason::ValidationError
        );
        assert_eq!(failure.attempts.len(), 1);
        assert_eq!(
            failure.diagnostics[0].code.as_str(),
            "preflight.provider.validation_error"
        );
    }

    #[tokio::test]
    async fn preflight_provider_classifies_invalid_json_provider_error_and_timeout() {
        let invalid_json_provider = FakePreflightProvider::text("preflight-provider", "{");
        let endpoint = provider_endpoint(
            invalid_json_provider,
            "preflight-provider",
            "preflight-model",
        );
        let invalid_json = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;
        let invalid_json = match invalid_json {
            TurnPreflightProviderCallResult::Failure(failure) => failure,
            TurnPreflightProviderCallResult::Success(success) => {
                panic!("expected invalid JSON failure, got {success:?}")
            }
        };
        assert_eq!(
            invalid_json.fallback_reason,
            TurnPreflightFallbackReason::InvalidJson
        );
        assert_eq!(
            invalid_json.diagnostics[0].code.as_str(),
            "preflight.provider.invalid_json"
        );

        let provider_error_provider =
            FakePreflightProvider::failing("preflight-provider", "provider is down");
        let endpoint = provider_endpoint(
            provider_error_provider,
            "preflight-provider",
            "preflight-model",
        );
        let provider_error = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;
        let provider_error = match provider_error {
            TurnPreflightProviderCallResult::Failure(failure) => failure,
            TurnPreflightProviderCallResult::Success(success) => {
                panic!("expected provider error failure, got {success:?}")
            }
        };
        assert_eq!(
            provider_error.fallback_reason,
            TurnPreflightFallbackReason::ProviderError
        );
        assert_eq!(
            provider_error.diagnostics[0].code.as_str(),
            "preflight.provider.error"
        );

        let timeout_provider = FakePreflightProvider::delayed(
            "preflight-provider",
            50,
            sample_provider_plan_json().to_string(),
        );
        let endpoint = provider_endpoint(timeout_provider, "preflight-provider", "preflight-model");
        let mut input = provider_call_input(endpoint.clone(), endpoint);
        input.timeout_ms = 1;
        let timeout = call_turn_preflight_provider_with_retry(input).await;
        let timeout = match timeout {
            TurnPreflightProviderCallResult::Failure(failure) => failure,
            TurnPreflightProviderCallResult::Success(success) => {
                panic!("expected timeout failure, got {success:?}")
            }
        };
        assert_eq!(
            timeout.fallback_reason,
            TurnPreflightFallbackReason::Timeout
        );
        assert_eq!(
            timeout.diagnostics[0].code.as_str(),
            "preflight.provider.timeout"
        );
    }

    #[tokio::test]
    async fn preflight_retry_uses_thread_model_after_primary_failure() {
        let primary = FakePreflightProvider::text("configured-provider", "{");
        let thread =
            FakePreflightProvider::text("thread-provider", sample_provider_plan_json().to_string());

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            provider_endpoint(primary.clone(), "configured-provider", "configured-model"),
            provider_endpoint(thread.clone(), "thread-provider", "thread-model"),
        ))
        .await;

        let success = match result {
            TurnPreflightProviderCallResult::Success(success) => success,
            TurnPreflightProviderCallResult::Failure(failure) => {
                panic!("expected retry success, got {failure:?}")
            }
        };
        assert_eq!(primary.requests().len(), 1);
        assert_eq!(thread.requests().len(), 1);
        assert_eq!(success.provider_call.provider, "thread-provider");
        assert_eq!(success.provider_call.model, "thread-model");
        assert_eq!(success.provider_call.attempt, 2);
        assert_eq!(
            success.diagnostics[0].code.as_str(),
            "preflight.provider.invalid_json"
        );
        assert_eq!(
            success.diagnostics[1].code.as_str(),
            "preflight.provider.thread_model_retry_used"
        );
    }

    #[tokio::test]
    async fn preflight_retry_is_skipped_when_thread_endpoint_matches_primary() {
        let provider = FakePreflightProvider::text("thread-provider", "{");
        let endpoint = provider_endpoint(provider.clone(), "thread-provider", "thread-model");

        let result = call_turn_preflight_provider_with_retry(provider_call_input(
            endpoint.clone(),
            endpoint,
        ))
        .await;

        let failure = match result {
            TurnPreflightProviderCallResult::Failure(failure) => failure,
            TurnPreflightProviderCallResult::Success(success) => {
                panic!("expected no-retry failure, got {success:?}")
            }
        };
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(failure.attempts.len(), 1);
        assert_eq!(
            failure.fallback_reason,
            TurnPreflightFallbackReason::InvalidJson
        );
    }

    #[test]
    fn provider_memory_active_recall_validation_uses_existing_memory_parser() {
        let mut value = sample_provider_plan_json();
        value["memory"]["activeRecall"]["modes"] = json!([]);

        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(value.clone()).expect("provider plan shape parses");
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");

        let error = parse_provider_memory_active_recall_plan(&active_recall)
            .expect_err("memory parser should reject invalid active recall semantics");
        assert!(
            error.to_string().contains("requires at least one mode"),
            "unexpected error: {error}"
        );

        let raw = serde_json::to_string(&value).expect("provider plan serializes");
        let error = parse_provider_turn_preflight_plan_json(raw.as_str())
            .expect_err("full provider parse helper should validate active recall");
        assert!(
            error.to_string().contains("requires at least one mode"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn host_local_memory_active_recall_wrapper_uses_memory_normalization() {
        let active_recall = wrap_memory_active_recall_plan(
            TurnPreflightPlanSource::HostLocal,
            None,
            ActiveRecallPlan {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
                confidence: 4.0,
                modes: vec![
                    ActiveRecallMode::Project,
                    ActiveRecallMode::Profile,
                    ActiveRecallMode::Profile,
                ],
                targets: Vec::new(),
                debug_fallback: false,
                provider_used: false,
                provider_fallback_used: false,
                provider_input_chars: None,
                provider_output_chars: None,
                diagnostics: vec![String::new(), "host_local".to_owned()],
            },
        );

        assert_eq!(active_recall.decision.confidence, 1.0);
        assert_eq!(active_recall.decision.modes.len(), 2);
        assert!(
            active_recall
                .decision
                .modes
                .contains(&ActiveRecallMode::Profile)
        );
        assert!(
            active_recall
                .decision
                .modes
                .contains(&ActiveRecallMode::Project)
        );
        assert_eq!(active_recall.decision.diagnostics, vec!["host_local"]);
    }

    #[test]
    fn final_preflight_plan_accepts_host_owned_metadata_and_module_fallbacks() {
        let active_recall = wrap_memory_active_recall_plan(
            TurnPreflightPlanSource::HostLocal,
            None,
            ActiveRecallPlan {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
                confidence: 1.0,
                modes: vec![ActiveRecallMode::ExactCanonical],
                targets: vec![ActiveRecallTarget {
                    scope_kind: Some(MemoryScopeKind::User),
                    fact_class: Some(MemoryFactClass::UserIdentity),
                    category: Some(MemoryCategory::Identity),
                    subject: Some(MemorySubject::CurrentUser),
                    attribute: Some(MemoryAttribute::Name),
                    canonical_key: Some("identity.current_user.name".to_owned()),
                }],
                debug_fallback: false,
                provider_used: false,
                provider_fallback_used: false,
                provider_input_chars: None,
                provider_output_chars: None,
                diagnostics: vec!["memory.active_recall.host_local".to_owned()],
            },
        );
        let plan = fallback_turn_preflight_plan(
            TurnPreflightFallbackReason::Timeout,
            active_recall,
            vec![TurnPreflightDiagnostic {
                code: diagnostic_code("preflight.timeout"),
                message: Some(diagnostic_message("provider timed out")),
            }],
            BTreeMap::from([(
                "tools".to_owned(),
                vec![TurnPreflightDiagnostic {
                    code: diagnostic_code("preflight.tools.no_optional"),
                    message: None,
                }],
            )]),
            Some(TurnPreflightProviderCallMetadata {
                provider: "thread".to_owned(),
                model: "thread-model".to_owned(),
                attempt: 2,
                input_chars: 1200,
                output_chars: 0,
                elapsed_ms: 30_000,
            }),
        );

        let value = serde_json::to_value(&plan).expect("final plan serializes");
        assert_eq!(value["source"], "fallback");
        assert_eq!(value["fallbackReason"], "timeout");
        assert_eq!(value["tools"]["visibleTools"], json!([]));
        assert!(value.get("visibleTools").is_none());
        assert_eq!(value["diagnostics"]["preflightFailed"], true);
        assert_eq!(value["providerCall"]["attempt"], 2);
        assert_eq!(
            value["memory"]["activeRecall"]["source"],
            JsonValue::String("host_local".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decision"]["reasonCode"],
            JsonValue::String("memory_likely".to_owned())
        );

        let decoded: TurnPreflightPlan =
            serde_json::from_value(value).expect("final plan deserializes");
        assert_eq!(decoded.source, TurnPreflightPlanSource::Fallback);
        assert_eq!(
            decoded.memory.active_recall.source,
            TurnPreflightPlanSource::HostLocal
        );
    }

    #[test]
    fn final_preflight_plan_can_represent_provider_owned_memory_plan() {
        let provider_call = TurnPreflightProviderCallMetadata {
            provider: "openai".to_owned(),
            model: "gpt-test".to_owned(),
            attempt: 1,
            input_chars: 900,
            output_chars: 240,
            elapsed_ms: 800,
        };
        let provider_plan: ProviderTurnPreflightPlan =
            serde_json::from_value(sample_provider_plan_json()).expect("provider plan parses");
        let provider_active_recall = provider_plan
            .memory
            .and_then(|memory| memory.active_recall)
            .expect("provider active recall");
        let decision_json = serde_json::to_string(&provider_active_recall)
            .expect("provider active recall serializes");
        let decision = parse_active_memory_decision_json(decision_json.as_str())
            .expect("provider active recall uses existing memory parser");
        let final_plan = TurnPreflightPlan {
            source: TurnPreflightPlanSource::Provider,
            fallback_reason: None,
            tools: TurnPreflightToolsPlan {
                visible_tools: provider_plan.tools.visible_tools,
            },
            memory: TurnPreflightMemoryPlan {
                active_recall: TurnPreflightMemoryActiveRecallPlan {
                    source: TurnPreflightPlanSource::Provider,
                    fallback_reason: None,
                    decision,
                },
            },
            diagnostics: TurnPreflightDiagnostics {
                preflight_failed: false,
                diagnostics: provider_plan.diagnostics,
                module_diagnostics: BTreeMap::new(),
            },
            provider_call: Some(provider_call),
        };

        assert_eq!(final_plan.source, TurnPreflightPlanSource::Provider);
        assert_eq!(
            final_plan.provider_call.as_ref().map(|call| call.attempt),
            Some(1)
        );
    }

    #[test]
    fn final_preflight_plan_can_represent_synthetic_module_fallback() {
        let value = json!({
            "source": "fallback",
            "fallbackReason": "validation_error",
            "tools": {
                "visibleTools": []
            },
            "memory": {
                "activeRecall": {
                    "source": "fallback",
                    "fallbackReason": "validation_error",
                    "decision": {
                        "status": "skip",
                        "reasonCode": "provider_skip",
                        "confidence": 0.0,
                        "modes": [],
                        "targets": [],
                        "debugFallback": false,
                        "providerUsed": false,
                        "providerFallbackUsed": true,
                        "diagnostics": [
                            "memory.active_recall.fallback"
                        ]
                    }
                }
            },
            "diagnostics": {
                "preflightFailed": true,
                "diagnostics": [
                    { "code": "preflight.validation_error" }
                ],
                "moduleDiagnostics": {
                    "memory.activeRecall": [
                        { "code": "memory.active_recall.provider_invalid_json" }
                    ]
                }
            }
        });

        let plan: TurnPreflightPlan =
            serde_json::from_value(value).expect("synthetic fallback final plan parses");
        assert_eq!(plan.source, TurnPreflightPlanSource::Fallback);
        assert!(plan.diagnostics.preflight_failed);
        assert_eq!(
            plan.memory.active_recall.fallback_reason,
            Some(TurnPreflightFallbackReason::ValidationError)
        );
        assert!(plan.memory.active_recall.decision.provider_fallback_used);
    }
}
