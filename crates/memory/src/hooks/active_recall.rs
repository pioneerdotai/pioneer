use super::*;
use crate::recall::compact_recall_content;
use serde::de::Error as _;

const ACTIVE_RECALL_MAX_MODES: usize = 4;
const ACTIVE_RECALL_MAX_TARGETS: usize = 6;
const ACTIVE_RECALL_MAX_DIAGNOSTICS: usize = 6;
const ACTIVE_RECALL_MAX_DIAGNOSTIC_CHARS: usize = 160;
const ACTIVE_RECALL_MAX_CANONICAL_KEY_CHARS: usize = 240;
const ACTIVE_RECALL_THREAD_SUMMARY_MAX_SOURCE_IDS: usize = 8;
const ACTIVE_RECALL_THREAD_SUMMARY_MAX_DIAGNOSTICS: usize = 8;
pub(super) const ACTIVE_RECALL_INPUT_PREVIEW_MAX_CHARS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveMemoryDecisionStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveMemoryDecisionReasonCode {
    PolicyDisabled,
    ConfigDisabled,
    DeterministicOnly,
    DeterministicSufficient,
    MemoryLikely,
    StrictDebug,
    ProviderRun,
    ProviderSkip,
    ProviderUncertain,
}

impl ActiveMemoryDecisionReasonCode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::ConfigDisabled => "config_disabled",
            Self::DeterministicOnly => "deterministic_only",
            Self::DeterministicSufficient => "deterministic_sufficient",
            Self::MemoryLikely => "memory_likely",
            Self::StrictDebug => "strict_debug",
            Self::ProviderRun => "provider_run",
            Self::ProviderSkip => "provider_skip",
            Self::ProviderUncertain => "provider_uncertain",
        }
    }

    pub(super) fn diagnostic_code(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "memory.active_recall.policy_disabled",
            Self::ConfigDisabled => "memory.active_recall.config_disabled",
            Self::DeterministicOnly => "memory.active_recall.deterministic_only",
            Self::DeterministicSufficient => "memory.active_recall.deterministic_sufficient",
            Self::MemoryLikely | Self::StrictDebug | Self::ProviderRun => {
                "memory.active_recall.started"
            }
            Self::ProviderSkip => "memory.active_recall.skipped",
            Self::ProviderUncertain => "memory.active_recall.uncertain",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveRecallMode {
    Profile,
    Project,
    Durable,
    #[serde(alias = "current_threads")]
    #[serde(alias = "current_thread_context")]
    #[serde(alias = "thread_context")]
    CurrentThread,
    #[serde(alias = "related_threads")]
    RelatedThread,
    #[serde(alias = "workspace_threads")]
    #[serde(alias = "workspace_thread_context")]
    WorkspaceThread,
    CurrentTask,
    CompletedTask,
    ThreadEpisodic,
    TaskContext,
    ExactCanonical,
}

impl ActiveRecallMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Profile => "profile",
            Self::Project => "project",
            Self::Durable => "durable",
            Self::CurrentThread => "current_thread",
            Self::RelatedThread => "related_thread",
            Self::WorkspaceThread => "workspace_thread",
            Self::CurrentTask => "current_task",
            Self::CompletedTask => "completed_task",
            Self::ThreadEpisodic => "thread_episodic",
            Self::TaskContext => "task_context",
            Self::ExactCanonical => "exact_canonical",
        }
    }

    fn rank(self) -> usize {
        match self {
            Self::ExactCanonical => 0,
            Self::Profile => 1,
            Self::Project => 2,
            Self::CurrentTask => 3,
            Self::TaskContext => 4,
            Self::CurrentThread => 5,
            Self::ThreadEpisodic => 6,
            Self::RelatedThread => 7,
            Self::WorkspaceThread => 8,
            Self::CompletedTask => 9,
            Self::Durable => 10,
        }
    }

    fn durable_recall_mode(self) -> Option<MemoryRecallMode> {
        match self {
            Self::Profile => Some(MemoryRecallMode::Profile),
            Self::Project => Some(MemoryRecallMode::Project),
            Self::Durable => Some(MemoryRecallMode::Durable),
            Self::ExactCanonical => Some(MemoryRecallMode::ExactCanonical),
            Self::CurrentThread
            | Self::RelatedThread
            | Self::WorkspaceThread
            | Self::CurrentTask
            | Self::CompletedTask
            | Self::ThreadEpisodic
            | Self::TaskContext => None,
        }
    }

    fn episodic_source_kind(self) -> Option<MemoryEpisodicRecallSourceKind> {
        match self {
            Self::CurrentThread | Self::ThreadEpisodic => {
                Some(MemoryEpisodicRecallSourceKind::CurrentThread)
            }
            Self::RelatedThread => Some(MemoryEpisodicRecallSourceKind::RelatedThread),
            Self::WorkspaceThread => Some(MemoryEpisodicRecallSourceKind::WorkspaceThread),
            Self::CurrentTask | Self::TaskContext => {
                Some(MemoryEpisodicRecallSourceKind::CurrentTask)
            }
            Self::CompletedTask => Some(MemoryEpisodicRecallSourceKind::CompletedTask),
            Self::Profile | Self::Project | Self::Durable | Self::ExactCanonical => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveRecallTarget {
    #[serde(default)]
    pub scope_kind: Option<MemoryScopeKind>,
    #[serde(default)]
    pub fact_class: Option<MemoryFactClass>,
    #[serde(default)]
    pub category: Option<MemoryCategory>,
    #[serde(default)]
    pub subject: Option<MemorySubject>,
    #[serde(default)]
    pub attribute: Option<MemoryAttribute>,
    #[serde(default)]
    pub canonical_key: Option<String>,
}

impl ActiveRecallTarget {
    #[cfg(test)]
    pub(super) fn exact_canonical(canonical_key: impl Into<String>) -> Self {
        Self {
            canonical_key: Some(canonical_key.into()),
            ..Self::default()
        }
    }

    fn normalized(mut self) -> Option<Self> {
        self.canonical_key = self.canonical_key.and_then(|key| {
            bounded_nonempty_text(key.as_str(), ACTIVE_RECALL_MAX_CANONICAL_KEY_CHARS)
        });
        self.has_any_field().then_some(self)
    }

    fn has_any_field(&self) -> bool {
        self.scope_kind.is_some()
            || self.fact_class.is_some()
            || self.category.is_some()
            || self.subject.is_some()
            || self.attribute.is_some()
            || self.canonical_key.is_some()
    }

    fn has_unknown_fact_class(&self) -> bool {
        self.fact_class == Some(MemoryFactClass::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveRecallPlan {
    pub status: ActiveMemoryDecisionStatus,
    pub reason_code: ActiveMemoryDecisionReasonCode,
    pub confidence: f32,
    #[serde(default)]
    pub modes: Vec<ActiveRecallMode>,
    #[serde(default)]
    pub targets: Vec<ActiveRecallTarget>,
    #[serde(default)]
    pub debug_fallback: bool,
    #[serde(default)]
    pub provider_used: bool,
    #[serde(default)]
    pub provider_fallback_used: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_input_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_output_chars: Option<usize>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

impl ActiveRecallPlan {
    pub(super) fn skip(
        reason_code: ActiveMemoryDecisionReasonCode,
        confidence: f32,
        diagnostics: Vec<String>,
    ) -> Self {
        Self {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code,
            confidence: confidence.clamp(0.0, 1.0),
            modes: Vec::new(),
            targets: Vec::new(),
            debug_fallback: false,
            provider_used: false,
            provider_fallback_used: false,
            provider_input_chars: None,
            provider_output_chars: None,
            diagnostics: normalize_active_recall_diagnostics(diagnostics),
        }
    }

    pub(super) fn run(
        reason_code: ActiveMemoryDecisionReasonCode,
        confidence: f32,
        modes: Vec<ActiveRecallMode>,
        targets: Vec<ActiveRecallTarget>,
        diagnostics: Vec<String>,
    ) -> Self {
        normalize_active_recall_plan(Self {
            status: ActiveMemoryDecisionStatus::Run,
            reason_code,
            confidence: confidence.clamp(0.0, 1.0),
            modes,
            targets,
            debug_fallback: false,
            provider_used: false,
            provider_fallback_used: false,
            provider_input_chars: None,
            provider_output_chars: None,
            diagnostics,
        })
    }

    pub(super) fn uncertain(
        reason_code: ActiveMemoryDecisionReasonCode,
        confidence: f32,
        diagnostics: Vec<String>,
    ) -> Self {
        Self {
            status: ActiveMemoryDecisionStatus::Uncertain,
            reason_code,
            confidence: confidence.clamp(0.0, 1.0),
            modes: Vec::new(),
            targets: Vec::new(),
            debug_fallback: false,
            provider_used: false,
            provider_fallback_used: false,
            provider_input_chars: None,
            provider_output_chars: None,
            diagnostics: normalize_active_recall_diagnostics(diagnostics),
        }
    }

    pub(super) fn with_debug_fallback(mut self) -> Self {
        self.debug_fallback = true;
        self
    }
}

pub type ActiveMemoryDecision = ActiveRecallPlan;

impl From<&ActiveRecallTarget> for MemoryRecallTarget {
    fn from(target: &ActiveRecallTarget) -> Self {
        Self {
            scope_kind: target.scope_kind,
            fact_class: target.fact_class,
            category: target.category,
            subject: target.subject,
            attribute: target.attribute,
            canonical_key: target.canonical_key.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct ActiveRecallExecutionInput {
    pub(super) context: MemoryTurnContext,
    pub(super) plan: ActiveRecallPlan,
    pub(super) deterministic: DeterministicRecallContextSummary,
    pub(super) config: MemoryActiveRecallConfig,
    pub(super) episodic_provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    pub(super) episodic_capabilities: MemoryEpisodicRecallCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveRecallModeBudget {
    pub(super) top_k: u32,
    pub(super) max_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveRecallModeRequest {
    pub(super) mode: ActiveRecallMode,
    pub(super) targets: Vec<ActiveRecallTarget>,
    pub(super) budget: ActiveRecallModeBudget,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ActiveRecallModeResult {
    pub(super) mode: ActiveRecallMode,
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) episodic_items: Vec<MemoryEpisodicRecallItem>,
    pub(super) diagnostics: Vec<String>,
    pub(super) truncated: bool,
    pub(super) skipped_reason: Option<String>,
}

impl ActiveRecallModeResult {
    pub(super) fn skipped(mode: ActiveRecallMode, reason: impl Into<String>) -> Self {
        Self {
            mode,
            items: Vec::new(),
            episodic_items: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
            skipped_reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct ActiveRecallExecutionResult {
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) episodic_items: Vec<MemoryEpisodicRecallItem>,
    pub(super) mode_results: Vec<ActiveRecallModeResult>,
    pub(super) diagnostics: Vec<String>,
    pub(super) truncated: bool,
    pub(super) raw_item_count: usize,
    pub(super) duplicate_count: usize,
}

impl ActiveRecallExecutionResult {
    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty() && self.episodic_items.is_empty()
    }
}

pub(super) async fn execute_active_recall_plan(
    provider: &dyn AgentMemoryProvider,
    input: ActiveRecallExecutionInput,
) -> ActiveRecallExecutionResult {
    let deterministic_recall_count = input.deterministic.memory_ids.len();
    let requests = active_recall_mode_requests(&input);
    if requests.is_empty() {
        return ActiveRecallExecutionResult {
            diagnostics: vec!["memory.active_recall.no_executable_modes".to_owned()],
            ..ActiveRecallExecutionResult::default()
        };
    }

    let mut mode_results = Vec::new();
    for request in requests {
        if let Some(skip_reason) = active_recall_mode_skip_reason(&request, &input.context) {
            mode_results.push(ActiveRecallModeResult::skipped(request.mode, skip_reason));
            continue;
        }
        let mode = request.mode;
        if let Some(source) = mode.episodic_source_kind() {
            mode_results.push(
                execute_episodic_active_recall_mode(
                    input.episodic_provider.as_deref(),
                    &input.episodic_capabilities,
                    &input.context,
                    input.context.input_text.as_str(),
                    &request,
                    source,
                    input.config.timeout_ms,
                )
                .await,
            );
            continue;
        }

        let Some(durable_mode) = mode.durable_recall_mode() else {
            mode_results.push(ActiveRecallModeResult::skipped(mode, "mode_not_supported"));
            continue;
        };
        match provider
            .recall_memory_mode(
                input.context.clone(),
                MemoryModeRecallParams {
                    mode: durable_mode,
                    targets: request
                        .targets
                        .iter()
                        .map(MemoryRecallTarget::from)
                        .collect(),
                    top_k: Some(request.budget.top_k),
                    max_chars: Some(request.budget.max_chars),
                },
            )
            .await
        {
            Ok(snapshot) => {
                let skipped_reason = snapshot.items.is_empty().then_some("no_hits".to_owned());
                mode_results.push(ActiveRecallModeResult {
                    mode,
                    items: snapshot.items,
                    episodic_items: Vec::new(),
                    diagnostics: snapshot.diagnostics,
                    truncated: snapshot.truncated,
                    skipped_reason,
                });
            }
            Err(_) => {
                mode_results.push(ActiveRecallModeResult {
                    mode,
                    items: Vec::new(),
                    episodic_items: Vec::new(),
                    diagnostics: vec![format!(
                        "memory.active_recall.mode_failed:{}",
                        mode.as_str()
                    )],
                    truncated: false,
                    skipped_reason: Some("provider_error".to_owned()),
                });
            }
        }
    }

    let mut result = merge_active_recall_mode_results(mode_results, &input.config);
    result.diagnostics.push(format!(
        "memory.active_recall.deterministic_recall_count:{deterministic_recall_count}"
    ));
    result
}

pub(super) async fn execute_active_recall_debug_fallback(
    provider: &dyn AgentMemoryProvider,
    context: MemoryTurnContext,
    input_text: &str,
    decision: &ActiveMemoryDecision,
    config: &MemoryActiveRecallConfig,
) -> ActiveRecallExecutionResult {
    if !decision.debug_fallback {
        return ActiveRecallExecutionResult::default();
    }
    let queries = active_memory_query_plan(input_text, decision, config);
    let mut mode_result = ActiveRecallModeResult {
        mode: ActiveRecallMode::Durable,
        items: Vec::new(),
        episodic_items: Vec::new(),
        diagnostics: vec!["memory.active_recall.debug_fallback_started".to_owned()],
        truncated: false,
        skipped_reason: None,
    };
    for query in queries {
        match provider
            .recall_memory(
                context.clone(),
                MemoryRecallRequest {
                    query: query.query,
                    categories: query.categories,
                    top_k: Some(config.top_k_per_query),
                    max_chars: Some(config.max_prompt_chars),
                },
            )
            .await
        {
            Ok(snapshot) => {
                mode_result.truncated |= snapshot.truncated;
                mode_result.diagnostics.extend(snapshot.diagnostics);
                mode_result.items.extend(snapshot.items);
            }
            Err(_) => {
                mode_result
                    .diagnostics
                    .push("memory.active_recall.debug_fallback_failed".to_owned());
                mode_result.skipped_reason = Some("provider_error".to_owned());
                break;
            }
        }
    }
    merge_active_recall_mode_results(vec![mode_result], config)
}

async fn execute_episodic_active_recall_mode(
    provider: Option<&dyn AgentEpisodicRecallProvider>,
    capabilities: &MemoryEpisodicRecallCapabilities,
    context: &MemoryTurnContext,
    query: &str,
    request: &ActiveRecallModeRequest,
    source: MemoryEpisodicRecallSourceKind,
    timeout_ms: u64,
) -> ActiveRecallModeResult {
    let Some(provider) = provider else {
        return ActiveRecallModeResult::skipped(request.mode, "capability_unavailable");
    };
    if !capabilities.supports_source(source) {
        return ActiveRecallModeResult::skipped(
            request.mode,
            format!("capability_unavailable:{}", source.as_str()),
        );
    }

    let targets = request
        .targets
        .iter()
        .map(MemoryRecallTarget::from)
        .collect::<Vec<_>>();
    let timeout = std::time::Duration::from_millis(timeout_ms.max(1));
    let response = match source {
        MemoryEpisodicRecallSourceKind::CurrentThread
        | MemoryEpisodicRecallSourceKind::TranscriptSummary => {
            tokio::time::timeout(
                timeout,
                provider.recall_current_thread(MemoryCurrentThreadRecallRequest {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn_id: context.turn_id.clone(),
                    query: query.to_owned(),
                    targets,
                    top_k: request.budget.top_k,
                    max_chars: request.budget.max_chars,
                }),
            )
            .await
        }
        MemoryEpisodicRecallSourceKind::RelatedThread => {
            tokio::time::timeout(
                timeout,
                provider.recall_related_threads(MemoryRelatedThreadRecallRequest {
                    workspace_id: context.workspace_id.clone(),
                    current_thread_id: context.thread_id.clone(),
                    query: query.to_owned(),
                    targets,
                    top_k: request.budget.top_k,
                    max_chars: request.budget.max_chars,
                }),
            )
            .await
        }
        MemoryEpisodicRecallSourceKind::WorkspaceThread => {
            tokio::time::timeout(
                timeout,
                provider.recall_workspace_threads(MemoryWorkspaceThreadRecallRequest {
                    workspace_id: context.workspace_id.clone(),
                    current_thread_id: context.thread_id.clone(),
                    query: query.to_owned(),
                    targets,
                    top_k: request.budget.top_k,
                    max_chars: request.budget.max_chars,
                }),
            )
            .await
        }
        MemoryEpisodicRecallSourceKind::CurrentTask => {
            let Some(task_id) = context
                .task_id
                .as_ref()
                .filter(|task_id| !task_id.trim().is_empty())
            else {
                return ActiveRecallModeResult::skipped(request.mode, "missing_task_context");
            };
            tokio::time::timeout(
                timeout,
                provider.recall_current_task(MemoryCurrentTaskRecallRequest {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    task_id: task_id.clone(),
                    query: query.to_owned(),
                    targets,
                    top_k: request.budget.top_k,
                    max_chars: request.budget.max_chars,
                }),
            )
            .await
        }
        MemoryEpisodicRecallSourceKind::CompletedTask => {
            tokio::time::timeout(
                timeout,
                provider.recall_completed_tasks(MemoryCompletedTaskRecallRequest {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    task_id: context.task_id.clone(),
                    query: query.to_owned(),
                    targets,
                    top_k: request.budget.top_k,
                    max_chars: request.budget.max_chars,
                }),
            )
            .await
        }
    };

    match response {
        Err(_) => ActiveRecallModeResult {
            mode: request.mode,
            items: Vec::new(),
            episodic_items: Vec::new(),
            diagnostics: vec![format!(
                "memory.episodic_recall.mode_timed_out:{}",
                request.mode.as_str()
            )],
            truncated: false,
            skipped_reason: Some("provider_timeout".to_owned()),
        },
        Ok(response) => {
            let response = match response {
                Ok(response) => response,
                Err(_) => {
                    return ActiveRecallModeResult {
                        mode: request.mode,
                        items: Vec::new(),
                        episodic_items: Vec::new(),
                        diagnostics: vec![format!(
                            "memory.episodic_recall.mode_failed:{}",
                            request.mode.as_str()
                        )],
                        truncated: false,
                        skipped_reason: Some("provider_error".to_owned()),
                    };
                }
            };
            let filtered = filter_rank_and_bound_episodic_items(
                response.items,
                request.mode,
                context,
                request.budget.top_k as usize,
                request.budget.max_chars,
            );
            let skipped_reason =
                filtered
                    .items
                    .is_empty()
                    .then_some(if filtered.suppressed_count > 0 {
                        "all_hits_filtered".to_owned()
                    } else {
                        "no_hits".to_owned()
                    });
            let mut diagnostics = response.diagnostics;
            diagnostics.push(format!(
                "memory.episodic_recall.mode_executed:{}:{}",
                request.mode.as_str(),
                source.as_str()
            ));
            if filtered.suppressed_count > 0 {
                diagnostics.push(format!(
                    "memory.episodic_recall.filtered_count:{}:{}",
                    request.mode.as_str(),
                    filtered.suppressed_count
                ));
            }
            if filtered.truncated || response.truncated {
                diagnostics.push(format!(
                    "memory.episodic_recall.truncated:{}",
                    request.mode.as_str()
                ));
            }
            ActiveRecallModeResult {
                mode: request.mode,
                items: Vec::new(),
                episodic_items: filtered.items,
                diagnostics,
                truncated: response.truncated || filtered.truncated,
                skipped_reason,
            }
        }
    }
}

pub(super) fn active_recall_execution_observability_diagnostic(
    result: &ActiveRecallExecutionResult,
) -> HookDiagnostic {
    let executed_modes = result
        .mode_results
        .iter()
        .filter(|mode_result| mode_result.skipped_reason.is_none())
        .map(|mode_result| mode_result.mode.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let skipped_modes = result
        .mode_results
        .iter()
        .filter(|mode_result| mode_result.skipped_reason.is_some())
        .map(|mode_result| mode_result.mode.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.execution",
        format!(
            "memory active recall execution: executed_modes={} skipped_modes={} mode_count={} rendered_count={} truncated={}",
            executed_modes,
            skipped_modes,
            result.mode_results.len(),
            result.items.len() + result.episodic_items.len(),
            result.truncated
        ),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("executed_modes"),
        HookValue::Text(executed_modes),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("skipped_modes"),
        HookValue::Text(skipped_modes),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "mode_count",
        result.mode_results.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "rendered_count",
        result.items.len() + result.episodic_items.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "durable_count",
        result.items.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "episodic_count",
        result.episodic_items.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "raw_item_count",
        result.raw_item_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "duplicate_count",
        result.duplicate_count,
    );
    diagnostic.metadata.insert(
        hook_metadata_key("mode_hit_counts"),
        HookValue::Text(
            result
                .mode_results
                .iter()
                .map(|mode_result| {
                    format!(
                        "{}={}",
                        mode_result.mode.as_str(),
                        mode_result.items.len() + mode_result.episodic_items.len()
                    )
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("truncated"),
        HookValue::Bool(result.truncated),
    );
    diagnostic
}

pub(super) fn deterministic_recall_debug_audit_contribution(
    synthesis: &MemoryRecallSynthesis,
) -> HookContribution {
    let mut details = BTreeMap::new();
    details.insert(
        metadata_key("planner_kind"),
        HookValue::Text("deterministic".to_owned()),
    );
    details.insert(
        metadata_key("planner_status"),
        HookValue::Text("run".to_owned()),
    );
    details.insert(
        metadata_key("planner_reason"),
        HookValue::Text("deterministic_recall".to_owned()),
    );
    details.insert(
        metadata_key("deterministic_sufficient"),
        HookValue::Bool(!synthesis.items.is_empty()),
    );
    details.insert(
        metadata_key("selected_modes"),
        hook_value_string_list(vec!["deterministic".to_owned()]),
    );
    details.insert(
        metadata_key("modes"),
        HookValue::List(vec![hook_value_object([
            ("mode", HookValue::Text("deterministic".to_owned())),
            (
                "hit_count",
                HookValue::I64(i64::try_from(synthesis.items.len()).unwrap_or(i64::MAX)),
            ),
            ("truncated", HookValue::Bool(synthesis.truncated)),
        ])]),
    );
    details.insert(
        metadata_key("suppression_counts"),
        hook_value_suppression_counts([
            ("duplicate", synthesis.duplicate_count()),
            (
                "empty_content",
                usize::from(
                    synthesis
                        .diagnostics
                        .iter()
                        .any(|diagnostic| diagnostic.contains("empty_content")),
                ),
            ),
        ]),
    );
    details.insert(
        metadata_key("synthesized_count"),
        HookValue::I64(i64::try_from(synthesis.items.len()).unwrap_or(i64::MAX)),
    );
    details.insert(
        metadata_key("prompt_contribution_chars"),
        HookValue::I64(
            i64::try_from(synthesis.rendered_text().chars().count()).unwrap_or(i64::MAX),
        ),
    );
    recall_debug_audit("memory.recall.deterministic", details)
}

pub(super) fn active_recall_debug_audit_contribution(
    decision: &ActiveMemoryDecision,
    deterministic: &DeterministicRecallContextSummary,
    execution: &ActiveRecallExecutionResult,
    dedup: Option<&ActiveRecallDedupResult>,
    synthesis: Option<&MemoryActiveSynthesisOutput>,
) -> HookContribution {
    let mut details = BTreeMap::new();
    details.insert(
        metadata_key("planner_kind"),
        HookValue::Text(if decision.provider_used {
            "provider".to_owned()
        } else if decision.provider_fallback_used {
            "fallback".to_owned()
        } else if decision.status == ActiveMemoryDecisionStatus::Skip {
            "skipped".to_owned()
        } else {
            "deterministic".to_owned()
        }),
    );
    details.insert(
        metadata_key("planner_status"),
        HookValue::Text(active_memory_decision_status_name(decision.status).to_owned()),
    );
    details.insert(
        metadata_key("planner_reason"),
        HookValue::Text(decision.reason_code.as_str().to_owned()),
    );
    details.insert(
        metadata_key("provider_used"),
        HookValue::Bool(decision.provider_used),
    );
    details.insert(
        metadata_key("provider_fallback_used"),
        HookValue::Bool(decision.provider_fallback_used),
    );
    details.insert(
        metadata_key("deterministic_sufficient"),
        HookValue::Bool(deterministic.sufficient),
    );
    details.insert(
        metadata_key("selected_modes"),
        hook_value_string_list(
            decision
                .modes
                .iter()
                .map(|mode| mode.as_str().to_owned())
                .collect(),
        ),
    );
    let dropped_modes = execution
        .mode_results
        .iter()
        .filter_map(|mode_result| {
            mode_result
                .skipped_reason
                .as_ref()
                .map(|reason| format!("{}:{reason}", mode_result.mode.as_str()))
        })
        .collect::<Vec<_>>();
    details.insert(
        metadata_key("dropped_modes"),
        hook_value_string_list(dropped_modes),
    );
    details.insert(
        metadata_key("modes"),
        HookValue::List(
            execution
                .mode_results
                .iter()
                .map(|mode_result| {
                    hook_value_object([
                        (
                            "mode",
                            HookValue::Text(mode_result.mode.as_str().to_owned()),
                        ),
                        (
                            "hit_count",
                            HookValue::I64(
                                i64::try_from(
                                    mode_result.items.len() + mode_result.episodic_items.len(),
                                )
                                .unwrap_or(i64::MAX),
                            ),
                        ),
                        (
                            "durable_hit_count",
                            HookValue::I64(
                                i64::try_from(mode_result.items.len()).unwrap_or(i64::MAX),
                            ),
                        ),
                        (
                            "episodic_hit_count",
                            HookValue::I64(
                                i64::try_from(mode_result.episodic_items.len()).unwrap_or(i64::MAX),
                            ),
                        ),
                        ("truncated", HookValue::Bool(mode_result.truncated)),
                        (
                            "skipped_reason",
                            mode_result
                                .skipped_reason
                                .as_ref()
                                .map(|reason| HookValue::Text(reason.clone()))
                                .unwrap_or(HookValue::Null),
                        ),
                    ])
                })
                .collect(),
        ),
    );
    let duplicate_count = dedup
        .map(ActiveRecallDedupResult::duplicate_count)
        .unwrap_or(execution.duplicate_count);
    details.insert(
        metadata_key("suppression_counts"),
        hook_value_suppression_counts([
            ("duplicate", duplicate_count),
            (
                "stale_backend",
                diagnostic_count(execution, "backend_stale_ids"),
            ),
            (
                "quality_penalty",
                diagnostic_count(execution, "quality_penalty_applied_count"),
            ),
            (
                "low_source_context",
                diagnostic_count(execution, "low_source_context_penalty_count"),
            ),
            (
                "rejected_related",
                diagnostic_count(execution, "rejected_related_penalty_count"),
            ),
        ]),
    );
    details.insert(
        metadata_key("suppressed_ids"),
        hook_value_string_list(
            dedup
                .map(|dedup| dedup.duplicate_ids.clone())
                .unwrap_or_default(),
        ),
    );
    details.insert(
        metadata_key("source_boundaries"),
        hook_value_source_boundary_counts(execution),
    );
    if let Some(synthesis) = synthesis {
        details.insert(
            metadata_key("synthesized_count"),
            HookValue::I64(i64::try_from(synthesis.items.len()).unwrap_or(i64::MAX)),
        );
        details.insert(
            metadata_key("prompt_contribution_chars"),
            HookValue::I64(
                i64::try_from(synthesis.rendered_text().chars().count()).unwrap_or(i64::MAX),
            ),
        );
    }
    recall_debug_audit("memory.recall.active", details)
}

fn active_recall_mode_requests(input: &ActiveRecallExecutionInput) -> Vec<ActiveRecallModeRequest> {
    let max_modes = input.config.max_queries.max(1);
    let mode_count = input.plan.modes.len().min(max_modes).max(1);
    let max_chars_per_mode = (input.config.max_prompt_chars / mode_count).max(1);
    input
        .plan
        .modes
        .iter()
        .copied()
        .take(max_modes)
        .map(|mode| ActiveRecallModeRequest {
            mode,
            targets: active_recall_targets_for_mode(mode, input.plan.targets.as_slice()),
            budget: ActiveRecallModeBudget {
                top_k: input.config.top_k_per_query,
                max_chars: max_chars_per_mode,
            },
        })
        .collect()
}

fn active_recall_targets_for_mode(
    mode: ActiveRecallMode,
    targets: &[ActiveRecallTarget],
) -> Vec<ActiveRecallTarget> {
    match mode {
        ActiveRecallMode::ExactCanonical => targets
            .iter()
            .filter(|target| target.canonical_key.is_some())
            .cloned()
            .collect(),
        _ => targets.to_vec(),
    }
}

fn active_recall_mode_skip_reason(
    request: &ActiveRecallModeRequest,
    context: &MemoryTurnContext,
) -> Option<String> {
    match request.mode {
        ActiveRecallMode::ExactCanonical if request.targets.is_empty() => {
            Some("missing_canonical_target".to_owned())
        }
        ActiveRecallMode::TaskContext | ActiveRecallMode::CurrentTask
            if context
                .task_id
                .as_ref()
                .is_none_or(|task_id| task_id.trim().is_empty()) =>
        {
            Some("missing_task_context".to_owned())
        }
        ActiveRecallMode::ThreadEpisodic
        | ActiveRecallMode::CurrentThread
        | ActiveRecallMode::RelatedThread
        | ActiveRecallMode::WorkspaceThread
            if context.thread_id.trim().is_empty() =>
        {
            Some("missing_thread_context".to_owned())
        }
        _ => None,
    }
}

fn merge_active_recall_mode_results(
    mode_results: Vec<ActiveRecallModeResult>,
    config: &MemoryActiveRecallConfig,
) -> ActiveRecallExecutionResult {
    let mut seen_ids = BTreeSet::new();
    let mut seen_lines = BTreeSet::new();
    let mut seen_episodic_ids = BTreeSet::new();
    let mut seen_episodic_lines = BTreeSet::new();
    let mut items = Vec::new();
    let mut episodic_items = Vec::new();
    let mut diagnostics = Vec::new();
    let mut truncated = false;
    let mut remaining_chars = config.max_prompt_chars;
    let mut remaining_episodic_chars = config.max_prompt_chars;
    let mut duplicate_count = 0usize;
    let raw_item_count = mode_results
        .iter()
        .map(|mode_result| mode_result.items.len() + mode_result.episodic_items.len())
        .sum::<usize>();

    for mode_result in &mode_results {
        truncated |= mode_result.truncated;
        diagnostics.extend(mode_result.diagnostics.iter().cloned());
        if let Some(skipped_reason) = &mode_result.skipped_reason {
            diagnostics.push(format!(
                "memory.active_recall.mode_skipped:{}:{}",
                mode_result.mode.as_str(),
                skipped_reason
            ));
        }
        for item in &mode_result.items {
            if items.len() >= config.top_k_per_query as usize * config.max_queries {
                truncated = true;
                break;
            }
            let memory_id = item.memory_id.trim();
            if memory_id.is_empty() || !seen_ids.insert(memory_id.to_owned()) {
                duplicate_count += 1;
                continue;
            }
            if let Some(fingerprint) = memory_recall_item_rendered_line_fingerprint(item)
                && !seen_lines.insert(fingerprint)
            {
                duplicate_count += 1;
                continue;
            }
            let item_chars = item.content.chars().count();
            if item_chars > remaining_chars {
                truncated = true;
                break;
            }
            remaining_chars = remaining_chars.saturating_sub(item_chars);
            items.push(item.clone());
        }
        for item in &mode_result.episodic_items {
            if episodic_items.len() >= config.top_k_per_query as usize * config.max_queries {
                truncated = true;
                break;
            }
            let item_id = item.id.trim();
            if item_id.is_empty() || !seen_episodic_ids.insert(item_id.to_owned()) {
                duplicate_count += 1;
                continue;
            }
            if let Some(fingerprint) = rendered_line_fingerprint(item.content.as_str())
                && !seen_episodic_lines.insert(fingerprint)
            {
                duplicate_count += 1;
                continue;
            }
            let item_chars = item.content.chars().count();
            if item_chars > remaining_episodic_chars {
                truncated = true;
                break;
            }
            remaining_episodic_chars = remaining_episodic_chars.saturating_sub(item_chars);
            episodic_items.push(item.clone());
        }
    }

    if duplicate_count > 0 {
        diagnostics.push(format!(
            "memory.active_recall.executor_duplicates_suppressed:{duplicate_count}"
        ));
    }

    ActiveRecallExecutionResult {
        items,
        episodic_items,
        mode_results,
        diagnostics: normalize_active_recall_diagnostics(diagnostics),
        truncated,
        raw_item_count,
        duplicate_count,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct EpisodicFilteringResult {
    items: Vec<MemoryEpisodicRecallItem>,
    suppressed_count: usize,
    truncated: bool,
}

fn filter_rank_and_bound_episodic_items(
    items: Vec<MemoryEpisodicRecallItem>,
    mode: ActiveRecallMode,
    context: &MemoryTurnContext,
    top_k: usize,
    max_chars: usize,
) -> EpisodicFilteringResult {
    let mut visible = Vec::new();
    let mut suppressed_count = 0usize;
    for item in items {
        if !item.visibility.is_prompt_visible()
            || item.content.trim().is_empty()
            || item.provenance.workspace_id != context.workspace_id
        {
            suppressed_count += 1;
            continue;
        }
        visible.push(item);
    }
    visible.sort_by(|left, right| {
        episodic_rank_score(right, mode, context)
            .partial_cmp(&episodic_rank_score(left, mode, context))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .updated_at_unix
                    .unwrap_or_default()
                    .cmp(&left.updated_at_unix.unwrap_or_default())
            })
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut bounded = Vec::new();
    let mut remaining_chars = max_chars.max(1);
    let mut truncated = false;
    for (index, mut item) in visible.into_iter().enumerate() {
        if bounded.len() >= top_k.max(1) {
            truncated = true;
            suppressed_count += 1;
            continue;
        }
        item.content = compact_recall_content(item.content.as_str(), remaining_chars);
        let item_chars = item.content.chars().count();
        if item_chars > remaining_chars || item.content.trim().is_empty() {
            truncated = true;
            suppressed_count += 1 + usize::from(index < top_k);
            break;
        }
        remaining_chars = remaining_chars.saturating_sub(item_chars);
        bounded.push(item);
    }

    EpisodicFilteringResult {
        items: bounded,
        suppressed_count,
        truncated,
    }
}

fn episodic_rank_score(
    item: &MemoryEpisodicRecallItem,
    mode: ActiveRecallMode,
    context: &MemoryTurnContext,
) -> f32 {
    let mut score = item
        .score
        .or(item.provenance.retrieval_score)
        .unwrap_or(0.0);
    if item.provenance.thread_id.as_deref() == Some(context.thread_id.as_str()) {
        score += 0.2;
    }
    if item
        .provenance
        .task_id
        .as_deref()
        .zip(context.task_id.as_deref())
        .is_some_and(|(left, right)| left == right)
    {
        score += 0.2;
    }
    score += match mode {
        ActiveRecallMode::CurrentTask | ActiveRecallMode::TaskContext => 0.08,
        ActiveRecallMode::CurrentThread | ActiveRecallMode::ThreadEpisodic => 0.07,
        ActiveRecallMode::RelatedThread => 0.03,
        ActiveRecallMode::WorkspaceThread => 0.01,
        ActiveRecallMode::CompletedTask => 0.02,
        _ => 0.0,
    };
    if item.provenance.boundary == MemoryEpisodicRecallBoundary::Summary {
        score += 0.05;
    }
    score
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveRecallInputLengthBucket {
    Empty,
    VeryShort,
    Short,
    Substantial,
}

impl ActiveRecallInputLengthBucket {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::VeryShort => "very_short",
            Self::Short => "short",
            Self::Substantial => "substantial",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveRecallPlannerInput {
    pub(super) workspace_id: String,
    pub(super) thread_id: String,
    pub(super) turn_id: String,
    pub(super) task_id: Option<String>,
    pub(super) agent_id: Option<String>,
    pub(super) mode: ThreadMode,
    pub(super) input_text_preview: String,
    pub(super) input_text_char_count: usize,
    pub(super) input_length_bucket: ActiveRecallInputLengthBucket,
    pub(super) read_allowed: bool,
    pub(super) active_memory_allowed: bool,
    pub(super) explicit_no_memory: bool,
    pub(super) config_mode: MemoryActiveRecallMode,
    pub(super) deterministic_context_count: usize,
    pub(super) deterministic_context_chars: usize,
    pub(super) deterministic_memory_ids: Vec<String>,
    pub(super) deterministic_sufficient: bool,
    pub(super) deterministic_recall_empty: bool,
    pub(super) deterministic_categories: Vec<MemoryCategory>,
    pub(super) typed_targets: Vec<ActiveRecallTarget>,
    pub(super) has_workspace_context: bool,
    pub(super) has_task_context: bool,
    pub(super) episodic_capabilities: MemoryEpisodicRecallCapabilities,
    pub(super) thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
}

pub(super) fn active_recall_planner_input(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
) -> ActiveRecallPlannerInput {
    let input_text_char_count = input.input_text.chars().count();
    let deterministic_memory_ids = deterministic.memory_ids.iter().cloned().collect::<Vec<_>>();
    ActiveRecallPlannerInput {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        task_id: context.task_id.clone(),
        agent_id: context.agent_id.clone(),
        mode: context.mode,
        input_text_preview: truncate_chars(
            input.input_text.as_str(),
            config
                .planner
                .max_input_chars
                .min(ACTIVE_RECALL_INPUT_PREVIEW_MAX_CHARS)
                .max(1),
        ),
        input_text_char_count,
        input_length_bucket: active_recall_input_length_bucket(input_text_char_count),
        read_allowed: policy.allow_pre_turn_recall(),
        active_memory_allowed: policy.active_memory == MemoryActiveContextPolicy::Allow,
        explicit_no_memory: policy.reason_code == MemoryPolicyReasonCode::MemoryNoUse,
        config_mode: config.mode,
        deterministic_context_count: deterministic.context_count,
        deterministic_context_chars: deterministic.context_chars,
        deterministic_memory_ids,
        deterministic_sufficient: deterministic.sufficient,
        deterministic_recall_empty: deterministic.memory_ids.is_empty(),
        deterministic_categories: Vec::new(),
        typed_targets: Vec::new(),
        has_workspace_context: !context.workspace_id.trim().is_empty(),
        has_task_context: context
            .task_id
            .as_ref()
            .is_some_and(|task_id| !task_id.trim().is_empty()),
        episodic_capabilities,
        thread_episodic,
    }
}

pub fn active_recall_thread_episodic_summary(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    context: &MemoryTurnContext,
    capabilities: &MemoryEpisodicRecallCapabilities,
) -> MemoryActiveRecallThreadEpisodicSummary {
    let current_thread_id_present = !context.thread_id.trim().is_empty();
    let mut source_ids = BTreeSet::new();
    let mut prompt_context_source_count = 0usize;
    let mut prompt_context_chars = 0usize;
    for entry in prompt_context_set.entries() {
        if entry.contribution_id.as_str() != MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID {
            continue;
        }
        prompt_context_chars += entry.content.as_str().chars().count();
        for source_ref in &entry.source_refs {
            prompt_context_source_count += 1;
            if source_ref.id.as_str().starts_with("thread:") {
                source_ids.insert(source_ref.id.as_str().to_owned());
            }
        }
        source_ids.extend(rendered_thread_source_ids(entry.content.as_str()));
    }
    if prompt_context_source_count == 0 {
        prompt_context_source_count = source_ids.len();
    }

    let mut diagnostics = Vec::new();
    if current_thread_id_present && capabilities.current_thread_search {
        diagnostics.push("current_thread_recall_available".to_owned());
    }
    if capabilities.related_thread_search {
        diagnostics.push("related_thread_recall_available".to_owned());
    }
    if capabilities.workspace_thread_search {
        diagnostics.push("workspace_thread_recall_available".to_owned());
    }
    if prompt_context_source_count > 0 || !source_ids.is_empty() {
        diagnostics.push("thread_prompt_context_present".to_owned());
    }
    if !current_thread_id_present {
        diagnostics.push("thread_id_missing".to_owned());
    }
    if source_ids.len() > ACTIVE_RECALL_THREAD_SUMMARY_MAX_SOURCE_IDS {
        diagnostics.push("thread_source_ids_truncated".to_owned());
    }

    MemoryActiveRecallThreadEpisodicSummary {
        current_thread_id_present,
        current_thread_recall_available: current_thread_id_present
            && capabilities.current_thread_search,
        related_thread_recall_available: capabilities.related_thread_search,
        workspace_thread_recall_available: capabilities.workspace_thread_search,
        prompt_context_source_count,
        prompt_context_chars,
        source_ids: source_ids
            .into_iter()
            .take(ACTIVE_RECALL_THREAD_SUMMARY_MAX_SOURCE_IDS)
            .collect(),
        diagnostics: normalize_active_recall_diagnostics(
            diagnostics
                .into_iter()
                .take(ACTIVE_RECALL_THREAD_SUMMARY_MAX_DIAGNOSTICS)
                .collect(),
        ),
    }
}

pub(super) fn deterministic_active_recall_plan(
    input: &ActiveRecallPlannerInput,
) -> ActiveRecallPlan {
    if !input.read_allowed || !input.active_memory_allowed || input.explicit_no_memory {
        return ActiveRecallPlan::skip(
            ActiveMemoryDecisionReasonCode::PolicyDisabled,
            1.0,
            vec!["policy_disabled".to_owned()],
        );
    }

    match input.config_mode {
        MemoryActiveRecallMode::Disabled => {
            return ActiveRecallPlan::skip(
                ActiveMemoryDecisionReasonCode::ConfigDisabled,
                1.0,
                vec!["config_disabled".to_owned()],
            );
        }
        MemoryActiveRecallMode::DeterministicOnly => {
            return ActiveRecallPlan::skip(
                ActiveMemoryDecisionReasonCode::DeterministicOnly,
                1.0,
                vec!["deterministic_only".to_owned()],
            );
        }
        MemoryActiveRecallMode::StrictDebug => {
            return ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::StrictDebug,
                1.0,
                Vec::new(),
                Vec::new(),
                vec!["strict_debug".to_owned()],
            )
            .with_debug_fallback();
        }
        MemoryActiveRecallMode::Hybrid => {}
    }

    if input.deterministic_sufficient {
        return ActiveRecallPlan::skip(
            ActiveMemoryDecisionReasonCode::DeterministicSufficient,
            0.9,
            vec!["deterministic_sufficient".to_owned()],
        );
    }

    let exact_targets = input
        .typed_targets
        .iter()
        .filter(|target| target.canonical_key.is_some())
        .cloned()
        .collect::<Vec<_>>();
    if !exact_targets.is_empty() {
        return ActiveRecallPlan::run(
            ActiveMemoryDecisionReasonCode::MemoryLikely,
            0.95,
            vec![ActiveRecallMode::ExactCanonical],
            exact_targets,
            vec!["structured_exact_canonical_target".to_owned()],
        );
    }

    let mut modes = Vec::new();
    let mut diagnostics = Vec::new();
    if input.has_task_context && input.episodic_capabilities.current_task_context {
        modes.push(ActiveRecallMode::CurrentTask);
        modes.push(ActiveRecallMode::TaskContext);
        diagnostics.push("structured_task_context_available".to_owned());
    }
    if input.thread_episodic.current_thread_recall_available && input.deterministic_recall_empty {
        modes.push(ActiveRecallMode::CurrentThread);
        diagnostics.push("structured_thread_context_available".to_owned());
    }
    if input.has_workspace_context && input.deterministic_recall_empty {
        modes.push(ActiveRecallMode::Project);
        modes.push(ActiveRecallMode::Durable);
        diagnostics.push("structured_workspace_context_available".to_owned());
    }

    if modes.is_empty() {
        return ActiveRecallPlan::uncertain(
            ActiveMemoryDecisionReasonCode::ProviderUncertain,
            0.35,
            vec!["planner_uncertain".to_owned()],
        );
    }

    ActiveRecallPlan::run(
        ActiveMemoryDecisionReasonCode::MemoryLikely,
        0.65,
        modes,
        Vec::new(),
        diagnostics,
    )
}

fn active_recall_input_length_bucket(char_count: usize) -> ActiveRecallInputLengthBucket {
    match char_count {
        0 => ActiveRecallInputLengthBucket::Empty,
        1..=24 => ActiveRecallInputLengthBucket::VeryShort,
        25..=240 => ActiveRecallInputLengthBucket::Short,
        _ => ActiveRecallInputLengthBucket::Substantial,
    }
}

pub fn normalize_active_recall_plan(mut plan: ActiveRecallPlan) -> ActiveRecallPlan {
    plan.confidence = plan.confidence.clamp(0.0, 1.0);
    plan.modes = normalize_active_recall_modes(plan.modes);
    plan.targets = plan
        .targets
        .into_iter()
        .filter_map(ActiveRecallTarget::normalized)
        .take(ACTIVE_RECALL_MAX_TARGETS)
        .collect();
    plan.diagnostics = normalize_active_recall_diagnostics(plan.diagnostics);
    plan
}

pub fn active_recall_planned_query_count(decision: &ActiveRecallPlan) -> usize {
    if decision.status != ActiveMemoryDecisionStatus::Run {
        return 0;
    }

    if decision.debug_fallback {
        return decision.modes.len().max(1);
    }

    decision.modes.len()
}

pub(super) fn normalize_active_recall_plan_for_input(
    mut plan: ActiveRecallPlan,
    input: &ActiveRecallPlannerInput,
) -> ActiveRecallPlan {
    let mut diagnostics = std::mem::take(&mut plan.diagnostics);
    let original_modes = std::mem::take(&mut plan.modes);
    let mut modes = Vec::new();
    for mode in original_modes {
        let drop_reason = match mode {
            ActiveRecallMode::TaskContext | ActiveRecallMode::CurrentTask
                if !input.has_task_context =>
            {
                Some("dropped_mode=task_context:no_task_context")
            }
            ActiveRecallMode::TaskContext | ActiveRecallMode::CurrentTask
                if !input
                    .episodic_capabilities
                    .supports_source(MemoryEpisodicRecallSourceKind::CurrentTask) =>
            {
                Some("dropped_mode=task_context:capability_unavailable")
            }
            ActiveRecallMode::ThreadEpisodic
            | ActiveRecallMode::CurrentThread
            | ActiveRecallMode::RelatedThread
            | ActiveRecallMode::WorkspaceThread
                if input.thread_id.trim().is_empty() =>
            {
                Some("dropped_mode=thread_episodic:no_thread_context")
            }
            ActiveRecallMode::ThreadEpisodic | ActiveRecallMode::CurrentThread
                if !input
                    .episodic_capabilities
                    .supports_source(MemoryEpisodicRecallSourceKind::CurrentThread) =>
            {
                Some("dropped_mode=thread_episodic:capability_unavailable")
            }
            ActiveRecallMode::RelatedThread
                if !input
                    .episodic_capabilities
                    .supports_source(MemoryEpisodicRecallSourceKind::RelatedThread) =>
            {
                Some("dropped_mode=related_thread:capability_unavailable")
            }
            ActiveRecallMode::WorkspaceThread if !input.has_workspace_context => {
                Some("dropped_mode=workspace_thread:no_workspace_context")
            }
            ActiveRecallMode::WorkspaceThread
                if !input
                    .episodic_capabilities
                    .supports_source(MemoryEpisodicRecallSourceKind::WorkspaceThread) =>
            {
                Some("dropped_mode=workspace_thread:capability_unavailable")
            }
            ActiveRecallMode::CompletedTask
                if !input
                    .episodic_capabilities
                    .supports_source(MemoryEpisodicRecallSourceKind::CompletedTask) =>
            {
                Some("dropped_mode=completed_task:capability_unavailable")
            }
            ActiveRecallMode::ExactCanonical
                if !plan
                    .targets
                    .iter()
                    .any(|target| target.canonical_key.is_some()) =>
            {
                Some("dropped_mode=exact_canonical:no_canonical_target")
            }
            _ => None,
        };
        if let Some(reason) = drop_reason {
            diagnostics.push(reason.to_owned());
        } else {
            modes.push(mode);
        }
    }
    plan.modes = modes;
    plan.diagnostics = diagnostics;
    normalize_active_recall_plan(plan)
}

fn normalize_active_recall_modes(modes: Vec<ActiveRecallMode>) -> Vec<ActiveRecallMode> {
    let mut deduped = Vec::new();
    for mode in modes {
        if !deduped.contains(&mode) {
            deduped.push(mode);
        }
    }
    deduped.sort_by_key(|mode| mode.rank());
    deduped.truncate(ACTIVE_RECALL_MAX_MODES);
    deduped
}

fn normalize_active_recall_diagnostics(diagnostics: Vec<String>) -> Vec<String> {
    diagnostics
        .into_iter()
        .filter_map(|diagnostic| {
            bounded_nonempty_text(diagnostic.as_str(), ACTIVE_RECALL_MAX_DIAGNOSTIC_CHARS)
        })
        .take(ACTIVE_RECALL_MAX_DIAGNOSTICS)
        .collect()
}

pub(super) fn local_active_memory_decision(
    planner_input: &ActiveRecallPlannerInput,
    diagnostic: &str,
) -> ActiveMemoryDecision {
    let mut plan = deterministic_active_recall_plan(planner_input);
    if !diagnostic.trim().is_empty() {
        plan.diagnostics.push(diagnostic.to_owned());
        plan.diagnostics = normalize_active_recall_diagnostics(plan.diagnostics);
    }
    plan
}

pub(super) fn active_recall_mode_names(modes: &[ActiveRecallMode]) -> String {
    modes
        .iter()
        .map(|mode| mode.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn active_recall_available_mode_names(input: &ActiveRecallPlannerInput) -> Vec<String> {
    let mut modes = vec![
        ActiveRecallMode::Profile,
        ActiveRecallMode::Project,
        ActiveRecallMode::Durable,
    ];
    if input.episodic_capabilities.current_thread_search {
        modes.push(ActiveRecallMode::CurrentThread);
        modes.push(ActiveRecallMode::ThreadEpisodic);
    }
    if input.episodic_capabilities.related_thread_search {
        modes.push(ActiveRecallMode::RelatedThread);
    }
    if input.episodic_capabilities.workspace_thread_search {
        modes.push(ActiveRecallMode::WorkspaceThread);
    }
    if input.has_task_context && input.episodic_capabilities.current_task_context {
        modes.push(ActiveRecallMode::CurrentTask);
        modes.push(ActiveRecallMode::TaskContext);
    }
    if input.episodic_capabilities.completed_task_summary {
        modes.push(ActiveRecallMode::CompletedTask);
    }
    if input
        .typed_targets
        .iter()
        .any(|target| target.canonical_key.is_some())
    {
        modes.push(ActiveRecallMode::ExactCanonical);
    }
    let mut deduped = Vec::new();
    for mode in modes {
        if !deduped.contains(&mode) {
            deduped.push(mode);
        }
    }
    deduped.sort_by_key(|mode| mode.rank());
    deduped
        .into_iter()
        .map(|mode| mode.as_str().to_owned())
        .collect()
}

pub(super) fn active_recall_available_scoped_contexts(
    input: &ActiveRecallPlannerInput,
) -> Vec<String> {
    let mut contexts = Vec::new();
    if input.has_workspace_context {
        contexts.push("workspace".to_owned());
    }
    if input.has_task_context {
        contexts.push("task".to_owned());
    }
    if !input.thread_id.trim().is_empty() {
        contexts.push("thread".to_owned());
    }
    contexts.extend(input.episodic_capabilities.available_context_names());
    if !input
        .agent_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        contexts.push("agent".to_owned());
    }
    contexts
}

pub(super) async fn resolve_episodic_recall_capabilities(
    provider: Option<&Arc<dyn AgentEpisodicRecallProvider>>,
    context: &MemoryTurnContext,
) -> MemoryEpisodicRecallCapabilities {
    let Some(provider) = provider else {
        return MemoryEpisodicRecallCapabilities::default();
    };
    provider.recall_capabilities(context.clone()).await
}

pub(super) fn active_memory_decision_observability_diagnostic(
    decision: &ActiveMemoryDecision,
    deterministic: &DeterministicRecallContextSummary,
) -> HookDiagnostic {
    let selected_modes = active_recall_mode_names(decision.modes.as_slice());
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.decision",
        format!(
            "memory active recall decision: status={} reason={} confidence={:.2} deterministic_sufficient={} deterministic_contexts={} deterministic_chars={} modes={} targets={} provider_used={} provider_fallback_used={} debug_fallback={}",
            active_memory_decision_status_name(decision.status),
            decision.reason_code.as_str(),
            decision.confidence,
            deterministic.sufficient,
            deterministic.context_count,
            deterministic.context_chars,
            selected_modes,
            decision.targets.len(),
            decision.provider_used,
            decision.provider_fallback_used,
            decision.debug_fallback
        ),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("planner_status"),
        HookValue::Text(active_memory_decision_status_name(decision.status).to_owned()),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("planner_reason"),
        HookValue::Text(decision.reason_code.as_str().to_owned()),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("selected_modes"),
        HookValue::Text(selected_modes),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "target_count",
        decision.targets.len(),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("provider_used"),
        HookValue::Bool(decision.provider_used),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("provider_fallback_used"),
        HookValue::Bool(decision.provider_fallback_used),
    );
    if let Some(chars) = decision.provider_input_chars {
        insert_usize_metadata(&mut diagnostic.metadata, "provider_input_chars", chars);
    }
    if let Some(chars) = decision.provider_output_chars {
        insert_usize_metadata(&mut diagnostic.metadata, "provider_output_chars", chars);
    }
    diagnostic.metadata.insert(
        hook_metadata_key("debug_fallback"),
        HookValue::Bool(decision.debug_fallback),
    );
    diagnostic
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveRecallPlanJson {
    pub status: ActiveRecallPlanJsonStatus,
    pub reason_code: ActiveMemoryDecisionReasonCodeJson,
    pub confidence: f32,
    #[serde(default)]
    pub modes: Vec<ActiveRecallMode>,
    #[serde(default)]
    pub targets: Vec<ActiveRecallTarget>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveRecallPlanJsonStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveMemoryDecisionReasonCodeJson {
    PolicyDisabled,
    ConfigDisabled,
    DeterministicOnly,
    DeterministicSufficient,
    MemoryLikely,
    StrictDebug,
    ProviderRun,
    ProviderSkip,
    ProviderUncertain,
}

impl ActiveMemoryDecisionReasonCodeJson {
    fn is_provider_allowed(&self) -> bool {
        matches!(
            self,
            Self::MemoryLikely
                | Self::DeterministicSufficient
                | Self::ProviderRun
                | Self::ProviderSkip
                | Self::ProviderUncertain
        )
    }

    fn into_reason_code(self) -> ActiveMemoryDecisionReasonCode {
        match self {
            Self::PolicyDisabled => ActiveMemoryDecisionReasonCode::PolicyDisabled,
            Self::ConfigDisabled => ActiveMemoryDecisionReasonCode::ConfigDisabled,
            Self::DeterministicOnly => ActiveMemoryDecisionReasonCode::DeterministicOnly,
            Self::DeterministicSufficient => {
                ActiveMemoryDecisionReasonCode::DeterministicSufficient
            }
            Self::MemoryLikely => ActiveMemoryDecisionReasonCode::MemoryLikely,
            Self::StrictDebug => ActiveMemoryDecisionReasonCode::StrictDebug,
            Self::ProviderRun => ActiveMemoryDecisionReasonCode::ProviderRun,
            Self::ProviderSkip => ActiveMemoryDecisionReasonCode::ProviderSkip,
            Self::ProviderUncertain => ActiveMemoryDecisionReasonCode::ProviderUncertain,
        }
    }
}

pub fn parse_active_memory_decision_json(
    raw: &str,
) -> Result<ActiveMemoryDecision, serde_json::Error> {
    let parsed = serde_json::from_str::<ActiveRecallPlanJson>(raw.trim())?;
    if parsed
        .targets
        .iter()
        .any(ActiveRecallTarget::has_unknown_fact_class)
    {
        return Err(serde_json::Error::custom(
            "unknown active recall fact_class",
        ));
    }
    let status = match parsed.status {
        ActiveRecallPlanJsonStatus::Skip => ActiveMemoryDecisionStatus::Skip,
        ActiveRecallPlanJsonStatus::Run => ActiveMemoryDecisionStatus::Run,
        ActiveRecallPlanJsonStatus::Uncertain => ActiveMemoryDecisionStatus::Uncertain,
    };
    if !parsed.reason_code.is_provider_allowed() {
        return Err(serde_json::Error::custom(
            "active recall provider reasonCode is not allowed",
        ));
    }
    if status == ActiveMemoryDecisionStatus::Run && parsed.modes.is_empty() {
        return Err(serde_json::Error::custom(
            "active recall run plan requires at least one mode",
        ));
    }
    if status != ActiveMemoryDecisionStatus::Run && !parsed.modes.is_empty() {
        return Err(serde_json::Error::custom(
            "active recall non-run plan must not include modes",
        ));
    }
    let plan = normalize_active_recall_plan(ActiveRecallPlan {
        status,
        reason_code: parsed.reason_code.into_reason_code(),
        confidence: parsed.confidence.clamp(0.0, 1.0),
        modes: parsed.modes,
        targets: parsed.targets,
        debug_fallback: false,
        provider_used: true,
        provider_fallback_used: false,
        provider_input_chars: None,
        provider_output_chars: None,
        diagnostics: parsed.diagnostics,
    });
    Ok(plan)
}

pub(super) fn active_memory_dedup_observability_diagnostic(
    deterministic: &DeterministicRecallContextSummary,
    dedup: &ActiveRecallDedupResult,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.dedup",
        format!(
            "memory active recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} duplicate_only={}",
            deterministic.memory_ids.len(),
            dedup.raw_count,
            dedup.duplicate_count(),
            dedup.rendered_count(),
            dedup.duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        deterministic.memory_ids.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        dedup.raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        dedup.duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        dedup.duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        dedup.rendered_count(),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(dedup.duplicate_only()),
    );
    diagnostic
}

pub(super) fn memory_prompt_recall_dedup_diagnostic(
    context: &MemoryRecallPromptContext,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.prompt_recall.dedup",
        format!(
            "memory prompt recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} active_synthesis_rendered={} active_duplicate_only={}",
            context.deterministic_memory_count,
            context.active_raw_count,
            context.active_duplicate_count(),
            context.active_rendered_count,
            context.active_synthesis_rendered,
            context.active_duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        context.deterministic_memory_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        context.active_raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        context.active_duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        context.active_duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        context.active_rendered_count,
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_synthesis_rendered"),
        HookValue::Bool(context.active_synthesis_rendered),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(context.active_duplicate_only()),
    );
    diagnostic
}

pub(super) fn active_memory_decision_status_name(
    status: ActiveMemoryDecisionStatus,
) -> &'static str {
    match status {
        ActiveMemoryDecisionStatus::Skip => "skip",
        ActiveMemoryDecisionStatus::Run => "run",
        ActiveMemoryDecisionStatus::Uncertain => "uncertain",
    }
}

fn recall_debug_audit(
    event_kind: &'static str,
    details: BTreeMap<HookMetadataKey, HookValue>,
) -> HookContribution {
    HookContribution::Audit(AuditContribution {
        event_kind: HookAuditEventKind::new(event_kind).expect("static event kind is valid"),
        details: HookValue::Object(details),
        safe_for_user: false,
    })
}

fn hook_value_object<const N: usize>(entries: [(&'static str, HookValue); N]) -> HookValue {
    HookValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (metadata_key(key), value))
            .collect(),
    )
}

fn hook_value_string_list(values: Vec<String>) -> HookValue {
    HookValue::List(values.into_iter().map(HookValue::Text).collect())
}

fn hook_value_suppression_counts<const N: usize>(entries: [(&'static str, usize); N]) -> HookValue {
    HookValue::Object(
        entries
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(key, count)| {
                (
                    metadata_key(key),
                    HookValue::I64(i64::try_from(count).unwrap_or(i64::MAX)),
                )
            })
            .collect(),
    )
}

fn hook_value_source_boundary_counts(result: &ActiveRecallExecutionResult) -> HookValue {
    let mut durable = 0usize;
    let mut current_thread = 0usize;
    let mut related_thread = 0usize;
    let mut workspace_thread = 0usize;
    let mut current_task = 0usize;
    let mut completed_task = 0usize;
    for mode_result in &result.mode_results {
        durable += mode_result.items.len();
        for item in &mode_result.episodic_items {
            match item.provenance.source {
                MemoryEpisodicRecallSourceKind::CurrentThread
                | MemoryEpisodicRecallSourceKind::TranscriptSummary => current_thread += 1,
                MemoryEpisodicRecallSourceKind::RelatedThread => related_thread += 1,
                MemoryEpisodicRecallSourceKind::WorkspaceThread => workspace_thread += 1,
                MemoryEpisodicRecallSourceKind::CurrentTask => current_task += 1,
                MemoryEpisodicRecallSourceKind::CompletedTask => completed_task += 1,
            }
        }
    }
    hook_value_suppression_counts([
        ("durable_memory", durable),
        ("current_thread", current_thread),
        ("related_thread", related_thread),
        ("workspace_thread", workspace_thread),
        ("current_task", current_task),
        ("completed_task", completed_task),
    ])
}

fn metadata_key(key: &str) -> HookMetadataKey {
    HookMetadataKey::new(key).expect("static memory debug metadata key is valid")
}

fn diagnostic_count(result: &ActiveRecallExecutionResult, needle: &str) -> usize {
    result
        .diagnostics
        .iter()
        .find_map(|diagnostic| {
            let (_, value) = diagnostic.rsplit_once(':')?;
            diagnostic
                .contains(needle)
                .then(|| value.parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveRecallBridgeQuery {
    pub(super) query: String,
    pub(super) categories: Vec<MemoryCategory>,
}

pub(super) fn active_memory_query_plan(
    input_text: &str,
    decision: &ActiveMemoryDecision,
    config: &MemoryActiveRecallConfig,
) -> Vec<ActiveRecallBridgeQuery> {
    let mut seen = BTreeSet::new();
    let mut queries = Vec::new();

    if decision.debug_fallback {
        for query in [
            bounded_active_recall_bridge_query(MEMORY_ACTIVE_RECALL_GENERIC_QUERY, Vec::new()),
            bounded_active_recall_bridge_query(input_text, Vec::new()),
        ]
        .into_iter()
        .flatten()
        {
            push_active_recall_bridge_query(&mut queries, &mut seen, query, config.max_queries);
            if queries.len() >= config.max_queries {
                return queries;
            }
        }
    }

    queries
}

fn bounded_active_recall_bridge_query(
    query: &str,
    categories: Vec<MemoryCategory>,
) -> Option<ActiveRecallBridgeQuery> {
    Some(ActiveRecallBridgeQuery {
        query: bounded_nonempty_text(query, 500)?,
        categories: dedup_memory_categories(categories),
    })
}

fn push_active_recall_bridge_query(
    queries: &mut Vec<ActiveRecallBridgeQuery>,
    seen: &mut BTreeSet<String>,
    query: ActiveRecallBridgeQuery,
    max_queries: usize,
) {
    if queries.len() >= max_queries {
        return;
    }
    let key = format!(
        "{}|{}",
        query.query.to_lowercase(),
        query
            .categories
            .iter()
            .map(|category| memory_category_label(*category))
            .collect::<Vec<_>>()
            .join(",")
    );
    if seen.insert(key) {
        queries.push(query);
    }
}

fn dedup_memory_categories(categories: Vec<MemoryCategory>) -> Vec<MemoryCategory> {
    let mut deduped = Vec::new();
    for category in categories {
        if !deduped.contains(&category) {
            deduped.push(category);
        }
    }
    deduped
}

fn memory_category_label(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct ActiveRecallDedupResult {
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) raw_count: usize,
    pub(super) duplicate_id_count: usize,
    pub(super) duplicate_line_count: usize,
    pub(super) duplicate_ids: Vec<String>,
}

impl ActiveRecallDedupResult {
    pub(super) fn rendered_count(&self) -> usize {
        self.items.len()
    }

    pub(super) fn duplicate_count(&self) -> usize {
        self.duplicate_id_count + self.duplicate_line_count
    }

    pub(super) fn duplicate_only(&self) -> bool {
        self.raw_count > 0 && self.rendered_count() == 0 && self.duplicate_count() > 0
    }
}

pub(super) fn dedup_active_recall_items_with_lines(
    items: Vec<MemoryRecallItem>,
    deterministic_ids: &BTreeSet<String>,
    deterministic_line_fingerprints: &BTreeSet<String>,
) -> ActiveRecallDedupResult {
    let mut seen = deterministic_ids.clone();
    let mut seen_lines = deterministic_line_fingerprints.clone();
    let mut result = ActiveRecallDedupResult {
        raw_count: items.len(),
        ..ActiveRecallDedupResult::default()
    };
    let mut deduped = Vec::new();
    for item in items {
        let memory_id = item.memory_id.trim();
        if memory_id.is_empty() || !seen.insert(memory_id.to_owned()) {
            result.duplicate_id_count += 1;
            if !memory_id.is_empty() {
                result.duplicate_ids.push(memory_id.to_owned());
            }
            continue;
        }
        if let Some(fingerprint) = memory_recall_item_rendered_line_fingerprint(&item)
            && !seen_lines.insert(fingerprint)
        {
            result.duplicate_line_count += 1;
            result.duplicate_ids.push(memory_id.to_owned());
            continue;
        }
        deduped.push(item);
    }
    result.items = deduped;
    result
}

pub(super) fn memory_recall_item_rendered_line_fingerprint(
    item: &MemoryRecallItem,
) -> Option<String> {
    let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
        source: MemoryRecallSynthesisSource::Deterministic,
        items: vec![item.clone()],
        deterministic_memory_ids: BTreeSet::new(),
        deterministic_line_fingerprints: BTreeSet::new(),
        input_text_preview: None,
        budget: MemoryRecallSynthesisBudget::default(),
    });
    rendered_line_fingerprint(synthesis.rendered_text().as_str())
}

pub(super) fn rendered_line_fingerprints(content: &str) -> BTreeSet<String> {
    content
        .lines()
        .filter_map(rendered_line_fingerprint)
        .collect()
}

pub(super) fn active_memory_context_lines(content: &str) -> Vec<&str> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "Active memory context:")
        .collect()
}

pub(super) fn rendered_memory_line_id(line: &str) -> Option<String> {
    let line = line.trim();
    if let Some(metadata) = line.strip_prefix("- [") {
        let end = metadata
            .char_indices()
            .find_map(|(index, ch)| (ch == ',' || ch == ']').then_some(index))?;
        let memory_id = metadata[..end].trim();
        if !memory_id.is_empty() {
            return Some(
                memory_id
                    .strip_prefix("memory:")
                    .unwrap_or(memory_id)
                    .to_owned(),
            );
        }
    }

    let mut remaining = line;
    while let Some(index) = remaining.find("memory:") {
        let candidate = &remaining[index + "memory:".len()..];
        let memory_id = candidate
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(*ch, '_' | '-' | '.'))
            .collect::<String>();
        if !memory_id.is_empty() {
            return Some(memory_id);
        }
        remaining = candidate;
    }
    None
}

fn rendered_thread_source_ids(content: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for line in content.lines() {
        let mut remaining = line;
        while let Some(index) = remaining.find("thread:") {
            let candidate = &remaining[index..];
            let source_id = candidate
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(*ch, ':' | '_' | '-' | '/'))
                .collect::<String>();
            if source_id.len() > "thread:".len() && source_id.matches('/').count() >= 2 {
                ids.insert(source_id.clone());
            }
            remaining = &candidate[source_id.len()..];
        }
    }
    ids
}

pub(super) fn rendered_line_fingerprint(line: &str) -> Option<String> {
    let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
pub(super) fn memory_active_recall_prompt_context_contribution(
    items: Vec<MemoryRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> Option<PromptContextContribution> {
    memory_active_recall_prompt_context_contribution_with_synthesis(
        items,
        snapshot_truncated,
        BTreeSet::new(),
        BTreeSet::new(),
        config,
    )
    .contribution
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ActiveMemoryRecallPromptContextContributionResult {
    pub(super) contribution: Option<PromptContextContribution>,
    pub(super) synthesis: MemoryActiveSynthesisOutput,
}

#[cfg(test)]
pub(super) fn memory_active_recall_prompt_context_contribution_with_synthesis(
    items: Vec<MemoryRecallItem>,
    snapshot_truncated: bool,
    deterministic_memory_ids: BTreeSet<String>,
    deterministic_line_fingerprints: BTreeSet<String>,
    config: &MemoryActiveRecallConfig,
) -> ActiveMemoryRecallPromptContextContributionResult {
    let active_dedup = dedup_active_recall_items_with_lines(
        items,
        &deterministic_memory_ids,
        &deterministic_line_fingerprints,
    );
    memory_active_recall_multi_source_prompt_context_contribution_with_synthesis(
        active_dedup.items,
        Vec::new(),
        snapshot_truncated,
        config,
    )
}

pub(super) fn memory_active_recall_multi_source_prompt_context_contribution_with_synthesis(
    items: Vec<MemoryRecallItem>,
    thread_items: Vec<MemoryEpisodicRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> ActiveMemoryRecallPromptContextContributionResult {
    let synthesis = synthesize_active_memory_context(ordered_active_synthesis_input(
        items,
        thread_items,
        Vec::new(),
        MemoryRecallSynthesisBudget::for_active_config(config),
    ));
    let content = synthesis.rendered_text();
    let Some(content) = HookPromptContent::new(content).ok() else {
        return ActiveMemoryRecallPromptContextContributionResult {
            contribution: None,
            synthesis,
        };
    };
    let contribution = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 490,
        content,
        max_chars: Some(config.max_prompt_chars),
        source_refs: active_synthesis_source_refs(&synthesis),
        diagnostics: hook_diagnostics_from_strings(synthesis.diagnostics.as_slice()),
        truncated: snapshot_truncated || synthesis.truncated,
    };
    ActiveMemoryRecallPromptContextContributionResult {
        contribution: Some(contribution),
        synthesis,
    }
}

pub(super) fn memory_episodic_recall_prompt_context_contributions(
    items: Vec<MemoryEpisodicRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> Vec<PromptContextContribution> {
    let mut current_thread_items = Vec::new();
    let mut related_thread_items = Vec::new();
    let mut workspace_thread_items = Vec::new();
    let mut task_items = Vec::new();
    for item in items {
        match item.provenance.source {
            MemoryEpisodicRecallSourceKind::CurrentThread
            | MemoryEpisodicRecallSourceKind::TranscriptSummary => current_thread_items.push(item),
            MemoryEpisodicRecallSourceKind::RelatedThread => related_thread_items.push(item),
            MemoryEpisodicRecallSourceKind::WorkspaceThread => workspace_thread_items.push(item),
            MemoryEpisodicRecallSourceKind::CurrentTask
            | MemoryEpisodicRecallSourceKind::CompletedTask => task_items.push(item),
        }
    }
    [
        episodic_prompt_context_contribution(
            MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID,
            "current_thread_context",
            480,
            current_thread_items,
            snapshot_truncated,
            config.max_prompt_chars,
        ),
        episodic_prompt_context_contribution(
            MEMORY_RELATED_THREAD_CONTEXT_CONTRIBUTION_ID,
            "related_thread_context",
            475,
            related_thread_items,
            snapshot_truncated,
            config.max_prompt_chars,
        ),
        episodic_prompt_context_contribution(
            MEMORY_WORKSPACE_THREAD_CONTEXT_CONTRIBUTION_ID,
            "workspace_thread_context",
            470,
            workspace_thread_items,
            snapshot_truncated,
            config.max_prompt_chars,
        ),
        episodic_prompt_context_contribution(
            MEMORY_TASK_CONTEXT_CONTRIBUTION_ID,
            "task_context",
            470,
            task_items,
            snapshot_truncated,
            config.max_prompt_chars,
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn episodic_prompt_context_contribution(
    contribution_id: &'static str,
    domain: &'static str,
    priority: i32,
    items: Vec<MemoryEpisodicRecallItem>,
    snapshot_truncated: bool,
    max_chars: usize,
) -> Option<PromptContextContribution> {
    let mut lines = Vec::new();
    let mut source_refs = Vec::new();
    let mut seen_lines = BTreeSet::new();
    let mut remaining_chars = max_chars.max(1);
    let mut truncated = snapshot_truncated;

    for item in items {
        let Some(line) = episodic_prompt_line(&item, remaining_chars) else {
            continue;
        };
        let Some(fingerprint) = rendered_line_fingerprint(line.as_str()) else {
            continue;
        };
        if !seen_lines.insert(fingerprint) {
            continue;
        }
        let line_chars = line.chars().count();
        let separator_chars = usize::from(!lines.is_empty());
        if separator_chars + line_chars > remaining_chars {
            truncated = true;
            break;
        }
        remaining_chars = remaining_chars.saturating_sub(separator_chars + line_chars);
        if let Some(source_ref) = episodic_source_ref(&item) {
            source_refs.push(source_ref);
        }
        lines.push(line);
    }

    let content = lines.join("\n");
    let Some(content) = HookPromptContent::new(content).ok() else {
        return None;
    };
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(contribution_id)
            .expect("static contribution id is valid"),
        domain: HookDomain::new(domain).expect("static contribution domain is valid"),
        priority,
        content,
        max_chars: Some(max_chars),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
    })
}

fn episodic_prompt_line(item: &MemoryEpisodicRecallItem, max_chars: usize) -> Option<String> {
    let content = bounded_nonempty_text(item.content.as_str(), max_chars)?;
    let source = match item.provenance.source {
        MemoryEpisodicRecallSourceKind::CurrentThread => "current thread",
        MemoryEpisodicRecallSourceKind::RelatedThread => "related thread",
        MemoryEpisodicRecallSourceKind::WorkspaceThread => "workspace thread",
        MemoryEpisodicRecallSourceKind::TranscriptSummary => "thread summary",
        MemoryEpisodicRecallSourceKind::CurrentTask => "current task",
        MemoryEpisodicRecallSourceKind::CompletedTask => "completed task",
    };
    let boundary = item.provenance.boundary.as_str().replace('_', " ");
    let id = item.id.trim();
    if id.starts_with("thread:")
        && matches!(
            item.provenance.source,
            MemoryEpisodicRecallSourceKind::CurrentThread
                | MemoryEpisodicRecallSourceKind::RelatedThread
                | MemoryEpisodicRecallSourceKind::WorkspaceThread
                | MemoryEpisodicRecallSourceKind::TranscriptSummary
        )
    {
        return Some(format!(
            "- [{id}, source={source}, boundary={boundary}]: {content}"
        ));
    }
    Some(format!("- {source} {boundary}: {content}"))
}

fn episodic_source_ref(item: &MemoryEpisodicRecallItem) -> Option<HookSourceRef> {
    let id = item.id.trim();
    if id.is_empty() {
        return None;
    }
    let kind = match item.provenance.source {
        MemoryEpisodicRecallSourceKind::CurrentThread
        | MemoryEpisodicRecallSourceKind::TranscriptSummary => "current_thread_context",
        MemoryEpisodicRecallSourceKind::RelatedThread => "related_thread_context",
        MemoryEpisodicRecallSourceKind::WorkspaceThread => "workspace_thread_context",
        MemoryEpisodicRecallSourceKind::CurrentTask
        | MemoryEpisodicRecallSourceKind::CompletedTask => "task_context",
    };
    Some(HookSourceRef {
        kind: HookSourceKind::Custom(kind.to_owned()),
        id: HookSourceId::new(id.to_owned()).ok()?,
        label: None,
    })
}

pub(super) fn bounded_nonempty_text(value: &str, max_chars: usize) -> Option<String> {
    let trimmed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_chars(trimmed.as_str(), max_chars))
    }
}

pub(super) fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
