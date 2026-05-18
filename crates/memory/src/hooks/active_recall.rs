use super::*;
use serde::de::Error as _;

const ACTIVE_RECALL_MAX_MODES: usize = 4;
const ACTIVE_RECALL_MAX_TARGETS: usize = 6;
const ACTIVE_RECALL_MAX_DIAGNOSTICS: usize = 6;
const ACTIVE_RECALL_MAX_DIAGNOSTIC_CHARS: usize = 160;
const ACTIVE_RECALL_MAX_CANONICAL_KEY_CHARS: usize = 240;
pub(super) const ACTIVE_RECALL_INPUT_PREVIEW_MAX_CHARS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveMemoryDecisionStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveMemoryDecisionReasonCode {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ActiveRecallMode {
    Profile,
    Project,
    Durable,
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
            Self::TaskContext => 3,
            Self::ThreadEpisodic => 4,
            Self::Durable => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ActiveRecallTarget {
    #[serde(default)]
    pub(super) scope_kind: Option<MemoryScopeKind>,
    #[serde(default)]
    pub(super) fact_class: Option<MemoryFactClass>,
    #[serde(default)]
    pub(super) category: Option<MemoryCategory>,
    #[serde(default)]
    pub(super) subject: Option<MemorySubject>,
    #[serde(default)]
    pub(super) attribute: Option<MemoryAttribute>,
    #[serde(default)]
    pub(super) canonical_key: Option<String>,
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

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ActiveRecallPlan {
    pub(super) status: ActiveMemoryDecisionStatus,
    pub(super) reason_code: ActiveMemoryDecisionReasonCode,
    pub(super) confidence: f32,
    pub(super) modes: Vec<ActiveRecallMode>,
    pub(super) targets: Vec<ActiveRecallTarget>,
    pub(super) debug_fallback: bool,
    pub(super) provider_used: bool,
    pub(super) diagnostics: Vec<String>,
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
            diagnostics: normalize_active_recall_diagnostics(diagnostics),
        }
    }

    pub(super) fn with_debug_fallback(mut self) -> Self {
        self.debug_fallback = true;
        self
    }
}

pub(super) type ActiveMemoryDecision = ActiveRecallPlan;

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
}

pub(super) fn active_recall_planner_input(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
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
            ACTIVE_RECALL_INPUT_PREVIEW_MAX_CHARS,
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
    if input.has_task_context {
        modes.push(ActiveRecallMode::TaskContext);
        diagnostics.push("structured_task_context_available".to_owned());
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

pub(super) fn normalize_active_recall_plan(mut plan: ActiveRecallPlan) -> ActiveRecallPlan {
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

pub(super) fn normalize_active_recall_plan_for_input(
    mut plan: ActiveRecallPlan,
    input: &ActiveRecallPlannerInput,
) -> ActiveRecallPlan {
    let mut diagnostics = std::mem::take(&mut plan.diagnostics);
    let original_modes = std::mem::take(&mut plan.modes);
    let mut modes = Vec::new();
    for mode in original_modes {
        let drop_reason = match mode {
            ActiveRecallMode::TaskContext if !input.has_task_context => {
                Some("dropped_mode=task_context:no_task_context")
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

pub(super) fn active_memory_decision_observability_diagnostic(
    decision: &ActiveMemoryDecision,
    deterministic: &DeterministicRecallContextSummary,
) -> HookDiagnostic {
    let selected_modes = active_recall_mode_names(decision.modes.as_slice());
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.decision",
        format!(
            "memory active recall decision: status={} reason={} confidence={:.2} deterministic_sufficient={} deterministic_contexts={} deterministic_chars={} modes={} targets={} provider_used={} debug_fallback={}",
            active_memory_decision_status_name(decision.status),
            decision.reason_code.as_str(),
            decision.confidence,
            deterministic.sufficient,
            deterministic.context_count,
            deterministic.context_chars,
            selected_modes,
            decision.targets.len(),
            decision.provider_used,
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
        hook_metadata_key("debug_fallback"),
        HookValue::Bool(decision.debug_fallback),
    );
    diagnostic
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ActiveRecallPlanJson {
    status: ActiveRecallPlanJsonStatus,
    #[serde(default)]
    reason_code: Option<ActiveMemoryDecisionReasonCodeJson>,
    confidence: f32,
    modes: Vec<ActiveRecallMode>,
    #[serde(default)]
    targets: Vec<ActiveRecallTarget>,
    #[serde(default)]
    debug_fallback: bool,
    #[serde(default)]
    diagnostics: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ActiveRecallPlanJsonStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ActiveMemoryDecisionReasonCodeJson {
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

pub(super) fn parse_active_memory_decision_json(
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
    let default_reason_code = match status {
        ActiveMemoryDecisionStatus::Skip => ActiveMemoryDecisionReasonCode::ProviderSkip,
        ActiveMemoryDecisionStatus::Run => ActiveMemoryDecisionReasonCode::ProviderRun,
        ActiveMemoryDecisionStatus::Uncertain => ActiveMemoryDecisionReasonCode::ProviderUncertain,
    };
    let plan = normalize_active_recall_plan(ActiveRecallPlan {
        status,
        reason_code: parsed
            .reason_code
            .map(ActiveMemoryDecisionReasonCodeJson::into_reason_code)
            .unwrap_or(default_reason_code),
        confidence: parsed.confidence.clamp(0.0, 1.0),
        modes: parsed.modes,
        targets: parsed.targets,
        debug_fallback: parsed.debug_fallback,
        provider_used: true,
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

    for mode in &decision.modes {
        for query in active_recall_bridge_queries_for_mode(*mode, decision.targets.as_slice()) {
            push_active_recall_bridge_query(&mut queries, &mut seen, query, config.max_queries);
            if queries.len() >= config.max_queries {
                return queries;
            }
        }
    }

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

fn active_recall_bridge_queries_for_mode(
    mode: ActiveRecallMode,
    targets: &[ActiveRecallTarget],
) -> Vec<ActiveRecallBridgeQuery> {
    match mode {
        ActiveRecallMode::Profile => vec![ActiveRecallBridgeQuery {
            query: "active recall profile identity preferences communication style".to_owned(),
            categories: vec![
                MemoryCategory::Identity,
                MemoryCategory::Preference,
                MemoryCategory::Biography,
                MemoryCategory::Relationship,
                MemoryCategory::CommunicationStyle,
                MemoryCategory::RecurringInstruction,
            ],
        }],
        ActiveRecallMode::Project => vec![ActiveRecallBridgeQuery {
            query: "active recall workspace project decisions policies constraints procedures"
                .to_owned(),
            categories: vec![
                MemoryCategory::ProjectDecision,
                MemoryCategory::ProjectPolicy,
                MemoryCategory::ProjectFact,
                MemoryCategory::Procedure,
                MemoryCategory::Constraint,
            ],
        }],
        ActiveRecallMode::Durable => vec![ActiveRecallBridgeQuery {
            query: "active recall durable memories in current scope".to_owned(),
            categories: Vec::new(),
        }],
        ActiveRecallMode::ThreadEpisodic => vec![ActiveRecallBridgeQuery {
            query: "active recall thread episodic context".to_owned(),
            categories: vec![MemoryCategory::Todo, MemoryCategory::Constraint],
        }],
        ActiveRecallMode::TaskContext => vec![ActiveRecallBridgeQuery {
            query: "active recall task context runtime state".to_owned(),
            categories: vec![MemoryCategory::Todo, MemoryCategory::Procedure],
        }],
        ActiveRecallMode::ExactCanonical => targets
            .iter()
            .filter_map(active_recall_target_bridge_query)
            .collect(),
    }
}

fn active_recall_target_bridge_query(
    target: &ActiveRecallTarget,
) -> Option<ActiveRecallBridgeQuery> {
    if let Some(canonical_key) = &target.canonical_key {
        return bounded_active_recall_bridge_query(
            canonical_key.as_str(),
            target_categories(target),
        );
    }

    let mut parts = Vec::new();
    if let Some(scope_kind) = target.scope_kind {
        parts.push(format!("scope={}", memory_scope_kind_label(scope_kind)));
    }
    if let Some(fact_class) = target.fact_class {
        parts.push(format!(
            "fact_class={}",
            memory_fact_class_label(fact_class)
        ));
    }
    if let Some(category) = target.category {
        parts.push(format!("category={}", memory_category_label(category)));
    }
    if let Some(subject) = target.subject {
        parts.push(format!("subject={}", memory_subject_label(subject)));
    }
    if let Some(attribute) = target.attribute {
        parts.push(format!("attribute={}", memory_attribute_label(attribute)));
    }
    if parts.is_empty() {
        return None;
    }
    bounded_active_recall_bridge_query(
        format!("active recall exact canonical {}", parts.join(" ")).as_str(),
        target_categories(target),
    )
}

fn target_categories(target: &ActiveRecallTarget) -> Vec<MemoryCategory> {
    target.category.into_iter().collect()
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

fn memory_scope_kind_label(scope_kind: MemoryScopeKind) -> &'static str {
    match scope_kind {
        MemoryScopeKind::User => "user",
        MemoryScopeKind::Workspace => "workspace",
        MemoryScopeKind::Thread => "thread",
        MemoryScopeKind::Agent => "agent",
        MemoryScopeKind::Task => "task",
    }
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

fn memory_fact_class_label(fact_class: MemoryFactClass) -> &'static str {
    match fact_class {
        MemoryFactClass::UserIdentity => "user_identity",
        MemoryFactClass::UserBiography => "user_biography",
        MemoryFactClass::UserRelationship => "user_relationship",
        MemoryFactClass::StableUserPreference => "stable_user_preference",
        MemoryFactClass::CommunicationPreference => "communication_preference",
        MemoryFactClass::RecurringUserInstruction => "recurring_user_instruction",
        MemoryFactClass::ProjectPolicy => "project_policy",
        MemoryFactClass::ProjectDecision => "project_decision",
        MemoryFactClass::ProjectProcedure => "project_procedure",
        MemoryFactClass::ProjectConstraint => "project_constraint",
        MemoryFactClass::TaskLifecycleState => "task_lifecycle_state",
        MemoryFactClass::OperationalObservation => "operational_observation",
        MemoryFactClass::ThreadLocalState => "thread_local_state",
        MemoryFactClass::ToolResultFact => "tool_result_fact",
        MemoryFactClass::AssistantSelfDescription => "assistant_self_description",
        MemoryFactClass::GeneratedSummaryFact => "generated_summary_fact",
        MemoryFactClass::DomainOwnedState => "domain_owned_state",
        MemoryFactClass::SecretOrCredential => "secret_or_credential",
        MemoryFactClass::RegulatedSensitiveFact => "regulated_sensitive_fact",
        MemoryFactClass::Unknown => "unknown",
    }
}

fn memory_subject_label(subject: MemorySubject) -> &'static str {
    match subject {
        MemorySubject::CurrentUser => "current_user",
        MemorySubject::CurrentAgent => "current_agent",
        MemorySubject::Workspace => "workspace",
        MemorySubject::Project => "project",
        MemorySubject::Person => "person",
        MemorySubject::Organization => "organization",
        MemorySubject::Artifact => "artifact",
        MemorySubject::Custom => "custom",
    }
}

fn memory_attribute_label(attribute: MemoryAttribute) -> &'static str {
    match attribute {
        MemoryAttribute::Name => "name",
        MemoryAttribute::Birthday => "birthday",
        MemoryAttribute::PreferredLanguage => "preferred_language",
        MemoryAttribute::CommunicationStyle => "communication_style",
        MemoryAttribute::MigrationPolicy => "migration_policy",
        MemoryAttribute::ReviewStyle => "review_style",
        MemoryAttribute::PhaseNaming => "phase_naming",
        MemoryAttribute::Custom => "custom",
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct ActiveRecallDedupResult {
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) raw_count: usize,
    pub(super) duplicate_id_count: usize,
    pub(super) duplicate_line_count: usize,
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
            continue;
        }
        if let Some(fingerprint) = memory_recall_item_rendered_line_fingerprint(&item)
            && !seen_lines.insert(fingerprint)
        {
            result.duplicate_line_count += 1;
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
    let prompt_item = memory_recall_prompt_item(item.clone());
    let (line, _) = render_memory_recall_context_block(&[prompt_item], false);
    rendered_line_fingerprint(line.as_str())
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
    let metadata = line.strip_prefix("- [")?;
    let end = metadata
        .char_indices()
        .find_map(|(index, ch)| (ch == ',' || ch == ']').then_some(index))?;
    let memory_id = metadata[..end].trim();
    if memory_id.is_empty() {
        None
    } else {
        Some(memory_id.to_owned())
    }
}

pub(super) fn rendered_line_fingerprint(line: &str) -> Option<String> {
    let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn memory_active_recall_prompt_context_contribution(
    items: Vec<MemoryRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> Option<PromptContextContribution> {
    if items.is_empty() {
        return None;
    }
    let source_refs = memory_recall_source_refs(items.as_slice());
    let prompt_items = items
        .into_iter()
        .map(memory_recall_prompt_item)
        .collect::<Vec<_>>();
    let (content, rendered_truncated) =
        render_memory_recall_context_block(prompt_items.as_slice(), snapshot_truncated);
    if content.trim().is_empty() {
        return None;
    }
    let mut content = content;
    let mut truncated = rendered_truncated;
    let content_chars = content.chars().count();
    if content_chars > config.max_prompt_chars {
        content = truncate_chars(content.as_str(), config.max_prompt_chars);
        truncated = true;
    }
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 490,
        content: HookPromptContent::new(content).ok()?,
        max_chars: Some(config.max_prompt_chars),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
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
