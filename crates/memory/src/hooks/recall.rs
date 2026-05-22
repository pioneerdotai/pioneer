use super::*;

pub(super) fn memory_turn_context_from_prompt_context_request(
    request: &HookHandlerRequest,
    input: &TurnPrePromptContextHookInput,
) -> HookResult<MemoryTurnContext> {
    let workspace_id = required_context_id(
        request.context.workspace_id.as_ref().map(|id| id.as_str()),
        "workspace_id",
    )?;
    let thread_id = required_context_id(
        request.context.thread_id.as_ref().map(|id| id.as_str()),
        "thread_id",
    )?;
    let turn_id = required_context_id(
        request.context.turn_id.as_ref().map(|id| id.as_str()),
        "turn_id",
    )?;
    Ok(MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input.input_text.clone(),
        task_id: request
            .context
            .task_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        agent_id: request
            .context
            .agent_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
    })
}

pub(super) fn memory_recall_request(input_text: &str) -> MemoryRecallRequest {
    MemoryRecallRequest {
        query: input_text.to_owned(),
        categories: Vec::new(),
        top_k: Some(5),
        max_chars: Some(1_500),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeterministicRecallContextSummary {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub memory_ids: BTreeSet<String>,
    #[serde(skip)]
    pub rendered_line_fingerprints: BTreeSet<String>,
    pub context_count: usize,
    pub context_chars: usize,
    pub sufficient: bool,
}

pub fn deterministic_recall_context_summary(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    config: &MemoryActiveRecallConfig,
) -> DeterministicRecallContextSummary {
    let mut summary = DeterministicRecallContextSummary::default();
    for entry in prompt_context_set.entries() {
        if entry.domain.as_str() != MEMORY_POLICY_DOMAIN
            || entry.contribution_id.as_str() != MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID
        {
            continue;
        }
        summary.context_count += 1;
        summary.context_chars += entry.content.as_str().chars().count();
        summary
            .rendered_line_fingerprints
            .extend(rendered_line_fingerprints(entry.content.as_str()));
        for source_ref in &entry.source_refs {
            if source_ref.kind.as_str() == "memory" {
                summary.memory_ids.insert(source_ref.id.as_str().to_owned());
            }
        }
    }
    summary.sufficient = !summary.memory_ids.is_empty()
        && (summary.memory_ids.len() >= config.deterministic_sufficient_min_items
            || summary.context_chars >= config.deterministic_sufficient_min_chars);
    summary
}

pub(super) async fn resolve_active_memory_decision(
    provider: Option<&Arc<dyn AgentActiveMemoryDecisionProvider>>,
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
) -> ActiveMemoryDecision {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
    );

    if local.local_final {
        return normalize_active_recall_plan_for_input(local.local_plan, &local.planner_input);
    }

    if config.planner.enabled
        && let Some(provider) = provider
    {
        let request = local.decision_request.clone();
        let provider_context = local.decision_context.clone();
        match call_active_recall_decision_provider(
            provider.as_ref(),
            provider_context.clone(),
            request.clone(),
            config,
        )
        .await
        {
            Ok(decision) => {
                return normalize_active_recall_plan_for_input(decision, &local.planner_input);
            }
            Err(primary_failure) => {
                if let Some(retry_context) =
                    active_recall_thread_model_retry_context(&provider_context, input)
                {
                    match call_active_recall_decision_provider(
                        provider.as_ref(),
                        retry_context,
                        request,
                        config,
                    )
                    .await
                    {
                        Ok(mut decision) => {
                            decision.diagnostics.insert(
                                0,
                                "memory.active_recall.thread_model_retry_used".to_owned(),
                            );
                            decision
                                .diagnostics
                                .insert(1, primary_failure.reason.to_owned());
                            return normalize_active_recall_plan_for_input(
                                decision,
                                &local.planner_input,
                            );
                        }
                        Err(retry_failure) => {
                            let mut decision = active_recall_provider_fallback(
                                local.local_plan.clone(),
                                retry_failure.reason,
                                &local.planner_input,
                                config,
                                retry_failure.provider_input_chars,
                                retry_failure.provider_output_chars,
                            );
                            decision.diagnostics.insert(
                                0,
                                "memory.active_recall.thread_model_retry_failed".to_owned(),
                            );
                            decision
                                .diagnostics
                                .insert(1, primary_failure.reason.to_owned());
                            return decision;
                        }
                    }
                }

                return active_recall_provider_fallback(
                    local.local_plan.clone(),
                    primary_failure.reason,
                    &local.planner_input,
                    config,
                    primary_failure.provider_input_chars,
                    primary_failure.provider_output_chars,
                );
            }
        }
    }

    active_recall_no_provider_local_decision(
        local.local_plan,
        &local.planner_input,
        config,
        provider.is_some(),
    )
}

pub(super) fn active_memory_decision_from_preflight_plan(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    plan: ActiveRecallPlan,
) -> ActiveMemoryDecision {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
    );
    if matches!(
        local.local_plan.reason_code,
        ActiveMemoryDecisionReasonCode::PolicyDisabled
            | ActiveMemoryDecisionReasonCode::ConfigDisabled
    ) {
        return normalize_active_recall_plan_for_input(local.local_plan, &local.planner_input);
    }
    normalize_active_recall_plan_for_input(plan, &local.planner_input)
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryActiveRecallLocalPlan {
    pub decision_context: MemoryActiveRecallDecisionContext,
    pub decision_request: MemoryActiveRecallDecisionRequest,
    pub local_decision: ActiveMemoryDecision,
    pub provider_planning_needed: bool,
    pub provider_fallback_context: Option<MemoryActiveRecallProviderFallbackContext>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryActiveRecallProviderFallbackContext {
    local_plan: ActiveMemoryDecision,
    planner_input: ActiveRecallPlannerInput,
    config: MemoryActiveRecallConfig,
}

pub fn build_active_recall_local_preflight_plan(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    provider_available: bool,
) -> MemoryActiveRecallLocalPlan {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
    );
    let provider_planning_needed =
        !local.local_final && config.planner.enabled && provider_available;
    let local_decision = if provider_planning_needed || local.local_final {
        normalize_active_recall_plan_for_input(local.local_plan.clone(), &local.planner_input)
    } else {
        active_recall_no_provider_local_decision(
            local.local_plan.clone(),
            &local.planner_input,
            config,
            provider_available,
        )
    };
    let provider_fallback_context =
        provider_planning_needed.then(|| MemoryActiveRecallProviderFallbackContext {
            local_plan: local.local_plan.clone(),
            planner_input: local.planner_input.clone(),
            config: config.normalized(),
        });

    MemoryActiveRecallLocalPlan {
        decision_context: local.decision_context,
        decision_request: local.decision_request,
        local_decision,
        provider_planning_needed,
        provider_fallback_context,
    }
}

pub fn active_recall_preflight_provider_fallback(
    context: &MemoryActiveRecallProviderFallbackContext,
    reason: &str,
    provider_input_chars: Option<usize>,
    provider_output_chars: Option<usize>,
) -> ActiveMemoryDecision {
    active_recall_provider_fallback(
        context.local_plan.clone(),
        reason,
        &context.planner_input,
        &context.config,
        provider_input_chars,
        provider_output_chars,
    )
}

#[derive(Debug, Clone, PartialEq)]
struct ActiveRecallLocalPlanningParts {
    planner_input: ActiveRecallPlannerInput,
    local_plan: ActiveMemoryDecision,
    decision_context: MemoryActiveRecallDecisionContext,
    decision_request: MemoryActiveRecallDecisionRequest,
    local_final: bool,
}

fn active_recall_local_planning_parts(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
) -> ActiveRecallLocalPlanningParts {
    let planner_input = active_recall_planner_input(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
    );
    let local_plan = local_active_memory_decision(&planner_input, "");
    let local_final = active_recall_local_plan_is_final(&local_plan);
    let decision_request = MemoryActiveRecallDecisionRequest {
        deterministic_context_count: planner_input.deterministic_context_count,
        deterministic_context_chars: planner_input.deterministic_context_chars,
        deterministic_memory_ids: planner_input.deterministic_memory_ids.clone(),
        deterministic_sufficient: planner_input.deterministic_sufficient,
        deterministic_recall_empty: planner_input.deterministic_recall_empty,
        has_workspace_context: planner_input.has_workspace_context,
        has_task_context: planner_input.has_task_context,
        input_length_bucket: planner_input.input_length_bucket.as_str().to_owned(),
        config_mode: planner_input.config_mode,
        read_allowed: planner_input.read_allowed,
        active_memory_allowed: planner_input.active_memory_allowed,
        explicit_no_memory: planner_input.explicit_no_memory,
        input_text_char_count: planner_input.input_text_char_count,
        available_modes: active_recall_available_mode_names(&planner_input),
        available_scoped_contexts: active_recall_available_scoped_contexts(&planner_input),
        episodic_capabilities: planner_input.episodic_capabilities.clone(),
        max_queries: config.max_queries,
        top_k_per_query: config.top_k_per_query,
        max_prompt_chars: config.max_prompt_chars,
        max_input_chars: config.planner.max_input_chars,
        max_output_chars: config.planner.max_output_chars,
        fallback_policy: config.planner.fallback,
    };
    let decision_context = MemoryActiveRecallDecisionContext {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        mode: context.mode,
        input_text_preview: planner_input.input_text_preview.clone(),
        model: config.planner.model.clone().or_else(|| input.model.clone()),
        model_provider: config
            .planner
            .provider_name
            .clone()
            .or_else(|| input.model_provider.clone()),
    };

    ActiveRecallLocalPlanningParts {
        planner_input,
        local_plan,
        decision_context,
        decision_request,
        local_final,
    }
}

fn active_recall_local_plan_is_final(local_plan: &ActiveMemoryDecision) -> bool {
    if matches!(
        local_plan.reason_code,
        ActiveMemoryDecisionReasonCode::PolicyDisabled
            | ActiveMemoryDecisionReasonCode::ConfigDisabled
            | ActiveMemoryDecisionReasonCode::DeterministicOnly
            | ActiveMemoryDecisionReasonCode::DeterministicSufficient
            | ActiveMemoryDecisionReasonCode::StrictDebug
    ) {
        return true;
    }

    local_plan.status == ActiveMemoryDecisionStatus::Run && local_plan.confidence >= 0.7
}

fn active_recall_no_provider_local_decision(
    mut local_plan: ActiveMemoryDecision,
    planner_input: &ActiveRecallPlannerInput,
    config: &MemoryActiveRecallConfig,
    provider_available: bool,
) -> ActiveMemoryDecision {
    if !config.planner.enabled {
        local_plan
            .diagnostics
            .push("memory.active_recall.provider_disabled".to_owned());
    } else if !provider_available {
        local_plan
            .diagnostics
            .push("memory.active_recall.provider_unavailable".to_owned());
    }
    normalize_active_recall_plan_for_input(local_plan, planner_input)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveRecallProviderFailure {
    reason: &'static str,
    provider_input_chars: Option<usize>,
    provider_output_chars: Option<usize>,
}

async fn call_active_recall_decision_provider(
    provider: &dyn AgentActiveMemoryDecisionProvider,
    context: MemoryActiveRecallDecisionContext,
    request: MemoryActiveRecallDecisionRequest,
    config: &MemoryActiveRecallConfig,
) -> Result<ActiveMemoryDecision, ActiveRecallProviderFailure> {
    let provider_input_chars = request.sanitized_input_json(&context).chars().count();
    match tokio::time::timeout(
        std::time::Duration::from_millis(config.planner.timeout_ms),
        provider.resolve_active_memory_decision_json(context, request),
    )
    .await
    {
        Err(_) => Err(ActiveRecallProviderFailure {
            reason: "memory.active_recall.provider_timeout",
            provider_input_chars: Some(provider_input_chars),
            provider_output_chars: None,
        }),
        Ok(provider_result) => match provider_result {
            Ok(json) => match parse_active_memory_decision_json(json.as_str()) {
                Ok(mut decision) => {
                    decision.provider_input_chars = Some(provider_input_chars);
                    decision.provider_output_chars = Some(json.chars().count());
                    decision
                        .diagnostics
                        .insert(0, "memory.active_recall.provider_called".to_owned());
                    Ok(decision)
                }
                Err(_) => Err(ActiveRecallProviderFailure {
                    reason: "memory.active_recall.invalid_json",
                    provider_input_chars: Some(provider_input_chars),
                    provider_output_chars: Some(json.chars().count()),
                }),
            },
            Err(_) => Err(ActiveRecallProviderFailure {
                reason: "memory.active_recall.provider_failed",
                provider_input_chars: Some(provider_input_chars),
                provider_output_chars: None,
            }),
        },
    }
}

fn active_recall_thread_model_retry_context(
    primary_context: &MemoryActiveRecallDecisionContext,
    input: &TurnPrePromptContextHookInput,
) -> Option<MemoryActiveRecallDecisionContext> {
    let turn_model = input.model.clone()?;
    let turn_model_provider = input.model_provider.clone()?;
    if primary_context.model.as_deref() == Some(turn_model.as_str())
        && primary_context.model_provider.as_deref() == Some(turn_model_provider.as_str())
    {
        return None;
    }

    let mut retry_context = primary_context.clone();
    retry_context.model = Some(turn_model);
    retry_context.model_provider = Some(turn_model_provider);
    Some(retry_context)
}

fn active_recall_provider_fallback(
    mut local_plan: ActiveRecallPlan,
    reason: &str,
    planner_input: &ActiveRecallPlannerInput,
    config: &MemoryActiveRecallConfig,
    provider_input_chars: Option<usize>,
    provider_output_chars: Option<usize>,
) -> ActiveMemoryDecision {
    match config.planner.fallback {
        MemoryActiveRecallPlannerFallbackPolicy::Deterministic => {
            local_plan.diagnostics.push(reason.to_owned());
            local_plan.provider_fallback_used = true;
            local_plan.provider_input_chars = provider_input_chars;
            local_plan.provider_output_chars = provider_output_chars;
            normalize_active_recall_plan_for_input(local_plan, planner_input)
        }
        MemoryActiveRecallPlannerFallbackPolicy::SkipActiveRecall => {
            let mut plan = ActiveRecallPlan::skip(
                ActiveMemoryDecisionReasonCode::ProviderSkip,
                0.0,
                vec![reason.to_owned(), "planner_fallback_skip".to_owned()],
            );
            plan.provider_fallback_used = true;
            plan.provider_input_chars = provider_input_chars;
            plan.provider_output_chars = provider_output_chars;
            plan
        }
    }
}

#[cfg(test)]
mod active_recall_local_preflight_tests {
    use super::*;

    fn test_context(task_id: Option<&str>) -> MemoryTurnContext {
        MemoryTurnContext {
            workspace_id: "ws".to_owned(),
            thread_id: "thr".to_owned(),
            turn_id: "turn".to_owned(),
            mode: ThreadMode::Agent,
            input_text: "как меня зовут?".to_owned(),
            task_id: task_id.map(str::to_owned),
            agent_id: None,
        }
    }

    fn test_input() -> TurnPrePromptContextHookInput {
        TurnPrePromptContextHookInput::from_parts(
            "как меня зовут?",
            Some("thread-model"),
            Some("thread-provider"),
        )
    }

    fn empty_deterministic() -> DeterministicRecallContextSummary {
        DeterministicRecallContextSummary::default()
    }

    fn sufficient_deterministic() -> DeterministicRecallContextSummary {
        DeterministicRecallContextSummary {
            memory_ids: BTreeSet::from(["memory_1".to_owned()]),
            rendered_line_fingerprints: BTreeSet::new(),
            context_count: 1,
            context_chars: 128,
            sufficient: true,
        }
    }

    #[test]
    fn active_recall_local_preflight_marks_policy_disabled_as_host_local_final() {
        let plan = build_active_recall_local_preflight_plan(
            &test_context(None),
            &test_input(),
            &MemoryTurnPolicy::no_use(),
            &MemoryActiveRecallConfig::default(),
            &empty_deterministic(),
            MemoryEpisodicRecallCapabilities::default(),
            true,
        );

        assert!(!plan.provider_planning_needed);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::PolicyDisabled
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Skip);
    }

    #[test]
    fn active_recall_local_preflight_marks_deterministic_sufficient_as_host_local_final() {
        let plan = build_active_recall_local_preflight_plan(
            &test_context(None),
            &test_input(),
            &MemoryTurnPolicy::normal_default_allow(),
            &MemoryActiveRecallConfig::default(),
            &sufficient_deterministic(),
            MemoryEpisodicRecallCapabilities::default(),
            true,
        );

        assert!(!plan.provider_planning_needed);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::DeterministicSufficient
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Skip);
        assert_eq!(
            plan.decision_request.deterministic_memory_ids,
            vec!["memory_1".to_owned()]
        );
        assert!(plan.decision_request.deterministic_sufficient);
    }

    #[test]
    fn active_recall_local_preflight_marks_strict_debug_as_host_local_final() {
        let config = MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::StrictDebug,
            ..MemoryActiveRecallConfig::default()
        };

        let plan = build_active_recall_local_preflight_plan(
            &test_context(None),
            &test_input(),
            &MemoryTurnPolicy::normal_default_allow(),
            &config,
            &empty_deterministic(),
            MemoryEpisodicRecallCapabilities::default(),
            true,
        );

        assert!(!plan.provider_planning_needed);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::StrictDebug
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Run);
        assert!(plan.local_decision.debug_fallback);
    }

    #[test]
    fn active_recall_local_preflight_marks_uncertain_low_confidence_as_provider_needed() {
        let plan = build_active_recall_local_preflight_plan(
            &test_context(None),
            &test_input(),
            &MemoryTurnPolicy::normal_default_allow(),
            &MemoryActiveRecallConfig::default(),
            &empty_deterministic(),
            MemoryEpisodicRecallCapabilities::default(),
            true,
        );

        assert!(plan.provider_planning_needed);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::MemoryLikely
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Run);
        assert!(plan.local_decision.confidence < 0.7);
        assert_eq!(plan.decision_context.model.as_deref(), Some("thread-model"));
        assert_eq!(
            plan.decision_context.model_provider.as_deref(),
            Some("thread-provider")
        );
        assert_eq!(
            plan.decision_request.available_modes,
            vec![
                "profile".to_owned(),
                "project".to_owned(),
                "durable".to_owned()
            ]
        );
        assert_eq!(
            plan.decision_request.available_scoped_contexts,
            vec!["workspace".to_owned(), "thread".to_owned()]
        );
    }

    #[tokio::test]
    async fn active_recall_local_preflight_matches_no_provider_local_resolution() {
        let context = test_context(None);
        let input = test_input();
        let policy = MemoryTurnPolicy::normal_default_allow();
        let config = MemoryActiveRecallConfig::default();
        let deterministic = empty_deterministic();
        let episodic_capabilities = MemoryEpisodicRecallCapabilities::default();

        let plan = build_active_recall_local_preflight_plan(
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities.clone(),
            false,
        );
        let resolved = resolve_active_memory_decision(
            None,
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities,
        )
        .await;

        assert!(!plan.provider_planning_needed);
        assert_eq!(plan.local_decision, resolved);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::MemoryLikely
        );
        assert!(
            plan.local_decision
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic == "memory.active_recall.provider_unavailable")
        );
    }

    #[tokio::test]
    async fn active_recall_local_preflight_matches_planner_disabled_local_resolution() {
        let context = test_context(None);
        let input = test_input();
        let policy = MemoryTurnPolicy::normal_default_allow();
        let mut config = MemoryActiveRecallConfig::default();
        config.planner.enabled = false;
        let deterministic = empty_deterministic();
        let episodic_capabilities = MemoryEpisodicRecallCapabilities::default();

        let plan = build_active_recall_local_preflight_plan(
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities.clone(),
            true,
        );
        let resolved = resolve_active_memory_decision(
            None,
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities,
        )
        .await;

        assert!(!plan.provider_planning_needed);
        assert_eq!(plan.local_decision, resolved);
        assert!(
            plan.local_decision
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic == "memory.active_recall.provider_disabled")
        );
    }
}

#[cfg(test)]
pub(super) fn memory_recall_prompt_context_contribution(
    recall_snapshot: MemoryRecallSnapshot,
) -> Option<PromptContextContribution> {
    memory_recall_prompt_context_contribution_with_synthesis(recall_snapshot).contribution
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryRecallPromptContextContributionResult {
    pub(super) contribution: Option<PromptContextContribution>,
    pub(super) synthesis: MemoryRecallSynthesis,
}

pub(super) fn memory_recall_prompt_context_contribution_with_synthesis(
    recall_snapshot: MemoryRecallSnapshot,
) -> MemoryRecallPromptContextContributionResult {
    let snapshot_truncated = recall_snapshot.truncated;
    let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput::deterministic(
        recall_snapshot,
        MemoryRecallSynthesisBudget::default(),
    ));
    let content = synthesis.rendered_text();
    let Some(content) = HookPromptContent::new(content).ok() else {
        return MemoryRecallPromptContextContributionResult {
            contribution: None,
            synthesis,
        };
    };
    let contribution = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 500,
        content,
        max_chars: Some(1_500),
        source_refs: synthesis.source_refs.clone(),
        diagnostics: hook_diagnostics_from_strings(synthesis.diagnostics.as_slice()),
        truncated: snapshot_truncated || synthesis.truncated,
    };
    MemoryRecallPromptContextContributionResult {
        contribution: Some(contribution),
        synthesis,
    }
}
