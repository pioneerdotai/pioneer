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
}

pub fn deterministic_recall_context_summary(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
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
    summary
}

pub(super) fn resolve_active_memory_decision_without_preflight_plan(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
) -> ActiveMemoryDecision {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
        thread_episodic,
    );

    if local.local_final {
        return normalize_active_recall_plan_for_input(local.local_plan, &local.planner_input);
    }

    active_recall_no_provider_local_decision(local.local_plan, &local.planner_input, config, false)
}

pub(super) fn active_memory_decision_from_preflight_plan(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
    plan: ActiveRecallPlan,
) -> ActiveMemoryDecision {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
        thread_episodic,
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
    pub provider_sections: MemoryActiveRecallProviderSections,
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
    build_active_recall_local_preflight_plan_with_thread_summary(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
        MemoryActiveRecallThreadEpisodicSummary::default(),
        provider_available,
    )
}

pub fn build_active_recall_local_preflight_plan_with_thread_summary(
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
    provider_available: bool,
) -> MemoryActiveRecallLocalPlan {
    let local = active_recall_local_planning_parts(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
        thread_episodic,
    );
    let provider_sections = active_recall_provider_sections(&local.planner_input);
    let provider_planning_needed = !local.local_final
        && config.planner.enabled
        && provider_available
        && provider_sections.any();
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
        provider_sections,
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

pub fn active_recall_preflight_provider_success(
    mut decision: ActiveMemoryDecision,
    provider_input_chars: Option<usize>,
    provider_output_chars: Option<usize>,
) -> ActiveMemoryDecision {
    decision.provider_used = true;
    decision.provider_input_chars = provider_input_chars;
    decision.provider_output_chars = provider_output_chars;
    decision
        .diagnostics
        .insert(0, "memory.active_recall.provider_called".to_owned());
    normalize_active_recall_plan(decision)
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
    thread_episodic: MemoryActiveRecallThreadEpisodicSummary,
) -> ActiveRecallLocalPlanningParts {
    let planner_input = active_recall_planner_input(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
        thread_episodic,
    );
    let local_plan = local_active_memory_decision(&planner_input, "");
    let local_final = active_recall_local_plan_is_final(&local_plan);
    let decision_request = MemoryActiveRecallDecisionRequest {
        deterministic_context_count: planner_input.deterministic_context_count,
        deterministic_context_chars: planner_input.deterministic_context_chars,
        deterministic_memory_ids: planner_input.deterministic_memory_ids.clone(),
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
        available_durable_modes: active_recall_available_durable_mode_names(&planner_input),
        available_episodic_modes: active_recall_available_episodic_mode_names(&planner_input),
        available_scoped_contexts: active_recall_available_scoped_contexts(&planner_input),
        episodic_capabilities: planner_input.episodic_capabilities.clone(),
        thread_episodic: planner_input.thread_episodic.clone(),
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
        model: input.model.clone(),
        model_provider: input.model_provider.clone(),
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
    matches!(
        local_plan.effective_reason_code(),
        ActiveMemoryDecisionReasonCode::PolicyDisabled
            | ActiveMemoryDecisionReasonCode::ConfigDisabled
            | ActiveMemoryDecisionReasonCode::DeterministicOnly
            | ActiveMemoryDecisionReasonCode::StrictDebug
    )
}

fn active_recall_provider_sections(
    planner_input: &ActiveRecallPlannerInput,
) -> MemoryActiveRecallProviderSections {
    MemoryActiveRecallProviderSections {
        durable: true,
        episodic: !planner_input.episodic_capabilities.full_input_query,
    }
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

    fn nonempty_deterministic() -> DeterministicRecallContextSummary {
        DeterministicRecallContextSummary {
            memory_ids: BTreeSet::from(["memory_1".to_owned()]),
            rendered_line_fingerprints: BTreeSet::new(),
            context_count: 1,
            context_chars: 128,
        }
    }

    fn current_thread_capabilities() -> MemoryEpisodicRecallCapabilities {
        MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            related_thread_search: false,
            workspace_thread_search: false,
            full_input_query: false,
            current_task_context: false,
            completed_task_summary: false,
        }
    }

    fn current_thread_summary() -> MemoryActiveRecallThreadEpisodicSummary {
        MemoryActiveRecallThreadEpisodicSummary {
            current_thread_id_present: true,
            current_thread_recall_available: true,
            related_thread_recall_available: false,
            workspace_thread_recall_available: false,
            prompt_context_source_count: 0,
            prompt_context_chars: 0,
            source_ids: Vec::new(),
            diagnostics: vec!["current_thread_recall_available".to_owned()],
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
    fn active_recall_local_preflight_marks_config_disabled_as_host_local_final() {
        let config = MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::Disabled,
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
            ActiveMemoryDecisionReasonCode::ConfigDisabled
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Skip);
    }

    #[test]
    fn active_recall_local_preflight_does_not_gate_on_nonempty_deterministic_recall() {
        let plan = build_active_recall_local_preflight_plan(
            &test_context(None),
            &test_input(),
            &MemoryTurnPolicy::normal_default_allow(),
            &MemoryActiveRecallConfig::default(),
            &nonempty_deterministic(),
            MemoryEpisodicRecallCapabilities::default(),
            true,
        );

        assert!(plan.provider_planning_needed);
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Run);
        assert_eq!(
            plan.decision_request.deterministic_memory_ids,
            vec!["memory_1".to_owned()]
        );
    }

    #[test]
    fn active_recall_local_preflight_keeps_provider_needed_with_deterministic_and_episodic_context()
    {
        let plan = build_active_recall_local_preflight_plan_with_thread_summary(
            &test_context(None),
            &TurnPrePromptContextHookInput::from_parts(
                "а завтра какая?",
                Some("thread-model"),
                Some("thread-provider"),
            ),
            &MemoryTurnPolicy::normal_default_allow(),
            &MemoryActiveRecallConfig::default(),
            &nonempty_deterministic(),
            current_thread_capabilities(),
            current_thread_summary(),
            true,
        );

        assert!(plan.provider_planning_needed);
        assert_eq!(
            plan.local_decision.durable.status,
            ActiveMemoryDecisionStatus::Run
        );
        assert!(!plan.local_decision.episodic.queries.is_empty());
    }

    #[test]
    fn active_recall_local_preflight_disables_provider_episodic_section_for_full_input_query() {
        let mut capabilities = current_thread_capabilities();
        capabilities.full_input_query = true;
        let plan = build_active_recall_local_preflight_plan_with_thread_summary(
            &test_context(None),
            &TurnPrePromptContextHookInput::from_parts(
                "а завтра какая?",
                Some("thread-model"),
                Some("thread-provider"),
            ),
            &MemoryTurnPolicy::normal_default_allow(),
            &MemoryActiveRecallConfig::default(),
            &empty_deterministic(),
            capabilities,
            current_thread_summary(),
            true,
        );

        assert!(plan.provider_planning_needed);
        assert_eq!(
            plan.provider_sections,
            MemoryActiveRecallProviderSections {
                durable: true,
                episodic: false,
            }
        );
        assert_eq!(
            plan.local_decision.episodic.status,
            ActiveMemoryDecisionStatus::Run
        );
        assert_eq!(plan.local_decision.episodic.queries.len(), 1);
        assert_eq!(plan.local_decision.episodic.queries[0].query, None);
    }

    #[test]
    fn active_recall_local_preflight_marks_deterministic_only_as_host_local_final() {
        let config = MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::DeterministicOnly,
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
            ActiveMemoryDecisionReasonCode::DeterministicOnly
        );
        assert_eq!(plan.local_decision.status, ActiveMemoryDecisionStatus::Skip);
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
    fn active_recall_local_preflight_does_not_treat_high_confidence_run_as_host_local_final() {
        let plan = ActiveRecallPlan::run(
            ActiveMemoryDecisionReasonCode::MemoryLikely,
            0.70,
            vec![ActiveRecallMode::Profile],
            Vec::new(),
            vec!["memory.active_recall.local_candidate".to_owned()],
        );
        assert!(!active_recall_local_plan_is_final(&plan));

        let plan = ActiveRecallPlan::run(
            ActiveMemoryDecisionReasonCode::MemoryLikely,
            0.69,
            vec![ActiveRecallMode::Profile],
            Vec::new(),
            vec!["memory.active_recall.local_candidate".to_owned()],
        );
        assert!(!active_recall_local_plan_is_final(&plan));
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

    #[test]
    fn active_recall_preflight_provider_success_preserves_provider_metadata() {
        let parsed = parse_active_memory_decision_json(
            r#"{
                "durable": {
                    "status": "run",
                    "reasonCode": "provider_run",
                    "confidence": 0.82,
                    "modes": ["profile"],
                    "targets": [],
                    "diagnostics": ["memory.active_recall.identity_lookup"]
                },
                "episodic": {
                    "status": "skip",
                    "reasonCode": "provider_skip",
                    "confidence": 1.0,
                    "queries": []
                }
            }"#,
        )
        .expect("provider active recall parses through memory contract");

        let decision = active_recall_preflight_provider_success(parsed, Some(123), Some(45));

        assert!(decision.provider_used);
        assert!(!decision.provider_fallback_used);
        assert_eq!(decision.provider_input_chars, Some(123));
        assert_eq!(decision.provider_output_chars, Some(45));
        assert_eq!(
            decision.all_diagnostics()[0],
            "memory.active_recall.provider_called"
        );
        assert!(
            decision
                .all_diagnostics()
                .iter()
                .any(|diagnostic| diagnostic == "memory.active_recall.identity_lookup")
        );
    }

    #[test]
    fn active_recall_local_preflight_matches_no_provider_local_resolution() {
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
        let resolved = resolve_active_memory_decision_without_preflight_plan(
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities,
            MemoryActiveRecallThreadEpisodicSummary::default(),
        );

        assert!(!plan.provider_planning_needed);
        assert_eq!(plan.local_decision, resolved);
        assert_eq!(
            plan.local_decision.reason_code,
            ActiveMemoryDecisionReasonCode::MemoryLikely
        );
        assert!(
            plan.local_decision
                .all_diagnostics()
                .iter()
                .any(|diagnostic| diagnostic == "memory.active_recall.provider_unavailable")
        );
    }

    #[test]
    fn active_recall_local_preflight_matches_planner_disabled_local_resolution() {
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
        let resolved = resolve_active_memory_decision_without_preflight_plan(
            &context,
            &input,
            &policy,
            &config,
            &deterministic,
            episodic_capabilities,
            MemoryActiveRecallThreadEpisodicSummary::default(),
        );

        assert!(!plan.provider_planning_needed);
        assert_eq!(plan.local_decision, resolved);
        assert!(
            plan.local_decision
                .all_diagnostics()
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
