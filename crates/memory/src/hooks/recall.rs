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

pub(super) fn deterministic_recall_context_summary(
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
    let planner_input = active_recall_planner_input(
        context,
        input,
        policy,
        config,
        deterministic,
        episodic_capabilities,
    );
    let local_plan = local_active_memory_decision(&planner_input, "");
    if matches!(
        local_plan.reason_code,
        ActiveMemoryDecisionReasonCode::PolicyDisabled
            | ActiveMemoryDecisionReasonCode::ConfigDisabled
            | ActiveMemoryDecisionReasonCode::DeterministicOnly
            | ActiveMemoryDecisionReasonCode::DeterministicSufficient
            | ActiveMemoryDecisionReasonCode::StrictDebug
    ) {
        return normalize_active_recall_plan_for_input(local_plan, &planner_input);
    }
    if local_plan.status == ActiveMemoryDecisionStatus::Run && local_plan.confidence >= 0.7 {
        return normalize_active_recall_plan_for_input(local_plan, &planner_input);
    }

    if config.planner.enabled
        && let Some(provider) = provider
    {
        let request = MemoryActiveRecallDecisionRequest {
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
        let provider_context = MemoryActiveRecallDecisionContext {
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
        match call_active_recall_decision_provider(
            provider.as_ref(),
            provider_context.clone(),
            request.clone(),
            config,
        )
        .await
        {
            Ok(decision) => {
                return normalize_active_recall_plan_for_input(decision, &planner_input);
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
                                &planner_input,
                            );
                        }
                        Err(retry_failure) => {
                            let mut decision = active_recall_provider_fallback(
                                local_plan,
                                retry_failure.reason,
                                &planner_input,
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
                    local_plan,
                    primary_failure.reason,
                    &planner_input,
                    config,
                    primary_failure.provider_input_chars,
                    primary_failure.provider_output_chars,
                );
            }
        }
    }

    let mut fallback = local_plan;
    if !config.planner.enabled {
        fallback
            .diagnostics
            .push("memory.active_recall.provider_disabled".to_owned());
    } else if provider.is_none() {
        fallback
            .diagnostics
            .push("memory.active_recall.provider_unavailable".to_owned());
    }
    normalize_active_recall_plan_for_input(fallback, &planner_input)
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
