use super::*;

pub(in crate::hooks) struct ActiveMemoryRecallHook {
    pub(in crate::hooks) memory_provider: Arc<dyn AgentMemoryProvider>,
    pub(in crate::hooks) episodic_provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    pub(in crate::hooks) config: MemoryActiveRecallConfig,
}

#[async_trait::async_trait]
impl HookHandler for ActiveMemoryRecallHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPostPreflightPromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_active_recall_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_post_preflight_prompt_context_input(&request)?;
        let prompt_input = TurnPrePromptContextHookInput::from_parts(
            input.input_text.clone(),
            input.model.clone(),
            input.model_provider.clone(),
        );
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory active recall skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => return Ok(memory_missing_policy_response(MEMORY_ACTIVE_RECALL_HOOK_ID)),
        };
        let config = self.config.normalized();
        let deterministic = deterministic_recall_context_summary(&request.prompt_context_set);
        let context = memory_turn_context_from_prompt_context_request(&request, &prompt_input)?;
        let episodic_capabilities =
            resolve_episodic_recall_capabilities(self.episodic_provider.as_ref(), &context).await;
        let thread_episodic = active_recall_thread_episodic_summary(
            &request.prompt_context_set,
            &context,
            &episodic_capabilities,
        );
        let mut response = HookHandlerResponse::default();
        let decision = match &input.active_memory_recall_plan {
            Some(plan) => match serde_json::from_value::<ActiveRecallPlan>(plan.clone()) {
                Ok(plan) => match validate_active_recall_preflight_plan_shape(&plan) {
                    Ok(()) => active_memory_decision_from_preflight_plan(
                        &context,
                        &prompt_input,
                        &policy,
                        &config,
                        &deterministic,
                        episodic_capabilities.clone(),
                        thread_episodic.clone(),
                        plan,
                    ),
                    Err(error) => {
                        response.diagnostics.push(memory_safe_warning_diagnostic(
                            "memory.active_recall.preflight_plan_invalid",
                            format!(
                                "memory active recall planner output invalid; using local fallback: {error}"
                            ),
                        ));
                        resolve_active_memory_decision_without_preflight_plan(
                            &context,
                            &prompt_input,
                            &policy,
                            &config,
                            &deterministic,
                            episodic_capabilities.clone(),
                            thread_episodic.clone(),
                        )
                    }
                },
                Err(error) => {
                    response.diagnostics.push(memory_safe_warning_diagnostic(
                        "memory.active_recall.preflight_plan_invalid",
                        format!(
                            "memory active recall planner output invalid; using local fallback: {error}"
                        ),
                    ));
                    resolve_active_memory_decision_without_preflight_plan(
                        &context,
                        &prompt_input,
                        &policy,
                        &config,
                        &deterministic,
                        episodic_capabilities.clone(),
                        thread_episodic.clone(),
                    )
                }
            },
            None => resolve_active_memory_decision_without_preflight_plan(
                &context,
                &prompt_input,
                &policy,
                &config,
                &deterministic,
                episodic_capabilities.clone(),
                thread_episodic.clone(),
            ),
        };
        response
            .diagnostics
            .extend(hook_diagnostics_from_strings(&decision.all_diagnostics()));
        response
            .diagnostics
            .push(active_memory_decision_observability_diagnostic(
                &decision,
                &deterministic,
            ));

        if decision.effective_status() != ActiveMemoryDecisionStatus::Run {
            response.diagnostics.push(memory_safe_info_diagnostic(
                decision.effective_reason_code().diagnostic_code(),
                format!(
                    "memory active recall skipped: reason={:?} confidence={:.2}",
                    decision.effective_reason_code(),
                    decision.effective_confidence()
                ),
            ));
            response
                .contributions
                .push(active_recall_debug_audit_contribution(
                    &decision,
                    &ActiveRecallExecutionResult::default(),
                    None,
                    None,
                ));
            return Ok(response);
        }

        let mut execution = execute_active_recall_plan(
            self.memory_provider.as_ref(),
            ActiveRecallExecutionInput {
                context: context.clone(),
                plan: decision.clone(),
                deterministic: deterministic.clone(),
                config: config.clone(),
                episodic_provider: self.episodic_provider.clone(),
                episodic_capabilities,
            },
        )
        .await;
        response
            .diagnostics
            .push(active_recall_execution_observability_diagnostic(&execution));
        response.diagnostics.extend(hook_diagnostics_from_strings(
            execution.diagnostics.as_slice(),
        ));

        if execution.is_empty() && decision.debug_fallback {
            let debug_execution = execute_active_recall_debug_fallback(
                self.memory_provider.as_ref(),
                context.clone(),
                input.input_text.as_str(),
                &decision,
                &config,
            )
            .await;
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.debug_fallback",
                "memory active recall used explicit debug fallback",
            ));
            response.diagnostics.extend(hook_diagnostics_from_strings(
                debug_execution.diagnostics.as_slice(),
            ));
            execution = debug_execution;
        }

        if execution.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_hits",
                "memory active recall returned no memory context",
            ));
            response
                .contributions
                .push(active_recall_debug_audit_contribution(
                    &decision, &execution, None, None,
                ));
            return Ok(response);
        }

        let active_dedup = dedup_active_recall_items_with_lines(
            execution.items.clone(),
            &deterministic.memory_ids,
            &deterministic.rendered_line_fingerprints,
        );
        response
            .diagnostics
            .push(active_memory_dedup_observability_diagnostic(
                &deterministic,
                &active_dedup,
            ));

        let episodic_items = execution.episodic_items.clone();

        let mut contributed = false;
        let mut active_synthesis = None;
        if !active_dedup.items.is_empty() {
            let synthesis_result =
                memory_active_recall_multi_source_prompt_context_contribution_with_synthesis(
                    active_dedup.items.clone(),
                    Vec::new(),
                    execution.truncated,
                    &config,
                );
            response
                .diagnostics
                .push(memory_active_synthesis_observability_diagnostic(
                    &synthesis_result.synthesis,
                ));
            response.diagnostics.extend(hook_diagnostics_from_strings(
                synthesis_result.synthesis.diagnostics.as_slice(),
            ));
            if let Some(contribution) = synthesis_result.contribution {
                contributed = true;
                response.diagnostics.push(memory_safe_info_diagnostic(
                    "memory.active_recall.context_contributed",
                    "memory active recall contributed durable prompt context",
                ));
                response
                    .contributions
                    .push(HookContribution::PromptContext(contribution));
            }
            active_synthesis = Some(synthesis_result.synthesis);
        }

        let episodic_contributions = memory_episodic_recall_prompt_context_contributions(
            episodic_items,
            execution.truncated,
            &config,
        );
        if !episodic_contributions.is_empty() {
            contributed = true;
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.episodic_recall.context_contributed",
                "memory episodic recall contributed prompt context",
            ));
            for contribution in episodic_contributions {
                response
                    .contributions
                    .push(HookContribution::PromptContext(contribution));
            }
        }
        if !contributed {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_hits",
                "memory active recall returned no prompt context after synthesis",
            ));
        }
        response
            .contributions
            .push(active_recall_debug_audit_contribution(
                &decision,
                &execution,
                Some(&active_dedup),
                active_synthesis.as_ref(),
            ));
        Ok(response)
    }
}

fn validate_active_recall_preflight_plan_shape(
    plan: &ActiveRecallPlan,
) -> Result<(), &'static str> {
    if plan.effective_status() == ActiveMemoryDecisionStatus::Run
        && plan.durable.modes.is_empty()
        && plan.episodic.queries.is_empty()
        && !plan.debug_fallback
    {
        return Err("run plan requires at least one durable mode or episodic query");
    }
    if plan.durable.status != ActiveMemoryDecisionStatus::Run && !plan.durable.modes.is_empty() {
        return Err("non-run durable plan must not include modes");
    }
    if plan.episodic.status != ActiveMemoryDecisionStatus::Run && !plan.episodic.queries.is_empty()
    {
        return Err("non-run episodic plan must not include queries");
    }
    Ok(())
}
