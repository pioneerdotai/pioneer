use super::*;

pub(in crate::hooks) struct ActiveMemoryRecallHook {
    pub(in crate::hooks) memory_provider: Arc<dyn AgentMemoryProvider>,
    pub(in crate::hooks) decision_provider: Option<Arc<dyn AgentActiveMemoryDecisionProvider>>,
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
        vec![HookPhase::TurnPrePromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_active_recall_capabilities(self.decision_provider.is_some())
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_context_input(&request)?;
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
        let deterministic =
            deterministic_recall_context_summary(&request.prompt_context_set, &config);
        let context = memory_turn_context_from_prompt_context_request(&request, input)?;
        let mut response = HookHandlerResponse::default();
        let decision = resolve_active_memory_decision(
            self.decision_provider.as_ref(),
            &context,
            input,
            &policy,
            &config,
            &deterministic,
        )
        .await;
        response.diagnostics.extend(hook_diagnostics_from_strings(
            decision.diagnostics.as_slice(),
        ));
        response
            .diagnostics
            .push(active_memory_decision_observability_diagnostic(
                &decision,
                &deterministic,
            ));

        if decision.status != ActiveMemoryDecisionStatus::Run {
            response.diagnostics.push(memory_safe_info_diagnostic(
                decision.reason_code.diagnostic_code(),
                format!(
                    "memory active recall skipped: reason={:?} confidence={:.2}",
                    decision.reason_code, decision.confidence
                ),
            ));
            return Ok(response);
        }

        let queries = active_memory_query_plan(input.input_text.as_str(), &decision, &config);
        if queries.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_query",
                "memory active recall skipped: no bounded query available",
            ));
            return Ok(response);
        }

        let mut active_items = Vec::new();
        let mut active_truncated = false;
        for query in queries {
            match self
                .memory_provider
                .recall_memory(
                    context.clone(),
                    MemoryRecallRequest {
                        query,
                        categories: Vec::new(),
                        top_k: Some(config.top_k_per_query),
                        max_chars: Some(config.max_prompt_chars),
                    },
                )
                .await
            {
                Ok(snapshot) => {
                    response.diagnostics.extend(hook_diagnostics_from_strings(
                        snapshot.diagnostics.as_slice(),
                    ));
                    active_truncated |= snapshot.truncated;
                    active_items.extend(snapshot.items);
                }
                Err(error) => {
                    let _ = error;
                    response.diagnostics.push(memory_safe_warning_diagnostic(
                        "memory.active_recall.failed",
                        "memory active recall failed",
                    ));
                    return Ok(response);
                }
            }
        }

        let active_dedup = dedup_active_recall_items_with_lines(
            active_items,
            &deterministic.memory_ids,
            &deterministic.rendered_line_fingerprints,
        );
        response
            .diagnostics
            .push(active_memory_dedup_observability_diagnostic(
                &deterministic,
                &active_dedup,
            ));
        if active_dedup.items.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_hits",
                "memory active recall returned no non-duplicate memory context",
            ));
            return Ok(response);
        }

        if let Some(contribution) = memory_active_recall_prompt_context_contribution(
            active_dedup.items,
            active_truncated,
            &config,
        ) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.context_contributed",
                "memory active recall contributed prompt context",
            ));
            response
                .contributions
                .push(HookContribution::PromptContext(contribution));
        }
        Ok(response)
    }
}
