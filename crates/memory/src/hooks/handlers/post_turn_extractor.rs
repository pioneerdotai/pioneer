use super::*;

pub(in crate::hooks) struct MemoryPostTurnExtractorHook {
    pub(in crate::hooks) write_provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    pub(in crate::hooks) extractor_provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    pub(in crate::hooks) config: MemoryPostTurnExtractorConfig,
}

#[async_trait::async_trait]
impl HookHandler for MemoryPostTurnExtractorHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_POST_TURN_EXTRACTOR_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPostTurn]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_post_turn_extractor_capabilities(self.extractor_provider.is_some())
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_post_turn_input(&request)?;
        let config = self.config.normalized();
        let mut response = HookHandlerResponse::default();

        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => MemoryPostTurnEligibilityPolicy::Available(policy),
            Some(Err(error)) => {
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory post-turn extractor policy decode failed: {error}"),
                ));
                MemoryPostTurnEligibilityPolicy::Malformed
            }
            None => MemoryPostTurnEligibilityPolicy::Missing,
        };

        let eligibility_input =
            memory_post_turn_eligibility_input_from_request(&request, input, &config, policy);
        let eligibility_decision = MemoryPostTurnEligibilityGate::evaluate(&eligibility_input);
        if !eligibility_decision.is_eligible() {
            let MemoryPostTurnEligibilityDecision::Skipped(reason) = eligibility_decision else {
                unreachable!("non-eligible decision must carry a skip reason");
            };
            response.diagnostics.push(memory_post_turn_skip_diagnostic(
                reason,
                input.status,
                eligibility_input.policy.as_available_policy(),
            ));
            return Ok(response);
        }
        let MemoryPostTurnEligibilityPolicy::Available(policy) = eligibility_input.policy else {
            unreachable!("eligible post-turn extraction requires a decoded policy");
        };
        let source_context_kind = eligibility_input.source_context_kind;

        if !config.provider_enabled {
            response
                .diagnostics
                .push(memory_post_turn_provider_skip_diagnostic(
                    "provider_disabled",
                    "memory post-turn extractor skipped: provider disabled",
                ));
            return Ok(response);
        }

        let Some(write_provider) = self.write_provider.as_ref() else {
            response
                .diagnostics
                .push(memory_post_turn_provider_skip_diagnostic(
                    "write_provider_unavailable",
                    "memory post-turn extractor skipped: write provider unavailable",
                ));
            return Ok(response);
        };
        let Some(extractor_provider) = self.extractor_provider.as_ref() else {
            response
                .diagnostics
                .push(memory_post_turn_provider_skip_diagnostic(
                    "provider_unavailable",
                    "memory post-turn extractor skipped: extractor provider unavailable",
                ));
            return Ok(response);
        };

        let context = memory_turn_context_from_post_turn_request(&request, input, &config)?;

        let manifest = match write_provider
            .load_memory_manifest(
                context.clone(),
                MemoryManifestRequest {
                    max_items: config.max_manifest_items,
                    max_item_chars: config.max_fact_content_chars,
                },
            )
            .await
        {
            Ok(manifest) => manifest,
            Err(_) => {
                response
                    .diagnostics
                    .push(memory_post_turn_provider_skip_diagnostic(
                        "manifest_failed",
                        "memory post-turn extractor skipped: manifest loading failed",
                    ));
                return Ok(response);
            }
        };

        let extractor_model = config.model.clone().or_else(|| input.model.clone());
        let extractor_model_provider = config
            .provider_name
            .clone()
            .or_else(|| input.model_provider.clone());
        let extractor_context = memory_post_turn_extractor_context_from_turn(
            &context,
            extractor_model.clone(),
            extractor_model_provider.clone(),
        );
        let extractor_request =
            memory_post_turn_extractor_request_from_input(input, manifest, &config);
        let raw_json = match extractor_provider
            .extract_post_turn_memory_json(extractor_context, extractor_request)
            .await
        {
            Ok(json) => json,
            Err(_) => {
                return Err(memory_retryable_safe_hook_error(
                    "memory.post_turn_extractor.provider_failed",
                    "memory post-turn extractor provider failed",
                ));
            }
        };

        let parsed = match parse_memory_post_turn_extractor_json(raw_json.as_str(), &config) {
            Ok(parsed) => parsed,
            Err(error) => {
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.post_turn_extractor.invalid_json",
                    format!("memory post-turn extractor returned invalid JSON: {error}"),
                ));
                return Ok(response);
            }
        };

        let mut stats = MemoryPostTurnExtractorStats {
            raw_fact_count: parsed.raw_fact_count,
            validation_rejected_count: parsed.validation_rejected_count,
            ..MemoryPostTurnExtractorStats::default()
        };
        response
            .diagnostics
            .extend(hook_diagnostics_from_strings(parsed.diagnostics.as_slice()));

        for (index, fact) in parsed.facts.into_iter().enumerate() {
            let Some(params) = memory_semantic_write_params_from_extracted_fact(
                index,
                fact,
                &context,
                &policy,
                &config,
                source_context_kind,
                extractor_model.as_deref(),
                extractor_model_provider.as_deref(),
            ) else {
                stats.validation_rejected_count += 1;
                continue;
            };
            if !post_turn_policy_allows_fact(&policy, &params.semantic) {
                stats.policy_rejected_count += 1;
                continue;
            }
            stats.write_attempt_count += 1;
            match write_provider
                .write_semantic_memory(context.clone(), params)
                .await
            {
                Ok(write_response) => {
                    stats.write_success_count += 1;
                    stats.observe_write_response(&write_response);
                }
                Err(_) => {
                    stats.write_failure_count += 1;
                    response.diagnostics.push(memory_safe_warning_diagnostic(
                        "memory.post_turn_extractor.write_failed",
                        "memory post-turn extractor semantic write failed",
                    ));
                }
            }
        }

        response
            .diagnostics
            .push(memory_post_turn_stats_diagnostic(&stats));
        response.metadata = memory_post_turn_stats_metadata(&stats);
        Ok(response)
    }
}
