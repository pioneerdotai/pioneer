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
        memory_post_turn_extractor_capabilities(
            &self.config,
            self.write_provider.is_some(),
            self.extractor_provider.is_some(),
        )
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_post_turn_input(&request)?;
        let config = self.config.normalized();
        let durable_terminal_effect = memory_durable_terminal_effect_claim(&request.context)?;
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
            if durable_terminal_effect.is_some() {
                return Err(memory_retryable_safe_hook_error(
                    "memory.post_turn_extractor.write_provider_unavailable",
                    "memory post-turn extractor write provider is unavailable",
                ));
            }
            response
                .diagnostics
                .push(memory_post_turn_provider_skip_diagnostic(
                    "write_provider_unavailable",
                    "memory post-turn extractor skipped: write provider unavailable",
                ));
            return Ok(response);
        };
        let Some(extractor_provider) = self.extractor_provider.as_ref() else {
            if durable_terminal_effect.is_some() {
                return Err(memory_retryable_safe_hook_error(
                    "memory.post_turn_extractor.provider_unavailable",
                    "memory post-turn extractor provider is unavailable",
                ));
            }
            response
                .diagnostics
                .push(memory_post_turn_provider_skip_diagnostic(
                    "provider_unavailable",
                    "memory post-turn extractor skipped: extractor provider unavailable",
                ));
            return Ok(response);
        };

        let context = memory_turn_context_from_post_turn_request(&request, input, &config)?;

        // External runtimes have no ProviderRegistry-backed thread model.
        // Do not invent a fallback provider (or retry an invalid configuration).
        if config.model.as_ref().or(input.model.as_ref()).is_none()
            || config
                .provider_name
                .as_ref()
                .or(input.model_provider.as_ref())
                .is_none()
        {
            response.diagnostics.push(memory_safe_warning_diagnostic(
                "memory.post_turn_extractor.model_unavailable",
                "Post-turn extraction requires an API model: select a custom proactive-writes model when the turn runs through CLI.",
            ));
            return Ok(response);
        }

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
            Err(error) => {
                tracing::error!(
                    target: "pioneer::memory_post_turn_extractor",
                    stage = "manifest_load",
                    durable = durable_terminal_effect.is_some(),
                    error = %error,
                    "memory post-turn extractor manifest load failed"
                );
                if durable_terminal_effect.is_some() {
                    return Err(memory_retryable_safe_hook_error(
                        "memory.post_turn_extractor.manifest_failed",
                        "memory post-turn extractor manifest loading failed",
                    ));
                }
                response
                    .diagnostics
                    .push(memory_post_turn_provider_skip_diagnostic(
                        "manifest_failed",
                        "memory post-turn extractor skipped: manifest loading failed",
                    ));
                return Ok(response);
            }
        };

        let extractor_context = memory_post_turn_extractor_context_from_turn(
            &context,
            config.model.clone().or_else(|| input.model.clone()),
            config
                .provider_name
                .clone()
                .or_else(|| input.model_provider.clone()),
            durable_terminal_effect.clone(),
        );
        let extractor_request =
            memory_post_turn_extractor_request_from_input(input, manifest, &config);

        let extraction = match extract_post_turn_memory_once(
            extractor_provider.as_ref(),
            extractor_context,
            extractor_request,
            &config,
        )
        .await
        {
            Ok(extraction) => extraction,
            Err(MemoryPostTurnExtractorFailure::ProviderFailed) => {
                return Err(memory_retryable_safe_hook_error(
                    "memory.post_turn_extractor.provider_failed",
                    "memory post-turn extractor provider failed",
                ));
            }
            Err(MemoryPostTurnExtractorFailure::InvalidJson(error)) => {
                if durable_terminal_effect.is_some() {
                    return Err(memory_hook_error(
                        "memory.post_turn_extractor.invalid_json",
                        format!("memory post-turn extractor returned invalid JSON: {error}"),
                    ));
                }
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.post_turn_extractor.invalid_json",
                    format!("memory post-turn extractor returned invalid JSON: {error}"),
                ));
                return Ok(response);
            }
        };
        let extractor_model = extraction.model;
        let extractor_model_provider = extraction.model_provider;
        let parsed = extraction.parsed;

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
                Err(error) => {
                    stats.write_failure_count += 1;
                    tracing::error!(
                        target: "pioneer::memory_post_turn_extractor",
                        stage = "semantic_write",
                        durable = durable_terminal_effect.is_some(),
                        extractor_provider = ?extractor_model_provider.as_deref(),
                        extractor_model = ?extractor_model.as_deref(),
                        fact_index = index,
                        error = %error,
                        "memory post-turn extractor semantic write failed"
                    );
                    response.diagnostics.push(memory_safe_warning_diagnostic(
                        "memory.post_turn_extractor.write_failed",
                        "memory post-turn extractor semantic write failed",
                    ));
                }
            }
        }

        if durable_terminal_effect.is_some() && stats.write_failure_count > 0 {
            return Err(memory_retryable_safe_hook_error(
                "memory.post_turn_extractor.write_failed",
                "memory post-turn extractor failed to persist one or more semantic writes",
            ));
        }

        response
            .diagnostics
            .push(memory_post_turn_stats_diagnostic(&stats));
        response.metadata = memory_post_turn_stats_metadata(&stats);
        Ok(response)
    }
}

struct MemoryPostTurnExtractionOutcome {
    parsed: MemoryPostTurnParsedFacts,
    model: Option<String>,
    model_provider: Option<String>,
}

enum MemoryPostTurnExtractorFailure {
    ProviderFailed,
    InvalidJson(String),
}

async fn extract_post_turn_memory_once(
    extractor_provider: &dyn AgentMemoryPostTurnExtractorProvider,
    context: MemoryPostTurnExtractorContext,
    request: MemoryPostTurnExtractorRequest,
    config: &MemoryPostTurnExtractorConfig,
) -> Result<MemoryPostTurnExtractionOutcome, MemoryPostTurnExtractorFailure> {
    let raw_json = extractor_provider
        .extract_post_turn_memory_json(context.clone(), request)
        .await
        .map_err(|error| {
            tracing::error!(
                target: "pioneer::memory_post_turn_extractor",
                stage = "extractor_provider",
                durable = context.durable_terminal_effect.is_some(),
                provider = ?context.model_provider.as_deref(),
                model = ?context.model.as_deref(),
                error = %error,
                "memory post-turn extractor provider execution failed"
            );
            MemoryPostTurnExtractorFailure::ProviderFailed
        })?;
    let parsed =
        parse_memory_post_turn_extractor_json(raw_json.as_str(), config).map_err(|error| {
            tracing::error!(
                target: "pioneer::memory_post_turn_extractor",
                stage = "response_json_parse",
                durable = context.durable_terminal_effect.is_some(),
                provider = ?context.model_provider.as_deref(),
                model = ?context.model.as_deref(),
                response_bytes = raw_json.len(),
                response_sha256 = %post_turn_extractor_response_sha256(raw_json.as_str()),
                error = %error,
                "memory post-turn extractor response JSON parse failed"
            );
            MemoryPostTurnExtractorFailure::InvalidJson(error)
        })?;
    Ok(MemoryPostTurnExtractionOutcome {
        parsed,
        model: context.model,
        model_provider: context.model_provider,
    })
}

fn post_turn_extractor_response_sha256(response: &str) -> String {
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(response.as_bytes()))
}
