use super::*;

pub(in crate::hooks) struct MemoryDeterministicRecallHook {
    pub(in crate::hooks) memory_provider: Arc<dyn AgentMemoryProvider>,
}

#[async_trait::async_trait]
impl HookHandler for MemoryDeterministicRecallHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_DETERMINISTIC_RECALL_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_deterministic_recall_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_context_input(&request)?;
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory deterministic recall skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(
                    MEMORY_DETERMINISTIC_RECALL_HOOK_ID,
                ));
            }
        };
        if !policy.allow_pre_turn_recall() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.recall_omitted",
                format!(
                    "memory deterministic recall skipped: source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let context = memory_turn_context_from_prompt_context_request(&request, input)?;
        let mut response = HookHandlerResponse::default();
        let recall_snapshot = match self
            .memory_provider
            .recall_memory(
                context.clone(),
                memory_recall_request(context.input_text.as_str()),
            )
            .await
        {
            Ok(snapshot) => {
                response.diagnostics.extend(hook_diagnostics_from_strings(
                    snapshot.diagnostics.as_slice(),
                ));
                snapshot
            }
            Err(error) => {
                let _ = error;
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.recall_failed",
                    "memory deterministic recall failed",
                ));
                return Ok(response);
            }
        };

        if let Some(contribution) = memory_recall_prompt_context_contribution(recall_snapshot) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.recall_context_contributed",
                "memory deterministic recall contributed prompt context",
            ));
            response
                .contributions
                .push(HookContribution::PromptContext(contribution));
        }
        Ok(response)
    }
}
