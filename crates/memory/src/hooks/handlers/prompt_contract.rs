use super::*;

pub(in crate::hooks) struct MemoryPromptContractHook;

#[async_trait::async_trait]
impl HookHandler for MemoryPromptContractHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_PROMPT_CONTRACT_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptCompile]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_prompt_contract_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_compile_input(&request)?;
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory prompt contract skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(
                    MEMORY_PROMPT_CONTRACT_HOOK_ID,
                ));
            }
        };
        if !input.provider_tool_calling {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                "memory prompt contract skipped: provider_tool_calling=false",
            ));
            return Ok(response);
        }
        if !policy.allow_memory_prompt() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                format!(
                    "memory prompt contract skipped: source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let available_tool_names = memory_tool_names_from_prompt_compile_input(input);
        if available_tool_names.is_empty() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                "memory prompt contract skipped: no visible memory tools",
            ));
            return Ok(response);
        }

        let Some(prompt_policy) = policy.recall_prompt_policy() else {
            return Ok(HookHandlerResponse::default());
        };
        let recall_context = memory_recall_context_from_prompt_context_set(
            &request.prompt_context_set,
            prompt_policy,
        );
        let mut response = HookHandlerResponse::default();
        if let Some(contribution) = memory_recall_prompt_section_contribution_from_context(
            available_tool_names,
            prompt_policy,
            recall_context.clone(),
            recall_context.truncated,
        ) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_rendered",
                format!(
                    "memory prompt contract rendered: source={} reason={} recalled_contexts={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str(),
                    recall_context.count
                ),
            ));
            response
                .diagnostics
                .push(memory_prompt_recall_dedup_diagnostic(&recall_context));
            response.contributions.push(contribution);
        }
        Ok(response)
    }
}
