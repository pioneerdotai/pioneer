use super::*;

pub(in crate::hooks) struct MemoryPolicyClassifierHook {
    pub(in crate::hooks) policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    pub(in crate::hooks) state: Arc<MemoryHookTurnStateStore>,
}

#[async_trait::async_trait]
impl HookHandler for MemoryPolicyClassifierHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_POLICY_CLASSIFIER_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePolicy]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_policy_classifier_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_policy_input(&request)?;
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

        let context = MemoryTurnPolicyContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            mode: ThreadMode::Agent,
            input_text: input.input_text.clone(),
            model: input.model.clone(),
            model_provider: input.model_provider.clone(),
        };
        let (policy_request, request_diagnostics) =
            memory_turn_policy_request_from_metadata(&request.context.metadata);
        let policy =
            resolve_memory_turn_policy(self.policy_provider.as_ref(), context, policy_request)
                .await;
        let turn_context = MemoryTurnContext {
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
        };
        self.state.set_turn_context(turn_context);

        let mut response = HookHandlerResponse::default();
        response.diagnostics.extend(hook_diagnostics_from_strings(
            request_diagnostics.as_slice(),
        ));
        response
            .diagnostics
            .extend(hook_diagnostics_from_strings(policy.diagnostics.as_slice()));
        response
            .contributions
            .push(HookContribution::Policy(memory_policy_contribution(
                &policy,
            )));
        Ok(response)
    }
}
