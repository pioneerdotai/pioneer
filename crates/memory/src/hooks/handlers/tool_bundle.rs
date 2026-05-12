use super::*;

pub(in crate::hooks) struct MemoryToolBundleHook {
    pub(in crate::hooks) memory_provider: Arc<dyn AgentMemoryProvider>,
    pub(in crate::hooks) state: Arc<MemoryHookTurnStateStore>,
    pub(in crate::hooks) tool_bundle_artifacts: Arc<dyn MemoryToolBundleArtifactStore>,
}

#[async_trait::async_trait]
impl HookHandler for MemoryToolBundleHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_TOOL_BUNDLE_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPreToolMaterialization]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_tool_bundle_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let Some(state) = self.state.state(&request) else {
            return Ok(memory_missing_state_response(MEMORY_TOOL_BUNDLE_HOOK_ID));
        };
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory tool bundle skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(MEMORY_TOOL_BUNDLE_HOOK_ID));
            }
        };
        if !turn_pre_tool_materialization_allows_tools(&request) {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                "memory tool bundle skipped: provider_tool_calling=false",
            ));
            return Ok(response);
        }
        if !policy.allows_any_memory_tool() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                format!(
                    "memory tool bundle skipped: no tools allowed by policy source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let mut materialization = match self
            .memory_provider
            .materialize_memory_tools(state.context.clone())
            .await
        {
            Ok(materialization) => filter_memory_tool_materialization(materialization, &policy),
            Err(error) => {
                let mut response = HookHandlerResponse::default();
                let _ = error;
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.tools_failed",
                    "memory tool bundle materialization failed",
                ));
                return Ok(response);
            }
        };

        let mut response = HookHandlerResponse::default();
        response.diagnostics.extend(hook_diagnostics_from_strings(
            materialization.diagnostics.as_slice(),
        ));
        if materialization.bundles.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                format!(
                    "memory tool bundle skipped: materializer returned no exposed tools source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }
        for (index, bundle) in materialization.bundles.drain(..).enumerate() {
            let bundle_id =
                HookToolBundleId::new(format!("{MEMORY_TOOL_BUNDLE_ID_PREFIX}.{index}"))
                    .expect("static bundle id is valid");
            self.tool_bundle_artifacts.insert_tool_bundle_artifact(
                state.context.turn_id.as_str(),
                bundle_id.clone(),
                bundle.clone(),
            );
            response.contributions.push(HookContribution::ToolBundle(
                memory_tool_bundle_contribution(index, bundle_id, &bundle, &policy),
            ));
        }
        Ok(response)
    }
}
