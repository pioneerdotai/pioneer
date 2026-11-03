use super::*;
#[tokio::test]
async fn local_policy_preserves_structured_override_and_state() {
    let hook = MemoryPolicyHook {
        state: Arc::new(MemoryHookTurnStateStore::default()),
    };
    let mut metadata = HookMetadata::default();
    metadata.insert(
        hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY),
        memory_turn_policy_to_hook_value(&MemoryTurnPolicy::no_save()),
    );
    let request = test_policy_hook_request(metadata);
    let response = hook.execute(request.clone()).await.expect("local policy");
    let policy = response
        .contributions
        .iter()
        .find_map(|contribution| match contribution {
            HookContribution::Policy(policy) => {
                Some(memory_turn_policy_from_hook_value(&policy.value).expect("policy"))
            }
            _ => None,
        })
        .expect("policy contribution");
    assert_eq!(policy.read_tools, MemoryReadToolPolicy::Allow);
    assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Disabled);
    assert_eq!(
        policy.post_turn_extraction,
        MemoryExtractionPolicy::Disabled
    );
    assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
    assert!(hook.state.state(&request).is_some());
}

#[tokio::test]
async fn memory_package_pre_prompt_phase_has_no_retrieval_or_context() {
    let provider = Arc::new(TestMemoryProvider::failing(
        "must not materialize before prompt",
    ));
    let runtime = HookRuntimeBuilder::new()
        .install(package(
            provider.clone(),
            None,
            None,
            Arc::new(TestToolBundleArtifactStore::new()),
            MemoryLoopConfig::default(),
        ))
        .expect("package installs")
        .build();
    let response = runtime
        .run_phase(HookPhaseRequest::new(
            HookPhase::TurnPrePromptContext,
            HookContext::default(),
            HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                "Continue using what you remember",
                Some("model"),
                Some("provider"),
            )),
        ))
        .await
        .expect("phase executes");
    assert!(response.runs.is_empty());
    assert!(response.contributions.is_empty());
    assert_eq!(provider.materialize_call_count(), 0);
}

#[tokio::test]
async fn memory_prompt_contract_preserves_visible_tools_and_policy() {
    let response = MemoryPromptContractHook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_save()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
            HookPromptContextSet::default(),
        ))
        .await
        .expect("contract renders");
    let section = response
        .contributions
        .iter()
        .find_map(|value| match value {
            HookContribution::PromptSection(section) => Some(section),
            _ => None,
        })
        .expect("tool contract");
    for guidance in [
        "Call memory_search when prior memory is relevant",
        "Use memory_get after memory_search",
        "If the user asks you to forget something, call memory_forget.",
    ] {
        assert!(section.content.as_str().contains(guidance));
    }
    assert!(
        !section
            .content
            .as_str()
            .contains("Call memory_remember proactively")
    );
    assert!(
        section
            .content
            .as_str()
            .contains("Memory writes are disabled")
    );
    assert_no_prompt_context_contributions(&response);
}

#[tokio::test]
async fn memory_contract_does_not_reinject_legacy_recall_context() {
    let context = HookPromptContextSet::aggregate_contributions(
        [PromptContextContribution {
            contribution_id: HookContributionId::new("memory.active_recall.context").unwrap(),
            domain: HookDomain::new("memory").unwrap(),
            priority: 500,
            content: HookPromptContent::new("LEGACY_RECALLED_PRIVATE_FACT").unwrap(),
            max_chars: None,
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        }],
        HookPromptContextLimits::default(),
    );
    let response = MemoryPromptContractHook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL],
            context,
        ))
        .await
        .expect("contract renders");
    assert!(response.contributions.iter().all(|value| {
        match value {
            HookContribution::PromptSection(section) => !section
                .content
                .as_str()
                .contains("LEGACY_RECALLED_PRIVATE_FACT"),
            HookContribution::PromptContext(_) => false,
            _ => true,
        }
    }));
}
