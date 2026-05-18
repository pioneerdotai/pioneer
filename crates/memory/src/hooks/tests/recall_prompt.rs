use super::*;

#[tokio::test]
async fn memory_deterministic_recall_hook_contributes_prompt_context_from_policy_set() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = MemoryDeterministicRecallHook {
        memory_provider: provider.clone(),
    };

    let response = hook
        .execute(test_prompt_context_hook_request(memory_policy_set(
            &MemoryTurnPolicy::normal_default_allow(),
        )))
        .await
        .expect("recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    assert_eq!(provider.materialize_call_count(), 0);
    let contributions = response.contributions;
    assert_eq!(contributions.len(), 1);
    let HookContribution::PromptContext(context) = &contributions[0] else {
        panic!("recall hook should contribute prompt context only");
    };
    assert_eq!(
        context.contribution_id.as_str(),
        MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID
    );
    assert_eq!(context.domain.as_str(), MEMORY_POLICY_DOMAIN);
    assert!(context.content.as_str().contains("User likes Porto."));
    assert_eq!(context.source_refs.len(), 1);
    assert_eq!(context.source_refs[0].id.as_str(), "mem_city");
}

#[tokio::test]
async fn memory_deterministic_recall_hook_skips_without_allowed_policy() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = MemoryDeterministicRecallHook {
        memory_provider: provider.clone(),
    };

    let response = hook
        .execute(test_prompt_context_hook_request(memory_policy_set(
            &MemoryTurnPolicy::no_use(),
        )))
        .await
        .expect("recall hook executes");

    assert_eq!(provider.recall_call_count(), 0);
    assert!(response.contributions.is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.recall_omitted" && diagnostic.safe_for_user
    }));
}

#[tokio::test]
async fn memory_deterministic_recall_hook_skips_malformed_policy_safely() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = MemoryDeterministicRecallHook {
        memory_provider: provider.clone(),
    };

    let response = hook
        .execute(test_prompt_context_hook_request(
            malformed_memory_policy_set(),
        ))
        .await
        .expect("malformed policy is best-effort");

    assert_eq!(provider.recall_call_count(), 0);
    assert!(response.contributions.is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.policy_decode_failed" && diagnostic.safe_for_user
    }));
}

#[tokio::test]
async fn memory_deterministic_recall_hook_failure_is_safe_best_effort() {
    let provider = Arc::new(TestRecallMemoryProvider::failing_recall(
        "raw provider error must not leak",
    ));
    let hook = MemoryDeterministicRecallHook {
        memory_provider: provider.clone(),
    };

    let response = hook
        .execute(test_prompt_context_hook_request(memory_policy_set(
            &MemoryTurnPolicy::normal_default_allow(),
        )))
        .await
        .expect("recall failure is best-effort");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(response.contributions.is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.recall_failed"
            && diagnostic.safe_for_user
            && !diagnostic.message.as_str().contains("raw provider error")
    }));
}

#[tokio::test]
async fn active_memory_hook_contributes_read_only_prompt_context() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "continue the architecture work using prior project decisions and constraints",
        ))
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    assert_eq!(provider.materialize_call_count(), 0);
    let request = provider
        .recall_requests()
        .into_iter()
        .next()
        .expect("active recall request recorded");
    assert_eq!(request.top_k, Some(5));
    assert_eq!(request.max_chars, Some(1_500));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic
                .message
                .as_str()
                .contains("deterministic_sufficient=false")
    }));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.context_contributed"
    }));
    let contributions = response.contributions;
    assert_eq!(contributions.len(), 1);
    let HookContribution::PromptContext(context) = &contributions[0] else {
        panic!("active recall should contribute prompt context only");
    };
    assert_eq!(
        context.contribution_id.as_str(),
        MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID
    );
    assert_eq!(context.domain.as_str(), MEMORY_POLICY_DOMAIN);
    assert!(!context.content.as_str().contains("Active memory context:"));
    assert!(
        context
            .content
            .as_str()
            .contains("Use hooks for memory domains.")
    );
    assert_eq!(context.source_refs.len(), 1);
    assert_eq!(context.source_refs[0].id.as_str(), "mem_active_project");
}

#[tokio::test]
async fn active_memory_hook_runs_for_memory_sensitive_turns() {
    for input_text in [
        "continue the previous architecture implementation with the same constraints",
        "use my durable preferences and identity details when answering this",
        "apply the project decisions and history from our earlier work",
        "before answering, consider what we discussed in prior threads",
    ] {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig {
                max_queries: 1,
                ..MemoryActiveRecallConfig::default()
            },
        };

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                HookPromptContextSet::default(),
                input_text,
            ))
            .await
            .expect("active recall hook executes");

        assert_eq!(provider.recall_call_count(), 1, "{input_text}");
        assert!(
            response
                .contributions
                .iter()
                .any(|contribution| matches!(contribution, HookContribution::PromptContext(_))),
            "{input_text}"
        );
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.decision"
                && diagnostic.message.as_str().contains("status=run")
        }));
    }
}

#[tokio::test]
async fn active_memory_hook_uses_valid_strict_json_query_hints() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
            r#"{"status":"run","confidence":0.92,"queryHints":["project hook runtime constraints"],"diagnostics":["provider ok"]}"#,
        ))),
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "finish the architecture task",
        ))
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    let request = provider
        .recall_requests()
        .into_iter()
        .next()
        .expect("strict JSON query hint should drive recall");
    assert_eq!(request.query, "project hook runtime constraints");
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic.message.as_str().contains("reason=provider_run")
    }));
    assert!(
        response
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.message.as_str().contains("provider ok") })
    );
}

#[tokio::test]
async fn active_memory_hook_respects_policy_and_config_skips() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig::default(),
    };

    let no_use = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_use()),
            HookPromptContextSet::default(),
            "continue prior decisions",
        ))
        .await
        .expect("no-use policy is best-effort");
    assert!(no_use.contributions.is_empty());

    let mut active_disabled_policy = MemoryTurnPolicy::normal_default_allow();
    active_disabled_policy.active_memory = MemoryActiveContextPolicy::Disabled;
    let disabled_policy = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&active_disabled_policy),
            HookPromptContextSet::default(),
            "continue prior decisions",
        ))
        .await
        .expect("disabled active policy is best-effort");
    assert!(disabled_policy.contributions.is_empty());

    let deterministic_only = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::DeterministicOnly,
            ..MemoryActiveRecallConfig::default()
        },
    }
    .execute(test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue prior decisions",
    ))
    .await
    .expect("deterministic-only config is best-effort");
    assert!(deterministic_only.contributions.is_empty());

    assert_eq!(provider.recall_call_count(), 0);
}

#[tokio::test]
async fn active_memory_hook_runs_bounded_generic_recall_for_short_turns() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "как меня зовут?",
        ))
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    let request = provider
        .recall_requests()
        .into_iter()
        .next()
        .expect("active recall request recorded");
    assert_eq!(request.query, MEMORY_ACTIVE_RECALL_GENERIC_QUERY);
    assert!(request.categories.is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic.message.as_str().contains("status=run")
    }));
}

#[tokio::test]
async fn active_memory_hook_skips_when_deterministic_is_sufficient() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig::default(),
    };
    let deterministic_context = prompt_context_set_from_prompt_context_contribution(
        memory_recall_prompt_context_contribution(recalled_city_snapshot())
            .expect("deterministic context contribution"),
    );

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            deterministic_context,
            "continue the previous work with the same constraints",
        ))
        .await
        .expect("active recall hook executes");

    assert!(response.contributions.is_empty());
    assert_eq!(provider.recall_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.deterministic_sufficient"
    }));
}

#[tokio::test]
async fn active_memory_hook_deduplicates_deterministic_ids() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: None,
        config: MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::StrictDebug,
            max_queries: 1,
            deterministic_sufficient_min_items: 99,
            ..MemoryActiveRecallConfig::default()
        },
    };
    let deterministic_context = prompt_context_set_from_prompt_context_contribution(
        memory_recall_prompt_context_contribution(recalled_city_snapshot())
            .expect("deterministic context contribution"),
    );

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            deterministic_context,
            "continue the previous memory-dependent task",
        ))
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(response.contributions.is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.no_hits"
            && diagnostic.message.as_str().contains("non-duplicate")
    }));
}

#[tokio::test]
async fn active_memory_hook_ignores_malformed_internal_json() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
            "{not json",
        ))),
        config: MemoryActiveRecallConfig::default(),
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "continue the architecture work using prior project decisions and constraints",
        ))
        .await
        .expect("malformed provider json is best-effort");

    assert!(response.contributions.is_empty());
    assert_eq!(provider.recall_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("memory.active_recall.invalid_json")
    }));
}

#[tokio::test]
async fn memory_prompt_contract_hook_renders_from_prompt_context_and_compile_input() {
    let recall_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let recall_hook = MemoryDeterministicRecallHook {
        memory_provider: recall_provider,
    };
    let recall_response = recall_hook
        .execute(test_prompt_context_hook_request(memory_policy_set(
            &MemoryTurnPolicy::normal_default_allow(),
        )))
        .await
        .expect("recall hook executes");
    let prompt_context_set = prompt_context_set_from_response(recall_response);
    let hook = MemoryPromptContractHook;

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[
                MEMORY_FORGET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_SEARCH_TOOL,
            ],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");

    let content = prompt_section_content(response).expect("prompt section is rendered");
    assert!(content.contains(
        "Available memory tools: memory_search, memory_get, memory_remember, memory_forget."
    ));
    assert!(content.contains("User likes Porto."));
    assert!(content.contains("Call memory_remember proactively"));
}

#[tokio::test]
async fn memory_prompt_contract_consumes_active_context_allowlist() {
    let hook = MemoryPromptContractHook;
    let active_context = memory_active_recall_prompt_context_contribution(
        active_project_snapshot().items,
        false,
        &MemoryActiveRecallConfig::default(),
    )
    .expect("active prompt context contribution");
    let unrelated_memory_context = PromptContextContribution {
        contribution_id: HookContributionId::new("memory.unrelated.context")
            .expect("valid contribution id"),
        domain: memory_policy_domain(),
        priority: 480,
        content: HookPromptContent::new("Unrelated memory-domain context must stay out.")
            .expect("valid prompt content"),
        max_chars: Some(500),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [active_context, unrelated_memory_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");

    let content = prompt_section_content(response).expect("prompt section is rendered");
    assert!(content.contains("Active memory context:"));
    assert!(content.contains("Use hooks for memory domains."));
    assert!(!content.contains("Unrelated memory-domain context"));
}

#[tokio::test]
async fn deterministic_only_recall_omits_active_heading() {
    let hook = MemoryPromptContractHook;
    let deterministic_context = memory_recall_prompt_context_contribution(recalled_city_snapshot())
        .expect("deterministic prompt context");
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [deterministic_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");

    let content = prompt_section_content(response).expect("prompt section is rendered");
    assert!(content.contains("Relevant memories:"));
    assert!(content.contains("User likes Porto."));
    assert!(!content.contains("Active memory context:"));
}

#[tokio::test]
async fn duplicate_active_memory_id_is_suppressed() {
    let hook = MemoryPromptContractHook;
    let deterministic_context = memory_recall_prompt_context_contribution(recalled_city_snapshot())
        .expect("deterministic prompt context");
    let active_context = memory_active_recall_prompt_context_contribution(
        recalled_city_snapshot().items,
        false,
        &MemoryActiveRecallConfig::default(),
    )
    .expect("active prompt context");
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [deterministic_context, active_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.prompt_recall.dedup"
            && diagnostic.message.as_str().contains("active_raw_count=1")
            && diagnostic
                .message
                .as_str()
                .contains("active_duplicate_count=1")
            && diagnostic
                .message
                .as_str()
                .contains("active_rendered_count=0")
            && diagnostic
                .message
                .as_str()
                .contains("active_duplicate_only=true")
    }));
    let content = prompt_section_content(response).expect("prompt section is rendered");
    assert_eq!(content.matches("User likes Porto.").count(), 1);
    assert!(!content.contains("Active memory context:"));
}

#[tokio::test]
async fn mixed_active_duplicates_keep_only_unique_context() {
    let hook = MemoryPromptContractHook;
    let deterministic_context = memory_recall_prompt_context_contribution(recalled_city_snapshot())
        .expect("deterministic prompt context");
    let mut active_items = recalled_city_snapshot().items;
    active_items.extend(active_project_snapshot().items);
    let active_context = memory_active_recall_prompt_context_contribution(
        active_items,
        false,
        &MemoryActiveRecallConfig::default(),
    )
    .expect("active prompt context");
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [deterministic_context, active_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");
    let content = prompt_section_content(response).expect("prompt section is rendered");

    assert_eq!(content.matches("User likes Porto.").count(), 1);
    assert!(content.contains("Active memory context:"));
    assert!(content.contains("Use hooks for memory domains."));
    let active_section = content
        .split("Active memory context:")
        .nth(1)
        .expect("active section should render");
    assert!(!active_section.contains("mem_city"));
}

#[tokio::test]
async fn exact_active_line_duplicate_is_suppressed() {
    let hook = MemoryPromptContractHook;
    let deterministic_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: memory_policy_domain(),
        priority: 500,
        content: HookPromptContent::new("Shared synthesized line.").expect("valid prompt content"),
        max_chars: Some(500),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let active_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: memory_policy_domain(),
        priority: 490,
        content: HookPromptContent::new("Shared synthesized line.").expect("valid prompt content"),
        max_chars: Some(500),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [deterministic_context, active_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");
    let content = prompt_section_content(response).expect("prompt section is rendered");

    assert_eq!(content.matches("Shared synthesized line.").count(), 1);
    assert!(!content.contains("Active memory context:"));
}

#[tokio::test]
async fn active_synthesis_context_is_kept_when_unique() {
    let hook = MemoryPromptContractHook;
    let deterministic_context = memory_recall_prompt_context_contribution(recalled_city_snapshot())
        .expect("deterministic prompt context");
    let active_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: memory_policy_domain(),
        priority: 490,
        content: HookPromptContent::new("User is continuing Pioneer memory architecture work.")
            .expect("valid prompt content"),
        max_chars: Some(500),
        source_refs: vec![memory_source_ref("mem_city")],
        diagnostics: Vec::new(),
        truncated: false,
    };
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [deterministic_context, active_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.prompt_recall.dedup"
            && diagnostic
                .message
                .as_str()
                .contains("active_synthesis_rendered=true")
    }));
    let content = prompt_section_content(response).expect("prompt section is rendered");

    assert!(content.contains("Relevant memories:"));
    assert!(content.contains("Active memory context:"));
    assert!(content.contains("User is continuing Pioneer memory architecture work."));
}

#[tokio::test]
async fn memory_prompt_contract_hook_policy_and_tool_visibility_matrix() {
    let hook = MemoryPromptContractHook;
    let prompt_context_set = HookPromptContextSet::default();

    let no_use = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_use()),
            true,
            &[MEMORY_SEARCH_TOOL],
            prompt_context_set.clone(),
        ))
        .await
        .expect("no-use policy is best-effort");
    assert!(prompt_section_content(no_use).is_none());

    let no_tools = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &["exec_command"],
            prompt_context_set.clone(),
        ))
        .await
        .expect("no visible memory tools is best-effort");
    assert!(prompt_section_content(no_tools).is_none());

    let no_provider_tool_calling = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            false,
            &[MEMORY_SEARCH_TOOL],
            prompt_context_set.clone(),
        ))
        .await
        .expect("provider tool-calling disabled is best-effort");
    assert!(prompt_section_content(no_provider_tool_calling).is_none());

    let malformed = hook
        .execute(test_prompt_compile_hook_request(
            malformed_memory_policy_set(),
            true,
            &[MEMORY_SEARCH_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("malformed policy is best-effort");
    assert!(prompt_section_content(malformed).is_none());
}

#[tokio::test]
async fn memory_prompt_contract_hook_renders_no_save_and_forget_contracts() {
    let hook = MemoryPromptContractHook;

    let no_save = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_save()),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
            HookPromptContextSet::default(),
        ))
        .await
        .expect("no-save prompt contract executes");
    let no_save_content = prompt_section_content(no_save).expect("no-save section renders");
    assert!(no_save_content.contains("Memory writes are disabled for this turn"));
    assert!(no_save_content.contains("Do not store, update, infer, or extract new memories"));
    assert!(!no_save_content.contains("memory_remember"));

    let forget = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::explicit_forget(Some(
                "birthday".to_owned(),
            ))),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
            HookPromptContextSet::default(),
        ))
        .await
        .expect("forget prompt contract executes");
    let forget_content = prompt_section_content(forget).expect("forget section renders");
    assert!(
        forget_content.contains("If the user asks you to forget something, call memory_forget.")
    );
    assert!(forget_content.contains("only to identify and forget"));
    assert!(!forget_content.contains("memory_remember"));
}
