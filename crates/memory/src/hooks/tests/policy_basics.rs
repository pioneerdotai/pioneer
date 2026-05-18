use super::*;

#[test]
fn memory_active_recall_config_defaults_to_bounded_hybrid() {
    let config = MemoryLoopConfig::default().normalized();

    assert_eq!(config.active_recall.mode, MemoryActiveRecallMode::Hybrid);
    assert!(config.active_recall.timeout_ms > 0);
    assert!(config.active_recall.max_queries > 0);
    assert!(config.active_recall.top_k_per_query > 0);
    assert!(config.active_recall.max_prompt_chars > 0);
    assert!(config.active_recall.planner.enabled);
    assert!(config.active_recall.planner.timeout_ms > 0);
    assert!(config.active_recall.planner.max_input_chars > 0);
    assert!(config.active_recall.planner.max_output_chars > 0);
    assert_eq!(
        config.active_recall.planner.fallback,
        MemoryActiveRecallPlannerFallbackPolicy::Deterministic
    );

    let zero = MemoryActiveRecallConfig {
        timeout_ms: 0,
        max_queries: 0,
        top_k_per_query: 0,
        max_prompt_chars: 0,
        deterministic_sufficient_min_items: 0,
        deterministic_sufficient_min_chars: 0,
        ..MemoryActiveRecallConfig::default()
    }
    .normalized();
    assert_eq!(zero.timeout_ms, 1);
    assert_eq!(zero.max_queries, 1);
    assert_eq!(zero.top_k_per_query, 1);
    assert_eq!(zero.max_prompt_chars, 1);
    assert!(zero.planner.enabled);
    assert_eq!(zero.planner.timeout_ms, 8_000);
    assert_eq!(zero.planner.max_input_chars, 4_000);
    assert_eq!(zero.planner.max_output_chars, 2_000);
}

#[test]
fn memory_active_recall_planner_config_normalizes_bounds_and_names() {
    let config = MemoryActiveRecallConfig {
        planner: MemoryActiveRecallPlannerConfig {
            enabled: true,
            provider_name: Some("  provider-name-that-is-kept  ".to_owned()),
            model: Some("  model-name-that-is-kept  ".to_owned()),
            timeout_ms: 0,
            max_input_chars: 0,
            max_output_chars: 0,
            fallback: MemoryActiveRecallPlannerFallbackPolicy::SkipActiveRecall,
        },
        ..MemoryActiveRecallConfig::default()
    }
    .normalized();

    assert_eq!(
        config.mode.as_str(),
        MemoryActiveRecallMode::Hybrid.as_str()
    );
    assert_eq!(
        config.planner.provider_name.as_deref(),
        Some("provider-name-that-is-kept")
    );
    assert_eq!(
        config.planner.model.as_deref(),
        Some("model-name-that-is-kept")
    );
    assert_eq!(config.planner.timeout_ms, 1);
    assert_eq!(config.planner.max_input_chars, 1);
    assert_eq!(config.planner.max_output_chars, 1);
    assert_eq!(
        config.planner.fallback,
        MemoryActiveRecallPlannerFallbackPolicy::SkipActiveRecall
    );
}

#[test]
fn memory_turn_policy_constructors_have_separate_controls() {
    let default_policy = MemoryTurnPolicy::normal_default_allow();
    assert!(default_policy.allow_pre_turn_recall());
    assert!(default_policy.allows_memory_tool(MEMORY_SEARCH_TOOL));
    assert!(default_policy.allows_memory_tool(MEMORY_REMEMBER_TOOL));
    assert_eq!(default_policy.detected_language, None);
    assert_eq!(
        default_policy.post_turn_extraction,
        MemoryExtractionPolicy::Allow
    );
    assert_eq!(
        default_policy.active_memory,
        MemoryActiveContextPolicy::Allow
    );

    let no_save = MemoryTurnPolicy::no_save();
    assert!(no_save.allow_pre_turn_recall());
    assert!(no_save.allows_memory_tool(MEMORY_SEARCH_TOOL));
    assert!(!no_save.allows_memory_tool(MEMORY_REMEMBER_TOOL));
    assert!(no_save.allows_memory_tool(MEMORY_FORGET_TOOL));
    assert_eq!(no_save.active_memory, MemoryActiveContextPolicy::Allow);

    let forget = MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned()));
    assert!(!forget.allow_pre_turn_recall());
    assert!(forget.allows_memory_tool(MEMORY_SEARCH_TOOL));
    assert!(forget.allows_memory_tool(MEMORY_GET_TOOL));
    assert!(!forget.allows_memory_tool(MEMORY_REMEMBER_TOOL));
    assert!(forget.allows_memory_tool(MEMORY_FORGET_TOOL));
}

#[test]
fn permissive_classifier_fallback_contract_keeps_explicit_memory_available() {
    let policy = MemoryTurnPolicy::permissive_classifier_fallback(
        MemoryPolicyReasonCode::ClassifierUnavailable,
    );

    assert_eq!(policy.source, MemoryPolicySource::DefaultFallback);
    assert_eq!(
        policy.reason_code,
        MemoryPolicyReasonCode::ClassifierUnavailable
    );
    assert_eq!(policy.confidence, 0.0);
    assert!(policy.allow_pre_turn_recall());
    assert_eq!(policy.prompt, MemoryPromptPolicy::Full);
    assert!(policy.allow_memory_prompt());
    assert_eq!(policy.read_tools, MemoryReadToolPolicy::Allow);
    assert!(policy.allows_memory_tool(MEMORY_SEARCH_TOOL));
    assert!(policy.allows_memory_tool(MEMORY_GET_TOOL));
    assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Allow);
    assert!(policy.allows_memory_tool(MEMORY_REMEMBER_TOOL));
    assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Allow);
    assert!(policy.allows_memory_tool(MEMORY_FORGET_TOOL));
    assert_eq!(policy.post_turn_extraction, MemoryExtractionPolicy::Allow);
    assert_eq!(policy.active_memory, MemoryActiveContextPolicy::Allow);
    assert!(!policy.explicit_remember);
    assert!(!policy.explicit_forget);
}

#[test]
fn memory_turn_policy_hook_value_roundtrips_full_policy() {
    let policy = MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned()))
        .with_detected_language(Some("ru".to_owned()))
        .with_diagnostic("memory.policy.resolved: safe");

    let value = memory_turn_policy_to_hook_value(&policy);
    let decoded = memory_turn_policy_from_hook_value(&value).expect("policy hook value decodes");

    assert_eq!(decoded, policy);
    let HookValue::Object(object) = value else {
        panic!("policy should be encoded as object");
    };
    assert!(object.contains_key(&hook_metadata_key("recall")));
    assert!(object.contains_key(&hook_metadata_key("remember_tool")));
    assert!(object.contains_key(&hook_metadata_key("detected_language")));
    assert!(object.contains_key(&hook_metadata_key("diagnostics_summary")));
}

#[test]
fn memory_policy_contribution_emits_full_policy_object() {
    let policy = MemoryTurnPolicy::no_save().with_detected_language(Some("de".to_owned()));
    let contribution = memory_policy_contribution(&policy);

    assert_eq!(contribution.domain.as_str(), MEMORY_POLICY_DOMAIN);
    assert_eq!(contribution.key.as_str(), MEMORY_TURN_POLICY_KEY);
    let decoded = memory_turn_policy_from_hook_value(&contribution.value)
        .expect("contribution should contain full policy");
    assert_eq!(decoded, policy);
    assert_ne!(
        contribution.value,
        HookValue::Text(MemoryPolicyReasonCode::MemoryNoSave.as_str().to_owned())
    );
}

#[test]
fn memory_turn_policy_decodes_from_hook_policy_set() {
    let policy = MemoryTurnPolicy::explicit_remember().with_detected_language(Some("es".into()));
    let set = HookPolicySet::merge_contributions([memory_policy_contribution(&policy)]);

    let decoded = memory_turn_policy_from_hook_policy_set(&set)
        .expect("memory policy entry exists")
        .expect("memory policy entry decodes");

    assert_eq!(decoded, policy);
}

#[test]
fn memory_turn_policy_from_hook_policy_set_reports_malformed_policy() {
    let malformed = PolicyContribution {
        domain: memory_policy_domain(),
        key: memory_turn_policy_key(),
        value: HookValue::Text("memory_no_use".to_owned()),
        priority: 500,
        diagnostics: Vec::new(),
    };
    let set = HookPolicySet::merge_contributions([malformed]);

    let decoded =
        memory_turn_policy_from_hook_policy_set(&set).expect("memory policy entry exists");

    assert!(decoded.is_err());
}
