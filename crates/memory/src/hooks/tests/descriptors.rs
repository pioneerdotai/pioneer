use super::*;

#[test]
fn memory_local_policy_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryPolicyHook {
        state: Arc::new(MemoryHookTurnStateStore::default()),
    };

    assert_eq!(hook.id().as_str(), MEMORY_POLICY_HOOK_ID);
    assert_eq!(hook.supported_phases(), vec![HookPhase::TurnPrePolicy]);
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        )
    );
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );
    assert!(!capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
}

#[test]
fn memory_tool_bundle_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryToolBundleHook {
        memory_provider: Arc::new(TestMemoryProvider::with_materialization(
            empty_tool_materialization(),
        )),
        state: Arc::new(MemoryHookTurnStateStore::default()),
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    };

    assert_eq!(hook.id().as_str(), MEMORY_TOOL_BUNDLE_HOOK_ID);
    assert_eq!(
        hook.supported_phases(),
        vec![HookPhase::TurnPreToolMaterialization]
    );
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(&HookCapability::new("memory").expect("static capability is valid"))
    );
    assert!(capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
    assert!(
        !capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        )
    );
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
}

#[test]
fn memory_hook_package_respects_product_feature_switches() {
    let runtime = Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ));
    install_memory_hook_package_for_test(
        &runtime,
        Arc::new(TestMemoryProvider::with_materialization(
            empty_tool_materialization(),
        )),
        Some(Arc::new(TestMemoryWriteProvider::default())),
        Some(Arc::new(TestPostTurnExtractorProvider::json(
            r#"{"facts":[]}"#,
        ))),
        Arc::new(TestToolBundleArtifactStore::new()),
        MemoryLoopConfig {
            tools_enabled: false,
            post_turn_extractor: MemoryPostTurnExtractorConfig {
                enabled: false,
                ..MemoryPostTurnExtractorConfig::default()
            },
            ..MemoryLoopConfig::default()
        },
    )
    .expect("memory hooks install");

    assert_subscription_registered(&runtime, MEMORY_POLICY_SUBSCRIPTION_ID);
    assert_subscription_registered(&runtime, MEMORY_PROMPT_CONTRACT_SUBSCRIPTION_ID);
    assert_subscription_missing(&runtime, MEMORY_TOOL_BUNDLE_SUBSCRIPTION_ID);
    assert_subscription_missing(&runtime, MEMORY_POST_TURN_EXTRACTOR_SUBSCRIPTION_ID);
}

#[test]
fn memory_prompt_contract_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryPromptContractHook;

    assert_eq!(hook.id().as_str(), MEMORY_PROMPT_CONTRACT_HOOK_ID);
    assert_eq!(
        hook.supported_phases(),
        vec![HookPhase::TurnPrePromptCompile]
    );
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(&HookCapability::new("memory").expect("static capability is valid"))
    );
    assert!(capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
    assert!(
        !capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        )
    );
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
}

#[test]
fn post_turn_extractor_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(Arc::new(TestMemoryWriteProvider::default())),
        extractor_provider: Some(Arc::new(TestPostTurnExtractorProvider::json(
            r#"{"facts":[]}"#,
        ))),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    assert_eq!(hook.id().as_str(), MEMORY_POST_TURN_EXTRACTOR_HOOK_ID);
    assert_eq!(hook.supported_phases(), vec![HookPhase::TurnPostTurn]);
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(&HookCapability::new("memory").expect("static capability is valid"))
    );
    assert!(capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("idempotent_side_effect").expect("static capability is valid")
    ));
    assert!(
        capabilities
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
    ));
}

#[test]
fn post_turn_extractor_subscription_is_retryable_with_idempotency_proof() {
    let runtime = Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ));
    install_memory_hook_package_for_test(
        &runtime,
        Arc::new(TestMemoryProvider::with_materialization(
            empty_tool_materialization(),
        )),
        Some(Arc::new(TestMemoryWriteProvider::default())),
        Some(Arc::new(TestPostTurnExtractorProvider::json(
            r#"{"facts":[]}"#,
        ))),
        Arc::new(TestToolBundleArtifactStore::new()),
        MemoryLoopConfig::default(),
    )
    .expect("memory hooks install");

    let subscription = runtime
        .subscriptions()
        .get_subscription(
            &HookSubscriptionId::new(MEMORY_POST_TURN_EXTRACTOR_SUBSCRIPTION_ID)
                .expect("static subscription id is valid"),
        )
        .expect("subscription lookup succeeds")
        .expect("post-turn extractor subscription exists");
    assert_eq!(
        subscription.hook_id.as_str(),
        MEMORY_POST_TURN_EXTRACTOR_HOOK_ID
    );
    assert_eq!(subscription.phase, HookPhase::TurnPostTurn);
    assert_eq!(
        subscription.execution_policy.await_policy,
        HookAwaitPolicy::FireAndRecord
    );
    assert_eq!(subscription.failure_policy, HookFailurePolicy::BestEffort);
    assert_eq!(
        subscription.visibility,
        HookSubscriptionVisibility::Internal
    );
    assert_eq!(subscription.retry_policy.max_attempts, 2);
    assert_eq!(subscription.retry_policy.backoff, HookRetryBackoff::Fixed);
    assert_eq!(subscription.retry_policy.initial_delay_ms, Some(1_000));
    assert!(subscription.retry_policy.idempotency_required);
    assert_eq!(subscription.execution_policy.timeout_ms, Some(180_000));

    let handler = runtime
        .handlers()
        .get_handler(&subscription.hook_id)
        .expect("handler lookup succeeds")
        .expect("post-turn extractor handler exists");
    assert!(handler.capabilities().contains(
        &HookCapability::new("idempotent_side_effect").expect("static capability is valid")
    ));
}

#[test]
fn post_turn_extractor_runtime_fingerprint_fences_configuration_and_provider_binding() {
    let key = HookMetadataKey::new("runtime_config_sha256").expect("valid metadata key");
    let base = MemoryPostTurnExtractorConfig::default();
    let configured = MemoryPostTurnExtractorConfig {
        model: Some("different-model".to_owned()),
        ..base.clone()
    };
    let fingerprint = |config: &MemoryPostTurnExtractorConfig, extractor_available: bool| {
        let capabilities =
            memory_post_turn_extractor_capabilities(config, true, extractor_available);
        let HookValue::Text(value) = capabilities
            .metadata
            .get(&key)
            .expect("post-turn handler must publish a runtime fingerprint")
        else {
            panic!("runtime fingerprint must be text");
        };
        value.clone()
    };

    let base_fingerprint = fingerprint(&base, true);
    assert_eq!(base_fingerprint.len(), 64);
    assert!(
        base_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );
    assert_ne!(base_fingerprint, fingerprint(&configured, true));
    assert_ne!(base_fingerprint, fingerprint(&base, false));
}

fn assert_subscription_registered(runtime: &HookRuntime, subscription_id: &str) {
    let subscription_id =
        HookSubscriptionId::new(subscription_id).expect("static subscription id is valid");
    assert!(
        runtime
            .subscriptions()
            .get_subscription(&subscription_id)
            .expect("subscription lookup succeeds")
            .is_some(),
        "subscription `{subscription_id}` should be registered"
    );
}

fn assert_subscription_missing(runtime: &HookRuntime, subscription_id: &str) {
    let subscription_id =
        HookSubscriptionId::new(subscription_id).expect("static subscription id is valid");
    assert!(
        runtime
            .subscriptions()
            .get_subscription(&subscription_id)
            .expect("subscription lookup succeeds")
            .is_none(),
        "subscription `{subscription_id}` should not be registered"
    );
}
