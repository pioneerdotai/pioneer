use super::*;

#[test]
fn memory_policy_classifier_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryPolicyClassifierHook {
        policy_provider: None,
        state: Arc::new(MemoryHookTurnStateStore::default()),
    };

    assert_eq!(hook.id().as_str(), MEMORY_POLICY_CLASSIFIER_HOOK_ID);
    assert_eq!(hook.supported_phases(), vec![HookPhase::TurnPrePolicy]);
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        )
    );
    assert!(
        capabilities
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
fn memory_deterministic_recall_hook_descriptor_is_stable_and_narrow() {
    let hook = MemoryDeterministicRecallHook {
        memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
            MemoryRecallSnapshot::empty(),
        )),
    };

    assert_eq!(hook.id().as_str(), MEMORY_DETERMINISTIC_RECALL_HOOK_ID);
    assert_eq!(
        hook.supported_phases(),
        vec![HookPhase::TurnPrePromptContext]
    );
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(&HookCapability::new("memory").expect("static capability is valid"))
    );
    assert!(capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
}

#[test]
fn memory_active_recall_hook_descriptor_is_stable_and_read_only() {
    let hook = ActiveMemoryRecallHook {
        memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
            MemoryRecallSnapshot::empty(),
        )),
        decision_provider: None,
        config: MemoryActiveRecallConfig::default(),
    };

    assert_eq!(hook.id().as_str(), MEMORY_ACTIVE_RECALL_HOOK_ID);
    assert_eq!(
        hook.supported_phases(),
        vec![HookPhase::TurnPrePromptContext]
    );
    let capabilities = hook.capabilities();
    assert!(
        capabilities.contains(&HookCapability::new("memory").expect("static capability is valid"))
    );
    assert!(capabilities.contains(
        &HookCapability::new("read_domain_context").expect("static capability is valid")
    ));
    assert!(capabilities.contains(
        &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
    ));
    assert!(!capabilities.contains(
        &HookCapability::new("write_domain_context").expect("static capability is valid")
    ));
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
    );
    assert!(
        !capabilities
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );

    let hook_with_provider = ActiveMemoryRecallHook {
        memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
            MemoryRecallSnapshot::empty(),
        )),
        decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
            r#"{"status":"skip","confidence":1.0}"#,
        ))),
        config: MemoryActiveRecallConfig::default(),
    };
    assert!(
        hook_with_provider
            .capabilities()
            .contains(&HookCapability::new("call_provider").expect("static capability is valid"))
    );
}

#[test]
fn memory_hook_package_registers_active_recall_with_deadline_dependency() {
    let runtime = Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ));
    let artifacts = Arc::new(TestToolBundleArtifactStore::new());
    install_memory_hook_package_for_test(
        &runtime,
        Arc::new(TestRecallMemoryProvider::with_recall(
            MemoryRecallSnapshot::empty(),
        )),
        None,
        None,
        None,
        artifacts,
        MemoryLoopConfig {
            active_recall: MemoryActiveRecallConfig {
                timeout_ms: 321,
                ..MemoryActiveRecallConfig::default()
            },
            ..MemoryLoopConfig::default()
        },
    )
    .expect("memory hooks install");

    let subscription_id = HookSubscriptionId::new(MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID)
        .expect("static subscription id is valid");
    let subscription = runtime
        .subscriptions()
        .get_subscription(&subscription_id)
        .expect("subscription lookup succeeds")
        .expect("active recall subscription registered");

    assert_eq!(subscription.hook_id.as_str(), MEMORY_ACTIVE_RECALL_HOOK_ID);
    assert_eq!(subscription.phase, HookPhase::TurnPrePromptContext);
    assert_eq!(
        subscription.execution_policy.await_policy,
        HookAwaitPolicy::Deadline
    );
    assert_eq!(subscription.execution_policy.timeout_ms, Some(321));
    assert_eq!(subscription.failure_policy, HookFailurePolicy::BestEffort);
    assert_eq!(
        subscription.dependencies.after,
        vec![
            HookSubscriptionId::new(MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID)
                .expect("static subscription id is valid")
        ]
    );
    assert_eq!(
        subscription.visibility,
        HookSubscriptionVisibility::Internal
    );

    let post_turn_subscription_id =
        HookSubscriptionId::new(MEMORY_POST_TURN_EXTRACTOR_SUBSCRIPTION_ID)
            .expect("static subscription id is valid");
    let post_turn_subscription = runtime
        .subscriptions()
        .get_subscription(&post_turn_subscription_id)
        .expect("subscription lookup succeeds")
        .expect("post-turn extractor subscription registered");
    assert_eq!(
        post_turn_subscription.hook_id.as_str(),
        MEMORY_POST_TURN_EXTRACTOR_HOOK_ID
    );
    assert_eq!(post_turn_subscription.phase, HookPhase::TurnPostTurn);
    assert_eq!(
        post_turn_subscription.execution_policy.await_policy,
        HookAwaitPolicy::FireAndRecord
    );
    assert_eq!(
        post_turn_subscription.failure_policy,
        HookFailurePolicy::BestEffort
    );
    assert_eq!(
        post_turn_subscription.visibility,
        HookSubscriptionVisibility::Internal
    );
}

#[tokio::test]
async fn active_memory_timeout_falls_back_without_prompt_context() {
    let runtime = Arc::new(HookRuntime::new(
        Arc::new(HookRegistry::new()),
        Arc::new(HookSubscriptionRegistry::new()),
    ));
    let handler = Arc::new(ActiveMemoryRecallHook {
        memory_provider: Arc::new(SlowRecallMemoryProvider),
        decision_provider: None,
        config: MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::StrictDebug,
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    });
    let subscription_id = HookSubscriptionId::new(MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID)
        .expect("static subscription id is valid");
    install_single_hook_definition_for_test(
        &runtime,
        handler,
        HookSubscription::new(
            subscription_id,
            HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid"),
            HookPhase::TurnPrePromptContext,
        )
        .with_execution_policy(HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Deadline,
            timeout_ms: Some(1),
            max_parallelism: None,
        })
        .with_failure_policy(HookFailurePolicy::BestEffort),
    )
    .expect("active hook package installs");

    let response = runtime
        .run_phase(
            HookPhaseRequest::new(
                HookPhase::TurnPrePromptContext,
                HookContext {
                    workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                    thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                    turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                    ..HookContext::default()
                },
                HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                    "continue the previous memory-aware architecture work",
                    Some("test-model"),
                    Some("test-provider"),
                )),
            )
            .with_policy_set(memory_policy_set(&MemoryTurnPolicy::normal_default_allow())),
        )
        .await
        .expect("best-effort timeout should not fail phase");

    assert!(response.contributions.is_empty());
    assert!(
        response
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code.as_str() == "hook.timeout")
    );
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
        None,
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
