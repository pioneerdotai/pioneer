use super::*;

#[tokio::test]
async fn memory_policy_classifier_hook_uses_metadata_structured_override() {
    struct PanickingProvider;

    #[async_trait::async_trait]
    impl AgentMemoryTurnPolicyProvider for PanickingProvider {
        async fn resolve_memory_turn_policy(
            &self,
            _context: MemoryTurnPolicyContext,
            _request: MemoryTurnPolicyRequest,
        ) -> Result<MemoryTurnPolicy, String> {
            panic!("structured override should bypass classifier provider")
        }
    }

    let hook = MemoryPolicyClassifierHook {
        policy_provider: Some(Arc::new(PanickingProvider)),
        state: Arc::new(MemoryHookTurnStateStore::default()),
    };
    let mut metadata = HookMetadata::default();
    metadata.insert(
        hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY),
        memory_turn_policy_to_hook_value(&MemoryTurnPolicy::no_use()),
    );

    let response = hook
        .execute(test_policy_hook_request(metadata))
        .await
        .expect("hook executes");

    let contribution = response
        .contributions
        .into_iter()
        .find_map(|contribution| match contribution {
            HookContribution::Policy(policy) => Some(policy),
            _ => None,
        })
        .expect("policy contribution exists");
    let policy = memory_turn_policy_from_hook_value(&contribution.value)
        .expect("policy contribution decodes");

    assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
    assert!(!policy.allow_pre_turn_recall());
    assert!(!policy.allows_any_memory_tool());
}

#[tokio::test]
async fn memory_policy_classifier_hook_accepts_structured_override_variants() {
    let variants = vec![
        MemoryTurnPolicy::no_use(),
        MemoryTurnPolicy::no_save(),
        MemoryTurnPolicy::explicit_remember(),
        MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned())),
    ];

    for expected in variants {
        let hook = MemoryPolicyClassifierHook {
            policy_provider: None,
            state: Arc::new(MemoryHookTurnStateStore::default()),
        };
        let mut metadata = HookMetadata::default();
        metadata.insert(
            hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY),
            memory_turn_policy_to_hook_value(&expected),
        );

        let response = hook
            .execute(test_policy_hook_request(metadata))
            .await
            .expect("hook executes");
        let contribution = response
            .contributions
            .into_iter()
            .find_map(|contribution| match contribution {
                HookContribution::Policy(policy) => Some(policy),
                _ => None,
            })
            .expect("policy contribution exists");
        let policy = memory_turn_policy_from_hook_value(&contribution.value)
            .expect("policy contribution decodes");

        assert_eq!(policy.recall, expected.recall);
        assert_eq!(policy.prompt, expected.prompt);
        assert_eq!(policy.read_tools, expected.read_tools);
        assert_eq!(policy.remember_tool, expected.remember_tool);
        assert_eq!(policy.forget_tool, expected.forget_tool);
        assert_eq!(policy.post_turn_extraction, expected.post_turn_extraction);
        assert_eq!(policy.active_memory, expected.active_memory);
        assert_eq!(policy.explicit_remember, expected.explicit_remember);
        assert_eq!(policy.explicit_forget, expected.explicit_forget);
        assert_eq!(policy.forget_target_hint, expected.forget_target_hint);
        assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
        assert_eq!(
            policy.reason_code,
            MemoryPolicyReasonCode::StructuredOverride
        );
    }
}

#[tokio::test]
async fn structured_override_wins_over_classifier() {
    struct AllowAllProvider;

    #[async_trait::async_trait]
    impl AgentMemoryTurnPolicyProvider for AllowAllProvider {
        async fn resolve_memory_turn_policy(
            &self,
            _context: MemoryTurnPolicyContext,
            _request: MemoryTurnPolicyRequest,
        ) -> Result<MemoryTurnPolicy, String> {
            Ok(MemoryTurnPolicy::normal_default_allow())
        }
    }

    let provider: Arc<dyn AgentMemoryTurnPolicyProvider> = Arc::new(AllowAllProvider);
    let policy = resolve_memory_turn_policy(
        Some(&provider),
        MemoryTurnPolicyContext {
            workspace_id: "ws".to_owned(),
            thread_id: "thr".to_owned(),
            turn_id: "turn".to_owned(),
            mode: ThreadMode::Agent,
            input_text: "anything".to_owned(),
            model: None,
            model_provider: None,
        },
        MemoryTurnPolicyRequest {
            structured_override: Some(MemoryTurnPolicyOverride::new(MemoryTurnPolicy::no_use())),
            ..MemoryTurnPolicyRequest::default()
        },
    )
    .await;

    assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
    assert!(!policy.allows_any_memory_tool());
    assert!(!policy.allow_pre_turn_recall());
}

#[tokio::test]
async fn classifier_error_uses_default_allow_fallback() {
    struct FailingProvider;

    #[async_trait::async_trait]
    impl AgentMemoryTurnPolicyProvider for FailingProvider {
        async fn resolve_memory_turn_policy(
            &self,
            _context: MemoryTurnPolicyContext,
            _request: MemoryTurnPolicyRequest,
        ) -> Result<MemoryTurnPolicy, String> {
            Err("invalid json".to_owned())
        }
    }

    let provider: Arc<dyn AgentMemoryTurnPolicyProvider> = Arc::new(FailingProvider);
    let policy = resolve_memory_turn_policy(
        Some(&provider),
        MemoryTurnPolicyContext {
            workspace_id: "ws".to_owned(),
            thread_id: "thr".to_owned(),
            turn_id: "turn".to_owned(),
            mode: ThreadMode::Agent,
            input_text: "hola".to_owned(),
            model: None,
            model_provider: None,
        },
        MemoryTurnPolicyRequest::default(),
    )
    .await;

    assert_eq!(policy.source, MemoryPolicySource::DefaultFallback);
    assert_eq!(
        policy.reason_code,
        MemoryPolicyReasonCode::ClassifierInvalidJson
    );
    assert!(policy.allows_memory_tool(MEMORY_REMEMBER_TOOL));
    assert!(policy.allow_pre_turn_recall());
    assert!(
        policy
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("classifier_failed"))
    );
}
