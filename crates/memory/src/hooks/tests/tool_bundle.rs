use super::*;

#[test]
fn memory_tool_bundle_contribution_uses_stable_ids_and_policy_diagnostic() {
    let policy = MemoryTurnPolicy::normal_default_allow();
    let bundle = test_memory_tool_bundle(&[
        MEMORY_SEARCH_TOOL,
        MEMORY_GET_TOOL,
        MEMORY_REMEMBER_TOOL,
        MEMORY_FORGET_TOOL,
    ]);
    let bundle_id = HookToolBundleId::new(format!("{MEMORY_TOOL_BUNDLE_ID_PREFIX}.7"))
        .expect("valid bundle id");

    let contribution = memory_tool_bundle_contribution(7, bundle_id.clone(), &bundle, &policy);

    assert_eq!(
        contribution.contribution_id.as_str(),
        "memory.tool_bundle.contribution.7"
    );
    assert_eq!(contribution.bundle_id, bundle_id);
    assert_eq!(contribution.domain.as_str(), MEMORY_POLICY_DOMAIN);
    assert_eq!(
        hook_tool_names_to_strings(&contribution.tool_names),
        vec![
            MEMORY_SEARCH_TOOL,
            MEMORY_GET_TOOL,
            MEMORY_REMEMBER_TOOL,
            MEMORY_FORGET_TOOL,
        ]
    );
    assert!(contribution.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.tools_exposed"
            && diagnostic.safe_for_user
            && diagnostic
                .message
                .as_str()
                .contains("reason=default_allow_read")
    }));
}

#[tokio::test]
async fn memory_tool_bundle_hook_applies_policy_visibility_matrix() {
    let cases = vec![
        (
            MemoryTurnPolicy::normal_default_allow(),
            vec![
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ],
            1,
        ),
        (MemoryTurnPolicy::no_use(), Vec::new(), 0),
        (
            MemoryTurnPolicy::no_save(),
            vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
            1,
        ),
        (
            MemoryTurnPolicy::explicit_remember(),
            vec![
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ],
            1,
        ),
        (
            MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned())),
            vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
            1,
        ),
    ];

    for (policy, expected_tools, expected_materialize_calls) in cases {
        let provider = Arc::new(TestMemoryProvider::with_materialization(
            standard_tool_materialization(),
        ));
        let state = Arc::new(MemoryHookTurnStateStore::default());
        state.set_turn_context(test_memory_turn_context());
        let hook = MemoryToolBundleHook {
            memory_provider: provider.clone(),
            state,
            tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
        };

        let response = hook
            .execute(test_tool_bundle_hook_request(
                HookPolicySet::merge_contributions([memory_policy_contribution(&policy)]),
                true,
            ))
            .await
            .expect("tool bundle hook executes");

        assert_eq!(
            provider.materialize_call_count(),
            expected_materialize_calls,
            "policy {:?}",
            policy.reason_code
        );
        assert_eq!(
            response_tool_names(&response),
            expected_tools,
            "policy {:?}",
            policy.reason_code
        );
    }
}

#[tokio::test]
async fn memory_tool_bundle_hook_omits_tools_without_valid_policy_or_tool_calling() {
    let provider = Arc::new(TestMemoryProvider::with_materialization(
        standard_tool_materialization(),
    ));
    let state = Arc::new(MemoryHookTurnStateStore::default());
    state.set_turn_context(test_memory_turn_context());
    let hook = MemoryToolBundleHook {
        memory_provider: provider.clone(),
        state,
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    };

    let missing = hook
        .execute(test_tool_bundle_hook_request(HookPolicySet::empty(), true))
        .await
        .expect("missing policy is best-effort");
    assert!(response_tool_names(&missing).is_empty());
    assert!(
        missing
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.code.as_str() == "memory.missing_policy" })
    );

    let malformed = PolicyContribution {
        domain: memory_policy_domain(),
        key: memory_turn_policy_key(),
        value: HookValue::Text("memory_no_use".to_owned()),
        priority: 500,
        diagnostics: Vec::new(),
    };
    let malformed = hook
        .execute(test_tool_bundle_hook_request(
            HookPolicySet::merge_contributions([malformed]),
            true,
        ))
        .await
        .expect("malformed policy is best-effort");
    assert!(response_tool_names(&malformed).is_empty());
    assert!(malformed.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.policy_decode_failed" && diagnostic.safe_for_user
    }));

    let disabled = hook
        .execute(test_tool_bundle_hook_request(
            HookPolicySet::merge_contributions([memory_policy_contribution(
                &MemoryTurnPolicy::normal_default_allow(),
            )]),
            false,
        ))
        .await
        .expect("provider tool-calling disabled is best-effort");
    assert!(response_tool_names(&disabled).is_empty());
    assert!(disabled.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.tools_omitted"
            && diagnostic
                .message
                .as_str()
                .contains("provider_tool_calling=false")
    }));
    assert_eq!(provider.materialize_call_count(), 0);
}

#[tokio::test]
async fn memory_tool_bundle_hook_does_not_execute_tool_handlers_during_materialization() {
    let provider = Arc::new(TestMemoryProvider::with_materialization(
        panicking_handler_tool_materialization(),
    ));
    let state = Arc::new(MemoryHookTurnStateStore::default());
    state.set_turn_context(test_memory_turn_context());
    let hook = MemoryToolBundleHook {
        memory_provider: provider.clone(),
        state,
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    };

    let response = hook
        .execute(test_tool_bundle_hook_request(
            HookPolicySet::merge_contributions([memory_policy_contribution(
                &MemoryTurnPolicy::normal_default_allow(),
            )]),
            true,
        ))
        .await
        .expect("tool bundle hook executes without invoking handlers");

    assert_eq!(provider.materialize_call_count(), 1);
    assert_eq!(
        response_tool_names(&response),
        vec![
            MEMORY_SEARCH_TOOL,
            MEMORY_GET_TOOL,
            MEMORY_REMEMBER_TOOL,
            MEMORY_FORGET_TOOL
        ]
    );
}

#[tokio::test]
async fn memory_tool_bundle_hook_materialization_error_is_safe_best_effort() {
    let provider = Arc::new(TestMemoryProvider::failing(
        "raw provider error must not leak",
    ));
    let state = Arc::new(MemoryHookTurnStateStore::default());
    state.set_turn_context(test_memory_turn_context());
    let hook = MemoryToolBundleHook {
        memory_provider: provider.clone(),
        state,
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    };

    let response = hook
        .execute(test_tool_bundle_hook_request(
            HookPolicySet::merge_contributions([memory_policy_contribution(
                &MemoryTurnPolicy::normal_default_allow(),
            )]),
            true,
        ))
        .await
        .expect("materialization error is best-effort");

    assert_eq!(provider.materialize_call_count(), 1);
    assert!(response_tool_names(&response).is_empty());
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.tools_failed"
            && diagnostic.safe_for_user
            && !diagnostic.message.as_str().contains("raw provider error")
    }));
}

#[test]
fn memory_tool_filtering_applies_turn_policy() {
    let materialization = MemoryToolMaterialization {
        bundles: vec![pioneer_tools::ToolExtensionBundle {
            specs: [
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ]
            .into_iter()
            .map(test_tool_spec)
            .collect(),
            handlers: [
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ]
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    Arc::new(TestToolHandler) as Arc<dyn pioneer_tools::ToolHandler>,
                )
            })
            .collect(),
        }],
        diagnostics: Vec::new(),
    };

    let filtered = filter_memory_tool_materialization(
        materialization,
        &MemoryTurnPolicy::explicit_forget(None),
    );
    assert_eq!(
        memory_tool_names(&filtered),
        vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL]
    );
    assert!(
        filtered
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains(MEMORY_REMEMBER_TOOL))
    );
}
