use super::*;

#[tokio::test]
async fn permissive_classifier_fallback_keeps_memory_surfaces_available() {
    let policy = MemoryTurnPolicy::permissive_classifier_fallback(
        MemoryPolicyReasonCode::ClassifierUnavailable,
    );

    let recall_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let recall_response = MemoryDeterministicRecallHook {
        memory_provider: recall_provider.clone(),
    }
    .execute(test_prompt_context_hook_request(memory_policy_set(&policy)))
    .await
    .expect("deterministic recall executes");

    assert_eq!(recall_provider.recall_call_count(), 1);
    assert_eq!(recall_provider.materialize_call_count(), 0);
    assert_eq!(recall_response.contributions.len(), 1);
    let prompt_context_set = prompt_context_set_from_response(recall_response);

    let prompt_response = MemoryPromptContractHook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&policy),
            true,
            &[
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract executes");
    let prompt = prompt_section_content(prompt_response).expect("memory prompt renders");
    assert!(prompt.contains(
        "Available memory tools: memory_search, memory_get, memory_remember, memory_forget."
    ));
    assert!(prompt.contains("User likes Porto."));

    let tool_provider = Arc::new(TestMemoryProvider::with_materialization(
        standard_tool_materialization(),
    ));
    let state = Arc::new(MemoryHookTurnStateStore::default());
    state.set_turn_context(test_memory_turn_context());
    let tool_response = MemoryToolBundleHook {
        memory_provider: tool_provider.clone(),
        state,
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    }
    .execute(test_tool_bundle_hook_request(
        memory_policy_set(&policy),
        true,
    ))
    .await
    .expect("tool bundle executes");

    assert_eq!(tool_provider.materialize_call_count(), 1);
    assert_eq!(
        response_tool_names(&tool_response),
        vec![
            MEMORY_SEARCH_TOOL,
            MEMORY_GET_TOOL,
            MEMORY_REMEMBER_TOOL,
            MEMORY_FORGET_TOOL,
        ]
    );

    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let post_turn_response = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    }
    .execute(test_post_turn_hook_request(
        memory_policy_set(&policy),
        "durable user fact",
        "acknowledged",
    ))
    .await
    .expect("post-turn extractor executes");

    assert!(post_turn_response.contributions.is_empty());
    assert_eq!(write_provider.manifest_call_count(), 1);
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 1);
    assert!(post_turn_response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("write_attempts=1")
            && diagnostic.message.as_str().contains("write_successes=1")
    }));
}

#[tokio::test]
async fn explicit_no_use_policy_remains_stronger_than_fallback_defaults() {
    let policy = MemoryTurnPolicy::no_use();

    let recall_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let recall_response = MemoryDeterministicRecallHook {
        memory_provider: recall_provider.clone(),
    }
    .execute(test_prompt_context_hook_request(memory_policy_set(&policy)))
    .await
    .expect("recall no-use skip is best-effort");

    assert_eq!(recall_provider.recall_call_count(), 0);
    assert!(recall_response.contributions.is_empty());

    let prompt_context_set = prompt_context_set_from_prompt_context_contribution(
        memory_recall_prompt_context_contribution(recalled_city_snapshot())
            .expect("deterministic memory context can be built"),
    );
    let prompt_response = MemoryPromptContractHook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&policy),
            true,
            &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt no-use skip is best-effort");
    assert!(prompt_section_content(prompt_response).is_none());

    let tool_provider = Arc::new(TestMemoryProvider::with_materialization(
        standard_tool_materialization(),
    ));
    let state = Arc::new(MemoryHookTurnStateStore::default());
    state.set_turn_context(test_memory_turn_context());
    let tool_response = MemoryToolBundleHook {
        memory_provider: tool_provider.clone(),
        state,
        tool_bundle_artifacts: Arc::new(TestToolBundleArtifactStore::new()),
    }
    .execute(test_tool_bundle_hook_request(
        memory_policy_set(&policy),
        true,
    ))
    .await
    .expect("tool no-use skip is best-effort");

    assert_eq!(tool_provider.materialize_call_count(), 0);
    assert!(response_tool_names(&tool_response).is_empty());

    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let post_turn_response = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    }
    .execute(test_post_turn_hook_request(
        memory_policy_set(&policy),
        "durable user fact",
        "acknowledged",
    ))
    .await
    .expect("post-turn no-use skip is best-effort");

    assert_eq!(write_provider.manifest_call_count(), 0);
    assert_eq!(extractor_provider.call_count(), 0);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(post_turn_response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_eligibility.policy_disabled"
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text("policy_disabled".to_owned()))
            && diagnostic
                .metadata
                .get(&hook_metadata_key("policy_reason_code"))
                == Some(&HookValue::Text("memory_no_use".to_owned()))
    }));
}
