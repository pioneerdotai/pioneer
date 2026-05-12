use super::*;

#[test]
fn phase_19_strict_json_parser_accepts_typed_semantic_fact() {
    let parsed = parse_memory_post_turn_extractor_json(
        valid_post_turn_extractor_json().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.facts.len(), 1);
    let fact = &parsed.facts[0];
    assert_eq!(fact.semantic.intent, MemoryIntent::ExplicitStore);
    assert_eq!(fact.semantic.category, MemoryCategory::Identity);
    assert_eq!(fact.semantic.subject, MemorySubject::CurrentUser);
    assert_eq!(fact.semantic.attribute, MemoryAttribute::Name);
    assert_eq!(fact.content, "Имя пользователя: Александр");
    assert_eq!(fact.value.as_deref(), Some("Александр"));
    assert_eq!(
        fact.evidence.quote_or_span.as_deref(),
        Some("Меня зовут Александр")
    );
}

#[test]
fn phase_19_strict_json_parser_rejects_unknown_enums_and_keys() {
    let unknown_enum = r#"{"facts":[{"semantic":{"intent":"write_now","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{"quote_or_span":"My name is Alexander"}}]}"#;
    assert!(
        parse_memory_post_turn_extractor_json(
            unknown_enum,
            &MemoryPostTurnExtractorConfig::default(),
        )
        .is_err()
    );

    let canonical_key = r#"{"facts":[{"canonical_key":"user/global:identity:self:name","semantic":{"intent":"explicit_store","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{"quote_or_span":"My name is Alexander"}}]}"#;
    assert!(
        parse_memory_post_turn_extractor_json(
            canonical_key,
            &MemoryPostTurnExtractorConfig::default(),
        )
        .is_err()
    );
}

#[test]
fn phase_19_parser_rejects_secrets_transient_and_missing_evidence() {
    let secret = r#"{"facts":[{"semantic":{"intent":"implicit_candidate","explicitness":"implicit","category":"custom","subject":"current_user","attribute":"custom","custom_attribute":"api_key","scope_hint":"user_global","durability":"long_lived","sensitivity":"secret","certainty":"high"},"content":"User API key is sk-test","evidence":{"quote_or_span":"sk-test"}}]}"#;
    let parsed =
        parse_memory_post_turn_extractor_json(secret, &MemoryPostTurnExtractorConfig::default())
            .expect("secret JSON shape parses but fact is rejected");
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("secret_or_regulated"))
    );

    let missing_evidence = r#"{"facts":[{"semantic":{"intent":"explicit_store","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{}}]}"#;
    let parsed = parse_memory_post_turn_extractor_json(
        missing_evidence,
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("missing evidence JSON shape parses but fact is rejected");
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("missing_evidence_quote"))
    );
}

#[tokio::test]
async fn phase_19_post_turn_extractor_writes_semantic_fact_through_provider() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("post-turn extractor executes");

    assert!(response.contributions.is_empty());
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.manifest_call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 1);
    let params = write_provider
        .write_params()
        .into_iter()
        .next()
        .expect("write params recorded");
    assert_eq!(
        params.disposition,
        Some(MemorySemanticWriteDisposition::RouteToCandidatePolicy)
    );
    assert_eq!(params.client_provided_key, None);
    assert_eq!(params.semantic.intent, MemoryIntent::ExplicitStore);
    assert_eq!(params.scope.kind, MemoryScopeKind::User);
    assert_eq!(params.scope.key, MEMORY_DEFAULT_USER_SCOPE_KEY);
    assert!(
        params
            .evidence
            .as_ref()
            .and_then(|e| e.source_thread_id.as_deref())
            .is_some()
    );
    assert!(params.metadata.contains_key("hook_id"));
    assert_eq!(
        params.metadata.get("model"),
        Some(&serde_json::json!("test-model"))
    );
    assert_eq!(
        params.metadata.get("model_provider"),
        Some(&serde_json::json!("test-provider"))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("write_successes=1")
    }));
}

#[tokio::test]
async fn phase_21_post_turn_extractor_provider_failure_is_retryable_hook_failure() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestFailingPostTurnExtractorProvider::default());
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let error = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect_err("provider failure should be visible to hook runtime");

    assert_eq!(
        error.code.as_str(),
        "memory.post_turn_extractor.provider_failed"
    );
    assert!(error.retryable);
    assert!(error.safe_for_user);
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 0);
}

#[tokio::test]
async fn phase_19_post_turn_extractor_preserves_manifest_and_observes_auto_approve() {
    let mut auto_response = test_semantic_write_response();
    auto_response.record = Some(test_memory_record("mem_auto"));
    auto_response.created = true;
    let write_provider = Arc::new(TestMemoryWriteProvider {
        response: Some(auto_response),
        ..TestMemoryWriteProvider::default()
    });
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("post-turn extractor executes");

    let prompt = extractor_provider
        .prompts()
        .into_iter()
        .next()
        .expect("extractor prompt recorded");
    assert!(prompt.contains("Memory manifest:"));
    assert!(prompt.contains("Do not generate canonical memory keys"));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("auto_approved=1")
    }));
}

#[tokio::test]
async fn phase_19_post_turn_extractor_skips_non_success_turns() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };
    let mut request = test_post_turn_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        "Меня зовут Александр",
        "Понял.",
    );
    request.input = HookInput::turn_post_turn(TurnPostTurnHookInput::from_parts_with_model(
        TurnPostTurnStatus::ProviderFailure,
        Some("test-model"),
        Some("test-provider"),
        Some("Меня зовут Александр"),
        None::<&str>,
        Some("provider failed"),
        Vec::new(),
        Vec::new(),
        pioneer_hooks::TurnPostTurnHookInputLimits::default(),
    ));

    let response = hook
        .execute(request)
        .await
        .expect("non-success post-turn hook is best-effort");
    assert_eq!(extractor_provider.call_count(), 0);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.skipped"
            && diagnostic.message.as_str().contains("provider_failure")
    }));
}

#[tokio::test]
async fn phase_19_post_turn_extractor_respects_policy_and_provider_availability() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let no_save = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_save()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("no-save policy is best-effort");
    assert!(no_save.contributions.is_empty());
    assert_eq!(extractor_provider.call_count(), 0);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(no_save.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.skipped"
            && diagnostic
                .message
                .as_str()
                .contains("source=pre_memory_classifier")
    }));

    let provider_disabled = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig {
            provider_enabled: false,
            ..MemoryPostTurnExtractorConfig::default()
        },
    }
    .execute(test_post_turn_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        "Меня зовут Александр",
        "Понял.",
    ))
    .await
    .expect("provider-disabled policy is best-effort");
    assert!(provider_disabled.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.skipped"
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text("provider_disabled".to_owned()))
    }));
}

#[tokio::test]
async fn phase_19_post_turn_extractor_suppresses_implicit_when_proactive_disabled() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        implicit_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider),
        config: MemoryPostTurnExtractorConfig {
            proactive_writes_enabled: false,
            ..MemoryPostTurnExtractorConfig::default()
        },
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Мне нравится лаконичный стиль ответов.",
            "Ок.",
        ))
        .await
        .expect("post-turn extractor executes");

    assert_eq!(write_provider.write_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic
                .message
                .as_str()
                .contains("validation_rejected=1")
    }));
}
