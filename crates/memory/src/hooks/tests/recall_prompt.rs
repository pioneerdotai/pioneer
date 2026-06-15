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

    assert!(provider.recall_call_count() > 0);
    assert_eq!(provider.materialize_call_count(), 0);
    assert_has_memory_recall_audit(&response, "memory.recall.deterministic");
    let contributions = prompt_context_contributions(&response);
    assert_eq!(contributions.len(), 1);
    let context = contributions[0];
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

    assert!(provider.recall_call_count() > 0);
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
        episodic_provider: None,
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

    assert!(provider.recall_call_count() > 0);
    assert_eq!(provider.materialize_call_count(), 0);
    let request = provider
        .mode_recall_requests()
        .into_iter()
        .next()
        .expect("active recall request recorded");
    assert_eq!(request.mode, MemoryRecallMode::Project);
    assert_eq!(request.top_k, Some(5));
    assert_eq!(request.max_chars, Some(1_500));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic
                .message
                .as_str()
                .contains("deterministic_contexts=0")
    }));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.context_contributed"
    }));
    assert_has_memory_recall_audit(&response, "memory.recall.active");
    let contributions = prompt_context_contributions(&response);
    assert_eq!(contributions.len(), 1);
    let context = contributions[0];
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
            episodic_provider: None,
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
async fn active_memory_hook_uses_valid_preflight_json_plan() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };
    let plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.92,"modes":["profile"],"targets":[{"scopeKind":"user","factClass":"user_identity","category":"identity","subject":"current_user","attribute":"name"}],"diagnostics":["preflight provider ok"]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
    )
    .expect("provider-owned preflight plan uses memory parser");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "finish the architecture task",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "finish the architecture task",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert!(provider.recall_call_count() > 0);
    let request = provider
        .mode_recall_requests()
        .into_iter()
        .next()
        .expect("strict JSON plan should drive recall");
    assert_eq!(request.mode, MemoryRecallMode::Profile);
    assert_eq!(request.targets.len(), 1);
    assert_eq!(request.targets[0].category, Some(MemoryCategory::Identity));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic.message.as_str().contains("reason=provider_run")
    }));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("preflight provider ok")
    }));
    let HookContribution::PromptContext(context) = response
        .contributions
        .iter()
        .find(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
        .expect("provider plan should still produce memory prompt context")
    else {
        panic!("expected prompt context contribution");
    };
    assert!(!context.content.as_str().contains("preflight provider ok"));
}

#[tokio::test]
async fn active_recall_hook_executes_preflight_plan_without_legacy_provider() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };
    let plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.92,"modes":["profile"],"targets":[{"scopeKind":"user","factClass":"user_identity","category":"identity","subject":"current_user","attribute":"name"}],"diagnostics":["preflight provider ok"]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
    )
    .expect("provider-owned preflight plan uses memory parser");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "what is my name?",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "what is my name?",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    let request = provider
        .mode_recall_requests()
        .into_iter()
        .next()
        .expect("preflight plan should drive mode-native recall");
    assert_eq!(request.mode, MemoryRecallMode::Profile);
    assert!(
        response
            .contributions
            .iter()
            .any(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic.message.as_str().contains("reason=provider_run")
    }));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("preflight provider ok")
    }));
}

#[tokio::test]
async fn active_memory_hook_respects_policy_and_config_skips() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
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
    assert_no_prompt_context_contributions(&no_use);

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
    assert_no_prompt_context_contributions(&disabled_policy);

    let deterministic_only = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
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
    assert_no_prompt_context_contributions(&deterministic_only);

    assert_eq!(provider.recall_call_count(), 0);
}

#[tokio::test]
async fn active_memory_hook_uses_mode_native_recall_for_short_turns() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
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
        .mode_recall_requests()
        .into_iter()
        .next()
        .expect("active recall request recorded");
    assert_eq!(request.mode, MemoryRecallMode::Project);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.decision"
            && diagnostic.message.as_str().contains("status=run")
    }));
}

#[tokio::test]
async fn active_memory_hook_without_preflight_plan_uses_local_deterministic_plan() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "continue the architecture work using prior project decisions",
        ))
        .await
        .expect("missing preflight plan uses local active recall fallback");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(
        response
            .contributions
            .iter()
            .any(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("memory.active_recall.provider_unavailable")
    }));
}

#[tokio::test]
async fn active_memory_hook_runs_even_when_deterministic_context_exists() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
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

    assert!(provider.recall_call_count() > 0);
    assert!(
        response
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.code.as_str() == "memory.active_recall.execution" })
    );
}

#[tokio::test]
async fn active_memory_hook_deduplicates_after_running_with_deterministic_context() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
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

    assert!(provider.recall_call_count() > 0);
    assert!(
        response
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.code.as_str() == "memory.active_recall.dedup" })
    );
}

#[tokio::test]
async fn active_memory_hook_does_not_call_disabled_planner_provider() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            planner: MemoryActiveRecallPlannerConfig {
                enabled: false,
                ..MemoryActiveRecallPlannerConfig::default()
            },
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };

    let response = hook
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "continue the architecture work using prior project decisions",
        ))
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("memory.active_recall.provider_disabled")
    }));
}

#[tokio::test]
async fn active_memory_hook_deduplicates_deterministic_ids() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        recalled_city_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::StrictDebug,
            max_queries: 1,
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
    assert_no_prompt_context_contributions(&response);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.no_hits"
            && diagnostic
                .message
                .as_str()
                .contains("no prompt context after synthesis")
    }));
}

#[tokio::test]
async fn active_memory_hook_rejects_invalid_preflight_plan_without_legacy_provider() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig::default(),
    };
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue the architecture work using prior project decisions and constraints",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(json!({
        "status": "run",
        "reasonCode": "provider_run",
        "confidence": 0.9,
        "modes": [],
        "targets": []
    }));
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue the architecture work using prior project decisions and constraints",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("invalid preflight plan is best-effort");

    assert!(provider.recall_call_count() >= 1);
    assert!(
        response
            .contributions
            .iter()
            .any(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.preflight_plan_invalid"
    }));
}

#[tokio::test]
async fn active_memory_hook_executes_preflight_provider_error_fallback_plan() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue the architecture work using prior project decisions",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(json!({
        "durable": {
            "status": "run",
            "reasonCode": "memory_likely",
            "confidence": 0.65,
            "modes": ["project"],
            "targets": [],
            "providerFallbackUsed": true,
            "diagnostics": ["memory.active_recall.provider_failed"]
        },
        "episodic": {
            "status": "skip",
            "reasonCode": "provider_skip",
            "confidence": 1.0,
            "queries": []
        }
    }));
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue the architecture work using prior project decisions",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("preflight provider failure fallback is best-effort");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(
        response
            .contributions
            .iter()
            .any(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("memory.active_recall.provider_failed")
    }));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .metadata
            .get(&hook_metadata_key("provider_fallback_used"))
            == Some(&HookValue::Bool(true))
    }));
}

#[tokio::test]
async fn active_memory_hook_executes_preflight_timeout_fallback_plan() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            max_queries: 1,
            ..MemoryActiveRecallConfig::default()
        },
    };
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue the architecture work using prior project decisions",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(json!({
        "durable": {
            "status": "run",
            "reasonCode": "memory_likely",
            "confidence": 0.65,
            "modes": ["project"],
            "targets": [],
            "providerFallbackUsed": true,
            "diagnostics": ["memory.active_recall.provider_timeout"]
        },
        "episodic": {
            "status": "skip",
            "reasonCode": "provider_skip",
            "confidence": 1.0,
            "queries": []
        }
    }));
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue the architecture work using prior project decisions",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("preflight timeout fallback is best-effort");

    assert_eq!(provider.recall_call_count(), 1);
    assert!(
        response
            .contributions
            .iter()
            .any(|contribution| matches!(contribution, HookContribution::PromptContext(_)))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("memory.active_recall.provider_timeout")
    }));
}

#[tokio::test]
async fn active_recall_executor_runs_mode_native_requests_and_deduplicates() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let result = execute_active_recall_plan(
        provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::MemoryLikely,
                0.8,
                vec![ActiveRecallMode::Project, ActiveRecallMode::Durable],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig {
                max_queries: 2,
                ..MemoryActiveRecallConfig::default()
            },
            episodic_provider: None,
            episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
        },
    )
    .await;

    assert_eq!(provider.recall_call_count(), 2);
    assert!(
        provider.recall_requests().is_empty(),
        "normal executor path must not use broad query recall"
    );
    let mode_requests = provider.mode_recall_requests();
    assert_eq!(mode_requests.len(), 2);
    assert_eq!(mode_requests[0].mode, MemoryRecallMode::Project);
    assert_eq!(mode_requests[1].mode, MemoryRecallMode::Durable);
    assert_eq!(result.raw_item_count, 2);
    assert_eq!(result.duplicate_count, 1);
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0].memory_id, "mem_active_project");
}

#[tokio::test]
async fn active_recall_executor_skips_missing_structured_modes() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let result = execute_active_recall_plan(
        provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::MemoryLikely,
                0.8,
                vec![
                    ActiveRecallMode::ExactCanonical,
                    ActiveRecallMode::TaskContext,
                ],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig::default(),
            episodic_provider: None,
            episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
        },
    )
    .await;

    assert_eq!(provider.recall_call_count(), 0);
    assert!(result.items.is_empty());
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .contains("memory.active_recall.mode_skipped:exact_canonical:missing_canonical_target")
    }));
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.active_recall.mode_skipped:task_context:missing_task_context")
    }));
}

#[tokio::test]
async fn active_memory_hook_keeps_broad_query_recall_in_debug_fallback_only() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        active_project_snapshot(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig {
            mode: MemoryActiveRecallMode::StrictDebug,
            max_queries: 2,
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

    assert_eq!(provider.mode_recall_requests().len(), 0);
    let recall_requests = provider.recall_requests();
    assert_eq!(recall_requests.len(), 2);
    assert_eq!(recall_requests[0].query, MEMORY_ACTIVE_RECALL_GENERIC_QUERY);
    assert_eq!(recall_requests[1].query, "как меня зовут?");
    assert!(
        response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.debug_fallback"
        })
    );
}

#[test]
fn active_recall_plan_normalizes_modes_targets_and_diagnostics() {
    let plan = ActiveRecallPlan::run(
        ActiveMemoryDecisionReasonCode::MemoryLikely,
        9.0,
        vec![
            ActiveRecallMode::Durable,
            ActiveRecallMode::Project,
            ActiveRecallMode::Project,
            ActiveRecallMode::Profile,
            ActiveRecallMode::TaskContext,
            ActiveRecallMode::ThreadEpisodic,
        ],
        vec![
            ActiveRecallTarget::exact_canonical("key:1"),
            ActiveRecallTarget::exact_canonical("key:2"),
            ActiveRecallTarget::exact_canonical("key:3"),
            ActiveRecallTarget::exact_canonical("key:4"),
            ActiveRecallTarget::exact_canonical("key:5"),
            ActiveRecallTarget::exact_canonical("key:6"),
            ActiveRecallTarget::exact_canonical("key:7"),
        ],
        vec![
            "one".to_owned(),
            "two".to_owned(),
            "three".to_owned(),
            "four".to_owned(),
            "five".to_owned(),
            "six".to_owned(),
            "seven".to_owned(),
        ],
    );

    assert_eq!(plan.confidence, 1.0);
    assert_eq!(
        plan.modes,
        vec![
            ActiveRecallMode::Profile,
            ActiveRecallMode::Project,
            ActiveRecallMode::TaskContext,
            ActiveRecallMode::ThreadEpisodic
        ]
    );
    assert_eq!(plan.targets.len(), 6);
    assert_eq!(plan.all_diagnostics().len(), 6);
}

#[test]
fn active_recall_provider_plan_parses_typed_modes_and_targets() {
    let plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.91,"modes":["exact_canonical","profile"],"targets":[{"scopeKind":"user","factClass":"user_identity","category":"identity","subject":"current_user","attribute":"name","canonicalKey":"user/global:identity:self:name"}],"diagnostics":["provider ok"]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
    )
    .expect("typed provider plan parses");

    assert_eq!(plan.status, ActiveMemoryDecisionStatus::Run);
    assert_eq!(
        plan.reason_code,
        ActiveMemoryDecisionReasonCode::ProviderRun
    );
    assert!(plan.provider_used);
    assert_eq!(
        plan.modes,
        vec![ActiveRecallMode::ExactCanonical, ActiveRecallMode::Profile]
    );
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(
        plan.targets[0].canonical_key.as_deref(),
        Some("user/global:identity:self:name")
    );
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.91,"modes":["profile"],"targets":[],"ignoredExtraKey":true},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
}

#[test]
fn active_recall_provider_plan_parses_envelope_with_durable_and_episodic_plans() {
    let plan = parse_active_memory_decision_json(
        r#"{
            "durable": {
                "status": "run",
                "reasonCode": "provider_run",
                "confidence": 0.91,
                "modes": ["profile"],
                "targets": [
                    {
                        "scopeKind": "user",
                        "factClass": "user_identity",
                        "category": "identity",
                        "subject": "current_user",
                        "attribute": "name",
                        "canonicalKey": "user/global:identity:self:name"
                    }
                ]
            },
            "episodic": {
                "status": "run",
                "reasonCode": "provider_run",
                "confidence": 0.84,
                "queries": [
                    {
                        "mode": "current_thread",
                        "query": "weather forecast Moscow tomorrow",
                        "targets": [],
                        "topK": 3,
                        "maxChars": 700
                    }
                ]
            },
            "diagnostics": ["envelope_ok"]
        }"#,
    )
    .expect("envelope provider plan parses");

    assert_eq!(plan.durable.status, ActiveMemoryDecisionStatus::Run);
    assert_eq!(plan.durable.modes, vec![ActiveRecallMode::Profile]);
    assert_eq!(plan.durable.targets.len(), 1);
    assert_eq!(plan.episodic.status, ActiveMemoryDecisionStatus::Run);
    assert_eq!(plan.episodic.queries.len(), 1);
    assert_eq!(
        plan.episodic.queries[0].mode,
        ActiveRecallMode::CurrentThread
    );
    assert_eq!(
        plan.episodic.queries[0].query.as_str(),
        "weather forecast Moscow tomorrow"
    );
    assert_eq!(plan.diagnostics, vec!["envelope_ok".to_owned()]);
}

#[test]
fn active_recall_provider_plan_rejects_invalid_enum_values() {
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.7,"modes":["anything"],"targets":[]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.7,"modes":["profile"],"targets":[{"factClass":"future_fact"}]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
}

#[test]
fn active_recall_provider_plan_requires_reason_and_run_modes() {
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"run","confidence":0.7,"modes":["profile"],"targets":[]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.7,"modes":[],"targets":[]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
    assert!(
        parse_active_memory_decision_json(
            r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":["profile"],"targets":[]},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
        )
        .is_err()
    );
}

#[test]
fn active_recall_provider_plan_ignores_debug_fallback_and_drops_impossible_modes() {
    let plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"run","reasonCode":"provider_run","confidence":0.8,"modes":["exact_canonical","task_context","thread_episodic","profile"],"targets":[],"debugFallback":true},"episodic":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"queries":[]}}"#,
    )
    .expect("provider plan parses");
    assert!(!plan.debug_fallback);

    let mut input = active_recall_planner_input_for_test();
    input.task_id = None;
    input.thread_id.clear();
    let normalized = normalize_active_recall_plan_for_input(plan, &input);
    assert_eq!(normalized.modes, vec![ActiveRecallMode::Profile]);
    assert!(
        normalized
            .all_diagnostics()
            .iter()
            .any(|diagnostic| { diagnostic == "dropped_mode=exact_canonical:no_canonical_target" })
    );
    assert!(
        normalized
            .all_diagnostics()
            .iter()
            .any(|diagnostic| { diagnostic == "dropped_mode=task_context:no_task_context" })
    );
    assert!(
        normalized
            .all_diagnostics()
            .iter()
            .any(|diagnostic| { diagnostic == "dropped_mode=thread_episodic:no_thread_context" })
    );
}

#[test]
fn active_recall_planner_input_is_structured_from_hook_context() {
    let policy = MemoryTurnPolicy::normal_default_allow();
    let config = MemoryActiveRecallConfig::default().normalized();
    let deterministic_context = prompt_context_set_from_prompt_context_contribution(
        memory_recall_prompt_context_contribution(recalled_city_snapshot())
            .expect("deterministic context contribution"),
    );
    let request = test_active_prompt_context_hook_request(
        memory_policy_set(&policy),
        deterministic_context,
        "short input",
    );
    let post_preflight_input = turn_post_preflight_prompt_context_input(&request)
        .expect("post-preflight prompt context input decodes");
    let input = TurnPrePromptContextHookInput::from_parts(
        post_preflight_input.input_text.clone(),
        post_preflight_input.model.clone(),
        post_preflight_input.model_provider.clone(),
    );
    let context = memory_turn_context_from_prompt_context_request(&request, &input)
        .expect("memory turn context builds");
    let deterministic = deterministic_recall_context_summary(&request.prompt_context_set);

    let planner_input = active_recall_planner_input(
        &context,
        &input,
        &policy,
        &config,
        &deterministic,
        MemoryEpisodicRecallCapabilities::default(),
        MemoryActiveRecallThreadEpisodicSummary::default(),
    );

    assert_eq!(planner_input.workspace_id, "ws");
    assert_eq!(planner_input.thread_id, "thr");
    assert_eq!(planner_input.turn_id, "turn");
    assert!(planner_input.read_allowed);
    assert!(planner_input.active_memory_allowed);
    assert!(!planner_input.explicit_no_memory);
    assert_eq!(
        planner_input.input_length_bucket,
        ActiveRecallInputLengthBucket::VeryShort
    );
    assert_eq!(planner_input.deterministic_context_count, 1);
    assert_eq!(
        planner_input.deterministic_memory_ids,
        vec!["mem_city".to_owned()]
    );
    assert!(!planner_input.has_task_context);
    assert!(
        !planner_input
            .thread_episodic
            .current_thread_recall_available
    );
}

#[test]
fn active_recall_decision_request_renders_sanitized_preflight_input() {
    let context = MemoryActiveRecallDecisionContext {
        workspace_id: "workspace-secret-id".to_owned(),
        thread_id: "thread-secret-id".to_owned(),
        turn_id: "turn-secret-id".to_owned(),
        mode: ThreadMode::Agent,
        input_text_preview: "current bounded input".to_owned(),
        model: Some("test-model".to_owned()),
        model_provider: Some("test-provider".to_owned()),
    };
    let request = MemoryActiveRecallDecisionRequest {
        deterministic_context_count: 0,
        deterministic_context_chars: 0,
        deterministic_memory_ids: Vec::new(),
        deterministic_recall_empty: true,
        has_workspace_context: true,
        has_task_context: false,
        input_length_bucket: ActiveRecallInputLengthBucket::Short.as_str().to_owned(),
        config_mode: MemoryActiveRecallMode::Hybrid,
        read_allowed: true,
        active_memory_allowed: true,
        explicit_no_memory: false,
        input_text_char_count: 21,
        available_modes: vec!["profile".to_owned(), "project".to_owned()],
        available_durable_modes: vec!["profile".to_owned(), "project".to_owned()],
        available_episodic_modes: vec!["current_thread".to_owned()],
        available_scoped_contexts: vec!["workspace".to_owned(), "thread".to_owned()],
        episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
        thread_episodic: MemoryActiveRecallThreadEpisodicSummary {
            current_thread_id_present: true,
            current_thread_recall_available: true,
            related_thread_recall_available: false,
            workspace_thread_recall_available: false,
            prompt_context_source_count: 1,
            prompt_context_chars: 128,
            source_ids: vec!["thread:turn_41/item_1/chunk_0".to_owned()],
            diagnostics: vec!["current_thread_recall_available".to_owned()],
        },
        max_queries: 3,
        top_k_per_query: 5,
        max_prompt_chars: 1_500,
        max_input_chars: 4_000,
        max_output_chars: 2_000,
        fallback_policy: MemoryActiveRecallPlannerFallbackPolicy::Deterministic,
    };

    let json = request.sanitized_input_json(&context);
    assert!(json.contains(r#""workspaceIdPresent": true"#));
    assert!(json.contains(r#""inputTextPreview": "current bounded input""#));
    assert!(json.contains(r#""threadEpisodic""#));
    assert!(!json.contains(r#""recentThreadContext""#));
    assert!(!json.contains("Какая сегодня погода в Москве?"));
    assert!(json.contains(r#""availableDurableModes""#));
    assert!(json.contains(r#""availableEpisodicModes""#));
    assert!(json.contains(r#""currentThreadRecallAvailable": true"#));
    assert!(json.contains(r#""thread:turn_41/item_1/chunk_0""#));
    assert!(!json.contains("workspace-secret-id"));
    assert!(!json.contains("thread-secret-id"));
    assert!(!json.contains("turn-secret-id"));
    assert!(!json.contains("tool schema"));
    assert!(!json.contains("hidden system prompt content"));
}

#[test]
fn active_recall_thread_episodic_summary_is_bounded_and_metadata_only() {
    let context = MemoryTurnContext {
        workspace_id: "ws".to_owned(),
        thread_id: "thr".to_owned(),
        turn_id: "turn".to_owned(),
        mode: ThreadMode::Agent,
        input_text: "continue".to_owned(),
        task_id: None,
        agent_id: None,
    };
    let long_content = (0..12)
        .map(|index| {
            format!("- [thread:turn_{index}/item_1/chunk_0, role=user] Raw chunk text {index}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let prompt_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: HookDomain::new("thread_context").expect("valid domain"),
        priority: 480,
        content: HookPromptContent::new(long_content).expect("valid prompt content"),
        max_chars: Some(4_000),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [prompt_context],
        HookPromptContextLimits::default(),
    );
    let summary = active_recall_thread_episodic_summary(
        &prompt_context_set,
        &context,
        &MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            related_thread_search: true,
            workspace_thread_search: false,
            current_task_context: false,
            completed_task_summary: false,
        },
    );

    assert!(summary.current_thread_id_present);
    assert!(summary.current_thread_recall_available);
    assert!(summary.related_thread_recall_available);
    assert_eq!(summary.source_ids.len(), 8);
    assert!(
        summary
            .diagnostics
            .contains(&"thread_source_ids_truncated".to_owned())
    );
    assert!(!format!("{summary:?}").contains("Raw chunk text"));
}

#[test]
fn active_recall_query_plan_uses_debug_fallback_only_when_explicit() {
    let config = MemoryActiveRecallConfig {
        max_queries: 2,
        ..MemoryActiveRecallConfig::default()
    };
    let normal_plan = ActiveRecallPlan::run(
        ActiveMemoryDecisionReasonCode::MemoryLikely,
        0.8,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    assert!(active_memory_query_plan("как меня зовут?", &normal_plan, &config).is_empty());

    let debug_plan = ActiveRecallPlan::run(
        ActiveMemoryDecisionReasonCode::StrictDebug,
        1.0,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_debug_fallback();
    let queries = active_memory_query_plan("как меня зовут?", &debug_plan, &config);
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0].query, MEMORY_ACTIVE_RECALL_GENERIC_QUERY);
    assert_eq!(queries[1].query, "как меня зовут?");
}

#[test]
fn deterministic_active_recall_plan_uses_only_structural_context() {
    let mut input = active_recall_planner_input_for_test();
    input.has_workspace_context = false;
    input.input_text_preview = "continue like yesterday".to_owned();
    let english = deterministic_active_recall_plan(&input);

    input.input_text_preview = "продолжай как вчера".to_owned();
    let russian = deterministic_active_recall_plan(&input);

    assert_eq!(english.status, ActiveMemoryDecisionStatus::Uncertain);
    assert_eq!(russian.status, ActiveMemoryDecisionStatus::Uncertain);
    assert_eq!(english.modes, russian.modes);

    input.has_task_context = true;
    input.episodic_capabilities.current_task_context = true;
    let task_plan = deterministic_active_recall_plan(&input);
    assert_eq!(
        task_plan.effective_status(),
        ActiveMemoryDecisionStatus::Run
    );
    assert_eq!(task_plan.durable.modes, Vec::<ActiveRecallMode>::new());
    assert_eq!(
        task_plan.selected_modes(),
        vec![ActiveRecallMode::CurrentTask, ActiveRecallMode::TaskContext]
    );

    input.typed_targets = vec![ActiveRecallTarget::exact_canonical(
        "user/global:identity:self:name",
    )];
    let exact_plan = deterministic_active_recall_plan(&input);
    assert_eq!(exact_plan.modes, vec![ActiveRecallMode::ExactCanonical]);
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
                MEMORY_LIST_TOOL,
                MEMORY_SEARCH_TOOL,
            ],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");

    let content = prompt_section_content(response).expect("prompt section is rendered");
    assert!(content.contains(
        "Available memory tools: memory_search, memory_list, memory_get, memory_remember, memory_forget."
    ));
    assert!(content.contains("Use memory_list, not memory_search"));
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
    assert!(content.contains("Additional active memory context for this turn:"));
    assert!(content.contains("Use hooks for memory domains."));
    assert!(!content.contains("Unrelated memory-domain context"));
}

#[tokio::test]
async fn memory_prompt_contract_deduplicates_thread_context_consumed_by_active_synthesis() {
    let hook = MemoryPromptContractHook;
    let active_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: memory_policy_domain(),
        priority: 480,
        content: HookPromptContent::new(
            "- active synthesis: Continue with the hook-based prompt contract. Sources: thread:turn_41/item_1/chunk_0",
        )
        .expect("valid prompt content"),
        max_chars: Some(500),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let thread_context = PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID)
            .expect("valid contribution id"),
        domain: HookDomain::new("thread_context").expect("valid domain"),
        priority: 480,
        content: HookPromptContent::new(
            "- [thread:turn_41/item_1/chunk_0, role=user, context=message, score=0.91] Duplicate direct context.\n- [thread:turn_42/item_1/chunk_0, role=assistant, context=message, score=0.83] Retained direct context.",
        )
        .expect("valid prompt content"),
        max_chars: Some(1_000),
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    };
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        [active_context, thread_context],
        HookPromptContextLimits::default(),
    );

    let response = hook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &[MEMORY_SEARCH_TOOL],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract hook executes");

    let sections = prompt_section_contents(response);
    let combined = sections
        .iter()
        .map(|(_, content)| content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let thread_content = sections
        .iter()
        .find(|(section_id, _)| section_id == "thread_context")
        .map(|(_, content)| content.as_str())
        .expect("thread context section renders with retained non-duplicate hit");

    assert_eq!(combined.matches("thread:turn_41/item_1/chunk_0").count(), 1);
    assert!(!thread_content.contains("Duplicate direct context."));
    assert!(thread_content.contains("thread:turn_42/item_1/chunk_0"));
    assert!(thread_content.contains("Retained direct context."));
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
    assert!(content.contains("Relevant memory context for this turn:"));
    assert!(content.contains("User likes Porto."));
    assert!(!content.contains("Additional active memory context for this turn:"));
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
    assert!(!content.contains("Additional active memory context for this turn:"));
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
    assert!(content.contains("Additional active memory context for this turn:"));
    assert!(content.contains("Use hooks for memory domains."));
    let active_section = content
        .split("Additional active memory context for this turn:")
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
    assert!(!content.contains("Additional active memory context for this turn:"));
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

    assert!(content.contains("Relevant memory context for this turn:"));
    assert!(content.contains("Additional active memory context for this turn:"));
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

fn active_recall_planner_input_for_test() -> ActiveRecallPlannerInput {
    ActiveRecallPlannerInput {
        workspace_id: "ws".to_owned(),
        thread_id: "thr".to_owned(),
        turn_id: "turn".to_owned(),
        task_id: None,
        agent_id: None,
        mode: ThreadMode::Agent,
        input_text_preview: "test input".to_owned(),
        input_text_char_count: 10,
        input_length_bucket: ActiveRecallInputLengthBucket::VeryShort,
        read_allowed: true,
        active_memory_allowed: true,
        explicit_no_memory: false,
        config_mode: MemoryActiveRecallMode::Hybrid,
        deterministic_context_count: 0,
        deterministic_context_chars: 0,
        deterministic_memory_ids: Vec::new(),
        deterministic_recall_empty: true,
        deterministic_categories: Vec::new(),
        typed_targets: Vec::new(),
        has_workspace_context: true,
        has_task_context: false,
        episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
        thread_episodic: MemoryActiveRecallThreadEpisodicSummary::default(),
    }
}
