use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_post_turn_extracts_user_memory_through_shared_provider_and_durable_worker() {
    let extracted = json!({"facts": [{
        "semantic": {"intent":"explicit_store", "explicitness":"explicit", "category":"identity",
            "subject":"current_user", "attribute":"name", "scope_hint":"user_global",
            "durability":"long_lived", "sensitivity":"personal", "certainty":"high"},
        "ontology": {"fact_class":"user_identity", "lifetime_class":"long_lived",
            "evidence_class":"direct_user_assertion", "proposed_ownership_class":"durable_user_memory"},
        "content":"Имя пользователя: Александр", "value":"Александр",
        "evidence":{"source_ref":"turn.post_turn:user", "quote_or_span":"Меня зовут Александр",
            "extractor_reason":"The user directly stated their name."},
        "confidence":0.98, "importance":0.7
    }]}).to_string();
    let provider = Arc::new(CaptureSummaryProvider::new(extracted));
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));
    let mut config = test_tool_loop_config();
    config.memory.post_turn_extractor.enabled = true;
    config.memory.post_turn_extractor.provider_enabled = true;
    config.memory.post_turn_extractor.proactive_writes_enabled = true;
    config.memory.post_turn_extractor.provider_name = Some("openai".into());
    config.memory.post_turn_extractor.model = Some("test-model".into());
    let harness =
        setup_memory_agent_e2e_harness_with_tool_loop_config("cli_post_turn", registry, config)
            .await;
    // Both external adapters enter the same Pioneer turn completion path.
    for (kind, turn_id) in [
        ("codex", "cli-memory-codex"),
        ("claude", "cli-memory-claude"),
    ] {
        let thread_id = format!("thread-{turn_id}");
        seed_cli_runtime_turn_with_text(
            &harness.crud_store,
            &harness.workspace_id,
            kind,
            kind,
            &thread_id,
            turn_id,
            &format!("native-{turn_id}"),
            "Меня зовут Александр",
        )
        .await;
        let binding = harness
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await
            .unwrap()
            .unwrap();
        harness
            .processor
            .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("Запомнил.")))
            .await
            .unwrap();
        commit_turn(&harness.crud_store, &binding).await;
        harness
            .processor
            .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
            .await
            .unwrap();
        let status = harness
            .crud_store
            .native_terminal_effect_status(&format!("{turn_id}:terminal-effect:post-turn"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status, "succeeded", "{kind}: {status:?}");
    }
    assert_eq!(provider.call_count(), 2);
    let records = harness
        .crud_store
        .list_agent_memory_records(pioneer_crud::AgentMemoryListFilter {
            scopes: vec![user_memory_scope()],
            limit: Some(10),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        1,
        "canonical user fact is updated, not duplicated across CLI turns"
    );
    assert_eq!(
        records[0].source_turn_id.as_deref(),
        Some("cli-memory-codex"),
        "an unchanged duplicate preserves the original evidence, just like native extraction"
    );
    let requests = provider.snapshot_requests();
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|m| m.content.contains("Меня зовут Александр"))
    );
}

fn final_answer(text: &str) -> TurnItem {
    TurnItem::AgentMessage {
        id: "cli-final".to_owned(),
        text: text.to_owned(),
        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    }
}

#[tokio::test]
async fn cli_post_turn_tool_summaries_are_bounded_metadata_not_tool_output() {
    let (processor, _, _rx, workspace, store, _) = cli_runtime_approval_processor().await;
    let processor = Arc::new(processor);
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(calls.clone()),
    )
    .await;
    for id in ["summary-a", "summary-b"] {
        let mut item = command_execution_item(id);
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: "thread_cli_command_approval".into(),
                    turn_id: "codex-turn-command".into(),
                    item: item.clone(),
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        if let TurnItem::CommandExecution {
            status,
            success,
            outcome,
            arguments,
            ..
        } = &mut item
        {
            *status = pioneer_protocol::ToolCallStatus::Completed;
            *success = Some(true);
            *arguments = json!({"private_argument":"must-not-be-extracted"});
            *outcome = Some(pioneer_protocol::ToolOutcome {
                status: pioneer_protocol::ToolOutcomeStatus::Ok,
                error_class: None,
                should_retry: false,
                retry_hint: None,
                incomplete: false,
                incomplete_reason: None,
            });
        }
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: "thread_cli_command_approval".into(),
                    turn_id: "codex-turn-command".into(),
                    item,
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .post_turn_tool_summaries("codex-turn-command", 1)
            .await
            .unwrap()
            .len(),
        1
    );
    let binding = store
        .get_cli_runtime_turn_binding("codex-turn-command")
        .await
        .unwrap()
        .unwrap();
    processor
        .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
        .await
        .unwrap();
    commit_turn(&store, &binding).await;
    processor
        .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
        .await
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let pioneer_hooks::HookInputPayload::TurnPostTurn(input) = &calls[0].input.payload else {
        panic!("post-turn input expected")
    };
    assert_eq!(input.tool_events.len(), 2);
    assert_eq!(input.domain_events.len(), 2);
    assert_eq!(
        input.tool_events[0].outcome_status,
        Some(pioneer_hooks::TurnPostTurnToolOutcomeStatus::Ok)
    );
    assert!(
        !serde_json::to_string(input)
            .unwrap()
            .contains("must-not-be-extracted")
    );
}

async fn commit_turn(store: &CrudStore, binding: &pioneer_crud::CliRuntimeTurnBindingRecord) {
    let (_, mut turn) = store
        .get_turn(&binding.thread_id, &binding.turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: binding.workspace_id.clone(),
                thread_id: binding.thread_id.clone(),
                turn,
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn cli_post_turn_replay_preserves_first_snapshot_and_executes_once_after_commit() {
    let (processor, _, _rx, _, store, _) = cli_runtime_approval_processor().await;
    let processor = Arc::new(processor);
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(calls.clone()),
    )
    .await;
    let binding = store
        .get_cli_runtime_turn_binding("codex-turn-command")
        .await
        .unwrap()
        .unwrap();
    let answer = final_answer("done");
    let (first, concurrent_replay) = tokio::join!(
        processor.prepare_cli_post_turn_hook(&binding, 1, Some(&answer)),
        processor.prepare_cli_post_turn_hook(&binding, 1, Some(&answer)),
    );
    first.unwrap();
    concurrent_replay.unwrap();
    assert_eq!(
        processor
            .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
            .await
            .unwrap(),
        0
    );
    assert!(calls.lock().unwrap().is_empty());
    // A duplicate event must not replace the admitted request, even if its
    // payload or live hook configuration has changed.
    processor.agent_manager.set_hook_runtime(None).await;
    processor
        .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("different")))
        .await
        .unwrap();
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(calls.clone()),
    )
    .await;
    commit_turn(&store, &binding).await;
    processor
        .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
        .await
        .unwrap();
    processor
        .prepare_cli_post_turn_hook(&binding, 1, Some(&answer))
        .await
        .unwrap();
    processor
        .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
        .await
        .unwrap();
    let status = store
        .native_terminal_effect_status("codex-turn-command:terminal-effect:post-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, "succeeded", "{status:?}");
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let value = serde_json::to_value(&calls[0]).unwrap().to_string();
    assert!(value.contains("approval"));
    assert!(value.contains("done"));
    assert!(!value.contains("different"));
}

#[tokio::test]
async fn cli_post_turn_rejects_obsolete_attempt_and_non_final_item_without_writing() {
    let (processor, _, _rx, _, store, _) = cli_runtime_approval_processor().await;
    let processor = Arc::new(processor);
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(Default::default()),
    )
    .await;
    let binding = store
        .get_cli_runtime_turn_binding("codex-turn-command")
        .await
        .unwrap()
        .unwrap();
    assert!(
        processor
            .prepare_cli_post_turn_hook(&binding, 2, Some(&final_answer("done")))
            .await
            .is_err()
    );
    let not_final = TurnItem::UserMessage {
        id: "wrong".into(),
        text: "wrong".into(),
        attachments: Vec::new(),
    };
    assert!(
        processor
            .prepare_cli_post_turn_hook(&binding, 1, Some(&not_final))
            .await
            .is_err()
    );
    assert!(
        store
            .native_terminal_effect_status("codex-turn-command:terminal-effect:post-turn")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cli_post_turn_without_subscribers_is_noop_and_user_preview_is_bounded() {
    let (processor, _, _rx, _, store, _) = cli_runtime_approval_processor().await;
    let binding = store
        .get_cli_runtime_turn_binding("codex-turn-command")
        .await
        .unwrap()
        .unwrap();
    processor
        .prepare_cli_post_turn_hook(&binding, 1, None)
        .await
        .unwrap();
    assert!(
        store
            .native_terminal_effect_status("codex-turn-command:terminal-effect:post-turn")
            .await
            .unwrap()
            .is_none()
    );
    let (text, truncated) = store
        .post_turn_user_text(&binding.turn_id, 3)
        .await
        .unwrap();
    assert_eq!(text, "app");
    assert!(truncated);
    let (text, truncated) = store
        .post_turn_user_text(&binding.turn_id, 100)
        .await
        .unwrap();
    assert_eq!(text, "approval");
    assert!(!truncated);
}
