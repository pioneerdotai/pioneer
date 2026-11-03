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
            .unwrap()
            .count,
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

#[tokio::test]
async fn failed_gate_bookkeeping_dispatches_independent_hooks_and_defers_worker() {
    use pioneer_entity::native_terminal_effect_outbox as effect;
    use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Expr};

    let (processor, _, _rx, workspace, store, _) = cli_runtime_approval_processor().await;
    let processor = Arc::new(processor);
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(calls.clone()),
    )
    .await;
    for suffix in ["poison", "healthy"] {
        let turn_id = format!("gate-bookkeeping-{suffix}");
        seed_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "codex",
            "codex",
            &format!("thread-{turn_id}"),
            &turn_id,
            &format!("native-{turn_id}"),
            "input",
        )
        .await;
        let binding = store
            .get_cli_runtime_turn_binding(&turn_id)
            .await
            .unwrap()
            .unwrap();
        processor
            .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
            .await
            .unwrap();
        commit_turn(&store, &binding).await;
    }
    let poison = "gate-bookkeeping-poison:terminal-effect:post-turn";
    effect::Entity::update_many()
        .col_expr(effect::Column::Status, Expr::value("waiting_acceptance"))
        .col_expr(
            effect::Column::NextRunAt,
            Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
        )
        .col_expr(
            effect::Column::GateKind,
            Expr::value("accepted_task_result"),
        )
        .filter(effect::Column::EffectId.eq(poison))
        .exec(&store.database_connection())
        .await
        .unwrap();
    // Reject reservation AND conditional deferral permanently. Healthy claims
    // must still reach the actual hook executor, not just return from CRUD.
    store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_gate_bookkeeping BEFORE UPDATE OF gate_probe_at ON native_terminal_effect_outbox
         WHEN OLD.effect_id='gate-bookkeeping-poison:terminal-effect:post-turn'
         BEGIN SELECT RAISE(ABORT,'local bookkeeping fault'); END"
    ).await.unwrap();
    let outcome = processor
        .process_due_native_terminal_effects(chrono::Utc::now().timestamp(), 8)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.count, 1);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .native_terminal_effect_status("gate-bookkeeping-healthy:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "succeeded"
    );

    // Put another independently executable hook through the real kick loop.
    let turn_id = "gate-bookkeeping-next";
    seed_cli_runtime_turn_with_text(
        &store,
        &workspace,
        "codex",
        "codex",
        "thread-gate-next",
        turn_id,
        "native-gate-next",
        "input",
    )
    .await;
    let binding = store
        .get_cli_runtime_turn_binding(turn_id)
        .await
        .unwrap()
        .unwrap();
    processor
        .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
        .await
        .unwrap();
    commit_turn(&store, &binding).await;
    processor.kick_native_terminal_effects();
    timeout(Duration::from_secs(3), async {
        loop {
            if store
                .native_terminal_effect_status("gate-bookkeeping-next:terminal-effect:post-turn")
                .await
                .unwrap()
                .unwrap()
                .status
                == "succeeded"
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.lock().unwrap().len(), 2);
    // Let completion return to the worker before freezing its five-second sleep.
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    processor.kick_native_terminal_effects();
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(
        processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
    );
    assert!(
        processor
            .native_terminal_effect_kick_pending
            .load(Ordering::Acquire)
    );
    tokio::time::resume();
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_gate_bookkeeping")
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        while processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let row = effect::Entity::find_by_id(poison)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.attempt_count, 0);
    assert_eq!(row.status, "waiting_acceptance");
}

#[tokio::test]
async fn terminal_effect_single_kick_drains_more_than_one_full_quantum() {
    for (rotation, first_wave) in [(0_i64, 3_usize), (1, 8), (2, 4)] {
        let (processor, _, _rx, workspace, store, _) = cli_runtime_approval_processor().await;
        let processor = Arc::new(processor);
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        install_recoverable_test_hook_runtime(
            &processor,
            task_post_turn_recording_hook_runtime(calls.clone()),
        )
        .await;
        for index in 0..17 {
            let turn_id = format!("terminal-drain-{index}");
            seed_cli_runtime_turn_with_text(
                &store,
                &workspace,
                "codex",
                "codex",
                &format!("thread-{turn_id}"),
                &turn_id,
                &format!("native-{turn_id}"),
                "input",
            )
            .await;
            let binding = store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            processor
                .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
                .await
                .unwrap();
            commit_turn(&store, &binding).await;
            assert_eq!(
                store
                    .native_terminal_effect_status(&format!("{turn_id}:terminal-effect:post-turn"))
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "ready"
            );
        }
        // Each rotation gets a fresh ready-only backlog. Fix the discovery clock
        // after seeding, far enough ahead that every prepared row is due. The
        // existing cfg(test) claim path also uses this time after writer admission.
        let discovery_now = (now_timestamp_secs() + 3600).div_euclid(6) * 6 + rotation * 2;
        let wave_starts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_starts = wave_starts.clone();
        let observed_calls = calls.clone();
        // No resilience worker is started. This is one kick into the real owner,
        // with no producer kicks between bounded waves. Observe completed hook
        // calls at the next discovery to verify the actual first claim quota.
        processor.spawn_native_terminal_effect_kick(Some(Box::new(move || {
            observed_starts
                .lock()
                .unwrap()
                .push(observed_calls.lock().unwrap().len());
            discovery_now
        })));
        timeout(Duration::from_secs(10), async {
            while processor
                .native_terminal_effect_kick_running
                .load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.lock().unwrap().len(), 17, "rotation {rotation}");
        let starts = wave_starts.lock().unwrap();
        assert_eq!(starts[0], 0);
        assert_eq!(starts[1], first_wave, "rotation {rotation}: {starts:?}");
        drop(starts);
        for index in 0..17 {
            assert_eq!(
                store
                    .native_terminal_effect_status(&format!(
                        "terminal-drain-{index}:terminal-effect:post-turn"
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "succeeded"
            );
        }
    }
}

#[tokio::test]
async fn terminal_effect_owner_retains_lower_retry_deadline_and_coalesces_settled_kicks() {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(calls.clone()),
    )
    .await;
    let label = "__terminal_effect_owner_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    let worker = tokio::spawn(MessageProcessor::run_native_terminal_effect_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    processor.arm_completed_history_preparation_barrier(label);
    store.background_work("recovery.effects").invalidate();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let lower = CrudStore::new(store.database_connection());
    seed_cli_runtime_turn_with_text(
        &lower,
        &workspace,
        "codex",
        "codex",
        "effect-owner-thread",
        "effect-owner-turn",
        "native-effect-owner",
        "input",
    )
    .await;
    let binding = lower
        .get_cli_runtime_turn_binding("effect-owner-turn")
        .await
        .unwrap()
        .unwrap();
    processor
        .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
        .await
        .unwrap();
    commit_turn(&lower, &binding).await;
    // A real prior claim/failure creates its durable retry. No kick or payload
    // event replay is needed to discover this supported lower-store mutation.
    let now = now_timestamp_secs();
    let claimed = lower
        .claim_due_native_terminal_effects(now, 90, 8)
        .await
        .unwrap();
    assert_eq!(claimed.records.len(), 1);
    let claimed = &claimed.records[0];
    assert!(
        lower
            .fail_native_terminal_effect(
                &claimed.effect_id,
                &claimed.claim_token,
                "effect_timeout",
                "fixture transient",
                true,
                now + 3,
                now
            )
            .await
            .unwrap()
    );
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        calls.lock().unwrap().is_empty(),
        "future retry cannot dispatch early"
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(7),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert!(observer.reads.lock().unwrap().iter().all(|event| !matches!(event,
        pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued {class,..} if *class!=pioneer_sqlite::SqliteReadClass::Maintenance)));
    assert_eq!(
        lower
            .native_terminal_effect_status("effect-owner-turn:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "succeeded"
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    for _ in 0..1000 {
        processor.kick_native_terminal_effects();
    }
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(
                event,
                pioneer_sqlite::SqliteReadEvent::OperationFinished {
                    class: pioneer_sqlite::SqliteReadClass::Maintenance,
                    ..
                }
            ))
            .count(),
        1,
        "retained hints coalesce into one metadata observation, without an empty claim writer"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "wake hints never replay business effects"
    );
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
}

// Both matrices use the actual pooled file runtime and the existing owner
// barriers. No recovery kick, synthetic receipt, or direct handler call rescues
// the operation after the race has been released.
async fn activation_owner_barrier(processor: &MessageProcessor) {
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .expect("owner must reach its armed publication boundary");
}

async fn prepare_frontier_intent(store: &CrudStore, workspace: &str, index: usize, at: i64) {
    let mut answer = final_answer("accepted answer");
    if let TurnItem::AgentMessage { id, .. } = &mut answer {
        *id = format!("frontier-answer-{index:03}");
    }
    store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id: workspace.into(),
                thread_id: format!("frontier-thread-{index:03}"),
                turn_id: format!("frontier-turn-{index:03}"),
                item: answer,
            },
            1,
            None,
            at,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn finalization_owner_rescans_new_intent_after_owner_or_foreground_frontier() {
    use pioneer_entity::turn_finalization as intent;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    for foreground in [false, true] {
        let observer = Arc::new(NativeSchedulingObserver::default());
        let (_directory, manager, store, workspace) =
            setup_pooled_file_workspace_manager_with_observers(
                Some(observer.clone()),
                Some(observer.clone()),
            )
            .await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let lower = CrudStore::new(store.database_connection());
        let now = now_timestamp_secs();
        // All 65 real Turns already exist; only the final supported PREPARE is
        // withheld. Ascending prepared_at gives an unambiguous finite upper.
        for index in 0..65 {
            materialize_cli_runtime_turn_with_text(
                &lower,
                &workspace,
                &format!("frontier-thread-{index:03}"),
                &format!("frontier-turn-{index:03}"),
                "question",
            )
            .await;
        }
        if !foreground {
            for index in 0..64 {
                prepare_frontier_intent(&lower, &workspace, index, now).await;
            }
        }
        let label = "__finalization_before_wait__";
        processor.arm_completed_history_preparation_barrier(label);
        let worker = tokio::spawn(MessageProcessor::run_native_projection_owner(
            Arc::downgrade(&processor),
            true,
        ));
        activation_owner_barrier(&processor).await;
        if foreground {
            // Hold the owner's bootstrap EOF while the ordinary foreground
            // entry point establishes and advances the SAME shared finite pass.
            for index in 0..64 {
                prepare_frontier_intent(&lower, &workspace, index, now).await;
            }
            let summary = lower
                .reconcile_prepared_turn_finalizations(64, now)
                .await
                .unwrap();
            assert_eq!(summary.committed_count(), 64);
        }
        let db = lower.database_connection();
        assert_eq!(
            intent::Entity::find()
                .filter(intent::Column::Status.eq("committed"))
                .all(&db)
                .await
                .unwrap()
                .len(),
            64
        );
        prepare_frontier_intent(&lower, &workspace, 64, now + 1).await;
        // No further writes or kicks. The next quantum reaches EOF of OLD U;
        // only retained rescan debt can make a fresh pass visit N > U.
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        activation_owner_barrier(&processor).await;
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        activation_owner_barrier(&processor).await;
        let row = intent::Entity::find_by_id("frontier-turn-064")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "committed", "foreground={foreground}");
        assert!(row.committed_at.is_some());
        assert_eq!(
            lower
                .get_turn("frontier-thread-064", "frontier-turn-064")
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Completed
        );
        // This <64 fresh page includes N and establishes settled EOF. Release
        // its barrier and wait for any already spawned optional deliveries.
        processor.release_completed_history_preparation_barrier();
        timeout(Duration::from_secs(10), async {
            while processor
                .native_turn_event_delivery_workers
                .live
                .running
                .load(Ordering::Acquire)
                || processor
                    .native_turn_event_delivery_workers
                    .episodic
                    .running
                    .load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "foreground={foreground}"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        worker.abort();
        let _ = worker.await;
    }
}

#[tokio::test]
async fn dormant_effect_owner_is_woken_by_replay_and_recovered_finalization_activation() {
    use pioneer_crud::CanonicalTurnEventPayload;
    for finalization in [false, true] {
        let observer = Arc::new(NativeSchedulingObserver::default());
        let (_directory, manager, store, workspace) =
            setup_pooled_file_workspace_manager_with_observers(
                Some(observer.clone()),
                Some(observer.clone()),
            )
            .await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        install_recoverable_test_hook_runtime(
            &processor,
            task_post_turn_recording_hook_runtime(calls.clone()),
        )
        .await;
        let lower = CrudStore::new(store.database_connection());
        let thread = "activation-thread";
        let turn = "activation-turn";
        seed_cli_runtime_turn_with_text(
            &lower,
            &workspace,
            "codex",
            "codex",
            thread,
            turn,
            "native-activation-thread",
            "question",
        )
        .await;
        let binding = lower
            .get_cli_runtime_turn_binding(turn)
            .await
            .unwrap()
            .unwrap();
        let answer = final_answer("accepted answer");
        processor
            .prepare_cli_post_turn_hook(&binding, 1, Some(&answer))
            .await
            .unwrap();
        let effect_id = format!("{turn}:terminal-effect:post-turn");
        assert_eq!(
            lower
                .native_terminal_effect_status(&effect_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "prepared"
        );
        let now = now_timestamp_secs();
        // Real accepted predecessor, with its production claim already expired.
        // No fabricated projection status or watermark is inserted.
        let mut predecessor = final_answer("causal predecessor");
        if let TurnItem::AgentMessage { id, .. } = &mut predecessor {
            *id = "activation-predecessor".into();
        }
        lower
            .accept_turn_event_before_projection_for_test(
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item: predecessor,
                }),
                now - 61,
            )
            .await
            .unwrap();
        let mut finalizer = None;
        if finalization {
            lower
                .prepare_turn_finalization(
                    &ItemCompletedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: thread.into(),
                        turn_id: turn.into(),
                        item: answer,
                    },
                    1,
                    None,
                    now,
                )
                .await
                .unwrap();
            processor.arm_completed_history_preparation_barrier("__finalization_before_wait__");
            finalizer = Some(tokio::spawn(MessageProcessor::run_native_projection_owner(
                Arc::downgrade(&processor),
                true,
            )));
            activation_owner_barrier(&processor).await;
            processor.release_completed_history_preparation_barrier();
        } else {
            let (_, mut terminal) = lower.get_turn(thread, turn).await.unwrap().unwrap();
            terminal.status = TurnStatus::Completed;
            let error = lower
                .materialize_turn_completed(
                    TurnCompletedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: thread.into(),
                        turn: terminal,
                    },
                    now,
                )
                .await
                .unwrap_err();
            assert!(pioneer_crud::turn_event_was_appended_before_error(&error));
        }
        // The effect owner starts AFTER all early prepare/append wakes and
        // legitimately settles PREPARED as Dormant before any activation.
        processor.arm_completed_history_preparation_barrier("__terminal_effect_owner_settled__");
        let effects = tokio::spawn(MessageProcessor::run_native_terminal_effect_worker(
            Arc::downgrade(&processor),
        ));
        activation_owner_barrier(&processor).await;
        assert!(calls.lock().unwrap().is_empty());
        assert_eq!(
            lower
                .native_terminal_effect_status(&effect_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "prepared"
        );
        assert!(
            lower
                .native_terminal_effect_work_deadline()
                .await
                .unwrap()
                .is_none()
        );
        processor.release_completed_history_preparation_barrier();
        // This is the only subsequently started producer. Ordinary replay
        // commits the predecessor, then either projects terminal itself or
        // releases the already dormant finalization owner. No manual kick.
        let replay = tokio::spawn(MessageProcessor::run_native_projection_owner(
            Arc::downgrade(&processor),
            false,
        ));
        timeout(Duration::from_secs(10), async {
            loop {
                let effect = lower
                    .native_terminal_effect_status(&effect_id)
                    .await
                    .unwrap()
                    .unwrap();
                if effect.status == "succeeded" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late activation must wake the dormant effect owner");
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "finalization={finalization}"
        );
        assert_eq!(
            lower
                .get_turn(thread, turn)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Completed
        );
        if finalization {
            use sea_orm::EntityTrait;
            let row = pioneer_entity::turn_finalization::Entity::find_by_id(turn)
                .one(&lower.database_connection())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.status, "committed");
        }
        replay.abort();
        let _ = replay.await;
        if let Some(worker) = finalizer {
            worker.abort();
            let _ = worker.await;
        }
        effects.abort();
        let _ = effects.await;
    }
}

#[tokio::test]
async fn retry_owner_crosses_excluded_raw_history_requeues_tail_and_parks() {
    use pioneer_entity::native_terminal_effect_outbox as effect;
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    install_recoverable_test_hook_runtime(
        &processor,
        task_post_turn_recording_hook_runtime(Arc::new(std::sync::Mutex::new(Vec::new()))),
    )
    .await;
    let now = now_timestamp_secs();
    for i in 0..132 {
        let turn = format!("f05-retry-{i:03}");
        seed_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "codex",
            "codex",
            &format!("thread-{turn}"),
            &turn,
            &format!("native-{turn}"),
            "input",
        )
        .await;
        let binding = store
            .get_cli_runtime_turn_binding(&turn)
            .await
            .unwrap()
            .unwrap();
        processor
            .prepare_cli_post_turn_hook(&binding, 1, Some(&final_answer("done")))
            .await
            .unwrap();
        commit_turn(&store, &binding).await;
        let id = format!("{turn}:terminal-effect:post-turn");
        // Restored unresolved history has real production preparation/identity.
        let mut row: effect::ActiveModel = effect::Entity::find_by_id(&id)
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .into();
        row.status = Set("unresolved".into());
        row.next_run_at = Set(None);
        row.completed_at = Set(Some(pioneer_crud::unix_to_datetime(now - 3601)));
        row.last_error_code = Set(Some(
            if i < 65 {
                "memory.post_turn_extractor.provider_http_403"
            } else {
                "effect_timeout"
            }
            .into(),
        ));
        if (65..130).contains(&i) {
            row.prepared_at = Set(pioneer_crud::unix_to_datetime(now - 90000));
        }
        if i == 131 {
            row.completed_at = Set(Some(pioneer_crud::unix_to_datetime(now)));
        }
        row.update(&store.database_connection()).await.unwrap();
    }
    processor.arm_completed_history_preparation_barrier("__effect_retry_raw_page__");
    let worker = tokio::spawn(MessageProcessor::run_native_terminal_effect_archive_worker(
        Arc::downgrade(&processor),
        false,
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .native_terminal_effect_status("f05-retry-130:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "unresolved",
        "excluded first page cannot consume the healthy tail"
    );
    processor.arm_completed_history_preparation_barrier("__effect_retry_settled__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .native_terminal_effect_status("f05-retry-130:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "retry_wait"
    );
    assert_eq!(
        store
            .native_terminal_effect_status("f05-retry-000:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "unresolved"
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    ticker.abort();
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // Old-key restored eligible transition uses the same explicit source guard;
    // no owner kick or heartbeat repairs the completed frontier.
    let change = store.background_work("recovery.effects.retry").changing();
    let mut old: effect::ActiveModel =
        effect::Entity::find_by_id("f05-retry-000:terminal-effect:post-turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .into();
    old.last_error_code = Set(Some("effect_timeout".into()));
    processor.arm_completed_history_preparation_barrier("__effect_retry_settled__");
    old.update(&store.database_connection()).await.unwrap();
    change.finish(true);
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .native_terminal_effect_status("f05-retry-000:terminal-effect:post-turn")
            .await
            .unwrap()
            .unwrap()
            .status,
        "retry_wait"
    );
    // All remaining unresolved inputs are excluded. EOF has no retry clock.
    let change = store.background_work("recovery.effects.retry").changing();
    let mut last: effect::ActiveModel =
        effect::Entity::find_by_id("f05-retry-131:terminal-effect:post-turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .into();
    last.last_error_code = Set(Some("memory.post_turn_extractor.provider_http_403".into()));
    last.update(&store.database_connection()).await.unwrap();
    change.finish(true);
    processor.arm_completed_history_preparation_barrier("__effect_retry_settled__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    ticker.abort();
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    processor.cli_history_shutdown.cancel();
    timeout(Duration::from_secs(10), worker)
        .await
        .unwrap()
        .unwrap();
}
