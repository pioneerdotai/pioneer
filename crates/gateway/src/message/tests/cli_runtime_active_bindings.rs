use super::*;
use crate::message::cli_runtime::CliRuntimeStaleTurnScan;
use pioneer_entity::{turn, turn_cli_runtime_binding as binding, turn_liveness};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter};

// Real persisted Turns make the healthy prefix exercise the validator rather
// than its missing-Turn shortcut. Equal timestamps exercise the turn_id tie.
async fn insert_turn(
    store: &CrudStore,
    workspace: &str,
    id: &str,
    status: &str,
    created_at: chrono::DateTime<chrono::FixedOffset>,
    activity_at: chrono::DateTime<chrono::FixedOffset>,
) {
    materialize_cli_runtime_turn_with_text(store, workspace, id, id, "active scan fixture").await;
    store
        .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
            turn_id: id.into(),
            thread_id: id.into(),
            continuation_thread_id: id.into(),
            workspace_id: workspace.into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: format!("native_{id}"),
            native_turn_id: None,
            request_id: None,
            status: status.into(),
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".into(),
            created_at,
            updated_at: activity_at,
        })
        .await
        .unwrap();
    turn_liveness::Entity::update_many()
        .col_expr(
            turn_liveness::Column::LastActivityAt,
            Expr::value(activity_at),
        )
        .filter(turn_liveness::Column::TurnId.eq(id))
        .exec(&store.database_connection())
        .await
        .unwrap();
}

async fn has_recovery(store: &CrudStore, id: &str) -> bool {
    store
        .find_unresolved_recovery_job_for_turn(id)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_scan_reaches_stale_after_130_healthy_with_errors_and_wrap() {
    let (processor, _, rx, workspace, store, _session) = cli_runtime_approval_processor().await;
    drop(rx);
    // Keep the fixture's sessions, but discovery here has its own active set.
    binding::Entity::delete_many()
        .exec(&store.database_connection())
        .await
        .unwrap();
    let now = chrono::Utc::now().fixed_offset();
    let old = now - chrono::Duration::minutes(20);
    for status in ["starting", "running"] {
        for i in 0..65 {
            insert_turn(
                &store,
                &workspace,
                &format!("{status}_{i:03}"),
                status,
                now,
                now,
            )
            .await;
        }
        insert_turn(
            &store,
            &workspace,
            &format!("{status}_stale"),
            status,
            now,
            old,
        )
        .await;
    }
    // A lookup error must not count as processed, become a destructive stale
    // decision, or hold the cursor on the prefix forever.
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::value("{"))
        .filter(turn::Column::Id.eq("starting_000"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let processor = processor.scoped_for_background_reconciliation();
    let mut scan = CliRuntimeStaleTurnScan::default();
    let first = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(first.selected, 128);
    assert_eq!(first.processed, 127);
    for status in ["starting", "running"] {
        assert!(!has_recovery(&store, &format!("{status}_stale")).await);
        // Created behind the cursor after discovery; it waits for the next round.
        insert_turn(
            &store,
            &workspace,
            &format!("{status}_behind"),
            status,
            old,
            old,
        )
        .await;
    }
    // Change status both ahead of and behind the other status cursor, then
    // remove one row from the active set. A newly stale row moved behind the
    // starting cursor must be revisited in the next round.
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("running"))
        .filter(binding::Column::TurnId.eq("starting_001"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("starting"))
        .col_expr(binding::Column::UpdatedAt, Expr::value(old))
        .filter(binding::Column::TurnId.eq("running_001"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    turn_liveness::Entity::update_many()
        .col_expr(turn_liveness::Column::LastActivityAt, Expr::value(old))
        .filter(turn_liveness::Column::TurnId.eq("running_001"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("completed"))
        .filter(binding::Column::TurnId.eq("starting_002"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let second = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    // starting_001 sorts after the running prefix when it changes status.
    assert_eq!(second.selected, 5);
    assert_eq!(second.processed, 5);
    assert!(!has_recovery(&store, "running_001").await);
    for status in ["starting", "running"] {
        assert!(has_recovery(&store, &format!("{status}_stale")).await);
        assert!(!has_recovery(&store, &format!("{status}_behind")).await);
    }
    let third = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(
        third.selected, 128,
        "wrap gets a new budget only in a new quantum"
    );
    assert_eq!(third.processed, 127);
    assert!(has_recovery(&store, "running_001").await);
    for status in ["starting", "running"] {
        assert!(has_recovery(&store, &format!("{status}_behind")).await);
    }
    assert!(!has_recovery(&store, "starting_000").await);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding("starting_000")
            .await
            .unwrap()
            .unwrap()
            .status,
        "starting"
    );
    // Restart repeats the current active round and retains error isolation.
    let restarted = processor
        .fail_stale_cli_runtime_turns(
            now.timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!(restarted.selected, 128);
    assert_eq!(restarted.processed, 127);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_scan_cancellation_advances_selected_page_and_keeps_runtime_alive() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    binding::Entity::delete_many()
        .filter(binding::Column::TurnId.ne("codex-turn-command"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let now = chrono::Utc::now().fixed_offset() + chrono::Duration::minutes(20);
    let old = now - chrono::Duration::minutes(30);
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("starting"))
        .col_expr(binding::Column::CreatedAt, Expr::value(old))
        .filter(binding::Column::TurnId.eq("codex-turn-command"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    for i in 0..63 {
        insert_turn(
            &store,
            &workspace,
            &format!("healthy_{i:03}"),
            "starting",
            now,
            now,
        )
        .await;
    }
    insert_turn(&store, &workspace, "z_stale_tail", "starting", now, old).await;
    let processor = processor.scoped_for_background_reconciliation();
    let mut scan = CliRuntimeStaleTurnScan::default();
    // Hold the runtime probe rather than any DB resource. Dropping the scan
    // future must leave all selected active rows available on the next round.
    let mut probe = session.turn_liveness_probe.lock().await;
    *probe = Some(CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive);
    let mut quantum =
        Box::pin(processor.fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = session.turn_liveness_probe_started.notified() => {},
            result = &mut quantum => panic!("scan finished before the blocked runtime probe: {result:?}"),
        }
    }).await.unwrap();
    drop(quantum);
    drop(probe);
    assert!(!has_recovery(&store, "codex-turn-command").await);
    let next = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(
        next.selected, 1,
        "cancellation must not retry the whole selected page"
    );
    assert_eq!(next.processed, 1);
    assert!(has_recovery(&store, "z_stale_tail").await);
    let repeated = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(repeated.selected, 64);
    assert_eq!(repeated.processed, 64);
    assert!(
        !has_recovery(&store, "codex-turn-command").await,
        "confirmed native liveness must survive an old starting binding"
    );
    let (_, command) = store
        .get_turn("thread_cli_command_approval", "codex-turn-command")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, TurnStatus::InProgress);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_scan_preserves_maintenance_reads_and_critical_correctness_writes() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteClass, SqliteWriteEvent,
        SqliteWriteExecutor,
    };
    use sea_orm::ConnectOptions;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("cli-scan.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let (_, _, workspace) = setup_workspace_manager_with_connection(writer.clone()).await;
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|opts| opts.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = Arc::new(CrudStore::new(database.clone()));
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database.clone())),
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    )
    .scoped_for_background_reconciliation();
    let now = chrono::Utc::now().fixed_offset();
    insert_turn(
        &store,
        &workspace,
        "route_stale",
        "starting",
        now,
        now - chrono::Duration::minutes(20),
    )
    .await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let result = processor
        .fail_stale_cli_runtime_turns(
            now.timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!(result.selected, 1);
    assert_eq!(result.processed, 1);
    let reads = observer.reads.lock().unwrap();
    assert!(reads.iter().any(|event| matches!(
        event,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(reads.iter().all(|event| !matches!(
        event,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Interactive,
            ..
        }
    )));
    drop(reads);
    let writes = observer.writes.lock().unwrap();
    assert!(writes.iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Critical,
            ..
        }
    )));
    assert!(writes.iter().all(|event| !matches!(event, SqliteWriteEvent::Acquired { class, .. } if *class != SqliteWriteClass::Critical)));
    drop(writes);
    assert!(database.reader_query_only_enabled().await.unwrap());
    assert!(has_recovery(&store, "route_stale").await);
}

const GUARDED_TURN: &str = "codex-turn-command";
const GUARDED_THREAD: &str = "thread_cli_command_approval";

async fn block_guarded_fixture(store: &CrudStore) -> String {
    let now = chrono::Utc::now().timestamp();
    store
        .update_turn_status(
            GUARDED_THREAD,
            GUARDED_TURN,
            TurnStatus::Blocked,
            Some("blocked"),
            now,
        )
        .await
        .unwrap();
    let job = store
        .enqueue_recovery_job(
            GUARDED_TURN.into(),
            "guard-fixture".into(),
            pioneer_protocol::TurnItemType::SystemEvent,
            None,
            pioneer_protocol::RecoveryTrigger::Timeout,
            pioneer_protocol::RecoveryAction::BlockResumable,
            Some("blocked".into()),
            None,
            None,
            None,
            0,
            0,
            json!({}),
            json!({}),
            now,
        )
        .await
        .unwrap();
    store
        .mark_recovery_job_terminal(
            &job.id,
            pioneer_protocol::RecoveryJobStatus::Blocked,
            Some("blocked".into()),
            now,
        )
        .await
        .unwrap();
    job.id
}

async fn resume_guarded_fixture(
    store: &CrudStore,
    job: &str,
) -> pioneer_crud::CliRuntimeTurnAttemptRecord {
    let now = chrono::Utc::now();
    assert!(matches!(
        store
            .resume_blocked_turn_recovery(
                GUARDED_THREAD,
                GUARDED_TURN,
                Some(job),
                now.timestamp(),
                "guard-owner",
                now.timestamp() + 60,
            )
            .await
            .unwrap(),
        pioneer_crud::BlockedTurnRecoveryResumeOutcome::Resumed(_)
    ));
    let claimed = store
        .claim_due_recovery_jobs(now.timestamp(), 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.id == job)
        .expect("resumed job must be due");
    let recovery_attempt_id = pioneer_protocol::generate_id(21);
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                job,
                claimed.claim_token.as_deref().unwrap(),
                &recovery_attempt_id,
                now.timestamp(),
            )
            .await
            .unwrap(),
        pioneer_crud::ClaimedRecoveryActivation::Activated
    ));
    let (_, attempt) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            GUARDED_TURN,
            pioneer_protocol::generate_id(21),
            job.to_owned(),
            recovery_attempt_id,
            2,
            "explicit resume".into(),
            now.fixed_offset(),
        )
        .await
        .unwrap();
    let (_, attempt) = store
        .activate_cli_runtime_turn_attempt(
            GUARDED_TURN,
            &attempt.id,
            "native_guard_new",
            None,
            now.fixed_offset(),
        )
        .await
        .unwrap();
    store
        .register_cli_runtime_execution_segment(
            GUARDED_TURN,
            "codex-thread-command",
            "native_guard_new",
            now.fixed_offset(),
        )
        .await
        .unwrap();
    attempt
}

async fn pending_for_new_execution(store: &CrudStore, workspace: &str) {
    let now = chrono::Utc::now().fixed_offset();
    store
        .open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
            request_id: "guard-new-human".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            workspace_id: workspace.into(),
            thread_id: GUARDED_THREAD.into(),
            turn_id: Some(GUARDED_TURN.into()),
            native_thread_id: Some("codex-thread-command".into()),
            native_turn_id: Some("native_guard_new".into()),
            native_item_id: None,
            request_kind: "user_input".into(),
            payload_json: "{}".into(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_terminal_snapshot_resume_wins_without_new_execution_effects_and_scan_continues()
{
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let job = block_guarded_fixture(&store).await;
    let processor = processor.scoped_for_background_reconciliation();
    processor.arm_completed_history_preparation_barrier("__cli_terminal_before_commit__");
    let mut scan = CliRuntimeStaleTurnScan::default();
    let quantum =
        processor.fail_stale_cli_runtime_turns(chrono::Utc::now().timestamp_millis(), &mut scan);
    tokio::pin!(quantum);
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {}
        _ = &mut quantum => panic!("snapshot must pause before guarded commit"),
    }
    let transition = processor
        .cli_runtime_turn_resume_transition(GUARDED_TURN)
        .await
        .unwrap();
    let next = resume_guarded_fixture(&store, &job).await;
    pending_for_new_execution(&store, &workspace).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let _invocation = processor
        .mcp_service
        .hold_test_turn_mcp_invocation(GUARDED_TURN, cancel.clone());
    let lease = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    processor
        .cli_runtime_session_turn_leases
        .lock()
        .await
        .insert(GUARDED_TURN.into(), lease);
    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(GUARDED_TURN))
        .count(&store.database_connection())
        .await
        .unwrap();
    drop(transition);
    processor.release_completed_history_preparation_barrier();
    let summary = quantum.await;
    assert_eq!((summary.selected, summary.processed), (4, 4));
    assert_eq!(
        store
            .get_turn(GUARDED_THREAD, GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    let owner = store
        .resolve_cli_runtime_native_turn_owner("codex", "native_guard_new")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.attempt.id, next.id);
    assert_eq!(owner.binding.status, "running");
    assert!(!cancel.is_cancelled());
    assert_eq!(
        store
            .get_cli_runtime_pending_request("guard-new-human")
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_crud::CliRuntimePendingRequestStatus::Pending
    );
    assert!(
        processor
            .cli_runtime_session_turn_leases
            .lock()
            .await
            .contains_key(GUARDED_TURN)
    );
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.goal_clears.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(GUARDED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap(),
        events
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_terminal_commit_serializes_new_admission_until_old_effects_finish() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let job = block_guarded_fixture(&store).await;
    let processor = processor.scoped_for_background_reconciliation();
    processor.arm_completed_history_preparation_barrier("__cli_terminal_after_commit__");
    let mut scan = CliRuntimeStaleTurnScan::default();
    let quantum =
        processor.fail_stale_cli_runtime_turns(chrono::Utc::now().timestamp_millis(), &mut scan);
    tokio::pin!(quantum);
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {}
        _ = &mut quantum => panic!("commit must pause before external effects"),
    }
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        "blocked"
    );
    let key = CLIAgentRuntimeSessionKey::new(workspace.clone(), "codex", GUARDED_THREAD).unwrap();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    assert!(
        gate.try_lock().is_err(),
        "admission uses this same session gate"
    );
    let manager = processor.cli_runtime_manager.as_ref().unwrap();
    let old_instance = manager
        .existing_session(&key)
        .await
        .unwrap()
        .instance()
        .clone();
    let admission = async {
        let _transition = processor
            .cli_runtime_turn_resume_transition(GUARDED_TURN)
            .await
            .unwrap();
        let replacement = manager.get_or_start(key.clone()).await.unwrap();
        assert!(replacement.instance().generation() > old_instance.generation());
        let next = resume_guarded_fixture(&store, &job).await;
        pending_for_new_execution(&store, &workspace).await;
        next
    };
    tokio::pin!(admission);
    assert!(futures_util::poll!(&mut admission).is_pending());
    processor.release_completed_history_preparation_barrier();
    let (summary, next) = tokio::join!(quantum, admission);
    assert_eq!((summary.selected, summary.processed), (4, 4));
    assert_eq!(
        session.interrupts.lock().await.as_slice(),
        &[(
            Some("codex-thread-command".into()),
            Some(GUARDED_TURN.into())
        )]
    );
    assert!(manager.existing_session(&key).await.is_some());
    assert_eq!(session.goal_clears.lock().await.len(), 1);
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    assert_eq!(
        store
            .get_cli_runtime_pending_request("guard-new-human")
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_crud::CliRuntimePendingRequestStatus::Pending
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_active_terminal_cancellation_leaves_no_queued_terminal_effect_for_resumed_turn() {
    let (processor, _, rx, _, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let job = block_guarded_fixture(&store).await;
    let processor = processor.scoped_for_background_reconciliation();
    processor.arm_completed_history_preparation_barrier("__cli_terminal_after_commit__");
    let mut scan = CliRuntimeStaleTurnScan::default();
    {
        let quantum = processor
            .fail_stale_cli_runtime_turns(chrono::Utc::now().timestamp_millis(), &mut scan);
        tokio::pin!(quantum);
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {}
            _ = &mut quantum => panic!("commit must pause before external effects"),
        }
        // Dropping this owning future cancels the old cleanup and its session gate.
    }
    let transition = processor
        .cli_runtime_turn_resume_transition(GUARDED_TURN)
        .await
        .unwrap();
    let next = resume_guarded_fixture(&store, &job).await;
    drop(transition);
    let summary = processor
        .fail_stale_cli_runtime_turns(chrono::Utc::now().timestamp_millis(), &mut scan)
        .await;
    assert!(summary.selected <= 128);
    assert_eq!(summary.processed, summary.selected);
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.goal_clears.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cli_terminal_queued_event_keeps_transition_through_cancelled_waiter_and_preserves_order() {
    let hub = pioneer_runtime_events::ExecutionEventHub::new();
    let mut receiver = hub.take_durable_receiver().await.unwrap();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let transition = Arc::new(gate.clone().lock_owned().await);
    hub.publish_durable(AgentDurableEvent::TurnBlocked {
        thread_id: "ordered-thread".into(),
        turn_id: "earlier".into(),
        reason: "earlier durable event".into(),
        recovery: None,
    })
    .await
    .unwrap();
    {
        let publish = hub.publish_durable_and_wait_with_turn_transition(
            AgentDurableEvent::TurnBlocked {
                thread_id: "ordered-thread".into(),
                turn_id: "terminal".into(),
                reason: "terminal observation".into(),
                recovery: None,
            },
            Some(transition),
        );
        tokio::pin!(publish);
        assert!(futures_util::poll!(&mut publish).is_pending());
        assert!(
            matches!(receiver.recv().await, Some(AgentDurableEvent::TurnBlocked { turn_id, .. }) if turn_id == "earlier")
        );
        assert!(!receiver.owns_turn_transition());
        receiver.acknowledge_last(Ok(()));
        assert!(
            matches!(receiver.recv().await, Some(AgentDurableEvent::TurnBlocked { turn_id, .. }) if turn_id == "terminal")
        );
        assert!(receiver.owns_turn_transition());
        // Cancel only the waiter. The existing durable consumer still owns the
        // old execution's terminal handler and must keep admission fenced.
    }
    assert!(gate.try_lock().is_err());
    receiver.acknowledge_last(Ok(()));
    assert!(gate.try_lock().is_ok());
}

#[tokio::test]
async fn cli_active_scan_defers_busy_session_without_waiting_or_refetching_pages() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    binding::Entity::delete_many()
        .filter(binding::Column::TurnId.ne(GUARDED_TURN))
        .exec(&store.database_connection())
        .await
        .unwrap();
    block_guarded_fixture(&store).await;
    let now = chrono::Utc::now().fixed_offset();
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("starting"))
        .col_expr(
            binding::Column::CreatedAt,
            Expr::value(now - chrono::Duration::hours(1)),
        )
        .filter(binding::Column::TurnId.eq(GUARDED_TURN))
        .exec(&store.database_connection())
        .await
        .unwrap();
    insert_turn(&store, &workspace, "busy_running_gate", "running", now, now).await;
    // Both gate boundaries: terminal Starting and healthy InProgress Running
    // have busy session gates. Each range still has a full page and stale tail.
    for status in ["starting", "running"] {
        for index in 0..62 {
            insert_turn(
                &store,
                &workspace,
                &format!("busy_{status}_{index:03}"),
                status,
                now,
                now,
            )
            .await;
        }
        insert_turn(
            &store,
            &workspace,
            &format!("busy_{status}_stale"),
            status,
            now,
            now - chrono::Duration::minutes(30),
        )
        .await;
    }
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", GUARDED_THREAD).unwrap();
    let processor = processor.scoped_for_background_reconciliation();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    let held = gate.lock().await;
    let running_key =
        CLIAgentRuntimeSessionKey::new(&workspace, "codex", "busy_running_gate").unwrap();
    let running_gate = processor
        .cli_runtime_session_transition_mutex(&running_key)
        .await;
    let running_held = running_gate.lock().await;
    let mut scan = CliRuntimeStaleTurnScan::default();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        processor.fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan),
    )
    .await
    .expect("a busy gate must not hold the resilience worker");
    assert_eq!((result.selected, result.processed), (128, 128));
    assert!(has_recovery(&store, "busy_starting_stale").await);
    assert!(has_recovery(&store, "busy_running_stale").await);
    assert!(!has_recovery(&store, "busy_running_gate").await);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        "starting"
    );
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    drop(held);
    drop(running_held);
    // Full-page cursors reach the end once; wrap is a later quantum.
    let end = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(end.selected, 0);
    let next_round = processor
        .fail_stale_cli_runtime_turns(now.timestamp_millis(), &mut scan)
        .await;
    assert_eq!(next_round.selected, 128);
    assert_eq!(next_round.processed, 128);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        "blocked"
    );
    assert_eq!(session.interrupts.lock().await.len(), 1);
    assert!(gate.try_lock().is_ok());
    assert!(running_gate.try_lock().is_ok());
}

const BLOCKED_THREAD: &str = "blocked-observation-thread";
const BLOCKED_TURN: &str = "blocked-observation-turn";
const BLOCKED_NATIVE_THREAD: &str = "blocked-native-thread";

// Uses the project's persisted admission and recording provider. Reopening
// builds a new processor, store and session from the same isolated disk schema.
async fn blocked_observation_fixture(
    path: &std::path::Path,
    kind: &str,
    seed: bool,
) -> (
    MessageProcessor,
    Arc<CrudStore>,
    Arc<RecordingCliRuntimeSession>,
    String,
) {
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(path));
    options.max_connections(1).sqlx_logging(false);
    let connection = Database::connect(options).await.unwrap();
    let (workspace_manager, store, workspace) =
        setup_workspace_manager_with_connection(connection).await;
    if seed {
        seed_cli_runtime_turn_with_text(
            &store,
            &workspace,
            kind,
            kind,
            BLOCKED_THREAD,
            BLOCKED_TURN,
            BLOCKED_NATIVE_THREAD,
            "blocked observation",
        )
        .await;
    }
    let session = Arc::new(RecordingCliRuntimeSession::default());
    *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
        status: CLIAgentRuntimeObservedTurnStatus::Blocked,
        message: Some("provider blocked".into()),
        reconciliation_events: Vec::new(),
    });
    let manager = test_cli_runtime_manager(session.clone());
    manager
        .get_or_start(CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap())
        .await
        .unwrap();
    let processor = with_enabled_test_cli_runtime_catalog(
        MessageProcessor::new(
            Arc::new(ThreadManager::new("test", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            workspace_manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        )
        .with_cli_runtime_manager_for_tests(manager),
    )
    .scoped_for_background_reconciliation();
    (processor, store, session, workspace)
}

async fn blocked_event_count(store: &CrudStore) -> u64 {
    pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
        .filter(pioneer_entity::turn_event::Column::EventType.eq("turn_blocked"))
        .count(&store.database_connection())
        .await
        .unwrap()
}

async fn assert_blocked_observation_active(store: &CrudStore) {
    assert_eq!(
        store
            .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert!(
        store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_active()
    );
    assert_eq!(blocked_event_count(store).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_blocked_observation_rejects_old_segment_at_writer_and_fresh_snapshot_commits() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, workspace) =
        blocked_observation_fixture(&temp.path().join("segments.sqlite"), "codex", true).await;
    processor.arm_completed_history_preparation_barrier("__cli_blocked_before_enqueue__");
    let now = chrono::Utc::now().fixed_offset();
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let lease = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    processor
        .cli_runtime_session_turn_leases
        .lock()
        .await
        .insert(BLOCKED_TURN.into(), lease);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _invocation = processor
        .mcp_service
        .hold_test_turn_mcp_invocation(BLOCKED_TURN, cancellation.clone());
    let mut scan = CliRuntimeStaleTurnScan::default();
    let quantum = processor.fail_stale_cli_runtime_turns(
        (now + chrono::Duration::minutes(30)).timestamp_millis(),
        &mut scan,
    );
    tokio::pin!(quantum);
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut quantum => panic!("Blocked owner must be captured before enqueue"),
    }
    let owner_a = store
        .resolve_cli_runtime_native_turn_owner("codex", BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    let timestamp = owner_a.segment.as_ref().unwrap().updated_at;
    store
        .terminalize_cli_runtime_execution_segment(
            "codex",
            BLOCKED_TURN,
            pioneer_crud::CliRuntimeExecutionSegmentStatus::Completed,
            None,
            timestamp,
        )
        .await
        .unwrap()
        .unwrap();
    let (_, attempt_b, segment_b) = store
        .register_cli_runtime_execution_segment(
            BLOCKED_TURN,
            BLOCKED_NATIVE_THREAD,
            "blocked-native-B",
            timestamp,
        )
        .await
        .unwrap();
    store
        .open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
            request_id: "blocked-new-human".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            workspace_id: workspace.clone(),
            thread_id: BLOCKED_THREAD.into(),
            turn_id: Some(BLOCKED_TURN.into()),
            native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
            native_turn_id: Some("blocked-native-B".into()),
            native_item_id: None,
            request_kind: "user_input".into(),
            payload_json: "{}".into(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    processor.release_completed_history_preparation_barrier();
    let summary = quantum.await;
    assert_eq!(summary.selected, 1);
    assert_eq!(
        summary.processed, 0,
        "rejected writer decision is not a repair"
    );
    assert_blocked_observation_active(&store).await;
    let current = store
        .resolve_cli_runtime_native_turn_owner("codex", "blocked-native-B")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.attempt, attempt_b);
    assert_eq!(current.segment.unwrap(), segment_b);
    assert!(!cancellation.is_cancelled());
    assert_eq!(
        store
            .get_cli_runtime_pending_request("blocked-new-human")
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_crud::CliRuntimePendingRequestStatus::Pending
    );
    assert!(
        processor
            .cli_runtime_session_turn_leases
            .lock()
            .await
            .contains_key(BLOCKED_TURN)
    );
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.goal_clears.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    assert!(
        processor
            .cli_runtime_session_transition_mutex(&key)
            .await
            .try_lock()
            .is_ok()
    );
    store
        .resolve_cli_runtime_pending_request(pioneer_crud::ResolveCliRuntimePendingRequest {
            request_id: "blocked-new-human".into(),
            status: pioneer_crud::CliRuntimePendingRequestStatus::Resolved,
            response_json: None,
            updated_at: now,
            resolved_at: now,
        })
        .await
        .unwrap()
        .unwrap();
    let fresh = processor
        .fail_stale_cli_runtime_turns(
            (now + chrono::Duration::minutes(30)).timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!((fresh.selected, fresh.processed), (1, 1));
    assert_eq!(
        store
            .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Blocked
    );
    assert_eq!(blocked_event_count(&store).await, 1);
    assert_eq!(
        session.interrupts.lock().await.as_slice(),
        &[(
            Some(BLOCKED_NATIVE_THREAD.into()),
            Some("blocked-native-B".into())
        )]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_blocked_before_enqueue_cancellation_reopens_and_retries_codex_and_claude() {
    for kind in ["codex", "claude"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("before-enqueue.sqlite");
        let (processor, store, session, workspace) =
            blocked_observation_fixture(&path, kind, true).await;
        processor.arm_completed_history_preparation_barrier("__cli_blocked_before_enqueue__");
        let now_ms = (chrono::Utc::now() + chrono::Duration::minutes(30)).timestamp_millis();
        let mut scan = CliRuntimeStaleTurnScan::default();
        {
            let quantum = processor.fail_stale_cli_runtime_turns(now_ms, &mut scan);
            tokio::pin!(quantum);
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut quantum => panic!("must pause before terminal enqueue"),
            }
            assert_blocked_observation_active(&store).await;
        }
        let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
        assert!(
            processor
                .cli_runtime_session_transition_mutex(&key)
                .await
                .try_lock()
                .is_ok()
        );
        assert!(session.interrupts.lock().await.is_empty());
        // A fresh store/processor read the on-disk active binding, with no cursor,
        // mutex or queued obligation inherited from the cancelled publisher.
        let (reopened, reopened_store, _, _) =
            blocked_observation_fixture(&path, kind, false).await;
        let result = reopened
            .fail_stale_cli_runtime_turns(now_ms, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!((result.selected, result.processed), (1, 1));
        assert_eq!(
            reopened_store
                .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Blocked
        );
        assert_eq!(
            reopened_store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            "blocked"
        );
        assert_eq!(
            reopened_store
                .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimeTurnAttemptStatus::Interrupted
        );
        assert_eq!(blocked_event_count(&reopened_store).await, 1);
        let repeated = reopened
            .fail_stale_cli_runtime_turns(now_ms, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!(repeated.selected, 0);
        assert_eq!(blocked_event_count(&reopened_store).await, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_blocked_projection_failure_rolls_back_attempt_segments_event_and_retries_after_reopen()
{
    for kind in ["codex", "claude"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("projection.sqlite");
        let (processor, store, session, _) = blocked_observation_fixture(&path, kind, true).await;
        let before_attempt = store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let before_owner = store
            .resolve_cli_runtime_native_turn_owner(kind, BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let before_projection =
            pioneer_entity::turn_work_projection::Entity::find_by_id(BLOCKED_TURN)
                .one(&store.database_connection())
                .await
                .unwrap();
        assert!(before_projection.is_some());
        store.database_connection().execute_unprepared("CREATE TRIGGER reject_blocked_projection BEFORE UPDATE OF status ON turn WHEN NEW.status='blocked' BEGIN SELECT RAISE(ABORT, 'blocked projection failure'); END").await.unwrap();
        let now_ms = (chrono::Utc::now() + chrono::Duration::minutes(30)).timestamp_millis();
        let result = processor
            .fail_stale_cli_runtime_turns(now_ms, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!((result.selected, result.processed), (1, 0));
        assert_blocked_observation_active(&store).await;
        assert_eq!(
            store
                .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap(),
            before_attempt
        );
        assert_eq!(
            store
                .resolve_cli_runtime_native_turn_owner(kind, BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap(),
            before_owner
        );
        assert_eq!(
            pioneer_entity::turn_work_projection::Entity::find_by_id(BLOCKED_TURN)
                .one(&store.database_connection())
                .await
                .unwrap(),
            before_projection
        );
        assert!(session.interrupts.lock().await.is_empty());
        assert!(session.mcp_terminals.lock().await.is_empty());
        store
            .database_connection()
            .execute_unprepared("DROP TRIGGER reject_blocked_projection")
            .await
            .unwrap();
        let (reopened, reopened_store, _, _) =
            blocked_observation_fixture(&path, kind, false).await;
        let result = reopened
            .fail_stale_cli_runtime_turns(now_ms, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!((result.selected, result.processed), (1, 1));
        assert_eq!(blocked_event_count(&reopened_store).await, 1);
        assert_eq!(
            reopened_store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            "blocked"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_blocked_atomic_commit_keeps_queued_consumer_owned_after_publisher_cancellation() {
    for kind in ["codex", "claude"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("after-commit.sqlite");
        let (processor, store, session, workspace) =
            blocked_observation_fixture(&path, kind, true).await;
        processor.arm_completed_history_preparation_barrier("__cli_blocked_after_atomic_commit__");
        let now_ms = (chrono::Utc::now() + chrono::Duration::minutes(30)).timestamp_millis();
        let mut scan = CliRuntimeStaleTurnScan::default();
        {
            let quantum = processor.fail_stale_cli_runtime_turns(now_ms, &mut scan);
            tokio::pin!(quantum);
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut quantum => panic!("consumer must pause after atomic lifecycle commit"),
            }
            assert_eq!(
                store
                    .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Blocked
            );
            assert_eq!(
                store
                    .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_crud::CliRuntimeTurnAttemptStatus::Interrupted
            );
            assert_eq!(
                store
                    .get_cli_runtime_turn_binding(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "running"
            );
        }
        let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
        let gate = processor.cli_runtime_session_transition_mutex(&key).await;
        assert!(
            gate.try_lock().is_err(),
            "queued consumer still owns transition through ACK"
        );
        // Reconstruct only from the persisted state, as after a process restart.
        // No native running lookup or queued obligation is needed: the active
        // binding projects the already terminal canonical Turn without another event.
        let (reopened, reopened_store, reopened_session, _) =
            blocked_observation_fixture(&path, kind, false).await;
        let repaired = reopened
            .fail_stale_cli_runtime_turns(now_ms, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!((repaired.selected, repaired.processed), (1, 1));
        assert_eq!(
            reopened_store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            "blocked"
        );
        assert_eq!(blocked_event_count(&reopened_store).await, 1);
        assert_eq!(
            reopened_session.interrupts.lock().await.as_slice(),
            &[(
                Some(BLOCKED_NATIVE_THREAD.into()),
                Some(BLOCKED_TURN.into())
            )]
        );
        processor.release_completed_history_preparation_barrier();
        let _finished = gate.lock().await;
        assert_eq!(
            store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            "blocked"
        );
        assert_eq!(blocked_event_count(&store).await, 1);
        assert_eq!(session.interrupts.lock().await.len(), 1);
    }
}
