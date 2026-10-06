use super::*;
use crate::message::cli_runtime::CliRuntimeStaleTurnScan;
use crate::message::turn_handlers::CliRuntimeLaunchSpecRestore;
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
            None,
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
// Claude's durable launch contract requires a real provider session UUID.
const BLOCKED_NATIVE_THREAD: &str = "01900000-0000-7000-8000-000000000071";

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
    blocked_observation_fixture_with_receipt(path, kind, seed, false).await
}

async fn blocked_observation_fixture_with_receipt(
    path: &std::path::Path,
    kind: &str,
    seed: bool,
    receipt: bool,
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
    let session = Arc::new(RecordingCliRuntimeSession::default());
    *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
        status: CLIAgentRuntimeObservedTurnStatus::Blocked,
        message: Some("provider blocked".into()),
        reconciliation_events: Vec::new(),
    });
    let manager = test_cli_runtime_manager(session.clone());
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
        .with_cli_runtime_manager_for_tests(manager.clone()),
    )
    .scoped_for_background_reconciliation();
    if seed {
        if receipt {
            let now = chrono::Utc::now().fixed_offset();
            materialize_cli_runtime_turn_with_execution(
                &store,
                &workspace,
                BLOCKED_THREAD,
                BLOCKED_TURN,
                "accepted native outcome",
                Some(pioneer_crud::NewTurnExecution {
                    turn_id: BLOCKED_TURN.into(),
                    thread_id: BLOCKED_THREAD.into(),
                    workspace_id: workspace.clone(),
                    executor_kind: pioneer_crud::TurnExecutorKind::CliRuntime,
                    executor_key: Some(kind.into()),
                    status: pioneer_crud::TurnExecutionStatus::Starting,
                    owner_id: processor.turn_execution_owner_id.to_string(),
                    lease_until: now + chrono::Duration::seconds(60),
                    created_at: now,
                }),
            )
            .await;
            seed_cli_runtime_binding(
                &store,
                &workspace,
                kind,
                kind,
                BLOCKED_THREAD,
                BLOCKED_TURN,
                BLOCKED_NATIVE_THREAD,
            )
            .await;
        } else {
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
        persist_test_cli_instruction_projection(
            &store,
            BLOCKED_TURN,
            match kind {
                "codex" => CLIAgentRuntimeKind::Codex,
                "claude" => CLIAgentRuntimeKind::Claude,
                _ => panic!("unsupported fixture runtime"),
            },
        )
        .await;
    }
    let binding = store
        .get_cli_runtime_turn_binding(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    let (_, turn) = store
        .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    if turn.status == TurnStatus::InProgress {
        match processor.restore_cli_runtime_launch_spec(&binding).await {
            CliRuntimeLaunchSpecRestore::Ready(restored) => {
                manager
                    .get_or_start_with_launch_spec(restored.session_key, restored.launch_spec)
                    .await
                    .unwrap();
            }
            CliRuntimeLaunchSpecRestore::Unavailable { diagnostic }
            | CliRuntimeLaunchSpecRestore::InvalidBinding { diagnostic } => {
                panic!("the fixture must persist the real CLI launch contract: {diagnostic}");
            }
        }
    } else {
        // Cleanup rehydrates a terminal Turn; it does not authorize a new
        // provider launch. Supply the recorded session only as an effects target.
        manager
            .get_or_start(CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap())
            .await
            .unwrap();
    }
    (processor, store, session, workspace)
}

async fn blocked_event_count(store: &CrudStore) -> u64 {
    pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
        .filter(pioneer_entity::turn_event::Column::EventType.eq(events::TURN_BLOCKED))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_timeout_and_command_heartbeat_defer_busy_transition_without_timeout_evidence() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let processor = processor.scoped_for_background_reconciliation();
    let now = chrono::Utc::now().timestamp();
    materialize_expired_cli_command_attempt(&store, &workspace, now).await;
    *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
        status: CLIAgentRuntimeObservedTurnStatus::Blocked,
        message: Some("must not observe while transition is owned".into()),
        reconciliation_events: Vec::new(),
    });
    let binding = store
        .get_cli_runtime_turn_binding(GUARDED_TURN)
        .await
        .unwrap()
        .unwrap();
    let source = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap();
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", GUARDED_THREAD).unwrap();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    let owned = gate.lock_owned().await;
    let heartbeat_at = now + processor.cli_runtime_command_heartbeats.interval_secs() + 2;
    let (timeouts, heartbeat) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let timeouts = processor
            .poll_timeouts_respecting_human_wait(heartbeat_at, 64)
            .await
            .unwrap();
        let heartbeat = processor
            .heartbeat_due_cli_runtime_command_items(heartbeat_at)
            .await;
        (timeouts, heartbeat)
    })
    .await
    .expect("background entry points must finish while the gate remains owned");
    assert!(timeouts.is_empty());
    assert_eq!(heartbeat, 0);
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        source
    );
    assert!(!has_recovery(&store, GUARDED_TURN).await);
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
    assert!(session.goal_clears.lock().await.is_empty());
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    drop(owned);
    *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
        status: CLIAgentRuntimeObservedTurnStatus::InProgress,
        message: None,
        reconciliation_events: Vec::new(),
    });
    *session.turn_liveness_probe.lock().await =
        Some(CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive);
    assert_eq!(
        processor
            .heartbeat_due_cli_runtime_command_items(heartbeat_at)
            .await,
        1,
        "busy does not discard the due command or advance its heartbeat deadline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_common_blocked_reconciliation_retains_consumer_ownership_after_cancel_until_ack() {
    for kind in ["codex", "claude"] {
        let temp = tempfile::tempdir().unwrap();
        let (processor, store, session, workspace) =
            blocked_observation_fixture(&temp.path().join("common-blocked.sqlite"), kind, true)
                .await;
        let binding = store
            .get_cli_runtime_turn_binding(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let (_, turn) = store
            .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        processor.arm_completed_history_preparation_barrier("__cli_blocked_after_atomic_commit__");
        let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
        let gate = processor.cli_runtime_session_transition_mutex(&key).await;
        let mut admission = Box::pin(processor.cli_runtime_turn_resume_transition(BLOCKED_TURN));
        {
            let reconciliation =
                processor.reconcile_cli_runtime_turn_from_runtime(&binding, &workspace, &turn);
            tokio::pin!(reconciliation);
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut reconciliation => panic!("common entry must reach atomic consumer commit"),
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
            assert!(futures_util::poll!(&mut admission).is_pending());
            assert!(gate.try_lock().is_err());
            // Cancel the real common-entry publisher before ACK.
        }
        assert!(
            gate.try_lock().is_err(),
            "the queued consumer retains ownership independently of publisher cancellation"
        );
        assert!(futures_util::poll!(&mut admission).is_pending());
        processor.release_completed_history_preparation_barrier();
        let admitted = admission.await.unwrap().unwrap();
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
        assert_eq!(
            session.interrupts.lock().await.as_slice(),
            &[(
                Some(BLOCKED_NATIVE_THREAD.into()),
                Some(BLOCKED_TURN.into())
            )]
        );
        assert_eq!(session.mcp_terminals.lock().await.len(), 1);
        drop(admitted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_timeout_terminal_cleanup_rejects_resume_and_late_effects_after_competing_scan() {
    // Both the direct timeout race and timeout competing with a complete scan
    // use the actual supervisor entry, not the owned cleanup inner helper.
    for scan_first in [false, true] {
        let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
        drop(rx);
        let processor = processor.scoped_for_background_reconciliation();
        let now = chrono::Utc::now().timestamp();
        materialize_expired_cli_command_attempt(&store, &workspace, now).await;
        let job = block_guarded_fixture(&store).await;
        processor.arm_completed_history_preparation_barrier("__cli_cleanup_before_gate__");
        let timeout = processor.poll_timeouts_respecting_human_wait(now, 64);
        tokio::pin!(timeout);
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {},
            _ = &mut timeout => panic!("timeout must pause with old canonical terminal source"),
        }
        if scan_first {
            let summary = processor
                .fail_stale_cli_runtime_turns(now * 1_000, &mut CliRuntimeStaleTurnScan::default())
                .await;
            assert_eq!((summary.selected, summary.processed), (4, 4));
            assert_eq!(session.interrupts.lock().await.len(), 1);
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
        processor
            .cli_runtime_session_turn_leases
            .lock()
            .await
            .insert(
                GUARDED_TURN.into(),
                Arc::new(tokio::sync::Mutex::new(())).lock_owned().await,
            );
        let binding = store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap();
        let source = store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap();
        let effects = (
            session.interrupts.lock().await.len(),
            session.mcp_terminals.lock().await.len(),
            session.goal_clears.lock().await.len(),
            session.closes.load(Ordering::SeqCst),
        );
        let events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(GUARDED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap();
        if scan_first {
            let due = now + processor.cli_runtime_command_heartbeats.interval_secs() + 2;
            assert_eq!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    processor.heartbeat_due_cli_runtime_command_items(due)
                )
                .await
                .unwrap(),
                0
            );
        }
        drop(transition);
        processor.release_completed_history_preparation_barrier();
        assert!(timeout.await.unwrap().is_empty());
        assert_eq!(
            store
                .cli_runtime_turn_terminal_guard(&binding)
                .await
                .unwrap(),
            source
        );
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
                .get_cli_runtime_pending_request("guard-new-human")
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
        assert!(!cancel.is_cancelled());
        assert!(
            processor
                .cli_runtime_session_turn_leases
                .lock()
                .await
                .contains_key(GUARDED_TURN)
        );
        assert_eq!(
            (
                session.interrupts.lock().await.len(),
                session.mcp_terminals.lock().await.len(),
                session.goal_clears.lock().await.len(),
                session.closes.load(Ordering::SeqCst)
            ),
            effects
        );
        assert_eq!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(GUARDED_TURN))
                .count(&store.database_connection())
                .await
                .unwrap(),
            events
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_normal_terminal_consumer_rejects_busy_without_blocking_lane_and_fences_effects() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let binding = store
        .get_cli_runtime_turn_binding(GUARDED_TURN)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap();
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", GUARDED_THREAD).unwrap();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    let owned = gate.clone().lock_owned().await;
    let event = AgentDurableEvent::TurnBlocked {
        thread_id: GUARDED_THREAD.into(),
        turn_id: GUARDED_TURN.into(),
        reason: "normal terminal event".into(),
        recovery: None,
    };
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            processor.commit_durable_agent_event(event.clone())
        )
        .await
        .unwrap()
        .is_err(),
        "a busy terminal event must not hold up the admission window ACK behind it"
    );
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        before
    );
    assert!(session.interrupts.lock().await.is_empty());
    drop(owned);
    processor.arm_completed_history_preparation_barrier("__cli_terminal_after_commit__");
    let commit = processor.commit_durable_agent_event(event);
    tokio::pin!(commit);
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut commit => panic!("normal consumer must reach guarded terminal effects"),
    }
    assert!(gate.try_lock().is_err());
    let resume = processor.cli_runtime_turn_resume_transition(GUARDED_TURN);
    tokio::pin!(resume);
    assert!(futures_util::poll!(&mut resume).is_pending());
    processor.release_completed_history_preparation_barrier();
    let (committed, resumed) = tokio::join!(commit, resume);
    committed.unwrap();
    let resumed = resumed.unwrap().unwrap();
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        "blocked"
    );
    assert_eq!(
        session.interrupts.lock().await.as_slice(),
        &[(
            Some("codex-thread-command".into()),
            Some(GUARDED_TURN.into())
        )]
    );
    drop(resumed);
}

fn ordinary_native_failure(native_thread: &str, native_turn: &str) -> RuntimeEvent {
    RuntimeEvent::TurnFailed(RuntimeTurnFailed {
        native_thread_id: Some(native_thread.into()),
        native_turn_id: Some(native_turn.into()),
        message: "ordinary native outcome saved before admission ACK".into(),
        code: None,
        native: None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_native_terminal_waits_outside_lane_and_active_scan_replays_cancelled_producer() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, workspace) =
        blocked_observation_fixture(&temp.path().join("native-delivery.sqlite"), "codex", true)
            .await;
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    let owned = gate.clone().lock_owned().await;
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("producer must save its execution outcome before waiting"),
    }
    processor.release_completed_history_preparation_barrier();
    assert!(futures_util::poll!(&mut producer).is_pending());
    assert_blocked_observation_active(&store).await;
    assert!(!has_recovery(&store, BLOCKED_TURN).await);
    // Admission's actual window event must pass the same ordered durable lane,
    // while the terminal producer still waits for the admission-owned gate.
    processor
        .publish_cli_runtime_durable_and_wait(
            handle.instance(),
            AgentDurableEvent::TurnExecutionWindowStarted {
                notification: pioneer_protocol::TurnExecutionWindowStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: BLOCKED_THREAD.into(),
                    turn_id: BLOCKED_TURN.into(),
                    window_id: "terminal_delivery_window".into(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    started_at_unix_ms: chrono::Utc::now().timestamp_millis(),
                },
            },
        )
        .await
        .unwrap();
    assert!(gate.try_lock().is_err());
    drop(producer); // The provider may exit or its event pump may be cancelled.
    drop(owned);
    *session.turn_observation.lock().await = None;
    // The real worker entry processes a fresh active row immediately. No
    // stale age, old process snapshot or manual event resubmission is needed.
    let result = processor
        .fail_stale_cli_runtime_turns(
            chrono::Utc::now().timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!((result.selected, result.processed), (1, 1));
    let job = store
        .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.trigger,
        pioneer_protocol::RecoveryTrigger::RuntimeFailure
    );
    assert!(
        job.reason
            .as_deref()
            .unwrap()
            .contains("ordinary native outcome")
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_crud::CliRuntimeTurnAttemptStatus::Failed
    );
    assert!(gate.try_lock().is_ok());
    assert!(session.interrupts.lock().await.is_empty());
    let before = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
        .count(&store.database_connection())
        .await
        .unwrap();
    processor
        .fail_stale_cli_runtime_turns(
            chrono::Utc::now().timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_saved_native_outcome_reopens_for_codex_and_claude_without_provider_snapshot() {
    for (kind, completed) in [
        ("codex", false),
        ("claude", false),
        ("codex", true),
        ("claude", true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join(format!("{kind}-{completed}-native-delivery.sqlite"));
        let (processor, store, session, workspace) =
            blocked_observation_fixture_with_receipt(&path, kind, true, true).await;
        let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
        let handle = processor
            .cli_runtime_manager
            .as_ref()
            .unwrap()
            .existing_session(&key)
            .await
            .unwrap();
        let owned = processor
            .cli_runtime_session_transition_mutex(&key)
            .await
            .lock_owned()
            .await;
        processor.arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
        let record_uuid = uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000072").unwrap();
        let event = if completed {
            RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
                native_turn_id: BLOCKED_TURN.into(),
                status: "completed".into(),
                native: Some(pioneer_cli_agent_runtime::event::RuntimeNativeEvent {
                    method: "turn/completed".into(),
                    payload_redacted: Some(json!({"assistantRecordUuid": record_uuid.to_string()})),
                    raw_redacted: None,
                }),
            })
        } else {
            ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN)
        };
        let mut producer =
            Box::pin(processor.handle_cli_runtime_timeline_event(handle.instance(), event));
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {},
            _ = &mut producer => panic!("native outcome must be durable before cancellation"),
        }
        drop(producer);
        drop(owned);
        assert!(!has_recovery(&store, BLOCKED_TURN).await);
        assert!(session.interrupts.lock().await.is_empty());
        let original_owner = store
            .get_turn_execution(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let original_attempt = store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        drop(handle);
        drop(store);
        drop(processor);
        // A different manager/session has no Claude in-memory turn outcome.
        let (reopened, store, session, _) =
            blocked_observation_fixture_with_receipt(&path, kind, false, true).await;
        *session.turn_observation.lock().await = None;
        assert!(
            reopened
                .thread_manager
                .turn_get(BLOCKED_THREAD, BLOCKED_TURN)
                .await
                .is_none(),
            "restart replay must restore the canonical lifecycle from storage"
        );
        let now = chrono::Utc::now().timestamp();
        // Production order: the coordinator runs before Gateway events and
        // active discovery. A foreign unexpired receipt cannot be bypassed.
        let events = reopened
            .recovery_coordinator
            .run_ready_jobs(now, 16)
            .await
            .unwrap();
        for event in events {
            reopened.handle_recovery_event(event, now).await;
        }
        let deferred = reopened
            .fail_stale_cli_runtime_turns(now * 1000, &mut CliRuntimeStaleTurnScan::default())
            .await;
        assert_eq!((deferred.selected, deferred.processed), (1, 0));
        assert_blocked_observation_active(&store).await;
        assert_eq!(
            store
                .get_turn_execution(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap(),
            original_owner
        );
        assert!(session.turn_starts.lock().await.is_empty());
        use pioneer_entity::turn_execution as execution;
        let expired_at = chrono::DateTime::from_timestamp(now - 1, 0)
            .unwrap()
            .fixed_offset();
        execution::Entity::update_many()
            .col_expr(execution::Column::HeartbeatAt, Expr::value(expired_at))
            .col_expr(execution::Column::LeaseUntil, Expr::value(expired_at))
            .filter(execution::Column::TurnId.eq(BLOCKED_TURN))
            .exec(&store.database_connection())
            .await
            .unwrap();
        // Exercise the real lease CAS, rather than assigning the new owner.
        let events = reopened
            .recovery_coordinator
            .run_ready_jobs(now, 16)
            .await
            .unwrap();
        let claimed = store
            .get_turn_execution(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.owner_id, reopened.turn_execution_owner_id.as_ref());
        assert_eq!(
            claimed.owner_generation,
            original_owner.owner_generation + 1
        );
        assert_eq!(
            claimed.status,
            pioneer_crud::TurnExecutionStatus::Recovering
        );
        assert!(
            !has_recovery(&store, BLOCKED_TURN).await,
            "accepted outcome precedes orphan recovery synthesis"
        );
        for event in events {
            reopened.handle_recovery_event(event, now).await;
        }
        assert_eq!(
            store
                .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .id,
            original_attempt.id
        );
        assert!(
            session.turn_starts.lock().await.is_empty(),
            "recovery must not replace the accepted outcome"
        );
        let repaired = reopened
            .fail_stale_cli_runtime_turns(
                chrono::Utc::now().timestamp_millis(),
                &mut CliRuntimeStaleTurnScan::default(),
            )
            .await;
        assert_eq!((repaired.selected, repaired.processed), (1, 1));
        if completed {
            assert_eq!(
                store
                    .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Completed
            );
            assert_eq!(
                store
                    .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_crud::CliRuntimeTurnAttemptStatus::Completed
            );
            let binding = store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(binding.status, "completed");
            assert!(!has_recovery(&store, BLOCKED_TURN).await);
            if kind == "claude" {
                assert_eq!(
                    crate::cli_runtime::thread_binding::claude_assistant_record_boundary(
                        store.as_ref(),
                        &binding
                    )
                    .await
                    .unwrap(),
                    Some(record_uuid)
                );
            }
            let before = pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                .count(&store.database_connection())
                .await
                .unwrap();
            reopened
                .fail_stale_cli_runtime_turns(
                    chrono::Utc::now().timestamp_millis(),
                    &mut CliRuntimeStaleTurnScan::default(),
                )
                .await;
            assert_eq!(
                pioneer_entity::turn_event::Entity::find()
                    .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                    .count(&store.database_connection())
                    .await
                    .unwrap(),
                before
            );
            continue;
        }
        let job = store
            .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            job.trigger,
            pioneer_protocol::RecoveryTrigger::RuntimeFailure
        );
        assert!(
            job.reason
                .as_deref()
                .unwrap()
                .contains("ordinary native outcome")
        );
        assert!(session.interrupts.lock().await.is_empty());
        let before = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap();
        reopened
            .fail_stale_cli_runtime_turns(
                chrono::Utc::now().timestamp_millis(),
                &mut CliRuntimeStaleTurnScan::default(),
            )
            .await;
        assert_eq!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                .count(&store.database_connection())
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_old_native_failed_producer_cannot_open_recovery_or_cleanup_resumed_attempt() {
    let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
    drop(rx);
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", GUARDED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_before_gate__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure("codex-thread-command", GUARDED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("old native owner must be selected before admission"),
    }
    let job = block_guarded_fixture(&store).await;
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
    let before = store
        .cli_runtime_turn_terminal_guard(
            &store
                .get_cli_runtime_turn_binding(GUARDED_TURN)
                .await
                .unwrap()
                .unwrap(),
        )
        .await
        .unwrap();
    drop(transition);
    processor.release_completed_history_preparation_barrier();
    producer.await;
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(
                &store
                    .get_cli_runtime_turn_binding(GUARDED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap()
            .id,
        next.id
    );
    assert!(
        store
            .find_recovery_jobs_by_turn_and_status(
                GUARDED_TURN,
                pioneer_protocol::RecoveryJobStatus::Pending
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!cancel.is_cancelled());
    assert!(
        processor
            .cli_runtime_session_turn_leases
            .lock()
            .await
            .contains_key(GUARDED_TURN)
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
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_old_native_failed_producer_rejects_same_attempt_new_segment_with_equal_timestamp() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, workspace) = blocked_observation_fixture_with_receipt(
        &temp.path().join("native-source-segment.sqlite"),
        "codex",
        true,
        true,
    )
    .await;
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    let owner = store
        .resolve_cli_runtime_native_turn_owner("codex", BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_before_gate__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("must select segment A"),
    }
    let timestamp = owner.segment.unwrap().updated_at;
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
    let (_, attempt, segment) = store
        .register_cli_runtime_execution_segment(
            BLOCKED_TURN,
            BLOCKED_NATIVE_THREAD,
            "native-segment-B",
            timestamp,
        )
        .await
        .unwrap();
    assert_eq!(attempt.id, owner.attempt.id);
    processor.release_completed_history_preparation_barrier();
    producer.await;
    assert_blocked_observation_active(&store).await;
    assert_eq!(
        store
            .get_cli_runtime_execution_segment_by_native_turn("codex", "native-segment-B")
            .await
            .unwrap()
            .unwrap()
            .id,
        segment.id
    );
    assert!(!has_recovery(&store, BLOCKED_TURN).await);
    assert!(session.interrupts.lock().await.is_empty());
}

async fn activate_observation_request(
    processor: &MessageProcessor,
    store: &CrudStore,
) -> (
    crate::resilience::RecoveryCoordinatorEvent,
    pioneer_crud::RecoveryJobRecord,
    i64,
) {
    assert!(
        processor
            .report_turn_failure(
                BLOCKED_THREAD.into(),
                BLOCKED_TURN.into(),
                super::super::agent_runtime::TurnFailureRecoveryKind::ObservationGap,
                "missing observation".into()
            )
            .await
    );
    let job = store
        .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    let now = job.next_run_at_unix;
    let request = processor.recovery_coordinator.run_ready_jobs(now, 16).await.unwrap().into_iter()
        .find(|event| matches!(event, crate::resilience::RecoveryCoordinatorEvent::CliRuntimeTerminalReconciliationRequested(_)))
        .expect("the coordinator must activate and dispatch the real reconciliation request");
    let job = store.get_recovery_job(&job.id).await.unwrap().unwrap();
    assert_eq!(job.status, pioneer_protocol::RecoveryJobStatus::Active);
    (request, job, now)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_reconciliation_stale_and_storage_sources_defer_durably_without_recovery_budget() {
    for phase in ["before_handler", "after_source", "lookup_failure"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("recovery-{phase}.sqlite"));
        let (processor, store, _, _) = blocked_observation_fixture(&path, "codex", true).await;
        let (request, active, now) = activate_observation_request(&processor, &store).await;
        if phase == "before_handler" {
            binding::Entity::update_many()
                .col_expr(binding::Column::Model, Expr::value("changed model"))
                .filter(binding::Column::TurnId.eq(BLOCKED_TURN))
                .exec(&store.database_connection())
                .await
                .unwrap();
            assert!(processor.handle_recovery_event(request, now).await);
        } else {
            processor.arm_completed_history_preparation_barrier(if phase == "after_source" {
                "__cli_recovery_after_source__"
            } else {
                "__cli_recovery_before_source__"
            });
            let mut handler = Box::pin(processor.handle_recovery_event(request, now));
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut handler => panic!("request must reach the selected reader boundary"),
            }
            if phase == "after_source" {
                binding::Entity::update_many()
                    .col_expr(binding::Column::Model, Expr::value("changed model"))
                    .filter(binding::Column::TurnId.eq(BLOCKED_TURN))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
            } else {
                // The real reader reports an invalid persisted source; this
                // must remain a storage error, not an absent/superseded request.
                turn::Entity::update_many()
                    .col_expr(
                        turn::Column::Status,
                        Expr::value("invalid persisted status"),
                    )
                    .filter(turn::Column::Id.eq(BLOCKED_TURN))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
            }
            processor.release_completed_history_preparation_barrier();
            if phase == "lookup_failure" {
                processor
                    .arm_completed_history_preparation_barrier("__cli_recovery_before_defer__");
                tokio::select! {
                    _ = processor.wait_for_completed_history_preparation_barrier() => {},
                    _ = &mut handler => panic!("storage failure must reach fenced deferral"),
                }
                // The fault is transient. Restore readable source facts before
                // the writer proves that deferral cannot revoke a saved outcome.
                turn::Entity::update_many()
                    .col_expr(turn::Column::Status, Expr::value("in_progress"))
                    .filter(turn::Column::Id.eq(BLOCKED_TURN))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
                processor.release_completed_history_preparation_barrier();
            }
            assert!(handler.await);
        }
        let deferred = store.get_recovery_job(&active.id).await.unwrap().unwrap();
        assert_eq!(
            deferred.status,
            pioneer_protocol::RecoveryJobStatus::Pending
        );
        assert!(deferred.active_attempt_id.is_none());
        assert_eq!(deferred.next_run_at_unix, now + 5);
        assert_eq!(deferred.run_count, active.run_count);
        assert_eq!(
            deferred.provider_attempt_number,
            active.provider_attempt_number
        );
        assert_eq!(deferred.policy_json, active.policy_json);
        assert_eq!(deferred.policy_snapshot, active.policy_snapshot);
        if phase == "lookup_failure" {
            assert!(
                deferred
                    .last_error
                    .as_deref()
                    .unwrap()
                    .contains("storage failure")
            );
        }
        drop(store);
        drop(processor);
        let (reopened, store, session, _) =
            blocked_observation_fixture(&path, "codex", false).await;
        *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
            status: CLIAgentRuntimeObservedTurnStatus::InProgress,
            message: None,
            reconciliation_events: Vec::new(),
        });
        let events = reopened
            .recovery_coordinator
            .run_ready_jobs(deferred.next_run_at_unix, 16)
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            crate::resilience::RecoveryCoordinatorEvent::CliRuntimeTerminalReconciliationRequested(
                _
            )
        )));
        for event in events {
            assert!(
                reopened
                    .handle_recovery_event(event, deferred.next_run_at_unix)
                    .await
            );
        }
        let finished = store.get_recovery_job(&active.id).await.unwrap().unwrap();
        assert_eq!(
            finished.status,
            pioneer_protocol::RecoveryJobStatus::Succeeded,
            "the scheduled retry must progress without watchdog activation"
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
        assert!(session.interrupts.lock().await.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_reconciliation_late_deferral_cannot_change_replacement_recovery_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, _) =
        blocked_observation_fixture(&temp.path().join("recovery-replaced.sqlite"), "codex", true)
            .await;
    let (request, active, now) = activate_observation_request(&processor, &store).await;
    binding::Entity::update_many()
        .col_expr(binding::Column::Model, Expr::value("metadata refresh"))
        .filter(binding::Column::TurnId.eq(BLOCKED_TURN))
        .exec(&store.database_connection())
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_recovery_before_defer__");
    let mut handler = Box::pin(processor.handle_recovery_event(request, now));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut handler => panic!("the old request must pause before fenced deferral"),
    }
    assert!(
        store
            .defer_active_recovery_job(
                &active.id,
                active.active_attempt_id.as_deref().unwrap(),
                now,
                Some("replacement owns the work".into()),
                now
            )
            .await
            .unwrap()
    );
    let claimed = store
        .claim_due_recovery_jobs(now, 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.id == active.id)
        .unwrap();
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &active.id,
                claimed.claim_token.as_deref().unwrap(),
                "replacement-recovery-attempt",
                now
            )
            .await
            .unwrap(),
        pioneer_crud::ClaimedRecoveryActivation::Activated
    ));
    let replacement = store.get_recovery_job(&active.id).await.unwrap().unwrap();
    processor.release_completed_history_preparation_barrier();
    assert!(handler.await);
    let after = store.get_recovery_job(&active.id).await.unwrap().unwrap();
    assert_eq!(after.status, replacement.status);
    assert_eq!(after.active_attempt_id, replacement.active_attempt_id);
    assert_eq!(after.next_run_at_unix, replacement.next_run_at_unix);
    assert_eq!(after.updated_at_unix, replacement.updated_at_unix);
    assert_eq!(after.last_error, replacement.last_error);
    assert_eq!(after.run_count, replacement.run_count);
    assert!(session.interrupts.lock().await.is_empty());
    assert_blocked_observation_active(&store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_native_terminal_consumer_acks_delivery_and_retains_ownership_after_publisher_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, _, workspace) = blocked_observation_fixture(
        &temp.path().join("native-lane-cancel.sqlite"),
        "codex",
        true,
    )
    .await;
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    let gate = processor.cli_runtime_session_transition_mutex(&key).await;
    processor
        .arm_completed_history_preparation_barrier("__cli_native_terminal_consumer_committed__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("real native event must reach its durable consumer"),
    }
    let job = store
        .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.trigger,
        pioneer_protocol::RecoveryTrigger::RuntimeFailure
    );
    drop(producer);
    assert!(
        gate.try_lock().is_err(),
        "consumer retains ownership independently of its publisher"
    );
    let mut admission = Box::pin(processor.cli_runtime_turn_resume_transition(BLOCKED_TURN));
    assert!(futures_util::poll!(&mut admission).is_pending());
    let before = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
        .count(&store.database_connection())
        .await
        .unwrap();
    processor.release_completed_history_preparation_barrier();
    let admission = admission.await.unwrap().unwrap();
    drop(admission);
    processor
        .fail_stale_cli_runtime_turns(
            chrono::Utc::now().timestamp_millis(),
            &mut CliRuntimeStaleTurnScan::default(),
        )
        .await;
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap(),
        before,
        "consumer ACK prevents a replay from adding recovery notifications"
    );
    assert_eq!(
        store
            .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .id,
        job.id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_old_native_failed_producer_rejects_owner_generation_aba_at_equal_timestamp() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, workspace) = blocked_observation_fixture_with_receipt(
        &temp.path().join("native-owner-aba.sqlite"),
        "codex",
        true,
        true,
    )
    .await;
    let db = store.database_connection();
    use pioneer_entity::turn_execution as execution;
    let owner = store
        .get_turn_execution(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_before_gate__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("producer must capture the old owner epoch"),
    }
    execution::Entity::update_many()
        .col_expr(execution::Column::Status, Expr::value("blocked"))
        .col_expr(
            execution::Column::CompletedAt,
            Expr::value(Some(owner.updated_at)),
        )
        .filter(execution::Column::TurnId.eq(BLOCKED_TURN))
        .exec(&db)
        .await
        .unwrap();
    execution::Entity::update_many()
        .col_expr(
            execution::Column::Status,
            Expr::value(owner.status.as_str()),
        )
        .col_expr(
            execution::Column::CompletedAt,
            Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
        )
        .col_expr(
            execution::Column::OwnerGeneration,
            Expr::value(owner.owner_generation as i64 + 1),
        )
        .filter(execution::Column::TurnId.eq(BLOCKED_TURN))
        .exec(&db)
        .await
        .unwrap();
    let after = store
        .get_turn_execution(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.owner_id, owner.owner_id);
    assert_eq!(after.status, owner.status);
    assert_eq!(after.updated_at, owner.updated_at);
    assert_eq!(after.owner_generation, owner.owner_generation + 1);
    processor.release_completed_history_preparation_barrier();
    producer.await;
    assert_blocked_observation_active(&store).await;
    assert!(!has_recovery(&store, BLOCKED_TURN).await);
    assert!(session.interrupts.lock().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_native_goal_segment_ack_does_not_suppress_canonical_goal_completion() {
    for (cleared, cancel) in [(false, false), (true, false), (false, true), (true, true)] {
        let temp = tempfile::tempdir().unwrap();
        let (processor, store, _, workspace) =
            blocked_observation_fixture(&temp.path().join("native-goal-ack.sqlite"), "codex", true)
                .await;
        let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
        let handle = processor
            .cli_runtime_manager
            .as_ref()
            .unwrap()
            .existing_session(&key)
            .await
            .unwrap();
        // Prime the ordinary native binding cache before Goal metadata changes.
        processor
            .handle_cli_runtime_timeline_event(
                handle.instance(),
                RuntimeEvent::TurnStarted(RuntimeTurnStarted {
                    native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
                    native_turn_id: BLOCKED_TURN.into(),
                    native: None,
                }),
            )
            .await;
        processor
            .handle_cli_runtime_timeline_event(
                handle.instance(),
                RuntimeEvent::ThreadGoalUpdated(RuntimeThreadGoalUpdated {
                    native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                    native_turn_id: Some(BLOCKED_TURN.into()),
                    status: RuntimeThreadGoalStatus::Active,
                    native: None,
                }),
            )
            .await;
        processor
            .handle_cli_runtime_timeline_event(
                handle.instance(),
                RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                    native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
                    native_turn_id: BLOCKED_TURN.into(),
                    status: "completed".into(),
                    native: None,
                }),
            )
            .await;
        assert_eq!(
            store
                .get_cli_runtime_execution_segment_by_native_turn("codex", BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimeExecutionSegmentStatus::Completed,
            "Goal metadata changes must not discard completion from the cached native binding"
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
        for status in [
            RuntimeThreadGoalStatus::Active,
            RuntimeThreadGoalStatus::Paused,
            RuntimeThreadGoalStatus::Blocked,
            RuntimeThreadGoalStatus::UsageLimited,
            RuntimeThreadGoalStatus::BudgetLimited,
        ] {
            processor
                .handle_cli_runtime_timeline_event(
                    handle.instance(),
                    RuntimeEvent::ThreadGoalUpdated(RuntimeThreadGoalUpdated {
                        native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                        native_turn_id: Some(BLOCKED_TURN.into()),
                        status,
                        native: None,
                    }),
                )
                .await;
            processor
                .fail_stale_cli_runtime_turns(
                    chrono::Utc::now().timestamp_millis(),
                    &mut CliRuntimeStaleTurnScan::default(),
                )
                .await;
            assert_blocked_observation_active(&store).await;
        }
        assert!(
            !store
                .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
                .await
                .unwrap(),
            "a segment ACK with an open Goal is not pending canonical delivery"
        );
        let binding = store
            .get_cli_runtime_turn_binding(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let turn = store
            .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert!(!matches!(
            processor
                .reconcile_cli_runtime_turn_from_runtime(&binding, &workspace, &turn)
                .await
                .unwrap(),
            crate::message::cli_runtime::CLIRuntimeAuthoritativeTurnState::Terminal
        ));
        let closing = if cleared {
            RuntimeEvent::ThreadGoalCleared(
                pioneer_cli_agent_runtime::event::RuntimeThreadGoalCleared {
                    native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                    native: None,
                },
            )
        } else {
            RuntimeEvent::ThreadGoalUpdated(RuntimeThreadGoalUpdated {
                native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                native_turn_id: Some(BLOCKED_TURN.into()),
                status: RuntimeThreadGoalStatus::Complete,
                native: None,
            })
        };
        let (processor, store, handle) = if cancel {
            processor
                .arm_completed_history_preparation_barrier("__cli_native_terminal_before_gate__");
            let mut delivery = Box::pin(
                processor.handle_cli_runtime_timeline_event(handle.instance(), closing.clone()),
            );
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut delivery => panic!("Goal closure must persist before canonical delivery"),
            }
            drop(delivery);
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
            drop(handle);
            drop(store);
            drop(processor);
            let (reopened, store, _, workspace) = blocked_observation_fixture(
                &temp.path().join("native-goal-ack.sqlite"),
                "codex",
                false,
            )
            .await;
            let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
            let handle = reopened
                .cli_runtime_manager
                .as_ref()
                .unwrap()
                .existing_session(&key)
                .await
                .unwrap();
            reopened
                .fail_stale_cli_runtime_turns(
                    chrono::Utc::now().timestamp_millis(),
                    &mut CliRuntimeStaleTurnScan::default(),
                )
                .await;
            (reopened, store, handle)
        } else {
            processor
                .handle_cli_runtime_timeline_event(handle.instance(), closing.clone())
                .await;
            (processor, store, handle)
        };
        assert_eq!(
            store
                .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Completed
        );
        assert_eq!(
            store
                .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimeTurnAttemptStatus::Completed
        );
        assert_eq!(
            store
                .get_cli_runtime_turn_binding(BLOCKED_TURN)
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
        let before = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap();
        processor
            .handle_cli_runtime_timeline_event(handle.instance(), closing)
            .await;
        assert_eq!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                .count(&store.database_connection())
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_goal_waiting_for_admission_refreshes_same_execution_and_rejects_replacement() {
    for phase in [
        "activation",
        "metadata_complete",
        "metadata_cleared",
        "replacement_attempt",
        "replacement_segment",
        "owner_aba",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (processor, store, session, workspace) = blocked_observation_fixture_with_receipt(
            &temp.path().join("goal-admission.sqlite"),
            "codex",
            true,
            true,
        )
        .await;
        let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
        let handle = processor
            .cli_runtime_manager
            .as_ref()
            .unwrap()
            .existing_session(&key)
            .await
            .unwrap();
        let attempt = store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        let db = store.database_connection();
        let starting = !phase.starts_with("metadata");
        if starting {
            use pioneer_entity::{
                turn_cli_runtime_attempt as attempt_row,
                turn_cli_runtime_execution_segment as segment_row,
            };
            segment_row::Entity::delete_many()
                .filter(segment_row::Column::AttemptId.eq(&attempt.id))
                .exec(&db)
                .await
                .unwrap();
            attempt_row::Entity::update_many()
                .col_expr(attempt_row::Column::Status, Expr::value("starting"))
                .col_expr(
                    attempt_row::Column::NativeTurnId,
                    Expr::value(Option::<String>::None),
                )
                .filter(attempt_row::Column::Id.eq(&attempt.id))
                .exec(&db)
                .await
                .unwrap();
            binding::Entity::update_many()
                .col_expr(binding::Column::Status, Expr::value("starting"))
                .col_expr(
                    binding::Column::NativeTurnId,
                    Expr::value(Option::<String>::None),
                )
                .filter(binding::Column::TurnId.eq(BLOCKED_TURN))
                .exec(&db)
                .await
                .unwrap();
        }
        let gate = processor.cli_runtime_session_transition_mutex(&key).await;
        let admission = gate.clone().lock_owned().await;
        let event = if phase == "metadata_cleared" {
            RuntimeEvent::ThreadGoalCleared(
                pioneer_cli_agent_runtime::event::RuntimeThreadGoalCleared {
                    native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                    native: None,
                },
            )
        } else {
            RuntimeEvent::ThreadGoalUpdated(RuntimeThreadGoalUpdated {
                native_thread_id: BLOCKED_NATIVE_THREAD.into(),
                native_turn_id: Some(BLOCKED_TURN.into()),
                status: if phase == "metadata_complete" {
                    RuntimeThreadGoalStatus::Complete
                } else {
                    RuntimeThreadGoalStatus::Active
                },
                native: None,
            })
        };
        processor.arm_completed_history_preparation_barrier("__cli_goal_before_gate__");
        let mut goal =
            Box::pin(processor.handle_cli_runtime_timeline_event(handle.instance(), event));
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {},
            _ = &mut goal => panic!("provider event must capture admission's selected attempt"),
        }
        if starting {
            let now = chrono::Utc::now().fixed_offset();
            store
                .activate_cli_runtime_turn_attempt_owned(
                    BLOCKED_TURN,
                    &attempt.id,
                    BLOCKED_TURN,
                    None,
                    now,
                    processor.turn_execution_owner_id.as_ref(),
                    now + chrono::Duration::seconds(60),
                )
                .await
                .unwrap();
            store
                .register_cli_runtime_execution_segment(
                    BLOCKED_TURN,
                    BLOCKED_NATIVE_THREAD,
                    BLOCKED_TURN,
                    now,
                )
                .await
                .unwrap();
        }
        binding::Entity::update_many()
            .col_expr(
                binding::Column::Model,
                Expr::value("metadata changed by admission"),
            )
            .filter(binding::Column::TurnId.eq(BLOCKED_TURN))
            .exec(&db)
            .await
            .unwrap();
        match phase {
            "replacement_attempt" => {
                store
                    .mark_cli_runtime_turn_attempt_terminal(
                        &attempt.id,
                        pioneer_crud::CliRuntimeTurnAttemptStatus::Interrupted,
                        Some("superseded".into()),
                        chrono::Utc::now().fixed_offset(),
                    )
                    .await
                    .unwrap();
                store
                    .prepare_cli_runtime_recovery_turn_attempt(
                        BLOCKED_TURN,
                        "new-goal-attempt".into(),
                        "new-goal-job".into(),
                        "new-goal-recovery".into(),
                        2,
                        "resume".into(),
                        chrono::Utc::now().fixed_offset(),
                        None,
                    )
                    .await
                    .unwrap();
            }
            "replacement_segment" => {
                let now = chrono::Utc::now().fixed_offset();
                store
                    .terminalize_cli_runtime_execution_segment(
                        "codex",
                        BLOCKED_TURN,
                        pioneer_crud::CliRuntimeExecutionSegmentStatus::Completed,
                        None,
                        now,
                    )
                    .await
                    .unwrap();
                store
                    .register_cli_runtime_execution_segment(
                        BLOCKED_TURN,
                        BLOCKED_NATIVE_THREAD,
                        "new-goal-segment",
                        now,
                    )
                    .await
                    .unwrap();
            }
            "owner_aba" => {
                use pioneer_entity::turn_execution as execution;
                execution::Entity::update_many()
                    .col_expr(execution::Column::OwnerGeneration, Expr::value(2_i64))
                    .filter(execution::Column::TurnId.eq(BLOCKED_TURN))
                    .exec(&db)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        // Window ACK in the actual ordered consumer remains independent of
        // the provider event waiting outside that consumer for admission.
        processor
            .publish_cli_runtime_durable_and_wait(
                handle.instance(),
                AgentDurableEvent::TurnExecutionWindowStarted {
                    notification: pioneer_protocol::TurnExecutionWindowStartedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: BLOCKED_THREAD.into(),
                        turn_id: BLOCKED_TURN.into(),
                        window_id: "goal-admission-window".into(),
                        window_index: 1,
                        status: ExecutionWindowStatus::Running,
                        started_at_unix_ms: chrono::Utc::now().timestamp_millis(),
                    },
                },
            )
            .await
            .unwrap();
        processor.release_completed_history_preparation_barrier();
        assert!(futures_util::poll!(&mut goal).is_pending());
        drop(admission);
        goal.await;
        let binding = store
            .get_cli_runtime_turn_binding(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap();
        if phase.starts_with("replacement") || phase == "owner_aba" {
            assert!(
                binding.native_goal_observed_at.is_none(),
                "an old provider event must not bind to a replacement"
            );
            assert!(session.interrupts.lock().await.is_empty());
            assert!(!has_recovery(&store, BLOCKED_TURN).await);
        } else {
            assert!(binding.native_goal_observed_at.is_some());
            assert_eq!(
                binding.native_goal_status.as_deref(),
                match phase {
                    "metadata_complete" => Some("complete"),
                    "metadata_cleared" => None,
                    _ => Some("active"),
                }
            );
            if phase == "activation" {
                processor
                    .handle_cli_runtime_timeline_event(
                        handle.instance(),
                        RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                            native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
                            native_turn_id: BLOCKED_TURN.into(),
                            status: "completed".into(),
                            native: None,
                        }),
                    )
                    .await;
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
                assert!(
                    store
                        .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        .is_active()
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_unsaved_native_failure_cannot_follow_receipt_resume_into_attempt_b() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, session, workspace) = blocked_observation_fixture_with_receipt(
        &temp.path().join("receipt-resume.sqlite"),
        "codex",
        true,
        true,
    )
    .await;
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_before_gate__");
    let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
        handle.instance(),
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN),
    ));
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("producer A must pause before saving its outcome"),
    }
    let binding = store
        .get_cli_runtime_turn_binding(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap();
    let turn = store
        .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert!(matches!(
        processor
            .reconcile_cli_runtime_turn_from_runtime(&binding, &workspace, &turn)
            .await
            .unwrap(),
        crate::message::cli_runtime::CLIRuntimeAuthoritativeTurnState::Terminal
    ));
    let now = chrono::Utc::now();
    let job = store
        .enqueue_recovery_job(
            BLOCKED_TURN.into(),
            "receipt-resume-fixture".into(),
            TurnItemType::SystemEvent,
            None,
            pioneer_protocol::RecoveryTrigger::Timeout,
            pioneer_protocol::RecoveryAction::BlockResumable,
            Some("explicitly blocked".into()),
            None,
            None,
            None,
            0,
            0,
            json!({}),
            json!({}),
            now.timestamp(),
        )
        .await
        .unwrap();
    store
        .mark_recovery_job_terminal(
            &job.id,
            pioneer_protocol::RecoveryJobStatus::Blocked,
            Some("explicitly blocked".into()),
            now.timestamp(),
        )
        .await
        .unwrap();
    let transition = processor
        .cli_runtime_turn_resume_transition(BLOCKED_TURN)
        .await
        .unwrap();
    assert!(matches!(
        store
            .resume_blocked_turn_recovery(
                BLOCKED_THREAD,
                BLOCKED_TURN,
                Some(&job.id),
                now.timestamp(),
                processor.turn_execution_owner_id.as_ref(),
                now.timestamp() + 60
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
        .find(|claimed| claimed.id == job.id)
        .unwrap();
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claimed.claim_token.as_deref().unwrap(),
                "receipt-recovery-B",
                now.timestamp()
            )
            .await
            .unwrap(),
        pioneer_crud::ClaimedRecoveryActivation::Activated
    ));
    let (_, attempt) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            BLOCKED_TURN,
            "receipt-attempt-B".into(),
            job.id,
            "receipt-recovery-B".into(),
            2,
            "explicit resume".into(),
            now.fixed_offset(),
            None,
        )
        .await
        .unwrap();
    store
        .activate_cli_runtime_turn_attempt_owned(
            BLOCKED_TURN,
            &attempt.id,
            "receipt-native-B",
            None,
            now.fixed_offset(),
            processor.turn_execution_owner_id.as_ref(),
            (now + chrono::Duration::seconds(60)).fixed_offset(),
        )
        .await
        .unwrap();
    store
        .register_cli_runtime_execution_segment(
            BLOCKED_TURN,
            BLOCKED_NATIVE_THREAD,
            "receipt-native-B",
            now.fixed_offset(),
        )
        .await
        .unwrap();
    let before = store
        .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
        .await
        .unwrap();
    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
        .count(&store.database_connection())
        .await
        .unwrap();
    session.interrupts.lock().await.clear();
    session.mcp_terminals.lock().await.clear();
    let cancel = tokio_util::sync::CancellationToken::new();
    let _invocation = processor
        .mcp_service
        .hold_test_turn_mcp_invocation(BLOCKED_TURN, cancel.clone());
    drop(transition);
    processor.release_completed_history_preparation_barrier();
    producer.await;
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
            .count(&store.database_connection())
            .await
            .unwrap(),
        events
    );
    assert!(!cancel.is_cancelled());
    assert!(session.interrupts.lock().await.is_empty());
    assert!(session.mcp_terminals.lock().await.is_empty());
}

const ACTIVE_RECOVERY_NATIVE: &str = "active-recovery-native";

// Real admission receipt and the same public activation/preparation APIs used
// by coordinator dispatch. The recovery attempt has not emitted confirmation.
async fn activate_unconfirmed_cli_recovery(
    processor: &MessageProcessor,
    store: &CrudStore,
    kind: &str,
    exhaust: bool,
) -> pioneer_crud::RecoveryJobRecord {
    let now = chrono::Utc::now();
    let job = store
        .enqueue_recovery_job(
            BLOCKED_TURN.into(),
            "unconfirmed-recovery".into(),
            TurnItemType::SystemEvent,
            None,
            pioneer_protocol::RecoveryTrigger::RuntimeFailure,
            pioneer_protocol::RecoveryAction::RestartTurn,
            Some("original recovery cause".into()),
            None,
            None,
            None,
            0,
            if exhaust { 1 } else { 4 },
            json!({"max_wall_clock_secs": 3600, "no_progress_limit": 4}),
            json!({}),
            now.timestamp(),
        )
        .await
        .unwrap();
    let claim = store
        .claim_due_recovery_jobs(now.timestamp(), 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.id == job.id)
        .unwrap();
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claim.claim_token.as_deref().unwrap(),
                "active-recovery-authority",
                now.timestamp()
            )
            .await
            .unwrap(),
        pioneer_crud::ClaimedRecoveryActivation::Activated
    ));
    let (_, attempt) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            BLOCKED_TURN,
            "active-recovery-attempt".into(),
            job.id.clone(),
            "active-recovery-authority".into(),
            2,
            "original recovery cause".into(),
            now.fixed_offset(),
            None,
        )
        .await
        .unwrap();
    store
        .activate_cli_runtime_turn_attempt_owned(
            BLOCKED_TURN,
            &attempt.id,
            ACTIVE_RECOVERY_NATIVE,
            None,
            now.fixed_offset(),
            processor.turn_execution_owner_id.as_ref(),
            (now + chrono::Duration::seconds(60)).fixed_offset(),
        )
        .await
        .unwrap();
    if kind == "codex" {
        store
            .register_cli_runtime_execution_segment(
                BLOCKED_TURN,
                BLOCKED_NATIVE_THREAD,
                ACTIVE_RECOVERY_NATIVE,
                now.fixed_offset(),
            )
            .await
            .unwrap();
    }
    // Expire the per-attempt window, keeping the overall retry budget available.
    use pioneer_entity::recovery_job as recovery;
    recovery::Entity::update_many()
        .col_expr(
            recovery::Column::ActiveAttemptStartedAt,
            Expr::value((now - chrono::Duration::seconds(901)).fixed_offset()),
        )
        .filter(recovery::Column::Id.eq(&job.id))
        .exec(&store.database_connection())
        .await
        .unwrap();
    // Admission's causal activity must also predate this expired window;
    // otherwise the coordinator correctly suppresses expiration as progress.
    turn_liveness::Entity::update_many()
        .col_expr(
            turn_liveness::Column::LastActivityAt,
            Expr::value((now - chrono::Duration::seconds(901)).fixed_offset()),
        )
        .filter(turn_liveness::Column::TurnId.eq(BLOCKED_TURN))
        .exec(&store.database_connection())
        .await
        .unwrap();
    assert!(
        store
            .latest_cli_runtime_turn_attempt(BLOCKED_TURN)
            .await
            .unwrap()
            .unwrap()
            .recovery_confirmed_at
            .is_none()
    );
    store.get_recovery_job(&job.id).await.unwrap().unwrap()
}

fn recovery_native_outcome(completed: bool) -> RuntimeEvent {
    if completed {
        RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
            native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
            native_turn_id: ACTIVE_RECOVERY_NATIVE.into(),
            status: "completed".into(),
            native: Some(pioneer_cli_agent_runtime::event::RuntimeNativeEvent {
                method: "turn/completed".into(),
                payload_redacted: Some(
                    json!({"assistantRecordUuid": "01900000-0000-7000-8000-000000000072"}),
                ),
                raw_redacted: None,
            }),
        })
    } else {
        ordinary_native_failure(BLOCKED_NATIVE_THREAD, ACTIVE_RECOVERY_NATIVE)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_accepted_unconfirmed_recovery_outcome_survives_expiration_takeover_and_reopen() {
    for kind in ["codex", "claude"] {
        for completed in [true, false] {
            for exhaust in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("unconfirmed-recovery.sqlite");
                let (processor, store, session, workspace) =
                    blocked_observation_fixture_with_receipt(&path, kind, true, true).await;
                let job =
                    activate_unconfirmed_cli_recovery(&processor, &store, kind, exhaust).await;
                let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
                let handle = processor
                    .cli_runtime_manager
                    .as_ref()
                    .unwrap()
                    .existing_session(&key)
                    .await
                    .unwrap();
                processor
                    .arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
                let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
                    handle.instance(),
                    recovery_native_outcome(completed),
                ));
                tokio::select! {
                    _ = processor.wait_for_completed_history_preparation_barrier() => {},
                    _ = &mut producer => panic!("accepted outcome must precede cancellation"),
                }
                drop(producer);
                assert!(
                    store
                        .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
                        .await
                        .unwrap()
                );
                let source = store
                    .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap()
                    .terminal_event_source()
                    .unwrap();
                let outcome_id = source.terminal_delivery_id();
                assert!(session.interrupts.lock().await.is_empty());
                let original_owner = store
                    .get_turn_execution(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap();
                drop(handle);
                drop(processor);
                drop(store);
                let (reopened, store, session, _) =
                    blocked_observation_fixture_with_receipt(&path, kind, false, true).await;
                *session.turn_observation.lock().await = None;
                let now = chrono::Utc::now().timestamp();
                // Actual resilience order, including the expired Active job.
                let events = reopened
                    .recovery_coordinator
                    .run_ready_jobs(now, 16)
                    .await
                    .unwrap();
                assert!(
                    events.is_empty(),
                    "expiration cannot synthesize an outcome over the journal"
                );
                let unchanged = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                assert_eq!(unchanged.status, job.status);
                assert_eq!(unchanged.active_attempt_id, job.active_attempt_id);
                assert_eq!(unchanged.run_count, job.run_count);
                assert_eq!(unchanged.last_error, job.last_error);
                assert_eq!(unchanged.updated_at_unix, job.updated_at_unix);
                let deferred = reopened
                    .fail_stale_cli_runtime_turns(
                        now * 1000,
                        &mut CliRuntimeStaleTurnScan::default(),
                    )
                    .await;
                assert_eq!((deferred.selected, deferred.processed), (1, 0));
                assert_eq!(
                    store
                        .get_turn_execution(BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap(),
                    original_owner
                );
                use pioneer_entity::turn_execution as execution;
                let expired_at = chrono::DateTime::from_timestamp(now - 1, 0)
                    .unwrap()
                    .fixed_offset();
                execution::Entity::update_many()
                    .col_expr(execution::Column::HeartbeatAt, Expr::value(expired_at))
                    .col_expr(execution::Column::LeaseUntil, Expr::value(expired_at))
                    .filter(execution::Column::TurnId.eq(BLOCKED_TURN))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
                let events = reopened
                    .recovery_coordinator
                    .run_ready_jobs(now, 16)
                    .await
                    .unwrap();
                let owner = store
                    .get_turn_execution(BLOCKED_TURN)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(owner.owner_id, reopened.turn_execution_owner_id.as_ref());
                assert_eq!(owner.owner_generation, original_owner.owner_generation + 1);
                let unchanged = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                assert_eq!(unchanged.status, job.status);
                assert_eq!(unchanged.active_attempt_id, job.active_attempt_id);
                assert_eq!(unchanged.run_count, job.run_count);
                assert_eq!(unchanged.last_error, job.last_error);
                assert_eq!(unchanged.updated_at_unix, job.updated_at_unix);
                for event in events {
                    reopened.handle_recovery_event(event, now).await;
                }
                assert!(
                    session.turn_starts.lock().await.is_empty(),
                    "the accepted execution is not replaced"
                );
                let delivered = reopened
                    .fail_stale_cli_runtime_turns(
                        now * 1000,
                        &mut CliRuntimeStaleTurnScan::default(),
                    )
                    .await;
                assert_eq!(
                    (delivered.selected, delivered.processed),
                    (1, 1),
                    "runtime={kind}, completed={completed}, exhaust={exhaust}"
                );
                let delivered_job = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                let ack = store
                    .get_cli_runtime_native_event(&format!("{outcome_id}:ack"))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    ack.native_method,
                    if completed {
                        "gateway/terminal_ack"
                    } else {
                        "gateway/terminal_recovery_ack"
                    }
                );
                if completed {
                    assert_eq!(
                        delivered_job.status,
                        pioneer_protocol::RecoveryJobStatus::Succeeded
                    );
                    assert_eq!(
                        store
                            .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                            .await
                            .unwrap()
                            .unwrap()
                            .1
                            .status,
                        TurnStatus::Completed
                    );
                    let binding = store
                        .get_cli_runtime_turn_binding(BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(binding.status, "completed");
                    if kind == "claude" {
                        assert_eq!(
                            crate::cli_runtime::thread_binding::claude_assistant_record_boundary(
                                &store, &binding
                            )
                            .await
                            .unwrap(),
                            Some(
                                uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000072")
                                    .unwrap()
                            )
                        );
                    }
                } else {
                    assert_eq!(
                        delivered_job.status,
                        if exhaust {
                            pioneer_protocol::RecoveryJobStatus::Exhausted
                        } else {
                            pioneer_protocol::RecoveryJobStatus::Pending
                        }
                    );
                    assert_eq!(delivered_job.last_failure_attempt_id, job.active_attempt_id);
                    let reason = delivered_job.last_error.as_deref().unwrap();
                    assert!(reason.contains("ordinary native outcome"));
                    assert!(!reason.contains("recovery attempt exceeded"));
                    if exhaust {
                        reopened
                            .process_due_recovery_terminalizations(now, 16)
                            .await
                            .unwrap();
                        assert_eq!(
                            store
                                .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                                .await
                                .unwrap()
                                .unwrap()
                                .1
                                .status,
                            TurnStatus::Failed,
                            "the existing outbox completes the accepted failure duty"
                        );
                    }
                }
                assert!(
                    !store
                        .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
                        .await
                        .unwrap()
                );
                let count = pioneer_entity::turn_event::Entity::find()
                    .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                    .count(&store.database_connection())
                    .await
                    .unwrap();
                reopened
                    .fail_stale_cli_runtime_turns(
                        now * 1000,
                        &mut CliRuntimeStaleTurnScan::default(),
                    )
                    .await;
                assert_eq!(
                    pioneer_entity::turn_event::Entity::find()
                        .filter(pioneer_entity::turn_event::Column::TurnId.eq(BLOCKED_TURN))
                        .count(&store.database_connection())
                        .await
                        .unwrap(),
                    count
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_recovery_expiration_first_rejects_unsaved_producer_and_preserves_retry_budget() {
    for kind in ["codex", "claude"] {
        for completed in [true, false] {
            for exhaust in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let (processor, store, session, workspace) =
                    blocked_observation_fixture_with_receipt(
                        &temp.path().join("expiration-first.sqlite"),
                        kind,
                        true,
                        true,
                    )
                    .await;
                let job =
                    activate_unconfirmed_cli_recovery(&processor, &store, kind, exhaust).await;
                let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
                let handle = processor
                    .cli_runtime_manager
                    .as_ref()
                    .unwrap()
                    .existing_session(&key)
                    .await
                    .unwrap();
                processor.arm_completed_history_preparation_barrier(
                    "__cli_native_terminal_before_gate__",
                );
                let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
                    handle.instance(),
                    recovery_native_outcome(completed),
                ));
                tokio::select! {
                    _ = processor.wait_for_completed_history_preparation_barrier() => {},
                    _ = &mut producer => panic!("source must be selected before authority expires"),
                }
                let events = processor
                    .recovery_coordinator
                    .run_ready_jobs(chrono::Utc::now().timestamp(), 16)
                    .await
                    .unwrap();
                assert!(events.iter().any(|event| matches!(
                    event,
                    crate::resilience::RecoveryCoordinatorEvent::RetryScheduled { .. }
                        | crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(_)
                )));
                let expired = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                assert_eq!(
                    expired.status,
                    if exhaust {
                        pioneer_protocol::RecoveryJobStatus::Exhausted
                    } else {
                        pioneer_protocol::RecoveryJobStatus::Pending
                    }
                );
                assert!(expired.active_attempt_id.is_none());
                assert!(
                    !store
                        .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
                        .await
                        .unwrap()
                );
                if !exhaust && !completed {
                    let due = expired.next_run_at_unix;
                    let claim = store
                        .claim_due_recovery_jobs(due, 60, 16)
                        .await
                        .unwrap()
                        .into_iter()
                        .find(|claim| claim.id == job.id)
                        .unwrap();
                    assert!(matches!(
                        store
                            .mark_claimed_recovery_job_active(
                                &job.id,
                                claim.claim_token.as_deref().unwrap(),
                                "replacement-authority",
                                due
                            )
                            .await
                            .unwrap(),
                        pioneer_crud::ClaimedRecoveryActivation::Activated
                    ));
                    let now = chrono::Utc::now();
                    let (_, replacement) = store
                        .prepare_cli_runtime_recovery_turn_attempt(
                            BLOCKED_TURN,
                            "replacement-attempt".into(),
                            job.id.clone(),
                            "replacement-authority".into(),
                            3,
                            "expired attempt".into(),
                            now.fixed_offset(),
                            None,
                        )
                        .await
                        .unwrap();
                    store
                        .activate_cli_runtime_turn_attempt_owned(
                            BLOCKED_TURN,
                            &replacement.id,
                            "replacement-native",
                            None,
                            now.fixed_offset(),
                            processor.turn_execution_owner_id.as_ref(),
                            (now + chrono::Duration::seconds(60)).fixed_offset(),
                        )
                        .await
                        .unwrap();
                    if kind == "codex" {
                        store
                            .register_cli_runtime_execution_segment(
                                BLOCKED_TURN,
                                BLOCKED_NATIVE_THREAD,
                                "replacement-native",
                                now.fixed_offset(),
                            )
                            .await
                            .unwrap();
                    }
                }
                let protected_job = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                let protected_execution = store
                    .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
                    .await
                    .unwrap();
                processor.release_completed_history_preparation_barrier();
                producer.await;
                let unchanged = store.get_recovery_job(&job.id).await.unwrap().unwrap();
                assert_eq!(unchanged.status, protected_job.status);
                assert_eq!(unchanged.active_attempt_id, protected_job.active_attempt_id);
                assert_eq!(unchanged.run_count, protected_job.run_count);
                assert_eq!(unchanged.last_error, protected_job.last_error);
                assert!(
                    !store
                        .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
                        .await
                        .unwrap()
                );
                assert!(session.interrupts.lock().await.is_empty());
                assert!(session.turn_starts.lock().await.is_empty());
                assert_eq!(
                    store
                        .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
                        .await
                        .unwrap(),
                    protected_execution
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_common_observation_writer_loses_to_accepted_native_outcome() {
    for kind in ["codex", "claude"] {
        for completed in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let (processor, store, session, workspace) = blocked_observation_fixture_with_receipt(
                &temp.path().join("observation-race.sqlite"),
                kind,
                true,
                true,
            )
            .await;
            let key = CLIAgentRuntimeSessionKey::new(&workspace, kind, BLOCKED_THREAD).unwrap();
            let handle = processor
                .cli_runtime_manager
                .as_ref()
                .unwrap()
                .existing_session(&key)
                .await
                .unwrap();
            processor.arm_completed_history_preparation_barrier("__cli_blocked_before_enqueue__");
            // Real timeout entry, not a direct invocation of the guarded inner.
            let mut timeout = Box::pin(processor.renew_active_cli_runtime_turn_deadlines(
                BLOCKED_TURN,
                chrono::Utc::now().timestamp(),
            ));
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut timeout => panic!("Blocked snapshot must pause before its writer commit"),
            }
            // Leave the selected timeout future unpolled while the producer
            // saves outside its owned gate. Both boundaries are deterministic.
            processor.release_completed_history_preparation_barrier();
            processor.arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
            let event = if completed {
                RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                    native_thread_id: Some(BLOCKED_NATIVE_THREAD.into()),
                    native_turn_id: BLOCKED_TURN.into(),
                    status: "completed".into(),
                    native: Some(pioneer_cli_agent_runtime::event::RuntimeNativeEvent {
                        method: "turn/completed".into(),
                        payload_redacted: Some(
                            json!({"assistantRecordUuid":"01900000-0000-7000-8000-000000000072"}),
                        ),
                        raw_redacted: None,
                    }),
                })
            } else {
                ordinary_native_failure(BLOCKED_NATIVE_THREAD, BLOCKED_TURN)
            };
            let mut producer =
                Box::pin(processor.handle_cli_runtime_timeline_event(handle.instance(), event));
            tokio::select! {
                _ = processor.wait_for_completed_history_preparation_barrier() => {},
                _ = &mut producer => panic!("native acceptance must occur while timeout owns the gate"),
            }
            drop(producer);
            processor.release_completed_history_preparation_barrier();
            let _ = timeout.await.unwrap();
            assert_eq!(blocked_event_count(&store).await, 0);
            assert_blocked_observation_active(&store).await;
            assert!(session.interrupts.lock().await.is_empty());
            // Common reconciliation now delivers journal facts, not the
            // recording provider's conflicting Blocked observation.
            let delivered = processor
                .renew_active_cli_runtime_turn_deadlines(
                    BLOCKED_TURN,
                    chrono::Utc::now().timestamp(),
                )
                .await
                .unwrap();
            assert_eq!(
                delivered,
                crate::resilience::RuntimeTimeoutObservation::Terminal
            );
            assert_eq!(blocked_event_count(&store).await, 0);
            if completed {
                assert_eq!(
                    store
                        .get_turn(BLOCKED_THREAD, BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap()
                        .1
                        .status,
                    TurnStatus::Completed
                );
                if kind == "claude" {
                    let binding = store
                        .get_cli_runtime_turn_binding(BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        crate::cli_runtime::thread_binding::claude_assistant_record_boundary(
                            &store, &binding
                        )
                        .await
                        .unwrap(),
                        Some(
                            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000072").unwrap()
                        )
                    );
                }
            } else {
                assert!(
                    store
                        .find_unresolved_recovery_job_for_turn(BLOCKED_TURN)
                        .await
                        .unwrap()
                        .unwrap()
                        .reason
                        .unwrap()
                        .contains("ordinary native outcome")
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_accepted_recovery_failure_ack_and_authority_change_roll_back_together() {
    let temp = tempfile::tempdir().unwrap();
    let (processor, store, _, workspace) = blocked_observation_fixture_with_receipt(
        &temp.path().join("ack-rollback.sqlite"),
        "codex",
        true,
        true,
    )
    .await;
    let job = activate_unconfirmed_cli_recovery(&processor, &store, "codex", false).await;
    let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", BLOCKED_THREAD).unwrap();
    let handle = processor
        .cli_runtime_manager
        .as_ref()
        .unwrap()
        .existing_session(&key)
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
    let mut producer = Box::pin(
        processor
            .handle_cli_runtime_timeline_event(handle.instance(), recovery_native_outcome(false)),
    );
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut producer => panic!("failure must be accepted before delivery"),
    }
    drop(producer);
    let source = store
        .cli_runtime_turn_terminal_guard_by_id(BLOCKED_TURN)
        .await
        .unwrap()
        .unwrap()
        .terminal_event_source()
        .unwrap();
    let record =
        crate::message::cli_runtime::cli_runtime_terminal_event_record(&source, String::new());
    store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_recovery_ack BEFORE INSERT ON cli_runtime_native_event WHEN NEW.native_method = 'gateway/terminal_recovery_ack' BEGIN SELECT RAISE(ABORT, 'injected ACK failure'); END"
    ).await.unwrap();
    let result = processor
        .renew_active_cli_runtime_turn_deadlines(BLOCKED_TURN, chrono::Utc::now().timestamp())
        .await
        .unwrap();
    assert_eq!(
        result,
        crate::resilience::RuntimeTimeoutObservation::Unavailable
    );
    let unchanged = store.get_recovery_job(&job.id).await.unwrap().unwrap();
    assert_eq!(unchanged.status, job.status);
    assert_eq!(unchanged.active_attempt_id, job.active_attempt_id);
    assert_eq!(unchanged.run_count, job.run_count);
    assert_eq!(unchanged.last_error, job.last_error);
    assert!(
        store
            .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
            .await
            .unwrap()
    );
    assert_blocked_observation_active(&store).await;
    assert_eq!(blocked_event_count(&store).await, 0);
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_recovery_ack")
        .await
        .unwrap();
    assert_eq!(
        processor
            .renew_active_cli_runtime_turn_deadlines(BLOCKED_TURN, chrono::Utc::now().timestamp())
            .await
            .unwrap(),
        crate::resilience::RuntimeTimeoutObservation::Terminal
    );
    let retried = store.get_recovery_job(&job.id).await.unwrap().unwrap();
    assert_eq!(retried.status, pioneer_protocol::RecoveryJobStatus::Pending);
    assert_eq!(retried.run_count, job.run_count + 1);
    assert_eq!(retried.last_failure_attempt_id, job.active_attempt_id);
    assert!(
        !store
            .has_pending_cli_runtime_terminal_event(BLOCKED_TURN)
            .await
            .unwrap()
    );
    let ack_id = format!("{}:ack", record.id);
    let ack = store
        .get_cli_runtime_native_event(&ack_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack.native_method, "gateway/terminal_recovery_ack");
    processor
        .ack_cli_runtime_terminal_event(&record)
        .await
        .unwrap();
    let mut conflicting = record;
    conflicting.native_turn_id = Some("another-native-execution".into());
    assert!(
        processor
            .ack_cli_runtime_terminal_event(&conflicting)
            .await
            .is_err()
    );
    assert_eq!(
        store.get_cli_runtime_native_event(&ack_id).await.unwrap(),
        Some(ack),
        "generic ACK replay preserves the recovery marker and its exact native identity"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_timeout_poll_and_command_heartbeat_prioritize_saved_native_failure() {
    for heartbeat in [false, true] {
        let (processor, _, rx, workspace, store, session) = cli_runtime_approval_processor().await;
        drop(rx);
        let processor = processor.scoped_for_background_reconciliation();
        let now = chrono::Utc::now().timestamp();
        materialize_expired_cli_command_attempt(&store, &workspace, now).await;
        *session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
            status: CLIAgentRuntimeObservedTurnStatus::Blocked,
            message: Some("conflicting provider observation".into()),
            reconciliation_events: Vec::new(),
        });
        let binding = store
            .get_cli_runtime_turn_binding(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap();
        let key = CLIAgentRuntimeSessionKey::new(&workspace, "codex", GUARDED_THREAD).unwrap();
        let handle = processor
            .cli_runtime_manager
            .as_ref()
            .unwrap()
            .existing_session(&key)
            .await
            .unwrap();
        assert!(
            store
                .get_cli_runtime_instruction_projection(GUARDED_TURN)
                .await
                .unwrap()
                .is_none(),
            "saved delivery on the existing session must not require fresh launch admission"
        );
        processor.arm_completed_history_preparation_barrier("__cli_native_terminal_saved__");
        let mut producer = Box::pin(processor.handle_cli_runtime_timeline_event(
            handle.instance(),
            ordinary_native_failure(
                &binding.native_thread_id,
                binding.native_turn_id.as_deref().unwrap(),
            ),
        ));
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {},
            _ = &mut producer => panic!("real producer must save before publisher cancellation"),
        }
        drop(producer);
        let due = now + processor.cli_runtime_command_heartbeats.interval_secs() + 2;
        if heartbeat {
            assert_eq!(
                processor.heartbeat_due_cli_runtime_command_items(due).await,
                0
            );
        } else {
            assert!(
                processor
                    .poll_timeouts_respecting_human_wait(due, 64)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let job = store
            .find_unresolved_recovery_job_for_turn(GUARDED_TURN)
            .await
            .unwrap()
            .unwrap();
        assert!(
            job.reason
                .as_deref()
                .unwrap()
                .contains("ordinary native outcome")
        );
        assert!(
            !job.reason
                .as_deref()
                .unwrap()
                .contains("conflicting provider observation")
        );
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
        assert!(session.interrupts.lock().await.is_empty());
        assert!(
            !store
                .has_pending_cli_runtime_terminal_event(GUARDED_TURN)
                .await
                .unwrap()
        );
    }
}
