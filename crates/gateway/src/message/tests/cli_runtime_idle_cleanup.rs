use super::*;
use crate::cli_runtime::manager::CLIAgentRuntimeSessionLifecycle;
use crate::cli_runtime::session_instance::CliSessionInstanceId;
use crate::post_startup::PostStartupSupervisor;
use tokio_util::sync::CancellationToken;

// Advance only the monotonic idle clock; SQLite/authorization timestamps keep
// their real wall clock. No sleeps or provider activity simulate idleness.
async fn elapse(duration: Duration) {
    tokio::time::pause();
    tokio::time::advance(duration).await;
    tokio::time::resume();
}

#[tokio::test]
async fn idle_cleanup_preserves_real_admission_silent_turn_terminal_effects_and_continuation() {
    let mut harness = setup_cli_runtime_security_harness(Some(CLIAgentRuntimeKind::Codex)).await;
    let processor = harness.processor.clone();
    let thread = "idle-admission-thread";
    let turn = "idle-admission-turn";
    seed_cli_runtime_security_thread(
        &harness,
        thread,
        "Idle cleanup",
        "o4-mini",
        "openai",
        Some(SandboxMode::FullAccess),
    )
    .await;
    let request_id = generate_test_request_id("idle", "first");
    let request = cli_runtime_turn_start_request(
        &request_id,
        thread,
        turn,
        cli_runtime_execution_backend("codex", CLIAgentRuntimeKind::Codex),
        "full_access",
        "silent long work",
        None,
    );
    processor.arm_completed_history_preparation_barrier("__cli_predispatch__");
    let admission = {
        let processor = processor.clone();
        let connection = harness.connection_id;
        tokio::spawn(async move {
            processor
                .process_request_for_connection(connection, &request)
                .await
        })
    };
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .expect("real admission must reach the predispatch barrier");
    let cancel = CancellationToken::new();
    elapse(Duration::from_secs(601)).await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    assert!(harness.cli_session.turn_starts.lock().await.is_empty());
    assert!(
        processor
            .cli_runtime_session_turn_leases
            .lock()
            .await
            .contains_key(turn)
    );
    processor.release_completed_history_preparation_barrier();
    admission.await.unwrap();
    recv_response_by_id(&mut harness.rx, &request_id).await;
    let starts = wait_for_cli_runtime_turn_starts(&harness.cli_session, 1).await;
    let binding = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(binding) = harness
                .crud_store
                .get_cli_runtime_turn_binding(turn)
                .await
                .unwrap()
                && binding.status == "running"
            {
                break binding;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real admission must persist its running binding");
    // The running binding precedes the execution-window commit and MCP
    // activation. Let admission release its transition gate before advancing
    // Tokio's clock, or pending SQLite waits can expire during the jump.
    let key = CLIAgentRuntimeSessionKey::new(&harness.workspace_id, "codex", thread).unwrap();
    let transition = processor.cli_runtime_session_transition_mutex(&key).await;
    let admitted = timeout(Duration::from_secs(10), transition.lock())
        .await
        .expect("real admission must finish before simulating silent work");
    assert_eq!(
        harness
            .crud_store
            .latest_turn_execution_window(turn)
            .await
            .unwrap()
            .expect("real admission must persist its execution window")
            .status,
        pioneer_protocol::ExecutionWindowStatus::Running
    );
    drop(admitted);
    // No event, token, approval response or activity heartbeat is required to
    // keep this real admitted Turn alive through a wait longer than its TTL.
    elapse(Duration::from_secs(601)).await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    assert_eq!(harness.cli_session.closes.load(Ordering::SeqCst), 0);
    processor.arm_completed_history_preparation_barrier("__cli_terminal_after_commit__");
    let completion = complete_recorded_cli_task_turn(
        &processor,
        &harness.cli_session,
        &harness.cli_manager,
        &harness.workspace_id,
        "codex",
        thread,
        &starts[0].native_thread_id,
        binding.native_turn_id.as_deref().unwrap(),
        "finished long work",
    );
    tokio::pin!(completion);
    tokio::select! {
        _ = processor.wait_for_completed_history_preparation_barrier() => {},
        _ = &mut completion => panic!("terminal effects must reach the barrier"),
    }
    elapse(Duration::from_secs(601)).await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    processor.release_completed_history_preparation_barrier();
    completion.await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    elapse(Duration::from_secs(30)).await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    elapse(Duration::from_secs(31)).await;
    let close_entered = Arc::new(tokio::sync::Notify::new());
    let close_release = Arc::new(tokio::sync::Notify::new());
    *harness.cli_session.close_pause.lock().await =
        Some((close_entered.clone(), close_release.clone()));
    let cleanup = processor.close_idle_cli_runtime_sessions(&cancel);
    tokio::pin!(cleanup);
    tokio::select! {
        _ = close_entered.notified() => {},
        _ = &mut cleanup => panic!("expired idle process must reach close"),
    }
    let request_id = generate_test_request_id("idle", "next");
    let request = cli_runtime_turn_start_request(
        &request_id,
        thread,
        "idle-next-turn",
        cli_runtime_execution_backend("codex", CLIAgentRuntimeKind::Codex),
        "full_access",
        "continue after eviction",
        None,
    );
    processor.arm_completed_history_preparation_barrier("__cli_before_transition__");
    {
        let next_admission = process_cli_runtime_turn_start(&harness, &request);
        tokio::pin!(next_admission);
        tokio::select! {
            _ = processor.wait_for_completed_history_preparation_barrier() => {},
            _ = &mut next_admission => panic!("cleanup must not reject new admission as an active Turn"),
        }
        // Admission owns its real Turn lease, but cannot touch the retiring process
        // until cleanup releases the transition gate. No spurious AlreadyActive.
        close_release.notify_one();
        assert_eq!(cleanup.await, 1);
        assert_eq!(harness.cli_session.closes.load(Ordering::SeqCst), 1);
        assert_eq!(
            harness
                .crud_store
                .get_turn(thread, turn)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Completed
        );
        assert!(
            harness
                .crud_store
                .find_unresolved_recovery_job_for_turn(turn)
                .await
                .unwrap()
                .is_none()
        );
        processor.release_completed_history_preparation_barrier();
        next_admission.await;
    }
    recv_response_by_id(&mut harness.rx, &request_id).await;
    let next = wait_for_cli_runtime_turn_starts(&harness.cli_session, 2).await;
    assert_eq!(next[1].native_thread_id, starts[0].native_thread_id);
    assert_eq!(harness.cli_session.factory_starts.load(Ordering::SeqCst), 2);
    assert_eq!(harness.cli_session.thread_starts.lock().await.len(), 1);
    assert!(
        harness
            .cli_session
            .thread_resumes
            .lock()
            .await
            .iter()
            .any(|(id, _)| id == &starts[0].native_thread_id)
    );
    processor.shutdown_cli_runtime_manager().await;
}

#[tokio::test]
async fn idle_cleanup_defers_gates_recovered_continuation_and_activity_lookup_error() {
    use pioneer_entity::turn_cli_runtime_binding as binding;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let harness = setup_cli_runtime_security_harness(Some(CLIAgentRuntimeKind::Codex)).await;
    let processor = &harness.processor;
    let store = &harness.crud_store;
    let workspace = &harness.workspace_id;
    let session = &harness.cli_session;
    let manager = &harness.cli_manager;
    seed_cli_runtime_approval_turn(
        store,
        workspace,
        "hidden-execution",
        "hidden-turn",
        "native-root",
    )
    .await;
    let key =
        CLIAgentRuntimeSessionKey::new(workspace.as_str(), "codex", "continuation-root").unwrap();
    let idle =
        CLIAgentRuntimeSessionKey::new(workspace.as_str(), "codex", "idle-neighbor").unwrap();
    manager.get_or_start(key.clone()).await.unwrap();
    manager.get_or_start(idle.clone()).await.unwrap();
    // Execution-thread ID differs from the process's continuation-thread ID;
    // no retained in-memory lease exists after a Gateway restart.
    binding::Entity::update_many()
        .col_expr(
            binding::Column::ContinuationThreadId,
            Expr::value(key.thread_id.clone()),
        )
        .col_expr(binding::Column::Status, Expr::value("starting"))
        .filter(binding::Column::TurnId.eq("hidden-turn"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    elapse(Duration::from_secs(61)).await;
    let cancel = CancellationToken::new();
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 1);
    assert!(manager.existing_session(&key).await.is_some());
    assert!(manager.existing_session(&idle).await.is_none());
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("running"))
        .filter(binding::Column::TurnId.eq("hidden-turn"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    elapse(Duration::from_secs(61)).await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    // These are the actual shared gates used by admission and recovery. A
    // management-only process without an active binding must obey them too.
    manager.get_or_start(idle.clone()).await.unwrap();
    elapse(Duration::from_secs(61)).await;
    let transition = processor.cli_runtime_session_transition_mutex(&idle).await;
    let guard = transition.lock().await;
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    drop(guard);
    // A closed DB is unknown activity, never permission to evict.
    store.database_connection().close().await.unwrap();
    assert_eq!(processor.close_idle_cli_runtime_sessions(&cancel).await, 0);
    assert!(manager.existing_session(&idle).await.is_some());
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
    manager.close_all().await.unwrap();
}

#[derive(Default)]
struct IdleLifecycle {
    events: TokioMutex<Vec<(CliSessionInstanceId, &'static str, usize)>>,
    session: Arc<RecordingCliRuntimeSession>,
}

#[async_trait]
impl CLIAgentRuntimeSessionLifecycle for IdleLifecycle {
    async fn before_session_close(&self, instance: &CliSessionInstanceId) {
        self.events.lock().await.push((
            instance.clone(),
            "before",
            self.session.closes.load(Ordering::SeqCst),
        ));
    }
    async fn after_session_close(&self, instance: &CliSessionInstanceId) {
        self.events.lock().await.push((
            instance.clone(),
            "after",
            self.session.closes.load(Ordering::SeqCst),
        ));
    }
}

#[tokio::test]
async fn idle_worker_continues_after_close_error_and_shutdown_joins_inflight_lifecycle() {
    let (mut processor, _, rx, workspace, _, _) = cli_runtime_approval_processor().await;
    drop(rx);
    let session = Arc::new(RecordingCliRuntimeSession::default());
    let lifecycle = Arc::new(IdleLifecycle {
        session: session.clone(),
        ..Default::default()
    });
    let manager = Arc::new(
        CLIAgentRuntimeManager::new_with_lifecycle(
            Arc::new(StaticCliRuntimeSessionFactory {
                session: session.clone(),
                launch_specs: None,
            }),
            Duration::from_secs(60),
            lifecycle.clone(),
        )
        .unwrap(),
    );
    processor.cli_runtime_manager = Some(manager.clone());
    for thread in ["close-error", "close-next"] {
        manager
            .get_or_start(
                CLIAgentRuntimeSessionKey::new(&workspace, "custom-codex-id", thread).unwrap(),
            )
            .await
            .unwrap();
    }
    session.fail_next_close.store(true, Ordering::SeqCst);
    elapse(Duration::from_secs(61)).await;
    assert_eq!(
        processor
            .close_idle_cli_runtime_sessions(&CancellationToken::new())
            .await,
        1
    );
    assert_eq!(session.closes.load(Ordering::SeqCst), 2);
    {
        let events = lifecycle.events.lock().await;
        assert_eq!(events.len(), 4);
        for pair in events.chunks_exact(2) {
            assert_eq!(pair[0].0, pair[1].0);
            assert_eq!((pair[0].1, pair[1].1), ("before", "after"));
            assert_eq!(pair[1].2, pair[0].2 + 1);
        }
    }
    let key =
        CLIAgentRuntimeSessionKey::new(&workspace, "custom-codex-id", "worker-owned").unwrap();
    manager.get_or_start(key.clone()).await.unwrap();
    elapse(Duration::from_secs(61)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *session.close_pause.lock().await = Some((entered.clone(), release.clone()));
    tokio::time::pause();
    let mut supervisor = PostStartupSupervisor::start(move |scope| async move {
        // The same registration entry point is called by Gateway startup.
        processor.spawn_cli_runtime_idle_cleanup(&scope);
        scope.cancellation().cancelled().await;
    });
    entered.notified().await;
    tokio::time::resume();
    assert!(manager.existing_session(&key).await.is_none());
    let mut shutdown = Box::pin(supervisor.shutdown());
    assert!(futures_util::poll!(&mut shutdown).is_pending());
    assert_eq!(lifecycle.events.lock().await.len(), 5);
    release.notify_one();
    shutdown.await;
    assert_eq!(lifecycle.events.lock().await.len(), 6);
    elapse(Duration::from_secs(120)).await;
    assert_eq!(session.closes.load(Ordering::SeqCst), 3);
    manager.close_all().await.unwrap();
}

#[tokio::test]
async fn idle_cleanup_keeps_response_generation_pinned_after_lane_removal_through_interrupt() {
    let (processor, connection, mut rx, workspace, store, session) =
        cli_runtime_approval_processor().await;
    let opened =
        open_test_codex_command_approval(&processor, &mut rx, &workspace, json!(901)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *session.interrupt_pause.lock().await = Some((entered.clone(), release.clone()));
    let response_id = generate_test_request_id("idle-response", "cancel");
    let response = json!({
        "jsonrpc":"2.0", "id":response_id,
        "method":pioneer_protocol::constants::methods::CLI_RUNTIME_REQUEST_RESPOND,
        "params":{"workspace_id":workspace,"runtime_id":"codex",
            "request_id":opened.request_id,"resolution":{"status":"cancelled"}}
    })
    .to_string();
    let request = {
        let processor = processor.clone();
        tokio::spawn(async move {
            processor
                .process_request_for_connection(connection, &response)
                .await
        })
    };
    timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("native cancel response must reach interrupt");
    // The real response handler has delivered the answer and removed its lane,
    // but its follow-up interrupt still owns the process. Model concurrent
    // completion through the normal binding repository, without a test pin.
    assert!(
        processor
            .cli_runtime_machine_requests
            .lock()
            .await
            .by_lane
            .is_empty()
    );
    store
        .terminalize_cli_runtime_turn_binding(
            "codex-turn-command",
            "completed",
            pioneer_crud::CliRuntimeTurnAttemptStatus::Completed,
            None,
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .unwrap()
        .unwrap();
    elapse(Duration::from_secs(601)).await;
    assert_eq!(
        processor
            .close_idle_cli_runtime_sessions(&CancellationToken::new())
            .await,
        0
    );
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    release.notify_one();
    request.await.unwrap();
    recv_response_by_id(&mut rx, &response_id).await;
    assert_eq!(session.interrupts.lock().await.len(), 1);
    assert_eq!(
        processor
            .close_idle_cli_runtime_sessions(&CancellationToken::new())
            .await,
        0
    );
    elapse(Duration::from_secs(60)).await;
    assert_eq!(
        processor
            .close_idle_cli_runtime_sessions(&CancellationToken::new())
            .await,
        1
    );
    processor.shutdown_cli_runtime_manager().await;
}

#[tokio::test]
async fn idle_cleanup_uses_maintenance_reader_and_releases_capacity_before_process_io() {
    use pioneer_sqlite::{SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteExecutor};
    use sea_orm::ConnectOptions;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("idle-cleanup.sqlite");
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
        .max_connections(1)
        .map_sqlx_sqlite_opts(|opts| opts.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = Arc::new(CrudStore::new(database.clone()));
    let session = Arc::new(RecordingCliRuntimeSession::default());
    let manager = test_cli_runtime_manager(session.clone());
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
    .with_cli_runtime_manager_for_tests(manager.clone());
    manager
        .get_or_start(CLIAgentRuntimeSessionKey::new(&workspace, "codex", "idle-route").unwrap())
        .await
        .unwrap();
    elapse(Duration::from_secs(61)).await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *session.close_pause.lock().await = Some((entered.clone(), release.clone()));
    let cancel = CancellationToken::new();
    let cleanup = processor.close_idle_cli_runtime_sessions(&cancel);
    tokio::pin!(cleanup);
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut cleanup => panic!("cleanup must wait in process close"),
    }
    {
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
        assert!(observer.writes.lock().unwrap().is_empty());
    }
    // With exactly one physical reader, this would wait if cleanup retained a
    // connection/transaction across close.
    assert!(
        timeout(
            Duration::from_secs(3),
            store.get_cli_runtime_turn_binding("absent")
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert!(database.reader_query_only_enabled().await.unwrap());
    release.notify_one();
    assert_eq!(cleanup.await, 1);
    manager.close_all().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn idle_retirement_fences_late_transport_recovery_before_replacement_publication() {
    let harness = setup_cli_runtime_security_harness(Some(CLIAgentRuntimeKind::Codex)).await;
    let key =
        CLIAgentRuntimeSessionKey::new(&harness.workspace_id, "codex", "late-idle-eof").unwrap();
    let handle = harness.cli_manager.get_or_start(key.clone()).await.unwrap();
    let instance = handle.instance().clone();
    let retirement = harness
        .cli_manager
        .idle_retirement_signal(&instance)
        .await
        .unwrap();
    drop(handle);
    elapse(Duration::from_secs(61)).await;
    assert_eq!(
        harness
            .processor
            .close_idle_cli_runtime_sessions(&CancellationToken::new())
            .await,
        1
    );
    assert!(retirement.is_cancelled());
    // New admission may durably create its binding before publishing a process.
    // The late old diagnostic must not recover that new execution in the gap.
    seed_cli_runtime_approval_turn(
        &harness.crud_store,
        &harness.workspace_id,
        &key.thread_id,
        "new-after-idle",
        "native-continuation",
    )
    .await;
    assert!(harness.cli_manager.existing_session(&key).await.is_none());
    harness
        .processor
        .enqueue_codex_transport_session_recovery(
            &instance,
            "late old transport diagnostic",
            Some(&retirement),
        )
        .await;
    assert!(
        harness
            .crud_store
            .find_unresolved_recovery_job_for_turn("new-after-idle")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        harness
            .crud_store
            .get_turn(&key.thread_id, "new-after-idle")
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    let events = harness
        .crud_store
        .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
            runtime_id: Some("codex".into()),
            thread_id: Some(key.thread_id.clone()),
            native_method: Some("observation_gap".into()),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(events.is_empty());
    harness.processor.shutdown_cli_runtime_manager().await;
}
