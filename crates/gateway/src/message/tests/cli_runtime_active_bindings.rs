use super::*;
use crate::message::cli_runtime::CliRuntimeStaleTurnScan;
use pioneer_entity::{turn, turn_cli_runtime_binding as binding, turn_liveness};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

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
