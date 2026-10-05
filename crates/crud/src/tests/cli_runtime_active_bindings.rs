use super::*;
use crate::CliRuntimeActiveTurnBindingStatus::{Running, Starting};
use crate::repositories::cli_runtime_binding::active_turn_binding_page_query;
use pioneer_entity::turn_cli_runtime_binding as binding;
use sea_orm::QueryTrait;
use std::time::Duration;

#[derive(Default)]
struct ReadObserver {
    events: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    enqueued: tokio::sync::Notify,
}

impl pioneer_sqlite::SqliteReadObserver for ReadObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.events.lock().unwrap().push(event);
        if matches!(
            event,
            pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { .. }
        ) {
            self.enqueued.notify_one();
        }
    }
}

fn fixture(id: String, status: &str) -> binding::ActiveModel {
    let timestamp = unix_to_datetime(1_700_000_000);
    binding::ActiveModel {
        turn_id: Set(id),
        thread_id: Set("thr_active_page".into()),
        continuation_thread_id: Set("thr_active_page".into()),
        workspace_id: Set("ws_active_page".into()),
        runtime_id: Set("codex".into()),
        runtime_kind: Set("codex".into()),
        native_thread_id: Set("native_thread".into()),
        status: Set(status.into()),
        input_mapping_json: Set("{}".into()),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
        ..Default::default()
    }
}

async fn history(store: &CrudStore) {
    // A large terminal prefix must not consume the active row budget.
    for batch in 0..64 {
        binding::Entity::insert_many(
            (0..128).map(|i| fixture(format!("history_{:05}", batch * 128 + i), "completed")),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cli_active_binding_pages_bound_ties_status_changes_and_restart() {
    let store = test_store_with_workspace("ws_active_page").await;
    history(&store).await;
    for status in [Starting, Running] {
        binding::Entity::insert_many(
            (0..145).map(|i| fixture(format!("{}_{i:03}", status.as_str()), status.as_str())),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    }
    let maintenance = store.with_maintenance_access();
    for status in [Starting, Running] {
        let first = maintenance
            .list_active_cli_runtime_turn_binding_page(status, None)
            .await
            .unwrap();
        assert_eq!(first.len(), 64);
        let after = (first[63].created_at, first[63].turn_id.clone());
        assert_eq!(first[0].turn_id, format!("{}_000", status.as_str()));
        let second = maintenance
            .list_active_cli_runtime_turn_binding_page(status, Some(&after))
            .await
            .unwrap();
        assert_eq!(second.len(), 64);
        assert_eq!(second[0].turn_id, format!("{}_064", status.as_str()));
        assert!(second.iter().all(|row| row.status == status.as_str()));
        let end = (second[63].created_at, second[63].turn_id.clone());
        let last = maintenance
            .list_active_cli_runtime_turn_binding_page(status, Some(&end))
            .await
            .unwrap();
        assert_eq!(last.len(), 17);
        assert_eq!(last[16].turn_id, format!("{}_144", status.as_str()));

        let mut behind = fixture(format!("{}_behind", status.as_str()), status.as_str());
        behind.created_at = Set(unix_to_datetime(1_699_999_999));
        binding::Entity::insert(behind)
            .exec(&store.database_connection())
            .await
            .unwrap();
        assert!(
            maintenance
                .list_active_cli_runtime_turn_binding_page(status, Some(&end))
                .await
                .unwrap()
                .iter()
                .all(|row| !row.turn_id.ends_with("behind"))
        );
        let restarted = CrudStore::new(store.database_connection()).with_maintenance_access();
        assert_eq!(
            restarted
                .list_active_cli_runtime_turn_binding_page(status, None)
                .await
                .unwrap()[0]
                .turn_id,
            format!("{}_behind", status.as_str())
        );
    }
    binding::Entity::update_many()
        .col_expr(binding::Column::Status, Expr::value("running"))
        .filter(binding::Column::TurnId.eq("starting_000"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let page = maintenance
        .list_active_cli_runtime_turn_binding_page(Starting, None)
        .await
        .unwrap();
    assert!(page.iter().all(|row| row.turn_id != "starting_000"));
    // Existing foreground semantics still include terminal rows with its filter.
    let foreground = store
        .list_cli_runtime_turn_bindings(CliRuntimeTurnBindingListFilter {
            workspace_id: Some("ws_active_page".into()),
            statuses: vec!["completed".into()],
            limit: Some(3),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(foreground.len(), 3);
}

#[tokio::test]
async fn cli_active_binding_plans_seek_both_statuses_and_empty_active_history() {
    let (store, queries) = test_store_with_workspace_and_query_counter("ws_active_page").await;
    history(&store).await;
    let maintenance = store.with_maintenance_access();
    queries.store(0, Ordering::Relaxed);
    for status in [Starting, Running] {
        assert!(
            maintenance
                .list_active_cli_runtime_turn_binding_page(status, None)
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        queries.load(Ordering::Relaxed),
        2,
        "idle is two empty seeks"
    );
    let after = (unix_to_datetime(1_700_000_000), "cursor".into());
    for status in [Starting, Running] {
        for cursor in [None, Some(&after)] {
            let statement =
                active_turn_binding_page_query(status, cursor).build(DatabaseBackend::Sqlite);
            assert!(
                !statement.sql.contains("OFFSET")
                    && !statement.sql.contains("JOIN")
                    && !statement.sql.contains(" IN ")
            );
            let plan = store
                .database_connection()
                .query_all_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    format!("EXPLAIN QUERY PLAN {}", statement.sql),
                    statement.values.unwrap(),
                ))
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.try_get::<String>("", "detail").unwrap())
                .collect::<Vec<_>>();
            assert!(
                plan.iter().any(|detail| detail.starts_with("SEARCH ")
                    && detail.contains("idx_cli_turn_binding_status_created_turn")
                    && detail.contains("status=?")),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|detail| detail.starts_with("SCAN ") || detail.contains("TEMP B-TREE")),
                "{plan:?}"
            );
            if cursor.is_some() {
                assert!(
                    plan.iter().any(|detail| detail.contains("created_at>?")
                        || detail.contains("(created_at,turn_id)>(?,?)")),
                    "keyset must be a range seek: {plan:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn cli_active_binding_discovery_uses_query_only_maintenance_reader_and_cancels() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteExecutor,
        sqlite_read_only_connection_url,
    };
    use sea_orm::{ConnectOptions, TransactionTrait};
    let path = OptionalDeliveryDatabasePath(std::env::temp_dir().join(format!(
        "pioneer-cli-active-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.0.display()));
    options.max_connections(1);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path.0));
    options.max_connections(2);
    options.map_sqlx_sqlite_opts(|opts| {
        opts.read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(OptionalDeliveryDatabaseObserver::default());
    let reads = Arc::new(ReadObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        reads.clone(),
    );
    database.validate_reader().await.unwrap();
    let interactive = CrudStore::new(database.clone());
    let maintenance = interactive.with_maintenance_access();
    // A held writer cannot block these SELECTs: they use the physical reader.
    let writer_transaction = database.with_critical_writes().begin().await.unwrap();
    observer.events.lock().unwrap().clear();
    reads.events.lock().unwrap().clear();
    for status in [Starting, Running] {
        tokio::time::timeout(
            Duration::from_secs(2),
            maintenance.list_active_cli_runtime_turn_binding_page(status, None),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(
        reads
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                SqliteReadEvent::OperationFinished { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SqliteReadClass::Maintenance; 2]
    );
    assert!(
        observer.events.lock().unwrap().is_empty(),
        "idle discovery must not enqueue writes"
    );
    writer_transaction.rollback().await.unwrap();

    // Cancellation while queued for the sole Maintenance reader releases its
    // registration; interactive access remains available and later work runs.
    let held_read = maintenance
        .database_connection()
        .begin_read()
        .await
        .unwrap();
    reads.events.lock().unwrap().clear();
    let cancelled = tokio::spawn({
        let store = maintenance.clone();
        async move {
            store
                .list_active_cli_runtime_turn_binding_page(Starting, None)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if reads.events.lock().unwrap().iter().any(|event| {
                matches!(
                    event,
                    SqliteReadEvent::AdmissionEnqueued {
                        queue_depth: 1,
                        active: 1,
                        ..
                    }
                )
            }) {
                break;
            }
            reads.enqueued.notified().await;
        }
    })
    .await
    .unwrap();
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(reads.events.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionCancelled {
            queue_depth: 0,
            active: 1,
            ..
        }
    )));
    tokio::time::timeout(
        Duration::from_secs(2),
        interactive.list_active_cli_runtime_turn_binding_page(Running, None),
    )
    .await
    .unwrap()
    .unwrap();
    held_read.rollback().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        maintenance.list_active_cli_runtime_turn_binding_page(Starting, None),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn cli_active_binding_index_retry_down_and_up_preserve_domain_and_workspace_index() {
    let store = test_store_with_workspace("ws_active_page").await;
    history(&store).await;
    let database = store.database_connection();
    let transaction = database.begin().await.unwrap();
    let manager = migration::SchemaManager::new(&*transaction);
    let index_migration = Migrator::migrations()
        .into_iter()
        .find(|m| m.name() == "m20261004_000001_cli_runtime_active_binding_index")
        .unwrap();
    // Retrying index creation is idempotent and does not rebuild domain data.
    index_migration.up(&manager).await.unwrap();
    index_migration.down(&manager).await.unwrap();
    index_migration.down(&manager).await.unwrap();
    assert_eq!(
        binding::Entity::find().count(&transaction).await.unwrap(),
        8192
    );
    let indexes = transaction
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_list('turn_cli_runtime_binding')",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "name").unwrap())
        .collect::<Vec<_>>();
    assert!(
        !indexes
            .iter()
            .any(|name| name == "idx_cli_turn_binding_status_created_turn")
    );
    assert!(
        indexes
            .iter()
            .any(|name| name == "idx_turn_cli_runtime_binding_workspace_status")
    );
    index_migration.up(&manager).await.unwrap();
    let columns = transaction
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_info('idx_cli_turn_binding_status_created_turn')",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "name").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(columns, ["status", "created_at", "turn_id"]);
    assert_eq!(
        binding::Entity::find().count(&transaction).await.unwrap(),
        8192
    );
    index_migration.down(&manager).await.unwrap();
    drop(manager);
    transaction.rollback().await.unwrap();
    let columns_after_rollback = database
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_info('idx_cli_turn_binding_status_created_turn')",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "name").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(columns_after_rollback, columns);
}

async fn terminal_guard_fixture() -> (CrudStore, crate::CliRuntimeTurnTerminalGuard) {
    let (store, thread, turn) =
        test_store_with_started_turn("ws_cli_guard", "thr_cli_guard", "turn_cli_guard").await;
    let timestamp = unix_to_datetime(1_700_000_000);
    store
        .prepare_cli_runtime_initial_turn_attempt(
            crate::NewCliRuntimeTurnBinding {
                turn_id: turn.id.clone(),
                thread_id: thread.id.clone(),
                continuation_thread_id: thread.id.clone(),
                workspace_id: thread.workspace_id.clone(),
                runtime_id: "codex".into(),
                runtime_kind: "codex".into(),
                native_thread_id: "native_thread".into(),
                native_turn_id: None,
                request_id: None,
                status: "starting".into(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".into(),
                created_at: timestamp,
                updated_at: timestamp,
            },
            "attempt_guard_1".into(),
            1,
        )
        .await
        .unwrap();
    store
        .update_turn_status(
            &thread.id,
            &turn.id,
            TurnStatus::Blocked,
            Some("blocked"),
            1_700_000_000,
        )
        .await
        .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&turn.id)
        .await
        .unwrap()
        .unwrap();
    let guard = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap()
        .unwrap();
    (store, guard)
}

async fn commit_guard(
    store: &CrudStore,
    guard: &crate::CliRuntimeTurnTerminalGuard,
) -> anyhow::Result<Option<crate::CliRuntimeTurnBindingRecord>> {
    store
        .terminalize_cli_runtime_turn_binding_guarded(
            &guard.binding.turn_id,
            "blocked",
            CliRuntimeTurnAttemptStatus::Interrupted,
            Some("old decision".into()),
            unix_to_datetime(1_700_000_000),
            Some(guard),
        )
        .await
}

#[tokio::test]
async fn cli_terminal_guard_rejects_starting_activation_terminal_and_canonical_resume() {
    for change in ["running", "terminal", "resume"] {
        let (store, guard) = terminal_guard_fixture().await;
        match change {
            "running" => {
                store
                    .activate_cli_runtime_turn_attempt(
                        &guard.binding.turn_id,
                        &guard.attempt.as_ref().unwrap().id,
                        "native_running",
                        None,
                        guard.binding.updated_at,
                    )
                    .await
                    .unwrap();
            }
            "terminal" => {
                store
                    .terminalize_cli_runtime_turn_binding(
                        &guard.binding.turn_id,
                        "completed",
                        CliRuntimeTurnAttemptStatus::Completed,
                        None,
                        guard.binding.updated_at,
                    )
                    .await
                    .unwrap();
            }
            "resume" => {
                store
                    .update_turn_status(
                        &guard.binding.thread_id,
                        &guard.binding.turn_id,
                        TurnStatus::InProgress,
                        None,
                        1_700_000_000,
                    )
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let current = store
            .get_cli_runtime_turn_binding(&guard.binding.turn_id)
            .await
            .unwrap();
        let attempt = store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap();
        assert!(commit_guard(&store, &guard).await.unwrap().is_none());
        assert_eq!(
            store
                .get_cli_runtime_turn_binding(&guard.binding.turn_id)
                .await
                .unwrap(),
            current
        );
        assert_eq!(
            store
                .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
                .await
                .unwrap(),
            attempt
        );
    }
}

#[tokio::test]
async fn cli_terminal_guard_rejects_same_timestamp_status_aba_with_new_recovery_attempt() {
    let (store, guard) = terminal_guard_fixture().await;
    let (_, next) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            &guard.binding.turn_id,
            "attempt_guard_2".into(),
            "recovery_job_2".into(),
            "recovery_attempt_2".into(),
            2,
            "resume".into(),
            guard.binding.updated_at,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        guard.binding
    );
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    assert_eq!(next.status, CliRuntimeTurnAttemptStatus::Starting);
}

#[tokio::test]
async fn cli_terminal_guard_repairs_active_projection_atomically_and_rolls_back_on_projection_error()
 {
    let (store, guard) = terminal_guard_fixture().await;
    // The same project fixture uses a failing projection write to prove that
    // binding and attempt mutations do not survive an error later in the txn.
    let before = pioneer_entity::turn_work_projection::Entity::find_by_id(&guard.binding.turn_id)
        .one(&store.database_connection())
        .await
        .unwrap();
    assert!(
        before.is_some(),
        "rollback fixture requires an existing projection"
    );
    store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_cli_guard_projection BEFORE UPDATE ON turn_work_projection BEGIN SELECT RAISE(ABORT, 'guard projection failure'); END",
    ).await.unwrap();
    assert!(commit_guard(&store, &guard).await.is_err());
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        guard.binding
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap(),
        guard.attempt
    );
    assert_eq!(
        pioneer_entity::turn_work_projection::Entity::find_by_id(&guard.binding.turn_id)
            .one(&store.database_connection())
            .await
            .unwrap(),
        before
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_cli_guard_projection")
        .await
        .unwrap();
    let result = commit_guard(&store, &guard).await.unwrap().unwrap();
    assert_eq!(result.status, "blocked");
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Interrupted
    );
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
}

#[tokio::test]
async fn cli_terminal_guard_rejects_reacquired_execution_owner_at_equal_timestamp() {
    let (store, guard) = terminal_guard_fixture().await;
    let db = store.database_connection();
    let owner = crate::repositories::turn_execution::insert_immutable(
        &db,
        crate::NewTurnExecution {
            turn_id: guard.binding.turn_id.clone(),
            thread_id: guard.binding.thread_id.clone(),
            workspace_id: guard.binding.workspace_id.clone(),
            executor_kind: crate::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: crate::TurnExecutionStatus::Starting,
            owner_id: "owner".into(),
            lease_until: guard.binding.updated_at + chrono::Duration::seconds(60),
            created_at: guard.binding.created_at,
        },
    )
    .await
    .unwrap();
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::Blocked,
            Some("blocked"),
            1_700_000_000,
        )
        .await
        .unwrap();
    let guard = store
        .cli_runtime_turn_terminal_guard(&guard.binding)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::repositories::turn_execution::reacquire_blocked(
            &db,
            &owner.turn_id,
            &owner.owner_id,
            guard.binding.updated_at,
            guard.binding.updated_at,
        )
        .await
        .unwrap()
    );
    // The canonical status and binding are unchanged; the existing ownership
    // generation alone fences this otherwise identical terminal decision.
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
    let new_owner = store
        .get_turn_execution(&owner.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_owner.owner_id, owner.owner_id);
    assert_eq!(new_owner.owner_generation, owner.owner_generation + 1);
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&owner.turn_id)
            .await
            .unwrap(),
        guard.attempt
    );
}

#[tokio::test]
async fn cli_terminal_guard_accepts_completed_attempt_after_its_own_starting_running_transitions() {
    let (store, guard) = terminal_guard_fixture().await;
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::InProgress,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let (running, attempt) = store
        .activate_cli_runtime_turn_attempt(
            &guard.binding.turn_id,
            &guard.attempt.as_ref().unwrap().id,
            "valid-native",
            None,
            guard.binding.updated_at,
        )
        .await
        .unwrap();
    assert!(
        store
            .mark_cli_runtime_turn_attempt_terminal(
                &attempt.id,
                CliRuntimeTurnAttemptStatus::Completed,
                None,
                guard.binding.updated_at,
            )
            .await
            .unwrap()
    );
    store
        .update_turn_status(
            &running.thread_id,
            &running.turn_id,
            TurnStatus::Completed,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let completed_guard = store
        .cli_runtime_turn_terminal_guard(&running)
        .await
        .unwrap()
        .unwrap();
    let binding = store
        .terminalize_cli_runtime_turn_binding_guarded(
            &running.turn_id,
            "completed",
            CliRuntimeTurnAttemptStatus::Completed,
            None,
            guard.binding.updated_at,
            Some(&completed_guard),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.status, "completed");
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Completed
    );
}

async fn blocked_source_fixture() -> (
    CrudStore,
    crate::CliRuntimeTurnTerminalGuard,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
) {
    let (store, old) = terminal_guard_fixture().await;
    let timestamp = old.binding.created_at;
    store
        .update_turn_status(
            &old.binding.thread_id,
            &old.binding.turn_id,
            TurnStatus::InProgress,
            None,
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    store
        .activate_cli_runtime_turn_attempt(
            &old.binding.turn_id,
            &old.attempt.unwrap().id,
            "blocked-source-A",
            None,
            timestamp,
        )
        .await
        .unwrap();
    store
        .register_cli_runtime_execution_segment(
            &old.binding.turn_id,
            &old.binding.native_thread_id,
            "blocked-source-A",
            timestamp,
        )
        .await
        .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&old.binding.turn_id)
        .await
        .unwrap()
        .unwrap();
    let snapshot = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap()
        .unwrap();
    let guard = store
        .cli_runtime_blocked_turn_guard(&binding, "blocked-source-A")
        .await
        .unwrap()
        .unwrap();
    (store, snapshot, guard)
}

async fn commit_blocked_source(
    store: &CrudStore,
    guard: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
) -> anyhow::Result<bool> {
    let (_, turn) = store
        .get_turn(&guard.thread_id, &guard.turn_id)
        .await?
        .unwrap();
    store
        .materialize_cli_runtime_blocked_turn_guarded(
            pioneer_protocol::TurnBlockedNotification {
                workspace_id: guard.workspace_id.clone(),
                thread_id: guard.thread_id.clone(),
                turn: Turn {
                    status: TurnStatus::Blocked,
                    error: Some("old blocked source".into()),
                    ..turn
                },
                resume: None,
            },
            guard,
            "owner",
            1_700_000_000,
        )
        .await
}

#[tokio::test]
async fn cli_blocked_atomic_source_guard_rejects_new_attempt_and_changed_recovery_authority() {
    let (store, snapshot, old_guard) = blocked_source_fixture().await;
    let timestamp = snapshot.binding.created_at;
    let job = store
        .enqueue_recovery_job(
            snapshot.binding.turn_id.clone(),
            "blocked-source".into(),
            TurnItemType::SystemEvent,
            None,
            RecoveryTrigger::Timeout,
            RecoveryAction::OpenNextExecutionWindow,
            Some("recovery".into()),
            None,
            None,
            None,
            0,
            2,
            serde_json::json!({}),
            serde_json::json!({}),
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    let claim = store
        .claim_due_recovery_jobs(timestamp.timestamp(), 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claim.id, job.id);
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claim.claim_token.as_deref().unwrap(),
                "blocked-recovery-owner",
                timestamp.timestamp()
            )
            .await
            .unwrap(),
        crate::ClaimedRecoveryActivation::Activated
    ));
    let (_, next) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            &snapshot.binding.turn_id,
            "blocked-attempt-B".into(),
            job.id.clone(),
            "blocked-recovery-owner".into(),
            2,
            "next execution".into(),
            timestamp,
        )
        .await
        .unwrap();
    let (binding, next) = store
        .activate_cli_runtime_turn_attempt(
            &snapshot.binding.turn_id,
            &next.id,
            "blocked-source-B",
            None,
            timestamp,
        )
        .await
        .unwrap();
    let (_, _, segment) = store
        .register_cli_runtime_execution_segment(
            &snapshot.binding.turn_id,
            &snapshot.binding.native_thread_id,
            "blocked-source-B",
            timestamp,
        )
        .await
        .unwrap();
    assert!(!commit_blocked_source(&store, &old_guard).await.unwrap());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    let fresh = store
        .cli_runtime_blocked_turn_guard(&binding, "blocked-source-B")
        .await
        .unwrap()
        .unwrap();
    // Same binding/attempt/segment values and timestamps; only the recovery
    // authority changed between capture and writer validation.
    store
        .mark_recovery_job_terminal(
            &job.id,
            RecoveryJobStatus::Blocked,
            Some("authority ended".into()),
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    assert!(!commit_blocked_source(&store, &fresh).await.unwrap());
    let current = store
        .resolve_cli_runtime_native_turn_owner("codex", "blocked-source-B")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.binding, binding);
    assert_eq!(current.attempt, next);
    assert_eq!(current.segment.unwrap(), segment);
    assert_eq!(
        store
            .get_turn(&binding.thread_id, &binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
}

#[tokio::test]
async fn cli_blocked_atomic_source_guard_rejects_execution_generation_aba_at_equal_timestamp() {
    let (store, snapshot, _) = blocked_source_fixture().await;
    let binding = &snapshot.binding;
    let timestamp = binding.created_at;
    let owner = crate::repositories::turn_execution::insert_immutable(
        &store.database_connection(),
        crate::NewTurnExecution {
            turn_id: binding.turn_id.clone(),
            thread_id: binding.thread_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            executor_kind: crate::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: crate::TurnExecutionStatus::Starting,
            owner_id: "owner".into(),
            lease_until: timestamp + chrono::Duration::seconds(60),
            created_at: timestamp,
        },
    )
    .await
    .unwrap();
    let guard = store
        .cli_runtime_blocked_turn_guard(binding, "blocked-source-A")
        .await
        .unwrap()
        .unwrap();
    crate::repositories::turn_execution::mark_terminal(
        &store.database_connection(),
        &binding.turn_id,
        crate::TurnExecutionStatus::Blocked,
        timestamp,
    )
    .await
    .unwrap();
    assert!(
        crate::repositories::turn_execution::reacquire_blocked(
            &store.database_connection(),
            &binding.turn_id,
            &owner.owner_id,
            timestamp,
            timestamp
        )
        .await
        .unwrap()
    );
    assert!(!commit_blocked_source(&store, &guard).await.unwrap());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap(),
        snapshot.attempt
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        *binding
    );
    assert_eq!(
        store
            .get_turn(&binding.thread_id, &binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(
        store
            .get_turn_execution(&binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .owner_generation,
        owner.owner_generation + 1
    );
}

async fn native_delivery_fixture() -> (
    CrudStore,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
    crate::NewCliRuntimeNativeEvent,
) {
    let (store, guard) = terminal_guard_fixture().await;
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::InProgress,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let (binding, _) = store
        .activate_cli_runtime_turn_attempt(
            &guard.binding.turn_id,
            &guard.attempt.unwrap().id,
            "native-delivery-A",
            None,
            guard.binding.updated_at,
        )
        .await
        .unwrap();
    let source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    let event = crate::NewCliRuntimeNativeEvent {
        id: "native-terminal-delivery-fixture".into(),
        runtime_id: source.runtime_id.clone(),
        runtime_kind: source.runtime_kind.clone(),
        turn_id: Some(source.turn_id.clone()),
        thread_id: Some(source.thread_id.clone()),
        workspace_id: Some(source.workspace_id.clone()),
        native_thread_id: Some(source.native_thread_id.clone()),
        native_turn_id: Some("native-delivery-A".into()),
        native_method: "gateway/terminal_delivery".into(),
        payload_redacted_json: "{\"outcome\":\"failed\"}".into(),
        sequence: 1,
        created_at: binding.updated_at,
    };
    (store.with_maintenance_access(), source, event)
}

#[tokio::test]
async fn cli_native_terminal_source_write_preserves_first_outcome_and_rolls_back_on_storage_failure()
 {
    let (store, source, event) = native_delivery_fixture().await;
    let binding = store
        .get_cli_runtime_turn_binding(&source.turn_id)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap();
    store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_native_terminal_delivery BEFORE INSERT ON cli_runtime_native_event WHEN NEW.native_method = 'gateway/terminal_delivery' BEGIN SELECT RAISE(ABORT, 'injected delivery failure'); END"
    ).await.unwrap();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event.clone(), &source)
            .await
            .is_err()
    );
    assert!(
        store
            .get_cli_runtime_native_event(&event.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        before
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_native_terminal_delivery")
        .await
        .unwrap();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event.clone(), &source)
            .await
            .unwrap()
    );
    let mut later = event.clone();
    later.payload_redacted_json = "{\"outcome\":\"different\"}".into();
    assert!(
        store
            .persist_cli_runtime_terminal_event(later, &source)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .get_cli_runtime_native_event(&event.id)
            .await
            .unwrap()
            .unwrap()
            .payload_redacted_json,
        event.payload_redacted_json,
        "a retransmission cannot replace the accepted native outcome"
    );
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn cli_native_terminal_source_write_rejects_owner_aba_even_with_same_binding_and_timestamp() {
    let (store, _, event) = native_delivery_fixture().await;
    let timestamp = event.created_at;
    let db = store.database_connection();
    let owner = crate::repositories::turn_execution::insert_immutable(
        &db,
        crate::NewTurnExecution {
            turn_id: event.turn_id.clone().unwrap(),
            thread_id: event.thread_id.clone().unwrap(),
            workspace_id: event.workspace_id.clone().unwrap(),
            executor_kind: crate::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: crate::TurnExecutionStatus::Blocked,
            owner_id: "native-delivery-owner".into(),
            lease_until: timestamp,
            created_at: timestamp,
        },
    )
    .await
    .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&owner.turn_id)
        .await
        .unwrap()
        .unwrap();
    let source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::repositories::turn_execution::reacquire_blocked(
            &db,
            &owner.turn_id,
            &owner.owner_id,
            timestamp,
            timestamp
        )
        .await
        .unwrap()
    );
    assert!(
        !store
            .persist_cli_runtime_terminal_event(event.clone(), &source)
            .await
            .unwrap()
    );
    assert!(
        store
            .get_cli_runtime_native_event(&event.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&owner.turn_id)
            .await
            .unwrap()
            .unwrap(),
        binding
    );
    assert!(
        store
            .latest_cli_runtime_turn_attempt(&owner.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_active()
    );
}
