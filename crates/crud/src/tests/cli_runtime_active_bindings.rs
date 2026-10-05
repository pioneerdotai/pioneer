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
