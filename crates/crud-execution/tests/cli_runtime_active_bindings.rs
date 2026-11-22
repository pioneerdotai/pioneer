use pioneer_crud_execution::cli_runtime_binding::CliRuntimeActiveTurnBindingStatus::{
    Running, Starting,
};
use pioneer_crud_execution::cli_runtime_binding::{
    CliRuntimeTurnBindingListFilter, active_turn_binding_page_query,
};
use pioneer_crud_execution::cli_runtime_store::CliRuntimeStore;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_entity::{turn_cli_runtime_binding as binding, workspace};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait,
    QueryFilter, QueryTrait, Set, Statement, TransactionTrait,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct WriteObserver {
    events: Mutex<Vec<pioneer_postgres_pools::PostgresWriteEvent>>,
}

impl pioneer_postgres_pools::PostgresWriteObserver for WriteObserver {
    fn observe(&self, event: pioneer_postgres_pools::PostgresWriteEvent) {
        self.events.lock().unwrap().push(event);
    }
}

async fn seed_workspace(db: &impl ConnectionTrait, id: &str) {
    let timestamp = unix_to_datetime(1_700_000_000);
    workspace::Entity::insert(workspace::ActiveModel {
        id: Set(id.to_owned()),
        name: Set("Test Workspace".to_owned()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(db)
    .await
    .expect("seed active CLI binding workspace");
}

async fn postgres_store_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> CliRuntimeStore {
    seed_workspace(fixture.connection(), workspace_id).await;
    let reader = fixture.connect_read_only().await.unwrap();
    CliRuntimeStore::new(PostgresDatabase::new(reader, fixture.connection().clone()))
}

async fn test_store_with_workspace(workspace_id: &str) -> CliRuntimeStore {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, workspace_id).await
}

async fn test_store_with_workspace_and_query_counter(
    workspace_id: &str,
) -> (CliRuntimeStore, Arc<AtomicU64>) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let mut writer = fixture.connection().clone();
    seed_workspace(&writer, workspace_id).await;
    let mut reader = fixture.connect_read_only().await.unwrap();
    let query_count = Arc::new(AtomicU64::new(0));
    for connection in [&mut writer, &mut reader] {
        let query_count = query_count.clone();
        connection.set_metric_callback(move |_| {
            query_count.fetch_add(1, Ordering::Relaxed);
        });
    }
    (
        CliRuntimeStore::new(PostgresDatabase::new(reader, writer)),
        query_count,
    )
}

#[derive(Default)]
struct ReadObserver {
    events: std::sync::Mutex<Vec<pioneer_postgres_pools::PostgresReadEvent>>,
    enqueued: tokio::sync::Notify,
}

impl pioneer_postgres_pools::PostgresReadObserver for ReadObserver {
    fn observe(&self, event: pioneer_postgres_pools::PostgresReadEvent) {
        self.events.lock().unwrap().push(event);
        if matches!(
            event,
            pioneer_postgres_pools::PostgresReadEvent::AdmissionEnqueued { .. }
        ) {
            self.enqueued.notify_one();
        }
    }
}

fn binding_fixture(id: String, status: &str) -> binding::ActiveModel {
    let timestamp = unix_to_datetime(1_700_000_000);
    let mut row = binding::ActiveModel {
        turn_id: Set(id),
        thread_id: Set("thr_active_page".into()),
        continuation_thread_id: Set("thr_active_page".into()),
        workspace_id: Set("ws_active_page".into()),
        runtime_id: Set("codex".into()),
        runtime_kind: Set("codex".into()),
        native_thread_id: Set("native_thread".into()),
        status: Set(status.into()),
        frozen_manifest_id: Set(None),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
        ..Default::default()
    };
    pioneer_crud_execution::input_mapping::InputMapping::prepare("{}", false)
        .unwrap()
        .set(&mut row)
        .unwrap();
    row
}

async fn history(store: &CliRuntimeStore) {
    // A large terminal prefix must not consume the active row budget.
    for batch in 0..64 {
        binding::Entity::insert_many(
            (0..128)
                .map(|i| binding_fixture(format!("history_{:05}", batch * 128 + i), "completed")),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cli_active_binding_pages_bound_ties_status_changes_and_restart() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_active_page").await;
        history(&store).await;
        for status in [Starting, Running] {
            binding::Entity::insert_many(
                (0..145).map(|i| {
                    binding_fixture(format!("{}_{i:03}", status.as_str()), status.as_str())
                }),
            )
            .exec(&store.database_connection())
            .await
            .unwrap();
        }
        let maintenance = store.maintenance();
        for status in [Starting, Running] {
            let through = maintenance
                .active_cli_runtime_turn_binding_frontier(status)
                .await
                .unwrap()
                .unwrap();
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

            // New suffix pages must not postpone EOF, while an ordinary fresh
            // pass sees them. The captured frontier remains inclusive.
            binding::Entity::insert_many((0..65).map(|i| {
                let mut row = binding_fixture(
                    format!("{}_suffix_{i:03}", status.as_str()),
                    status.as_str(),
                );
                row.created_at = Set(unix_to_datetime(1_700_000_001));
                row
            }))
            .exec(&store.database_connection())
            .await
            .unwrap();
            let bounded = maintenance
                .list_active_cli_runtime_turn_binding_page_through(status, Some(&end), &through)
                .await
                .unwrap();
            assert_eq!(bounded.len(), 17);
            let boundary = (bounded[16].created_at, bounded[16].turn_id.clone());
            assert!(
                maintenance
                    .list_active_cli_runtime_turn_binding_page_through(
                        status,
                        Some(&boundary),
                        &through,
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                maintenance
                    .list_active_cli_runtime_turn_binding_page(status, Some(&boundary),)
                    .await
                    .unwrap()
                    .len(),
                64
            );

            let mut behind =
                binding_fixture(format!("{}_behind", status.as_str()), status.as_str());
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
            let restarted = CliRuntimeStore::new(store.database_connection()).maintenance();
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
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn cli_active_binding_plans_seek_both_statuses_and_empty_active_history() {
    let (store, queries) = test_store_with_workspace_and_query_counter("ws_active_page").await;
    history(&store).await;
    store
        .database_connection()
        .execute_unprepared("ANALYZE turn_cli_runtime_binding")
        .await
        .unwrap();

    let maintenance = store.maintenance();
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
                active_turn_binding_page_query(status, cursor).build(DatabaseBackend::Postgres);
            assert!(
                !statement.sql.contains("OFFSET")
                    && !statement.sql.contains("JOIN")
                    && !statement.sql.contains(" IN ")
            );
            let plan = store
                .database_connection()
                .query_all_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    format!("EXPLAIN (COSTS OFF) {}", statement.sql),
                    statement.values.unwrap(),
                ))
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
                .collect::<Vec<_>>();
            assert!(
                plan.iter().any(|detail| detail.contains("Index Scan")
                    && detail.contains("idx_cli_turn_binding_status_created_turn")
                    && plan
                        .iter()
                        .any(|line| line.contains("Index Cond:") && line.contains("status"))),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|detail| detail.contains("Seq Scan") || detail.contains("Sort")),
                "{plan:?}"
            );
            if cursor.is_some() {
                assert!(
                    plan.iter().any(|detail| detail.contains("Index Cond:")
                        && detail.contains("created_at")
                        && detail.contains(">")),
                    "keyset must be a range seek: {plan:?}"
                );
            }
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn cli_active_binding_discovery_uses_read_only_maintenance_reader_and_cancels() {
    use pioneer_postgres_pools::{
        PostgresDatabase, PostgresReadClass, PostgresReadEvent, PostgresWriteExecutor,
    };
    use sea_orm::TransactionTrait;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(WriteObserver::default());
    let reads = Arc::new(ReadObserver::default());
    let database = PostgresDatabase::from_executor_with_read_observer(
        reader,
        PostgresWriteExecutor::with_observer(writer, observer.clone()),
        reads.clone(),
    );
    database.validate_reader().await.unwrap();
    let interactive = CliRuntimeStore::new(database.clone());
    let maintenance = interactive.maintenance();
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
                PostgresReadEvent::OperationFinished { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![PostgresReadClass::Maintenance; 2]
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
                    PostgresReadEvent::AdmissionEnqueued {
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
        PostgresReadEvent::AdmissionCancelled {
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

#[pioneer_postgres_test::postgres_test]
async fn cli_active_binding_index_rollback_preserves_domain_and_workspace_index() {
    let store = test_store_with_workspace("ws_active_page").await;
    history(&store).await;
    let database = store.database_connection();
    let transaction = database.begin().await.unwrap();
    let columns = |row: sea_orm::QueryResult| {
        ["status", "created_at", "turn_id"].map(|name| row.try_get::<String>("", name).unwrap())
    };
    let definition = || {
        Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_get_indexdef('idx_cli_turn_binding_status_created_turn'::regclass,1,true) AS status,                 pg_get_indexdef('idx_cli_turn_binding_status_created_turn'::regclass,2,true) AS created_at,                 pg_get_indexdef('idx_cli_turn_binding_status_created_turn'::regclass,3,true) AS turn_id".to_owned())
    };
    assert_eq!(
        columns(
            transaction
                .query_one_raw(definition())
                .await
                .unwrap()
                .unwrap()
        ),
        ["status", "created_at", "turn_id"]
    );
    transaction
        .execute_unprepared("DROP INDEX idx_cli_turn_binding_status_created_turn")
        .await
        .unwrap();
    let indexes = transaction.query_all_raw(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename='turn_cli_runtime_binding'".to_owned()))
        .await.unwrap().into_iter().map(|row| row.try_get::<String>("", "indexname").unwrap()).collect::<Vec<_>>();
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
    assert_eq!(
        binding::Entity::find().count(&transaction).await.unwrap(),
        8192
    );
    transaction.rollback().await.unwrap();
    assert_eq!(
        columns(database.query_one_raw(definition()).await.unwrap().unwrap()),
        ["status", "created_at", "turn_id"]
    );
    assert_eq!(
        binding::Entity::find().count(&database).await.unwrap(),
        8192
    );
}
