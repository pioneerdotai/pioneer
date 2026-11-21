//! Recovery boundaries that only update durable compaction lifecycle state.
use super::*;
use crate::lifecycle_pending::CompactionLifecycleClaim;
use crate::store::CompactionStore;
use pioneer_compaction::ModelBudget;
use pioneer_entity::compaction_lifecycle_pending as pending;
use pioneer_postgres_test::TestDatabase;
use pioneer_sqlite::{SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{DbBackend, Statement};
use std::{sync::Arc, time::Duration};

const NOW: i64 = 2_000_000_000_000;

async fn open(fixture: &TestDatabase) -> (CompactionStore, SqliteWriteExecutor) {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let executor = SqliteWriteExecutor::new(writer);
    let reader = fixture.connect_read_only().await.unwrap();
    let db = SqliteDatabase::from_executor(reader, executor.clone());
    assert!(db.reader_query_only_enabled().await.unwrap());
    (CompactionStore::for_test(db), executor)
}

async fn fixture() -> (Arc<TestDatabase>, CompactionStore, SqliteWriteExecutor) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let (store, writer) = open(&fixture).await;
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES ('owner','ws','thread',1)",
    ] {
        store
            .with_maintenance_access()
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    (fixture, store, writer)
}

async fn operation(store: &CompactionStore, id: &str, status: &str, deadline: i64) {
    store.with_maintenance_access().database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms) VALUES($1,'owner',$2,$3, '{}','turn',$4)",
        [id.into(),id.into(),status.into(),deadline.into()])).await.unwrap();
}

async fn state(store: &CompactionStore, id: &str) -> RunnerState {
    let state = RunnerState::new(
        NOW as u64,
        &ModelBudget::new(Some(128000), None, Some(16384)),
        1000,
        None,
    )
    .unwrap();
    store
        .with_maintenance_access()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES ($1,$2,$3::text::json)",
            [
                id.into(),
                (state.generation as i64).into(),
                serde_json::to_string(&state).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    state
}

async fn row(store: &CompactionStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id)
        .one(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
}

async fn scalar(store: &CompactionStore, sql: &str) -> i64 {
    store
        .with_maintenance_access()
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn claim(store: &CompactionStore, id: &str, now: i64) -> CompactionLifecycleClaim {
    let candidate = row(store, id).await.unwrap();
    store
        .compaction_claim_lifecycle(&candidate, &|| now)
        .await
        .unwrap()
        .unwrap()
}

async fn apply(store: &CompactionStore, id: &str, now: i64) -> bool {
    let claimed = claim(store, id, now).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, now)
        .await
        .unwrap()
        .unwrap();
    store
        .with_maintenance_access()
        .repair_lifecycle_without_publication(prepared)
        .await
        .unwrap()
        .0
}

async fn expand_all(store: &CompactionStore, now: i64) {
    for _ in 0..100 {
        let seed = store
            .compaction_expand_lifecycle_scope(true, &|| now)
            .await
            .unwrap();
        let scope = store
            .compaction_expand_lifecycle_scope(false, &|| now)
            .await
            .unwrap();
        assert!(seed + scope <= 8);
        if seed + scope == 0 {
            return;
        }
    }
    panic!("bounded fixture scopes must drain");
}
#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.reads.lock().unwrap().push(event);
    }
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        if matches!(
            event,
            pioneer_sqlite::SqliteWriteEvent::Enqueued {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        ) {
            self.queued.notify_one();
        }
        self.writes.lock().unwrap().push(event);
    }
}

async fn observed_store(fixture: &TestDatabase, routes: Arc<Routes>) -> CompactionStore {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    CompactionStore::for_test(SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes,
    ))
}
#[pioneer_postgres_test::postgres_test]
async fn lifecycle_deadline_units_stop_future_deadline_and_missing_runner() {
    let (_file, store, _) = fixture().await;
    operation(&store, "deadline", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    assert!(apply(&store, "deadline", NOW).await); // scope evaluation restores deadline
    let future = row(&store, "deadline").await.unwrap();
    assert_eq!(future.due_at, NOW + 60_000);
    assert_eq!(future.retry_not_before, 0);
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_claim_lifecycle(&future, &|| NOW)
            .await
            .unwrap()
            .is_none()
    );
    assert!(apply(&store, "deadline", NOW + 60_000).await);
    assert_eq!(scalar(&store,"SELECT count(*) n FROM compaction_operation WHERE id='deadline' AND status='failed' AND outcome='deadline'").await,1);
    assert!(
        row(&store, "deadline").await.is_some(),
        "finish keeps terminal debt"
    );
    assert!(apply(&store, "deadline", NOW + 60_000).await);
    assert!(
        row(&store, "deadline").await.is_none(),
        "terminal without runner follows old predicate"
    );
    operation(&store, "stop", "running", NOW + 600_000).await;
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "stop", NOW).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE id='stop' AND status='cancelled'"
        )
        .await,
        1
    );
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    assert!(apply(&store, "stop", NOW).await);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_source_refresh_preserves_real_error_backoff_not_old_deadline() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "running", NOW + 600_000).await;
    expand_all(&store, NOW).await;
    let claimed = claim(&store, "op", NOW).await; // pretend preparation was interrupted
    let reserved = row(&store, "op").await.unwrap();
    assert_eq!(reserved.retry_not_before, NOW + 5_000);
    assert!(reserved.claim_token.is_some());
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    let refreshed = row(&store, "op").await.unwrap();
    assert!(refreshed.generation > reserved.generation);
    assert_eq!(refreshed.eligible_at, 0);
    assert_eq!(refreshed.due_at, NOW + 5_000);
    assert_eq!(refreshed.attempts, 1);
    assert!(refreshed.claim_token.is_none());
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='cancelled'"
        )
        .await,
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_running_missing_binding_retries_without_creating_sources() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "running", NOW).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    let claimed = claim(&store, "op", NOW).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .is_err()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn").await, 0);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='running'"
        )
        .await,
        1
    );
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 5_000);
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    expand_all(&store, NOW).await;
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='failed'"
        )
        .await,
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_maintenance_routes_clock_after_writer_and_cancellation_preserve_claim() {
    use pioneer_postgres_test::TestDatabase;
    use pioneer_sqlite::{SqliteReadClass, SqliteReadEvent, SqliteWriteEvent};
    let (file, store, writer) = fixture().await;
    operation(&store, "op", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    drop(store);
    drop(writer);
    let routes = Arc::new(Routes::default());
    let store = observed_store(&file, routes.clone()).await;
    let candidate = store.compaction_due_lifecycle(NOW).await.unwrap().remove(0);
    assert!(
        routes.writes.lock().unwrap().is_empty(),
        "discovery uses physical reader"
    );
    assert!(routes.reads.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(routes.reads.lock().unwrap().iter().all(|e| !matches!(
        e,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Interactive,
            ..
        }
    )));
    let hold = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    routes.queued.notified().await; // consume the holder's enqueue
    let time = Arc::new(std::sync::atomic::AtomicI64::new(NOW));
    let worker = store.clone();
    let c = candidate.clone();
    let t = time.clone();
    let waiting = tokio::spawn(async move {
        worker
            .compaction_claim_lifecycle(&c, &|| t.load(std::sync::atomic::Ordering::SeqCst))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    assert!(
        pioneer_crud_conversations::turn_reads::get_turn_for_thread(
            &store.database_connection(),
            "thread",
            "turn"
        )
        .await
        .unwrap()
        .is_some(),
        "interactive reader remains available while writer waits"
    );
    time.store(NOW + 30_000, std::sync::atomic::Ordering::SeqCst);
    hold.rollback().await.unwrap();
    let claimed = waiting.await.unwrap().unwrap().unwrap();
    assert_eq!(
        row(&store, "op").await.unwrap().retry_not_before,
        NOW + 35_000
    );
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 30_000)
        .await
        .unwrap()
        .unwrap();
    let hold = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    // Consume any queued notifications left by earlier successful work.
    while tokio::time::timeout(Duration::from_millis(1), routes.queued.notified())
        .await
        .is_ok()
    {}
    let worker = store.clone();
    let waiting = tokio::spawn(async move {
        worker
            .with_maintenance_access()
            .repair_lifecycle_without_publication(prepared)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    let after = row(&store, "op").await.unwrap();
    assert_eq!(after.retry_not_before, NOW + 35_000);
    assert!(after.claim_token.is_some());
    assert!(
        store
            .compaction_due_lifecycle(NOW + 30_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e|matches!(e,SqliteWriteEvent::Cancelled{class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
    assert!(routes.writes.lock().unwrap().iter().all(|e|!matches!(e,SqliteWriteEvent::Acquired{class,..} if *class!=SqliteWriteClass::Maintenance)));
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
    // Caller disappearance/panic after claim leaves the same durable retry.
    let worker = store.clone();
    let crash = tokio::spawn(async move {
        let _ = worker;
        panic!("owned candidate interrupted after claim")
    });
    assert!(crash.await.unwrap_err().is_panic());
    assert_eq!(row(&store, "op").await.unwrap(), after);
    assert!(apply(&store, "op", NOW + 35_000).await);
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 60_000);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_retry_policy_saturates_and_unchanged_source_values_do_not_refresh() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared(
        "UPDATE compaction_runner_state SET state=to_json('broken'::text) WHERE operation_id='op'",
    )
    .await
    .unwrap();
    let mut now = NOW;
    for (index, expected) in [
        5_000, 10_000, 20_000, 40_000, 80_000, 160_000, 300_000, 300_000, 300_000, 300_000,
        300_000, 300_000, 300_000, 300_000, 300_000, 300_000, 300_000,
    ]
    .into_iter()
    .enumerate()
    {
        let claimed = claim(&store, "op", now).await;
        assert!(
            store
                .compaction_prepare_lifecycle(&claimed, now)
                .await
                .is_err()
        );
        let before = row(&store, "op").await.unwrap();
        assert_eq!(before.attempts, (index as i64 + 1).min(16));
        assert_eq!(before.retry_not_before, now + expected);
        db.execute_unprepared("UPDATE compaction_runner_state SET generation=generation,state=state WHERE operation_id='op'").await.unwrap();
        db.execute_unprepared(
            "UPDATE compaction_operation SET status=status,deadline_ms=deadline_ms WHERE id='op'",
        )
        .await
        .unwrap();
        assert_eq!(row(&store, "op").await.unwrap(), before);
        now = before.due_at;
    }
}

async fn admit_stale_repair(
    store: &CompactionStore,
    mut prepared: PreparedCompactionLifecycle,
) -> anyhow::Result<bool> {
    let tx = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await?;
    // Only an admitted repair can append an event or acknowledge the claim.
    let admitted = prepared.repair_in_connection(&tx).await?.is_some();
    tx.rollback().await?;
    Ok(admitted)
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_fresh_values_fence_repeated_generation_and_concurrent_apply() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    let state = state(&store, "op").await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let mut changed = state.clone();
    changed.attempts += 1;
    store
        .with_maintenance_access()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE compaction_runner_state SET state=$1::text::json WHERE operation_id='op'",
            [serde_json::to_string(&changed).unwrap().into()],
        ))
        .await
        .unwrap();
    assert!(!admit_stale_repair(&store, prepared).await.unwrap());
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    assert!(row(&store, "op").await.unwrap().claim_token.is_none());
    // A concurrent successful domain apply changes operation/runner atomically.
    // This test exercises stale repair; real successful-apply coverage lives in
    // compaction.rs with its full manifest/checkpoint fixture.
    let claimed = claim(&store, "op", NOW + 5_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
        .await
        .unwrap()
        .unwrap();
    store
        .with_maintenance_access()
        .database_connection()
        .execute_unprepared(
            "UPDATE compaction_operation SET status='completed',outcome='applied' WHERE id='op'",
        )
        .await
        .unwrap();
    assert!(!admit_stale_repair(&store, prepared).await.unwrap());
    assert!(row(&store, "op").await.is_some());
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_claim_token_fences_retry_with_unchanged_source_generation() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let candidate = row(&store, "op").await.unwrap();
    let clock = || NOW;
    let (first, second) = tokio::join!(
        store.compaction_claim_lifecycle(&candidate, &clock),
        store.compaction_claim_lifecycle(&candidate, &clock),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first.is_some(), second.is_some());
    let first = first.or(second).unwrap();
    let prepared = store
        .compaction_prepare_lifecycle(&first, NOW)
        .await
        .unwrap()
        .unwrap();
    let before = row(&store, "op").await.unwrap();
    let _next = claim(&store, "op", NOW + 5_000).await;
    let next = row(&store, "op").await.unwrap();
    assert_eq!(next.generation, before.generation);
    assert_ne!(next.claim_token, before.claim_token);
    assert_eq!(next.attempts, 2);
    assert_eq!(next.retry_not_before, NOW + 15_000);
    assert!(!admit_stale_repair(&store, prepared).await.unwrap());
    assert_eq!(row(&store, "op").await.unwrap(), next);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
}
