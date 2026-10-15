use super::*;
use pioneer_sqlite::{SqliteWriteClass, SqliteWriteEvent};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

const HEARTBEAT_WRITTEN: &str = "__execution_owner_heartbeat_written__";

pub(super) async fn hold_history_pass(processor: &Arc<MessageProcessor>) {
    processor.arm_completed_history_preparation_barrier("__resilience_before_history_poll__");
    processor.start_resilience_workers().await.unwrap();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .expect("the production resilience pass must reach its history boundary");
}

pub(super) async fn next_heartbeat(processor: &Arc<MessageProcessor>) {
    processor
        .turn_execution_heartbeat_barrier
        .arm(HEARTBEAT_WRITTEN);
    timeout(
        Duration::from_secs(5),
        processor
            .turn_execution_heartbeat_barrier
            .wait_until_reached(),
    )
    .await
    .expect("production heartbeat must run while the common pass is held");
    processor.turn_execution_heartbeat_barrier.release();
}

async fn fixture(
    observer: Option<Arc<dyn pioneer_sqlite::SqliteWriteObserver>>,
) -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    Arc<CrudStore>,
    String,
) {
    let (directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observer(observer).await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    (directory, processor, store, workspace)
}

async fn seed_execution(
    store: &CrudStore,
    workspace: &str,
    id: &str,
    owner: &str,
    status: &str,
    executor: &str,
) {
    materialize_cli_runtime_turn_with_text(store, workspace, id, id, "heartbeat fixture").await;
    if status == "completed" {
        store
            .update_turn_status(id, id, TurnStatus::Completed, None, now_timestamp_secs())
            .await
            .unwrap();
    }
    // Seed an old receipt before starting workers. Every active status and
    // backend shares the owner heartbeat contract; no runtime is needed here.
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_execution(turn_id,thread_id,workspace_id,executor_kind,status,owner_id,owner_generation,lease_until,heartbeat_at,created_at,updated_at,started_at,completed_at)
         VALUES (?,?,?,?,?,?,1,'2000-01-01T00:00:15+00:00','2000-01-01T00:00:00+00:00','2000-01-01T00:00:00+00:00','2000-01-01T00:00:00+00:00',CASE WHEN ?='running' THEN '2000-01-01T00:00:00+00:00' END,CASE WHEN ?='completed' THEN '2000-01-01T00:00:00+00:00' END)",
        [id.into(), id.into(), workspace.into(), executor.into(), status.into(), owner.into(), status.into(), status.into()]))
        .await.unwrap();
}

async fn install_heartbeat_audit(store: &CrudStore) {
    store
        .database_connection()
        .execute_unprepared(
            "CREATE TABLE heartbeat_updates(n INTEGER); INSERT INTO heartbeat_updates VALUES(0);
         CREATE TRIGGER heartbeat_count AFTER UPDATE OF heartbeat_at ON turn_execution
         BEGIN UPDATE heartbeat_updates SET n=n+1; END",
        )
        .await
        .unwrap();
}

async fn heartbeat_update_count(store: &CrudStore) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT n FROM heartbeat_updates",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

// Keep time running during real SQLx I/O and Notify/timeout waits. Pause only
// for the explicit timer advances, and resume before awaiting database work.
#[tokio::test]
async fn owner_heartbeat_survives_held_pass_is_single_and_stops_with_parent() {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, processor, store, workspace) = fixture(Some(observer.clone())).await;
    for (status, executor) in [
        ("queued", "native_agent"),
        ("starting", "api_provider"),
        ("running", "cli_runtime"),
        ("recovering", "acp_runtime"),
    ] {
        seed_execution(
            &store,
            &workspace,
            status,
            processor.turn_execution_owner_id.as_ref(),
            status,
            executor,
        )
        .await;
    }
    seed_execution(
        &store,
        &workspace,
        "foreign",
        "another-owner",
        "running",
        "cli_runtime",
    )
    .await;
    seed_execution(
        &store,
        &workspace,
        "terminal",
        processor.turn_execution_owner_id.as_ref(),
        "completed",
        "cli_runtime",
    )
    .await;
    let foreign = store.get_turn_execution("foreign").await.unwrap().unwrap();
    let terminal = store.get_turn_execution("terminal").await.unwrap().unwrap();
    hold_history_pass(&processor).await;
    let worker_id = processor
        .resilience_worker
        .lock()
        .await
        .as_ref()
        .unwrap()
        .id();
    processor.start_resilience_workers().await.unwrap();
    assert_eq!(
        worker_id,
        processor
            .resilience_worker
            .lock()
            .await
            .as_ref()
            .unwrap()
            .id()
    );
    install_heartbeat_audit(&store).await;
    processor
        .turn_execution_heartbeat_barrier
        .arm(HEARTBEAT_WRITTEN);
    timeout(
        Duration::from_secs(5),
        processor
            .turn_execution_heartbeat_barrier
            .wait_until_reached(),
    )
    .await
    .unwrap();
    let count = heartbeat_update_count(&store).await;
    // One worker is parked after its UPDATE. A duplicate started by the second
    // start call, or an overlapping request, would keep writing during this gap.
    processor.start_resilience_workers().await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    assert_eq!(heartbeat_update_count(&store).await, count);
    processor.turn_execution_heartbeat_barrier.release();
    let writes_before = observer.writes.lock().unwrap().len();
    for _ in 0..3 {
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        next_heartbeat(&processor).await;
        for status in ["queued", "starting", "running", "recovering"] {
            let receipt = store.get_turn_execution(status).await.unwrap().unwrap();
            assert_eq!(receipt.status.as_str(), status);
            assert!(receipt.lease_until > chrono::Utc::now().fixed_offset());
            assert_eq!(
                receipt.lease_until.timestamp() - receipt.heartbeat_at.timestamp(),
                TURN_EXECUTION_OWNER_LEASE_SECONDS
            );
        }
    }
    assert_eq!(
        store.get_turn_execution("foreign").await.unwrap().unwrap(),
        foreign
    );
    assert_eq!(
        store.get_turn_execution("terminal").await.unwrap().unwrap(),
        terminal
    );
    assert!(
        observer.writes.lock().unwrap()[writes_before..]
            .iter()
            .any(|e| matches!(
                e,
                SqliteWriteEvent::Acquired {
                    class: SqliteWriteClass::Critical,
                    ..
                }
            ))
    );
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );

    // Parent task termination uses the same Drop guard as explicit shutdown.
    let handle = processor.resilience_worker.lock().await.take().unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    tokio::task::yield_now().await;
    let stopped = store.get_turn_execution("running").await.unwrap().unwrap();
    let write_count = heartbeat_update_count(&store).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
    assert_eq!(
        store.get_turn_execution("running").await.unwrap().unwrap(),
        stopped
    );
    assert_eq!(heartbeat_update_count(&store).await, write_count);
    // The old pass was aborted at its barrier: do not leave a release permit
    // that could make the new pass skip its own barrier.
    hold_history_pass(&processor).await;
    next_heartbeat(&processor).await;
    processor.shutdown_resilience_workers().await;
    tokio::task::yield_now().await;
    let write_count = heartbeat_update_count(&store).await;
    let stopped = store.get_turn_execution("running").await.unwrap().unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
    assert_eq!(
        store.get_turn_execution("running").await.unwrap().unwrap(),
        stopped
    );
    assert_eq!(heartbeat_update_count(&store).await, write_count);
}

#[tokio::test]
async fn owner_cancellation_stops_heartbeat_and_worker_does_not_retain_processor() {
    let (_directory, processor, store, workspace) = fixture(None).await;
    seed_execution(
        &store,
        &workspace,
        "owned",
        processor.turn_execution_owner_id.as_ref(),
        "running",
        "api_provider",
    )
    .await;
    install_heartbeat_audit(&store).await;
    let cancellation = tokio_util::sync::CancellationToken::new();
    processor.arm_completed_history_preparation_barrier("__resilience_before_history_poll__");
    processor
        .start_resilience_workers_with_cancellation(&cancellation, |_| {})
        .await
        .unwrap();
    processor
        .wait_for_completed_history_preparation_barrier()
        .await;
    next_heartbeat(&processor).await;
    cancellation.cancel();
    let handle = processor.resilience_worker.lock().await.take().unwrap();
    handle.await.unwrap();
    tokio::task::yield_now().await;
    let stopped = store.get_turn_execution("owned").await.unwrap().unwrap();
    let write_count = heartbeat_update_count(&store).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
    assert_eq!(
        store.get_turn_execution("owned").await.unwrap().unwrap(),
        stopped
    );
    assert_eq!(heartbeat_update_count(&store).await, write_count);
    processor.shutdown_resilience_workers().await;
    // Check the heartbeat's own pause without another worker retaining the processor.
    let weak = Arc::downgrade(&processor);
    let barrier = processor.turn_execution_heartbeat_barrier.clone();
    barrier.arm(HEARTBEAT_WRITTEN);
    let worker = tokio::spawn(MessageProcessor::run_turn_execution_owner_heartbeat_worker(
        weak.clone(),
    ));
    barrier.wait_until_reached().await;
    drop(processor);
    assert!(weak.upgrade().is_none());
    barrier.release();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    worker.await.unwrap();
}

#[test]
fn native_child_and_task_run_complete_while_other_turn_holds_history_pass() {
    run_standard_stack_message_test(
        "child completion with independently heartbeating owner",
        attached_cli_task_starts_while_its_parent_provider_turn_is_active_impl(true),
    );
}

// SQLite 'now' is a wall clock. Tokio's paused clock controls worker timers,
// but cannot move SQLite's time. Cross one wall-clock second with yields (no
// race sleeps), then release the existing writer probe before any other write.
async fn cross_sqlite_clock_second() -> i64 {
    let before = chrono::Utc::now().timestamp();
    timeout(Duration::from_secs(2), async {
        loop {
            let now = chrono::Utc::now().timestamp();
            if now > before {
                return now;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn heartbeat_time_is_evaluated_after_writer_wait_and_again_on_internal_retry() {
    let observer = Arc::new(PausedCliHeartbeatWriter::default());
    let (_directory, processor, store, workspace) = fixture(Some(observer.clone())).await;
    seed_execution(
        &store,
        &workspace,
        "owned",
        processor.turn_execution_owner_id.as_ref(),
        "running",
        "cli_runtime",
    )
    .await;
    let scope = Arc::new(store.with_maintenance_reads_and_critical_writes());
    let owner = processor.turn_execution_owner_id.to_string();
    for retry in [false, true] {
        if retry {
            scope.database_connection().execute_unprepared(
                "CREATE TABLE heartbeat_fault(armed INTEGER); INSERT INTO heartbeat_fault VALUES(1);
                 CREATE TRIGGER heartbeat_lock BEFORE UPDATE ON turn_execution
                 WHEN EXISTS(SELECT 1 FROM heartbeat_fault WHERE armed=1)
                 BEGIN DELETE FROM heartbeat_fault; SELECT RAISE(FAIL,'database is locked'); END")
                .await.unwrap();
        }
        observer.armed.store(true, Ordering::SeqCst);
        let task_store = scope.clone();
        let task_owner = owner.clone();
        let heartbeat = tokio::spawn(async move {
            task_store
                .heartbeat_turn_executions_owned_by(&task_owner, TURN_EXECUTION_OWNER_LEASE_SECONDS)
                .await
        });
        timeout(Duration::from_secs(5), observer.entered.notified())
            .await
            .unwrap();
        assert_eq!(
            *observer.acquired_class.lock().unwrap(),
            Some(SqliteWriteClass::Critical)
        );
        if retry {
            // Arm the next attempt before releasing the failing statement.
            observer.armed.store(true, Ordering::SeqCst);
            observer.release.notify_one();
            timeout(Duration::from_secs(5), observer.entered.notified())
                .await
                .unwrap();
            // RAISE(FAIL) preserves the one-shot fault marker's deletion while
            // rejecting the first execution UPDATE. The next real CRUD retry
            // is paused after admission, before evaluating SQLite's clock.
            let row = scope
                .database_connection()
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "SELECT count(*) AS remaining FROM heartbeat_fault",
                ))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.try_get::<i64>("", "remaining").unwrap(), 0);
        }
        let released_at = cross_sqlite_clock_second().await;
        observer.release.notify_one();
        let result = heartbeat.await.unwrap();
        assert_eq!(result.unwrap(), 1);
        let receipt = store.get_turn_execution("owned").await.unwrap().unwrap();
        assert!(receipt.heartbeat_at.timestamp() >= released_at);
        assert_eq!(receipt.updated_at, receipt.heartbeat_at);
        assert_eq!(
            receipt.lease_until.timestamp() - receipt.heartbeat_at.timestamp(),
            TURN_EXECUTION_OWNER_LEASE_SECONDS
        );
        assert!(
            receipt.lease_until.timestamp() >= released_at + TURN_EXECUTION_OWNER_LEASE_SECONDS
        );
        assert!(receipt.lease_until > chrono::Utc::now().fixed_offset());
        if retry {
            scope
                .database_connection()
                .execute_unprepared("DROP TRIGGER heartbeat_lock; DROP TABLE heartbeat_fault")
                .await
                .unwrap();
        }
    }
}
