use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

// Admission is checked BETWEEN existing bounded operations, never by timing
// out an operation or retaining capacity for this entire batch. Their existing
// row/actual-byte limits and partial preparation buffers remain authoritative.
const BATCH_STEPS: usize = 64;
// A conversion step already compares up to 128 imports and their two target
// references. Keep that existing heavy envelope to one per outer portion;
// lifetime metadata/fragments use the other bounded slots without extra timers.
const BATCH_CONVERSION_STEPS: usize = 1;
const BATCH_TIME: Duration = Duration::from_millis(50);
const BATCH_PAUSE: Duration = Duration::from_millis(100);

pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = FrozenStorageLifetimeProgress::default();
    let mut lifetime = false;
    let mut idle = [false; 2];
    loop {
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            outcome = batch(&store, &mut progress, &mut lifetime, &mut idle, BATCH_STEPS, BATCH_TIME) => outcome,
        };
        let pause = match outcome {
            Ok(more) => pause_after_batch(more),
            Err(_) => {
                tracing::warn!(
                    "frozen storage maintenance quantum failed; original history preserved"
                );
                Duration::from_secs(60)
            }
        };
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(pause) => {},
        }
    }
}

async fn batch(
    store: &CrudStore,
    progress: &mut FrozenStorageLifetimeProgress,
    lifetime: &mut bool,
    idle: &mut [bool; 2],
    step_limit: usize,
    time_limit: Duration,
) -> anyhow::Result<bool> {
    let started = Instant::now();
    // Poll an empty path at most once per batch, but reconsider it in the next
    // batch: new conversion work must not wait for a long proof traversal's EOF.
    let mut finished = [false; 2];
    let mut steps = 0;
    let mut conversion_steps = 0;
    while can_start_step(steps, started.elapsed(), step_limit, time_limit) {
        let mut path = usize::from(*lifetime);
        if finished[path] {
            path = 1 - path;
        }
        *lifetime = path == 0;
        let more = if path == 1 {
            progress.quantum(store).await?
        } else {
            conversion_steps += 1;
            store.compact_frozen_storage_quantum(progress).await?
        };
        steps += 1;
        finished[path] = !more;
        idle[path] = !more;
        // EOF is meaningful only after both discovery paths finish. A changed
        // lifetime pass returns true and confirms EOF again before this wait.
        if idle.iter().all(|done| *done) {
            *idle = [false; 2];
            return Ok(false);
        }
        if conversion_steps == BATCH_CONVERSION_STEPS {
            finished[0] = true;
        }
        if finished.iter().all(|done| *done) {
            break;
        }
        // DB resources are already released. Ensure ready futures also give
        // cancellation and other tasks a cooperative scheduling point.
        tokio::task::yield_now().await;
    }
    Ok(true)
}

fn can_start_step(
    steps: usize,
    elapsed: Duration,
    step_limit: usize,
    time_limit: Duration,
) -> bool {
    steps < step_limit && (steps == 0 || elapsed < time_limit)
}

fn pause_after_batch(more: bool) -> Duration {
    if more {
        BATCH_PAUSE
    } else {
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_compaction::frozen::FrozenHistoryRef;
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
        SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
    };
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Observer {
        reads: Mutex<Vec<SqliteReadEvent>>,
        writes: Mutex<Vec<SqliteWriteEvent>>,
        queued: tokio::sync::Notify,
    }
    impl SqliteReadObserver for Observer {
        fn observe(&self, event: SqliteReadEvent) {
            self.reads.lock().unwrap().push(event);
            if matches!(
                event,
                SqliteReadEvent::AdmissionEnqueued {
                    active: 1,
                    queue_depth: 1,
                    ..
                }
            ) {
                self.queued.notify_one();
            }
        }
    }
    impl SqliteWriteObserver for Observer {
        fn observe(&self, event: SqliteWriteEvent) {
            self.writes.lock().unwrap().push(event);
        }
    }
    impl Observer {
        fn operations(&self) -> usize {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        SqliteReadEvent::OperationFinished {
                            outcome: pioneer_sqlite::SqliteReadOutcome::Ok,
                            ..
                        }
                    )
                })
                .count()
        }
        async fn queued(&self) {
            tokio::time::timeout(Duration::from_secs(5), self.queued.notified())
                .await
                .unwrap();
        }
    }

    // Real separate read-only reader and FK OFF serialized writer. Seed through
    // existing domain APIs; installed guards remain on during every action.
    async fn fixture() -> (tempfile::TempDir, CrudStore, Arc<Observer>) {
        use sha2::{Digest, Sha256};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("frozen-batch.sqlite");
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false);
        let writer = Database::connect(options).await.unwrap();
        writer
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        Migrator::up(&writer, None).await.unwrap();
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        writer
            .execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            )
            .await
            .unwrap();
        writer.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let seed = CrudStore::new(writer.clone());
        for index in 0..512 {
            seed.compaction_begin_frozen_history(
                "ws",
                "thread",
                &FrozenHistoryRef {
                    format: 1,
                    manifest_id: format!("metadata-{index:04}"),
                    messages: 0,
                    identity_sha256: hex::encode(Sha256::digest([])),
                },
            )
            .await
            .unwrap();
        }
        let live = FrozenHistoryRef {
            format: 1,
            manifest_id: "live".into(),
            messages: 0,
            identity_sha256: hex::encode(Sha256::digest([])),
        };
        seed.compaction_begin_frozen_history("ws", "thread", &live)
            .await
            .unwrap();
        drop(
            seed.compaction_finish_frozen_history_held("ws", "thread", &live)
                .await
                .unwrap(),
        );
        let now = chrono::Utc::now().fixed_offset();
        seed.upsert_cli_runtime_thread_binding(pioneer_crud::NewCliRuntimeThreadBinding {
            thread_id: "thread".into(), workspace_id: "ws".into(), runtime_id: "runtime".into(), runtime_kind: "claude".into(), native_thread_id: "native".into(),
            native_session_id: None, native_root_thread_id: None, native_cwd: None, native_model: None,
            resume_cursor_json: serde_json::json!({"provider":"claude","pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextManifestOwnerThreadId":"thread","contextOwnerThreadId":"thread","contextHistoryJson":serde_json::to_string(&live).unwrap()}}).to_string(),
            status: "active".into(), created_at: now, updated_at: now,
        }).await.unwrap();
        drop(seed);
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options
            .max_connections(2)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let observer = Arc::new(Observer::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        let store = CrudStore::new(database).with_maintenance_access();
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        (directory, store, observer)
    }

    #[tokio::test]
    async fn real_metadata_batches_are_bounded_continue_and_reset_idle_without_empty_sleeps() {
        let (_directory, store, observer) = fixture().await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut lifetime = false;
        let mut idle = [false; 2];
        let before = observer.operations();
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut idle,
                BATCH_STEPS,
                Duration::ZERO
            )
            .await
            .unwrap()
        );
        assert_eq!(
            observer.operations() - before,
            2,
            "time budget stops the NEXT operation, after one complete scope step"
        );
        let before = observer.operations();
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut idle,
                BATCH_STEPS,
                Duration::from_secs(60)
            )
            .await
            .unwrap()
        );
        let operations = observer.operations() - before;
        assert!(
            operations > 16 && operations < 256,
            "one portion performs many bounded lookups, not all 513 headers"
        );
        let mut eof = false;
        for _ in 0..100 {
            if !batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut idle,
                BATCH_STEPS,
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            {
                eof = true;
                break;
            }
        }
        assert!(eof);
        assert_eq!(idle, [false; 2]);
        assert_eq!(pause_after_batch(false), Duration::from_secs(60));
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut idle,
                1,
                BATCH_TIME
            )
            .await
            .unwrap(),
            "fresh round revisits after idle"
        );
        assert_eq!(
            store.database_connection().read_class(),
            SqliteReadClass::Maintenance
        );
        assert_eq!(
            store.database_connection().write_class(),
            SqliteWriteClass::Maintenance
        );
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    SqliteReadEvent::OperationFinished { class, .. } => Some(*class),
                    _ => None,
                })
                .all(|class| class == SqliteReadClass::Maintenance)
        );
        let writes = observer.writes.lock().unwrap();
        assert!(writes.iter().any(|e| matches!(
            e,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        )));
        assert!(!writes.iter().any(|e| matches!(e, SqliteWriteEvent::Acquired { class, .. } if *class != SqliteWriteClass::Maintenance)));
    }

    #[tokio::test]
    async fn slow_reader_admission_finishes_current_step_and_cancelled_batch_releases_queue() {
        let (_directory, store, observer) = fixture().await;
        let held = store.database_connection().begin_read().await.unwrap();
        let worker_store = store.clone();
        let operations = observer.operations();
        let work = tokio::spawn(async move {
            let mut progress = FrozenStorageLifetimeProgress::default();
            batch(
                &worker_store,
                &mut progress,
                &mut false,
                &mut [false; 2],
                BATCH_STEPS,
                BATCH_TIME,
            )
            .await
        });
        observer.queued().await;
        tokio::time::sleep(BATCH_TIME + Duration::from_millis(30)).await;
        assert!(
            !work.is_finished(),
            "batch time is not an admission timeout"
        );
        drop(held);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), work)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            observer.operations() - operations,
            2,
            "only current scope step completes after wait"
        );
        assert_eq!(pause_after_batch(true), BATCH_PAUSE);

        let held = store.database_connection().begin_read().await.unwrap();
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(run(Arc::new(store.clone()), cancellation.clone()));
        observer.queued().await;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        assert!(observer.reads.lock().unwrap().iter().any(|event| matches!(
            event,
            SqliteReadEvent::AdmissionCancelled {
                class: SqliteReadClass::Maintenance,
                queue_depth: 0,
                active: 1,
                ..
            }
        )));
        drop(held);
        tokio::time::timeout(
            Duration::from_secs(5),
            store.database_connection().begin_read(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(store.database_connection().writer_max_connections(), 1);
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        let pragma = store
            .database_connection()
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA foreign_keys",
            ))
            .await
            .unwrap()
            .unwrap();
        // FK policy belongs to the writer; this read also proves reader routing
        // still works after cancellation, while the fixture uses FK OFF above.
        let _: i64 = pragma.try_get("", "foreign_keys").unwrap();
    }

    #[test]
    fn budget_stops_next_step_without_amplifying_a_slow_operation() {
        let admits = |steps, elapsed| can_start_step(steps, elapsed, BATCH_STEPS, BATCH_TIME);
        assert!(admits(0, Duration::ZERO));
        assert!(!admits(BATCH_STEPS, Duration::ZERO));
        assert!(!admits(1, BATCH_TIME));
        assert!(!admits(1, Duration::from_secs(30)));
        // A DB queue wait ends this batch, but cannot multiply the next pause.
        assert_eq!(pause_after_batch(true), Duration::from_millis(100));
        assert_eq!(pause_after_batch(false), Duration::from_secs(60));
    }
}
