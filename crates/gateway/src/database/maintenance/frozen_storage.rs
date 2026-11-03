use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use pioneer_sqlite::background::{Plan, Ready};
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
const BATCH_PAUSE: Duration = Duration::from_millis(1);
const ERROR_RETRY: Duration = Duration::from_secs(60);

// Owned by this worker, like the bounded preparations. Only a successful false
// result records EOF; budgets, scope steps, errors and cancellation do not.
#[derive(Default)]
struct DiscoveryAdmission {
    complete: [bool; 2],
}
impl DiscoveryAdmission {
    fn ready(&self, path: usize) -> bool {
        !self.complete[path]
    }
    fn completed(&mut self, path: usize, more: bool) {
        self.complete[path] = !more;
    }
    fn invalidate(&mut self) {
        self.complete = [false; 2];
    }
}

#[cfg(test)]
#[derive(Default)]
struct DormantBarrier {
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let work = store.background_work("storage.frozen");
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut progress = FrozenStorageLifetimeProgress::default();
    let mut lifetime = false;
    let mut admission = DiscoveryAdmission::default();
    let mut previous = None;
    let mut rescan = false;
    loop {
        let ready = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            ready = owner.ready() => ready,
        };
        if ready == Ready::Closed {
            return;
        }
        if ready == Ready::SettlementRequired {
            let settle_observation = owner.observe();
            rescan = true;
            admission.invalidate();
            progress.invalidate_completed_cleanup();
            if store
                .database_connection()
                .settle_background_change(&owner)
                .await
                .is_err()
            {
                owner.retry_after(settle_observation, ERROR_RETRY);
            }
            continue;
        }
        let observed = owner.observe();
        if previous.is_some_and(|old| old != observed) {
            rescan = true;
            admission.invalidate();
            progress.invalidate_completed_cleanup();
        }
        previous = Some(observed);
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            outcome = owner.scope(crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::FrozenStorageMaintenance,
                pioneer_observability::DatabaseOperationSite::FrozenStorageBatch,
                batch(&store, &mut progress, &mut lifetime, &mut admission, BATCH_STEPS, BATCH_TIME),
            )) => outcome,
        };
        match outcome {
            Ok(more) if !owner.requires_settlement() => {
                let more = if !more && rescan {
                    rescan = false;
                    admission.invalidate();
                    progress.invalidate_completed_cleanup();
                    true
                } else {
                    more
                };
                let plan = pause_after_batch(more).map_or(Plan::Dormant, |pause| {
                    Plan::MonotonicDeadline(tokio::time::Instant::now() + pause)
                });
                let settled = owner.publish(observed, plan);
                #[cfg(test)]
                if settled && matches!(plan, Plan::Dormant) {
                    let barrier = store
                        .database_connection()
                        .shared_resource::<DormantBarrier>();
                    if barrier
                        .armed
                        .swap(false, std::sync::atomic::Ordering::AcqRel)
                    {
                        barrier.entered.notify_one();
                        tokio::select! {
                            _ = cancellation.cancelled() => return,
                            _ = barrier.release.notified() => {},
                        }
                    }
                }
                #[cfg(not(test))]
                let _ = settled;
            }
            _ => {
                tracing::warn!(
                    "frozen storage maintenance quantum failed; original history preserved"
                );
                owner.retry_after(observed, ERROR_RETRY);
            }
        }
    }
}

async fn batch(
    store: &CrudStore,
    progress: &mut FrozenStorageLifetimeProgress,
    lifetime: &mut bool,
    admission: &mut DiscoveryAdmission,
    step_limit: usize,
    time_limit: Duration,
) -> anyhow::Result<bool> {
    let started = Instant::now();
    // A per-batch conversion budget is distinct from actual discovery EOF.
    // Completed paths remain latched across batches while the other advances.
    let mut finished = [!admission.ready(0), !admission.ready(1)];
    let mut steps = 0;
    let mut conversion_steps = 0;
    while !finished.iter().all(|done| *done)
        && can_start_step(steps, started.elapsed(), step_limit, time_limit)
    {
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
        admission.completed(path, more);
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
    Ok(admission.ready(0) || admission.ready(1))
}

fn can_start_step(
    steps: usize,
    elapsed: Duration,
    step_limit: usize,
    time_limit: Duration,
) -> bool {
    steps < step_limit && (steps == 0 || elapsed < time_limit)
}

fn pause_after_batch(more: bool) -> Option<Duration> {
    more.then_some(BATCH_PAUSE)
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
        fixture_with_metadata(512).await
    }

    async fn fixture_with_metadata(count: usize) -> (tempfile::TempDir, CrudStore, Arc<Observer>) {
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
        for index in 0..count {
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
    async fn receipt_owner_keeps_empty_closed_and_revisits_an_old_turn_after_projection() {
        use pioneer_entity::{turn, turn_event_projection_stream_state as stream};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let db = store.database_connection();
        let now = chrono::Utc::now().fixed_offset();
        turn::Entity::insert(turn::ActiveModel {
            id: Set("000-old-turn".into()),
            thread_id: Set("thread".into()),
            status: Set("in_progress".into()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&db)
        .await
        .unwrap();
        stream::Entity::insert(stream::ActiveModel {
            turn_id: Set("000-old-turn".into()),
            thread_id: Set("thread".into()),
            status: Set("healthy".into()),
            projected_through_sequence: Set(0),
            receipts_compacted_through_sequence: Set(0),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&db)
        .await
        .unwrap();
        store
            .upsert_projection_meta(pioneer_crud::ProjectionMetaRecord {
                projection_key: "turn_event_projection_stream_state_backfill".into(),
                projection_version: 3,
                status: pioneer_crud::PROJECTION_META_STATUS_COMPLETE.into(),
                source_thread_count: 0,
                source_turn_count: 0,
                source_turn_item_count: 0,
                source_turn_event_count: 0,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(super::super::projection_receipt_cleanup::run(
            Arc::new(store.clone()),
            cancellation.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while observer.operations() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            2,
            "backfill + indexed empty stream discovery"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            2,
            "settled EOF has no periodic receipt SQL"
        );
        tokio::time::resume();
        let lower = CrudStore::new(db.clone());
        lower
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "000-old-turn".into(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: "receipt-wake-item".into(),
                        text: "new projection below the previous cursor".into(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                now.timestamp(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = stream::Entity::find_by_id("000-old-turn")
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                if current.receipts_compacted_through_sequence == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::TurnId.eq("000-old-turn")
                )
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(db.reader_query_only_enabled().await.unwrap());
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn native_cleanup_owner_parks_empty_and_requeues_waiting_after_binding_release() {
        use pioneer_entity::{cli_runtime_native_event as event, native_event_cleanup_job as job};
        use pioneer_protocol::{
            PersistedActorRef, SandboxMode, Thread, ThreadMode, ThreadOriginKind,
            ThreadSidebarVisibility, ThreadStatus, Turn, TurnOrigin, TurnStatus,
        };
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let db = store.database_connection();
        let now = chrono::Utc::now().fixed_offset();
        let thread = Thread {
            workspace_id: "ws".into(),
            id: "thread".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "m".into(),
            model_provider: "p".into(),
            reasoning_effort: None,
            created_at: now.timestamp(),
            updated_at: now.timestamp(),
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![],
        };
        let mut turn = Turn {
            work_owner: Default::default(),
            id: "native-old-turn".into(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: vec![],
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                PersistedActorRef::System,
            )
            .await
            .unwrap();
        turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn,
                },
                now.timestamp(),
            )
            .await
            .unwrap();
        store
            .upsert_cli_runtime_turn_binding(pioneer_crud::NewCliRuntimeTurnBinding {
                turn_id: "native-old-turn".into(),
                thread_id: "thread".into(),
                continuation_thread_id: "thread".into(),
                workspace_id: "ws".into(),
                runtime_id: "runtime".into(),
                runtime_kind: "claude".into(),
                native_thread_id: "native".into(),
                native_turn_id: Some("native-turn".into()),
                request_id: None,
                status: "running".into(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".into(),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        // Restored dependency-waiting history is outside ALL queued lanes.
        // A complete Empty must be independent of its population size.
        for index in 0..129 {
            job::Entity::insert(job::ActiveModel {
                turn_id: Set(format!("waiting-history-{index:03}")),
                state: Set("waiting".into()),
                available_at: Set(0),
                last_served_at: Set(0),
                revision: Set(1),
                last_error: Set(None),
            })
            .exec(&db)
            .await
            .unwrap();
        }
        assert!(
            store
                .next_native_event_cleanup_deadline()
                .await
                .unwrap()
                .is_none()
        );
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let idle = db.shared_resource::<super::super::native_event_cleanup::IdleBarrier>();
        idle.armed.store(true, std::sync::atomic::Ordering::Release);
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(super::super::native_event_cleanup::run(
            Arc::new(store.clone()),
            cancellation.clone(),
        ));
        // A minimum reader count cannot prove finite bootstrap/duty portions
        // have completed. Observe the actual successful Empty publication.
        tokio::time::timeout(Duration::from_secs(5), idle.entered.notified())
            .await
            .unwrap();
        idle.release.notify_one();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        let initial = observer.operations();
        let writes = observer.writes.lock().unwrap().len();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            initial,
            "completed bootstrap + empty queue has no periodic SQL"
        );
        assert_eq!(observer.writes.lock().unwrap().len(), writes);
        tokio::time::resume();
        let lower = CrudStore::new(db.clone());
        lower
            .append_cli_runtime_native_event(pioneer_crud::NewCliRuntimeNativeEvent {
                id: "native-release-event".into(),
                runtime_id: "runtime".into(),
                runtime_kind: "claude".into(),
                workspace_id: Some("ws".into()),
                thread_id: Some("thread".into()),
                turn_id: Some("native-old-turn".into()),
                native_thread_id: Some("native".into()),
                native_turn_id: Some("native-turn".into()),
                native_method: "item/completed".into(),
                payload_redacted_json: "{}".into(),
                sequence: 1,
                created_at: now,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if job::Entity::find_by_id("native-old-turn")
                    .one(&db)
                    .await
                    .unwrap()
                    .is_some_and(|job| job.state == "waiting")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            event::Entity::find_by_id("native-release-event")
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            lower
                .terminalize_cli_runtime_turn_binding(
                    "native-old-turn",
                    "completed",
                    pioneer_crud::CliRuntimeTurnAttemptStatus::Completed,
                    None,
                    now
                )
                .await
                .unwrap()
                .is_some()
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if event::Entity::find()
                    .filter(event::Column::Id.eq("native-release-event"))
                    .one(&db)
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        // Represent an existing retry obligation, then publish another real
        // native source. INSERT OR IGNORE registration preserves this queued
        // future clock; no client, manual worker call, or later wake is needed.
        let due = (chrono::Utc::now().timestamp() + 3) * 1_000_000 + 500_123;
        job::Entity::insert(job::ActiveModel {
            turn_id: Set("native-old-turn".into()),
            state: Set("queued".into()),
            available_at: Set(due),
            ..Default::default()
        })
        .exec(&db)
        .await
        .unwrap();
        assert_eq!(
            lower
                .next_native_event_cleanup_deadline()
                .await
                .unwrap()
                .unwrap()
                .timestamp_micros(),
            due,
            "positive retry lane preserves exact microsecond deadline"
        );
        lower
            .append_cli_runtime_native_event(pioneer_crud::NewCliRuntimeNativeEvent {
                id: "native-timed-retry".into(),
                runtime_id: "runtime".into(),
                runtime_kind: "claude".into(),
                workspace_id: Some("ws".into()),
                thread_id: Some("thread".into()),
                turn_id: Some("native-old-turn".into()),
                native_thread_id: Some("native".into()),
                native_turn_id: Some("native-turn".into()),
                native_method: "item/completed".into(),
                payload_redacted_json: "{}".into(),
                sequence: 2,
                created_at: now,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if event::Entity::find_by_id("native-timed-retry")
                    .one(&db)
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(db.reader_query_only_enabled().await.unwrap());
        cancellation.cancel();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn frozen_owner_keeps_eof_until_old_source_seals_and_last_blocking_hold_releases() {
        use pioneer_entity::compaction_frozen_history as history;
        use sea_orm::EntityTrait;
        use sha2::{Digest, Sha256};
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let descriptor = FrozenHistoryRef {
            format: 1,
            manifest_id: "000-worker-old".into(),
            messages: 0,
            identity_sha256: hex::encode(Sha256::digest([])),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        let barrier = store
            .database_connection()
            .shared_resource::<DormantBarrier>();
        barrier
            .armed
            .store(true, std::sync::atomic::Ordering::Release);
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(run(Arc::new(store.clone()), cancellation.clone()));
        tokio::time::timeout(Duration::from_secs(10), barrier.entered.notified())
            .await
            .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        barrier.release.notify_one();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            0,
            "unchanged completed history does not reopen discovery"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        let lower = CrudStore::new(store.database_connection());
        barrier
            .armed
            .store(true, std::sync::atomic::Ordering::Release);
        let hold = lower
            .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), barrier.entered.notified())
            .await
            .unwrap();
        let row = history::Entity::find_by_id(&descriptor.manifest_id)
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (row.storage_registered, row.expired),
            (1, 0),
            "old key became convertible, but live input is retained"
        );
        barrier
            .armed
            .store(true, std::sync::atomic::Ordering::Release);
        drop(hold);
        barrier.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), barrier.entered.notified())
            .await
            .unwrap();
        assert_eq!(
            history::Entity::find_by_id(&descriptor.manifest_id)
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap()
                .expired,
            1,
            "last release must finish refused expiry without a timer or new commit"
        );
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        cancellation.cancel();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn portable_owner_stays_dormant_and_publishes_new_tool_bytes_from_lower_crud() {
        use pioneer_entity::{tool_output_chunk as chunk, turn};
        use sea_orm::{EntityTrait, Set};
        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let db = store.database_connection();
        let now = chrono::Utc::now().fixed_offset();
        turn::Entity::insert(turn::ActiveModel {
            id: Set("portable-turn".into()),
            thread_id: Set("thread".into()),
            status: Set("completed".into()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&db)
        .await
        .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(super::super::portable_payload::run(
            Arc::new(store.clone()),
            cancellation.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while observer.operations() < 6 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            6,
            "one raw cleanup EOF plus five family source discoveries"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer.operations(),
            6,
            "confirmed Empty does not reopen portable discovery"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        let lower = CrudStore::new(db.clone());
        let output = pioneer_protocol::ItemDeltaNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "portable-turn".into(),
            item_id: "output".into(),
            delta: "portable unicode 🦀\n".repeat(4000),
            stream: None,
            payload: None,
            markdown: None,
            markdown_version: None,
        };
        lower
            .record_tool_output("000-old-output-key", &output)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if chunk::Entity::find_by_id("000-old-output-key")
                    .one(&db)
                    .await
                    .unwrap()
                    .is_some_and(|row| row.text_payload_id.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let page = lower
            .tool_output_page("ws", "thread", "portable-turn", "output", -1)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].1.text, output.delta);
        assert!(db.reader_query_only_enabled().await.unwrap());
        cancellation.cancel();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn real_metadata_batches_are_bounded_and_eof_needs_input_invalidation() {
        let (_directory, store, observer) = fixture().await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut lifetime = false;
        let mut admission = DiscoveryAdmission::default();
        let before = observer.operations();
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut admission,
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
                &mut admission,
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
                &mut admission,
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
        assert!(admission.complete.iter().all(|done| *done));
        assert_eq!(pause_after_batch(false), None);
        admission.invalidate();
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut admission,
                1,
                BATCH_TIME
            )
            .await
            .unwrap(),
            "explicit source invalidation admits another round"
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
                &mut DiscoveryAdmission::default(),
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
        assert_eq!(pause_after_batch(true), Some(BATCH_PAUSE));

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
        assert_eq!(pause_after_batch(true), Some(Duration::from_millis(1)));
        let mut admission = DiscoveryAdmission::default();
        admission.completed(0, false);
        admission.completed(1, false);
        assert_eq!(pause_after_batch(false), None);
        assert!(!admission.ready(0));
        assert!(!admission.ready(1));
        admission.invalidate();
        assert!(admission.ready(0) && admission.ready(1));
    }
    #[tokio::test]
    async fn conversion_eof_survives_batches_and_source_invalidation_discovers_new_work() {
        use pioneer_entity::{
            compaction_frozen_history as history, compaction_frozen_layout as layout,
        };
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        use sha2::{Digest, Sha256};

        for finish_lifetime in [false, true] {
            let (_directory, store, observer) = fixture_with_metadata(0).await;
            let mut progress = FrozenStorageLifetimeProgress::default();
            let mut lifetime = false;
            let mut admission = DiscoveryAdmission::default();
            admission.complete[1] = true;
            let mut useful = 0;
            for _ in 0..128 {
                let more = batch(
                    &store,
                    &mut progress,
                    &mut lifetime,
                    &mut admission,
                    1,
                    BATCH_TIME,
                )
                .await
                .unwrap();
                if admission.complete[0] {
                    assert!(!more);
                    break;
                }
                useful += 1;
                assert!(
                    more,
                    "scope and active conversion continue in the next batch"
                );
            }
            assert!(
                useful > 4,
                "scope and staged layout validation took bounded steps"
            );
            assert!(admission.complete[0], "actual conversion EOF");
            assert_eq!(
                history::Entity::find_by_id("live")
                    .one(&store.database_connection())
                    .await
                    .unwrap()
                    .unwrap()
                    .storage_registered,
                1
            );
            let layouts = layout::Entity::find()
                .filter(layout::Column::ManifestId.eq("live"))
                .all(&store.database_connection())
                .await
                .unwrap();
            assert_eq!(layouts.len(), 2);
            assert!(
                layouts
                    .iter()
                    .all(|row| row.active == 1 && row.pending == 0 && row.failed == 0)
            );

            admission.complete[1] = false;
            // Initial locator and proof discovery each issue one metadata read.
            // Repeating the three conversion discovery reads would change the count.
            for _ in 0..2 {
                let before = observer.operations();
                assert!(
                    batch(
                        &store,
                        &mut progress,
                        &mut lifetime,
                        &mut admission,
                        1,
                        BATCH_TIME
                    )
                    .await
                    .unwrap()
                );
                assert_eq!(observer.operations() - before, 1);
                assert!(admission.complete[0]);
            }
            if finish_lifetime {
                let mut idle = false;
                for _ in 0..32 {
                    if !batch(
                        &store,
                        &mut progress,
                        &mut lifetime,
                        &mut admission,
                        BATCH_STEPS,
                        BATCH_TIME,
                    )
                    .await
                    .unwrap()
                    {
                        idle = true;
                        break;
                    }
                }
                assert!(
                    idle,
                    "lifetime reaches its real EOF while conversion remains latched"
                );
                assert!(admission.complete[0]);
                assert!(admission.complete[1]);
            }

            let new = FrozenHistoryRef {
                format: 1,
                manifest_id: "new-work".into(),
                messages: 0,
                identity_sha256: hex::encode(Sha256::digest([])),
            };
            store
                .compaction_begin_frozen_history("ws", "thread", &new)
                .await
                .unwrap();
            drop(
                store
                    .compaction_finish_frozen_history_held("ws", "thread", &new)
                    .await
                    .unwrap(),
            );
            if finish_lifetime {
                let before = observer.operations();
                for _ in 0..3 {
                    assert!(
                        !batch(
                            &store,
                            &mut progress,
                            &mut lifetime,
                            &mut admission,
                            BATCH_STEPS,
                            BATCH_TIME
                        )
                        .await
                        .unwrap()
                    );
                }
                assert_eq!(
                    observer.operations(),
                    before,
                    "latched EOF performs no SQL or RAM quantum"
                );
                assert_eq!(pause_after_batch(false), None);
            } else {
                assert!(!admission.complete[1], "lifetime is still admitted");
            }
            assert_eq!(
                history::Entity::find_by_id("new-work")
                    .one(&store.database_connection())
                    .await
                    .unwrap()
                    .unwrap()
                    .storage_registered,
                0
            );

            admission.complete[0] = false;
            assert!(
                batch(
                    &store,
                    &mut progress,
                    &mut lifetime,
                    &mut admission,
                    1,
                    BATCH_TIME
                )
                .await
                .unwrap()
            );
            assert!(!admission.complete[0]);
            assert_eq!(
                history::Entity::find_by_id("new-work")
                    .one(&store.database_connection())
                    .await
                    .unwrap()
                    .unwrap()
                    .storage_registered,
                1
            );
        }
    }

    #[tokio::test]
    async fn discard_preflight_uses_maintenance_reader_without_writer_admission() {
        use pioneer_compaction::{
            ModelSelection, Transport,
            runner::{FailureKind, RunnerPhase, RunnerState},
        };
        use sha2::{Digest, Sha256};

        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let origin = FrozenHistoryRef {
            format: 1,
            manifest_id: "discard-origin".into(),
            messages: 0,
            identity_sha256: hex::encode(Sha256::digest([])),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &origin)
            .await
            .unwrap();
        drop(
            store
                .compaction_finish_frozen_history_held("ws", "thread", &origin)
                .await
                .unwrap(),
        );
        // Bind the projection while the ready input is available; expire it
        // only after that guarded insert, before maintenance starts.
        for sql in [
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('discard-owner','ws','thread',1)",
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('discard-op','discard-owner','discard-op','failed','{}',1000,0)",
            "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'discard-op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='discard-origin'",
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='discard-origin'",
        ] {
            store
                .database_connection()
                .execute_unprepared(sql)
                .await
                .unwrap();
        }
        let state = RunnerState {
            generation: 0,
            deadline_ms: 1000,
            attempts: 0,
            retries: 0,
            portion_attempts: 0,
            overflow_rejections: 0,
            overflow_input_ceiling: None,
            corrections: 0,
            target_tokens: 10,
            source_text_projection_version: 0,
            cursor: Default::default(),
            previous_checkpoint: None,
            phase: RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            },
            resume_phase: None,
            observation: None,
            diagnostic: None,
        };
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('discard-op',0,?1)",
            [serde_json::to_string(&state).unwrap().into()],
        )).await.unwrap();
        let selection = ModelSelection {
            transport: Transport::Api,
            instance: "p".into(),
            model: "m".into(),
            effort: None,
        };
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES('discard-key','discard-op','discard-owner',0,'s','identity',?1,1,1,'candidate')",
            [serde_json::to_string(&selection).unwrap().into()],
        )).await.unwrap();
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut eof = false;
        for _ in 0..128 {
            if !store
                .compact_frozen_storage_quantum(&mut progress)
                .await
                .unwrap()
            {
                eof = true;
                break;
            }
        }
        assert!(eof, "initial scope and conversion have completed");
        // The first rotation visits locator, proof, expiry and sweep, then the
        // concrete discard candidate. Scope was established by conversion.
        for _ in 0..4 {
            assert!(progress.quantum(&store).await.unwrap());
        }
        let reads = observer.operations();
        let writes = observer.writes.lock().unwrap().len();
        assert!(progress.quantum(&store).await.unwrap());
        assert_eq!(
            observer.operations() - reads,
            2,
            "candidate + exact-key preflight, no reverse read"
        );
        assert_eq!(observer.writes.lock().unwrap().len(), writes);
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|event| match event {
                    SqliteReadEvent::OperationFinished { class, .. } =>
                        *class == SqliteReadClass::Maintenance,
                    _ => true,
                })
        );
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn budget_error_and_cancelled_admission_do_not_record_eof() {
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut lifetime = false;
        let mut admission = DiscoveryAdmission::default();
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut admission,
                1,
                Duration::ZERO
            )
            .await
            .unwrap()
        );
        assert_eq!(
            admission.complete, [false; 2],
            "scope/budget stop is not EOF"
        );

        let held = store.database_connection().begin_read().await.unwrap();
        {
            let work = batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut admission,
                1,
                BATCH_TIME,
            );
            tokio::pin!(work);
            tokio::select! {
                result = &mut work => panic!("queued batch unexpectedly completed: {result:?}"),
                _ = observer.queued() => {},
            }
            // Dropping this owned future cancels the queued operation.
        }
        assert_eq!(admission.complete, [false; 2]);
        drop(held);
        store
            .database_connection()
            .execute_unprepared("ALTER TABLE compaction_frozen_layout RENAME TO unavailable_layout")
            .await
            .unwrap();
        progress = FrozenStorageLifetimeProgress::default();
        lifetime = false;
        assert!(
            batch(
                &store,
                &mut progress,
                &mut lifetime,
                &mut admission,
                1,
                BATCH_TIME
            )
            .await
            .is_err()
        );
        assert_eq!(admission.complete, [false; 2], "DB errors do not latch EOF");
    }

    #[tokio::test]
    async fn durable_sweep_restarts_quietly_without_writes_and_keeps_maintenance_routes() {
        let (_directory, store, observer) = fixture_with_metadata(0).await;
        let db = store.database_connection();
        db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES('held-physical','ws','thread','fixture','fixture',1,1,1,1,0)").await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('held-physical',0,'{}',2)").await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('held-physical',0,0,'event','e','v','thread','{}',2)").await.unwrap();
        let before = observer.operations();
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut idle = false;
        for _ in 0..1000 {
            if !progress.quantum(&store).await.unwrap() {
                idle = true;
                break;
            }
        }
        assert!(idle);
        let first_reads = observer.operations() - before;
        let row = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='held-physical' AND pending=0")).await.unwrap().unwrap();
        assert_eq!(row.try_get::<i64>("", "n").unwrap(), 2);
        let writes = observer.writes.lock().unwrap().len();
        let before = observer.operations();
        let mut restart = FrozenStorageLifetimeProgress::default();
        restart.invalidate_completed_cleanup();
        let mut idle = false;
        for _ in 0..1000 {
            if !restart.quantum(&store).await.unwrap() {
                idle = true;
                break;
            }
        }
        assert!(idle);
        assert!(
            observer.operations() - before < first_reads,
            "restart skips completed physical preparations and fanout"
        );
        assert_eq!(
            observer.writes.lock().unwrap().len(),
            writes,
            "quiet restart performs no sweep housekeeping writes"
        );
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|event| match event {
                    SqliteReadEvent::OperationFinished { class, .. } =>
                        *class == SqliteReadClass::Maintenance,
                    _ => true,
                })
        );
        assert!(
            observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .all(|event| match event {
                    SqliteWriteEvent::Acquired { class, .. } =>
                        *class == SqliteWriteClass::Maintenance,
                    _ => true,
                })
        );
        assert!(db.reader_query_only_enabled().await.unwrap());
    }

    #[tokio::test]
    async fn html_preview_index_uses_scoped_readers_and_serial_writer_and_cancels_admission() {
        let (directory, store, observer) = fixture().await;
        use pioneer_entity::{
            compaction_frozen_history as history, html_preview_frozen_index as index,
            html_preview_frozen_result as receipt, html_preview_frozen_turn as preview_turn,
            html_preview_index_backfill as backfill,
        };
        use sea_orm::sea_query::Expr;
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
        let maintenance = store.with_maintenance_access();
        let db = maintenance.database_connection();
        let tx = db.begin().await.unwrap();
        let fk = tx
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA foreign_keys",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fk.try_get::<i64>("", "foreign_keys").unwrap(), 0);
        let seed = backfill::Entity::find_by_id(1_i64)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((seed.complete, seed.inputs_complete), (0, 0));
        let marker = index::Entity::find_by_id("metadata-0000")
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                marker.ready,
                marker.expired,
                marker.next_ordinal,
                marker.complete,
                marker.unknown
            ),
            (0, 0, 0, 0, 0)
        );
        // No cascade is available: header triggers must make derived markers
        // conservative on the actual FK OFF writer, with rollback preserving both.
        history::Entity::update_many()
            .col_expr(history::Column::Expired, Expr::val(1))
            .filter(history::Column::Id.eq("metadata-0000"))
            .exec(&tx)
            .await
            .unwrap();
        assert_eq!(
            index::Entity::find_by_id("metadata-0000")
                .one(&tx)
                .await
                .unwrap()
                .unwrap()
                .expired,
            1
        );
        preview_turn::Entity::insert(preview_turn::ActiveModel {
            manifest_id: Set("metadata-0000".into()),
            turn_id: Set("preview-fixture".into()),
        })
        .exec(&tx)
        .await
        .unwrap();
        receipt::Entity::insert(receipt::ActiveModel {
            manifest_id: Set("metadata-0000".into()),
            ordinal: Set(0),
            reference_json: Set("{}".into()),
            turn_id: Set("preview-fixture".into()),
            source_id: Set("preview-source".into()),
            source_version: Set("revision:1".into()),
            archive_wire_sha256: Set(None),
        })
        .exec(&tx)
        .await
        .unwrap();
        assert!(
            preview_turn::Entity::find_by_id((
                "metadata-0000".to_owned(),
                "preview-fixture".to_owned()
            ))
            .one(&tx)
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            receipt::Entity::find_by_id(("metadata-0000".to_owned(), 0_i64))
                .one(&tx)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            index::Entity::delete_by_id("metadata-0000")
                .exec(&tx)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            index::Entity::find_by_id("metadata-0000")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
        assert!(
            preview_turn::Entity::find_by_id((
                "metadata-0000".to_owned(),
                "preview-fixture".to_owned()
            ))
            .one(&db)
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            receipt::Entity::find_by_id(("metadata-0000".to_owned(), 0_i64))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        let service = Arc::new(pioneer_artifacts::ArtifactService::new(
            Arc::new(store.clone()),
            Arc::new(pioneer_artifacts::LocalArtifactBlobStore::new(
                directory.path(),
            )),
        ));
        let mut progress = pioneer_artifacts::HtmlPreviewMaintenanceProgress::default();
        service
            .html_preview_maintenance_quantum(&mut progress)
            .await
            .unwrap();
        assert!(observer.reads.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                ..
            }
        )));
        assert!(observer.writes.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        )));
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        let held = store.database_connection().begin_read().await.unwrap();
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(super::super::html_previews::run(
            Arc::new(store.clone()),
            service,
            cancellation.clone(),
        ));
        observer.queued().await;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        drop(held);
        assert!(observer.reads.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteReadEvent::AdmissionCancelled {
                class: SqliteReadClass::Maintenance,
                queue_depth: 0,
                ..
            }
        )));
    }
}
