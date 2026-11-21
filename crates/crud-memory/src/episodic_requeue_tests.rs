use super::*;
use crate::episodic_source_store::{commit_episodic_source_save, prepare_episodic_source_save};
use pioneer_crud_conversations::thread_timeline_projection::{
    PROJECTION_META_STATUS_PENDING, ProjectionMetaConfigRecord, ProjectionMetaRecord,
    find_projection_meta, upsert_projection_meta_with_config,
};
use pioneer_crud_conversations::turn_item::{
    prepare_turn_item_projection, upsert_prepared_turn_item,
};
use pioneer_entity::{thread, turn};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor};
use sea_orm::{DbBackend, Statement};
use std::sync::Arc;

impl MemoryStore {
    fn maintenance(&self) -> Self {
        Self {
            connection: self.connection.maintenance(),
            ..self.clone()
        }
    }
}

async fn episodic_requeue_source_fixture() -> (
    MemoryStore,
    thread::Model,
    turn::Model,
    ThreadEpisodicIndexJobRecord,
    Arc<WriteQueueObserver>,
) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(WriteQueueObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    database.validate_reader().await.unwrap();
    let store = MemoryStore::new(database);
    store.connection.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_requeue_source','Requeue source',TRUE,FALSE);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES ('thr_requeue_source','ws_requeue_source','','agent','test','test','active');
         INSERT INTO turn(id,thread_id,turn_kind,origin,status)
         VALUES ('turn_requeue_source','thr_requeue_source','conversation','user','in_progress')"
    ).await.unwrap();
    let thread = thread::Entity::find_by_id("thr_requeue_source")
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let turn = turn::Entity::find_by_id("turn_requeue_source")
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let item = TurnItem::UserMessage {
        id: "requeue_source".to_owned(),
        text: "A".to_owned(),
        attachments: vec![],
    };
    let now = unix_to_datetime(1_700_000_010);
    let projected =
        prepare_turn_item_projection(&turn.id, &item, Some("completed"), now, now).unwrap();
    let source = prepare_episodic_source_save(
        &store.connection,
        &thread.workspace_id,
        &thread.id,
        &turn.id,
        &item,
    )
    .await
    .unwrap();
    let tx = store.connection.begin().await.unwrap();
    upsert_prepared_turn_item(&tx, projected).await.unwrap();
    commit_episodic_source_save(&tx, &source, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let job = store
        .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let running = store
        .maintenance()
        .claim_thread_episodic_index_job_if_due(&job.id, 1_700_000_010, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.attempt_count, 1);
    (store, thread, turn, running, observer)
}

#[derive(Default)]
struct WriteQueueObserver {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    events: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    enqueued: tokio::sync::Notify,
}

impl pioneer_sqlite::SqliteReadObserver for WriteQueueObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}

impl pioneer_sqlite::SqliteWriteObserver for WriteQueueObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.events.lock().unwrap().push(event);
        if matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. }) {
            self.enqueued.notify_one();
        }
    }
}

impl WriteQueueObserver {
    async fn wait_for_enqueued(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let ready = self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| {
                        matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. })
                    })
                    .count()
                    >= count;
                if ready {
                    break;
                }
                self.enqueued.notified().await;
            }
        })
        .await
        .expect("delivery claims should reach the writer queue");
    }
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_late_requeue_cannot_revive_user_exclusion_or_deletion() {
    for deletion in [false, true] {
        let (store, thread, turn, running, observer) = episodic_requeue_source_fixture().await;
        let maintenance = store.maintenance();
        let blocker = store
            .connection
            .with_critical_writes()
            .begin()
            .await
            .unwrap();
        observer.events.lock().unwrap().clear();
        let suppress = async {
            if deletion {
                maintenance
                    .tombstone_thread_episodic_items_for_item(
                        &thread.workspace_id,
                        &thread.id,
                        &turn.id,
                        "requeue_source",
                        1_700_000_011,
                    )
                    .await
                    .unwrap();
            } else {
                maintenance
                    .exclude_thread_episodic_item(
                        NewThreadEpisodicExclusionRecord {
                            id: None,
                            workspace_id: thread.workspace_id.clone(),
                            thread_id: thread.id.clone(),
                            index_item_id: running.index_item_id.clone(),
                            reason: ThreadEpisodicExclusionReason::UserRequested,
                            created_by: "test-user".to_owned(),
                        },
                        1_700_000_011,
                    )
                    .await
                    .unwrap();
            }
        };
        let requeue = maintenance.requeue_thread_episodic_index_attempt(
            &running.id,
            running.attempt_count,
            1_700_000_012,
            Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
        );
        tokio::pin!(suppress, requeue);
        tokio::select! { biased;
            _ = suppress.as_mut() => panic!("suppression bypassed occupied writer"),
            _ = observer.wait_for_enqueued(1) => {},
        }
        observer.reads.lock().unwrap().clear();
        tokio::select! { biased;
            result = requeue.as_mut() => panic!("requeue bypassed occupied writer: {result:?}"),
            _ = observer.wait_for_enqueued(2) => {},
        }
        assert!(observer.reads.lock().unwrap().is_empty());
        blocker.rollback().await.unwrap();
        let (_, outcome) = tokio::join!(suppress, requeue);
        assert_eq!(
            outcome.unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        let mut expected = running.clone();
        expected.status = ThreadEpisodicIndexJobStatus::Canceled;
        expected.last_error = Some(
            if deletion {
                THREAD_EPISODIC_USER_DELETED_ERROR
            } else {
                THREAD_EPISODIC_USER_EXCLUDED_ERROR
            }
            .to_owned(),
        );
        expected.updated_at = unix_to_datetime(1_700_000_011);
        expected.completed_at = Some(expected.updated_at);
        assert_eq!(
            store
                .find_thread_episodic_index_job(&running.id)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
        let source = store
            .find_thread_episodic_item(&running.index_item_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.status,
            if deletion {
                ThreadEpisodicItemStatus::Deleted
            } else {
                ThreadEpisodicItemStatus::Excluded
            }
        );
        assert!(
            maintenance
                .list_due_thread_episodic_index_jobs_after(1_700_000_013, None, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
        store.connection.close().await.unwrap();
    }
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_requeue_reason_is_atomic_and_cancellation_and_rollback_preserve_running() {
    use pioneer_sqlite::{SqliteWriteClass, SqliteWriteEvent};
    let (store, _thread, _turn, running, observer) = episodic_requeue_source_fixture().await;
    let maintenance = store.maintenance();
    let blocker = store
        .connection
        .with_critical_writes()
        .begin()
        .await
        .unwrap();
    observer.events.lock().unwrap().clear();
    let work = tokio::spawn({
        let store = maintenance.clone();
        let running = running.clone();
        async move {
            store
                .requeue_thread_episodic_index_attempt(
                    &running.id,
                    running.attempt_count,
                    1_700_000_012,
                    Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
                )
                .await
        }
    });
    observer.wait_for_enqueued(1).await;
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    assert!(
        observer
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event,
            SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. }
                if queue.maintenance == 0))
    );
    blocker.rollback().await.unwrap();
    assert_eq!(
        store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        running
    );
    assert_eq!(
        maintenance
            .requeue_thread_episodic_index_attempt(
                &running.id,
                running.attempt_count + 1,
                1_700_000_012,
                Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
            )
            .await
            .unwrap(),
        ThreadEpisodicIndexAttemptOutcome::StaleAttempt
    );
    assert_eq!(
        store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        running
    );
    // AFTER UPDATE makes the status/reason mutation occur before the forced
    // rollback. No test-only pause or non-DB await is added inside the writer.
    store.connection.execute_unprepared(
            "CREATE FUNCTION reject_requeue_after_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled requeue rollback'; END $$; CREATE TRIGGER reject_requeue_after_update AFTER UPDATE ON thread_episodic_index_jobs FOR EACH ROW WHEN (NEW.status = 'queued') EXECUTE FUNCTION reject_requeue_after_update()"
        ).await.unwrap();
    assert!(
        maintenance
            .requeue_thread_episodic_index_attempt(
                &running.id,
                running.attempt_count,
                1_700_000_012,
                Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        running
    );
    store
            .connection
            .execute_unprepared("DROP TRIGGER reject_requeue_after_update ON thread_episodic_index_jobs; DROP FUNCTION reject_requeue_after_update()")
            .await
            .unwrap();
    observer.events.lock().unwrap().clear();
    observer.reads.lock().unwrap().clear();
    assert_eq!(
        maintenance
            .requeue_thread_episodic_index_attempt(
                &running.id,
                running.attempt_count,
                1_700_000_012,
                Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
            )
            .await
            .unwrap(),
        ThreadEpisodicIndexAttemptOutcome::Applied
    );
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "guarded reads use the transaction's writer"
    );
    let acquired = observer
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| {
            if let SqliteWriteEvent::Acquired { class, .. } = event {
                Some(*class)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(acquired, vec![SqliteWriteClass::Maintenance]);
    let mut expected = running.clone();
    expected.status = ThreadEpisodicIndexJobStatus::Queued;
    expected.next_run_at = unix_to_datetime(1_700_000_012);
    expected.updated_at = expected.next_run_at;
    expected.last_error = Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR.to_owned());
    expected.last_attempt_latency_ms = None;
    expected.completed_at = None;
    assert_eq!(
        store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        expected
    );
    // A stale repeated callback cannot reset delay, count or transition reason.
    assert_eq!(
        maintenance
            .requeue_thread_episodic_index_attempt(
                &running.id,
                running.attempt_count,
                1_700_000_013,
                None,
            )
            .await
            .unwrap(),
        ThreadEpisodicIndexAttemptOutcome::StaleAttempt
    );
    assert_eq!(
        store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        expected
    );
    store
        .connection
        .begin()
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    store.connection.close().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_model_reset_commits_cursor_with_replacement_and_preserves_budget_on_restart() {
    let workspace_id = "ws_reset_cursor";
    let thread_id = "thr_reset_cursor";
    let turn_id = "turn_reset_cursor";
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection().clone();
    let reader = fixture.connect_read_only().await.unwrap();
    let store = MemoryStore::new(SqliteDatabase::new(reader, writer));
    store.connection.execute_unprepared(
        "INSERT INTO workspace (id, name, is_active, is_current)
             VALUES ('ws_reset_cursor', 'Episodic reset', TRUE, FALSE);
         INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thr_reset_cursor', 'ws_reset_cursor', '', 'agent', 'test', 'test', 'active');
         INSERT INTO turn (id, thread_id, turn_kind, origin, status)
             VALUES ('turn_reset_cursor', 'thr_reset_cursor', 'conversation', 'user', 'in_progress')",
    ).await.unwrap();
    let mut originals = Vec::new();
    for index in 0..34 {
        let item = TurnItem::UserMessage {
            id: format!("reset_{index:02}"),
            text: format!("reset source {index}"),
            attachments: vec![],
        };
        let created_at = unix_to_datetime(1_700_010_000);
        for timestamp in [1_700_010_000, 1_700_010_001] {
            let updated_at = unix_to_datetime(timestamp);
            let projected = prepare_turn_item_projection(
                turn_id,
                &item,
                Some("completed"),
                created_at,
                updated_at,
            )
            .unwrap();
            let source = prepare_episodic_source_save(
                &store.connection,
                workspace_id,
                thread_id,
                turn_id,
                &item,
            )
            .await
            .unwrap();
            let tx = store.connection.begin().await.unwrap();
            upsert_prepared_turn_item(&tx, projected).await.unwrap();
            commit_episodic_source_save(&tx, &source, updated_at)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
    }
    let sources = store
        .list_thread_episodic_items_for_thread(workspace_id, thread_id, 100)
        .await
        .unwrap();
    for source in &sources {
        originals.push(
            store
                .find_thread_episodic_index_job_by_item(&source.id)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let first_source = sources
        .iter()
        .find(|source| source.item_id == "reset_00")
        .unwrap();
    let duplicated_job = originals
        .iter()
        .find(|job| job.index_item_id == first_source.id)
        .unwrap();
    // Historical nullable capsule keys admitted duplicate claims. A reset
    // interrupted after source progress must retire every old identity.
    store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO thread_episodic_index_jobs(id,workspace_id,thread_id,index_item_id,status,attempt_count,next_run_at,created_at,updated_at) SELECT 'legacy-duplicate-' || lpad(n::text,3,'0'),workspace_id,thread_id,index_item_id,'running',1,next_run_at,created_at,updated_at FROM generate_series(0,129) AS rows(n) CROSS JOIN thread_episodic_index_jobs WHERE id=$1",
        [duplicated_job.id.clone().into()])).await.unwrap();
    let key = thread_episodic_projection_reset_key(workspace_id).unwrap();
    let now = unix_to_datetime(1_700_010_002);
    upsert_projection_meta_with_config(
        &store.connection,
        ProjectionMetaRecord {
            projection_key: key.clone(),
            projection_version: 1,
            status: PROJECTION_META_STATUS_PENDING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
        ProjectionMetaConfigRecord {
            projection_config_hash: Some("model-b-reset".to_owned()),
            projection_config_json: Some(
                serde_json::to_string(&ThreadEpisodicProjectionResetProgress::default()).unwrap(),
            ),
        },
    )
    .await
    .unwrap();
    // The first quantum commits. The next source forces rollback of its
    // replacement and cursor together, simulating interruption mid-reset.
    store.connection.execute_unprepared("CREATE FUNCTION reject_second_reset_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled reset bookkeeping failure'; END; $$; CREATE TRIGGER reject_second_reset_batch BEFORE UPDATE ON thread_episodic_items FOR EACH ROW WHEN (OLD.item_id = 'reset_32') EXECUTE FUNCTION reject_second_reset_batch()").await.unwrap();
    assert!(
        store
            .maintenance()
            .reset_thread_episodic_projection(workspace_id, 1_700_010_002)
            .await
            .is_err()
    );
    let progress: ThreadEpisodicProjectionResetProgress = serde_json::from_str(
        find_projection_meta(&store.connection, &key)
            .await
            .unwrap()
            .unwrap()
            .projection_config_json
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert!(progress.reset_started);
    assert_eq!(progress.after_source.as_ref().unwrap()[2], "reset_31");
    let mut prefix = Vec::new();
    for source in &sources {
        let job = store
            .find_thread_episodic_index_job_by_item(&source.id)
            .await
            .unwrap()
            .unwrap();
        let original = originals
            .iter()
            .find(|old| old.index_item_id == source.id)
            .unwrap();
        if source.item_id.as_str() < "reset_32" {
            assert_ne!(job.id, original.id);
            assert_eq!(job.attempt_count, 0);
            prefix.push(job);
        } else {
            assert_eq!(&job, original);
        }
    }
    assert_eq!(prefix.len(), 32);
    let remaining_duplicates = store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres, "SELECT count(*) AS count FROM thread_episodic_index_jobs WHERE id LIKE 'legacy-duplicate-%'")).await.unwrap().unwrap().try_get::<i64>("", "count").unwrap();
    assert_eq!(remaining_duplicates, 0);
    assert_eq!(
        store
            .fail_thread_episodic_index_attempt_without_source_validation(
                "legacy-duplicate-000",
                1,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: true,
                    next_run_at_unix: Some(1_700_010_100),
                    last_error: Some("late legacy callback".into()),
                    last_attempt_latency_ms: None
                },
                1_700_010_003
            )
            .await
            .unwrap(),
        ThreadEpisodicIndexAttemptOutcome::StaleAttempt
    );

    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_second_reset_batch ON thread_episodic_items; DROP FUNCTION reject_second_reset_batch()")
        .await
        .unwrap();
    store
        .maintenance()
        .reset_thread_episodic_projection(workspace_id, 1_700_010_003)
        .await
        .unwrap();
    for job in &prefix {
        assert_eq!(
            store
                .find_thread_episodic_index_job(&job.id)
                .await
                .unwrap()
                .unwrap(),
            *job
        );
    }
    let replacement = store
        .find_thread_episodic_index_job_by_item(&originals[0].index_item_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement.id, originals[0].id);
    let claim = store
        .claim_due_thread_episodic_index_jobs_for_workspace(workspace_id, 1_700_010_004, 1, 5)
        .await
        .unwrap()
        .assert_no_failures_for_test()
        .pop()
        .unwrap();
    store
        .fail_thread_episodic_index_attempt_without_source_validation(
            &claim.id,
            claim.attempt_count,
            ThreadEpisodicIndexJobFailureUpdate {
                retryable: false,
                next_run_at_unix: None,
                last_error: Some("new model exhausted".to_owned()),
                last_attempt_latency_ms: None,
            },
            1_700_010_004,
        )
        .await
        .unwrap();
    let exhausted = store
        .find_thread_episodic_index_job(&claim.id)
        .await
        .unwrap()
        .unwrap();
    store
        .maintenance()
        .reset_thread_episodic_projection(workspace_id, 1_700_010_005)
        .await
        .unwrap();
    assert_eq!(
        store
            .find_thread_episodic_index_job(&claim.id)
            .await
            .unwrap()
            .unwrap(),
        exhausted
    );
    assert_eq!(exhausted.status, ThreadEpisodicIndexJobStatus::Canceled);
    assert_eq!(exhausted.attempt_count, 1);
    assert_eq!(
        store
            .fail_thread_episodic_index_attempt_without_source_validation(
                &originals[0].id,
                1,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: true,
                    next_run_at_unix: Some(1_700_010_100),
                    last_error: Some("old callback".to_owned()),
                    last_attempt_latency_ms: None,
                },
                1_700_010_005
            )
            .await
            .unwrap(),
        ThreadEpisodicIndexAttemptOutcome::StaleAttempt
    );
}
