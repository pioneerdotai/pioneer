use super::super::{
    turn_event_projection_state as projections, turn_event_projection_stream_state as streams,
};
use super::*;
use crate::{CrudStore, ProjectionMetaRecord, upsert_projection_meta};
use migration::{Migrator, MigratorTrait};
use pioneer_entity::turn_event;
use sea_orm::{Database, PaginatorTrait, TransactionTrait};

async fn store() -> CrudStore {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    CrudStore::new(db).with_maintenance_access()
}

async fn ready(store: &CrudStore) {
    let now = chrono::Utc::now().fixed_offset();
    upsert_projection_meta(
        &store.database_connection(),
        ProjectionMetaRecord {
            projection_key: "turn_event_projection_stream_state_backfill".into(),
            projection_version: 3,
            status: crate::PROJECTION_META_STATUS_COMPLETE.into(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
    .unwrap();
}

async fn seed(store: &CrudStore, turn_id: &str, count: i64, watermark: i64) {
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    streams::ensure_healthy(&db, "thread", turn_id, now)
        .await
        .unwrap();
    for sequence in 1..=count {
        insert_event(store, turn_id, sequence, "projected").await;
    }
    if watermark > 0 {
        assert!(
            streams::advance_projected_through(&db, turn_id, 0, watermark, now)
                .await
                .unwrap()
        );
    }
}

async fn insert_event(store: &CrudStore, turn_id: &str, sequence: i64, status: &str) {
    let db = store.database_connection();
    let event_id = format!("{turn_id}-{sequence}");
    db.execute_raw(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) VALUES (?, 'thread', ?, ?, 'test', '{}', CURRENT_TIMESTAMP)",
        [event_id.clone().into(), turn_id.to_owned().into(), sequence.into()],
    )).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO turn_event_projection_state(event_id, thread_id, turn_id, sequence, status, next_run_at, projection_context_json, created_at, updated_at) VALUES (?, 'thread', ?, ?, ?, CURRENT_TIMESTAMP, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        [event_id.into(), turn_id.to_owned().into(), sequence.into(), status.to_owned().into()],
    )).await.unwrap();
}

async fn boundary(store: &CrudStore, turn_id: &str) -> i64 {
    streams::find(&store.database_connection(), turn_id)
        .await
        .unwrap()
        .unwrap()
        .receipts_compacted_through_sequence
}

#[tokio::test]
async fn schema_upgrade_only_adds_the_boundary_and_does_not_delete_old_receipts() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let migrations_before_cleanup = Migrator::migrations().len() as u32 - 1;
    Migrator::up(&db, Some(migrations_before_cleanup))
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO turn_event_projection_stream_state(turn_id, thread_id, status, projected_through_sequence, created_at, updated_at) VALUES ('turn', 'thread', 'healthy', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
         INSERT INTO turn_event_projection_state(event_id, thread_id, turn_id, sequence, status, next_run_at, created_at, updated_at) VALUES ('event', 'thread', 'turn', 1, 'projected', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"
    ).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let stream = streams::find(&db, "turn").await.unwrap().unwrap();
    assert_eq!(stream.projected_through_sequence, 1);
    assert_eq!(stream.receipts_compacted_through_sequence, 0);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 1);
}

#[tokio::test]
async fn byte_budget_can_end_a_quantum_before_the_row_limit() {
    let store = store().await;
    seed(&store, "turn", 3, 3).await;
    ready(&store).await;
    receipt::Entity::update_many()
        .col_expr(
            receipt::Column::ProjectionContextJson,
            Expr::value("x".repeat(128 * 1024)),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    let first = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(first.rows_deleted, 1);
    assert!(first.source_bytes <= RECEIPT_CLEANUP_MAX_SOURCE_BYTES as u64);
    assert_eq!(boundary(&store, "turn").await, 1);
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        1
    );
}

#[tokio::test]
async fn waits_for_backfill_and_requires_maintenance_reads_and_writes() {
    let store = store().await;
    seed(&store, "turn", 2, 2).await;
    assert!(
        !store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .backfill_ready
    );
    assert_eq!(boundary(&store, "turn").await, 0);
    ready(&store).await;
    let wrong_scope = store.with_maintenance_reads_and_critical_writes();
    assert!(
        wrong_scope
            .cleanup_projection_receipts_quantum(None)
            .await
            .is_err()
    );
    assert_eq!(
        receipt::Entity::find()
            .count(&store.database_connection())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        2
    );
}

#[tokio::test]
async fn bounded_cleanup_resumes_and_observes_the_retained_suffix() {
    let store = store().await;
    seed(&store, "turn", 130, 130).await;
    ready(&store).await;
    let first = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(first.rows_deleted, RECEIPT_CLEANUP_MAX_ROWS);
    assert!(first.source_bytes <= RECEIPT_CLEANUP_MAX_SOURCE_BYTES as u64);
    assert_eq!(boundary(&store, "turn").await, 128);
    let db = store.database_connection();
    let observed =
        projections::backfill_projected_watermark(&db, "turn", chrono::Utc::now().fixed_offset())
            .await
            .unwrap();
    assert_eq!(observed.observed_projected_through_sequence, 130);
    assert!(observed.matches());
    assert!(!observed.watermark_advanced);
    let restarted = CrudStore::new(db.clone()).with_maintenance_access();
    assert_eq!(
        restarted
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        2
    );
    assert_eq!(
        restarted
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        0
    );
    assert_eq!(boundary(&restarted, "turn").await, 130);
    assert!(
        projections::backfill_projected_watermark(&db, "turn", chrono::Utc::now().fixed_offset())
            .await
            .unwrap()
            .matches()
    );
}

#[tokio::test]
async fn legacy_gap_is_not_bridged_by_compaction_or_restart_observation() {
    let store = store().await;
    seed(&store, "turn", 131, 129).await;
    let db = store.database_connection();
    receipt::Entity::delete_by_id("turn-130")
        .exec(&db)
        .await
        .unwrap();
    ready(&store).await;
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        128
    );
    // Reconstructing the store discards any in-memory traversal state.
    let restarted = CrudStore::new(db.clone()).with_maintenance_access();
    assert_eq!(
        restarted
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        1
    );
    assert_eq!(
        restarted
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        0
    );
    assert_eq!(boundary(&restarted, "turn").await, 129);
    assert!(
        receipt::Entity::find_by_id("turn-131")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    let observation =
        projections::backfill_projected_watermark(&db, "turn", chrono::Utc::now().fixed_offset())
            .await
            .unwrap();
    assert!(observation.matches());
    assert_eq!(observation.stored_projected_through_sequence, 129);
    assert!(
        projections::is_projected(&db, "turn-1", "turn", 1)
            .await
            .unwrap()
    );
    assert!(
        !projections::is_projected(&db, "turn-131", "turn", 131)
            .await
            .unwrap()
    );
    assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 131);
}

#[tokio::test]
async fn unfinished_receipts_and_quarantined_streams_survive_without_stalling_other_streams() {
    for status in ["pending", "projecting", "failed", "exhausted"] {
        let store = store().await;
        seed(&store, "a", 3, 3).await;
        seed(&store, "b", 1, 1).await;
        let db = store.database_connection();
        receipt::Entity::update_many()
            .col_expr(receipt::Column::Status, Expr::value(status))
            .filter(receipt::Column::EventId.eq("a-2"))
            .exec(&db)
            .await
            .unwrap();
        ready(&store).await;
        let first = store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap();
        assert_eq!(first.rows_deleted, 1);
        assert!(first.deferred);
        let second = store
            .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref())
            .await
            .unwrap();
        assert_eq!(second.rows_deleted, 1);
        assert_eq!(boundary(&store, "a").await, 1);
        assert_eq!(
            receipt::Entity::find_by_id("a-2")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            status
        );
        assert!(
            receipt::Entity::find_by_id("a-3")
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }
    let store = store().await;
    seed(&store, "quarantined", 1, 1).await;
    stream::Entity::update_many()
        .col_expr(stream::Column::Status, Expr::value("quarantined"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    ready(&store).await;
    let outcome = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert!(outcome.deferred);
    assert_eq!(outcome.rows_deleted, 0);
}

#[tokio::test]
async fn oversized_receipt_and_canonical_gap_are_deferred() {
    let store = store().await;
    seed(&store, "a", 3, 3).await;
    seed(&store, "b", 1, 1).await;
    let db = store.database_connection();
    receipt::Entity::update_many()
        .col_expr(
            receipt::Column::ProjectionContextJson,
            Expr::value("x".repeat(RECEIPT_CLEANUP_MAX_SOURCE_BYTES as usize)),
        )
        .filter(receipt::Column::EventId.eq("a-1"))
        .exec(&db)
        .await
        .unwrap();
    ready(&store).await;
    let first = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert!(first.deferred);
    assert_eq!(first.rows_deleted, 0);
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref())
            .await
            .unwrap()
            .rows_deleted,
        1
    );
    receipt::Entity::update_many()
        .col_expr(receipt::Column::ProjectionContextJson, Expr::value("{}"))
        .filter(receipt::Column::EventId.eq("a-1"))
        .exec(&db)
        .await
        .unwrap();
    turn_event::Entity::delete_by_id("a-2")
        .exec(&db)
        .await
        .unwrap();
    let gap = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(gap.rows_deleted, 1);
    assert!(gap.deferred);
    assert_eq!(boundary(&store, "a").await, 1);
}

#[tokio::test]
async fn stale_preparation_is_revalidated_under_the_writer() {
    for mutation in [
        "UPDATE turn_event_projection_state SET status = 'failed' WHERE event_id = 'turn-2'",
        "UPDATE turn_event_projection_stream_state SET projected_through_sequence = 1 WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_stream_state SET status = 'quarantined' WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_state SET thread_id = 'other' WHERE event_id = 'turn-2'",
        "UPDATE turn_event SET sequence = 4 WHERE id = 'turn-2'",
        "UPDATE turn_event_projection_state SET projection_context_json = 'changed' WHERE event_id = 'turn-2'",
    ] {
        let store = store().await;
        seed(&store, "turn", 2, 2).await;
        ready(&store).await;
        let db = store.database_connection();
        let prepared = prepare(&db, next_stream(&db, None).await.unwrap().unwrap())
            .await
            .unwrap();
        db.execute_unprepared(mutation).await.unwrap();
        let transaction = db.begin().await.unwrap();
        assert_eq!(apply(&transaction, &prepared).await.unwrap(), 0);
        transaction.commit().await.unwrap();
        assert_eq!(boundary(&store, "turn").await, 0);
        assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 2);
    }
}

#[tokio::test]
async fn boundary_failure_rolls_back_deletion_and_does_not_starve_next_stream() {
    let store = store().await;
    seed(&store, "a", 2, 2).await;
    seed(&store, "b", 1, 1).await;
    ready(&store).await;
    let db = store.database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_cleanup_boundary BEFORE UPDATE OF receipts_compacted_through_sequence ON turn_event_projection_stream_state WHEN NEW.turn_id = 'a' BEGIN SELECT RAISE(ABORT, 'injected failure'); END").await.unwrap();
    let first = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert!(first.failed);
    assert_eq!(first.rows_deleted, 0);
    assert_eq!(boundary(&store, "a").await, 0);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 3);
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref())
            .await
            .unwrap()
            .rows_deleted,
        1
    );
}

#[tokio::test]
async fn new_claims_and_atomic_watermark_updates_continue_after_all_receipts_are_compacted() {
    let store = store().await;
    seed(&store, "turn", 2, 2).await;
    ready(&store).await;
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        2
    );
    insert_event(&store, "turn", 3, "pending").await;
    insert_event(&store, "turn", 4, "pending").await;
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    let claimed = projections::claim_due(&db, now, now + chrono::Duration::minutes(1), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].state.sequence, 3);
    let transaction = db.begin().await.unwrap();
    assert!(
        projections::mark_projected_claimed(
            &transaction,
            "turn-3",
            "turn",
            3,
            &claimed[0].claim_token,
            now
        )
        .await
        .unwrap()
    );
    transaction.commit().await.unwrap();
    assert!(
        projections::backfill_projected_watermark(&db, "turn", now)
            .await
            .unwrap()
            .matches()
    );
    assert!(
        projections::has_unprojected_event(&db, "turn")
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        1
    );
    let next = projections::claim_due(&db, now, now + chrono::Duration::minutes(1), 10)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].state.sequence, 4);
}

#[tokio::test]
async fn cleanup_reads_canonical_keys_through_the_production_zstd_view() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let store = store().await;
    seed(&store, "turn", 3, 3).await;
    ready(&store).await;
    let db = store.database_connection();
    db.query_one_write_raw(Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT zstd_enable_transparent(?)",
        [serde_json::json!({
            "table": "turn_event", "column": "payload", "compression_level": 3,
            "dict_chooser": "'[nodict]'",
        })
        .to_string()
        .into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        3
    );
    assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
    assert!(
        projections::backfill_projected_watermark(&db, "turn", chrono::Utc::now().fixed_offset())
            .await
            .unwrap()
            .matches()
    );
}

#[derive(Default)]
struct WriteEvents(std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>);

impl pioneer_sqlite::SqliteWriteObserver for WriteEvents {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn discovery_uses_read_only_pool_and_cancelled_writer_wait_does_not_block_interactive_work() {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteEvent};
    use std::{sync::Arc, time::Duration};
    let path = std::env::temp_dir().join(format!(
        "pioneer-receipt-cleanup-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1);
    let writer = Database::connect(options).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    Migrator::up(&writer, None).await.unwrap();
    let reader = Database::connect(format!("sqlite://{}?mode=ro", path.display()))
        .await
        .unwrap();
    let events = Arc::new(WriteEvents::default());
    let database = SqliteDatabase::new_with_observer(reader, writer, events.clone());
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    ready(&store).await;
    seed(&store, "turn", 2, 2).await;
    let blocker = database.begin().await.unwrap();
    events.0.lock().unwrap().clear();

    // Exhausted discovery must finish while the only writer is occupied.
    let empty = tokio::time::timeout(
        Duration::from_secs(1),
        store.cleanup_projection_receipts_quantum(Some("zzzz")),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(empty.last_turn_id.is_none());
    assert!(events.0.lock().unwrap().is_empty());

    let mut work = Box::pin(store.cleanup_projection_receipts_quantum(None));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            tokio::select! {
                result = &mut work => panic!("cleanup acquired an occupied writer: {result:?}"),
                _ = tokio::task::yield_now() => {},
            }
            if events.0.lock().unwrap().iter().any(|event| {
                matches!(
                    event,
                    SqliteWriteEvent::Enqueued {
                        class: SqliteWriteClass::Maintenance,
                        ..
                    }
                )
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(work);
    assert!(events.0.lock().unwrap().iter().any(|event| matches!(event,
        SqliteWriteEvent::Cancelled {class: SqliteWriteClass::Maintenance, queue, ..} if queue.maintenance == 0
    )));
    blocker.rollback().await.unwrap();
    assert_eq!(boundary(&store, "turn").await, 0);
    assert_eq!(receipt::Entity::find().count(&database).await.unwrap(), 2);
    let interactive = database.begin();
    let transaction = tokio::time::timeout(Duration::from_secs(1), interactive)
        .await
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            store.cleanup_projection_receipts_quantum(None)
        )
        .await
        .unwrap()
        .unwrap()
        .rows_deleted,
        2
    );
    drop(store);
    database.close().await.unwrap();
    let _ = std::fs::remove_file(path);
}
