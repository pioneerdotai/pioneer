use super::super::{
    turn_event_projection_state as projections, turn_event_projection_stream_state as streams,
};
use super::*;
use crate::{CrudStore, ProjectionMetaRecord, upsert_projection_meta};
use migration::{Migrator, MigratorTrait};
use pioneer_entity::turn_event;
use sea_orm::{Database, DbBackend, PaginatorTrait, TransactionTrait};

const COMPACTED_HISTORY_TURNS: u64 = 20_000;

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

async fn seed_compacted_history(store: &CrudStore) {
    // Retain canonical events, with no old receipts and equal durable boundaries.
    // Bulk insertion is fixture setup only, never a production history backfill.
    store.database_connection().execute_unprepared(&format!(
        "WITH RECURSIVE history(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM history WHERE n + 1 < {COMPACTED_HISTORY_TURNS})
         INSERT INTO turn_event_projection_stream_state(turn_id, thread_id, status, projected_through_sequence, receipts_compacted_through_sequence, created_at, updated_at)
         SELECT printf('history-%05d', n), 'thread', 'healthy', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM history;
         INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at)
         SELECT turn_id || '-1', thread_id, turn_id, 1, 'test', '{{}}', CURRENT_TIMESTAMP FROM turn_event_projection_stream_state;"
    )).await.unwrap();
}

async fn discovered_turn<C: ConnectionTrait>(db: &C, after: Option<&str>) -> Option<String> {
    next_stream(db, after)
        .await
        .unwrap()
        .map(|stream| stream.turn_id)
}

#[tokio::test]
async fn fully_compacted_history_has_no_discovery_candidates() {
    let store = store().await;
    seed_compacted_history(&store).await;
    ready(&store).await;
    let db = store.database_connection();
    assert_eq!(
        stream::Entity::find().count(&db).await.unwrap(),
        COMPACTED_HISTORY_TURNS
    );
    assert!(discovered_turn(&db, None).await.is_none());
    assert!(discovered_turn(&db, Some("history-10000")).await.is_none());
    let outcome = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert!(outcome.backfill_ready);
    assert!(outcome.last_turn_id.is_none());
    assert_eq!(outcome.rows_deleted, 0);
    assert_eq!(outcome.source_bytes, 0);
    assert!(!outcome.deferred);
    assert!(!outcome.failed);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        turn_event::Entity::find().count(&db).await.unwrap(),
        COMPACTED_HISTORY_TURNS
    );
}

#[tokio::test]
async fn discovery_seeks_only_work_in_keyset_order_and_uses_the_partial_index() {
    let store = store().await;
    seed_compacted_history(&store).await;
    let candidates = [
        "history-00010-work",
        "history-10000-work",
        "history-19999-work",
    ];
    for turn_id in candidates {
        seed(&store, turn_id, 2, 2).await;
    }
    seed(&store, "history-00000-quarantined", 1, 1).await;
    let db = store.database_connection();
    streams::quarantine(
        &db,
        "thread",
        "history-00000-quarantined",
        "blocked",
        "protected".into(),
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .unwrap();
    ready(&store).await;

    // Check the access path against the current schema without pinning names.
    let partial_indexes = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM pragma_index_list('turn_event_projection_stream_state') WHERE partial = 1",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "name").unwrap())
        .collect::<Vec<_>>();
    let uses_partial_index = |detail: &str| {
        detail.split_once(" INDEX ").is_some_and(|(_, access)| {
            partial_indexes
                .iter()
                .any(|name| access == name || access.starts_with(&format!("{name} (")))
        })
    };

    // Explain both production statements with their original bind values.
    for after in [None, Some(candidates[0])] {
        let mut statement = discovery_statement(after);
        assert!(statement.sql.contains("status = 'healthy'"));
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        let access = if after.is_some() { "SEARCH " } else { "SCAN " };
        assert!(
            plan.iter()
                .any(|detail| detail.starts_with(access) && uses_partial_index(detail)),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("TEMP B-TREE")
                || (detail.starts_with("SCAN ") && !uses_partial_index(detail))),
            "{plan:?}"
        );
        if after.is_some() {
            assert!(
                plan.iter().any(|detail| detail.contains("turn_id>?")),
                "{plan:?}"
            );
        }
    }

    assert_eq!(
        discovered_turn(&db, Some("history-05000")).await.as_deref(),
        Some(candidates[1])
    );
    let mut after = None::<String>;
    for expected in candidates {
        assert_eq!(
            discovered_turn(&db, after.as_deref()).await.as_deref(),
            Some(expected)
        );
        let outcome = store
            .cleanup_projection_receipts_quantum(after.as_deref())
            .await
            .unwrap();
        assert_eq!(outcome.last_turn_id.as_deref(), Some(expected));
        assert_eq!(outcome.rows_deleted, 2);
        assert!(!outcome.deferred);
        assert!(!outcome.failed);
        after = outcome.last_turn_id;
    }
    assert!(discovered_turn(&db, after.as_deref()).await.is_none());
    // The next pass starts at the beginning, without walking cleaned history.
    assert!(discovered_turn(&db, None).await.is_none());
    assert_eq!(boundary(&store, "history-00000-quarantined").await, 0);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 1);
    assert_eq!(
        turn_event::Entity::find().count(&db).await.unwrap(),
        COMPACTED_HISTORY_TURNS + 7
    );
}

#[tokio::test]
async fn index_membership_follows_projection_cleanup_quarantine_and_restore() {
    let store = store().await;
    seed(&store, "turn", 0, 0).await;
    ready(&store).await;
    let db = store.database_connection();
    assert!(discovered_turn(&db, None).await.is_none());
    insert_event(&store, "turn", 1, "pending").await;
    assert!(discovered_turn(&db, None).await.is_none());
    let empty = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert!(empty.last_turn_id.is_none());
    assert_eq!(empty.rows_deleted, 0);
    assert_eq!(boundary(&store, "turn").await, 0);

    let now = chrono::Utc::now().fixed_offset();
    let claimed = projections::claim_due(&db, now, now + chrono::Duration::minutes(1), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(discovered_turn(&db, None).await.is_none());
    // Projection receipt and watermark roll back together; index membership
    // rolls back with them, without any independent discovery job writes.
    for commit in [false, true] {
        let transaction = db.begin().await.unwrap();
        assert!(
            projections::mark_projected_claimed(
                &transaction,
                "turn-1",
                "turn",
                1,
                &claimed[0].claim_token,
                now
            )
            .await
            .unwrap()
        );
        assert_eq!(
            discovered_turn(&transaction, None).await.as_deref(),
            Some("turn")
        );
        if commit {
            transaction.commit().await.unwrap();
        } else {
            transaction.rollback().await.unwrap();
            assert!(discovered_turn(&db, None).await.is_none());
            assert_eq!(
                streams::find(&db, "turn")
                    .await
                    .unwrap()
                    .unwrap()
                    .projected_through_sequence,
                0
            );
            assert_eq!(
                receipt::Entity::find_by_id("turn-1")
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "projecting"
            );
        }
    }
    assert_eq!(discovered_turn(&db, None).await.as_deref(), Some("turn"));
    insert_event(&store, "turn", 2, "pending").await;
    assert!(
        streams::quarantine(&db, "thread", "turn", "turn-2", "blocked".into(), now)
            .await
            .unwrap()
    );
    assert!(discovered_turn(&db, None).await.is_none());
    assert!(streams::restore(&db, "turn", "turn-2", now).await.unwrap());
    assert_eq!(discovered_turn(&db, None).await.as_deref(), Some("turn"));
    let cleaned = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(cleaned.rows_deleted, 1);
    assert_eq!(boundary(&store, "turn").await, 1);
    assert!(discovered_turn(&db, None).await.is_none());
    assert!(
        projections::is_projected(&db, "turn-1", "turn", 1)
            .await
            .unwrap()
    );
    assert!(
        !projections::is_projected(&db, "turn-2", "turn", 2)
            .await
            .unwrap()
    );
    assert_eq!(
        receipt::Entity::find_by_id("turn-2")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 2);
}

#[tokio::test]
async fn discovery_excludes_invalid_compaction_boundaries_and_preserves_receipts() {
    let store = store().await;
    seed(&store, "turn", 1, 1).await;
    ready(&store).await;
    let db = store.database_connection();
    for floor in [-1_i64, 2] {
        stream::Entity::update_many()
            .col_expr(
                stream::Column::ReceiptsCompactedThroughSequence,
                Expr::value(floor),
            )
            .exec(&db)
            .await
            .unwrap();
        assert!(discovered_turn(&db, None).await.is_none());
        let empty = store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap();
        assert!(empty.last_turn_id.is_none());
        assert_eq!(empty.rows_deleted, 0);
        assert_eq!(boundary(&store, "turn").await, floor);
        assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 1);
        assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 1);
    }
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
    for invalid_marker in [
        "UPDATE thread_timeline_projection_meta SET projection_version = 2",
        "UPDATE thread_timeline_projection_meta SET status = 'backfilling'",
        "UPDATE thread_timeline_projection_meta SET last_error = 'injected failure'",
    ] {
        store
            .database_connection()
            .execute_unprepared(invalid_marker)
            .await
            .unwrap();
        let outcome = store
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap();
        assert!(!outcome.backfill_ready);
        assert!(outcome.last_turn_id.is_none());
        assert_eq!(outcome.rows_deleted, 0);
        assert_eq!(boundary(&store, "turn").await, 0);
        assert_eq!(
            receipt::Entity::find()
                .count(&store.database_connection())
                .await
                .unwrap(),
            2
        );
        ready(&store).await;
    }
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
    let empty = restarted
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(empty.rows_deleted, 0);
    assert!(empty.last_turn_id.is_none());
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
    assert!(outcome.last_turn_id.is_none());
    assert!(!outcome.deferred);
    assert_eq!(outcome.rows_deleted, 0);
    assert_eq!(boundary(&store, "quarantined").await, 0);
    assert_eq!(
        receipt::Entity::find()
            .count(&store.database_connection())
            .await
            .unwrap(),
        1
    );
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
    assert_eq!(first.last_turn_id.as_deref(), Some("a"));
    assert_eq!(boundary(&store, "a").await, 0);
    assert_eq!(
        receipt::Entity::find()
            .filter(receipt::Column::TurnId.eq("a"))
            .count(&db)
            .await
            .unwrap(),
        3
    );
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
    // Give B independent new work after the oversized A pass cleaned its old
    // receipt, so the gap pass must also prove cursor progress to B.
    insert_event(&store, "b", 2, "projected").await;
    assert!(
        streams::advance_projected_through(&db, "b", 1, 2, chrono::Utc::now().fixed_offset())
            .await
            .unwrap()
    );
    let gap = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(gap.rows_deleted, 1);
    assert!(gap.deferred);
    assert_eq!(boundary(&store, "a").await, 1);
    assert_eq!(gap.last_turn_id.as_deref(), Some("a"));
    assert!(
        receipt::Entity::find_by_id("a-2")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        receipt::Entity::find_by_id("a-3")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    let next = store
        .cleanup_projection_receipts_quantum(gap.last_turn_id.as_deref())
        .await
        .unwrap();
    assert_eq!(next.last_turn_id.as_deref(), Some("b"));
    assert_eq!(next.rows_deleted, 1);
    assert_eq!(boundary(&store, "a").await, 1);
    assert_eq!(boundary(&store, "b").await, 2);
}

#[tokio::test]
async fn stale_preparation_is_revalidated_under_the_writer() {
    for mutation in [
        "UPDATE thread_timeline_projection_meta SET projection_version = 2",
        "UPDATE thread_timeline_projection_meta SET status = 'backfilling'",
        "UPDATE thread_timeline_projection_meta SET last_error = 'injected failure'",
        "DELETE FROM thread_timeline_projection_meta",
        "UPDATE turn_event_projection_state SET status = 'failed' WHERE event_id = 'turn-2'",
        "UPDATE turn_event_projection_stream_state SET projected_through_sequence = 1 WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_stream_state SET status = 'quarantined' WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other' WHERE turn_id = 'turn'",
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
    assert_eq!(first.last_turn_id.as_deref(), Some("a"));
    assert_eq!(boundary(&store, "a").await, 0);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 3);
    assert_eq!(discovered_turn(&db, None).await.as_deref(), Some("a"));
    assert_eq!(
        store
            .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref())
            .await
            .unwrap()
            .rows_deleted,
        1
    );
    db.execute_unprepared("DROP TRIGGER reject_cleanup_boundary")
        .await
        .unwrap();
    let restarted = CrudStore::new(db.clone()).with_maintenance_access();
    let retried = restarted
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(retried.last_turn_id.as_deref(), Some("a"));
    assert_eq!(retried.rows_deleted, 2);
    assert_eq!(boundary(&restarted, "a").await, 2);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 0);
    assert!(discovered_turn(&db, None).await.is_none());
    assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
}

#[tokio::test]
async fn concurrent_projection_and_repeated_preparation_preserve_the_new_suffix() {
    let store = store().await;
    seed(&store, "turn", 2, 2).await;
    ready(&store).await;
    let db = store.database_connection();
    let prepared = prepare(&db, next_stream(&db, None).await.unwrap().unwrap())
        .await
        .unwrap();
    insert_event(&store, "turn", 3, "pending").await;
    let now = chrono::Utc::now().fixed_offset();
    let claimed = projections::claim_due(&db, now, now + chrono::Duration::minutes(1), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
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

    let transaction = db.begin().await.unwrap();
    assert_eq!(apply(&transaction, &prepared).await.unwrap(), 2);
    transaction.commit().await.unwrap();
    assert_eq!(boundary(&store, "turn").await, 2);
    assert_eq!(
        streams::find(&db, "turn")
            .await
            .unwrap()
            .unwrap()
            .projected_through_sequence,
        3
    );
    assert!(
        receipt::Entity::find_by_id("turn-3")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(discovered_turn(&db, None).await.as_deref(), Some("turn"));
    let transaction = db.begin().await.unwrap();
    assert_eq!(apply(&transaction, &prepared).await.unwrap(), 0);
    transaction.commit().await.unwrap();
    assert_eq!(boundary(&store, "turn").await, 2);
    assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 1);
    assert!(discovered_turn(&db, Some("turn")).await.is_none());
    let restarted = CrudStore::new(db.clone()).with_maintenance_access();
    assert_eq!(
        restarted
            .cleanup_projection_receipts_quantum(None)
            .await
            .unwrap()
            .rows_deleted,
        1
    );
    assert_eq!(boundary(&restarted, "turn").await, 3);
    assert!(discovered_turn(&db, None).await.is_none());
    assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
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

#[derive(Default)]
struct ReadEvents(std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>);

impl pioneer_sqlite::SqliteReadObserver for ReadEvents {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn discovery_uses_read_only_pool_and_cancelled_writer_wait_does_not_block_interactive_work() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteClass,
        SqliteWriteEvent, SqliteWriteExecutor,
    };
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
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(1);
    options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options).await.unwrap();
    let events = Arc::new(WriteEvents::default());
    let reads = Arc::new(ReadEvents::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, events.clone()),
        reads.clone(),
    );
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    store.database_connection().validate_reader().await.unwrap();
    ready(&store).await;
    seed_compacted_history(&store).await;
    let blocker = database.begin().await.unwrap();
    events.0.lock().unwrap().clear();
    reads.0.lock().unwrap().clear();

    // With only cleaned history, an initial page performs exactly the marker
    // read and the empty indexed discovery, even while the writer is occupied.
    let empty = tokio::time::timeout(
        Duration::from_secs(1),
        store.cleanup_projection_receipts_quantum(None),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(empty.backfill_ready);
    assert!(empty.last_turn_id.is_none());
    assert_eq!(empty.rows_deleted, 0);
    assert!(events.0.lock().unwrap().is_empty());
    {
        let observed = reads.0.lock().unwrap();
        assert_eq!(
            observed
                .iter()
                .filter(|event| matches!(event, SqliteReadEvent::OperationFinished { .. }))
                .count(),
            2
        );
        assert_eq!(
            observed
                .iter()
                .filter(|event| matches!(
                    event,
                    SqliteReadEvent::OperationFinished {
                        class: SqliteReadClass::Maintenance,
                        outcome: SqliteReadOutcome::Ok,
                        ..
                    }
                ))
                .count(),
            2
        );
        assert_eq!(
            observed
                .iter()
                .filter(|event| matches!(
                    event,
                    SqliteReadEvent::AdmissionReleased {
                        class: SqliteReadClass::Maintenance,
                        active: 0,
                        queue_depth: 0,
                        ..
                    }
                ))
                .count(),
            2
        );
    }
    blocker.rollback().await.unwrap();
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
