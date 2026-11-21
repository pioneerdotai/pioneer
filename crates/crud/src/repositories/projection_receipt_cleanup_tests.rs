use super::projection_receipt_cleanup::*;
use super::{
    turn_event_projection_state as projections, turn_event_projection_stream_state as streams,
};
use crate::CrudStore;
use pioneer_entity::{turn, turn_event};
use pioneer_entity::{
    turn_event_projection_state as receipt, turn_event_projection_stream_state as stream_entity,
};
use pioneer_postgres_test::{TestDatabase, with_database};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter, QueryTrait,
    Statement, TransactionTrait,
};

const COMPACTED_HISTORY_TURNS: u64 = 20_000;

async fn seed_scope(database: &sea_orm::DatabaseConnection) {
    database
        .execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Projection receipts');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'ws', '', 'agent', 'm', 'p', 'active')",
        )
        .await
        .unwrap();
}

async fn open(fixture: &TestDatabase) -> CrudStore {
    CrudStore::new(pioneer_sqlite::SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connect().await.unwrap(),
    ))
    .with_maintenance_access()
}

async fn store(fixture: &TestDatabase) -> CrudStore {
    seed_scope(fixture.connection()).await;
    open(fixture).await
}

async fn write_fixture<T>(
    database: &pioneer_sqlite::SqliteDatabase,
    operation: impl AsyncFnOnce(&pioneer_sqlite::SqliteTransaction) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let tx = database.begin().await?;
    let value = operation(&tx).await?;
    tx.commit().await?;
    Ok(value)
}

async fn seed(store: &CrudStore, turn_id: &str, count: i64, watermark: i64) {
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
        id: sea_orm::Set(turn_id.into()),
        thread_id: sea_orm::Set("thread".into()),
        status: sea_orm::Set("completed".into()),
        prompt_manifest_json: sea_orm::Set("{}".into()),
        mentions_json: sea_orm::Set("[]".into()),
        created_at: sea_orm::Set(now),
        updated_at: sea_orm::Set(now),
        ..Default::default()
    })
    .exec(&db)
    .await
    .unwrap();
    write_fixture(&db, async |tx| {
        streams::ensure_healthy(tx, "thread", turn_id, now).await
    })
    .await
    .unwrap();
    for sequence in 1..=count {
        insert_event(store, turn_id, sequence, "projected").await;
    }
    if watermark > 0 {
        assert!(
            write_fixture(&db, async |tx| streams::advance_projected_through(
                tx, turn_id, 0, watermark, now
            )
            .await)
            .await
            .unwrap()
        );
    }
}

async fn insert_event(store: &CrudStore, turn_id: &str, sequence: i64, status: &str) {
    let db = store.database_connection();
    let event_id = format!("{turn_id}-{sequence}");
    db.execute_raw(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) VALUES ($1, 'thread', $2, $3, 'test', '{}', CURRENT_TIMESTAMP)",
        [event_id.clone().into(), turn_id.to_owned().into(), sequence.into()],
    )).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO turn_event_projection_state(event_id, thread_id, turn_id, sequence, status, next_run_at, projection_context_json, created_at, updated_at) VALUES ($1, 'thread', $2, $3, $4, CURRENT_TIMESTAMP, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
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

async fn watermark(db: &pioneer_sqlite::SqliteDatabase, turn_id: &str) -> i64 {
    stream_entity::Entity::find_by_id(turn_id)
        .one(db)
        .await
        .unwrap()
        .unwrap()
        .projected_through_sequence
}

async fn seed_compacted_history(store: &CrudStore) {
    // Retain canonical events, with no old receipts and equal durable boundaries.
    // Bulk insertion is fixture setup only, never a production history backfill.
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO turn(id, thread_id, status, turn_kind, origin)
         SELECT 'history-' || lpad(n::text, 5, '0'), 'thread', 'completed', 'conversation', 'user'
         FROM generate_series(0, {COMPACTED_HISTORY_TURNS} - 1) AS history(n);
         INSERT INTO turn_event_projection_stream_state(turn_id, thread_id, status, projected_through_sequence, receipts_compacted_through_sequence, created_at, updated_at)
         SELECT id, thread_id, 'healthy', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM turn;
         INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at)
         SELECT turn_id || '-1', thread_id, turn_id, 1, 'test', '{{}}', CURRENT_TIMESTAMP FROM turn_event_projection_stream_state;"
    )).await.unwrap();
}

async fn discovered_turn<C: ConnectionTrait>(db: &C, after: Option<&str>) -> Option<String> {
    next_stream(db, after, None)
        .await
        .unwrap()
        .map(|stream| stream.turn_id)
}

#[tokio::test]
async fn discovery_seeks_only_work_in_keyset_order_and_uses_the_partial_index() {
    with_database(async |fixture| {
        let store = store(fixture).await;
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
        write_fixture(&db, async |tx| {
            streams::quarantine(
                tx,
                "thread",
                "history-00000-quarantined",
                "blocked",
                "protected".into(),
                chrono::Utc::now().fixed_offset(),
            )
            .await
        })
        .await
        .unwrap();

        let upper = store
            .projection_receipt_cleanup_upper_bound()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(upper, candidates[2]);

        // The partial index must exclude the large compacted prefix before PostgreSQL orders the page.
        for after in [None, Some(candidates[0])] {
            let mut statement = discovery_query(after, None, false).build(DbBackend::Postgres);
            statement.sql = format!("EXPLAIN {}", statement.sql);
            let plan = db
                .query_all_raw(statement)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
                .collect::<Vec<_>>();
            assert!(
                plan.iter()
                    .any(|line| line.contains("idx_turn_event_projection_stream_cleanup_work")),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|line| line.contains("Seq Scan on turn_event_projection_stream_state")),
                "{plan:?}"
            );
            if after.is_some() {
                assert!(
                    plan.iter()
                        .any(|line| line.contains("Index Cond") && line.contains("turn_id >")),
                    "{plan:?}"
                );
            }
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn index_membership_follows_projection_cleanup_quarantine_and_restore() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 0, 0).await;
        let db = store.database_connection();
        assert!(discovered_turn(&db, None).await.is_none());
        insert_event(&store, "turn", 1, "pending").await;
        assert!(discovered_turn(&db, None).await.is_none());
        let empty = store
            .cleanup_projection_receipts_quantum(None, None)
            .await
            .unwrap();
        assert!(empty.last_turn_id.is_none());
        assert_eq!(empty.rows_deleted, 0);
        assert_eq!(boundary(&store, "turn").await, 0);

        let now = chrono::Utc::now().fixed_offset();
        let claimed = write_fixture(&db, async |tx| {
            projections::claim_due(tx, now, now + chrono::Duration::minutes(1), 10).await
        })
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
            write_fixture(&db, async |tx| streams::quarantine(
                tx,
                "thread",
                "turn",
                "turn-2",
                "blocked".into(),
                now
            )
            .await)
            .await
            .unwrap()
        );
        assert!(discovered_turn(&db, None).await.is_none());
        assert!(
            write_fixture(&db, async |tx| streams::restore(tx, "turn", "turn-2", now)
                .await)
            .await
            .unwrap()
        );
        assert_eq!(discovered_turn(&db, None).await.as_deref(), Some("turn"));
        let cleaned = store
            .cleanup_projection_receipts_quantum(None, None)
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn discovery_excludes_invalid_compaction_boundaries_and_preserves_receipts() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 1, 1).await;
        let db = store.database_connection();
        for floor in [-1_i64, 2] {
            stream_entity::Entity::update_many()
                .col_expr(
                    stream_entity::Column::ReceiptsCompactedThroughSequence,
                    Expr::value(floor),
                )
                .exec(&db)
                .await
                .unwrap();
            assert!(discovered_turn(&db, None).await.is_none());
            let empty = store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap();
            assert!(empty.last_turn_id.is_none());
            assert_eq!(empty.rows_deleted, 0);
            assert_eq!(boundary(&store, "turn").await, floor);
            assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 1);
            assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 1);
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_cleanup_resumes_and_observes_the_retained_suffix() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 130, 130).await;
        let first = store
            .cleanup_projection_receipts_quantum(None, None)
            .await
            .unwrap();
        assert_eq!(first.rows_deleted, RECEIPT_CLEANUP_MAX_ROWS);
        assert!(first.source_bytes <= RECEIPT_CLEANUP_MAX_SOURCE_BYTES as u64);
        assert_eq!(boundary(&store, "turn").await, 128);
        let db = store.database_connection();
        assert_eq!(watermark(&db, "turn").await, 130);
        let restarted = CrudStore::new(db.clone()).with_maintenance_access();
        assert_eq!(
            restarted
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            2
        );
        let empty = restarted
            .cleanup_projection_receipts_quantum(None, None)
            .await
            .unwrap();
        assert_eq!(empty.rows_deleted, 0);
        assert!(empty.last_turn_id.is_none());
        assert_eq!(boundary(&restarted, "turn").await, 130);
        assert_eq!(watermark(&db, "turn").await, 130);
        assert!(
            projections::is_projected(&db, "turn-130", "turn", 130)
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receipt_gap_is_not_bridged_by_cleanup_or_restart() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 131, 129).await;
        let db = store.database_connection();
        receipt::Entity::delete_by_id("turn-130")
            .exec(&db)
            .await
            .unwrap();
        assert_eq!(
            store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            128
        );
        // Reconstructing the store discards any in-memory traversal state.
        let restarted = CrudStore::new(db.clone()).with_maintenance_access();
        assert_eq!(
            restarted
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        assert_eq!(
            restarted
                .cleanup_projection_receipts_quantum(None, None)
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
        assert_eq!(watermark(&db, "turn").await, 129);
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unfinished_receipts_and_quarantined_streams_survive_without_stalling_other_streams() {
    for status in ["pending", "projecting", "failed", "exhausted"] {
        with_database(async |fixture| {
            let store = store(fixture).await;
            seed(&store, "a", 3, 3).await;
            seed(&store, "b", 1, 1).await;
            let db = store.database_connection();
            receipt::Entity::update_many()
                .col_expr(receipt::Column::Status, Expr::value(status))
                .filter(receipt::Column::EventId.eq("a-2"))
                .exec(&db)
                .await
                .unwrap();
            let first = store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap();
            assert_eq!(first.rows_deleted, 1);
            assert!(first.deferred);
            let second = store
                .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref(), None)
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
            Ok(())
        })
        .await
        .unwrap();
    }
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "quarantined", 1, 1).await;
        stream_entity::Entity::update_many()
            .col_expr(stream_entity::Column::Status, Expr::value("quarantined"))
            .exec(&store.database_connection())
            .await
            .unwrap();
        let outcome = store
            .cleanup_projection_receipts_quantum(None, None)
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn oversized_receipt_and_canonical_gap_are_deferred() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "a", 3, 3).await;
        seed(&store, "b", 1, 1).await;
        let db = store.database_connection();
        receipt::Entity::update_many()
            .col_expr(
                receipt::Column::ProjectionContextJson,
                Expr::value(serde_json::to_string(
                    &"x".repeat(RECEIPT_CLEANUP_MAX_SOURCE_BYTES as usize - 2),
                )?),
            )
            .filter(receipt::Column::EventId.eq("a-1"))
            .exec(&db)
            .await
            .unwrap();
        let first = store
            .cleanup_projection_receipts_quantum(None, None)
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
                .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref(), None)
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
            write_fixture(&db, async |tx| streams::advance_projected_through(
                tx,
                "b",
                1,
                2,
                chrono::Utc::now().fixed_offset()
            )
            .await)
            .await
            .unwrap()
        );
        let gap = store
            .cleanup_projection_receipts_quantum(None, None)
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
            .cleanup_projection_receipts_quantum(gap.last_turn_id.as_deref(), None)
            .await
            .unwrap();
        assert_eq!(next.last_turn_id.as_deref(), Some("b"));
        assert_eq!(next.rows_deleted, 1);
        assert_eq!(boundary(&store, "a").await, 1);
        assert_eq!(boundary(&store, "b").await, 2);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stale_preparation_is_revalidated_under_the_writer() {
    for mutation in [
        "UPDATE turn_event_projection_state SET status = 'failed' WHERE event_id = 'turn-2'",
        "UPDATE turn_event_projection_stream_state SET projected_through_sequence = 1 WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_stream_state SET status = 'quarantined' WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other' WHERE turn_id = 'turn'",
        "UPDATE turn_event_projection_state SET thread_id = 'other' WHERE event_id = 'turn-2'",
        "UPDATE turn_event SET sequence = 4 WHERE id = 'turn-2'",
        r#"UPDATE turn_event_projection_state SET projection_context_json = '{"changed":true}' WHERE event_id = 'turn-2'"#,
    ] {
        with_database(async |fixture| {
            let store = store(fixture).await;
            seed(&store, "turn", 2, 2).await;
            let db = store.database_connection();
            let prepared = prepare(&db, next_stream(&db, None, None).await.unwrap().unwrap())
                .await
                .unwrap();
            db.execute_unprepared(mutation).await.unwrap();
            let transaction = db.begin().await.unwrap();
            assert_eq!(apply(&transaction, &prepared).await.unwrap(), 0);
            transaction.commit().await.unwrap();
            assert_eq!(boundary(&store, "turn").await, 0);
            assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 2);
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn boundary_failure_rolls_back_deletion_and_does_not_starve_next_stream() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "a", 2, 2).await;
        seed(&store, "b", 1, 1).await;
        let db = store.database_connection();
        db.execute_unprepared(r#"
            CREATE FUNCTION reject_cleanup_boundary() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected failure'; END; $$;
            CREATE TRIGGER reject_cleanup_boundary BEFORE UPDATE OF receipts_compacted_through_sequence
            ON turn_event_projection_stream_state FOR EACH ROW WHEN (NEW.turn_id = 'a')
            EXECUTE FUNCTION reject_cleanup_boundary();
        "#).await.unwrap();
        let first = store
            .cleanup_projection_receipts_quantum(None, None)
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
                .cleanup_projection_receipts_quantum(first.last_turn_id.as_deref(), None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        db.execute_unprepared("DROP TRIGGER reject_cleanup_boundary ON turn_event_projection_stream_state")
            .await
            .unwrap();
        let restarted = CrudStore::new(db.clone()).with_maintenance_access();
        let retried = restarted
            .cleanup_projection_receipts_quantum(None, None)
            .await
            .unwrap();
        assert_eq!(retried.last_turn_id.as_deref(), Some("a"));
        assert_eq!(retried.rows_deleted, 2);
        assert_eq!(boundary(&restarted, "a").await, 2);
        assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 0);
        assert!(discovered_turn(&db, None).await.is_none());
        assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn concurrent_projection_and_repeated_preparation_preserve_the_new_suffix() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 2, 2).await;
        let db = store.database_connection();
        let prepared = prepare(&db, next_stream(&db, None, None).await.unwrap().unwrap())
            .await
            .unwrap();
        insert_event(&store, "turn", 3, "pending").await;
        let now = chrono::Utc::now().fixed_offset();
        let claimed = write_fixture(&db, async |tx| {
            projections::claim_due(tx, now, now + chrono::Duration::minutes(1), 10).await
        })
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
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        assert_eq!(boundary(&restarted, "turn").await, 3);
        assert!(discovered_turn(&db, None).await.is_none());
        assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn new_claims_and_atomic_watermark_updates_continue_after_all_receipts_are_compacted() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 2, 2).await;
        assert_eq!(
            store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            2
        );
        insert_event(&store, "turn", 3, "pending").await;
        insert_event(&store, "turn", 4, "pending").await;
        let db = store.database_connection();
        let now = chrono::Utc::now().fixed_offset();
        let claimed = write_fixture(&db, async |tx| {
            projections::claim_due(tx, now, now + chrono::Duration::minutes(1), 10).await
        })
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
        assert_eq!(watermark(&db, "turn").await, 3);
        assert!(
            projections::has_unprojected_event(&db, "turn")
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        let next = write_fixture(&db, async |tx| {
            projections::claim_due(tx, now, now + chrono::Duration::minutes(1), 10).await
        })
        .await
        .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].state.sequence, 4);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cleanup_reads_canonical_keys_with_portable_event_bodies() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 3, 3).await;
        let db = store.database_connection();
        let payload = serde_json::json!({"text":"canonical receipt".repeat(5_000)}).to_string();
        for sequence in 1..=3 {
            let prepared = pioneer_crud_payloads::storage::PreparedPayload::new(&payload).unwrap();
            let transaction = db.begin().await.unwrap();
            prepared.insert(&transaction).await.unwrap();
            transaction
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE turn_event SET payload = NULL, payload_payload_id = $1 WHERE id = $2",
                    [prepared.id().into(), format!("turn-{sequence}").into()],
                ))
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
        assert_eq!(
            store
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            3
        );
        assert_eq!(turn_event::Entity::find().count(&db).await.unwrap(), 3);
        assert_eq!(watermark(&db, "turn").await, 3);
        for sequence in 1..=3 {
            assert!(
                projections::is_projected(&db, &format!("turn-{sequence}"), "turn", sequence)
                    .await
                    .unwrap()
            );
        }
        Ok(())
    })
    .await
    .unwrap();
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
    with_database(async |fixture| {
        use pioneer_sqlite::{
            SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteClass,
            SqliteWriteEvent, SqliteWriteExecutor,
        };
        use std::{sync::Arc, time::Duration};
        seed_scope(fixture.connection()).await;
        let writer = fixture.connect_with_limit(1).await.unwrap();
        let reader = fixture.connect_read_only().await.unwrap();
        let events = Arc::new(WriteEvents::default());
        let reads = Arc::new(ReadEvents::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, events.clone()),
            reads.clone(),
        );
        let store = CrudStore::new(database.clone()).with_maintenance_access();
        store.database_connection().validate_reader().await.unwrap();
        seed_compacted_history(&store).await;
        let blocker = database.begin().await.unwrap();
        events.0.lock().unwrap().clear();
        reads.0.lock().unwrap().clear();

        // With only cleaned history, the indexed discovery is the only read.
        // It can finish even while the writer is occupied.
        let empty = tokio::time::timeout(
            Duration::from_secs(1),
            store.cleanup_projection_receipts_quantum(None, None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(empty.last_turn_id.is_none());
        assert_eq!(empty.rows_deleted, 0);
        assert_eq!(empty.source_bytes, 0);
        assert!(!empty.deferred);
        assert!(!empty.failed);
        assert!(events.0.lock().unwrap().is_empty());
        {
            let observed = reads.0.lock().unwrap();
            assert_eq!(
                observed
                    .iter()
                    .filter(|event| matches!(event, SqliteReadEvent::OperationFinished { .. }))
                    .count(),
                1
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
                1
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
                1
            );
        }
        assert_eq!(receipt::Entity::find().count(&database).await.unwrap(), 0);
        assert_eq!(
            stream_entity::Entity::find().count(&database).await.unwrap(),
            COMPACTED_HISTORY_TURNS
        );
        assert_eq!(
            turn_event::Entity::find().count(&database).await.unwrap(),
            COMPACTED_HISTORY_TURNS
        );
        blocker.rollback().await.unwrap();
        seed(&store, "turn", 2, 2).await;
        let blocker = database.begin().await.unwrap();
        events.0.lock().unwrap().clear();

        // Exhausted discovery must finish while the only writer is occupied.
        let empty = tokio::time::timeout(
            Duration::from_secs(1),
            store.cleanup_projection_receipts_quantum(Some("zzzz"), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(empty.last_turn_id.is_none());
        assert!(events.0.lock().unwrap().is_empty());

        let mut work = Box::pin(store.cleanup_projection_receipts_quantum(None, None));
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
                store.cleanup_projection_receipts_quantum(None, None)
            )
            .await
            .unwrap()
            .unwrap()
            .rows_deleted,
            2
        );
        drop(store);
        database.close().await.unwrap();
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn running_turn_receipts_wait_for_terminal_and_resume_invalidates_preparation() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "turn", 3, 3).await;
        let db = store.database_connection();
        for status in ["in_progress", "completed", "failed", "interrupted"] {
            turn::Entity::update_many()
                .col_expr(turn::Column::Status, Expr::value(status))
                .filter(turn::Column::Id.eq("turn"))
                .exec(&db)
                .await
                .unwrap();
            assert_eq!(
                discovered_turn(&db, None).await.is_some(),
                status != "in_progress"
            );
        }
        let selected = next_stream(&db, None, None).await.unwrap().unwrap();
        let prepared = prepare(&db, selected).await.unwrap();
        // A genuine state change between reader preparation and writer apply.
        turn::Entity::update_many()
            .col_expr(turn::Column::Status, Expr::value("in_progress"))
            .filter(turn::Column::Id.eq("turn"))
            .exec(&db)
            .await
            .unwrap();
        let tx = db.begin().await.unwrap();
        assert_eq!(apply(&tx, &prepared).await.unwrap(), 0);
        tx.commit().await.unwrap();
        assert_eq!(boundary(&store, "turn").await, 0);
        assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 3);
        turn::Entity::update_many()
            .col_expr(turn::Column::Status, Expr::value("completed"))
            .filter(turn::Column::Id.eq("turn"))
            .exec(&db)
            .await
            .unwrap();
        let restarted = CrudStore::new(db.clone()).with_maintenance_access();
        assert_eq!(
            restarted
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            3
        );
        assert_eq!(boundary(&restarted, "turn").await, 3);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn addressed_batches_freeze_the_frontier_preserve_other_turns_and_recover_on_restart() {
    with_database(async |fixture| {
        let store = store(fixture).await;
        seed(&store, "a", 130, 130).await;
        seed(&store, "b", 1, 1).await;
        let first = store
            .cleanup_projection_receipts_for_turn("a", None)
            .await
            .unwrap();
        assert_eq!(first.rows_deleted, 128);
        assert_eq!(first.through_sequence, Some(130));
        assert_eq!(boundary(&store, "b").await, 0);
        insert_event(&store, "a", 131, "projected").await;
        let db = store.database_connection();
        assert!(
            write_fixture(&db, async |tx| streams::advance_projected_through(
                tx,
                "a",
                130,
                131,
                chrono::Utc::now().fixed_offset()
            )
            .await)
            .await
            .unwrap()
        );
        let second = store
            .cleanup_projection_receipts_for_turn("a", first.through_sequence)
            .await
            .unwrap();
        assert_eq!(second.rows_deleted, 2);
        assert_eq!(boundary(&store, "a").await, 130);
        assert_eq!(
            store
                .cleanup_projection_receipts_for_turn("a", first.through_sequence)
                .await
                .unwrap()
                .rows_deleted,
            0
        );
        assert!(
            receipt::Entity::find_by_id("a-131")
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        // A later notification opens a new frontier for the same ID, not an
        // inventory of B or any other old stream.
        assert_eq!(
            store
                .cleanup_projection_receipts_for_turn("a", None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        assert_eq!(boundary(&store, "b").await, 0);
        let restarted = open(fixture).await;
        // Process-local hints may disappear on restart; durable recovery is enough.
        assert_eq!(
            restarted
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .last_turn_id
                .as_deref(),
            Some("b")
        );
        assert_eq!(boundary(&restarted, "b").await, 1);
        Ok(())
    })
    .await
    .unwrap();
}
