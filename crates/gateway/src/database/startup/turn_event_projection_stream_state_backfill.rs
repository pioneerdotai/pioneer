use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use sea_orm::ConnectionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const PROJECTION_STREAM_STATE_BACKFILL_KEY: &str = "turn_event_projection_stream_state_backfill";
const PROJECTION_STREAM_STATE_BACKFILL_VERSION: i64 = 3;
const PROJECTION_STREAM_STATE_BACKFILL_BATCH_SIZE: u64 = 32;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProjectionStreamStateBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) streams_scanned: u64,
    pub(crate) streams_quarantined: u64,
    pub(crate) orphan_streams_quarantined: u64,
    pub(crate) streams_repaired: u64,
    pub(crate) events_repaired: u64,
    pub(crate) watermarks_advanced: u64,
    pub(crate) watermark_mismatches: u64,
}

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                streams_scanned = summary.streams_scanned,
                streams_quarantined = summary.streams_quarantined,
                orphan_streams_quarantined = summary.orphan_streams_quarantined,
                streams_repaired = summary.streams_repaired,
                events_repaired = summary.events_repaired,
                watermarks_advanced = summary.watermarks_advanced,
                watermark_mismatches = summary.watermark_mismatches,
                "turn event projection stream state background backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn event projection stream state background backfill failed"
            );
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
) -> Result<ProjectionStreamStateBackfillSummary> {
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(ProjectionStreamStateBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: PROJECTION_STREAM_STATE_BACKFILL_KEY.to_owned(),
            projection_version: PROJECTION_STREAM_STATE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
    )
    .await?;

    let result =
        backfill_all_batches(crud_store, PROJECTION_STREAM_STATE_BACKFILL_BATCH_SIZE).await;
    match result {
        Ok(summary) => {
            mark_backfill_complete(&db, started_at, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_backfill_failed(&db, started_at, &error).await?;
            Err(error)
        }
    }
}

async fn backfill_all_batches(
    crud_store: &CrudStore,
    batch_size: u64,
) -> Result<ProjectionStreamStateBackfillSummary> {
    let mut summary = ProjectionStreamStateBackfillSummary::default();
    let mut after_turn_id = None;

    loop {
        let batch = crud_store
            .backfill_turn_event_projection_stream_states_batch(
                after_turn_id.as_deref(),
                batch_size,
            )
            .await?;
        if batch.streams_scanned == 0 {
            break;
        }
        let next_turn_id = batch.last_turn_id.with_context(
            || "projection stream state backfill batch did not return its keyset cursor",
        )?;
        summary.batches = summary.batches.saturating_add(1);
        summary.streams_scanned = summary
            .streams_scanned
            .saturating_add(batch.streams_scanned as u64);
        summary.streams_quarantined = summary
            .streams_quarantined
            .saturating_add(batch.streams_quarantined as u64);
        summary.orphan_streams_quarantined = summary
            .orphan_streams_quarantined
            .saturating_add(batch.orphan_streams_quarantined as u64);
        summary.streams_repaired = summary
            .streams_repaired
            .saturating_add(batch.streams_repaired as u64);
        summary.events_repaired = summary
            .events_repaired
            .saturating_add(batch.events_repaired as u64);
        summary.watermarks_advanced = summary
            .watermarks_advanced
            .saturating_add(batch.watermarks_advanced as u64);
        summary.watermark_mismatches = summary
            .watermark_mismatches
            .saturating_add(batch.watermark_mismatches as u64);
        after_turn_id = Some(next_turn_id);
        super::maintenance_checkpoint().await?;
    }

    Ok(summary)
}

async fn backfill_is_current<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, PROJECTION_STREAM_STATE_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == PROJECTION_STREAM_STATE_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete<C: ConnectionTrait>(
    db: &C,
    started_at: DateTimeWithTimeZone,
    summary: &ProjectionStreamStateBackfillSummary,
) -> Result<()> {
    let completed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: PROJECTION_STREAM_STATE_BACKFILL_KEY.to_owned(),
            projection_version: PROJECTION_STREAM_STATE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary.streams_scanned as i64,
            source_turn_item_count: 0,
            source_turn_event_count: summary.events_repaired as i64,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: Some(completed_at),
            created_at: started_at,
            updated_at: completed_at,
        },
    )
    .await
}

async fn mark_backfill_failed<C: ConnectionTrait>(
    db: &C,
    started_at: DateTimeWithTimeZone,
    error: &anyhow::Error,
) -> Result<()> {
    let failed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: PROJECTION_STREAM_STATE_BACKFILL_KEY.to_owned(),
            projection_version: PROJECTION_STREAM_STATE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(format!("{error:#}")),
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: failed_at,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::{PROJECTION_STREAM_STATE_BACKFILL_KEY, backfill_once};
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CanonicalTurnEventPayload, CrudStore, PROJECTION_META_STATUS_COMPLETE,
        PROJECTION_META_STATUS_FAILED, TurnProjectionStreamHealth, find_projection_meta,
    };
    use pioneer_protocol::{
        ItemCompletedNotification, PersistedActorRef, SandboxMode, SystemEventLevel, Thread,
        ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, Turn, TurnItem,
        TurnStatus, default_turn_permission_profile_snapshot,
    };
    use sea_orm::sea_query::Expr;
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, EntityTrait, PaginatorTrait, QueryFilter, Set,
        Statement,
    };

    #[tokio::test]
    async fn background_backfill_is_batched_idempotent_and_quarantines_only_blocked_streams() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");
        db.execute_unprepared(
            r#"
INSERT INTO turn (
    id, thread_id, status, prompt_manifest_json, created_at, updated_at,
    turn_kind, origin, mentions_json
) VALUES
    (
        'turn_poison', 'thread_poison', 'in_progress', '{}',
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'conversation', 'user', '[]'
    ),
    (
        'turn_healthy', 'thread_healthy', 'in_progress', '{}',
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'conversation', 'user', '[]'
    );
INSERT INTO turn_event_projection_state (
    event_id, thread_id, turn_id, sequence, status, attempt_count,
    last_error, next_run_at, claim_token, claim_expires_at,
    projection_context_json, projected_at, created_at, updated_at
) VALUES
    (
        'projection_poison_1', 'thread_poison', 'turn_poison', 1,
        'exhausted', 10, 'invalid projection context', CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'projection_poison_2', 'thread_poison', 'turn_poison', 2,
        'pending', 0, NULL, CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'projection_healthy_1', 'thread_healthy', 'turn_healthy', 1,
        'pending', 0, NULL, CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
"#,
        )
        .await
        .expect("legacy projection streams should insert");
        let store = CrudStore::new(db.clone());

        let partial = store
            .backfill_turn_event_projection_stream_states_batch(None, 1)
            .await
            .expect("one bounded pre-restart batch should commit");
        assert_eq!(partial.streams_scanned, 1);
        assert_eq!(partial.last_turn_id.as_deref(), Some("turn_healthy"));

        let summary = backfill_once(&store)
            .await
            .expect("background backfill should resume idempotently");
        assert!(!summary.skipped);
        assert_eq!(summary.streams_scanned, 2);
        assert_eq!(summary.streams_quarantined, 1);
        assert_eq!(summary.watermarks_advanced, 0);
        assert_eq!(summary.watermark_mismatches, 0);

        let poisoned = store
            .get_turn_event_projection_stream_state("turn_poison")
            .await
            .expect("poisoned stream state should query")
            .expect("poisoned stream should be backfilled");
        assert_eq!(poisoned.health, TurnProjectionStreamHealth::Quarantined);
        assert_eq!(poisoned.projected_through_sequence, 0);
        assert_eq!(
            poisoned.blocking_event_id.as_deref(),
            Some("projection_poison_1")
        );
        assert_eq!(
            poisoned.last_error.as_deref(),
            Some("invalid projection context")
        );

        let healthy = store
            .get_turn_event_projection_stream_state("turn_healthy")
            .await
            .expect("healthy stream state should query")
            .expect("healthy stream should be backfilled");
        assert_eq!(healthy.health, TurnProjectionStreamHealth::Healthy);
        assert_eq!(healthy.projected_through_sequence, 0);
        assert!(healthy.blocking_event_id.is_none());

        let successor =
            pioneer_entity::turn_event_projection_state::Entity::find_by_id("projection_poison_2")
                .one(&db)
                .await
                .expect("successor state should query")
                .expect("successor state should remain present");
        assert_eq!(successor.status, "pending");
        assert_eq!(successor.attempt_count, 0);
        assert!(successor.last_error.is_none());

        let marker = find_projection_meta(&db, PROJECTION_STREAM_STATE_BACKFILL_KEY)
            .await
            .expect("backfill marker should query")
            .expect("backfill marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(marker.source_turn_count, 2);
        assert!(marker.backfilled_at.is_some());

        let repeated = backfill_once(&store)
            .await
            .expect("completed background backfill should be a cheap no-op");
        assert!(repeated.skipped);
        assert_eq!(repeated.streams_scanned, 0);
        assert_eq!(
            pioneer_entity::turn_event_projection_stream_state::Entity::find()
                .all(&db)
                .await
                .expect("stream states should list")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn background_backfill_quarantines_exhausted_legacy_orphans_and_keeps_scanning() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");
        db.execute_unprepared(
            r#"
INSERT INTO turn (
    id, thread_id, status, prompt_manifest_json, created_at, updated_at,
    turn_kind, origin, mentions_json
) VALUES (
    'turn_valid_z', 'thread_valid_z', 'in_progress', '{}',
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'conversation', 'user', '[]'
);
INSERT INTO turn_event_projection_state (
    event_id, thread_id, turn_id, sequence, status, attempt_count,
    last_error, next_run_at, claim_token, claim_expires_at,
    projection_context_json, projected_at, created_at, updated_at
) VALUES
    (
        'orphan_a_1', 'thread_deleted_a', 'turn_orphan_a', 1,
        'exhausted', 10, 'canonical Turn is missing', CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'orphan_a_2', 'thread_deleted_a', 'turn_orphan_a', 2,
        'pending', 0, NULL, CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'orphan_b_1', 'thread_deleted_b', 'turn_orphan_b', 1,
        'exhausted', 10, 'canonical Turn is missing', CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'orphan_b_2', 'thread_deleted_b', 'turn_orphan_b', 2,
        'pending', 0, NULL, CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    ),
    (
        'valid_z_1', 'thread_valid_z', 'turn_valid_z', 1,
        'pending', 0, NULL, CURRENT_TIMESTAMP,
        NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    );
"#,
        )
        .await
        .expect("legacy projection streams should insert");
        let store = CrudStore::new(db.clone());

        let summary = backfill_once(&store)
            .await
            .expect("legacy orphan streams must not block healthy successors");
        assert_eq!(summary.streams_scanned, 3);
        assert_eq!(summary.streams_quarantined, 2);
        assert_eq!(summary.orphan_streams_quarantined, 2);

        for (turn_id, blocker_id) in [
            ("turn_orphan_a", "orphan_a_1"),
            ("turn_orphan_b", "orphan_b_1"),
        ] {
            let stream = store
                .get_turn_event_projection_stream_state(turn_id)
                .await
                .expect("orphan stream state should query")
                .expect("orphan stream should be retained as quarantine evidence");
            assert_eq!(stream.health, TurnProjectionStreamHealth::Quarantined);
            assert_eq!(stream.projected_through_sequence, 0);
            assert_eq!(stream.blocking_event_id.as_deref(), Some(blocker_id));
            assert_eq!(
                stream.last_error.as_deref(),
                Some("canonical Turn is missing")
            );
        }

        let valid = store
            .get_turn_event_projection_stream_state("turn_valid_z")
            .await
            .expect("valid successor should query")
            .expect("valid successor should still be backfilled");
        assert_eq!(valid.health, TurnProjectionStreamHealth::Healthy);
        assert_eq!(valid.projected_through_sequence, 0);

        assert_eq!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .all(&db)
                .await
                .expect("projection evidence should list")
                .len(),
            5,
            "quarantine must preserve the legacy event-state evidence"
        );
        let marker = find_projection_meta(&db, PROJECTION_STREAM_STATE_BACKFILL_KEY)
            .await
            .expect("backfill marker should query")
            .expect("backfill marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(marker.source_turn_count, 3);

        let repeated = backfill_once(&store)
            .await
            .expect("completed orphan migration should be idempotent");
        assert!(repeated.skipped);
    }

    #[tokio::test]
    async fn background_backfill_does_not_silently_classify_a_new_missing_turn_stream() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");
        db.execute_unprepared(
            r#"
INSERT INTO turn_event_projection_state (
    event_id, thread_id, turn_id, sequence, status, attempt_count,
    last_error, next_run_at, claim_token, claim_expires_at,
    projection_context_json, projected_at, created_at, updated_at
) VALUES (
    'unclassified_1', 'thread_unclassified', 'turn_unclassified', 1,
    'pending', 0, NULL, CURRENT_TIMESTAMP,
    NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
"#,
        )
        .await
        .expect("unclassified projection stream should insert");
        let store = CrudStore::new(db.clone());

        let error = backfill_once(&store)
            .await
            .expect_err("a new missing-Turn shape must still fail closed");
        assert!(
            format!("{error:#}").contains("orphan stream has no exhausted causal head"),
            "unexpected error: {error:#}"
        );
        assert!(
            store
                .get_turn_event_projection_stream_state("turn_unclassified")
                .await
                .expect("stream state should query")
                .is_none(),
            "failed classification must not create a guessed stream state"
        );
        let marker = find_projection_meta(&db, PROJECTION_STREAM_STATE_BACKFILL_KEY)
            .await
            .expect("backfill marker should query")
            .expect("failed backfill marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_FAILED);
    }

    /// Runs the real startup backfill against an explicitly supplied,
    /// disposable database copy. The path guard prevents accidental use of a
    /// live Pioneer database. This is ignored in the normal test suite because
    /// the production-shaped fixture is several gigabytes.
    #[tokio::test]
    #[ignore = "requires PIONEER_PROJECTION_BACKFILL_COPY_DB under a migration-audit directory"]
    async fn projection_stream_backfill_completes_on_explicit_database_copy() {
        let path = std::env::var("PIONEER_PROJECTION_BACKFILL_COPY_DB")
            .expect("PIONEER_PROJECTION_BACKFILL_COPY_DB must name an isolated database copy");
        let path = std::fs::canonicalize(path).expect("database copy path must resolve");
        assert!(
            path.components()
                .any(|component| component.as_os_str() == "migration-audit"),
            "refusing to mutate a database outside a migration-audit directory"
        );

        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let database_url = format!("sqlite://{}?mode=rw", path.display());
        let db = Database::connect(database_url)
            .await
            .expect("isolated database copy should open");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations should complete on the database copy");
        let store = CrudStore::new(db.clone());

        backfill_once(&store)
            .await
            .expect("projection stream backfill should complete on the database copy");
        let marker = find_projection_meta(&db, PROJECTION_STREAM_STATE_BACKFILL_KEY)
            .await
            .expect("backfill marker should query")
            .expect("backfill marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);

        let unquarantined_orphans = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                r#"
SELECT COUNT(*) AS count
FROM (
    SELECT projection.turn_id
    FROM turn_event_projection_state AS projection
    LEFT JOIN turn AS canonical_turn ON canonical_turn.id = projection.turn_id
    LEFT JOIN turn_event_projection_stream_state AS stream
      ON stream.turn_id = projection.turn_id
    WHERE canonical_turn.id IS NULL
      AND COALESCE(stream.status, '') <> 'quarantined'
    GROUP BY projection.turn_id
)
"#,
            ))
            .await
            .expect("orphan verification should query")
            .expect("orphan verification should return one row")
            .try_get::<i64>("", "count")
            .expect("orphan count should decode");
        assert_eq!(unquarantined_orphans, 0);

        let repeated = backfill_once(&store)
            .await
            .expect("completed copy backfill should be idempotent");
        assert!(repeated.skipped);
    }

    #[tokio::test]
    async fn watermark_backfill_observes_only_the_continuous_projected_prefix() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");
        db.execute_unprepared(
            r#"
INSERT INTO turn (
    id, thread_id, status, prompt_manifest_json, created_at, updated_at,
    turn_kind, origin, mentions_json
) VALUES (
    'turn_watermark_observe', 'thread_watermark', 'in_progress', '{}',
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'conversation', 'user', '[]'
);
INSERT INTO turn_event (
    id, thread_id, turn_id, sequence, event_type, payload,
    created_at, idempotency_key
) VALUES
    ('watermark_event_1', 'thread_watermark', 'turn_watermark_observe', 1, 'test/one', '{}', CURRENT_TIMESTAMP, 'watermark-1'),
    ('watermark_event_2', 'thread_watermark', 'turn_watermark_observe', 2, 'test/two', '{}', CURRENT_TIMESTAMP, 'watermark-2'),
    ('watermark_event_3', 'thread_watermark', 'turn_watermark_observe', 3, 'test/three', '{}', CURRENT_TIMESTAMP, 'watermark-3'),
    ('watermark_event_5', 'thread_watermark', 'turn_watermark_observe', 5, 'test/five', '{}', CURRENT_TIMESTAMP, 'watermark-5');
INSERT INTO turn_event_projection_state (
    event_id, thread_id, turn_id, sequence, status, attempt_count,
    last_error, next_run_at, claim_token, claim_expires_at,
    projection_context_json, projected_at, created_at, updated_at
) VALUES
    ('watermark_event_1', 'thread_watermark', 'turn_watermark_observe', 1, 'projected', 0, NULL, CURRENT_TIMESTAMP, NULL, NULL, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('watermark_event_2', 'thread_watermark', 'turn_watermark_observe', 2, 'projected', 0, NULL, CURRENT_TIMESTAMP, NULL, NULL, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('watermark_event_3', 'thread_watermark', 'turn_watermark_observe', 3, 'failed', 1, 'retryable', CURRENT_TIMESTAMP, NULL, NULL, '{}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('watermark_event_5', 'thread_watermark', 'turn_watermark_observe', 5, 'projected', 0, NULL, CURRENT_TIMESTAMP, NULL, NULL, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
"#,
        )
        .await
        .expect("legacy projection stream should insert");
        let store = CrudStore::new(db.clone());

        let summary = backfill_once(&store)
            .await
            .expect("watermark observation should backfill");
        assert_eq!(summary.streams_scanned, 1);
        assert_eq!(summary.watermarks_advanced, 1);
        assert_eq!(summary.watermark_mismatches, 0);

        let stream = store
            .get_turn_event_projection_stream_state("turn_watermark_observe")
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 2);
        assert_eq!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::TurnId
                        .eq("turn_watermark_observe"),
                )
                .count(&db)
                .await
                .expect("projection receipts should count"),
            4,
            "watermark observation must not delete projection receipts"
        );

        pioneer_entity::turn_event_projection_state::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::Status,
                Expr::value("projected"),
            )
            .filter(
                pioneer_entity::turn_event_projection_state::Column::EventId
                    .eq("watermark_event_3"),
            )
            .exec(&db)
            .await
            .expect("projection receipt gap should close");

        let caught_up = store
            .backfill_turn_event_projection_stream_states_batch(None, 32)
            .await
            .expect("watermark should advance only to the canonical sequence gap");
        assert_eq!(caught_up.watermarks_advanced, 1);
        assert_eq!(caught_up.watermark_mismatches, 0);
        let stream = store
            .get_turn_event_projection_stream_state("turn_watermark_observe")
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 3);
        assert_eq!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::TurnId
                        .eq("turn_watermark_observe"),
                )
                .count(&db)
                .await
                .expect("projection receipts should count"),
            4
        );

        db.execute_unprepared(
            r#"
INSERT INTO turn_event (
    id, thread_id, turn_id, sequence, event_type, payload,
    created_at, idempotency_key
) VALUES (
    'watermark_event_4', 'thread_watermark', 'turn_watermark_observe', 4,
    'test/four', '{}', CURRENT_TIMESTAMP, 'watermark-4'
);
INSERT INTO turn_event_projection_state (
    event_id, thread_id, turn_id, sequence, status, attempt_count,
    last_error, next_run_at, claim_token, claim_expires_at,
    projection_context_json, projected_at, created_at, updated_at
) VALUES (
    'watermark_event_4', 'thread_watermark', 'turn_watermark_observe', 4,
    'projected', 0, NULL, CURRENT_TIMESTAMP, NULL, NULL, '{}',
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
"#,
        )
        .await
        .expect("canonical sequence gap should close");
        let fully_caught_up = store
            .backfill_turn_event_projection_stream_states_batch(None, 32)
            .await
            .expect("watermark should catch up after both gaps close");
        assert_eq!(fully_caught_up.watermarks_advanced, 1);
        assert_eq!(fully_caught_up.watermark_mismatches, 0);
        let stream = store
            .get_turn_event_projection_stream_state("turn_watermark_observe")
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 5);
        assert_eq!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::TurnId
                        .eq("turn_watermark_observe"),
                )
                .count(&db)
                .await
                .expect("projection receipts should count"),
            5,
            "watermark observation must retain all projection receipts"
        );

        pioneer_entity::turn_event_projection_state::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::Status,
                Expr::value("failed"),
            )
            .filter(
                pioneer_entity::turn_event_projection_state::Column::EventId
                    .eq("watermark_event_3"),
            )
            .exec(&db)
            .await
            .expect("observation mismatch should be introduced");
        let mismatch = store
            .backfill_turn_event_projection_stream_states_batch(None, 32)
            .await
            .expect("watermark mismatch should be observable without mutation");
        assert_eq!(mismatch.watermarks_advanced, 0);
        assert_eq!(mismatch.watermark_mismatches, 1);
        let stream = store
            .get_turn_event_projection_stream_state("turn_watermark_observe")
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 5);
    }

    #[tokio::test]
    async fn background_backfill_repairs_legacy_cross_thread_agent_diff_instead_of_quarantining() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");

        let workspace_id = "workspace_diff_repair";
        let canonical_thread_id = "thread_task_run";
        let legacy_thread_id = "thread_parent_session";
        let turn_id = "turn_diff_repair";
        let event_id = "event_diff_repair_01";
        let timestamp = 1_700_000_000;
        let now = super::now_datetime();

        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Diff Repair Workspace".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .expect("workspace should insert");

        let store = CrudStore::new(db.clone());
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: canonical_thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::TaskRun,
            sidebar_visibility: ThreadSidebarVisibility::Hidden,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
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
            .expect("canonical Turn should materialize");

        let legacy_payload = CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: legacy_thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item: TurnItem::SystemEvent {
                id: "agent_diff_native_turn".to_owned(),
                level: SystemEventLevel::Info,
                message: "Diff updated".to_owned(),
                code: Some("agent_diff_updated".to_owned()),
                details: Some(serde_json::json!({"payload": "diff --git a/a b/a"})),
            },
        });
        let legacy_idempotency_key = legacy_payload
            .idempotency_key()
            .expect("legacy identity should derive");
        let legacy_payload_json =
            serde_json::to_string(&legacy_payload).expect("legacy payload should encode");

        pioneer_entity::turn_event::Entity::insert(pioneer_entity::turn_event::ActiveModel {
            id: Set(event_id.to_owned()),
            thread_id: Set(legacy_thread_id.to_owned()),
            turn_id: Set(turn_id.to_owned()),
            sequence: Set(2),
            event_type: Set("item/completed".to_owned()),
            payload: Set(legacy_payload_json),
            created_at: Set(now),
            idempotency_key: Set(Some(legacy_idempotency_key.clone())),
        })
        .exec(&db)
        .await
        .expect("legacy raw event should insert");
        pioneer_entity::turn_event_projection_state::Entity::insert(
            pioneer_entity::turn_event_projection_state::ActiveModel {
                event_id: Set(event_id.to_owned()),
                thread_id: Set(legacy_thread_id.to_owned()),
                turn_id: Set(turn_id.to_owned()),
                sequence: Set(2),
                status: Set("projected".to_owned()),
                attempt_count: Set(0),
                last_error: Set(None),
                next_run_at: Set(now),
                claim_token: Set(None),
                claim_expires_at: Set(None),
                projection_context_json: Set(
                    serde_json::json!({"enqueue_optional_deliveries": false}).to_string(),
                ),
                projected_at: Set(Some(now)),
                created_at: Set(now),
                updated_at: Set(now),
            },
        )
        .exec(&db)
        .await
        .expect("legacy projection state should insert");
        pioneer_entity::turn_liveness::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_liveness::Column::ThreadId,
                Expr::value(legacy_thread_id.to_owned()),
            )
            .col_expr(
                pioneer_entity::turn_liveness::Column::LastActivitySequence,
                Expr::value(2),
            )
            .filter(pioneer_entity::turn_liveness::Column::TurnId.eq(turn_id))
            .exec(&db)
            .await
            .expect("legacy liveness owner should update");

        let compression = crate::database::zstd_column::run_startup_once(
            &store,
            crate::database::zstd_column::TURN_EVENT_PAYLOAD,
            None,
            1.0,
        )
        .await
        .expect("turn events should convert to the production Zstd view");
        assert!(compression.enabled_now);

        let summary = backfill_once(&store)
            .await
            .expect("cross-thread final diff should be repaired");
        assert_eq!(summary.streams_scanned, 1);
        assert_eq!(summary.streams_repaired, 1);
        assert_eq!(summary.events_repaired, 1);
        assert_eq!(summary.watermarks_advanced, 1);
        assert_eq!(summary.watermark_mismatches, 0);
        assert_eq!(summary.streams_quarantined, 0);

        let raw_event = pioneer_entity::turn_event::Entity::find_by_id(event_id)
            .one(&db)
            .await
            .expect("repaired raw event should query")
            .expect("repaired raw event should remain present");
        assert_eq!(raw_event.thread_id, canonical_thread_id);
        let repaired_payload: CanonicalTurnEventPayload =
            serde_json::from_str(raw_event.payload.as_str())
                .expect("repaired payload should decode");
        assert_eq!(repaired_payload.thread_id(), canonical_thread_id);
        assert_ne!(
            raw_event.idempotency_key.as_deref(),
            Some(legacy_idempotency_key.as_str())
        );
        assert_eq!(
            raw_event.idempotency_key,
            Some(
                repaired_payload
                    .idempotency_key()
                    .expect("repaired identity should derive")
            )
        );

        let projection = pioneer_entity::turn_event_projection_state::Entity::find_by_id(event_id)
            .one(&db)
            .await
            .expect("repaired projection should query")
            .expect("repaired projection should remain present");
        assert_eq!(projection.thread_id, canonical_thread_id);
        let liveness = pioneer_entity::turn_liveness::Entity::find_by_id(turn_id)
            .one(&db)
            .await
            .expect("repaired liveness should query")
            .expect("repaired liveness should remain present");
        assert_eq!(liveness.thread_id, canonical_thread_id);

        let stream = store
            .get_turn_event_projection_stream_state(turn_id)
            .await
            .expect("repaired stream should query")
            .expect("repaired stream should remain present");
        assert_eq!(stream.thread_id, canonical_thread_id);
        assert_eq!(stream.health, TurnProjectionStreamHealth::Healthy);
        assert_eq!(stream.projected_through_sequence, 2);

        let marker = find_projection_meta(&db, PROJECTION_STREAM_STATE_BACKFILL_KEY)
            .await
            .expect("repair marker should query")
            .expect("repair marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(marker.source_turn_event_count, 1);

        let repeated = backfill_once(&store)
            .await
            .expect("completed repair should be idempotent");
        assert!(repeated.skipped);
    }
}
