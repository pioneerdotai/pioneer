use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const PROJECTION_STREAM_STATE_BACKFILL_KEY: &str = "turn_event_projection_stream_state_backfill";
const PROJECTION_STREAM_STATE_BACKFILL_VERSION: i64 = 1;
const PROJECTION_STREAM_STATE_BACKFILL_BATCH_SIZE: u64 = 256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProjectionStreamStateBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) streams_scanned: u64,
    pub(crate) streams_quarantined: u64,
}

pub(super) async fn run(crud_store: &CrudStore) {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                streams_scanned = summary.streams_scanned,
                streams_quarantined = summary.streams_quarantined,
                "turn event projection stream state background backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn event projection stream state background backfill failed"
            );
        }
    }
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
        after_turn_id = Some(next_turn_id);
        tokio::task::yield_now().await;
    }

    Ok(summary)
}

async fn backfill_is_current(db: &sea_orm::DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, PROJECTION_STREAM_STATE_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == PROJECTION_STREAM_STATE_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete(
    db: &sea_orm::DatabaseConnection,
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
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: Some(completed_at),
            created_at: started_at,
            updated_at: completed_at,
        },
    )
    .await
}

async fn mark_backfill_failed(
    db: &sea_orm::DatabaseConnection,
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
        CrudStore, PROJECTION_META_STATUS_COMPLETE, TurnProjectionStreamHealth,
        find_projection_meta,
    };
    use sea_orm::{ConnectionTrait, Database, EntityTrait};

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

        let poisoned = store
            .get_turn_event_projection_stream_state("turn_poison")
            .await
            .expect("poisoned stream state should query")
            .expect("poisoned stream should be backfilled");
        assert_eq!(poisoned.health, TurnProjectionStreamHealth::Quarantined);
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
}
