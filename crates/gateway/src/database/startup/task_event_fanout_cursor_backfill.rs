use anyhow::Result;
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const TASK_EVENT_FANOUT_CURSOR_BACKFILL_KEY: &str = "task_event_fanout_cursor_backfill";
const TASK_EVENT_FANOUT_CURSOR_BACKFILL_VERSION: i64 = 1;
const TASK_EVENT_FANOUT_CURSOR_BACKFILL_BATCH_SIZE: u64 = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TaskEventFanoutCursorBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) tasks_initialized: u64,
}

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                tasks_initialized = summary.tasks_initialized,
                "task event fanout cursor background backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "task event fanout cursor background backfill failed"
            );
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
) -> Result<TaskEventFanoutCursorBackfillSummary> {
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(TaskEventFanoutCursorBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: TASK_EVENT_FANOUT_CURSOR_BACKFILL_KEY.to_owned(),
            projection_version: TASK_EVENT_FANOUT_CURSOR_BACKFILL_VERSION,
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

    let result = backfill_all_batches(crud_store).await;
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
) -> Result<TaskEventFanoutCursorBackfillSummary> {
    let mut summary = TaskEventFanoutCursorBackfillSummary::default();
    loop {
        let processed = crud_store
            .backfill_task_event_fanout_cursors_batch(TASK_EVENT_FANOUT_CURSOR_BACKFILL_BATCH_SIZE)
            .await?;
        if processed == 0 {
            break;
        }
        summary.batches = summary.batches.saturating_add(1);
        summary.tasks_initialized = summary.tasks_initialized.saturating_add(processed as u64);
        super::maintenance_checkpoint().await?;
    }
    Ok(summary)
}

async fn backfill_is_current(db: &sea_orm::DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, TASK_EVENT_FANOUT_CURSOR_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == TASK_EVENT_FANOUT_CURSOR_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete(
    db: &sea_orm::DatabaseConnection,
    started_at: DateTimeWithTimeZone,
    summary: &TaskEventFanoutCursorBackfillSummary,
) -> Result<()> {
    let completed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TASK_EVENT_FANOUT_CURSOR_BACKFILL_KEY.to_owned(),
            projection_version: TASK_EVENT_FANOUT_CURSOR_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary.tasks_initialized as i64,
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
            projection_key: TASK_EVENT_FANOUT_CURSOR_BACKFILL_KEY.to_owned(),
            projection_version: TASK_EVENT_FANOUT_CURSOR_BACKFILL_VERSION,
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
