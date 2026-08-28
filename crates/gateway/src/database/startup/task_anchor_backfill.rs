use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_COMPLETE, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use pioneer_entity::turn_item;
use pioneer_protocol::TurnItem;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
};
use tracing::{info, warn};

const TASK_ANCHOR_BACKFILL_KEY: &str = "task_anchor_payload_backfill";
const TASK_ANCHOR_BACKFILL_VERSION: i64 = 3;
const TASK_ANCHOR_BACKFILL_BATCH_SIZE: u64 = 32;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TaskAnchorBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) task_rows_seen: usize,
    pub(crate) anchors_updated: usize,
    pub(crate) invalid_payloads: usize,
    pub(crate) missing_tasks: usize,
}

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                task_rows = summary.task_rows_seen,
                anchors_updated = summary.anchors_updated,
                invalid_payloads = summary.invalid_payloads,
                missing_tasks = summary.missing_tasks,
                "task anchor backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "task anchor backfill failed at startup"
            );
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) async fn backfill_once(crud_store: &CrudStore) -> Result<TaskAnchorBackfillSummary> {
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(TaskAnchorBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let total_task_rows = turn_item::Entity::find()
        .filter(turn_item::Column::ItemType.eq("task"))
        .count(&db)
        .await
        .context("failed to count task turn items for task anchor backfill")?;

    let mut summary = TaskAnchorBackfillSummary::default();
    let mut cursor: Option<(DateTimeWithTimeZone, String)> = None;

    loop {
        let mut query = turn_item::Entity::find().filter(turn_item::Column::ItemType.eq("task"));
        if let Some((updated_at, id)) = cursor.as_ref() {
            query = query.filter(
                Condition::any()
                    .add(turn_item::Column::UpdatedAt.gt(*updated_at))
                    .add(
                        Condition::all()
                            .add(turn_item::Column::UpdatedAt.eq(*updated_at))
                            .add(turn_item::Column::Id.gt(id.as_str())),
                    ),
            );
        }

        let rows = query
            .order_by_asc(turn_item::Column::UpdatedAt)
            .order_by_asc(turn_item::Column::Id)
            .limit(TASK_ANCHOR_BACKFILL_BATCH_SIZE)
            .all(&db)
            .await
            .context("failed to list task turn items for task anchor backfill")?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            cursor = Some((row.updated_at, row.id.clone()));
            summary.task_rows_seen = summary.task_rows_seen.saturating_add(1);

            let parsed = match serde_json::from_str::<TurnItem>(row.payload.as_str()) {
                Ok(TurnItem::Task { item }) => item,
                Ok(_) => continue,
                Err(_) => {
                    summary.invalid_payloads = summary.invalid_payloads.saturating_add(1);
                    continue;
                }
            };

            let Some(response) = crud_store
                .get_task(parsed.task_id.as_str())
                .await
                .with_context(|| {
                    format!(
                        "failed to load task `{}` for task anchor backfill",
                        parsed.task_id
                    )
                })?
            else {
                summary.missing_tasks = summary.missing_tasks.saturating_add(1);
                continue;
            };

            let refreshed = crate::task_tools::task_turn_item_from_response_with_store(
                crud_store,
                &response,
                parsed.run_id.as_deref(),
                row.item_id.clone(),
                None,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to rebuild task anchor `{}` for task `{}`",
                    row.item_id, parsed.task_id
                )
            })?;

            if refreshed == parsed {
                continue;
            }

            let refreshed_payload = serde_json::to_string(&TurnItem::Task { item: refreshed })
                .context("failed to serialize refreshed task anchor payload")?;
            let row_for_update = row.clone();
            let updated = crud_store
                .run_background_database_quantum(|| {
                    let db = db.clone();
                    let row = row_for_update.clone();
                    let refreshed_payload = refreshed_payload.clone();
                    async move {
                        turn_item::Entity::update_many()
                            .filter(turn_item::Column::Id.eq(row.id))
                            .filter(turn_item::Column::Payload.eq(row.payload))
                            .filter(turn_item::Column::UpdatedAt.eq(row.updated_at))
                            .col_expr(turn_item::Column::Payload, Expr::value(refreshed_payload))
                            .col_expr(turn_item::Column::UpdatedAt, Expr::value(now_datetime()))
                            .exec(&db)
                            .await
                            .context("failed to conditionally update task anchor payload")
                    }
                })
                .await?;
            summary.anchors_updated = summary
                .anchors_updated
                .saturating_add(updated.rows_affected as usize);
        }
        super::maintenance_checkpoint().await?;
    }

    mark_backfill_complete(&db, total_task_rows as i64).await?;
    Ok(summary)
}

async fn backfill_is_current(db: &sea_orm::DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, TASK_ANCHOR_BACKFILL_KEY).await? else {
        return Ok(false);
    };

    Ok(meta.projection_version == TASK_ANCHOR_BACKFILL_VERSION
        && meta.status == PROJECTION_META_STATUS_COMPLETE)
}

async fn mark_backfill_complete(
    db: &sea_orm::DatabaseConnection,
    task_row_count: i64,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TASK_ANCHOR_BACKFILL_KEY.to_owned(),
            projection_version: TASK_ANCHOR_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: task_row_count,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}
