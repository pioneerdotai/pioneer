//! Progress and generation fences for incremental conversation repair.

use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    read_model_repair_change_sequence as sequence, read_model_repair_checkpoint as checkpoint,
    read_model_repair_dirty_turn_item as dirty,
};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, UpdateMany,
};

use checkpoint::Column as F;

pub const TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY: &str = "terminal_turn_item_payload";
pub const STATUS_RUNNING: &str = "running";
pub const STATUS_COMPLETED: &str = "completed";

pub type Checkpoint = checkpoint::Model;

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct DirtyTurnItem {
    pub turn_item_id: String,
    pub generation: i64,
}

pub async fn load_checkpoint<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
) -> Result<Option<Checkpoint>> {
    checkpoint::Entity::find_by_id(repair_key)
        .one(db)
        .await
        .context("load read-model repair checkpoint")
}

pub async fn reset_full_scan<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_id: Option<&str>,
) -> Result<()> {
    let now = chrono::Utc::now().fixed_offset();
    checkpoint::Entity::insert(checkpoint::ActiveModel {
        repair_key: Set(repair_key.to_owned()),
        algorithm_version: Set(algorithm_version),
        full_scan_status: Set(STATUS_RUNNING.to_owned()),
        full_scan_cursor_id: Set(None),
        full_scan_high_watermark_id: Set(high_watermark_id.map(str::to_owned)),
        full_scan_started_at: Set(Some(now)),
        full_scan_completed_at: Set(None),
        incremental_status: Set(STATUS_COMPLETED.to_owned()),
        incremental_high_watermark_generation: Set(None),
        incremental_cursor_generation: Set(None),
        incremental_started_at: Set(None),
        incremental_completed_at: Set(None),
        last_completed_generation: Set(0),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(F::RepairKey)
            .update_columns([
                F::AlgorithmVersion,
                F::FullScanStatus,
                F::FullScanCursorId,
                F::FullScanHighWatermarkId,
                F::FullScanStartedAt,
                F::FullScanCompletedAt,
                F::IncrementalStatus,
                F::IncrementalHighWatermarkGeneration,
                F::IncrementalCursorGeneration,
                F::IncrementalStartedAt,
                F::IncrementalCompletedAt,
                F::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("initialize read-model repair full scan")?;
    Ok(())
}

fn progress_update(repair_key: &str, algorithm_version: i64) -> UpdateMany<checkpoint::Entity> {
    checkpoint::Entity::update_many()
        .filter(F::RepairKey.eq(repair_key))
        .filter(F::AlgorithmVersion.eq(algorithm_version))
        .col_expr(F::UpdatedAt, Expr::current_timestamp())
}

pub async fn advance_full_scan_cursor<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    cursor_id: &str,
) -> Result<()> {
    let result = progress_update(repair_key, algorithm_version)
        .filter(F::FullScanStatus.eq(STATUS_RUNNING))
        .col_expr(F::FullScanCursorId, Expr::val(cursor_id))
        .exec(db)
        .await
        .context("advance read-model repair full-scan cursor")?;
    ensure!(
        result.rows_affected == 1,
        "full-scan repair checkpoint changed"
    );
    Ok(())
}

pub async fn complete_full_scan<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
) -> Result<()> {
    let result = progress_update(repair_key, algorithm_version)
        .filter(F::FullScanStatus.eq(STATUS_RUNNING))
        .col_expr(F::FullScanStatus, Expr::val(STATUS_COMPLETED))
        .col_expr(F::FullScanCursorId, Expr::val(None::<String>))
        .col_expr(F::FullScanHighWatermarkId, Expr::val(None::<String>))
        .col_expr(F::FullScanCompletedAt, Expr::current_timestamp())
        .exec(db)
        .await
        .context("complete read-model repair full scan")?;
    ensure!(
        result.rows_affected == 1,
        "full-scan repair checkpoint changed"
    );
    Ok(())
}

pub async fn current_change_generation<C: ConnectionTrait>(db: &C) -> Result<i64> {
    sequence::Entity::find_by_id(1_i64)
        .select_only()
        .column(sequence::Column::Generation)
        .into_tuple::<i64>()
        .one(db)
        .await?
        .context("read-model repair change sequence is missing")
}

pub async fn begin_incremental_pass<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
) -> Result<()> {
    let result = progress_update(repair_key, algorithm_version)
        .filter(F::FullScanStatus.eq(STATUS_COMPLETED))
        .filter(F::IncrementalStatus.eq(STATUS_COMPLETED))
        .col_expr(F::IncrementalStatus, Expr::val(STATUS_RUNNING))
        .col_expr(
            F::IncrementalHighWatermarkGeneration,
            Expr::val(high_watermark_generation),
        )
        .col_expr(
            F::IncrementalCursorGeneration,
            Expr::col(F::LastCompletedGeneration),
        )
        .col_expr(F::IncrementalStartedAt, Expr::current_timestamp())
        .col_expr(
            F::IncrementalCompletedAt,
            Expr::val(None::<chrono::DateTime<chrono::FixedOffset>>),
        )
        .exec(db)
        .await
        .context("begin incremental read-model repair pass")?;
    ensure!(
        result.rows_affected == 1,
        "incremental pass could not acquire its checkpoint"
    );
    Ok(())
}

fn dirty_window(after: i64, through: i64) -> sea_orm::Select<dirty::Entity> {
    dirty::Entity::find()
        .filter(dirty::Column::Generation.gt(after))
        .filter(dirty::Column::Generation.lte(through))
}

pub async fn list_dirty_turn_items<C: ConnectionTrait>(
    db: &C,
    after_generation: i64,
    high_watermark_generation: i64,
    limit: u64,
) -> Result<Vec<DirtyTurnItem>> {
    dirty_window(after_generation, high_watermark_generation)
        .select_only()
        .columns([dirty::Column::TurnItemId, dirty::Column::Generation])
        .order_by_asc(dirty::Column::Generation)
        .order_by_asc(dirty::Column::TurnItemId)
        .limit(limit)
        .into_model::<DirtyTurnItem>()
        .all(db)
        .await
        .context("list dirty turn items for incremental repair")
}

pub async fn clear_dirty_turn_item_if_unchanged<C: ConnectionTrait>(
    db: &C,
    turn_item_id: &str,
    generation: i64,
) -> Result<()> {
    dirty::Entity::delete_many()
        .filter(dirty::Column::TurnItemId.eq(turn_item_id))
        .filter(dirty::Column::Generation.eq(generation))
        .exec(db)
        .await
        .context("clear unchanged dirty turn item")?;
    Ok(())
}

pub async fn clear_dirty_turn_item<C: ConnectionTrait>(db: &C, turn_item_id: &str) -> Result<()> {
    dirty::Entity::delete_by_id(turn_item_id)
        .exec(db)
        .await
        .context("clear dirty turn item")?;
    Ok(())
}

pub async fn count_dirty_turn_items_in_window<C: ConnectionTrait>(
    db: &C,
    after_generation: i64,
    through_generation: i64,
) -> Result<i64> {
    let count = dirty_window(after_generation, through_generation)
        .count(db)
        .await
        .context("count unconsumed dirty turn items")?;
    Ok(i64::try_from(count)?)
}

fn incremental_update(
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
) -> UpdateMany<checkpoint::Entity> {
    progress_update(repair_key, algorithm_version)
        .filter(F::IncrementalStatus.eq(STATUS_RUNNING))
        .filter(F::IncrementalHighWatermarkGeneration.eq(high_watermark_generation))
}

pub async fn advance_incremental_cursor<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
    cursor_generation: i64,
) -> Result<()> {
    let result = incremental_update(repair_key, algorithm_version, high_watermark_generation)
        .col_expr(F::IncrementalCursorGeneration, Expr::val(cursor_generation))
        .exec(db)
        .await
        .context("advance incremental read-model repair cursor")?;
    ensure!(
        result.rows_affected == 1,
        "incremental repair checkpoint changed"
    );
    Ok(())
}

pub async fn complete_incremental_pass<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
) -> Result<()> {
    let result = incremental_update(repair_key, algorithm_version, high_watermark_generation)
        .col_expr(F::IncrementalStatus, Expr::val(STATUS_COMPLETED))
        .col_expr(
            F::LastCompletedGeneration,
            Expr::val(high_watermark_generation),
        )
        .col_expr(
            F::IncrementalHighWatermarkGeneration,
            Expr::val(None::<i64>),
        )
        .col_expr(F::IncrementalCursorGeneration, Expr::val(None::<i64>))
        .col_expr(F::IncrementalCompletedAt, Expr::current_timestamp())
        .exec(db)
        .await
        .context("complete incremental read-model repair pass")?;
    ensure!(
        result.rows_affected == 1,
        "incremental repair checkpoint changed"
    );
    Ok(())
}
