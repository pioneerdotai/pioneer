use anyhow::{Context, Result, bail};
use sea_orm::{ConnectionTrait, Statement};

const CHECKPOINT_TABLE: &str = "read_model_repair_checkpoint";
const DIRTY_TABLE: &str = "read_model_repair_dirty_turn_item";
const SEQUENCE_TABLE: &str = "read_model_repair_change_sequence";

pub(crate) const TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY: &str = "terminal_turn_item_payload";
pub(crate) const STATUS_RUNNING: &str = "running";
pub(crate) const STATUS_COMPLETED: &str = "completed";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub(crate) algorithm_version: i64,
    pub(crate) full_scan_status: String,
    pub(crate) full_scan_cursor_id: Option<String>,
    pub(crate) full_scan_high_watermark_id: Option<String>,
    pub(crate) incremental_status: String,
    pub(crate) incremental_high_watermark_generation: Option<i64>,
    pub(crate) incremental_cursor_generation: Option<i64>,
    pub(crate) last_completed_generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirtyTurnItem {
    pub(crate) turn_item_id: String,
    pub(crate) generation: i64,
}

pub(crate) async fn load_checkpoint<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
) -> Result<Option<Checkpoint>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                SELECT
                    algorithm_version,
                    full_scan_status,
                    full_scan_cursor_id,
                    full_scan_high_watermark_id,
                    incremental_status,
                    incremental_high_watermark_generation,
                    incremental_cursor_generation,
                    last_completed_generation
                FROM {CHECKPOINT_TABLE}
                WHERE repair_key = ?
                "#
            ),
            [repair_key.to_owned().into()],
        ))
        .await
        .context("failed to load read-model repair checkpoint")?;

    row.map(|row| -> std::result::Result<Checkpoint, sea_orm::DbErr> {
        Ok(Checkpoint {
            algorithm_version: row.try_get("", "algorithm_version")?,
            full_scan_status: row.try_get("", "full_scan_status")?,
            full_scan_cursor_id: row.try_get("", "full_scan_cursor_id")?,
            full_scan_high_watermark_id: row.try_get("", "full_scan_high_watermark_id")?,
            incremental_status: row.try_get("", "incremental_status")?,
            incremental_high_watermark_generation: row
                .try_get("", "incremental_high_watermark_generation")?,
            incremental_cursor_generation: row.try_get("", "incremental_cursor_generation")?,
            last_completed_generation: row.try_get("", "last_completed_generation")?,
        })
    })
    .transpose()
    .context("failed to decode read-model repair checkpoint")
}

pub(crate) async fn reset_full_scan<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_id: Option<&str>,
) -> Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            r#"
            INSERT INTO {CHECKPOINT_TABLE} (
                repair_key,
                algorithm_version,
                full_scan_status,
                full_scan_cursor_id,
                full_scan_high_watermark_id,
                incremental_status,
                incremental_high_watermark_generation,
                incremental_cursor_generation,
                last_completed_generation,
                full_scan_started_at,
                full_scan_completed_at,
                incremental_started_at,
                incremental_completed_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, 'running', NULL, ?, 'completed', NULL, NULL, 0,
                    CURRENT_TIMESTAMP, NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(repair_key) DO UPDATE SET
                algorithm_version = excluded.algorithm_version,
                full_scan_status = 'running',
                full_scan_cursor_id = NULL,
                full_scan_high_watermark_id = excluded.full_scan_high_watermark_id,
                incremental_status = 'completed',
                incremental_high_watermark_generation = NULL,
                incremental_cursor_generation = NULL,
                full_scan_started_at = CURRENT_TIMESTAMP,
                full_scan_completed_at = NULL,
                incremental_started_at = NULL,
                incremental_completed_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            "#
        ),
        vec![
            repair_key.to_owned().into(),
            algorithm_version.into(),
            high_watermark_id.map(str::to_owned).into(),
        ],
    ))
    .await
    .context("failed to initialize read-model repair full scan")?;
    Ok(())
}

pub(crate) async fn advance_full_scan_cursor<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    cursor_id: &str,
) -> Result<()> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                UPDATE {CHECKPOINT_TABLE}
                SET full_scan_cursor_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE repair_key = ?
                  AND algorithm_version = ?
                  AND full_scan_status = 'running'
                "#
            ),
            vec![
                cursor_id.to_owned().into(),
                repair_key.to_owned().into(),
                algorithm_version.into(),
            ],
        ))
        .await
        .context("failed to advance read-model repair full-scan cursor")?;
    if result.rows_affected() != 1 {
        bail!("read-model repair full-scan checkpoint changed concurrently");
    }
    Ok(())
}

pub(crate) async fn complete_full_scan<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
) -> Result<()> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                UPDATE {CHECKPOINT_TABLE}
                SET
                    full_scan_status = 'completed',
                    full_scan_cursor_id = NULL,
                    full_scan_high_watermark_id = NULL,
                    full_scan_completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE repair_key = ?
                  AND algorithm_version = ?
                  AND full_scan_status = 'running'
                "#
            ),
            vec![repair_key.to_owned().into(), algorithm_version.into()],
        ))
        .await
        .context("failed to complete read-model repair full scan")?;
    if result.rows_affected() != 1 {
        bail!("read-model repair full-scan completion lost its checkpoint");
    }
    Ok(())
}

pub(crate) async fn current_change_generation<C: ConnectionTrait>(db: &C) -> Result<i64> {
    let row = db
        .query_one_raw(Statement::from_string(
            db.get_database_backend(),
            format!("SELECT generation FROM {SEQUENCE_TABLE} WHERE singleton_key = 1"),
        ))
        .await
        .context("failed to load read-model repair change generation")?
        .context("read-model repair change sequence is missing")?;
    row.try_get("", "generation")
        .context("failed to decode read-model repair change generation")
}

pub(crate) async fn begin_incremental_pass<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
) -> Result<()> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                UPDATE {CHECKPOINT_TABLE}
                SET
                    incremental_status = 'running',
                    incremental_high_watermark_generation = ?,
                    incremental_cursor_generation = last_completed_generation,
                    incremental_started_at = CURRENT_TIMESTAMP,
                    incremental_completed_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE repair_key = ?
                  AND algorithm_version = ?
                  AND full_scan_status = 'completed'
                  AND incremental_status = 'completed'
                "#
            ),
            vec![
                high_watermark_generation.into(),
                repair_key.to_owned().into(),
                algorithm_version.into(),
            ],
        ))
        .await
        .context("failed to begin incremental read-model repair pass")?;
    if result.rows_affected() != 1 {
        bail!("incremental read-model repair could not acquire its checkpoint");
    }
    Ok(())
}

pub(crate) async fn list_dirty_turn_items<C: ConnectionTrait>(
    db: &C,
    after_generation: i64,
    high_watermark_generation: i64,
    limit: u64,
) -> Result<Vec<DirtyTurnItem>> {
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                SELECT turn_item_id, generation
                FROM {DIRTY_TABLE}
                WHERE generation > ? AND generation <= ?
                ORDER BY generation ASC, turn_item_id ASC
                LIMIT ?
                "#
            ),
            vec![
                after_generation.into(),
                high_watermark_generation.into(),
                i64::try_from(limit).unwrap_or(i64::MAX).into(),
            ],
        ))
        .await
        .context("failed to list dirty turn_items for incremental repair")?;
    rows.into_iter()
        .map(|row| {
            Ok(DirtyTurnItem {
                turn_item_id: row.try_get("", "turn_item_id")?,
                generation: row.try_get("", "generation")?,
            })
        })
        .collect::<Result<Vec<_>>>()
        .context("failed to decode dirty turn_item repair rows")
}

pub(crate) async fn clear_dirty_turn_item_if_unchanged<C: ConnectionTrait>(
    db: &C,
    turn_item_id: &str,
    generation: i64,
) -> Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("DELETE FROM {DIRTY_TABLE} WHERE turn_item_id = ? AND generation = ?"),
        vec![turn_item_id.to_owned().into(), generation.into()],
    ))
    .await
    .context("failed to clear unchanged dirty turn_item repair row")?;
    Ok(())
}

pub(crate) async fn clear_dirty_turn_item<C: ConnectionTrait>(
    db: &C,
    turn_item_id: &str,
) -> Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("DELETE FROM {DIRTY_TABLE} WHERE turn_item_id = ?"),
        [turn_item_id.to_owned().into()],
    ))
    .await
    .context("failed to clear dirty turn_item repair row")?;
    Ok(())
}

pub(crate) async fn count_dirty_turn_items_in_window<C: ConnectionTrait>(
    db: &C,
    after_generation: i64,
    through_generation: i64,
) -> Result<i64> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                SELECT COUNT(*) AS dirty_count
                FROM {DIRTY_TABLE}
                WHERE generation > ? AND generation <= ?
                "#
            ),
            vec![after_generation.into(), through_generation.into()],
        ))
        .await
        .context("failed to count unconsumed dirty turn_item repair rows")?
        .context("dirty turn_item count query returned no row")?;
    row.try_get("", "dirty_count")
        .context("failed to decode unconsumed dirty turn_item count")
}

pub(crate) async fn advance_incremental_cursor<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
    cursor_generation: i64,
) -> Result<()> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                UPDATE {CHECKPOINT_TABLE}
                SET incremental_cursor_generation = ?, updated_at = CURRENT_TIMESTAMP
                WHERE repair_key = ?
                  AND algorithm_version = ?
                  AND incremental_status = 'running'
                  AND incremental_high_watermark_generation = ?
                "#
            ),
            vec![
                cursor_generation.into(),
                repair_key.to_owned().into(),
                algorithm_version.into(),
                high_watermark_generation.into(),
            ],
        ))
        .await
        .context("failed to advance incremental read-model repair cursor")?;
    if result.rows_affected() != 1 {
        bail!("incremental read-model repair checkpoint changed concurrently");
    }
    Ok(())
}

pub(crate) async fn complete_incremental_pass<C: ConnectionTrait>(
    db: &C,
    repair_key: &str,
    algorithm_version: i64,
    high_watermark_generation: i64,
) -> Result<()> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"
                UPDATE {CHECKPOINT_TABLE}
                SET
                    incremental_status = 'completed',
                    last_completed_generation = ?,
                    incremental_high_watermark_generation = NULL,
                    incremental_cursor_generation = NULL,
                    incremental_completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE repair_key = ?
                  AND algorithm_version = ?
                  AND incremental_status = 'running'
                  AND incremental_high_watermark_generation = ?
                "#
            ),
            vec![
                high_watermark_generation.into(),
                repair_key.to_owned().into(),
                algorithm_version.into(),
                high_watermark_generation.into(),
            ],
        ))
        .await
        .context("failed to complete incremental read-model repair pass")?;
    if result.rows_affected() != 1 {
        bail!("incremental read-model repair completion lost its checkpoint");
    }
    Ok(())
}
