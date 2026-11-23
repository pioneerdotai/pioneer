//! Completion timestamp repair for Task read models.
use crate::event_store::TaskEventStore;
use crate::status::{is_terminal_task_run_status_db, is_terminal_task_status_db};
use crate::task_projector::TaskOutputBinder;
use anyhow::{Context, Result};
use pioneer_crud_execution::read_model_repair::{
    ReadModelInvariantKind, ReadModelInvariantViolation, RepairSummary,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait};

const READ_MODEL_REPAIR_BATCH_SIZE: u64 = 32;

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn repair_terminal_tasks_missing_completed_at(&self) -> Result<RepairSummary> {
        let terminal_statuses = ["completed", "failed", "blocked", "cancelled"];
        let high_watermark = self
            .retry_write_on_conflict(|| async {
                pioneer_entity::task::Entity::find()
                    .filter(pioneer_entity::task::Column::Status.is_in(terminal_statuses))
                    .filter(pioneer_entity::task::Column::CompletedAt.is_null())
                    .order_by_desc(pioneer_entity::task::Column::Id)
                    .one(&self.connection)
                    .await
                    .context("failed to load terminal task repair high-watermark")
                    .map(|row| row.map(|row| row.id))
            })
            .await?;
        let Some(high_watermark) = high_watermark else {
            return Ok(RepairSummary::default());
        };

        let mut summary = RepairSummary::default();
        let mut cursor: Option<String> = None;
        loop {
            let cursor_for_query = cursor.clone();
            let high_watermark_for_query = high_watermark.clone();
            let rows = self
                .retry_write_on_conflict(|| {
                    let cursor = cursor_for_query.clone();
                    let high_watermark = high_watermark_for_query.clone();
                    async move {
                        let query = pioneer_entity::task::Entity::find()
                            .filter(pioneer_entity::task::Column::Status.is_in([
                                "completed",
                                "failed",
                                "blocked",
                                "cancelled",
                            ]))
                            .filter(pioneer_entity::task::Column::CompletedAt.is_null())
                            .filter(pioneer_entity::task::Column::Id.lte(high_watermark))
                            .order_by_asc(pioneer_entity::task::Column::Id)
                            .limit(READ_MODEL_REPAIR_BATCH_SIZE);
                        let query = if let Some(cursor) = cursor {
                            query.filter(pioneer_entity::task::Column::Id.gt(cursor))
                        } else {
                            query
                        };
                        query
                            .all(&self.connection)
                            .await
                            .context("failed to load terminal task repair batch")
                    }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            cursor = rows.last().map(|row| row.id.clone());
            summary.detected = summary.detected.saturating_add(rows.len());
            let ids = rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
            let ids_for_write = ids.clone();
            let repaired = self
                .retry_write_on_conflict(|| {
                    let ids = ids_for_write.clone();
                    async move {
                        let transaction = self
                            .connection
                            .begin()
                            .await
                            .context("failed to begin terminal task repair transaction")?;
                        let now: DateTimeWithTimeZone = chrono::Utc::now().into();
                        let result = pioneer_entity::task::Entity::update_many()
                            .filter(pioneer_entity::task::Column::Id.is_in(ids))
                            .filter(pioneer_entity::task::Column::Status.is_in([
                                "completed",
                                "failed",
                                "blocked",
                                "cancelled",
                            ]))
                            .filter(pioneer_entity::task::Column::CompletedAt.is_null())
                            .col_expr(
                                pioneer_entity::task::Column::CompletedAt,
                                Expr::value(Some(now)),
                            )
                            .exec(&transaction)
                            .await
                            .context("failed to repair terminal tasks missing completed_at")?;
                        transaction
                            .commit()
                            .await
                            .context("failed to commit terminal task repair transaction")?;
                        Ok(result.rows_affected as usize)
                    }
                })
                .await?;
            summary.repaired = summary.repaired.saturating_add(repaired);
        }
        Ok(summary)
    }

    pub async fn repair_terminal_runs_missing_completed_at(&self) -> Result<RepairSummary> {
        let high_watermark = self
            .retry_write_on_conflict(|| async {
                crate::task_run::missing_completion_high_watermark(&self.connection).await
            })
            .await?;
        let Some(high_watermark) = high_watermark else {
            return Ok(RepairSummary::default());
        };

        let mut summary = RepairSummary::default();
        let mut cursor: Option<String> = None;
        loop {
            let cursor_for_query = cursor.clone();
            let high_watermark_for_query = high_watermark.clone();
            let rows = self
                .retry_write_on_conflict(|| {
                    let cursor = cursor_for_query.clone();
                    let high_watermark = high_watermark_for_query.clone();
                    async move {
                        crate::task_run::missing_completion_page(
                            &self.connection,
                            &high_watermark,
                            cursor.as_deref(),
                            READ_MODEL_REPAIR_BATCH_SIZE,
                        )
                        .await
                    }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            cursor = rows.last().cloned();
            summary.detected = summary.detected.saturating_add(rows.len());
            let repaired = self
                .retry_write_on_conflict(|| {
                    let ids = rows.clone();
                    async move {
                        let transaction = self
                            .connection
                            .begin()
                            .await
                            .context("failed to begin terminal task_run repair transaction")?;
                        let repaired = crate::task_run::repair_missing_completion(
                            &transaction,
                            ids,
                            chrono::Utc::now().into(),
                        )
                        .await?;
                        transaction
                            .commit()
                            .await
                            .context("failed to commit terminal task_run repair transaction")?;
                        Ok(repaired)
                    }
                })
                .await?;
            summary.repaired = summary.repaired.saturating_add(repaired);
        }
        Ok(summary)
    }
}

pub async fn list_task_invariant_violations(
    connection: &PostgresDatabase,
) -> Result<Vec<ReadModelInvariantViolation>> {
    let mut violations = Vec::new();
    let terminal_tasks_missing_completed_at = pioneer_entity::task::Entity::find()
        .filter(pioneer_entity::task::Column::CompletedAt.is_null())
        .all(connection)
        .await
        .context("failed to list terminal tasks missing completed_at")?;

    for task in terminal_tasks_missing_completed_at {
        if !is_terminal_task_status_db(task.status.as_str()) {
            continue;
        }
        violations.push(ReadModelInvariantViolation {
            kind: ReadModelInvariantKind::TerminalTaskMissingCompletedAt,
            entity_id: task.id,
            details: "terminal task row has null completed_at".to_owned(),
        });
    }

    let terminal_runs_missing_completed_at = pioneer_entity::task_run::Entity::find()
        .filter(pioneer_entity::task_run::Column::CompletedAt.is_null())
        .all(connection)
        .await
        .context("failed to list terminal task runs missing completed_at")?;

    for run in terminal_runs_missing_completed_at {
        if !is_terminal_task_run_status_db(run.status.as_str()) {
            continue;
        }
        violations.push(ReadModelInvariantViolation {
            kind: ReadModelInvariantKind::TerminalRunMissingCompletedAt,
            entity_id: run.id,
            details: "terminal task_run row has null completed_at".to_owned(),
        });
    }

    Ok(violations)
}
