//! Task occurrence fencing in the caller's Agent graph cancellation transaction.
use anyhow::{Context, Result, bail};
use pioneer_entity::{task_occurrence_contract, task_run_execution};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect};

pub struct PreparedTaskGraphCancellation {
    executions: Vec<task_run_execution::Model>,
    contracts: Vec<task_occurrence_contract::Model>,
}

impl PreparedTaskGraphCancellation {
    pub async fn lock(
        db: &DatabaseTransaction,
        root_execution_id: &str,
        active_ids: &[String],
        graph_row_limit: u64,
    ) -> Result<Self> {
        let occurrence_old_executions = task_run_execution::Entity::find()
            .filter(task_run_execution::Column::Id.is_in(active_ids.iter().cloned()))
            .limit(graph_row_limit)
            .lock_exclusive()
            .all(db)
            .await?;
        let occurrence_old_contracts = task_occurrence_contract::Entity::find()
            .filter(
                task_occurrence_contract::Column::WorkGraphRootExecutionId
                    .eq(root_execution_id.to_owned()),
            )
            .limit(graph_row_limit)
            .lock_exclusive()
            .all(db)
            .await?;
        if occurrence_old_contracts.len() >= usize::try_from(graph_row_limit).unwrap_or(usize::MAX)
        {
            bail!("Task occurrence graph exceeds bounded cancellation policy");
        }
        Ok(Self {
            executions: occurrence_old_executions,
            contracts: occurrence_old_contracts,
        })
    }

    pub async fn fence_tasks(
        &self,
        db: &DatabaseTransaction,
        root_execution_id: &str,
        active_ids: &[String],
        terminal_reason: &str,
        finished_at: DateTimeWithTimeZone,
    ) -> Result<()> {
        if !active_ids.is_empty() {
            task_run_execution::Entity::update_many()
                .col_expr(
                    task_run_execution::Column::Status,
                    sea_orm::sea_query::Expr::value("cancelled"),
                )
                .col_expr(
                    task_run_execution::Column::WorkerId,
                    sea_orm::sea_query::Expr::value::<Option<String>>(None),
                )
                .col_expr(
                    task_run_execution::Column::LeaseUntil,
                    sea_orm::sea_query::Expr::value::<Option<DateTimeWithTimeZone>>(None),
                )
                .col_expr(
                    task_run_execution::Column::CompletedAt,
                    sea_orm::sea_query::Expr::value(Some(finished_at)),
                )
                .col_expr(
                    task_run_execution::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(finished_at),
                )
                .filter(task_run_execution::Column::Id.is_in(active_ids.iter().cloned()))
                .filter(task_run_execution::Column::Status.is_not_in([
                    "succeeded",
                    "failed",
                    "blocked",
                    "cancelled",
                    "timed_out",
                ]))
                .exec(db)
                .await
                .context("failed to fence Task executions in cancelled Agent work graph")?;
        }

        task_occurrence_contract::Entity::update_many()
            .col_expr(
                task_occurrence_contract::Column::Status,
                sea_orm::sea_query::Expr::value("cancelled"),
            )
            .col_expr(
                task_occurrence_contract::Column::TerminalReason,
                sea_orm::sea_query::Expr::value(Some(terminal_reason.to_owned())),
            )
            .col_expr(
                task_occurrence_contract::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(
                task_occurrence_contract::Column::WorkGraphRootExecutionId
                    .eq(root_execution_id.to_owned()),
            )
            .filter(task_occurrence_contract::Column::Status.is_in([
                "dormant",
                "queued",
                "recovering",
                "running",
                "waiting_review",
            ]))
            .exec(db)
            .await
            .context("failed to cancel Task occurrences in Agent work graph")?;

        Ok(())
    }

    pub async fn publish_changes(self, db: &DatabaseTransaction) -> Result<()> {
        for old in &self.executions {
            let new = task_run_execution::Entity::find_by_id(old.id.clone())
                .lock_exclusive()
                .one(db)
                .await?;
            super::task_occurrence_producers::execution_change(db, Some(old), new.as_ref()).await?;
        }
        for old in &self.contracts {
            let new = task_occurrence_contract::Entity::find_by_id(old.occurrence_id.clone())
                .lock_exclusive()
                .one(db)
                .await?;
            super::task_occurrence_producers::contract_change(db, Some(old), new.as_ref()).await?;
        }
        Ok(())
    }
}
