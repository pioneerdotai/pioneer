//! Preserve the released inbox activation fact before applying unread filtering.
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveIden)]
enum TaskInboxActivation {
    Table,
    Id,
    StartedAt,
}
#[derive(DeriveIden)]
enum SeaqlMigrations {
    Table,
    Version,
    AppliedAt,
}
#[derive(DeriveIden)]
enum TaskDelivery {
    Table,
    WorkspaceId,
    TargetThreadId,
    DeliveredTurnId,
    TaskId,
    RunId,
}
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TaskInboxActivation::Table)
                    .col(integer(TaskInboxActivation::Id).primary_key())
                    .col(big_integer(TaskInboxActivation::StartedAt))
                    .to_owned(),
            )
            .await?;
        // SeaORM persists applied_at as Unix seconds after each successful migration.
        // Fail closed if that durable fact is missing; never substitute repair time.
        let source = Query::select()
            .expr(Expr::val(1))
            .column(SeaqlMigrations::AppliedAt)
            .from(SeaqlMigrations::Table)
            .and_where(
                Expr::col(SeaqlMigrations::Version).eq("m20261016_000001_task_completion_inbox"),
            )
            .to_owned();
        let insert = Query::insert()
            .into_table(TaskInboxActivation::Table)
            .columns([TaskInboxActivation::Id, TaskInboxActivation::StartedAt])
            .select_from(source)
            .map_err(|e| DbErr::Custom(e.to_string()))?
            .to_owned();
        let result = manager.get_connection().execute(&insert).await?;
        if result.rows_affected() != 1 {
            return Err(DbErr::Custom(
                "missing original inbox migration activation fact".into(),
            ));
        }
        manager
            .create_index(
                Index::create()
                    .name("idx_task_delivery_inbox_result")
                    .table(TaskDelivery::Table)
                    .col(TaskDelivery::WorkspaceId)
                    .col(TaskDelivery::TargetThreadId)
                    .col(TaskDelivery::DeliveredTurnId)
                    .col(TaskDelivery::TaskId)
                    .col(TaskDelivery::RunId)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_task_delivery_inbox_result")
                    .table(TaskDelivery::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(TaskInboxActivation::Table).to_owned())
            .await
    }
}
