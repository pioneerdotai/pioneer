//! Narrow covering indexes for the workspace-first unread inbox scan.
use sea_orm_migration::{
    prelude::*,
    sea_query::{ExprTrait, IndexColumn, PostgresQueryBuilder},
};

#[derive(DeriveIden)]
enum Task {
    Table,
    WorkspaceId,
    Id,
}
#[derive(DeriveIden)]
enum TaskRun {
    Table,
    TaskId,
    CompletedAt,
    Id,
    Status,
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
            .create_index(
                Index::create()
                    .name("idx_task_inbox_workspace_id")
                    .table(Task::Table)
                    .col(Task::WorkspaceId)
                    .col(Task::Id)
                    .to_owned(),
            )
            .await?;
        let epoch = Func::cust(Alias::new("strftime"))
            .args([Expr::val("%s"), Expr::col(TaskRun::CompletedAt)])
            .cast_as(Alias::new("INTEGER"));
        let run_index = Index::create()
            .name("idx_task_run_inbox_epoch")
            .table(TaskRun::Table)
            .col(TaskRun::TaskId)
            .col(IndexColumn::from(epoch))
            .col(TaskRun::Id)
            .col(TaskRun::Status)
            .col(TaskRun::CompletedAt)
            .to_owned();
        // SeaQuery 1.0.2 accepts IndexColumn::Expr but its SQLite IndexBuilder
        // inherits prepare_index_columns' panic for expressions. The Postgres
        // renderer supports them and emits SQLite-compatible CREATE INDEX for
        // this restricted AST (ordinary columns and CAST/strftime only). Keep
        // the complete statement typed; no hand-written DDL/string rewriting.
        manager
            .get_connection()
            .execute_unprepared(&run_index.to_string(PostgresQueryBuilder))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_task_run_inbox_epoch")
                    .table(TaskRun::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_task_inbox_workspace_id")
                    .table(Task::Table)
                    .to_owned(),
            )
            .await
    }
}
