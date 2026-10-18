//! Retain the authenticated initiator of accepted Turn/graph cancellation for
//! the exact still-active runs, until their later terminal reconciliation.
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveIden)]
enum TaskRunCancellationActor {
    Table,
    RunId,
    TaskId,
    WorkspaceId,
    PrincipalId,
    OriginTurnId,
}
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TaskRunCancellationActor::Table)
                    .col(string(TaskRunCancellationActor::RunId).primary_key())
                    .col(string(TaskRunCancellationActor::TaskId))
                    .col(string(TaskRunCancellationActor::WorkspaceId))
                    .col(string(TaskRunCancellationActor::PrincipalId))
                    .col(string(TaskRunCancellationActor::OriginTurnId))
                    .to_owned(),
            )
            .await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(TaskRunCancellationActor::Table)
                    .to_owned(),
            )
            .await
    }
}
