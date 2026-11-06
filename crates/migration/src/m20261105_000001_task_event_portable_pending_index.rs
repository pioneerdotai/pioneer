//! Rebuilds the existing pending index once; construction scans task_event.
//! payload_json is NOT NULL, so removing its redundant predicate preserves
//! index membership and keys without changing rows, payloads or triggers.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEX: &str = "task_event_payload_json_portable_pending";

#[derive(DeriveIden)]
enum TaskEvent {
    Table,
    Id,
    PayloadJson,
    PayloadJsonPayloadId,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rebuild(manager, false).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rebuild(manager, true).await
    }
}

async fn rebuild(manager: &SchemaManager<'_>, legacy: bool) -> Result<(), DbErr> {
    manager
        .drop_index(Index::drop().name(INDEX).table(TaskEvent::Table).to_owned())
        .await?;
    let mut predicate = Expr::col(TaskEvent::PayloadJsonPayloadId).is_null();
    if legacy {
        predicate = predicate.and(Expr::col(TaskEvent::PayloadJson).is_not_null());
    }
    manager
        .create_index(
            Index::create()
                .name(INDEX)
                .table(TaskEvent::Table)
                .col(TaskEvent::PayloadJsonPayloadId)
                .col(TaskEvent::Id)
                .and_where(predicate)
                .to_owned(),
        )
        .await
}
