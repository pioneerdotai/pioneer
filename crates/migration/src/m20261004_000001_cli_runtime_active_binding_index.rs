use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEX: &str = "idx_cli_turn_binding_status_created_turn";

// The one-time build reads existing bindings, including terminal history.
// Retain the workspace-leading index for foreground consumers.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name(INDEX)
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .col(Alias::new("status"))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("turn_id"))
                    .if_not_exists()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name(INDEX)
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
