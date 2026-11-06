use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEX: &str = "idx_agent_action_outbox_status_created_id";

// One full installation read/build; no backfill or Entity shape change.
// Each terminal lane seeks by status then future created_at and reads in order.
// Residual terminal/marker checks can still visit multiple recent rows.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name(INDEX)
                    .table(Alias::new("agent_action_outbox"))
                    .col(Alias::new("status"))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name(INDEX)
                    .table(Alias::new("agent_action_outbox"))
                    .to_owned(),
            )
            .await
    }
}
