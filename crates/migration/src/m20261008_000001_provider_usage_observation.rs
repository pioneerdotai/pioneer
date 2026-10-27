use sea_orm_migration::{prelude::*, schema::*};

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
                    .table("provider_usage_observation")
                    .if_not_exists()
                    .col(string("id").primary_key())
                    .col(string("workspace_id"))
                    .col(string("operation_kind"))
                    .col(string("owner_id"))
                    .col(string("status"))
                    .col(text("usage_json"))
                    .col(big_integer("started_at"))
                    .col(big_integer("updated_at"))
                    .foreign_key(
                        ForeignKey::create()
                            .from("provider_usage_observation", "workspace_id")
                            .to("workspace", "id")
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_provider_usage_owner")
                    .table("provider_usage_observation")
                    .col("workspace_id")
                    .col("owner_id")
                    .col("id")
                    .if_not_exists()
                    .to_owned(),
            )
            .await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table("provider_usage_observation")
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
