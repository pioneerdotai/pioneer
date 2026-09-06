use sea_orm_migration::{prelude::*, schema::text};

const IDENTITY: &str = "agent_memory_identity";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // No inference/backfill of arbitrary legacy keys. Existing canonical
        // keys remain readable; an explicit addressed write establishes a link.
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(IDENTITY))
                    .if_not_exists()
                    .col(text("scope_kind"))
                    .col(text("scope_key_hash"))
                    .col(text("namespace"))
                    .col(text("canonical_key"))
                    .col(text("memory_id").unique_key())
                    .primary_key(
                        Index::create()
                            .col("scope_kind")
                            .col("scope_key_hash")
                            .col("namespace")
                            .col("canonical_key"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_agent_memory_identity_memory")
                            .from(Alias::new(IDENTITY), Alias::new("memory_id"))
                            .to(Alias::new("agent_memory"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(IDENTITY))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
