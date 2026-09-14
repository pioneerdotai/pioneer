use sea_orm_migration::{prelude::*, schema::*};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.create_table(
            Table::create()
                .table(Alias::new("tool_output_chunk"))
                .col(big_integer("ordinal").primary_key().auto_increment())
                .col(text("id").unique_key())
                .col(text("turn_id"))
                .col(text("item_id"))
                .col(text("stream"))
                .col(text("text"))
                .col(text("metadata").null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Alias::new("tool_output_chunk"), Alias::new("turn_id"))
                        .to(Alias::new("turn"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("tool_output_chunk_turn_item")
                .table(Alias::new("tool_output_chunk"))
                .col("turn_id")
                .col("item_id")
                .col("ordinal")
                .to_owned(),
        )
        .await?;
        Ok(())
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.drop_table(
            Table::drop()
                .table(Alias::new("tool_output_chunk"))
                .to_owned(),
        )
        .await
    }
}
