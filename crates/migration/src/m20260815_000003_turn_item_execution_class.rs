use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("turn_item_attempt")
                    .add_column(string("execution_class").string_len(32).null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_execution_class_id")
                    .table("turn_item_attempt")
                    .col("execution_class")
                    .col("id")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_turn_item_attempt_execution_class_id")
                    .table("turn_item_attempt")
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table("turn_item_attempt")
                    .drop_column("execution_class")
                    .to_owned(),
            )
            .await
    }
}
