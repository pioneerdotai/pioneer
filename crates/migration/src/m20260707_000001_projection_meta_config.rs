use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("thread_timeline_projection_meta")
                    .add_column(text("projection_config_hash").null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table("thread_timeline_projection_meta")
                    .add_column(text("projection_config_json").null())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("thread_timeline_projection_meta")
                    .drop_column("projection_config_json")
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table("thread_timeline_projection_meta")
                    .drop_column("projection_config_hash")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
