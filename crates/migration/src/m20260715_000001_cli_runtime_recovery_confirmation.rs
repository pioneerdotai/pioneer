use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn_cli_runtime_attempt", "recovery_confirmed_at")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("turn_cli_runtime_attempt")
                        .add_column(timestamp_with_time_zone("recovery_confirmed_at").null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("turn_cli_runtime_attempt", "recovery_confirmed_at")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("turn_cli_runtime_attempt")
                        .drop_column("recovery_confirmed_at")
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
