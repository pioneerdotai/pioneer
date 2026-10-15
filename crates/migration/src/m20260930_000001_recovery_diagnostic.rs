use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column("recovery_job", "diagnostic").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table("recovery_job")
                        .add_column(text("diagnostic").null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("recovery_job", "last_failure_attempt_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("recovery_job")
                        .add_column(text("last_failure_attempt_id").null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("recovery_job", "last_failure_attempt_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("recovery_job")
                        .drop_column("last_failure_attempt_id")
                        .to_owned(),
                )
                .await?;
        }
        if manager.has_column("recovery_job", "diagnostic").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table("recovery_job")
                        .drop_column("diagnostic")
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
