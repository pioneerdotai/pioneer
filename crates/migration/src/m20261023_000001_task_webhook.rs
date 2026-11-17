//! Additive, transactional and replay-safe webhook state; no existing task is changed.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (table, column) in [
            ("task_trigger", "webhook_secret_ref"),
            ("task_run", "trigger_input_json"),
        ] {
            if !manager.has_column(table, column).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .add_column(ColumnDef::new(Alias::new(column)).text().null())
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (table, column) in [
            ("task_run", "trigger_input_json"),
            ("task_trigger", "webhook_secret_ref"),
        ] {
            if manager.has_column(table, column).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .drop_column(Alias::new(column))
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
