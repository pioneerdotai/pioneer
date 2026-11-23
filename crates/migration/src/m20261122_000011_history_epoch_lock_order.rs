use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SeaQuery cannot build PostgreSQL functions or triggers.
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261122_000011_history_epoch_lock_order/up.sql"
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261122_000011_history_epoch_lock_order/down.sql"
            ))
            .await?;
        Ok(())
    }
}
