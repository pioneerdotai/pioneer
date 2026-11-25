use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SeaQuery cannot replace PostgreSQL trigger functions.
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261125_000013_compact_observation_turn_cleanup/up.sql"
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261125_000013_compact_observation_turn_cleanup/down.sql"
            ))
            .await?;
        Ok(())
    }
}
