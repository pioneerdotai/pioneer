use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SeaQuery does not build PostgreSQL views, PL/pgSQL functions or triggers.
        for sql in [
            include_str!("sql/m20261119_000010_database_rules/views.sql"),
            include_str!("sql/m20261119_000010_database_rules/execution.sql"),
            include_str!("sql/m20261119_000010_database_rules/history.sql"),
            include_str!("sql/m20261119_000010_database_rules/memory.sql"),
            include_str!("sql/m20261119_000010_database_rules/packages.sql"),
            include_str!("sql/m20261119_000010_database_rules/payloads.sql"),
            include_str!("sql/m20261119_000010_database_rules/tasks.sql"),
        ] {
            manager.get_connection().execute_unprepared(sql).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261119_000010_database_rules/drop_rules.sql"
            ))
            .await?;
        Ok(())
    }
}
