use sea_orm_migration::prelude::*;
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("skill_upload_session", "purpose")
            .await?
        {
            manager.get_connection().execute_unprepared("ALTER TABLE skill_upload_session ADD COLUMN purpose TEXT NOT NULL DEFAULT 'skill' CHECK(purpose IN ('skill','plugin'));").await?;
        }
        if !manager.has_column("turn", "plugin_selection_json").await? {
            manager
                .get_connection()
                .execute_unprepared("ALTER TABLE turn ADD COLUMN plugin_selection_json TEXT;")
                .await?;
        }
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared("ALTER TABLE turn DROP COLUMN plugin_selection_json; ALTER TABLE skill_upload_session DROP COLUMN purpose;").await?;
        Ok(())
    }
}
