use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Ordered discovery after the legacy gate. The workspace-oriented
        // html_preview_frozen_unready index still serves retention checks.
        // Keep this predicate identical to CRUD index_quantum's literal terms.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS html_preview_frozen_work \
                 ON html_preview_frozen_index(manifest_id) \
                 WHERE expired=0 AND ready=1 AND (complete<>1 OR unknown=1)",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS html_preview_frozen_work")
            .await?;
        Ok(())
    }
}
