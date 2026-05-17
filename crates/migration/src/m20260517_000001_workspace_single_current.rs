use sea_orm_migration::{prelude::*, sea_orm::ConnectionTrait};

#[derive(DeriveMigrationName)]
pub struct Migration;

const UNIQUE_CURRENT_WORKSPACE_INDEX: &str = "uidx_workspace_single_active_current";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        normalize_active_current_workspace(manager).await?;

        manager
            .get_connection()
            .execute_unprepared(
                "CREATE UNIQUE INDEX IF NOT EXISTS uidx_workspace_single_active_current \
                 ON workspace (is_current) \
                 WHERE is_active = TRUE AND is_current = TRUE",
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name(UNIQUE_CURRENT_WORKSPACE_INDEX)
                    .table("workspace")
                    .to_owned(),
            )
            .await
    }
}

async fn normalize_active_current_workspace(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();

    connection
        .execute_unprepared(
            "UPDATE workspace \
             SET is_current = FALSE \
             WHERE is_active = TRUE \
               AND is_current = TRUE \
               AND id NOT IN ( \
                   SELECT id \
                   FROM workspace \
                   WHERE is_active = TRUE AND is_current = TRUE \
                   ORDER BY created_at ASC, id ASC \
                   LIMIT 1 \
               )",
        )
        .await?;

    connection
        .execute_unprepared(
            "UPDATE workspace \
             SET is_current = TRUE \
             WHERE id = ( \
                   SELECT id \
                   FROM workspace \
                   WHERE is_active = TRUE \
                   ORDER BY created_at ASC, id ASC \
                   LIMIT 1 \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM workspace \
                   WHERE is_active = TRUE AND is_current = TRUE \
               )",
        )
        .await?;

    Ok(())
}
