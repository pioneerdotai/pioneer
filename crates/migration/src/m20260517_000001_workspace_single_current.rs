use sea_orm_migration::{prelude::*, sea_orm::ConnectionTrait};

#[derive(DeriveMigrationName)]
pub struct Migration;

const UNIQUE_CURRENT_WORKSPACE_INDEX: &str = "uidx_workspace_single_active_current";

#[derive(DeriveIden)]
enum AgentMemory {
    Table,
    SourceContextKind,
}

#[derive(DeriveIden)]
enum AgentMemoryCandidate {
    Table,
    SourceContextKind,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        normalize_active_current_workspace(manager).await?;
        add_agent_memory_source_context_columns(manager).await?;

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
            .await?;

        drop_agent_memory_source_context_columns(manager).await
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

async fn add_agent_memory_source_context_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager
        .has_column("agent_memory", "source_context_kind")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemory::Table)
                    .add_column(
                        ColumnDef::new(AgentMemory::SourceContextKind)
                            .string()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;
    }

    if !manager
        .has_column("agent_memory_candidate", "source_context_kind")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemoryCandidate::Table)
                    .add_column(
                        ColumnDef::new(AgentMemoryCandidate::SourceContextKind)
                            .string()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn drop_agent_memory_source_context_columns(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    if manager
        .has_column("agent_memory_candidate", "source_context_kind")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemoryCandidate::Table)
                    .drop_column(AgentMemoryCandidate::SourceContextKind)
                    .to_owned(),
            )
            .await?;
    }

    if manager
        .has_column("agent_memory", "source_context_kind")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemory::Table)
                    .drop_column(AgentMemory::SourceContextKind)
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}
