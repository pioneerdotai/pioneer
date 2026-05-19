use sea_orm_migration::{prelude::*, sea_orm::ConnectionTrait};

#[derive(DeriveMigrationName)]
pub struct Migration;

const UNIQUE_CURRENT_WORKSPACE_INDEX: &str = "uidx_workspace_single_active_current";
const UNIQUE_ACTIVE_QUARANTINE_INDEX: &str = "uidx_agent_memory_quarantine_active_memory";

#[derive(DeriveIden)]
enum AgentMemory {
    Table,
    SourceKind,
    SourceContextKind,
}

#[derive(DeriveIden)]
enum AgentMemoryCandidate {
    Table,
    SourceKind,
    SourceContextKind,
}

#[derive(DeriveIden)]
enum AgentMemoryQualityDecision {
    Table,
    Id,
    WorkspaceId,
    ThreadId,
    TurnId,
    ItemId,
    TaskId,
    MemoryId,
    CandidateId,
    CanonicalKey,
    Action,
    TargetOwnership,
    SourceContextKind,
    FactClass,
    LifetimeClass,
    OwnershipClass,
    EvidenceClass,
    Relation,
    ReasonCodesJson,
    InputSnapshotJson,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentMemoryQuarantine {
    Table,
    Id,
    MemoryId,
    WorkspaceId,
    ReasonCode,
    ActorKind,
    ActorId,
    CreatedAt,
    ResolvedAt,
    ResolvedReasonCode,
    ResolvedActorKind,
    ResolvedActorId,
    DetailsJson,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        normalize_active_current_workspace(manager).await?;
        add_agent_memory_source_context_columns(manager).await?;
        create_agent_memory_quality_decision_table(manager).await?;
        create_agent_memory_quarantine_table(manager).await?;
        drop_agent_memory_source_kind_columns(manager).await?;

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

        drop_agent_memory_quarantine_table(manager).await?;
        drop_agent_memory_quality_decision_table(manager).await?;
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

async fn drop_agent_memory_source_kind_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.has_column("agent_memory", "source_kind").await? {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemory::Table)
                    .drop_column(AgentMemory::SourceKind)
                    .to_owned(),
            )
            .await?;
    }

    if manager
        .has_column("agent_memory_candidate", "source_kind")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(AgentMemoryCandidate::Table)
                    .drop_column(AgentMemoryCandidate::SourceKind)
                    .to_owned(),
            )
            .await?;
    }

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

async fn create_agent_memory_quality_decision_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(AgentMemoryQualityDecision::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::Id)
                        .string_len(21)
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::WorkspaceId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::ThreadId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::TurnId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::ItemId)
                        .string_len(128)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::TaskId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::MemoryId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::CandidateId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::CanonicalKey)
                        .string_len(512)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::Action)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::TargetOwnership)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::SourceContextKind)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::FactClass)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::LifetimeClass)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::OwnershipClass)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::EvidenceClass)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::Relation)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::ReasonCodesJson)
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::InputSnapshotJson)
                        .text()
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::CreatedAt)
                        .timestamp_with_time_zone()
                        .default(Expr::current_timestamp())
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQualityDecision::UpdatedAt)
                        .timestamp_with_time_zone()
                        .default(Expr::current_timestamp())
                        .not_null(),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_memory")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::MemoryId)
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_candidate")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::CandidateId)
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_thread_turn")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::ThreadId)
                .col(AgentMemoryQualityDecision::TurnId)
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_workspace_created")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::WorkspaceId)
                .col(AgentMemoryQualityDecision::CreatedAt)
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_action_created")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::Action)
                .col(AgentMemoryQualityDecision::CreatedAt)
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quality_canonical_key")
                .table(AgentMemoryQualityDecision::Table)
                .col(AgentMemoryQualityDecision::CanonicalKey)
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn drop_agent_memory_quality_decision_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .if_exists()
                .table(AgentMemoryQualityDecision::Table)
                .to_owned(),
        )
        .await
}

async fn create_agent_memory_quarantine_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(AgentMemoryQuarantine::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::Id)
                        .string_len(21)
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::MemoryId)
                        .string_len(21)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::WorkspaceId)
                        .string_len(21)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ReasonCode)
                        .string_len(96)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ActorKind)
                        .string_len(64)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ActorId)
                        .string_len(128)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::CreatedAt)
                        .timestamp_with_time_zone()
                        .default(Expr::current_timestamp()),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ResolvedAt)
                        .timestamp_with_time_zone()
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ResolvedReasonCode)
                        .string_len(96)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ResolvedActorKind)
                        .string_len(64)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::ResolvedActorId)
                        .string_len(128)
                        .null(),
                )
                .col(
                    ColumnDef::new(AgentMemoryQuarantine::DetailsJson)
                        .text()
                        .null(),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quarantine_memory_created")
                .table(AgentMemoryQuarantine::Table)
                .col(AgentMemoryQuarantine::MemoryId)
                .col(AgentMemoryQuarantine::CreatedAt)
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quarantine_workspace_created")
                .table(AgentMemoryQuarantine::Table)
                .col(AgentMemoryQuarantine::WorkspaceId)
                .col(AgentMemoryQuarantine::CreatedAt)
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_memory_quarantine_resolved")
                .table(AgentMemoryQuarantine::Table)
                .col(AgentMemoryQuarantine::ResolvedAt)
                .to_owned(),
        )
        .await?;

    manager
        .get_connection()
        .execute_unprepared(
            "CREATE UNIQUE INDEX IF NOT EXISTS uidx_agent_memory_quarantine_active_memory \
             ON agent_memory_quarantine (memory_id) \
             WHERE resolved_at IS NULL",
        )
        .await?;

    Ok(())
}

async fn drop_agent_memory_quarantine_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name(UNIQUE_ACTIVE_QUARANTINE_INDEX)
                .table(AgentMemoryQuarantine::Table)
                .to_owned(),
        )
        .await?;

    manager
        .drop_table(
            Table::drop()
                .if_exists()
                .table(AgentMemoryQuarantine::Table)
                .to_owned(),
        )
        .await
}
