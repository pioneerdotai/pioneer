use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum SelfImprovementWorkspaceState {
    Table,
    HistoryBackfillAfterEventId,
    HistoryBackfillComplete,
}

#[derive(DeriveIden)]
enum SelfImprovementRun {
    Table,
    WorkspaceId,
    Status,
    SourceLowerExclusive,
    SourceUpperInclusive,
}

#[derive(DeriveIden)]
enum AgentSkill {
    Table,
    EvidenceLatestAtUnix,
}

#[derive(DeriveIden)]
enum AgentSkillVersion {
    Table,
    EvidenceLatestAtUnix,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column(
                "self_improvement_workspace_state",
                "history_backfill_after_event_id",
            )
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(SelfImprovementWorkspaceState::Table)
                        .add_column(
                            text(SelfImprovementWorkspaceState::HistoryBackfillAfterEventId).null(),
                        )
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column(
                "self_improvement_workspace_state",
                "history_backfill_complete",
            )
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(SelfImprovementWorkspaceState::Table)
                        .add_column(
                            boolean(SelfImprovementWorkspaceState::HistoryBackfillComplete)
                                .not_null()
                                .default(false),
                        )
                        .to_owned(),
                )
                .await?;
        }
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_self_improvement_completed_ranges")
                    .table(SelfImprovementRun::Table)
                    .col(SelfImprovementRun::WorkspaceId)
                    .col(SelfImprovementRun::Status)
                    .col(SelfImprovementRun::SourceLowerExclusive)
                    .col(SelfImprovementRun::SourceUpperInclusive)
                    .to_owned(),
            )
            .await?;
        // Legacy skills have unknown evidence age; never infer it from created_at.
        if !manager
            .has_column("agent_skill", "evidence_latest_at_unix")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(AgentSkill::Table)
                        .add_column(big_integer(AgentSkill::EvidenceLatestAtUnix).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("agent_skill_version", "evidence_latest_at_unix")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(AgentSkillVersion::Table)
                        .add_column(big_integer(AgentSkillVersion::EvidenceLatestAtUnix).null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("agent_skill_version", "evidence_latest_at_unix")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(AgentSkillVersion::Table)
                        .drop_column(AgentSkillVersion::EvidenceLatestAtUnix)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("agent_skill", "evidence_latest_at_unix")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(AgentSkill::Table)
                        .drop_column(AgentSkill::EvidenceLatestAtUnix)
                        .to_owned(),
                )
                .await?;
        }
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_self_improvement_completed_ranges")
                    .table(SelfImprovementRun::Table)
                    .to_owned(),
            )
            .await?;
        for (name, column) in [
            (
                "history_backfill_complete",
                SelfImprovementWorkspaceState::HistoryBackfillComplete,
            ),
            (
                "history_backfill_after_event_id",
                SelfImprovementWorkspaceState::HistoryBackfillAfterEventId,
            ),
        ] {
            if manager
                .has_column("self_improvement_workspace_state", name)
                .await?
            {
                manager
                    .alter_table(
                        Table::alter()
                            .table(SelfImprovementWorkspaceState::Table)
                            .drop_column(column)
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
