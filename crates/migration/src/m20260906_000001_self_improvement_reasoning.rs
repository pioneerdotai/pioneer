use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum SelfImprovementRun {
    Table,
    LearnerReasoningEffort,
    ReviewerReasoningEffort,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, column) in [
            (
                "learner_reasoning_effort",
                SelfImprovementRun::LearnerReasoningEffort,
            ),
            (
                "reviewer_reasoning_effort",
                SelfImprovementRun::ReviewerReasoningEffort,
            ),
        ] {
            if !manager.has_column("self_improvement_run", name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(SelfImprovementRun::Table)
                            .add_column(text(column).null())
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, column) in [
            (
                "reviewer_reasoning_effort",
                SelfImprovementRun::ReviewerReasoningEffort,
            ),
            (
                "learner_reasoning_effort",
                SelfImprovementRun::LearnerReasoningEffort,
            ),
        ] {
            if manager.has_column("self_improvement_run", name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(SelfImprovementRun::Table)
                            .drop_column(column)
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
