use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum Turn {
    Table,
    ReasoningEffort,
}

#[derive(DeriveIden)]
enum TurnRuntimeSnapshot {
    Table,
    ReasoningEffort,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column("turn", "reasoning_effort").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(text(Turn::ReasoningEffort).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("turn_runtime_snapshot", "reasoning_effort")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .add_column(text(TurnRuntimeSnapshot::ReasoningEffort).null())
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.has_column("turn", "reasoning_effort").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::ReasoningEffort)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn_runtime_snapshot", "reasoning_effort")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .drop_column(TurnRuntimeSnapshot::ReasoningEffort)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}
