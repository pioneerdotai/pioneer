use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;
#[derive(DeriveIden)]
enum TurnRuntimeSnapshot {
    Table,
    InitialPhaseJson,
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn_runtime_snapshot", "initial_phase_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .add_column(text(TurnRuntimeSnapshot::InitialPhaseJson).null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("turn_runtime_snapshot", "initial_phase_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .drop_column(TurnRuntimeSnapshot::InitialPhaseJson)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
