use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum TurnRuntimeSnapshot {
    Table,
    OrdinarySkillCatalogJson,
    ProviderModelContractJson,
    InitialPhaseJson,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, column) in [
            (
                "ordinary_skill_catalog_json",
                TurnRuntimeSnapshot::OrdinarySkillCatalogJson,
            ),
            (
                "provider_model_contract_json",
                TurnRuntimeSnapshot::ProviderModelContractJson,
            ),
            ("initial_phase_json", TurnRuntimeSnapshot::InitialPhaseJson),
        ] {
            if !manager.has_column("turn_runtime_snapshot", name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(TurnRuntimeSnapshot::Table)
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
            ("initial_phase_json", TurnRuntimeSnapshot::InitialPhaseJson),
            (
                "provider_model_contract_json",
                TurnRuntimeSnapshot::ProviderModelContractJson,
            ),
            (
                "ordinary_skill_catalog_json",
                TurnRuntimeSnapshot::OrdinarySkillCatalogJson,
            ),
        ] {
            if manager.has_column("turn_runtime_snapshot", name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(TurnRuntimeSnapshot::Table)
                            .drop_column(column)
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
