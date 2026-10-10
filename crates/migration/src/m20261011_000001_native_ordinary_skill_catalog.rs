use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;
#[derive(DeriveIden)]
enum TurnRuntimeSnapshot {
    Table,
    OrdinarySkillCatalogJson,
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn_runtime_snapshot", "ordinary_skill_catalog_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .add_column(text(TurnRuntimeSnapshot::OrdinarySkillCatalogJson).null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("turn_runtime_snapshot", "ordinary_skill_catalog_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnRuntimeSnapshot::Table)
                        .drop_column(TurnRuntimeSnapshot::OrdinarySkillCatalogJson)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
