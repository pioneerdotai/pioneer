use sea_orm_migration::{prelude::*, schema::string};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum TurnMcpBinding {
    Table,
    SelectionReason,
    CapabilityId,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn_mcp_binding", "selection_reason")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnMcpBinding::Table)
                        .add_column(
                            string(TurnMcpBinding::SelectionReason)
                                .string_len(64)
                                .not_null()
                                .default("implicit_policy"),
                        )
                        .to_owned(),
                )
                .await?;
        }

        if !manager
            .has_column("turn_mcp_binding", "capability_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnMcpBinding::Table)
                        .add_column(string(TurnMcpBinding::CapabilityId).string_len(255).null())
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("turn_mcp_binding", "capability_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnMcpBinding::Table)
                        .drop_column(TurnMcpBinding::CapabilityId)
                        .to_owned(),
                )
                .await?;
        }

        if manager
            .has_column("turn_mcp_binding", "selection_reason")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TurnMcpBinding::Table)
                        .drop_column(TurnMcpBinding::SelectionReason)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}
