use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("task_agent_spec", "review_policy_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("task_agent_spec")
                        .add_column(
                            ColumnDef::new(Alias::new("review_policy_json"))
                                .text()
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("task_agent_spec", "review_policy_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("task_agent_spec")
                        .drop_column(Alias::new("review_policy_json"))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
