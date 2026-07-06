use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum Turn {
    Table,
    ExecutionSecuritySnapshotVersion,
    ExecutionSecuritySnapshotJson,
}

#[derive(DeriveIden)]
enum TaskAgentSpec {
    Table,
    SecurityCapJson,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn", "execution_security_snapshot_version")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(integer(Turn::ExecutionSecuritySnapshotVersion).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("turn", "execution_security_snapshot_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(text(Turn::ExecutionSecuritySnapshotJson).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("task_agent_spec", "security_cap_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TaskAgentSpec::Table)
                        .add_column(text(TaskAgentSpec::SecurityCapJson).null())
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("task_agent_spec", "security_cap_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TaskAgentSpec::Table)
                        .drop_column(TaskAgentSpec::SecurityCapJson)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn", "execution_security_snapshot_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::ExecutionSecuritySnapshotJson)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn", "execution_security_snapshot_version")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::ExecutionSecuritySnapshotVersion)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}
