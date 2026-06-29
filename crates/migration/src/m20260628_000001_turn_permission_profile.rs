use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum Turn {
    Table,
    PermissionProfileMode,
    PermissionProfileSource,
    PermissionProfileSnapshotJson,
}

#[derive(DeriveIden)]
enum TaskAgentSpec {
    Table,
    PermissionCapJson,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn", "permission_profile_mode")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(string(Turn::PermissionProfileMode).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("turn", "permission_profile_source")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(string(Turn::PermissionProfileSource).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("turn", "permission_profile_snapshot_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .add_column(text(Turn::PermissionProfileSnapshotJson).null())
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("task_agent_spec", "permission_cap_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TaskAgentSpec::Table)
                        .add_column(text(TaskAgentSpec::PermissionCapJson).null())
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("task_agent_spec", "permission_cap_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(TaskAgentSpec::Table)
                        .drop_column(TaskAgentSpec::PermissionCapJson)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn", "permission_profile_snapshot_json")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::PermissionProfileSnapshotJson)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn", "permission_profile_source")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::PermissionProfileSource)
                        .to_owned(),
                )
                .await?;
        }
        if manager
            .has_column("turn", "permission_profile_mode")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Turn::Table)
                        .drop_column(Turn::PermissionProfileMode)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}
