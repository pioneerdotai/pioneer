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

const LEGACY_FULL_ACCESS_SECURITY_CAP_JSON: &str = concat!(
    r#"{"maxPermissionProfile":{"mode":"full_access","effective_policy":{"#,
    r#""default_behavior":"allow","file_read":"allow","file_write":"allow","#,
    r#""shell_command":"allow","network":"allow","mcp_read":"allow","#,
    r#""mcp_write_or_unknown":"allow","dynamic_skill_tool":"allow","#,
    r#""computer_use":"allow","task_subagent":"allow"}},"#,
    r#""maxNetworkPolicy":{"mode":"enabled","allow_localhost":true,"allow_unix_sockets":true},"#,
    r#""maxSandboxMode":"unrestricted","maxProcessPolicy":{"#,
    r#""shell":{"enabled":true,"allow_stdin":true,"allow_session_inheritance":true},"#,
    r#""environment":{"inherit":true},"timeout":{"max_duration_ms":1800000},"#,
    r#""command_risk":{}}}"#,
);

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
        if manager
            .has_column("task_agent_spec", "security_cap_json")
            .await?
        {
            backfill_missing_security_caps(manager).await?;
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

async fn backfill_missing_security_caps(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            format!(
                "UPDATE task_agent_spec \
                 SET security_cap_json = '{}' \
                 WHERE security_cap_json IS NULL",
                LEGACY_FULL_ACCESS_SECURITY_CAP_JSON
            )
            .as_str(),
        )
        .await?;

    Ok(())
}
