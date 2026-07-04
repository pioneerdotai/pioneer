use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const LEGACY_FULL_ACCESS_PERMISSION_CAP_JSON: &str = r#"{"mode":"full_access","effective_policy":{"default_behavior":"allow","file_read":"allow","file_write":"allow","shell_command":"allow","network":"allow","mcp_read":"allow","mcp_write_or_unknown":"allow","dynamic_skill_tool":"allow","computer_use":"allow","task_subagent":"allow"}}"#;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("task_agent_spec", "permission_cap_json")
            .await?
        {
            backfill_missing_permission_caps(manager).await?;
        }

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

async fn backfill_missing_permission_caps(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            format!(
                "UPDATE task_agent_spec \
                 SET permission_cap_json = '{}' \
                 WHERE permission_cap_json IS NULL",
                LEGACY_FULL_ACCESS_PERMISSION_CAP_JSON
            )
            .as_str(),
        )
        .await?;

    Ok(())
}
