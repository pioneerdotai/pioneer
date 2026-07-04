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

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm_migration::sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

    #[tokio::test]
    async fn backfills_missing_task_agent_permission_caps() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory db");
        db.execute_unprepared(
            "CREATE TABLE task_agent_spec ( \
                id TEXT PRIMARY KEY, \
                permission_cap_json TEXT NULL \
            )",
        )
        .await
        .expect("create task_agent_spec");
        db.execute_unprepared(
            "INSERT INTO task_agent_spec (id, permission_cap_json) VALUES \
                ('legacy', NULL), \
                ('existing', '{\"mode\":\"supervised\"}')",
        )
        .await
        .expect("insert specs");

        let manager = SchemaManager::new(&db);
        backfill_missing_permission_caps(&manager)
            .await
            .expect("backfill should succeed");

        let row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT \
                    MAX(CASE WHEN id = 'existing' THEN permission_cap_json END) AS existing, \
                    MAX(CASE WHEN id = 'legacy' THEN permission_cap_json END) AS legacy \
                 FROM task_agent_spec"
                    .to_owned(),
            ))
            .await
            .expect("query specs")
            .expect("summary row");
        let existing: String = row.try_get("", "existing").unwrap();
        let legacy: String = row.try_get("", "legacy").unwrap();

        assert_eq!(existing, r#"{"mode":"supervised"}"#);
        assert!(legacy.contains(r#""mode":"full_access""#));
        assert!(legacy.contains(r#""network":"allow""#));
        assert!(legacy.contains(r#""task_subagent":"allow""#));
    }
}
