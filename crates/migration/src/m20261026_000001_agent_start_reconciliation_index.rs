use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_exhausted \
             ON agent_action_outbox(COALESCE(next_attempt_at,''),created_at,id) \
             WHERE status IN ('pending','failed') AND attempts=8 \
             AND COALESCE(CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.kind') END,'start_agent')='start_agent'",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS idx_agent_action_outbox_exhausted")
            .await?;
        Ok(())
    }
}
