use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS idx_agent_action_outbox_immediate")
            .await?;
        manager.get_connection().execute_unprepared(
            "CREATE INDEX idx_agent_action_outbox_immediate ON agent_action_outbox(created_at,id) WHERE next_attempt_at IS NULL AND status IN ('pending','failed') AND attempts<8 AND (status='pending' OR last_error IS NULL OR last_error NOT IN ('outbox_start_integrity_rejected','outbox_start_authority_denied','outbox_start_runtime_unavailable','outbox_foreground_conflict','outbox_start_outcome_unknown'))",
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS idx_agent_action_outbox_immediate")
            .await?;
        manager.get_connection().execute_unprepared(
            "CREATE INDEX idx_agent_action_outbox_immediate ON agent_action_outbox(created_at,id) WHERE next_attempt_at IS NULL AND status IN ('pending','failed') AND attempts<8",
        ).await?;
        Ok(())
    }
}
