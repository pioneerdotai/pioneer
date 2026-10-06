use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

// Retry budget is 8 (AGENT_ACTION_OUTBOX_MAX_ATTEMPTS in CRUD). Future policy
// changes require replacing these predicates and the specialized ID selectors.
// SQLite builds each index from existing source rows once: O(history) build,
// O(current eligible/leased work) storage. No seed or history worker is needed.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_immediate \
             ON agent_action_outbox(created_at,id) \
             WHERE next_attempt_at IS NULL AND status IN ('pending','failed') AND attempts<8",
            )
            .await?;
        manager.get_connection().execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_timed \
             ON agent_action_outbox(next_attempt_at,created_at,id) \
             WHERE next_attempt_at IS NOT NULL AND (status='pending' OR (status='failed' AND attempts<8))",
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_agent_action_outbox_immediate",
            "idx_agent_action_outbox_timed",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(name)
                        .table(Alias::new("agent_action_outbox"))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
