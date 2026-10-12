use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Match the causal guard in turn_event_delivery::claim_due. Completed
        // history stays outside the predecessor search for each consumer/Turn.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_turn_event_delivery_unfinished_predecessor \
                 ON turn_event_delivery(consumer, turn_id, sequence) \
                 WHERE status NOT IN ('delivered', 'exhausted')",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "DROP INDEX IF EXISTS idx_turn_event_delivery_unfinished_predecessor",
            )
            .await?;
        Ok(())
    }
}
