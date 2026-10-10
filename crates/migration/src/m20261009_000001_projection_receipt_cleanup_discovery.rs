use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One-time index construction reads existing stream state. No receipt
        // deletion or data backfill; membership follows existing state writes.
        // Keep the predicate identical to CRUD receipt cleanup discovery.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_turn_event_projection_stream_cleanup_work \
                 ON turn_event_projection_stream_state(turn_id) \
                 WHERE status = 'healthy' AND receipts_compacted_through_sequence >= 0 \
                 AND projected_through_sequence > receipts_compacted_through_sequence",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "DROP INDEX IF EXISTS idx_turn_event_projection_stream_cleanup_work",
            )
            .await?;
        Ok(())
    }
}
