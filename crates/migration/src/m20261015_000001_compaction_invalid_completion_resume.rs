use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TRIGGER operation_frozen_admission")
            .await?;
        // Keep the existing Deadline exception. InvalidCompletion may replace
        // only the request deadline in the same saved admission, on a new turn.
        // The repository atomically rechecks execution/head/Stop fences and
        // advances runner generation in this same serialized write transaction.
        db.execute_unprepared(
            r#"CREATE TRIGGER operation_frozen_admission
               BEFORE UPDATE OF snapshot,fingerprint ON compaction_operation
               WHEN OLD.next_portion>0 AND NOT (
                   OLD.status='failed' AND NEW.status='running' AND NEW.outcome IS NULL
                   AND NEW.fingerprint=OLD.fingerprint
                   AND (
                       (OLD.outcome IS 'deadline' AND NEW.deadline_ms>OLD.deadline_ms
                        AND EXISTS(SELECT 1 FROM compaction_runner_state WHERE operation_id=OLD.id))
                       OR
                       (NEW.execution_turn IS NOT NULL AND OLD.execution_turn IS NOT NULL
                        AND NEW.execution_turn<>OLD.execution_turn
                        AND json_remove(NEW.snapshot,'$.admission.deadline_ms')
                            IS json_remove(OLD.snapshot,'$.admission.deadline_ms')
                        AND json_extract(NEW.snapshot,'$.admission.deadline_ms') IS NEW.deadline_ms
                        AND EXISTS(SELECT 1 FROM compaction_runner_state
                            WHERE operation_id=OLD.id
                            AND json_extract(state,'$.phase.Failed.kind')='InvalidCompletion'))
                   )
               )
               BEGIN SELECT RAISE(ABORT,'prepared admission immutable'); END"#,
        )
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TRIGGER operation_frozen_admission")
            .await?;
        db.execute_unprepared(
            r#"CREATE TRIGGER operation_frozen_admission
               BEFORE UPDATE OF snapshot,fingerprint ON compaction_operation
               WHEN OLD.next_portion>0 AND NOT (
                   OLD.status='failed' AND OLD.outcome IS 'deadline'
                   AND NEW.status='running' AND NEW.outcome IS NULL
                   AND NEW.deadline_ms>OLD.deadline_ms AND NEW.fingerprint=OLD.fingerprint
                   AND EXISTS(SELECT 1 FROM compaction_runner_state WHERE operation_id=OLD.id)
               )
               BEGIN SELECT RAISE(ABORT,'prepared admission immutable'); END"#,
        )
        .await?;
        Ok(())
    }
}
