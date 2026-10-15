//! Schema only: release terminal Task inputs without touching accepted descriptors.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Both sides must use the GC release set. An upsert from a releasing
        // Task must verify roots before becoming retaining, including blocked.
        // Terminal-to-terminal writes acquire no root and need no payload.
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER task_frozen_roots_insert")
            .await?;
        manager.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_insert BEFORE INSERT ON task WHEN NEW.status NOT IN ('completed','failed','cancelled') AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=NEW.id AND t.workspace_id=NEW.workspace_id AND t.status NOT IN ('completed','failed','cancelled')) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER task_frozen_roots_update")
            .await?;
        manager.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_update BEFORE UPDATE OF status,workspace_id ON task WHEN NEW.status NOT IN ('completed','failed','cancelled') AND (OLD.status IN ('completed','failed','cancelled') OR OLD.workspace_id<>NEW.workspace_id) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Restore the released guards only. Expired inputs stay expired;
        // downgrading cannot restore any payload already reclaimed by GC.
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER task_frozen_roots_insert")
            .await?;
        manager.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_insert BEFORE INSERT ON task WHEN NEW.status<>'completed' AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=NEW.id AND t.workspace_id=NEW.workspace_id AND t.status<>'completed') AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER task_frozen_roots_update")
            .await?;
        manager.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_update BEFORE UPDATE OF status,workspace_id ON task WHEN NEW.status<>'completed' AND (OLD.status='completed' OR OLD.workspace_id<>NEW.workspace_id) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        Ok(())
    }
}
