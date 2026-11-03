//! Snapshot storage is physical state. Its cutover must preserve every logical
//! compaction fence. Trigger DDL/OLD/NEW are not supported by SeaQuery.
//! The helper's pending index scans existing snapshot keys once; installation
//! does not read/convert historical payloads or perform a data backfill.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const DOMAIN_COLUMNS: &str = "history_json,run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,created_at,frozen_manifest_id";
const LOGICAL_CHANGE: &str = "OLD.history_json_payload_id IS NEW.history_json_payload_id OR OLD.run_id IS NOT NEW.run_id OR OLD.task_id IS NOT NEW.task_id OR OLD.workspace_id IS NOT NEW.workspace_id OR OLD.conversation_thread_id IS NOT NEW.conversation_thread_id OR OLD.source_turn_id IS NOT NEW.source_turn_id OR OLD.created_at IS NOT NEW.created_at OR OLD.frozen_manifest_id IS NOT NEW.frozen_manifest_id";
const BASIS_BODY: &str = "INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES (OLD.run_id,2) ON CONFLICT(run_id) DO UPDATE SET revision=revision+1; INSERT INTO compaction_projection_epoch(thread_id,version) SELECT id,1 FROM thread WHERE id IN (OLD.conversation_thread_id,NEW.conversation_thread_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1;";
// Publication triggers were retired by 20261008_000009. This migration
// changes only the surviving basis trigger; down must not revive retired state.

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        super::portable_payload_target::up(
            m,
            "task_run_conversation_snapshot",
            "run_id",
            &["history_json"],
            &[
                "run_id",
                "task_id",
                "workspace_id",
                "conversation_thread_id",
                "source_turn_id",
                "created_at",
                "frozen_manifest_id",
            ],
        )
        .await?;
        replace_update_triggers(m).await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        // The transaction rolls these drops back if helper.down refuses to
        // discard a live compressed value. Empty-schema rollback is supported.
        drop_update_triggers(m).await?;
        super::portable_payload_target::down(
            m,
            "task_run_conversation_snapshot",
            &["history_json"],
        )
        .await?;
        install_update_triggers(m, "AFTER UPDATE ON task_run_conversation_snapshot").await
    }
}

async fn drop_update_triggers(m: &SchemaManager<'_>) -> Result<(), DbErr> {
    m.get_connection()
        .execute_unprepared("DROP TRIGGER compaction_task_basis_update")
        .await?;
    Ok(())
}
async fn replace_update_triggers(m: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_update_triggers(m).await?;
    install_update_triggers(m, &format!("AFTER UPDATE OF {DOMAIN_COLUMNS} ON task_run_conversation_snapshot WHEN {LOGICAL_CHANGE}")).await
}
async fn install_update_triggers(m: &SchemaManager<'_>, event: &str) -> Result<(), DbErr> {
    m.get_connection()
        .execute_unprepared(&format!(
            "CREATE TRIGGER compaction_task_basis_update {event} BEGIN {BASIS_BODY} END"
        ))
        .await?;
    Ok(())
}
