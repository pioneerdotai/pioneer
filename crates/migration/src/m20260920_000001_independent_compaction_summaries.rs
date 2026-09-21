use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const LIVE_SOURCES_WITH_INDEPENDENT_CHECKPOINTS: &str = "CREATE VIEW compaction_live_sources AS \
SELECT 'context:'||r.turn_id AS source_scope,r.source_id,'revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_source_revision r JOIN turn_llm_context s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'item:'||r.turn_id AS source_scope,r.source_id,'item-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_item_revision r JOIN turn_item s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'event:'||r.turn_id AS source_scope,r.source_id,'event-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_event_revision r JOIN turn_event s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'input:'||r.turn_id,r.source_id,'input-revision:'||r.revision,r.turn_id,t.thread_id,th.workspace_id FROM compaction_input_revision r JOIN turn_input s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256,NULL,c.thread_id,c.workspace_id FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner WHERE p.format_version=1 AND (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation committed WHERE committed.id=p.operation_id AND committed.status='completed'))) \
UNION ALL SELECT 'task-basis:'||s.run_id,s.run_id,'task-basis-revision:'||COALESCE(r.revision,1),NULL,s.conversation_thread_id,s.workspace_id FROM task_run_conversation_snapshot s JOIN thread th ON th.id=s.conversation_thread_id AND th.workspace_id=s.workspace_id LEFT JOIN compaction_task_basis_revision r ON r.run_id=s.run_id WHERE substr(ltrim(s.history_json),1,1)='['";

const LIVE_SOURCES_WITH_EPOCH_BOUND_CHECKPOINTS: &str = "CREATE VIEW compaction_live_sources AS \
SELECT 'context:'||r.turn_id AS source_scope,r.source_id,'revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_source_revision r JOIN turn_llm_context s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'item:'||r.turn_id AS source_scope,r.source_id,'item-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_item_revision r JOIN turn_item s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'event:'||r.turn_id AS source_scope,r.source_id,'event-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_event_revision r JOIN turn_event s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'input:'||r.turn_id,r.source_id,'input-revision:'||r.revision,r.turn_id,t.thread_id,th.workspace_id FROM compaction_input_revision r JOIN turn_input s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 \
UNION ALL SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256,NULL,c.thread_id,c.workspace_id FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner LEFT JOIN compaction_projection_epoch e ON e.thread_id=c.thread_id WHERE (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation committed WHERE committed.id=p.operation_id AND committed.status='completed'))) AND p.projection_version=COALESCE(e.version,0) \
UNION ALL SELECT 'task-basis:'||s.run_id,s.run_id,'task-basis-revision:'||COALESCE(r.revision,1),NULL,s.conversation_thread_id,s.workspace_id FROM task_run_conversation_snapshot s JOIN thread th ON th.id=s.conversation_thread_id AND th.workspace_id=s.workspace_id LEFT JOIN compaction_task_basis_revision r ON r.run_id=s.run_id WHERE substr(ltrim(s.history_json),1,1)='['";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP VIEW compaction_live_sources")
            .await?;
        db.execute_unprepared(LIVE_SOURCES_WITH_INDEPENDENT_CHECKPOINTS)
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP VIEW compaction_live_sources")
            .await?;
        db.execute_unprepared(LIVE_SOURCES_WITH_EPOCH_BOUND_CHECKPOINTS)
            .await?;
        Ok(())
    }
}
