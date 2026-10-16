//! Personal read receipts; completed runs remain the only inbox source.
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
        db.execute_unprepared("CREATE TABLE task_run_read (principal_id TEXT NOT NULL, run_id TEXT NOT NULL, read_at TEXT NOT NULL, PRIMARY KEY(principal_id, run_id), FOREIGN KEY(run_id) REFERENCES task_run(id) ON DELETE CASCADE)").await?;
        // An old acknowledgement has exact principal/run provenance. Never fan
        // it out to collaborators, and never infer an unknown historical view.
        db.execute_unprepared("INSERT INTO task_run_read(principal_id,run_id,read_at) SELECT n.recipient_principal_id,n.run_id,MIN(n.acknowledged_at) FROM user_notification_outbox n JOIN task_run r ON r.id=n.run_id AND r.task_id=n.task_id JOIN task t ON t.id=r.task_id AND t.workspace_id=n.workspace_id WHERE n.acknowledged_at IS NOT NULL GROUP BY n.recipient_principal_id,n.run_id").await?;
        db.execute_unprepared("CREATE INDEX idx_user_notification_run_read ON user_notification_outbox(recipient_principal_id,run_id,workspace_id,task_id,acknowledged_at) WHERE acknowledged_at IS NOT NULL").await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP INDEX idx_user_notification_run_read")
            .await?;
        db.execute_unprepared("DROP TABLE task_run_read").await?;
        Ok(())
    }
}
