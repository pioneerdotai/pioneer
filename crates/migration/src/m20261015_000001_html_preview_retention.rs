use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Derived lifetime metadata only: no media, payload, new history format
        // or independent lease. An absent/incomplete index retains its scope.
        let db = manager.get_connection();
        db.execute_unprepared("CREATE TABLE html_preview_frozen_index (manifest_id TEXT PRIMARY KEY NOT NULL, workspace_id TEXT NOT NULL, identity_sha256 TEXT NOT NULL, message_count INTEGER NOT NULL, ready INTEGER NOT NULL, expired INTEGER NOT NULL, next_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(next_ordinal>=0), complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)), unknown INTEGER NOT NULL DEFAULT 0 CHECK(unknown IN (0,1)))").await?;
        db.execute_unprepared("CREATE TABLE html_preview_index_backfill(singleton INTEGER PRIMARY KEY CHECK(singleton=1),complete INTEGER NOT NULL CHECK(complete IN (0,1)), inputs_complete INTEGER NOT NULL DEFAULT 0 CHECK(inputs_complete IN (0,1)))").await?;
        db.execute_unprepared(
            "INSERT INTO html_preview_index_backfill(singleton,complete) VALUES(1,0)",
        )
        .await?;
        db.execute_unprepared("CREATE INDEX html_preview_frozen_unready ON html_preview_frozen_index(workspace_id) WHERE expired=0 AND (ready<>1 OR complete<>1 OR unknown<>0)").await?;
        // New and changing headers publish an unsafe marker before the writer
        // is released. Legacy headers are covered by the bounded backfill gate.
        db.execute_unprepared("CREATE TRIGGER html_preview_header_insert AFTER INSERT ON compaction_frozen_history BEGIN INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired) VALUES(NEW.id,NEW.workspace_id,NEW.identity_sha256,NEW.message_count,NEW.ready,NEW.expired) ON CONFLICT(manifest_id) DO UPDATE SET workspace_id=excluded.workspace_id,identity_sha256=excluded.identity_sha256,message_count=excluded.message_count,ready=excluded.ready,expired=excluded.expired,next_ordinal=0,complete=0,unknown=0; END").await?;
        db.execute_unprepared("CREATE TRIGGER html_preview_header_update AFTER UPDATE OF id,workspace_id,identity_sha256,message_count,ready,expired ON compaction_frozen_history BEGIN INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired) VALUES(NEW.id,NEW.workspace_id,NEW.identity_sha256,NEW.message_count,NEW.ready,NEW.expired) ON CONFLICT(manifest_id) DO UPDATE SET workspace_id=excluded.workspace_id,identity_sha256=excluded.identity_sha256,message_count=excluded.message_count,ready=excluded.ready,expired=excluded.expired,next_ordinal=CASE WHEN OLD.id=NEW.id AND OLD.identity_sha256=NEW.identity_sha256 AND OLD.message_count=NEW.message_count THEN next_ordinal ELSE 0 END,complete=CASE WHEN OLD.id=NEW.id AND OLD.identity_sha256=NEW.identity_sha256 AND OLD.message_count=NEW.message_count THEN complete ELSE 0 END; UPDATE html_preview_frozen_index SET expired=1 WHERE manifest_id=OLD.id AND OLD.id<>NEW.id; END").await?;
        db.execute_unprepared("CREATE TRIGGER html_preview_header_delete AFTER DELETE ON compaction_frozen_history BEGIN UPDATE html_preview_frozen_index SET expired=1 WHERE manifest_id=OLD.id; END").await?;
        db.execute_unprepared("CREATE TRIGGER html_preview_index_retain BEFORE DELETE ON html_preview_frozen_index BEGIN SELECT RAISE(ABORT,'HTML preview lifetime marker retained'); END").await?;
        db.execute_unprepared("CREATE TABLE html_preview_frozen_turn (manifest_id TEXT NOT NULL, turn_id TEXT NOT NULL, PRIMARY KEY(manifest_id,turn_id))").await?;
        db.execute_unprepared("CREATE INDEX html_preview_frozen_turn_lookup ON html_preview_frozen_turn(turn_id,manifest_id)").await?;
        // Compact verification receipts for the archival projection of a
        // completed Task result. References and manifest identity stay immutable.
        db.execute_unprepared("CREATE TABLE html_preview_frozen_result(manifest_id TEXT NOT NULL,ordinal INTEGER NOT NULL,reference_json TEXT NOT NULL,turn_id TEXT NOT NULL,source_id TEXT NOT NULL,source_version TEXT NOT NULL,archive_wire_sha256 TEXT,PRIMARY KEY(manifest_id,ordinal))").await?;
        db.execute_unprepared("CREATE INDEX html_preview_frozen_result_replay ON html_preview_frozen_result(source_id,source_version)").await?;
        db.execute_unprepared("CREATE INDEX artifact_html_preview_discovery ON artifact(id) WHERE created_by_kind='tool' AND created_by_actor_id='html_preview'").await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for trigger in [
            "html_preview_header_insert",
            "html_preview_header_update",
            "html_preview_header_delete",
            "html_preview_index_retain",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER {trigger}"))
                .await?;
        }
        db.execute_unprepared("DROP TABLE html_preview_index_backfill")
            .await?;
        db.execute_unprepared("DROP INDEX artifact_html_preview_discovery")
            .await?;
        db.execute_unprepared("DROP TABLE html_preview_frozen_result")
            .await?;
        db.execute_unprepared("DROP TABLE html_preview_frozen_turn")
            .await?;
        db.execute_unprepared("DROP TABLE html_preview_frozen_index")
            .await?;
        Ok(())
    }
}
