use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // Trigger replacement, column removal and the migration marker form one
        // atomic schema transition. Storage discovery reads schema inside that
        // transaction, so it cannot become stale before the DDL commits.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // sqlite-zstd exposes turn_item as a view. Revision triggers must remain
        // on physical storage, including when compression predates this upgrade.
        let storage = if manager.has_table("_turn_item_zstd").await? {
            "_turn_item_zstd"
        } else if manager.has_table("turn_item").await? {
            "turn_item"
        } else {
            return Err(DbErr::Migration("turn item storage is missing".into()));
        };
        let db = manager.get_connection();
        for name in [
            "compaction_item_insert",
            "compaction_item_update",
            "compaction_item_delete",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        db.execute_unprepared("DROP INDEX IF EXISTS compaction_item_revision_capture_order")
            .await?;
        if manager
            .has_column("compaction_item_revision", "capture_order")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("compaction_item_revision")
                        .drop_column("capture_order")
                        .to_owned(),
                )
                .await?;
        }
        // Preserve identity, revisions and tombstones. Only the unused discovery
        // counter disappears; every canonical edit still invalidates old refs.
        for (name, event, values, update) in [
            (
                "insert",
                "INSERT",
                "NEW.id,NEW.turn_id,1,1",
                "revision=revision+1,present=1,turn_id=NEW.turn_id",
            ),
            (
                "update",
                "UPDATE OF payload,turn_id,item_id,item_type,status",
                "NEW.id,NEW.turn_id,2,1",
                "revision=revision+1,present=1,turn_id=NEW.turn_id",
            ),
            (
                "delete",
                "DELETE",
                "OLD.id,OLD.turn_id,2,0",
                "revision=revision+1,present=0",
            ),
        ] {
            db.execute_unprepared(&format!(
                "CREATE TRIGGER compaction_item_{name} AFTER {event} ON \"{storage}\" BEGIN INSERT INTO compaction_item_revision(source_id,turn_id,revision,present) VALUES ({values}) ON CONFLICT(source_id) DO UPDATE SET {update}; END"
            )).await?;
        }
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "removed tool item capture order cannot be reconstructed for downgrade".into(),
        ))
    }
}
