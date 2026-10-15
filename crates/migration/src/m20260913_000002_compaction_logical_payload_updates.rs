use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // Schema discovery and trigger replacement share the migration/marker
        // transaction. No source payloads are read or transformed here.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (logical, revision, epoch, fields, epoch_filter) in [
            (
                "turn_event",
                "event",
                "event",
                "turn_id,event_type,sequence",
                "1",
            ),
            (
                "turn_item",
                "item",
                "item",
                "turn_id,item_id,item_type,status",
                "OLD.item_type='user_message' OR (OLD.item_type IN ('command_execution','file_change','web_search','web_fetch','download','dynamic_tool_call') AND (OLD.status IS NULL OR OLD.status IN ('completed','failed')))",
            ),
            (
                "turn_input",
                "input",
                "input",
                "text,turn_id,input_type,input_index",
                "1",
            ),
            (
                "turn_llm_context",
                "source",
                "context",
                "turn_id,source,item_id,sequence",
                "1",
            ),
        ] {
            let compressed = format!("_{logical}_zstd");
            let storage = if manager.has_table(&compressed).await? {
                compressed
            } else if manager.has_table(logical).await? {
                logical.to_owned()
            } else {
                return Err(DbErr::Migration(format!("{logical} storage is missing")));
            };
            // Canonical payloads are JSON Strings (SQLite TEXT). Transparent
            // compression alone writes BLOBs to physical storage; its logical
            // view writes TEXT and clears the dictionary on a payload edit.
            // This guard works before AND after enable_transparent renames the
            // table. It avoids decompressing/comparing JSON under the writer.
            // Metadata changes still invalidate a source with a compressed body.
            let changed = std::iter::once(
                "(typeof(NEW.payload) <> 'blob' AND NEW.payload IS NOT OLD.payload)".to_owned(),
            )
            .chain(
                fields
                    .split(',')
                    .map(|field| format!("NEW.{field} IS NOT OLD.{field}")),
            )
            .collect::<Vec<_>>()
            .join(" OR ");
            let db = manager.get_connection();
            for name in [
                format!("compaction_{revision}_update"),
                format!("compaction_{epoch}_epoch_update"),
            ] {
                db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                    .await?;
            }
            let table = format!("compaction_{revision}_revision");
            let (capture_column, capture_value) = if revision == "item" {
                (String::new(), String::new())
            } else {
                (
                    ",capture_order".into(),
                    format!(",(SELECT COALESCE(MAX(capture_order),0)+1 FROM {table})"),
                )
            };
            db.execute_unprepared(&format!(
                "CREATE TRIGGER compaction_{revision}_update AFTER UPDATE OF payload,{fields} ON \"{storage}\" WHEN {changed} BEGIN INSERT INTO {table}(source_id,turn_id,revision,present{capture_column}) VALUES (NEW.id,NEW.turn_id,2,1{capture_value}) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END"
            )).await?;
            db.execute_unprepared(&format!(
                "CREATE TRIGGER compaction_{epoch}_epoch_update AFTER UPDATE OF payload,{fields} ON \"{storage}\" WHEN ({changed}) AND ({epoch_filter}) BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT DISTINCT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id IN (OLD.turn_id,NEW.turn_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END"
            )).await?;
        }
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "physical compression must not invalidate canonical history".into(),
        ))
    }
}
