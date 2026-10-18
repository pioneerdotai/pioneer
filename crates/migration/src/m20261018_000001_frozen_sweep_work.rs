//! Schema only. Physical inventory and consumer fanout run in bounded maintenance
//! quanta, never in this migration or a foreground startup transaction.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn enqueue(table: &str, id: &str, kind: &str) -> String {
    format!(
        "INSERT INTO {table}(manifest_id,kind) SELECT {id},{kind} WHERE {id} IS NOT NULL AND {kind} IN (0,1) ON CONFLICT(manifest_id,kind) DO UPDATE SET revision=revision+1,pending=1{};",
        if table == "frozen_sweep_work" {
            ",retry_at=0"
        } else {
            ",after_source=NULL"
        }
    )
}

fn consumer(id: &str, kind: &str) -> String {
    format!(
        "{}{}",
        enqueue("frozen_sweep_work", id, kind),
        enqueue("frozen_sweep_consumer", id, kind)
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        let db = m.get_connection();
        db.execute_unprepared(r#"
CREATE TABLE frozen_sweep_work (
 work_row INTEGER PRIMARY KEY CHECK(work_row>0),
 manifest_id TEXT NOT NULL, kind INTEGER NOT NULL CHECK(kind IN (0,1)),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(typeof(revision)='integer' AND revision>0),
 pending INTEGER NOT NULL DEFAULT 1 CHECK(pending IN (0,1)),
 retry_at INTEGER NOT NULL DEFAULT 0,
 UNIQUE(manifest_id,kind)
);
CREATE INDEX frozen_sweep_pending ON frozen_sweep_work(pending,kind,retry_at,manifest_id);
CREATE INDEX frozen_sweep_service ON frozen_sweep_work(pending,kind,work_row);
CREATE TABLE frozen_sweep_consumer (
 consumer_row INTEGER PRIMARY KEY CHECK(consumer_row>0),
 manifest_id TEXT NOT NULL, kind INTEGER NOT NULL CHECK(kind IN (0,1)),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(typeof(revision)='integer' AND revision>0),
 pending INTEGER NOT NULL DEFAULT 1 CHECK(pending IN (0,1)),
 after_source TEXT,
 UNIQUE(manifest_id,kind)
);
CREATE INDEX frozen_sweep_consumer_pending ON frozen_sweep_consumer(pending);
CREATE INDEX frozen_sweep_consumer_sources ON compaction_frozen_span(manifest_id,kind,source_manifest);
CREATE INDEX frozen_sweep_source_consumers ON compaction_frozen_span(source_manifest,kind,manifest_id);
CREATE TABLE frozen_sweep_fanout (
 id INTEGER PRIMARY KEY CHECK(id=1),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(typeof(revision)='integer' AND revision>0),
 after_row INTEGER NOT NULL DEFAULT 0, until_row INTEGER NOT NULL DEFAULT 0
);
INSERT INTO frozen_sweep_fanout(id) VALUES(1);
CREATE TABLE frozen_sweep_bootstrap (
 kind INTEGER PRIMARY KEY CHECK(kind IN (0,1)),
 started INTEGER NOT NULL DEFAULT 0 CHECK(started IN (0,1)),
 upper_manifest TEXT, after_manifest TEXT,
 complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1))
);
INSERT INTO frozen_sweep_bootstrap(kind) VALUES(0),(1);
CREATE INDEX frozen_sweep_bootstrap_pending ON frozen_sweep_bootstrap(complete,kind);
"#).await?;
        // Permanent tokens prevent ABA across domain delete/recreate. Integer
        // overflow fails CHECK (SQLite promotes overflowing arithmetic to REAL),
        // rolling back the domain mutation rather than resetting a generation.
        for table in ["frozen_sweep_work", "frozen_sweep_consumer"] {
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_retain BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'sweep generations are retained'); END")).await?;
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_identity BEFORE UPDATE OF manifest_id,kind ON {table} WHEN NEW.manifest_id IS NOT OLD.manifest_id OR NEW.kind IS NOT OLD.kind BEGIN SELECT RAISE(ABORT,'sweep generation identity immutable'); END")).await?;
        }
        db.execute_unprepared("CREATE TRIGGER frozen_sweep_consumer_row_identity BEFORE UPDATE OF consumer_row ON frozen_sweep_consumer WHEN NEW.consumer_row IS NOT OLD.consumer_row BEGIN SELECT RAISE(ABORT,'consumer service identity immutable'); END").await?;
        // Retention plus SQLite's automatic rowid allocation gives insertion
        // order. Reject random rowid fallback at i64 exhaustion; never insert
        // behind a captured frontier. This runs only on actual new work rows,
        // not the UPDATE branch of a coalesced UPSERT.
        db.execute_unprepared("CREATE TRIGGER frozen_sweep_work_row_insert AFTER INSERT ON frozen_sweep_work WHEN NEW.work_row<>(SELECT MAX(work_row) FROM frozen_sweep_work) BEGIN SELECT RAISE(ABORT,'work service identity exhausted'); END").await?;
        db.execute_unprepared("CREATE TRIGGER frozen_sweep_work_row_identity BEFORE UPDATE OF work_row ON frozen_sweep_work WHEN NEW.work_row IS NOT OLD.work_row BEGIN SELECT RAISE(ABORT,'work service identity immutable'); END").await?;
        for (table, kind) in [
            ("compaction_frozen_message_data", "0"),
            ("compaction_frozen_import_data", "1"),
        ] {
            let fields = if kind == "0" {
                "manifest_id,ordinal,reference_json,bytes"
            } else {
                "manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes"
            };
            let changed = fields
                .split(',')
                .map(|field| format!("OLD.{field} IS NOT NEW.{field}"))
                .collect::<Vec<_>>()
                .join(" OR ");
            db.execute_unprepared(&format!(
                "CREATE TRIGGER sweep_{table}_insert AFTER INSERT ON {table} BEGIN {} END",
                enqueue("frozen_sweep_work", "NEW.manifest_id", kind)
            ))
            .await?;
            db.execute_unprepared(&format!(
                "CREATE TRIGGER sweep_{table}_update AFTER UPDATE ON {table} WHEN {changed} BEGIN {}{} END",
                enqueue("frozen_sweep_work", "OLD.manifest_id", kind),
                enqueue("frozen_sweep_work", "NEW.manifest_id", kind)
            ))
            .await?;
            // DELETE cannot add physical work. In particular our own bounded
            // deletes must not dirty themselves. INSERT and repair UPDATE do.
        }
        let header_fields = "id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready,expired,storage_registered";
        let layout_fields = "manifest_id,kind,active,pending,candidate,compared,copy_to,copy_next,cleanup_to,failed";
        for (table, fields) in [
            ("compaction_frozen_history", header_fields),
            ("compaction_frozen_layout", layout_fields),
            (
                "compaction_frozen_span",
                "manifest_id,kind,start,end,source_manifest",
            ),
        ] {
            for (event, sides) in [
                ("INSERT", vec!["NEW"]),
                ("DELETE", vec!["OLD"]),
                ("UPDATE", vec!["OLD", "NEW"]),
            ] {
                let when = if event == "UPDATE" {
                    format!(
                        "WHEN {}",
                        fields
                            .split(',')
                            .map(|f| format!("OLD.{f} IS NOT NEW.{f}"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    )
                } else {
                    String::new()
                };
                let mut body = String::new();
                for side in sides {
                    if table.ends_with("history") {
                        for kind in ["0", "1"] {
                            body += &consumer(&format!("{side}.id"), kind);
                        }
                    } else {
                        body += &consumer(&format!("{side}.manifest_id"), &format!("{side}.kind"));
                        let target = if table.ends_with("layout") {
                            "candidate"
                        } else {
                            "source_manifest"
                        };
                        body += &enqueue(
                            "frozen_sweep_work",
                            &format!("{side}.{target}"),
                            &format!("{side}.kind"),
                        );
                    }
                }
                db.execute_unprepared(&format!("CREATE TRIGGER sweep_{table}_{} AFTER {event} ON {table} {when} BEGIN {body} END", event.to_lowercase())).await?;
            }
        }
        // Valid cleanup cursor advancement does not affect geometry or holds.
        // Crossing from/to invalid bounds does affect LayoutScan's refusal.
        db.execute_unprepared(&format!("CREATE TRIGGER sweep_cleanup_bounds AFTER UPDATE OF cleanup_next ON compaction_frozen_layout WHEN (OLD.cleanup_next<0 OR OLD.cleanup_next>OLD.cleanup_to OR NEW.cleanup_next<0 OR NEW.cleanup_next>NEW.cleanup_to) AND OLD.cleanup_next IS NOT NEW.cleanup_next BEGIN {} END", consumer("NEW.manifest_id", "NEW.kind"))).await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "durable sweep generations cannot be discarded safely".into(),
        ))
    }
}
