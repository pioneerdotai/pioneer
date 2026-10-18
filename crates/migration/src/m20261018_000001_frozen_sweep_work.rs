//! Schema only. Physical inventory and consumer fanout run in bounded maintenance
//! quanta, never in this migration or a foreground startup transaction.
use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_query::{Expr, ExprTrait, Func, Query};

#[derive(DeriveMigrationName)]
pub struct Migration;

fn enqueue(table: &str, id: &str, kind: &str) -> String {
    format!(
        "INSERT INTO {table}(manifest_id,kind) SELECT {id},{kind} WHERE {id} IS NOT NULL AND {kind} IN (0,1) ON CONFLICT(manifest_id,kind) DO UPDATE SET revision=revision+1,pending=1{};",
        if table == "compaction_frozen_sweep_work" {
            ",retry_at=0"
        } else {
            ",after_source=NULL"
        }
    )
}

fn consumer(id: &str, kind: &str) -> String {
    format!(
        "{}{}",
        enqueue("compaction_frozen_sweep_work", id, kind),
        enqueue("compaction_frozen_sweep_consumer", id, kind)
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        let db = m.get_connection();
        // INTEGER PRIMARY KEY deliberately aliases rowid, without AUTOINCREMENT
        // or an explicit NOT NULL column flag. These are service identities.
        for (table, key) in [
            ("compaction_frozen_sweep_work", "work_row"),
            ("compaction_frozen_sweep_consumer", "consumer_row"),
        ] {
            let mut create = Table::create();
            create
                .table(table)
                .col(
                    ColumnDef::new(key)
                        .integer()
                        .primary_key()
                        .check(Expr::col(key).gt(0)),
                )
                .col(ColumnDef::new("manifest_id").text().not_null())
                .col(
                    ColumnDef::new("kind")
                        .integer()
                        .not_null()
                        .check(Expr::col("kind").is_in([0, 1])),
                )
                .col(
                    ColumnDef::new("revision")
                        .integer()
                        .not_null()
                        .default(1)
                        .check(
                            Expr::expr(Func::cust(Alias::new("typeof")).arg(Expr::col("revision")))
                                .eq("integer")
                                .and(Expr::col("revision").gt(0)),
                        ),
                )
                .col(
                    ColumnDef::new("pending")
                        .integer()
                        .not_null()
                        .default(1)
                        .check(Expr::col("pending").is_in([0, 1])),
                );
            if table == "compaction_frozen_sweep_work" {
                create.col(ColumnDef::new("retry_at").integer().not_null().default(0));
            } else {
                create.col(ColumnDef::new("after_source").text());
            }
            create.index(Index::create().unique().col("manifest_id").col("kind"));
            m.create_table(create.to_owned()).await?;
        }
        m.create_table(
            Table::create()
                .table("compaction_frozen_sweep_fanout")
                .col(
                    ColumnDef::new("id")
                        .integer()
                        .primary_key()
                        .check(Expr::col("id").eq(1)),
                )
                .col(
                    ColumnDef::new("revision")
                        .integer()
                        .not_null()
                        .default(1)
                        .check(
                            Expr::expr(Func::cust(Alias::new("typeof")).arg(Expr::col("revision")))
                                .eq("integer")
                                .and(Expr::col("revision").gt(0)),
                        ),
                )
                .col(ColumnDef::new("after_row").integer().not_null().default(0))
                .col(ColumnDef::new("until_row").integer().not_null().default(0))
                .to_owned(),
        )
        .await?;
        m.create_table(
            Table::create()
                .table("compaction_frozen_sweep_bootstrap")
                .col(
                    ColumnDef::new("kind")
                        .integer()
                        .primary_key()
                        .check(Expr::col("kind").is_in([0, 1])),
                )
                .col(
                    ColumnDef::new("started")
                        .integer()
                        .not_null()
                        .default(0)
                        .check(Expr::col("started").is_in([0, 1])),
                )
                .col(ColumnDef::new("upper_manifest").text())
                .col(ColumnDef::new("after_manifest").text())
                .col(
                    ColumnDef::new("complete")
                        .integer()
                        .not_null()
                        .default(0)
                        .check(Expr::col("complete").is_in([0, 1])),
                )
                .to_owned(),
        )
        .await?;
        for (name, table, columns) in [
            (
                "compaction_frozen_sweep_work_pending",
                "compaction_frozen_sweep_work",
                vec!["pending", "kind", "retry_at", "manifest_id"],
            ),
            (
                "compaction_frozen_sweep_work_service",
                "compaction_frozen_sweep_work",
                vec!["pending", "kind", "work_row"],
            ),
            (
                "compaction_frozen_sweep_consumer_pending",
                "compaction_frozen_sweep_consumer",
                vec!["pending"],
            ),
            (
                "compaction_frozen_sweep_consumer_sources",
                "compaction_frozen_span",
                vec!["manifest_id", "kind", "source_manifest"],
            ),
            (
                "compaction_frozen_sweep_source_consumers",
                "compaction_frozen_span",
                vec!["source_manifest", "kind", "manifest_id"],
            ),
            (
                "compaction_frozen_sweep_bootstrap_pending",
                "compaction_frozen_sweep_bootstrap",
                vec!["complete", "kind"],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(table);
            for column in columns {
                index.col(column);
            }
            m.create_index(index.to_owned()).await?;
        }
        db.execute(
            &Query::insert()
                .into_table("compaction_frozen_sweep_fanout")
                .columns(["id"])
                .values_panic([1_i64.into()])
                .to_owned(),
        )
        .await?;
        db.execute(
            &Query::insert()
                .into_table("compaction_frozen_sweep_bootstrap")
                .columns(["kind"])
                .values_panic([0_i64.into()])
                .values_panic([1_i64.into()])
                .to_owned(),
        )
        .await?;
        // Permanent tokens prevent ABA across domain delete/recreate. Integer
        // overflow fails CHECK (SQLite promotes overflowing arithmetic to REAL),
        // rolling back the domain mutation rather than resetting a generation.
        for table in [
            "compaction_frozen_sweep_work",
            "compaction_frozen_sweep_consumer",
        ] {
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_retain BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'sweep generations are retained'); END")).await?;
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_identity BEFORE UPDATE OF manifest_id,kind ON {table} WHEN NEW.manifest_id IS NOT OLD.manifest_id OR NEW.kind IS NOT OLD.kind BEGIN SELECT RAISE(ABORT,'sweep generation identity immutable'); END")).await?;
        }
        db.execute_unprepared("CREATE TRIGGER compaction_frozen_sweep_consumer_row_identity BEFORE UPDATE OF consumer_row ON compaction_frozen_sweep_consumer WHEN NEW.consumer_row IS NOT OLD.consumer_row BEGIN SELECT RAISE(ABORT,'consumer service identity immutable'); END").await?;
        // Retention plus SQLite's automatic rowid allocation gives insertion
        // order. Reject random rowid fallback at i64 exhaustion; never insert
        // behind a captured frontier. This runs only on actual new work rows,
        // not the UPDATE branch of a coalesced UPSERT.
        db.execute_unprepared("CREATE TRIGGER compaction_frozen_sweep_work_row_insert AFTER INSERT ON compaction_frozen_sweep_work WHEN NEW.work_row<>(SELECT MAX(work_row) FROM compaction_frozen_sweep_work) BEGIN SELECT RAISE(ABORT,'work service identity exhausted'); END").await?;
        db.execute_unprepared("CREATE TRIGGER compaction_frozen_sweep_work_row_identity BEFORE UPDATE OF work_row ON compaction_frozen_sweep_work WHEN NEW.work_row IS NOT OLD.work_row BEGIN SELECT RAISE(ABORT,'work service identity immutable'); END").await?;
        for (table, family, kind) in [
            ("compaction_frozen_message_data", "message_data", "0"),
            ("compaction_frozen_import_data", "import_data", "1"),
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
                "CREATE TRIGGER compaction_frozen_sweep_{family}_insert AFTER INSERT ON {table} BEGIN {} END",
                enqueue("compaction_frozen_sweep_work", "NEW.manifest_id", kind)
            ))
            .await?;
            db.execute_unprepared(&format!(
                "CREATE TRIGGER compaction_frozen_sweep_{family}_update AFTER UPDATE ON {table} WHEN {changed} BEGIN {}{} END",
                enqueue("compaction_frozen_sweep_work", "OLD.manifest_id", kind),
                enqueue("compaction_frozen_sweep_work", "NEW.manifest_id", kind)
            ))
            .await?;
            // DELETE cannot add physical work. In particular our own bounded
            // deletes must not dirty themselves. INSERT and repair UPDATE do.
        }
        let header_fields = "id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready,expired,storage_registered";
        let layout_fields = "manifest_id,kind,active,pending,candidate,compared,copy_to,copy_next,cleanup_to,failed";
        for (table, family, fields) in [
            ("compaction_frozen_history", "history", header_fields),
            ("compaction_frozen_layout", "layout", layout_fields),
            (
                "compaction_frozen_span",
                "span",
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
                            "compaction_frozen_sweep_work",
                            &format!("{side}.{target}"),
                            &format!("{side}.kind"),
                        );
                    }
                }
                db.execute_unprepared(&format!("CREATE TRIGGER compaction_frozen_sweep_{family}_{} AFTER {event} ON {table} {when} BEGIN {body} END", event.to_lowercase())).await?;
            }
        }
        // Valid cleanup cursor advancement does not affect geometry or holds.
        // Crossing from/to invalid bounds does affect LayoutScan's refusal.
        db.execute_unprepared(&format!("CREATE TRIGGER compaction_frozen_sweep_cleanup_bounds AFTER UPDATE OF cleanup_next ON compaction_frozen_layout WHEN (OLD.cleanup_next<0 OR OLD.cleanup_next>OLD.cleanup_to OR NEW.cleanup_next<0 OR NEW.cleanup_next>NEW.cleanup_to) AND OLD.cleanup_next IS NOT NEW.cleanup_next BEGIN {} END", consumer("NEW.manifest_id", "NEW.kind"))).await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "durable sweep generations cannot be discarded safely".into(),
        ))
    }
}
