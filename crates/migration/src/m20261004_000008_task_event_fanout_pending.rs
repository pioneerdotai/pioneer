use sea_orm_migration::{prelude::*, schema::*, sea_orm::Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;
const PENDING: &str = "task_event_fanout_pending";
const SEQUENCE: &str = "task_event_fanout_sequence";
const PREFIX: &str = "task_event_fanout";

// Cursor mutations fence existing work only. They never discover event history.
fn fence(id: &str, condition: &str) -> String {
    let eligible = format!("({condition}) AND EXISTS(SELECT 1 FROM {PENDING} WHERE task_id={id})");
    format!(
        r#"
        SELECT CASE WHEN {eligible} AND NOT EXISTS(SELECT 1 FROM {SEQUENCE}
            WHERE singleton=1 AND typeof(generation)='integer' AND generation<9223372036854775807)
            THEN RAISE(ABORT,'task fanout generation missing or exhausted') END;
        UPDATE {SEQUENCE} SET generation=generation+1 WHERE singleton=1 AND {eligible};
        UPDATE {PENDING} SET generation=(SELECT generation FROM {SEQUENCE} WHERE singleton=1),
            claim_token=NULL WHERE task_id={id} AND ({condition});
    "#
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // octet_length(column) is the stored byte size. Fanout's Rust payload
        // budget is UTF-8, as are Pioneer/SQLx-created databases; reject an
        // externally created UTF-16 schema rather than undercount its JSON.
        let db = manager.get_connection();
        let encoding = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA encoding".to_owned(),
            ))
            .await?
            .ok_or_else(|| DbErr::Custom("database encoding unavailable".into()))?;
        if encoding.try_get::<String>("", "encoding")? != "UTF-8" {
            return Err(DbErr::Custom(
                "task fanout requires UTF-8 database encoding".into(),
            ));
        }
        manager
            .create_table(
                Table::create()
                    .table(PENDING)
                    .col(text("task_id").primary_key())
                    .col(big_integer("first_sequence").check(Expr::cust(
                        "typeof(first_sequence)='integer' AND first_sequence>0 AND first_sequence<=newest_sequence",
                    )))
                    .col(big_integer("newest_sequence").check(Expr::cust(
                        "typeof(newest_sequence)='integer' AND newest_sequence>0",
                    )))
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>0")),
                    )
                    .col(big_integer("due_at").check(Expr::cust("typeof(due_at)='integer'")))
                    .col(text("claim_token").null())
                    .col(
                        big_integer("attempts")
                            .default(0)
                            .check(Expr::col("attempts").between(0, 16)),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(SEQUENCE)
                    .col(
                        big_integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>=0")),
                    )
                    .to_owned(),
            )
            .await?;
        let db = manager.get_connection();
        db.execute_unprepared(&format!("INSERT INTO {SEQUENCE} VALUES(1,0)"))
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_task_event_fanout_due")
                    .table(PENDING)
                    .col("due_at")
                    .col("generation")
                    .col("task_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uidx_task_event_fanout_generation")
                    .table(PENDING)
                    .col("generation")
                    .unique()
                    .to_owned(),
            )
            .await?;
        // Ordered point context probes. Building these indexes is a one-time
        // O(existing metadata rows) migration cost, not an idle history scan.
        for (name, table, key) in [
            ("idx_task_trigger_task_created", "task_trigger", "task_id"),
            (
                "idx_task_agent_spec_task_created",
                "task_agent_spec",
                "task_id",
            ),
            (
                "idx_task_agent_spec_run_created",
                "task_agent_spec",
                "run_id",
            ),
        ] {
            manager
                .create_index(
                    Index::create()
                        .name(name)
                        .table(table)
                        .col(key)
                        .col("created_at")
                        .to_owned(),
                )
                .await?;
        }
        db.execute_unprepared(
            "CREATE INDEX idx_task_delivery_fanout_pending ON task_delivery(task_id,run_id,workspace_id,updated_at) WHERE status IN ('pending','delivering')",
        ).await?;
        // Source allocation is monotonic per Task, including within an atomic
        // batch. The first physical INSERT establishes the floor; duplicates
        // have no INSERT and appends retain the floor, holder and retry delay.
        let append = format!(
            r#"
            SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE}
                WHERE singleton=1 AND typeof(generation)='integer' AND generation<9223372036854775807)
                THEN RAISE(ABORT,'task fanout generation missing or exhausted') END;
            UPDATE {SEQUENCE} SET generation=generation+1 WHERE singleton=1;
            INSERT INTO {PENDING}(task_id,first_sequence,newest_sequence,generation,due_at,claim_token,attempts)
                SELECT NEW.task_id,NEW.sequence,NEW.sequence,generation,
                    CAST(strftime('%s','now') AS INTEGER),NULL,0
                FROM {SEQUENCE} WHERE singleton=1
                ON CONFLICT(task_id) DO UPDATE SET
                    newest_sequence=max(newest_sequence,excluded.newest_sequence),
                    generation=excluded.generation;
        "#
        );
        let reset = fence(
            "NEW.task_id",
            "NEW.last_sequence<OLD.last_sequence OR NEW.task_id IS NOT OLD.task_id",
        );
        let delete = fence(
            "OLD.task_id",
            "EXISTS(SELECT 1 FROM task WHERE id=OLD.task_id)",
        );
        let moved = fence("OLD.task_id", "OLD.task_id IS NOT NEW.task_id");
        for (name, timing, table, body) in [
            ("append", "AFTER INSERT", "task_event", append),
            (
                "cursor_insert",
                "AFTER INSERT",
                "task_event_fanout_cursor",
                format!(
                    "DELETE FROM {PENDING} WHERE task_id=NEW.task_id AND newest_sequence<=NEW.last_sequence;"
                ),
            ),
            (
                "cursor_update",
                "AFTER UPDATE OF task_id,last_sequence",
                "task_event_fanout_cursor",
                format!(
                    "{moved} DELETE FROM {PENDING} WHERE task_id=NEW.task_id AND newest_sequence<=NEW.last_sequence; {reset}"
                ),
            ),
            (
                "cursor_delete",
                "AFTER DELETE",
                "task_event_fanout_cursor",
                format!(
                    "{delete} DELETE FROM {PENDING} WHERE task_id=OLD.task_id AND NOT EXISTS(SELECT 1 FROM task WHERE id=OLD.task_id);"
                ),
            ),
            (
                "task_delete",
                "AFTER DELETE",
                "task",
                format!("DELETE FROM {PENDING} WHERE task_id=OLD.id;"),
            ),
        ] {
            db.execute_unprepared(&format!(
                "CREATE TRIGGER {PREFIX}_{name} {timing} ON {table} BEGIN {body} END"
            ))
            .await?;
        }
        // Installation deliberately leaves pending empty. Cursor INSERT only
        // reconciles completion: DELETE already fenced an old holder, and the
        // selected claim may create its absent zero cursor in the same commit.
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "append",
            "cursor_insert",
            "cursor_update",
            "cursor_delete",
            "task_delete",
        ] {
            manager
                .get_connection()
                .execute_unprepared(&format!("DROP TRIGGER IF EXISTS {PREFIX}_{name}"))
                .await?;
        }
        for (name, table) in [
            ("idx_task_trigger_task_created", "task_trigger"),
            ("idx_task_agent_spec_task_created", "task_agent_spec"),
            ("idx_task_agent_spec_run_created", "task_agent_spec"),
        ] {
            manager
                .drop_index(Index::drop().name(name).table(table).to_owned())
                .await?;
        }
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX idx_task_delivery_fanout_pending")
            .await?;
        for table in [PENDING, SEQUENCE] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
