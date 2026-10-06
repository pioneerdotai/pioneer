use sea_orm_migration::{prelude::*, schema::*, sea_orm::Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;
const PENDING: &str = "task_event_fanout_pending";
const SEQUENCE: &str = "task_event_fanout_sequence";
const PREFIX: &str = "task_event_fanout";

// Latest is a unique (task_id, sequence) reverse seek, never an event scan.
fn latest(id: &str) -> String {
    format!("(SELECT sequence FROM task_event WHERE task_id={id} ORDER BY sequence DESC LIMIT 1)")
}
fn refresh(id: &str, cursor: &str, reset: bool, condition: &str) -> String {
    let latest = latest(id);
    let eligible = format!("({condition}) AND {latest}>({cursor})");
    let holder = if reset { ",claim_token=NULL" } else { "" };
    format!(
        r#"
        SELECT CASE WHEN {eligible} AND NOT EXISTS(SELECT 1 FROM {SEQUENCE}
            WHERE singleton=1 AND typeof(generation)='integer' AND generation<9223372036854775807)
            THEN RAISE(ABORT,'task fanout generation missing or exhausted') END;
        UPDATE {SEQUENCE} SET generation=generation+1 WHERE singleton=1 AND {eligible};
        INSERT INTO {PENDING}(task_id,newest_sequence,generation,due_at,claim_token,attempts)
            SELECT {id},{latest},generation,CAST(strftime('%s','now') AS INTEGER),NULL,0
            FROM {SEQUENCE} WHERE singleton=1 AND {eligible}
            ON CONFLICT(task_id) DO UPDATE SET
                newest_sequence=max(newest_sequence,excluded.newest_sequence),
                generation=excluded.generation{holder};
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
        let append = refresh(
            "NEW.task_id",
            "COALESCE((SELECT last_sequence FROM task_event_fanout_cursor WHERE task_id=NEW.task_id),0)",
            false,
            "1",
        );
        let insert = refresh("NEW.task_id", "NEW.last_sequence", true, "1");
        let reset = refresh(
            "NEW.task_id",
            "NEW.last_sequence",
            true,
            "NEW.last_sequence<OLD.last_sequence OR NEW.task_id IS NOT OLD.task_id OR NOT EXISTS(SELECT 1 FROM task_event_fanout_pending WHERE task_id=NEW.task_id)",
        );
        // Ordinary monotonic ACK neither changes generation nor revokes the lane.
        // Reinitialisation/deletion deliberately fences the old reservation.
        let delete = refresh(
            "OLD.task_id",
            "0",
            true,
            "EXISTS(SELECT 1 FROM task WHERE id=OLD.task_id)",
        );
        let moved = refresh(
            "OLD.task_id",
            "0",
            true,
            "OLD.task_id IS NOT NEW.task_id AND EXISTS(SELECT 1 FROM task WHERE id=OLD.task_id)",
        );
        for (name, timing, table, body) in [
            ("append", "AFTER INSERT", "task_event", append),
            (
                "cursor_insert",
                "AFTER INSERT",
                "task_event_fanout_cursor",
                format!(
                    "DELETE FROM {PENDING} WHERE task_id=NEW.task_id AND newest_sequence<=NEW.last_sequence; {insert}"
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
        // No history scan here. The separately versioned bounded bootstrap runs
        // after installation and uses the existing durable repair checkpoint.
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
        let db = manager.get_connection();
        db.execute_raw(
            db.get_database_backend().build(
                &Query::delete()
                    .from_table("read_model_repair_checkpoint")
                    .and_where(Expr::col("repair_key").eq("task_event_fanout_frontier"))
                    .to_owned(),
            ),
        )
        .await?;
        for table in [PENDING, SEQUENCE] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
