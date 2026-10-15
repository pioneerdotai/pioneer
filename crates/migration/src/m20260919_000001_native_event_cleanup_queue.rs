use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const METHODS: &str = "'item/started','item/completed','item/agentMessage/delta',
    'item/commandExecution/outputDelta','turn/diff/updated',
    'thread/tokenUsage/updated','account/rateLimits/updated'";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        // Schema/index installation only: source backfill is cooperative maintenance.
        // Stable primary-key cursor survives VACUUM; live inserts are captured by
        // triggers even when their random ID precedes the bootstrap cursor.
        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_job")
                    .col(text("turn_id").primary_key())
                    .col(text("state").check(Expr::col("state").is_in(["queued", "waiting"])))
                    .col(big_integer("available_at").default(0))
                    .col(big_integer("last_served_at").default(0))
                    .col(big_integer("revision").default(1))
                    .col(text("last_error").null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_bootstrap")
                    .col(
                        integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    .col(text("cursor_id").null())
                    .col(
                        boolean("complete")
                            .default(false)
                            .check(Expr::col("complete").is_in([0, 1])),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_scheduler")
                    .col(
                        integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    .col(
                        big_integer("new_jobs_since_served")
                            .default(0)
                            .check(Expr::col("new_jobs_since_served").between(0, 4)),
                    )
                    .to_owned(),
            )
            .await?;
        for table in [
            "native_event_cleanup_bootstrap",
            "native_event_cleanup_scheduler",
        ] {
            let insert = Query::insert()
                .into_table(table)
                .columns(["singleton"])
                .values_panic([1.into()])
                .to_owned();
            db.execute_raw(db.get_database_backend().build(&insert))
                .await?;
        }
        let queued = Expr::col("state").eq("queued");
        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_new")
                    .table("native_event_cleanup_job")
                    .col("turn_id")
                    .and_where(
                        queued
                            .clone()
                            .and(Expr::col("available_at").eq(0))
                            .and(Expr::col("last_served_at").eq(0)),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_served")
                    .table("native_event_cleanup_job")
                    .col("last_served_at")
                    .col("turn_id")
                    .and_where(
                        queued
                            .clone()
                            .and(Expr::col("available_at").eq(0))
                            .and(Expr::col("last_served_at").gt(0)),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_due_retry")
                    .table("native_event_cleanup_job")
                    .col("available_at")
                    .col("turn_id")
                    .and_where(queued.and(Expr::col("available_at").gt(0)))
                    .to_owned(),
            )
            .await?;
        // SeaQuery 1.0.2's SQLite IndexBuilder panics on IndexColumn::Expr.
        // Keep only this expression-index DDL in SQL; the other indexes above
        // and below use SchemaManager. Preserve the accepted payload-size key.
        db.execute_unprepared(&format!(
            "CREATE INDEX native_event_cleanup_candidate ON cli_runtime_native_event(
                turn_id,runtime_id,id,length(CAST(payload_redacted_json AS BLOB)))
             WHERE turn_id IS NOT NULL AND native_method IN ({METHODS})
                AND length(CAST(payload_redacted_json AS BLOB))<=262144"
        ))
        .await?;
        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_recovery_blocker")
                    .table("recovery_job")
                    .col("turn_id")
                    .and_where(
                        Expr::col("status")
                            .is_in(["pending", "active"])
                            .or(Expr::col("resolution_pending").eq(1)),
                    )
                    .to_owned(),
            )
            .await?;

        // SeaQuery has no SQLite CREATE TRIGGER builder; trigger DDL is raw,
        // matching the other trigger migrations. Tables, ordinary indexes and
        // seed rows above use builders.
        db.execute_unprepared(&format!(r#"
            CREATE TRIGGER native_event_cleanup_event_insert_register
            AFTER INSERT ON cli_runtime_native_event
            WHEN NEW.turn_id IS NOT NULL AND NEW.native_method IN ({METHODS})
                AND length(CAST(NEW.payload_redacted_json AS BLOB))<=262144
            BEGIN
                INSERT OR IGNORE INTO native_event_cleanup_job(turn_id,state) VALUES(NEW.turn_id,'queued');
            END;
            CREATE TRIGGER native_event_cleanup_event_insert_wake
            AFTER INSERT ON cli_runtime_native_event
            WHEN NEW.turn_id IS NOT NULL AND NEW.native_method IN ({METHODS})
                AND length(CAST(NEW.payload_redacted_json AS BLOB))<=262144
                AND EXISTS(SELECT 1 FROM "turn" t JOIN turn_cli_runtime_binding b ON b.turn_id=t.id
                    WHERE t.id=NEW.turn_id AND t.status IN ('completed','failed','interrupted')
                        AND b.status IN ('completed','failed','interrupted') AND b.runtime_id=NEW.runtime_id)
            BEGIN
                UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                WHERE turn_id=NEW.turn_id AND state='waiting';
            END;
            CREATE TRIGGER native_event_cleanup_event_delete AFTER DELETE ON cli_runtime_native_event
            WHEN EXISTS(SELECT 1 FROM native_event_cleanup_job WHERE turn_id=OLD.turn_id AND state='waiting')
            BEGIN
                UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                WHERE turn_id=OLD.turn_id AND state='waiting' AND CASE
                    WHEN OLD.turn_id IS NULL THEN 0
                    WHEN OLD.native_method IS NULL OR OLD.native_method NOT IN ({METHODS}) THEN 0
                    ELSE length(CAST(OLD.payload_redacted_json AS BLOB))<=262144 END;
            END;
            CREATE TRIGGER native_event_cleanup_event_update
            AFTER UPDATE OF turn_id,runtime_id,native_method,payload_redacted_json ON cli_runtime_native_event
            BEGIN
                UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                WHERE turn_id=OLD.turn_id AND state='waiting' AND OLD.turn_id IS NOT NULL
                    AND OLD.native_method IN ({METHODS}) AND length(CAST(OLD.payload_redacted_json AS BLOB))<=262144
                    AND (OLD.turn_id IS NOT NEW.turn_id OR OLD.runtime_id IS NOT NEW.runtime_id
                        OR OLD.native_method IS NOT NEW.native_method OR OLD.payload_redacted_json IS NOT NEW.payload_redacted_json)
                    AND EXISTS(SELECT 1 FROM "turn" WHERE id=OLD.turn_id AND status IN ('completed','failed','interrupted'));
                INSERT OR IGNORE INTO native_event_cleanup_job(turn_id,state)
                SELECT NEW.turn_id,'queued' WHERE NEW.turn_id IS NOT NULL AND NEW.native_method IN ({METHODS})
                    AND length(CAST(NEW.payload_redacted_json AS BLOB))<=262144;
                UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                WHERE turn_id=NEW.turn_id AND state='waiting' AND NEW.turn_id IS NOT NULL
                    AND NEW.native_method IN ({METHODS}) AND length(CAST(NEW.payload_redacted_json AS BLOB))<=262144
                    AND EXISTS(SELECT 1 FROM "turn" t JOIN turn_cli_runtime_binding b ON b.turn_id=t.id
                        WHERE t.id=NEW.turn_id AND t.status IN ('completed','failed','interrupted')
                            AND b.status IN ('completed','failed','interrupted') AND b.runtime_id=NEW.runtime_id);
            END;
        "#)).await?;

        for (name, timing, table, turn, predicate) in [
            (
                "turn_insert",
                "AFTER INSERT",
                "\"turn\"",
                "NEW.id",
                "NEW.status IN ('completed','failed','interrupted')",
            ),
            (
                "turn_update",
                "AFTER UPDATE OF status",
                "\"turn\"",
                "NEW.id",
                "OLD.status IS NOT NEW.status AND NEW.status IN ('completed','failed','interrupted')",
            ),
            (
                "binding_insert",
                "AFTER INSERT",
                "turn_cli_runtime_binding",
                "NEW.turn_id",
                "NEW.status IN ('completed','failed','interrupted')",
            ),
            (
                "binding_update",
                "AFTER UPDATE OF runtime_id,status",
                "turn_cli_runtime_binding",
                "NEW.turn_id",
                "(OLD.runtime_id IS NOT NEW.runtime_id OR OLD.status IS NOT NEW.status) AND NEW.status IN ('completed','failed','interrupted')",
            ),
            (
                "attempt_update",
                "AFTER UPDATE OF status",
                "turn_cli_runtime_attempt",
                "NEW.turn_id",
                "OLD.status IN ('starting','running') AND NEW.status NOT IN ('starting','running')",
            ),
            (
                "attempt_delete",
                "AFTER DELETE",
                "turn_cli_runtime_attempt",
                "OLD.turn_id",
                "OLD.status IN ('starting','running')",
            ),
            (
                "segment_update",
                "AFTER UPDATE OF status",
                "turn_cli_runtime_execution_segment",
                "NEW.turn_id",
                "OLD.status='running' AND NEW.status<>'running'",
            ),
            (
                "segment_delete",
                "AFTER DELETE",
                "turn_cli_runtime_execution_segment",
                "OLD.turn_id",
                "OLD.status='running'",
            ),
            (
                "recovery_update",
                "AFTER UPDATE OF status,resolution_pending",
                "recovery_job",
                "NEW.turn_id",
                "(OLD.status IN ('pending','active') OR OLD.resolution_pending=1) AND NOT (NEW.status IN ('pending','active') OR NEW.resolution_pending=1)",
            ),
            (
                "recovery_delete",
                "AFTER DELETE",
                "recovery_job",
                "OLD.turn_id",
                "OLD.status IN ('pending','active') OR OLD.resolution_pending=1",
            ),
            (
                "projection_insert",
                "AFTER INSERT",
                "turn_event_projection_stream_state",
                "NEW.turn_id",
                "NEW.status='healthy' AND NEW.projected_through_sequence>0",
            ),
            (
                "projection_update",
                "AFTER UPDATE OF status,projected_through_sequence",
                "turn_event_projection_stream_state",
                "NEW.turn_id",
                "(OLD.status IS NOT NEW.status OR OLD.projected_through_sequence IS NOT NEW.projected_through_sequence) AND NEW.status='healthy' AND NEW.projected_through_sequence>0",
            ),
        ] {
            db.execute_unprepared(&format!(r#"
                CREATE TRIGGER native_event_cleanup_{name} {timing} ON {table}
                WHEN ({predicate}) AND EXISTS(SELECT 1 FROM "turn" WHERE id={turn} AND status IN ('completed','failed','interrupted'))
                BEGIN
                    UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                    WHERE turn_id={turn} AND state='waiting';
                END;
            "#)).await?;
        }
        for (name, timing, changed) in [
            ("receipt_delete", "AFTER DELETE", "1"),
            (
                "receipt_update",
                "AFTER UPDATE OF turn_id,sequence",
                "(OLD.turn_id IS NOT NEW.turn_id OR OLD.sequence IS NOT NEW.sequence)",
            ),
        ] {
            db.execute_unprepared(&format!(r#"
                CREATE TRIGGER native_event_cleanup_{name} {timing} ON turn_event_projection_state
                WHEN {changed} AND EXISTS(
                    SELECT 1 FROM "turn" t JOIN turn_event_projection_stream_state p ON p.turn_id=t.id
                    WHERE t.id=OLD.turn_id AND t.status IN ('completed','failed','interrupted')
                        AND OLD.sequence>p.projected_through_sequence
                        AND NOT EXISTS(SELECT 1 FROM turn_event_projection_state remaining
                            WHERE remaining.turn_id=OLD.turn_id AND remaining.sequence>p.projected_through_sequence))
                BEGIN
                    UPDATE native_event_cleanup_job SET state='queued',available_at=0,last_error=NULL,revision=revision+1
                    WHERE turn_id=OLD.turn_id AND state='waiting';
                END;
            "#)).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for name in [
            "event_insert_register",
            "event_insert_wake",
            "event_delete",
            "event_update",
            "turn_insert",
            "turn_update",
            "binding_insert",
            "binding_update",
            "attempt_update",
            "attempt_delete",
            "segment_update",
            "segment_delete",
            "recovery_update",
            "recovery_delete",
            "projection_insert",
            "projection_update",
            "receipt_delete",
            "receipt_update",
        ] {
            db.execute_unprepared(&format!(
                "DROP TRIGGER IF EXISTS native_event_cleanup_{name}"
            ))
            .await?;
        }
        for name in [
            "native_event_cleanup_candidate",
            "native_event_cleanup_recovery_blocker",
        ] {
            manager
                .drop_index(Index::drop().name(name).if_exists().to_owned())
                .await?;
        }
        for table in [
            "native_event_cleanup_job",
            "native_event_cleanup_bootstrap",
            "native_event_cleanup_scheduler",
        ] {
            manager
                .drop_table(Table::drop().table(table).if_exists().to_owned())
                .await?;
        }
        Ok(())
    }
}
