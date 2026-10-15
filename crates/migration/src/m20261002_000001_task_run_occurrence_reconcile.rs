use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const PENDING: &str = "task_run_occurrence_reconcile_pending";
const SEQUENCE: &str = "task_run_occurrence_reconcile_sequence";
const PREFIX: &str = "task_run_occurrence_reconcile";

// Keep the original SQL's three-valued comparisons: NULL is not a mismatch,
// unknown non-NULL Turn statuses are mismatches, unknown Run statuses are not.
fn mismatch(id: &str) -> String {
    format!(
        r#"EXISTS(SELECT 1 FROM task_run r JOIN "turn" t ON t.id=r.id
        WHERE r.id={id} AND t.turn_kind='task_run' AND (
            (r.status='succeeded' AND t.status<>'completed') OR
            (r.status IN ('failed','timed_out') AND t.status<>'failed') OR
            (r.status='blocked' AND t.status<>'blocked') OR
            (r.status='cancelled' AND t.status<>'interrupted')))"#
    )
}

fn refresh(id: &str, condition: &str) -> String {
    let m = mismatch(id);
    format!(
        r#"
        DELETE FROM {PENDING} WHERE run_id={id} AND ({condition}) AND NOT {m};
        SELECT CASE WHEN ({condition}) AND {m} AND
            (SELECT generation FROM {SEQUENCE} WHERE singleton=1)=9223372036854775807
            THEN RAISE(ABORT,'task occurrence generation exhausted') END;
        UPDATE {SEQUENCE} SET generation=generation+1
            WHERE singleton=1 AND ({condition}) AND {m};
        INSERT INTO {PENDING}(run_id,generation,next_attempt_at,attempt_count,claim_token)
            SELECT {id},generation,CAST(strftime('%s','now') AS INTEGER),0,NULL
            FROM {SEQUENCE} WHERE singleton=1 AND ({condition}) AND {m}
            ON CONFLICT(run_id) DO UPDATE SET generation=excluded.generation,claim_token=NULL;
    "#
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PENDING)
                    .col(text("run_id").primary_key())
                    // SQLite affinity alone permits INTEGER overflow to become REAL.
                    // typeof is SQLite-specific; the surrounding DDL uses builders.
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>0")),
                    )
                    .col(
                        big_integer("next_attempt_at")
                            .check(Expr::cust("typeof(next_attempt_at)='integer'")),
                    )
                    .col(
                        integer("attempt_count")
                            .default(0)
                            .check(Expr::col("attempt_count").between(0, 16)),
                    )
                    .col(text("claim_token").null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(SEQUENCE)
                    .col(
                        integer("singleton")
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
        db.execute_raw(
            db.get_database_backend().build(
                &Query::insert()
                    .into_table(SEQUENCE)
                    .columns(["singleton", "generation"])
                    .values_panic([1.into(), 0.into()])
                    .to_owned(),
            ),
        )
        .await?;
        // Discovery projects only run_id/generation. All selected columns and
        // the due range/order are covered; attempts/tokens need no index copy.
        manager
            .create_index(
                Index::create()
                    .name("idx_task_run_occurrence_reconcile_due")
                    .table(PENDING)
                    .col("next_attempt_at")
                    .col("generation")
                    .col("run_id")
                    .to_owned(),
            )
            .await?;

        for (table, columns) in [
            (
                "task_run",
                &["id", "status", "completed_at", "error_json"][..],
            ),
            ("turn", &["id", "status", "turn_kind", "thread_id"][..]),
        ] {
            for event in ["insert", "update", "delete"] {
                let (timing, when, body) = match event {
                    "insert" => ("INSERT".to_owned(), String::new(), refresh("NEW.id", "1")),
                    "delete" => (
                        "DELETE".to_owned(),
                        String::new(),
                        format!("DELETE FROM {PENDING} WHERE run_id=OLD.id;"),
                    ),
                    _ => (
                        format!("UPDATE OF {}", columns.join(",")),
                        format!(
                            "WHEN {}",
                            columns
                                .iter()
                                .map(|c| format!("OLD.{c} IS NOT NEW.{c}"))
                                .collect::<Vec<_>>()
                                .join(" OR ")
                        ),
                        format!(
                            "{}{}",
                            refresh("OLD.id", "OLD.id IS NOT NEW.id"),
                            refresh("NEW.id", "1")
                        ),
                    ),
                };
                // SeaQuery has no SQLite CREATE TRIGGER builder. Identifiers
                // and predicate fragments here are static migration constants.
                db.execute_unprepared(&format!(
                    r#"
                    CREATE TRIGGER {PREFIX}_{table}_{event} AFTER {timing} ON "{table}" {when}
                    BEGIN
                        SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE}
                            WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0)
                            THEN RAISE(ABORT,'task occurrence sequence missing or invalid') END;
                        {body}
                    END;
                "#
                ))
                .await?;
            }
        }
        // Deliberately no source SELECT/backfill: installation accepts history.
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in ["task_run", "turn"] {
            for event in ["insert", "update", "delete"] {
                // SQLite DROP TRIGGER likewise has no builder.
                manager
                    .get_connection()
                    .execute_unprepared(&format!("DROP TRIGGER IF EXISTS {PREFIX}_{table}_{event}"))
                    .await?;
            }
        }
        // Dropping the pending table also removes its dependent due index.
        for table in [PENDING, SEQUENCE] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
