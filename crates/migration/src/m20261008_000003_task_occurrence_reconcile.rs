//! Dirty locators for Task occurrence contracts (independent of parent Turns).
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const PENDING: &str = "task_occurrence_reconcile_pending";
const SEQUENCE: &str = "task_occurrence_reconcile_sequence";
const SCOPE: &str = "task_occurrence_reconcile_scope";
const SEED: &str = "task_occurrence_reconcile_seed";
const PREFIX: &str = "task_occurrence_reconcile";

fn advance(condition: &str) -> String {
    format!(
        "SELECT CASE WHEN ({condition}) AND (SELECT generation FROM {SEQUENCE} WHERE singleton=1)=9223372036854775807 THEN RAISE(ABORT,'task contract generation exhausted') END;\n\
         UPDATE {SEQUENCE} SET generation=generation+1 WHERE singleton=1 AND ({condition});"
    )
}

fn dirty(key: &str, condition: &str) -> String {
    format!(
        "{}\n\
         INSERT INTO {PENDING}(run_id,generation,next_attempt_at,attempt_count,claim_token)\n\
         SELECT {key},generation,CAST(strftime('%s','now') AS INTEGER),0,NULL FROM {SEQUENCE}\n\
         WHERE singleton=1 AND ({condition}) AND {key} IS NOT NULL\n\
         ON CONFLICT(run_id) DO UPDATE SET generation=excluded.generation,claim_token=NULL;",
        advance(&format!("({condition}) AND {key} IS NOT NULL"))
    )
}

fn scope(key: &str, condition: &str) -> String {
    format!(
        "{}\n\
         INSERT INTO {SCOPE}(task_id,generation,after_run_id,upper_run_id)\n\
         SELECT {key},generation,NULL,(SELECT id FROM task_run WHERE task_id={key} ORDER BY id DESC LIMIT 1)\n\
         FROM {SEQUENCE} WHERE singleton=1 AND ({condition})\n\
         ON CONFLICT(task_id) DO UPDATE SET generation=excluded.generation,after_run_id=NULL,upper_run_id=excluded.upper_run_id;",
        advance(condition)
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
        manager
            .create_table(
                Table::create()
                    .table(SCOPE)
                    .col(text("task_id").primary_key())
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>0")),
                    )
                    .col(text("after_run_id").null())
                    .col(text("upper_run_id").null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(SEED)
                    .col(
                        integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    // 0..4 are unfinished statuses; 5 is the durable completion marker.
                    .col(integer("status_index").check(Expr::col("status_index").between(0, 5)))
                    .col(text("after_run_id").null())
                    .col(text("upper_run_id").null())
                    .to_owned(),
            )
            .await?;
        for (name, table, columns) in [
            (
                "idx_task_occurrence_reconcile_due",
                PENDING,
                &["next_attempt_at", "generation", "run_id"][..],
            ),
            (
                "idx_task_occurrence_reconcile_scope",
                SCOPE,
                &["generation", "task_id"][..],
            ),
            ("idx_task_run_task_id", "task_run", &["task_id", "id"][..]),
            ("idx_task_run_status_id", "task_run", &["status", "id"][..]),
        ] {
            let mut index = Index::create();
            index.name(name).table(table);
            for column in columns {
                index.col(*column);
            }
            manager.create_index(index.to_owned()).await?;
        }
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
        for (table, columns, key) in [
            (
                "task_run",
                &["id", "task_id", "status", "executor_kind", "completed_at"][..],
                "id",
            ),
            (
                "task_occurrence_contract",
                &[
                    "occurrence_id",
                    "run_id",
                    "task_id",
                    "status",
                    "execution_generation",
                    "agent_execution_id",
                    "work_graph_root_execution_id",
                    "root_resource_scope_id",
                ][..],
                "run_id",
            ),
            (
                "task_run_execution",
                &[
                    "id",
                    "task_run_id",
                    "task_id",
                    "executor_kind",
                    "status",
                    "completed_at",
                ][..],
                "task_run_id",
            ),
            (
                "agent_execution",
                &[
                    "id",
                    "workspace_id",
                    "parent_task_id",
                    "execution_generation",
                    "status",
                    "finished_at",
                    "work_graph_root_execution_id",
                ][..],
                "id",
            ),
            ("task", &["id", "workspace_id", "executor_kind"][..], "id"),
        ] {
            let locator = |side: &str| {
                if table == "agent_execution" {
                    // PK lookup only, including late INSERT/deletion; no history fanout.
                    format!("(SELECT task_run_id FROM task_run_execution WHERE id={side}.id)")
                } else {
                    format!("{side}.{key}")
                }
            };
            let refresh = |side: &str, condition: &str| {
                if table == "task" {
                    scope(&locator(side), condition)
                } else {
                    dirty(&locator(side), condition)
                }
            };
            for event in ["insert", "update", "delete"] {
                let (timing, when, body) = match event {
                    "insert" => ("INSERT".to_owned(), String::new(), refresh("NEW", "1")),
                    "delete" => ("DELETE".to_owned(), String::new(), refresh("OLD", "1")),
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
                            "{}\n{}",
                            refresh("OLD", &format!("OLD.{key} IS NOT NEW.{key}")),
                            refresh("NEW", "1")
                        ),
                    ),
                };
                db.execute_unprepared(&format!(
                    "CREATE TRIGGER {PREFIX}_{table}_{event} AFTER {timing} ON {table} {when} BEGIN\n\
                     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE} WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0) THEN RAISE(ABORT,'task contract sequence missing or invalid') END;\n\
                     {body}\nEND;"
                )).await?;
            }
        }
        // Triggers precede the fixed seed bound. Each MAX uses (status,id):
        // terminal history is accepted without reading it. Index construction
        // above does read existing task_run storage once per new index.
        db.execute_unprepared(&format!(
            "INSERT INTO {SEED}(singleton,status_index,after_run_id,upper_run_id)\n\
             SELECT 1,0,NULL,MAX(id) FROM (\n\
             SELECT MAX(id) AS id FROM task_run WHERE status='queued' UNION ALL\n\
             SELECT MAX(id) FROM task_run WHERE status='starting' UNION ALL\n\
             SELECT MAX(id) FROM task_run WHERE status='running' UNION ALL\n\
             SELECT MAX(id) FROM task_run WHERE status='waiting' UNION ALL\n\
             SELECT MAX(id) FROM task_run WHERE status='waiting_review');"
        ))
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in [
            "task_run",
            "task_occurrence_contract",
            "task_run_execution",
            "agent_execution",
            "task",
        ] {
            for event in ["insert", "update", "delete"] {
                manager
                    .get_connection()
                    .execute_unprepared(&format!("DROP TRIGGER IF EXISTS {PREFIX}_{table}_{event}"))
                    .await?;
            }
        }
        for name in ["idx_task_run_task_id", "idx_task_run_status_id"] {
            manager
                .drop_index(Index::drop().name(name).to_owned())
                .await?;
        }
        for table in [PENDING, SCOPE, SEED, SEQUENCE] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
