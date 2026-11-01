//! Scoped replacement of exactly the two known occurrence producer generations.
//! Trigger DDL/catalog inspection is the local API gap: SeaQuery has no trigger
//! builder or portable metadata model. Unknown generations remain installed.
use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::{DbBackend, Statement};
#[derive(DeriveMigrationName)]
pub struct Migration;
fn normalized(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
fn names() -> Vec<String> {
    let mut out = Vec::new();
    for (prefix, tables) in [
        ("task_run_occurrence_reconcile", &["task_run", "turn"][..]),
        (
            "task_occurrence_reconcile",
            &[
                "task_run",
                "task_occurrence_contract",
                "task_run_execution",
                "agent_execution",
                "task",
            ][..],
        ),
    ] {
        for table in tables {
            for event in ["insert", "update", "delete"] {
                out.push(format!("{prefix}_{table}_{event}"));
            }
        }
    }
    out
}
async fn installed(manager: &SchemaManager<'_>) -> Result<Vec<String>, DbErr> {
    let rows=manager.get_connection().query_all_raw(Statement::from_string(DbBackend::Sqlite,
 "SELECT sql FROM sqlite_master WHERE type='trigger' AND (name GLOB 'task_run_occurrence_reconcile_*' OR name GLOB 'task_occurrence_reconcile_*') ORDER BY name")).await?;
    rows.into_iter().map(|r| r.try_get("", "sql")).collect()
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        if m.get_database_backend() != DbBackend::Sqlite {
            return Ok(());
        }
        let mut actual = installed(m)
            .await?
            .iter()
            .map(|s| normalized(s))
            .collect::<Vec<_>>();
        let mut expected = parent::definitions()
            .into_iter()
            .chain(contract::definitions())
            .map(|s| normalized(&s))
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        // Partial or altered historical generations retain all their triggers. The
        // runtime detects their presence and never double-fills with typed helpers.
        if actual != expected {
            return Ok(());
        }
        for name in names() {
            m.get_connection()
                .execute_unprepared(&format!("DROP TRIGGER {name}"))
                .await?;
        }
        Ok(())
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        if m.get_database_backend() != DbBackend::Sqlite {
            return Ok(());
        }
        if !installed(m).await?.is_empty() {
            return Ok(());
        }
        for sql in parent::definitions()
            .into_iter()
            .chain(contract::definitions())
        {
            m.get_connection().execute_unprepared(&sql).await?;
        }
        Ok(())
    }
}
mod parent {
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

    pub(super) fn definitions() -> Vec<String> {
        let mut sql = Vec::new();
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
                sql.push(format!(
                    r#"
                    CREATE TRIGGER {PREFIX}_{table}_{event} AFTER {timing} ON "{table}" {when}
                    BEGIN
                        SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE}
                            WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0)
                            THEN RAISE(ABORT,'task occurrence sequence missing or invalid') END;
                        {body}
                    END;
                "#
                ));
            }
        }

        sql
    }
}
mod contract {
    const PENDING: &str = "task_occurrence_reconcile_pending";
    const SEQUENCE: &str = "task_occurrence_reconcile_sequence";
    const SCOPE: &str = "task_occurrence_reconcile_scope";
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

    pub(super) fn definitions() -> Vec<String> {
        let mut sql = Vec::new();
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
                sql.push(format!(
                    "CREATE TRIGGER {PREFIX}_{table}_{event} AFTER {timing} ON {table} {when} BEGIN\n\
                     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE} WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0) THEN RAISE(ABORT,'task contract sequence missing or invalid') END;\n\
                     {body}\nEND;"
                ));
            }
        }

        sql
    }
}
