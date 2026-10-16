//! Set queries over saved runs. No delivery rows or per-recipient result copies.
use super::authorization_scope::{TASK_SCOPE_JOINS, TASK_SCOPE_VALID};
use anyhow::{Context, Result};
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};

const TERMINAL: &str = "r.completed_at IS NOT NULL AND r.status IN ('succeeded','failed','blocked','cancelled','timed_out')";
// Set form of TaskRead's inherited ThreadAccessFacts policy: the role action
// gate is evaluated by Gateway; membership is re-read in every statement.
const ACCESS: &str = r#"(?3 OR (
    scope_workspace.is_active=1 AND EXISTS (
        SELECT 1 FROM workspace_membership wm
        WHERE wm.principal_id=?2 AND wm.workspace_id=t.workspace_id
    ) AND (scope_thread.access_class='workspace' OR EXISTS (
        SELECT 1 FROM thread_membership tm
        WHERE tm.principal_id=?2 AND tm.thread_id=scope_thread.id
    ))
))"#;
const READ_AT: &str = "COALESCE(receipt.read_at,(SELECT MIN(n.acknowledged_at) FROM user_notification_outbox n WHERE n.recipient_principal_id=?2 AND n.run_id=r.id AND n.task_id=t.id AND n.workspace_id=t.workspace_id AND n.acknowledged_at IS NOT NULL))";

#[derive(Clone, Debug)]
pub struct TaskCompletionInboxAccess<'a> {
    pub workspace_id: &'a str,
    pub principal_id: &'a str,
    /// Only the canonical TaskRead action gate's AllowAbsolute permits this.
    pub absolute: bool,
}
impl TaskCompletionInboxAccess<'_> {
    fn values(&self) -> Vec<Value> {
        vec![
            self.workspace_id.into(),
            self.principal_id.into(),
            self.absolute.into(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct TaskCompletionInboxRow {
    pub task_id: String,
    pub run_id: String,
    pub title: String,
    pub thread_id: String,
    pub thread_title: String,
    pub status: pioneer_protocol::TaskRunStatus,
    pub completed_at: i64,
    pub completed_key: String,
    pub summary: Option<String>,
    /// Bounded public-error input. Full result/data/HTML never leaves SQLite.
    pub error_json: Option<String>,
    pub read_at: Option<i64>,
}

fn relation(workspace_scan: bool) -> String {
    // Pages/count start with this workspace's tasks, then seek runs by task.
    // CROSS JOIN prevents the planner from starting at a global run index;
    // empty/small workspaces never walk other workspaces' completion history.
    // Exact get/read retain their run-PK lookup path.
    let source = if workspace_scan {
        "FROM task t INDEXED BY idx_task_workspace_status CROSS JOIN task_run r INDEXED BY idx_task_run_task_id ON r.task_id=t.id"
    } else {
        "FROM task_run r JOIN task t ON t.id=r.task_id"
    };
    format!(
        "{source} {TASK_SCOPE_JOINS} LEFT JOIN task_run_read receipt ON receipt.principal_id=?2 AND receipt.run_id=r.id WHERE t.workspace_id=?1 AND {TERMINAL} AND {TASK_SCOPE_VALID} AND scope_thread.id IS NOT NULL AND {ACCESS}"
    )
}
fn projection() -> String {
    // Only fields needed by the existing public failure projection. In
    // particular, diagnostic/provider details and full HTML are never loaded.
    format!(
        "SELECT t.id task_id,r.id run_id,t.title,scope_thread.id thread_id,COALESCE(scope_thread.name,scope_thread.id) thread_title,r.status,CAST(r.completed_at AS TEXT) completed_key,CAST(strftime('%s',r.completed_at) AS INTEGER) completed_at,substr(json_extract(r.result_json,'$.summary'),1,1024) summary,CASE WHEN r.error_json IS NOT NULL THEN json_object('code',substr(json_extract(r.error_json,'$.code'),1,128),'message',substr(json_extract(r.error_json,'$.message'),1,1024),'class',json_extract(r.error_json,'$.class')) END error_json,CAST(strftime('%s',{READ_AT}) AS INTEGER) read_at"
    )
}
fn from_row(row: sea_orm::QueryResult) -> Result<TaskCompletionInboxRow> {
    Ok(TaskCompletionInboxRow {
        task_id: row.try_get("", "task_id")?,
        run_id: row.try_get("", "run_id")?,
        title: row.try_get("", "title")?,
        thread_id: row.try_get("", "thread_id")?,
        thread_title: row.try_get("", "thread_title")?,
        status: crate::convention::task_run_status_from_db(&row.try_get::<String>("", "status")?)
            .context("invalid inbox run status")?,
        completed_at: row.try_get("", "completed_at")?,
        completed_key: row.try_get("", "completed_key")?,
        summary: row.try_get("", "summary")?,
        error_json: row.try_get("", "error_json")?,
        read_at: row.try_get("", "read_at")?,
    })
}

fn page_statement(
    access: &TaskCompletionInboxAccess<'_>,
    before: Option<(&str, &str)>,
    limit: usize,
) -> Statement {
    let mut values = access.values();
    let mut page = format!(
        "SELECT t.id task_id,r.id run_id,t.title,scope_thread.id thread_id,COALESCE(scope_thread.name,scope_thread.id) thread_title,r.status,r.completed_at,{READ_AT} read_at {}",
        relation(true)
    );
    if let Some((at, id)) = before {
        page.push_str(" AND (r.completed_at,r.id)<(?4,?5)");
        values.extend([at.into(), id.into()]);
    }
    page.push_str(&format!(
        " ORDER BY r.completed_at DESC,r.id DESC LIMIT {}",
        limit.clamp(1, 101)
    ));
    // Materialize only the bounded metadata page before extracting summaries.
    // A planner choosing a sort must never parse every historical result/HTML.
    let sql = format!(
        r#"WITH inbox_page AS MATERIALIZED ({page})
        SELECT p.task_id,p.run_id,p.title,p.thread_id,p.thread_title,p.status,
            CAST(p.completed_at AS TEXT) completed_key,CAST(strftime('%s',p.completed_at) AS INTEGER) completed_at,
            substr(json_extract(r.result_json,'$.summary'),1,1024) summary,
            CASE WHEN r.error_json IS NOT NULL THEN json_object(
                'code',substr(json_extract(r.error_json,'$.code'),1,128),
                'message',substr(json_extract(r.error_json,'$.message'),1,1024),
                'class',json_extract(r.error_json,'$.class')) END error_json,
            CAST(strftime('%s',p.read_at) AS INTEGER) read_at
        FROM inbox_page p CROSS JOIN task_run r ON r.id=p.run_id
        ORDER BY p.completed_at DESC,p.run_id DESC"#
    );
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

pub async fn list_task_completion_inbox<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    before: Option<(&str, &str)>,
    limit: usize,
) -> Result<Vec<TaskCompletionInboxRow>> {
    db.query_all_raw(page_statement(access, before, limit))
        .await?
        .into_iter()
        .map(from_row)
        .collect()
}

fn unread_count_statement(access: &TaskCompletionInboxAccess<'_>) -> Statement {
    let sql = format!(
        "SELECT COUNT(*) count {} AND {READ_AT} IS NULL",
        relation(true)
    );
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, access.values())
}

pub async fn count_unread_task_completion_inbox<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
) -> Result<u64> {
    let row = db
        .query_one_raw(unread_count_statement(access))
        .await?
        .context("inbox count missing")?;
    Ok(row.try_get::<i64>("", "count")? as u64)
}

pub async fn find_task_completion_inbox_entry<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    run_id: &str,
) -> Result<Option<TaskCompletionInboxRow>> {
    let sql = format!("{} {} AND r.id=?4", projection(), relation(false));
    let mut values = access.values();
    values.push(run_id.into());
    db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        sql,
        values,
    ))
    .await?
    .map(from_row)
    .transpose()
}

/// ACL, terminal state and receipt commit are a single writer statement. No
/// preparation reads can become stale before this write is admitted.
pub async fn mark_task_completion_inbox_read<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    run_id: &str,
    read_at: &str,
) -> Result<()> {
    let sql = format!(
        "INSERT INTO task_run_read(principal_id,run_id,read_at) SELECT ?2,r.id,?5 {} AND r.id=?4 ON CONFLICT(principal_id,run_id) DO NOTHING",
        relation(false)
    );
    let mut values = access.values();
    values.extend([run_id.into(), read_at.into()]);
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        sql,
        values,
    ))
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Production statements on the migrated schema, prepared for post-review
    // execution only. Keep the physical reader and serialized writer separate.
    #[tokio::test]
    async fn page_and_count_plans_seek_workspace_tasks_before_runs() {
        use migration::Migrator;
        use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
        use sea_orm::{ConnectOptions, Database};
        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("plan.sqlite").display()
        );
        let mut options = ConnectOptions::new(url.clone());
        options
            .max_connections(1)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        let writer = SqliteWriteExecutor::new(Database::connect(options).await.unwrap());
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        let mut options = ConnectOptions::new(url);
        options
            .max_connections(1)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let db = SqliteDatabase::from_executor(Database::connect(options).await.unwrap(), writer)
            .maintenance();
        assert!(db.reader_query_only_enabled().await.unwrap());
        let access = TaskCompletionInboxAccess {
            workspace_id: "empty",
            principal_id: "reader",
            absolute: false,
        };
        for mut statement in [
            page_statement(&access, None, 31),
            page_statement(&access, Some(("2026-10-16 10:00:00 +00:00", "run")), 31),
            unread_count_statement(&access),
        ] {
            statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
            let plan = db
                .query_all_raw(statement)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.try_get::<String>("", "detail").unwrap())
                .collect::<Vec<_>>();
            let task = plan
                .iter()
                .position(|line| {
                    line.starts_with("SEARCH t ")
                        && line.contains("idx_task_workspace_status")
                        && line.contains("workspace_id=?")
                })
                .expect("workspace index seek");
            let run = plan
                .iter()
                .position(|line| {
                    line.starts_with("SEARCH r ")
                        && line.contains("idx_task_run_task_id")
                        && line.contains("task_id=?")
                })
                .expect("task-local run index seek");
            assert!(task < run, "{plan:?}");
            assert!(
                !plan.iter().any(|line| line.starts_with("SCAN t ")
                    || line.starts_with("SCAN r ")
                    || line.contains("idx_task_run_inbox_terminal")),
                "{plan:?}"
            );
            // A temporary sort of this workspace's metadata is expected. Full
            // result extraction still occurs only after the LIMIT in the CTE.
        }
    }
}
