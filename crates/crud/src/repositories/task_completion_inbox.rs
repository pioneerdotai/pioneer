//! Set queries over saved runs. No delivery rows or per-recipient result copies.
use super::authorization_scope::{TaskScopeAlias as S, task_scope_query};
use anyhow::{Context, Result};
use pioneer_entity::{
    task, task_run, task_run_read, thread, thread_membership, user_notification_outbox, workspace,
    workspace_membership,
};
use sea_orm::sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, JoinType, OnConflict,
    Order, Query, SelectStatement,
};
use sea_orm::{ConnectionTrait, DbBackend, DeriveIden, EntityName, FromQueryResult, Statement};

#[derive(Clone, Copy, DeriveIden)]
enum InboxAlias {
    #[sea_orm(iden = "r")]
    Run,
    Receipt,
    Legacy,
    WorkspaceMember,
    ThreadMember,
    InboxPage,
    #[sea_orm(iden = "p")]
    Page,
    ResultRun,
}
#[derive(Clone, Copy, DeriveIden)]
enum InboxField {
    TaskId,
    RunId,
    Title,
    ThreadId,
    ThreadTitle,
    Status,
    CompletedKey,
    ReadKey,
    CompletedAt,
    Summary,
    ErrorJson,
    ReadAt,
    Count,
}

#[derive(Clone, Debug)]
pub struct TaskCompletionInboxAccess<'a> {
    pub workspace_id: &'a str,
    pub principal_id: &'a str,
    /// Only the canonical TaskRead action gate's AllowAbsolute permits this.
    pub absolute: bool,
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

#[derive(FromQueryResult)]
struct InboxRow {
    task_id: String,
    run_id: String,
    title: String,
    thread_id: String,
    thread_title: String,
    status: String,
    completed_at: i64,
    completed_key: String,
    summary: Option<String>,
    error_json: Option<String>,
    read_at: Option<i64>,
}
impl TryFrom<InboxRow> for TaskCompletionInboxRow {
    type Error = anyhow::Error;
    fn try_from(row: InboxRow) -> Result<Self> {
        Ok(Self {
            task_id: row.task_id,
            run_id: row.run_id,
            title: row.title,
            thread_id: row.thread_id,
            thread_title: row.thread_title,
            status: crate::convention::task_run_status_from_db(&row.status)
                .context("invalid inbox run status")?,
            completed_at: row.completed_at,
            completed_key: row.completed_key,
            summary: row.summary,
            error_json: row.error_json,
            read_at: row.read_at,
        })
    }
}

fn access_condition(access: &TaskCompletionInboxAccess<'_>) -> Condition {
    use InboxAlias as A;
    let workspace_member = Query::select()
        .expr(Expr::val(1))
        .from_as(workspace_membership::Entity, A::WorkspaceMember)
        .and_where(
            Expr::col((
                A::WorkspaceMember,
                workspace_membership::Column::PrincipalId,
            ))
            .eq(access.principal_id),
        )
        .and_where(
            Expr::col((
                A::WorkspaceMember,
                workspace_membership::Column::WorkspaceId,
            ))
            .equals((S::Task, task::Column::WorkspaceId)),
        )
        .to_owned();
    let thread_member = Query::select()
        .expr(Expr::val(1))
        .from_as(thread_membership::Entity, A::ThreadMember)
        .and_where(
            Expr::col((A::ThreadMember, thread_membership::Column::PrincipalId))
                .eq(access.principal_id),
        )
        .and_where(
            Expr::col((A::ThreadMember, thread_membership::Column::ThreadId))
                .equals((S::Thread, thread::Column::Id)),
        )
        .to_owned();
    Condition::any().add(Expr::val(access.absolute)).add(
        Condition::all()
            .add(Expr::col((S::Workspace, workspace::Column::IsActive)).eq(true))
            .add(Expr::exists(workspace_member))
            .add(
                Condition::any()
                    .add(Expr::col((S::Thread, thread::Column::AccessClass)).eq("workspace"))
                    .add(Expr::exists(thread_member)),
            ),
    )
}

fn read_at(access: &TaskCompletionInboxAccess<'_>) -> Expr {
    use InboxAlias as A;
    use user_notification_outbox::Column as N;
    let legacy = Query::select()
        .expr(Expr::col((A::Legacy, N::AcknowledgedAt)).min())
        .from_as(user_notification_outbox::Entity, A::Legacy)
        .and_where(Expr::col((A::Legacy, N::RecipientPrincipalId)).eq(access.principal_id))
        .and_where(Expr::col((A::Legacy, N::RunId)).equals((A::Run, task_run::Column::Id)))
        .and_where(Expr::col((A::Legacy, N::TaskId)).equals((S::Task, task::Column::Id)))
        .and_where(
            Expr::col((A::Legacy, N::WorkspaceId)).equals((S::Task, task::Column::WorkspaceId)),
        )
        .and_where(Expr::col((A::Legacy, N::AcknowledgedAt)).is_not_null())
        .to_owned();
    Func::coalesce([
        Expr::col((A::Receipt, task_run_read::Column::ReadAt)),
        Expr::from(legacy),
    ])
    .into()
}

fn relation(access: &TaskCompletionInboxAccess<'_>, workspace_scan: bool) -> SelectStatement {
    use InboxAlias as A;
    let mut query = Query::select();
    if workspace_scan {
        // CROSS JOIN fences task -> run order before root/ACL joins. The hints
        // below require workspace and task-local index seeks even on empty sets.
        query
            .from_as(task::Entity, S::Task)
            .cross_join(task_run::Entity.table_ref().alias(A::Run))
            .and_where(
                Expr::col((A::Run, task_run::Column::TaskId)).equals((S::Task, task::Column::Id)),
            );
    } else {
        query.from_as(task_run::Entity, A::Run).join_as(
            JoinType::InnerJoin,
            task::Entity,
            S::Task,
            Expr::col((S::Task, task::Column::Id)).equals((A::Run, task_run::Column::TaskId)),
        );
    }
    let mut query = task_scope_query(query);
    query
        .join_as(
            JoinType::LeftJoin,
            task_run_read::Entity,
            A::Receipt,
            Condition::all()
                .add(
                    Expr::col((A::Receipt, task_run_read::Column::PrincipalId))
                        .eq(access.principal_id),
                )
                .add(
                    Expr::col((A::Receipt, task_run_read::Column::RunId))
                        .equals((A::Run, task_run::Column::Id)),
                ),
        )
        .and_where(Expr::col((S::Task, task::Column::WorkspaceId)).eq(access.workspace_id))
        .and_where(Expr::col((A::Run, task_run::Column::CompletedAt)).is_not_null())
        .and_where(
            Expr::col((A::Run, task_run::Column::Status)).is_in(
                [
                    pioneer_protocol::TaskRunStatus::Succeeded,
                    pioneer_protocol::TaskRunStatus::Failed,
                    pioneer_protocol::TaskRunStatus::Blocked,
                    pioneer_protocol::TaskRunStatus::Cancelled,
                    pioneer_protocol::TaskRunStatus::TimedOut,
                ]
                .map(crate::convention::task_run_status_to_db),
            ),
        )
        .and_where(Expr::col((S::Thread, thread::Column::Id)).is_not_null())
        .cond_where(access_condition(access));
    query
}

fn select_metadata(query: &mut SelectStatement, access: &TaskCompletionInboxAccess<'_>) {
    use InboxAlias as A;
    use InboxField as F;
    query
        .expr_as(Expr::col((S::Task, task::Column::Id)), F::TaskId)
        .expr_as(Expr::col((A::Run, task_run::Column::Id)), F::RunId)
        .expr_as(Expr::col((S::Task, task::Column::Title)), F::Title)
        .expr_as(Expr::col((S::Thread, thread::Column::Id)), F::ThreadId)
        .expr_as(
            Func::coalesce([
                Expr::col((S::Thread, thread::Column::Name)),
                Expr::col((S::Thread, thread::Column::Id)),
            ]),
            F::ThreadTitle,
        )
        .expr_as(Expr::col((A::Run, task_run::Column::Status)), F::Status)
        .expr_as(
            Expr::col((A::Run, task_run::Column::CompletedAt)),
            F::CompletedKey,
        )
        .expr_as(read_at(access), F::ReadKey);
}

fn unix_seconds(value: Expr) -> Expr {
    // Func::cust is SeaQuery's supported named-function API: all arguments stay
    // typed/bound. SQLite strftime returns text; retain the original integer cast.
    Func::cust(Alias::new("strftime"))
        .args([Expr::val("%s"), value])
        .cast_as(Alias::new("INTEGER"))
}
fn result_field(column: task_run::Column, path: &str) -> Expr {
    Func::cust(Alias::new("json_extract"))
        .args([Expr::col((InboxAlias::ResultRun, column)), Expr::val(path)])
        .into()
}
fn bounded_text(value: Expr, limit: i32) -> Expr {
    Func::cust(Alias::new("substr"))
        .args([value, Expr::val(1), Expr::val(limit)])
        .into()
}

fn project_page(page: SelectStatement) -> SelectStatement {
    use InboxAlias as A;
    use InboxField as F;
    let cte = CommonTableExpression::new()
        .table_name(A::InboxPage)
        .materialized(true)
        .query(page)
        .to_owned();
    let mut query = Query::select();
    // The materialized CTE contains bounded metadata only. JSON extraction is
    // outside it, so sorting/filtering cannot parse historical result payloads.
    query
        .with_cte(cte)
        .from_as(A::InboxPage, A::Page)
        .cross_join(task_run::Entity.table_ref().alias(A::ResultRun))
        .and_where(Expr::col((A::ResultRun, task_run::Column::Id)).equals((A::Page, F::RunId)));
    for field in [
        F::TaskId,
        F::RunId,
        F::Title,
        F::ThreadId,
        F::ThreadTitle,
        F::Status,
    ] {
        query.expr_as(Expr::col((A::Page, field)), field);
    }
    let public_error = Func::cust(Alias::new("json_object")).args([
        Expr::val("code"),
        bounded_text(result_field(task_run::Column::ErrorJson, "$.code"), 128),
        Expr::val("message"),
        bounded_text(result_field(task_run::Column::ErrorJson, "$.message"), 1024),
        Expr::val("class"),
        result_field(task_run::Column::ErrorJson, "$.class"),
    ]);
    query
        .expr_as(
            Expr::col((A::Page, F::CompletedKey)).cast_as(Alias::new("TEXT")),
            F::CompletedKey,
        )
        .expr_as(
            unix_seconds(Expr::col((A::Page, F::CompletedKey))),
            F::CompletedAt,
        )
        .expr_as(
            bounded_text(
                result_field(task_run::Column::ResultJson, "$.summary"),
                1024,
            ),
            F::Summary,
        )
        .expr_as(
            Expr::case(
                Expr::col((A::ResultRun, task_run::Column::ErrorJson)).is_not_null(),
                public_error,
            )
            .finally(Expr::val(None::<String>)),
            F::ErrorJson,
        )
        .expr_as(unix_seconds(Expr::col((A::Page, F::ReadKey))), F::ReadAt)
        .order_by((A::Page, F::CompletedKey), Order::Desc)
        .order_by((A::Page, F::RunId), Order::Desc);
    query
}

/// SeaQuery 1.0.2 has MySQL index hints but no SQLite INDEXED BY node/API.
/// Render the typed query first, then add ONLY the two fixed table hints. Values
/// remain bound. Fail closed if rendering changes; never execute an unhinted
/// workspace scan. Exact get/read statements do not use this adapter.
fn workspace_statement(query: &SelectStatement) -> Result<Statement> {
    let mut statement = DbBackend::Sqlite.build(query);
    for (table, index) in [
        ("\"task\" AS \"t\"", "idx_task_workspace_status"),
        ("\"task_run\" AS \"r\"", "idx_task_run_task_id"),
    ] {
        anyhow::ensure!(
            statement.sql.matches(table).count() == 1,
            "inbox workspace index target changed"
        );
        statement.sql =
            statement
                .sql
                .replacen(table, &format!("{table} INDEXED BY \"{index}\""), 1);
    }
    Ok(statement)
}

fn page_statement(
    access: &TaskCompletionInboxAccess<'_>,
    before: Option<(&str, &str)>,
    limit: usize,
) -> Result<Statement> {
    use InboxAlias as A;
    let mut page = relation(access, true);
    select_metadata(&mut page, access);
    if let Some((at, id)) = before {
        page.and_where(
            Expr::tuple([
                Expr::col((A::Run, task_run::Column::CompletedAt)),
                Expr::col((A::Run, task_run::Column::Id)),
            ])
            .lt(Expr::tuple([Expr::val(at), Expr::val(id)])),
        );
    }
    page.order_by((A::Run, task_run::Column::CompletedAt), Order::Desc)
        .order_by((A::Run, task_run::Column::Id), Order::Desc)
        .limit(limit.clamp(1, 101) as u64);
    workspace_statement(&project_page(page))
}

pub async fn list_task_completion_inbox<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    before: Option<(&str, &str)>,
    limit: usize,
) -> Result<Vec<TaskCompletionInboxRow>> {
    db.query_all_raw(page_statement(access, before, limit)?)
        .await?
        .iter()
        .map(|row| InboxRow::from_query_result(row, "")?.try_into())
        .collect()
}
fn unread_count_statement(access: &TaskCompletionInboxAccess<'_>) -> Result<Statement> {
    let mut query = relation(access, true);
    query
        .expr_as(Func::count(Expr::col(Asterisk)), InboxField::Count)
        .and_where(read_at(access).is_null());
    workspace_statement(&query)
}
#[derive(FromQueryResult)]
struct UnreadCount {
    count: i64,
}
pub async fn count_unread_task_completion_inbox<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
) -> Result<u64> {
    let row = db
        .query_one_raw(unread_count_statement(access)?)
        .await?
        .context("inbox count missing")?;
    Ok(UnreadCount::from_query_result(&row, "")?.count as u64)
}

pub async fn find_task_completion_inbox_entry<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    run_id: &str,
) -> Result<Option<TaskCompletionInboxRow>> {
    let mut page = relation(access, false);
    select_metadata(&mut page, access);
    page.and_where(Expr::col((InboxAlias::Run, task_run::Column::Id)).eq(run_id))
        .limit(1);
    db.query_one(&project_page(page))
        .await?
        .map(|row| InboxRow::from_query_result(&row, "")?.try_into())
        .transpose()
}

/// ACL, terminal state and receipt commit remain a single writer statement.
pub async fn mark_task_completion_inbox_read<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    run_id: &str,
    read_at: &str,
) -> Result<()> {
    let mut source = relation(access, false);
    source
        .expr(Expr::val(access.principal_id))
        .column((InboxAlias::Run, task_run::Column::Id))
        .expr(Expr::val(read_at))
        .and_where(Expr::col((InboxAlias::Run, task_run::Column::Id)).eq(run_id));
    let insert = Query::insert()
        .into_table(task_run_read::Entity)
        .columns([
            task_run_read::Column::PrincipalId,
            task_run_read::Column::RunId,
            task_run_read::Column::ReadAt,
        ])
        .select_from(source)?
        .on_conflict(
            OnConflict::columns([
                task_run_read::Column::PrincipalId,
                task_run_read::Column::RunId,
            ])
            .do_nothing()
            .to_owned(),
        )
        .to_owned();
    db.execute(&insert).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_hints_fail_closed_when_a_typed_source_is_missing_or_ambiguous() {
        let missing_run = Query::select()
            .expr(Expr::val(1))
            .from_as(task::Entity, S::Task)
            .to_owned();
        assert!(workspace_statement(&missing_run).is_err());
        let mut ambiguous = missing_run;
        ambiguous.from_as(task::Entity, S::Task);
        assert!(workspace_statement(&ambiguous).is_err());
    }

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
            page_statement(&access, None, 31).unwrap(),
            page_statement(&access, Some(("2026-10-16 10:00:00 +00:00", "run")), 31).unwrap(),
            unread_count_statement(&access).unwrap(),
        ] {
            // Inspect the exact final statement, including INDEXED BY, which
            // SeaQuery cannot represent in its ExplainStatement AST either.
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
