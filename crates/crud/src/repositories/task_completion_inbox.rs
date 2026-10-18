//! Set queries over saved runs. No delivery rows or per-recipient result copies.
use super::authorization_scope::{TaskScopeAlias as S, task_scope_query};
use anyhow::{Context, Result};
use pioneer_entity::{
    task, task_delivery, task_inbox_activation, task_result_candidate, task_run, task_run_read,
    task_run_turn, thread, thread_membership, user_notification_outbox, workspace,
    workspace_membership,
};
use sea_orm::sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, JoinType, OnConflict,
    Order, Query, SelectStatement,
};
use sea_orm::{
    ConnectionTrait, DbBackend, DeriveIden, EntityName, FromQueryResult, Statement,
    TransactionTrait,
};

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

fn unread_condition(access: &TaskCompletionInboxAccess<'_>) -> Condition {
    // Activation suppresses historical notifications without manufacturing a
    // personal read timestamp or one receipt per historical run. Include the
    // complete activation second: the durable migration fact has second precision.
    let activation = Query::select()
        .column(task_inbox_activation::Column::StartedAt)
        .from(task_inbox_activation::Entity)
        .and_where(Expr::col(task_inbox_activation::Column::Id).eq(1))
        .to_owned();
    Condition::all().add(read_at(access).is_null()).add(
        unix_seconds(Expr::col((InboxAlias::Run, task_run::Column::CompletedAt)))
            .gte(Expr::from(activation)),
    )
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
        // Exact run/candidate lookup must not be reordered into a workspace
        // task scan. Only the mass path above starts from workspace tasks.
        query
            .from_as(task_run::Entity, A::Run)
            .cross_join(task::Entity.table_ref().alias(S::Task))
            .and_where(
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
    // The fixed format is structural: SQLite must match the expression index's
    // literal strftime('%s', completed_at). Caller values remain bound; retain
    // epoch normalization and the integer cast of strftime's text result.
    Func::cust(Alias::new("strftime"))
        .args([Expr::Constant("%s".into()), value])
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
    workspace_index_hints(DbBackend::Sqlite.build(query))
}
fn workspace_index_hints(mut statement: Statement) -> Result<Statement> {
    for (table, index) in [
        ("\"task\" AS \"t\"", "idx_task_inbox_workspace_id"),
        ("\"task_run\" AS \"r\"", "idx_task_run_inbox_epoch"),
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
    page.cond_where(unread_condition(access));
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
        .cond_where(unread_condition(access));
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

/// A single serialized writer statement defines the operation's exact row set.
/// A completion committed after this statement cannot be included, even when its
/// timestamp equals a previous completion. ACL and workspace are evaluated here.
pub async fn mark_all_task_completion_inbox_read<C: ConnectionTrait>(
    db: &C,
    access: &TaskCompletionInboxAccess<'_>,
    at: &str,
) -> Result<()> {
    let mut source = relation(access, true);
    source
        .expr(Expr::val(access.principal_id))
        .column((InboxAlias::Run, task_run::Column::Id))
        .expr(Expr::val(at))
        .cond_where(unread_condition(access));
    let insert = receipt_insert(source)?;
    db.execute_raw(workspace_index_hints(DbBackend::Sqlite.build(&insert))?)
        .await?;
    Ok(())
}

fn receipt_insert(source: SelectStatement) -> Result<sea_orm::sea_query::InsertStatement> {
    Ok(Query::insert()
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
        .to_owned())
}

// A failed dedicated delivery has a status row without a result item. Prepare
// only bounded link metadata on readers, then validate its exact immutable
// canonical Turn identity outside DB capacity. The writer rechecks every link
// and the Turn role; a changed/missing delivery can never authorize a receipt.
async fn dedicated_delivery_status_links(
    db: &pioneer_sqlite::SqliteDatabase,
    viewed: &pioneer_protocol::TaskCompletionInboxViewedParams,
) -> Result<std::collections::HashMap<String, String>> {
    use pioneer_entity::turn;
    let mut links = std::collections::HashMap::new();
    if viewed.result_turn_ids.is_empty() {
        return Ok(links);
    }
    // Strict negative superset of the exact per-Turn lookup below. Fetch at
    // most one metadata row, not up to 100 deliveries for each of 100 Turns.
    // Positive links are still canonically checked and revalidated by writer.
    let probe = Query::select()
        .expr(Expr::val(1))
        .from(task_delivery::Entity)
        .inner_join(
            turn::Entity,
            Expr::col((turn::Entity, turn::Column::Id)).equals((
                task_delivery::Entity,
                task_delivery::Column::DeliveredTurnId,
            )),
        )
        .and_where(
            Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                .eq(&viewed.workspace_id),
        )
        .and_where(
            Expr::col((task_delivery::Entity, task_delivery::Column::TargetThreadId))
                .eq(&viewed.thread_id),
        )
        .and_where(
            Expr::col((
                task_delivery::Entity,
                task_delivery::Column::DeliveredTurnId,
            ))
            .is_in(viewed.result_turn_ids.iter().map(String::as_str)),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Origin)).eq("task_delivery"))
        .limit(1)
        .to_owned();
    if db.query_one(&probe).await?.is_none() {
        return Ok(links);
    }
    for turn_id in &viewed.result_turn_ids {
        let query = Query::select()
            .column((task_delivery::Entity, task_delivery::Column::Id))
            .from(task_delivery::Entity)
            .inner_join(
                turn::Entity,
                Expr::col((turn::Entity, turn::Column::Id)).equals((
                    task_delivery::Entity,
                    task_delivery::Column::DeliveredTurnId,
                )),
            )
            .and_where(
                Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                    .eq(&viewed.workspace_id),
            )
            .and_where(
                Expr::col((task_delivery::Entity, task_delivery::Column::TargetThreadId))
                    .eq(&viewed.thread_id),
            )
            .and_where(
                Expr::col((
                    task_delivery::Entity,
                    task_delivery::Column::DeliveredTurnId,
                ))
                .eq(turn_id),
            )
            .and_where(Expr::col((turn::Entity, turn::Column::Origin)).eq("task_delivery"))
            .limit(101)
            .to_owned();
        let rows = db.query_all(&query).await?;
        anyhow::ensure!(rows.len() <= 100, "ambiguous delivery status links");
        for row in rows {
            let delivery_id: String = row.try_get("", "id")?;
            if crate::canonical_agent_id('T', &format!("task-delivery-turn\0{delivery_id}"))
                == *turn_id
            {
                links.insert(turn_id.clone(), delivery_id);
            }
        }
    }
    Ok(links)
}

/// Accepted candidates identify the actual successful outcome, even when a
/// later reviewer Turn exists. Without a candidate, the run's ordered work
/// frontier must have the corresponding terminal failure (or legacy success).
/// Sequence is the durable causal work order, never a wall-clock heuristic.
fn terminal_turn_evidence() -> Condition {
    use task_result_candidate as candidate;
    let candidate_for_run = Query::select()
        .expr(Expr::val(1))
        .from(candidate::Entity)
        .and_where(
            Expr::col((candidate::Entity, candidate::Column::RunId))
                .equals((task_run_turn::Entity, task_run_turn::Column::RunId)),
        )
        .to_owned();
    let mut accepted = candidate_for_run.clone();
    accepted
        .and_where(
            Expr::col((candidate::Entity, candidate::Column::TaskId))
                .equals((task_run_turn::Entity, task_run_turn::Column::TaskId)),
        )
        .and_where(Expr::col((candidate::Entity, candidate::Column::Status)).eq("accepted"))
        .and_where(
            Expr::col((candidate::Entity, candidate::Column::TaskRunTurnId))
                .equals((task_run_turn::Entity, task_run_turn::Column::Id)),
        )
        .and_where(
            Expr::col((candidate::Entity, candidate::Column::ThreadId))
                .equals((task_run_turn::Entity, task_run_turn::Column::ThreadId)),
        )
        .and_where(
            Expr::col((candidate::Entity, candidate::Column::TurnId))
                .equals((task_run_turn::Entity, task_run_turn::Column::TurnId)),
        );
    let successor = Alias::new("outcome_successor");
    let later_work = Query::select()
        .expr(Expr::val(1))
        .from_as(task_run_turn::Entity, successor.clone())
        .and_where(
            Expr::col((successor.clone(), task_run_turn::Column::RunId))
                .equals((task_run_turn::Entity, task_run_turn::Column::RunId)),
        )
        .and_where(
            Expr::col((successor, task_run_turn::Column::Sequence)).gt(Expr::col((
                task_run_turn::Entity,
                task_run_turn::Column::Sequence,
            ))),
        )
        .to_owned();
    let run_status = || Expr::col((task_run::Entity, task_run::Column::Status));
    let work_status = || Expr::col((task_run_turn::Entity, task_run_turn::Column::Status));
    let kind = || Expr::col((task_run_turn::Entity, task_run_turn::Column::Kind));
    let success = Condition::all()
        .add(run_status().eq("succeeded"))
        .add(kind().is_in(["initial", "revision", "recovery"]))
        .add(work_status().eq("candidate_created"))
        .add(
            Condition::any().add(Expr::exists(accepted)).add(
                Condition::all()
                    .add(Expr::exists(candidate_for_run).not())
                    .add(Expr::exists(later_work.clone()).not()),
            ),
        );
    let failure = Condition::any()
        .add(
            Condition::all()
                .add(run_status().is_in(["failed", "timed_out"]))
                .add(work_status().is_in(["failed", "interrupted"])),
        )
        .add(
            Condition::all()
                .add(run_status().eq("blocked"))
                .add(work_status().eq("blocked")),
        )
        .add(
            Condition::all()
                .add(run_status().eq("cancelled"))
                .add(work_status().is_in(["cancelled", "interrupted"])),
        );
    Condition::any().add(success).add(
        Condition::all()
            .add(failure)
            .add(kind().is_in(["initial", "revision", "recovery", "review"]))
            .add(Expr::exists(later_work).not()),
    )
}

/// Resolve final result Turns using durable run/turn and delivery links. Never
/// infer a run from a task ID, a title, or the currently executing run.
pub async fn mark_viewed_task_completion_inbox_read(
    db: &pioneer_sqlite::SqliteDatabase,
    access: &TaskCompletionInboxAccess<'_>,
    viewed: &pioneer_protocol::TaskCompletionInboxViewedParams,
    at: &str,
) -> Result<Vec<String>> {
    anyhow::ensure!(
        viewed.workspace_id == access.workspace_id
            && viewed.run_ids.len() + viewed.result_turn_ids.len() + viewed.result_items.len()
                <= 100,
        "invalid inbox viewport"
    );
    let status_links = dedicated_delivery_status_links(db, viewed).await?;
    // Only immutable request IDs are prepared outside the transaction. A
    // canonical delivery item or dedicated status link is a separate source
    // of evidence and must bypass this negative task_run_turn prefilter.
    let candidate_turn_ids = (viewed.run_ids.is_empty()
        && status_links.is_empty()
        && viewed.result_items.iter().all(|item| {
            pioneer_protocol::task_delivery_id_from_result_item_id(&item.item_id).is_none()
        }))
    .then(|| {
        viewed
            .result_turn_ids
            .iter()
            .map(String::as_str)
            .chain(viewed.result_items.iter().map(|item| item.turn_id.as_str()))
            .collect::<std::collections::BTreeSet<_>>()
    });
    let tx = db.begin().await?;
    let viewed_thread = Query::select()
        .expr(Expr::val(1))
        .from(thread::Entity)
        .and_where(Expr::col(thread::Column::Id).eq(&viewed.thread_id))
        .and_where(Expr::col(thread::Column::WorkspaceId).eq(access.workspace_id))
        .to_owned();
    anyhow::ensure!(
        tx.query_one(&viewed_thread).await?.is_some(),
        "invalid inbox viewport thread"
    );
    if let Some(turns) = candidate_turn_ids
        .as_ref()
        .filter(|turns| !turns.is_empty())
    {
        // Deliberately a superset: no role/status/ACL filters can accidentally
        // exclude a real proof. Existence is checked under the same writer
        // transaction as viewport belonging and all positive receipt writes.
        let candidates = Query::select()
            .expr(Expr::val(1))
            .from(task_run_turn::Entity)
            .and_where(Expr::col(task_run_turn::Column::TurnId).is_in(turns.iter().copied()))
            .limit(1)
            .to_owned();
        if tx.query_one(&candidates).await?.is_none() {
            tx.commit().await?;
            return Ok(Vec::new());
        }
    }
    let mut confirmed = Vec::new();
    #[derive(Clone, Copy)]
    enum Evidence<'a> {
        Run(&'a str),
        Status(&'a str),
        Item(&'a pioneer_protocol::TaskCompletionInboxViewedResult),
    }
    let evidence = viewed
        .run_ids
        .iter()
        .map(|id| Evidence::Run(id))
        .chain(viewed.result_turn_ids.iter().map(|id| Evidence::Status(id)))
        .chain(viewed.result_items.iter().map(Evidence::Item));
    for evidence in evidence {
        let (run_id, result_turn, delivery_id, delivery_only, is_status, proof) = match evidence {
            Evidence::Run(id) => (Some(id), None, None, false, false, format!("run:{id}")),
            Evidence::Status(id) => (
                None,
                Some(id),
                status_links.get(id).map(String::as_str),
                false,
                true,
                format!("turn:{id}"),
            ),
            Evidence::Item(item) => {
                let delivery =
                    pioneer_protocol::task_delivery_id_from_result_item_id(&item.item_id);
                (
                    None,
                    Some(item.turn_id.as_str()),
                    delivery,
                    delivery.is_some(),
                    false,
                    item.proof_key(),
                )
            }
        };
        let mut source = relation(access, false);
        if let Some(id) = result_turn {
            use pioneer_entity::turn;
            // FK enforcement is OFF: reject orphaned or mismatched turn links.
            let valid_turn = Query::select()
                .expr(Expr::val(1))
                .from(turn::Entity)
                .inner_join(
                    thread::Entity,
                    Expr::col((thread::Entity, thread::Column::Id))
                        .equals((turn::Entity, turn::Column::ThreadId)),
                )
                .and_where(Expr::col((turn::Entity, turn::Column::Id)).eq(id))
                .and_where(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(&viewed.thread_id))
                .and_where(
                    Expr::col((thread::Entity, thread::Column::WorkspaceId))
                        .eq(access.workspace_id),
                )
                .to_owned();
            // A completed child delivery can be viewed while its containing
            // parent Turn is still running. Status/child-work proofs continue
            // to require a terminal Turn.
            let mut terminal_turn = valid_turn.clone();
            terminal_turn.and_where(Expr::col((turn::Entity, turn::Column::Status)).is_in([
                "completed",
                "failed",
                "interrupted",
                "blocked",
            ]));
            source.and_where(Expr::exists(if delivery_only {
                valid_turn
            } else {
                terminal_turn.clone()
            }));
            if let Evidence::Item(item) = &evidence {
                use pioneer_entity::turn_item;
                let saved_item = Query::select()
                    .expr(Expr::val(1))
                    .from(turn_item::Entity)
                    .and_where(Expr::col(turn_item::Column::TurnId).eq(id))
                    .and_where(Expr::col(turn_item::Column::ItemId).eq(&item.item_id))
                    .and_where(Expr::col(turn_item::Column::ItemType).eq("agent_message"))
                    .and_where(Expr::col(turn_item::Column::Status).eq("completed"))
                    .to_owned();
                source.and_where(Expr::exists(saved_item));
            }
            let mut candidates = Query::select()
                .column((task_run_turn::Entity, task_run_turn::Column::RunId))
                .from(task_run_turn::Entity)
                .inner_join(
                    task_run::Entity,
                    Condition::all()
                        .add(
                            Expr::col((task_run::Entity, task_run::Column::Id))
                                .equals((task_run_turn::Entity, task_run_turn::Column::RunId)),
                        )
                        .add(
                            Expr::col((task_run::Entity, task_run::Column::TaskId))
                                .equals((task_run_turn::Entity, task_run_turn::Column::TaskId)),
                        ),
                )
                .and_where(Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId)).eq(id))
                .and_where(
                    Expr::col((task_run_turn::Entity, task_run_turn::Column::ThreadId))
                        .eq(&viewed.thread_id),
                )
                .to_owned();
            candidates.cond_where(terminal_turn_evidence());
            if delivery_only {
                candidates.and_where(Expr::val(false));
            }
            let mut delivery = Query::select()
                .column((task_delivery::Entity, task_delivery::Column::RunId))
                .from(task_delivery::Entity)
                .inner_join(
                    task_run::Entity,
                    Condition::all()
                        .add(
                            Expr::col((task_run::Entity, task_run::Column::Id))
                                .equals((task_delivery::Entity, task_delivery::Column::RunId)),
                        )
                        .add(
                            Expr::col((task_run::Entity, task_run::Column::TaskId))
                                .equals((task_delivery::Entity, task_delivery::Column::TaskId)),
                        ),
                )
                .and_where(
                    Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                        .eq(access.workspace_id),
                )
                .and_where(
                    Expr::col((task_delivery::Entity, task_delivery::Column::TargetThreadId))
                        .eq(&viewed.thread_id),
                )
                .and_where(
                    Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::DeliveredTurnId,
                    ))
                    .eq(id),
                )
                .to_owned();
            delivery
                .and_where(Expr::val(delivery_id.is_some()))
                .and_where(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                        .eq(delivery_id.unwrap_or("")),
                )
                .and_where(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Mode)).eq("thread"),
                );
            if is_status {
                let mut dedicated_turn = terminal_turn;
                dedicated_turn
                    .and_where(Expr::col((turn::Entity, turn::Column::Origin)).eq("task_delivery"));
                delivery.and_where(Expr::exists(dedicated_turn));
            }
            candidates.union(sea_orm::sea_query::UnionType::Distinct, delivery);
            source.and_where(
                Expr::col((InboxAlias::Run, task_run::Column::Id)).in_subquery(candidates),
            );
        } else {
            source.and_where(
                Expr::col((InboxAlias::Run, task_run::Column::Id))
                    .eq(run_id.context("missing viewed run")?),
            );
        }
        // Bounded metadata lookup before writing also confirms already-read runs.
        let mut lookup = source.clone();
        lookup
            .column((InboxAlias::Run, task_run::Column::Id))
            .limit(1);
        if tx.query_one(&lookup).await?.is_some() {
            source
                .expr(Expr::val(access.principal_id))
                .column((InboxAlias::Run, task_run::Column::Id))
                .expr(Expr::val(at));
            // This is a real viewport read, including an explicitly viewed old
            // result. Activation alone never creates a receipt; actual viewing does.
            source.and_where(read_at(access).is_null());
            tx.execute(&receipt_insert(source)?).await?;
            confirmed.push(proof);
        }
    }
    tx.commit().await?;
    Ok(confirmed)
}

/// Internal cancellation suppression follows the authorized mutation, even if
/// the cancelled run currently has no inbox presentation root. It grants no
/// TaskRead access; list/get/count continue to enforce their canonical ACL.
pub(crate) async fn mark_cancelled_task_run_read<C: ConnectionTrait>(
    db: &C,
    principal: &str,
    workspace_id: &str,
    task_id: &str,
    run_id: &str,
    at: &str,
) -> Result<()> {
    let source = Query::select()
        .expr(Expr::val(principal))
        .column((task_run::Entity, task_run::Column::Id))
        .expr(Expr::val(at))
        .from(task_run::Entity)
        .inner_join(
            task::Entity,
            Expr::col((task::Entity, task::Column::Id))
                .equals((task_run::Entity, task_run::Column::TaskId)),
        )
        .inner_join(
            workspace::Entity,
            Expr::col((workspace::Entity, workspace::Column::Id))
                .equals((task::Entity, task::Column::WorkspaceId)),
        )
        .and_where(Expr::col((task_run::Entity, task_run::Column::Id)).eq(run_id))
        .and_where(Expr::col((task_run::Entity, task_run::Column::TaskId)).eq(task_id))
        .and_where(Expr::col((task_run::Entity, task_run::Column::Status)).eq("cancelled"))
        .and_where(Expr::col((task::Entity, task::Column::WorkspaceId)).eq(workspace_id))
        .to_owned();
    anyhow::ensure!(
        db.query_one(&source).await?.is_some(),
        "cancelled run receipt scope is missing or inconsistent"
    );
    db.execute(&receipt_insert(source)?).await?;
    Ok(())
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
        .and_where(Expr::col((InboxAlias::Run, task_run::Column::Id)).eq(run_id))
        .and_where(self::read_at(access).is_null());
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
            // Inspect the exact final statement with its original bound Values,
            // including INDEXED BY, rather than an inlined Display SQL string.
            let values = statement.values.as_ref().expect("bound inbox values");
            assert!(values.0.contains(&access.workspace_id.into()));
            assert!(values.0.contains(&access.principal_id.into()));
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
                        && line.contains("idx_task_inbox_workspace_id")
                        && line.contains("workspace_id=?")
                })
                .expect("workspace index seek");
            let run = plan
                .iter()
                .position(|line| {
                    line.starts_with("SEARCH r ")
                        && line.contains("idx_task_run_inbox_epoch")
                        && line.contains("task_id=?")
                        && line.contains("<expr>>?")
                })
                .expect("task-local epoch range index seek");
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
        // The exact path must start with an ID seek of the requested run, not
        // enumerate the workspace's task index before evaluating an empty IN.
        let mut exact = relation(&access, false);
        exact
            .expr(Expr::val(1))
            .and_where(Expr::col((InboxAlias::Run, task_run::Column::Id)).eq("absent-run"));
        let mut statement = DbBackend::Sqlite.build(&exact);
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        // A covering index may include workspace/status before the bound ID.
        // Match the ID term itself, not the suffix of workspace_id/task_id.
        let seeks_id = |line: &str| line.contains("(id=?") || line.contains(" AND id=?");
        let run = plan
            .iter()
            .position(|line| line.starts_with("SEARCH r ") && seeks_id(line))
            .expect("exact run seek");
        let task = plan
            .iter()
            .position(|line| line.starts_with("SEARCH t ") && seeks_id(line))
            .expect("exact owning task seek");
        assert!(run < task, "{plan:?}");
        assert!(
            !plan.iter().any(|line| line.starts_with("SCAN r ")
                || line.starts_with("SCAN t ")
                || (line.starts_with("SEARCH t ")
                    && line.contains("workspace_id=?")
                    && !seeks_id(line))),
            "{plan:?}"
        );
    }
}
