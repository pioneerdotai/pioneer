//! Durable locators for optional root conversation preparation. No history copy.
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_history_check,
    compaction_root_activity, task, task_run_turn, thread, thread_lineage, turn,
    turn_cli_runtime_binding,
};
use sea_orm::sea_query::{
    Alias, Condition, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query, SelectStatement,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
#[derive(Debug, Clone, FromQueryResult)]
pub struct CompletedHistoryCheck {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub descriptor: Option<String>,
    pub revision: i64,
    pub failures: i64,
    pub attempt_deadline_ms: Option<i64>,
    pub diagnostic: Option<serde_json::Value>,
    pub config_hash: Option<String>,
    pub outcome: Option<String>,
    pub managed: i64,
}
pub async fn compaction_enqueue_native_history_check<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    descriptor: &str,
) -> Result<bool> {
    ensure!(
        descriptor.len() <= 16384,
        "history descriptor exceeds bound"
    );
    let inserted = db
        .execute(
            &Query::insert()
                .into_table(compaction_history_check::Entity)
                .columns([
                    compaction_history_check::Column::TurnId,
                    compaction_history_check::Column::Descriptor,
                ])
                .select_from(
                    turn::Entity::find()
                        .select_only()
                        .join(
                            JoinType::InnerJoin,
                            turn::Entity::belongs_to(thread::Entity)
                                .from(turn::Column::ThreadId)
                                .to(thread::Column::Id)
                                .into(),
                        )
                        .expr(Expr::col((turn::Entity, turn::Column::Id)))
                        .expr(Expr::Value(descriptor.into()))
                        .filter(
                            Expr::col((turn::Entity, turn::Column::Id))
                                .eq(Expr::Value(turn.into()))
                                .and(
                                    Expr::col((turn::Entity, turn::Column::ThreadId))
                                        .eq(Expr::Value(thread.into())),
                                )
                                .and(
                                    Expr::col((thread::Entity, thread::Column::WorkspaceId))
                                        .eq(Expr::Value(workspace.into())),
                                )
                                .and(
                                    Expr::col((turn::Entity, turn::Column::Status))
                                        .eq(Expr::Value("in_progress".into())),
                                ),
                        )
                        .into_query(),
                )?
                .on_conflict(
                    OnConflict::column(compaction_history_check::Column::TurnId)
                        .update_column(compaction_history_check::Column::Descriptor)
                        .action_and_where(
                            Expr::col((
                                compaction_history_check::Entity,
                                compaction_history_check::Column::State,
                            ))
                            .eq("pending")
                            .and(
                                Expr::col((
                                    compaction_history_check::Entity,
                                    compaction_history_check::Column::Managed,
                                ))
                                .eq(0_i64),
                            )
                            .and(
                                Expr::col((
                                    compaction_history_check::Entity,
                                    compaction_history_check::Column::Descriptor,
                                ))
                                .ne(Expr::col((
                                    "excluded",
                                    compaction_history_check::Column::Descriptor,
                                ))),
                            ),
                        )
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    if inserted.rows_affected() == 0 {
        let valid_existing = compaction_history_check::Entity::find_by_id(turn)
            .join(
                JoinType::InnerJoin,
                compaction_history_check::Entity::belongs_to(turn::Entity)
                    .from(compaction_history_check::Column::TurnId)
                    .to(turn::Column::Id)
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn::Entity::belongs_to(thread::Entity)
                    .from(turn::Column::ThreadId)
                    .to(thread::Column::Id)
                    .into(),
            )
            .filter(turn::Column::ThreadId.eq(thread))
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .one(db)
            .await?
            .is_some();
        ensure!(valid_existing, "native history intent scope mismatch");
    }
    Ok(inserted.rows_affected() > 0)
}
// Query-local names only: physical tables/columns always use generated entities.
#[derive(Clone, Copy, sea_orm::DeriveIden)]
enum HistoryAlias {
    Root,
    Human,
    Composer,
    Execution,
}

// Constants that define an expression/partial index are literal AST nodes.
// Keep partial-index predicates visible to the PostgreSQL planner.
fn literal(value: impl Into<sea_orm::Value>) -> Expr {
    Expr::Constant(value.into())
}

fn no_child_lineage(root: Expr) -> Expr {
    Expr::exists(
        Query::select()
            .expr(literal(1_i64))
            .from(thread_lineage::Entity)
            .and_where(
                Expr::col((
                    thread_lineage::Entity,
                    thread_lineage::Column::ChildThreadId,
                ))
                .eq(root),
            )
            .to_owned(),
    )
    .not()
}

fn human_conversation(alias: HistoryAlias) -> Condition {
    Condition::all()
        .add(Expr::col((alias, turn::Column::Origin)).eq(literal("user")))
        .add(Expr::col((alias, turn::Column::TurnKind)).eq(literal("conversation")))
        .add(Expr::col((alias, turn::Column::InitiatedByActorKind)).eq(literal("principal")))
}

// A single SQL scope for busy detection and foreground binding. The
// snake_case launch keys are the TurnStartParams wire contract.
fn composer_task_scope() -> SelectStatement {
    use HistoryAlias::{Composer as K, Human as U, Root as H};
    use pioneer_crud_payloads::json_projection::{json_path_text, json_path_value, queryable_json};

    let metadata = queryable_json(Expr::col((K, task::Column::MetadataJson)));
    let version = json_path_value(metadata.clone(), &["composerWork", "version"]);
    let version_kind = Func::cust(Alias::new("json_typeof")).arg(version.clone());
    let launch = |key| json_path_text(metadata.clone(), &["composerWork", "launch", key]);
    Query::select()
        .from_as(task::Entity, K)
        .join_as(
            JoinType::InnerJoin,
            thread::Entity,
            H,
            Expr::col((H, thread::Column::Id))
                .eq(Expr::col((K, task::Column::CreatedByThreadId)))
                .and(
                    Expr::col((H, thread::Column::WorkspaceId))
                        .eq(Expr::col((K, task::Column::WorkspaceId))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            turn::Entity,
            U,
            Condition::all()
                .add(
                    Expr::col((U, turn::Column::Id))
                        .eq(Expr::col((K, task::Column::CreatedByTurnId))),
                )
                .add(Expr::col((U, turn::Column::ThreadId)).eq(Expr::col((H, thread::Column::Id))))
                .add(human_conversation(U))
                .add(Expr::col((U, turn::Column::WorkOwner)).eq(literal("detached_task"))),
        )
        .and_where(version_kind.eq("number"))
        .and_where(
            version
                .cast_as(Alias::new("text"))
                .eq(pioneer_protocol::TASK_COMPOSER_WORK_VERSION.to_string()),
        )
        .and_where(launch("thread_id").eq(Expr::col((H, thread::Column::Id))))
        .and_where(launch("turn_id").eq(Expr::col((U, turn::Column::Id))))
        .to_owned()
}

// Shared currency/discovery predicate; optional warming never discovers old
// execution-purpose debt. The activity join ties it to the latest human input.
fn root_history_checks() -> SelectStatement {
    use compaction_history_check as h;
    use compaction_root_activity as a;
    Query::select()
        .from(h::Entity)
        .join(
            JoinType::InnerJoin,
            turn::Entity,
            Expr::col((turn::Entity, turn::Column::Id))
                .eq(Expr::col((h::Entity, h::Column::TurnId))),
        )
        .join(
            JoinType::InnerJoin,
            a::Entity,
            Expr::col((a::Entity, a::Column::ThreadId))
                .eq(Expr::col((turn::Entity, turn::Column::ThreadId)))
                .and(
                    Expr::col((a::Entity, a::Column::UserTurnId))
                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                ),
        )
        .and_where(Expr::col((h::Entity, h::Column::State)).eq(literal("pending")))
        .and_where(Expr::col((h::Entity, h::Column::Managed)).eq(literal(1_i64)))
        .and_where(Expr::col((h::Entity, h::Column::Purpose)).eq(literal("root_conversation")))
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).is_in([
            "completed",
            "failed",
            "blocked",
        ]))
        .and_where(no_child_lineage(Expr::col((
            turn::Entity,
            turn::Column::ThreadId,
        ))))
        .to_owned()
}

/// A newer execution invalidates an optional older check. Inspect one locator.
pub async fn compaction_history_check_is_current<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    revision: Option<i64>,
) -> Result<bool> {
    let stopped = Query::select()
        .expr(literal(1_i64))
        .from(compaction_execution_stop::Entity)
        .join(
            JoinType::InnerJoin,
            compaction_context::Entity,
            Expr::col((
                compaction_context::Entity,
                compaction_context::Column::Owner,
            ))
            .eq(Expr::col((
                compaction_execution_stop::Entity,
                compaction_execution_stop::Column::Owner,
            ))),
        )
        .and_where(
            Expr::col((
                compaction_execution_stop::Entity,
                compaction_execution_stop::Column::TurnId,
            ))
            .eq(Expr::col((turn::Entity, turn::Column::Id))),
        )
        .and_where(
            Expr::col((
                compaction_context::Entity,
                compaction_context::Column::ThreadId,
            ))
            .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
        )
        .to_owned();
    let mut query = root_history_checks();
    query
        .expr(literal(1_i64))
        .and_where(
            Expr::col((
                compaction_history_check::Entity,
                compaction_history_check::Column::TurnId,
            ))
            .eq(turn),
        )
        .and_where(Expr::exists(stopped).not());
    if let Some(revision) = revision {
        query.and_where(
            Expr::col((
                compaction_history_check::Entity,
                compaction_history_check::Column::Revision,
            ))
            .eq(revision),
        );
    }
    Ok(db.query_one(&query).await?.is_some())
}

/// Indexed control metadata only; no reader held across capture or admission.
pub async fn root_history_activity<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<i64>> {
    use compaction_root_activity as a;
    let query = Query::select()
        .column((a::Entity, a::Column::LastUserActivityMs))
        .from(a::Entity)
        .join(
            JoinType::InnerJoin,
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .eq(Expr::col((a::Entity, a::Column::ThreadId))),
        )
        .and_where(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .and_where(Expr::col((a::Entity, a::Column::ThreadId)).eq(thread))
        .and_where(Expr::col((a::Entity, a::Column::UserTurnId)).eq(turn))
        .to_owned();
    db.query_one(&query)
        .await?
        .map(|r| r.try_get("", "last_user_activity_ms"))
        .transpose()
        .map_err(Into::into)
}

pub async fn root_history_busy<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    root: &str,
) -> Result<bool> {
    use HistoryAlias::{Composer as K, Root as H};
    let direct = Query::select()
        .expr(literal(1_i64))
        .from(turn::Entity)
        .and_where(
            Expr::col((turn::Entity, turn::Column::ThreadId))
                .eq(Expr::col((thread::Entity, thread::Column::Id))),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).eq(literal("in_progress")))
        .and_where(Expr::col((turn::Entity, turn::Column::Origin)).eq(literal("user")))
        .and_where(Expr::col((turn::Entity, turn::Column::WorkOwner)).eq(literal("turn")))
        .to_owned();
    let composer = composer_task_scope()
        .expr(literal(1_i64))
        .and_where(
            Expr::col((H, thread::Column::Id)).eq(Expr::col((thread::Entity, thread::Column::Id))),
        )
        // Match the partial live-work index literally.
        .and_where(Expr::col((K, task::Column::Status)).is_not_in([
            literal("completed"),
            literal("failed"),
            literal("blocked"),
            literal("cancelled"),
        ]))
        .and_where(Expr::col((K, task::Column::MetadataJson)).is_not_null())
        .to_owned();
    let query = Query::select()
        .expr(literal(1_i64))
        .from(thread::Entity)
        .and_where(Expr::col((thread::Entity, thread::Column::Id)).eq(root))
        .and_where(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .cond_where(
            Condition::any()
                .add(Expr::exists(direct))
                .add(Expr::exists(composer)),
        )
        .to_owned();
    Ok(db.query_one(&query).await?.is_some())
}

/// A technical Composer execution shares the visible conversation's priority.
pub async fn root_for_history_execution<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<String>> {
    use HistoryAlias::{Composer as K, Execution as X, Human as U, Root as H};
    let query = composer_task_scope()
        .expr_as(Expr::col((H, thread::Column::Id)), Alias::new("root"))
        .expr_as(Expr::col((U, turn::Column::Id)), Alias::new("human_turn"))
        .expr_as(
            Expr::col((K, task::Column::MetadataJson)).cast_as("text"),
            Alias::new("metadata_json"),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_turn::Entity,
            X,
            Expr::col((X, task_run_turn::Column::TaskId)).eq(Expr::col((K, task::Column::Id))),
        )
        .and_where(Expr::col((X, task_run_turn::Column::TurnId)).eq(turn))
        .and_where(Expr::col((X, task_run_turn::Column::ThreadId)).eq(thread))
        .and_where(Expr::col((K, task::Column::WorkspaceId)).eq(workspace))
        .and_where(Expr::col((H, thread::Column::OriginKind)).is_in([
            "user",
            "collaborative",
            "direct_message",
        ]))
        .and_where(no_child_lineage(Expr::col((H, thread::Column::Id))))
        .limit(1)
        .to_owned();
    let Some(row) = db.query_one(&query).await? else {
        return Ok(None);
    };
    let root: String = row.try_get("", "root")?;
    let human: String = row.try_get("", "human_turn")?;
    let metadata: Option<String> = row.try_get("", "metadata_json")?;
    // Match the existing protocol decoder after releasing reader capacity.
    let composer = metadata
        .as_deref()
        .and_then(|json| serde_json::from_str::<pioneer_protocol::TaskMetadata>(json).ok())
        .and_then(|metadata| metadata.composer_work);
    Ok(composer
        .filter(|work| {
            work.version == pioneer_protocol::TASK_COMPOSER_WORK_VERSION
                && work.launch.thread_id == root
                && work.launch.turn_id == human
        })
        .map(|_| root))
}

/// One bounded metadata page; caller releases reader capacity before decoding.
pub async fn compaction_pending_history_checks<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<CompletedHistoryCheck>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .min(i64::MAX as u128) as i64;
    compaction_due_history_checks(db, now).await
}
pub async fn compaction_due_history_checks<C: ConnectionTrait>(
    db: &C,
    now: i64,
) -> Result<Vec<CompletedHistoryCheck>> {
    compaction_due_history_checks_excluding(db, now, &[]).await
}
pub async fn compaction_due_history_checks_excluding<C: ConnectionTrait>(
    db: &C,
    now: i64,
    owned: &[(String, String)],
) -> Result<Vec<CompletedHistoryCheck>> {
    use compaction_history_check as h;
    use turn_cli_runtime_binding as b;
    let mut query = root_history_checks();
    query
        .columns(
            [
                h::Column::TurnId,
                h::Column::Descriptor,
                h::Column::Revision,
                h::Column::Failures,
                h::Column::AttemptDeadlineMs,
                h::Column::Diagnostic,
                h::Column::ConfigHash,
                h::Column::Outcome,
                h::Column::Managed,
            ]
            .map(|col| (h::Entity, col)),
        )
        .columns(
            [turn::Column::ThreadId, turn::Column::ReasoningEffort].map(|col| (turn::Entity, col)),
        )
        .column((thread::Entity, thread::Column::WorkspaceId))
        .join(
            JoinType::InnerJoin,
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
        )
        .join(
            JoinType::LeftJoin,
            b::Entity,
            Condition::all()
                .add(
                    Expr::col((b::Entity, b::Column::TurnId))
                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                )
                .add(
                    Expr::col((b::Entity, b::Column::ThreadId))
                        .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                )
                .add(
                    Expr::col((b::Entity, b::Column::WorkspaceId))
                        .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                ),
        )
        .expr_as(
            Func::coalesce([
                Expr::col((b::Entity, b::Column::RuntimeId)),
                Expr::col((thread::Entity, thread::Column::ModelProvider)),
            ]),
            b::Column::RuntimeId,
        )
        .expr_as(
            Func::coalesce([Expr::col((b::Entity, b::Column::RuntimeKind)), literal("")]),
            b::Column::RuntimeKind,
        )
        .expr_as(
            Func::coalesce([
                Expr::col((b::Entity, b::Column::Model)),
                Expr::col((thread::Entity, thread::Column::Model)),
            ]),
            b::Column::Model,
        )
        .and_where(Expr::col((h::Entity, h::Column::NextAttemptMs)).lte(now))
        .order_by((h::Entity, h::Column::NextAttemptMs), Order::Asc)
        .order_by((h::Entity, h::Column::TurnId), Order::Asc)
        .limit(16);
    exclude_owned_roots(&mut query, owned);
    CompletedHistoryCheck::find_by_statement(db.get_database_backend().build(&query))
        .all(db)
        .await
        .map_err(Into::into)
}

/// Only current root obligations, excluding conversations owned in memory.
/// The exclusion is bounded by the service's 16-job cap.
pub async fn next_history_deadline<C: ConnectionTrait>(
    db: &C,
    owned: &[(String, String)],
) -> Result<Option<i64>> {
    use compaction_history_check::{Column as H, Entity as E};
    let mut current = root_history_checks();
    current
        .column((compaction_history_check::Entity, H::TurnId))
        .join(
            JoinType::InnerJoin,
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
        );
    exclude_owned_roots(&mut current, owned);
    let row = E::find()
        .select_only()
        .column(H::NextAttemptMs)
        .filter(H::TurnId.in_subquery(current))
        .order_by_asc(H::NextAttemptMs)
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?;
    Ok(row)
}

fn exclude_owned_roots(query: &mut SelectStatement, owned: &[(String, String)]) {
    // The in-memory owner is keyed by conversation, not Turn. A newer Turn
    // in the same root must wait for that owner's completion notification.
    for (workspace, root) in owned {
        query.and_where(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(workspace)
                .and(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(root))
                .not(),
        );
    }
}

/// Persist the captured settings/deadline before service admission; restart
/// reuses this exact descriptor. It contains model metadata, never messages.
pub async fn compaction_capture_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    descriptor: &str,
) -> Result<Option<String>> {
    ensure!(
        descriptor.len() <= 16384,
        "CLI history descriptor exceeds bound"
    );
    let rows = compaction_history_check::Entity::update_many()
        .col_expr(
            compaction_history_check::Column::Descriptor,
            Func::coalesce([
                Expr::col(compaction_history_check::Column::Descriptor),
                Expr::val(descriptor),
            ])
            .into(),
        )
        .filter(compaction_history_check::Column::TurnId.eq(turn))
        .filter(compaction_history_check::Column::State.eq("pending"))
        .exec_with_returning(db)
        .await?;
    Ok(rows.into_iter().next().and_then(|row| row.descriptor))
}
pub async fn compaction_finish_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    outcome: &str,
) -> Result<()> {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
    ensure!(
        matches!(outcome, "completed" | "cancelled" | "failed"),
        "invalid CLI history outcome"
    );
    check::Entity::update_many()
        .set(check::ActiveModel {
            state: Set("finished".to_owned()),
            outcome: Set(Some(outcome.to_owned())),
            ..Default::default()
        })
        .filter(check::Column::TurnId.eq(turn))
        .filter(check::Column::State.eq("pending"))
        .exec(db)
        .await?;
    Ok(())
}

use sea_orm::QueryTrait;

/// CAS admission. Revalidate currency within the writer transaction: neither a
/// stale discovery page nor Stop/new input can resurrect a completed check.
pub async fn claim_history_check(
    tx: &sea_orm::DatabaseTransaction,
    id: &str,
    revision: i64,
    now: i64,
) -> Result<Option<compaction_history_check::Model>> {
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};
    let Some(_) = E::find_by_id(id)
        .lock_exclusive()
        .one(tx)
        .await?
        .filter(|row| {
            row.revision == revision
                && row.next_attempt_ms <= now
                && row.state == "pending"
                && row.purpose == "root_conversation"
        })
    else {
        return Ok(None);
    };
    E::update_many()
        .set(A {
            state: Set("pending".into()),
            managed: Set(1),
            revision: Set(revision + 1),
            ..Default::default()
        })
        .filter(C::TurnId.eq(id))
        .exec(tx)
        .await?;
    if !compaction_history_check_is_current(tx, id, None).await? {
        compaction_finish_history_check(tx, id, "cancelled").await?;
        return Ok(None);
    }
    Ok(E::find_by_id(id).one(tx).await?)
}
/// Captured input was prepared without holding database capacity. CAS prevents
/// a cancelled or replaced job from installing it. Deadlines survive restart.
pub async fn begin_history_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    descriptor: &str,
    hash: &str,
    now: i64,
) -> Result<Option<i64>> {
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};
    use sea_orm::Set;
    ensure!(
        descriptor.len() <= 16384 && hash.len() <= 128,
        "history check metadata exceeds bound"
    );
    let rows = E::update_many()
        .set(A {
            descriptor: Set(Some(descriptor.into())),
            config_hash: Set(Some(hash.into())),
            ..Default::default()
        })
        .col_expr(
            C::AttemptDeadlineMs,
            Func::coalesce([
                Expr::col(C::AttemptDeadlineMs),
                Expr::val(now.saturating_add(pioneer_compaction::OPERATION_MILLIS as i64)),
            ])
            .into(),
        )
        .filter(C::TurnId.eq(id))
        .filter(C::Revision.eq(revision))
        .filter(C::State.eq("pending"))
        .exec_with_returning(db)
        .await?;
    Ok(rows.first().and_then(|r| r.attempt_deadline_ms))
}
pub async fn record_history_result(
    tx: &sea_orm::DatabaseTransaction,
    id: &str,
    revision: i64,
    failures: i64,
    outcome: super::history_check_result::HistoryCheckOutcome,
    diagnostic: serde_json::Value,
    now: i64,
) -> Result<bool> {
    use super::history_check_result::HistoryCheckOutcome;
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};

    let outcome = if compaction_history_check_is_current(tx, id, None).await? {
        outcome
    } else {
        HistoryCheckOutcome::Cancelled
    };
    let (state, failures, due) = outcome.schedule(failures, now);
    let label = if state == "finished" && outcome == HistoryCheckOutcome::Retryable {
        "failed"
    } else {
        outcome.as_str()
    };
    let result = E::update_many()
        .set(A {
            state: Set(state.into()),
            outcome: Set(Some(label.into())),
            failures: Set(failures),
            next_attempt_ms: Set(due),
            diagnostic: Set(Some(diagnostic)),
            attempt_deadline_ms: Set(None),
            revision: Set(revision + 1),
            ..Default::default()
        })
        .filter(C::TurnId.eq(id))
        .filter(C::Revision.eq(revision))
        .filter(C::State.eq("pending"))
        .exec(tx)
        .await?;
    Ok(result.rows_affected == 1)
}
