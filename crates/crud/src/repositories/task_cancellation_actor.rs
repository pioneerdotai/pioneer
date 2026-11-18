//! Durable attribution of an accepted Stop to its exact still-active runs.
//! This is intent, not a read receipt: only a successful RunCancelled commit
//! uses it. Replayed requests cannot replace the first accepted actor.
use anyhow::Result;
use pioneer_entity::{
    agent_execution, task, task_occurrence_contract, task_run,
    task_run_cancellation_actor as actor, task_run_turn, thread, turn, workspace,
};
use sea_orm::sea_query::{
    Alias, Condition, Expr, ExprTrait, Func, OnConflict, Query, SelectStatement,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

const TERMINAL: [&str; 5] = ["succeeded", "failed", "blocked", "cancelled", "timed_out"];

fn active_runs(principal: &str, origin: &str, workspace: &str) -> SelectStatement {
    Query::select()
        .column((task_run::Entity, task_run::Column::Id))
        .column((task::Entity, task::Column::Id))
        .column((task::Entity, task::Column::WorkspaceId))
        .expr(Expr::val(principal))
        .expr(Expr::val(origin))
        .from(task::Entity)
        .inner_join(
            task_run::Entity,
            Expr::col((task_run::Entity, task_run::Column::TaskId))
                .equals((task::Entity, task::Column::Id)),
        )
        .inner_join(
            workspace::Entity,
            Expr::col((workspace::Entity, workspace::Column::Id))
                .equals((task::Entity, task::Column::WorkspaceId)),
        )
        .and_where(Expr::col((task::Entity, task::Column::WorkspaceId)).eq(workspace))
        .and_where(Expr::col((task_run::Entity, task_run::Column::Status)).is_not_in(TERMINAL))
        .to_owned()
}
async fn insert<C: ConnectionTrait>(db: &C, source: SelectStatement) -> Result<()> {
    let insert = Query::insert()
        .into_table(actor::Entity)
        .columns([
            actor::Column::RunId,
            actor::Column::TaskId,
            actor::Column::WorkspaceId,
            actor::Column::PrincipalId,
            actor::Column::OriginTurnId,
        ])
        .select_from(source)?
        .on_conflict(
            OnConflict::column(actor::Column::RunId)
                .do_nothing()
                .to_owned(),
        )
        .to_owned();
    db.execute(&insert).await?;
    Ok(())
}

pub(crate) async fn record_turn<C: ConnectionTrait>(
    db: &C,
    principal: &str,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<()> {
    let mut source = active_runs(principal, turn_id, workspace);
    source
        .inner_join(
            task_run_turn::Entity,
            Condition::all()
                .add(
                    Expr::col((task_run_turn::Entity, task_run_turn::Column::RunId))
                        .equals((task_run::Entity, task_run::Column::Id)),
                )
                .add(
                    Expr::col((task_run_turn::Entity, task_run_turn::Column::TaskId))
                        .equals((task::Entity, task::Column::Id)),
                ),
        )
        .inner_join(
            turn::Entity,
            Expr::col((turn::Entity, turn::Column::Id))
                .equals((task_run_turn::Entity, task_run_turn::Column::TurnId)),
        )
        .inner_join(
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .equals((turn::Entity, turn::Column::ThreadId)),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Id)).eq(turn_id))
        .and_where(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(thread_id))
        .and_where(
            Expr::col((task_run_turn::Entity, task_run_turn::Column::ThreadId)).eq(thread_id),
        )
        .and_where(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace));
    insert(db, source).await
}

pub(crate) async fn record_graph<C: ConnectionTrait>(
    db: &C,
    principal: &str,
    thread_id: &str,
    turn_id: &str,
    root_id: &str,
) -> Result<()> {
    let Some(root) = agent_execution::Entity::find_by_id(root_id).one(db).await? else {
        return Ok(());
    };
    if root.id != root.work_graph_root_execution_id
        || root.parent_execution_id.is_some()
        || [
            "completed",
            "succeeded",
            "failed",
            "blocked",
            "cancelled",
            "timed_out",
        ]
        .contains(&root.status.as_str())
    {
        return Ok(());
    }
    // FK OFF: validate the actual originating turn, rather than trusting a
    // caller-supplied workspace or a dangling occurrence contract.
    let valid_turn = Query::select()
        .expr(Expr::val(1))
        .from(turn::Entity)
        .inner_join(
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .equals((turn::Entity, turn::Column::ThreadId)),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Id)).eq(turn_id))
        .and_where(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(thread_id))
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).eq("in_progress"))
        .and_where(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(&root.workspace_id))
        .to_owned();
    let occurrence = Query::select()
        .expr(Expr::val(1))
        .from(task_occurrence_contract::Entity)
        .and_where(
            Expr::col((
                task_occurrence_contract::Entity,
                task_occurrence_contract::Column::RunId,
            ))
            .equals((task_run::Entity, task_run::Column::Id)),
        )
        .and_where(
            Expr::col((
                task_occurrence_contract::Entity,
                task_occurrence_contract::Column::TaskId,
            ))
            .equals((task::Entity, task::Column::Id)),
        )
        .and_where(
            Expr::col((
                task_occurrence_contract::Entity,
                task_occurrence_contract::Column::WorkGraphRootExecutionId,
            ))
            .eq(root_id),
        )
        .to_owned();
    let policy = |path: &str| {
        Func::cust(Alias::new("json_extract")).args([
            Expr::col((task::Entity, task::Column::LifecyclePolicyJson)).into(),
            Expr::val(path).into(),
        ])
    };
    let attached = Condition::all()
        .add(Expr::col((task::Entity, task::Column::CreatedByThreadId)).eq(thread_id))
        .add(Expr::col((task::Entity, task::Column::CreatedByTurnId)).eq(turn_id))
        .add(Expr::expr(policy("$.attachment")).eq("attached"))
        .add(Expr::expr(policy("$.onParentCancel")).eq("cancel"));
    let mut source = active_runs(principal, turn_id, &root.workspace_id);
    source
        .and_where(Expr::exists(valid_turn))
        .cond_where(Condition::any().add(Expr::exists(occurrence)).add(attached));
    insert(db, source).await
}

pub(crate) async fn principal<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    task_id: &str,
    run_id: &str,
) -> Result<Option<String>> {
    Ok(actor::Entity::find_by_id(run_id)
        .filter(actor::Column::TaskId.eq(task_id))
        .filter(actor::Column::WorkspaceId.eq(workspace))
        .one(db)
        .await?
        .map(|r| r.principal_id))
}

pub(crate) async fn accepted_for_turn<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    task_id: &str,
    run_id: &str,
    turn_id: &str,
) -> Result<bool> {
    Ok(actor::Entity::find_by_id(run_id)
        .filter(actor::Column::TaskId.eq(task_id))
        .filter(actor::Column::WorkspaceId.eq(workspace))
        .filter(actor::Column::OriginTurnId.eq(turn_id))
        .one(db)
        .await?
        .is_some())
}
