use anyhow::{Context, Result};
use pioneer_entity::{task, task_run, thread, thread_membership, workspace, workspace_membership};
use sea_orm::sea_query::{Expr, ExprTrait, Order, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, FromQueryResult};

use crate::task_scope::{TaskScopeAlias as Scope, task_scope_query};

#[derive(FromQueryResult)]
pub struct RunningTaskScope {
    pub task_id: String,
    pub root_thread_id: String,
    pub access_class: String,
    pub root_actor_kind: Option<String>,
    pub root_actor_id: Option<String>,
    pub cause_actor_kind: Option<String>,
    pub cause_actor_id: Option<String>,
    pub owner_kind: String,
    pub owner_id: Option<String>,
    pub is_active: bool,
    pub workspace_member: bool,
    pub thread_member: bool,
}

pub async fn running_task_scopes<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_ids: &[String],
    principal_id: &str,
    task_ids: &[String],
) -> Result<Vec<RunningTaskScope>> {
    if thread_ids.is_empty() || task_ids.is_empty() {
        return Ok(Vec::new());
    }
    anyhow::ensure!(
        thread_ids.len() <= 500,
        "task activity candidate batch exceeds tree limit"
    );
    anyhow::ensure!(
        task_ids.len() <= 128,
        "task scope lookup exceeds batch limit"
    );

    let workspace_member = Query::select()
        .expr(1)
        .from(workspace_membership::Entity)
        .and_where(workspace_membership::Column::PrincipalId.eq(principal_id))
        .and_where(
            Expr::col(workspace_membership::Column::WorkspaceId)
                .equals((Scope::Workspace, workspace::Column::Id)),
        )
        .to_owned();
    let thread_member = Query::select()
        .expr(1)
        .from(thread_membership::Entity)
        .and_where(thread_membership::Column::PrincipalId.eq(principal_id))
        .and_where(
            Expr::col(thread_membership::Column::ThreadId)
                .equals((Scope::Thread, thread::Column::Id)),
        )
        .to_owned();
    let executing = Query::select()
        .expr(1)
        .from(task_run::Entity)
        .and_where(Expr::col(task_run::Column::TaskId).equals((Scope::Task, task::Column::Id)))
        .and_where(task_run::Column::Status.is_in(["starting", "running"]))
        .to_owned();
    let mut query = task_scope_query(
        Query::select()
            .from_as(task::Entity, Scope::Task)
            .to_owned(),
    );
    query
        .expr_as(Expr::col((Scope::Task, task::Column::Id)), "task_id")
        .expr_as(
            Expr::col((Scope::Thread, thread::Column::Id)),
            "root_thread_id",
        )
        .expr_as(
            Expr::col((Scope::Thread, thread::Column::AccessClass)),
            "access_class",
        )
        .expr_as(
            Expr::col((Scope::Thread, thread::Column::CreatedByActorKind)),
            "root_actor_kind",
        )
        .expr_as(
            Expr::col((Scope::Thread, thread::Column::CreatedByActorId)),
            "root_actor_id",
        )
        .expr_as(
            Expr::col((Scope::Origin, thread::Column::CreatedByActorKind)),
            "cause_actor_kind",
        )
        .expr_as(
            Expr::col((Scope::Origin, thread::Column::CreatedByActorId)),
            "cause_actor_id",
        )
        .expr_as(
            Expr::col((Scope::Root, task::Column::OwnerKind)),
            "owner_kind",
        )
        .expr_as(Expr::col((Scope::Root, task::Column::OwnerId)), "owner_id")
        .expr_as(
            Expr::col((Scope::Workspace, workspace::Column::IsActive)),
            "is_active",
        )
        .expr_as(Expr::exists(workspace_member), "workspace_member")
        .expr_as(Expr::exists(thread_member), "thread_member")
        .and_where(Expr::col((Scope::Task, task::Column::WorkspaceId)).eq(workspace_id))
        .and_where(Expr::col((Scope::Task, task::Column::Id)).is_in(task_ids.iter().cloned()))
        .and_where(Expr::col((Scope::Thread, thread::Column::Id)).is_in(thread_ids.iter().cloned()))
        .and_where(Expr::exists(executing))
        .order_by((Scope::Task, task::Column::Id), Order::Asc);
    db.query_all(&query)
        .await
        .context("failed to load running task scopes")?
        .into_iter()
        .map(|row| RunningTaskScope::from_query_result(&row, "").map_err(Into::into))
        .collect()
}
