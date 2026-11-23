use anyhow::{Context, Result};
use pioneer_entity::{
    gateway_principal as principal, task, task_execution_admission as admission, thread, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Active execution row inspected by the startup authority-integrity worker.
/// The envelope remains opaque to CRUD and is validated by Gateway policy
/// code; CRUD never fabricates or repairs it.
#[derive(Debug, Clone, PartialEq, Eq, sea_orm::FromQueryResult)]
pub struct ActiveExecutionAuthorityRecord {
    pub thread_id: String,
    pub turn_id: String,
    pub authority_envelope_json: Option<String>,
}

/// Non-terminal Agent Task inspected by the startup authority-integrity gate.
/// Nullable admission columns let Gateway quarantine incomplete development
/// data without CRUD inventing an execution grant.
#[derive(Debug, Clone, PartialEq, Eq, sea_orm::FromQueryResult)]
pub struct ActiveTaskExecutionAuthorityRecord {
    pub task_id: String,
    pub task_workspace_id: String,
    pub admission_workspace_id: Option<String>,
    pub root_thread_id: Option<String>,
    pub initiating_principal_id: Option<String>,
    pub authority_envelope_json: Option<String>,
    pub root_thread_workspace_id: Option<String>,
    pub root_thread_access_class: Option<String>,
    pub principal_id: Option<String>,
    pub principal_kind: Option<String>,
}

pub async fn active_turns<C: ConnectionTrait>(
    db: &C,
    after: Option<&str>,
    limit: u64,
) -> Result<Vec<ActiveExecutionAuthorityRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    turn::Entity::find()
        .select_only()
        .column(turn::Column::ThreadId)
        .column_as(turn::Column::Id, "turn_id")
        .expr_as(
            Expr::col(turn::Column::ExecutionAuthorizationContextJson).cast_as("text"),
            "authority_envelope_json",
        )
        .filter(turn::Column::Status.eq("in_progress"))
        .filter(turn::Column::Id.gt(after.unwrap_or_default()))
        .order_by_asc(turn::Column::Id)
        .limit(limit)
        .into_model::<ActiveExecutionAuthorityRecord>()
        .all(db)
        .await
        .context("failed to list active execution authority envelopes")
}

pub async fn active_agent_tasks<C: ConnectionTrait>(
    db: &C,
    after: Option<&str>,
    limit: u64,
) -> Result<Vec<ActiveTaskExecutionAuthorityRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    task::Entity::find()
        .select_only()
        .column_as(task::Column::Id, "task_id")
        .column_as(task::Column::WorkspaceId, "task_workspace_id")
        .column_as(admission::Column::WorkspaceId, "admission_workspace_id")
        .column(admission::Column::RootThreadId)
        .column(admission::Column::InitiatingPrincipalId)
        .column_as(
            admission::Column::AuthorizationContextJson,
            "authority_envelope_json",
        )
        .column_as(thread::Column::WorkspaceId, "root_thread_workspace_id")
        .column_as(thread::Column::AccessClass, "root_thread_access_class")
        .column_as(principal::Column::Id, "principal_id")
        .column_as(principal::Column::Kind, "principal_kind")
        .join(
            JoinType::LeftJoin,
            task::Entity::belongs_to(admission::Entity)
                .from(task::Column::Id)
                .to(admission::Column::TaskId)
                .into(),
        )
        .join(
            JoinType::LeftJoin,
            admission::Entity::belongs_to(thread::Entity)
                .from(admission::Column::RootThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::LeftJoin,
            admission::Entity::belongs_to(principal::Entity)
                .from(admission::Column::InitiatingPrincipalId)
                .to(principal::Column::Id)
                .into(),
        )
        .filter(task::Column::ExecutorKind.eq("agent"))
        .filter(task::Column::Status.is_not_in(["completed", "failed", "blocked", "cancelled"]))
        .filter(task::Column::Id.gt(after.unwrap_or_default()))
        .order_by_asc(task::Column::Id)
        .limit(limit)
        .into_model::<ActiveTaskExecutionAuthorityRecord>()
        .all(db)
        .await
        .context("failed to list active Task execution authority envelopes")
}
