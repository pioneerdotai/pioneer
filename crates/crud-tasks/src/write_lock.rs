use anyhow::{Context, Result};
use pioneer_entity::task_write_lock;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::extension::postgres::PgExpr;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct TaskWriteLockConflict {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
}

/// Return the oldest active lock that overlaps an owned path on component boundaries.
pub async fn find_active_conflict_for_run<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    run_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<TaskWriteLockConflict>> {
    use task_write_lock::{Column as Field, Entity as Locks};
    let owned_path = Expr::col(("owned", Field::ScopePath));
    let candidate_path = Expr::col((Locks, Field::ScopePath));
    // PostgreSQL starts_with compares text literally, including SQL wildcard characters.
    let is_below = |path: Expr, parent: Expr| {
        Expr::from(Func::cust(Alias::new("starts_with")).args([path, parent.concat("/")]))
    };
    let owned_overlap = Query::select()
        .expr(1)
        .from_as(Locks, "owned")
        .and_where(Expr::col(("owned", Field::RunId)).eq(run_id))
        .cond_where(
            Condition::any()
                .add(owned_path.clone().eq("."))
                .add(candidate_path.clone().eq("."))
                .add(owned_path.clone().eq(candidate_path.clone()))
                .add(is_below(owned_path.clone(), candidate_path.clone()))
                .add(is_below(candidate_path, owned_path)),
        )
        .to_owned();
    Locks::find()
        .select_only()
        .columns([Field::Id, Field::TaskId, Field::RunId])
        .filter(Field::WorkspaceId.eq(workspace_id))
        .filter(Field::RunId.ne(run_id))
        .filter(Field::Status.eq("acquired"))
        .filter(
            Condition::any()
                .add(Field::ExpiresAt.is_null())
                .add(Field::ExpiresAt.gt(now)),
        )
        .filter(Expr::exists(owned_overlap))
        .order_by_asc(Field::CreatedAt)
        .order_by_asc(Field::Id)
        .limit(1)
        .into_model::<TaskWriteLockConflict>()
        .one(db)
        .await
        .context("failed to find conflicting active task write lock")
}
