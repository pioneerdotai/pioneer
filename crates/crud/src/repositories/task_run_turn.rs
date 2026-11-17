use anyhow::{Context, Result};
use pioneer_entity::task_run_turn;
use pioneer_protocol::{TaskRunTurnKind, TaskRunTurnStatus};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{task_run_turn_kind_to_db, task_run_turn_status_to_db};
use crate::util::unix_to_datetime;

pub struct NewTaskRunTurn {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub execution_id: Option<String>,
    pub thread_id: String,
    pub turn_id: String,
    pub kind: TaskRunTurnKind,
    pub round: u32,
    pub sequence: u32,
    pub status: TaskRunTurnStatus,
    pub reviews_candidate_id: Option<String>,
    pub requested_by_candidate_id: Option<String>,
    pub requested_by_review_event_id: Option<String>,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}

pub async fn upsert_turn<C: ConnectionTrait>(db: &C, turn: NewTaskRunTurn) -> Result<()> {
    let id = turn.id.clone();
    let old = task_run_turn::Entity::find_by_id(&id)
        .lock_exclusive()
        .one(db)
        .await?;
    task_run_turn::Entity::insert(active_model_from_new_turn(turn)?)
        .on_conflict(
            OnConflict::column(task_run_turn::Column::Id)
                .update_columns([
                    task_run_turn::Column::TaskId,
                    task_run_turn::Column::RunId,
                    task_run_turn::Column::ExecutionId,
                    task_run_turn::Column::ThreadId,
                    task_run_turn::Column::TurnId,
                    task_run_turn::Column::Kind,
                    task_run_turn::Column::Round,
                    task_run_turn::Column::Sequence,
                    task_run_turn::Column::Status,
                    task_run_turn::Column::ReviewsCandidateId,
                    task_run_turn::Column::RequestedByCandidateId,
                    task_run_turn::Column::RequestedByReviewEventId,
                    task_run_turn::Column::StartedAt,
                    task_run_turn::Column::CompletedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task run turn")?;
    let new = task_run_turn::Entity::find_by_id(&id)
        .lock_exclusive()
        .one(db)
        .await?;
    super::task_child_producers::link_change(db, old.as_ref(), new.as_ref()).await?;
    Ok(())
}

pub async fn find_turn_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<task_run_turn::Model>> {
    task_run_turn::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query task run turn by id")
}

pub async fn find_turn_by_thread_and_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<task_run_turn::Model>> {
    task_run_turn::Entity::find()
        .filter(task_run_turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(task_run_turn::Column::TurnId.eq(turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query task run turn by thread and turn")
}

pub async fn list_turns_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_run_turn::Model>> {
    task_run_turn::Entity::find()
        .filter(task_run_turn::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_run_turn::Column::Sequence)
        .order_by_asc(task_run_turn::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task run turns by run")
}

pub async fn find_latest_turn_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<task_run_turn::Model>> {
    task_run_turn::Entity::find()
        .filter(task_run_turn::Column::RunId.eq(run_id.to_owned()))
        .order_by_desc(task_run_turn::Column::Sequence)
        .order_by_desc(task_run_turn::Column::CreatedAt)
        .one(db)
        .await
        .context("failed to query latest task run turn by run")
}

pub async fn update_turn_status<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: TaskRunTurnStatus,
    completed_at: Option<i64>,
) -> Result<Option<task_run_turn::Model>> {
    let old = task_run_turn::Entity::find_by_id(id)
        .lock_exclusive()
        .one(db)
        .await?;
    let mut update = task_run_turn::Entity::update_many()
        .filter(task_run_turn::Column::Id.eq(id.to_owned()))
        .col_expr(
            task_run_turn::Column::Status,
            Expr::value(task_run_turn_status_to_db(status)),
        );
    if let Some(completed_at) = completed_at {
        update = update.col_expr(
            task_run_turn::Column::CompletedAt,
            Expr::value(Some(unix_to_datetime(completed_at))),
        );
    }
    let result = update
        .exec(db)
        .await
        .context("failed to update task run turn status")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    let new = task_run_turn::Entity::find_by_id(id)
        .lock_exclusive()
        .one(db)
        .await?;
    super::task_child_producers::link_change(db, old.as_ref(), new.as_ref()).await?;
    Ok(new)
}

fn active_model_from_new_turn(turn: NewTaskRunTurn) -> Result<task_run_turn::ActiveModel> {
    Ok(task_run_turn::ActiveModel {
        id: Set(turn.id),
        task_id: Set(turn.task_id),
        run_id: Set(turn.run_id),
        execution_id: Set(turn.execution_id),
        thread_id: Set(turn.thread_id),
        turn_id: Set(turn.turn_id),
        kind: Set(task_run_turn_kind_to_db(turn.kind)),
        round: Set(i64::from(turn.round)),
        sequence: Set(i64::from(turn.sequence)),
        status: Set(task_run_turn_status_to_db(turn.status)),
        reviews_candidate_id: Set(turn.reviews_candidate_id),
        requested_by_candidate_id: Set(turn.requested_by_candidate_id),
        requested_by_review_event_id: Set(turn.requested_by_review_event_id),
        created_at: Set(unix_to_datetime(turn.created_at)),
        started_at: Set(turn.started_at.map(unix_to_datetime)),
        completed_at: Set(turn.completed_at.map(unix_to_datetime)),
    })
}

pub(crate) async fn find_turns_by_sources<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    sources: &[(String, String)],
) -> Result<Vec<task_run_turn::Model>> {
    use pioneer_entity::thread;
    use sea_orm::{Condition, QueryTrait};
    let mut rows = Vec::new();
    for sources in sources.chunks(64) {
        let mut exact = Condition::any();
        for (thread, turn) in sources {
            exact = exact.add(
                Condition::all()
                    .add(task_run_turn::Column::ThreadId.eq(thread.clone()))
                    .add(task_run_turn::Column::TurnId.eq(turn.clone())),
            );
        }
        rows.extend(
            task_run_turn::Entity::find()
                .filter(exact)
                .filter(
                    task_run_turn::Column::ThreadId.in_subquery(
                        thread::Entity::find()
                            .select_only()
                            .column(thread::Column::Id)
                            .filter(thread::Column::WorkspaceId.eq(workspace))
                            .into_query(),
                    ),
                )
                .all(db)
                .await
                .context("failed to load exact execution sources")?,
        );
    }
    Ok(rows)
}
