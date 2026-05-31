use anyhow::{Context, Result};
use pioneer_entity::task_result_candidate;
use pioneer_protocol::{TaskError, TaskResult, TaskResultCandidateStatus};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::task_result_candidate_status_to_db;
use crate::util::{optional_typed_json_to_db, typed_json_to_db, unix_to_datetime};

pub struct NewTaskResultCandidate {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub round: u32,
    pub status: TaskResultCandidateStatus,
    pub result: Option<TaskResult>,
    pub extraction_error: Option<TaskError>,
    pub summary: Option<String>,
    pub diagnostics: Vec<String>,
    pub final_review_event_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub resolved_at: Option<i64>,
}

pub async fn upsert_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: NewTaskResultCandidate,
) -> Result<()> {
    task_result_candidate::Entity::insert(active_model_from_new_candidate(candidate)?)
        .on_conflict(
            OnConflict::column(task_result_candidate::Column::Id)
                .update_columns([
                    task_result_candidate::Column::TaskId,
                    task_result_candidate::Column::RunId,
                    task_result_candidate::Column::TaskRunTurnId,
                    task_result_candidate::Column::ThreadId,
                    task_result_candidate::Column::TurnId,
                    task_result_candidate::Column::Round,
                    task_result_candidate::Column::Status,
                    task_result_candidate::Column::ResultJson,
                    task_result_candidate::Column::ExtractionErrorJson,
                    task_result_candidate::Column::Summary,
                    task_result_candidate::Column::DiagnosticsJson,
                    task_result_candidate::Column::FinalReviewEventId,
                    task_result_candidate::Column::UpdatedAt,
                    task_result_candidate::Column::ResolvedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task result candidate")?;
    Ok(())
}

pub async fn find_candidate_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<task_result_candidate::Model>> {
    task_result_candidate::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query task result candidate by id")
}

pub async fn find_candidate_by_turn<C: ConnectionTrait>(
    db: &C,
    task_run_turn_id: &str,
) -> Result<Option<task_result_candidate::Model>> {
    task_result_candidate::Entity::find()
        .filter(task_result_candidate::Column::TaskRunTurnId.eq(task_run_turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query task result candidate by turn")
}

pub async fn list_candidates_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_result_candidate::Model>> {
    task_result_candidate::Entity::find()
        .filter(task_result_candidate::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_result_candidate::Column::Round)
        .order_by_asc(task_result_candidate::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task result candidates by run")
}

pub async fn find_candidate_by_run_and_status<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    status: TaskResultCandidateStatus,
) -> Result<Option<task_result_candidate::Model>> {
    task_result_candidate::Entity::find()
        .filter(task_result_candidate::Column::RunId.eq(run_id.to_owned()))
        .filter(
            task_result_candidate::Column::Status.eq(task_result_candidate_status_to_db(status)),
        )
        .order_by_desc(task_result_candidate::Column::Round)
        .order_by_desc(task_result_candidate::Column::CreatedAt)
        .one(db)
        .await
        .context("failed to query task result candidate by run and status")
}

pub async fn update_candidate_resolution<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: TaskResultCandidateStatus,
    final_review_event_id: Option<&str>,
    resolved_at: Option<i64>,
    updated_at: i64,
) -> Result<Option<task_result_candidate::Model>> {
    let update_result = task_result_candidate::Entity::update_many()
        .filter(task_result_candidate::Column::Id.eq(id.to_owned()))
        .col_expr(
            task_result_candidate::Column::Status,
            Expr::value(task_result_candidate_status_to_db(status)),
        )
        .col_expr(
            task_result_candidate::Column::FinalReviewEventId,
            Expr::value(final_review_event_id.map(str::to_owned)),
        )
        .col_expr(
            task_result_candidate::Column::ResolvedAt,
            Expr::value(resolved_at.map(unix_to_datetime)),
        )
        .col_expr(
            task_result_candidate::Column::UpdatedAt,
            Expr::value(unix_to_datetime(updated_at)),
        )
        .exec(db)
        .await
        .context("failed to update task result candidate resolution")?;
    if update_result.rows_affected == 0 {
        return Ok(None);
    }
    find_candidate_by_id(db, id).await
}

fn active_model_from_new_candidate(
    candidate: NewTaskResultCandidate,
) -> Result<task_result_candidate::ActiveModel> {
    Ok(task_result_candidate::ActiveModel {
        id: Set(candidate.id),
        task_id: Set(candidate.task_id),
        run_id: Set(candidate.run_id),
        task_run_turn_id: Set(candidate.task_run_turn_id),
        thread_id: Set(candidate.thread_id),
        turn_id: Set(candidate.turn_id),
        round: Set(i32::try_from(candidate.round)
            .context("task result candidate round is out of range")?),
        status: Set(task_result_candidate_status_to_db(candidate.status)),
        result_json: Set(optional_typed_json_to_db(&candidate.result)?),
        extraction_error_json: Set(optional_typed_json_to_db(&candidate.extraction_error)?),
        summary: Set(candidate.summary),
        diagnostics_json: Set(Some(typed_json_to_db(&candidate.diagnostics)?)),
        final_review_event_id: Set(candidate.final_review_event_id),
        created_at: Set(unix_to_datetime(candidate.created_at)),
        updated_at: Set(unix_to_datetime(candidate.updated_at)),
        resolved_at: Set(candidate.resolved_at.map(unix_to_datetime)),
    })
}
