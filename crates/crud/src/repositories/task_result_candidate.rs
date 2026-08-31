use anyhow::{Context, Result, bail};
use pioneer_entity::{task_result_candidate, task_result_review_event};
use pioneer_protocol::{TaskError, TaskResult, TaskResultCandidateStatus};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::task_result_candidate_status_to_db;
use crate::util::{optional_typed_json_to_db, typed_json_to_db, unix_to_datetime};

#[derive(Clone)]
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
    let expected = candidate.clone();
    let gate_candidate_id = candidate.id.clone();
    let gate_updated_at = unix_to_datetime(candidate.updated_at);
    let expected_result = optional_typed_json_to_db(&expected.result)?;
    let expected_extraction_error = optional_typed_json_to_db(&expected.extraction_error)?;
    let expected_diagnostics = Some(typed_json_to_db(&expected.diagnostics)?);
    let expected_status = task_result_candidate_status_to_db(expected.status);
    task_result_candidate::Entity::insert(active_model_from_new_candidate(candidate)?)
        .on_conflict(
            OnConflict::column(task_result_candidate::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        // This method is an idempotent upsert: the projector deliberately
        // presents the same candidate in `candidate_created` and the
        // immediately following terminal review event.  `exec` asks SQLite
        // to return an inserted row and SeaORM maps a legitimate
        // `ON CONFLICT DO NOTHING` replay to `RecordNotInserted`.  Execute
        // without RETURNING so a zero-row conflict remains a successful
        // replay; the authoritative row is loaded and validated below.
        .exec_without_returning(db)
        .await
        .context("failed to upsert task result candidate")?;

    let current = find_candidate_by_id(db, gate_candidate_id.as_str())
        .await?
        .context("task result candidate disappeared after insert")?;
    if current.task_id != expected.task_id
        || current.run_id != expected.run_id
        || current.task_run_turn_id != expected.task_run_turn_id
        || current.thread_id != expected.thread_id
        || current.turn_id != expected.turn_id
        || current.round != i64::from(expected.round)
        || current.created_at != unix_to_datetime(expected.created_at)
    {
        bail!(
            "task result candidate `{}` conflicts with a different immutable candidate",
            expected.id
        );
    }
    let current_terminal = matches!(
        current.status.as_str(),
        "accepted" | "rejected" | "superseded" | "cancelled"
    );
    let expected_terminal = matches!(
        expected.status,
        TaskResultCandidateStatus::Accepted
            | TaskResultCandidateStatus::Rejected
            | TaskResultCandidateStatus::Superseded
            | TaskResultCandidateStatus::Cancelled
    );
    if current_terminal && !expected_terminal {
        // A delayed replay of the creation/extraction event cannot reopen a
        // candidate. Keep the already resolved projection and reconcile its
        // gate idempotently.
        super::native_terminal_effect_outbox::resolve_gate_for_candidate(
            db,
            current.id.as_str(),
            current.thread_id.as_str(),
            current.turn_id.as_str(),
            current.status.as_str(),
            gate_updated_at,
        )
        .await
        .context("failed to reconcile terminal native terminal-effect candidate gate")?;
        return Ok(());
    }
    if current_terminal && expected_terminal && current.status != expected_status {
        let authorized_override = match expected.final_review_event_id.as_deref() {
            Some(review_event_id) if expected.resolved_at.is_some() => {
                task_result_review_event::Entity::find_by_id(review_event_id.to_owned())
                    .one(db)
                    .await
                    .context("failed to validate candidate terminal override review event")?
                    .is_some_and(|review| {
                        review.candidate_id == expected.id
                            && review.supersedes_review_event_id == current.final_review_event_id
                    })
            }
            _ => false,
        };
        if !authorized_override {
            bail!(
                "task result candidate `{}` conflicts with terminal status `{}` without a superseding review event",
                expected.id,
                current.status
            );
        }
    }

    // Candidate identity is immutable, while its extracted result, artifacts,
    // diagnostics and review resolution are an evolving projection. Update
    // only when every identity column matches. This preserves the historical
    // upsert contract without allowing an ID collision to retarget a row.
    let update = task_result_candidate::Entity::update_many()
        .filter(task_result_candidate::Column::Id.eq(expected.id.clone()))
        .filter(task_result_candidate::Column::Status.eq(current.status.clone()))
        .filter(task_result_candidate::Column::UpdatedAt.eq(current.updated_at))
        .filter(task_result_candidate::Column::TaskId.eq(expected.task_id.clone()))
        .filter(task_result_candidate::Column::RunId.eq(expected.run_id.clone()))
        .filter(task_result_candidate::Column::TaskRunTurnId.eq(expected.task_run_turn_id.clone()))
        .filter(task_result_candidate::Column::ThreadId.eq(expected.thread_id.clone()))
        .filter(task_result_candidate::Column::TurnId.eq(expected.turn_id.clone()))
        .filter(task_result_candidate::Column::Round.eq(i64::from(expected.round)))
        .filter(task_result_candidate::Column::CreatedAt.eq(unix_to_datetime(expected.created_at)))
        .col_expr(
            task_result_candidate::Column::Status,
            Expr::value(expected_status),
        )
        .col_expr(
            task_result_candidate::Column::ResultJson,
            Expr::value(expected_result),
        )
        .col_expr(
            task_result_candidate::Column::ExtractionErrorJson,
            Expr::value(expected_extraction_error),
        )
        .col_expr(
            task_result_candidate::Column::Summary,
            Expr::value(expected.summary.clone()),
        )
        .col_expr(
            task_result_candidate::Column::DiagnosticsJson,
            Expr::value(expected_diagnostics),
        )
        .col_expr(
            task_result_candidate::Column::FinalReviewEventId,
            Expr::value(expected.final_review_event_id.clone()),
        )
        .col_expr(
            task_result_candidate::Column::UpdatedAt,
            Expr::value(unix_to_datetime(expected.updated_at)),
        )
        .col_expr(
            task_result_candidate::Column::ResolvedAt,
            Expr::value(expected.resolved_at.map(unix_to_datetime)),
        )
        .exec(db)
        .await
        .context("failed to update task result candidate projection")?;
    if update.rows_affected == 0 {
        bail!(
            "task result candidate `{}` conflicts with a different immutable candidate",
            expected.id
        );
    }
    let persisted = find_candidate_by_id(db, gate_candidate_id.as_str())
        .await?
        .context("task result candidate disappeared after insert")?;
    super::native_terminal_effect_outbox::resolve_gate_for_candidate(
        db,
        persisted.id.as_str(),
        persisted.thread_id.as_str(),
        persisted.turn_id.as_str(),
        persisted.status.as_str(),
        gate_updated_at,
    )
    .await
    .context("failed to resolve native terminal-effect candidate gate")?;
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
    let desired_status = task_result_candidate_status_to_db(status);
    if !matches!(
        status,
        TaskResultCandidateStatus::Accepted
            | TaskResultCandidateStatus::Rejected
            | TaskResultCandidateStatus::Cancelled
            | TaskResultCandidateStatus::Superseded
    ) {
        bail!("task result candidate resolution requires a terminal status");
    }
    if final_review_event_id.is_none() || resolved_at.is_none() {
        bail!("terminal task result candidate resolution requires review and resolution identity");
    }
    let update_result = task_result_candidate::Entity::update_many()
        .filter(task_result_candidate::Column::Id.eq(id.to_owned()))
        .filter(task_result_candidate::Column::Status.is_in([
            task_result_candidate_status_to_db(TaskResultCandidateStatus::PendingReview),
            task_result_candidate_status_to_db(TaskResultCandidateStatus::ExtractionFailed),
        ]))
        .col_expr(
            task_result_candidate::Column::Status,
            Expr::value(desired_status.clone()),
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
        let existing = find_candidate_by_id(db, id).await?;
        if let Some(existing) = existing.as_ref() {
            if existing.status == desired_status
                && existing.final_review_event_id.as_deref() == final_review_event_id
                && existing.resolved_at == resolved_at.map(unix_to_datetime)
            {
                super::native_terminal_effect_outbox::resolve_gate_for_candidate(
                    db,
                    existing.id.as_str(),
                    existing.thread_id.as_str(),
                    existing.turn_id.as_str(),
                    existing.status.as_str(),
                    unix_to_datetime(updated_at),
                )
                .await
                .context("failed to reconcile idempotent native terminal-effect candidate gate")?;
                return Ok(Some(existing.clone()));
            }
            bail!(
                "task result candidate `{id}` cannot transition from `{}` to `{desired_status}`",
                existing.status
            );
        }
        return Ok(None);
    }
    let candidate = find_candidate_by_id(db, id).await?;
    if let Some(candidate) = candidate.as_ref() {
        super::native_terminal_effect_outbox::resolve_gate_for_candidate(
            db,
            candidate.id.as_str(),
            candidate.thread_id.as_str(),
            candidate.turn_id.as_str(),
            candidate.status.as_str(),
            unix_to_datetime(updated_at),
        )
        .await
        .context("failed to resolve updated native terminal-effect candidate gate")?;
    }
    Ok(candidate)
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
        round: Set(i64::from(candidate.round)),
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
