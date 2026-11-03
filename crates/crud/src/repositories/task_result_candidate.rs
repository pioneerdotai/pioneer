use anyhow::{Context, Result, bail};
use pioneer_entity::{task_result_candidate, task_result_review_event};
use pioneer_protocol::{TaskError, TaskResult, TaskResultCandidate, TaskResultCandidateStatus};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    Set,
};

use crate::convention::task_result_candidate_status_to_db;
use crate::util::{optional_typed_json_to_db, typed_json_to_db, unix_to_datetime};

/// The same active candidate used by the authoritative service: highest round
/// among pending_review/extraction_failed, with a result, and both Task and Run
/// still waiting for review. Unique (run_id, round) removes the tie case.
fn review_timeout_query() -> sea_orm::Select<task_result_candidate::Entity> {
    use pioneer_entity::{task, task_run};
    // Literal AST nodes preserve the existing partial UNIQUE index predicate;
    // bound status parameters do not establish its physical input bound.
    let active = || {
        [
            Expr::Constant("pending_review".into()),
            Expr::Constant("extraction_failed".into()),
        ]
    };
    let newer = Alias::new("newer_review_candidate");
    let mut later = task_result_candidate::Entity::find()
        .select_only()
        .column(task_result_candidate::Column::Id);
    QueryTrait::query(&mut later)
        .from_clear()
        .from_as(task_result_candidate::Entity, newer.clone());
    later = later
        .filter(
            Expr::col((newer.clone(), task_result_candidate::Column::RunId)).equals((
                task_result_candidate::Entity,
                task_result_candidate::Column::RunId,
            )),
        )
        .filter(Expr::col((newer.clone(), task_result_candidate::Column::Status)).is_in(active()))
        .filter(
            Expr::col((newer, task_result_candidate::Column::Round)).gt(Expr::col((
                task_result_candidate::Entity,
                task_result_candidate::Column::Round,
            ))),
        );
    task_result_candidate::Entity::find()
        .filter(Expr::col(task_result_candidate::Column::Status).is_in(active()))
        .filter(task_result_candidate::Column::Status.eq("pending_review"))
        .filter(task_result_candidate::Column::ResultJson.is_not_null())
        .filter(Expr::exists(later.into_query()).not())
        .filter(Expr::exists(
            task::Entity::find()
                .select_only()
                .column(task::Column::Id)
                .filter(Expr::col((task::Entity, task::Column::Id)).equals((
                    task_result_candidate::Entity,
                    task_result_candidate::Column::TaskId,
                )))
                .filter(task::Column::Status.eq("waiting_review"))
                .into_query(),
        ))
        .filter(Expr::exists(
            task_run::Entity::find()
                .select_only()
                .column(task_run::Column::Id)
                .filter(Expr::col((task_run::Entity, task_run::Column::Id)).equals((
                    task_result_candidate::Entity,
                    task_result_candidate::Column::RunId,
                )))
                .filter(task_run::Column::Status.eq("waiting_review"))
                .into_query(),
        ))
}

/// Compatibility full observation, with each database hold bounded. The
/// owned worker calls review_runs_page once per quantum and retains its minimum.
pub async fn next_review_timeout<C: ConnectionTrait>(db: &C, timeout: i64) -> Result<Option<i64>> {
    let mut cursor: Option<(String, String)> = None;
    let mut earliest = None;
    loop {
        let (_, next, nearest, failed) = review_runs_page(
            db,
            i64::MIN,
            timeout,
            64,
            cursor.as_ref().map(|(a, _)| a.as_str()),
            cursor.as_ref().map(|(_, u)| u.as_str()),
        )
        .await?;
        anyhow::ensure!(!failed, "review deadline point observation failed");
        earliest = earliest.into_iter().chain(nearest).min();
        cursor = next;
        if cursor.is_none() {
            return Ok(earliest);
        }
    }
}

pub async fn due_review_runs<C: ConnectionTrait>(
    db: &C,
    now: i64,
    timeout: i64,
    limit: u64,
    after: Option<&str>,
    upper: Option<&str>,
) -> Result<(
    Vec<pioneer_entity::task_run::Model>,
    Option<(String, String)>,
)> {
    let (rows, next, _, failed) = review_runs_page(db, now, timeout, limit, after, upper).await?;
    anyhow::ensure!(!failed, "review point observation failed");
    Ok((rows, next))
}

/// Immutable raw Run PKs, including ineligible/future/history inputs. Fixed
/// upper and continuation advance through every raw key, never filtered matches.
pub async fn review_runs_page<C: ConnectionTrait>(
    db: &C,
    now: i64,
    timeout: i64,
    limit: u64,
    after: Option<&str>,
    upper: Option<&str>,
) -> Result<(
    Vec<pioneer_entity::task_run::Model>,
    Option<(String, String)>,
    Option<i64>,
    bool,
)> {
    use pioneer_entity::task_run;
    let limit = limit.clamp(1, 64);
    let upper = match upper {
        Some(u) => Some(u.to_owned()),
        None => {
            task_run::Entity::find()
                .select_only()
                .column(task_run::Column::Id)
                .order_by_desc(task_run::Column::Id)
                .limit(1)
                .into_tuple::<String>()
                .one(db)
                .await?
        }
    };
    let Some(upper) = upper else {
        return Ok((Vec::new(), None, None, false));
    };
    let mut query = task_run::Entity::find()
        .select_only()
        .column(task_run::Column::Id)
        .filter(task_run::Column::Id.lte(upper.clone()));
    if let Some(a) = after {
        query = query.filter(task_run::Column::Id.gt(a));
    }
    let ids = query
        .order_by_asc(task_run::Column::Id)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await?;
    let next = ids
        .last()
        .filter(|id| ids.len() == limit as usize && *id < &upper)
        .map(|id| (id.clone(), upper));
    let mut rows = Vec::new();
    let mut earliest = None;
    let mut failed = false;
    for id in ids {
        // Unique active-per-run index bounds active rows, including the newer
        // round exclusion. Task and Run eligibility use correlated PK lookups.
        let observation = review_timeout_query()
            .select_only()
            .column(task_result_candidate::Column::CreatedAt)
            .filter(task_result_candidate::Column::RunId.eq(&id))
            .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
            .one(db)
            .await;
        match observation {
            Ok(Some(created)) => {
                let deadline = created.timestamp().saturating_add(timeout);
                if deadline > now {
                    earliest = earliest.into_iter().chain(Some(deadline)).min();
                    continue;
                }
                match task_run::Entity::find_by_id(&id).one(db).await {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(_) => failed = true,
                }
            }
            Ok(None) => {}
            Err(_) => failed = true,
        }
    }
    Ok((rows, next, earliest, failed))
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub(crate) struct PreparedTaskResultCandidate {
    expected: NewTaskResultCandidate,
    active_model: task_result_candidate::ActiveModel,
    expected_status: String,
    expected_result_json: Option<String>,
    expected_extraction_error_json: Option<String>,
    expected_diagnostics_json: Option<String>,
    expected_created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    expected_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    expected_resolved_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
}

impl PreparedTaskResultCandidate {
    pub(crate) fn expected(&self) -> &NewTaskResultCandidate {
        &self.expected
    }
}

pub(crate) fn prepare_candidate(
    candidate: NewTaskResultCandidate,
) -> Result<PreparedTaskResultCandidate> {
    let expected_status = task_result_candidate_status_to_db(candidate.status).to_owned();
    let expected_result_json = optional_typed_json_to_db(&candidate.result)?;
    let expected_extraction_error_json = optional_typed_json_to_db(&candidate.extraction_error)?;
    let expected_diagnostics_json = Some(typed_json_to_db(&candidate.diagnostics)?);
    let expected_created_at = unix_to_datetime(candidate.created_at);
    let expected_updated_at = unix_to_datetime(candidate.updated_at);
    let expected_resolved_at = candidate.resolved_at.map(unix_to_datetime);
    let active_model = task_result_candidate::ActiveModel {
        id: Set(candidate.id.clone()),
        task_id: Set(candidate.task_id.clone()),
        run_id: Set(candidate.run_id.clone()),
        task_run_turn_id: Set(candidate.task_run_turn_id.clone()),
        thread_id: Set(candidate.thread_id.clone()),
        turn_id: Set(candidate.turn_id.clone()),
        round: Set(i64::from(candidate.round)),
        status: Set(expected_status.clone()),
        result_json: Set(expected_result_json.clone()),
        extraction_error_json: Set(expected_extraction_error_json.clone()),
        summary: Set(candidate.summary.clone()),
        diagnostics_json: Set(expected_diagnostics_json.clone()),
        final_review_event_id: Set(candidate.final_review_event_id.clone()),
        created_at: Set(expected_created_at),
        updated_at: Set(expected_updated_at),
        resolved_at: Set(expected_resolved_at),
    };
    Ok(PreparedTaskResultCandidate {
        expected: candidate,
        active_model,
        expected_status,
        expected_result_json,
        expected_extraction_error_json,
        expected_diagnostics_json,
        expected_created_at,
        expected_updated_at,
        expected_resolved_at,
    })
}

pub(crate) fn prepare_protocol_candidate(
    candidate: &TaskResultCandidate,
) -> Result<PreparedTaskResultCandidate> {
    prepare_candidate(NewTaskResultCandidate {
        id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        task_run_turn_id: candidate.task_run_turn_id.clone(),
        thread_id: candidate.thread_id.clone(),
        turn_id: candidate.turn_id.clone(),
        round: candidate.round,
        status: candidate.status,
        result: candidate.result.clone(),
        extraction_error: candidate.extraction_error.clone(),
        summary: candidate.summary.clone(),
        diagnostics: candidate.diagnostics.clone(),
        final_review_event_id: candidate.final_review_event_id.clone(),
        created_at: candidate.created_at,
        updated_at: candidate.updated_at,
        resolved_at: candidate.resolved_at,
    })
}

pub async fn upsert_candidate<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskResultCandidate,
) -> Result<()> {
    let PreparedTaskResultCandidate {
        expected,
        active_model,
        expected_status,
        expected_result_json,
        expected_extraction_error_json,
        expected_diagnostics_json,
        expected_created_at,
        expected_updated_at,
        expected_resolved_at,
    } = prepared;
    let candidate_id = expected.id.clone();
    task_result_candidate::Entity::insert(active_model)
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

    let current = find_candidate_by_id(db, candidate_id.as_str())
        .await?
        .context("task result candidate disappeared after insert")?;
    if current.task_id != expected.task_id
        || current.run_id != expected.run_id
        || current.task_run_turn_id != expected.task_run_turn_id
        || current.thread_id != expected.thread_id
        || current.turn_id != expected.turn_id
        || current.round != i64::from(expected.round)
        || current.created_at != expected_created_at
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
        // candidate. Its caller owns the separately prepared acceptance-gate
        // plan and applies it in the same transaction when required.
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
        .filter(task_result_candidate::Column::CreatedAt.eq(expected_created_at))
        .col_expr(
            task_result_candidate::Column::Status,
            Expr::value(expected_status),
        )
        .col_expr(
            task_result_candidate::Column::ResultJson,
            Expr::value(expected_result_json),
        )
        .col_expr(
            task_result_candidate::Column::ExtractionErrorJson,
            Expr::value(expected_extraction_error_json),
        )
        .col_expr(
            task_result_candidate::Column::Summary,
            Expr::value(expected.summary.clone()),
        )
        .col_expr(
            task_result_candidate::Column::DiagnosticsJson,
            Expr::value(expected_diagnostics_json),
        )
        .col_expr(
            task_result_candidate::Column::FinalReviewEventId,
            Expr::value(expected.final_review_event_id.clone()),
        )
        .col_expr(
            task_result_candidate::Column::UpdatedAt,
            Expr::value(expected_updated_at),
        )
        .col_expr(
            task_result_candidate::Column::ResolvedAt,
            Expr::value(expected_resolved_at),
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
    Ok(())
}

/// Mirrors the legacy projector's creation eligibility, without loading JSON.
/// This is not a terminal-effect gate predicate.
pub(crate) async fn has_accepted_candidate_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<bool> {
    Ok(task_result_candidate::Entity::find()
        .select_only()
        .column(task_result_candidate::Column::Id)
        .filter(task_result_candidate::Column::RunId.eq(run_id.to_owned()))
        .filter(Expr::cust("status = 'accepted'"))
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
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
    Ok(candidate)
}

#[derive(Clone, Debug, PartialEq, Eq, sea_orm::FromQueryResult)]
pub(crate) struct TaskDeliveryReviewBinding {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub final_review_event_id: Option<String>,
}

pub(crate) async fn find_delivery_review_binding<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<TaskDeliveryReviewBinding>> {
    task_result_candidate::Entity::find()
        .select_only()
        .columns([
            task_result_candidate::Column::Id,
            task_result_candidate::Column::TaskId,
            task_result_candidate::Column::RunId,
            task_result_candidate::Column::FinalReviewEventId,
        ])
        .filter(task_result_candidate::Column::RunId.eq(run_id.to_owned()))
        .filter(task_result_candidate::Column::Status.eq("accepted"))
        .order_by_desc(task_result_candidate::Column::Round)
        .order_by_desc(task_result_candidate::Column::CreatedAt)
        .into_model::<TaskDeliveryReviewBinding>()
        .one(db)
        .await
        .context("failed to read exact delivery final review binding")
}
