use anyhow::{Context, Result, ensure};
use pioneer_entity::task_result_review_event;
use pioneer_protocol::{
    TaskResultReviewDecision, TaskResultReviewEventKind, TaskResultReviewerKind,
    TaskResultReviewerRef, TaskValue,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::{
    task_result_review_decision_to_db, task_result_review_event_kind_to_db,
    task_result_reviewer_kind_to_db,
};
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

#[derive(Clone)]
pub struct NewTaskResultReviewEvent {
    pub id: String,
    pub candidate_id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub reviewer_kind: TaskResultReviewerKind,
    pub reviewer: TaskResultReviewerRef,
    pub reviewer_thread_id: Option<String>,
    pub reviewer_turn_id: Option<String>,
    pub reviewer_user_id: Option<String>,
    pub reviewer_agent_spec_id: Option<String>,
    pub event_kind: TaskResultReviewEventKind,
    pub decision: TaskResultReviewDecision,
    pub feedback_text: Option<String>,
    pub feedback: Option<TaskValue>,
    pub confidence: Option<f64>,
    pub supersedes_review_event_id: Option<String>,
    pub next_task_run_turn_id: Option<String>,
    pub created_at: i64,
}

pub async fn upsert_review_event<C: ConnectionTrait>(
    db: &C,
    event: NewTaskResultReviewEvent,
) -> Result<()> {
    task_result_review_event::Entity::insert(active_model_from_new_review_event(event.clone())?)
        .on_conflict(
            OnConflict::column(task_result_review_event::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to upsert task result review event")?;
    let persisted = find_review_event_by_id(db, event.id.as_str())
        .await?
        .context("task result review event missing after insert")?;
    ensure_review_event_is_exact(&persisted, &event)?;
    Ok(())
}

fn ensure_review_event_is_exact(
    persisted: &task_result_review_event::Model,
    expected: &NewTaskResultReviewEvent,
) -> Result<()> {
    let reviewer_ref_json = serde_json::to_string(&expected.reviewer)?;
    let feedback_json = optional_typed_json_to_db(&expected.feedback)?;
    ensure!(
        persisted.candidate_id == expected.candidate_id
            && persisted.task_id == expected.task_id
            && persisted.run_id == expected.run_id
            && persisted.task_run_turn_id == expected.task_run_turn_id
            && persisted.reviewer_kind == task_result_reviewer_kind_to_db(expected.reviewer_kind)
            && persisted.reviewer_ref_json == reviewer_ref_json
            && persisted.reviewer_thread_id == expected.reviewer_thread_id
            && persisted.reviewer_turn_id == expected.reviewer_turn_id
            && persisted.reviewer_user_id == expected.reviewer_user_id
            && persisted.reviewer_agent_spec_id == expected.reviewer_agent_spec_id
            && persisted.event_kind == task_result_review_event_kind_to_db(expected.event_kind)
            && persisted.decision == task_result_review_decision_to_db(expected.decision)
            && persisted.feedback_text == expected.feedback_text
            && persisted.feedback_json == feedback_json
            && persisted.confidence == expected.confidence
            && persisted.supersedes_review_event_id == expected.supersedes_review_event_id
            && persisted.next_task_run_turn_id == expected.next_task_run_turn_id
            && persisted.created_at.timestamp() == expected.created_at,
        "task result review event {} already exists with different immutable facts",
        expected.id
    );
    Ok(())
}

pub async fn find_review_event_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<task_result_review_event::Model>> {
    task_result_review_event::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query task result review event by id")
}

pub async fn list_review_events_by_candidate<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
) -> Result<Vec<task_result_review_event::Model>> {
    task_result_review_event::Entity::find()
        .filter(task_result_review_event::Column::CandidateId.eq(candidate_id.to_owned()))
        .order_by_asc(task_result_review_event::Column::CreatedAt)
        .order_by_asc(task_result_review_event::Column::Id)
        .all(db)
        .await
        .context("failed to list task result review events by candidate")
}

pub async fn list_review_events_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_result_review_event::Model>> {
    task_result_review_event::Entity::find()
        .filter(task_result_review_event::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_result_review_event::Column::CreatedAt)
        .order_by_asc(task_result_review_event::Column::Id)
        .all(db)
        .await
        .context("failed to list task result review events by run")
}

fn active_model_from_new_review_event(
    event: NewTaskResultReviewEvent,
) -> Result<task_result_review_event::ActiveModel> {
    Ok(task_result_review_event::ActiveModel {
        id: Set(event.id),
        candidate_id: Set(event.candidate_id),
        task_id: Set(event.task_id),
        run_id: Set(event.run_id),
        task_run_turn_id: Set(event.task_run_turn_id),
        reviewer_kind: Set(task_result_reviewer_kind_to_db(event.reviewer_kind)),
        reviewer_ref_json: Set(serde_json::to_string(&event.reviewer)?),
        reviewer_thread_id: Set(event.reviewer_thread_id),
        reviewer_turn_id: Set(event.reviewer_turn_id),
        reviewer_user_id: Set(event.reviewer_user_id),
        reviewer_agent_spec_id: Set(event.reviewer_agent_spec_id),
        event_kind: Set(task_result_review_event_kind_to_db(event.event_kind)),
        decision: Set(task_result_review_decision_to_db(event.decision)),
        feedback_text: Set(event.feedback_text),
        feedback_json: Set(optional_typed_json_to_db(&event.feedback)?),
        confidence: Set(event.confidence),
        supersedes_review_event_id: Set(event.supersedes_review_event_id),
        next_task_run_turn_id: Set(event.next_task_run_turn_id),
        created_at: Set(unix_to_datetime(event.created_at)),
    })
}
