use anyhow::{Context, Result, ensure};
use pioneer_entity::task_result_review_event;
use pioneer_protocol::{
    TaskResultReviewDecision, TaskResultReviewEvent, TaskResultReviewEventKind,
    TaskResultReviewerKind, TaskResultReviewerRef, TaskValue,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::{
    task_result_review_decision_to_db, task_result_review_event_kind_to_db,
    task_result_reviewer_kind_to_db,
};
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub(crate) struct PreparedTaskResultReviewEvent {
    expected: NewTaskResultReviewEvent,
    active_model: task_result_review_event::ActiveModel,
    reviewer_ref_json: String,
    feedback_json: Option<String>,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub(crate) fn prepare_review_event(
    event: NewTaskResultReviewEvent,
) -> Result<PreparedTaskResultReviewEvent> {
    let reviewer_ref_json = serde_json::to_string(&event.reviewer)?;
    let feedback_json = optional_typed_json_to_db(&event.feedback)?;
    let created_at = unix_to_datetime(event.created_at);
    let active_model = task_result_review_event::ActiveModel {
        id: Set(event.id.clone()),
        candidate_id: Set(event.candidate_id.clone()),
        task_id: Set(event.task_id.clone()),
        run_id: Set(event.run_id.clone()),
        task_run_turn_id: Set(event.task_run_turn_id.clone()),
        reviewer_kind: Set(task_result_reviewer_kind_to_db(event.reviewer_kind)),
        reviewer_ref_json: Set(reviewer_ref_json.clone()),
        reviewer_thread_id: Set(event.reviewer_thread_id.clone()),
        reviewer_turn_id: Set(event.reviewer_turn_id.clone()),
        reviewer_user_id: Set(event.reviewer_user_id.clone()),
        reviewer_agent_spec_id: Set(event.reviewer_agent_spec_id.clone()),
        event_kind: Set(task_result_review_event_kind_to_db(event.event_kind)),
        decision: Set(task_result_review_decision_to_db(event.decision)),
        feedback_text: Set(event.feedback_text.clone()),
        feedback_json: Set(feedback_json.clone()),
        confidence: Set(event.confidence),
        supersedes_review_event_id: Set(event.supersedes_review_event_id.clone()),
        next_task_run_turn_id: Set(event.next_task_run_turn_id.clone()),
        created_at: Set(created_at),
    };
    Ok(PreparedTaskResultReviewEvent {
        expected: event,
        active_model,
        reviewer_ref_json,
        feedback_json,
        created_at,
    })
}

pub(crate) fn prepare_protocol_review_event(
    event: &TaskResultReviewEvent,
) -> Result<PreparedTaskResultReviewEvent> {
    prepare_review_event(NewTaskResultReviewEvent {
        id: event.id.clone(),
        candidate_id: event.candidate_id.clone(),
        task_id: event.task_id.clone(),
        run_id: event.run_id.clone(),
        task_run_turn_id: event.task_run_turn_id.clone(),
        reviewer_kind: event.reviewer_kind,
        reviewer: event.reviewer.clone(),
        reviewer_thread_id: event.reviewer_thread_id.clone(),
        reviewer_turn_id: event.reviewer_turn_id.clone(),
        reviewer_user_id: event.reviewer_user_id.clone(),
        reviewer_agent_spec_id: event.reviewer_agent_spec_id.clone(),
        event_kind: event.event_kind,
        decision: event.decision,
        feedback_text: event.feedback_text.clone(),
        feedback: event.feedback.clone(),
        confidence: event.confidence,
        supersedes_review_event_id: event.supersedes_review_event_id.clone(),
        next_task_run_turn_id: event.next_task_run_turn_id.clone(),
        created_at: event.created_at,
    })
}

pub async fn upsert_review_event<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskResultReviewEvent,
) -> Result<()> {
    let PreparedTaskResultReviewEvent {
        expected,
        active_model,
        reviewer_ref_json,
        feedback_json,
        created_at,
    } = prepared;
    task_result_review_event::Entity::insert(active_model)
        .on_conflict(
            OnConflict::column(task_result_review_event::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to upsert task result review event")?;
    let persisted = find_review_event_by_id(db, expected.id.as_str())
        .await?
        .context("task result review event missing after insert")?;
    ensure_review_event_is_exact(
        &persisted,
        &expected,
        reviewer_ref_json.as_str(),
        feedback_json.as_deref(),
        created_at,
    )?;
    Ok(())
}

fn ensure_review_event_is_exact(
    persisted: &task_result_review_event::Model,
    expected: &NewTaskResultReviewEvent,
    reviewer_ref_json: &str,
    feedback_json: Option<&str>,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<()> {
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
            && persisted.feedback_json.as_deref() == feedback_json
            && persisted.confidence == expected.confidence
            && persisted.supersedes_review_event_id == expected.supersedes_review_event_id
            && persisted.next_task_run_turn_id == expected.next_task_run_turn_id
            && persisted.created_at == created_at,
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
