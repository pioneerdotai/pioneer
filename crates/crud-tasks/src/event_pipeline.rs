//! Preparation and application of an atomic task-event batch.
use crate::event_records::DeferredAppendedTaskEvent;
use crate::task as task_repository;
use crate::task_projector::{TaskOutputBinder, TaskProjector};
use crate::values::unix_to_datetime;
use crate::{task_event, task_result_candidate, task_result_review_event, task_run_turn};
use anyhow::{Context, Result, bail};
use pioneer_crud_execution::{execution_admission_lease, native_terminal_effect_outbox};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{
    TaskEventPayload, TaskResultCandidateStatus, TaskResultReviewDecision,
    TaskResultReviewEventKind, TaskResultReviewerKind, TaskTriggerStatus,
};
use sea_orm::ConnectionTrait;
use std::collections::HashMap;

pub const MAX_ATOMIC_TASK_EVENT_BATCH_SIZE: usize = 64;

#[derive(Clone, Debug)]
struct PreparedLegacyTaskRunTurn {
    id: String,
    run_id: String,
    thread_id: String,
    turn_id: String,
    round: i64,
    sequence: i64,
    created_at: i64,
}

impl PreparedLegacyTaskRunTurn {
    fn from_model(model: pioneer_entity::task_run_turn::Model) -> Self {
        Self {
            id: model.id,
            run_id: model.run_id,
            thread_id: model.thread_id,
            turn_id: model.turn_id,
            round: model.round,
            sequence: model.sequence,
            created_at: model.created_at.timestamp(),
        }
    }

    const fn order_key(&self) -> (i64, i64) {
        (self.sequence, self.created_at)
    }
}

fn prepare_legacy_task_result(
    task_id: &str,
    run_id: &str,
    task_run_turn: &PreparedLegacyTaskRunTurn,
    result: &pioneer_protocol::TaskResult,
    completed_at: i64,
) -> Result<(
    task_result_candidate::PreparedTaskResultCandidate,
    task_result_review_event::PreparedTaskResultReviewEvent,
)> {
    let candidate_id = format!("trc_{run_id}");
    let review_event_id = format!("trre_auto_{run_id}");
    let candidate =
        task_result_candidate::prepare_candidate(task_result_candidate::NewTaskResultCandidate {
            id: candidate_id.clone(),
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            task_run_turn_id: task_run_turn.id.clone(),
            thread_id: task_run_turn.thread_id.clone(),
            turn_id: task_run_turn.turn_id.clone(),
            round: u32::try_from(task_run_turn.round)
                .context("legacy task run turn round is out of range")?,
            status: TaskResultCandidateStatus::Accepted,
            result: Some(result.clone()),
            extraction_error: None,
            summary: result.summary.clone(),
            diagnostics: Vec::new(),
            final_review_event_id: Some(review_event_id.clone()),
            created_at: completed_at,
            updated_at: completed_at,
            resolved_at: Some(completed_at),
        })?;
    let review = task_result_review_event::prepare_review_event(
        task_result_review_event::NewTaskResultReviewEvent {
            id: review_event_id,
            candidate_id: candidate_id.clone(),
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            task_run_turn_id: task_run_turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: completed_at,
        },
    )?;
    Ok((candidate, review))
}

fn projected_legacy_task_run_turn(event: &TaskEventPayload) -> Option<PreparedLegacyTaskRunTurn> {
    match event {
        TaskEventPayload::TaskRunTurnStarted { task_run_turn }
        | TaskEventPayload::TaskRunTurnCompleted { task_run_turn }
        | TaskEventPayload::TaskRunTurnFailed { task_run_turn, .. }
        | TaskEventPayload::TaskRunTurnBlocked { task_run_turn, .. } => {
            Some(PreparedLegacyTaskRunTurn {
                id: task_run_turn.id.clone(),
                run_id: task_run_turn.run_id.clone(),
                thread_id: task_run_turn.thread_id.clone(),
                turn_id: task_run_turn.turn_id.clone(),
                round: i64::from(task_run_turn.round),
                sequence: i64::from(task_run_turn.sequence),
                created_at: task_run_turn.created_at,
            })
        }
        TaskEventPayload::ChildThreadLinked { lineage } => Some(PreparedLegacyTaskRunTurn {
            id: format!("trt_{}", lineage.child_turn_id),
            run_id: lineage.task_run_id.clone(),
            thread_id: lineage.child_thread_id.clone(),
            turn_id: lineage.child_turn_id.clone(),
            round: 0,
            sequence: 0,
            created_at: lineage.created_at,
        }),
        TaskEventPayload::TaskRevisionRequested {
            run_id,
            task_run_turn_id,
            thread_id,
            turn_id,
            round,
            requested_at,
            ..
        } => Some(PreparedLegacyTaskRunTurn {
            id: task_run_turn_id.clone(),
            run_id: run_id.clone(),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            round: i64::from(*round),
            sequence: i64::from(*round),
            created_at: *requested_at,
        }),
        _ => None,
    }
}

pub async fn prepare_task_events_for_write(
    db: &PostgresDatabase,
    events: Vec<TaskEventPayload>,
) -> Result<Vec<task_event::PreparedTaskEvent>> {
    if events.len() > MAX_ATOMIC_TASK_EVENT_BATCH_SIZE {
        anyhow::bail!("atomic Task event batch exceeds {MAX_ATOMIC_TASK_EVENT_BATCH_SIZE} events");
    }
    let mut prepared = Vec::with_capacity(events.len());
    let mut batch_turns = HashMap::<String, PreparedLegacyTaskRunTurn>::new();
    for payload in events {
        let mut legacy_candidate = None;
        let mut legacy_review = None;
        if let Some(turn) = projected_legacy_task_run_turn(&payload) {
            if batch_turns
                .get(&turn.run_id)
                .is_none_or(|current| turn.order_key() >= current.order_key())
            {
                batch_turns.insert(turn.run_id.clone(), turn);
            }
        }
        let target = match &payload {
            TaskEventPayload::TaskResultCandidateCreated { candidate }
            | TaskEventPayload::TaskResultCandidateAccepted { candidate, .. }
            | TaskEventPayload::TaskResultCandidateRejected { candidate, .. }
            | TaskEventPayload::TaskResultCandidateCancelled { candidate, .. } => {
                Some((candidate.thread_id.clone(), candidate.turn_id.clone()))
            }
            TaskEventPayload::RunCompleted {
                task_id,
                run_id,
                result: Some(result),
                completed_at,
                ..
            } => {
                let persisted = task_run_turn::find_latest_turn_by_run(db, run_id)
                    .await?
                    .map(PreparedLegacyTaskRunTurn::from_model);
                let latest = match (persisted, batch_turns.get(run_id)) {
                    (Some(persisted), Some(batch))
                        if batch.order_key() >= persisted.order_key() =>
                    {
                        Some(batch.clone())
                    }
                    (Some(persisted), _) => Some(persisted),
                    (None, batch) => batch.cloned(),
                };
                if let Some(turn) = latest {
                    let (candidate, review) =
                        prepare_legacy_task_result(task_id, run_id, &turn, result, *completed_at)?;
                    legacy_candidate = Some(candidate);
                    legacy_review = Some(review);
                    Some((turn.thread_id, turn.turn_id))
                } else {
                    None
                }
            }
            _ => None,
        };
        let mut event = task_event::PreparedTaskEvent::prepare(payload)?;
        if let Some(candidate) = legacy_candidate {
            event = event.with_candidate_projection(Some(candidate));
        }
        if let Some(review) = legacy_review {
            event = event.with_review_projection(Some(review));
        }
        if let Some((thread_id, turn_id)) = target {
            event.gate_payloads = Some(
                native_terminal_effect_outbox::prepare_gate_payloads(db, &thread_id, &turn_id)
                    .await?,
            );
        }
        prepared.push(event.preflight_idempotency(db).await?);
    }
    task_event::prepare_batch_idempotency(&mut prepared);
    Ok(prepared)
}
pub async fn refresh_task_event_preflight(
    db: &PostgresDatabase,
    events: Vec<task_event::PreparedTaskEvent>,
) -> Result<Vec<task_event::PreparedTaskEvent>> {
    let mut refreshed = Vec::with_capacity(events.len());
    for event in events {
        refreshed.push(event.preflight_idempotency(db).await?);
    }
    task_event::prepare_batch_idempotency(&mut refreshed);
    Ok(refreshed)
}
async fn prepare_candidate_writes_for_task_event<C: ConnectionTrait>(
    db: &C,
    event: &TaskEventPayload,
    event_timestamp_secs: i64,
    batch_run_turn: Option<&PreparedLegacyTaskRunTurn>,
    payloads: Option<&native_terminal_effect_outbox::PreparedGatePayloads>,
    legacy_candidate: Option<&task_result_candidate::PreparedTaskResultCandidate>,
) -> Result<Option<native_terminal_effect_outbox::PreparedCandidateGateResolution>> {
    let mut replace_candidate = true;
    let target = match event {
        TaskEventPayload::TaskResultCandidateCreated { candidate } => Some((
            candidate.id.clone(),
            candidate.thread_id.clone(),
            candidate.turn_id.clone(),
            crate::status::task_result_candidate_status_to_db(candidate.status).to_owned(),
            candidate.updated_at,
            candidate.updated_at,
        )),
        TaskEventPayload::TaskResultCandidateAccepted { candidate, .. } => Some((
            candidate.id.clone(),
            candidate.thread_id.clone(),
            candidate.turn_id.clone(),
            "accepted".to_owned(),
            candidate
                .updated_at
                .max(candidate.resolved_at.unwrap_or(event_timestamp_secs)),
            if candidate.status == TaskResultCandidateStatus::Accepted {
                candidate.updated_at
            } else {
                candidate
                    .updated_at
                    .max(candidate.resolved_at.unwrap_or(event_timestamp_secs))
            },
        )),
        TaskEventPayload::TaskResultCandidateRejected { candidate, .. } => Some((
            candidate.id.clone(),
            candidate.thread_id.clone(),
            candidate.turn_id.clone(),
            "rejected".to_owned(),
            candidate
                .updated_at
                .max(candidate.resolved_at.unwrap_or(event_timestamp_secs)),
            if candidate.status == TaskResultCandidateStatus::Rejected {
                candidate.updated_at
            } else {
                candidate
                    .updated_at
                    .max(candidate.resolved_at.unwrap_or(event_timestamp_secs))
            },
        )),
        TaskEventPayload::TaskResultCandidateCancelled { candidate, .. } => Some((
            candidate.id.clone(),
            candidate.thread_id.clone(),
            candidate.turn_id.clone(),
            "cancelled".to_owned(),
            candidate
                .updated_at
                .max(candidate.resolved_at.unwrap_or(event_timestamp_secs)),
            if candidate.status == TaskResultCandidateStatus::Cancelled {
                candidate.updated_at
            } else {
                candidate
                    .updated_at
                    .max(candidate.resolved_at.unwrap_or(event_timestamp_secs))
            },
        )),
        TaskEventPayload::RunCompleted {
            run_id,
            result: Some(_),
            completed_at,
            ..
        } => {
            let persisted = task_run_turn::find_latest_turn_by_run(db, run_id)
                .await?
                .map(PreparedLegacyTaskRunTurn::from_model);
            let task_run_turn = match (persisted, batch_run_turn.cloned()) {
                (Some(persisted), Some(batch)) => {
                    if batch.order_key() >= persisted.order_key() {
                        batch
                    } else {
                        persisted
                    }
                }
                (Some(persisted), None) => persisted,
                (None, Some(batch)) => batch,
                (None, None) => {
                    if legacy_candidate.is_some() {
                        bail!("legacy candidate parent disappeared after preparation");
                    }
                    return Ok(None);
                }
            };
            if task_run_turn.run_id != *run_id {
                if legacy_candidate.is_some() {
                    bail!("legacy candidate parent disappeared after preparation");
                }
                return Ok(None);
            }
            let prepared = legacy_candidate
                .context("legacy candidate was not prepared before writer admission")?;
            let expected = prepared.expected();
            if expected.task_run_turn_id != task_run_turn.id
                || expected.thread_id != task_run_turn.thread_id
                || expected.turn_id != task_run_turn.turn_id
                || i64::from(expected.round) != task_run_turn.round
            {
                bail!("legacy candidate parent changed after preparation");
            }
            // The existing projector skips synthetic creation if this run
            // already has an accepted candidate. This predicts writes only;
            // the gate itself always chooses latest terminal metadata.
            replace_candidate =
                !task_result_candidate::has_accepted_candidate_by_run(db, run_id).await?;
            Some((
                expected.id.clone(),
                task_run_turn.thread_id,
                task_run_turn.turn_id,
                "accepted".to_owned(),
                *completed_at,
                expected.updated_at,
            ))
        }
        _ => None,
    };
    let Some((candidate_id, thread_id, turn_id, status, resolved_at, candidate_updated_at)) =
        target
    else {
        return Ok(None);
    };
    let gate_resolution =
        native_terminal_effect_outbox::prepare_gate_resolution_for_candidate(
            db,
            replace_candidate.then_some(native_terminal_effect_outbox::CandidateGateMetadata {
                id: candidate_id,
                status,
                updated_at: unix_to_datetime(candidate_updated_at),
            }),
            thread_id.as_str(),
            turn_id.as_str(),
            unix_to_datetime(resolved_at),
            Some(payloads.context(
                "Task candidate gate payloads were not prepared before writer admission",
            )?),
        )
        .await?;
    Ok(Some(gate_resolution))
}
pub fn append_task_events_in_connection_with_delivery_policy<'a, C: ConnectionTrait + Sync>(
    db: &'a C,
    events: Vec<task_event::PreparedTaskEvent>,
    event_timestamp_secs: i64,
    delivery_cancellation: bool,
    task_outputs: &'a impl TaskOutputBinder,
) -> futures_util::future::BoxFuture<'a, Result<Vec<DeferredAppendedTaskEvent>>> {
    Box::pin(async move {
        let created_at = unix_to_datetime(event_timestamp_secs);
        let mut appended_events = Vec::with_capacity(events.len());
        let mut batch_run_turns = HashMap::<String, PreparedLegacyTaskRunTurn>::new();
        let mut batch_identities = task_event::TaskEventBatchIdentities::new();

        // Lock all participating streams in canonical ID order. Relational
        // admission and projection then share the sequence/idempotency fence.
        let task_ids = events
            .iter()
            .map(|event| event.payload().task_id())
            .collect::<std::collections::BTreeSet<_>>();
        for task_id in task_ids {
            crate::event_log::lock_task(db, task_id).await?;
        }

        for event in events {
            let mut event = if delivery_cancellation {
                let Some(event) =
                    crate::delivery_event_checks::preflight_cancellation(db, event).await?
                else {
                    continue;
                };
                event
            } else {
                event
            };
            let needs_open_task = match event.payload() {
                TaskEventPayload::RunCreated { .. }
                | TaskEventPayload::RunRetryScheduled { .. }
                | TaskEventPayload::TaskQueued { .. }
                | TaskEventPayload::TriggerCreated { .. }
                | TaskEventPayload::TaskUpdated { .. }
                | TaskEventPayload::TaskResumed { .. }
                | TaskEventPayload::TaskPaused { .. } => true,
                TaskEventPayload::TaskDetached { task, .. } => task.finish_request().is_none(),
                TaskEventPayload::TaskRescheduled { trigger, .. } => {
                    trigger.status != TaskTriggerStatus::Cancelled
                }
                _ => false,
            };
            if needs_open_task
                && task_repository::finish_requested(db, event.payload().task_id()).await?
            {
                anyhow::bail!("task_finish_requested: task cannot admit new work or be reopened");
            }
            // Only state-independent work (validation, serialization and CPU
            // projection preparation) is performed before writer admission.
            // Database-dependent preparation is deliberately sequential here:
            // event N must observe the projection committed by event N-1 in
            // this same atomic batch.
            if let Some(turn) = projected_legacy_task_run_turn(event.payload()) {
                let replace = batch_run_turns
                    .get(turn.run_id.as_str())
                    .is_none_or(|current| turn.order_key() >= current.order_key());
                if replace {
                    batch_run_turns.insert(turn.run_id.clone(), turn);
                }
            }
            let delivery_authority = if let Some(authority) = event.take_delivery_authority() {
                Some(authority)
            } else {
                match event.payload() {
                    TaskEventPayload::DeliveryQueued { delivery }
                    | TaskEventPayload::DeliveryStarted { delivery, .. }
                    | TaskEventPayload::DeliveryDelivered { delivery, .. }
                    | TaskEventPayload::DeliveryFailed { delivery, .. }
                    | TaskEventPayload::DeliveryCancelled { delivery, .. } => Some(
                        crate::task_projector::prepare_task_delivery_authority(db, delivery)
                            .await?,
                    ),
                    _ => None,
                }
            };
            let gate_resolution = prepare_candidate_writes_for_task_event(
                db,
                event.payload(),
                event_timestamp_secs,
                event
                    .payload()
                    .run_id()
                    .and_then(|run_id| batch_run_turns.get(run_id)),
                event.gate_payloads.as_ref(),
                event.candidate_projection(),
            )
            .await?;
            let event = event
                .with_candidate_gate_resolution(gate_resolution)
                .with_delivery_authority(delivery_authority);
            let mut appended_event = task_event::append_prepared_event_with_batch(
                db,
                event,
                created_at,
                &mut batch_identities,
            )
            .await?;

            if appended_event.append_status.is_inserted() {
                TaskProjector::new()
                    .project(db, &appended_event, task_outputs)
                    .await
                    .context("failed to project task event to read models")?;
                if matches!(
                    appended_event.payload.as_ref(),
                    TaskEventPayload::TaskCompleted { .. }
                        | TaskEventPayload::TaskFailed { .. }
                        | TaskEventPayload::TaskBlocked { .. }
                        | TaskEventPayload::TaskCancelled { .. }
                ) {
                    execution_admission_lease::release_by_subject(
                        db,
                        "task",
                        appended_event.task_id.as_str(),
                        created_at,
                    )
                    .await?;
                }
                // The pending floor selects new work. A zero cursor also keeps
                // an earlier tracked event safe after cursor deletion in a batch.
                task_event::initialize_fanout_cursor(
                    db,
                    appended_event.task_id.as_str(),
                    0,
                    created_at,
                )
                .await
                .context("failed to initialize task event fanout cursor")?;
            }

            (
                appended_event.workspace_id,
                appended_event.root_task_id,
                appended_event.parent_task_id,
            ) = load_task_event_metadata(db, &appended_event.task_id)
                .await
                .context("failed to hydrate task event metadata")?;
            appended_events.push(appended_event);
        }

        Ok(appended_events)
    })
}
pub async fn load_task_event_metadata<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    Ok(match task_repository::find_task_by_id(db, task_id).await? {
        Some(task) => (
            Some(task.workspace_id),
            task.root_task_id,
            task.parent_task_id,
        ),
        None => (None, None, None),
    })
}
