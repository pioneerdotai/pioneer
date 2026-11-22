#![allow(dead_code)]

use crate::event_log::{self, EventPayload};
use anyhow::{Context, Result};
use pioneer_entity::task_event;
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use crate::event_records::{AppendedTaskEvent, DeferredAppendedTaskEvent, TaskEventAppendStatus};
use pioneer_protocol::TaskEventPayload;

const DB_ID_LEN: usize = 21;

#[derive(Clone, Debug)]
pub struct PreparedTaskEvent {
    id: String,
    task_id: String,
    run_id: Option<String>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    event_type: String,
    idempotency_key: Option<String>,
    encoded_payload: EventPayload,
    payload: TaskEventPayload,
    semantically_matching_existing: Option<SemanticallyMatchingExistingTaskEvent>,
    batch_existing: Vec<PreparedBatchExisting>,
    pub gate_payloads:
        Option<pioneer_crud_execution::native_terminal_effect_outbox::PreparedGatePayloads>,
    candidate_gate_resolution: Option<
        pioneer_crud_execution::native_terminal_effect_outbox::PreparedCandidateGateResolution,
    >,
    candidate_projection: Option<super::task_result_candidate::PreparedTaskResultCandidate>,
    review_projection: Option<super::task_result_review_event::PreparedTaskResultReviewEvent>,
    delivery_authority: Option<super::task_actor_contract::PreparedTaskDeliveryAuthority>,
    projection: crate::task_projector::PreparedTaskProjection,
    delivery_boundary: Option<crate::task_projector::PreparedDeliveryAuthorityBoundary>,
}

#[derive(Clone, Debug)]
struct SemanticallyMatchingExistingTaskEvent {
    identity: TaskEventIdentity,
    payload: Arc<TaskEventPayload>,
    equivalent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskEventIdentity {
    id: String,
    task_id: String,
    run_id: Option<String>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    event_type: String,
    idempotency_key: Option<String>,
    sequence: i64,
    created_at: DateTimeWithTimeZone,
    sha256: Vec<u8>,
}

impl From<&task_event::Model> for TaskEventIdentity {
    fn from(row: &task_event::Model) -> Self {
        Self {
            id: row.id.clone(),
            task_id: row.task_id.clone(),
            run_id: row.run_id.clone(),
            thread_id: row.thread_id.clone(),
            turn_id: row.turn_id.clone(),
            event_type: row.event_type.clone(),
            idempotency_key: row.idempotency_key.clone(),
            sequence: row.sequence,
            created_at: row.created_at,
            sha256: row.payload_json_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct PreparedBatchExisting {
    source_prepared_id: String,
    winner_id: String,
    // None denotes a new INSERT by the earlier event in this transaction.
    persisted_identity: Option<TaskEventIdentity>,
    payload: Arc<TaskEventPayload>,
    equivalent: bool,
}

// Prepared input ID -> (actual row fence, inserted by this batch).
pub type TaskEventBatchIdentities = HashMap<String, (TaskEventIdentity, bool)>;

#[derive(Debug)]
struct TaskEventIdempotencyPreflightRace {
    idempotency_key: String,
}

impl fmt::Display for TaskEventIdempotencyPreflightRace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "task event idempotency key `{}` changed after preflight",
            self.idempotency_key
        )
    }
}

impl StdError for TaskEventIdempotencyPreflightRace {}

#[derive(Debug)]
pub struct TaskChildCompletionReplay {
    pub idempotency_key: String,
    pub completed_at: Option<i64>,
}

impl fmt::Display for TaskChildCompletionReplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "task event idempotency key `{}` already exists with a different payload",
            self.idempotency_key
        )
    }
}

impl StdError for TaskChildCompletionReplay {}

pub fn is_idempotency_preflight_race(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<TaskEventIdempotencyPreflightRace>()
            .is_some()
    })
}

impl PreparedTaskEvent {
    pub fn prepare(payload: TaskEventPayload) -> Result<Self> {
        let projection = crate::task_projector::PreparedTaskProjection::prepare(&payload)?;
        let candidate_projection = match &payload {
            TaskEventPayload::TaskResultCandidateCreated { candidate }
            | TaskEventPayload::TaskResultCandidateAccepted { candidate, .. }
            | TaskEventPayload::TaskResultCandidateRejected { candidate, .. }
            | TaskEventPayload::TaskResultCandidateCancelled { candidate, .. } => Some(
                super::task_result_candidate::prepare_protocol_candidate(candidate)?,
            ),
            _ => None,
        };
        let review_projection = match &payload {
            TaskEventPayload::TaskResultReviewEventRecorded { review_event } => {
                Some(super::task_result_review_event::prepare_protocol_review_event(review_event)?)
            }
            _ => None,
        };
        let task_id = payload.task_id().to_owned();
        let run_id = payload.run_id().map(str::to_owned);
        let thread_id = payload.thread_id().map(str::to_owned);
        let turn_id = payload.turn_id().map(str::to_owned);
        let event_type = payload.event_type().to_owned();
        let idempotency_key = payload.idempotency_key();
        let payload_json =
            serde_json::to_string(&payload).context("failed to serialize task event payload")?;
        Ok(Self {
            id: generate_id(DB_ID_LEN),
            task_id,
            run_id,
            thread_id,
            turn_id,
            event_type,
            idempotency_key,
            encoded_payload: EventPayload::prepare(&payload_json)?,
            payload,
            semantically_matching_existing: None,
            batch_existing: Vec::new(),
            gate_payloads: None,
            candidate_gate_resolution: None,
            candidate_projection,
            review_projection,
            delivery_authority: None,
            projection,
            delivery_boundary: None,
        })
    }

    /// Reader-only semantic work: queries release their capacity before holds,
    /// decompression and JSON comparison. The writer resolves the terminal key
    /// again against its projected state and trusts only the captured row fence.
    pub async fn preflight_idempotency(
        mut self,
        db: &pioneer_postgres_pools::PostgresDatabase,
    ) -> Result<Self> {
        self.semantically_matching_existing = None;
        self.batch_existing.clear();
        let key = if self.idempotency_key == self.payload.idempotency_key() {
            terminal_idempotency_key(db, &self.payload).await?
        } else {
            self.idempotency_key.clone()
        };
        if let Some(key) = key {
            let query = task_event::Entity::find()
                .filter(task_event::Column::TaskId.eq(&self.task_id))
                .filter(task_event::Column::IdempotencyKey.eq(key));
            if let Some(mut row) = load_rows(db, query).await?.pop() {
                let identity = TaskEventIdentity::from(&row);
                let payload = decode_event(&mut row)?;
                let equivalent = equivalent_payloads(&payload, &self.payload);
                // Defer a semantic conflict until the writer validates this
                // exact row: an earlier event may change the terminal key.
                self.semantically_matching_existing = Some(SemanticallyMatchingExistingTaskEvent {
                    identity,
                    payload: Arc::new(payload),
                    equivalent,
                });
            }
        }
        Ok(self)
    }

    pub fn validate_delivery_start_fields(&self) -> Result<()> {
        self.projection.validate_delivery_start_fields()
    }

    pub fn validate_delivery_fields(&self) -> Result<()> {
        self.projection.validate_delivery_fields()
    }

    pub fn with_delivery_rows(
        mut self,
        delivery: pioneer_entity::task_delivery::Model,
        attempt: Option<pioneer_entity::task_delivery_attempt::Model>,
    ) -> Self {
        self.projection = self.projection.with_delivery_rows(delivery, attempt);
        self
    }

    pub fn validate_delivery_identity(
        &self,
        row: &pioneer_entity::task_delivery::Model,
    ) -> Result<()> {
        self.projection.validate_delivery_identity(row)
    }

    pub fn set_delivery_boundary(
        &mut self,
        boundary: crate::task_projector::PreparedDeliveryAuthorityBoundary,
    ) {
        self.delivery_boundary = Some(boundary);
    }

    pub fn take_delivery_boundary(
        &mut self,
    ) -> Option<crate::task_projector::PreparedDeliveryAuthorityBoundary> {
        self.delivery_boundary.take()
    }

    pub fn take_delivery_authority(
        &mut self,
    ) -> Option<super::task_actor_contract::PreparedTaskDeliveryAuthority> {
        self.delivery_authority.take()
    }

    pub fn payload(&self) -> &TaskEventPayload {
        &self.payload
    }

    pub fn with_candidate_gate_resolution(
        mut self,
        prepared: Option<
            pioneer_crud_execution::native_terminal_effect_outbox::PreparedCandidateGateResolution,
        >,
    ) -> Self {
        self.candidate_gate_resolution = prepared;
        self
    }

    pub fn candidate_projection(
        &self,
    ) -> Option<&super::task_result_candidate::PreparedTaskResultCandidate> {
        self.candidate_projection.as_ref()
    }

    pub fn with_candidate_projection(
        mut self,
        prepared: Option<super::task_result_candidate::PreparedTaskResultCandidate>,
    ) -> Self {
        self.candidate_projection = prepared;
        self
    }

    pub fn with_review_projection(
        mut self,
        prepared: Option<super::task_result_review_event::PreparedTaskResultReviewEvent>,
    ) -> Self {
        self.review_projection = prepared;
        self
    }

    pub fn with_delivery_authority(
        mut self,
        prepared: Option<super::task_actor_contract::PreparedTaskDeliveryAuthority>,
    ) -> Self {
        self.delivery_authority = prepared;
        self
    }
}

/// Compare only already prepared typed payloads, before writer admission.
/// At append time bind each proof to the actual winner of the earlier event.
/// This includes timestamp-equivalent retries whose keys were absent in preflight.
pub fn prepare_batch_idempotency(events: &mut [PreparedTaskEvent]) {
    // One shared typed candidate per input, independent of the number of later
    // proofs. A 64-event batch must not retain 2016 full payload copies.
    let mut candidates: Vec<Option<Arc<TaskEventPayload>>> = vec![None; events.len()];
    for index in 0..events.len() {
        let (prior, current) = events.split_at_mut(index);
        let current = &mut current[0];
        current.batch_existing.clear();
        for (earlier_index, earlier) in prior.iter().enumerate() {
            if earlier.task_id != current.task_id
                || earlier.idempotency_key.is_none()
                || earlier.idempotency_key != current.idempotency_key
            {
                continue;
            }
            let mut add = |winner_id: String,
                           persisted_identity: Option<TaskEventIdentity>,
                           payload: &Arc<TaskEventPayload>| {
                current.batch_existing.push(PreparedBatchExisting {
                    source_prepared_id: earlier.id.clone(),
                    winner_id,
                    persisted_identity,
                    equivalent: equivalent_payloads(payload, &current.payload),
                    payload: payload.clone(),
                });
            };
            let candidate =
                candidates[earlier_index].get_or_insert_with(|| Arc::new(earlier.payload.clone()));
            add(earlier.id.clone(), None, candidate);
            if let Some(existing) = &earlier.semantically_matching_existing {
                add(
                    existing.identity.id.clone(),
                    Some(existing.identity.clone()),
                    &existing.payload,
                );
            }
        }
    }
}

/// A task-owned resume can reopen the same run and turn. Read its persisted
/// retry generation in the existing writer transaction, before deduplication;
/// the original terminal event must not consume the new outcome's projection.
pub async fn terminal_idempotency_key<C: ConnectionTrait>(
    db: &C,
    payload: &TaskEventPayload,
) -> Result<Option<String>> {
    let key = payload.idempotency_key();
    let run_id = match payload {
        TaskEventPayload::RunCompleted { run_id, .. }
        | TaskEventPayload::RunFailed { run_id, .. }
        | TaskEventPayload::RunBlocked { run_id, .. }
        | TaskEventPayload::RunCancelled { run_id, .. } => run_id.clone(),
        TaskEventPayload::TaskRunTurnCompleted { task_run_turn }
        | TaskEventPayload::TaskRunTurnFailed { task_run_turn, .. }
        | TaskEventPayload::TaskRunTurnBlocked { task_run_turn, .. } => {
            task_run_turn.run_id.clone()
        }
        TaskEventPayload::TaskCompleted { task_id, .. }
        | TaskEventPayload::TaskFailed { task_id, .. }
        | TaskEventPayload::TaskBlocked { task_id, .. }
        | TaskEventPayload::TaskCancelled { task_id, .. } => {
            let Some(run) = super::task_run::find_latest_numbered_run(db, task_id).await? else {
                return Ok(key);
            };
            run.id
        }
        _ => return Ok(key),
    };
    let Some(occurrence) =
        super::task_actor_contract::find_task_occurrence_by_run_id(db, &run_id).await?
    else {
        return Ok(key);
    };
    anyhow::ensure!(
        occurrence.task_id == payload.task_id(),
        "terminal occurrence crosses Task boundary"
    );
    // Keep the existing keys for initial execution and legacy replay.
    Ok(key.map(|key| {
        if occurrence.retry_attempt == 0 {
            key
        } else {
            format!("{key}:run:{run_id}:retry:{}", occurrence.retry_attempt)
        }
    }))
}

pub async fn append_event(
    db: &pioneer_postgres_pools::PostgresDatabase,
    payload: &TaskEventPayload,
    created_at: DateTimeWithTimeZone,
    idempotency_key: Option<&str>,
) -> Result<AppendedTaskEvent> {
    let mut prepared = PreparedTaskEvent::prepare(payload.clone())?;
    prepared.idempotency_key = idempotency_key.map(str::to_owned);
    let prepared = prepared.preflight_idempotency(db).await?;
    let transaction = db.begin().await?;
    let event = append_prepared_event(&transaction, prepared, created_at).await?;
    transaction.commit().await?;
    Ok(event.into_owned())
}

pub async fn append_prepared_event<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskEvent,
    created_at: DateTimeWithTimeZone,
) -> Result<DeferredAppendedTaskEvent> {
    append_prepared_event_with_batch(
        db,
        prepared,
        created_at,
        &mut TaskEventBatchIdentities::new(),
    )
    .await
}

pub async fn append_prepared_event_with_batch<C: ConnectionTrait>(
    db: &C,
    mut prepared: PreparedTaskEvent,
    created_at: DateTimeWithTimeZone,
    batch: &mut TaskEventBatchIdentities,
) -> Result<DeferredAppendedTaskEvent> {
    event_log::lock_task(db, &prepared.task_id).await?;
    // Keep database-dependent key selection sequential: event N sees event N-1.
    if prepared.idempotency_key == prepared.payload.idempotency_key() {
        prepared.idempotency_key = terminal_idempotency_key(db, &prepared.payload).await?;
    }
    let prepared_id = prepared.id.clone();
    let outcome = append_prepared_event_inner(db, prepared, created_at, batch).await?;
    let identity = task_event_metadata_query()
        .filter(task_event::Column::Id.eq(&outcome.id))
        .one(db)
        .await?
        .context("appended Task event identity disappeared")?;
    batch.insert(
        prepared_id,
        (
            TaskEventIdentity::from(&identity),
            outcome.append_status.is_inserted(),
        ),
    );
    Ok(outcome)
}

async fn append_prepared_event_inner<C: ConnectionTrait>(
    db: &C,
    mut prepared: PreparedTaskEvent,
    created_at: DateTimeWithTimeZone,
    batch: &TaskEventBatchIdentities,
) -> Result<DeferredAppendedTaskEvent> {
    if let Some(idempotency_key) = prepared.idempotency_key.as_deref()
        && let Some(existing) =
            find_event_by_idempotency_key(db, &prepared.task_id, idempotency_key).await?
    {
        let cached = take_matching_preflight(
            &mut prepared.semantically_matching_existing,
            &mut prepared.batch_existing,
            batch,
            &existing,
        );
        return existing_event_outcome_after_preflight(
            existing,
            cached,
            idempotency_key,
            &prepared.payload,
        );
    }

    let PreparedTaskEvent {
        id,
        task_id,
        run_id,
        thread_id,
        turn_id,
        event_type,
        idempotency_key,
        encoded_payload,
        payload,
        semantically_matching_existing: _,
        batch_existing: _,
        gate_payloads: _,
        candidate_gate_resolution,
        candidate_projection,
        review_projection,
        delivery_authority,
        projection,
        delivery_boundary: _,
    } = prepared;
    let sequence = event_log::next_sequence(db, &task_id).await?;
    event_log::append(
        db,
        event_log::NewEvent {
            id: &id,
            task_id: &task_id,
            run_id: run_id.as_deref(),
            thread_id: thread_id.as_deref(),
            turn_id: turn_id.as_deref(),
            event_type: &event_type,
            idempotency_key: idempotency_key.as_deref(),
            sequence,
            created_at,
        },
        encoded_payload,
    )
    .await?;

    Ok(DeferredAppendedTaskEvent {
        id,
        task_id,
        run_id,
        thread_id,
        turn_id,
        sequence,
        event_type,
        idempotency_key,
        payload: Arc::new(payload),
        workspace_id: None,
        root_task_id: None,
        parent_task_id: None,
        created_at,
        append_status: TaskEventAppendStatus::Inserted,
        candidate_gate_resolution,
        candidate_projection,
        review_projection,
        delivery_authority,
        projection,
    })
}

fn take_matching_preflight(
    preflight: &mut Option<SemanticallyMatchingExistingTaskEvent>,
    prepared_batch: &mut Vec<PreparedBatchExisting>,
    batch: &TaskEventBatchIdentities,
    row: &task_event::Model,
) -> Option<SemanticallyMatchingExistingTaskEvent> {
    let identity = TaskEventIdentity::from(row);
    if preflight
        .as_ref()
        .is_some_and(|cached| cached.identity == identity)
    {
        return preflight.take();
    }
    let index = prepared_batch.iter().position(|proof| {
        proof.winner_id == row.id
            && batch
                .get(&proof.source_prepared_id)
                .is_some_and(|(actual, inserted)| {
                    actual == &identity
                        && match &proof.persisted_identity {
                            Some(expected) => expected == &identity,
                            None => *inserted,
                        }
                })
    })?;
    let proof = prepared_batch.swap_remove(index);
    Some(SemanticallyMatchingExistingTaskEvent {
        identity,
        payload: proof.payload,
        equivalent: proof.equivalent,
    })
}

fn existing_event_outcome_after_preflight(
    model: task_event::Model,
    preflight: Option<SemanticallyMatchingExistingTaskEvent>,
    idempotency_key: &str,
    attempted_payload: &TaskEventPayload,
) -> Result<DeferredAppendedTaskEvent> {
    let Some(preflight) = preflight else {
        // The metadata-only writer never parses an unexpected raw/legacy body.
        return Err(TaskEventIdempotencyPreflightRace {
            idempotency_key: idempotency_key.to_owned(),
        }
        .into());
    };
    if !preflight.equivalent {
        if let (
            TaskEventPayload::TaskRunTurnCompleted {
                task_run_turn: existing,
            },
            TaskEventPayload::TaskRunTurnCompleted {
                task_run_turn: attempted,
            },
        ) = (preflight.payload.as_ref(), attempted_payload)
        {
            let mut attempted = attempted.clone();
            attempted.completed_at = existing.completed_at;
            if &attempted == existing {
                return Err(TaskChildCompletionReplay {
                    idempotency_key: idempotency_key.to_owned(),
                    completed_at: existing.completed_at,
                }
                .into());
            }
        }
        anyhow::bail!(
            "task event idempotency key `{}` already exists with a different payload",
            idempotency_key
        );
    }
    Ok(appended_task_event_from_known_payload(
        model,
        // The caller materializes this winner only after releasing its writer.
        preflight.payload,
        TaskEventAppendStatus::AlreadyExists,
    ))
}

fn appended_task_event_from_known_payload(
    model: task_event::Model,
    payload: Arc<TaskEventPayload>,
    append_status: TaskEventAppendStatus,
) -> DeferredAppendedTaskEvent {
    DeferredAppendedTaskEvent {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        event_type: model.event_type,
        idempotency_key: model.idempotency_key,
        payload,
        workspace_id: None,
        root_task_id: None,
        parent_task_id: None,
        created_at: model.created_at,
        append_status,
        candidate_gate_resolution: None,
        candidate_projection: None,
        review_projection: None,
        delivery_authority: None,
        projection: Default::default(),
    }
}

pub use crate::event_fanout::{
    advance_fanout_cursor, find_fanout_cursor, initialize_fanout_cursor,
};
use crate::event_reads::{decode_event, equivalent_payloads, task_event_metadata_query};
pub use crate::event_reads::{
    fanout_page, find_event_by_idempotency_key, find_queued_delivery_for_run, load_rows,
};
pub use crate::event_reads::{
    list_event_task_ids, list_events_for_run, list_events_for_task, list_events_for_thread_turn,
    task_event_from_model,
};

pub fn appended_task_event_from_model(
    mut model: task_event::Model,
    append_status: TaskEventAppendStatus,
) -> Result<AppendedTaskEvent> {
    let payload = decode_event(&mut model)?;
    Ok(AppendedTaskEvent {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        event_type: model.event_type,
        idempotency_key: model.idempotency_key,
        payload,
        workspace_id: None,
        root_task_id: None,
        parent_task_id: None,
        created_at: model.created_at,
        append_status,
    })
}
