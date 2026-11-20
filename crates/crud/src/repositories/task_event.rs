#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_crud_tasks::event_log::{self, EventPayload};
use pioneer_entity::task_event;
use pioneer_protocol::{TaskEvent, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use crate::task_events::{
    AppendedTaskEvent, DeferredAppendedTaskEvent, TaskEventAppendStatus, TaskEventPayload,
};

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
    pub(crate) gate_payloads: Option<super::native_terminal_effect_outbox::PreparedGatePayloads>,
    candidate_gate_resolution:
        Option<super::native_terminal_effect_outbox::PreparedCandidateGateResolution>,
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
pub(crate) struct TaskEventIdentity {
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
pub(crate) type TaskEventBatchIdentities = HashMap<String, (TaskEventIdentity, bool)>;

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

pub(crate) fn is_idempotency_preflight_race(error: &anyhow::Error) -> bool {
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
        db: &pioneer_sqlite::SqliteDatabase,
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

    pub(crate) fn validate_delivery_start_fields(&self) -> Result<()> {
        self.projection.validate_delivery_start_fields()
    }

    pub(crate) fn validate_delivery_fields(&self) -> Result<()> {
        self.projection.validate_delivery_fields()
    }

    pub(crate) fn with_delivery_rows(
        mut self,
        delivery: pioneer_entity::task_delivery::Model,
        attempt: Option<pioneer_entity::task_delivery_attempt::Model>,
    ) -> Self {
        self.projection = self.projection.with_delivery_rows(delivery, attempt);
        self
    }

    pub(crate) fn validate_delivery_identity(
        &self,
        row: &pioneer_entity::task_delivery::Model,
    ) -> Result<()> {
        self.projection.validate_delivery_identity(row)
    }

    pub(crate) fn set_delivery_boundary(
        &mut self,
        boundary: crate::task_projector::PreparedDeliveryAuthorityBoundary,
    ) {
        self.delivery_boundary = Some(boundary);
    }

    pub(crate) fn take_delivery_boundary(
        &mut self,
    ) -> Option<crate::task_projector::PreparedDeliveryAuthorityBoundary> {
        self.delivery_boundary.take()
    }

    pub(crate) fn take_delivery_authority(
        &mut self,
    ) -> Option<super::task_actor_contract::PreparedTaskDeliveryAuthority> {
        self.delivery_authority.take()
    }

    pub(crate) fn payload(&self) -> &TaskEventPayload {
        &self.payload
    }

    pub(crate) fn with_candidate_gate_resolution(
        mut self,
        prepared: Option<super::native_terminal_effect_outbox::PreparedCandidateGateResolution>,
    ) -> Self {
        self.candidate_gate_resolution = prepared;
        self
    }

    pub(crate) fn candidate_projection(
        &self,
    ) -> Option<&super::task_result_candidate::PreparedTaskResultCandidate> {
        self.candidate_projection.as_ref()
    }

    pub(crate) fn with_candidate_projection(
        mut self,
        prepared: Option<super::task_result_candidate::PreparedTaskResultCandidate>,
    ) -> Self {
        self.candidate_projection = prepared;
        self
    }

    pub(crate) fn with_review_projection(
        mut self,
        prepared: Option<super::task_result_review_event::PreparedTaskResultReviewEvent>,
    ) -> Self {
        self.review_projection = prepared;
        self
    }

    pub(crate) fn with_delivery_authority(
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
pub(crate) fn prepare_batch_idempotency(events: &mut [PreparedTaskEvent]) {
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
pub(crate) async fn terminal_idempotency_key<C: ConnectionTrait>(
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
    db: &pioneer_sqlite::SqliteDatabase,
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

pub(crate) async fn append_prepared_event<C: ConnectionTrait>(
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

pub(crate) async fn append_prepared_event_with_batch<C: ConnectionTrait>(
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
        return existing_event_outcome_after_preflight(existing, cached, idempotency_key);
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
) -> Result<DeferredAppendedTaskEvent> {
    let Some(preflight) = preflight else {
        // The metadata-only writer never parses an unexpected raw/legacy body.
        return Err(TaskEventIdempotencyPreflightRace {
            idempotency_key: idempotency_key.to_owned(),
        }
        .into());
    };
    anyhow::ensure!(
        preflight.equivalent,
        "task event idempotency key `{}` already exists with a different payload",
        idempotency_key
    );
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

pub(crate) async fn find_event_by_idempotency_key<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    idempotency_key: &str,
) -> Result<Option<task_event::Model>> {
    task_event_metadata_query()
        .filter(task_event::Column::TaskId.eq(task_id.to_owned()))
        .filter(task_event::Column::IdempotencyKey.eq(idempotency_key.to_owned()))
        .one(db)
        .await
        .context("failed to query task event by idempotency key")
}

/// Typed Model projection for identity/control checks under the writer. Never
/// fetch the body just to decide whether preflight is current.
fn task_event_metadata_query() -> sea_orm::Select<task_event::Entity> {
    use task_event::Column as Col;
    task_event::Entity::find()
        .select_only()
        .columns([
            Col::Id,
            Col::TaskId,
            Col::RunId,
            Col::ThreadId,
            Col::TurnId,
            Col::Sequence,
            Col::EventType,
            Col::CreatedAt,
            Col::IdempotencyKey,
            Col::PayloadJsonCodec,
            Col::PayloadJsonBytes,
            Col::PayloadJsonChars,
            Col::PayloadJsonSha256,
        ])
        .expr_as(Expr::val(Vec::<u8>::new()), Col::PayloadJson)
}

fn equivalent_payloads(existing: &TaskEventPayload, attempted: &TaskEventPayload) -> bool {
    existing == attempted
        || equivalent_cancelled_terminal_payload(existing, attempted)
        || equivalent_replayed_task_run_turn_terminal_payload(existing, attempted)
}

fn equivalent_replayed_task_run_turn_terminal_payload(
    existing: &TaskEventPayload,
    attempted: &TaskEventPayload,
) -> bool {
    let (existing_turn, existing_error, attempted_turn, attempted_error) =
        match (existing, attempted) {
            (
                TaskEventPayload::TaskRunTurnFailed {
                    task_run_turn: existing_turn,
                    error: existing_error,
                },
                TaskEventPayload::TaskRunTurnFailed {
                    task_run_turn: attempted_turn,
                    error: attempted_error,
                },
            )
            | (
                TaskEventPayload::TaskRunTurnBlocked {
                    task_run_turn: existing_turn,
                    error: existing_error,
                },
                TaskEventPayload::TaskRunTurnBlocked {
                    task_run_turn: attempted_turn,
                    error: attempted_error,
                },
            ) => (
                existing_turn,
                existing_error,
                attempted_turn,
                attempted_error,
            ),
            _ => return false,
        };

    if existing_error != attempted_error {
        return false;
    }

    let mut existing_turn = existing_turn.clone();
    let mut attempted_turn = attempted_turn.clone();
    existing_turn.completed_at = None;
    attempted_turn.completed_at = None;
    existing_turn == attempted_turn
}

fn equivalent_cancelled_terminal_payload(
    existing: &TaskEventPayload,
    attempted: &TaskEventPayload,
) -> bool {
    matches!(terminal_outcome(existing), Some(TerminalOutcome::Cancelled))
        && matches!(
            terminal_outcome(attempted),
            Some(TerminalOutcome::Cancelled)
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalOutcome {
    Succeeded,
    Blocked,
    Failed,
    Cancelled,
}

fn terminal_outcome(payload: &TaskEventPayload) -> Option<TerminalOutcome> {
    match payload {
        TaskEventPayload::RunCompleted { .. } | TaskEventPayload::TaskCompleted { .. } => {
            Some(TerminalOutcome::Succeeded)
        }
        TaskEventPayload::RunFailed { error, .. } | TaskEventPayload::TaskFailed { error, .. } => {
            if error
                .as_ref()
                .is_some_and(|error| error.class == pioneer_protocol::TaskErrorClass::Cancelled)
            {
                Some(TerminalOutcome::Cancelled)
            } else {
                Some(TerminalOutcome::Failed)
            }
        }
        TaskEventPayload::RunBlocked { .. } | TaskEventPayload::TaskBlocked { .. } => {
            Some(TerminalOutcome::Blocked)
        }
        TaskEventPayload::RunCancelled { .. } | TaskEventPayload::TaskCancelled { .. } => {
            Some(TerminalOutcome::Cancelled)
        }
        _ => None,
    }
}

pub(crate) async fn find_queued_delivery_for_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    retry_attempt: u32,
) -> Result<Option<task_event::Model>> {
    if retry_attempt > 0 {
        // Earlier outcomes of this same run retain their own deliveries.
        // Bound this lookup to the current retry rather than mixing them.
        use pioneer_entity::task_delivery;
        let mut deliveries = task_delivery::Entity::find()
            .select_only()
            .columns([task_delivery::Column::TaskId, task_delivery::Column::Id])
            .filter(task_delivery::Column::RunId.eq(run_id.to_owned()))
            .filter(
                task_delivery::Column::DeliveryKey.starts_with(format!("retry:{retry_attempt}:")),
            )
            .limit(2)
            .into_tuple::<(String, String)>()
            .all(db)
            .await?;
        if deliveries.len() > 1 {
            anyhow::bail!("terminal Task retry has multiple queued deliveries");
        }
        let Some((task_id, delivery_id)) = deliveries.pop() else {
            return Ok(None);
        };
        let event =
            find_event_by_idempotency_key(db, &task_id, &format!("delivery:{delivery_id}:queued"))
                .await?
                .context("terminal Task retry delivery has no queued event")?;
        anyhow::ensure!(
            event.run_id.as_deref() == Some(run_id),
            "delivery event crosses run boundary"
        );
        return Ok(Some(event));
    }
    let mut rows = task_event_metadata_query()
        .filter(task_event::Column::RunId.eq(run_id.to_owned()))
        .filter(
            task_event::Column::EventType
                .eq(pioneer_protocol::constants::events::TASK_DELIVERY_QUEUED),
        )
        .limit(2)
        .all(db)
        .await?;
    if rows.len() > 1 {
        anyhow::bail!("terminal Task run has multiple queued deliveries");
    }
    Ok(rows.pop())
}

pub(crate) async fn load_rows(
    db: &pioneer_sqlite::SqliteDatabase,
    query: sea_orm::Select<task_event::Entity>,
) -> Result<Vec<task_event::Model>> {
    Ok(query.all(db).await?)
}

pub async fn list_events_for_task(
    db: &pioneer_sqlite::SqliteDatabase,
    task_id: &str,
    after_sequence: Option<i64>,
    limit: Option<u64>,
) -> Result<Vec<task_event::Model>> {
    let mut query = task_event::Entity::find()
        .filter(task_event::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_event::Column::Sequence);

    if let Some(after_sequence) = after_sequence {
        query = query.filter(task_event::Column::Sequence.gt(after_sequence));
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    load_rows(db, query)
        .await
        .context("failed to query task events")
}

pub async fn list_event_task_ids<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    task_event::Entity::find()
        .select_only()
        .column(task_event::Column::TaskId)
        .distinct()
        .order_by_asc(task_event::Column::TaskId)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to query task event task ids")
}

pub(crate) fn fanout_metadata_query(
    task_id: &str,
    after: i64,
    limit: usize,
) -> sea_orm::Select<task_event::Entity> {
    // Bundled SQLite 3.51.3 recognizes the direct column argument and sets
    // OP_Column's OPFLAG_BYTELENARG. No CAST/content conversion before budget.
    task_event::Entity::find()
        .select_only()
        .columns([task_event::Column::Id, task_event::Column::Sequence])
        .column_as(task_event::Column::PayloadJsonBytes, "payload_bytes")
        .filter(task_event::Column::TaskId.eq(task_id))
        .filter(task_event::Column::Sequence.gt(after))
        .order_by_asc(task_event::Column::Sequence)
        .limit(limit.min(128) as u64)
}

/// First fetch IDs/sequence/byte lengths, then only an admissible prefix.
/// Oversized immutable events are supported singly, with no new domain limit.
pub(crate) async fn fanout_page(
    db: &pioneer_sqlite::SqliteDatabase,
    task_id: &str,
    after: i64,
    limit: usize,
    bytes_left: &mut usize,
    allow_oversized: bool,
) -> Result<super::task_event_fanout::TaskEventFanoutPage<task_event::Model>> {
    let bytes = *bytes_left;
    let metadata = fanout_metadata_query(task_id, after, limit)
        .into_tuple::<(String, i64, i64)>()
        .all(db)
        .await?;
    use super::task_event_fanout::TaskEventFanoutPage;
    let mut ids = Vec::new();
    let mut used = 0usize;
    for (id, _, length) in metadata {
        let length = usize::try_from(length).context("invalid task event byte length")?;
        if length > bytes.saturating_sub(used) {
            if ids.is_empty()
                && allow_oversized
                && length > super::task_event_fanout::TASK_EVENT_FANOUT_BYTE_BUDGET
            {
                ids.push(id);
                used = length;
            }
            if ids.is_empty() {
                return Ok(TaskEventFanoutPage::BudgetDeferred);
            }
            break;
        }
        used = used.saturating_add(length);
        ids.push(id);
    }
    if ids.is_empty() {
        return Ok(TaskEventFanoutPage::Prefix {
            events: Vec::new(),
            bytes: 0,
        });
    }
    // Charge before the full read: an error or unwind cannot reuse bytes
    // whose payload may already have been fetched. No reader is held here.
    *bytes_left = bytes_left.saturating_sub(used);
    // Retry only this already charged prefix, never metadata/budget admission.
    let rows = load_rows(
        db,
        task_event::Entity::find()
            .filter(task_event::Column::Id.is_in(ids))
            .order_by_asc(task_event::Column::Sequence),
    )
    .await?;
    Ok(TaskEventFanoutPage::Prefix {
        events: rows,
        bytes: used,
    })
}

pub use pioneer_crud_tasks::event_fanout::{
    advance_fanout_cursor, find_fanout_cursor, initialize_fanout_cursor,
};

pub async fn list_events_for_run(
    db: &pioneer_sqlite::SqliteDatabase,
    run_id: &str,
) -> Result<Vec<task_event::Model>> {
    load_rows(
        db,
        task_event::Entity::find()
            .filter(task_event::Column::RunId.eq(run_id.to_owned()))
            .order_by_asc(task_event::Column::Sequence),
    )
    .await
    .context("failed to query task events for run")
}

pub async fn list_events_for_thread_turn(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<Vec<task_event::Model>> {
    let mut query = task_event::Entity::find()
        .filter(task_event::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(task_event::Column::CreatedAt)
        .order_by_asc(task_event::Column::Sequence);

    if let Some(turn_id) = turn_id {
        query = query.filter(task_event::Column::TurnId.eq(turn_id.to_owned()));
    }

    load_rows(db, query)
        .await
        .context("failed to query task events for thread/turn")
}

fn decode_event(row: &mut task_event::Model) -> Result<TaskEventPayload> {
    let json = EventPayload::read(row)?;
    serde_json::from_str(&json).context("failed to decode task event JSON")
}

pub fn task_event_from_model(mut model: task_event::Model) -> Result<TaskEvent> {
    let payload = decode_event(&mut model)?;
    Ok(TaskEvent {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        event_type: model.event_type,
        idempotency_key: model.idempotency_key,
        payload,
        created_at: model.created_at.timestamp(),
    })
}

pub(crate) fn appended_task_event_from_model(
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

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TaskError, TaskErrorClass, TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus,
    };

    fn terminal_turn(status: TaskRunTurnStatus, completed_at: i64) -> TaskRunTurn {
        TaskRunTurn {
            id: "task-run-turn-1".to_owned(),
            task_id: "task-1".to_owned(),
            run_id: "run-1".to_owned(),
            execution_id: Some("execution-1".to_owned()),
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 10,
            started_at: Some(11),
            completed_at: Some(completed_at),
        }
    }

    fn cancelled_error() -> TaskError {
        TaskError {
            recovery_diagnostic: None,
            code: "task_cli_runtime_start_superseded".to_owned(),
            message: "task run became terminal before activation".to_owned(),
            class: TaskErrorClass::Cancelled,
            details: None,
            failed_run_id: Some("run-1".to_owned()),
        }
    }

    #[test]
    fn repeated_blocked_turn_terminalization_ignores_only_completion_time() {
        let existing = TaskEventPayload::TaskRunTurnBlocked {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Blocked, 20),
            error: Some(cancelled_error()),
        };
        let replay = TaskEventPayload::TaskRunTurnBlocked {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Blocked, 30),
            error: Some(cancelled_error()),
        };

        assert!(equivalent_replayed_task_run_turn_terminal_payload(
            &existing, &replay
        ));
    }

    #[test]
    fn repeated_failed_turn_terminalization_ignores_only_completion_time() {
        let existing = TaskEventPayload::TaskRunTurnFailed {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Failed, 20),
            error: Some(cancelled_error()),
        };
        let replay = TaskEventPayload::TaskRunTurnFailed {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Failed, 30),
            error: Some(cancelled_error()),
        };

        assert!(equivalent_replayed_task_run_turn_terminal_payload(
            &existing, &replay
        ));
    }

    #[test]
    fn repeated_turn_terminalization_rejects_meaningful_payload_changes() {
        let existing = TaskEventPayload::TaskRunTurnBlocked {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Blocked, 20),
            error: Some(cancelled_error()),
        };
        let mut different_error = cancelled_error();
        different_error.code = "different_reason".to_owned();
        let replay = TaskEventPayload::TaskRunTurnBlocked {
            task_run_turn: terminal_turn(TaskRunTurnStatus::Blocked, 30),
            error: Some(different_error),
        };

        assert!(!equivalent_replayed_task_run_turn_terminal_payload(
            &existing, &replay
        ));
    }
}
