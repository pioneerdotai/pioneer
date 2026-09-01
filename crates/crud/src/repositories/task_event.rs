#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::{task, task_event, task_event_fanout_cursor};
use pioneer_protocol::{TaskEvent, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};
use std::error::Error as StdError;
use std::fmt;

use crate::task_events::{AppendedTaskEvent, TaskEventAppendStatus, TaskEventPayload};

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
    payload_json: String,
    payload: TaskEventPayload,
    semantically_matching_existing: Option<SemanticallyMatchingExistingTaskEvent>,
    candidate_gate_resolution:
        Option<super::native_terminal_effect_outbox::PreparedCandidateGateResolution>,
    candidate_projection: Option<super::task_result_candidate::PreparedTaskResultCandidate>,
    review_projection: Option<super::task_result_review_event::PreparedTaskResultReviewEvent>,
    delivery_authority: Option<super::task_actor_contract::PreparedTaskDeliveryAuthority>,
    projection: crate::task_projector::PreparedTaskProjection,
}

#[derive(Clone, Debug)]
struct SemanticallyMatchingExistingTaskEvent {
    id: String,
    payload: TaskEventPayload,
}

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
            payload_json,
            payload,
            semantically_matching_existing: None,
            candidate_gate_resolution: None,
            candidate_projection,
            review_projection,
            delivery_authority: None,
            projection,
        })
    }

    pub async fn preflight_idempotency<C: ConnectionTrait>(mut self, db: &C) -> Result<Self> {
        let Some(idempotency_key) = self.idempotency_key.as_deref() else {
            return Ok(self);
        };
        if let Some(existing) =
            find_event_by_idempotency_key(db, &self.task_id, idempotency_key).await?
        {
            let existing = existing_event_outcome(existing, &self.payload, idempotency_key)?;
            self.semantically_matching_existing = Some(SemanticallyMatchingExistingTaskEvent {
                id: existing.id,
                payload: existing.payload,
            });
        }
        Ok(self)
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

pub async fn append_event<C: ConnectionTrait>(
    db: &C,
    payload: &TaskEventPayload,
    created_at: DateTimeWithTimeZone,
    idempotency_key: Option<&str>,
) -> Result<AppendedTaskEvent> {
    let mut prepared = PreparedTaskEvent::prepare(payload.clone())?;
    prepared.idempotency_key = idempotency_key.map(str::to_owned);
    let prepared = prepared.preflight_idempotency(db).await?;
    append_prepared_event(db, prepared, created_at).await
}

pub async fn append_prepared_event<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskEvent,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTaskEvent> {
    if let Some(idempotency_key) = prepared.idempotency_key.as_deref()
        && let Some(existing) =
            find_event_by_idempotency_key(db, prepared.task_id.as_str(), idempotency_key).await?
    {
        return existing_event_outcome_after_preflight(
            existing,
            &prepared.payload,
            prepared.payload_json.as_str(),
            prepared.semantically_matching_existing.as_ref(),
            idempotency_key,
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
        payload_json,
        payload,
        semantically_matching_existing,
        candidate_gate_resolution,
        candidate_projection,
        review_projection,
        delivery_authority,
        projection,
    } = prepared;
    let sequence = next_sequence_for_task(db, task_id.as_str()).await?;

    let mut insert = Query::insert();
    insert
        .into_table(Alias::new("task_event"))
        .columns([
            Alias::new("id"),
            Alias::new("task_id"),
            Alias::new("run_id"),
            Alias::new("thread_id"),
            Alias::new("turn_id"),
            Alias::new("sequence"),
            Alias::new("event_type"),
            Alias::new("idempotency_key"),
            Alias::new("payload_json"),
            Alias::new("created_at"),
        ])
        .values_panic([
            id.clone().into(),
            task_id.clone().into(),
            run_id.clone().into(),
            thread_id.clone().into(),
            turn_id.clone().into(),
            sequence.into(),
            event_type.clone().into(),
            idempotency_key.clone().into(),
            payload_json.clone().into(),
            created_at.into(),
        ]);

    if let Err(error) = db.execute(&insert).await {
        if let Some(idempotency_key) = idempotency_key.as_deref()
            && let Some(existing) =
                find_event_by_idempotency_key(db, task_id.as_str(), idempotency_key).await?
        {
            return existing_event_outcome_after_preflight(
                existing,
                &payload,
                payload_json.as_str(),
                semantically_matching_existing.as_ref(),
                idempotency_key,
            );
        }
        return Err(error).context("failed to append task event");
    }

    Ok(AppendedTaskEvent {
        id,
        task_id,
        run_id,
        thread_id,
        turn_id,
        sequence,
        event_type,
        idempotency_key,
        payload,
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

fn existing_event_outcome_after_preflight(
    model: task_event::Model,
    attempted_payload: &TaskEventPayload,
    attempted_payload_json: &str,
    preflight: Option<&SemanticallyMatchingExistingTaskEvent>,
    idempotency_key: &str,
) -> Result<AppendedTaskEvent> {
    let payload = if model.payload_json == attempted_payload_json {
        attempted_payload.clone()
    } else if let Some(preflight) = preflight.filter(|preflight| preflight.id == model.id) {
        preflight.payload.clone()
    } else {
        // Deserializing and semantically comparing an unexpected row can be
        // arbitrarily CPU-heavy. Release the writer and let the operation's
        // next preflight resolve it through the reader pool instead.
        return Err(TaskEventIdempotencyPreflightRace {
            idempotency_key: idempotency_key.to_owned(),
        }
        .into());
    };
    Ok(appended_task_event_from_known_payload(
        model,
        payload,
        TaskEventAppendStatus::AlreadyExists,
    ))
}

fn appended_task_event_from_known_payload(
    model: task_event::Model,
    payload: TaskEventPayload,
    append_status: TaskEventAppendStatus,
) -> AppendedTaskEvent {
    AppendedTaskEvent {
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

async fn find_event_by_idempotency_key<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    idempotency_key: &str,
) -> Result<Option<task_event::Model>> {
    task_event::Entity::find()
        .filter(task_event::Column::TaskId.eq(task_id.to_owned()))
        .filter(task_event::Column::IdempotencyKey.eq(idempotency_key.to_owned()))
        .one(db)
        .await
        .context("failed to query task event by idempotency key")
}

fn existing_event_outcome(
    model: task_event::Model,
    attempted_payload: &TaskEventPayload,
    idempotency_key: &str,
) -> Result<AppendedTaskEvent> {
    let existing = appended_task_event_from_model(model, TaskEventAppendStatus::AlreadyExists)?;
    if existing.payload != *attempted_payload
        && !equivalent_cancelled_terminal_payload(&existing.payload, attempted_payload)
    {
        anyhow::bail!(
            "task event idempotency key `{}` already exists with a different payload",
            idempotency_key
        );
    }
    Ok(existing)
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

pub async fn list_events_for_task<C: ConnectionTrait>(
    db: &C,
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

    query.all(db).await.context("failed to query task events")
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

pub async fn list_pending_fanout_task_ids<C: ConnectionTrait>(
    db: &C,
    after_task_id: Option<&str>,
    limit: u64,
) -> Result<Vec<String>> {
    let mut query = task_event_fanout_cursor::Entity::find()
        .select_only()
        .column(task_event_fanout_cursor::Column::TaskId)
        .filter(Expr::cust(
            "EXISTS (SELECT 1 FROM task_event AS pending_event \
             WHERE pending_event.task_id = task_event_fanout_cursor.task_id \
             AND pending_event.sequence > task_event_fanout_cursor.last_sequence)",
        ));
    if let Some(after_task_id) = after_task_id {
        query = query.filter(task_event_fanout_cursor::Column::TaskId.gt(after_task_id.to_owned()));
    }
    query
        .order_by_asc(task_event_fanout_cursor::Column::TaskId)
        .limit(std::cmp::max(limit, 1))
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to query task event fanout backlog")
}

pub async fn find_fanout_cursor<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<Option<i64>> {
    Ok(
        task_event_fanout_cursor::Entity::find_by_id(task_id.to_owned())
            .one(db)
            .await
            .with_context(|| format!("failed to load task event fanout cursor for `{task_id}`"))?
            .map(|cursor| cursor.last_sequence),
    )
}

pub async fn initialize_fanout_cursor<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    sequence: i64,
    initialized_at: DateTimeWithTimeZone,
) -> Result<()> {
    if sequence < 0 {
        anyhow::bail!("task event fanout sequence must be non-negative");
    }

    if task_event_fanout_cursor::Entity::find_by_id(task_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to check task event fanout cursor for `{task_id}`"))?
        .is_some()
    {
        return Ok(());
    }
    if task::Entity::find_by_id(task_id.to_owned())
        .select_only()
        .column(task::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await
        .with_context(|| format!("failed to check task read model for fanout cursor `{task_id}`"))?
        .is_none()
    {
        return Ok(());
    }

    task_event_fanout_cursor::Entity::insert(task_event_fanout_cursor::ActiveModel {
        task_id: Set(task_id.to_owned()),
        last_sequence: Set(sequence),
        created_at: Set(initialized_at),
        updated_at: Set(initialized_at),
    })
    .on_conflict(
        OnConflict::column(task_event_fanout_cursor::Column::TaskId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!("failed to initialize task event fanout cursor for `{task_id}` at {sequence}")
    })?;

    Ok(())
}

pub async fn advance_fanout_cursor<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    sequence: i64,
    advanced_at: DateTimeWithTimeZone,
) -> Result<()> {
    if sequence < 0 {
        anyhow::bail!("task event fanout sequence must be non-negative");
    }

    initialize_fanout_cursor(db, task_id, sequence, advanced_at).await?;

    task_event_fanout_cursor::Entity::update_many()
        .col_expr(
            task_event_fanout_cursor::Column::LastSequence,
            sea_orm::sea_query::Expr::value(sequence),
        )
        .col_expr(
            task_event_fanout_cursor::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(advanced_at),
        )
        .filter(task_event_fanout_cursor::Column::TaskId.eq(task_id.to_owned()))
        .filter(task_event_fanout_cursor::Column::LastSequence.lt(sequence))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to advance task event fanout cursor for `{task_id}` to {sequence}")
        })?;

    Ok(())
}

pub async fn list_events_for_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_event::Model>> {
    task_event::Entity::find()
        .filter(task_event::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_event::Column::Sequence)
        .all(db)
        .await
        .context("failed to query task events for run")
}

pub async fn list_events_for_thread_turn<C: ConnectionTrait>(
    db: &C,
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

    query
        .all(db)
        .await
        .context("failed to query task events for thread/turn")
}

pub fn task_event_from_model(model: task_event::Model) -> Result<TaskEvent> {
    let payload: TaskEventPayload = serde_json::from_str(model.payload_json.as_str())
        .context("failed to decode task_event payload_json")?;
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
    model: task_event::Model,
    append_status: TaskEventAppendStatus,
) -> Result<AppendedTaskEvent> {
    let payload: TaskEventPayload = serde_json::from_str(model.payload_json.as_str())
        .context("failed to decode task_event payload_json")?;
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
        candidate_gate_resolution: None,
        candidate_projection: None,
        review_projection: None,
        delivery_authority: None,
        projection: Default::default(),
    })
}

async fn next_sequence_for_task<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("task_event"))
                .and_where(Expr::col(Alias::new("task_id")).eq(task_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max task_event sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);

    Ok(max_sequence + 1)
}
