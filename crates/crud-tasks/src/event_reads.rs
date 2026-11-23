//! Task event read projections, byte-budget paging, and payload decoding.
use anyhow::{Context, Result};
use pioneer_entity::task_event;
use pioneer_protocol::{TaskEvent, TaskEventPayload};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

use crate::event_log::EventPayload;

pub async fn find_event_by_idempotency_key<C: ConnectionTrait>(
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
pub fn task_event_metadata_query() -> sea_orm::Select<task_event::Entity> {
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

pub fn equivalent_payloads(existing: &TaskEventPayload, attempted: &TaskEventPayload) -> bool {
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

pub async fn find_queued_delivery_for_run<C: ConnectionTrait>(
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

pub async fn load_rows(
    db: &pioneer_postgres_pools::PostgresDatabase,
    query: sea_orm::Select<task_event::Entity>,
) -> Result<Vec<task_event::Model>> {
    Ok(query.all(db).await?)
}

pub async fn list_events_for_task(
    db: &pioneer_postgres_pools::PostgresDatabase,
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

fn fanout_metadata_query(
    task_id: &str,
    after: i64,
    limit: usize,
) -> sea_orm::Select<task_event::Entity> {
    // Select the persisted original length without fetching the payload.
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
pub async fn fanout_page(
    db: &pioneer_postgres_pools::PostgresDatabase,
    task_id: &str,
    after: i64,
    limit: usize,
    bytes_left: &mut usize,
    allow_oversized: bool,
) -> Result<crate::event_fanout::TaskEventFanoutPage<task_event::Model>> {
    let bytes = *bytes_left;
    let metadata = fanout_metadata_query(task_id, after, limit)
        .into_tuple::<(String, i64, i64)>()
        .all(db)
        .await?;
    use crate::event_fanout::TaskEventFanoutPage;
    let mut ids = Vec::new();
    let mut used = 0usize;
    for (id, _, length) in metadata {
        let length = usize::try_from(length).context("invalid task event byte length")?;
        if length > bytes.saturating_sub(used) {
            if ids.is_empty()
                && allow_oversized
                && length > crate::event_fanout::TASK_EVENT_FANOUT_BYTE_BUDGET
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

pub async fn list_events_for_run(
    db: &pioneer_postgres_pools::PostgresDatabase,
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
    db: &pioneer_postgres_pools::PostgresDatabase,
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

pub fn decode_event(row: &mut task_event::Model) -> Result<TaskEventPayload> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TaskError, TaskErrorClass, TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus,
    };

    #[test]
    fn fanout_metadata_selects_original_byte_length_without_payload_body() {
        use sea_orm::{DatabaseBackend, QueryTrait};

        let statement = fanout_metadata_query("task", 1, 128).build(DatabaseBackend::Postgres);
        let projection = statement.sql.split_once(" FROM ").unwrap().0;
        assert!(projection.contains("\"payload_json_bytes\""));
        assert!(!projection.contains("\"payload_json\""));
    }

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
