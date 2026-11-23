use anyhow::{Context, Result, bail};
use pioneer_entity::turn_event_projection_stream_state;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, QueryTrait, Set,
};

pub const STREAM_STATUS_HEALTHY: &str = "healthy";
pub const STREAM_STATUS_QUARANTINED: &str = "quarantined";

pub async fn ensure_healthy<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<turn_event_projection_stream_state::Model> {
    turn_event_projection_stream_state::Entity::insert(
        turn_event_projection_stream_state::ActiveModel {
            turn_id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            accepted_terminal_event_id: Set(None),
            accepted_terminal_event_type: Set(None),
            accepted_terminal_sequence: Set(None),
            projected_through_sequence: Set(0),
            receipts_compacted_through_sequence: Set(0),
            status: Set(STREAM_STATUS_HEALTHY.to_owned()),
            blocking_event_id: Set(None),
            last_error: Set(None),
            quarantined_at: Set(None),
            restored_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        },
    )
    .on_conflict(
        OnConflict::column(turn_event_projection_stream_state::Column::TurnId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!("failed to initialize projection stream state for Turn `{turn_id}`")
    })?;

    turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?
        .with_context(|| {
            format!("projection stream state for Turn `{turn_id}` is missing after initialization")
        })
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_event_projection_stream_state::Model>> {
    turn_event_projection_stream_state::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load projection stream state for Turn `{turn_id}`"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnProjectionStreamHealth {
    Healthy,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnProjectionStreamStateRecord {
    pub thread_id: String,
    pub turn_id: String,
    /// Authoritative highest contiguous projected event sequence for this Turn.
    pub projected_through_sequence: i64,
    pub health: TurnProjectionStreamHealth,
    pub blocking_event_id: Option<String>,
    pub last_error: Option<String>,
    pub quarantined_at_unix: Option<i64>,
    pub restored_at_unix: Option<i64>,
    pub updated_at_unix_micros: i64,
}

pub async fn get_turn_event_projection_stream_state<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnProjectionStreamStateRecord>> {
    let Some(state) = find(db, turn_id).await? else {
        return Ok(None);
    };

    let health = match state.status.as_str() {
        STREAM_STATUS_HEALTHY => TurnProjectionStreamHealth::Healthy,
        STREAM_STATUS_QUARANTINED => TurnProjectionStreamHealth::Quarantined,
        unknown => {
            bail!("projection stream for Turn `{turn_id}` has unknown health status `{unknown}`")
        }
    };

    Ok(Some(TurnProjectionStreamStateRecord {
        thread_id: state.thread_id,
        turn_id: state.turn_id,
        projected_through_sequence: state.projected_through_sequence,
        health,
        blocking_event_id: state.blocking_event_id,
        last_error: state.last_error,
        quarantined_at_unix: state.quarantined_at.map(|value| value.timestamp()),
        restored_at_unix: state.restored_at.map(|value| value.timestamp()),
        updated_at_unix_micros: state.updated_at.timestamp_micros(),
    }))
}

/// One reader snapshot proves scope, immutable ownership, projected admission,
/// stream health and summary absence. Never synthesize a zero-revision tombstone.
pub async fn find_detached_work_tombstone<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<(i64, i64)>> {
    use pioneer_entity::{thread, turn, turn_work_projection};

    let state = turn_event_projection_stream_state::Entity::find_by_id(turn_id.to_owned())
        .filter(turn_event_projection_stream_state::Column::ThreadId.eq(thread_id))
        .filter(turn_event_projection_stream_state::Column::Status.eq(STREAM_STATUS_HEALTHY))
        .filter(turn_event_projection_stream_state::Column::BlockingEventId.is_null())
        .filter(turn_event_projection_stream_state::Column::ProjectedThroughSequence.gt(0))
        .filter(
            turn_event_projection_stream_state::Column::TurnId.in_subquery(
                turn::Entity::find_by_id(turn_id.to_owned())
                    .select_only()
                    .column(turn::Column::Id)
                    .filter(turn::Column::ThreadId.eq(thread_id))
                    .filter(
                        turn::Column::WorkOwner
                            .eq(pioneer_protocol::TurnWorkOwner::DetachedTask.as_db_str()),
                    )
                    .filter(
                        turn::Column::ThreadId.in_subquery(
                            thread::Entity::find_by_id(thread_id.to_owned())
                                .select_only()
                                .column(thread::Column::Id)
                                .filter(thread::Column::WorkspaceId.eq(workspace_id))
                                .into_query(),
                        ),
                    )
                    .into_query(),
            ),
        )
        .filter(
            turn_event_projection_stream_state::Column::TurnId.not_in_subquery(
                turn_work_projection::Entity::find_by_id(turn_id.to_owned())
                    .select_only()
                    .column(turn_work_projection::Column::TurnId)
                    .into_query(),
            ),
        )
        .one(db)
        .await?;
    Ok(state.map(|state| {
        (
            state.projected_through_sequence,
            state.updated_at.timestamp_micros(),
        )
    }))
}

pub async fn advance_projected_through<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    expected_current_sequence: i64,
    projected_through_sequence: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<turn_event_projection_stream_state::Model>> {
    if expected_current_sequence < 0 || projected_through_sequence <= expected_current_sequence {
        bail!(
            "projection watermark for Turn `{turn_id}` cannot advance from `{expected_current_sequence}` through `{projected_through_sequence}`"
        );
    }

    let changed = turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            turn_event_projection_stream_state::Column::ProjectedThroughSequence,
            Expr::value(projected_through_sequence),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_event_projection_stream_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_event_projection_stream_state::Column::ProjectedThroughSequence
                .eq(expected_current_sequence),
        )
        .exec_with_returning(db)
        .await
        .with_context(|| {
            format!(
                "failed to advance projection watermark for Turn `{turn_id}` from `{expected_current_sequence}` through `{projected_through_sequence}`"
            )
        })?;
    Ok(changed.into_iter().next())
}

/// Durable transition facts for a dependent projection using the same transaction.
pub struct StreamChange {
    pub before: Option<turn_event_projection_stream_state::Model>,
    pub after: Option<turn_event_projection_stream_state::Model>,
}

pub async fn quarantine<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    blocking_event_id: &str,
    last_error: String,
    quarantined_at: DateTimeWithTimeZone,
) -> Result<Option<StreamChange>> {
    let current = ensure_healthy(db, thread_id, turn_id, quarantined_at).await?;
    if current.thread_id != thread_id {
        bail!(
            "projection stream `{turn_id}` belongs to thread `{}`, not `{thread_id}`",
            current.thread_id
        );
    }
    if current.status == STREAM_STATUS_QUARANTINED {
        if current.blocking_event_id.as_deref() == Some(blocking_event_id) {
            return Ok(None);
        }
        bail!(
            "projection stream `{turn_id}` is already quarantined by event `{}` instead of causal head `{blocking_event_id}`",
            current.blocking_event_id.as_deref().unwrap_or("<missing>")
        );
    }
    if current.status != STREAM_STATUS_HEALTHY {
        bail!(
            "projection stream `{turn_id}` has unknown health status `{}`",
            current.status
        );
    }

    let changed = turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            turn_event_projection_stream_state::Column::Status,
            Expr::value(STREAM_STATUS_QUARANTINED.to_owned()),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::BlockingEventId,
            Expr::value(Some(blocking_event_id.to_owned())),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::LastError,
            Expr::value(Some(last_error)),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::QuarantinedAt,
            Expr::value(Some(quarantined_at)),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::RestoredAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::UpdatedAt,
            Expr::value(quarantined_at),
        )
        .filter(turn_event_projection_stream_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_event_projection_stream_state::Column::Status.eq(STREAM_STATUS_HEALTHY.to_owned()),
        )
        .exec(db)
        .await
        .with_context(|| format!("failed to quarantine projection stream for Turn `{turn_id}`"))?
        .rows_affected
        > 0;

    if changed {
        let mut new = current.clone();
        new.status = STREAM_STATUS_QUARANTINED.to_owned();
        return Ok(Some(StreamChange {
            before: Some(current),
            after: Some(new),
        }));
    }

    let current = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?
        .with_context(|| format!("projection stream `{turn_id}` disappeared during quarantine"))?;
    if current.status == STREAM_STATUS_QUARANTINED
        && current.blocking_event_id.as_deref() == Some(blocking_event_id)
    {
        return Ok(None);
    }
    bail!("projection stream `{turn_id}` changed concurrently during quarantine")
}

pub async fn restore<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    blocking_event_id: &str,
    restored_at: DateTimeWithTimeZone,
) -> Result<Option<StreamChange>> {
    let old = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?;
    let changed = turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            turn_event_projection_stream_state::Column::Status,
            Expr::value(STREAM_STATUS_HEALTHY.to_owned()),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::BlockingEventId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::QuarantinedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::RestoredAt,
            Expr::value(Some(restored_at)),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::UpdatedAt,
            Expr::value(restored_at),
        )
        .filter(turn_event_projection_stream_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_event_projection_stream_state::Column::Status
                .eq(STREAM_STATUS_QUARANTINED.to_owned()),
        )
        .filter(
            turn_event_projection_stream_state::Column::BlockingEventId
                .eq(blocking_event_id.to_owned()),
        )
        .exec(db)
        .await
        .with_context(|| format!("failed to restore projection stream for Turn `{turn_id}`"))?
        .rows_affected
        > 0;
    if changed {
        let new = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
            .lock_exclusive()
            .one(db)
            .await?;
        return Ok(Some(StreamChange {
            before: old,
            after: new,
        }));
    }
    Ok(None)
}

pub async fn accept_terminal<C: ConnectionTrait>(
    db: &C,
    event: &crate::events::AppendedTurnEvent,
) -> Result<Option<StreamChange>> {
    if !matches!(
        &event.payload,
        crate::events::CanonicalTurnEventPayload::TurnCompleted(_)
            | crate::events::CanonicalTurnEventPayload::TurnFailed(_)
            | crate::events::CanonicalTurnEventPayload::TurnBlocked(_)
    ) {
        return Ok(None);
    }
    let old = turn_event_projection_stream_state::Entity::find_by_id(&event.turn_id)
        .lock_exclusive()
        .one(db)
        .await?;
    let changed = turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalEventId,
            Expr::value(event.id.clone()),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalEventType,
            Expr::value(event.payload.event_type().to_owned()),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalSequence,
            Expr::value(event.sequence),
        )
        .filter(turn_event_projection_stream_state::Column::TurnId.eq(event.turn_id.clone()))
        .filter(turn_event_projection_stream_state::Column::ThreadId.eq(event.thread_id.clone()))
        .filter(turn_event_projection_stream_state::Column::AcceptedTerminalEventId.is_null())
        .filter(turn_event_projection_stream_state::Column::AcceptedTerminalEventType.is_null())
        .filter(turn_event_projection_stream_state::Column::AcceptedTerminalSequence.is_null())
        .exec(db)
        .await?
        .rows_affected;
    anyhow::ensure!(
        changed == 1,
        "terminal append conflicts with accepted canonical result"
    );
    let new = turn_event_projection_stream_state::Entity::find_by_id(&event.turn_id)
        .lock_exclusive()
        .one(db)
        .await?;
    Ok(Some(StreamChange {
        before: old,
        after: new,
    }))
}

pub async fn clear_confirmed_blocked_for_resume<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<StreamChange>> {
    let Some(current) = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    anyhow::ensure!(
        current.thread_id == thread_id,
        "blocked resume crosses projection stream scope"
    );
    if current.accepted_terminal_event_id.is_none()
        && current.accepted_terminal_event_type.is_none()
        && current.accepted_terminal_sequence.is_none()
    {
        return Ok(None);
    } // Legacy marker absence.
    let sequence = current
        .accepted_terminal_sequence
        .context("blocked marker has no accepted sequence")?;
    let id = current
        .accepted_terminal_event_id
        .clone()
        .context("blocked marker has no canonical identity")?;
    anyhow::ensure!(
        current.accepted_terminal_event_type.as_deref() == Some("turn/blocked")
            && sequence > 0
            && sequence <= current.projected_through_sequence,
        "blocked resume conflicts with unconfirmed or non-blocked terminal acceptance"
    );
    let changed = turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalEventId,
            Expr::value(None::<String>),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalEventType,
            Expr::value(None::<String>),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::AcceptedTerminalSequence,
            Expr::value(None::<i64>),
        )
        .col_expr(
            turn_event_projection_stream_state::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(turn_event_projection_stream_state::Column::TurnId.eq(turn_id))
        .filter(turn_event_projection_stream_state::Column::AcceptedTerminalEventId.eq(id))
        .filter(
            turn_event_projection_stream_state::Column::AcceptedTerminalEventType
                .eq("turn/blocked"),
        )
        .filter(turn_event_projection_stream_state::Column::AcceptedTerminalSequence.eq(sequence))
        .exec(db)
        .await?
        .rows_affected;
    anyhow::ensure!(
        changed == 1,
        "blocked terminal marker changed while resuming"
    );
    let new = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?;
    Ok(Some(StreamChange {
        before: Some(current),
        after: new,
    }))
}
